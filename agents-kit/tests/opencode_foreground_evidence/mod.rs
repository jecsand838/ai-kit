//! Private, case-local observation of the native foreground HTTP exchange.
//! The relay never parses data in order to forward it. Framing is interpreted
//! separately from bounded copies, after the original bytes have been relayed.

use std::collections::BTreeMap;
use std::io::{self, Write};
use std::net::{Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::native_cleanup::NativeCleanup;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;

const COPY_BYTES: usize = 8 << 10;
const CAPTURE_BYTES: usize = 64 << 20;
const MAX_CONNECTIONS: usize = 16;
const MAX_DESCRIPTORS: usize = 128;
const MAX_REQUEST_BYTES: usize = 2 << 20;
const MAX_RESPONSE_BYTES: usize = 16 << 20;
const MAX_HEADERS: usize = 64 << 10;
const MAX_CHUNK_LINE: usize = 8 << 10;
const MAX_FRAMING: usize = 256 << 10;

pub(crate) struct OpenCodeForegroundCapture {
    address: SocketAddr,
    observation: Arc<Mutex<ForegroundObservation>>,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<io::Result<()>>>,
    connections: Arc<Mutex<Vec<JoinHandle<io::Result<()>>>>>,
    settling: Option<JoinHandle<io::Result<()>>>,
}

#[derive(Default)]
struct ForegroundObservation {
    connections: BTreeMap<usize, CapturedConnection>,
    expectations: Vec<PromptExpectation>,
    retained: usize,
    failure: Option<&'static str>,
    settled: bool,
    joined_connections: usize,
    listener_joined: bool,
}

struct PromptExpectation {
    phase: String,
    prompt: String,
    model: String,
    expected: String,
    public: Option<String>,
}

#[derive(Default)]
struct CapturedConnection {
    request: CapturedDirection,
    response: CapturedDirection,
    failure: Option<&'static str>,
}

#[derive(Default)]
struct CapturedDirection {
    bytes: Vec<u8>,
    forwarded: usize,
    eof: bool,
    truncated: bool,
}

#[derive(Clone, Copy)]
enum Direction {
    Request,
    Response,
}

impl OpenCodeForegroundCapture {
    pub(crate) async fn start(native: SocketAddr) -> io::Result<Self> {
        if !native.ip().is_loopback() || native.port() == 0 {
            return Err(io::Error::other(
                "foreground capture requires native loopback",
            ));
        }
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let observation = Arc::new(Mutex::new(ForegroundObservation::default()));
        let connections = Arc::new(Mutex::new(Vec::new()));
        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(Self::serve(
            listener,
            native,
            Arc::clone(&observation),
            stop.clone(),
            stopped,
            Arc::clone(&connections),
        ));
        Ok(Self {
            address,
            observation,
            stop,
            task: Some(task),
            connections,
            settling: None,
        })
    }

    pub(crate) fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }

    pub(crate) fn expect(
        &self,
        phase: &str,
        prompt: &str,
        model: &str,
        expected: &str,
    ) -> Result<(), String> {
        let mut observation = self
            .observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(phase, "unary" | "stream")
            || prompt.len() > MAX_REQUEST_BYTES
            || model.len() > 1024
            || expected.len() > 64 * 1024
            || prompt.is_empty()
            || model.is_empty()
            || expected.is_empty()
            || observation.expectations.len() == 2
            || observation
                .expectations
                .iter()
                .any(|item| item.phase == phase || item.prompt == prompt)
        {
            return Err("foreground expectation invalid or repeated".to_owned());
        }
        let retained = phase.len() + prompt.len() + model.len() + expected.len();
        if retained > CAPTURE_BYTES.saturating_sub(observation.retained) {
            return Err("foreground expectation exceeds capture budget".to_owned());
        }
        observation.retained += retained;
        observation.expectations.push(PromptExpectation {
            phase: phase.to_owned(),
            prompt: prompt.to_owned(),
            model: model.to_owned(),
            expected: expected.to_owned(),
            public: None,
        });
        Ok(())
    }

    pub(crate) fn observe(&self, phase: &str, public_output: &str) -> Value {
        let mut observation = self
            .observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if public_output.len() > MAX_RESPONSE_BYTES
            || public_output.len() > CAPTURE_BYTES.saturating_sub(observation.retained)
        {
            observation.failure.get_or_insert("public_output_bound");
        } else if let Some(expectation) = observation
            .expectations
            .iter_mut()
            .find(|item| item.phase == phase)
        {
            if expectation.public.is_some() {
                observation.failure.get_or_insert("public_output_repeated");
            } else {
                expectation.public = Some(public_output.to_owned());
                observation.retained += public_output.len();
            }
        } else {
            observation
                .failure
                .get_or_insert("public_output_unregistered");
        }
        observation.summary()
    }

    pub(crate) fn summary(&self) -> Value {
        self.observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .summary()
    }

    pub(crate) async fn finish(&mut self, cleanup: &mut NativeCleanup) {
        let _ = self.stop.send(true);
        if let Some(task) = self.task.as_mut() {
            Self::join_owned(task, cleanup).await;
            self.task.take();
            self.observation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .listener_joined = true;
        }
        // The listener has stopped publishing new handles. Keep the currently
        // awaited handle in self, not in a cancellable cleanup future's locals.
        loop {
            if self.settling.is_none() {
                self.settling = self
                    .connections
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .pop();
            }
            let Some(task) = self.settling.as_mut() else {
                break;
            };
            Self::join_owned(task, cleanup).await;
            self.settling.take();
            self.observation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .joined_connections += 1;
        }
        self.observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .settled = true;
    }

    async fn join_owned(task: &mut JoinHandle<io::Result<()>>, cleanup: &mut NativeCleanup) {
        let mut joined = false;
        cleanup
            .settle(async {
                let result = (&mut *task).await;
                joined = true;
                result.map_err(|_| io::Error::other("foreground task join failed"))?
            })
            .await;
        if !joined {
            task.abort();
            // Abort is only a request. Consume the original join even after the
            // cleanup observation expired; these tasks contain only bounded
            // synchronous copies and cancellable socket waits. No child handle
            // is owned by the listener task, so aborting it cannot lose joins.
            let result = (&mut *task).await;
            cleanup
                .settle(async {
                    match result {
                        Ok(result) => result,
                        Err(error) if error.is_cancelled() => Ok(()),
                        Err(_) => Err(io::Error::other("foreground aborted task join failed")),
                    }
                })
                .await;
        }
    }

    pub(crate) fn stage_private_bodies(&self, directory: &Path) -> io::Result<()> {
        let observation = self
            .observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !observation.settled {
            return Err(io::Error::other(
                "foreground capture must settle before staging",
            ));
        }
        observation.stage(directory)
    }

    async fn serve(
        listener: TcpListener,
        native: SocketAddr,
        observation: Arc<Mutex<ForegroundObservation>>,
        stop: watch::Sender<bool>,
        mut stopped: watch::Receiver<bool>,
        connections: Arc<Mutex<Vec<JoinHandle<io::Result<()>>>>>,
    ) -> io::Result<()> {
        let mut ordinal = 0;
        let mut failed = false;
        loop {
            tokio::select! {
                biased;
                _ = stopped.changed() => break,
                accepted = listener.accept() => {
                    let socket = match accepted {
                        Ok((socket, _)) => socket,
                        Err(_) => {
                            observation.lock().unwrap_or_else(std::sync::PoisonError::into_inner).failure.get_or_insert("listener_failed");
                            failed = true;
                            break;
                        }
                    };
                    let active = connections.lock().unwrap_or_else(std::sync::PoisonError::into_inner).iter().filter(|task| !task.is_finished()).count();
                    if active >= MAX_CONNECTIONS || ordinal >= MAX_DESCRIPTORS {
                        observation.lock().unwrap_or_else(std::sync::PoisonError::into_inner).failure.get_or_insert("connection_bound");
                        failed = true;
                        break;
                    }
                    let id = ordinal;
                    ordinal += 1;
                    observation.lock().unwrap_or_else(std::sync::PoisonError::into_inner).connections.insert(id, CapturedConnection::default());
                    let task = tokio::spawn(CapturedConnection::relay(id, socket, native, Arc::clone(&observation), stopped.clone()));
                    connections.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(task);
                }
            }
        }
        drop(listener);
        let _ = stop.send(true);
        if failed {
            Err(io::Error::other("foreground relay did not settle cleanly"))
        } else {
            Ok::<(), io::Error>(())
        }
    }
}

impl CapturedConnection {
    fn direction(&mut self, direction: Direction) -> &mut CapturedDirection {
        match direction {
            Direction::Request => &mut self.request,
            Direction::Response => &mut self.response,
        }
    }

    async fn relay(
        id: usize,
        mut downstream: TcpStream,
        native: SocketAddr,
        observation: Arc<Mutex<ForegroundObservation>>,
        mut stopped: watch::Receiver<bool>,
    ) -> io::Result<()> {
        let operation = async {
            let mut upstream = TcpStream::connect(native).await?;
            let (client_read, client_write) = downstream.split();
            let (native_read, native_write) = upstream.split();
            tokio::try_join!(
                Self::pump(
                    id,
                    Direction::Request,
                    client_read,
                    native_write,
                    &observation
                ),
                Self::pump(
                    id,
                    Direction::Response,
                    native_read,
                    client_write,
                    &observation
                ),
            )?;
            Ok::<(), io::Error>(())
        };
        let result = tokio::select! {
            biased;
            _ = stopped.changed() => Ok(()),
            result = operation => result,
        };
        if result.is_err() {
            if let Some(connection) = observation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .connections
                .get_mut(&id)
            {
                connection.failure = Some("socket_forward_failed");
            }
        }
        // A peer reset can be an ordinary later SSE shutdown. The observation
        // records the error; only complete frames whose byte endpoints were
        // forwarded can establish a foreground boundary. Do not replace the
        // scenario's original failure with a second socket error.
        Ok(())
    }

    async fn pump(
        id: usize,
        direction: Direction,
        mut reader: tokio::net::tcp::ReadHalf<'_>,
        mut writer: tokio::net::tcp::WriteHalf<'_>,
        observation: &Arc<Mutex<ForegroundObservation>>,
    ) -> io::Result<()> {
        let mut buffer = [0; COPY_BYTES];
        loop {
            let count = reader.read(&mut buffer).await?;
            {
                let mut observation = observation
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let available = CAPTURE_BYTES.saturating_sub(observation.retained);
                let retained = count.min(available);
                observation.retained += retained;
                if retained < count {
                    observation.failure.get_or_insert("capture_byte_bound");
                }
                if let Some(connection) = observation.connections.get_mut(&id) {
                    let captured = connection.direction(direction);
                    captured.bytes.extend_from_slice(&buffer[..retained]);
                    captured.truncated |= retained < count;
                    captured.eof |= count == 0;
                }
            }
            if count == 0 {
                return writer.shutdown().await;
            }
            let mut offset = 0;
            while offset < count {
                let written = writer.write(&buffer[offset..count]).await?;
                if written == 0 {
                    return Err(io::Error::from(io::ErrorKind::WriteZero));
                }
                offset += written;
                if let Some(connection) = observation
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .connections
                    .get_mut(&id)
                {
                    connection.direction(direction).forwarded += written;
                }
            }
        }
    }
}

impl ForegroundObservation {
    fn summary(&self) -> Value {
        let wire = CapturedHttp::collect(self);
        let phases: Vec<_> = self
            .expectations
            .iter()
            .map(|expected| wire.compare(expected).summary())
            .collect();
        json!({
            "version":1,
            "capture_complete":self.settled && self.failure.is_none() && wire.reasons.is_empty()
                && !phases.is_empty() && phases.iter().all(|phase| phase["complete"] == true),
            "transport_settled":self.settled,
            "reasons":self.failure.iter().copied().chain(wire.reasons.iter().copied()).collect::<Vec<_>>(),
            "retained_observation_bytes":self.retained,
            "connections":self.connections.len(),
            "joined_connections":self.joined_connections,
            "listener_joined":self.listener_joined,
            "phases":phases,
        })
    }

    fn stage(&self, directory: &Path) -> io::Result<()> {
        let wire = CapturedHttp::collect(self);
        let root = directory.join("foreground");
        PrivateBodyFiles::create_directory(&root)?;
        let mut phases = Vec::new();
        for expected in &self.expectations {
            let comparison = wire.compare(expected);
            let mut phase = comparison.summary();
            for (kind, body) in [
                (
                    "request",
                    comparison
                        .foreground
                        .map(|exchange| exchange.request.as_slice()),
                ),
                (
                    "response",
                    comparison
                        .foreground
                        .map(|exchange| exchange.response.as_slice()),
                ),
                (
                    "parent",
                    comparison
                        .parent
                        .map(|exchange| exchange.response.as_slice()),
                ),
            ] {
                let key = format!("{kind}_file");
                if let Some(body) = body {
                    let name = format!("{}.{kind}.json", expected.phase);
                    PrivateBodyFiles::write(&root.join(&name), body)?;
                    phase[key] = json!(name);
                } else {
                    phase[key] = Value::Null;
                }
            }
            phases.push(phase);
        }
        let manifest = json!({
            "version":1,"transport_settled":self.settled,
            "capture_complete":self.settled && self.failure.is_none() && wire.reasons.is_empty()
                && !phases.is_empty() && phases.iter().all(|phase| phase["complete"] == true),
            "reasons":self.failure.iter().copied().chain(wire.reasons.iter().copied()).collect::<Vec<_>>(),
            "phases":phases,
        });
        PrivateBodyFiles::write(
            &root.join("manifest.json"),
            &serde_json::to_vec_pretty(&manifest)?,
        )
    }
}

struct PrivateBodyFiles;

impl PrivateBodyFiles {
    fn create_directory(path: &Path) -> io::Result<()> {
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(path)
    }

    fn write(path: &Path, bytes: &[u8]) -> io::Result<()> {
        let mut options = std::fs::OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options.open(path)?.write_all(bytes)
    }
}

/// Offline HTTP/1 framing over captured bytes. It never creates a request,
/// follows a redirect, decompresses content, or supplies bytes to the relay.
struct CapturedHttp {
    exchanges: Vec<ForegroundExchange>,
    reasons: Vec<&'static str>,
}

struct ForegroundExchange {
    route: NativeRoute,
    request: Vec<u8>,
    response: Vec<u8>,
    status: u16,
    supported: bool,
    request_forwarded: bool,
    response_forwarded: bool,
    response_complete: bool,
}

enum NativeRoute {
    Foreground { session: String },
    Parent { session: String, message: String },
    Other,
}

struct HttpFrame<'a> {
    method: &'a str,
    target: &'a str,
    status: u16,
    supported: bool,
    body: Vec<u8>,
    end: usize,
    complete: bool,
    framing_bytes: usize,
}

enum HttpBodyFraming {
    Empty,
    Length(usize),
    Chunked,
    Close,
}

impl NativeRoute {
    fn parse(method: &str, target: &str) -> Self {
        let path = target.split('?').next().unwrap_or(target);
        let segments: Vec<_> = path.split('/').collect();
        match segments.as_slice() {
            ["", "session", session, "message"]
                if method == "POST" && Self::valid_identity(session) =>
            {
                Self::Foreground {
                    session: (*session).to_owned(),
                }
            }
            ["", "session", session, "message", message]
                if method == "GET"
                    && Self::valid_identity(session)
                    && Self::valid_identity(message) =>
            {
                Self::Parent {
                    session: (*session).to_owned(),
                    message: (*message).to_owned(),
                }
            }
            _ => Self::Other,
        }
    }

    fn valid_identity(identity: &str) -> bool {
        !identity.is_empty()
            && identity.len() <= 256
            && identity
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    }

    fn selected(&self) -> bool {
        !matches!(self, Self::Other)
    }
}

impl<'a> HttpFrame<'a> {
    fn parse(
        bytes: &'a [u8],
        start: usize,
        request: bool,
        response_method: &str,
        eof: bool,
        retain: bool,
        budget: usize,
    ) -> Result<Option<Self>, &'static str> {
        let remaining = &bytes[start..];
        let Some(header_size) = remaining
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|position| position + 4)
        else {
            return if remaining.len() > MAX_HEADERS {
                Err("http_header_bound")
            } else {
                Ok(None)
            };
        };
        if header_size > MAX_HEADERS {
            return Err("http_header_bound");
        }
        let header =
            std::str::from_utf8(&remaining[..header_size]).map_err(|_| "http_header_encoding")?;
        let mut lines = header[..header.len() - 4].split("\r\n");
        let first = lines.next().ok_or("http_start_line")?;
        let mut method = "";
        let mut target = "";
        let mut status = 0;
        if request {
            let parts: Vec<_> = first.split(' ').collect();
            let [selected_method, selected_target, version] = parts.as_slice() else {
                return Err("http_request_line");
            };
            if !matches!(*version, "HTTP/1.0" | "HTTP/1.1")
                || selected_method.is_empty()
                || !selected_method
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase())
                || !selected_target.starts_with('/')
                || selected_target.len() > MAX_CHUNK_LINE
            {
                return Err("http_request_line");
            }
            method = selected_method;
            target = selected_target;
        } else {
            let mut parts = first.splitn(3, ' ');
            if !matches!(parts.next(), Some("HTTP/1.0" | "HTTP/1.1")) {
                return Err("http_response_protocol");
            }
            let code = parts.next().ok_or("http_response_status")?;
            if code.len() != 3 || !code.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err("http_response_status");
            }
            status = code.parse().map_err(|_| "http_response_status")?;
            if !(100..600).contains(&status) || status == 101 {
                return Err("http_response_status");
            }
        }
        let mut length = None;
        let mut transfer = None;
        let mut supported = true;
        for line in lines {
            let (name, value) = line.split_once(':').ok_or("http_header_shape")?;
            if name.is_empty()
                || name.bytes().any(|byte| byte <= b' ' || byte >= 127)
                || value.bytes().any(|byte| byte < b' ' && byte != b'\t')
            {
                return Err("http_header_shape");
            }
            let value = value.trim_matches([' ', '\t']);
            if name.eq_ignore_ascii_case("content-length") {
                if length.is_some()
                    || value.is_empty()
                    || !value.bytes().all(|byte| byte.is_ascii_digit())
                {
                    return Err("http_content_length");
                }
                length = Some(value.parse::<usize>().map_err(|_| "http_content_length")?);
            } else if name.eq_ignore_ascii_case("transfer-encoding") {
                if transfer.is_some() || !value.eq_ignore_ascii_case("chunked") {
                    return Err("http_transfer_encoding");
                }
                transfer = Some(());
            } else if name.eq_ignore_ascii_case("content-encoding")
                && !value.eq_ignore_ascii_case("identity")
            {
                supported = false;
            }
        }
        if length.is_some() && transfer.is_some() {
            return Err("http_ambiguous_framing");
        }
        let no_body =
            !request && (status < 200 || matches!(status, 204 | 304) || response_method == "HEAD");
        let framing = if no_body {
            HttpBodyFraming::Empty
        } else if transfer.is_some() {
            HttpBodyFraming::Chunked
        } else if let Some(length) = length {
            HttpBodyFraming::Length(length)
        } else if request {
            HttpBodyFraming::Empty
        } else {
            HttpBodyFraming::Close
        };
        let body_start = start + header_size;
        let limit = if request {
            MAX_REQUEST_BYTES
        } else {
            MAX_RESPONSE_BYTES
        };
        let Some((body, end, complete)) =
            framing.decode(bytes, body_start, eof, limit, header_size, retain, budget)?
        else {
            return Ok(None);
        };
        let framing_bytes = end - start - body.len();
        Ok(Some(Self {
            method,
            target,
            status,
            supported,
            body,
            end,
            complete,
            framing_bytes,
        }))
    }
}

impl HttpBodyFraming {
    fn decode(
        &self,
        bytes: &[u8],
        start: usize,
        eof: bool,
        limit: usize,
        mut framing_bytes: usize,
        retain: bool,
        budget: usize,
    ) -> Result<Option<(Vec<u8>, usize, bool)>, &'static str> {
        match self {
            Self::Empty => Ok(Some((Vec::new(), start, true))),
            Self::Length(length) => {
                if *length > limit {
                    return Err("http_body_bound");
                }
                let end = start.checked_add(*length).ok_or("http_body_bound")?;
                if end > bytes.len() {
                    if retain && bytes.len() - start > budget {
                        return Err("capture_materialization_bound");
                    }
                    return Ok(Some((
                        if retain {
                            bytes[start..].to_vec()
                        } else {
                            Vec::new()
                        },
                        bytes.len(),
                        false,
                    )));
                }
                if retain && *length > budget {
                    return Err("capture_materialization_bound");
                }
                Ok(Some((
                    if retain {
                        bytes[start..end].to_vec()
                    } else {
                        Vec::new()
                    },
                    end,
                    true,
                )))
            }
            Self::Close => {
                if bytes.len() - start > limit {
                    return Err("http_body_bound");
                }
                if retain && bytes.len() - start > budget {
                    return Err("capture_materialization_bound");
                }
                Ok(Some((
                    if retain {
                        bytes[start..].to_vec()
                    } else {
                        Vec::new()
                    },
                    bytes.len(),
                    eof,
                )))
            }
            Self::Chunked => {
                let mut cursor = start;
                let mut length = 0usize;
                let mut body = Vec::new();
                loop {
                    let Some(line_end) = bytes[cursor..]
                        .windows(2)
                        .position(|window| window == b"\r\n")
                    else {
                        return if bytes.len() - cursor > MAX_CHUNK_LINE {
                            Err("http_chunk_line_bound")
                        } else {
                            Ok(Some((body, bytes.len(), false)))
                        };
                    };
                    if line_end > MAX_CHUNK_LINE {
                        return Err("http_chunk_line_bound");
                    }
                    framing_bytes += line_end + 2;
                    if framing_bytes > MAX_FRAMING {
                        return Err("http_framing_bound");
                    }
                    let line = std::str::from_utf8(&bytes[cursor..cursor + line_end])
                        .map_err(|_| "http_chunk_line")?;
                    let size = line.split(';').next().ok_or("http_chunk_size")?;
                    if size.is_empty() || !size.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                        return Err("http_chunk_size");
                    }
                    let size = usize::from_str_radix(size, 16).map_err(|_| "http_chunk_size")?;
                    cursor += line_end + 2;
                    if size == 0 {
                        let mut trailers = 0;
                        loop {
                            let Some(end) = bytes[cursor..]
                                .windows(2)
                                .position(|window| window == b"\r\n")
                            else {
                                return if bytes.len() - cursor + trailers > MAX_HEADERS {
                                    Err("http_trailer_bound")
                                } else {
                                    Ok(Some((body, bytes.len(), false)))
                                };
                            };
                            trailers += end + 2;
                            framing_bytes += end + 2;
                            if trailers > MAX_HEADERS || framing_bytes > MAX_FRAMING {
                                return Err("http_trailer_bound");
                            }
                            if end > 0 && !bytes[cursor..cursor + end].contains(&b':') {
                                return Err("http_trailer_shape");
                            }
                            cursor += end + 2;
                            if end == 0 {
                                return Ok(Some((body, cursor, true)));
                            }
                        }
                    }
                    length = length.checked_add(size).ok_or("http_body_bound")?;
                    if length > limit {
                        return Err("http_body_bound");
                    }
                    if retain && length > budget {
                        return Err("capture_materialization_bound");
                    }
                    let end = cursor
                        .checked_add(size)
                        .and_then(|end| end.checked_add(2))
                        .ok_or("http_body_bound")?;
                    if end > bytes.len() {
                        if retain {
                            let available_end = bytes.len().min(cursor + size);
                            body.extend_from_slice(&bytes[cursor..available_end]);
                        }
                        return Ok(Some((body, bytes.len(), false)));
                    }
                    if &bytes[end - 2..end] != b"\r\n" {
                        return Err("http_chunk_terminator");
                    }
                    if retain {
                        body.extend_from_slice(&bytes[cursor..end - 2]);
                    }
                    cursor = end;
                    framing_bytes += 2;
                }
            }
        }
    }
}

impl CapturedHttp {
    fn collect(observation: &ForegroundObservation) -> Self {
        let mut capture = Self {
            exchanges: Vec::new(),
            reasons: Vec::new(),
        };
        let mut descriptors = 0;
        let mut budget = CAPTURE_BYTES.saturating_sub(observation.retained);
        for connection in observation.connections.values() {
            if let Err(reason) = capture.connection(connection, &mut descriptors, &mut budget) {
                if !capture.reasons.contains(&reason) {
                    capture.reasons.push(reason);
                }
            }
        }
        capture
    }

    fn connection(
        &mut self,
        connection: &CapturedConnection,
        descriptors: &mut usize,
        budget: &mut usize,
    ) -> Result<(), &'static str> {
        let mut request_cursor = 0;
        let mut response_cursor = 0;
        while request_cursor < connection.request.bytes.len() {
            if *descriptors >= MAX_DESCRIPTORS {
                return Err("http_descriptor_bound");
            }
            *descriptors += 1;
            let Some(request) = HttpFrame::parse(
                &connection.request.bytes,
                request_cursor,
                true,
                "",
                connection.request.eof,
                true,
                *budget,
            )?
            else {
                return Err("http_request_incomplete");
            };
            let route = NativeRoute::parse(request.method, request.target);
            if !request.complete {
                return Err("http_request_incomplete");
            }
            let mut framing_bytes = request.framing_bytes;
            request_cursor = request.end;
            let response = loop {
                let parsed = HttpFrame::parse(
                    &connection.response.bytes,
                    response_cursor,
                    false,
                    request.method,
                    connection.response.eof,
                    route.selected(),
                    budget.saturating_sub(request.body.len()),
                );
                let response = match parsed {
                    Ok(Some(response)) => response,
                    Ok(None) if !route.selected() => return Ok(()),
                    Ok(None) => return Err("http_response_incomplete"),
                    Err(_) if !route.selected() => return Ok(()),
                    Err(reason) => return Err(reason),
                };
                if route.selected() {
                    framing_bytes = framing_bytes
                        .checked_add(response.framing_bytes)
                        .ok_or("http_framing_bound")?;
                    if framing_bytes > MAX_FRAMING {
                        return Err("http_framing_bound");
                    }
                }
                response_cursor = response.end;
                if response.status >= 200 {
                    break response;
                }
            };
            if route.selected() {
                let complete = response.complete;
                *budget = budget.saturating_sub(request.body.len() + response.body.len());
                self.exchanges.push(ForegroundExchange {
                    route,
                    request: request.body,
                    response: response.body,
                    status: response.status,
                    supported: request.supported && response.supported,
                    request_forwarded: request.end <= connection.request.forwarded
                        && !connection.request.truncated,
                    response_forwarded: response.end <= connection.response.forwarded
                        && !connection.response.truncated,
                    response_complete: complete,
                });
                if !complete {
                    return Ok(());
                }
            } else if !response.complete {
                return Ok(());
            }
        }
        Ok(())
    }

    fn compare<'a>(&'a self, expected: &'a PromptExpectation) -> PromptComparison<'a> {
        let mut comparison = PromptComparison {
            expected,
            foreground: None,
            parent: None,
            native: None,
            parts: Vec::new(),
            reason: None,
        };
        let mut matches = Vec::new();
        for exchange in &self.exchanges {
            if matches!(exchange.route, NativeRoute::Foreground { .. })
                && let Ok(request) =
                    serde_json::from_slice::<NativePromptRequest>(&exchange.request)
                && request.parts.len() == 1
                && request.parts[0].text == expected.prompt
            {
                matches.push((exchange, request));
            }
        }
        if matches.len() != 1 {
            comparison.reason = Some(if matches.is_empty() {
                "foreground_request_missing"
            } else {
                "foreground_request_ambiguous"
            });
            return comparison;
        }
        let (exchange, request) = matches.remove(0);
        comparison.foreground = Some(exchange);
        if let Err(reason) = comparison.correlate(self, &request) {
            comparison.reason = Some(reason);
        }
        comparison
    }
}

#[derive(Deserialize)]
struct NativePromptRequest {
    #[serde(rename = "noReply")]
    no_reply: bool,
    model: NativeModel,
    parts: Vec<NativeInputPart>,
}

#[derive(Deserialize)]
struct NativeModel {
    #[serde(rename = "providerID")]
    provider: String,
    #[serde(rename = "modelID")]
    model: String,
}

#[derive(Deserialize)]
struct NativeInputPart {
    id: String,
    #[serde(rename = "type")]
    kind: String,
    text: String,
}

#[derive(Deserialize)]
struct NativeMessage {
    info: NativeInfo,
    parts: Vec<NativePart>,
}

#[derive(Deserialize)]
struct NativeInfo {
    id: String,
    #[serde(rename = "sessionID")]
    session: String,
    role: String,
    #[serde(rename = "parentID")]
    parent: Option<String>,
    #[serde(rename = "providerID")]
    provider: Option<String>,
    #[serde(rename = "modelID")]
    model_id: Option<String>,
    model: Option<NativeModel>,
    time: Option<NativeTime>,
    error: Option<serde::de::IgnoredAny>,
}

#[derive(Deserialize)]
struct NativeTime {
    completed: Option<f64>,
}

#[derive(Deserialize)]
struct NativePart {
    id: String,
    #[serde(rename = "sessionID")]
    session: String,
    #[serde(rename = "messageID")]
    message: String,
    #[serde(rename = "type")]
    kind: String,
    text: Option<String>,
    synthetic: Option<bool>,
    ignored: Option<bool>,
}

struct PromptComparison<'a> {
    expected: &'a PromptExpectation,
    foreground: Option<&'a ForegroundExchange>,
    parent: Option<&'a ForegroundExchange>,
    native: Option<String>,
    parts: Vec<Value>,
    reason: Option<&'static str>,
}

impl<'a> PromptComparison<'a> {
    fn correlate(
        &mut self,
        capture: &'a CapturedHttp,
        request: &NativePromptRequest,
    ) -> Result<(), &'static str> {
        let exchange = self.foreground.ok_or("foreground_request_missing")?;
        let NativeRoute::Foreground { session } = &exchange.route else {
            return Err("foreground_route");
        };
        if !exchange.supported {
            return Err("http_content_encoding");
        }
        if !exchange.response_complete {
            return Err("foreground_framing_incomplete");
        }
        if exchange.status != 200 {
            return Err("foreground_http_status");
        }
        if !exchange.request_forwarded || !exchange.response_forwarded {
            return Err("foreground_forward_incomplete");
        }
        if request.no_reply
            || request.model.provider != "openai"
            || request.model.model != self.expected.model
            || request.parts.len() != 1
            || request.parts[0].kind != "text"
            || !NativeRoute::valid_identity(&request.parts[0].id)
        {
            return Err("foreground_request_identity");
        }
        let response: NativeMessage =
            serde_json::from_slice(&exchange.response).map_err(|_| "foreground_response_shape")?;
        let info = &response.info;
        let parent = info.parent.as_deref().ok_or("foreground_parent_missing")?;
        if info.role != "assistant"
            || info.session != *session
            || !NativeRoute::valid_identity(&info.id)
            || !NativeRoute::valid_identity(parent)
            || parent == info.id
            || info.provider.as_deref() != Some("openai")
            || info.model_id.as_deref() != Some(self.expected.model.as_str())
            || info.error.is_some()
            || info
                .time
                .as_ref()
                .and_then(|time| time.completed)
                .is_none_or(|time| !time.is_finite())
        {
            return Err("foreground_response_identity");
        }
        let parents: Vec<_> = capture.exchanges.iter().filter(|candidate| matches!(&candidate.route, NativeRoute::Parent { session: candidate_session, message } if candidate_session == session && message == parent)).collect();
        if parents.len() != 1 {
            return Err("foreground_parent_ambiguous_or_missing");
        }
        let parent_exchange = parents[0];
        self.parent = Some(parent_exchange);
        if parent_exchange.status != 200
            || !parent_exchange.response_complete
            || !parent_exchange.supported
            || !parent_exchange.request_forwarded
            || !parent_exchange.response_forwarded
        {
            return Err("foreground_parent_transport");
        }
        let parent_body: NativeMessage = serde_json::from_slice(&parent_exchange.response)
            .map_err(|_| "foreground_parent_shape")?;
        if parent_body.info.id != parent
            || parent_body.info.session != *session
            || parent_body.info.role != "user"
            || parent_body.info.parent.is_some()
            || parent_body.info.model.as_ref().is_none_or(|model| {
                model.provider != "openai" || model.model != self.expected.model
            })
            || parent_body.parts.len() != 1
        {
            return Err("foreground_parent_identity");
        }
        let parent_part = &parent_body.parts[0];
        if parent_part.id != request.parts[0].id
            || parent_part.session != *session
            || parent_part.message != parent
            || parent_part.kind != "text"
            || parent_part.synthetic == Some(true)
            || parent_part.text.as_deref() != Some(self.expected.prompt.as_str())
        {
            return Err("foreground_parent_prompt");
        }
        if response.parts.len() > 1024 {
            return Err("foreground_part_bound");
        }
        let mut parts = response.parts;
        parts.sort_by(|left, right| left.id.cmp(&right.id));
        if parts.windows(2).any(|pair| pair[0].id == pair[1].id) {
            return Err("foreground_part_duplicate");
        }
        let mut native = String::new();
        for (ordinal, part) in parts.iter().enumerate() {
            if !NativeRoute::valid_identity(&part.id)
                || part.session != *session
                || part.message != info.id
            {
                return Err("foreground_part_ownership");
            }
            let selected = part.kind == "text" && part.synthetic != Some(true);
            if selected {
                let text = part.text.as_deref().ok_or("foreground_part_text_missing")?;
                if native.len().saturating_add(text.len()) > MAX_RESPONSE_BYTES {
                    return Err("foreground_text_bound");
                }
                native.push_str(text);
            }
            self.parts.push(json!({
                "ordinal":ordinal,"text_part":part.kind == "text", "selected":selected,
                "synthetic":part.synthetic.unwrap_or(false),"ignored":part.ignored.unwrap_or(false),
                "text_bytes":part.text.as_ref().map(String::len),
                "text_equals_expected":part.text.as_ref().map(|text| text == &self.expected.expected),
                "text_equals_public":part.text.as_ref().zip(self.expected.public.as_ref()).map(|(text, public)| text == public),
            }));
        }
        self.native = Some(native);
        Ok(())
    }

    fn summary(&self) -> Value {
        json!({
            "phase":self.expected.phase,
            "complete":self.reason.is_none() && self.native.is_some() && self.expected.public.is_some(),
            "correlation_complete":self.reason.is_none() && self.native.is_some(),
            "reason":self.reason,
            "request_forwarded":self.foreground.is_some_and(|exchange| exchange.request_forwarded),
            "response_forwarded":self.foreground.is_some_and(|exchange| exchange.response_forwarded),
            "response_framing_complete":self.foreground.is_some_and(|exchange| exchange.response_complete),
            "parent_framing_complete":self.parent.is_some_and(|exchange| exchange.response_complete),
            "parent_forwarded":self.parent.is_some_and(|exchange| exchange.request_forwarded && exchange.response_forwarded),
            "response_status":self.foreground.map(|exchange| exchange.status),
            "request_body_bytes":self.foreground.map(|exchange| exchange.request.len()),
            "response_body_bytes":self.foreground.map(|exchange| exchange.response.len()),
            "parent_body_bytes":self.parent.map(|exchange| exchange.response.len()),
            "native_text_bytes":self.native.as_ref().map(String::len),
            "public_text_bytes":self.expected.public.as_ref().map(String::len),
            "expected_text_bytes":self.expected.expected.len(),
            "native_equals_expected":self.native.as_ref().map(|text| text == &self.expected.expected),
            "native_equals_public":self.native.as_ref().zip(self.expected.public.as_ref()).map(|(text, public)| text == public),
            "public_equals_expected":self.expected.public.as_ref().map(|text| text == &self.expected.expected),
            "parts":self.parts,
        })
    }
}
