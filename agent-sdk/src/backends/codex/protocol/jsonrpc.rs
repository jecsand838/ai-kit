use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::SdkError;
use crate::error::{DeadlineKind, ErrorDetail, ErrorPhase, Operation};

const QUEUE_CAPACITY: usize = 64;
const MAX_MESSAGE_SIZE: usize = 128 << 20;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum CodexRequestId {
    String(String),
    Number(i64),
}

struct CodexPendingRequests {
    replies: Mutex<BTreeMap<CodexRequestId, oneshot::Sender<Result<Value, CodexRequestError>>>>,
}

pub(crate) enum CodexRequestError {
    Rejected(SdkError),
    ServerRejected { code: i64, message: String },
    Ambiguous(SdkError),
}

#[derive(Clone, Copy)]
pub(crate) enum CodexTransportFailure {
    Protocol,
    Integration,
}

pub(crate) enum CodexTransportInbound {
    Value(Value),
    Failure(CodexTransportFailure),
    Closed,
}

enum CodexInbound {
    Request {
        value: Value,
    },
    Notification {
        value: Value,
    },
    Response {
        id: CodexRequestId,
        result: Result<Value, CodexRequestError>,
    },
}

pub(crate) struct CodexTransportRuntime {
    outbound: mpsc::Sender<Value>,
    inbound: mpsc::Receiver<CodexTransportInbound>,
    tasks: CodexTransportTasks,
}

pub(crate) struct CodexTransportTasks {
    workers: Vec<JoinHandle<Result<(), CodexTransportFailure>>>,
    dispatcher: Option<JoinHandle<()>>,
    shutdown: Option<oneshot::Sender<()>>,
    child: Option<tokio::process::Child>,
}

pub(crate) struct CodexRuntimeBuilder;

#[derive(Clone)]
pub(crate) struct CodexRpc {
    inner: Arc<CodexRpcInner>,
}

struct CodexRpcInner {
    outbound: mpsc::Sender<Value>,
    pending: Arc<CodexPendingRequests>,
    next_id: AtomicI64,
    request_timeout: Duration,
    tasks: std::sync::Mutex<Option<CodexTransportTasks>>,
}

impl CodexRuntimeBuilder {
    pub(crate) fn websocket_config() -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
        tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
            .max_frame_size(Some(MAX_MESSAGE_SIZE))
            .max_message_size(Some(MAX_MESSAGE_SIZE))
    }

    pub(crate) fn json_lines<R, W>(
        reader: R,
        writer: W,
        child: tokio::process::Child,
        stderr: Option<tokio::process::ChildStderr>,
    ) -> CodexTransportRuntime
    where
        R: tokio::io::AsyncRead + Unpin + Send + 'static,
        W: tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let (outbound, mut outbound_rx) = mpsc::channel::<Value>(QUEUE_CAPACITY);
        let (inbound_tx, inbound) = mpsc::channel::<CodexTransportInbound>(QUEUE_CAPACITY);
        let (shutdown, mut shutdown_rx) = oneshot::channel();

        let writer_inbound = inbound_tx.clone();
        let writer = tokio::spawn(async move {
            let mut writer = writer;
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => {
                        return writer
                            .shutdown()
                            .await
                            .map_err(|_| CodexTransportFailure::Integration);
                    }
                    outbound = outbound_rx.recv() => {
                        let Some(value) = outbound else {
                            return writer
                                .shutdown()
                                .await
                                .map_err(|_| CodexTransportFailure::Integration);
                        };
                        let mut line = match serde_json::to_vec(&value) {
                            Ok(line) => line,
                            Err(_) => {
                                let _ = writer_inbound
                                    .send(CodexTransportInbound::Failure(
                                        CodexTransportFailure::Protocol,
                                    ))
                                    .await;
                                return Err(CodexTransportFailure::Protocol);
                            }
                        };
                        line.push(b'\n');
                        if writer.write_all(&line).await.is_err()
                            || writer.flush().await.is_err()
                        {
                            let _ = writer_inbound
                                .send(CodexTransportInbound::Failure(
                                    CodexTransportFailure::Integration,
                                ))
                                .await;
                            return Err(CodexTransportFailure::Integration);
                        }
                    }
                }
            }
        });

        let reader = tokio::spawn(async move {
            let mut reader = tokio::io::BufReader::new(reader);
            loop {
                let mut frame = Vec::new();
                let read = {
                    let mut limited = (&mut reader).take((MAX_MESSAGE_SIZE + 2) as u64);
                    limited.read_until(b'\n', &mut frame).await
                };
                match read {
                    Ok(0) => {
                        let _ = inbound_tx.send(CodexTransportInbound::Closed).await;
                        return Ok(());
                    }
                    Ok(_) => {
                        if frame.last() == Some(&b'\n') {
                            frame.pop();
                            if frame.last() == Some(&b'\r') {
                                frame.pop();
                            }
                        }
                        if frame.len() > MAX_MESSAGE_SIZE {
                            let _ = inbound_tx
                                .send(CodexTransportInbound::Failure(
                                    CodexTransportFailure::Protocol,
                                ))
                                .await;
                            return Err(CodexTransportFailure::Protocol);
                        }
                        match serde_json::from_slice(&frame) {
                            Ok(value) => {
                                if inbound_tx
                                    .send(CodexTransportInbound::Value(value))
                                    .await
                                    .is_err()
                                {
                                    return Ok(());
                                }
                            }
                            Err(_) => {
                                let _ = inbound_tx
                                    .send(CodexTransportInbound::Failure(
                                        CodexTransportFailure::Protocol,
                                    ))
                                    .await;
                                return Err(CodexTransportFailure::Protocol);
                            }
                        }
                    }
                    Err(_) => {
                        let _ = inbound_tx
                            .send(CodexTransportInbound::Failure(
                                CodexTransportFailure::Integration,
                            ))
                            .await;
                        return Err(CodexTransportFailure::Integration);
                    }
                }
            }
        });

        let mut workers = vec![writer, reader];
        if let Some(stderr) = stderr {
            workers.push(tokio::spawn(async move {
                let mut reader = stderr;
                let mut buffer = [0_u8; 8192];
                loop {
                    match tokio::io::AsyncReadExt::read(&mut reader, &mut buffer).await {
                        Ok(0) => return Ok(()),
                        Ok(_) => {}
                        Err(_) => return Err(CodexTransportFailure::Integration),
                    }
                }
            }));
        }

        CodexTransportRuntime {
            outbound,
            inbound,
            tasks: CodexTransportTasks {
                workers,
                dispatcher: None,
                shutdown: Some(shutdown),
                child: Some(child),
            },
        }
    }

    pub(crate) fn websocket<S>(
        _endpoint: String,
        mut stream: tokio_tungstenite::WebSocketStream<S>,
    ) -> CodexTransportRuntime
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let (outbound, mut outbound_rx) = mpsc::channel::<Value>(QUEUE_CAPACITY);
        let (inbound_tx, inbound) = mpsc::channel::<CodexTransportInbound>(QUEUE_CAPACITY);
        let (shutdown, mut shutdown_rx) = oneshot::channel();

        let worker = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => {
                        return stream
                            .close(None)
                            .await
                            .map_err(|_| CodexTransportFailure::Integration);
                    }
                    outbound = outbound_rx.recv() => {
                        let Some(value) = outbound else {
                            return stream
                                .close(None)
                                .await
                                .map_err(|_| CodexTransportFailure::Integration);
                        };
                        if stream
                            .send(tokio_tungstenite::tungstenite::Message::Text(
                                value.to_string().into(),
                            ))
                            .await
                            .is_err()
                        {
                            let _ = inbound_tx
                                .send(CodexTransportInbound::Failure(
                                    CodexTransportFailure::Integration,
                                ))
                                .await;
                            return Err(CodexTransportFailure::Integration);
                        }
                    }
                    inbound = stream.next() => {
                        let Some(inbound) = inbound else {
                            let _ = inbound_tx.send(CodexTransportInbound::Closed).await;
                            return Ok(());
                        };
                        match inbound {
                            Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                                match serde_json::from_str(text.as_ref()) {
                                    Ok(value) => {
                                        if inbound_tx
                                            .send(CodexTransportInbound::Value(value))
                                            .await
                                            .is_err()
                                        {
                                            return Ok(());
                                        }
                                    }
                                    Err(_) => {
                                        let _ = inbound_tx
                                            .send(CodexTransportInbound::Failure(
                                                CodexTransportFailure::Protocol,
                                            ))
                                            .await;
                                        return Err(CodexTransportFailure::Protocol);
                                    }
                                }
                            }
                            Ok(tokio_tungstenite::tungstenite::Message::Binary(_)) => {}
                            Ok(tokio_tungstenite::tungstenite::Message::Ping(payload)) => {
                                if stream
                                    .send(tokio_tungstenite::tungstenite::Message::Pong(payload))
                                    .await
                                    .is_err()
                                {
                                    let _ = inbound_tx
                                        .send(CodexTransportInbound::Failure(
                                            CodexTransportFailure::Integration,
                                        ))
                                        .await;
                                    return Err(CodexTransportFailure::Integration);
                                }
                            }
                            Ok(tokio_tungstenite::tungstenite::Message::Pong(_)) => {}
                            Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => {
                                let _ = inbound_tx.send(CodexTransportInbound::Closed).await;
                                return Ok(());
                            }
                            Ok(tokio_tungstenite::tungstenite::Message::Frame(_)) => {}
                            Err(_) => {
                                let _ = inbound_tx
                                    .send(CodexTransportInbound::Failure(
                                        CodexTransportFailure::Integration,
                                    ))
                                    .await;
                                return Err(CodexTransportFailure::Integration);
                            }
                        }
                    }
                }
            }
        });

        CodexTransportRuntime {
            outbound,
            inbound,
            tasks: CodexTransportTasks {
                workers: vec![worker],
                dispatcher: None,
                shutdown: Some(shutdown),
                child: None,
            },
        }
    }
}

impl CodexRpc {
    pub(crate) fn new(
        mut runtime: CodexTransportRuntime,
        request_timeout: Duration,
    ) -> (Self, mpsc::Receiver<Result<Value, SdkError>>) {
        let pending = Arc::new(CodexPendingRequests::new());
        let dispatcher_pending = pending.clone();
        let (inbound, inbound_receiver) = mpsc::channel(QUEUE_CAPACITY);
        let dispatcher = tokio::spawn(async move {
            let failure = loop {
                match runtime.inbound.recv().await {
                    Some(CodexTransportInbound::Value(value)) => {
                        if let Err(failure) =
                            Self::apply_inbound(&dispatcher_pending, &inbound, value).await
                        {
                            break failure;
                        }
                    }
                    Some(CodexTransportInbound::Failure(failure)) => break failure,
                    Some(CodexTransportInbound::Closed) | None => {
                        break CodexTransportFailure::Integration;
                    }
                }
            };
            dispatcher_pending.fail(failure).await;
            let _ = inbound.send(Err(failure.error())).await;
        });
        runtime.tasks.dispatcher = Some(dispatcher);

        (
            Self {
                inner: Arc::new(CodexRpcInner {
                    outbound: runtime.outbound,
                    pending,
                    next_id: AtomicI64::new(1),
                    request_timeout,
                    tasks: std::sync::Mutex::new(Some(runtime.tasks)),
                }),
            },
            inbound_receiver,
        )
    }

    pub(crate) async fn request(&self, method: &str, params: Value) -> Result<Value, SdkError> {
        self.request_with_acceptance(method, params)
            .await
            .map_err(CodexRequestError::into_sdk_error)
    }

    pub(crate) async fn request_with_acceptance(
        &self,
        method: &str,
        params: Value,
    ) -> Result<Value, CodexRequestError> {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        if id <= 0 {
            return Err(CodexRequestError::Rejected(SdkError::invalid_state(
                "JSON-RPC request id exhausted",
            )));
        }
        let key = CodexRequestId::Number(id);
        let (sender, receiver) = oneshot::channel();
        self.inner
            .pending
            .register(key.clone(), sender)
            .await
            .map_err(CodexRequestError::Rejected)?;

        let frame = serde_json::json!({
            "id": id,
            "method": method,
            "params": params,
        });
        if self.inner.outbound.send(frame).await.is_err() {
            self.inner.pending.remove(&key).await;
            return Err(CodexRequestError::Rejected(
                SdkError::integration("transport is closed")
                    .with_context(Operation::Unspecified, ErrorPhase::RequestSend)
                    .with_detail(ErrorDetail::TransportFailure),
            ));
        }

        match tokio::time::timeout(self.inner.request_timeout, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(CodexRequestError::Ambiguous(
                SdkError::integration("response owner ended before completion")
                    .with_context(Operation::Unspecified, ErrorPhase::ResponseRead)
                    .with_detail(ErrorDetail::TaskTerminated),
            )),
            Err(_) => {
                self.inner.pending.remove(&key).await;
                Err(CodexRequestError::Ambiguous(
                    SdkError::timeout("request deadline elapsed")
                        .with_context(Operation::Unspecified, ErrorPhase::ResponseRead)
                        .with_detail(ErrorDetail::DeadlineElapsed {
                            kind: DeadlineKind::Request,
                            timeout: Some(self.inner.request_timeout),
                        }),
                ))
            }
        }
    }

    pub(crate) async fn notify(&self, method: &str) -> Result<(), SdkError> {
        self.inner
            .outbound
            .send(serde_json::json!({
                "method": method,
            }))
            .await
            .map_err(|_| SdkError::integration("transport is closed"))
    }

    pub(crate) async fn respond(&self, response: Value) -> Result<(), SdkError> {
        self.inner
            .outbound
            .send(response)
            .await
            .map_err(|_| SdkError::integration("transport is closed"))
    }

    pub(crate) async fn shutdown(self, timeout: Duration) -> Result<(), SdkError> {
        let tasks = self
            .inner
            .tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(tasks) = tasks {
            tasks.shutdown(timeout).await?;
        }
        Ok(())
    }

    async fn apply_inbound(
        pending: &CodexPendingRequests,
        inbound: &mpsc::Sender<Result<Value, SdkError>>,
        value: Value,
    ) -> Result<(), CodexTransportFailure> {
        match CodexInbound::decode(value)? {
            CodexInbound::Request { value } | CodexInbound::Notification { value } => inbound
                .send(Ok(value))
                .await
                .map_err(|_| CodexTransportFailure::Integration),
            CodexInbound::Response { id, result } => {
                let sender = pending.remove(&id).await;
                if let Some(sender) = sender {
                    let _ = sender.send(result);
                }
                Ok(())
            }
        }
    }
}

impl CodexPendingRequests {
    fn new() -> Self {
        Self {
            replies: Mutex::new(BTreeMap::new()),
        }
    }

    async fn register(
        &self,
        id: CodexRequestId,
        reply: oneshot::Sender<Result<Value, CodexRequestError>>,
    ) -> Result<(), SdkError> {
        let mut replies = self.replies.lock().await;
        if replies.contains_key(&id) {
            return Err(SdkError::invalid_state("JSON-RPC request id was reused"));
        }
        replies.insert(id, reply);
        Ok(())
    }

    async fn remove(
        &self,
        id: &CodexRequestId,
    ) -> Option<oneshot::Sender<Result<Value, CodexRequestError>>> {
        self.replies.lock().await.remove(id)
    }

    async fn fail(&self, failure: CodexTransportFailure) {
        let replies = {
            let mut pending = self.replies.lock().await;
            std::mem::take(&mut *pending)
        };
        for reply in replies.into_values() {
            let _ = reply.send(Err(CodexRequestError::Ambiguous(failure.error())));
        }
    }
}

impl CodexRequestError {
    pub(crate) fn is_rejected(&self) -> bool {
        matches!(self, Self::Rejected(_) | Self::ServerRejected { .. })
    }

    pub(crate) fn into_sdk_error(self) -> SdkError {
        match self {
            Self::Rejected(error) | Self::Ambiguous(error) => error,
            Self::ServerRejected { code, .. } => {
                let detail = match code {
                    -32600 | -32602 => ErrorDetail::InvalidRequest,
                    -32603 => ErrorDetail::RemoteInternal,
                    -32001 => ErrorDetail::ServiceUnavailable,
                    _ => ErrorDetail::RemoteRejected,
                };
                SdkError::integration("server request was rejected")
                    .with_context(Operation::Unspecified, ErrorPhase::ResponseRead)
                    .with_detail(detail)
            }
        }
    }
}

impl CodexTransportFailure {
    fn error(self) -> SdkError {
        match self {
            Self::Protocol => SdkError::protocol("transport received malformed protocol data")
                .with_context(Operation::Unspecified, ErrorPhase::ResponseDecode)
                .with_detail(ErrorDetail::MalformedData),
            Self::Integration => SdkError::integration("transport ended unexpectedly")
                .with_context(Operation::Unspecified, ErrorPhase::ResponseRead)
                .with_detail(ErrorDetail::TransportFailure),
        }
    }
}

impl CodexRequestId {
    fn from_value(value: &Value) -> Option<Self> {
        if let Some(value) = value.as_str() {
            return Some(Self::String(value.to_owned()));
        }
        value.as_i64().map(Self::Number)
    }
}

impl CodexInbound {
    fn decode(value: Value) -> Result<Self, CodexTransportFailure> {
        let object = value.as_object().ok_or(CodexTransportFailure::Protocol)?;
        if let Some(version) = object.get("jsonrpc")
            && version.as_str() != Some("2.0")
        {
            return Err(CodexTransportFailure::Protocol);
        }

        if let Some(method) = object.get("method") {
            if method.as_str().is_none()
                || object.contains_key("result")
                || object.contains_key("error")
            {
                return Err(CodexTransportFailure::Protocol);
            }
            let request = match object.get("id") {
                None => false,
                Some(raw_id) if CodexRequestId::from_value(raw_id).is_some() => true,
                Some(_) => return Err(CodexTransportFailure::Protocol),
            };
            return if request {
                Ok(Self::Request { value })
            } else {
                Ok(Self::Notification { value })
            };
        }

        if object.contains_key("params") {
            return Err(CodexTransportFailure::Protocol);
        }
        let id = object
            .get("id")
            .and_then(CodexRequestId::from_value)
            .ok_or(CodexTransportFailure::Protocol)?;
        let result = match (object.get("result"), object.get("error")) {
            (Some(result), None) => Ok(result.clone()),
            (None, Some(error)) => {
                let error = error.as_object().ok_or(CodexTransportFailure::Protocol)?;
                let code = error
                    .get("code")
                    .and_then(Value::as_i64)
                    .ok_or(CodexTransportFailure::Protocol)?;
                let message = error
                    .get("message")
                    .and_then(Value::as_str)
                    .ok_or(CodexTransportFailure::Protocol)?;
                Err(CodexRequestError::ServerRejected {
                    code,
                    message: message.to_owned(),
                })
            }
            _ => return Err(CodexTransportFailure::Protocol),
        };
        Ok(Self::Response { id, result })
    }
}

impl CodexTransportTasks {
    async fn shutdown(mut self, timeout: Duration) -> Result<(), SdkError> {
        let deadline = Instant::now() + timeout;
        let graceful_deadline = Instant::now() + (timeout / 2).min(Duration::from_secs(1));
        let mut cleanup_error: Option<SdkError> = None;

        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }

        if let Some(mut child) = self.child.take() {
            let needs_escalation =
                match tokio::time::timeout_at(graceful_deadline, child.wait()).await {
                    Ok(Ok(_)) => false,
                    Ok(Err(_)) => {
                        cleanup_error =
                            Some(SdkError::integration("server process could not be reaped"));
                        true
                    }
                    Err(_) => true,
                };
            if needs_escalation {
                if child.start_kill().is_err() && !matches!(child.try_wait(), Ok(Some(_))) {
                    let error = SdkError::integration("server process termination could not start")
                        .in_phase(ErrorPhase::Cleanup);
                    cleanup_error = Some(match cleanup_error.take() {
                        Some(primary) => primary.with_supplementary(error),
                        None => error,
                    });
                }
                match tokio::time::timeout_at(deadline, child.wait()).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(_)) => {
                        let error = SdkError::integration("server process could not be reaped")
                            .in_phase(ErrorPhase::Cleanup);
                        cleanup_error = Some(match cleanup_error.take() {
                            Some(primary) => primary.with_supplementary(error),
                            None => error,
                        });
                    }
                    Err(_) => {
                        let error = SdkError::timeout("server process termination timed out")
                            .in_phase(ErrorPhase::Cleanup);
                        cleanup_error = Some(match cleanup_error.take() {
                            Some(primary) => primary.with_supplementary(error),
                            None => error,
                        });
                    }
                }
            }
        }

        for mut worker in self.workers {
            match tokio::time::timeout_at(deadline, &mut worker).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(CodexTransportFailure::Protocol))) => {
                    let error = SdkError::protocol("transport received malformed protocol data")
                        .with_detail(ErrorDetail::MalformedData)
                        .in_phase(ErrorPhase::Cleanup);
                    cleanup_error = Some(match cleanup_error.take() {
                        Some(primary) => primary.with_supplementary(error),
                        None => error,
                    });
                }
                Ok(Ok(Err(CodexTransportFailure::Integration))) | Ok(Err(_)) => {
                    let error = SdkError::integration("transport worker failed")
                        .in_phase(ErrorPhase::Cleanup);
                    cleanup_error = Some(match cleanup_error.take() {
                        Some(primary) => primary.with_supplementary(error),
                        None => error,
                    });
                }
                Err(_) => {
                    worker.abort();
                    let error = SdkError::timeout("transport cleanup timed out")
                        .in_phase(ErrorPhase::Cleanup);
                    cleanup_error = Some(match cleanup_error.take() {
                        Some(primary) => primary.with_supplementary(error),
                        None => error,
                    });
                }
            }
        }

        if let Some(mut dispatcher) = self.dispatcher.take() {
            match tokio::time::timeout_at(deadline, &mut dispatcher).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    let error = SdkError::integration("response dispatcher failed")
                        .in_phase(ErrorPhase::Cleanup);
                    cleanup_error = Some(match cleanup_error.take() {
                        Some(primary) => primary.with_supplementary(error),
                        None => error,
                    });
                }
                Err(_) => {
                    dispatcher.abort();
                    let error = SdkError::timeout("response dispatcher cleanup timed out")
                        .in_phase(ErrorPhase::Cleanup);
                    cleanup_error = Some(match cleanup_error.take() {
                        Some(primary) => primary.with_supplementary(error),
                        None => error,
                    });
                }
            }
        }

        match cleanup_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
