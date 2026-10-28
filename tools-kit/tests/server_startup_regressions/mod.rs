use std::{
    collections::BTreeMap,
    future::Future,
    net::SocketAddr,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    sync::Arc,
    task::Poll,
    time::Duration,
};

use rmcp::{
    ServiceExt,
    transport::{
        StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    runtime::{Builder, RuntimeMetrics},
    time::timeout,
};
use tools_kit::{
    Toolset, ToolsetBackend,
    mcp::{McpServer, McpServerConfig},
};

const IO_TIMEOUT: Duration = Duration::from_secs(3);

struct StartupCase {
    address: SocketAddr,
    tools: Arc<Toolset>,
    server: Option<McpServer>,
}

impl StartupCase {
    async fn start() -> Result<Self, String> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| error.to_string())?;
        let address = listener.local_addr().map_err(|error| error.to_string())?;
        drop(listener);
        let tools = Arc::new(Toolset::local(Vec::new()).map_err(|error| error.to_string())?);
        let config = McpServerConfig::new(address, "/mcp", "startup:")
            .and_then(|config| config.with_shutdown_timeout(Duration::from_secs(1)))
            .map_err(|error| error.to_string())?;
        let server = timeout(IO_TIMEOUT, McpServer::start(Arc::clone(&tools), config))
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        Ok(Self {
            address,
            tools,
            server: Some(server),
        })
    }

    async fn rejected_then_fresh_session(&self, body: &str) -> Result<(), String> {
        let mut wire = HttpWire::post(self.address, body.len(), false).await?;
        wire.write(body).await?;
        let reply = wire
            .response()
            .await?
            .ok_or("rejection closed without an HTTP response")?;
        // This lifecycle case proves rejection and subsequent usability. MCP
        // recommends HTTP 400 for a missing session ID but does not mandate one
        // exact status for these invalid first messages; pinned rmcp returns
        // 415 for malformed JSON and 422 for a parsed unexpected first method.
        if !(400..500).contains(&reply.status) {
            return Err(format!(
                "invalid first message must receive an HTTP client error: {reply:?}"
            ));
        }
        drop(wire);

        // Use the independent native client after the raw rejection. A local
        // endpoint that merely refuses every request cannot pass this control.
        let transport = StreamableHttpClientTransport::from_config(
            StreamableHttpClientTransportConfig::with_uri(format!("http://{}/mcp", self.address)),
        );
        let mut client = timeout(IO_TIMEOUT, ().serve(transport))
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        let inventory = timeout(IO_TIMEOUT, client.peer().list_tools(None)).await;
        let close = timeout(IO_TIMEOUT, client.close()).await;
        if !matches!(&inventory, Ok(Ok(result)) if result.tools.is_empty()) {
            return Err(format!(
                "fresh initialized session failed: {inventory:?}; client cleanup={close:?}"
            ));
        }
        if !matches!(close, Ok(Ok(_))) {
            return Err(format!("fresh client cleanup failed: {close:?}"));
        }
        Ok(())
    }

    async fn raw_initialize_reply(&self) -> Result<HttpReply, String> {
        let body = json!({
            "jsonrpc":"2.0", "id":1, "method":"initialize",
            "params": {
                "protocolVersion":"2025-11-25", "capabilities":{},
                "clientInfo":{"name":"session-quiescence-control", "version":"1"}
            }
        })
        .to_string();
        let mut wire = HttpWire::post(self.address, body.len(), false).await?;
        wire.write(&body).await?;
        let reply = wire
            .response()
            .await?
            .ok_or("missing initialize response")?;
        drop(wire);
        Ok(reply)
    }

    async fn raw_session_initialized(&self) -> Result<String, String> {
        let reply = self.raw_initialize_reply().await?;
        if reply.status != 200 {
            return Err(format!("initialization was rejected: {reply:?}"));
        }
        reply.validate_initialize()?;
        reply
            .headers
            .get("mcp-session-id")
            .cloned()
            .ok_or_else(|| "validated initialization lost its session identity".to_owned())
    }

    async fn lose_initialize_observation(&self) -> Result<(), String> {
        let body = json!({
            "jsonrpc":"2.0", "id":1, "method":"initialize",
            "params": {
                "protocolVersion":"2025-11-25", "capabilities":{},
                "clientInfo":{"name":"lost-initialize-observer", "version":"1"}
            }
        })
        .to_string();
        let mut wire = HttpWire::post(self.address, body.len(), false).await?;
        wire.write(&body).await?;
        let reply = wire
            .head()
            .await?
            .ok_or("initialization closed before native admission")?;
        if reply.status != 200
            || reply
                .headers
                .get("mcp-session-id")
                .is_none_or(String::is_empty)
        {
            return Err(format!(
                "initialization supplied no admitted session: {reply:?}"
            ));
        }
        // Pinned rmcp emits this header after creating and initializing its
        // native session. Discard only the HTTP observation before consuming
        // its response body; disconnect is not protocol cancellation.
        drop(wire);
        Ok(())
    }

    async fn raw_session_ready(&self, session: &str) -> Result<(), String> {
        let mut wire = HttpWire::session_post(
            self.address,
            session,
            r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
        )
        .await?;
        let reply = wire
            .response()
            .await?
            .ok_or("missing initialized notification response")?;
        if reply.status != 202 || !reply.decoded_body()?.is_empty() {
            return Err(format!(
                "initialized notification was not accepted with an empty response: {reply:?}"
            ));
        }
        // HTTP202 proves native queuing, not callback completion.
        drop(wire);
        Ok(())
    }

    async fn independent_sessions(&self) -> Result<(), String> {
        let (first, second) = tokio::join!(
            self.raw_session_initialized(),
            self.raw_session_initialized()
        );
        let first = first?;
        let second = second?;
        if first == second {
            return Err("independent initializations reused the same session identity".to_owned());
        }
        self.raw_session_ready(&first).await?;
        self.raw_session_ready(&second).await?;
        let mut close = HttpWire::delete(self.address, &first).await?;
        let reply = close
            .response()
            .await?
            .ok_or("missing selected DELETE response")?;
        if !(200..300).contains(&reply.status) {
            return Err(format!("first session termination was rejected: {reply:?}"));
        }
        drop(close);
        let mut remaining = HttpWire::session_post(
            self.address,
            &second,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}"#,
        )
        .await?;
        let reply = remaining
            .response()
            .await?
            .ok_or("remaining session lost its tools/list response")?;
        if reply.status != 200
            || !reply.rpc_result(2)?["tools"]
                .as_array()
                .is_some_and(Vec::is_empty)
        {
            return Err(format!(
                "closing the first session must preserve the second inventory: {reply:?}"
            ));
        }
        drop(remaining);
        // The second independent session intentionally remains open for the
        // consuming server shutdown; no client task or pool owns its lifetime.
        Ok(())
    }

    async fn session_inventory(&self, session: &str) -> Result<(), String> {
        let mut wire = HttpWire::session_post(
            self.address,
            session,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}"#,
        )
        .await?;
        let reply = wire
            .response()
            .await?
            .ok_or("missing session inventory response")?;
        if reply.status != 200
            || !reply.rpc_result(2)?["tools"]
                .as_array()
                .is_some_and(Vec::is_empty)
        {
            return Err(format!(
                "admitted session lost its exact inventory: {reply:?}"
            ));
        }
        Ok(())
    }

    async fn delete_session(&self, session: &str) -> Result<(), String> {
        let mut wire = HttpWire::delete(self.address, session).await?;
        let reply = wire
            .response()
            .await?
            .ok_or("missing session DELETE response")?;
        if !(200..300).contains(&reply.status) {
            return Err(format!("owned session deletion was rejected: {reply:?}"));
        }
        Ok(())
    }

    async fn capacity_session(&self, sessions: &mut Vec<String>) -> Result<String, String> {
        let reply = self.raw_initialize_reply().await?;
        let session = reply.headers.get("mcp-session-id").cloned();
        let duplicate = session.as_ref().is_some_and(|id| sessions.contains(id));
        if let Some(session) = &session
            && !duplicate
        {
            // Even a malformed InitializeResult may already own a native
            // session. Retain its observed identity before validating the body.
            sessions.push(session.clone());
        }
        if reply.status != 200 {
            return Err(format!(
                "capacity setup initialization was rejected: {reply:?}"
            ));
        }
        reply.validate_initialize()?;
        if duplicate {
            return Err("distinct initializations reused a native session identity".to_owned());
        }
        session.ok_or_else(|| "validated initialization lost its identity".to_owned())
    }

    async fn excess_session_rejected(
        &self,
        sessions: &mut Vec<String>,
        metrics: &RuntimeMetrics,
    ) -> Result<(), String> {
        let baseline = metrics.num_alive_tasks();
        let reply = self.raw_initialize_reply().await?;
        // Retain every observed owned identity for cleanup even on the red
        // path; a bad successful extra admission must not escape fixture cleanup.
        if let Some(session) = reply.headers.get("mcp-session-id")
            && !sessions.contains(session)
        {
            sessions.push(session.clone());
        }
        if !(400..600).contains(&reply.status) || reply.headers.contains_key("mcp-session-id") {
            return Err(format!(
                "session capacity admitted an excess initialization: status={}, session_header={}, runtime_before={baseline}, runtime_after={}",
                reply.status,
                reply.headers.contains_key("mcp-session-id"),
                metrics.num_alive_tasks()
            ));
        }
        // This proves no *retained* extra work after rejection, not that no
        // transient task was allocated. Exact pre-spawn admission is reviewed
        // in production source; runtime topology is not the public oracle.
        Self::await_task_baseline(metrics, baseline).await
    }

    async fn session_capacity(&self, metrics: &RuntimeMetrics) -> Result<(), String> {
        // Proposed independent-server admission policy: 64 concurrent native
        // sessions. This tests the externally observable bound/reuse, not the
        // spelling, layout or task count of a private owner. Business invocation
        // capacity has separate tests and does not establish this session bound.
        const SESSION_LIMIT: usize = 64;
        let mut sessions = Vec::new();
        let mut capacity_errors = Vec::new();
        let observed = timeout(Duration::from_secs(30), async {
            for _ in 0..SESSION_LIMIT {
                let session = self.capacity_session(&mut sessions).await?;
                self.raw_session_ready(&session).await?;
                self.session_inventory(&session).await?;
            }
            if let Err(error) = self.excess_session_rejected(&mut sessions, metrics).await {
                capacity_errors.push(format!("initial saturation: {error}"));
            }
            // A rejected admission must leave every prior independent session
            // usable. This also rules out losing the endpoint as a fake bound.
            for session in &sessions[..SESSION_LIMIT] {
                self.session_inventory(session).await?;
            }
            let selected = sessions[0].clone();
            self.delete_session(&selected).await?;
            sessions.remove(0);
            let replacement = self.capacity_session(&mut sessions).await?;
            if replacement == selected {
                return Err("capacity reuse must admit a distinct replacement session".to_owned());
            }
            self.raw_session_ready(&replacement).await?;
            self.session_inventory(&replacement).await?;
            if let Err(error) = self.excess_session_rejected(&mut sessions, metrics).await {
                capacity_errors.push(format!("saturation after selected DELETE/reuse: {error}"));
            }
            for session in &sessions {
                self.session_inventory(session).await?;
            }
            if !capacity_errors.is_empty() {
                return Err("native session capacity was exceeded".to_owned());
            }
            Ok(())
        })
        .await;
        // Independently terminate all known native sessions before server
        // shutdown. A capacity failure must not be masked by the separate known
        // open-session shutdown defect. The enclosing owner still performs
        // server/Toolset cleanup and destroys the isolated runtime on every path.
        let mut cleanup_errors = Vec::new();
        let cleanup = timeout(Duration::from_secs(30), async {
            while let Some(session) = sessions.pop() {
                if let Err(error) = self.delete_session(&session).await {
                    cleanup_errors.push(error);
                }
            }
        })
        .await;
        eprintln!(
            "native session capacity observation={observed:?}; capacity_errors={capacity_errors:?}; explicit session cleanup={cleanup:?}; cleanup_errors={cleanup_errors:?}"
        );
        match observed {
            Ok(Ok(())) if cleanup.is_ok() && cleanup_errors.is_empty() => Ok(()),
            other => Err(format!(
                "session capacity={other:?}; capacity_errors={capacity_errors:?}; explicit session cleanup={cleanup:?}; cleanup_errors={cleanup_errors:?}"
            )),
        }
    }

    async fn shutdown_at_task_baseline(
        &mut self,
        metrics: &RuntimeMetrics,
        baseline: usize,
    ) -> Result<(), String> {
        let server = self.server.take().ok_or("server already consumed")?;
        let shutdown = timeout(IO_TIMEOUT, server.shutdown()).await;
        // No await before this observation on the isolated current-thread
        // runtime: cleanup or later scheduling cannot hide premature success.
        let alive = metrics.num_alive_tasks();
        eprintln!(
            "session shutdown={shutdown:?}; immediate runtime tasks={alive}; pre-start baseline={baseline}"
        );
        if matches!(shutdown, Ok(Ok(()))) && alive == baseline {
            Ok(())
        } else {
            Err(format!(
                "server shutdown={shutdown:?}; immediate runtime tasks={alive}; pre-start baseline={baseline}"
            ))
        }
    }

    async fn raw_session_closed(&self) -> Result<(), String> {
        let session = self.raw_session_initialized().await?;
        let mut wire = HttpWire::delete(self.address, &session).await?;
        let reply = wire.response().await?.ok_or("missing DELETE response")?;
        if !(200..300).contains(&reply.status) {
            return Err(format!("session deletion was rejected: {reply:?}"));
        }
        // Both raw requests use Connection: close; response() consumes their
        // entire bounded body through EOF. No client runtime or pool is spawned.
        drop(wire);
        Ok(())
    }

    async fn await_task_baseline(metrics: &RuntimeMetrics, baseline: usize) -> Result<(), String> {
        let settled = timeout(IO_TIMEOUT, async {
            while metrics.num_alive_tasks() != baseline {
                tokio::task::yield_now().await;
            }
        })
        .await;
        if settled.is_err() {
            return Err(format!(
                "runtime tasks did not return to baseline {baseline}; alive={}",
                metrics.num_alive_tasks()
            ));
        }
        Ok(())
    }

    async fn initialize_during_seal(&mut self) -> Result<usize, String> {
        let body = json!({
            "jsonrpc":"2.0", "id":1, "method":"initialize",
            "params": {
                "protocolVersion":"2025-11-25", "capabilities":{},
                "clientInfo":{"name":"initialize-seal-control", "version":"1"}
            }
        })
        .to_string();
        let mut wire = HttpWire::post(self.address, body.len(), true).await?;
        let interim = wire
            .head()
            .await?
            .ok_or("connection closed before100Continue")?;
        if interim.status != 100 {
            return Err(format!("expected body-admission barrier, got {interim:?}"));
        }

        // Receiving100Continue proves the server has begun reading this valid
        // HTTP request. TCP connection alone would not establish admission.
        let server = self.server.take().ok_or("server already consumed")?;
        let mut shutdown = Box::pin(server.shutdown());
        let first =
            std::future::poll_fn(|context| Poll::Ready(shutdown.as_mut().poll(context))).await;
        let first_alive = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        // Always release the body, even if shutdown completed prematurely.
        let release = wire.write(&body).await;
        let reply = wire.response().await;
        let (terminal, alive) = match first {
            Poll::Ready(result) => (Ok(result), first_alive),
            Poll::Pending => {
                let terminal = timeout(IO_TIMEOUT, &mut shutdown).await;
                let alive = tokio::runtime::Handle::current()
                    .metrics()
                    .num_alive_tasks();
                (terminal, alive)
            }
        };
        drop(shutdown);
        drop(wire);

        if !matches!(terminal, Ok(Ok(()))) {
            return Err(format!(
                "shutdown did not settle admitted initialization: {terminal:?}; body={release:?}; reply={reply:?}"
            ));
        }
        // A seal may reject the request or finish its already admitted
        // initialization. Successful responses must still be real negotiated
        // InitializeResults, and response() requires the connection to close.
        match reply {
            Ok(Some(reply)) if reply.status == 200 => {
                release?;
                reply.validate_initialize()?;
            }
            Ok(Some(reply)) if (400..600).contains(&reply.status) => {}
            Ok(None) => {}
            other => {
                return Err(format!(
                    "invalid terminal initialization response: {other:?}"
                ));
            }
        }
        if !matches!(
            timeout(IO_TIMEOUT, TcpStream::connect(self.address)).await,
            Ok(Err(_))
        ) {
            return Err("successful shutdown left the endpoint accepting connections".to_owned());
        }
        Ok(alive)
    }

    async fn finish(mut self) -> Vec<String> {
        let mut failures = Vec::new();
        if let Some(server) = self.server.take() {
            let result = timeout(IO_TIMEOUT, server.shutdown()).await;
            if !matches!(result, Ok(Ok(()))) {
                failures.push(format!("server cleanup: {result:?}"));
            }
        }
        let usable = timeout(IO_TIMEOUT, self.tools.list_tools()).await;
        if !matches!(&usable, Ok(Ok(tools)) if tools.is_empty()) {
            failures.push(format!(
                "server changed caller Toolset authority: {usable:?}"
            ));
        }
        let result = timeout(IO_TIMEOUT, self.tools.shutdown()).await;
        if !matches!(result, Ok(Ok(()))) {
            failures.push(format!("caller Toolset cleanup: {result:?}"));
        }
        failures
    }
}

struct HttpWire {
    stream: BufReader<TcpStream>,
}

impl HttpWire {
    async fn session_post(address: SocketAddr, session: &str, body: &str) -> Result<Self, String> {
        let mut stream = timeout(IO_TIMEOUT, TcpStream::connect(address))
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        let request = format!(
            "POST /mcp HTTP/1.1\r\nHost: {address}\r\nMCP-Session-Id: {session}\r\nMCP-Protocol-Version: 2025-11-25\r\nAccept: application/json, text/event-stream\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        timeout(IO_TIMEOUT, stream.write_all(request.as_bytes()))
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        Ok(Self {
            stream: BufReader::new(stream),
        })
    }

    async fn delete(address: SocketAddr, session: &str) -> Result<Self, String> {
        let mut stream = timeout(IO_TIMEOUT, TcpStream::connect(address))
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        let headers = format!(
            "DELETE /mcp HTTP/1.1\r\nHost: {address}\r\nMCP-Session-Id: {session}\r\nMCP-Protocol-Version: 2025-11-25\r\nConnection: close\r\n\r\n"
        );
        timeout(IO_TIMEOUT, stream.write_all(headers.as_bytes()))
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        Ok(Self {
            stream: BufReader::new(stream),
        })
    }

    async fn post(
        address: SocketAddr,
        bytes: usize,
        expect_continue: bool,
    ) -> Result<Self, String> {
        let mut stream = timeout(IO_TIMEOUT, TcpStream::connect(address))
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        let expect = if expect_continue {
            "Expect: 100-continue\r\n"
        } else {
            ""
        };
        let headers = format!(
            "POST /mcp HTTP/1.1\r\nHost: {address}\r\nAccept: application/json, text/event-stream\r\nContent-Type: application/json\r\nContent-Length: {bytes}\r\nConnection: close\r\n{expect}\r\n"
        );
        timeout(IO_TIMEOUT, stream.write_all(headers.as_bytes()))
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        Ok(Self {
            stream: BufReader::new(stream),
        })
    }

    async fn write(&mut self, body: &str) -> Result<(), String> {
        timeout(IO_TIMEOUT, self.stream.get_mut().write_all(body.as_bytes()))
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())
    }

    async fn head(&mut self) -> Result<Option<HttpReply>, String> {
        timeout(IO_TIMEOUT, async {
            let mut line = String::new();
            match self.stream.read_line(&mut line).await {
                Ok(0) => return Ok(None),
                Ok(_) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
                    ) =>
                {
                    return Ok(None);
                }
                Err(error) => return Err(error.to_string()),
            }
            let status = line
                .split_whitespace()
                .nth(1)
                .and_then(|status| status.parse().ok())
                .ok_or_else(|| format!("invalid HTTP status: {line:?}"))?;
            let mut headers = BTreeMap::new();
            let mut bytes = line.len();
            loop {
                line.clear();
                let read = self
                    .stream
                    .read_line(&mut line)
                    .await
                    .map_err(|error| error.to_string())?;
                bytes += read;
                if read == 0 || bytes > 16 * 1024 {
                    return Err("incomplete or oversized HTTP headers".to_owned());
                }
                if line == "\r\n" {
                    break;
                }
                let (name, value) = line
                    .split_once(':')
                    .ok_or_else(|| format!("invalid header: {line:?}"))?;
                headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
            }
            Ok(Some(HttpReply {
                status,
                headers,
                body: Vec::new(),
            }))
        })
        .await
        .map_err(|error| error.to_string())?
    }

    async fn response(&mut self) -> Result<Option<HttpReply>, String> {
        let Some(mut response) = self.head().await? else {
            return Ok(None);
        };
        timeout(
            IO_TIMEOUT,
            (&mut self.stream)
                .take(64 * 1024)
                .read_to_end(&mut response.body),
        )
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?;
        if response.body.len() == 64 * 1024 {
            return Err("oversized HTTP response".to_owned());
        }
        Ok(Some(response))
    }
}

#[derive(Debug)]
struct HttpReply {
    status: u16,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

impl HttpReply {
    fn validate_initialize(&self) -> Result<(), String> {
        if self
            .headers
            .get("mcp-session-id")
            .is_none_or(|id| id.is_empty())
        {
            return Err("successful initialization supplied no session identity".to_owned());
        }
        let result = self.rpc_result(1)?;
        if result["protocolVersion"] != "2025-11-25" || !result["capabilities"].is_object() {
            return Err(format!("no negotiated InitializeResult: {result:?}"));
        }
        Ok(())
    }

    fn rpc_result(&self, id: u64) -> Result<Value, String> {
        let body = self.decoded_body()?;
        let text = std::str::from_utf8(&body).map_err(|error| error.to_string())?;
        let json = self.headers.get("content-type").is_some_and(|value| {
            value
                .split(';')
                .next()
                .is_some_and(|media| media.trim() == "application/json")
        });
        let payloads = if json {
            vec![text]
        } else {
            text.lines()
                .filter_map(|line| line.strip_prefix("data:").map(str::trim_start))
                .collect()
        };
        payloads
            .into_iter()
            .find_map(|payload| {
                let value = serde_json::from_str::<Value>(payload).ok()?;
                (value["jsonrpc"] == "2.0" && value["id"] == id && value.get("error").is_none())
                    .then(|| value.get("result").cloned())
                    .flatten()
            })
            .ok_or_else(|| format!("no JSON-RPC result for request {id}: {text:?}"))
    }

    fn decoded_body(&self) -> Result<Vec<u8>, String> {
        if self
            .headers
            .get("transfer-encoding")
            .is_some_and(|value| value.eq_ignore_ascii_case("chunked"))
        {
            let mut decoded = Vec::new();
            let mut remaining = self.body.as_slice();
            loop {
                let end = remaining
                    .windows(2)
                    .position(|pair| pair == b"\r\n")
                    .ok_or("missing HTTP chunk size")?;
                let size = std::str::from_utf8(&remaining[..end])
                    .map_err(|error| error.to_string())?
                    .split(';')
                    .next()
                    .ok_or("missing chunk size")?;
                let size = usize::from_str_radix(size, 16).map_err(|error| error.to_string())?;
                remaining = &remaining[end + 2..];
                if size == 0 {
                    break;
                }
                if size > remaining.len().saturating_sub(2) || &remaining[size..size + 2] != b"\r\n"
                {
                    return Err("incomplete HTTP chunk".to_owned());
                }
                decoded.extend_from_slice(&remaining[..size]);
                remaining = &remaining[size + 2..];
            }
            Ok(decoded)
        } else {
            Ok(self.body.clone())
        }
    }
}

#[tokio::test]
async fn admitted_initialize_cannot_escape_server_shutdown() {
    let mut case = StartupCase::start().await.expect("startup control starts");
    let observed = case.initialize_during_seal().await;
    let cleanup = case.finish().await;
    assert!(
        observed.is_ok(),
        "initialization/seal: {observed:?}; cleanup={cleanup:?}"
    );
    assert!(cleanup.is_empty(), "cleanup={cleanup:?}");
}

#[tokio::test]
async fn malformed_json_rejection_preserves_fresh_session() {
    let case = StartupCase::start().await.expect("startup control starts");
    let observed = case.rejected_then_fresh_session("{").await;
    let cleanup = case.finish().await;
    assert!(
        observed.is_ok(),
        "malformed JSON: {observed:?}; cleanup={cleanup:?}"
    );
    assert!(cleanup.is_empty(), "cleanup={cleanup:?}");
}

#[tokio::test]
async fn wrong_first_method_rejection_preserves_fresh_session() {
    let case = StartupCase::start().await.expect("startup control starts");
    let observed = case
        .rejected_then_fresh_session(r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#)
        .await;
    let cleanup = case.finish().await;
    assert!(
        observed.is_ok(),
        "wrong initial method: {observed:?}; cleanup={cleanup:?}"
    );
    assert!(cleanup.is_empty(), "cleanup={cleanup:?}");
}

enum SessionQuiescence {
    RejectedFirstRequest,
    InitializedThenDeleted,
    ShutdownInitialized,
    ShutdownReady,
    ShutdownLostInitializeObserver,
    ShutdownIndependentSessions,
    ShutdownDuringInitialize,
    CapacityAndReuse,
}

impl SessionQuiescence {
    fn check(self) {
        // An isolated current-thread runtime excludes other tests and Tokio's
        // documented multi-thread metric consistency limitation. block_on's
        // caller is not spawned, and this fixture creates no client tasks.
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("isolated server runtime builds");
        let metrics = runtime.metrics();
        let empty_baseline = metrics.num_alive_tasks();
        let mut case = runtime
            .block_on(StartupCase::start())
            .expect("startup control starts");
        runtime.block_on(tokio::task::yield_now());
        let serving_baseline = metrics.num_alive_tasks();
        let observed = catch_unwind(AssertUnwindSafe(|| {
            runtime.block_on(async {
                let request = match self {
                    Self::RejectedFirstRequest => {
                        let body = r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#;
                        let mut wire = HttpWire::post(case.address, body.len(), false).await?;
                        wire.write(body).await?;
                        let reply = wire.response().await?.ok_or("missing rejection response")?;
                        if !(400..500).contains(&reply.status) {
                            return Err(format!("first tools/list must be rejected: {reply:?}"));
                        }
                        drop(wire);
                        Ok(())
                    }
                    Self::InitializedThenDeleted => case.raw_session_closed().await,
                    Self::CapacityAndReuse => return case.session_capacity(&metrics).await,
                    mode @ (Self::ShutdownInitialized | Self::ShutdownReady) => {
                        // The HTTP observation has reached EOF and been dropped,
                        // but no DELETE has terminated this negotiated session.
                        let session = case.raw_session_initialized().await?;
                        if matches!(mode, Self::ShutdownReady) {
                            case.raw_session_ready(&session).await?;
                        }
                        return case.shutdown_at_task_baseline(&metrics, empty_baseline).await;
                    }
                    Self::ShutdownLostInitializeObserver => {
                        case.lose_initialize_observation().await?;
                        case.raw_session_closed().await?;
                        return case.shutdown_at_task_baseline(&metrics, empty_baseline).await;
                    }
                    Self::ShutdownIndependentSessions => {
                        case.independent_sessions().await?;
                        return case.shutdown_at_task_baseline(&metrics, empty_baseline).await;
                    }
                    Self::ShutdownDuringInitialize => {
                        let alive = case.initialize_during_seal().await?;
                        eprintln!("initialization/seal immediate runtime tasks={alive}; pre-start baseline={empty_baseline}");
                        return if alive == empty_baseline {
                            Ok(())
                        } else {
                            Err(format!("initialization/seal shutdown returned with runtime tasks={alive}; pre-start baseline={empty_baseline}"))
                        };
                    }
                };
                let settled = StartupCase::await_task_baseline(&metrics, serving_baseline).await;
                // Always check endpoint usability after observing quiescence,
                // even when it failed. Losing the endpoint cannot hide a leak
                // by reducing the task count back to the baseline.
                let usable = case.raw_session_closed().await;
                if request.is_err() || settled.is_err() || usable.is_err() {
                    return Err(format!(
                        "request={request:?}; request settlement={settled:?}; fresh session={usable:?}"
                    ));
                }
                Ok(())
            })
        }));
        let cleanup = catch_unwind(AssertUnwindSafe(|| runtime.block_on(case.finish())));
        let after_shutdown =
            runtime.block_on(StartupCase::await_task_baseline(&metrics, empty_baseline));
        // Capture both observations before shutting down the fixture runtime.
        // Runtime destruction cancels/drains remaining tasks even on the red
        // path. Neither metric equality nor this fallback proves the Kit itself
        // consumed every native JoinHandle.
        drop(metrics);
        drop(runtime);
        match observed {
            Ok(observed) => {
                assert!(
                    observed.is_ok(),
                    "session lifecycle: {observed:?}; cleanup={cleanup:?}; post-shutdown={after_shutdown:?}"
                );
            }
            Err(primary) => {
                eprintln!("session cleanup={cleanup:?}; post-shutdown={after_shutdown:?}");
                resume_unwind(primary);
            }
        }
        let cleanup = cleanup.unwrap_or_else(|primary| resume_unwind(primary));
        assert!(cleanup.is_empty(), "cleanup={cleanup:?}");
        assert!(after_shutdown.is_ok(), "post-shutdown={after_shutdown:?}");
    }
}

#[test]
fn rejected_first_request_releases_native_session_work() {
    SessionQuiescence::RejectedFirstRequest.check();
}

#[test]
fn initialized_deleted_session_releases_native_session_work() {
    SessionQuiescence::InitializedThenDeleted.check();
}

#[test]
fn server_shutdown_settles_initialized_native_session_before_success() {
    SessionQuiescence::ShutdownInitialized.check();
}

#[test]
fn server_shutdown_settles_ready_native_session_before_success() {
    SessionQuiescence::ShutdownReady.check();
}

#[test]
fn server_shutdown_settles_session_after_initialize_observer_loss() {
    SessionQuiescence::ShutdownLostInitializeObserver.check();
}

#[test]
fn server_shutdown_settles_independent_sessions_after_selected_delete() {
    SessionQuiescence::ShutdownIndependentSessions.check();
}

#[test]
fn server_shutdown_settles_native_work_during_initialize_admission() {
    SessionQuiescence::ShutdownDuringInitialize.check();
}

#[test]
fn native_session_capacity_rejects_excess_and_reuses_selected_slot() {
    SessionQuiescence::CapacityAndReuse.check();
}
