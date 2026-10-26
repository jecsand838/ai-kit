use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use rmcp::{
    ServiceExt,
    model::{CallToolRequestParams, ClientRequest, Content, Request, ServerResult},
    service::{PeerRequestOptions, RequestHandle, RoleClient, RunningService, ServiceError},
    transport::{
        StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use serde_json::json;
use tokio::{
    net::TcpListener,
    sync::watch,
    time::{Instant, timeout, timeout_at},
};
use tool_sdk::{
    ToolError, ToolHandlerBackend, ToolsetBackend,
    declaration::{ToolInfo, ToolName},
    execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation, ToolResult},
    mcp::{McpServer, McpServerConfig},
    schema::JsonSchema,
};

// The selected server policy is 64 admitted callbacks. This is a behavioral
// capacity contract, not an assertion about the private task container.
const CAPACITY: usize = 64;
const WAIT: Duration = Duration::from_secs(5);

struct CapacityTools {
    release: watch::Sender<bool>,
    entered: watch::Sender<usize>,
    constructed: AtomicUsize,
    dropped: AtomicUsize,
    cancelled: AtomicUsize,
    closed: AtomicBool,
}

impl CapacityTools {
    fn new() -> Self {
        Self {
            release: watch::channel(false).0,
            entered: watch::channel(0).0,
            constructed: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
            cancelled: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
        }
    }

    fn info() -> Result<ToolInfo, ToolError> {
        Ok(ToolInfo::new(
            ToolName::new("capacity")?,
            JsonSchema::strict(json!({
                "type":"object", "properties":{"hold":{"type":"boolean"},"id":{"type":"integer"}},
                "required":["hold","id"], "additionalProperties":false
            })),
        ))
    }
}

// Both the caller Toolset and its unique associated Handler deliberately lack
// Clone. The server is exercised through the direct public trait boundary.
struct CapacitySource {
    observed: Arc<CapacityTools>,
}

struct CapacityHandler {
    observed: Arc<CapacityTools>,
}

impl ToolsetBackend for CapacitySource {
    type Handler = CapacityHandler;

    async fn list_tools(&self) -> Result<Vec<ToolInfo>, ToolError> {
        if self.observed.closed.load(Ordering::SeqCst) {
            return Err(ToolError::Closed {
                operation: "list_tools".to_owned(),
            });
        }
        Ok(vec![CapacityTools::info()?])
    }

    async fn tool_handler(&self, name: ToolName) -> Result<Self::Handler, ToolError> {
        if self.observed.closed.load(Ordering::SeqCst) {
            return Err(ToolError::Closed {
                operation: "tool_handler".to_owned(),
            });
        }
        if name.as_str() != "capacity" {
            return Err(ToolError::UnknownTool { name });
        }
        self.observed.constructed.fetch_add(1, Ordering::SeqCst);
        Ok(CapacityHandler {
            observed: Arc::clone(&self.observed),
        })
    }

    async fn shutdown(&self) -> Result<(), ToolError> {
        self.observed.closed.store(true, Ordering::SeqCst);
        Ok(())
    }
}

impl ToolHandlerBackend for CapacityHandler {
    async fn step(&mut self, input: ToolHandlerInput) -> Result<ToolHandlerOutput, ToolError> {
        match input {
            ToolHandlerInput::Invoke(invocation) => {
                self.observed.entered.send_modify(|entered| *entered += 1);
                if invocation.arguments["hold"] == true {
                    let mut release = self.observed.release.subscribe();
                    release
                        .wait_for(|released| *released)
                        .await
                        .map_err(|error| ToolError::Transport {
                            tool: None,
                            message: error.to_string(),
                        })?;
                }
                let id =
                    invocation.arguments["id"]
                        .as_u64()
                        .ok_or_else(|| ToolError::Protocol {
                            message: "capacity fixture received invalid correlation".to_owned(),
                        })?;
                Ok(ToolHandlerOutput::Completed(ToolResult::text(format!(
                    "done:{id}"
                ))))
            }
            ToolHandlerInput::Cancel { .. } => {
                self.observed.cancelled.fetch_add(1, Ordering::SeqCst);
                Err(ToolError::Cancelled {
                    tool: ToolName::new("capacity")?,
                    reason: None,
                })
            }
            ToolHandlerInput::Interaction(_) => Err(ToolError::Protocol {
                message: "capacity fixture has no interactions".to_owned(),
            }),
        }
    }
}

impl Drop for CapacityHandler {
    fn drop(&mut self) {
        self.observed.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

struct CapacityCase {
    source: Arc<CapacitySource>,
    server: Option<McpServer>,
    client: RunningService<RoleClient, ()>,
    held: Vec<(usize, RequestHandle<RoleClient>)>,
}

impl CapacityCase {
    async fn start() -> Result<Self, String> {
        let source = Arc::new(CapacitySource {
            observed: Arc::new(CapacityTools::new()),
        });
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| error.to_string())?;
        let address: SocketAddr = listener.local_addr().map_err(|error| error.to_string())?;
        drop(listener);
        let config = McpServerConfig::new(address, "/mcp", "capacity:")
            .and_then(|config| config.with_shutdown_timeout(Duration::from_secs(2)))
            .map_err(|error| error.to_string())?;
        let server = timeout(WAIT, McpServer::start(Arc::clone(&source), config))
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        let transport = StreamableHttpClientTransport::from_config(
            StreamableHttpClientTransportConfig::with_uri(format!("http://{address}/mcp")),
        );
        let client = match timeout(WAIT, ().serve(transport)).await {
            Ok(Ok(client)) => client,
            failure => {
                let server_cleanup = timeout(WAIT, server.shutdown()).await;
                let source_cleanup = source.shutdown().await;
                return Err(format!(
                    "client initialization: {failure:?}; server={server_cleanup:?}; source={source_cleanup:?}"
                ));
            }
        };
        Ok(Self {
            source,
            server: Some(server),
            client,
            held: Vec::new(),
        })
    }

    async fn admit(&self, id: usize, hold: bool) -> Result<RequestHandle<RoleClient>, String> {
        timeout(
            WAIT,
            self.client.peer().send_cancellable_request(
                ClientRequest::CallToolRequest(Request::new(
                    CallToolRequestParams::new("capacity").with_arguments(
                        json!({"id":id,"hold":hold})
                            .as_object()
                            .cloned()
                            .ok_or("fixture arguments must be an object")?,
                    ),
                )),
                PeerRequestOptions::no_options(),
            ),
        )
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())
    }

    fn completed(id: usize, result: &ServerResult) -> bool {
        matches!(result, ServerResult::CallToolResult(result)
            if result.is_error != Some(true) && result.content == vec![Content::text(format!("done:{id}"))])
    }

    async fn drain(&mut self) -> Vec<String> {
        self.source.observed.release.send_replace(true);
        let deadline = Instant::now() + WAIT;
        let mut failures = Vec::new();
        for (id, request) in self.held.drain(..) {
            let result = timeout_at(deadline, request.await_response()).await;
            if !matches!(&result, Ok(Ok(result)) if Self::completed(id, result)) {
                failures.push(format!(
                    "accepted call {id} failed to drain exactly: {result:?}"
                ));
            }
        }
        failures
    }

    async fn bounded_admission(&mut self) -> Result<(), String> {
        for id in 0..CAPACITY {
            let request = self.admit(id, true).await?;
            self.held.push((id, request));
        }
        let mut entered = self.source.observed.entered.subscribe();
        timeout(WAIT, entered.wait_for(|count| *count == CAPACITY))
            .await
            .map_err(|error| format!("accepted calls did not enter: {error}"))?
            .map_err(|error| error.to_string())?;
        let mut overflow = self.admit(CAPACITY, true).await?;
        // Borrow the receipt so a failing baseline timeout does not drop its
        // observation before cleanup releases the actually accepted call.
        let rejected = timeout(Duration::from_secs(2), &mut overflow.rx).await;
        let construction = self.source.observed.constructed.load(Ordering::SeqCst);
        let rejected_before_construction =
            matches!(&rejected, Ok(Ok(Err(ServiceError::McpError(_))))) && construction == CAPACITY;
        self.source.observed.release.send_replace(true);
        let overflow_cleanup = if rejected.is_err() {
            let result = timeout(WAIT, overflow.await_response()).await;
            if matches!(&result, Ok(Ok(result)) if Self::completed(CAPACITY, result)) {
                None
            } else {
                Some(format!("overflow cleanup: {result:?}"))
            }
        } else {
            None
        };
        let drained = self.drain().await;
        if !rejected_before_construction {
            return Err(format!(
                "the 65th request was not rejected before construction: result={rejected:?}, constructed={construction}; overflow={overflow_cleanup:?}; drain={drained:?}"
            ));
        }
        if overflow_cleanup.is_some() || !drained.is_empty() {
            return Err(format!(
                "capacity cleanup: overflow={overflow_cleanup:?}; drain={drained:?}"
            ));
        }
        // A drained server must accept actual subsequent execution, not merely
        // keep rejecting once it has exhausted capacity.
        let result = timeout(
            WAIT,
            self.admit(CAPACITY + 1, false).await?.await_response(),
        )
        .await;
        if !matches!(&result, Ok(Ok(result)) if Self::completed(CAPACITY + 1, result)) {
            return Err(format!("capacity was not reusable: {result:?}"));
        }
        Ok(())
    }

    async fn completed_entries_are_reaped(&self) -> Result<(), String> {
        for id in 0..(CAPACITY + 8) {
            let result = timeout(WAIT, self.admit(id, false).await?.await_response()).await;
            if !matches!(&result, Ok(Ok(result)) if Self::completed(id, result)) {
                return Err(format!(
                    "sequential completed call {id} lost capacity: {result:?}"
                ));
            }
        }
        if self.source.observed.constructed.load(Ordering::SeqCst) != CAPACITY + 8 {
            return Err(
                "sequential execution constructed an unexpected number of handlers".to_owned(),
            );
        }
        Ok(())
    }

    async fn finish(mut self) -> Vec<String> {
        let mut failures = self.drain().await;
        let client = timeout(WAIT, self.client.close()).await;
        if !matches!(client, Ok(Ok(_))) {
            failures.push(format!("client cleanup: {client:?}"));
        }
        if let Some(server) = self.server.take() {
            let result = timeout(WAIT, server.shutdown()).await;
            if !matches!(result, Ok(Ok(()))) {
                failures.push(format!("server cleanup: {result:?}"));
            }
        }
        let constructed = self.source.observed.constructed.load(Ordering::SeqCst);
        let dropped = self.source.observed.dropped.load(Ordering::SeqCst);
        if constructed != dropped {
            failures.push(format!(
                "unique handlers remain alive: constructed={constructed}, dropped={dropped}"
            ));
        }
        let cancelled = self.source.observed.cancelled.load(Ordering::SeqCst);
        if cancelled != 0 {
            failures.push(format!(
                "completed, released calls were unexpectedly cancelled: {cancelled}"
            ));
        }
        let closed_by_server = self.source.observed.closed.load(Ordering::SeqCst);
        let usable = async {
            let mut handler = self.source.tool_handler(ToolName::new("capacity")?).await?;
            handler
                .step(ToolHandlerInput::Invoke(ToolInvocation {
                    call_id: None,
                    arguments: json!({"id":999,"hold":false}),
                }))
                .await
        };
        let usable = timeout(WAIT, usable).await;
        if closed_by_server
            || !matches!(&usable, Ok(Ok(ToolHandlerOutput::Completed(result))) if *result == ToolResult::text("done:999"))
        {
            failures.push(format!(
                "server changed caller Toolset ownership: {usable:?}"
            ));
        }
        let source = self.source.shutdown().await;
        if let Err(error) = source {
            failures.push(format!("source cleanup: {error}"));
        }
        failures
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_request_capacity_rejects_before_handler_construction_and_reuses_slots() {
    let mut case = CapacityCase::start()
        .await
        .expect("capacity fixture starts");
    let observed = case.bounded_admission().await;
    let cleanup = case.finish().await;
    assert!(
        observed.is_ok(),
        "capacity: {observed:?}; cleanup={cleanup:?}"
    );
    assert!(cleanup.is_empty(), "cleanup={cleanup:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_completed_requests_do_not_accumulate_capacity() {
    let case = CapacityCase::start()
        .await
        .expect("capacity fixture starts");
    let observed = case.completed_entries_are_reaped().await;
    let cleanup = case.finish().await;
    assert!(
        observed.is_ok(),
        "reaping: {observed:?}; cleanup={cleanup:?}"
    );
    assert!(cleanup.is_empty(), "cleanup={cleanup:?}");
}
