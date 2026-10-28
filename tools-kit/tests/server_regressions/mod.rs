use std::{
    collections::BTreeMap,
    future::Future,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};

use rmcp::{
    ClientHandler, ServiceExt,
    model::{
        CallToolRequestParams, ClientCapabilities, ClientInfo, ClientRequest,
        CreateElicitationRequestParams, CreateElicitationResult, ElicitationAction,
    },
    service::{PeerRequestOptions, RequestContext, RoleClient},
    transport::{
        StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use serde_json::json;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{Notify, watch},
    time::timeout,
};
use tools_kit::{
    ToolError, ToolHandlerBackend, Toolset, ToolsetBackend,
    declaration::{ToolInfo, ToolName},
    execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation, ToolResult},
    interaction::{
        ToolInteractionId, ToolInteractionKind, ToolInteractionRequest, ToolInteractionResponse,
    },
    local::ToolRegistration,
    mcp::{
        McpClientConfig, McpConnectionConfig, McpServer, McpServerConfig,
        McpServerInteractionPolicy,
    },
    schema::{JsonSchema, ValidationIssue},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InvocationState {
    Idle,
    Listing,
    Constructing,
    Entered,
    Eliciting,
    Cancelling,
    Settled,
}

#[derive(Debug)]
struct GatedTools {
    state: watch::Sender<InvocationState>,
    release: Notify,
    effects: AtomicUsize,
    invocations: AtomicUsize,
    cancelled: AtomicBool,
    cancellations: AtomicUsize,
    cancellation_settlements: AtomicUsize,
    handler_constructions: AtomicUsize,
    gate_listing: AtomicBool,
    listing_release: Notify,
    listing_abandoned: AtomicBool,
    cancellation_failure: AtomicBool,
    gate_cancellation: AtomicBool,
    cancellation_release: Notify,
    gate_construction: AtomicBool,
    construction_release: Notify,
    construction_abandoned: AtomicBool,
    panic_on_invoke: AtomicBool,
    elicit: AtomicBool,
    validate_form_schema: AtomicBool,
    interaction_responses: AtomicUsize,
    elicitation_release: Notify,
    elicitation_settled: watch::Sender<bool>,
    closed: AtomicBool,
}

impl GatedTools {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: watch::channel(InvocationState::Idle).0,
            release: Notify::new(),
            effects: AtomicUsize::new(0),
            invocations: AtomicUsize::new(0),
            cancelled: AtomicBool::new(false),
            cancellations: AtomicUsize::new(0),
            cancellation_settlements: AtomicUsize::new(0),
            handler_constructions: AtomicUsize::new(0),
            gate_listing: AtomicBool::new(false),
            listing_release: Notify::new(),
            listing_abandoned: AtomicBool::new(false),
            cancellation_failure: AtomicBool::new(false),
            gate_cancellation: AtomicBool::new(false),
            cancellation_release: Notify::new(),
            gate_construction: AtomicBool::new(false),
            construction_release: Notify::new(),
            construction_abandoned: AtomicBool::new(false),
            panic_on_invoke: AtomicBool::new(false),
            elicit: AtomicBool::new(false),
            validate_form_schema: AtomicBool::new(true),
            interaction_responses: AtomicUsize::new(0),
            elicitation_release: Notify::new(),
            elicitation_settled: watch::channel(false).0,
            closed: AtomicBool::new(false),
        })
    }

    fn info(name: &str) -> ToolInfo {
        ToolInfo::new(
            ToolName::new(name).unwrap(),
            JsonSchema::strict(json!({"type":"object"})),
        )
    }

    async fn wait_for(&self, expected: InvocationState) -> bool {
        let mut state = self.state.subscribe();
        timeout(Duration::from_secs(5), async {
            state.wait_for(|actual| *actual == expected).await.is_ok()
        })
        .await
        .unwrap_or(false)
    }
}

// A direct, non-Clone caller Toolset and associated Handler. No supplied
// normalization ingress or other crate participates in this server contract.
struct ServedGate {
    observation: Arc<GatedTools>,
}
struct GatedHandler {
    observation: Arc<GatedTools>,
    gated: bool,
    pending: Option<ToolInteractionRequest>,
}

struct ConstructionObserver {
    observation: Arc<GatedTools>,
    returned: bool,
}

impl Drop for ConstructionObserver {
    fn drop(&mut self) {
        if !self.returned {
            self.observation
                .construction_abandoned
                .store(true, Ordering::SeqCst);
        }
    }
}

struct ListingObserver {
    observation: Arc<GatedTools>,
    returned: bool,
}

impl Drop for ListingObserver {
    fn drop(&mut self) {
        if !self.returned {
            self.observation
                .listing_abandoned
                .store(true, Ordering::SeqCst);
        }
        self.observation
            .state
            .send_replace(InvocationState::Settled);
    }
}

impl ToolsetBackend for ServedGate {
    type Handler = GatedHandler;
    async fn list_tools(&self) -> Result<Vec<ToolInfo>, ToolError> {
        if self.observation.closed.load(Ordering::SeqCst) {
            return Err(ToolError::Closed {
                operation: "list_tools".into(),
            });
        }
        if self.observation.gate_listing.load(Ordering::SeqCst) {
            let mut listing = ListingObserver {
                observation: Arc::clone(&self.observation),
                returned: false,
            };
            self.observation
                .state
                .send_replace(InvocationState::Listing);
            self.observation.listing_release.notified().await;
            listing.returned = true;
        }
        Ok(vec![GatedTools::info("gated"), GatedTools::info("probe")])
    }
    async fn tool_handler(&self, name: ToolName) -> Result<Self::Handler, ToolError> {
        self.observation
            .handler_constructions
            .fetch_add(1, Ordering::SeqCst);
        self.list_tools().await?;
        if name.as_str() == "gated" && self.observation.gate_construction.load(Ordering::SeqCst) {
            let mut construction = ConstructionObserver {
                observation: Arc::clone(&self.observation),
                returned: false,
            };
            self.observation
                .state
                .send_replace(InvocationState::Constructing);
            self.observation.construction_release.notified().await;
            construction.returned = true;
        }
        Ok(GatedHandler {
            observation: Arc::clone(&self.observation),
            gated: name.as_str() == "gated",
            pending: None,
        })
    }
    async fn shutdown(&self) -> Result<(), ToolError> {
        self.observation.closed.store(true, Ordering::SeqCst);
        Ok(())
    }
}

impl ToolHandlerBackend for GatedHandler {
    fn step(
        &mut self,
        input: ToolHandlerInput,
    ) -> impl Future<Output = Result<ToolHandlerOutput, ToolError>> + Send {
        // RPITIT implementations may act before returning their future. A
        // cancellation fence must precede the method call, not just its poll.
        if self.gated && matches!(&input, ToolHandlerInput::Invoke(_)) {
            self.observation.invocations.fetch_add(1, Ordering::SeqCst);
        }
        async move {
            match input {
                ToolHandlerInput::Invoke(_) => {
                    if self.gated {
                        assert!(
                            !self.observation.panic_on_invoke.load(Ordering::SeqCst),
                            "deliberate caller handler panic for server ownership proof"
                        );
                        self.observation
                            .state
                            .send_replace(InvocationState::Entered);
                        if self.observation.elicit.load(Ordering::SeqCst) {
                            let request = ToolInteractionRequest {
                                id: ToolInteractionId::new(),
                                tool_name: ToolName::new("gated").unwrap(),
                                call_id: None,
                                prompt: "Confirm execution".to_owned(),
                                kind: ToolInteractionKind::Form {
                                    schema: json!({
                                        "type": "object",
                                        "properties": {"confirm": {"type": "boolean"}},
                                        "required": ["confirm"]
                                    }),
                                    schema_validation: self
                                        .observation
                                        .validate_form_schema
                                        .load(Ordering::SeqCst),
                                },
                            };
                            self.pending = Some(request.clone());
                            return Ok(ToolHandlerOutput::Interaction(request));
                        }
                        self.observation.release.notified().await;
                        self.observation.effects.fetch_add(1, Ordering::SeqCst);
                    }
                    Ok(ToolHandlerOutput::Completed(ToolResult::text("probe")))
                }
                ToolHandlerInput::Cancel { .. } => {
                    self.observation.cancelled.store(true, Ordering::SeqCst);
                    self.observation
                        .cancellations
                        .fetch_add(1, Ordering::SeqCst);
                    self.observation
                        .state
                        .send_replace(InvocationState::Cancelling);
                    if self.observation.gate_cancellation.load(Ordering::SeqCst) {
                        self.observation.cancellation_release.notified().await;
                    }
                    // This is after the optional gate: Handler Drop alone must
                    // not count as a completed cancellation future.
                    self.observation
                        .cancellation_settlements
                        .fetch_add(1, Ordering::SeqCst);
                    if self.observation.cancellation_failure.load(Ordering::SeqCst) {
                        return Err(ToolError::Transport {
                            tool: Some(ToolName::new("gated").unwrap()),
                            message: "deliberate cancellation cleanup failure".to_owned(),
                        });
                    }
                    Err(ToolError::Cancelled {
                        tool: ToolName::new("gated").unwrap(),
                        reason: Some("cancelled".into()),
                    })
                }
                ToolHandlerInput::Interaction(response) => {
                    self.observation
                        .interaction_responses
                        .fetch_add(1, Ordering::SeqCst);
                    // This caller handler keeps its pending invocation after a
                    // rejected response, as the supplied Local handler does.
                    // Its declared form has exactly one boolean input; no test
                    // schema interpreter or private Kit validator is involved.
                    let pending =
                        self.pending
                            .as_ref()
                            .ok_or_else(|| ToolError::InvalidInteraction {
                                id: response.interaction_id().as_str().to_owned(),
                                message: "no form is pending".to_owned(),
                            })?;
                    let ToolInteractionResponse::Submit { id, content } = response else {
                        return Err(ToolError::InvalidInteraction {
                            id: pending.id.as_str().to_owned(),
                            message: "the confirmation requires a submitted form".to_owned(),
                        });
                    };
                    if id != pending.id || !content.is_object() {
                        return Err(ToolError::InvalidInteraction {
                            id: id.as_str().to_owned(),
                            message: "confirmation form must be an object for the pending id"
                                .to_owned(),
                        });
                    }
                    if self.observation.validate_form_schema.load(Ordering::SeqCst)
                        && !content
                            .get("confirm")
                            .is_some_and(serde_json::Value::is_boolean)
                    {
                        return Err(ToolError::InvalidArguments {
                            tool: pending.tool_name.clone(),
                            message: "confirmation form failed schema validation".to_owned(),
                            issues: vec![ValidationIssue {
                                path: "/confirm".to_owned(),
                                message: "expected boolean".to_owned(),
                            }],
                        });
                    }
                    self.pending = None;
                    self.observation.effects.fetch_add(1, Ordering::SeqCst);
                    Ok(ToolHandlerOutput::Completed(ToolResult::text("confirmed")))
                }
            }
        }
    }
}

#[derive(Debug)]
struct GatedClient {
    observation: Arc<GatedTools>,
    response: FormResponse,
}

impl ClientHandler for GatedClient {
    fn get_info(&self) -> ClientInfo {
        let mut info = ClientInfo::default();
        info.capabilities = ClientCapabilities::builder().enable_elicitation().build();
        info
    }

    async fn create_elicitation(
        &self,
        _request: CreateElicitationRequestParams,
        context: RequestContext<RoleClient>,
    ) -> Result<CreateElicitationResult, rmcp::ErrorData> {
        self.observation
            .state
            .send_replace(InvocationState::Eliciting);
        let action = tokio::select! {
            _ = context.ct.cancelled() => ElicitationAction::Cancel,
            _ = self.observation.elicitation_release.notified() => ElicitationAction::Accept,
        };
        self.observation.elicitation_settled.send_replace(true);
        Ok(CreateElicitationResult {
            action,
            content: self.response.content(),
            meta: None,
        })
    }
}

impl Drop for GatedHandler {
    fn drop(&mut self) {
        if self.gated {
            self.observation
                .state
                .send_replace(InvocationState::Settled);
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum FormResponse {
    Valid,
    SchemaFree,
    SchemaFreeNull,
    InvalidSchema,
    Missing,
    Null,
    InvalidSchemaCleanupFailure,
}

impl FormResponse {
    fn content(self) -> Option<serde_json::Value> {
        match self {
            Self::Valid => Some(json!({"confirm": true})),
            Self::SchemaFree | Self::InvalidSchema | Self::InvalidSchemaCleanupFailure => {
                Some(json!({"confirm": "not a boolean"}))
            }
            Self::Missing => None,
            Self::Null | Self::SchemaFreeNull => Some(serde_json::Value::Null),
        }
    }

    fn rejects(self) -> bool {
        !matches!(self, Self::Valid | Self::SchemaFree)
    }

    async fn run(self) {
        let observation = GatedTools::new();
        observation.elicit.store(true, Ordering::SeqCst);
        observation.validate_form_schema.store(
            !matches!(self, Self::SchemaFree | Self::SchemaFreeNull),
            Ordering::SeqCst,
        );
        observation
            .gate_cancellation
            .store(self.rejects(), Ordering::SeqCst);
        let cleanup_failure = matches!(self, Self::InvalidSchemaCleanupFailure);
        observation
            .cancellation_failure
            .store(cleanup_failure, Ordering::SeqCst);
        let tools = Arc::new(ServedGate {
            observation: Arc::clone(&observation),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);
        let config = McpServerConfig::new(bind, "/mcp", "form:")
            .unwrap()
            .with_shutdown_timeout(Duration::from_secs(1))
            .unwrap()
            .with_interaction_policy(McpServerInteractionPolicy::ElicitFormIfClientSupports);
        let server = McpServer::start(Arc::clone(&tools), config).await.unwrap();
        let transport = StreamableHttpClientTransport::from_config(
            StreamableHttpClientTransportConfig::with_uri(format!("http://{bind}/mcp")),
        );
        let connected = timeout(
            Duration::from_secs(5),
            GatedClient {
                observation: Arc::clone(&observation),
                response: self,
            }
            .serve(transport),
        )
        .await;
        let mut client = match connected {
            Ok(Ok(client)) => client,
            failure => {
                let cleanup = timeout(Duration::from_secs(10), server.shutdown()).await;
                let source_cleanup = tools.shutdown().await;
                panic!(
                    "form client setup failed: {failure:?}; server: {cleanup:?}; source: {source_cleanup:?}"
                );
            }
        };
        let admission = timeout(
            Duration::from_secs(5),
            client.peer().send_cancellable_request(
                ClientRequest::CallToolRequest(rmcp::model::Request::new(
                    CallToolRequestParams::new("gated"),
                )),
                PeerRequestOptions::no_options(),
            ),
        )
        .await;
        let request = match admission {
            Ok(Ok(request)) => request,
            failure => {
                observation.elicitation_release.notify_one();
                observation.cancellation_release.notify_one();
                let close = timeout(Duration::from_secs(5), client.close()).await;
                let cleanup = timeout(Duration::from_secs(10), server.shutdown()).await;
                let source_cleanup = tools.shutdown().await;
                panic!(
                    "form admission failed: {failure:?}; client: {close:?}; server: {cleanup:?}; source: {source_cleanup:?}"
                );
            }
        };
        let entered = observation.wait_for(InvocationState::Eliciting).await;
        observation.elicitation_release.notify_one();
        let mut response = Box::pin(request.await_response());
        let mut result = None;
        let mut cancel_entered = false;
        let mut response_waited_for_cleanup = true;
        if self.rejects() {
            tokio::select! {
                ready = &mut response => {
                    result = Some(ready);
                    response_waited_for_cleanup = false;
                }
                entered = observation.wait_for(InvocationState::Cancelling) => {
                    cancel_entered = entered;
                }
            }
            if cancel_entered {
                // Entry is an exact handler signal, and its cancellation gate
                // is still closed. Drop is not cancellation settlement.
                if let Poll::Ready(ready) =
                    std::future::poll_fn(|cx| Poll::Ready(response.as_mut().poll(cx))).await
                {
                    result = Some(ready);
                    response_waited_for_cleanup = false;
                }
                response_waited_for_cleanup &=
                    observation.cancellation_settlements.load(Ordering::SeqCst) == 0;
            }
            observation.cancellation_release.notify_one();
        }
        if result.is_none() {
            result = timeout(Duration::from_secs(5), &mut response).await.ok();
        }
        drop(response);
        let settled = observation.wait_for(InvocationState::Settled).await;
        let cancellations = observation.cancellations.load(Ordering::SeqCst);
        let cancellation_settlements = observation.cancellation_settlements.load(Ordering::SeqCst);
        let interactions = observation.interaction_responses.load(Ordering::SeqCst);
        let effects = observation.effects.load(Ordering::SeqCst);

        // Always unblock independent cleanup before evaluating the observations.
        observation.elicitation_release.notify_one();
        observation.cancellation_release.notify_one();
        let close = timeout(Duration::from_secs(5), client.close()).await;
        let shutdown = timeout(Duration::from_secs(10), server.shutdown()).await;
        let closed_by_server = observation.closed.load(Ordering::SeqCst);
        let source_cleanup = tools.shutdown().await;
        assert!(
            matches!(close, Ok(Ok(_))) && source_cleanup.is_ok() && !closed_by_server,
            "{self:?}: client={close:?}; source={source_cleanup:?}; closed_by_server={closed_by_server}; primary={result:?}; server={shutdown:?}"
        );
        let diagnostic = format!(
            "{self:?}: entered={entered}, settled={settled}, cancel_entered={cancel_entered}, cancellations={cancellations}, cancellation_settlements={cancellation_settlements}, interactions={interactions}, effects={effects}, response_waited_for_cleanup={response_waited_for_cleanup}, result={result:?}, server={shutdown:?}"
        );
        assert!(entered && settled, "{diagnostic}");
        assert_eq!(effects, usize::from(!self.rejects()), "{diagnostic}");
        assert_eq!(
            interactions,
            usize::from(!self.rejects()),
            "malformed protocol input must not advance the pending handler; {diagnostic}"
        );
        assert_eq!(cancellations, usize::from(self.rejects()), "{diagnostic}");
        assert_eq!(
            cancellation_settlements,
            usize::from(self.rejects()),
            "{diagnostic}"
        );
        assert!(
            !self.rejects() || cancel_entered && response_waited_for_cleanup,
            "the response escaped before cancellation settled; {diagnostic}"
        );
        if cleanup_failure {
            assert!(
                matches!(&result,
                    Some(Err(rmcp::service::ServiceError::McpError(error)))
                        if error.code == rmcp::model::ErrorCode::INTERNAL_ERROR
                            && error.message.contains("form response failed schema validation")
                            && error.message.contains("deliberate cancellation cleanup failure")),
                "primary and cleanup errors must survive protocol mapping; {diagnostic}"
            );
            assert!(
                matches!(&shutdown, Ok(Err(ToolError::Transport { message, .. }))
                    if message.contains("form response failed schema validation")
                        && message.contains("deliberate cancellation cleanup failure")),
                "server terminal result must retain both errors; {diagnostic}"
            );
        } else {
            assert!(matches!(shutdown, Ok(Ok(()))), "{diagnostic}");
            if self.rejects() {
                assert!(
                    matches!(&result,
                        Some(Err(rmcp::service::ServiceError::McpError(error)))
                            if error.code == rmcp::model::ErrorCode::INVALID_PARAMS),
                    "the form error must remain a protocol input rejection; {diagnostic}"
                );
            } else {
                assert!(
                    matches!(&result,
                        Some(Ok(rmcp::model::ServerResult::CallToolResult(output)))
                            if output.is_error != Some(true)
                                && output.content == vec![rmcp::model::Content::text("confirmed")]),
                    "valid accepted response did not complete; {diagnostic}"
                );
            }
        }
    }
}

#[tokio::test]
async fn accepted_form_valid_response_control() {
    FormResponse::Valid.run().await;
}

#[tokio::test]
async fn accepted_form_schema_validation_disabled_control() {
    FormResponse::SchemaFree.run().await;
}

#[tokio::test]
async fn accepted_form_schema_validation_disabled_still_rejects_null() {
    FormResponse::SchemaFreeNull.run().await;
}

#[tokio::test]
async fn accepted_form_schema_mismatch_cancels_pending_handler() {
    FormResponse::InvalidSchema.run().await;
}

#[tokio::test]
async fn accepted_form_missing_content_cancels_pending_handler() {
    FormResponse::Missing.run().await;
}

#[tokio::test]
async fn accepted_form_null_content_cancels_pending_handler() {
    FormResponse::Null.run().await;
}

#[tokio::test]
async fn accepted_form_rejection_retains_primary_and_cleanup_errors() {
    FormResponse::InvalidSchemaCleanupFailure.run().await;
}

#[derive(Clone, Copy, Debug)]
enum ServerOwnership {
    Complete,
    Shutdown,
    Drop,
    CancelShutdown,
    CancelRequest,
    CancelConstruction,
    CancelShutdownConstruction,
    CancelElicitation,
    ElicitationDeadline,
    ShutdownCleanupFailure,
    ShutdownCleanupTimeout,
    CancelShutdownCleanup,
    ShutdownListing,
    ShutdownDeclaration,
}

impl ServerOwnership {
    fn elicits(self) -> bool {
        matches!(self, Self::CancelElicitation | Self::ElicitationDeadline)
    }

    async fn run(self) {
        let observation = GatedTools::new();
        let listing = matches!(self, Self::ShutdownListing | Self::ShutdownDeclaration);
        observation.gate_listing.store(listing, Ordering::SeqCst);
        let constructing = matches!(
            self,
            Self::CancelConstruction | Self::CancelShutdownConstruction
        );
        observation
            .gate_construction
            .store(constructing, Ordering::SeqCst);
        observation.elicit.store(self.elicits(), Ordering::SeqCst);
        observation.cancellation_failure.store(
            matches!(self, Self::ShutdownCleanupFailure),
            Ordering::SeqCst,
        );
        observation.gate_cancellation.store(
            matches!(
                self,
                Self::ShutdownCleanupTimeout | Self::CancelShutdownCleanup
            ),
            Ordering::SeqCst,
        );
        let tools = Arc::new(ServedGate {
            observation: Arc::clone(&observation),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);
        let config = McpServerConfig::new(bind, "/mcp", "ownership:")
            .unwrap()
            .with_shutdown_timeout(Duration::from_secs(1))
            .unwrap()
            .with_interaction_policy(McpServerInteractionPolicy::ElicitFormIfClientSupports);
        let mut server = Some(McpServer::start(Arc::clone(&tools), config).await.unwrap());
        let transport = StreamableHttpClientTransport::from_config(
            StreamableHttpClientTransportConfig::with_uri(format!("http://{bind}/mcp")),
        );
        let connection = timeout(
            Duration::from_secs(5),
            GatedClient {
                observation: Arc::clone(&observation),
                response: FormResponse::Valid,
            }
            .serve(transport),
        )
        .await;
        let mut client = match connection {
            Ok(Ok(client)) => client,
            failure => {
                let cleanup =
                    timeout(Duration::from_secs(10), server.take().unwrap().shutdown()).await;
                let source_cleanup = tools.shutdown().await;
                panic!(
                    "native client setup failed: {failure:?}; cleanup: {cleanup:?}; source: {source_cleanup:?}"
                );
            }
        };
        let admission = timeout(
            Duration::from_secs(5),
            client.peer().send_cancellable_request(
                if matches!(self, Self::ShutdownListing) {
                    ClientRequest::ListToolsRequest(rmcp::model::ListToolsRequest::default())
                } else {
                    ClientRequest::CallToolRequest(rmcp::model::Request::new(
                        CallToolRequestParams::new("gated"),
                    ))
                },
                PeerRequestOptions::no_options(),
            ),
        )
        .await;
        let request = match admission {
            Ok(Ok(request)) => request,
            failure => {
                observation.release.notify_one();
                observation.gate_listing.store(false, Ordering::SeqCst);
                observation.listing_release.notify_one();
                observation.construction_release.notify_one();
                observation.elicitation_release.notify_one();
                observation.cancellation_release.notify_one();
                let close = timeout(Duration::from_secs(5), client.close()).await;
                let cleanup =
                    timeout(Duration::from_secs(10), server.take().unwrap().shutdown()).await;
                let source_cleanup = tools.shutdown().await;
                panic!(
                    "request admission failed: {failure:?}; client: {close:?}; server: {cleanup:?}; source: {source_cleanup:?}"
                );
            }
        };
        let entered = observation
            .wait_for(if listing {
                InvocationState::Listing
            } else if constructing {
                InvocationState::Constructing
            } else if self.elicits() {
                InvocationState::Eliciting
            } else {
                InvocationState::Entered
            })
            .await;
        let mut shutdown_ok = true;
        let mut shutdown_was_pending = true;
        let mut cancel_ok = true;
        let mut cleanup_retained = true;
        let mut shutdown_diagnostic = None;
        match self {
            Self::Complete => {
                observation.release.notify_one();
                cancel_ok = matches!(
                    timeout(Duration::from_secs(5), request.await_response()).await,
                    Ok(Ok(rmcp::model::ServerResult::CallToolResult(result)))
                        if result.is_error != Some(true)
                            && result.content == vec![rmcp::model::Content::text("probe")]
                );
                shutdown_ok = matches!(
                    timeout(Duration::from_secs(10), server.take().unwrap().shutdown()).await,
                    Ok(Ok(()))
                );
            }
            Self::Shutdown => {
                shutdown_ok = matches!(
                    timeout(Duration::from_secs(10), server.take().unwrap().shutdown()).await,
                    Ok(Ok(()))
                );
            }
            Self::ShutdownCleanupFailure
            | Self::ShutdownCleanupTimeout
            | Self::ShutdownListing
            | Self::ShutdownDeclaration => {
                let result =
                    timeout(Duration::from_secs(5), server.take().unwrap().shutdown()).await;
                shutdown_ok = match &result {
                    Ok(Err(ToolError::Transport { message, .. })) => {
                        if matches!(self, Self::ShutdownCleanupFailure) {
                            message.contains("deliberate cancellation cleanup failure")
                        } else {
                            message.contains("timeout")
                                || message.contains("timed out")
                                || message.contains("deadline")
                        }
                    }
                    _ => false,
                };
                shutdown_diagnostic = Some(format!("{result:?}"));
                if listing {
                    cleanup_retained = *observation.state.borrow() == InvocationState::Listing
                        && !observation.listing_abandoned.load(Ordering::SeqCst);
                }
                if matches!(self, Self::ShutdownCleanupTimeout) {
                    // The configured observation deadline may expire, but the
                    // admitted Cancel future must remain owned until released.
                    cleanup_retained = *observation.state.borrow() == InvocationState::Cancelling;
                    observation.cancellation_release.notify_one();
                }
            }
            Self::CancelShutdownCleanup => {
                let mut shutdown = Box::pin(server.take().unwrap().shutdown());
                // Poll the consuming shutdown while waiting for the exact Cancel
                // entry signal. Dropping an unpolled future proves nothing here.
                tokio::select! {
                    result = &mut shutdown => {
                        shutdown_was_pending = false;
                        shutdown_diagnostic = Some(format!("{result:?}"));
                    }
                    cancelling = observation.wait_for(InvocationState::Cancelling) => {
                        cleanup_retained = cancelling;
                    }
                }
                cleanup_retained &= *observation.state.borrow() == InvocationState::Cancelling;
                drop(shutdown);
                observation.cancellation_release.notify_one();
            }
            Self::Drop => {
                drop(server.take());
            }
            Self::CancelShutdown | Self::CancelShutdownConstruction => {
                let mut shutdown = Box::pin(server.take().unwrap().shutdown());
                let completed =
                    std::future::poll_fn(|cx| Poll::Ready(shutdown.as_mut().poll(cx))).await;
                shutdown_was_pending = completed.is_pending();
                if let Poll::Ready(result) = completed {
                    shutdown_ok = result.is_ok();
                }
                drop(shutdown);
            }
            Self::CancelRequest | Self::CancelConstruction | Self::CancelElicitation => {
                cancel_ok = matches!(
                    timeout(
                        Duration::from_secs(5),
                        request.cancel(Some("cancel entered invocation".into()))
                    )
                    .await,
                    Ok(Ok(()))
                );
                if constructing && cancel_ok {
                    // A following protocol request confirms that the service has
                    // consumed the preceding cancellation notification before
                    // the construction gate is released.
                    cancel_ok = matches!(
                        timeout(
                            Duration::from_secs(5),
                            client.peer().send_request(ClientRequest::PingRequest(
                                rmcp::model::PingRequest::default(),
                            ))
                        )
                        .await,
                        Ok(Ok(_))
                    );
                }
            }
            Self::ElicitationDeadline => {
                // The client deliberately leaves the form pending. The server
                // must enforce its existing interaction deadline and cancel
                // both the nested request and the unique pending handler.
                cancel_ok = matches!(
                    timeout(Duration::from_secs(35), request.await_response()).await,
                    Ok(Err(rmcp::service::ServiceError::McpError(_)))
                );
            }
        }
        if constructing {
            observation.construction_release.notify_one();
        }
        if listing {
            observation.gate_listing.store(false, Ordering::SeqCst);
            observation.listing_release.notify_one();
        }
        // Capture contract evidence before cleanup can make a broken server look correct.
        let settled = if matches!(self, Self::Shutdown | Self::Complete)
            || matches!(self, Self::CancelShutdown) && !shutdown_was_pending
        {
            *observation.state.borrow() == InvocationState::Settled
        } else {
            observation.wait_for(InvocationState::Settled).await
        };
        let mut continued_request = true;
        if matches!(
            self,
            Self::CancelRequest
                | Self::CancelConstruction
                | Self::CancelElicitation
                | Self::ElicitationDeadline
        ) {
            continued_request = matches!(
                timeout(
                    Duration::from_secs(5),
                    client.peer().call_tool(CallToolRequestParams::new("probe"))
                )
                .await,
                Ok(Ok(result)) if result.is_error != Some(true)
                    && result.content == vec![rmcp::model::Content::text("probe")]
            );
        }
        let existing_session_closed = if matches!(
            self,
            Self::Shutdown
                | Self::Drop
                | Self::CancelShutdown
                | Self::CancelShutdownConstruction
                | Self::ShutdownCleanupFailure
                | Self::ShutdownCleanupTimeout
                | Self::CancelShutdownCleanup
                | Self::ShutdownListing
                | Self::ShutdownDeclaration
        ) {
            matches!(
                timeout(
                    Duration::from_secs(5),
                    client.peer().call_tool(CallToolRequestParams::new("probe"))
                )
                .await,
                Ok(Err(_))
            )
        } else {
            true
        };
        let elicitation_cancelled = if self.elicits() {
            let mut completion = observation.elicitation_settled.subscribe();
            matches!(
                timeout(Duration::from_secs(5), completion.wait_for(|done| *done)).await,
                Ok(Ok(_))
            )
        } else {
            true
        };
        observation.release.notify_one();
        observation.elicitation_release.notify_one();
        observation.cancellation_release.notify_one();
        let elicitation_drained = if self.elicits() {
            let mut completion = observation.elicitation_settled.subscribe();
            matches!(
                timeout(Duration::from_secs(5), completion.wait_for(|done| *done)).await,
                Ok(Ok(_))
            )
        } else {
            true
        };
        observation.gate_construction.store(false, Ordering::SeqCst);
        let drained = observation.wait_for(InvocationState::Settled).await;
        let close = timeout(Duration::from_secs(5), client.close()).await;
        let cleanup = if let Some(server) = server {
            Some(timeout(Duration::from_secs(10), server.shutdown()).await)
        } else {
            None
        };
        let handler_constructions = observation.handler_constructions.load(Ordering::SeqCst);
        let usable = match tools.tool_handler(ToolName::new("probe").unwrap()).await {
            Ok(mut probe) => {
                let output = probe
                    .step(ToolHandlerInput::Invoke(ToolInvocation {
                        call_id: None,
                        arguments: json!({}),
                    }))
                    .await;
                drop(probe);
                output
            }
            Err(error) => Err(error),
        };
        let closed_by_server = observation.closed.load(Ordering::SeqCst);
        let source_cleanup = tools.shutdown().await;
        assert!(entered, "handler never entered; no ownership evidence");
        assert!(
            settled,
            "{self:?} left entered work alive; shutdown_was_pending={shutdown_was_pending}; shutdown={shutdown_diagnostic:?}; existing_session_closed={existing_session_closed}; cleanup drained={drained}, client={close:?}, server={cleanup:?}"
        );
        assert!(
            drained,
            "fixture handler must settle even after a red observation"
        );
        assert!(
            elicitation_drained,
            "fixture elicitation callback did not settle"
        );
        assert!(
            elicitation_cancelled,
            "parent cancellation must cancel its outstanding client elicitation before fixture release"
        );
        assert!(matches!(close, Ok(Ok(_))), "client cleanup failed");
        assert!(
            cleanup.is_none_or(|result| matches!(result, Ok(Ok(())))),
            "server cleanup failed"
        );
        assert!(
            source_cleanup.is_ok(),
            "caller toolset cleanup failed: {source_cleanup:?}"
        );
        assert!(
            shutdown_ok && cancel_ok,
            "scenario setup/termination failed: {self:?}; shutdown={shutdown_diagnostic:?}"
        );
        assert!(
            cleanup_retained,
            "shutdown lost owned lookup or handler cleanup: {self:?}"
        );
        assert!(
            !matches!(self, Self::CancelShutdownCleanup) || shutdown_was_pending,
            "shutdown settled before the gated Cancel operation was released"
        );
        assert!(
            !listing || handler_constructions == 0,
            "a sealed declaration lookup must not construct a handler: {handler_constructions}"
        );
        assert!(
            !observation.listing_abandoned.load(Ordering::SeqCst),
            "server termination abandoned its accepted caller lookup"
        );
        assert!(
            !constructing || shutdown_was_pending,
            "shutdown reported completion while handler construction was still gated"
        );
        assert!(
            !closed_by_server
                && matches!(usable, Ok(ToolHandlerOutput::Completed(result))
                    if result == ToolResult::text("probe")),
            "server must leave caller tools usable"
        );
        assert!(
            continued_request,
            "cancelling one request must leave another invocation usable"
        );
        assert!(
            existing_session_closed,
            "server termination must close the established session, not only refuse new connections"
        );
        assert_eq!(
            observation.effects.load(Ordering::SeqCst),
            usize::from(matches!(self, Self::Complete)),
            "only the explicitly completed control may mutate after release"
        );
        assert_eq!(
            observation.invocations.load(Ordering::SeqCst),
            usize::from(!constructing && !listing),
            "cancellation during construction must precede even synchronous Invoke work"
        );
        assert_eq!(
            observation.cancelled.load(Ordering::SeqCst),
            !matches!(self, Self::Complete) && !listing,
            "termination must reach the unique handler's Cancel operation"
        );
        assert_eq!(
            observation.cancellations.load(Ordering::SeqCst),
            usize::from(!matches!(self, Self::Complete) && !listing),
            "each accepted handler receives exactly one Cancel"
        );
        assert_eq!(
            observation.cancellation_settlements.load(Ordering::SeqCst),
            usize::from(!matches!(self, Self::Complete) && !listing),
            "each entered Cancel must complete its future, not just drop its handler"
        );
        assert!(
            !observation.construction_abandoned.load(Ordering::SeqCst),
            "cancellation must retain resource-acquiring construction through settlement"
        );
    }
}

#[tokio::test]
async fn server_ownership_completed_invocation_control() {
    ServerOwnership::Complete.run().await;
}

#[tokio::test]
async fn server_ownership_shutdown_settles_active_work() {
    ServerOwnership::Shutdown.run().await;
}
#[tokio::test]
async fn server_ownership_drop_settles_active_work() {
    ServerOwnership::Drop.run().await;
}
#[tokio::test]
async fn server_ownership_cancelled_shutdown_settles_active_work() {
    ServerOwnership::CancelShutdown.run().await;
}
#[tokio::test]
async fn request_cancellation_stops_entered_handler() {
    ServerOwnership::CancelRequest.run().await;
}

#[tokio::test]
async fn request_cancellation_during_handler_construction_cancels_without_invoking() {
    ServerOwnership::CancelConstruction.run().await;
}

#[tokio::test]
async fn cancelled_shutdown_during_handler_construction_retains_cleanup() {
    ServerOwnership::CancelShutdownConstruction.run().await;
}

#[tokio::test]
async fn request_cancellation_during_elicitation_cancels_unique_handler() {
    ServerOwnership::CancelElicitation.run().await;
}

#[tokio::test]
async fn elicitation_deadline_cancels_nested_request_and_unique_handler() {
    ServerOwnership::ElicitationDeadline.run().await;
}

#[tokio::test]
async fn server_shutdown_preserves_handler_cleanup_failure() {
    ServerOwnership::ShutdownCleanupFailure.run().await;
}

#[tokio::test]
async fn server_shutdown_deadline_retains_pending_handler_cleanup() {
    ServerOwnership::ShutdownCleanupTimeout.run().await;
}

#[tokio::test]
async fn cancelled_shutdown_retains_already_entered_handler_cleanup() {
    ServerOwnership::CancelShutdownCleanup.run().await;
}

#[tokio::test]
async fn server_shutdown_retains_entered_tool_inventory_lookup() {
    ServerOwnership::ShutdownListing.run().await;
}

#[tokio::test]
async fn server_shutdown_retains_entered_tool_declaration_lookup() {
    ServerOwnership::ShutdownDeclaration.run().await;
}

// A small raw HTTP observer avoids confusing the native client's reconnect or
// local cancellation with closure of a server-owned, established SSE body.
struct SessionConnection {
    stream: BufReader<TcpStream>,
    status: u16,
    headers: BTreeMap<String, String>,
}

impl SessionConnection {
    async fn request(address: SocketAddr, request: String) -> Result<Self, String> {
        let mut socket = TcpStream::connect(address)
            .await
            .map_err(|error| error.to_string())?;
        socket
            .write_all(request.as_bytes())
            .await
            .map_err(|error| error.to_string())?;
        let mut stream = BufReader::new(socket);
        let mut line = String::new();
        stream
            .read_line(&mut line)
            .await
            .map_err(|error| error.to_string())?;
        let status = line
            .split_whitespace()
            .nth(1)
            .and_then(|status| status.parse::<u16>().ok())
            .ok_or_else(|| format!("invalid HTTP response status: {line:?}"))?;
        let mut headers = BTreeMap::new();
        let mut bytes = line.len();
        loop {
            line.clear();
            let read = stream
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
                .ok_or_else(|| format!("invalid HTTP response header: {line:?}"))?;
            headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
        }
        Ok(Self {
            stream,
            status,
            headers,
        })
    }

    async fn initialization(self) -> Result<String, String> {
        let session = self
            .headers
            .get("mcp-session-id")
            .ok_or_else(|| "initialization supplied no session identity".to_owned())?
            .clone();
        let chunked = self
            .headers
            .get("transfer-encoding")
            .is_some_and(|value| value.eq_ignore_ascii_case("chunked"));
        let json_body = self.headers.get("content-type").is_some_and(|value| {
            value
                .split(';')
                .next()
                .is_some_and(|media| media.trim().eq_ignore_ascii_case("application/json"))
        });
        let mut wire = Vec::new();
        self.stream
            .take(64 * 1024)
            .read_to_end(&mut wire)
            .await
            .map_err(|error| error.to_string())?;
        if wire.len() == 64 * 1024 {
            return Err("oversized initialization response".to_owned());
        }
        let mut body = Vec::new();
        if chunked {
            let mut remaining = wire.as_slice();
            loop {
                let end = remaining
                    .windows(2)
                    .position(|pair| pair == b"\r\n")
                    .ok_or_else(|| "incomplete HTTP chunk size".to_owned())?;
                let size = std::str::from_utf8(&remaining[..end])
                    .map_err(|error| error.to_string())?
                    .split(';')
                    .next()
                    .ok_or_else(|| "missing HTTP chunk size".to_owned())?;
                let size = usize::from_str_radix(size, 16).map_err(|error| error.to_string())?;
                remaining = &remaining[end + 2..];
                if size == 0 {
                    break;
                }
                if size > remaining.len().saturating_sub(2) || &remaining[size..size + 2] != b"\r\n"
                {
                    return Err("incomplete HTTP response chunk".to_owned());
                }
                body.extend_from_slice(&remaining[..size]);
                remaining = &remaining[size + 2..];
            }
        } else {
            body = wire;
        }
        let text = std::str::from_utf8(&body).map_err(|error| error.to_string())?;
        let payloads = if json_body {
            vec![text]
        } else {
            text.lines()
                .filter_map(|line| line.strip_prefix("data:").map(str::trim_start))
                .collect()
        };
        let valid = payloads.into_iter().any(|data| {
            serde_json::from_str::<serde_json::Value>(data).is_ok_and(|value| {
                value["jsonrpc"] == "2.0"
                    && value["id"] == 1
                    && value.get("error").is_none()
                    && value["result"]["protocolVersion"] == "2025-11-25"
                    && value["result"]["capabilities"].is_object()
            })
        });
        if !valid {
            return Err(format!(
                "no successful negotiated InitializeResult: {text:?}"
            ));
        }
        Ok(session)
    }

    async fn initialized_sse(address: SocketAddr) -> Result<(String, Self), String> {
        let initialize = json!({
            "jsonrpc":"2.0", "id":1, "method":"initialize",
            "params": {
                "protocolVersion":"2025-11-25", "capabilities":{},
                "clientInfo":{"name":"owned-sse-observer", "version":"1"}
            }
        })
        .to_string();
        let response = Self::request(
            address,
            format!(
                "POST /mcp HTTP/1.1\r\nHost: {address}\r\nAccept: application/json, text/event-stream\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{initialize}",
                initialize.len()
            ),
        )
        .await?;
        if response.status != 200 {
            return Err(format!("initialization status: {}", response.status));
        }
        let session = response.initialization().await?;
        let initialized = r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;
        let response = Self::request(
            address,
            format!(
                "POST /mcp HTTP/1.1\r\nHost: {address}\r\nAccept: application/json, text/event-stream\r\nContent-Type: application/json\r\nMcp-Session-Id: {session}\r\nMcp-Protocol-Version: 2025-11-25\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{initialized}",
                initialized.len()
            ),
        )
        .await?;
        if response.status != 202 {
            return Err(format!(
                "initialized notification status: {}",
                response.status
            ));
        }
        drop(response);
        let response = Self::request(
            address,
            format!(
                "GET /mcp HTTP/1.1\r\nHost: {address}\r\nAccept: text/event-stream\r\nMcp-Session-Id: {session}\r\nMcp-Protocol-Version: 2025-11-25\r\nConnection: close\r\n\r\n"
            ),
        )
        .await?;
        if response.status != 200
            || !response.headers.get("content-type").is_some_and(|value| {
                value
                    .split(';')
                    .next()
                    .is_some_and(|media| media.trim().eq_ignore_ascii_case("text/event-stream"))
            })
        {
            return Err(format!(
                "standalone SSE not established: status={}, headers={:?}",
                response.status, response.headers
            ));
        }
        Ok((session, response))
    }

    async fn observe_eof(&mut self) -> Result<(), String> {
        let mut bytes = [0_u8; 1024];
        loop {
            match self.stream.read(&mut bytes).await {
                Ok(0) => return Ok(()),
                Ok(_) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
                    ) =>
                {
                    return Ok(());
                }
                Err(error) => return Err(error.to_string()),
            }
        }
    }
}

#[tokio::test]
async fn server_shutdown_closes_all_established_idle_session_streams() {
    let observation = GatedTools::new();
    let tools = Arc::new(ServedGate {
        observation: Arc::clone(&observation),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let config = McpServerConfig::new(address, "/mcp", "idle:")
        .unwrap()
        .with_shutdown_timeout(Duration::from_secs(1))
        .unwrap();
    let server = McpServer::start(Arc::clone(&tools), config).await.unwrap();
    let mut connections = Vec::new();
    let setup = timeout(Duration::from_secs(10), async {
        for _ in 0..2 {
            connections.push(SessionConnection::initialized_sse(address).await?);
        }
        if connections[0].0 == connections[1].0 {
            return Err("independent initializations reused the same session identity".to_owned());
        }
        for (_, connection) in &mut connections {
            let closure = connection.observe_eof();
            tokio::pin!(closure);
            let observed =
                std::future::poll_fn(|context| Poll::Ready(closure.as_mut().poll(context))).await;
            if let Poll::Ready(result) = observed {
                return Err(format!(
                    "SSE body already closed before shutdown: {result:?}"
                ));
            }
        }
        Ok::<_, String>(())
    })
    .await;
    let shutdown = timeout(Duration::from_secs(5), server.shutdown()).await;
    let mut closure = Vec::new();
    for (_, connection) in &mut connections {
        closure.push(timeout(Duration::from_secs(2), connection.observe_eof()).await);
    }
    drop(connections);
    let usable = tools.list_tools().await;
    let cleanup = tools.shutdown().await;
    assert!(
        matches!(setup, Ok(Ok(()))),
        "SSE setup: {setup:?}; shutdown={shutdown:?}"
    );
    assert!(
        matches!(shutdown, Ok(Ok(()))),
        "shutdown: {shutdown:?}; closure={closure:?}"
    );
    assert!(
        closure.len() == 2 && closure.iter().all(|result| matches!(result, Ok(Ok(())))),
        "successful shutdown left established SSE bodies open: {closure:?}"
    );
    assert!(
        usable.is_ok(),
        "server must not shut down caller tools: {usable:?}"
    );
    assert!(cleanup.is_ok(), "caller toolset cleanup: {cleanup:?}");
    assert_eq!(observation.invocations.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn server_reports_owned_handler_panic_after_settlement() {
    let observation = GatedTools::new();
    observation.panic_on_invoke.store(true, Ordering::SeqCst);
    let tools = Arc::new(ServedGate {
        observation: Arc::clone(&observation),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bind = listener.local_addr().unwrap();
    drop(listener);
    let config = McpServerConfig::new(bind, "/mcp", "panic:").unwrap();
    let server = McpServer::start(Arc::clone(&tools), config).await.unwrap();
    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(format!("http://{bind}/mcp")),
    );
    let mut client = match timeout(Duration::from_secs(5), ().serve(transport)).await {
        Ok(Ok(client)) => client,
        failure => {
            let cleanup = timeout(Duration::from_secs(10), server.shutdown()).await;
            tools.shutdown().await.unwrap();
            panic!("native client setup failed: {failure:?}; cleanup: {cleanup:?}");
        }
    };
    let request = timeout(
        Duration::from_secs(5),
        client.peer().send_cancellable_request(
            ClientRequest::CallToolRequest(rmcp::model::Request::new(CallToolRequestParams::new(
                "gated",
            ))),
            PeerRequestOptions::no_options(),
        ),
    )
    .await;
    let settled = observation.wait_for(InvocationState::Settled).await;
    let close = timeout(Duration::from_secs(5), client.close()).await;
    let shutdown = timeout(Duration::from_secs(10), server.shutdown()).await;
    let usable = tools.list_tools().await;
    tools.shutdown().await.unwrap();
    assert!(matches!(request, Ok(Ok(_))), "request admission failed");
    assert!(
        settled,
        "panicking handler never settled; close={close:?}; shutdown={shutdown:?}"
    );
    assert!(
        matches!(close, Ok(Ok(_))),
        "client cleanup: {close:?}; shutdown={shutdown:?}"
    );
    assert!(
        matches!(&shutdown, Ok(Err(ToolError::Transport { message, .. })) if message.contains("panic")),
        "owned worker panic must be reported after settlement: {shutdown:?}"
    );
    assert!(usable.is_ok(), "server must not close caller-owned tools");
}

struct SchemaOutput {
    declared: bool,
    result: ToolResult,
}
impl SchemaOutput {
    fn info(&self) -> ToolInfo {
        let mut info = GatedTools::info("schema_output");
        if self.declared {
            info.output_schema = Some(JsonSchema::strict(
                json!({"type":"object", "required":["answer"], "properties":{"answer":{"type":"integer"}}}),
            ));
        }
        info
    }
    async fn run(self, local: bool) -> Result<ToolResult, ToolError> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);
        let config = McpServerConfig::new(bind, "/mcp", "schema:")
            .unwrap()
            .with_shutdown_timeout(Duration::from_secs(1))
            .unwrap();
        let mut supplied = None;
        let server = if local {
            let info = self.info();
            let result = self.result;
            let tools = Arc::new(
                Toolset::local(vec![ToolRegistration::function(info, move |_| {
                    let result = result.clone();
                    async move { Ok(result) }
                })])
                .unwrap(),
            );
            let server = McpServer::start(Arc::clone(&tools), config).await.unwrap();
            supplied = Some(tools);
            server
        } else {
            McpServer::start(Arc::new(self), config).await.unwrap()
        };
        let connection = timeout(
            Duration::from_secs(5),
            Toolset::mcp(McpClientConfig::new(McpConnectionConfig::StreamableHttp {
                endpoint: format!("http://{bind}/mcp").parse().unwrap(),
                headers: BTreeMap::new(),
            })),
        )
        .await;
        let client = match connection {
            Ok(Ok(client)) => client,
            failure => {
                let cleanup = timeout(Duration::from_secs(10), server.shutdown()).await;
                if let Some(tools) = supplied {
                    tools.shutdown().await.unwrap();
                }
                panic!("MCP schema client setup failed: {failure:?}; cleanup: {cleanup:?}");
            }
        };

        let result = timeout(Duration::from_secs(5), async {
            let mut handler = client
                .tool_handler(ToolName::new("schema_output").unwrap())
                .await?;
            match handler
                .step(ToolHandlerInput::Invoke(ToolInvocation {
                    call_id: None,
                    arguments: json!({}),
                }))
                .await?
            {
                ToolHandlerOutput::Completed(result) => Ok(result),
                ToolHandlerOutput::Interaction(_) => Err(ToolError::Protocol {
                    message: "unexpected interaction".into(),
                }),
            }
        })
        .await;
        let client_cleanup = timeout(Duration::from_secs(5), client.shutdown()).await;
        let server_cleanup = timeout(Duration::from_secs(10), server.shutdown()).await;
        let local_cleanup = if let Some(tools) = supplied {
            Some(timeout(Duration::from_secs(5), tools.shutdown()).await)
        } else {
            None
        };
        let result = result.expect("schema call deadline; all cleanup has been attempted");
        assert!(
            matches!(client_cleanup, Ok(Ok(())))
                && matches!(server_cleanup, Ok(Ok(())))
                && local_cleanup.is_none_or(|cleanup| matches!(cleanup, Ok(Ok(())))),
            "schema cleanup failed; primary result={result:?}"
        );
        result
    }
}
struct SchemaHandler {
    result: ToolResult,
}
impl ToolsetBackend for SchemaOutput {
    type Handler = SchemaHandler;
    async fn list_tools(&self) -> Result<Vec<ToolInfo>, ToolError> {
        Ok(vec![self.info()])
    }
    async fn tool_handler(&self, _name: ToolName) -> Result<Self::Handler, ToolError> {
        Ok(SchemaHandler {
            result: self.result.clone(),
        })
    }
    async fn shutdown(&self) -> Result<(), ToolError> {
        Ok(())
    }
}
impl ToolHandlerBackend for SchemaHandler {
    async fn step(&mut self, _input: ToolHandlerInput) -> Result<ToolHandlerOutput, ToolError> {
        Ok(ToolHandlerOutput::Completed(self.result.clone()))
    }
}

#[tokio::test]
async fn output_schema_served_caller_rejects_missing_structured_success() {
    let result = SchemaOutput {
        declared: true,
        result: ToolResult::text("done"),
    }
    .run(false)
    .await;
    assert!(
        matches!(
            result,
            Err(ToolError::InvalidOutput { .. }) | Err(ToolError::Protocol { .. })
        ),
        "served caller must not deliver malformed success: {result:?}"
    );
}
#[tokio::test]
async fn output_schema_served_local_rejects_missing_structured_success() {
    let result = SchemaOutput {
        declared: true,
        result: ToolResult::text("done"),
    }
    .run(true)
    .await;
    assert!(
        matches!(result, Err(ToolError::Protocol { .. })),
        "served Local must return the mapped MCP validation error, not malformed success: {result:?}"
    );
}
#[tokio::test]
async fn output_schema_mcp_controls() {
    for local in [false, true] {
        for result in [
            ToolResult::json(json!({"answer":42})),
            ToolResult::text("accepted error").with_error(true),
        ] {
            let expected = result.structured_content.clone();
            let is_error = result.is_error;
            let actual = SchemaOutput {
                declared: true,
                result,
            }
            .run(local)
            .await
            .unwrap();
            assert_eq!(actual.structured_content, expected);
            assert_eq!(actual.is_error, is_error);
        }
        assert!(
            SchemaOutput {
                declared: false,
                result: ToolResult::text("plain")
            }
            .run(local)
            .await
            .is_ok()
        );
        assert!(
            SchemaOutput {
                declared: true,
                result: ToolResult::json(json!({"answer":"wrong"}))
            }
            .run(local)
            .await
            .is_err()
        );
    }
}

#[derive(Clone, Copy, Debug)]
enum DeclarationBoundary {
    MissingOutput,
    InvalidOutput,
    ValidOutput,
    ModelVisibleError,
    InvalidInput,
    UnknownTool,
    DuplicateTool,
}

struct DeclaredTools {
    case: DeclarationBoundary,
    constructed: AtomicUsize,
}

impl ToolsetBackend for DeclaredTools {
    type Handler = SchemaHandler;

    async fn list_tools(&self) -> Result<Vec<ToolInfo>, ToolError> {
        let mut info = GatedTools::info("declared");
        info.input_schema = JsonSchema::strict(json!({
            "type":"object", "required":["value"],
            "properties":{"value":{"type":"integer"}}
        }));
        info.output_schema = Some(JsonSchema::strict(json!({
            "type":"object", "required":["answer"],
            "properties":{"answer":{"type":"integer"}}
        })));
        Ok(if matches!(self.case, DeclarationBoundary::DuplicateTool) {
            vec![info.clone(), info]
        } else {
            vec![info]
        })
    }

    async fn tool_handler(&self, _name: ToolName) -> Result<Self::Handler, ToolError> {
        self.constructed.fetch_add(1, Ordering::SeqCst);
        let result = match self.case {
            DeclarationBoundary::MissingOutput => ToolResult::text("done"),
            DeclarationBoundary::InvalidOutput => ToolResult::json(json!({"answer":"wrong"})),
            DeclarationBoundary::ModelVisibleError => ToolResult::text("failed").with_error(true),
            _ => ToolResult::json(json!({"answer":42})),
        };
        Ok(SchemaHandler { result })
    }

    async fn shutdown(&self) -> Result<(), ToolError> {
        Ok(())
    }
}

impl DeclarationBoundary {
    async fn run(self) {
        let tools = Arc::new(DeclaredTools {
            case: self,
            constructed: AtomicUsize::new(0),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bind = listener.local_addr().unwrap();
        drop(listener);
        let config = McpServerConfig::new(bind, "/mcp", "declaration:").unwrap();
        let server = McpServer::start(Arc::clone(&tools), config).await.unwrap();
        // The native client deliberately has no Kit output validator. These
        // assertions prove that the supplied server enforces its declaration.
        let transport = StreamableHttpClientTransport::from_config(
            StreamableHttpClientTransportConfig::with_uri(format!("http://{bind}/mcp")),
        );
        let connected = timeout(Duration::from_secs(5), ().serve(transport)).await;
        let mut client = match connected {
            Ok(Ok(client)) => client,
            failure => {
                let cleanup = timeout(Duration::from_secs(10), server.shutdown()).await;
                tools.shutdown().await.unwrap();
                panic!("native client setup failed: {failure:?}; cleanup: {cleanup:?}");
            }
        };
        let name = if matches!(self, Self::UnknownTool) {
            "absent"
        } else {
            "declared"
        };
        let arguments = if matches!(self, Self::InvalidInput) {
            json!({"value":"wrong"})
        } else {
            json!({"value":1})
        };
        let result = timeout(
            Duration::from_secs(5),
            client.peer().call_tool(
                CallToolRequestParams::new(name)
                    .with_arguments(arguments.as_object().unwrap().clone()),
            ),
        )
        .await;
        let close = timeout(Duration::from_secs(5), client.close()).await;
        let shutdown = timeout(Duration::from_secs(10), server.shutdown()).await;
        tools.shutdown().await.unwrap();
        assert!(
            matches!(close, Ok(Ok(_))),
            "native client cleanup: {close:?}; primary: {result:?}"
        );
        assert!(
            matches!(shutdown, Ok(Ok(()))),
            "server cleanup: {shutdown:?}; primary: {result:?}"
        );
        let result = result.expect("call watchdog expired after cleanup");
        match self {
            Self::ValidOutput => {
                let output = result.expect("valid structured output");
                assert_eq!(output.structured_content, Some(json!({"answer":42})));
                assert_ne!(output.is_error, Some(true));
            }
            Self::ModelVisibleError => {
                let output = result.expect("model-visible error exemption");
                assert_eq!(output.is_error, Some(true));
                assert_eq!(output.content, vec![rmcp::model::Content::text("failed")]);
            }
            _ => {
                let expected = if matches!(self, Self::InvalidInput | Self::UnknownTool) {
                    rmcp::model::ErrorCode::INVALID_PARAMS
                } else {
                    rmcp::model::ErrorCode::INTERNAL_ERROR
                };
                assert!(
                    matches!(&result, Err(rmcp::service::ServiceError::McpError(error)) if error.code == expected),
                    "{self:?} must reject through the expected MCP protocol error: {result:?}"
                );
            }
        }
        let before_construction = matches!(
            self,
            Self::InvalidInput | Self::UnknownTool | Self::DuplicateTool
        );
        assert_eq!(
            tools.constructed.load(Ordering::SeqCst),
            usize::from(!before_construction),
            "invalid declaration/input must reject before handler construction: {self:?}"
        );
    }
}

#[tokio::test]
async fn server_declaration_rejects_missing_output_without_client_validation() {
    DeclarationBoundary::MissingOutput.run().await;
}

#[tokio::test]
async fn server_declaration_rejects_invalid_output_without_client_validation() {
    DeclarationBoundary::InvalidOutput.run().await;
}

#[tokio::test]
async fn server_declaration_rejects_invalid_input_before_handler_construction() {
    DeclarationBoundary::InvalidInput.run().await;
}

#[tokio::test]
async fn server_declaration_rejects_unknown_tool_before_handler_construction() {
    DeclarationBoundary::UnknownTool.run().await;
}

#[tokio::test]
async fn server_declaration_rejects_ambiguous_tool_before_handler_construction() {
    DeclarationBoundary::DuplicateTool.run().await;
}

#[tokio::test]
async fn server_declaration_valid_output_and_model_visible_error_controls() {
    DeclarationBoundary::ValidOutput.run().await;
    DeclarationBoundary::ModelVisibleError.run().await;
}
