mod serving;
mod sessions;

use std::{sync::Arc, time::Duration};

use axum::{
    Router,
    extract::Request,
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use rmcp::{
    ErrorData as McpError, RoleServer, ServerHandler,
    model::{
        CallToolRequestParams, CallToolResult, ClientResult, CreateElicitationRequestParams,
        ElicitationAction, ElicitationSchema, ListToolsResult, PaginatedRequestParams,
        ServerCapabilities, ServerInfo, ServerRequest,
    },
    service::{ElicitationMode, PeerRequestOptions, RequestContext},
    transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService},
};
use tokio::{net::TcpListener, time};
use url::{Host, Url};

use crate::{
    ToolError, ToolHandlerBackend, ToolsetBackend,
    declaration::ToolInfo,
    execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation},
    interaction::{ToolInteractionKind, ToolInteractionRequest, ToolInteractionResponse},
    mcp::{McpServerConfig, McpServerInteractionPolicy},
    name::ToolName,
    sources::mcp::catalog::McpProtocol,
};

use self::{
    serving::Serving,
    sessions::{Protocol, Sessions},
};

const LIST_TOOLS_PAGE_SIZE: usize = 64;
const INTERACTION_TIMEOUT: Duration = Duration::from_secs(30);
const INTERACTION_CLEANUP_TIMEOUT: Duration = Duration::from_secs(1);

/// Independently owned MCP server serving any concrete [`ToolsetBackend`].
///
/// The server retains a shared reference to the caller's toolset. Server shutdown
/// never shuts down that toolset; the caller owns its cleanup separately.
/// This example binds a real loopback endpoint and requires an available port.
/// Keep the Tokio runtime driven until both server and toolset cleanup settle.
/// Dropping the server or its shutdown observation signals its retained cleanup owner;
/// only explicit toolset shutdown closes the separate executable source.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use tool_sdk::{Toolset, ToolsetBackend, ToolHandlerBackend, ToolError};
/// use tool_sdk::declaration::ToolName;
/// use tool_sdk::execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation};
/// use tool_sdk::local::ToolRegistration;
/// use tool_sdk::mcp::{McpServer, McpServerConfig, McpClientConfig, McpConnectionConfig};
/// // MCP requires object-shaped arguments; local typed tools may use scalars.
/// #[derive(serde::Deserialize, schemars::JsonSchema)]
/// #[serde(deny_unknown_fields)]
/// struct DoubleArgs { value: i64 }
/// #[derive(serde::Serialize, schemars::JsonSchema)]
/// struct DoubleResult { value: i64 }
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), ToolError> {
/// let source = Arc::new(Toolset::local(vec![ToolRegistration::typed(
///     "double", |input: DoubleArgs| async move {
///         let value = input.value.checked_mul(2).ok_or(ToolError::InvalidArguments {
///             tool: ToolName::new("double")?, message: "doubling would overflow i64".into(), issues: Vec::new(),
///         })?;
///         Ok(DoubleResult { value })
///     },
/// )?])?);
/// let config = McpServerConfig::new("127.0.0.1:8000".parse().unwrap(), "/mcp", "page")?;
/// let server = McpServer::start(Arc::clone(&source), config).await;
/// let observed = async { match server {
///     Ok(server) => {
///         let client_work = async {
///             let client = Toolset::mcp(McpClientConfig::new(McpConnectionConfig::StreamableHttp {
///                 endpoint: "http://127.0.0.1:8000/mcp".parse().unwrap(), headers: Default::default(),
///             })).await?;
///             let invoked = async {
///                 let mut handler = client.tool_handler(ToolName::new("double")?).await?;
///                 let output = handler.step(ToolHandlerInput::Invoke(ToolInvocation {
///                     call_id: Some("client-call".into()), arguments: serde_json::json!({"value":21}),
///                 })).await?;
///                 let ToolHandlerOutput::Completed(result) = output else {
///                     return Err(ToolError::Protocol { message: "unexpected interaction".into() });
///                 };
///                 assert!(!result.is_error);
///                 assert_eq!(result.structured_content, Some(serde_json::json!({"value":42})));
///                 Ok::<(), ToolError>(())
///             }.await;
///             let client_closed = client.shutdown().await;
///             invoked?;
///             client_closed
///         }.await;
///         let serving_closed = server.shutdown().await; // Consumes and joins serving.
///         client_work?;
///         serving_closed?;
///         assert_eq!(source.list_tools().await?.len(), 1); // Source remains usable.
///         Ok(())
///     }
///     Err(error) => Err(error),
/// }
/// }.await;
/// let source_closed = source.shutdown().await;
/// observed?;
/// source_closed?;
/// # Ok(())
/// # }
/// ```
///
pub struct McpServer {
    serving: Arc<Serving>,
}

impl McpServer {
    /// Binds and starts the configured loopback endpoint eagerly.
    ///
    /// A failed start leaves the caller's toolset usable. Neither the toolset nor
    /// its associated handler needs to implement `Clone` or use supplied handles.
    ///
    /// This example binds a real loopback endpoint and requires an available port.
    /// Keep the Tokio runtime driven through both independently owned cleanup operations.
    ///
    /// ```no_run
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), tool_sdk::ToolError> {
    /// use std::sync::Arc;
    /// use tool_sdk::{Toolset, ToolsetBackend, mcp::{McpServer, McpServerConfig}};
    /// let tools = Arc::new(Toolset::local(Vec::new())?);
    /// let config = McpServerConfig::new("127.0.0.1:8000".parse().unwrap(), "/mcp", "page")?;
    /// let result = McpServer::start(Arc::clone(&tools), config).await;
    /// let stopped = match result {
    ///     Ok(server) => server.shutdown().await,
    ///     Err(error) => Err(error),
    /// };
    /// let source_closed = tools.shutdown().await;
    /// stopped?;
    /// source_closed?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`ToolError::InvalidConfiguration`] for invalid server settings or [`ToolError::Transport`] when the endpoint cannot bind. Failed startup leaves source shutdown with the caller.
    ///
    pub async fn start<T: ToolsetBackend>(
        toolset: Arc<T>,
        config: McpServerConfig,
    ) -> Result<Self, ToolError> {
        config.validate()?;
        let listener =
            TcpListener::bind(config.bind())
                .await
                .map_err(|error| ToolError::Transport {
                    tool: None,
                    message: format!("failed to bind MCP server endpoint: {error}"),
                })?;
        let serving = Serving::new(config.shutdown_timeout());
        let protocol = Arc::new(Protocol::new(Arc::clone(&serving)));
        let handler = McpServeHandler {
            serving: Arc::clone(&serving),
            toolset,
            interaction_policy: config.interaction_policy(),
            cursor_prefix: config.cursor_prefix().to_owned(),
            _protocol: Arc::clone(&protocol),
        };
        let transport_config = StreamableHttpServerConfig::default();
        let stream_cancellation = transport_config.cancellation_token.clone();
        let service: StreamableHttpService<McpServeHandler<T>, Sessions> =
            StreamableHttpService::new(
                move || {
                    if handler.serving.closing() {
                        Err(std::io::Error::other("MCP server is closing"))
                    } else {
                        Ok(handler.clone())
                    }
                },
                Arc::new(Sessions::new(Arc::clone(&serving), protocol)),
                transport_config,
            );
        let router = Router::new()
            .nest_service(config.endpoint_path(), service)
            .layer(middleware::from_fn(McpOriginPolicy::enforce));
        let stopping = Arc::clone(&serving);
        serving.start(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    stopping.stopped().await;
                    stream_cancellation.cancel();
                })
                .await
        });
        Ok(Self { serving })
    }

    /// Seals admission and observes the retained server owner through cleanup.
    ///
    /// A timeout reports unsettled cleanup; it does not abort the cleanup owner.
    /// This example binds a real loopback endpoint and requires an available port.
    /// Keep the Tokio runtime driven until both server and toolset cleanup settle.
    /// Dropping the server or its shutdown observation signals its retained cleanup owner;
    /// only explicit toolset shutdown closes the separate executable source.
    ///
    /// # Errors
    ///
    /// Returns the retained server cleanup failure, including timeout or handler-cleanup failure. A timeout leaves cleanup owned rather than aborting it.
    ///
    /// # Examples
    ///
    /// ```no_run
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), tool_sdk::ToolError> {
    /// use std::sync::Arc;
    /// use tool_sdk::{Toolset, ToolsetBackend, mcp::{McpServer, McpServerConfig}};
    /// let tools = Arc::new(Toolset::local(Vec::new())?);
    /// let config = McpServerConfig::new("127.0.0.1:8000".parse().unwrap(), "/mcp", "page")?;
    /// let server = McpServer::start(Arc::clone(&tools), config).await?;
    /// let stopped = server.shutdown().await;
    /// let source_closed = tools.shutdown().await;
    /// stopped?;
    /// source_closed?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    pub async fn shutdown(self) -> Result<(), ToolError> {
        self.serving.shutdown().await
    }
}

impl Drop for McpServer {
    fn drop(&mut self) {
        self.serving.release_observer();
    }
}

impl std::fmt::Debug for McpServer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("McpServer").finish_non_exhaustive()
    }
}

struct McpServeHandler<T: ToolsetBackend> {
    serving: Arc<Serving>,
    toolset: Arc<T>,
    interaction_policy: McpServerInteractionPolicy,
    cursor_prefix: String,
    // Final field: retire protocol ownership after caller resources.
    _protocol: Arc<Protocol>,
}

impl<T: ToolsetBackend> Clone for McpServeHandler<T> {
    fn clone(&self) -> Self {
        Self {
            serving: Arc::clone(&self.serving),
            toolset: Arc::clone(&self.toolset),
            interaction_policy: self.interaction_policy,
            cursor_prefix: self.cursor_prefix.clone(),
            _protocol: Arc::clone(&self._protocol),
        }
    }
}

impl<T: ToolsetBackend> ServerHandler for McpServeHandler<T> {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
    }

    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        let handler = self.clone();
        let owner = Arc::clone(&self.serving);
        let cancellation = context.ct.clone();
        let observation = self.serving.admit(async move {
            let operation = handler.list_tools_owned(request, context);
            tokio::pin!(operation);
            tokio::select! {
                biased;
                _ = owner.stopped() => {
                    cancellation.cancel();
                    operation.await
                }
                result = &mut operation => result,
            }
        })?;
        observation
            .await
            .map_err(|_| McpError::internal_error("MCP server inventory task terminated", None))?
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let handler = self.clone();
        let owner = Arc::clone(&self.serving);
        let cancellation = context.ct.clone();
        let observation = self.serving.admit(async move {
            let operation = handler.call_tool_owned(request, context);
            tokio::pin!(operation);
            tokio::select! {
                biased;
                _ = owner.stopped() => {
                    cancellation.cancel();
                    operation.await
                }
                result = &mut operation => result,
            }
        })?;
        observation
            .await
            .map_err(|_| McpError::internal_error("MCP server invocation task terminated", None))?
    }
}

impl<T: ToolsetBackend> McpServeHandler<T> {
    async fn list_tools_owned(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        if context.ct.is_cancelled() {
            return Err(McpError::internal_error(
                "MCP tools/list request was cancelled",
                None,
            ));
        }
        let tools = self
            .toolset
            .list_tools()
            .await
            .map_err(|error| McpError::internal_error(error.to_string(), None))?;
        let start = self.list_tools_offset(request, &context, tools.len())?;
        let page = tools
            .iter()
            .skip(start)
            .take(LIST_TOOLS_PAGE_SIZE)
            .map(McpProtocol::tool_info_to_mcp)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| McpError::internal_error(error.to_string(), None))?;
        let next_offset = start + page.len();
        let mut result = ListToolsResult::with_all_items(page);
        result.next_cursor =
            (next_offset < tools.len()).then(|| format!("{}{next_offset}", self.cursor_prefix));
        Ok(result)
    }

    async fn call_tool_owned(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let name = ToolName::new(request.name.to_string())
            .map_err(|error| McpError::invalid_params(error.to_string(), None))?;
        self.check_cancellation(&name, &context)
            .map_err(McpProtocol::tool_error_to_mcp_error)?;
        let info = self
            .declaration(&name)
            .await
            .map_err(McpProtocol::tool_error_to_mcp_error)?;
        self.check_cancellation(&name, &context)
            .map_err(McpProtocol::tool_error_to_mcp_error)?;
        let arguments = serde_json::Value::Object(request.arguments.unwrap_or_default());
        let issues = info
            .input_schema
            .validate(&arguments)
            .map_err(McpProtocol::tool_error_to_mcp_error)?;
        if !issues.is_empty() {
            return Err(McpProtocol::tool_error_to_mcp_error(
                ToolError::InvalidArguments {
                    tool: name,
                    message: "invocation failed the declared input schema".to_owned(),
                    issues,
                },
            ));
        }
        let mut handler = self
            .toolset
            .tool_handler(name.clone())
            .await
            .map_err(McpProtocol::tool_error_to_mcp_error)?;
        // Construction may acquire caller-owned resources. Retain it even when
        // cancelled, then cancel the returned handler before its first Invoke.
        let mut output = self
            .step(
                &mut handler,
                ToolHandlerInput::Invoke(ToolInvocation {
                    call_id: None,
                    arguments,
                }),
                &name,
                &context,
            )
            .await
            .map_err(McpProtocol::tool_error_to_mcp_error)?;
        loop {
            match output {
                ToolHandlerOutput::Completed(result) => {
                    info.validate_output(&result)
                        .map_err(McpProtocol::tool_error_to_mcp_error)?;
                    return McpProtocol::tool_result_to_mcp(result)
                        .map_err(|error| McpError::internal_error(error.to_string(), None));
                }
                ToolHandlerOutput::Interaction(request) => {
                    let response = self
                        .interaction_policy
                        .resolve(request, &context, &self.serving)
                        .await;
                    let response = match response {
                        Ok(response) => response,
                        Err(primary) => {
                            return Err(McpProtocol::tool_error_to_mcp_error(
                                self.cancel(&mut handler, &name, primary).await,
                            ));
                        }
                    };
                    output = self
                        .step(
                            &mut handler,
                            ToolHandlerInput::Interaction(response),
                            &name,
                            &context,
                        )
                        .await
                        .map_err(McpProtocol::tool_error_to_mcp_error)?;
                }
            }
        }
    }

    fn check_cancellation(
        &self,
        name: &ToolName,
        context: &RequestContext<RoleServer>,
    ) -> Result<(), ToolError> {
        if context.ct.is_cancelled() {
            Err(ToolError::Cancelled {
                tool: name.clone(),
                reason: Some("MCP invocation was cancelled".to_owned()),
            })
        } else {
            Ok(())
        }
    }

    async fn step(
        &self,
        handler: &mut T::Handler,
        input: ToolHandlerInput,
        name: &ToolName,
        context: &RequestContext<RoleServer>,
    ) -> Result<ToolHandlerOutput, ToolError> {
        if let Err(primary) = self.check_cancellation(name, context) {
            return Err(self.cancel(handler, name, primary).await);
        }
        let output = {
            let step = handler.step(input);
            tokio::pin!(step);
            tokio::select! {
                biased;
                _ = context.ct.cancelled() => None,
                output = &mut step => Some(output),
            }
        };
        // End the pending step's mutable borrow before issuing Cancel on the
        // same unique handler. Await cleanup rather than cancelling its waiter.
        match output {
            Some(output) => output,
            None => Err(self
                .cancel(
                    handler,
                    name,
                    ToolError::Cancelled {
                        tool: name.clone(),
                        reason: Some("MCP invocation was cancelled".to_owned()),
                    },
                )
                .await),
        }
    }

    async fn cancel(
        &self,
        handler: &mut T::Handler,
        name: &ToolName,
        primary: ToolError,
    ) -> ToolError {
        match handler
            .step(ToolHandlerInput::Cancel {
                reason: Some(primary.to_string()),
            })
            .await
        {
            Ok(ToolHandlerOutput::Completed(_)) | Err(ToolError::Cancelled { .. }) => primary,
            Ok(ToolHandlerOutput::Interaction(_)) => {
                let error = ToolError::Transport {
                    tool: Some(name.clone()),
                    message: format!("{primary}; handler cancellation left an interaction pending"),
                };
                self.serving.record(error.clone());
                error
            }
            Err(cleanup) => {
                let error = ToolError::Transport {
                    tool: Some(name.clone()),
                    message: format!("{primary}; handler cancellation also failed: {cleanup}"),
                };
                self.serving.record(error.clone());
                error
            }
        }
    }

    async fn declaration(&self, name: &ToolName) -> Result<ToolInfo, ToolError> {
        let mut matching = self
            .toolset
            .list_tools()
            .await?
            .into_iter()
            .filter(|info| &info.name == name);
        let info = matching
            .next()
            .ok_or_else(|| ToolError::UnknownTool { name: name.clone() })?;
        if matching.next().is_some() {
            return Err(ToolError::DuplicateName { name: name.clone() });
        }
        Ok(info)
    }

    fn list_tools_offset(
        &self,
        request: Option<PaginatedRequestParams>,
        context: &RequestContext<RoleServer>,
        tool_count: usize,
    ) -> Result<usize, McpError> {
        if context.ct.is_cancelled() {
            return Err(McpError::internal_error(
                "MCP tools/list request was cancelled".to_owned(),
                None,
            ));
        }
        let Some(cursor) = request.and_then(|params| params.cursor) else {
            return Ok(0);
        };
        let Some(raw_offset) = cursor.strip_prefix(&self.cursor_prefix) else {
            return Err(McpError::invalid_params(
                format!("invalid MCP tools/list cursor `{cursor}`"),
                None,
            ));
        };
        let offset = raw_offset.parse::<usize>().map_err(|_| {
            McpError::invalid_params(format!("invalid MCP tools/list cursor `{cursor}`"), None)
        })?;
        if offset == 0 || offset >= tool_count || offset % LIST_TOOLS_PAGE_SIZE != 0 {
            return Err(McpError::invalid_params(
                format!("invalid MCP tools/list cursor `{cursor}`"),
                None,
            ));
        }
        Ok(offset)
    }
}

impl McpServerInteractionPolicy {
    async fn resolve(
        self,
        request: ToolInteractionRequest,
        context: &RequestContext<RoleServer>,
        serving: &Serving,
    ) -> Result<ToolInteractionResponse, ToolError> {
        match (self, &request.kind) {
            (Self::RejectWithToolError, _) => Err(ToolError::Unsupported {
                operation: "MCP server tool interaction".to_owned(),
            }),
            (Self::ElicitFormIfClientSupports, ToolInteractionKind::Form { schema, .. })
                if context
                    .peer
                    .supported_elicitation_modes()
                    .contains(&ElicitationMode::Form) =>
            {
                let schema = self.elicitation_schema(schema.clone())?;
                let deadline = time::Instant::now() + INTERACTION_TIMEOUT;
                let admission = context.peer.send_cancellable_request(
                    ServerRequest::CreateElicitationRequest(rmcp::model::Request::new(
                        CreateElicitationRequestParams::FormElicitationParams {
                            meta: None,
                            message: request.prompt.clone(),
                            requested_schema: schema,
                        },
                    )),
                    PeerRequestOptions::no_options(),
                );
                // rmcp returns its handle synchronously after queue admission.
                // Cancelling this wait cannot discard an already admitted handle.
                let mut elicitation = tokio::select! {
                    biased;
                    _ = context.ct.cancelled() => return Err(ToolError::Cancelled {
                        tool: request.tool_name.clone(),
                        reason: Some("MCP invocation was cancelled before elicitation admission".to_owned()),
                    }),
                    _ = time::sleep_until(deadline) => return Err(ToolError::Timeout {
                        tool: request.tool_name.clone(),
                        timeout_secs: INTERACTION_TIMEOUT.as_secs(),
                    }),
                    result = admission => result,
                }
                .map_err(|error| ToolError::Transport {
                        tool: Some(request.tool_name.clone()),
                        message: format!("MCP client elicitation failed: {error}"),
                    })?;
                // Keep the native handle until observation or explicit cancellation
                // completes. Dropping rmcp's response future sends no cancellation.
                let observed = tokio::select! {
                    biased;
                    _ = context.ct.cancelled() => Err(ToolError::Cancelled {
                        tool: request.tool_name.clone(),
                        reason: Some("MCP invocation was cancelled during elicitation".to_owned()),
                    }),
                    _ = time::sleep_until(deadline) => Err(ToolError::Timeout {
                        tool: request.tool_name.clone(),
                        timeout_secs: INTERACTION_TIMEOUT.as_secs(),
                    }),
                    result = &mut elicitation.rx => Ok(result),
                };
                let response = match observed {
                    Ok(response) => response,
                    Err(primary) => {
                        return Err(match time::timeout(
                            INTERACTION_CLEANUP_TIMEOUT,
                            elicitation.cancel(Some(primary.to_string())),
                        )
                        .await
                        {
                            Ok(Ok(())) => primary,
                            Ok(Err(cleanup)) => {
                                let error = ToolError::Transport {
                                    tool: Some(request.tool_name.clone()),
                                    message: format!(
                                        "{primary}; elicitation cancellation also failed: {cleanup}"
                                    ),
                                };
                                serving.record(error.clone());
                                error
                            }
                            Err(_) => {
                                let error = ToolError::Transport {
                                    tool: Some(request.tool_name.clone()),
                                    message: format!(
                                        "{primary}; elicitation cancellation acknowledgement timed out; client request settlement is unconfirmed"
                                    ),
                                };
                                serving.record(error.clone());
                                error
                            }
                        });
                    }
                }
                .map_err(|error| ToolError::Transport {
                    tool: Some(request.tool_name.clone()),
                    message: format!("MCP client elicitation response was lost: {error}"),
                })?
                .map_err(|error| ToolError::Transport {
                    tool: Some(request.tool_name.clone()),
                    message: format!("MCP client elicitation failed: {error}"),
                })?;
                let ClientResult::CreateElicitationResult(result) = response else {
                    return Err(ToolError::Protocol {
                        message: "MCP client returned an unexpected elicitation response"
                            .to_owned(),
                    });
                };
                let id = request.id.clone();
                let response = match result.action {
                    ElicitationAction::Accept => ToolInteractionResponse::submit(
                        id,
                        result.content.unwrap_or(serde_json::Value::Null),
                    ),
                    ElicitationAction::Decline => ToolInteractionResponse::decline(id, None),
                    ElicitationAction::Cancel => ToolInteractionResponse::cancel(id, None),
                };
                // A rejected response can leave the unique handler pending.
                // Validate before advancing it so the owning call cancels and
                // awaits that handler through its existing resolution-error path.
                request.validate_response(&response)?;
                Ok(response)
            }
            (Self::ElicitFormIfClientSupports, ToolInteractionKind::Form { .. }) => {
                Err(ToolError::Unsupported {
                    operation: "MCP client form elicitation".to_owned(),
                })
            }
            (Self::ElicitFormIfClientSupports, ToolInteractionKind::Text { .. }) => {
                Err(ToolError::Unsupported {
                    operation: "MCP server text interaction".to_owned(),
                })
            }
        }
    }

    fn elicitation_schema(self, schema: serde_json::Value) -> Result<ElicitationSchema, ToolError> {
        let serde_json::Value::Object(object) = schema else {
            return Err(ToolError::Unsupported {
                operation: "non-object MCP form schema".to_owned(),
            });
        };
        ElicitationSchema::from_json_schema(object).map_err(|error| ToolError::Unsupported {
            operation: format!("MCP form schema: {error}"),
        })
    }
}

struct McpOriginPolicy;

impl McpOriginPolicy {
    async fn enforce(request: Request, next: Next) -> Response {
        if let Some(origin) = request.headers().get(header::ORIGIN)
            && !Self::is_loopback(origin)
        {
            return StatusCode::FORBIDDEN.into_response();
        }
        next.run(request).await
    }

    fn is_loopback(origin: &axum::http::HeaderValue) -> bool {
        let Ok(origin) = origin.to_str() else {
            return false;
        };
        let Ok(url) = Url::parse(origin) else {
            return false;
        };
        match url.host() {
            Some(Host::Domain("localhost")) => true,
            Some(Host::Ipv4(address)) => address.is_loopback(),
            Some(Host::Ipv6(address)) => address.is_loopback(),
            _ => false,
        }
    }
}
