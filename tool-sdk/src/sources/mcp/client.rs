use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use http::{HeaderName, HeaderValue};
use rmcp::{
    ServiceExt,
    model::{
        CallToolRequest, CallToolRequestParams, ClientRequest, PaginatedRequestParams,
        ServerResult, Tool,
    },
    service::{Peer, PeerRequestOptions, RequestHandle, RoleClient, RunningService, ServiceError},
    transport::{
        StreamableHttpClientTransport, TokioChildProcess,
        streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use tokio::sync::{Notify, oneshot};

use crate::{
    ToolError,
    mcp::{McpClientConfig, McpConnectionConfig},
    name::ToolName,
};

use super::{interaction::McpInteractionBroker, session::ToolSdkClientHandler};

const MCP_CANCEL_NOTIFICATION_TIMEOUT: Duration = Duration::from_secs(1);
const MCP_SESSION_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub(crate) struct McpClient {
    peer: Peer<RoleClient>,
    request_timeout: Duration,
    broker: McpInteractionBroker,
}

impl std::fmt::Debug for McpClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpClient")
            .field("request_timeout", &self.request_timeout)
            .finish_non_exhaustive()
    }
}

pub(crate) struct McpInFlightRequest {
    tool_name: ToolName,
    response: oneshot::Receiver<Result<ServerResult, ServiceError>>,
    timeout: Duration,
    remote_cancel: McpRemoteCancelSlot,
}

#[derive(Clone)]
pub(crate) struct McpRemoteCancelSlot {
    state: Arc<Mutex<McpRemoteCancelState>>,
    changed: Arc<Notify>,
}

enum McpRemoteCancelState {
    Establishing,
    Active(RequestHandle<RoleClient>),
    Finished,
}

impl McpRemoteCancelSlot {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(McpRemoteCancelState::Establishing)),
            changed: Arc::new(Notify::new()),
        }
    }

    fn state(&self) -> MutexGuard<'_, McpRemoteCancelState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn store(&self, handle: RequestHandle<RoleClient>) {
        let mut state = self.state();
        if !matches!(&*state, McpRemoteCancelState::Finished) {
            *state = McpRemoteCancelState::Active(handle);
        }
        drop(state);
        self.changed.notify_waiters();
    }

    fn clear(&self) {
        *self.state() = McpRemoteCancelState::Finished;
        self.changed.notify_waiters();
    }

    pub(crate) async fn cancel(&self, reason: Option<String>) {
        let cancellation = async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let handle = {
                    let mut state = self.state();
                    match std::mem::replace(&mut *state, McpRemoteCancelState::Finished) {
                        McpRemoteCancelState::Active(handle) => Some(handle),
                        McpRemoteCancelState::Establishing => {
                            *state = McpRemoteCancelState::Establishing;
                            None
                        }
                        McpRemoteCancelState::Finished => return,
                    }
                };
                if let Some(handle) = handle {
                    let _notification_result = handle.cancel(reason.clone()).await;
                    return;
                }
                changed.await;
            }
        };
        let _bounded = tokio::time::timeout(MCP_CANCEL_NOTIFICATION_TIMEOUT, cancellation).await;
    }
}

impl McpInFlightRequest {
    pub(crate) async fn finish(mut self) -> Result<rmcp::model::CallToolResult, ToolError> {
        let result = tokio::time::timeout(self.timeout, &mut self.response).await;

        let result = match result {
            Ok(Ok(Ok(result))) => {
                self.remote_cancel.clear();
                result
            }
            Ok(Ok(Err(error))) => {
                self.remote_cancel.clear();
                return Err(self.response_error(error));
            }
            Ok(Err(_closed)) => {
                self.remote_cancel.clear();
                return Err(ToolError::Protocol {
                    message: format!(
                        "MCP tools/call response failed for `{}`: Transport closed",
                        self.tool_name
                    ),
                });
            }
            Err(_elapsed) => {
                self.remote_cancel
                    .cancel(Some(
                        RequestHandle::<RoleClient>::REQUEST_TIMEOUT_REASON.to_owned(),
                    ))
                    .await;
                return Err(ToolError::Protocol {
                    message: format!(
                        "MCP tools/call response failed for `{}`: request timeout after {:?}",
                        self.tool_name, self.timeout
                    ),
                });
            }
        };

        let ServerResult::CallToolResult(result) = result else {
            return Err(ToolError::Protocol {
                message: format!(
                    "unexpected MCP response type while calling `{}`",
                    self.tool_name
                ),
            });
        };
        Ok(result)
    }

    fn response_error(&self, error: ServiceError) -> ToolError {
        ToolError::Protocol {
            message: format!(
                "MCP tools/call response failed for `{}`: {error}",
                self.tool_name
            ),
        }
    }
}

impl McpClient {
    pub(crate) async fn connect(
        config: &McpClientConfig,
    ) -> Result<
        (
            Self,
            Vec<Tool>,
            RunningService<RoleClient, ToolSdkClientHandler>,
        ),
        ToolError,
    > {
        let broker = McpInteractionBroker::new(config.supports_forms(), config.validates_forms());
        let handler = ToolSdkClientHandler::new(broker.clone());
        let running_service = match config.connection() {
            McpConnectionConfig::Stdio {
                program,
                args,
                cwd,
                env,
            } => {
                let mut command = tokio::process::Command::new(program);
                command.args(args);
                if let Some(cwd) = cwd {
                    command.current_dir(cwd);
                }
                command.envs(env);
                let transport =
                    TokioChildProcess::new(command).map_err(|error| ToolError::Transport {
                        tool: None,
                        message: format!("failed to launch MCP stdio transport: {error}"),
                    })?;
                handler.serve(transport).await
            }
            McpConnectionConfig::StreamableHttp { endpoint, headers } => {
                let transport = StreamableHttpClientTransport::from_config(
                    Self::http_transport_config(endpoint, headers)?,
                );
                handler.serve(transport).await
            }
        }
        .map_err(|error| ToolError::Transport {
            tool: None,
            message: format!("failed to initialize MCP session: {error}"),
        })?;

        let peer = running_service.peer().clone();
        let client = Self {
            peer,
            request_timeout: config.request_timeout(),
            broker,
        };
        let tools = match client.list_all_tools().await {
            Ok(tools) => tools,
            Err(error) => {
                return Err(Self::close_after_build_failure(running_service, error).await);
            }
        };
        Ok((client, tools, running_service))
    }

    pub(crate) fn broker(&self) -> McpInteractionBroker {
        self.broker.clone()
    }

    pub(crate) async fn start_tool_call(
        &self,
        tool_name: ToolName,
        arguments: serde_json::Value,
        remote_cancel: McpRemoteCancelSlot,
    ) -> Result<McpInFlightRequest, ToolError> {
        let native_name = tool_name
            .mcp_native_name()
            .ok_or_else(|| ToolError::Protocol {
                message: format!("tool `{tool_name}` is not an MCP tool name"),
            })?;
        let arguments = Self::arguments_as_object(&tool_name, arguments)?;
        let params = CallToolRequestParams::new(native_name.to_owned()).with_arguments(arguments);
        let request = ClientRequest::CallToolRequest(CallToolRequest::new(params));
        let mut options = PeerRequestOptions::no_options();
        options.timeout = Some(self.request_timeout);
        let mut handle = match self.peer.send_cancellable_request(request, options).await {
            Ok(handle) => handle,
            Err(error) => {
                remote_cancel.clear();
                return Err(ToolError::Protocol {
                    message: format!("MCP tools/call failed for `{tool_name}`: {error}"),
                });
            }
        };
        let (_dummy_tx, dummy_rx) = oneshot::channel();
        let response = std::mem::replace(&mut handle.rx, dummy_rx);
        remote_cancel.store(handle);

        Ok(McpInFlightRequest {
            tool_name,
            response,
            timeout: self.request_timeout,
            remote_cancel,
        })
    }

    async fn list_all_tools(&self) -> Result<Vec<Tool>, ToolError> {
        let mut tools = Vec::new();
        let mut cursor = None;
        loop {
            let page = self
                .peer
                .list_tools(Some(PaginatedRequestParams::default().with_cursor(cursor)))
                .await
                .map_err(|error| ToolError::Protocol {
                    message: format!("MCP tools/list failed: {error}"),
                })?;
            tools.extend(page.tools);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        Ok(tools)
    }

    fn arguments_as_object(
        tool_name: &ToolName,
        arguments: serde_json::Value,
    ) -> Result<serde_json::Map<String, serde_json::Value>, ToolError> {
        match arguments {
            serde_json::Value::Object(object) => Ok(object),
            _ => Err(ToolError::InvalidArguments {
                tool: tool_name.clone(),
                message: "MCP tool arguments must be a JSON object".to_owned(),
                issues: Vec::new(),
            }),
        }
    }

    fn http_transport_config(
        endpoint: &url::Url,
        headers: &BTreeMap<String, String>,
    ) -> Result<StreamableHttpClientTransportConfig, ToolError> {
        let mut config = StreamableHttpClientTransportConfig::with_uri(endpoint.as_str());
        for (name, value) in headers {
            let header_name = HeaderName::from_bytes(name.as_bytes()).map_err(|error| {
                ToolError::InvalidConfiguration {
                    message: format!("invalid MCP HTTP header name `{name}`: {error}"),
                }
            })?;
            let header_value =
                HeaderValue::from_str(value).map_err(|error| ToolError::InvalidConfiguration {
                    message: format!("invalid MCP HTTP header value for `{name}`: {error}"),
                })?;
            config.custom_headers.insert(header_name, header_value);
        }
        Ok(config)
    }

    pub(crate) async fn close_session(
        mut session: RunningService<RoleClient, ToolSdkClientHandler>,
    ) -> Result<(), ToolError> {
        match session.close_with_timeout(MCP_SESSION_CLOSE_TIMEOUT).await {
            Ok(Some(_reason)) => Ok(()),
            Ok(None) => Err(ToolError::Transport {
                tool: None,
                message: format!(
                    "MCP session cleanup timed out after {:?}",
                    MCP_SESSION_CLOSE_TIMEOUT
                ),
            }),
            Err(error) => Err(ToolError::Transport {
                tool: None,
                message: format!("failed to close MCP session: {error}"),
            }),
        }
    }

    pub(crate) async fn close_after_build_failure(
        session: RunningService<RoleClient, ToolSdkClientHandler>,
        primary: ToolError,
    ) -> ToolError {
        match Self::close_session(session).await {
            Ok(()) => primary,
            Err(cleanup) => ToolError::BuildCleanup {
                primary: Box::new(primary),
                cleanup: Box::new(cleanup),
            },
        }
    }
}
