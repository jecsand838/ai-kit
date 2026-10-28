use std::{path::PathBuf, sync::Arc};

use rmcp::{
    ErrorData as McpError, RoleServer, ServerHandler,
    model::AnnotateAble,
    model::{
        CallToolRequestMethod, CallToolRequestParams, CallToolResult, CancelledNotificationParam,
        CreateElicitationRequestParams, ElicitationSchema, Icon, JsonObject, ListToolsResult, Meta,
        RawContent, RawResource, RawTextContent, ServerCapabilities, ServerInfo, TaskSupport, Tool,
        ToolAnnotations, ToolExecution,
    },
    service::{NotificationContext, RequestContext},
};
use serde::Deserialize;
use serde_json::{Map, Value, json};

pub const ENV_EXIT_MARKER: &str = "TOOLS_KIT_MCP_EXIT_MARKER";
pub const ENV_LIST_TOOLS_ERROR: &str = "TOOLS_KIT_MCP_LIST_TOOLS_ERROR";
pub const ENV_CANCEL_MARKER: &str = "TOOLS_KIT_MCP_CANCEL_MARKER";

#[derive(Clone)]
pub struct RepoManagedMcpServer {
    _state: Arc<ServerState>,
}

impl RepoManagedMcpServer {
    pub fn new(
        exit_marker: Option<PathBuf>,
        fail_list_tools: bool,
        cancel_marker: Option<PathBuf>,
        extra_echo_tools: usize,
    ) -> Self {
        Self {
            _state: Arc::new(ServerState {
                exit_marker,
                fail_list_tools,
                cancel_marker,
                extra_echo_tools,
            }),
        }
    }

    pub fn from_env() -> Self {
        Self::new(
            std::env::var_os(ENV_EXIT_MARKER).map(PathBuf::from),
            std::env::var_os(ENV_LIST_TOOLS_ERROR).is_some(),
            std::env::var_os(ENV_CANCEL_MARKER).map(PathBuf::from),
            0,
        )
    }

    fn current_tools(&self) -> Vec<Tool> {
        let mut tools = vec![
            echo_tool(),
            invalid_output_tool(),
            metadata_echo_tool(),
            error_metadata_tool(),
            missing_is_error_tool(),
            content_metadata_tool(),
            resource_link_tool(),
            form_elicitation_tool(),
            cancel_observer_tool(),
            blocking_cancel_observer_tool(),
        ];
        tools.extend((0..self._state.extra_echo_tools).map(extra_echo_tool));
        tools
    }
}

impl ServerHandler for RepoManagedMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_instructions("Repo-managed MCP server for tools-kit validation.")
    }

    async fn list_tools(
        &self,
        _request: Option<rmcp::model::PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        if self._state.fail_list_tools {
            return Err(McpError::invalid_params(
                "repo-managed tools/list failure".to_owned(),
                None,
            ));
        }

        Ok(ListToolsResult::with_all_items(self.current_tools()))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        match request.name.as_ref() {
            "echo" => {
                let EchoArgs { message } = parse_arguments(request.arguments)?;
                Ok(CallToolResult::structured(json!({ "echoed": message })))
            }
            "invalid_output" => {
                if request
                    .arguments
                    .as_ref()
                    .and_then(|args| args.get("omit_structured"))
                    .and_then(Value::as_bool)
                    == Some(true)
                {
                    Ok(CallToolResult::success(vec![rmcp::model::Content::text(
                        "done",
                    )]))
                } else {
                    Ok(CallToolResult::structured(json!({ "valid": "no" })))
                }
            }
            "metadata_echo" => {
                let EchoArgs { message } = parse_arguments(request.arguments)?;
                let client_info = context.peer.peer_info().map(|info| &info.client_info);
                Ok(
                    CallToolResult::structured(json!({ "echoed": message })).with_meta(Some(
                        meta_object(&[
                            ("trace_id", json!("mcp-result")),
                            ("client_info", json!(client_info)),
                        ]),
                    )),
                )
            }
            "error_metadata" => Ok(CallToolResult::structured_error(json!({ "valid": "no" }))
                .with_meta(Some(meta_object(&[(
                    "trace_id",
                    json!("mcp-error-result"),
                )])))),
            "missing_is_error" => {
                let mut result = CallToolResult::structured(json!({ "ok": true }));
                result.is_error = None;
                Ok(result)
            }
            "content_metadata" => Ok(CallToolResult::success(vec![
                RawContent::Text(RawTextContent {
                    text: "content metadata".to_owned(),
                    meta: Some(meta_object(&[("content_trace", json!("mcp-content"))])),
                })
                .with_priority(0.75),
            ])),
            "resource_link" => Ok(CallToolResult::success(vec![
                RawContent::resource_link(
                    RawResource::new("file:///tools-kit/resource.md", "resource-guide")
                        .with_title("Resource Guide")
                        .with_description("A repo-managed resource link.")
                        .with_mime_type("text/markdown")
                        .with_size(123)
                        .with_icons(vec![
                            Icon::new("https://example.com/tools-kit-resource.png")
                                .with_mime_type("image/png"),
                        ])
                        .with_meta(meta_object(&[("resource_trace", json!("mcp-resource"))])),
                )
                .with_priority(0.5),
            ])),
            "form_elicitation" => {
                let schema = ElicitationSchema::builder()
                    .required_string("choice")
                    .optional_string("note")
                    .build()
                    .map_err(|error| McpError::invalid_params(error.to_owned(), None))?;
                let result = context
                    .peer
                    .create_elicitation(CreateElicitationRequestParams::FormElicitationParams {
                        meta: None,
                        message: "Choose a form option".to_owned(),
                        requested_schema: schema,
                    })
                    .await
                    .map_err(|error| McpError::internal_error(error.to_string(), None))?;
                Ok(CallToolResult::structured(json!({
                    "action": result.action,
                    "content": result.content,
                })))
            }
            "cancel_observer" => {
                let schema = ElicitationSchema::builder()
                    .required_string("choice")
                    .build()
                    .map_err(|error| McpError::invalid_params(error.to_owned(), None))?;
                let _result = context
                    .peer
                    .create_elicitation(CreateElicitationRequestParams::FormElicitationParams {
                        meta: None,
                        message: "Hold until cancellation".to_owned(),
                        requested_schema: schema,
                    })
                    .await
                    .map_err(|error| McpError::internal_error(error.to_string(), None))?;
                context.ct.cancelled().await;
                Ok(CallToolResult::structured(json!({ "cancelled": true })))
            }
            "blocking_cancel_observer" => {
                if let Some(path) = &self._state.cancel_marker {
                    let _ = std::fs::write(path, format!("started_request_id={}\n", context.id));
                }
                context.ct.cancelled().await;
                Ok(CallToolResult::structured(json!({ "cancelled": true })))
            }
            name if name.starts_with("extra_echo_") => {
                Ok(CallToolResult::structured(json!({ "tool": name })))
            }
            _ => Err(McpError::method_not_found::<CallToolRequestMethod>()),
        }
    }

    async fn on_cancelled(
        &self,
        notification: CancelledNotificationParam,
        _context: NotificationContext<RoleServer>,
    ) {
        if let Some(path) = &self._state.cancel_marker {
            let reason = notification.reason.unwrap_or_default();
            let contents = format!("request_id={}\nreason={reason}\n", notification.request_id);
            let _ = std::fs::write(path, contents);
        }
    }
}

#[derive(Debug)]
struct ServerState {
    exit_marker: Option<PathBuf>,
    fail_list_tools: bool,
    cancel_marker: Option<PathBuf>,
    extra_echo_tools: usize,
}

impl Drop for ServerState {
    fn drop(&mut self) {
        if let Some(path) = &self.exit_marker {
            let _ = std::fs::write(path, b"closed");
        }
    }
}

fn parse_arguments<T>(arguments: Option<JsonObject>) -> Result<T, McpError>
where
    T: for<'de> Deserialize<'de>,
{
    let Some(arguments) = arguments else {
        return Err(McpError::invalid_params(
            "missing required tool arguments".to_owned(),
            None,
        ));
    };

    serde_json::from_value(Value::Object(arguments))
        .map_err(|error| McpError::invalid_params(error.to_string(), None))
}

fn echo_tool() -> Tool {
    Tool::new(
        "echo",
        "Echo the provided message.",
        schema_object(json!({
            "type": "object",
            "required": ["message"],
            "properties": {
                "message": { "type": "string" }
            },
            "additionalProperties": false
        })),
    )
    .with_title("Echo")
    .with_annotations(
        ToolAnnotations::with_title("Repo Echo")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(false),
    )
    .with_execution(ToolExecution::new().with_task_support(TaskSupport::Optional))
    .with_icons(vec![
        Icon::new("https://example.com/tools-kit.png").with_mime_type("image/png"),
    ])
    .with_meta(meta_object(&[("catalog_trace", json!("repo-echo"))]))
    .with_raw_output_schema(schema_object(json!({
        "type": "object",
        "required": ["echoed"],
        "properties": {
            "echoed": { "type": "string" }
        },
        "additionalProperties": false
    })))
}

fn invalid_output_tool() -> Tool {
    Tool::new(
        "invalid_output",
        "Return structured output that intentionally violates the advertised output schema.",
        schema_object(json!({
            "type": "object",
            "properties": {"omit_structured": {"type":"boolean"}},
            "additionalProperties": false
        })),
    )
    .with_title("Invalid Output")
    .with_raw_output_schema(schema_object(json!({
        "type": "object",
        "required": ["valid"],
        "properties": {
            "valid": { "type": "boolean" }
        },
        "additionalProperties": false
    })))
}

fn metadata_echo_tool() -> Tool {
    Tool::new(
        "metadata_echo",
        "Echo the provided message with MCP result metadata.",
        schema_object(json!({
            "type": "object",
            "required": ["message"],
            "properties": {
                "message": { "type": "string" }
            },
            "additionalProperties": false
        })),
    )
    .with_raw_output_schema(schema_object(json!({
        "type": "object",
        "required": ["echoed"],
        "properties": {
            "echoed": { "type": "string" }
        },
        "additionalProperties": false
    })))
}

fn error_metadata_tool() -> Tool {
    Tool::new(
        "error_metadata",
        "Return an MCP error result with metadata and schema-invalid structured content.",
        schema_object(json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        })),
    )
    .with_raw_output_schema(schema_object(json!({
        "type": "object",
        "required": ["valid"],
        "properties": {
            "valid": { "type": "boolean" }
        },
        "additionalProperties": false
    })))
}

fn missing_is_error_tool() -> Tool {
    Tool::new(
        "missing_is_error",
        "Return an MCP result that omits isError for strict validation.",
        schema_object(json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        })),
    )
}

fn content_metadata_tool() -> Tool {
    Tool::new(
        "content_metadata",
        "Return MCP text content with annotations and _meta.",
        schema_object(json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        })),
    )
}

fn resource_link_tool() -> Tool {
    Tool::new(
        "resource_link",
        "Return an MCP resource_link with native metadata fields.",
        schema_object(json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        })),
    )
}

fn form_elicitation_tool() -> Tool {
    Tool::new(
        "form_elicitation",
        "Request a form elicitation from the MCP client and return the selected action.",
        schema_object(json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        })),
    )
    .with_raw_output_schema(schema_object(json!({
        "type": "object",
        "required": ["action"],
        "properties": {
            "action": { "type": "string" },
            "content": {}
        },
        "additionalProperties": false
    })))
}

fn cancel_observer_tool() -> Tool {
    Tool::new(
        "cancel_observer",
        "Wait for cancellation after requesting form input.",
        schema_object(json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        })),
    )
}

fn blocking_cancel_observer_tool() -> Tool {
    Tool::new(
        "blocking_cancel_observer",
        "Block until the caller sends an MCP cancellation notification.",
        schema_object(json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        })),
    )
}

fn extra_echo_tool(index: usize) -> Tool {
    Tool::new(
        format!("extra_echo_{index:03}"),
        "Return this generated fixture tool's name.",
        schema_object(json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        })),
    )
    .with_raw_output_schema(schema_object(json!({
        "type": "object",
        "required": ["tool"],
        "properties": {
            "tool": { "type": "string" }
        },
        "additionalProperties": false
    })))
}

fn schema_object(value: Value) -> Arc<Map<String, Value>> {
    Arc::new(serde_json::from_value(value).expect("tool schemas must be JSON objects"))
}

fn meta_object(entries: &[(&str, Value)]) -> Meta {
    let mut meta = Meta::new();
    for (key, value) in entries {
        meta.insert((*key).to_owned(), value.clone());
    }
    meta
}

#[derive(Deserialize)]
struct EchoArgs {
    message: String,
}
