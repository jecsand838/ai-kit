#[cfg(feature = "mcp-server")]
pub mod mcp_http_harness;
pub mod mcp_server_binary;
pub mod mcp_stdio_harness;
#[cfg(feature = "mcp-server")]
pub mod repo_managed_mcp_service;

use serde_json::Value;
use tool_sdk::{
    ToolError, ToolHandlerBackend, Toolset, ToolsetBackend,
    declaration::ToolName,
    execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation, ToolResult},
};

pub async fn invoke_tool(
    toolset: &Toolset,
    name: &str,
    call_id: Option<&str>,
    arguments: Value,
) -> Result<ToolResult, ToolError> {
    let mut handler = toolset.tool_handler(ToolName::new(name)?).await?;
    match handler
        .step(ToolHandlerInput::Invoke(ToolInvocation {
            call_id: call_id.map(str::to_owned),
            arguments,
        }))
        .await?
    {
        ToolHandlerOutput::Completed(result) => Ok(result),
        ToolHandlerOutput::Interaction(request) => Err(ToolError::InvalidInteraction {
            id: request.id.as_str().to_owned(),
            message: "test helper expected a completed result".to_owned(),
        }),
    }
}
