use serde_json::{Value, json};

use crate::SdkError;
use crate::generation::{AllowedToolMode, ProviderToolSpec, ToolChoice};
use crate::providers::anthropic::tools::AnthropicHostedPlanner;

// Lower-level Anthropic tool renderer for selected Claude models. It serializes
// already-validated portable tool_use/tool_result, forced tool choice,
// allowed-tool modes, strict schema payloads, and server tools that need no
// provider-specific public configuration. Sources:
// https://platform.claude.com/docs/en/agents-and-tools/tool-use/overview
// https://platform.claude.com/docs/en/agents-and-tools/tool-use/tool-reference
pub(super) fn tool_choice_to_value(choice: &ToolChoice) -> Value {
    match choice {
        ToolChoice::Auto => json!({"type": "auto"}),
        ToolChoice::None => json!({"type": "none"}),
        ToolChoice::Required | ToolChoice::Any => json!({"type": "any"}),
        ToolChoice::Tool { name } => json!({"type": "tool", "name": name.trim()}),
        ToolChoice::Allowed { mode, .. } => match mode {
            AllowedToolMode::Auto => json!({"type": "auto"}),
            AllowedToolMode::Required => json!({"type": "any"}),
        },
    }
}

pub(super) fn tool_spec_to_value(tool: &ProviderToolSpec) -> Result<Value, SdkError> {
    match tool {
        ProviderToolSpec::Function(tool) => Ok(json!({
            "name": tool.name(),
            "description": tool.description(),
            "input_schema": tool.input_schema().schema(),
            "strict": true,
        })),
        ProviderToolSpec::Hosted(tool) => hosted_tool_to_value(tool),
    }
}
fn hosted_tool_to_value(declaration: &crate::hosted::HostedTool) -> Result<Value, SdkError> {
    AnthropicHostedPlanner::new().render_declaration(declaration)
}
