use serde_json::{Value, json};

use crate::KitError;
use crate::error::ValidationViolation;
use crate::generation::{ProviderToolSpec, ResponseFormat};

// Lower-level OpenAI tool and structured-output renderer for selected GPT
// models. It serializes already-validated function tools, strict schemas,
// allowed_tools, forced tool names, and response_format JSON schema payloads.
// Hosted tools that need provider-specific configuration remain unsupported
// instead of using metadata or native payload escape hatches. Sources:
// https://developers.openai.com/api/docs/guides/function-calling,
// https://developers.openai.com/api/docs/guides/tools-web-search,
// https://developers.openai.com/api/docs/guides/structured-outputs
pub(crate) fn tool_spec_to_value(tool: &ProviderToolSpec) -> Result<Value, KitError> {
    match tool {
        ProviderToolSpec::Function(function) => Ok(json!({
            "type": "function",
            "name": function.name(),
            "description": function.description(),
            "parameters": function.input_schema().schema().clone(),
            "strict": true,
        })),
        ProviderToolSpec::Hosted(tool) => openai_hosted_tool_to_value(tool),
    }
}
fn openai_hosted_tool_to_value(declaration: &crate::hosted::HostedTool) -> Result<Value, KitError> {
    crate::providers::openai::tools::render_hosted_tool(declaration)
}

pub(super) fn response_format_to_value(
    format: Option<&ResponseFormat>,
) -> Result<Option<Value>, KitError> {
    let Some(format) = format else {
        return Ok(None);
    };

    match format {
        ResponseFormat::Text => Ok(None),
        ResponseFormat::JsonObject => Ok(Some(json!({"type": "json_object"}))),
        ResponseFormat::JsonSchema(schema) => Ok(Some(json!({
            "type": "json_schema",
            "name": "response",
            "strict": true,
            "schema": schema.schema(),
        }))),
    }
}

pub(crate) fn tool_choice_to_value(
    choice: &crate::generation::ToolChoice,
    selected_tools: &[ProviderToolSpec],
) -> Result<Value, KitError> {
    match choice {
        crate::generation::ToolChoice::Auto => Ok(Value::String("auto".to_string())),
        crate::generation::ToolChoice::None => Ok(Value::String("none".to_string())),
        crate::generation::ToolChoice::Required | crate::generation::ToolChoice::Any => {
            Ok(Value::String("required".to_string()))
        }
        crate::generation::ToolChoice::Tool { name } => {
            let tool = selected_tools.first().ok_or_else(|| {
                KitError::invalid_input(
                    ValidationViolation::InvalidValue {
                        field: "tool_choice.name",
                        rule: "must reference a declared tool",
                    },
                    format!("OpenAI tool choice references unknown tool {name}"),
                )
            })?;
            let resolved_name = crate::core::tool_name(tool).unwrap_or_else(|| name.trim());
            openai_named_tool_choice(tool, resolved_name)
        }
        crate::generation::ToolChoice::Allowed { mode, .. } => Ok(json!({
            "type": "allowed_tools",
            "mode": match mode {
                crate::generation::AllowedToolMode::Auto => "auto",
                crate::generation::AllowedToolMode::Required => "required",
            },
            "tools": selected_tools
                .iter()
                .map(openai_allowed_tool_descriptor)
                .collect::<Result<Vec<_>, _>>()?,
        })),
    }
}

fn openai_allowed_tool_descriptor(tool: &ProviderToolSpec) -> Result<Value, KitError> {
    let name = crate::core::tool_name(tool).ok_or_else(|| {
        KitError::invalid_input(
            ValidationViolation::MissingField {
                field: "tool_choice.names",
            },
            "OpenAI allowed_tools entries require named tools",
        )
    })?;
    openai_named_tool_choice(tool, name)
}

fn openai_named_tool_choice(tool: &ProviderToolSpec, name: &str) -> Result<Value, KitError> {
    match tool {
        ProviderToolSpec::Function(_) => Ok(json!({"type": "function", "name": name})),
        ProviderToolSpec::Hosted(_) => Err(KitError::invalid_input(
            ValidationViolation::InvalidValue {
                field: "tool_choice.name",
                rule: "named selection requires a function tool",
            },
            format!(
                "OpenAI hosted tools cannot be targeted by name through ToolChoice::Tool ({name})"
            ),
        )),
    }
}
