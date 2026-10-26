use serde_json::{Value, json};

use crate::SdkError;
use crate::error::ValidationViolation;
use crate::error::{ErrorPhase, ResponseViolation};
use crate::generation::{FunctionCall, ProviderToolSpec};
use crate::providers::ollama::wire::OllamaToolCall;

pub(crate) fn tool_spec_to_ollama(tool: &ProviderToolSpec) -> Result<Value, SdkError> {
    match tool {
        ProviderToolSpec::Function(function) => Ok(json!({
            "type": "function",
            "function": {
                "name": function.name(),
                "description": function.description(),
                "parameters": function.input_schema().schema().clone(),
            }
        })),
        ProviderToolSpec::Hosted(_) => Err(SdkError::unsupported(
            ValidationViolation::Unsupported {
                field: "tools.hosted",
                rule: "hosted tools unavailable",
            },
            "Ollama hosted tools",
        )),
    }
}

pub(crate) fn tool_call_to_ollama(call: &FunctionCall) -> Value {
    let mut value = json!({
        "type": "function",
        "function": {
            "name": call.name(),
            "arguments": call.arguments(),
        }
    });
    if let Some(id) = call.id() {
        value["id"] = Value::String(id.to_owned());
    }
    value
}

pub(crate) fn tool_calls_from_wire(
    calls: &[OllamaToolCall],
) -> Result<Vec<FunctionCall>, SdkError> {
    calls
        .iter()
        .map(|call| {
            let function = call.function.as_ref().ok_or_else(|| {
                SdkError::invalid_response(ResponseViolation::MissingField { field: "function" })
                    .at_phase(ErrorPhase::Normalize)
                    .with_message("Ollama tool call did not include function".to_owned())
                    .with_diagnostic(None)
            })?;
            FunctionCall::new(
                call.id.clone(),
                function.name.clone().ok_or_else(|| {
                    SdkError::invalid_response(ResponseViolation::MissingField {
                        field: "function.name",
                    })
                    .at_phase(ErrorPhase::Normalize)
                    .with_message("Ollama function call did not include name".to_owned())
                    .with_diagnostic(None)
                })?,
                function
                    .arguments
                    .clone()
                    .unwrap_or_else(|| Value::Object(serde_json::Map::new())),
            )
            .map_err(|_| {
                SdkError::invalid_response(ResponseViolation::InvalidField { field: "function" })
                    .at_phase(ErrorPhase::Normalize)
                    .with_message(
                        "Ollama function call used an invalid id, name, or arguments".to_string(),
                    )
                    .with_diagnostic(crate::transport::CheckedRequestSpec::diagnostic_json(call))
            })
        })
        .collect()
}
