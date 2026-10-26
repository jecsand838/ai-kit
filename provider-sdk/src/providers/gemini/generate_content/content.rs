use serde_json::{Map, Value, json};

use crate::SdkError;
use crate::error::ResponseViolation;
use crate::error::ValidationViolation;

use crate::core::GenerateRequestView;
use crate::generation::{ContentPart, GenerationContinuation, Message, NativeContinuation, Role};
use crate::models::ModelId;

use super::super::wire::GeminiFunctionCall;
use super::attachments::{part_to_wire_part, parts_to_wire_parts};

pub(super) fn build_contents(
    model: &ModelId,
    request: GenerateRequestView<'_>,
) -> Result<Vec<Value>, SdkError> {
    request
        .messages
        .iter()
        .filter(|message| !matches!(message.role(), Role::System | Role::Developer))
        .map(|message| message_to_content(model, message))
        .collect()
}

pub(super) fn build_system_instruction(
    request: GenerateRequestView<'_>,
) -> Result<Option<Value>, SdkError> {
    let parts = request
        .messages
        .iter()
        .take_while(|message| matches!(message.role(), Role::System | Role::Developer))
        .flat_map(Message::content)
        .map(part_to_wire_part)
        .collect::<Result<Vec<_>, _>>()?;
    Ok((!parts.is_empty()).then(|| json!({"parts": parts})))
}

fn message_to_content(
    model: &ModelId,
    message: &Message<GenerationContinuation>,
) -> Result<Value, SdkError> {
    if let Some(continuation) = message.continuation() {
        if continuation.model_id != *model {
            return Err(SdkError::invalid_input(
                ValidationViolation::Incompatible {
                    field: "message.continuation.model",
                    other: "selected_model",
                },
                "Gemini continuation belongs to a different model",
            ));
        }
        return match &continuation.inner {
            NativeContinuation::Gemini { signed_parts } => Ok(json!({
                "role": "model",
                "parts": signed_parts,
            })),
            _ => Err(SdkError::invalid_input(
                ValidationViolation::Incompatible {
                    field: "message.continuation",
                    other: "selected_provider",
                },
                "Gemini generation requires Gemini continuation state",
            )),
        };
    }
    match message.role() {
        Role::Tool => tool_result_content(model, message),
        Role::Assistant => assistant_content(message),
        Role::User => Ok(json!({
            "role": "user",
            "parts": parts_to_wire_parts(message.content())?,
        })),
        Role::System | Role::Developer => Err(SdkError::invalid_state(
            "instruction messages must be rendered as systemInstruction",
        )),
    }
}

fn assistant_content(message: &Message<GenerationContinuation>) -> Result<Value, SdkError> {
    let mut parts = parts_to_wire_parts(message.content())?;
    for call in message.function_calls() {
        let mut function_call = Map::new();
        function_call.insert("name".to_string(), Value::String(call.name().to_string()));
        function_call.insert("args".to_string(), call.arguments().clone());
        if let Some(id) = call.id() {
            function_call.insert("id".to_string(), Value::String(id.to_string()));
        }
        parts.push(json!({"functionCall": function_call}));
    }
    Ok(json!({"role": "model", "parts": parts}))
}

fn tool_result_content(
    model: &ModelId,
    message: &Message<GenerationContinuation>,
) -> Result<Value, SdkError> {
    let supports_multimodal = matches!(
        model.as_str(),
        "models/gemini-4-argon"
            | "models/gemini-3.8-flash"
            | "models/gemini-3.1-pro-preview"
            | "models/gemini-3-flash-preview"
    );
    let mut parts = Vec::new();
    for result in message.function_results() {
        let mut content = Vec::new();
        let mut media = Vec::new();
        for part in result.content() {
            match part {
                ContentPart::Text { text } => content.push(Value::String(text.clone())),
                ContentPart::Json { value } => content.push(value.clone()),
                ContentPart::Image { media_type, .. }
                    if supports_multimodal
                        && matches!(
                            media_type.as_str(),
                            "image/png" | "image/jpeg" | "image/webp"
                        ) =>
                {
                    media.push(part_to_wire_part(part)?);
                }
                ContentPart::Document { media_type, .. }
                    if supports_multimodal
                        && matches!(media_type.as_str(), "application/pdf" | "text/plain") =>
                {
                    media.push(part_to_wire_part(part)?);
                }
                ContentPart::Image { .. }
                | ContentPart::Audio { .. }
                | ContentPart::Document { .. } => {
                    return Err(SdkError::unsupported(
                        ValidationViolation::Unsupported {
                            field: "function_result.content",
                            rule: "media unavailable for selected model",
                        },
                        format!("Gemini function-result media for selected model `{model}`"),
                    ));
                }
                ContentPart::ImageUrl { .. } | ContentPart::File { .. } => {
                    return Err(SdkError::unsupported(
                        ValidationViolation::Unsupported {
                            field: "function_result.content",
                            rule: "remote media references unavailable",
                        },
                        "Gemini function-result remote media references",
                    ));
                }
                ContentPart::Thinking { .. } => {
                    return Err(SdkError::invalid_input(
                        ValidationViolation::InvalidValue {
                            field: "function_result.content",
                            rule: "thinking content is not permitted",
                        },
                        "Gemini function results cannot contain thinking content",
                    ));
                }
            }
        }
        let content = match content.as_slice() {
            [] => Value::Null,
            [value] => value.clone(),
            _ => Value::Array(content),
        };
        let mut response = Map::new();
        response.insert("name".to_string(), Value::String(result.name().to_string()));
        let mut output = Map::new();
        output.insert(
            if result.is_error() { "error" } else { "result" }.to_string(),
            content,
        );
        response.insert("response".to_string(), Value::Object(output));
        if !media.is_empty() {
            response.insert("parts".to_string(), Value::Array(media));
        }
        if let Some(id) = result.call_id() {
            response.insert("id".to_string(), Value::String(id.to_string()));
        }
        parts.push(json!({"functionResponse": response}));
    }
    Ok(json!({"role": "user", "parts": parts}))
}

pub(crate) fn function_call_to_tool_call(
    function_call: &GeminiFunctionCall,
) -> Result<crate::generation::FunctionCall, SdkError> {
    crate::core::tool_call(
        function_call.id.clone(),
        function_call.name.clone(),
        function_call.args.clone().ok_or_else(|| {
            SdkError::invalid_response(ResponseViolation::MissingField {
                field: "functionCall.args",
            })
            .with_message("Gemini functionCall did not include args".to_string())
            .with_diagnostic(None)
        })?,
    )
}
