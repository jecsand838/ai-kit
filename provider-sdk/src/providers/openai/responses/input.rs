use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};

use crate::SdkError;
use crate::core::GenerateRequestView;
use crate::error::ValidationViolation;
use crate::generation::{
    ContentPart, FileReference, FunctionResult, Message, NativeContinuation, Role,
};
use crate::models::ModelId;

// Lower-level OpenAI Responses renderer for already-selected GPT models.
// Gpt5_5/Gpt5_4 family paths enter through concrete model dispatch before this
// helper renders system/developer content, image input, input_file/file_url/
// file_id support, assistant tool-call replay, and function_call_output.call_id.
// Unsupported thinking/audio/JSON history returns explicit errors rather than
// being silently dropped or routed through fallback.
// Sources:
// https://developers.openai.com/api/docs/guides/function-calling,
// https://developers.openai.com/api/docs/guides/images-vision
pub(crate) fn build_input_items(
    model: &ModelId,
    request: GenerateRequestView<'_>,
) -> Result<Vec<Value>, SdkError> {
    let mut items = Vec::new();

    for message in request.messages {
        if let Some(continuation) = message.continuation() {
            if &continuation.model_id != model {
                return Err(SdkError::invalid_input(
                    ValidationViolation::Incompatible {
                        field: "message.continuation.model",
                        other: "selected_model",
                    },
                    "OpenAI continuation belongs to a different selected model",
                ));
            }
            let NativeContinuation::OpenAi { output_items } = &continuation.inner else {
                return Err(SdkError::invalid_input(
                    ValidationViolation::Incompatible {
                        field: "message.continuation",
                        other: "selected_provider",
                    },
                    "OpenAI generation cannot replay another provider's continuation",
                ));
            };
            items.extend(output_items.iter().cloned());
            continue;
        }
        if !message.function_results().is_empty() {
            items.extend(tool_result_message_to_input_items(model, message)?);
            continue;
        }

        items.push(message_to_input_item(model, message)?);
        push_assistant_tool_calls(&mut items, message)?;
    }
    Ok(items)
}

fn push_assistant_tool_calls(items: &mut Vec<Value>, message: &Message) -> Result<(), SdkError> {
    if message.role() != Role::Assistant {
        return Ok(());
    }
    for tool_call in message.function_calls() {
        let call_id = tool_call.id().ok_or_else(|| {
            SdkError::invalid_input(
                ValidationViolation::MissingField {
                    field: "function_call.id",
                },
                "OpenAI function calls require call_id",
            )
        })?;
        items.push(json!({
            "type": "function_call",
            "call_id": call_id,
            "name": tool_call.name(),
            "arguments": tool_call.arguments().to_string(),
        }));
    }
    Ok(())
}

pub(crate) fn message_to_input_item(model: &ModelId, message: &Message) -> Result<Value, SdkError> {
    let content = message
        .content()
        .iter()
        .map(|part| match message.role() {
            Role::Assistant => content_part_to_assistant_input_value(part),
            _ => content_part_to_input_value(model, part),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(json!({
        "type": "message",
        "role": match message.role() {
            Role::System => "system",
            Role::Developer => "developer",
            Role::User => "user",
            Role::Assistant => "assistant",
            Role::Tool => "tool",
        },
        "content": content,
    }))
}

fn content_part_to_assistant_input_value(part: &ContentPart) -> Result<Value, SdkError> {
    match part {
        ContentPart::Text { text } => Ok(json!({"type": "output_text", "text": text})),
        _ => Err(SdkError::invalid_input(
            ValidationViolation::InvalidValue {
                field: "message.content",
                rule: "assistant input requires text or continuation replay",
            },
            "OpenAI assistant input supports text plus opaque continuation replay only",
        )),
    }
}

fn tool_result_message_to_input_items(
    model: &ModelId,
    message: &Message,
) -> Result<Vec<Value>, SdkError> {
    message
        .function_results()
        .iter()
        .map(|result| tool_result_to_function_call_output(model, result))
        .collect()
}

pub(crate) fn tool_result_to_function_call_output(
    model: &ModelId,
    result: &FunctionResult,
) -> Result<Value, SdkError> {
    let call_id = result.call_id().ok_or_else(|| {
        SdkError::invalid_input(
            ValidationViolation::MissingField {
                field: "function_result.call_id",
            },
            "OpenAI function results require call_id",
        )
    })?;
    let output = content_parts_to_tool_output(model, result.content())?;
    Ok(json!({
        "type": "function_call_output",
        "call_id": call_id,
        "output": output,
    }))
}

fn content_parts_to_tool_output(model: &ModelId, parts: &[ContentPart]) -> Result<Value, SdkError> {
    if parts.is_empty() {
        return Err(SdkError::invalid_input(
            ValidationViolation::Empty {
                field: "function_result.content",
            },
            "OpenAI function results require content",
        ));
    }
    if let [ContentPart::Text { text }] = parts {
        return Ok(Value::String(text.clone()));
    }
    if let [ContentPart::Json { value }] = parts {
        return Ok(Value::String(value.to_string()));
    }
    Ok(Value::Array(
        parts
            .iter()
            .map(|part| match part {
                ContentPart::Json { value } => {
                    Ok(json!({"type": "input_text", "text": value.to_string()}))
                }
                ContentPart::Thinking { .. } | ContentPart::Audio { .. } => {
                    Err(SdkError::unsupported(
                        ValidationViolation::Unsupported {
                            field: "function_result.content",
                            rule: "thinking and audio unavailable",
                        },
                        "OpenAI function-result content type",
                    ))
                }
                other => content_part_to_input_value(model, other),
            })
            .collect::<Result<_, _>>()?,
    ))
}

pub(crate) fn content_part_to_input_value(
    model: &ModelId,
    part: &ContentPart,
) -> Result<Value, SdkError> {
    match part {
        ContentPart::Text { text } => Ok(json!({"type": "input_text", "text": text})),
        ContentPart::ImageUrl { url } => {
            Ok(json!({"type": "input_image", "image_url": url.to_string()}))
        }
        ContentPart::Image { media_type, data } => Ok(json!({
            "type": "input_image",
            "image_url": format!("data:{media_type};base64,{}", STANDARD.encode(data)),
        })),
        ContentPart::Audio { .. } => Err(SdkError::unsupported(
            ValidationViolation::Unsupported {
                field: "content.audio",
                rule: "stateless audio input is not supplied by this SDK",
            },
            format!(
                "OpenAI stateless audio input is currently unsupported by this SDK for exact model `{model}`"
            ),
        )),
        ContentPart::Document {
            media_type,
            data,
            file_name,
        } => Ok(json!({
            "type": "input_file",
            "file_data": format!("data:{media_type};base64,{}", STANDARD.encode(data)),
            "filename": file_name,
        })),
        ContentPart::File { reference } => file_ref_to_input_part(reference),
        ContentPart::Thinking { .. } | ContentPart::Json { .. } => Err(SdkError::invalid_input(
            ValidationViolation::InvalidValue {
                field: "message.content",
                rule: "ordinary input cannot include thinking or raw JSON",
            },
            "OpenAI ordinary input does not support thinking or JSON content",
        )),
    }
}

fn file_ref_to_input_part(file: &FileReference) -> Result<Value, SdkError> {
    match file {
        FileReference::Provider { id, .. } => Ok(json!({"type": "input_file", "file_id": id})),
        FileReference::Url { url, .. } => {
            Ok(json!({"type": "input_file", "file_url": url.to_string()}))
        }
    }
}
