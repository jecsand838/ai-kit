use super::{citations::content_items_to_citations, hosted};
use serde::Serialize;
use serde_json::Value;
use url::Url;

use crate::SdkError;
use crate::core::{
    GenerateResponseOptions, NormalizedGenerationParts, ensure_nonempty_generation_parts,
    file_reference_part, generate_response, inline_media_part, push_text_part, tool_call,
};
use crate::error::NativeErrorDetails;
use crate::error::{ErrorPhase, ResponseDetails, ResponseViolation};
use crate::generation::{
    ContentPart, FinishReason, GenerateResponse, GenerationContinuation, Usage,
};
use crate::models::ModelId;
use crate::transport::CheckedRequestSpec;

use super::super::wire::{OpenAiContentItem, OpenAiOutputItem, OpenAiResponseObject, OpenAiUsage};

pub(crate) fn normalize_generate_response(
    selected_model: Option<&ModelId>,
    raw: &Value,
) -> Result<GenerateResponse, SdkError> {
    let response = serde_json::from_value::<OpenAiResponseObject>(raw.clone()).map_err(|err| {
        SdkError::invalid_response(ResponseViolation::InvalidField { field: "response" })
            .with_response_details(ResponseDetails::json(&err))
            .at_phase(ErrorPhase::Decode)
            .with_message(format!("failed to decode OpenAI Responses object: {err}"))
            .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw))
    })?;
    if response.status.as_deref() == Some("failed") {
        let error = response.error.as_ref().ok_or_else(|| {
            SdkError::invalid_response(ResponseViolation::MissingField { field: "error" })
                .at_phase(ErrorPhase::Normalize)
                .with_message("failed OpenAI response did not include error detail".to_string())
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw))
        })?;
        return Err(error.clone().into_sdk_error(
            ErrorPhase::Normalize,
            CheckedRequestSpec::diagnostic_json(raw),
        ));
    }

    let mut parts = NormalizedGenerationParts::default();
    let mut saw_refusal = false;

    for item in &response.output {
        match item.kind.as_str() {
            "message" => {
                if item.role.as_deref() != Some("assistant") {
                    return Err(SdkError::invalid_response(ResponseViolation::InvalidField {
                        field: "output.role",
                    })
                    .at_phase(ErrorPhase::Normalize)
                    .with_message(
                        "OpenAI response message did not use role `assistant`".to_string(),
                    )
                    .with_diagnostic(Some(openai_body("OpenAI response output item", item)?)));
                }
                let content = match item.content.as_deref() {
                    Some(content) => content,
                    None => {
                        return Err(SdkError::invalid_response(ResponseViolation::MissingField {
                            field: "output.content",
                        })
                        .at_phase(ErrorPhase::Normalize)
                        .with_message(
                            "OpenAI message output item did not include content".to_string(),
                        )
                        .with_diagnostic(Some(openai_body(
                            "OpenAI response message output item",
                            item,
                        )?)));
                    }
                };
                saw_refusal |= content.iter().any(|item| item.kind == "refusal");
                parts.content.extend(content_items_to_parts(content)?);
                parts.extend_citations(content_items_to_citations(content)?);
            }
            "function_call" => {
                parts.function_calls.push(tool_call(
                    Some(tool_call_id(item)?),
                    item.name.clone(),
                    decode_function_call_arguments(
                        item.arguments.as_deref(),
                        "OpenAI tool call",
                        Some(openai_body("OpenAI tool call item", item)?),
                    )?,
                )?);
            }
            "custom_tool_call" | "mcp_call" | "shell_call" | "shell_call_output" => {
                return Err(
                    SdkError::invalid_response(ResponseViolation::UnknownVariant {
                        field: "output.type",
                    })
                    .at_phase(ErrorPhase::Normalize)
                    .with_message(format!(
                        "OpenAI returned undeclared native output type `{}`",
                        item.kind
                    ))
                    .with_diagnostic(Some(openai_body("OpenAI response output item", item)?)),
                );
            }
            "reasoning" => {
                parts.content.push(ContentPart::Thinking {
                    text: reasoning_item_text(item)?,
                });
            }
            "web_search_call" => {
                parts.push_hosted_output(hosted::OpenAiHostedOutput::new(item).web_search()?);
            }
            "file_search_call" => {
                parts.push_hosted_output(hosted::OpenAiHostedOutput::new(item).file_search()?);
            }
            "code_interpreter_call" => {
                parts.push_hosted_output(hosted::OpenAiHostedOutput::new(item).code_interpreter()?);
            }
            "image_generation_call" => {
                return Err(
                    SdkError::invalid_response(ResponseViolation::ProtocolViolation {
                        rule: "declared_hosted_tools",
                    })
                    .at_phase(ErrorPhase::Normalize)
                    .with_message(
                        "OpenAI returned image generation for an undeclared portable tool"
                            .to_string(),
                    )
                    .with_diagnostic(Some(openai_body("OpenAI image generation item", item)?)),
                );
            }
            other => {
                return Err(
                    SdkError::invalid_response(ResponseViolation::UnknownVariant {
                        field: "output.type",
                    })
                    .at_phase(ErrorPhase::Normalize)
                    .with_message(format!(
                        "OpenAI response output item type `{other}` is not supported"
                    ))
                    .with_diagnostic(Some(openai_body("OpenAI response output item", item)?)),
                );
            }
        }
    }

    let finish_reason = finish_reason_from_status(
        response.status.as_deref(),
        response.incomplete_details.as_ref(),
        !parts.function_calls.is_empty(),
        saw_refusal,
    )?;
    ensure_nonempty_generation_parts(&parts, CheckedRequestSpec::diagnostic_json(raw))?;
    let usage = usage_from_openai(response.usage.as_ref());
    let output_items = raw
        .get("output")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let model = ModelId::new(response.model.clone()).map_err(|_| {
        SdkError::invalid_response(ResponseViolation::InvalidField { field: "model" })
            .at_phase(ErrorPhase::Normalize)
            .with_message("OpenAI response contained an invalid model id".to_string())
            .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw))
    })?;
    let mut normalized = generate_response(
        model.clone(),
        parts,
        GenerateResponseOptions {
            finish_reason,
            usage,
        },
    );
    normalized.message = normalized
        .message
        .with_continuation(GenerationContinuation::open_ai(
            selected_model.cloned().unwrap_or(model),
            output_items,
        ))?;
    Ok(normalized)
}

pub(crate) fn usage_from_openai(usage: Option<&OpenAiUsage>) -> Usage {
    let Some(usage) = usage else {
        return Usage::default();
    };
    Usage::reported_tokens(usage.input_tokens, usage.output_tokens)
        .with_reported_total(usage.total_tokens)
        .with_reasoning(
            usage
                .output_tokens_details
                .as_ref()
                .and_then(|details| details.reasoning_tokens)
                .or_else(|| {
                    usage
                        .input_tokens_details
                        .as_ref()
                        .and_then(|details| details.reasoning_tokens)
                }),
        )
        .with_cached_input(
            usage
                .input_tokens_details
                .as_ref()
                .and_then(|details| details.cached_tokens),
        )
}

fn finish_reason_from_status(
    status: Option<&str>,
    incomplete: Option<&Value>,
    has_tool_calls: bool,
    has_refusal: bool,
) -> Result<Option<FinishReason>, SdkError> {
    match status.ok_or_else(|| {
        SdkError::invalid_response(ResponseViolation::MissingField { field: "status" })
            .at_phase(ErrorPhase::Normalize)
            .with_message("OpenAI response did not include status".to_string())
            .with_diagnostic(incomplete.and_then(CheckedRequestSpec::diagnostic_json))
    })? {
        "completed" if has_tool_calls => Ok(Some(FinishReason::ToolCall)),
        "completed" if has_refusal => Ok(Some(FinishReason::ContentFilter)),
        "completed" => Ok(Some(FinishReason::Stop)),
        "incomplete" => {
            let reason = incomplete
                .and_then(|details| details.get("reason"))
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    SdkError::invalid_response(ResponseViolation::MissingField {
                        field: "incomplete_details.reason",
                    })
                    .at_phase(ErrorPhase::Normalize)
                    .with_message(
                        "OpenAI incomplete response did not include incomplete_details.reason"
                            .to_string(),
                    )
                    .with_diagnostic(incomplete.and_then(CheckedRequestSpec::diagnostic_json))
                })?;
            match reason {
                "max_output_tokens" => Ok(Some(FinishReason::Length)),
                "content_filter" => Ok(Some(FinishReason::ContentFilter)),
                other => Err(
                    SdkError::invalid_response(ResponseViolation::UnknownVariant {
                        field: "incomplete_details.reason",
                    })
                    .at_phase(ErrorPhase::Normalize)
                    .with_message(format!(
                        "OpenAI returned unknown incomplete reason `{other}`"
                    ))
                    .with_diagnostic(incomplete.and_then(CheckedRequestSpec::diagnostic_json)),
                ),
            }
        }
        "failed" => Err(
            SdkError::invalid_response(ResponseViolation::ProtocolViolation {
                rule: "native_error_normalization",
            })
            .at_phase(ErrorPhase::Normalize)
            .with_message("failed OpenAI response bypassed native error normalization".to_string())
            .with_diagnostic(incomplete.and_then(CheckedRequestSpec::diagnostic_json)),
        ),
        other => {
            Err(
                SdkError::invalid_response(ResponseViolation::InvalidField { field: "status" })
                    .at_phase(ErrorPhase::Normalize)
                    .with_message(format!(
                        "OpenAI returned impossible synchronous response status `{other}`"
                    ))
                    .with_diagnostic(incomplete.and_then(CheckedRequestSpec::diagnostic_json)),
            )
        }
    }
}

fn content_items_to_parts(items: &[OpenAiContentItem]) -> Result<Vec<ContentPart>, SdkError> {
    let mut parts = Vec::new();
    for item in items {
        match item.kind.as_str() {
            "output_text" => {
                push_text_part(&mut parts, item.text.clone());
            }
            "refusal" => {
                let Some(refusal) = item.refusal.clone() else {
                    return Err(SdkError::invalid_response(ResponseViolation::MissingField {
                        field: "refusal",
                    })
                    .at_phase(ErrorPhase::Normalize)
                    .with_message(
                        "OpenAI refusal content item did not include refusal text".to_string(),
                    )
                    .with_diagnostic(Some(openai_body("OpenAI response content item", item)?)));
                };
                push_text_part(&mut parts, Some(refusal));
            }
            "output_image" => {
                let data = match item.data.clone() {
                    Some(data) => data,
                    None => {
                        return Err(SdkError::invalid_response(ResponseViolation::MissingField {
                            field: "data",
                        })
                        .at_phase(ErrorPhase::Normalize)
                        .with_message("OpenAI output_image item did not include data".to_string())
                        .with_diagnostic(Some(openai_body("OpenAI output_image item", item)?)));
                    }
                };
                let media_type = match item.media_type.clone() {
                    Some(media_type) => media_type,
                    None => {
                        return Err(SdkError::invalid_response(ResponseViolation::MissingField {
                            field: "media_type",
                        })
                        .at_phase(ErrorPhase::Normalize)
                        .with_message(
                            "OpenAI output_image item did not include media_type".to_string(),
                        )
                        .with_diagnostic(Some(openai_body("OpenAI output_image item", item)?)));
                    }
                };
                parts.push(inline_media_part(media_type, data)?);
            }
            "file" => {
                parts.push(file_reference_part(
                    item.file_id.clone(),
                    None,
                    item.media_type.clone(),
                    item.file_url
                        .as_ref()
                        .and_then(|value| Url::parse(value).ok()),
                )?);
            }
            other => {
                return Err(
                    SdkError::invalid_response(ResponseViolation::UnknownVariant {
                        field: "content.type",
                    })
                    .at_phase(ErrorPhase::Normalize)
                    .with_message(format!(
                        "OpenAI response content item type `{other}` is not supported"
                    ))
                    .with_diagnostic(Some(openai_body("OpenAI response content item", item)?)),
                );
            }
        }
    }
    Ok(parts)
}

fn reasoning_item_text(item: &OpenAiOutputItem) -> Result<Option<String>, SdkError> {
    let mut fragments = Vec::new();

    if let Some(summary) = item.extra.get("summary") {
        let Some(summary) = summary.as_array() else {
            return Err(SdkError::invalid_response(ResponseViolation::InvalidField {
                field: "summary",
            })
            .at_phase(ErrorPhase::Normalize)
            .with_message("OpenAI reasoning output item summary was not an array".to_string())
            .with_diagnostic(Some(openai_body("OpenAI reasoning output item", item)?)));
        };
        for summary_item in summary {
            let Some(kind) = summary_item.get("type").and_then(Value::as_str) else {
                return Err(SdkError::invalid_response(ResponseViolation::MissingField {
                    field: "summary.type",
                })
                .at_phase(ErrorPhase::Normalize)
                .with_message("OpenAI reasoning summary item did not include type".to_string())
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(summary_item)));
            };
            if kind != "summary_text" {
                return Err(
                    SdkError::invalid_response(ResponseViolation::UnknownVariant {
                        field: "summary.type",
                    })
                    .at_phase(ErrorPhase::Normalize)
                    .with_message(format!(
                        "OpenAI reasoning summary item type `{kind}` is not supported"
                    ))
                    .with_diagnostic(CheckedRequestSpec::diagnostic_json(summary_item)),
                );
            }
            let Some(text) = summary_item.get("text").and_then(Value::as_str) else {
                return Err(SdkError::invalid_response(ResponseViolation::MissingField {
                    field: "summary.text",
                })
                .at_phase(ErrorPhase::Normalize)
                .with_message("OpenAI reasoning summary item did not include text".to_string())
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(summary_item)));
            };
            if !text.trim().is_empty() {
                fragments.push(text.to_string());
            }
        }
    }

    if let Some(content) = item.content.as_deref() {
        for content_item in content {
            match content_item.kind.as_str() {
                "reasoning_text" => {
                    let Some(text) = content_item.text.as_deref() else {
                        return Err(SdkError::invalid_response(ResponseViolation::MissingField {
                            field: "reasoning_text.text",
                        })
                        .at_phase(ErrorPhase::Normalize)
                        .with_message("OpenAI reasoning_text item did not include text".to_string())
                        .with_diagnostic(Some(openai_body(
                            "OpenAI reasoning content item",
                            content_item,
                        )?)));
                    };
                    if !text.trim().is_empty() {
                        fragments.push(text.to_string());
                    }
                }
                other => {
                    return Err(
                        SdkError::invalid_response(ResponseViolation::UnknownVariant {
                            field: "reasoning_content.type",
                        })
                        .at_phase(ErrorPhase::Normalize)
                        .with_message(format!(
                            "OpenAI reasoning content item type `{other}` is not supported"
                        ))
                        .with_diagnostic(Some(openai_body(
                            "OpenAI reasoning content item",
                            content_item,
                        )?)),
                    );
                }
            }
        }
    }

    Ok((!fragments.is_empty()).then(|| fragments.join("\n")))
}

fn tool_call_id(item: &OpenAiOutputItem) -> Result<String, SdkError> {
    match item
        .call_id
        .clone()
        .filter(|call_id| !call_id.trim().is_empty())
    {
        Some(call_id) => Ok(call_id),
        None => {
            Err(
                SdkError::invalid_response(ResponseViolation::MissingField { field: "call_id" })
                    .at_phase(ErrorPhase::Normalize)
                    .with_message(format!("OpenAI {} item did not include call_id", item.kind))
                    .with_diagnostic(Some(openai_body("OpenAI tool call item", item)?)),
            )
        }
    }
}

pub(crate) fn decode_function_call_arguments(
    arguments: Option<&str>,
    context: &'static str,
    missing_body: Option<NativeErrorDetails>,
) -> Result<Value, SdkError> {
    let Some(arguments) = arguments else {
        return Err(SdkError::invalid_response(ResponseViolation::MissingField {
            field: "arguments",
        })
        .at_phase(ErrorPhase::Normalize)
        .with_message(format!("{context} did not include arguments"))
        .with_diagnostic(missing_body));
    };
    serde_json::from_str::<Value>(arguments).map_err(|err| {
        SdkError::invalid_response(ResponseViolation::MalformedJson)
            .with_response_details(ResponseDetails::json(&err))
            .at_phase(ErrorPhase::Decode)
            .with_message(format!("{context} arguments were not valid JSON: {err}"))
            .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(
                arguments.as_bytes(),
            )))
    })
}

fn openai_body<T: Serialize>(
    target: &'static str,
    value: &T,
) -> Result<NativeErrorDetails, SdkError> {
    CheckedRequestSpec::diagnostic_json(value).ok_or_else(|| {
        SdkError::invalid_response(ResponseViolation::ProtocolViolation {
            rule: "diagnostic_serialization",
        })
        .at_phase(ErrorPhase::Normalize)
        .with_message(format!("failed to preserve {target} for diagnostics"))
        .with_diagnostic(None)
    })
}
