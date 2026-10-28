use serde_json::Value;

use crate::KitError;
use crate::core::{
    GenerateResponseOptions, NormalizedGenerationParts, ensure_nonempty_generation_parts,
    generate_response, push_provider_thinking_content, push_text_part,
};
use crate::error::{ErrorPhase, ResponseDetails, ResponseViolation};
use crate::generation::{
    ContentPart, FinishReason, FunctionCall, GenerateResponse, GenerationContinuation, Usage,
};
use crate::models::ModelId;
use crate::transport::CheckedRequestSpec;

use super::super::native::tool_calling::tool_calls_from_wire;
use super::super::wire::{OllamaChatResponse, OllamaMessage};

// Ollama response normalization stays provider-local. Do not synthesize usage,
// tool calls, or thinking text when the provider omits fields. Sources:
// https://docs.ollama.com/api/chat,
// https://docs.ollama.com/capabilities/tool-calling
pub(crate) fn normalize_chat_response(
    raw: &Value,
    model_id: ModelId,
) -> Result<GenerateResponse, KitError> {
    let decoded = serde_json::from_value::<OllamaChatResponse>(raw.clone()).map_err(|err| {
        KitError::invalid_response(ResponseViolation::InvalidField { field: "response" })
            .with_response_details(ResponseDetails::json(&err))
            .at_phase(ErrorPhase::Decode)
            .with_message(format!("failed to decode Ollama chat response: {err}"))
            .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw))
    })?;
    if !decoded.done {
        return Err(
            KitError::invalid_response(ResponseViolation::ProtocolViolation {
                rule: "terminal_response",
            })
            .at_phase(ErrorPhase::Normalize)
            .with_message("Ollama non-streaming chat response was not terminal".to_string())
            .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw)),
        );
    }
    if decoded.model != model_id.as_str() {
        return Err(
            KitError::invalid_response(ResponseViolation::IdentityMismatch { field: "model" })
                .at_phase(ErrorPhase::Normalize)
                .with_message(format!(
                    "Ollama returned model `{}` for selected model `{model_id}`",
                    decoded.model
                ))
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw)),
        );
    }
    let message = decoded.message.as_ref().ok_or_else(|| {
        KitError::invalid_response(ResponseViolation::MissingField { field: "message" })
            .at_phase(ErrorPhase::Normalize)
            .with_message("Ollama chat response did not include message".to_string())
            .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw))
    })?;
    if message.role.as_deref() != Some("assistant") {
        return Err(KitError::invalid_response(ResponseViolation::InvalidField {
            field: "message.role",
        })
        .at_phase(ErrorPhase::Normalize)
        .with_message("Ollama chat response did not use the assistant role".to_string())
        .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw)));
    }
    if !message.images.is_empty() {
        return Err(
            KitError::invalid_response(ResponseViolation::ProtocolViolation {
                rule: "supported_output_modality",
            })
            .at_phase(ErrorPhase::Normalize)
            .with_message("Ollama chat response returned unsupported image output".to_string())
            .with_diagnostic(CheckedRequestSpec::diagnostic_json(raw)),
        );
    }
    let tool_calls = tool_calls_from_wire(&message.tool_calls)?;
    let content = message_to_content_parts(message);
    let finish_reason = decoded.finish_reason(&tool_calls)?;
    let parts = NormalizedGenerationParts {
        content,
        function_calls: tool_calls,
        ..Default::default()
    };
    if !allows_empty_generation(finish_reason.as_ref()) {
        ensure_nonempty_generation_parts(&parts, None)?;
    }
    let usage = decoded.usage();

    let mut response = generate_response(
        model_id.clone(),
        parts,
        GenerateResponseOptions {
            finish_reason,
            usage,
        },
    );
    if let Some(thinking) = message.thinking.as_ref().filter(|value| !value.is_empty()) {
        response.message = response
            .message
            .with_continuation(GenerationContinuation::ollama(model_id, thinking.clone()))?;
    }
    Ok(response)
}

pub(crate) fn allows_empty_generation(reason: Option<&FinishReason>) -> bool {
    matches!(reason, Some(FinishReason::Length | FinishReason::Error))
}

impl OllamaChatResponse {
    pub(crate) fn usage(&self) -> Usage {
        Usage::reported_tokens(self.prompt_eval_count, self.eval_count)
            .with_cached_input(self.prompt_eval_cached_count)
    }

    pub(crate) fn finish_reason(
        &self,
        tool_calls: &[FunctionCall],
    ) -> Result<Option<FinishReason>, KitError> {
        let reason = match self.done_reason.as_deref() {
            Some("stop") if !tool_calls.is_empty() => FinishReason::ToolCall,
            Some("stop") => FinishReason::Stop,
            Some("length") => FinishReason::Length,
            Some("error") => FinishReason::Error,
            Some(other) => {
                return Err(
                    KitError::invalid_response(ResponseViolation::UnknownVariant {
                        field: "done_reason",
                    })
                    .with_response_details(ResponseDetails::Discriminator {
                        actual: other.to_owned(),
                    })
                    .with_message(format!(
                        "Ollama returned unknown chat done_reason `{other}`"
                    )),
                );
            }
            None => {
                return Err(KitError::invalid_response(ResponseViolation::MissingField {
                    field: "done_reason",
                })
                .with_message("Ollama terminal chat response omitted done_reason"));
            }
        };
        Ok(Some(reason))
    }
}

fn message_to_content_parts(message: &OllamaMessage) -> Vec<ContentPart> {
    let mut content = Vec::new();
    push_text_part(&mut content, Some(message.content.clone()));
    push_provider_thinking_content(&mut content, message.thinking.clone());
    content
}
