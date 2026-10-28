use serde_json::Value;

use crate::KitError;
use crate::core::{
    FrameDecoder, GenerateResponseOptions, NormalizedGenerationParts, StreamFrame,
    StreamingAccumulator, ensure_nonempty_generation_parts, generate_response,
    push_provider_thinking_content,
};
use crate::error::{
    ClassificationBasis, ErrorKind, ErrorPhase, JsonValueKind, NativeErrorDetails, Operation,
    RemoteFailure, ResponseDetails, ResponseViolation,
};
use crate::generation::GenerationContinuation;
use crate::generation::{GenerateEvent, GenerateEventStream, GenerateResponse};
use crate::models::ModelId;
use crate::transport::CheckedRequestSpec;

use super::api::{OLLAMA_REQUESTS, OllamaApiContext};
use super::chat::allows_empty_generation;
use super::native::tool_calling::tool_calls_from_wire;
use super::wire::OllamaChatResponse;

#[derive(Debug)]
pub(crate) struct OllamaStreamingAccumulator {
    pub(crate) shared: StreamingAccumulator,
    pub(crate) thinking: String,
}

impl OllamaStreamingAccumulator {
    pub(crate) fn new(model: ModelId) -> Self {
        Self {
            shared: StreamingAccumulator::with_model(model),
            thinking: String::new(),
        }
    }
}

pub(crate) async fn start_generate_stream(
    api: OllamaApiContext,
    body: Value,
) -> Result<GenerateEventStream, KitError> {
    let model = ModelId::new(
        body.get("model")
            .and_then(Value::as_str)
            .ok_or_else(|| KitError::invalid_state("Ollama stream request lost its model id"))?,
    )?;
    let execution = api.transport().execution();
    let timeout = execution.timeout();
    let response = OLLAMA_REQUESTS
        .response(
            "chat",
            api.request(reqwest::Method::POST, "chat")?.json(&body),
            execution,
        )
        .await?;
    let mut accumulator = OllamaStreamingAccumulator::new(model);
    FrameDecoder::default().into_stream(
        response,
        "ollama generation stream",
        timeout,
        move |frame| {
            let line = match frame {
                StreamFrame::Line(line) => line,
                StreamFrame::End => return Ok(Vec::new()),
                StreamFrame::Sse(_) => {
                    return Err(
                        KitError::invalid_response(ResponseViolation::ProtocolViolation {
                            rule: "ndjson_frame",
                        })
                        .at_phase(ErrorPhase::Stream)
                        .with_message("Ollama streaming received a non-line frame".to_string())
                        .with_diagnostic(None),
                    );
                }
            };
            accumulator
                .normalize(&line)
                .map_err(|error| error.at_operation(Operation::GenerateStream))
        },
    )
}

impl OllamaStreamingAccumulator {
    fn normalize(&mut self, line: &str) -> Result<Vec<GenerateEvent>, KitError> {
        let payload = serde_json::from_str::<Value>(line).map_err(|err| {
            KitError::invalid_response(ResponseViolation::MalformedJson)
                .with_response_details(ResponseDetails::json(&err))
                .at_phase(ErrorPhase::Decode)
                .with_message(format!(
                    "failed to decode Ollama streamed chat payload: {err}"
                ))
                .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(line.as_bytes())))
        })?;
        if let Some(error) = payload.get("error") {
            let message = error
                .as_str()
                .filter(|message| !message.trim().is_empty())
                .ok_or_else(|| {
                    KitError::invalid_response(ResponseViolation::InvalidField { field: "error" })
                        .with_response_details(ResponseDetails::FieldType {
                            expected: JsonValueKind::String,
                            actual: JsonValueKind::of(Some(error)),
                        })
                        .at_phase(ErrorPhase::Normalize)
                        .with_message(
                            "Ollama stream error envelope did not include a nonblank message"
                                .to_string(),
                        )
                        .with_diagnostic(CheckedRequestSpec::diagnostic_json(&payload))
                })?;
            let failure = RemoteFailure::from_native(
                ErrorKind::ProviderRejected,
                ClassificationBasis::Unclassified,
                NativeErrorDetails {
                    message: Some(message.to_string()),
                    ..CheckedRequestSpec::diagnostic_json(&payload).unwrap_or_default()
                },
            )?;
            return Err(KitError::from_remote(
                failure,
                Operation::GenerateStream,
                ErrorPhase::Stream,
            ));
        }
        let mut decoded = serde_json::from_value::<OllamaChatResponse>(payload).map_err(|err| {
            KitError::invalid_response(ResponseViolation::InvalidField { field: "response" })
                .with_response_details(ResponseDetails::json(&err))
                .at_phase(ErrorPhase::Decode)
                .with_message(format!(
                    "failed to decode Ollama streamed chat payload: {err}"
                ))
                .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(line.as_bytes())))
        })?;
        let model = self.shared.model()?.clone();
        if decoded.model != model.as_str() {
            return Err(
                KitError::invalid_response(ResponseViolation::IdentityMismatch { field: "model" })
                    .with_response_details(ResponseDetails::Identity {
                        expected: model.to_string(),
                        actual: decoded.model.clone(),
                    })
                    .at_phase(ErrorPhase::Normalize)
                    .with_message(format!(
                        "Ollama stream returned model `{}` for selected model `{model}`",
                        decoded.model
                    ))
                    .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(line.as_bytes()))),
            );
        }
        if !decoded.done && decoded.done_reason.is_some() {
            return Err(
                KitError::invalid_response(ResponseViolation::ProtocolViolation {
                    rule: "terminal_done_reason",
                })
                .at_phase(ErrorPhase::Normalize)
                .with_message("Ollama nonterminal stream frame included done_reason".to_string())
                .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(line.as_bytes()))),
            );
        }
        let started = self.shared.started_event(model);
        let mut events = Vec::new();
        if let Some(started) = started {
            events.push(started);
        }

        if let Some(message) = decoded.message.take() {
            if message.role.as_deref() != Some("assistant") {
                return Err(KitError::invalid_response(ResponseViolation::InvalidField {
                    field: "message.role",
                })
                .with_response_details(match &message.role {
                    Some(role) => ResponseDetails::Discriminator {
                        actual: role.clone(),
                    },
                    None => ResponseDetails::FieldType {
                        expected: JsonValueKind::String,
                        actual: JsonValueKind::Absent,
                    },
                })
                .at_phase(ErrorPhase::Normalize)
                .with_message(
                    "Ollama streamed chat message did not use the assistant role".to_string(),
                )
                .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(line.as_bytes()))));
            }
            if !message.images.is_empty() {
                return Err(
                    KitError::invalid_response(ResponseViolation::ProtocolViolation {
                        rule: "supported_output_modality",
                    })
                    .at_phase(ErrorPhase::Normalize)
                    .with_message(
                        "Ollama streamed chat returned unsupported image output".to_string(),
                    )
                    .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(line.as_bytes()))),
                );
            }
            if !message.content.is_empty() {
                let delta = message.content;
                if let Some(event) = self.shared.push_text(&delta)? {
                    events.push(event);
                }
            }
            if let Some(thinking) = message.thinking
                && !thinking.is_empty()
            {
                self.shared.retain(thinking.len(), 0)?;
                self.thinking.push_str(&thinking);
                events.push(GenerateEvent::ThinkingDelta { delta: thinking });
            }
            if !message.tool_calls.is_empty() {
                let tool_calls = tool_calls_from_wire(&message.tool_calls)?;
                for call in tool_calls {
                    self.shared.push_call(call)?;
                }
            }
        }

        let usage = (decoded.prompt_eval_count.is_some()
            || decoded.eval_count.is_some()
            || decoded.prompt_eval_cached_count.is_some())
        .then(|| decoded.usage());
        if let Some(usage) = &usage {
            self.shared.set_usage(usage.clone());
        }

        if decoded.done {
            if let Some(usage) = usage {
                events.push(GenerateEvent::Usage(usage));
            }
            let response = self.finish(&decoded)?;
            events.push(GenerateEvent::Completed(Box::new(response)));
            return Ok(events);
        }

        if let Some(usage) = usage {
            events.push(GenerateEvent::Usage(usage));
        }

        Ok(events)
    }

    fn finish(&mut self, terminal: &OllamaChatResponse) -> Result<GenerateResponse, KitError> {
        let thinking = std::mem::take(&mut self.thinking);
        let mut content = Vec::new();
        push_provider_thinking_content(&mut content, Some(thinking.clone()));
        let (text, tool_calls) = self.shared.take_output();
        content.extend(text);
        let finish_reason = terminal.finish_reason(&tool_calls)?;
        let parts = NormalizedGenerationParts {
            content,
            function_calls: tool_calls,
            ..Default::default()
        };
        if !allows_empty_generation(finish_reason.as_ref()) {
            ensure_nonempty_generation_parts(&parts, None)?;
        }

        let model_id = self.shared.model()?.clone();
        let mut response = generate_response(
            model_id.clone(),
            parts,
            GenerateResponseOptions {
                finish_reason,
                usage: self.shared.usage().clone(),
            },
        );
        if !thinking.is_empty() {
            response.message = response
                .message
                .with_continuation(GenerationContinuation::ollama(model_id, thinking))?;
        }
        Ok(response)
    }
}
