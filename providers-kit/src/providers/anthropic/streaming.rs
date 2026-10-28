use std::collections::BTreeMap;

use reqwest::Method;
use serde_json::Value;

use crate::KitError;
use crate::core::{
    FrameDecoder, GenerateResponseOptions, StreamFrame, StreamingAccumulator,
    ensure_nonempty_generation_parts, generate_response,
};
use crate::error::{
    ErrorKind, ErrorPhase, JsonValueKind, Operation, ResponseDetails, ResponseViolation,
};
use crate::generation::{
    GenerateEvent, GenerateEventStream, GenerateResponse, GenerationContinuation,
};
use crate::models::ModelId;
use crate::transport::{CheckedRequestSpec, SseEvent};

use super::api::{ANTHROPIC_REQUESTS, AnthropicApiContext};
use super::messages::{blocks_to_portable, stop_reason_to_finish_reason, usage_from_anthropic};
use super::wire::{
    AnthropicCitation, AnthropicContentBlock, AnthropicErrorEnvelope, AnthropicMessageResponse,
};

#[derive(Debug, Default)]
pub(crate) struct AnthropicStreamingAccumulator {
    base: StreamingAccumulator,
    content: Vec<AnthropicContentBlock>,
    raw_content: Vec<Value>,
    tool_json_fragments: BTreeMap<usize, String>,
    raw_usage: Option<super::wire::AnthropicUsage>,
    stop_reason: Option<String>,
}

impl AnthropicStreamingAccumulator {
    fn invalid(reason: ResponseViolation, message: impl Into<String>) -> KitError {
        KitError::new(
            ErrorKind::InvalidResponse(reason),
            Operation::GenerateStream,
            ErrorPhase::Stream,
        )
        .with_message(message)
    }

    fn invalid_body(reason: ResponseViolation, message: impl Into<String>, body: &str) -> KitError {
        Self::invalid(reason, message)
            .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(body.as_bytes())))
    }

    fn replace_content(
        &mut self,
        content: Vec<AnthropicContentBlock>,
        raw_content: Vec<Value>,
    ) -> Result<(), KitError> {
        if content.len() != raw_content.len() {
            return Err(Self::invalid(
                ResponseViolation::ProtocolViolation {
                    rule: "message_start must preserve content block count",
                },
                "Anthropic message_start content did not preserve its wire block count",
            ));
        }
        let bytes = raw_content
            .iter()
            .map(Value::to_string)
            .map(|value| value.len())
            .sum();
        self.base.retain(bytes, raw_content.len())?;
        self.content = content;
        self.raw_content = raw_content;
        Ok(())
    }

    fn record_block(
        &mut self,
        index: usize,
        block: AnthropicContentBlock,
        raw: Value,
    ) -> Result<(), KitError> {
        if index > self.content.len() || index > self.raw_content.len() {
            return Err(Self::invalid(
                ResponseViolation::ProtocolViolation {
                    rule: "content blocks must start in sequence",
                },
                format!("Anthropic stream started content block index {index} out of sequence"),
            )
            .with_response_details(ResponseDetails::Index {
                index,
                available: self.content.len().min(self.raw_content.len()),
            }));
        }
        self.base.retain(raw.to_string().len(), 1)?;
        if index == self.content.len() {
            self.content.push(block);
        } else {
            self.content[index] = block;
        }
        if index == self.raw_content.len() {
            self.raw_content.push(raw);
        } else {
            self.raw_content[index] = raw;
        }
        Ok(())
    }

    fn append_raw_string(
        &mut self,
        index: usize,
        field: &'static str,
        delta: &str,
    ) -> Result<(), KitError> {
        let block = self
            .raw_content
            .get_mut(index)
            .and_then(Value::as_object_mut)
            .ok_or_else(|| {
                Self::invalid(
                    ResponseViolation::InvalidField {
                        field: "content_block",
                    },
                    format!("Anthropic streamed content block {index} was not a JSON object"),
                )
            })?;
        let mut value = match block.get(field) {
            Some(Value::String(value)) => value.clone(),
            Some(value) => {
                return Err(Self::invalid(
                    ResponseViolation::InvalidField { field },
                    format!(
                        "Anthropic streamed content block {index} field `{field}` was not a string: {value}"
                    ),
                ));
            }
            None => String::new(),
        };
        value.push_str(delta);
        block.insert(field.to_string(), Value::String(value));
        Ok(())
    }

    fn append_citation(&mut self, index: usize, raw: Value) -> Result<(), KitError> {
        let block = &mut self.content[index];
        if block.kind != "text" {
            return Err(Self::invalid(
                ResponseViolation::ProtocolViolation {
                    rule: "citation deltas require a text block",
                },
                "Anthropic citation delta targeted a non-text content block",
            ));
        }
        if !raw.is_object() {
            return Err(Self::invalid(
                ResponseViolation::InvalidField {
                    field: "delta.citation",
                },
                "Anthropic citation delta requires a citation object",
            )
            .with_response_details(ResponseDetails::FieldType {
                expected: JsonValueKind::Object,
                actual: JsonValueKind::of(Some(&raw)),
            })
            .with_diagnostic(CheckedRequestSpec::diagnostic_json(&raw)));
        }
        AnthropicCitation::decode(&raw).map_err(|error| {
            error
                .at_phase(ErrorPhase::Stream)
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(&raw))
        })?;
        let raw_block = self
            .raw_content
            .get_mut(index)
            .and_then(Value::as_object_mut)
            .ok_or_else(|| {
                Self::invalid(
                    ResponseViolation::InvalidField {
                        field: "content_block",
                    },
                    "Anthropic citation delta requires its original content block",
                )
            })?;
        if !matches!(
            raw_block.get("citations"),
            None | Some(Value::Null | Value::Array(_))
        ) {
            return Err(Self::invalid(
                ResponseViolation::InvalidField {
                    field: "content_block.citations",
                },
                "Anthropic text block citations must be an array or null",
            ));
        }
        self.base.retain(raw.to_string().len(), 1)?;
        block.citations.get_or_insert_default().push(raw.clone());
        if let Some(Value::Array(citations)) = raw_block.get_mut("citations") {
            citations.push(raw);
        } else {
            raw_block.insert("citations".to_owned(), Value::Array(vec![raw]));
        }
        Ok(())
    }

    fn set_raw_value(
        &mut self,
        index: usize,
        field: &'static str,
        value: Value,
    ) -> Result<(), KitError> {
        let block = self
            .raw_content
            .get_mut(index)
            .and_then(Value::as_object_mut)
            .ok_or_else(|| {
                Self::invalid(
                    ResponseViolation::InvalidField {
                        field: "content_block",
                    },
                    format!("Anthropic streamed content block {index} was not a JSON object"),
                )
            })?;
        block.insert(field.to_string(), value);
        Ok(())
    }
}

pub(crate) async fn start_generate_stream(
    api: AnthropicApiContext,
    selected_model: ModelId,
    body: Value,
) -> Result<GenerateEventStream, KitError> {
    let execution = api.transport().execution();
    let timeout = execution.timeout();
    let response = ANTHROPIC_REQUESTS
        .response(
            "v1/messages",
            api.request(Method::POST, "v1/messages")?.json(&body),
            execution,
        )
        .await?;
    let mut accumulator = AnthropicStreamingAccumulator::default();
    FrameDecoder::sse().into_stream(
        response,
        "anthropic generation stream",
        timeout,
        move |frame| {
            let event = match frame {
                StreamFrame::Sse(event) => event,
                StreamFrame::End => return Ok(Vec::new()),
                StreamFrame::Line(_) => {
                    return Err(AnthropicStreamingAccumulator::invalid(
                        ResponseViolation::ProtocolViolation {
                            rule: "expected SSE frame",
                        },
                        "Anthropic streaming received a non-SSE frame",
                    ));
                }
            };
            accumulator
                .normalize(&selected_model, &event)
                .map(|event| event.into_iter().collect())
        },
    )
}

impl AnthropicStreamingAccumulator {
    fn normalize(
        &mut self,
        selected_model: &ModelId,
        event: &SseEvent,
    ) -> Result<Option<GenerateEvent>, KitError> {
        if event.data.trim().is_empty() {
            return Ok(None);
        }
        let payload = serde_json::from_str::<Value>(&event.data).map_err(|err| {
            AnthropicStreamingAccumulator::invalid_body(
                ResponseViolation::MalformedJson,
                format!("failed to decode Anthropic SSE payload: {err}"),
                &event.data,
            )
            .with_response_details(ResponseDetails::json(&err))
            .at_phase(ErrorPhase::Decode)
        })?;
        let event_type = event.event.as_deref().ok_or_else(|| {
            AnthropicStreamingAccumulator::invalid_body(
                ResponseViolation::MissingField { field: "event" },
                "Anthropic SSE event did not include event type",
                &event.data,
            )
            .with_response_details(ResponseDetails::FieldType {
                expected: JsonValueKind::String,
                actual: JsonValueKind::Absent,
            })
        })?;

        match event_type {
            "ping" => Ok(None),
            "error" => {
                let error = match payload.get("error") {
                    None => Err(ResponseViolation::MissingField { field: "error" }),
                    Some(value) => value
                        .as_object()
                        .ok_or(ResponseViolation::InvalidField { field: "error" }),
                }
                .map_err(|reason| {
                    AnthropicStreamingAccumulator::invalid_body(
                        reason,
                        "Anthropic SSE error event requires an object error",
                        &event.data,
                    )
                    .with_response_details(ResponseDetails::FieldType {
                        expected: JsonValueKind::Object,
                        actual: JsonValueKind::of(payload.get("error")),
                    })
                })?;
                match error.get("message") {
                    None => Err(ResponseViolation::MissingField {
                        field: "error.message",
                    }),
                    Some(value) => value.as_str().ok_or(ResponseViolation::InvalidField {
                        field: "error.message",
                    }),
                }
                .map_err(|reason| {
                    AnthropicStreamingAccumulator::invalid_body(
                        reason,
                        "Anthropic SSE error event requires a string message",
                        &event.data,
                    )
                    .with_response_details(ResponseDetails::FieldType {
                        expected: JsonValueKind::String,
                        actual: JsonValueKind::of(error.get("message")),
                    })
                })?;
                let native = serde_json::from_value::<AnthropicErrorEnvelope>(payload.clone())
                    .map_err(|error| {
                        Self::invalid_body(
                            ResponseViolation::from_json_error(&error, "error"),
                            "failed to decode Anthropic stream error envelope",
                            &event.data,
                        )
                        .with_response_details(ResponseDetails::json(&error))
                        .at_phase(ErrorPhase::Decode)
                    })?;
                let mut diagnostic = CheckedRequestSpec::diagnostic_body(event.data.as_bytes());
                diagnostic.event_type = Some(event_type.to_owned());
                diagnostic.event_id = event.id.clone();
                let failure = native.remote_failure(Some(diagnostic))?;
                Err(KitError::from_remote(
                    failure,
                    Operation::GenerateStream,
                    ErrorPhase::Stream,
                ))
            }
            "message_start" => {
                let message = payload.get("message").ok_or_else(|| {
                    AnthropicStreamingAccumulator::invalid_body(
                        ResponseViolation::MissingField { field: "message" },
                        "Anthropic message_start event did not include message",
                        &event.data,
                    )
                })?;
                let decoded = serde_json::from_value::<AnthropicMessageResponse>(message.clone())
                    .map_err(|err| {
                    AnthropicStreamingAccumulator::invalid(
                        ResponseViolation::InvalidField { field: "message" },
                        format!("failed to decode Anthropic message_start payload: {err}"),
                    )
                    .with_response_details(ResponseDetails::json(&err))
                    .at_phase(ErrorPhase::Decode)
                })?;
                let raw_content = message
                    .get("content")
                    .and_then(Value::as_array)
                    .cloned()
                    .ok_or_else(|| {
                        AnthropicStreamingAccumulator::invalid_body(
                            ResponseViolation::MissingField {
                                field: "message.content",
                            },
                            "Anthropic message_start did not include array content",
                            &event.data,
                        )
                        .with_response_details(
                            ResponseDetails::FieldType {
                                expected: JsonValueKind::Array,
                                actual: JsonValueKind::of(message.get("content")),
                            },
                        )
                    })?;
                self.replace_content(decoded.content, raw_content)?;
                self.raw_usage = decoded.usage.clone();
                self.base
                    .set_usage(usage_from_anthropic(decoded.usage.as_ref()));
                self.stop_reason = decoded.stop_reason.clone();
                let model = ModelId::new(decoded.model).map_err(|_| {
                    AnthropicStreamingAccumulator::invalid(
                        ResponseViolation::InvalidField {
                            field: "message.model",
                        },
                        "Anthropic message_start contained an invalid model id",
                    )
                })?;
                if let Ok(previous) = self.base.model()
                    && previous != &model
                {
                    return Err(AnthropicStreamingAccumulator::invalid(
                        ResponseViolation::IdentityMismatch {
                            field: "message.model",
                        },
                        "Anthropic stream changed model id between message_start events",
                    )
                    .with_response_details(ResponseDetails::Identity {
                        expected: previous.to_string(),
                        actual: model.to_string(),
                    }));
                }
                Ok(self.base.started_event(model))
            }
            "content_block_start" => {
                let index = match payload.get("index") {
                    None => Err(ResponseViolation::MissingField { field: "index" }),
                    Some(value) => value
                        .as_u64()
                        .and_then(|value| usize::try_from(value).ok())
                        .ok_or(ResponseViolation::InvalidField { field: "index" }),
                }
                .map_err(|reason| {
                    AnthropicStreamingAccumulator::invalid_body(
                        reason,
                        "Anthropic content_block_start requires a nonnegative integer index",
                        &event.data,
                    )
                    .with_response_details(ResponseDetails::FieldType {
                        expected: JsonValueKind::Number,
                        actual: JsonValueKind::of(payload.get("index")),
                    })
                })?;
                let block_value = payload.get("content_block").cloned().ok_or_else(|| {
                    AnthropicStreamingAccumulator::invalid_body(
                        ResponseViolation::MissingField {
                            field: "content_block",
                        },
                        "Anthropic content_block_start did not include content_block",
                        &event.data,
                    )
                })?;
                let block = serde_json::from_value::<AnthropicContentBlock>(block_value.clone())
                    .map_err(|err| {
                        AnthropicStreamingAccumulator::invalid(
                            ResponseViolation::InvalidField {
                                field: "content_block",
                            },
                            format!("failed to decode Anthropic content block start: {err}"),
                        )
                        .with_response_details(ResponseDetails::json(&err))
                        .at_phase(ErrorPhase::Decode)
                    })?;
                self.record_block(index, block.clone(), block_value.clone())?;
                if block.kind == "tool_use" {
                    self.tool_json_fragments.entry(index).or_default();
                    match block_value.get("id") {
                        None => Err(ResponseViolation::MissingField {
                            field: "content_block.id",
                        }),
                        Some(value) => value.as_str().ok_or(ResponseViolation::InvalidField {
                            field: "content_block.id",
                        }),
                    }
                    .map_err(|reason| {
                        AnthropicStreamingAccumulator::invalid_body(
                            reason,
                            "Anthropic tool_use content block requires a string id",
                            &event.data,
                        )
                        .with_response_details(
                            ResponseDetails::FieldType {
                                expected: JsonValueKind::String,
                                actual: JsonValueKind::of(block_value.get("id")),
                            },
                        )
                    })?;
                }
                Ok(None)
            }
            "content_block_delta" => {
                let index = match payload.get("index") {
                    None => Err(ResponseViolation::MissingField { field: "index" }),
                    Some(value) => value
                        .as_u64()
                        .and_then(|value| usize::try_from(value).ok())
                        .ok_or(ResponseViolation::InvalidField { field: "index" }),
                }
                .map_err(|reason| {
                    AnthropicStreamingAccumulator::invalid_body(
                        reason,
                        "Anthropic content_block_delta requires a nonnegative integer index",
                        &event.data,
                    )
                    .with_response_details(ResponseDetails::FieldType {
                        expected: JsonValueKind::Number,
                        actual: JsonValueKind::of(payload.get("index")),
                    })
                })?;
                self.ensure_content_index(index)?;
                let delta = match payload.get("delta") {
                    None => Err(ResponseViolation::MissingField { field: "delta" }),
                    Some(value) => value
                        .as_object()
                        .ok_or(ResponseViolation::InvalidField { field: "delta" }),
                }
                .map_err(|reason| {
                    AnthropicStreamingAccumulator::invalid_body(
                        reason,
                        "Anthropic content_block_delta requires an object delta",
                        &event.data,
                    )
                    .with_response_details(ResponseDetails::FieldType {
                        expected: JsonValueKind::Object,
                        actual: JsonValueKind::of(payload.get("delta")),
                    })
                })?;
                let delta_type = match delta.get("type") {
                    None => Err(ResponseViolation::MissingField {
                        field: "delta.type",
                    }),
                    Some(value) => value.as_str().ok_or(ResponseViolation::InvalidField {
                        field: "delta.type",
                    }),
                }
                .map_err(|reason| {
                    AnthropicStreamingAccumulator::invalid_body(
                        reason,
                        "Anthropic content_block_delta requires a string delta.type",
                        &event.data,
                    )
                    .with_response_details(ResponseDetails::FieldType {
                        expected: JsonValueKind::String,
                        actual: JsonValueKind::of(delta.get("type")),
                    })
                })?;
                match delta_type {
                    "citations_delta" => {
                        let citation = delta.get("citation").ok_or_else(|| {
                            Self::invalid_body(
                                ResponseViolation::MissingField {
                                    field: "delta.citation",
                                },
                                "Anthropic citations_delta requires a citation",
                                &event.data,
                            )
                        })?;
                        self.append_citation(index, citation.clone())?;
                        Ok(None)
                    }
                    "text_delta" => {
                        let text = match delta.get("text") {
                            None => Err(ResponseViolation::MissingField {
                                field: "delta.text",
                            }),
                            Some(value) => value.as_str().ok_or(ResponseViolation::InvalidField {
                                field: "delta.text",
                            }),
                        }
                        .map_err(|reason| {
                            AnthropicStreamingAccumulator::invalid_body(
                                reason,
                                "Anthropic text_delta requires a string text",
                                &event.data,
                            )
                            .with_response_details(
                                ResponseDetails::FieldType {
                                    expected: JsonValueKind::String,
                                    actual: JsonValueKind::of(delta.get("text")),
                                },
                            )
                        })?
                        .to_string();
                        self.base.retain(text.len(), 0)?;
                        let block = &mut self.content[index];
                        let current = block.text.take().unwrap_or_default();
                        block.text = Some(format!("{current}{text}"));
                        self.append_raw_string(index, "text", &text)?;
                        Ok(Some(GenerateEvent::ContentDelta { delta: text }))
                    }
                    "thinking_delta" => {
                        let text = match delta.get("thinking") {
                            None => Err(ResponseViolation::MissingField {
                                field: "delta.thinking",
                            }),
                            Some(value) => value.as_str().ok_or(ResponseViolation::InvalidField {
                                field: "delta.thinking",
                            }),
                        }
                        .map_err(|reason| {
                            AnthropicStreamingAccumulator::invalid_body(
                                reason,
                                "Anthropic thinking_delta requires a string thinking",
                                &event.data,
                            )
                            .with_response_details(
                                ResponseDetails::FieldType {
                                    expected: JsonValueKind::String,
                                    actual: JsonValueKind::of(delta.get("thinking")),
                                },
                            )
                        })?
                        .to_string();
                        self.base.retain(text.len(), 0)?;
                        let block = &mut self.content[index];
                        let current = block.thinking.take().unwrap_or_default();
                        block.thinking = Some(format!("{current}{text}"));
                        self.append_raw_string(index, "thinking", &text)?;
                        Ok(Some(GenerateEvent::ThinkingDelta { delta: text }))
                    }
                    "input_json_delta" => {
                        let partial = match delta.get("partial_json") {
                            None => Err(ResponseViolation::MissingField {
                                field: "delta.partial_json",
                            }),
                            Some(value) => value.as_str().ok_or(ResponseViolation::InvalidField {
                                field: "delta.partial_json",
                            }),
                        }
                        .map_err(|reason| {
                            AnthropicStreamingAccumulator::invalid_body(
                                reason,
                                "Anthropic input_json_delta requires string partial JSON",
                                &event.data,
                            )
                            .with_response_details(
                                ResponseDetails::FieldType {
                                    expected: JsonValueKind::String,
                                    actual: JsonValueKind::of(delta.get("partial_json")),
                                },
                            )
                        })?
                        .to_string();
                        self.base.retain(partial.len(), 0)?;
                        self.tool_json_fragments
                            .entry(index)
                            .or_default()
                            .push_str(&partial);
                        self.content[index].id.as_ref().ok_or_else(|| {
                            AnthropicStreamingAccumulator::invalid(ResponseViolation::MissingField { field: "content_block.id" }, format!(
                                "Anthropic streamed tool_use block at index {index} did not include id"
                            ))
                        })?;
                        Ok(None)
                    }
                    "signature_delta" => {
                        let signature =
                            match delta.get("signature") {
                                None => Err(ResponseViolation::MissingField {
                                    field: "delta.signature",
                                }),
                                Some(value) => value
                                    .as_str()
                                    .filter(|signature| !signature.trim().is_empty())
                                    .ok_or(ResponseViolation::InvalidField {
                                        field: "delta.signature",
                                    }),
                            }
                            .map_err(|reason| {
                                AnthropicStreamingAccumulator::invalid_body(
                                reason,
                                "Anthropic signature_delta requires a nonblank string signature",
                                &event.data,
                            )
                            .with_response_details(ResponseDetails::FieldType {
                                expected: JsonValueKind::String,
                                actual: JsonValueKind::of(delta.get("signature")),
                            })
                            })?;
                        self.base.retain(signature.len(), 0)?;
                        let block = &mut self.content[index];
                        let current = block.signature.take().unwrap_or_default();
                        block.signature = Some(format!("{current}{signature}"));
                        self.append_raw_string(index, "signature", signature)?;
                        Ok(None)
                    }
                    _ => Err(AnthropicStreamingAccumulator::invalid_body(
                        ResponseViolation::UnknownVariant {
                            field: "delta.type",
                        },
                        format!("unknown Anthropic content_block_delta type `{delta_type}`"),
                        &event.data,
                    )
                    .with_response_details(ResponseDetails::Discriminator {
                        actual: delta_type.to_owned(),
                    })),
                }
            }
            "content_block_stop" => {
                let index = match payload.get("index") {
                    None => Err(ResponseViolation::MissingField { field: "index" }),
                    Some(value) => value
                        .as_u64()
                        .and_then(|value| usize::try_from(value).ok())
                        .ok_or(ResponseViolation::InvalidField { field: "index" }),
                }
                .map_err(|reason| {
                    AnthropicStreamingAccumulator::invalid_body(
                        reason,
                        "Anthropic content_block_stop requires a nonnegative integer index",
                        &event.data,
                    )
                    .with_response_details(ResponseDetails::FieldType {
                        expected: JsonValueKind::Number,
                        actual: JsonValueKind::of(payload.get("index")),
                    })
                })?;
                self.ensure_content_index(index)?;
                self.finalize_tool_input(index)?;
                Ok(None)
            }
            "message_delta" => {
                if let Some(delta_value) = payload.get("delta")
                    && let Some(stop_reason) =
                        delta_value.get("stop_reason").and_then(Value::as_str)
                {
                    self.stop_reason = Some(stop_reason.to_string());
                }
                if let Some(usage_value) = payload.get("usage") {
                    let usage = serde_json::from_value(usage_value.clone()).map_err(|err| {
                        AnthropicStreamingAccumulator::invalid_body(
                            ResponseViolation::InvalidField { field: "usage" },
                            format!("failed to decode Anthropic streaming usage payload: {err}"),
                            &event.data,
                        )
                        .with_response_details(ResponseDetails::json(&err))
                        .at_phase(ErrorPhase::Decode)
                    })?;
                    match &mut self.raw_usage {
                        Some(current) => current.update_output(usage),
                        None => self.raw_usage = Some(usage),
                    }
                    let usage = usage_from_anthropic(self.raw_usage.as_ref());
                    self.base.set_usage(usage.clone());
                    return Ok(Some(GenerateEvent::Usage(usage)));
                }
                Ok(None)
            }
            "message_stop" => {
                let response = self.finish(selected_model)?;
                Ok(Some(GenerateEvent::Completed(Box::new(response))))
            }
            _ => Self::unknown(event_type, event),
        }
    }

    fn unknown(event_type: &str, event: &SseEvent) -> Result<Option<GenerateEvent>, KitError> {
        Err(AnthropicStreamingAccumulator::invalid_body(
            ResponseViolation::UnknownVariant { field: "event" },
            format!("Anthropic streaming used unknown event `{event_type}`"),
            &event.data,
        )
        .with_response_details(ResponseDetails::Discriminator {
            actual: event_type.to_owned(),
        }))
    }

    fn ensure_content_index(&self, index: usize) -> Result<(), KitError> {
        if self.content.len() <= index {
            return Err(AnthropicStreamingAccumulator::invalid(
                ResponseViolation::ProtocolViolation {
                    rule: "content block must start before deltas",
                },
                format!(
                    "Anthropic stream referenced content block index {index} before content_block_start"
                ),
            )
            .with_response_details(ResponseDetails::Index {
                index,
                available: self.content.len(),
            }));
        }
        Ok(())
    }

    fn finalize_tool_input(&mut self, index: usize) -> Result<(), KitError> {
        let Some(fragment) = self.tool_json_fragments.remove(&index) else {
            return Ok(());
        };
        if fragment.trim().is_empty() {
            return Ok(());
        }

        let parsed = serde_json::from_str::<Value>(&fragment).map_err(|err| {
            AnthropicStreamingAccumulator::invalid_body(
                ResponseViolation::MalformedJson,
                format!("failed to decode Anthropic streamed tool JSON for block {index}: {err}"),
                &fragment,
            )
            .with_response_details(ResponseDetails::json(&err))
            .at_phase(ErrorPhase::Decode)
        })?;
        self.content[index].input = Some(parsed.clone());
        self.set_raw_value(index, "input", parsed)?;
        Ok(())
    }

    fn finish(&mut self, selected_model: &ModelId) -> Result<GenerateResponse, KitError> {
        let pending_indices = self.tool_json_fragments.keys().copied().collect::<Vec<_>>();
        for index in pending_indices {
            self.ensure_content_index(index)?;
            self.finalize_tool_input(index)?;
        }

        let content = std::mem::take(&mut self.content);
        let portable = blocks_to_portable(&content)?;
        ensure_nonempty_generation_parts(&portable, None)?;

        let model = self.base.model()?.clone();
        let stop_reason = self.stop_reason.as_deref().ok_or_else(|| {
            AnthropicStreamingAccumulator::invalid(
                ResponseViolation::MissingField {
                    field: "stop_reason",
                },
                "Anthropic stream completed without a terminal stop_reason",
            )
        })?;
        let mut response = generate_response(
            model.clone(),
            portable,
            GenerateResponseOptions {
                finish_reason: Some(stop_reason_to_finish_reason(stop_reason)?),
                usage: self.base.usage().clone(),
            },
        );
        response.message =
            response
                .message
                .with_continuation(GenerationContinuation::anthropic(
                    selected_model.clone(),
                    std::mem::take(&mut self.raw_content),
                ))?;
        Ok(response)
    }
}
