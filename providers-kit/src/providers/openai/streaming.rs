use reqwest::Method;
use serde_json::Value;

use crate::KitError;
use crate::core::{FrameDecoder, StreamFrame};
use crate::error::{
    ErrorKind, ErrorPhase, JsonValueKind, Operation, ResponseDetails, ResponseViolation,
};
use crate::generation::{GenerateEvent, GenerateEventStream};
use crate::models::ModelId;
use crate::transport::{CheckedRequestSpec, SseEvent};

use super::api::{OPENAI_REQUESTS, OpenAiApiContext};
use super::responses::{normalize_generate_response, usage_from_openai};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OpenAiStreamEventKind {
    Started,
    TextDelta,
    ThinkingDelta,
    Usage,
    Completed,
    Error,
    Lifecycle,
    Unknown,
}

pub(crate) async fn start_generate_stream(
    api: OpenAiApiContext,
    selected_model: ModelId,
    body: Value,
) -> Result<GenerateEventStream, KitError> {
    let execution = api.transport().execution();
    let timeout = execution.timeout();
    let response = OPENAI_REQUESTS
        .response(
            "responses",
            api.request(Method::POST, "responses")?.json(&body),
            execution,
        )
        .await?;
    FrameDecoder::sse().into_stream(
        response,
        "openai generation stream",
        timeout,
        move |frame| {
            let event =
                match frame {
                    StreamFrame::Sse(event) => event,
                    StreamFrame::End => return Ok(Vec::new()),
                    StreamFrame::Line(_) => {
                        return Err(KitError::invalid_response(
                            ResponseViolation::ProtocolViolation { rule: "sse_frame" },
                        )
                        .at_phase(ErrorPhase::Stream)
                        .with_message("OpenAI streaming received a non-SSE frame".to_string())
                        .with_diagnostic(None));
                    }
                };
            OpenAiStreamEventKind::normalize(&selected_model, &event)
                .map(|event| event.into_iter().collect())
                .map_err(|error| error.at_operation(Operation::GenerateStream))
        },
    )
}

impl OpenAiStreamEventKind {
    fn normalize(
        selected_model: &ModelId,
        event: &SseEvent,
    ) -> Result<Option<GenerateEvent>, KitError> {
        if event.data.trim().is_empty() || event.data.trim() == "[DONE]" {
            return Ok(None);
        }

        let payload = serde_json::from_str::<Value>(&event.data).map_err(|err| {
            KitError::invalid_response(ResponseViolation::MalformedJson)
                .with_response_details(ResponseDetails::json(&err))
                .at_phase(ErrorPhase::Decode)
                .with_message(format!("failed to decode OpenAI SSE payload: {err}"))
                .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(
                    event.data.as_bytes(),
                )))
        })?;

        let event_type = match payload.get("type") {
            None => Err(ResponseViolation::MissingField { field: "type" }),
            Some(value) => value
                .as_str()
                .ok_or(ResponseViolation::InvalidField { field: "type" }),
        }
        .map_err(|reason| {
            KitError::invalid_response(reason)
                .with_response_details(ResponseDetails::FieldType {
                    expected: JsonValueKind::String,
                    actual: JsonValueKind::of(payload.get("type")),
                })
                .at_phase(ErrorPhase::Normalize)
                .with_message("OpenAI SSE payload requires a string type")
                .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(
                    event.data.as_bytes(),
                )))
        })?;

        match Self::classify(event_type, &payload, &event.data)? {
            OpenAiStreamEventKind::Started => {
                let response = match payload.get("response") {
                    None => Err(ResponseViolation::MissingField { field: "response" }),
                    Some(value) => value
                        .as_object()
                        .ok_or(ResponseViolation::InvalidField { field: "response" }),
                }
                .map_err(|reason| {
                    KitError::invalid_response(reason)
                        .with_response_details(ResponseDetails::FieldType {
                            expected: JsonValueKind::Object,
                            actual: JsonValueKind::of(payload.get("response")),
                        })
                        .at_phase(ErrorPhase::Normalize)
                        .with_message("OpenAI response.created event requires an object response")
                        .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(
                            event.data.as_bytes(),
                        )))
                })?;
                let model = match response.get("model") {
                    None => Err(ResponseViolation::MissingField {
                        field: "response.model",
                    }),
                    Some(value) => value.as_str().ok_or(ResponseViolation::InvalidField {
                        field: "response.model",
                    }),
                }
                .map_err(|reason| {
                    KitError::invalid_response(reason)
                        .with_response_details(ResponseDetails::FieldType {
                            expected: JsonValueKind::String,
                            actual: JsonValueKind::of(response.get("model")),
                        })
                        .at_phase(ErrorPhase::Normalize)
                        .with_message("OpenAI response.created event requires a string model")
                        .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(
                            event.data.as_bytes(),
                        )))
                })?;
                let model = ModelId::new(model).map_err(|_| {
                    KitError::invalid_response(ResponseViolation::InvalidField {
                        field: "response.model",
                    })
                    .at_phase(ErrorPhase::Normalize)
                    .with_message(
                        "OpenAI SSE response.created event used an invalid model id".to_string(),
                    )
                    .with_diagnostic(Some(
                        CheckedRequestSpec::diagnostic_body(event.data.as_bytes()),
                    ))
                })?;
                Ok(Some(GenerateEvent::Started { model }))
            }
            OpenAiStreamEventKind::TextDelta | OpenAiStreamEventKind::ThinkingDelta => {
                let delta = match payload.get("delta") {
                    None => Err(ResponseViolation::MissingField { field: "delta" }),
                    Some(value) => value
                        .as_str()
                        .ok_or(ResponseViolation::InvalidField { field: "delta" }),
                }
                .map_err(|reason| {
                    KitError::invalid_response(reason)
                        .with_response_details(ResponseDetails::FieldType {
                            expected: JsonValueKind::String,
                            actual: JsonValueKind::of(payload.get("delta")),
                        })
                        .at_phase(ErrorPhase::Normalize)
                        .with_message("OpenAI SSE delta event requires a string delta")
                        .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(
                            event.data.as_bytes(),
                        )))
                })?
                .to_string();
                Ok(Some(
                    if matches!(
                        Self::classify(event_type, &payload, &event.data)?,
                        OpenAiStreamEventKind::ThinkingDelta
                    ) {
                        GenerateEvent::ThinkingDelta { delta }
                    } else {
                        GenerateEvent::ContentDelta { delta }
                    },
                ))
            }
            OpenAiStreamEventKind::Completed => {
                let response = payload.get("response").cloned().ok_or_else(|| {
                    KitError::invalid_response(ResponseViolation::MissingField {
                        field: "response",
                    })
                    .with_response_details(ResponseDetails::FieldType {
                        expected: JsonValueKind::Object,
                        actual: JsonValueKind::Absent,
                    })
                    .at_phase(ErrorPhase::Normalize)
                    .with_message(
                        "OpenAI SSE response.completed event did not include response".to_string(),
                    )
                    .with_diagnostic(Some(
                        CheckedRequestSpec::diagnostic_body(event.data.as_bytes()),
                    ))
                })?;
                let normalized = normalize_generate_response(Some(selected_model), &response)
                    .map_err(|error| {
                        let mut details = error.native_details().clone();
                        let frame = CheckedRequestSpec::diagnostic_body(event.data.as_bytes());
                        details.body = frame.body;
                        details.truncated |= frame.truncated;
                        details.event_type = Some(event_type.to_owned());
                        details.sequence_number =
                            payload.get("sequence_number").and_then(Value::as_u64);
                        details.event_id = event.id.clone().or(details.event_id);
                        let error = error.with_native_capture(details);
                        if response.get("status").and_then(Value::as_str) == Some("failed")
                            && !matches!(error.kind(), ErrorKind::InvalidResponse(_))
                        {
                            error.at_phase(ErrorPhase::Stream)
                        } else {
                            error
                        }
                    })?;
                Ok(Some(GenerateEvent::Completed(Box::new(normalized))))
            }
            OpenAiStreamEventKind::Error => {
                // Standalone Responses error events carry code/message/param at
                // the top level. response.failed carries the response's error.
                let error_value = payload.get("error").unwrap_or(&payload);
                let mut native =
                    serde_json::from_value::<super::wire::OpenAiErrorObject>(error_value.clone())
                        .map_err(|error| {
                        KitError::invalid_response(ResponseViolation::InvalidField {
                            field: "error",
                        })
                        .with_response_details(ResponseDetails::json(&error))
                        .at_phase(ErrorPhase::Decode)
                        .with_message(format!("failed to decode OpenAI SSE error: {error}"))
                        .with_diagnostic(Some(
                            CheckedRequestSpec::diagnostic_body(event.data.as_bytes()),
                        ))
                    })?;
                if payload.get("error").is_none() && event_type == "error" {
                    native.kind = None;
                }
                let mut diagnostic = CheckedRequestSpec::diagnostic_body(event.data.as_bytes());
                diagnostic.event_type = Some(event_type.to_owned());
                diagnostic.sequence_number = payload.get("sequence_number").and_then(Value::as_u64);
                diagnostic.event_id = event.id.clone();
                let failure = native.remote_failure(Some(diagnostic))?;
                Err(KitError::from_remote(
                    failure,
                    Operation::GenerateStream,
                    ErrorPhase::Stream,
                ))
            }
            OpenAiStreamEventKind::Lifecycle => Ok(None),
            OpenAiStreamEventKind::Usage => {
                let usage_value = payload.get("usage").cloned().ok_or_else(|| {
                    KitError::invalid_response(ResponseViolation::MissingField { field: "usage" })
                        .with_response_details(ResponseDetails::FieldType {
                            expected: JsonValueKind::Object,
                            actual: JsonValueKind::Absent,
                        })
                        .at_phase(ErrorPhase::Normalize)
                        .with_message("OpenAI SSE usage event did not include usage".to_string())
                        .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(
                            event.data.as_bytes(),
                        )))
                })?;
                let usage = serde_json::from_value::<super::wire::OpenAiUsage>(usage_value)
                    .map_err(|err| {
                        KitError::invalid_response(ResponseViolation::InvalidField {
                            field: "usage",
                        })
                        .with_response_details(ResponseDetails::json(&err))
                        .at_phase(ErrorPhase::Decode)
                        .with_message(format!("failed to decode OpenAI SSE usage payload: {err}"))
                        .with_diagnostic(Some(
                            CheckedRequestSpec::diagnostic_body(event.data.as_bytes()),
                        ))
                    })
                    .map(|usage| usage_from_openai(Some(&usage)))?;
                Ok(Some(GenerateEvent::Usage(usage)))
            }
            OpenAiStreamEventKind::Unknown => Self::unknown(event_type, event),
        }
    }

    fn classify(
        event_type: &str,
        payload: &Value,
        diagnostic_body: &str,
    ) -> Result<OpenAiStreamEventKind, KitError> {
        match event_type {
            "response.created" => Ok(OpenAiStreamEventKind::Started),
            "response.output_text.delta" => Ok(OpenAiStreamEventKind::TextDelta),
            "response.usage.updated" => Ok(OpenAiStreamEventKind::Usage),
            "response.completed" | "response.incomplete" | "response.failed" => {
                Ok(OpenAiStreamEventKind::Completed)
            }
            "response.error" | "error" => Ok(OpenAiStreamEventKind::Error),
            "response.function_call_arguments.delta" | "response.function_call_arguments.done" => {
                Ok(OpenAiStreamEventKind::Lifecycle)
            }
            "response.refusal.delta" => Ok(OpenAiStreamEventKind::TextDelta),
            "response.refusal.done" => Ok(OpenAiStreamEventKind::Lifecycle),
            "response.reasoning_summary_text.delta" | "response.reasoning_text.delta" => {
                Ok(OpenAiStreamEventKind::ThinkingDelta)
            }
            "response.reasoning_summary_text.done" | "response.reasoning_text.done" => {
                Ok(OpenAiStreamEventKind::Lifecycle)
            }
            "response.output_item.added" | "response.output_item.done" => {
                Self::validate_output_item(event_type, payload, diagnostic_body)?;
                Ok(OpenAiStreamEventKind::Lifecycle)
            }
            "response.content_part.added"
            | "response.content_part.done"
            | "response.output_text.done"
            | "response.in_progress"
            | "response.queued"
            | "response.reasoning_summary_part.added"
            | "response.reasoning_summary_part.done" => Ok(OpenAiStreamEventKind::Lifecycle),
            "response.output_text.annotation.added"
            | "response.web_search_call.in_progress"
            | "response.web_search_call.searching"
            | "response.web_search_call.completed"
            | "response.file_search_call.in_progress"
            | "response.file_search_call.searching"
            | "response.file_search_call.completed"
            | "response.code_interpreter_call.in_progress"
            | "response.code_interpreter_call_code.done"
            | "response.code_interpreter_call.interpreting"
            | "response.code_interpreter_call.completed" => Ok(OpenAiStreamEventKind::Lifecycle),
            "response.code_interpreter_call_code.delta" => Ok(OpenAiStreamEventKind::Lifecycle),
            _ => Ok(OpenAiStreamEventKind::Unknown),
        }
    }

    fn unknown(event_type: &str, event: &SseEvent) -> Result<Option<GenerateEvent>, KitError> {
        Err(
            KitError::invalid_response(ResponseViolation::UnknownVariant { field: "type" })
                .with_response_details(ResponseDetails::Discriminator {
                    actual: event_type.to_owned(),
                })
                .at_phase(ErrorPhase::Normalize)
                .with_message(format!(
                    "OpenAI returned unknown streaming event `{event_type}`"
                ))
                .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(
                    event.data.as_bytes(),
                ))),
        )
    }

    fn validate_output_item(
        event_type: &str,
        payload: &Value,
        diagnostic_body: &str,
    ) -> Result<(), KitError> {
        let item = match payload.get("item") {
            None => Err(ResponseViolation::MissingField { field: "item" }),
            Some(value) => value
                .as_object()
                .ok_or(ResponseViolation::InvalidField { field: "item" }),
        }
        .map_err(|reason| {
            KitError::invalid_response(reason)
                .with_response_details(ResponseDetails::FieldType {
                    expected: JsonValueKind::Object,
                    actual: JsonValueKind::of(payload.get("item")),
                })
                .at_phase(ErrorPhase::Normalize)
                .with_message(format!(
                    "OpenAI event `{event_type}` requires an object item"
                ))
                .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(
                    diagnostic_body.as_bytes(),
                )))
        })?;
        let item_type = match item.get("type") {
            None => Err(ResponseViolation::MissingField { field: "item.type" }),
            Some(value) => value
                .as_str()
                .ok_or(ResponseViolation::InvalidField { field: "item.type" }),
        }
        .map_err(|reason| {
            KitError::invalid_response(reason)
                .with_response_details(ResponseDetails::FieldType {
                    expected: JsonValueKind::String,
                    actual: JsonValueKind::of(item.get("type")),
                })
                .at_phase(ErrorPhase::Normalize)
                .with_message(format!(
                    "OpenAI event `{event_type}` requires a string item.type"
                ))
                .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(
                    diagnostic_body.as_bytes(),
                )))
        })?;
        match item_type {
            "message"
            | "web_search_call"
            | "file_search_call"
            | "code_interpreter_call"
            | "function_call"
            | "function_call_output"
            | "reasoning" => Ok(()),
            other => Err(
                KitError::invalid_response(ResponseViolation::UnknownVariant {
                    field: "item.type",
                })
                .with_response_details(ResponseDetails::Discriminator {
                    actual: other.to_owned(),
                })
                .at_phase(ErrorPhase::Normalize)
                .with_message(format!(
                    "OpenAI event `{event_type}` returned undeclared item type `{other}`"
                ))
                .with_diagnostic(Some(CheckedRequestSpec::diagnostic_body(
                    diagnostic_body.as_bytes(),
                ))),
            ),
        }
    }
}
