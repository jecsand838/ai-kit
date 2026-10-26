use std::collections::{BTreeMap, VecDeque};

use base64::Engine as _;
use serde_json::{Value, json};

use crate::SdkError;
use crate::core::{SessionSocket, SessionSocketMessage, tool_result_text};
use crate::error::ValidationViolation;
use crate::error::{ErrorPhase, Operation, ResponseDetails, ResponseViolation};
use crate::generation::{FunctionCall, ToolChoice};
use crate::models::{ModelBackend, ModelId};
use crate::session::{
    SessionBackend, SessionConfig, SessionEvent, SessionInput, SessionTurnDetection,
};

use super::api::OPENAI_REQUESTS;
use super::models::OpenAiModel;
use super::responses::usage_from_openai;
use super::wire::OpenAiErrorObject;
use crate::transport::CheckedRequestSpec;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OpenAiTurn {
    Idle,
    Text,
    AutomaticAudio,
    ManualAudioOpen,
    ManualAudioSubmitted,
}

/// Concrete OpenAI session child selected by one exact OpenAI model.
pub(crate) struct OpenAiSession {
    id: String,
    model_id: ModelId,
    socket: SessionSocket,
    config: SessionConfig,
    pending: VecDeque<SessionEvent>,
    outstanding_calls: BTreeMap<String, String>,
    function_response_pending: bool,
    turn: OpenAiTurn,
    closed: bool,
}

impl OpenAiSession {
    pub(crate) async fn open(model: &OpenAiModel, config: SessionConfig) -> Result<Self, SdkError> {
        Self::validate_model(model.model_id())?;
        Self::validate_config(&config)?;
        let update = Self::session_update_payload(&config)?;
        let api = model.api();
        let request = api.live_request(model.model_id().as_str())?;
        let (mut socket, deadline) = SessionSocket::connect(
            request,
            api.transport().connect_timeout(),
            api.transport().timeout(),
            OPENAI_REQUESTS,
        )
        .await?;
        let setup = async {
        let expected_modalities = update["session"]["output_modalities"].clone();
        socket.send(update, deadline).await?;
        let mut created = false;
        loop {
            let payload = socket
                .receive_payload(deadline, "OpenAI websocket ended during interactive setup")
                .await?;
            match payload.get("type").and_then(Value::as_str) {
                Some("session.created") if !created => created = true,
                Some("session.updated") => {
                    let valid = payload.get("session").is_some_and(|session| {
                        session["type"] == "realtime"
                            && session["model"] == model.model_id().as_str()
                            && session["output_modalities"] == expected_modalities
                    });
                    if !valid {
                        return Err(SdkError::invalid_response(ResponseViolation::ProtocolViolation { rule: "session_configuration_acknowledgement" }).at_phase(ErrorPhase::SessionHandshake).with_message("OpenAI session.updated did not acknowledge the selected model and modalities".to_string()).with_diagnostic(CheckedRequestSpec::diagnostic_json(&payload)));
                    }
                    break;
                }
                Some("error" | "response.error") => {
                    return Err(
                        Self::provider_error(&payload).at_phase(ErrorPhase::SessionHandshake)
                    );
                }
                unexpected => {
                    return Err(
                        SdkError::invalid_response(ResponseViolation::UnknownVariant {
                            field: "setup.event",
                        })
                        .at_phase(ErrorPhase::SessionHandshake)
                        .with_message(format!(
                            "OpenAI sent unexpected interactive setup event `{unexpected:?}`"
                        ))
                        .with_diagnostic(CheckedRequestSpec::diagnostic_json(&payload)),
                    );
                }
            }
        }

            Ok(())
        }.await;
        if let Err(error) = setup {
            return Err(socket.fail_setup(error).await);
        }

        Ok(Self {
            id: uuid::Uuid::new_v4().to_string(),
            model_id: model.model_id().clone(),
            socket,
            config,
            pending: VecDeque::new(),
            outstanding_calls: BTreeMap::new(),
            function_response_pending: false,
            turn: OpenAiTurn::Idle,
            closed: false,
        })
    }

    fn validate_model(model_id: &ModelId) -> Result<(), SdkError> {
        match model_id.as_str() {
            "gpt-realtime"
            | "gpt-realtime-mini"
            | "gpt-realtime-1.5"
            | "gpt-realtime-2"
            | "gpt-realtime-2.1"
            | "gpt-realtime-2.1-mini" => Ok(()),
            other => Err(SdkError::unsupported(
                ValidationViolation::Unsupported {
                    field: "session",
                    rule: "selected model does not support interactive sessions",
                },
                format!("OpenAI interactive sessions for selected model `{other}`"),
            )),
        }
    }

    fn validate_config(config: &SessionConfig) -> Result<(), SdkError> {
        if let Some(format) = config.input_audio_format() {
            Self::audio_format(format)?;
        }
        if let Some(format) = config.output_audio_format() {
            Self::audio_format(format)?;
        }
        if config.turn_detection() == SessionTurnDetection::Manual
            && config.input_audio_format().is_none()
        {
            return Err(SdkError::invalid_input(
                ValidationViolation::MissingField {
                    field: "session.input_audio_format",
                },
                "OpenAI manual turn detection requires audio input",
            ));
        }
        Ok(())
    }

    fn audio_format(media_type: &str) -> Result<Value, SdkError> {
        match media_type {
            "audio/pcm" | "audio/pcm;rate=24000" => {
                Ok(json!({ "type": "audio/pcm", "rate": 24_000 }))
            }
            "audio/pcmu" => Ok(json!({ "type": "audio/pcmu" })),
            "audio/pcma" => Ok(json!({ "type": "audio/pcma" })),
            _ => Err(SdkError::unsupported(
                ValidationViolation::Unsupported {
                    field: "session.audio_format",
                    rule: "unsupported audio format",
                },
                format!("OpenAI interactive audio format `{media_type}`"),
            )),
        }
    }

    fn session_update_payload(config: &SessionConfig) -> Result<Value, SdkError> {
        let tools = config
            .functions()
            .iter()
            .filter(|function| !matches!(config.tool_choice(), Some(ToolChoice::Tool { name }) if name != function.name()))
            .map(|function| json!({
                "type": "function",
                "name": function.name(),
                "description": function.description(),
                "parameters": function.input_schema().schema().clone(),
            }))
            .collect::<Vec<_>>();
        let mut session = json!({
            "type": "realtime",
            "output_modalities": if config.output_audio_format().is_some() {
                json!(["audio"])
            } else {
                json!(["text"])
            },
        });
        if let Some(instructions) = config.instructions() {
            session["instructions"] = Value::String(instructions.to_string());
        }
        if !tools.is_empty() {
            session["tools"] = Value::Array(tools);
            if let Some(choice) = config.tool_choice() {
                session["tool_choice"] = match choice {
                    ToolChoice::Auto => json!("auto"),
                    ToolChoice::None => json!("none"),
                    ToolChoice::Required | ToolChoice::Any => json!("required"),
                    ToolChoice::Tool { name } => json!({ "type": "function", "name": name }),
                    ToolChoice::Allowed { .. } => {
                        return Err(SdkError::unsupported(
                            ValidationViolation::Unsupported {
                                field: "session.tool_choice",
                                rule: "allowed-function subsets unavailable",
                            },
                            "OpenAI interactive allowed-function subsets",
                        ));
                    }
                };
            }
        }

        let mut audio = serde_json::Map::new();
        if let Some(format) = config.input_audio_format() {
            let mut input = serde_json::Map::new();
            input.insert("format".to_string(), Self::audio_format(format)?);
            if config.input_transcription() {
                input.insert(
                    "transcription".to_string(),
                    json!({ "model": "gpt-transcribe" }),
                );
            }
            input.insert(
                "turn_detection".to_string(),
                match config.turn_detection() {
                    SessionTurnDetection::Automatic => json!({ "type": "server_vad" }),
                    SessionTurnDetection::Manual => Value::Null,
                },
            );
            audio.insert("input".to_string(), Value::Object(input));
        }
        if let Some(format) = config.output_audio_format() {
            audio.insert(
                "output".to_string(),
                json!({ "format": Self::audio_format(format)? }),
            );
        }
        if !audio.is_empty() {
            session["audio"] = Value::Object(audio);
        }
        Ok(json!({ "type": "session.update", "session": session }))
    }

    fn ensure_open(&self, operation: &'static str) -> Result<(), SdkError> {
        if self.closed {
            Err(SdkError::closed(operation))
        } else {
            Ok(())
        }
    }

    fn ensure_idle(&self) -> Result<(), SdkError> {
        if self.turn == OpenAiTurn::Idle && self.outstanding_calls.is_empty() {
            Ok(())
        } else {
            Err(SdkError::invalid_state(
                "OpenAI interactive session already has an active turn",
            ))
        }
    }

    async fn send_input(
        &mut self,
        input: SessionInput,
        deadline: tokio::time::Instant,
    ) -> Result<(), SdkError> {
        self.ensure_open("session.send")?;
        match input {
            SessionInput::Text { text } => {
                if text.trim().is_empty() {
                    return Err(SdkError::invalid_input(
                        ValidationViolation::Empty {
                            field: "session.input.text",
                        },
                        "interactive text input must not be blank",
                    ));
                }
                self.ensure_idle()?;
                self.socket
                    .send(
                        json!({
                            "type": "conversation.item.create",
                            "item": {
                                "type": "message",
                                "role": "user",
                                "content": [{ "type": "input_text", "text": text }],
                            },
                        }),
                        deadline,
                    )
                    .await?;
                self.socket
                    .send(json!({ "type": "response.create" }), deadline)
                    .await?;
                self.turn = OpenAiTurn::Text;
            }
            SessionInput::Audio { media_type, data } => {
                let expected = self.config.input_audio_format().ok_or_else(|| {
                    SdkError::invalid_state(
                        "OpenAI interactive session was not configured for audio input",
                    )
                })?;
                if media_type != expected {
                    return Err(SdkError::invalid_input(
                        ValidationViolation::Incompatible {
                            field: "session.input.audio.media_type",
                            other: "session.input_audio_format",
                        },
                        format!(
                            "interactive audio media type `{media_type}` does not match configured `{expected}`"
                        ),
                    ));
                }
                if data.is_empty() {
                    return Err(SdkError::invalid_input(
                        ValidationViolation::Empty {
                            field: "session.input.audio.data",
                        },
                        "interactive audio data must not be empty",
                    ));
                }
                let automatic = match (self.config.turn_detection(), self.turn) {
                    (
                        SessionTurnDetection::Automatic,
                        OpenAiTurn::Idle | OpenAiTurn::AutomaticAudio,
                    ) => true,
                    (SessionTurnDetection::Manual, OpenAiTurn::ManualAudioOpen) => false,
                    (SessionTurnDetection::Manual, _) => {
                        return Err(SdkError::invalid_state(
                            "OpenAI manual audio input requires an open turn",
                        ));
                    }
                    (SessionTurnDetection::Automatic, _) => {
                        return Err(SdkError::invalid_state(
                            "OpenAI interactive session already has an active turn",
                        ));
                    }
                };
                self.socket
                    .send(
                        json!({
                            "type": "input_audio_buffer.append",
                            "audio": base64::engine::general_purpose::STANDARD.encode(data),
                        }),
                        deadline,
                    )
                    .await?;
                if automatic {
                    self.turn = OpenAiTurn::AutomaticAudio;
                }
            }
            SessionInput::FunctionResult(result) => {
                let call_id = result.call_id().ok_or_else(|| {
                    SdkError::invalid_input(
                        ValidationViolation::MissingField {
                            field: "function_result.call_id",
                        },
                        "OpenAI interactive function results require a call id",
                    )
                })?;
                let expected_name = self.outstanding_calls.get(call_id).ok_or_else(|| {
                    SdkError::invalid_state(format!(
                        "OpenAI interactive function call `{call_id}` is not outstanding"
                    ))
                })?;
                if result.name() != expected_name {
                    return Err(SdkError::invalid_input(
                        ValidationViolation::Incompatible {
                            field: "function_result.name",
                            other: "outstanding_function.name",
                        },
                        format!(
                            "OpenAI interactive function result `{}` does not match outstanding function `{expected_name}`",
                            result.name()
                        ),
                    ));
                }
                let output = tool_result_text(result.content())?;
                self.socket
                    .send(
                        json!({
                            "type": "conversation.item.create",
                            "item": {
                                "type": "function_call_output",
                                "call_id": call_id,
                                "output": output,
                            }
                        }),
                        deadline,
                    )
                    .await?;
                self.outstanding_calls.remove(call_id);
                if self.outstanding_calls.is_empty() {
                    self.socket
                        .send(json!({ "type": "response.create" }), deadline)
                        .await?;
                    self.turn = OpenAiTurn::Text;
                }
            }
            SessionInput::TurnStart => {
                if self.config.turn_detection() != SessionTurnDetection::Manual {
                    return Err(SdkError::invalid_state(
                        "OpenAI TurnStart requires manual turn detection",
                    ));
                }
                self.ensure_idle()?;
                self.turn = OpenAiTurn::ManualAudioOpen;
            }
            SessionInput::TurnEnd => {
                if self.turn != OpenAiTurn::ManualAudioOpen {
                    return Err(SdkError::invalid_state(
                        "OpenAI TurnEnd requires an open manual audio turn",
                    ));
                }
                self.socket
                    .send(json!({ "type": "input_audio_buffer.commit" }), deadline)
                    .await?;
                self.socket
                    .send(json!({ "type": "response.create" }), deadline)
                    .await?;
                self.turn = OpenAiTurn::ManualAudioSubmitted;
            }
            SessionInput::CancelTurn => {
                if self.turn == OpenAiTurn::Idle {
                    return Err(SdkError::invalid_state(
                        "OpenAI interactive session has no active turn to cancel",
                    ));
                }
                self.socket
                    .send(json!({ "type": "response.cancel" }), deadline)
                    .await?;
            }
        }
        Ok(())
    }

    async fn receive_event(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<SessionEvent, SdkError> {
        self.ensure_open("session.recv")?;
        loop {
            let function_call_waits_for_response = self.function_response_pending
                && matches!(self.pending.front(), Some(SessionEvent::FunctionCall(_)));
            if !function_call_waits_for_response && let Some(event) = self.pending.pop_front() {
                if matches!(event, SessionEvent::Completed | SessionEvent::TurnCancelled) {
                    self.turn = OpenAiTurn::Idle;
                }
                return Ok(event);
            }
            match self.socket.receive(deadline).await? {
                SessionSocketMessage::Payload(payload) => {
                    self.push_events(&payload).map_err(|error| {
                        if error.native_details().body.is_some() {
                            error
                        } else {
                            error.with_diagnostic(CheckedRequestSpec::diagnostic_json(&payload))
                        }
                    })?
                }
                SessionSocketMessage::Closed(details) => {
                    self.closed = true;
                    return Err(SdkError::closed(match details {
                        Some(details) => {
                            format!("OpenAI interactive session received a close frame: {details}")
                        }
                        None => "OpenAI interactive session received a close frame".to_string(),
                    })
                    .at_phase(ErrorPhase::SessionReceive));
                }
                SessionSocketMessage::Exhausted => {
                    self.closed = true;
                    return Err(
                        SdkError::closed("OpenAI interactive session websocket ended")
                            .at_phase(ErrorPhase::SessionReceive),
                    );
                }
            }
        }
    }

    fn push_events(&mut self, payload: &Value) -> Result<(), SdkError> {
        let event_type = payload.get("type").and_then(Value::as_str).ok_or_else(|| {
            SdkError::invalid_response(ResponseViolation::MissingField { field: "type" })
                .at_phase(ErrorPhase::SessionReceive)
                .with_message("OpenAI interactive event omitted type".to_string())
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(payload))
        })?;
        match event_type {
            "response.output_text.delta" => {
                self.pending.push_back(SessionEvent::TextDelta {
                    delta: Self::required_text(payload, "delta", event_type)?,
                });
            }
            "response.output_audio_transcript.delta" => {
                if self.config.output_transcription() {
                    self.pending
                        .push_back(SessionEvent::OutputTranscriptionDelta {
                            delta: Self::required_text(payload, "delta", event_type)?,
                        });
                }
            }
            "response.output_audio.delta" => {
                let media_type = self.config.output_audio_format().ok_or_else(|| {
                    SdkError::invalid_response(ResponseViolation::ProtocolViolation {
                        rule: "session_output_modality",
                    })
                    .at_phase(ErrorPhase::SessionReceive)
                    .with_message("OpenAI returned audio for a text-only session".to_string())
                    .with_diagnostic(CheckedRequestSpec::diagnostic_json(payload))
                })?;
                let data = base64::engine::general_purpose::STANDARD
                    .decode(Self::required_text(payload, "delta", event_type)?)
                    .map_err(|error| {
                        SdkError::invalid_response(ResponseViolation::InvalidField {
                            field: "delta",
                        })
                        .at_phase(ErrorPhase::SessionReceive)
                        .with_message(format!(
                            "OpenAI interactive audio was invalid base64: {error}"
                        ))
                        .with_diagnostic(CheckedRequestSpec::diagnostic_json(payload))
                    })?;
                self.pending.push_back(SessionEvent::Audio {
                    media_type: media_type.to_string(),
                    data,
                });
            }
            "conversation.item.input_audio_transcription.delta" => {
                if self.config.input_transcription() {
                    self.pending
                        .push_back(SessionEvent::InputTranscriptionDelta {
                            delta: Self::required_text(payload, "delta", event_type)?,
                        });
                }
            }
            "conversation.item.input_audio_transcription.completed" => {}
            "response.function_call_arguments.done" => {
                let call_id = Self::required_text(payload, "call_id", event_type)?;
                let name = Self::required_text(payload, "name", event_type)?;
                let arguments = serde_json::from_str::<Value>(&Self::required_text(
                    payload,
                    "arguments",
                    event_type,
                )?)
                .map_err(|error| {
                    SdkError::invalid_response(ResponseViolation::MalformedJson)
                        .with_response_details(ResponseDetails::json(&error))
                        .at_phase(ErrorPhase::Decode)
                        .with_message(format!(
                            "OpenAI interactive function arguments were invalid JSON: {error}"
                        ))
                        .with_diagnostic(CheckedRequestSpec::diagnostic_json(payload))
                })?;
                let call = FunctionCall::new(Some(call_id.clone()), name.as_str(), arguments)
                    .map_err(|error| {
                        SdkError::invalid_response(ResponseViolation::InvalidField {
                            field: "function_call",
                        })
                        .at_phase(ErrorPhase::SessionReceive)
                        .with_message("OpenAI returned an invalid function call")
                        .with_diagnostic(CheckedRequestSpec::diagnostic_json(payload))
                        .with_supplementary(error)
                    })?;
                if !self
                    .config
                    .functions()
                    .iter()
                    .any(|function| function.name() == name)
                {
                    return Err(
                        SdkError::invalid_response(ResponseViolation::ProtocolViolation {
                            rule: "declared_function",
                        })
                        .at_phase(ErrorPhase::SessionReceive)
                        .with_message(format!(
                            "OpenAI requested undeclared interactive function `{name}`"
                        ))
                        .with_diagnostic(CheckedRequestSpec::diagnostic_json(payload)),
                    );
                }
                if self.outstanding_calls.insert(call_id, name).is_some() {
                    return Err(
                        SdkError::invalid_response(ResponseViolation::ProtocolViolation {
                            rule: "unique_outstanding_call_id",
                        })
                        .at_phase(ErrorPhase::SessionReceive)
                        .with_message("OpenAI repeated an outstanding function-call id".to_string())
                        .with_diagnostic(CheckedRequestSpec::diagnostic_json(payload)),
                    );
                }
                self.function_response_pending = true;
                self.pending.push_back(SessionEvent::FunctionCall(call));
            }
            "response.usage.updated" => {
                let usage = payload.get("usage").cloned().ok_or_else(|| {
                    SdkError::invalid_response(ResponseViolation::MissingField { field: "usage" })
                        .at_phase(ErrorPhase::SessionReceive)
                        .with_message("OpenAI interactive usage event omitted usage".to_string())
                        .with_diagnostic(CheckedRequestSpec::diagnostic_json(payload))
                })?;
                let usage =
                    serde_json::from_value::<super::wire::OpenAiUsage>(usage).map_err(|error| {
                        SdkError::invalid_response(ResponseViolation::InvalidField {
                            field: "usage",
                        })
                        .with_response_details(ResponseDetails::json(&error))
                        .at_phase(ErrorPhase::Decode)
                        .with_message(format!("OpenAI interactive usage was invalid: {error}"))
                        .with_diagnostic(CheckedRequestSpec::diagnostic_json(payload))
                    })?;
                self.pending
                    .push_back(SessionEvent::Usage(usage_from_openai(Some(&usage))));
            }
            "response.done" => self.push_response_done(payload)?,
            "error" | "response.error" | "conversation.item.input_audio_transcription.failed" => {
                return Err(Self::provider_error(payload));
            }
            "session.created"
            | "session.updated"
            | "response.created"
            | "response.content_part.added"
            | "response.content_part.done"
            | "response.output_text.done"
            | "response.output_audio.done"
            | "response.output_audio_transcript.done"
            | "response.function_call_arguments.delta"
            | "input_audio_buffer.committed"
            | "input_audio_buffer.cleared"
            | "input_audio_buffer.speech_started"
            | "input_audio_buffer.speech_stopped"
            | "input_audio_buffer.timeout_triggered"
            | "rate_limits.updated"
            | "conversation.item.added"
            | "conversation.item.created"
            | "conversation.item.deleted"
            | "conversation.item.done"
            | "conversation.item.truncated"
            | "response.output_item.added"
            | "response.output_item.done" => {}
            other => {
                return Err(
                    SdkError::invalid_response(ResponseViolation::UnknownVariant { field: "type" })
                        .at_phase(ErrorPhase::SessionReceive)
                        .with_message(format!("unsupported OpenAI interactive event `{other}`"))
                        .with_diagnostic(CheckedRequestSpec::diagnostic_json(payload)),
                );
            }
        }
        Ok(())
    }

    fn push_response_done(&mut self, payload: &Value) -> Result<(), SdkError> {
        match payload
            .get("response")
            .and_then(|response| response.get("status"))
            .and_then(Value::as_str)
        {
            None | Some("completed") if self.function_response_pending => {
                self.function_response_pending = false;
            }
            None | Some("completed") if self.outstanding_calls.is_empty() => {
                self.pending.push_back(SessionEvent::Completed);
            }
            None | Some("completed") => {}
            Some("cancelled") => {
                self.function_response_pending = false;
                if !self.outstanding_calls.is_empty() {
                    self.pending
                        .push_back(SessionEvent::FunctionCallsCancelled {
                            call_ids: std::mem::take(&mut self.outstanding_calls)
                                .into_keys()
                                .collect(),
                        });
                }
                self.pending.push_back(SessionEvent::TurnCancelled);
            }
            Some("failed" | "incomplete") => return Err(Self::provider_error(payload)),
            Some(status) => {
                return Err(
                    SdkError::invalid_response(ResponseViolation::UnknownVariant {
                        field: "response.status",
                    })
                    .at_phase(ErrorPhase::SessionReceive)
                    .with_message(format!(
                        "OpenAI response.done used unknown status `{status}`"
                    ))
                    .with_diagnostic(CheckedRequestSpec::diagnostic_json(payload)),
                );
            }
        }
        Ok(())
    }

    fn required_text(
        payload: &Value,
        field: &'static str,
        event_type: &str,
    ) -> Result<String, SdkError> {
        payload
            .get(field)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .ok_or_else(|| {
                SdkError::invalid_response(ResponseViolation::MissingField { field })
                    .at_phase(ErrorPhase::SessionReceive)
                    .with_message(format!(
                        "OpenAI interactive event `{event_type}` omitted `{field}`"
                    ))
                    .with_diagnostic(CheckedRequestSpec::diagnostic_json(payload))
            })
    }

    fn provider_error(payload: &Value) -> SdkError {
        let error = payload
            .get("error")
            .or_else(|| payload.pointer("/response/status_details/error"))
            .unwrap_or(payload);
        let native = match serde_json::from_value::<OpenAiErrorObject>(error.clone()) {
            Ok(native) => native,
            Err(error) => {
                return SdkError::invalid_response(ResponseViolation::from_json_error(
                    &error, "error",
                ))
                .with_response_details(ResponseDetails::json(&error))
                .at_phase(ErrorPhase::Decode)
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(payload));
            }
        };
        let mut details = CheckedRequestSpec::diagnostic_json(payload).unwrap_or_default();
        details.event_id = payload
            .get("event_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        details.event_type = payload
            .get("type")
            .and_then(Value::as_str)
            .map(str::to_owned);
        match native.remote_failure(Some(details)) {
            Ok(failure) => {
                SdkError::from_remote(failure, Operation::Unspecified, ErrorPhase::SessionReceive)
            }
            Err(error) => error.at_phase(ErrorPhase::SessionReceive),
        }
    }

    async fn close_socket(mut self) -> Result<(), SdkError> {
        let deadline = self.socket.operation_deadline()?;
        self.closed = true;
        self.pending.clear();
        self.outstanding_calls.clear();
        self.function_response_pending = false;
        self.socket.close(deadline).await
    }
}

impl SessionBackend for OpenAiSession {
    fn id(&self) -> &str {
        &self.id
    }

    fn model_id(&self) -> &ModelId {
        &self.model_id
    }

    async fn send(&mut self, input: SessionInput) -> Result<(), SdkError> {
        let deadline = self.socket.operation_deadline()?;
        self.send_input(input, deadline)
            .await
            .map_err(|error| self.socket.attach_error(error))
    }

    async fn recv(&mut self) -> Result<SessionEvent, SdkError> {
        let deadline = self.socket.operation_deadline()?;
        self.receive_event(deadline)
            .await
            .map_err(|error| self.socket.attach_error(error))
    }

    async fn close(self) -> Result<(), SdkError> {
        self.close_socket().await
    }
}
