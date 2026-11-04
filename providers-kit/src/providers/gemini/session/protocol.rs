use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use base64::Engine as _;
use serde_json::{Map, Value, json};
use tokio::time::Instant;

use crate::KitError;
use crate::core::{SessionControl, SessionSocket, SessionSocketMessage};
use crate::error::{ErrorKind, ErrorPhase, Operation, ResponseViolation, ValidationViolation};
use crate::generation::{ContentPart, FunctionCall, FunctionResult, ProviderToolSpec, Usage};
use crate::models::ModelBackend;
use crate::provider::ProviderAdmission;
use crate::session::{
    CommandId, OutputSegmentId, ResponseId, ResponseOutcome, SessionCommand, SessionConfig,
    SessionContinuity, SessionEvent, SessionTurnDetection, ToolRequestId, TranscriptionSource,
    UsageScope,
};
use crate::transport::CheckedRequestSpec;

use super::super::api::GEMINI_REQUESTS;
use super::super::models::GeminiModel;

/// Owns the one native Live socket and its sequential, provider-local protocol.
pub(super) struct GeminiProtocol {
    socket: SessionSocket,
    generation: GeminiGeneration,
    tools: BTreeMap<String, ToolRequestId>,
    capture: GeminiCapture,
    command_started: bool,
    finished_turns: VecDeque<ResponseId>,
    input_transcript: GeminiTranscript,
    output_transcript: GeminiTranscript,
}

enum GeminiTranscript {
    Open(String),
    Finished,
}

impl GeminiTranscript {
    fn observation(payload: &Value) -> Result<Option<(&str, bool)>, KitError> {
        let object = payload.as_object().ok_or_else(|| {
            GeminiProtocol::protocol_error("Gemini Live transcription must be an object", payload)
        })?;
        let text = match object.get("text") {
            None | Some(Value::Null) => "",
            Some(Value::String(text)) => text,
            Some(_) => {
                return Err(GeminiProtocol::protocol_error(
                    "Gemini Live transcription text must be a string when supplied",
                    payload,
                ));
            }
        };
        let finished = match object.get("finished") {
            None | Some(Value::Null) => false,
            Some(Value::Bool(finished)) => *finished,
            Some(_) => {
                return Err(GeminiProtocol::protocol_error(
                    "Gemini Live transcription finished marker must be a boolean",
                    payload,
                ));
            }
        };
        Ok((!text.is_empty() || finished).then_some((text, finished)))
    }

    fn publish(
        &mut self,
        control: &SessionControl,
        source: TranscriptionSource,
        text: &str,
        finished: bool,
    ) -> Result<(), KitError> {
        if matches!(self, Self::Finished) {
            if matches!(source, TranscriptionSource::Input { command: None }) {
                // Native input has no utterance identifier. Every observation
                // after its final boundary begins a distinct unattributed input.
                *self = Self::Open(String::new());
            } else {
                return Err(KitError::invalid_response(
                    ResponseViolation::ProtocolViolation {
                        rule: "gemini_output_transcription_after_final",
                    },
                ));
            }
        }
        let Self::Open(buffer) = self else {
            return Err(KitError::invalid_state(
                "Gemini transcript ownership is closed",
            ));
        };
        control.reserve_transcription(text.len())?;
        buffer.push_str(text);
        if finished {
            let Self::Open(text) = std::mem::replace(self, Self::Finished) else {
                return Err(KitError::invalid_state("Gemini transcript lost its buffer"));
            };
            control.publish_transcription_snapshot(source, text)
        } else {
            control.publish(SessionEvent::Transcription {
                source,
                text: text.into(),
                finalized: false,
            })
        }
    }

    fn clear(&mut self, control: &SessionControl) -> Result<(), KitError> {
        match std::mem::replace(self, Self::Open(String::new())) {
            Self::Open(text) => control.release_transcription(text.len()),
            Self::Finished => Ok(()),
        }
    }
}

enum GeminiCapture {
    Idle,
    Admitted {
        admission: ProviderAdmission,
        deadline: Instant,
    },
    Submitted {
        command: CommandId,
        admission: ProviderAdmission,
        deadline: Instant,
    },
}

enum GeminiGeneration {
    Idle,
    Active {
        response: ResponseId,
        segment: OutputSegmentId,
        _admission: ProviderAdmission,
        deadline: Instant,
    },
    Settled {
        response: ResponseId,
        segment: OutputSegmentId,
    },
}

impl GeminiProtocol {
    pub(super) async fn open(
        model: &GeminiModel,
        config: &SessionConfig,
    ) -> Result<Self, KitError> {
        let setup = Self::setup(model, config)?;
        let api = model.api();
        let (mut socket, deadline) = SessionSocket::connect(
            api.live_request()?,
            api.transport().connect_timeout(),
            api.transport().timeout(),
            GEMINI_REQUESTS,
        )
        .await?;
        let handshake = async {
            socket.send(setup, deadline).await?;
            let payload = socket
                .receive_payload(deadline, "Gemini Live ended before setupComplete")
                .await?;
            if payload.get("setupComplete").is_some_and(Value::is_object)
                && payload.as_object().is_some_and(|object| object.len() == 1)
            {
                Ok(())
            } else {
                Err(
                    Self::protocol_error("Gemini Live did not acknowledge its one setup", &payload)
                        .at_phase(ErrorPhase::SessionHandshake),
                )
            }
        }
        .await;
        if let Err(error) = handshake {
            return Err(socket.fail_setup(error).await);
        }
        Ok(Self {
            socket,
            generation: GeminiGeneration::Idle,
            tools: BTreeMap::new(),
            capture: GeminiCapture::Idle,
            command_started: false,
            finished_turns: VecDeque::new(),
            input_transcript: GeminiTranscript::Open(String::new()),
            output_transcript: GeminiTranscript::Open(String::new()),
        })
    }

    fn setup(model: &GeminiModel, config: &SessionConfig) -> Result<Value, KitError> {
        if model.model_id().as_str() != "models/gemini-3.8-live" {
            return Err(Self::unsupported(
                "session.model",
                "Gemini Live requires exact models/gemini-3.8-live",
            ));
        }
        if config.output_audio_format() != Some("audio/pcm;rate=24000") {
            return Err(Self::unsupported(
                "session.output_audio",
                "Gemini Live requires PCM output at 24000 Hz",
            ));
        }
        if let Some(format) = config.input_audio_format()
            && format != "audio/pcm;rate=16000"
        {
            return Err(Self::unsupported(
                "session.input_audio",
                "Gemini Live input requires PCM at 16000 Hz",
            ));
        }
        if config.turn_detection() == SessionTurnDetection::Manual
            && config.input_audio_format().is_none()
        {
            return Err(Self::unsupported(
                "session.turn_detection",
                "manual Gemini Live activity requires configured audio input",
            ));
        }
        if config.response_format().is_some() || config.reasoning().is_some() {
            return Err(Self::unsupported(
                "session.generation",
                "Gemini Live does not implement the requested response-format or reasoning controls",
            ));
        }
        let requested = config.generation();
        if !requested.stop_sequences().is_empty()
            || requested.presence_penalty().is_some()
            || requested.frequency_penalty().is_some()
            || requested.seed().is_some()
            || requested.tool_choice().is_some()
        {
            return Err(Self::unsupported(
                "session.generation",
                "Gemini Live does not support stop, penalty, seed or tool-choice overrides",
            ));
        }
        let mut generation = Map::new();
        generation.insert("responseModalities".into(), json!(["AUDIO"]));
        if let Some(value) = requested.temperature() {
            generation.insert("temperature".into(), json!(value));
        }
        if let Some(value) = requested.top_p() {
            generation.insert("topP".into(), json!(value));
        }
        if let Some(value) = requested.top_k() {
            generation.insert("topK".into(), json!(value.get()));
        }
        if let Some(value) = requested.max_output_tokens() {
            generation.insert("maxOutputTokens".into(), json!(value.get()));
        }
        let mut setup = Map::new();
        setup.insert("model".into(), json!(model.model_id().as_str()));
        setup.insert("generationConfig".into(), Value::Object(generation));
        if let Some(instructions) = config.instructions() {
            setup.insert(
                "systemInstruction".into(),
                json!({"parts": [{"text": instructions}]}),
            );
        }
        if config.input_transcription() {
            setup.insert("inputAudioTranscription".into(), json!({}));
        }
        if config.output_transcription() {
            setup.insert("outputAudioTranscription".into(), json!({}));
        }
        if config.turn_detection() == SessionTurnDetection::Manual {
            setup.insert(
                "realtimeInputConfig".into(),
                json!({"automaticActivityDetection": {"disabled": true}}),
            );
        }
        let mut functions = Vec::new();
        for tool in config.tools() {
            match tool {
                ProviderToolSpec::Function(function) => {
                    let mut declaration = json!({"name": function.name(), "parametersJsonSchema": function.input_schema().schema(), "behavior": "BLOCKING"});
                    if let Some(description) = function.description() {
                        declaration["description"] = json!(description);
                    }
                    functions.push(declaration);
                }
                ProviderToolSpec::Hosted(_) => {
                    return Err(Self::unsupported(
                        "session.tools",
                        "Gemini Live hosted tools are not implemented",
                    ));
                }
            }
        }
        if !functions.is_empty() {
            setup.insert("tools".into(), json!([{"functionDeclarations": functions}]));
        }
        Ok(json!({"setup": setup}))
    }

    pub(super) async fn run(mut self, control: Arc<SessionControl>) -> Result<(), KitError> {
        let primary = self.drive(&control).await.map_err(|error| {
            if error.context().operation == Operation::Unspecified {
                error.at_operation(Operation::SessionReceive)
            } else {
                error
            }
        });
        let mut cleanup = match self.socket.operation_deadline() {
            Ok(deadline) => self.socket.close(deadline).await,
            Err(error) => Err(error),
        };
        for released in [
            self.input_transcript.clear(&control),
            self.output_transcript.clear(&control),
        ] {
            if let Err(error) = released {
                cleanup = Err(match cleanup {
                    Ok(()) => error,
                    Err(primary) => primary.with_supplementary(error),
                });
            }
        }
        let reason = match (&primary, &cleanup) {
            (Err(primary), Err(cleanup)) => primary.clone().with_supplementary(cleanup.clone()),
            (Err(primary), Ok(())) => primary.clone(),
            (Ok(()), Err(cleanup)) => cleanup.clone(),
            (Ok(()), Ok(())) => KitError::new(
                ErrorKind::Closed,
                Operation::SessionClose,
                ErrorPhase::Cleanup,
            )
            .with_message("Gemini Live session closed"),
        };
        self.generation = GeminiGeneration::Idle;
        self.capture = GeminiCapture::Idle;
        control.terminate(reason, SessionContinuity::Closed);
        // The primary protocol failure is delivered by the authoritative terminal;
        // consuming close reports only whether local cleanup itself completed.
        cleanup
    }

    async fn drive(&mut self, control: &Arc<SessionControl>) -> Result<(), KitError> {
        loop {
            let generation_deadline = match self.generation {
                GeminiGeneration::Active { deadline, .. } => Some(deadline),
                GeminiGeneration::Idle | GeminiGeneration::Settled { .. } => None,
            };
            let capture_deadline = match self.capture {
                GeminiCapture::Idle => None,
                GeminiCapture::Admitted { deadline, .. }
                | GeminiCapture::Submitted { deadline, .. } => Some(deadline),
            };
            let deadline = generation_deadline
                .into_iter()
                .chain(capture_deadline)
                .min();
            tokio::select! {
                biased;
                () = control.stopped() => return Ok(()),
                () = control.sealed(), if deadline.is_none() => return Ok(()),
                command = control.next_command() => {
                    let Some((id, command)) = command else { return Ok(()); };
                    if control.is_sealed() {
                        control.publish(SessionEvent::CommandFailed {
                            command: id,
                            error: KitError::new(ErrorKind::Closed, Operation::SessionSend, ErrorPhase::Lifecycle),
                            not_started: true,
                        })?;
                        continue;
                    }
                    self.command_started = false;
                    if let Err(error) = self.command(control, id.clone(), command).await {
                        control.publish(SessionEvent::CommandFailed { command:id, error:error.clone(), not_started:!self.command_started })?;
                        return Err(error);
                    }
                }
                received = async {
                    match deadline {
                        Some(deadline) => self.socket.receive(deadline).await,
                        None => self.socket.receive_unbounded().await,
                    }
                } => {
                    match received? {
                        SessionSocketMessage::Payload(payload) => self.observe(control, payload)?,
                        SessionSocketMessage::Closed(_) | SessionSocketMessage::Exhausted => {
                            return Err(KitError::new(ErrorKind::Closed, Operation::SessionReceive, ErrorPhase::SessionReceive)
                                .with_message("Gemini Live socket ended; unfinished responses remain unresolved"));
                        }
                    }
                }
            }
        }
    }

    async fn command(
        &mut self,
        control: &SessionControl,
        id: CommandId,
        command: SessionCommand,
    ) -> Result<(), KitError> {
        let mut work = None;
        let payload = match command {
            SessionCommand::Input(message) => {
                self.start(control, Some(id.clone()))?;
                let parts = message
                    .content()
                    .iter()
                    .map(|part| match part {
                        ContentPart::Text { text } => Ok(json!({"text": text})),
                        _ => Err(KitError::invalid_state(
                            "non-text Gemini Live input passed admission",
                        )),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                json!({"clientContent": {"turns": [{"role": "user", "parts": parts}], "turnComplete": true}})
            }
            SessionCommand::ToolResults(results) => {
                work = Some(control.generation_admission()?);
                if !matches!(self.generation, GeminiGeneration::Active { .. }) {
                    self.start(control, Some(id.clone()))?;
                }
                let mut responses = Vec::new();
                for result in results {
                    let native = result.result().call_id().ok_or_else(|| {
                        KitError::invalid_state("Gemini Live result has no native tool id")
                    })?;
                    if self.tools.remove(native).as_ref() != Some(result.request()) {
                        return Err(KitError::invalid_state(
                            "Gemini Live result lost its native tool request",
                        ));
                    }
                    responses.push(json!({"id": native, "name": result.result().name(), "response": Self::result_payload(result.result())}));
                }
                json!({"toolResponse": {"functionResponses": responses}})
            }
            SessionCommand::StartInput => {
                work = Some(control.generation_admission()?);
                json!({"realtimeInput": {"activityStart": {}}})
            }
            SessionCommand::EndInput => {
                if matches!(self.generation, GeminiGeneration::Active { .. }) {
                    if !matches!(self.capture, GeminiCapture::Idle) {
                        return Err(KitError::invalid_state(
                            "Gemini Live already has an unfinished input activity",
                        ));
                    }
                    // Native interruption may arrive only after activityEnd. The
                    // submitted input owns separate work until that previous
                    // generation settles and its own output can begin.
                    self.capture = GeminiCapture::Submitted {
                        command: id.clone(),
                        admission: control.generation_admission()?,
                        deadline: self.socket.operation_deadline()?,
                    };
                } else {
                    self.start(control, Some(id.clone()))?;
                }
                json!({"realtimeInput": {"activityEnd": {}}})
            }
            SessionCommand::Audio { media_type, data } => {
                let admission = control.generation_admission()?;
                if control.config().turn_detection() == SessionTurnDetection::Automatic
                    && matches!(self.capture, GeminiCapture::Idle)
                {
                    self.capture = GeminiCapture::Admitted {
                        admission,
                        deadline: self.socket.operation_deadline()?,
                    };
                } else {
                    work = Some(admission);
                }
                json!({"realtimeInput": {"audio": {"mimeType": media_type, "data": base64::engine::general_purpose::STANDARD.encode(data)}}})
            }
            SessionCommand::EndAudioStream => {
                work = Some(control.generation_admission()?);
                json!({"realtimeInput": {"audioStreamEnd": true}})
            }
            SessionCommand::Cancel(_)
            | SessionCommand::DiscardInput
            | SessionCommand::ReconcilePlayback { .. } => {
                return Err(KitError::invalid_state(
                    "unsupported Gemini Live command passed admission",
                ));
            }
        };
        let deadline = self.socket.operation_deadline()?;
        self.command_started = true;
        let sent = tokio::select! {
            biased;
            () = control.stopped() => return Ok(()),
            sent = self.socket.send(payload, deadline) => sent,
        };
        sent?;
        drop(work);
        control.publish(SessionEvent::ControlApplied { command: id })
    }

    fn result_payload(result: &FunctionResult) -> Value {
        let content = result
            .content()
            .iter()
            .map(|part| match part {
                ContentPart::Text { text } => Value::String(text.clone()),
                ContentPart::Json { value } => value.clone(),
                _ => Value::Null,
            })
            .collect::<Vec<_>>();
        let mut content = content.into_iter();
        let first = content.next().unwrap_or(Value::Null);
        let rest = content.collect::<Vec<_>>();
        let content = if rest.is_empty() {
            first
        } else {
            Value::Array(std::iter::once(first).chain(rest).collect())
        };
        if result.is_error() {
            json!({"error": content})
        } else if content.is_object() {
            content
        } else {
            json!({"result": content})
        }
    }

    fn start(
        &mut self,
        control: &SessionControl,
        command: Option<CommandId>,
    ) -> Result<ResponseId, KitError> {
        if let GeminiGeneration::Active { response, .. } = &self.generation {
            return Ok(response.clone());
        }
        let (command, admission, deadline) = if command.is_some() {
            (
                command,
                control.generation_admission()?,
                self.socket.operation_deadline()?,
            )
        } else {
            match std::mem::replace(&mut self.capture, GeminiCapture::Idle) {
                GeminiCapture::Admitted {
                    admission,
                    deadline,
                } => (None, admission, deadline),
                GeminiCapture::Submitted {
                    command,
                    admission,
                    deadline,
                } => (Some(command), admission, deadline),
                GeminiCapture::Idle => {
                    return Err(
                        KitError::invalid_response(ResponseViolation::ProtocolViolation {
                            rule: "gemini_live_generation_without_input",
                        })
                        .with_message("Gemini Live output has no new input or active generation"),
                    );
                }
            }
        };
        let response = control.next_response()?;
        let segment = control.next_segment()?;
        control.publish(SessionEvent::ResponseStarted {
            command,
            response: response.clone(),
        })?;
        self.generation = GeminiGeneration::Active {
            response: response.clone(),
            segment,
            _admission: admission,
            deadline,
        };
        Ok(response)
    }

    fn observe(&mut self, control: &SessionControl, payload: Value) -> Result<(), KitError> {
        let object = payload.as_object().ok_or_else(|| {
            Self::protocol_error("Gemini Live message must be an object", &payload)
        })?;
        // An empty server message carries no progress and does not refresh the
        // generation/capture deadlines retained by the driver.
        if object.is_empty() {
            return Ok(());
        }
        let native_members = [
            "setupComplete",
            "serverContent",
            "toolCall",
            "toolCallCancellation",
            "goAway",
            "sessionResumptionUpdate",
        ];
        let members = native_members
            .iter()
            .filter(|key| object.contains_key(**key))
            .count();
        if members > 1
            || (members == 0
                && !object.contains_key("usageMetadata")
                && !object.contains_key("voiceActivity"))
        {
            return Err(Self::protocol_error(
                "Gemini Live message has an invalid server union",
                &payload,
            ));
        }
        if let Some(activity) = object.get("voiceActivity") {
            let kind = activity.get("type");
            let offset = activity.get("audioOffset");
            if !activity.is_object()
                || kind.is_some_and(|kind| {
                    !kind.is_null()
                        && !matches!(
                            kind.as_str(),
                            Some("TYPE_UNSPECIFIED" | "ACTIVITY_START" | "ACTIVITY_END")
                        )
                })
                || offset.is_some_and(|offset| !offset.is_null() && !offset.is_string())
            {
                return Err(Self::protocol_error(
                    "Gemini Live input voice-activity telemetry is malformed",
                    &payload,
                ));
            }
            // Input VAD telemetry carries no command/response identity. It is
            // neither an acknowledgement of activity commands nor a native
            // interaction turn boundary, so it changes no portable state.
        }
        if let Some(usage) = object.get("usageMetadata") {
            self.usage(control, usage)?;
        }
        if let Some(call) = object.get("toolCall") {
            self.tool_calls(control, call)?;
        }
        if let Some(cancelled) = object.get("toolCallCancellation") {
            self.cancelled_tools(control, cancelled)?;
        }
        if let Some(content) = object.get("serverContent") {
            self.content(control, content)?;
        }
        if object.contains_key("setupComplete") {
            return Err(Self::protocol_error(
                "Gemini Live sent an unsolicited setup acknowledgement",
                &payload,
            ));
        }
        // Gemini can emit this known auxiliary message without resumption
        // configuration. Validate its shape without retaining a handle,
        // changing continuity, or enabling automatic reconnection.
        if let Some(update) = object.get("sessionResumptionUpdate")
            && (!update.is_object()
                || update
                    .get("newHandle")
                    .is_some_and(|value| !value.is_string())
                || update
                    .get("resumable")
                    .is_some_and(|value| !value.is_boolean()))
        {
            return Err(Self::protocol_error(
                "Gemini Live resumption metadata has invalid field types",
                &payload,
            ));
        }
        if object.contains_key("goAway") {
            return Err(KitError::new(
                ErrorKind::Closed,
                Operation::SessionReceive,
                ErrorPhase::SessionReceive,
            )
            .with_message(
                "Gemini Live announced session expiration; automatic reconnection is disabled",
            ));
        }
        Ok(())
    }

    fn tool_calls(&mut self, control: &SessionControl, payload: &Value) -> Result<(), KitError> {
        let calls = payload
            .get("functionCalls")
            .and_then(Value::as_array)
            .filter(|calls| !calls.is_empty())
            .ok_or_else(|| {
                Self::protocol_error("Gemini Live toolCall requires functionCalls", payload)
            })?;
        let response = self.start(control, None)?;
        for native in calls {
            let id = native
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .ok_or_else(|| {
                    Self::protocol_error("Gemini Live function call requires native id", native)
                })?;
            if self.tools.contains_key(id) {
                return Err(Self::protocol_error(
                    "Gemini Live repeated an outstanding native tool id",
                    native,
                ));
            }
            let name = native.get("name").and_then(Value::as_str).ok_or_else(|| {
                Self::protocol_error("Gemini Live function call requires name", native)
            })?;
            let arguments = native
                .get("args")
                .filter(|value| value.is_object())
                .ok_or_else(|| {
                    Self::protocol_error(
                        "Gemini Live function call requires object arguments",
                        native,
                    )
                })?;
            if !control.config().tools().iter().any(|tool| matches!(tool, ProviderToolSpec::Function(function) if function.name() == name)) {
                return Err(Self::protocol_error("Gemini Live requested an undeclared function", native));
            }
            let call =
                FunctionCall::new(Some(id.into()), name, arguments.clone()).map_err(|cause| {
                    Self::protocol_error(
                        "Gemini Live function call failed portable validation",
                        native,
                    )
                    .with_cause(cause)
                })?;
            let request = control.next_tool()?;
            control.publish(SessionEvent::ToolRequested {
                response: response.clone(),
                request: request.clone(),
                call: call.clone(),
            })?;
            control.reserve_history(
                id.len()
                    .checked_add(std::mem::size_of::<ToolRequestId>())
                    .ok_or_else(|| {
                        KitError::invalid_state("Gemini Live tool identity size overflow")
                    })?,
            )?;
            self.tools.insert(id.into(), request);
        }
        Ok(())
    }

    fn cancelled_tools(
        &mut self,
        control: &SessionControl,
        payload: &Value,
    ) -> Result<(), KitError> {
        let ids = payload
            .get("ids")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                Self::protocol_error("Gemini Live tool cancellation requires ids", payload)
            })?;
        let mut requests = Vec::new();
        for id in ids {
            let id = id.as_str().ok_or_else(|| {
                Self::protocol_error("Gemini Live cancelled tool id must be a string", payload)
            })?;
            if let Some(request) = self.tools.remove(id) {
                requests.push(request);
            }
        }
        if !requests.is_empty() {
            control.publish(SessionEvent::ToolsInvalidated { requests })?;
        }
        Ok(())
    }

    fn content(&mut self, control: &SessionControl, content: &Value) -> Result<(), KitError> {
        if !content.is_object() {
            return Err(Self::protocol_error(
                "Gemini Live serverContent must be an object",
                content,
            ));
        }
        for field in [
            "generationComplete",
            "turnComplete",
            "interrupted",
            "waitingForInput",
        ] {
            if content
                .get(field)
                .is_some_and(|value| !value.is_null() && !value.is_boolean())
            {
                return Err(Self::protocol_error(
                    "Gemini Live server-content boundary flags must be booleans",
                    content,
                ));
            }
        }
        if let Some(transcription) = content.get("inputTranscription")
            && let Some((text, finished)) = GeminiTranscript::observation(transcription)?
        {
            // Gemini explicitly provides no ordering/correlation between this
            // stream and model turns. Never attach it to the current response.
            self.input_transcript.publish(
                control,
                TranscriptionSource::Input { command: None },
                text,
                finished,
            )?;
        }
        if let Some(turn) = content.get("modelTurn") {
            let response = self.start(control, None)?;
            let segment = self.segment()?.clone();
            let parts = turn.get("parts").and_then(Value::as_array).ok_or_else(|| {
                Self::protocol_error("Gemini Live modelTurn requires parts", turn)
            })?;
            for part in parts {
                if let Some(text) = part.get("text").and_then(Value::as_str) {
                    control.publish(SessionEvent::Output {
                        response: response.clone(),
                        segment: Some(segment.clone()),
                        content: ContentPart::Text { text: text.into() },
                    })?;
                } else if let Some(audio) = part.get("inlineData") {
                    let media_type = audio
                        .get("mimeType")
                        .and_then(Value::as_str)
                        .filter(|format| *format == "audio/pcm;rate=24000")
                        .ok_or_else(|| {
                            Self::protocol_error(
                                "Gemini Live output audio must be PCM at 24000 Hz",
                                audio,
                            )
                        })?;
                    let data = audio.get("data").and_then(Value::as_str).ok_or_else(|| {
                        Self::protocol_error("Gemini Live output audio requires base64 data", audio)
                    })?;
                    let data = base64::engine::general_purpose::STANDARD
                        .decode(data)
                        .map_err(|_| {
                            Self::protocol_error(
                                "Gemini Live output audio contains invalid base64",
                                audio,
                            )
                        })?;
                    control.publish(SessionEvent::Output {
                        response: response.clone(),
                        segment: Some(segment.clone()),
                        content: ContentPart::Audio {
                            media_type: media_type.into(),
                            data,
                        },
                    })?;
                } else {
                    return Err(Self::protocol_error(
                        "Gemini Live model part has no supported output",
                        part,
                    ));
                }
            }
        }
        if let Some(transcription) = content.get("outputTranscription")
            && let Some((text, finished)) = GeminiTranscript::observation(transcription)?
        {
            let response = self.start(control, None)?;
            let segment = self.segment()?.clone();
            self.output_transcript.publish(
                control,
                TranscriptionSource::Output { response, segment },
                text,
                finished,
            )?;
        }
        if content.get("interrupted").and_then(Value::as_bool) == Some(true) {
            let response = self
                .finished_turns
                .front()
                .cloned()
                .or_else(|| self.response_id().cloned());
            control.publish(SessionEvent::Interruption {
                response: response.clone(),
            })?;
            if let GeminiGeneration::Active {
                response: active, ..
            } = &self.generation
                && response.as_ref() == Some(active)
            {
                control.invalidate_response(active)?;
                self.tools.clear();
                self.finish(control, ResponseOutcome::Interrupted)?;
            }
        } else if content.get("generationComplete").and_then(Value::as_bool) == Some(true) {
            self.finish(control, ResponseOutcome::Completed)?;
        }
        // turnComplete describes interaction/activity, never affirmative model
        // generation completion. It cannot manufacture a successful response.
        if content.get("turnComplete").and_then(Value::as_bool) == Some(true) {
            let response = self
                .finished_turns
                .pop_front()
                .or_else(|| self.response_id().cloned());
            control.publish(SessionEvent::TurnEnded { response })?;
        }
        Ok(())
    }

    fn response_id(&self) -> Option<&ResponseId> {
        match &self.generation {
            GeminiGeneration::Active { response, .. }
            | GeminiGeneration::Settled { response, .. } => Some(response),
            GeminiGeneration::Idle => None,
        }
    }

    fn segment(&self) -> Result<&OutputSegmentId, KitError> {
        match &self.generation {
            GeminiGeneration::Active { segment, .. }
            | GeminiGeneration::Settled { segment, .. } => Ok(segment),
            GeminiGeneration::Idle => {
                Err(KitError::invalid_state("Gemini Live has no output segment"))
            }
        }
    }

    fn finish(
        &mut self,
        control: &SessionControl,
        outcome: ResponseOutcome,
    ) -> Result<(), KitError> {
        match &self.generation {
            GeminiGeneration::Active {
                response, segment, ..
            } => {
                let response = response.clone();
                let segment = segment.clone();
                control.reserve_history(
                    response
                        .session_id()
                        .len()
                        .checked_add(std::mem::size_of::<ResponseId>())
                        .ok_or_else(|| {
                            KitError::invalid_state("Gemini Live boundary identity size overflow")
                        })?,
                )?;
                self.output_transcript.clear(control)?;
                control.publish(SessionEvent::ResponseFinished {
                    response: response.clone(),
                    outcome,
                    continuity: SessionContinuity::Reusable,
                    output: None,
                })?;
                self.finished_turns.push_back(response.clone());
                self.generation = GeminiGeneration::Settled { response, segment };
                Ok(())
            }
            GeminiGeneration::Settled { .. } => Err(KitError::invalid_response(
                ResponseViolation::ProtocolViolation {
                    rule: "duplicate_gemini_generation_terminal",
                },
            )),
            GeminiGeneration::Idle => Err(KitError::invalid_response(
                ResponseViolation::ProtocolViolation {
                    rule: "generation_terminal_without_response",
                },
            )),
        }
    }

    fn usage(&self, control: &SessionControl, metadata: &Value) -> Result<(), KitError> {
        let metadata = metadata
            .as_object()
            .ok_or_else(|| Self::protocol_error("Gemini Live usage must be an object", metadata))?;
        let count = |name: &str| -> Result<Option<u64>, KitError> {
            metadata
                .get(name)
                .map(|value| {
                    value.as_u64().ok_or_else(|| {
                        KitError::invalid_response(ResponseViolation::InvalidField {
                            field: "usageMetadata",
                        })
                    })
                })
                .transpose()
        };
        let usage =
            Usage::reported_tokens(count("promptTokenCount")?, count("responseTokenCount")?)
                .with_reported_total(count("totalTokenCount")?)
                .with_cached_input(count("cachedContentTokenCount")?)
                .with_reasoning(count("thoughtsTokenCount")?);
        control.publish(SessionEvent::Usage {
            scope: UsageScope::SessionSnapshot,
            usage,
            finalized: false,
        })
    }

    pub(super) fn unsupported(field: &'static str, message: &'static str) -> KitError {
        KitError::unsupported(
            ValidationViolation::Unsupported {
                field,
                rule: message,
            },
            message,
        )
    }

    fn protocol_error(message: &'static str, payload: &Value) -> KitError {
        KitError::invalid_response(ResponseViolation::ProtocolViolation {
            rule: "gemini_live_envelope",
        })
        .at_operation(Operation::SessionReceive)
        .at_phase(ErrorPhase::SessionReceive)
        .with_message(message)
        .with_diagnostic(CheckedRequestSpec::diagnostic_json(payload))
    }
}
