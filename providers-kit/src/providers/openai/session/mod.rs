use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use base64::Engine as _;
use serde_json::{Value, json};
use tokio::time::Instant;

use crate::KitError;
use crate::core::{
    ManagedCommands, ManagedConversation, SessionControl, SessionEventPort, SessionSocket,
    SessionSocketMessage, tool_result_text,
};
use crate::error::{
    ErrorKind, ErrorPhase, Operation, ResponseDetails, ResponseViolation, ValidationViolation,
};
use crate::generation::{
    ContentPart, FinishReason, FunctionCall, ProviderToolSpec, ResponseFormat, Role, ToolChoice,
};
use crate::models::{ModelBackend, ModelId};
use crate::provider::{ProviderAdmission, ProviderRuntime};
use crate::session::{
    CancelTarget, CancellationResolution, CommandId, OutputSegmentId, ResponseId, ResponseOutcome,
    SessionBackend, SessionCommand, SessionCommands, SessionConfig, SessionContinuity,
    SessionEvent, SessionProfile, SessionTurnDetection, ToolRequestId, TranscriptionSource,
    UsageScope,
};
use crate::transport::CheckedRequestSpec;

use super::api::OPENAI_REQUESTS;
use super::models::OpenAiModel;
use super::responses::usage_from_openai;
use super::wire::OpenAiErrorObject;

/// Exact selected OpenAI model's stateful child.
pub(crate) struct OpenAiSession {
    model_id: ModelId,
    mode: OpenAiMode,
}

enum OpenAiMode {
    Conversation(ManagedConversation<OpenAiModel>),
    Realtime(OpenAiRealtime),
}

struct OpenAiRealtime {
    control: Arc<SessionControl>,
    engine: OpenAiEngine,
    capturing: bool,
    segments: Arc<Mutex<BTreeMap<OutputSegmentId, OpenAiSegment>>>,
}

enum OpenAiEngine {
    Prepared(Box<OpenAiDriver>),
    Running,
}

pub(crate) struct OpenAiCommands<'a> {
    mode: OpenAiCommandMode<'a>,
}

enum OpenAiCommandMode<'a> {
    Conversation(ManagedCommands<'a, OpenAiModel>),
    Realtime {
        control: &'a Arc<SessionControl>,
        capturing: &'a mut bool,
        segments: &'a Arc<Mutex<BTreeMap<OutputSegmentId, OpenAiSegment>>>,
    },
}

/// Socket and native correlation stay within this independently mapped protocol owner.
struct OpenAiDriver {
    socket: SessionSocket,
    control: Arc<SessionControl>,
    timeout: Duration,
    pending: BTreeMap<CommandId, OpenAiPending>,
    responses: BTreeMap<String, OpenAiResponse>,
    segments: Arc<Mutex<BTreeMap<OutputSegmentId, OpenAiSegment>>>,
    calls: BTreeMap<ToolRequestId, OpenAiCall>,
    input_commands: BTreeMap<String, CommandId>,
    capture: Option<CommandId>,
    capture_work: Option<OpenAiPending>,
    continuation: Option<CommandId>,
    acknowledgements: BTreeMap<String, VecDeque<(CommandId, Instant)>>,
    command_started: bool,
}

struct OpenAiPending {
    admission: ProviderAdmission,
    deadline: Instant,
    cancellations: Vec<(CommandId, CancelTarget)>,
}

struct OpenAiResponse {
    id: ResponseId,
    command: Option<CommandId>,
    admission: Option<ProviderAdmission>,
    deadline: Instant,
    settled: bool,
    cancellations: Vec<(CommandId, CancelTarget)>,
    calls: Vec<(ToolRequestId, FunctionCall)>,
}

struct OpenAiSegment {
    response: ResponseId,
    item: String,
    content_index: u64,
}

struct OpenAiCall {
    response: ResponseId,
    native_id: String,
    settled: bool,
}

impl OpenAiSession {
    pub(crate) async fn open(model: &OpenAiModel, config: SessionConfig) -> Result<Self, KitError> {
        let timeout = model.api().transport().timeout();
        let mode = match config.profile() {
            SessionProfile::Conversation => {
                OpenAiMode::Conversation(ManagedConversation::new(model.clone(), config, timeout)?)
            }
            SessionProfile::Realtime => {
                Self::validate_model(model.model_id())?;
                Self::validate_config(&config)?;
                let update = Self::session_update_payload(&config)?;
                let control = SessionControl::new(config, timeout)?;
                let api = model.api();
                let (mut socket, deadline) = SessionSocket::connect(
                    api.live_request(model.model_id().as_str())?,
                    api.transport().connect_timeout(),
                    timeout,
                    OPENAI_REQUESTS,
                )
                .await?;
                let setup = async {
                    let expected_modalities = update["session"]["output_modalities"].clone();
                    socket.send(update, deadline).await?;
                    let mut created = false;
                    loop {
                        let payload = socket.receive_payload(deadline, "OpenAI websocket ended during interactive setup").await?;
                        match payload.get("type").and_then(Value::as_str) {
                            Some("session.created") if !created => created = true,
                            Some("session.updated") => {
                                if !payload.get("session").is_some_and(|session| session["type"] == "realtime" && session["model"] == model.model_id().as_str() && session["output_modalities"] == expected_modalities) {
                                    return Err(OpenAiDriver::protocol("session_configuration_acknowledgement", "OpenAI session.updated did not acknowledge selected model and modalities").at_phase(ErrorPhase::SessionHandshake).with_diagnostic(CheckedRequestSpec::diagnostic_json(&payload)));
                                }
                                break;
                            }
                            Some("error" | "response.error") => return Err(OpenAiDriver::provider_error(&payload).at_phase(ErrorPhase::SessionHandshake)),
                            _ => return Err(KitError::invalid_response(ResponseViolation::UnknownVariant {field: "setup.event"}).at_phase(ErrorPhase::SessionHandshake).with_diagnostic(CheckedRequestSpec::diagnostic_json(&payload))),
                        }
                    }
                    Ok(())
                }.await;
                if let Err(error) = setup {
                    return Err(socket.fail_setup(error).await);
                }
                let segments = Arc::new(Mutex::new(BTreeMap::new()));
                let driver = OpenAiDriver {
                    socket,
                    control: Arc::clone(&control),
                    timeout,
                    pending: BTreeMap::new(),
                    responses: BTreeMap::new(),
                    segments: Arc::clone(&segments),
                    calls: BTreeMap::new(),
                    input_commands: BTreeMap::new(),
                    capture: None,
                    capture_work: None,
                    continuation: None,
                    acknowledgements: BTreeMap::new(),
                    command_started: false,
                };
                OpenAiMode::Realtime(OpenAiRealtime {
                    control,
                    engine: OpenAiEngine::Prepared(Box::new(driver)),
                    capturing: false,
                    segments,
                })
            }
        };
        Ok(Self {
            model_id: model.model_id().clone(),
            mode,
        })
    }

    pub(crate) fn bind_provider(&mut self, provider: Arc<ProviderRuntime>) -> Result<(), KitError> {
        match &mut self.mode {
            OpenAiMode::Conversation(owner) => owner.bind_provider(provider),
            OpenAiMode::Realtime(owner) => {
                owner.control.bind_provider(provider)?;
                match std::mem::replace(&mut owner.engine, OpenAiEngine::Running) {
                    OpenAiEngine::Prepared(driver) => owner.control.spawn(driver.run()),
                    OpenAiEngine::Running => {
                        Err(KitError::invalid_state("OpenAI session already bound"))
                    }
                }
            }
        }
    }

    fn validate_model(model: &ModelId) -> Result<(), KitError> {
        match model.as_str() {
            "gpt-realtime"
            | "gpt-realtime-mini"
            | "gpt-realtime-1.5"
            | "gpt-realtime-2"
            | "gpt-realtime-2.1"
            | "gpt-realtime-2.1-mini" => Ok(()),
            _ => Err(Self::unsupported(
                "session.model",
                "selected model does not support OpenAI Realtime",
            )),
        }
    }

    fn unsupported(field: &'static str, rule: &'static str) -> KitError {
        KitError::unsupported(ValidationViolation::Unsupported { field, rule }, rule)
    }

    fn validate_config(config: &SessionConfig) -> Result<(), KitError> {
        if let Some(format) = config.input_audio_format() {
            Self::audio_format(format)?;
        }
        if let Some(format) = config.output_audio_format() {
            Self::audio_format(format)?;
        }
        if config.turn_detection() == SessionTurnDetection::Manual
            && config.input_audio_format().is_none()
        {
            return Err(KitError::invalid_input(
                ValidationViolation::MissingField {
                    field: "session.input_audio_format",
                },
                "manual input requires audio configuration",
            ));
        }
        let generation = config.generation();
        if generation.temperature().is_some()
            || generation.top_p().is_some()
            || generation.top_k().is_some()
            || generation.presence_penalty().is_some()
            || generation.frequency_penalty().is_some()
            || generation.seed().is_some()
            || !generation.stop_sequences().is_empty()
        {
            return Err(Self::unsupported(
                "session.generation",
                "OpenAI Realtime does not support requested sampling settings",
            ));
        }
        if config.reasoning().is_some()
            || !matches!(config.response_format(), None | Some(ResponseFormat::Text))
        {
            return Err(Self::unsupported(
                "session.generation",
                "OpenAI Realtime does not support requested reasoning or response format",
            ));
        }
        if config
            .tools()
            .iter()
            .any(|tool| matches!(tool, ProviderToolSpec::Hosted(_)))
        {
            return Err(Self::unsupported(
                "session.tools",
                "OpenAI Realtime supports function tools only",
            ));
        }
        if matches!(generation.tool_choice(), Some(ToolChoice::Allowed { .. })) {
            return Err(Self::unsupported(
                "session.tool_choice",
                "OpenAI Realtime allowed-function subsets are unavailable",
            ));
        }
        Ok(())
    }

    fn audio_format(media_type: &str) -> Result<Value, KitError> {
        match media_type {
            "audio/pcm" | "audio/pcm;rate=24000" => Ok(json!({"type":"audio/pcm","rate":24_000})),
            "audio/pcmu" => Ok(json!({"type":"audio/pcmu"})),
            "audio/pcma" => Ok(json!({"type":"audio/pcma"})),
            _ => Err(Self::unsupported(
                "session.audio_format",
                "unsupported OpenAI Realtime audio format",
            )),
        }
    }

    fn session_update_payload(config: &SessionConfig) -> Result<Value, KitError> {
        let tools = config.tools().iter().filter_map(|tool| match tool {
            ProviderToolSpec::Function(function) => Some(json!({"type":"function","name":function.name(),"description":function.description(),"parameters":function.input_schema().schema()})),
            ProviderToolSpec::Hosted(_) => None,
        }).collect::<Vec<_>>();
        let mut session = json!({"type":"realtime","output_modalities":if config.output_audio_format().is_some() {json!(["audio"])} else {json!(["text"])}});
        if let Some(instructions) = config.instructions() {
            session["instructions"] = json!(instructions);
        }
        if let Some(maximum) = config.generation().max_output_tokens() {
            session["max_output_tokens"] = json!(maximum.get());
        }
        if !tools.is_empty() {
            session["tools"] = json!(tools);
        }
        if let Some(choice) = config.generation().tool_choice() {
            session["tool_choice"] = match choice {
                ToolChoice::Auto => json!("auto"),
                ToolChoice::None => json!("none"),
                ToolChoice::Required | ToolChoice::Any => json!("required"),
                ToolChoice::Tool { name } => json!({"type":"function","name":name}),
                ToolChoice::Allowed { .. } => {
                    return Err(Self::unsupported(
                        "session.tool_choice",
                        "allowed-function subsets unavailable",
                    ));
                }
            };
        }
        let mut audio = serde_json::Map::new();
        if let Some(format) = config.input_audio_format() {
            let mut input = json!({"format":Self::audio_format(format)?,"turn_detection":match config.turn_detection(){SessionTurnDetection::Automatic=>json!({"type":"server_vad"}),SessionTurnDetection::Manual=>Value::Null}});
            if config.input_transcription() {
                input["transcription"] = json!({"model":"gpt-transcribe"});
            }
            audio.insert("input".to_string(), input);
        }
        if let Some(format) = config.output_audio_format() {
            audio.insert(
                "output".to_string(),
                json!({"format":Self::audio_format(format)?}),
            );
        }
        if !audio.is_empty() {
            session["audio"] = Value::Object(audio);
        }
        Ok(json!({"type":"session.update","session":session}))
    }
}

impl SessionBackend for OpenAiSession {
    type Commands<'a> = OpenAiCommands<'a>;
    type Events<'a> = SessionEventPort<'a>;
    fn id(&self) -> &str {
        match &self.mode {
            OpenAiMode::Conversation(owner) => owner.control().id(),
            OpenAiMode::Realtime(owner) => owner.control.id(),
        }
    }
    fn model_id(&self) -> &ModelId {
        &self.model_id
    }
    fn split(&mut self) -> (Self::Commands<'_>, Self::Events<'_>) {
        match &mut self.mode {
            OpenAiMode::Conversation(owner) => {
                let (commands, events) = owner.split();
                (
                    OpenAiCommands {
                        mode: OpenAiCommandMode::Conversation(commands),
                    },
                    events,
                )
            }
            OpenAiMode::Realtime(owner) => (
                OpenAiCommands {
                    mode: OpenAiCommandMode::Realtime {
                        control: &owner.control,
                        capturing: &mut owner.capturing,
                        segments: &owner.segments,
                    },
                },
                SessionEventPort::new(&owner.control),
            ),
        }
    }
    async fn close(self) -> Result<(), KitError> {
        match self.mode {
            OpenAiMode::Conversation(owner) => owner.close().await,
            OpenAiMode::Realtime(owner) => owner.control.close().await,
        }
    }
}

impl Drop for OpenAiRealtime {
    fn drop(&mut self) {
        self.control.stop();
    }
}

impl SessionCommands for OpenAiCommands<'_> {
    async fn submit(&mut self, command: SessionCommand) -> Result<CommandId, KitError> {
        match &mut self.mode {
            OpenAiCommandMode::Conversation(commands) => commands.submit(command).await,
            OpenAiCommandMode::Realtime {
                control,
                capturing,
                segments,
            } => {
                let config = control.config();
                match &command {
                    SessionCommand::Input(message) => {
                        if message.role() != Role::User
                            || message
                                .content()
                                .iter()
                                .any(|part| !matches!(part, ContentPart::Text { .. }))
                        {
                            return Err(OpenAiSession::unsupported(
                                "session.input",
                                "OpenAI Realtime Input requires user text; use Audio for capture",
                            ));
                        }
                    }
                    SessionCommand::Audio { media_type, data } => {
                        if config.input_audio_format() != Some(media_type.as_str()) {
                            return Err(KitError::invalid_input(
                                ValidationViolation::Incompatible {
                                    field: "session.input.audio.media_type",
                                    other: "session.input_audio_format",
                                },
                                "audio format does not match session",
                            ));
                        }
                        let encoded = data
                            .len()
                            .checked_add(2)
                            .and_then(|size| size.checked_div(3))
                            .and_then(|size| size.checked_mul(4))
                            .and_then(|size| size.checked_add(64))
                            .ok_or_else(|| KitError::invalid_state("audio size overflow"))?;
                        SessionSocket::validate_message_bytes(encoded)?;
                        if data.is_empty() {
                            return Err(KitError::invalid_input(
                                ValidationViolation::Empty {
                                    field: "session.input.audio.data",
                                },
                                "audio input must not be empty",
                            ));
                        }
                        if config.turn_detection() == SessionTurnDetection::Manual && !**capturing {
                            return Err(KitError::invalid_state(
                                "manual audio requires StartInput",
                            ));
                        }
                    }
                    SessionCommand::StartInput => {
                        if config.turn_detection() != SessionTurnDetection::Manual || **capturing {
                            return Err(KitError::invalid_state(
                                "StartInput requires idle manual capture",
                            ));
                        }
                    }
                    SessionCommand::EndInput => {
                        if config.turn_detection() != SessionTurnDetection::Manual || !**capturing {
                            return Err(KitError::invalid_state(
                                "EndInput requires manual capture",
                            ));
                        }
                    }
                    SessionCommand::EndAudioStream => {
                        return Err(OpenAiSession::unsupported(
                            "session.command",
                            "OpenAI has no separate end-of-audio-stream operation",
                        ));
                    }
                    SessionCommand::DiscardInput => {
                        if config.input_audio_format().is_none() {
                            return Err(OpenAiSession::unsupported(
                                "session.command",
                                "input discard requires audio capture",
                            ));
                        }
                    }
                    SessionCommand::ToolResults(results) => {
                        for result in results {
                            tool_result_text(result.result().content())?;
                        }
                    }
                    SessionCommand::ReconcilePlayback { segment, played } => {
                        if segment.session_id() != control.id() {
                            return Err(KitError::invalid_state("foreign playback segment"));
                        }
                        if !segments
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .contains_key(segment)
                        {
                            return Err(KitError::invalid_state(
                                "playback segment was not observed from this session",
                            ));
                        }
                        u64::try_from(played.as_millis()).map_err(|_| {
                            KitError::invalid_state("playback duration exceeds native range")
                        })?;
                        if config.output_audio_format().is_none() {
                            return Err(OpenAiSession::unsupported(
                                "session.command",
                                "playback reconciliation requires audio output",
                            ));
                        }
                    }
                    SessionCommand::Cancel(_) => {}
                }
                let capture = match &command {
                    SessionCommand::StartInput => Some(true),
                    SessionCommand::EndInput | SessionCommand::DiscardInput => Some(false),
                    _ => None,
                };
                let receipt = control.accept(command)?;
                if let Some(capture) = capture {
                    **capturing = capture;
                }
                Ok(receipt)
            }
        }
    }
}

impl OpenAiDriver {
    async fn run(mut self: Box<Self>) -> Result<(), KitError> {
        let outcome = self.drive().await.map_err(|error| {
            let error = if error.context().operation == Operation::Unspecified {
                error.at_operation(Operation::SessionReceive)
            } else {
                error
            };
            self.socket.attach_error(error)
        });
        let cleanup = match self.socket.operation_deadline() {
            Ok(deadline) => self.socket.close(deadline).await,
            Err(error) => Err(error),
        };
        let reason = match outcome {
            Err(error) => match &cleanup {
                Ok(()) => error,
                Err(cleanup) => error.with_supplementary(cleanup.clone()),
            },
            Ok(()) => match &cleanup {
                Ok(()) => KitError::closed("OpenAI session closed"),
                Err(error) => error.clone(),
            },
        };
        // A missing stop acknowledgement is never promoted to a successful cancellation.
        for response in self.responses.values_mut() {
            for (command, target) in std::mem::take(&mut response.cancellations) {
                let _ = self.control.publish(SessionEvent::Cancellation {
                    command,
                    target,
                    resolution: CancellationResolution::Unconfirmed(reason.clone()),
                    continuity: SessionContinuity::Uncertain,
                });
            }
        }
        for pending in self.pending.values_mut() {
            for (command, target) in std::mem::take(&mut pending.cancellations) {
                let _ = self.control.publish(SessionEvent::Cancellation {
                    command,
                    target,
                    resolution: CancellationResolution::Unconfirmed(reason.clone()),
                    continuity: SessionContinuity::Uncertain,
                });
            }
        }
        self.control.terminate(reason, SessionContinuity::Closed);
        cleanup
    }

    async fn drive(&mut self) -> Result<(), KitError> {
        let control = Arc::clone(&self.control);
        let mut sealed = false;
        loop {
            if sealed
                && self.pending.is_empty()
                && self.capture_work.is_none()
                && self.acknowledgements.values().all(VecDeque::is_empty)
                && self.responses.values().all(|response| response.settled)
            {
                return Ok(());
            }
            let deadline = self
                .pending
                .values()
                .map(|pending| pending.deadline)
                .chain(self.capture_work.as_ref().map(|work| work.deadline))
                .chain(
                    self.acknowledgements
                        .values()
                        .flat_map(|commands| commands.iter().map(|(_, deadline)| *deadline)),
                )
                .chain(
                    self.responses
                        .values()
                        .filter(|response| !response.settled)
                        .map(|response| response.deadline),
                )
                .min();
            tokio::select! {
                biased;
                ()=control.stopped()=>return Ok(()),
                ()=control.sealed(),if !sealed=>{sealed=true;},
                command=control.next_command()=>{
                    let Some((id,command))=command else{return Ok(());};
                    self.command_started=false;
                    if let Err(error)=self.command(id.clone(),command).await {
                        let error=if error.context().operation==Operation::Unspecified {error.at_operation(Operation::SessionSend)} else {error};
                        control.publish(SessionEvent::CommandFailed{command:id,error:error.clone(),not_started:!self.command_started})?;
                        return Err(error);
                    }
                },
                ()=async {match deadline{Some(deadline)=>tokio::time::sleep_until(deadline).await,None=>std::future::pending().await}}=>return Err(KitError::new(ErrorKind::Timeout,Operation::SessionReceive,ErrorPhase::SessionReceive).with_timeout(self.timeout)),
                message=self.socket.receive_unbounded()=>match message? {
                    SessionSocketMessage::Payload(payload)=>{
                        self.observe(&payload).await.map_err(|error|if error.native_details().body.is_some(){error}else{error.with_diagnostic(CheckedRequestSpec::diagnostic_json(&payload))})?;
                    },
                    SessionSocketMessage::Closed(_)|SessionSocketMessage::Exhausted=>return Err(KitError::closed("OpenAI websocket ended").at_phase(ErrorPhase::SessionReceive)),
                },
            }
        }
    }

    async fn write(&mut self, payload: Value) -> Result<(), KitError> {
        self.command_started = true;
        let deadline = self.socket.operation_deadline()?;
        let control = Arc::clone(&self.control);
        tokio::select! { biased; ()=control.stopped()=>Err(KitError::closed("OpenAI write interrupted by close")), result=self.socket.send(payload,deadline)=>result }
    }

    async fn create_response(&mut self, command: CommandId) -> Result<(), KitError> {
        let admission = self.control.generation_admission()?;
        self.pending.insert(
            command.clone(),
            OpenAiPending {
                admission,
                deadline: self.socket.operation_deadline()?,
                cancellations: Vec::new(),
            },
        );
        self.write(json!({"type":"response.create","event_id":format!("command-{}",command.sequence()),"response":{"metadata":{"ai_kit_command":command.sequence().to_string()}}})).await
    }

    async fn command(&mut self, id: CommandId, command: SessionCommand) -> Result<(), KitError> {
        match command {
            SessionCommand::Input(message) => {
                // Acquire admission before the first external mutation and record identity before writing.
                let admission = self.control.generation_admission()?;
                self.pending.insert(
                    id.clone(),
                    OpenAiPending {
                        admission,
                        deadline: self.socket.operation_deadline()?,
                        cancellations: Vec::new(),
                    },
                );
                let content = message
                    .content()
                    .iter()
                    .filter_map(|part| match part {
                        ContentPart::Text { text } => {
                            Some(json!({"type":"input_text","text":text}))
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                self.write(json!({"type":"conversation.item.create","item":{"type":"message","role":"user","content":content}})).await?;
                self.write(json!({"type":"response.create","event_id":format!("command-{}",id.sequence()),"response":{"metadata":{"ai_kit_command":id.sequence().to_string()}}})).await?;
            }
            SessionCommand::ToolResults(results) => {
                let admission = self.control.generation_admission()?;
                for result in results {
                    let call = self.calls.get_mut(result.request()).ok_or_else(|| {
                        KitError::invalid_state("OpenAI result references unknown native tool")
                    })?;
                    if call.settled {
                        return Err(KitError::invalid_state("OpenAI tool already settled"));
                    }
                    call.settled = true;
                    let native_id = call.native_id.clone();
                    let output = tool_result_text(result.result().content())?;
                    self.write(json!({"type":"conversation.item.create","item":{"type":"function_call_output","call_id":native_id,"output":output}})).await?;
                }
                if self.calls.values().all(|call| call.settled) {
                    if self.responses.values().any(|response| !response.settled) {
                        self.continuation = Some(id);
                    } else {
                        self.create_response(id).await?;
                    }
                } else {
                    self.control
                        .publish(SessionEvent::ControlApplied { command: id })?;
                }
                drop(admission);
            }
            SessionCommand::Cancel(target) => self.cancel(id, target).await?,
            SessionCommand::StartInput => {
                self.capture = Some(id.clone());
                self.control
                    .publish(SessionEvent::ControlApplied { command: id })?;
            }
            SessionCommand::Audio { data, .. } => {
                // Audio capture is admitted independently of an active output response.
                let admission = self.control.generation_admission()?;
                if self.capture.is_none() {
                    self.capture = Some(id.clone());
                }
                let local_admission = if self.control.config().turn_detection()
                    == SessionTurnDetection::Automatic
                    && self.capture_work.is_none()
                {
                    self.capture_work = Some(OpenAiPending {
                        admission,
                        deadline: self.socket.operation_deadline()?,
                        cancellations: Vec::new(),
                    });
                    None
                } else {
                    Some(admission)
                };
                self.write(json!({"type":"input_audio_buffer.append","audio":base64::engine::general_purpose::STANDARD.encode(data)})).await?;
                drop(local_admission);
                self.control
                    .publish(SessionEvent::ControlApplied { command: id })?;
            }
            SessionCommand::EndInput => {
                let admission = self.control.generation_admission()?;
                self.pending.insert(
                    id.clone(),
                    OpenAiPending {
                        admission,
                        deadline: self.socket.operation_deadline()?,
                        cancellations: Vec::new(),
                    },
                );
                self.write(json!({"type":"input_audio_buffer.commit"}))
                    .await?;
                self.write(json!({"type":"response.create","response":{"metadata":{"ai_kit_command":id.sequence().to_string()}}})).await?;
            }
            SessionCommand::DiscardInput => {
                self.capture = None;
                self.acknowledgements
                    .entry("input_audio_buffer.cleared".to_string())
                    .or_default()
                    .push_back((id, self.socket.operation_deadline()?));
                self.write(json!({"type":"input_audio_buffer.clear"}))
                    .await?;
            }
            SessionCommand::ReconcilePlayback { segment, played } => {
                let (item, content_index) = {
                    let segments = self.segments();
                    let segment = segments.get(&segment).ok_or_else(|| {
                        KitError::invalid_state("unknown OpenAI playback segment")
                    })?;
                    (segment.item.clone(), segment.content_index)
                };
                self.acknowledgements
                    .entry(format!("truncate:{item}:{content_index}"))
                    .or_default()
                    .push_back((id, self.socket.operation_deadline()?));
                let audio_end_ms = u64::try_from(played.as_millis()).map_err(|_| {
                    KitError::invalid_state("playback duration exceeds native range")
                })?;
                self.write(json!({"type":"conversation.item.truncate","item_id":item,"content_index":content_index,"audio_end_ms":audio_end_ms})).await?;
            }
            SessionCommand::EndAudioStream => {
                return Err(OpenAiSession::unsupported(
                    "session.command",
                    "end of stream unavailable",
                ));
            }
        }
        Ok(())
    }

    async fn cancel(&mut self, id: CommandId, target: CancelTarget) -> Result<(), KitError> {
        if let CancelTarget::Command(command) = &target
            && let Some(pending) = self.pending.get_mut(command)
        {
            pending.cancellations.push((id, target));
            return Ok(());
        }
        let found = self
            .responses
            .iter()
            .find(|(_, response)| match &target {
                CancelTarget::Response(id) => &response.id == id,
                CancelTarget::Command(id) => response.command.as_ref() == Some(id),
            })
            .map(|(native, response)| (native.clone(), response.id.clone(), response.settled));
        match found {
            Some((_, _, true)) | None => self.control.publish(SessionEvent::Cancellation {
                command: id,
                target,
                resolution: CancellationResolution::AlreadySettled,
                continuity: SessionContinuity::Reusable,
            }),
            Some((native, _, false)) => {
                if let Some(response) = self.responses.get_mut(&native) {
                    response.cancellations.push((id, target));
                }
                self.write(json!({"type":"response.cancel","response_id":native}))
                    .await
            }
        }
    }

    fn response(&self, payload: &Value) -> Result<&OpenAiResponse, KitError> {
        let id = Self::required_text(payload, "response_id")?;
        self.responses.get(&id).ok_or_else(|| {
            Self::protocol("known_response", "OpenAI event references unknown response")
        })
    }

    fn segments(&self) -> MutexGuard<'_, BTreeMap<OutputSegmentId, OpenAiSegment>> {
        self.segments
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn segment(
        &mut self,
        payload: &Value,
        response: &ResponseId,
    ) -> Result<OutputSegmentId, KitError> {
        let item = Self::required_text(payload, "item_id")?;
        let content_index = payload
            .get("content_index")
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                KitError::invalid_response(ResponseViolation::MissingField {
                    field: "content_index",
                })
                .at_phase(ErrorPhase::SessionReceive)
            })?;
        if let Some((id, _)) = self.segments().iter().find(|(_, segment)| {
            segment.response == *response
                && segment.item == item
                && segment.content_index == content_index
        }) {
            return Ok(id.clone());
        }
        self.control
            .reserve_history(item.len() + std::mem::size_of::<OpenAiSegment>())?;
        let id = self.control.next_segment()?;
        self.segments().insert(
            id.clone(),
            OpenAiSegment {
                response: response.clone(),
                item,
                content_index,
            },
        );
        Ok(id)
    }

    async fn observe(&mut self, payload: &Value) -> Result<(), KitError> {
        let event = Self::required_text(payload, "type")?;
        match event.as_str() {
            "response.created" => {
                let response = payload.get("response").ok_or_else(|| {
                    KitError::invalid_response(ResponseViolation::MissingField {
                        field: "response",
                    })
                })?;
                let native = Self::required_text(response, "id")?;
                if self.responses.contains_key(&native) {
                    return Err(Self::protocol(
                        "unique_response",
                        "duplicate OpenAI response identity",
                    ));
                }
                let command = match response
                    .pointer("/metadata/ai_kit_command")
                    .and_then(Value::as_str)
                {
                    Some(sequence) => self
                        .pending
                        .keys()
                        .find(|id| id.sequence().to_string() == sequence)
                        .cloned(),
                    None => None,
                };
                let pending = command
                    .as_ref()
                    .and_then(|id| self.pending.remove(id))
                    .or_else(|| self.capture_work.take());
                let (admission, deadline, cancellations) = match pending {
                    Some(pending) => (pending.admission, pending.deadline, pending.cancellations),
                    None => (
                        self.control.generation_admission()?,
                        self.socket.operation_deadline()?,
                        Vec::new(),
                    ),
                };
                self.control
                    .reserve_history(native.len() + std::mem::size_of::<OpenAiResponse>())?;
                let id = self.control.next_response()?;
                self.responses.insert(
                    native.clone(),
                    OpenAiResponse {
                        id: id.clone(),
                        command: command.clone(),
                        admission: Some(admission),
                        deadline,
                        settled: false,
                        cancellations: Vec::new(),
                        calls: Vec::new(),
                    },
                );
                self.control.publish(SessionEvent::ResponseStarted {
                    command,
                    response: id,
                })?;
                for (command, target) in cancellations {
                    self.cancel(command, target).await?;
                }
            }
            "response.output_text.delta" | "response.output_audio.delta" => {
                let response = self.response(payload)?;
                if response.settled {
                    return Err(Self::protocol(
                        "response_output_before_terminal",
                        "output arrived after response settled",
                    ));
                }
                let response = response.id.clone();
                let segment = self.segment(payload, &response)?;
                let delta = Self::required_text(payload, "delta")?;
                let content = if event == "response.output_text.delta" {
                    ContentPart::Text { text: delta }
                } else {
                    let media_type = self
                        .control
                        .config()
                        .output_audio_format()
                        .ok_or_else(|| {
                            Self::protocol(
                                "session_output_modality",
                                "audio returned for text-only session",
                            )
                        })?
                        .to_string();
                    let data = base64::engine::general_purpose::STANDARD
                        .decode(delta)
                        .map_err(|_| {
                            KitError::invalid_response(ResponseViolation::InvalidField {
                                field: "delta",
                            })
                            .at_phase(ErrorPhase::SessionReceive)
                        })?;
                    ContentPart::Audio { media_type, data }
                };
                self.control.publish(SessionEvent::Output {
                    response,
                    segment: Some(segment),
                    content,
                })?;
            }
            "response.output_audio_transcript.delta" | "response.output_audio_transcript.done" => {
                if self.control.config().output_transcription() {
                    let response = self.response(payload)?.id.clone();
                    let segment = self.segment(payload, &response)?;
                    let finalized = event.ends_with(".done");
                    let text = Self::required_text(
                        payload,
                        if finalized { "transcript" } else { "delta" },
                    )?;
                    self.control.publish(SessionEvent::Transcription {
                        source: TranscriptionSource::Output { response, segment },
                        text,
                        finalized,
                    })?;
                }
            }
            "conversation.item.input_audio_transcription.delta"
            | "conversation.item.input_audio_transcription.completed" => {
                if self.control.config().input_transcription() {
                    let command = payload
                        .get("item_id")
                        .and_then(Value::as_str)
                        .and_then(|id| self.input_commands.get(id))
                        .cloned();
                    let finalized = event.ends_with(".completed");
                    let text = Self::required_text(
                        payload,
                        if finalized { "transcript" } else { "delta" },
                    )?;
                    self.control.publish(SessionEvent::Transcription {
                        source: TranscriptionSource::Input { command },
                        text,
                        finalized,
                    })?;
                }
            }
            "response.function_call_arguments.done" => self.function_call(payload)?,
            "response.usage.updated" => {
                let usage = Self::usage(payload.get("usage"))?;
                let response = self.response(payload)?.id.clone();
                self.control.publish(SessionEvent::Usage {
                    scope: UsageScope::Response(response),
                    usage,
                    finalized: false,
                })?;
            }
            "response.done" => self.response_done(payload).await?,
            "input_audio_buffer.committed" => {
                let item = Self::required_text(payload, "item_id")?;
                if let Some(command) = self.capture.take() {
                    self.control
                        .reserve_history(item.len() + std::mem::size_of::<CommandId>())?;
                    self.input_commands.insert(item, command);
                }
            }
            "input_audio_buffer.cleared" => {
                self.capture_work.take();
                if let Some((command, _)) = self
                    .acknowledgements
                    .get_mut(&event)
                    .and_then(VecDeque::pop_front)
                {
                    self.control
                        .publish(SessionEvent::ControlApplied { command })?;
                }
            }
            "conversation.item.truncated" => {
                let item = Self::required_text(payload, "item_id")?;
                let content_index = payload
                    .get("content_index")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| {
                        Self::protocol("truncate_content_index", "truncation omitted content index")
                    })?;
                if let Some((command, _)) = self
                    .acknowledgements
                    .get_mut(&format!("truncate:{item}:{content_index}"))
                    .and_then(VecDeque::pop_front)
                {
                    self.control
                        .publish(SessionEvent::ControlApplied { command })?;
                }
            }
            "error" | "response.error" | "conversation.item.input_audio_transcription.failed" => {
                return Err(Self::provider_error(payload));
            }
            "session.created"
            | "session.updated"
            | "response.content_part.added"
            | "response.content_part.done"
            | "response.output_text.done"
            | "response.output_audio.done"
            | "response.function_call_arguments.delta"
            | "input_audio_buffer.speech_started"
            | "input_audio_buffer.speech_stopped"
            | "input_audio_buffer.timeout_triggered"
            | "rate_limits.updated"
            | "conversation.item.added"
            | "conversation.item.created"
            | "conversation.item.deleted"
            | "conversation.item.done"
            | "response.output_item.added"
            | "response.output_item.done" => {}
            _ => {
                return Err(
                    KitError::invalid_response(ResponseViolation::UnknownVariant { field: "type" })
                        .at_phase(ErrorPhase::SessionReceive),
                );
            }
        }
        Ok(())
    }

    fn function_call(&mut self, payload: &Value) -> Result<(), KitError> {
        // Decode independently before identity checks so malformed JSON retains decode diagnostics.
        let arguments = serde_json::from_str::<Value>(&Self::required_text(payload, "arguments")?)
            .map_err(|error| {
                KitError::invalid_response(ResponseViolation::MalformedJson)
                    .with_response_details(ResponseDetails::json(&error))
                    .at_phase(ErrorPhase::Decode)
            })?;
        let native_id = Self::required_text(payload, "call_id")?;
        let name = Self::required_text(payload, "name")?;
        if !self.control.config().tools().iter().any(
            |tool| matches!(tool,ProviderToolSpec::Function(function) if function.name()==name),
        ) {
            return Err(Self::protocol(
                "declared_function",
                "OpenAI requested undeclared function",
            ));
        }
        let response = self.response(payload)?;
        if response.settled {
            return Err(Self::protocol(
                "tool_before_terminal",
                "function call arrived after terminal",
            ));
        }
        if self.calls.values().any(|call| call.native_id == native_id) {
            return Err(Self::protocol(
                "unique_call",
                "OpenAI repeated function call identity",
            ));
        }
        let response = response.id.clone();
        let request = self.control.next_tool()?;
        let call =
            FunctionCall::new(Some(native_id.clone()), name, arguments).map_err(|error| {
                KitError::invalid_response(ResponseViolation::InvalidField {
                    field: "function_call",
                })
                .at_phase(ErrorPhase::SessionReceive)
                .with_message("OpenAI returned an invalid function call")
                .with_supplementary(error)
            })?;
        let mut retained = crate::generation::RetainedSize::new();
        retained.call(&call)?;
        retained.add(native_id.len() + std::mem::size_of::<OpenAiCall>())?;
        self.control.reserve_history(retained.bytes())?;
        self.calls.insert(
            request.clone(),
            OpenAiCall {
                response: response.clone(),
                native_id,
                settled: false,
            },
        );
        let native = Self::required_text(payload, "response_id")?;
        if let Some(response) = self.responses.get_mut(&native) {
            response.calls.push((request, call));
        }
        Ok(())
    }

    async fn response_done(&mut self, payload: &Value) -> Result<(), KitError> {
        let native_response = payload.get("response").ok_or_else(|| {
            KitError::invalid_response(ResponseViolation::MissingField { field: "response" })
                .at_phase(ErrorPhase::SessionReceive)
        })?;
        if !native_response.is_object() {
            return Err(KitError::invalid_response(ResponseViolation::InvalidField {
                field: "response",
            })
            .at_phase(ErrorPhase::SessionReceive));
        }
        let status = Self::required_text(native_response, "status")?;
        let outcome = match status.as_str() {
            "completed" => ResponseOutcome::Completed,
            "incomplete" => ResponseOutcome::Partial {
                reason: match native_response
                    .pointer("/status_details/reason")
                    .and_then(Value::as_str)
                {
                    Some("max_output_tokens") => Some(FinishReason::Length),
                    Some("content_filter") => Some(FinishReason::ContentFilter),
                    _ => None,
                },
            },
            "cancelled" => ResponseOutcome::Cancelled,
            "failed" => ResponseOutcome::Failed(Self::provider_error(payload)),
            _ => {
                return Err(
                    KitError::invalid_response(ResponseViolation::UnknownVariant {
                        field: "response.status",
                    })
                    .at_phase(ErrorPhase::SessionReceive),
                );
            }
        };
        let native = Self::required_text(native_response, "id")?;
        let response = self.responses.get_mut(&native).ok_or_else(|| {
            Self::protocol(
                "known_terminal_response",
                "terminal references unknown response",
            )
        })?;
        if response.settled {
            return Err(Self::protocol(
                "one_response_terminal",
                "duplicate response terminal",
            ));
        }
        response.settled = true;
        let response_id = response.id.clone();
        let calls = std::mem::take(&mut response.calls);
        let cancellations = std::mem::take(&mut response.cancellations);
        response.admission.take();
        if matches!(outcome, ResponseOutcome::Completed) {
            for (request, call) in calls {
                self.control.publish(SessionEvent::ToolRequested {
                    response: response_id.clone(),
                    request,
                    call,
                })?;
            }
        } else {
            self.control.invalidate_response(&response_id)?;
            for call in self
                .calls
                .values_mut()
                .filter(|call| call.response == response_id)
            {
                call.settled = true;
            }
        }
        if native_response
            .get("usage")
            .is_some_and(|usage| !usage.is_null())
        {
            self.control.publish(SessionEvent::Usage {
                scope: UsageScope::Response(response_id.clone()),
                usage: Self::usage(native_response.get("usage"))?,
                finalized: true,
            })?;
        }
        let cancelled = matches!(outcome, ResponseOutcome::Cancelled);
        self.control.publish(SessionEvent::ResponseFinished {
            response: response_id,
            outcome,
            continuity: SessionContinuity::Reusable,
            output: None,
        })?;
        for (command, target) in cancellations {
            self.control.publish(SessionEvent::Cancellation {
                command,
                target,
                resolution: if cancelled {
                    CancellationResolution::Stopped
                } else {
                    CancellationResolution::AlreadySettled
                },
                continuity: SessionContinuity::Reusable,
            })?;
        }
        if self.calls.values().all(|call| call.settled)
            && let Some(command) = self.continuation.take()
        {
            self.create_response(command).await?;
        }
        Ok(())
    }

    fn usage(value: Option<&Value>) -> Result<crate::generation::Usage, KitError> {
        let value = value.ok_or_else(|| {
            KitError::invalid_response(ResponseViolation::MissingField { field: "usage" })
                .at_phase(ErrorPhase::SessionReceive)
        })?;
        let usage =
            serde_json::from_value::<super::wire::OpenAiUsage>(value.clone()).map_err(|error| {
                KitError::invalid_response(ResponseViolation::InvalidField { field: "usage" })
                    .with_response_details(ResponseDetails::json(&error))
                    .at_phase(ErrorPhase::Decode)
            })?;
        Ok(usage_from_openai(Some(&usage)))
    }

    fn protocol(rule: &'static str, message: &'static str) -> KitError {
        KitError::invalid_response(ResponseViolation::ProtocolViolation { rule })
            .at_phase(ErrorPhase::SessionReceive)
            .with_message(message)
    }

    fn required_text(payload: &Value, field: &'static str) -> Result<String, KitError> {
        let value = payload.get(field).ok_or_else(|| {
            KitError::invalid_response(ResponseViolation::MissingField { field })
                .at_phase(ErrorPhase::SessionReceive)
        })?;
        value
            .as_str()
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .ok_or_else(|| {
                KitError::invalid_response(ResponseViolation::InvalidField { field })
                    .at_phase(ErrorPhase::SessionReceive)
            })
    }
    fn provider_error(payload: &Value) -> KitError {
        let error = payload
            .get("error")
            .or_else(|| payload.pointer("/response/status_details/error"))
            .unwrap_or(payload);
        let native = match serde_json::from_value::<OpenAiErrorObject>(error.clone()) {
            Ok(native) => native,
            Err(error) => {
                return KitError::invalid_response(ResponseViolation::from_json_error(
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
                KitError::from_remote(failure, Operation::Unspecified, ErrorPhase::SessionReceive)
            }
            Err(error) => error.at_phase(ErrorPhase::SessionReceive),
        }
    }
}
