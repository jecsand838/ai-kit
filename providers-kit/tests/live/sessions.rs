//! Independent real-service Session qualification. These cases have no copy-grade exception.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::num::NonZeroU64;
use std::time::Duration;

use providers_kit::SessionBackend;
use providers_kit::error::ErrorKind;
use providers_kit::generation::{
    ContentPart, FunctionCall, FunctionResult, GenerateResponse, JsonSchema, Message,
    ProviderFunctionTool, ProviderToolSpec,
};
use providers_kit::models::ModelBackend;
use providers_kit::session::{
    CancelTarget, CommandId, OutputSegmentId, ResponseId, ResponseOutcome, SessionCommand,
    SessionCommands, SessionConfig, SessionContinuity, SessionEvent, SessionEvents, SessionProfile,
    SessionToolResult, SessionTurnDetection, ToolRequestId, TranscriptionSource,
};
use serde_json::{Value, json};

use super::{LiveProvider, Service};
use crate::live_failures::CaseFailure;

struct SessionProof {
    model: String,
    session: Option<String>,
    records: Vec<Value>,
    record_bytes: usize,
    audio: Vec<u8>,
    output_transcription: String,
    input_transcription: String,
    input_segments: BTreeMap<Option<CommandId>, String>,
    output_segments: BTreeMap<OutputSegmentId, String>,
    started: BTreeSet<ResponseId>,
    finished: BTreeSet<ResponseId>,
}

impl SessionProof {
    fn new(model: &str) -> Self {
        Self {
            model: model.into(),
            session: None,
            records: Vec::new(),
            record_bytes: 0,
            audio: Vec::new(),
            output_transcription: String::new(),
            input_transcription: String::new(),
            input_segments: BTreeMap::new(),
            output_segments: BTreeMap::new(),
            started: BTreeSet::new(),
            finished: BTreeSet::new(),
        }
    }

    fn record(&mut self, value: Value) -> Result<(), CaseFailure> {
        let bytes = serde_json::to_vec(&value)
            .map_err(|_| CaseFailure::from("session evidence encoding failed"))?
            .len();
        if self.records.len() >= 4096 || bytes > 768 * 1024 - self.record_bytes {
            return Err("required session evidence exceeded its retained bound".into());
        }
        self.record_bytes += bytes;
        self.records.push(value);
        Ok(())
    }

    async fn submit(
        &mut self,
        commands: &mut impl SessionCommands,
        command: SessionCommand,
    ) -> Result<CommandId, CaseFailure> {
        let operation = match &command {
            SessionCommand::Audio { media_type, data } => {
                json!({"audio_media_type":media_type,"audio_bytes":data.len()})
            }
            _ => json!({"command":format!("{command:?}")}),
        };
        let result = commands.submit(command).await;
        self.record(json!({"submit":operation,"receipt":result.as_ref().ok().map(|id|json!({"session":id.session_id(),"sequence":id.sequence().get()})),"failure":result.as_ref().err().cloned().map(CaseFailure::from).as_ref().map(CaseFailure::evidence)}))?;
        result.map_err(LiveProvider::error)
    }

    async fn next(&mut self, events: &mut impl SessionEvents) -> Result<SessionEvent, CaseFailure> {
        let event = tokio::time::timeout(Duration::from_secs(90), events.recv())
            .await
            .map_err(|_| CaseFailure::from("session observation exceeded 90 seconds"))?
            .ok_or_else(|| CaseFailure::from("session drained before required result"))?;
        match &event {
            SessionEvent::ResponseStarted { response, .. } => {
                if !self.started.insert(response.clone()) {
                    return Err("duplicate response start".into());
                }
            }
            SessionEvent::Output {
                response, content, ..
            } => {
                if !self.started.contains(response) || self.finished.contains(response) {
                    return Err("output has no active original response".into());
                }
                if let ContentPart::Audio { media_type, data } = content {
                    if media_type != "audio/pcm;rate=24000"
                        || data.is_empty()
                        || data.len() > 8 * 1024 * 1024 - self.audio.len()
                    {
                        return Err("invalid or excessive native audio evidence".into());
                    }
                    self.record(json!({"audio":{"response":response.sequence().get(),"media_type":media_type,"offset":self.audio.len(),"bytes":data.len()}}))?;
                    self.audio.extend_from_slice(data);
                    return Ok(event);
                }
            }
            SessionEvent::Transcription {
                source,
                text,
                finalized,
            } => match source {
                TranscriptionSource::Input { command } => {
                    let observed = self.input_segments.entry(command.clone()).or_default();
                    if *finalized {
                        *observed = text.clone();
                    } else {
                        observed.push_str(text);
                    }
                    self.input_transcription =
                        self.input_segments.values().map(String::as_str).collect();
                }
                TranscriptionSource::Output { response, segment } => {
                    if !self.started.contains(response) {
                        return Err("output transcription lost original response".into());
                    }
                    let observed = self.output_segments.entry(segment.clone()).or_default();
                    if *finalized {
                        *observed = text.clone();
                    } else {
                        observed.push_str(text);
                    }
                    self.output_transcription =
                        self.output_segments.values().map(String::as_str).collect();
                }
            },
            SessionEvent::ToolRequested { response, .. } => {
                if !self.started.contains(response) {
                    return Err("tool request lost response correlation".into());
                }
            }
            SessionEvent::ResponseFinished { response, .. } => {
                if !self.started.contains(response) || !self.finished.insert(response.clone()) {
                    return Err("missing response start or duplicate response terminal".into());
                }
            }
            SessionEvent::Terminated {
                reason,
                continuity,
                unfinished_commands,
                unfinished_responses,
                outstanding_tools,
            } => {
                self.record(json!({"terminal":{"continuity":format!("{continuity:?}"),"unfinished_commands":unfinished_commands.iter().map(|id|id.sequence().get()).collect::<Vec<_>>(),"unfinished_responses":unfinished_responses.iter().map(|id|id.sequence().get()).collect::<Vec<_>>(),"outstanding_tools":outstanding_tools.iter().map(|id|id.sequence().get()).collect::<Vec<_>>(),"reason":CaseFailure::from(reason.clone()).evidence()}}))?;
                return Err(LiveProvider::error(reason.clone()));
            }
            SessionEvent::CommandFailed {
                command,
                error,
                not_started,
            } => {
                self.record(json!({"command_failed":{"command":command.sequence().get(),"not_started":not_started,"error":CaseFailure::from(error.clone()).evidence()}}))?;
                return Err(LiveProvider::error(error.clone()));
            }
            SessionEvent::Usage { .. }
            | SessionEvent::ControlApplied { .. }
            | SessionEvent::Cancellation { .. }
            | SessionEvent::ToolsInvalidated { .. }
            | SessionEvent::Interruption { .. }
            | SessionEvent::TurnEnded { .. } => {}
        }
        match &event {
            SessionEvent::ResponseFinished { response, outcome, continuity, output } => self.record(json!({"response_finished":{"response":response.sequence().get(),"outcome":format!("{outcome:?}"),"continuity":format!("{continuity:?}"),"output":output.as_ref().map(|output|CaseFailure::response_evidence(output))}}))?,
            _ => self.record(json!({"event":format!("{event:?}")}))?,
        }
        Ok(event)
    }

    async fn interrupt_tool(
        &mut self,
        commands: &mut impl SessionCommands,
        events: &mut impl SessionEvents,
    ) -> Result<(), CaseFailure> {
        self.submit(
            commands,
            SessionCommand::Input(Message::user_text(
                "Call strict_session_lookup with code session-probe. Wait for its result before answering; do not invent a result.",
            ).map_err(LiveProvider::error)?),
        ).await?;
        let (original, request, call) = loop {
            match self.next(events).await? {
                SessionEvent::ToolRequested {
                    response,
                    request,
                    call,
                } if call.name() == "strict_session_lookup"
                    && call.arguments()["code"] == "session-probe" =>
                {
                    break (response, request, call);
                }
                SessionEvent::ResponseStarted { .. }
                | SessionEvent::Output { .. }
                | SessionEvent::Transcription { .. }
                | SessionEvent::Usage { .. }
                | SessionEvent::ControlApplied { .. } => {}
                unexpected => {
                    return Err(
                        format!("pending Gemini tool was not observed: {unexpected:?}").into(),
                    );
                }
            }
        };
        self.submit(commands, SessionCommand::StartInput).await?;
        let audio = include_bytes!("../fixtures/input-speech-16000.pcm");
        self.record(json!({"input_fixture":{"path":"providers-kit/tests/fixtures/input-speech-16000.pcm","format":"audio/pcm;rate=16000","bytes":audio.len(),"interrupts_request":request.sequence().get()}}))?;
        for chunk in audio.chunks(4096) {
            self.submit(
                commands,
                SessionCommand::Audio {
                    media_type: "audio/pcm;rate=16000".into(),
                    data: chunk.to_vec(),
                },
            )
            .await?;
        }
        let activity = self.submit(commands, SessionCommand::EndInput).await?;
        let mut recovery = None;
        let mut invalidated = false;
        let mut interrupted = false;
        let mut completed = false;
        let mut turn_ended = false;
        let mut recovery_audio = false;
        let mut recovery_transcription = String::new();
        loop {
            match self.next(events).await? {
                SessionEvent::ToolsInvalidated { requests } => {
                    if invalidated || requests != [request.clone()] {
                        return Err(
                            "Gemini tool invalidation lost the exact delivered request".into()
                        );
                    }
                    invalidated = true;
                    let stale = FunctionResult::new(
                        call.id().map(str::to_owned),
                        call.name(),
                        vec![ContentPart::Json {
                            value: json!({"answer":"stale result must not execute"}),
                        }],
                        false,
                    )
                    .map_err(LiveProvider::error)?;
                    let result = commands
                        .submit(SessionCommand::ToolResults(vec![SessionToolResult::new(
                            request.clone(),
                            stale,
                        )]))
                        .await;
                    let error = result.err().ok_or_else(|| {
                        CaseFailure::from("cancelled Gemini tool result was accepted")
                    })?;
                    if !matches!(
                        error.kind(),
                        ErrorKind::InvalidInput | ErrorKind::InvalidState
                    ) {
                        return Err(LiveProvider::error(error));
                    }
                    self.record(json!({"stale_tool_rejected":{"request":request.sequence().get(),"failure":CaseFailure::from(error).evidence()}}))?;
                }
                SessionEvent::Interruption {
                    response: Some(response),
                } if response == original => interrupted = true,
                SessionEvent::ResponseStarted {
                    command: Some(command),
                    response,
                } if command == activity && response != original => {
                    if recovery.replace(response).is_some() {
                        return Err(
                            "manual Gemini activity created duplicate recovery responses".into(),
                        );
                    }
                }
                SessionEvent::Output {
                    response,
                    content: ContentPart::Audio { data, .. },
                    ..
                } if recovery.as_ref() == Some(&response) => recovery_audio |= !data.is_empty(),
                SessionEvent::Transcription {
                    source: TranscriptionSource::Output { response, .. },
                    text,
                    finalized,
                } if recovery.as_ref() == Some(&response) => {
                    if finalized {
                        recovery_transcription = text;
                    } else {
                        recovery_transcription.push_str(&text);
                    }
                }
                SessionEvent::Transcription {
                    source: TranscriptionSource::Input { command: Some(_) },
                    ..
                } => return Err("Gemini invented input transcription attribution".into()),
                SessionEvent::ResponseFinished {
                    response,
                    outcome: ResponseOutcome::Completed,
                    continuity: SessionContinuity::Reusable,
                    output: None,
                } if recovery.as_ref() == Some(&response) => completed = true,
                SessionEvent::ResponseFinished {
                    response,
                    outcome: ResponseOutcome::Completed | ResponseOutcome::Interrupted,
                    continuity: SessionContinuity::Reusable,
                    output: None,
                } if response == original => {}
                SessionEvent::TurnEnded {
                    response: Some(response),
                } if recovery.as_ref() == Some(&response) && completed => turn_ended = true,
                SessionEvent::TurnEnded {
                    response: Some(response),
                } if response == original => {}
                SessionEvent::Output { response, .. }
                | SessionEvent::Transcription {
                    source: TranscriptionSource::Output { response, .. },
                    ..
                } if response == original => {}
                SessionEvent::Transcription {
                    source: TranscriptionSource::Input { command: None },
                    ..
                }
                | SessionEvent::Usage { .. }
                | SessionEvent::ControlApplied { .. } => {}
                unexpected => {
                    return Err(format!(
                        "unexpected Gemini interruption/recovery observation: {unexpected:?}"
                    )
                    .into());
                }
            }
            if invalidated
                && interrupted
                && completed
                && turn_ended
                && recovery_audio
                && !self.input_transcription.trim().is_empty()
                && recovery_transcription
                    .to_lowercase()
                    .contains("audio input received")
            {
                return Ok(());
            }
        }
    }

    async fn managed(
        &mut self,
        events: &mut impl SessionEvents,
        command: &CommandId,
    ) -> Result<(GenerateResponse<()>, Vec<(ToolRequestId, FunctionCall)>), CaseFailure> {
        let mut response = None;
        let mut tools = Vec::new();
        loop {
            match self.next(events).await? {
                SessionEvent::ResponseStarted {
                    command: observed,
                    response: id,
                } => {
                    if observed.as_ref() != Some(command) || response.replace(id).is_some() {
                        return Err("managed response has wrong command receipt".into());
                    }
                }
                SessionEvent::ToolRequested {
                    response: id,
                    request,
                    call,
                } => {
                    if response.as_ref() != Some(&id) {
                        return Err("tool belongs to a different managed response".into());
                    }
                    tools.push((request, call));
                }
                SessionEvent::ResponseFinished {
                    response: id,
                    outcome: ResponseOutcome::Completed,
                    continuity: SessionContinuity::Reusable,
                    output: Some(output),
                } => {
                    if response.as_ref() != Some(&id) || output.model.as_str() != self.model {
                        return Err("managed completion changed model or response identity".into());
                    }
                    return Ok((*output, tools));
                }
                SessionEvent::Output { .. }
                | SessionEvent::Usage { .. }
                | SessionEvent::ControlApplied { .. } => {}
                unexpected => {
                    return Err(format!("unexpected managed observation: {unexpected:?}").into());
                }
            }
        }
    }

    fn text(response: &GenerateResponse<()>) -> String {
        response
            .message
            .content()
            .iter()
            .filter_map(|part| match part {
                ContentPart::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect()
    }

    fn export(
        &self,
        journey: &Result<(), CaseFailure>,
        cleanup: &Result<(), CaseFailure>,
    ) -> Result<(), CaseFailure> {
        let directory = CaseFailure::required_observation_directory(
            std::env::var_os("PROVIDERS_KIT_EVIDENCE_DIR").map(std::path::PathBuf::from),
        )?;
        let thread = std::thread::current();
        let prefix = format!(
            "{}.{}",
            thread.name().unwrap_or("unnamed-session").replace(':', "_"),
            LiveProvider::nonce()
        );
        let audio_path = directory.join(format!("{prefix}.output-24000.pcm"));
        if !self.audio.is_empty() {
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            options
                .open(&audio_path)
                .and_then(|mut file| file.write_all(&self.audio))
                .map_err(|error| {
                    CaseFailure::from(format!(
                        "required PCM evidence export failed: {:?}",
                        error.kind()
                    ))
                })?;
        }
        let observation = json!({
            "boundary":"public_session", "native_frames_captured":false, "selected_model":self.model, "session_id":self.session,
            "copy_grade_exception":false, "events":self.records, "audio_bytes":self.audio.len(), "audio_file":(!self.audio.is_empty()).then(||audio_path.file_name().and_then(|name|name.to_str())),
            "input_transcription":self.input_transcription,"output_transcription":self.output_transcription,
            "journey":{"passed":journey.is_ok(),"failure":journey.as_ref().err().map(CaseFailure::evidence)},
            "close":{"passed":cleanup.is_ok(),"failure":cleanup.as_ref().err().map(CaseFailure::evidence)}
        });
        CaseFailure::write_required_observation(
            &directory.join(format!("{prefix}.session.json")),
            observation,
            &LiveProvider::evidence_secrets(),
        )
        .map_err(|error| {
            CaseFailure::from(format!(
                "required Session observation failed: {:?}",
                error.kind()
            ))
        })
    }
}

impl LiveProvider {
    fn session_function() -> Result<ProviderFunctionTool, CaseFailure> {
        ProviderFunctionTool::new("strict_session_lookup", Some("Read the requested code and return its answer.".into()), JsonSchema::new(json!({"type":"object","properties":{"code":{"type":"string"}},"required":["code"],"additionalProperties":false})).map_err(Self::error)?).map_err(Self::error)
    }

    async fn conversation_session(&self) -> Result<(), CaseFailure> {
        let mut proof = SessionProof::new(self.default_model());
        let mut owner = None;
        let journey = tokio::time::timeout(Duration::from_secs(360), async {
            let model = self.model()?;
            let config = SessionConfig::new(SessionProfile::Conversation)
                .with_instructions("Remember user records exactly. Use strict_session_lookup only when explicitly requested. After a tool result return only its answer string, preserving every character.").map_err(Self::error)?
                .with_generation(Self::generation_config()).map_err(Self::error)?
                .with_tools(vec![ProviderToolSpec::Function(Self::session_function()?)]).map_err(Self::error)?;
            proof.record(json!({"configuration":{"profile":"Conversation","instructions":config.instructions(),"generation":config.generation(),"tools":config.tools()}}))?;
            owner = Some(model.open_session(config).await.map_err(Self::error)?);
            let session = owner.as_mut().expect("owned session");
            proof.session = Some(session.id().into());
            let (mut commands, mut events) = session.split();
            let memory = Self::nonce();
            let first = proof.submit(&mut commands, SessionCommand::Input(Message::user_text(format!("Remember this record code exactly: {memory}. Reply with MEMORY STORED without calling tools.")).map_err(Self::error)?)).await?;
            let (stored, tools) = proof.managed(&mut events, &first).await?;
            if SessionProof::text(&stored).trim().is_empty() || !tools.is_empty() { return Err("first Conversation exchange did not complete as text".into()); }
            let second = proof.submit(&mut commands, SessionCommand::Input(Message::user_text("What exact record code did I ask you to remember? Reply with only that code. Do not call a tool.").map_err(Self::error)?)).await?;
            let (recalled, tools) = proof.managed(&mut events, &second).await?;
            if SessionProof::text(&recalled).trim() != memory || !tools.is_empty() { return Err("Conversation did not preserve the first exchange's exact user memory".into()); }
            let request = proof.submit(&mut commands, SessionCommand::Input(Message::user_text("Now call strict_session_lookup for the record code I asked you to remember. After receiving its result, return only the answer string.").map_err(Self::error)?)).await?;
            let (_, tools) = proof.managed(&mut events, &request).await?;
            let [(request, call)] = tools.as_slice() else { return Err("Conversation requires one correlated function request".into()); };
            if call.name() != "strict_session_lookup" || call.arguments()["code"] != memory { return Err("function request lost remembered user code".into()); }
            let answer = Self::nonce();
            let result = FunctionResult::new(call.id().map(str::to_owned), call.name(), vec![ContentPart::Json { value:json!({"answer":answer}) }], false).map_err(Self::error)?;
            let submitted = proof.submit(&mut commands, SessionCommand::ToolResults(vec![SessionToolResult::new(request.clone(), result)])).await?;
            let (continued, tools) = proof.managed(&mut events, &submitted).await?;
            if SessionProof::text(&continued).trim() != answer || !tools.is_empty() { return Err("Conversation did not use its exact correlated function result".into()); }
            Ok(())
        }).await.map_err(|_|CaseFailure::from("Conversation Session journey exceeded 360 seconds")).and_then(|result|result);
        let cleanup = match owner {
            Some(session) => session.close().await.map_err(Self::error),
            None => Ok(()),
        };
        let export = proof.export(&journey, &cleanup);
        CaseFailure::combine(CaseFailure::combine(journey, cleanup), export)
    }

    async fn gemini_live_session(&self, scenario: GeminiScenario) -> Result<(), CaseFailure> {
        let selected = Self::required("TEST_GEMINI_MODEL_INTERACTIVE")?;
        if selected != "models/gemini-3.8-live" {
            return Err(
                "TEST_GEMINI_MODEL_INTERACTIVE must be exact models/gemini-3.8-live".into(),
            );
        }
        let mut proof = SessionProof::new(&selected);
        let mut owner = None;
        let journey = tokio::time::timeout(Duration::from_secs(180), async {
            let mut config = SessionConfig::new(SessionProfile::Realtime)
                .with_instructions("Answer clearly and briefly. Use strict_session_lookup when explicitly asked, then speak the answer returned by the tool. Follow audio input instructions.").map_err(Self::error)?
                .with_input_audio("audio/pcm;rate=16000", true).map_err(Self::error)?
                .with_output_audio("audio/pcm;rate=24000", true).map_err(Self::error)?
                .with_tools(vec![ProviderToolSpec::Function(Self::session_function()?)]).map_err(Self::error)?;
            if matches!(scenario, GeminiScenario::ManualAudio | GeminiScenario::ToolCancellation) { config = config.with_turn_detection(SessionTurnDetection::Manual); }
            proof.record(json!({"configuration":{"profile":"Realtime","instructions":config.instructions(),"generation":config.generation(),"tools":config.tools(),"input_audio":config.input_audio_format(),"output_audio":config.output_audio_format(),"input_transcription":config.input_transcription(),"output_transcription":config.output_transcription(),"turn_detection":format!("{:?}",config.turn_detection())}}))?;
            owner = Some(self.selected(&selected)?.open_session(config).await.map_err(Self::error)?);
            let session = owner.as_mut().expect("owned Live session");
            proof.session = Some(session.id().into());
            let foreign_target = ResponseId::new(session.id(), NonZeroU64::new(u64::MAX).expect("nonzero")).map_err(Self::error)?;
            let segment = OutputSegmentId::new(session.id(), NonZeroU64::new(u64::MAX).expect("nonzero")).map_err(Self::error)?;
            let (mut commands, mut events) = session.split();
            if matches!(scenario, GeminiScenario::ToolCancellation) {
                return proof.interrupt_tool(&mut commands, &mut events).await;
            }
            if matches!(scenario, GeminiScenario::ManualAudio) {
                for command in [SessionCommand::Cancel(CancelTarget::Response(foreign_target)), SessionCommand::DiscardInput, SessionCommand::ReconcilePlayback { segment, played:Duration::ZERO }] {
                    let error = commands.submit(command).await.err().ok_or_else(||CaseFailure::from("Gemini Live accepted unsupported native control"))?;
                    if error.kind() != ErrorKind::Unsupported { return Err(Self::error(error)); }
                    proof.record(json!({"unsupported_control":CaseFailure::from(error).evidence()}))?;
                }
                proof.submit(&mut commands, SessionCommand::StartInput).await?;
                let audio = include_bytes!("../fixtures/input-speech-16000.pcm");
                proof.record(json!({"input_fixture":{"path":"providers-kit/tests/fixtures/input-speech-16000.pcm","format":"audio/pcm;rate=16000","bytes":audio.len()}}))?;
                for chunk in audio.chunks(4096) {
                    proof.submit(&mut commands, SessionCommand::Audio {media_type:"audio/pcm;rate=16000".into(),data:chunk.to_vec()}).await?;
                }
                proof.submit(&mut commands, SessionCommand::EndInput).await?;
            } else {
                let prompt = match scenario { GeminiScenario::Text => "Say: The key is amber. Do not call a tool.", GeminiScenario::Tool => "Call strict_session_lookup with code session-probe. Speak the answer from its result, and do not invent that answer.", GeminiScenario::ManualAudio | GeminiScenario::ToolCancellation => unreachable!() };
                proof.submit(&mut commands, SessionCommand::Input(Message::user_text(prompt).map_err(Self::error)?)).await?;
            }
            let mut tool_submitted = false;
            let mut after_tool_audio = 0;
            let mut after_tool_transcript = BTreeMap::new();
            let mut completed = false;
            let mut turn_ended = false;
            loop {
                match proof.next(&mut events).await? {
                    SessionEvent::ToolRequested { request, call, .. } => {
                        if !matches!(scenario,GeminiScenario::Tool) || tool_submitted || call.name() != "strict_session_lookup" || call.arguments()["code"] != "session-probe" { return Err("unexpected Gemini Live function request".into()); }
                        after_tool_audio = proof.audio.len(); after_tool_transcript = proof.output_segments.clone();
                        let result = FunctionResult::new(call.id().map(str::to_owned), call.name(), vec![ContentPart::Json {value:json!({"answer":"violet"})}], false).map_err(Self::error)?;
                        proof.submit(&mut commands, SessionCommand::ToolResults(vec![SessionToolResult::new(request,result)])).await?;
                        tool_submitted = true; completed = false; turn_ended = false;
                    }
                    SessionEvent::ResponseFinished { outcome:ResponseOutcome::Completed, continuity:SessionContinuity::Reusable, output:None, .. } => completed = true,
                    SessionEvent::ResponseFinished { .. } => return Err("Gemini Live response did not affirmatively complete".into()),
                    SessionEvent::TurnEnded { response:Some(response) } if proof.finished.contains(&response) => turn_ended = true,
                    SessionEvent::Transcription { source:TranscriptionSource::Input {command:Some(_)}, .. } => return Err("Gemini invented input transcription attribution".into()),
                    SessionEvent::ResponseStarted { .. } | SessionEvent::Output { .. } | SessionEvent::Transcription { .. } | SessionEvent::Usage { .. } | SessionEvent::ControlApplied { .. } => {},
                    unexpected => return Err(format!("unexpected Gemini Live observation: {unexpected:?}").into()),
                }
                let text = proof.output_transcription.to_lowercase();
                let satisfied = match scenario {
                    GeminiScenario::Text => !proof.audio.is_empty() && text.contains("amber"),
                    GeminiScenario::Tool => tool_submitted && proof.audio.len() > after_tool_audio && proof.output_segments.iter().any(|(segment,text)| after_tool_transcript.get(segment) != Some(text) && text.to_lowercase().contains("violet")),
                    GeminiScenario::ManualAudio => !proof.audio.is_empty() && !proof.input_transcription.trim().is_empty() && text.contains("audio input received"),
                    GeminiScenario::ToolCancellation => unreachable!(),
                };
                if completed && turn_ended && satisfied { break; }
            }
            Ok(())
        }).await.map_err(|_|CaseFailure::from("Gemini Live journey exceeded 180 seconds")).and_then(|result|result);
        let cleanup = match owner {
            Some(session) => session.close().await.map_err(Self::error),
            None => Ok(()),
        };
        let export = proof.export(&journey, &cleanup);
        CaseFailure::combine(CaseFailure::combine(journey, cleanup), export)
    }
}

enum GeminiScenario {
    Text,
    Tool,
    ManualAudio,
    ToolCancellation,
}

#[tokio::test]
async fn openai_conversation_session() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::OpenAi)?;
    let result = live.conversation_session().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn anthropic_conversation_session() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Anthropic)?;
    let result = live.conversation_session().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn gemini_conversation_session() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Gemini)?;
    let result = live.conversation_session().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn ollama_conversation_session() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Ollama)?;
    let result = live.conversation_session().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn gemini_live_text_audio_transcription() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Gemini)?;
    let result = live.gemini_live_session(GeminiScenario::Text).await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn gemini_live_tool_round_trip() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Gemini)?;
    let result = live.gemini_live_session(GeminiScenario::Tool).await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn gemini_live_manual_audio_activity() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Gemini)?;
    let result = live.gemini_live_session(GeminiScenario::ManualAudio).await;
    live.finish(result).await;
    Ok(())
}

#[tokio::test]
async fn gemini_live_tool_cancellation_and_recovery() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Gemini)?;
    let result = live
        .gemini_live_session(GeminiScenario::ToolCancellation)
        .await;
    live.finish(result).await;
    Ok(())
}
