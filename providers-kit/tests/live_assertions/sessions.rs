use std::path::Path;
use std::time::Instant;

use providers_kit::generation::{ContentPart, FunctionCall};
use providers_kit::session::{
    CancelTarget, CancellationResolution, CommandId, ResponseId, ResponseOutcome, SessionCommand,
    SessionContinuity, SessionEvent, ToolRequestId, TranscriptionSource,
};
use serde_json::{Value, json};

use crate::live_failures::CaseFailure;

/// Retains input utterances using each final as an unattributed boundary.
/// Every later unattributed observation starts another utterance, even a final-only
/// or duplicate snapshot: missing native identity never permits deduplication.
pub struct InputTranscriptionEvidence {
    segments: Vec<(Option<CommandId>, String, bool)>,
    text: String,
    capture_failure: Option<String>,
}

impl InputTranscriptionEvidence {
    pub fn new() -> Self {
        Self {
            segments: Vec::new(),
            text: String::new(),
            capture_failure: None,
        }
    }

    pub fn observe(
        &mut self,
        command: Option<&CommandId>,
        text: &str,
        finalized: bool,
    ) -> Result<(), String> {
        if let Some(error) = &self.capture_failure {
            return Err(error.clone());
        }
        let mut index = self
            .segments
            .iter()
            .rposition(|(retained, _, _)| retained.as_ref() == command);
        if index.is_some_and(|index| self.segments[index].2) {
            if command.is_none() {
                index = None;
            } else if !finalized {
                return Err("session transcription delta followed its finalized snapshot".into());
            }
        }
        let previous = index.map_or(0, |index| self.segments[index].1.len());
        let next = if finalized {
            Some(text.len())
        } else {
            previous.checked_add(text.len())
        };
        let total = next.and_then(|next| {
            self.text
                .len()
                .checked_sub(previous)
                .and_then(|retained| retained.checked_add(next))
        });
        if total.is_none_or(|total| total > 32 * 1024) {
            let error = "session transcription exceeded 32 KiB".to_owned();
            self.capture_failure = Some(error.clone());
            return Err(error);
        }
        if let Some(index) = index {
            let (_, retained, finished) = &mut self.segments[index];
            if finalized {
                text.clone_into(retained);
            } else {
                retained.push_str(text);
            }
            *finished = finalized;
        } else {
            self.segments
                .push((command.cloned(), text.to_owned(), finalized));
        }
        self.text = self
            .segments
            .iter()
            .map(|(_, text, _)| text.as_str())
            .collect();
        Ok(())
    }

    pub fn text(&self) -> &str {
        &self.text
    }
}

/// Evidence belongs to exactly one turn; text and input transcripts are not audio output.
pub struct SessionTurnEvidence {
    expectation: SessionExpectation,
    audio_input: bool,
    function_call: Option<FunctionCall>,
    function_request: Option<ToolRequestId>,
    function_response: Option<ResponseId>,
    result_command: Option<CommandId>,
    continuation_response: Option<ResponseId>,
    completed_response: Option<ResponseId>,
    result_audio_bytes: usize,
    result_transcription: bool,
    transcripts: Vec<(TranscriptionSource, String, bool, bool)>,
    audio_bytes: usize,
    output_transcript: String,
    input_transcript: InputTranscriptionEvidence,
    completed: bool,
    started: Instant,
    events: Vec<Value>,
    event_bytes: usize,
    input_audio: Vec<u8>,
    output_audio: Vec<u8>,
    capture_failure: Option<String>,
    outcome: Option<Value>,
}

enum SessionExpectation {
    Complete {
        expected: Option<String>,
        function_result_pending: bool,
        function_result_checkpoint: Option<(usize, usize)>,
    },
    AudioCancellation {
        response: Option<ResponseId>,
        pending: Option<CancelTarget>,
        accepted: Option<(CommandId, CancelTarget)>,
        cancelled: bool,
    },
}

impl SessionTurnEvidence {
    pub fn new(expected: impl Into<String>, audio_input: bool) -> Self {
        Self {
            expectation: SessionExpectation::Complete {
                expected: Some(expected.into()),
                function_result_pending: false,
                function_result_checkpoint: None,
            },
            audio_input,
            function_call: None,
            function_request: None,
            function_response: None,
            result_command: None,
            continuation_response: None,
            completed_response: None,
            result_audio_bytes: 0,
            result_transcription: false,
            transcripts: Vec::new(),
            audio_bytes: 0,
            output_transcript: String::new(),
            input_transcript: InputTranscriptionEvidence::new(),
            completed: false,
            started: Instant::now(),
            events: Vec::new(),
            event_bytes: 0,
            input_audio: Vec::new(),
            output_audio: Vec::new(),
            capture_failure: None,
            outcome: None,
        }
    }

    pub fn interrupting_audio() -> Self {
        let mut evidence = Self::new("", false);
        evidence.expectation = SessionExpectation::AudioCancellation {
            response: None,
            pending: None,
            accepted: None,
            cancelled: false,
        };
        evidence
    }

    pub fn awaiting_function() -> Self {
        let mut evidence = Self::new("", false);
        evidence.expectation = SessionExpectation::Complete {
            expected: None,
            function_result_pending: false,
            function_result_checkpoint: None,
        };
        evidence
    }

    pub fn expect_after_function(&mut self, expected: String) -> Result<(), String> {
        let SessionExpectation::Complete {
            expected: retained, ..
        } = &mut self.expectation
        else {
            return Err("interrupted turns cannot acquire a successful answer".into());
        };
        if self.audio_input
            || self.function_call.is_none()
            || self.completed
            || retained.is_some()
            || expected.is_empty()
        {
            return Err(
                "session answer must be assigned once after the expected function call".into(),
            );
        }
        *retained = Some(expected);
        Ok(())
    }

    fn capture_error(&mut self, message: &str) -> String {
        self.capture_failure
            .get_or_insert_with(|| message.to_owned())
            .clone()
    }

    fn retain(&mut self, direction: &str, value: Value) -> Result<(), String> {
        if let Some(error) = &self.capture_failure {
            return Err(error.clone());
        }
        let mut entry = json!({"elapsed_ms": self.started.elapsed().as_millis()});
        entry[direction] = value;
        let bytes = serde_json::to_vec(&entry)
            .map_err(|error| error.to_string())?
            .len();
        if self.events.len() >= 8192 || bytes > 384 * 1024 - self.event_bytes {
            return Err(
                self.capture_error("session event evidence exceeded its 8192-event/384-KiB bound")
            );
        }
        self.event_bytes += bytes;
        self.events.push(entry);
        Ok(())
    }

    pub fn input(&mut self, input: &SessionCommand) -> Result<(), String> {
        if let SessionCommand::Audio { media_type, data } = input {
            if data.len() > 8 * 1024 * 1024 - self.input_audio.len() {
                return Err(self.capture_error("session input PCM evidence exceeded 8 MiB"));
            }
            self.retain(
                "send_attempt",
                json!({"type":"audio", "media_type":media_type,
                "pcm_offset":self.input_audio.len(), "byte_len":data.len()}),
            )?;
            self.input_audio.extend_from_slice(data);
            Ok(())
        } else {
            if let SessionCommand::Cancel(target) = input {
                let SessionExpectation::AudioCancellation {
                    response: Some(response),
                    pending: None,
                    accepted: None,
                    cancelled: false,
                } = &self.expectation
                else {
                    return Err(
                        "session cancellation must follow its observed audio response once".into(),
                    );
                };
                if target != &CancelTarget::Response(response.clone()) {
                    return Err(
                        "session cancellation differs from the observed audio response".into(),
                    );
                }
            }
            if let SessionCommand::ToolResults(results) = input {
                if results.len() != 1 {
                    return Err("session evidence requires its one correlated result".into());
                }
                let submitted = &results[0];
                if self.function_request.as_ref() != Some(submitted.request()) {
                    return Err("session result differs from the active tool request".into());
                }
                let result = submitted.result();
                let Some(call) = &self.function_call else {
                    return Err("session result has no validated function call".into());
                };
                let SessionExpectation::Complete {
                    expected: Some(_),
                    function_result_pending: false,
                    function_result_checkpoint: None,
                } = &self.expectation
                else {
                    return Err("session result must follow one answer assignment".into());
                };
                if self.audio_input
                    || self.completed
                    || call.id().is_none()
                    || result.call_id() != call.id()
                    || result.name() != call.name()
                {
                    return Err("session result differs from the active function identity".into());
                }
            }
            self.retain("send_attempt", Self::command_value(input)?)?;
            if let SessionCommand::Cancel(target) = input
                && let SessionExpectation::AudioCancellation { pending, .. } = &mut self.expectation
            {
                *pending = Some(target.clone());
            }
            if matches!(input, SessionCommand::ToolResults(_))
                && let SessionExpectation::Complete {
                    function_result_pending,
                    ..
                } = &mut self.expectation
            {
                *function_result_pending = true;
            }
            Ok(())
        }
    }

    pub fn sent(&mut self, result: &Result<CommandId, CaseFailure>) -> Result<(), String> {
        self.retain(
            "send_return",
            json!({"succeeded":result.is_ok(),
            "receipt":result.as_ref().ok().map(|id|json!({"session":id.session_id(),"sequence":id.sequence().get()})),
            "failure":result.as_ref().err().map(CaseFailure::evidence)}),
        )?;
        if let SessionExpectation::AudioCancellation {
            pending, accepted, ..
        } = &mut self.expectation
            && let Some(target) = pending.take()
            && let Ok(command) = result
        {
            *accepted = Some((command.clone(), target));
        }
        if let SessionExpectation::Complete {
            function_result_pending,
            function_result_checkpoint,
            ..
        } = &mut self.expectation
            && *function_result_pending
            && result.is_ok()
        {
            self.result_command = result.as_ref().ok().cloned();
            *function_result_pending = false;
            *function_result_checkpoint = Some((self.output_transcript.len(), self.audio_bytes));
        }
        Ok(())
    }

    pub fn observe(&mut self, event: &SessionEvent) -> Result<(), String> {
        if let SessionEvent::Output {
            content: ContentPart::Audio { media_type, data },
            response,
            segment,
        } = event
        {
            if data.len() > 8 * 1024 * 1024 - self.output_audio.len() {
                return Err(self.capture_error("session output PCM evidence exceeded 8 MiB"));
            }
            self.retain("received",json!({"event":"audio","response":{"session":response.session_id(),"sequence":response.sequence().get()},"segment":segment.as_ref().map(|id|id.sequence().get()),"media_type":media_type,"pcm_offset":self.output_audio.len(),"byte_len":data.len()}))?;
            self.output_audio.extend_from_slice(data);
        } else {
            self.retain("received", Self::event_value(event)?)?;
        }
        let terminal = self.completed
            || matches!(
                self.expectation,
                SessionExpectation::AudioCancellation {
                    cancelled: true,
                    ..
                }
            );
        if terminal
            && !matches!(
                event,
                SessionEvent::Transcription {
                    source: TranscriptionSource::Input { .. },
                    ..
                } | SessionEvent::Usage { .. }
                    | SessionEvent::Transcription {
                        finalized: true,
                        ..
                    }
                    | SessionEvent::TurnEnded { .. }
                    | SessionEvent::ControlApplied { .. }
            )
        {
            return Err("session emitted generation activity after turn completion".into());
        }
        match event {
            SessionEvent::ResponseStarted { command, response } => {
                if command
                    .as_ref()
                    .is_some_and(|command| self.result_command.as_ref() == Some(command))
                {
                    self.continuation_response = Some(response.clone());
                }
            }
            SessionEvent::ToolRequested {
                response,
                request,
                call,
            } => {
                if self.audio_input
                    || matches!(
                        self.expectation,
                        SessionExpectation::AudioCancellation { .. }
                    )
                    || self.completed
                    || self.function_call.is_some()
                    || call.name() != "strict_live_session_probe"
                    || call.arguments() != &json!({})
                {
                    return Err("session requested an unexpected or repeated function".into());
                }
                self.function_call = Some(call.clone());
                self.function_request = Some(request.clone());
                self.function_response = Some(response.clone());
            }
            SessionEvent::Output {
                content: ContentPart::Audio { data, .. },
                response,
                ..
            } => {
                if !data.is_empty()
                    && let SessionExpectation::AudioCancellation {
                        response: observed, ..
                    } = &mut self.expectation
                {
                    if observed.as_ref().is_some_and(|prior| prior != response) {
                        return Err(
                            "session cancellation audio belongs to multiple responses".into()
                        );
                    }
                    *observed = Some(response.clone());
                }
                self.audio_bytes += data.len();
                if self.continuation_response.as_ref() == Some(response) {
                    self.result_audio_bytes += data.len();
                }
            }
            SessionEvent::Transcription {
                source,
                text,
                finalized,
            } => self.transcript(source, text, *finalized)?,
            SessionEvent::Interruption { .. } => {
                return Err("session reported an unexpected native interruption".into());
            }
            SessionEvent::TurnEnded { .. } => {}
            SessionEvent::ResponseFinished {
                response,
                outcome: ResponseOutcome::Completed,
                continuity: SessionContinuity::Reusable,
                ..
            } => {
                if !matches!(self.expectation, SessionExpectation::Complete { .. }) {
                    return Err("interruption completed without required cancellation".into());
                }
                if self.function_response.as_ref() != Some(response) {
                    self.completed = true;
                    self.completed_response = Some(response.clone());
                }
            }
            SessionEvent::ResponseFinished {
                outcome: ResponseOutcome::Cancelled,
                ..
            } => {
                if matches!(self.expectation, SessionExpectation::Complete { .. }) {
                    return Err("positive session response was cancelled".into());
                }
            }
            SessionEvent::Cancellation {
                command,
                target,
                resolution: CancellationResolution::Stopped,
                continuity: SessionContinuity::Reusable,
            } => match &mut self.expectation {
                SessionExpectation::AudioCancellation {
                    accepted,
                    cancelled,
                    ..
                } => {
                    if accepted.as_ref().is_none_or(|(receipt, submitted)| {
                        receipt != command || submitted != target
                    }) {
                        return Err("session cancellation acknowledgement differs from its accepted receipt or target".into());
                    }
                    *cancelled = true;
                }
                SessionExpectation::Complete { .. } => {
                    return Err("positive session response was cancelled".into());
                }
            },
            SessionEvent::ToolsInvalidated { .. } => {
                return Err("session invalidated an owned function".into());
            }
            SessionEvent::CommandFailed { error, .. }
            | SessionEvent::Terminated { reason: error, .. } => {
                return Err(format!("session failed accepted work: {error}"));
            }
            SessionEvent::ResponseFinished { .. } | SessionEvent::Cancellation { .. } => {
                return Err("session did not affirm successful completion or cancellation".into());
            }
            SessionEvent::Output { .. }
            | SessionEvent::Usage { .. }
            | SessionEvent::ControlApplied { .. } => {}
        }
        Ok(())
    }

    fn transcript(
        &mut self,
        source: &TranscriptionSource,
        text: &str,
        finalized: bool,
    ) -> Result<(), String> {
        if let TranscriptionSource::Input { command } = source {
            let result = self
                .input_transcript
                .observe(command.as_ref(), text, finalized);
            if let Some(error) = &self.input_transcript.capture_failure {
                self.capture_failure.get_or_insert_with(|| error.clone());
            }
            return result;
        }
        let index = self
            .transcripts
            .iter()
            .position(|(retained, _, _, _)| retained == source);
        let previous = index.map_or(0, |index| self.transcripts[index].1.len());
        if index.is_some_and(|index| self.transcripts[index].2) && !finalized {
            return Err("session transcription delta followed its finalized snapshot".into());
        }
        let next = if finalized {
            Some(text.len())
        } else {
            previous.checked_add(text.len())
        };
        let retained = self.output_transcript.len();
        let total = next.and_then(|next| {
            retained
                .checked_sub(previous)
                .and_then(|retained| retained.checked_add(next))
        });
        if total.is_none_or(|total| total > 32 * 1024) {
            return Err(self.capture_error("session transcription exceeded 32 KiB"));
        }
        let new_progress = matches!(source,TranscriptionSource::Output{response,..} if self.continuation_response.as_ref()==Some(response))
            && text.chars().any(|character| !character.is_whitespace());
        if let Some(index) = index {
            let (_, retained, finished, progress) = &mut self.transcripts[index];
            if finalized {
                text.clone_into(retained);
            } else {
                retained.push_str(text);
            }
            *finished = finalized;
            if finalized {
                *progress = new_progress;
            } else {
                *progress |= new_progress;
            }
        } else {
            self.transcripts
                .push((source.clone(), text.to_owned(), finalized, new_progress));
        }
        self.output_transcript = self
            .transcripts
            .iter()
            .filter_map(|(source, text, _, _)| {
                matches!(source, TranscriptionSource::Output { .. }).then_some(text.as_str())
            })
            .collect();
        self.result_transcription=self.transcripts.iter().any(|(source,_,_,progress)|*progress && matches!(source,TranscriptionSource::Output{response,..} if self.continuation_response.as_ref()==Some(response)));
        Ok(())
    }

    fn command_value(command: &SessionCommand) -> Result<Value, String> {
        Ok(match command {
            SessionCommand::Input(message) => json!({"type":"input","message":message}),
            SessionCommand::ToolResults(results) => {
                json!({"type":"tool_results","results":results.iter().map(|result|json!({"request":{"session":result.request().session_id(),"sequence":result.request().sequence().get()},"result":result.result()})).collect::<Vec<_>>()})
            }
            SessionCommand::Cancel(target) => {
                json!({"type":"cancel","target":format!("{target:?}")})
            }
            SessionCommand::StartInput => json!({"type":"start_input"}),
            SessionCommand::EndInput => json!({"type":"end_input"}),
            SessionCommand::EndAudioStream => json!({"type":"end_audio_stream"}),
            SessionCommand::DiscardInput => json!({"type":"discard_input"}),
            SessionCommand::ReconcilePlayback { segment, played } => {
                json!({"type":"reconcile_playback","segment":{"session":segment.session_id(),"sequence":segment.sequence().get()},"played_ms":played.as_millis()})
            }
            SessionCommand::Audio { .. } => {
                return Err("audio evidence requires PCM sidecar capture".into());
            }
        })
    }

    fn event_value(event: &SessionEvent) -> Result<Value, String> {
        Ok(match event {
            SessionEvent::ResponseStarted { command, response } => {
                json!({"event":"response_started","response":{"session":response.session_id(),"sequence":response.sequence().get()},"command":command.as_ref().map(|id|json!({"session":id.session_id(),"sequence":id.sequence().get()}))})
            }
            SessionEvent::Output {
                response,
                segment,
                content,
            } => {
                json!({"event":"output","response":{"session":response.session_id(),"sequence":response.sequence().get()},"segment":segment.as_ref().map(|id|id.sequence().get()),"content":content})
            }
            SessionEvent::Transcription {
                source,
                text,
                finalized,
            } => {
                json!({"event":"transcription","source":format!("{source:?}"),"text":text,"finalized":finalized})
            }
            SessionEvent::ToolRequested {
                response,
                request,
                call,
            } => {
                json!({"event":"tool_requested","response":{"session":response.session_id(),"sequence":response.sequence().get()},"request":{"session":request.session_id(),"sequence":request.sequence().get()},"call":call})
            }
            SessionEvent::ToolsInvalidated { requests } => {
                json!({"event":"tools_invalidated","requests":requests.iter().map(|id|json!({"session":id.session_id(),"sequence":id.sequence().get()})).collect::<Vec<_>>()})
            }
            SessionEvent::ResponseFinished {
                response,
                outcome,
                continuity,
                output,
            } => {
                json!({"event":"response_finished","response":{"session":response.session_id(),"sequence":response.sequence().get()},"outcome":format!("{outcome:?}"),"continuity":format!("{continuity:?}"),"output":output})
            }
            SessionEvent::Usage {
                scope,
                usage,
                finalized,
            } => {
                json!({"event":"usage","scope":format!("{scope:?}"),"usage":usage,"finalized":finalized})
            }
            SessionEvent::Cancellation {
                command,
                target,
                resolution,
                continuity,
            } => {
                json!({"event":"cancellation","command":{"session":command.session_id(),"sequence":command.sequence().get()},"target":format!("{target:?}"),"resolution":format!("{resolution:?}"),"continuity":format!("{continuity:?}")})
            }
            SessionEvent::CommandFailed {
                command,
                error,
                not_started,
            } => {
                json!({"event":"command_failed","command":{"session":command.session_id(),"sequence":command.sequence().get()},"error":CaseFailure::from(error.clone()).evidence(),"not_started":not_started})
            }
            SessionEvent::Interruption { response } => {
                json!({"event":"interruption","response":response.as_ref().map(|id|json!({"session":id.session_id(),"sequence":id.sequence().get()}))})
            }
            SessionEvent::TurnEnded { response } => {
                json!({"event":"turn_ended","response":response.as_ref().map(|id|json!({"session":id.session_id(),"sequence":id.sequence().get()}))})
            }
            SessionEvent::ControlApplied { command } => {
                json!({"event":"control_applied","command":{"session":command.session_id(),"sequence":command.sequence().get()}})
            }
            SessionEvent::Terminated {
                reason,
                continuity,
                unfinished_commands,
                unfinished_responses,
                outstanding_tools,
            } => {
                json!({"event":"terminated","reason":CaseFailure::from(reason.clone()).evidence(),"continuity":format!("{continuity:?}"),"unfinished_commands":format!("{unfinished_commands:?}"),"unfinished_responses":format!("{unfinished_responses:?}"),"outstanding_tools":format!("{outstanding_tools:?}")})
            }
        })
    }

    pub fn ready(&self) -> bool {
        match &self.expectation {
            SessionExpectation::Complete { .. } => {
                self.completed
                    && (!self.audio_input || !self.input_transcript.text().trim().is_empty())
            }
            SessionExpectation::AudioCancellation { cancelled, .. } => *cancelled,
        }
    }

    pub fn verify(&self) -> Result<(), String> {
        if let Some(error) = &self.capture_failure {
            return Err(error.clone());
        }
        if !self.ready() {
            return Err("session omitted completion or required input transcription".into());
        }
        match &self.expectation {
            SessionExpectation::AudioCancellation { .. } => {
                return if self.audio_bytes > 0 && self.function_call.is_none() {
                    Ok(())
                } else {
                    Err(
                        "audio cancellation omitted actual output progress or returned a function"
                            .into(),
                    )
                };
            }
            SessionExpectation::Complete { .. } => {}
        }
        if self.function_call.is_some() == self.audio_input {
            return Err("session omitted its exactly-once function call".into());
        }
        if !self.audio_input
            && (self.continuation_response.is_none()
                || self.completed_response != self.continuation_response
                || self.result_audio_bytes == 0
                || !self.result_transcription)
        {
            return Err("session omitted correlated result-response completion, audio or substantive transcription".into());
        }
        if self.audio_bytes == 0 {
            return Err("session turn omitted output audio bytes".into());
        }
        let SessionExpectation::Complete {
            expected: Some(expected),
            function_result_pending,
            function_result_checkpoint,
        } = &self.expectation
        else {
            return Err("session expected answer was not assigned".into());
        };
        if expected.is_empty() {
            return Err("session expected answer was empty".into());
        }
        let (_transcript_start, audio_start) = if self.audio_input {
            (0, 0)
        } else {
            if *function_result_pending {
                return Err(
                    "session function result has no successful local acceptance receipt".into(),
                );
            }
            function_result_checkpoint.ok_or("session function result was not locally accepted")?
        };
        if self.audio_bytes <= audio_start {
            return Err("session omitted audio after successful function result".into());
        }
        if self.audio_input
            && !self
                .output_transcript
                .chars()
                .any(|character| !character.is_whitespace())
        {
            return Err("session omitted substantive output transcription after its input".into());
        }
        Ok(())
    }

    fn exact_copy_grade(&self) -> Result<bool, String> {
        self.verify()?;
        match &self.expectation {
            SessionExpectation::Complete {
                expected: Some(expected),
                ..
            } => Ok(&self.output_transcript == expected),
            _ => Err("session copy grade requires a completed answer turn".into()),
        }
    }

    pub fn realtime_grade_receipt(
        case: &str,
        model_id: &str,
        configured_model: &str,
        turns: &[Self],
    ) -> Result<Value, String> {
        if model_id.is_empty() || model_id != configured_model || turns.len() != 2 {
            return Err("Realtime grade identity or turn inventory differs".into());
        }
        let grades = match case {
            "live::resources::openai_interactive_session"
                if !turns[0].audio_input && turns[1].audio_input =>
            {
                json!({
                    "function_turn":turns[0].exact_copy_grade()?,
                    "audio_turn":turns[1].exact_copy_grade()?,
                })
            }
            "live::resources::openai_interactive_cancellation_and_subsequent_progress"
                if matches!(
                    turns[0].expectation,
                    SessionExpectation::AudioCancellation { .. }
                ) && !turns[1].audio_input =>
            {
                turns[0].verify()?;
                json!({"post_cancellation_function_turn":turns[1].exact_copy_grade()?})
            }
            _ => return Err("Realtime grade case or turn roles differ".into()),
        };
        Ok(
            json!({"version":1,"case":case,"model_id":model_id,"configured_model":configured_model,
            "sdk_contract":true,"model_exact_copy":grades}),
        )
    }

    pub fn finish(&mut self, result: &Result<(), CaseFailure>) {
        self.outcome = Some(json!({"succeeded":result.is_ok(),
            "failure":result.as_ref().err().map(CaseFailure::evidence)}));
    }

    pub fn observation(&self) -> Value {
        let (expected, expectation) = match &self.expectation {
            SessionExpectation::Complete { expected, .. } => {
                (expected.as_deref(), json!({"kind":"complete"}))
            }
            SessionExpectation::AudioCancellation { cancelled, .. } => (
                None,
                json!({"kind":"audio_cancellation", "cancelled":cancelled}),
            ),
        };
        let (copy_grade, checkpoint) = match &self.expectation {
            SessionExpectation::AudioCancellation { .. } => (json!("not_applicable"), Value::Null),
            SessionExpectation::Complete { expected, function_result_checkpoint, .. } => (
                if self.verify().is_ok() {
                    expected.as_ref().map_or(json!("unavailable"), |expected| json!(&self.output_transcript == expected))
                } else { json!("unavailable") },
                function_result_checkpoint.map_or(Value::Null, |(transcript_bytes, audio_bytes)| {
                    json!({"transcript_bytes":transcript_bytes,"audio_bytes":audio_bytes})
                }),
            ),
        };
        json!({"audio_input":self.audio_input, "expected":expected, "expectation":expectation,
            "model_exact_copy":copy_grade,"function_result_checkpoint":checkpoint,
            "function_calls":usize::from(self.function_call.is_some()), "output_audio_bytes":self.audio_bytes,
            "output_transcript":self.output_transcript, "input_transcript":self.input_transcript.text(),
            "completed":self.completed, "events":self.events, "event_bytes":self.event_bytes,
            "capture_failure":self.capture_failure, "outcome":self.outcome})
    }

    pub fn export(&self, directory: &Path, prefix: &str) -> (Value, Result<(), CaseFailure>) {
        let mut observation = self.observation();
        let mut audio = Vec::new();
        let mut failures = Vec::new();
        for (direction, bytes) in [("input", &self.input_audio), ("output", &self.output_audio)] {
            if bytes.is_empty() {
                continue;
            }
            let path = directory.join(format!("{prefix}-{direction}.pcm"));
            let exported = CaseFailure::write_session_pcm(&path, bytes).map_err(|error| {
                CaseFailure::from(format!(
                    "required session {direction} PCM export failed: {:?}",
                    error.kind()
                ))
            });
            audio.push(json!({"direction":direction,"path":path,"byte_len":bytes.len(),
                "exported":exported.is_ok(),"failure":exported.as_ref().err().map(CaseFailure::evidence)}));
            if let Err(error) = exported {
                failures.push(error);
            }
        }
        observation["pcm"] = json!(audio);
        (
            observation,
            CaseFailure::collected("session PCM export failures", failures),
        )
    }
}
