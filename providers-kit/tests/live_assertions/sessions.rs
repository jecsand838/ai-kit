use std::path::Path;
use std::time::Instant;

use providers_kit::generation::FunctionCall;
use providers_kit::session::{SessionEvent, SessionInput};
use serde_json::{Value, json};

use crate::live_failures::CaseFailure;

/// Evidence belongs to exactly one turn; text and input transcripts are not audio output.
pub struct SessionTurnEvidence {
    expectation: SessionExpectation,
    audio_input: bool,
    function_call: Option<FunctionCall>,
    audio_bytes: usize,
    output_transcript: String,
    input_transcript: String,
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
            audio_bytes: 0,
            output_transcript: String::new(),
            input_transcript: String::new(),
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
        evidence.expectation = SessionExpectation::AudioCancellation { cancelled: false };
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

    pub fn input(&mut self, input: &SessionInput) -> Result<(), String> {
        if let SessionInput::Audio { media_type, data } = input {
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
            if let SessionInput::FunctionResult(result) = input {
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
            self.retain(
                "send_attempt",
                serde_json::to_value(input).map_err(|error| error.to_string())?,
            )?;
            if matches!(input, SessionInput::FunctionResult(_))
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

    pub fn sent(&mut self, result: &Result<(), CaseFailure>) -> Result<(), String> {
        self.retain(
            "send_return",
            json!({"succeeded":result.is_ok(),
            "failure":result.as_ref().err().map(CaseFailure::evidence)}),
        )?;
        if let SessionExpectation::Complete {
            function_result_pending,
            function_result_checkpoint,
            ..
        } = &mut self.expectation
            && *function_result_pending
            && result.is_ok()
        {
            *function_result_pending = false;
            *function_result_checkpoint = Some((self.output_transcript.len(), self.audio_bytes));
        }
        Ok(())
    }

    pub fn observe(&mut self, event: &SessionEvent) -> Result<(), String> {
        if let SessionEvent::Audio { media_type, data } = event {
            if data.len() > 8 * 1024 * 1024 - self.output_audio.len() {
                return Err(self.capture_error("session output PCM evidence exceeded 8 MiB"));
            }
            self.retain(
                "received",
                json!({"event":"audio", "media_type":media_type,
                "pcm_offset":self.output_audio.len(), "byte_len":data.len()}),
            )?;
            self.output_audio.extend_from_slice(data);
        } else {
            self.retain(
                "received",
                serde_json::to_value(event).map_err(|error| error.to_string())?,
            )?;
        }
        let terminal = self.completed
            || match &self.expectation {
                SessionExpectation::AudioCancellation { cancelled } => *cancelled,
                SessionExpectation::Complete { .. } => false,
            };
        if terminal
            && !matches!(
                event,
                SessionEvent::InputTranscriptionDelta { .. } | SessionEvent::Usage(_)
            )
        {
            return Err("session emitted generation activity after turn completion".into());
        }
        match event {
            SessionEvent::FunctionCall(call) => {
                if self.audio_input
                    || matches!(
                        self.expectation,
                        SessionExpectation::AudioCancellation { .. }
                    )
                    || self.completed
                    || self.function_call.is_some()
                    || call.name() != "strict_live_session_probe"
                    || call.arguments() != &serde_json::json!({})
                {
                    return Err("session requested an unexpected or repeated function".into());
                }
                self.function_call = Some(call.clone());
            }
            SessionEvent::Audio { data, .. } => self.audio_bytes += data.len(),
            SessionEvent::OutputTranscriptionDelta { delta } => {
                if delta.len() > 32 * 1024 - self.output_transcript.len() {
                    return Err(self.capture_error("session output transcription exceeded 32 KiB"));
                }
                self.output_transcript.push_str(delta);
            }
            SessionEvent::InputTranscriptionDelta { delta } => {
                if delta.len() > 32 * 1024 - self.input_transcript.len() {
                    return Err(self.capture_error("session input transcription exceeded 32 KiB"));
                }
                self.input_transcript.push_str(delta);
            }
            SessionEvent::Completed => {
                if !matches!(self.expectation, SessionExpectation::Complete { .. }) {
                    return Err("interruption case completed without required cancellation".into());
                }
                self.completed = true;
            }
            SessionEvent::TurnCancelled => match &mut self.expectation {
                SessionExpectation::Complete { .. } => {
                    return Err("positive session turn was cancelled".into());
                }
                SessionExpectation::AudioCancellation { cancelled } => *cancelled = true,
            },
            SessionEvent::FunctionCallsCancelled { .. } => {
                return Err("session cancelled an unowned function".into());
            }
            SessionEvent::TextDelta { .. } | SessionEvent::Usage(_) => {}
        }
        Ok(())
    }

    pub fn ready(&self) -> bool {
        match &self.expectation {
            SessionExpectation::Complete { .. } => {
                self.completed && (!self.audio_input || !self.input_transcript.trim().is_empty())
            }
            SessionExpectation::AudioCancellation { cancelled } => *cancelled,
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
        let (transcript_start, audio_start) = if self.audio_input {
            (0, 0)
        } else {
            if *function_result_pending {
                return Err("session function result has no successful send return".into());
            }
            function_result_checkpoint.ok_or("session function result was not successfully sent")?
        };
        if self.audio_bytes <= audio_start {
            return Err("session omitted audio after successful function result".into());
        }
        if !self.output_transcript[transcript_start..]
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
            SessionExpectation::AudioCancellation { cancelled } => (
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
            "output_transcript":self.output_transcript, "input_transcript":self.input_transcript,
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
