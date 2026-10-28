use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

use futures_util::StreamExt;
use providers_kit::error::{ErrorKind, ResponseViolation};
use providers_kit::generation::{
    ContentPart, GenerateEvent, GenerateEventStream, GenerateRequest, GenerateResponse,
};
use providers_kit::{KitError, ModelId};
use serde_json::{Value, json};

#[derive(Clone, Copy)]
pub enum CloudPhase {
    Initial,
    Reopened,
}

impl CloudPhase {
    fn name(self) -> &'static str {
        match self {
            Self::Initial => "initial",
            Self::Reopened => "reopened",
        }
    }
}

// Diagnostic state for the existing Custom cloud journey. It never stores
// prompts, generated text, arguments, reasoning, continuations, or credentials.
pub struct AnthropicEvidence {
    model: String,
    providers: usize,
    generations: Vec<Value>,
    phase_cleanup: Vec<Value>,
    provider_shutdown: Vec<Value>,
    failure: Option<&'static str>,
}

impl AnthropicEvidence {
    pub fn new(model: &str) -> Self {
        let valid_model = model.len() <= 1_024 && !model.chars().any(char::is_control);
        Self {
            model: if valid_model {
                model.to_owned()
            } else {
                String::new()
            },
            providers: 0,
            generations: Vec::new(),
            phase_cleanup: Vec::new(),
            provider_shutdown: Vec::new(),
            failure: (!valid_model).then_some("selected_model_bound"),
        }
    }

    fn fail(&mut self, failure: &'static str) {
        self.failure.get_or_insert(failure);
    }

    pub(crate) fn provider_phase(&mut self) -> CloudPhase {
        let phase = if self.providers == 0 {
            CloudPhase::Initial
        } else {
            if self.providers > 1 {
                self.fail("provider_construction_bound");
            }
            CloudPhase::Reopened
        };
        self.providers = self.providers.saturating_add(1).min(3);
        phase
    }

    pub fn begin(
        &mut self,
        phase: CloudPhase,
        model: &ModelId,
        request: &GenerateRequest,
        streamed: bool,
    ) -> Option<usize> {
        if self.generations.len() == 12 {
            self.fail("generation_bound");
            return None;
        }
        if model.as_str() != self.model || self.model.len() > 1_024 {
            self.fail("selected_model_mismatch");
        }
        if request.messages().len() > 32 || request.tools().len() > 32 {
            self.fail("request_inventory_bound");
        }
        let index = self.generations.len();
        self.generations.push(json!({
            "ordinal":index + 1,"phase":phase.name(),"model_matches_selected":model.as_str()==self.model,
            "streamed":streamed,"request_messages":request.messages().len().min(33),
            "request_tools":request.tools().len().min(33),
            "events":{"started":0,"content_delta":0,"thinking_delta":0,"hosted_tool":0,"usage":0,"completed":0,"error":0},
            "event_count":0,"delta_text_bytes":0,"delta_nonblank_text":false,
            "completed_text_bytes":0,"completed_nonblank_text":false,
            "terminal":"pending","finish_reason":null,"function_calls":0,"error":null,
            "acquisition_error":false,
        }));
        Some(index)
    }

    pub fn inspect_stream(
        shared: &Arc<Mutex<Self>>,
        generation: Option<usize>,
        stream: GenerateEventStream,
    ) -> GenerateEventStream {
        let shared = Arc::clone(shared);
        GenerateEventStream::new(stream.inspect(move |item| {
            shared
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .observe(generation, item);
        }))
    }

    pub fn observe(&mut self, generation: Option<usize>, item: &Result<GenerateEvent, KitError>) {
        let Some(index) = generation else { return };
        let Some(row) = self.generations.get_mut(index) else {
            self.fail("unknown_generation");
            return;
        };
        if row["terminal"] != "pending" {
            self.fail("event_after_terminal");
            return;
        }
        let count = row["event_count"].as_u64().unwrap_or(0);
        if count >= 8_192 {
            self.fail("event_bound");
            return;
        }
        row["event_count"] = json!(count + 1);
        let key = match item {
            Ok(GenerateEvent::Started { model }) => {
                if model.as_str() != self.model {
                    self.fail("response_model_mismatch");
                    return;
                }
                "started"
            }
            Ok(GenerateEvent::ContentDelta { delta }) => {
                let bytes = row["delta_text_bytes"]
                    .as_u64()
                    .unwrap_or(0)
                    .saturating_add(delta.len() as u64);
                row["delta_text_bytes"] = json!(bytes.min(1_048_576));
                row["delta_nonblank_text"] =
                    json!(row["delta_nonblank_text"] == true || !delta.trim().is_empty());
                if bytes > 1_048_576 {
                    self.fail("text_observation_bound");
                    return;
                }
                "content_delta"
            }
            Ok(GenerateEvent::ThinkingDelta { .. }) => "thinking_delta",
            Ok(GenerateEvent::HostedTool(_)) => "hosted_tool",
            Ok(GenerateEvent::Usage(_)) => "usage",
            Ok(GenerateEvent::Completed(response)) => {
                self.completion(generation, response);
                "completed"
            }
            Err(error) => {
                self.record_error(index, error);
                "error"
            }
        };
        let row = &mut self.generations[index];
        row["events"][key] = json!(row["events"][key].as_u64().unwrap_or(0) + 1);
    }

    pub(crate) fn completion(&mut self, generation: Option<usize>, response: &GenerateResponse) {
        let Some(index) = generation else { return };
        if response.model.as_str() != self.model {
            self.fail("response_model_mismatch");
        }
        let row = &mut self.generations[index];
        row["terminal"] = json!("completed");
        row["finish_reason"] = json!(response.finish_reason);
        row["function_calls"] = json!(response.message.function_calls().len().min(257));
        let mut bytes = 0_usize;
        let mut nonblank = false;
        for part in response.message.content() {
            if let ContentPart::Text { text } = part {
                bytes = bytes.saturating_add(text.len());
                nonblank |= !text.trim().is_empty();
            }
        }
        row["completed_text_bytes"] = json!(bytes.min(1_048_576));
        row["completed_nonblank_text"] = json!(nonblank);
        if bytes > 1_048_576 || response.message.function_calls().len() > 256 {
            self.fail("completion_observation_bound");
        }
    }

    fn error_facts(error: &KitError) -> Result<Value, &'static str> {
        let (kind, violation) = match error.kind() {
            ErrorKind::InvalidResponse(violation) => (
                "InvalidResponse".to_owned(),
                Some(match violation {
                    ResponseViolation::MalformedJson => "MalformedJson",
                    ResponseViolation::InvalidUtf8 => "InvalidUtf8",
                    ResponseViolation::MissingField { .. } => "MissingField",
                    ResponseViolation::InvalidField { .. } => "InvalidField",
                    ResponseViolation::UnknownVariant { .. } => "UnknownVariant",
                    ResponseViolation::IdentityMismatch { .. } => "IdentityMismatch",
                    ResponseViolation::ProtocolViolation { .. } => "ProtocolViolation",
                    ResponseViolation::UnexpectedEnd => "UnexpectedEnd",
                    ResponseViolation::LimitExceeded { .. } => "LimitExceeded",
                }),
            ),
            kind => (format!("{kind:?}"), None),
        };
        let rule = match error.kind() {
            ErrorKind::InvalidResponse(ResponseViolation::ProtocolViolation { rule }) => {
                if rule.len() > 512 || rule.chars().any(char::is_control) {
                    return Err("protocol_rule_bound");
                }
                Some(rule)
            }
            _ => None,
        };
        Ok(
            json!({"kind":kind,"violation":violation,"protocol_rule":rule,
            "operation":format!("{:?}",error.context().operation),
            "phase":format!("{:?}",error.context().phase),"http_status":error.http_status()}),
        )
    }

    fn record_error(&mut self, index: usize, error: &KitError) {
        if self.generations[index]["terminal"] != "pending" {
            self.fail("event_after_terminal");
            return;
        }
        self.generations[index]["terminal"] = json!("error");
        match Self::error_facts(error) {
            Ok(facts) => self.generations[index]["error"] = facts,
            Err(failure) => self.fail(failure),
        }
    }

    pub fn acquisition_error(&mut self, generation: Option<usize>, error: &KitError) {
        if let Some(index) = generation {
            self.record_error(index, error);
            self.generations[index]["acquisition_error"] = json!(true);
        }
    }

    pub fn phase_cleanup(&mut self, phase: CloudPhase, succeeded: bool) {
        if self.phase_cleanup.len() >= 2 {
            self.fail("phase_cleanup_bound");
        } else {
            self.phase_cleanup
                .push(json!({"phase":phase.name(),"succeeded":succeeded}));
        }
    }

    pub(crate) fn shutdown(&mut self, phase: CloudPhase, result: &Result<(), KitError>) {
        if self.provider_shutdown.len() >= 2 {
            self.fail("provider_shutdown_bound");
        } else {
            self.provider_shutdown
                .push(json!({"phase":phase.name(),"attempted":true,"succeeded":result.is_ok()}));
        }
    }

    pub fn snapshot(&self, scenario_succeeded: bool, scenario_cleanup_succeeded: bool) -> Value {
        json!({"version":1,"case":"strict_live_suite::custom::custom_anthropic_continuation_stream_and_reopen_are_live",
            "configured_model":self.model,"capture_complete":self.failure.is_none() && !self.generations.is_empty() && self.generations.iter().all(|row|row["terminal"]!="pending"),
            "capture_failure":self.failure,"scenario_succeeded":scenario_succeeded,"scenario_cleanup_succeeded":scenario_cleanup_succeeded,
            "phase_cleanup":self.phase_cleanup,"provider_shutdown":self.provider_shutdown,"generations":self.generations})
    }

    pub fn export_after_cleanup(
        &self,
        path: &Path,
        succeeded: bool,
        cleaned: bool,
    ) -> io::Result<()> {
        if !path.is_absolute() {
            return Err(io::Error::other("Custom evidence path must be absolute"));
        }
        let body = serde_json::to_vec(&self.snapshot(succeeded, cleaned))?;
        if body.len() > 65_536 {
            return Err(io::Error::other("Custom evidence output bound"));
        }
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(path)?;
        file.write_all(&body)?;
        file.flush()
    }
}
