use providers_kit::generation::{ContentPart, GenerateRequest, GenerateResponse};
use providers_kit::{KitError as ProviderError, ModelId};
use serde_json::{Value, json};

#[derive(Default)]
pub struct KnowledgeEvidence {
    observation: Option<KnowledgeObservation>,
    unexpected_generation: bool,
    completed: Vec<KnowledgeObservation>,
    capture_failure: Option<&'static str>,
}

struct KnowledgeObservation {
    phase: &'static str,
    markers: [String; 6],
    expected_counts: [usize; 6],
    request_counts: [usize; 6],
    calls: usize,
    streamed: bool,
    provider_text: Option<String>,
    agent_matches: bool,
    agent_text: Option<String>,
    requests: Vec<Value>,
    response_received: bool,
    response_model: Option<String>,
    provider_error_kind: Option<String>,
    assertions_succeeded: bool,
}

impl KnowledgeEvidence {
    pub fn prepare(
        &mut self,
        phase: &'static str,
        markers: [String; 6],
        expected_counts: [usize; 6],
    ) {
        assert!(
            self.observation.is_none(),
            "previous knowledge evidence must be checked"
        );
        if self.completed.len() >= 3 || !matches!(phase, "primary" | "reopened" | "secondary") {
            self.capture_failure.get_or_insert("phase_bound");
            return;
        }
        if markers.iter().any(|marker| {
            marker.is_empty() || marker.len() > 128 || marker.chars().any(char::is_control)
        }) || expected_counts.iter().any(|count| *count > 1)
        {
            self.capture_failure.get_or_insert("expectation_bound");
            return;
        }
        self.observation = Some(KnowledgeObservation {
            phase,
            markers,
            expected_counts,
            request_counts: [0; 6],
            calls: 0,
            streamed: false,
            provider_text: None,
            agent_matches: false,
            agent_text: None,
            requests: Vec::new(),
            response_received: false,
            response_model: None,
            provider_error_kind: None,
            assertions_succeeded: false,
        });
    }

    pub fn request(&mut self, model: &ModelId, request: &GenerateRequest, streamed: bool) {
        let Some(observation) = self.observation.as_mut() else {
            self.unexpected_generation = true;
            return;
        };
        observation.calls = observation.calls.saturating_add(1);
        observation.streamed |= streamed;
        for message in request.messages() {
            for part in message.content() {
                if let ContentPart::Text { text } = part {
                    for (index, marker) in observation.markers.iter().enumerate() {
                        observation.request_counts[index] = observation.request_counts[index]
                            .saturating_add(text.matches(marker).count());
                    }
                }
            }
        }
        if observation.calls != 1 || streamed {
            self.capture_failure.get_or_insert("generation_shape");
            return;
        }
        match KnowledgeObservation::request_value(model, request) {
            Ok(value) => observation.requests.push(value),
            Err(failure) => {
                self.capture_failure.get_or_insert(failure);
            }
        }
    }

    pub fn response(&mut self, response: &Result<GenerateResponse, ProviderError>) {
        let Some(observation) = self.observation.as_mut() else {
            self.unexpected_generation = true;
            return;
        };
        if observation.response_received {
            self.capture_failure.get_or_insert("duplicate_response");
            return;
        }
        observation.response_received = true;
        match response {
            Ok(response) => {
                if response.model.as_str().len() > 1_024 {
                    self.capture_failure.get_or_insert("response_model_bound");
                    return;
                }
                observation.response_model = Some(response.model.as_str().to_owned());
                // This bounds evidence only: oversized responses still return unchanged.
                let mut text = String::new();
                for part in response.message.content() {
                    if let ContentPart::Text { text: part } = part {
                        if part.len() > 65_536_usize.saturating_sub(text.len()) {
                            eprintln!(
                                "knowledge_boundary phase={} evidence=oversized",
                                observation.phase
                            );
                            observation.provider_text = None;
                            self.capture_failure.get_or_insert("provider_text_bound");
                            return;
                        }
                        text.push_str(part);
                    }
                }
                let markers: Vec<&str> = text
                    .split_whitespace()
                    .filter(|word| {
                        word.starts_with("STRICT_")
                            && word.len() <= 128
                            && word.bytes().all(|byte| {
                                byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_'
                            })
                    })
                    .take(6)
                    .collect();
                eprintln!(
                    "knowledge_boundary phase={} calls={} streamed={} request_marker_counts={:?} expected_counts={:?} provider_bytes={} provider_markers={markers:?}",
                    observation.phase,
                    observation.calls,
                    observation.streamed,
                    observation.request_counts,
                    observation.expected_counts,
                    text.len()
                );
                // These fixtures generate only their fresh synthetic scope markers.
                // Retain the exact bounded text for same-generation native comparison.
                eprintln!(
                    "knowledge_provider_text={}",
                    serde_json::json!({"phase": observation.phase, "text": text})
                );
                observation.provider_text = Some(text);
            }
            Err(error) => {
                observation.provider_error_kind = Some(match error.kind() {
                    providers_kit::error::ErrorKind::InvalidResponse(_) => {
                        "InvalidResponse".to_owned()
                    }
                    kind => format!("{kind:?}"),
                });
                eprintln!(
                    "knowledge_boundary phase={} calls={} request_marker_counts={:?} provider_error={:?}",
                    observation.phase,
                    observation.calls,
                    observation.request_counts,
                    error.kind()
                );
            }
        }
    }

    pub fn agent_output(&mut self, output: &str) {
        if let Some(observation) = self.observation.as_mut() {
            observation.agent_matches = observation.provider_text.as_deref() == Some(output);
            if observation.agent_text.is_some() {
                self.capture_failure.get_or_insert("duplicate_agent_output");
            } else if output.len() > 65_536 {
                self.capture_failure.get_or_insert("agent_text_bound");
            } else {
                observation.agent_text = Some(output.to_owned());
            }
            eprintln!(
                "knowledge_boundary phase={} agent_bytes={} provider_agent_equal={}",
                observation.phase,
                output.len(),
                observation.agent_matches
            );
        } else {
            self.unexpected_generation = true;
        }
    }

    pub fn assert_phase(&mut self) {
        assert!(
            !self.unexpected_generation,
            "generation outside the observed phase"
        );
        let observation = self
            .observation
            .as_ref()
            .expect("knowledge phase evidence must exist");
        assert_eq!(
            observation.calls, 1,
            "exactly one real unary call per phase"
        );
        assert!(
            !observation.streamed,
            "knowledge prompts must retain their unary path"
        );
        assert_eq!(
            observation.request_counts, observation.expected_counts,
            "exact managed markers must reach the same real Provider call"
        );
        assert!(
            observation.agent_matches,
            "Agent output must equal the unchanged normalized Provider Text"
        );
        let mut observation = self
            .observation
            .take()
            .expect("checked knowledge phase must remain owned");
        observation.assertions_succeeded = true;
        self.completed.push(observation);
    }

    pub fn snapshot(&self, case: &str, succeeded: bool, cleaned: bool) -> Value {
        let rows: Vec<_> = self
            .completed
            .iter()
            .chain(self.observation.iter())
            .collect();
        let complete = self.capture_failure.is_none()
            && !self.unexpected_generation
            && !rows.is_empty()
            && rows.iter().all(|row| {
                row.calls == 1
                    && !row.streamed
                    && row.requests.len() == 1
                    && row.response_received
                    && (row.provider_error_kind.is_some()
                        || (row.provider_text.is_some() && row.agent_text.is_some()))
            });
        let phases: Vec<_> = rows.iter().map(|row| row.snapshot()).collect();
        json!({"version":1, "case":case, "scenario_succeeded":succeeded,
            "scenario_cleanup_succeeded":cleaned, "unexpected_generation":self.unexpected_generation,
            "capture_complete":complete,"capture_failure":self.capture_failure,"phases":phases})
    }

    pub fn export_after_cleanup(
        &self,
        path: &std::path::Path,
        case: &str,
        succeeded: bool,
        cleaned: bool,
    ) -> std::io::Result<()> {
        use std::io::Write;
        if !path.is_absolute() {
            return Err(std::io::Error::other(
                "knowledge evidence path must be absolute",
            ));
        }
        let body = serde_json::to_vec(&self.snapshot(case, succeeded, cleaned))?;
        if body.len() > 1_048_576 {
            return Err(std::io::Error::other("knowledge evidence output bound"));
        }
        let mut options = std::fs::OpenOptions::new();
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

impl KnowledgeObservation {
    fn request_value(model: &ModelId, request: &GenerateRequest) -> Result<Value, &'static str> {
        if model.as_str().len() > 1_024
            || request.messages().len() > 32
            || !request.tools().is_empty()
        {
            return Err("request_inventory_bound");
        }
        let mut messages = Vec::new();
        let mut bytes = 0_usize;
        for message in request.messages() {
            if message.content().len() > 32
                || !message.function_calls().is_empty()
                || !message.function_results().is_empty()
                || message.continuation().is_some()
            {
                return Err("request_message_shape");
            }
            let mut text_parts = Vec::new();
            for part in message.content() {
                let ContentPart::Text { text } = part else {
                    return Err("request_nontext_content");
                };
                bytes = bytes.saturating_add(text.len());
                if text.len() > 65_536 || bytes > 131_072 {
                    return Err("request_text_bound");
                }
                text_parts.push(text);
            }
            messages.push(json!({"role":message.role(),"text_parts":text_parts}));
        }
        Ok(
            json!({"model":model.as_str(),"messages":messages,"tools_count":0,"function_calls_count":0,"function_results_count":0}),
        )
    }

    fn snapshot(&self) -> Value {
        let sources = [
            ("strict-database", "Database scope"),
            ("strict-agent-a", "Agent A scope"),
            ("strict-workspace-a", "Workspace A scope"),
            ("strict-conversation-a", "Conversation A scope"),
            ("strict-conversation-reopened", "Conversation reopen scope"),
            ("strict-agent-b", "Agent B scope"),
        ];
        let expected_entries: Vec<_> = sources.iter().enumerate().map(|(index, (source_id, title))| json!({
            "source_id":source_id,"title":title,"text":format!("The managed marker value is {}.",self.markers[index]),
            "marker":self.markers[index],"expected_count":self.expected_counts[index],
        })).collect();
        json!({"phase":self.phase,"expected_entries":expected_entries,"expected_counts":self.expected_counts,
            "request_counts":self.request_counts,"calls":self.calls,"streamed":self.streamed,"requests":self.requests,
            "response_received":self.response_received,"response_model":self.response_model,"provider_error_kind":self.provider_error_kind,
            "provider_text":self.provider_text,"agent_text":self.agent_text,"provider_agent_equal":self.agent_matches,
            "assertions_succeeded":self.assertions_succeeded})
    }
}
