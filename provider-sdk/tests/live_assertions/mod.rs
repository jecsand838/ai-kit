mod batches;
mod sessions;
mod stores;
pub use sessions::SessionTurnEvidence;
pub use stores::StoreFixture;

pub use batches::{
    BatchCleanup, DeletedFileEvidence, FileInventoryEvidence, GeminiOutputRetention,
    InlinePublication,
};

use provider_sdk::error::{ErrorKind, ErrorPhase, NativeDetail, Operation};
use std::collections::BTreeSet;

use crate::hosted_assertions::FileSearchContract;
use crate::live_failures::CaseFailure;
use provider_sdk::SdkError;
use provider_sdk::batch::{BatchJob, BatchResult, BatchStatus};
use provider_sdk::generation::{ContentPart, FinishReason, GenerateEvent, GenerateResponse, Role};
use provider_sdk::hosted::{HostedToolOutput, HostedToolStatus};
use provider_sdk::models::ModelId;

pub struct GenerationEvidence;

impl GenerationEvidence {
    pub fn selected_model(selected: &ModelId, response: &GenerateResponse) -> Result<(), String> {
        // These frozen current catalog IDs are canonical snapshots. Do not
        // generalize equality to legacy aliases or Gemini's native modelVersion.
        // https://developers.openai.com/api/docs/models
        // https://platform.claude.com/docs/en/about-claude/models/model-ids-and-versions
        if matches!(
            selected.as_str(),
            "gpt-6-astra"
                | "gpt-6.1-sol"
                | "gpt-6-sol"
                | "gpt-6-luna"
                | "claude-fable-5-1"
                | "claude-opus-5-5"
                | "claude-sonnet-5-5"
                | "claude-haiku-5-5"
        ) && response.model != *selected
        {
            return Err("current canonical response model differs from exact selection".into());
        }
        Ok(())
    }

    pub fn text(response: &GenerateResponse) -> Result<String, String> {
        if response.message.role() != Role::Assistant {
            return Err("generation did not return an assistant message".to_owned());
        }
        let mut text = String::new();
        for part in response.message.content() {
            match part {
                ContentPart::Text { text: part } => text.push_str(part),
                ContentPart::Thinking { .. } => {}
                _ => return Err("text scenario returned an unexpected content type".to_owned()),
            }
        }
        Ok(text)
    }

    pub fn completed(response: &GenerateResponse) -> Result<(), String> {
        if response.finish_reason != Some(FinishReason::Stop) {
            return Err(format!(
                "generation did not stop successfully: {:?}",
                response.finish_reason
            ));
        }
        if response.model.as_str().is_empty() || Self::text(response)?.trim().is_empty() {
            return Err("generation omitted model identity or final text".to_owned());
        }
        if !response.message.function_calls().is_empty() {
            return Err("completed text generation returned unexpected function calls".to_owned());
        }
        Ok(())
    }

    pub fn exact_text(
        response: &GenerateResponse,
        expected: &str,
        allow_hosted: bool,
    ) -> Result<(), String> {
        Self::completed(response)?;
        let actual = Self::text(response)?;
        if actual != expected {
            return Err(format!(
                "generation answer differs from exact expected text; expected_bytes={}, actual_bytes={}",
                expected.len(),
                actual.len()
            ));
        }
        if !allow_hosted && !response.hosted_tool_outputs.is_empty() {
            return Err("plain generation returned unexpected hosted output".to_owned());
        }
        Ok(())
    }
}

pub struct NegativeStreamEvidence {
    state: NegativeStreamState,
}

enum NegativeStreamState {
    BeforeStart,
    Active,
    Rejected(SdkError),
}

impl NegativeStreamEvidence {
    pub fn new() -> Self {
        Self {
            state: NegativeStreamState::BeforeStart,
        }
    }

    pub fn observe(&mut self, item: Result<GenerateEvent, SdkError>) -> Result<(), CaseFailure> {
        if let NegativeStreamState::Rejected(error) = &self.state {
            let mut failure =
                CaseFailure::from("negative stream emitted an item after its terminal error")
                    .with_sdk(error.clone());
            if let Err(next) = item {
                failure = failure.with_sdk(next);
            }
            return Err(failure);
        }
        match item {
            Err(error) => self.state = NegativeStreamState::Rejected(error),
            Ok(GenerateEvent::Started { .. }) => {
                if matches!(self.state, NegativeStreamState::Active) {
                    return Err("negative stream repeated Started".into());
                }
                self.state = NegativeStreamState::Active;
            }
            Ok(GenerateEvent::Completed(_)) => {
                return Err("negative stream completed instead of rejecting the request".into());
            }
            Ok(
                GenerateEvent::ContentDelta { .. }
                | GenerateEvent::ThinkingDelta { .. }
                | GenerateEvent::Usage(_)
                | GenerateEvent::HostedTool(_),
            ) => {
                if matches!(self.state, NegativeStreamState::BeforeStart) {
                    return Err("negative stream emitted progress before Started".into());
                }
            }
        }
        Ok(())
    }

    pub fn finish(self) -> Result<SdkError, CaseFailure> {
        match self.state {
            NegativeStreamState::Rejected(error) => Ok(error),
            NegativeStreamState::BeforeStart | NegativeStreamState::Active => {
                Err("negative stream ended without its required error".into())
            }
        }
    }

    pub fn interrupted(self, label: &'static str) -> CaseFailure {
        let failure = CaseFailure::from(label);
        match self.state {
            NegativeStreamState::Rejected(error) => failure.with_sdk(error),
            NegativeStreamState::BeforeStart | NegativeStreamState::Active => failure,
        }
    }
}

#[derive(Clone, Copy)]
pub enum FileSearchStage {
    Retrieval,
    Preflight,
    PostDelete,
}

impl FileSearchStage {
    pub fn label(self) -> &'static str {
        match self {
            Self::Retrieval => "retrieval",
            Self::Preflight => "preflight",
            Self::PostDelete => "post_delete",
        }
    }
}

pub struct FileSearchCapture {
    pub contract: FileSearchContract,
    pub selected_model: String,
    pub stage: FileSearchStage,
    pub directory: std::path::PathBuf,
    pub secrets: Vec<String>,
}

pub struct StreamEvidence {
    model: Option<String>,
    hosted: Vec<HostedToolOutput>,
    started: bool,
    text: String,
    saw_text: bool,
    terminal: Option<Result<GenerateResponse, SdkError>>,
    capture: Option<FileSearchCapture>,
    completion_count: usize,
    export_failures: Vec<CaseFailure>,
}

impl StreamEvidence {
    pub fn new() -> Self {
        Self {
            model: None,
            hosted: Vec::new(),
            started: false,
            text: String::new(),
            saw_text: false,
            terminal: None,
            capture: None,
            completion_count: 0,
            export_failures: Vec::new(),
        }
    }

    pub fn with_file_search_capture(mut self, capture: FileSearchCapture) -> Self {
        if capture.contract == FileSearchContract::GeminiStreaming
            && capture.selected_model == "models/gemini-2.5-flash"
        {
            self.capture = Some(capture);
        }
        self
    }

    pub fn take_export_failures(&mut self) -> Vec<CaseFailure> {
        std::mem::take(&mut self.export_failures)
    }

    pub fn observe_result(
        &mut self,
        item: Result<GenerateEvent, SdkError>,
    ) -> Result<(), CaseFailure> {
        if let Ok(GenerateEvent::Completed(response)) = &item {
            self.capture_completion(response);
        }
        if let Some(Err(error)) = &self.terminal {
            let mut failure = CaseFailure::from("stream emitted an item after its terminal error")
                .with_sdk(error.clone());
            if let Err(next) = item {
                failure = failure.with_sdk(next);
            }
            return Err(failure);
        }
        match item {
            Ok(event) => self.observe(event).map_err(CaseFailure::from),
            Err(error) => {
                if self.terminal.is_some() {
                    return Err(CaseFailure::from("stream emitted an error after Completed")
                        .with_sdk(error));
                }
                self.terminal = Some(Err(error));
                Ok(())
            }
        }
    }

    fn capture_completion(&mut self, response: &GenerateResponse) {
        let Some(capture) = &self.capture else {
            return;
        };
        self.completion_count += 1;
        let mut observation = capture.contract.observation(response);
        observation["selected_model"] = serde_json::json!(capture.selected_model);
        observation["stage"] = serde_json::json!(capture.stage.label());
        observation["completion_ordinal"] = serde_json::json!(self.completion_count);
        observation["stream_acceptance"] = serde_json::json!("not_established");
        observation["retrieval_acceptance"] = serde_json::json!("not_established");
        observation["retrieval_revocation_proven"] = serde_json::json!(false);
        let exclusion = observation["strict_file_search_only_requirement_met"].clone();
        let thread = std::thread::current();
        let case = thread.name().unwrap_or("unnamed_case").replace(':', "_");
        let path = capture.directory.join(format!(
            "{case}.{}.{}.{}.response.json",
            capture.stage.label(),
            self.completion_count,
            uuid::Uuid::new_v4(),
        ));
        match CaseFailure::write_required_observation(&path, observation, &capture.secrets) {
            Ok(()) => eprintln!(
                "Gemini File Search response captured before validation: stage={}; File Search-only exclusion={exclusion}; stream/retrieval acceptance not established; no code execution or erasure claim; evidence={}",
                capture.stage.label(),
                path.display()
            ),
            Err(error) => self.export_failures.push(CaseFailure::from(format!(
                "required File Search response capture failed at {}: {:?}",
                capture.stage.label(),
                error.kind()
            ))),
        }
    }

    pub fn finish_observation(self) -> Result<GenerateResponse, CaseFailure> {
        match self.terminal {
            Some(Err(error)) => Err(error.into()),
            _ => self.finish_structure().map_err(CaseFailure::from),
        }
    }

    pub fn interrupted(&self, label: &'static str) -> CaseFailure {
        let failure = CaseFailure::from(label);
        match &self.terminal {
            Some(Err(error)) => failure.with_sdk(error.clone()),
            Some(Ok(_)) | None => failure,
        }
    }

    pub fn observe(&mut self, event: GenerateEvent) -> Result<(), String> {
        if self.terminal.is_some() {
            return Err("stream emitted an event after its terminal item".to_owned());
        }
        match event {
            GenerateEvent::Started { model } => {
                if self.started {
                    return Err("stream repeated Started".to_owned());
                }
                self.started = true;
                self.model = Some(model.as_str().to_owned());
            }
            event => {
                if !self.started {
                    return Err("stream emitted content before Started".to_owned());
                }
                match event {
                    GenerateEvent::ContentDelta { delta } => {
                        self.saw_text |= !delta.is_empty();
                        self.text.push_str(&delta);
                    }
                    GenerateEvent::ThinkingDelta { .. } | GenerateEvent::Usage(_) => {}
                    GenerateEvent::HostedTool(output) => self.hosted.push(*output),
                    GenerateEvent::Completed(response) => {
                        if Some(response.model.as_str()) != self.model.as_deref() {
                            return Err("stream terminal model differs from Started".to_owned());
                        }
                        if GenerationEvidence::text(&response)? != self.text {
                            return Err(
                                "stream text deltas differ from authoritative completed text"
                                    .to_owned(),
                            );
                        }
                        if !self.hosted.is_empty() && response.hosted_tool_outputs.is_empty() {
                            return Err(
                                "stream terminal response omitted observed hosted activity"
                                    .to_owned(),
                            );
                        }
                        for output in &self.hosted {
                            if !response.hosted_tool_outputs.iter().any(|terminal| {
                                std::mem::discriminant(terminal) == std::mem::discriminant(output)
                            }) {
                                return Err(
                                    "stream terminal response omitted an observed hosted family"
                                        .to_owned(),
                                );
                            }
                            if Self::hosted_failed(output)
                                && !response.hosted_tool_outputs.contains(output)
                            {
                                return Err(
                                    "stream terminal response omitted an observed hosted failure"
                                        .to_owned(),
                                );
                            }
                        }
                        self.terminal = Some(Ok(*response));
                    }
                    GenerateEvent::Started { .. } => {
                        return Err("unexpected repeated stream start".to_owned());
                    }
                }
            }
        }
        Ok(())
    }

    fn hosted_failed(output: &HostedToolOutput) -> bool {
        let status = match output {
            HostedToolOutput::WebSearch { status, .. }
            | HostedToolOutput::FileSearch { status, .. }
            | HostedToolOutput::UrlContext { status, .. } => status,
            HostedToolOutput::CodeExecution {
                status, results, ..
            } => {
                if results.iter().any(|result| {
                    matches!(
                        result.status,
                        Some(HostedToolStatus::Failed { .. } | HostedToolStatus::Incomplete)
                    ) || result.exit_code.is_some_and(|code| code != 0)
                }) {
                    return true;
                }
                status
            }
        };
        matches!(
            status,
            Some(HostedToolStatus::Failed { .. } | HostedToolStatus::Incomplete)
        )
    }

    pub fn finish_function_call(self) -> Result<GenerateResponse, CaseFailure> {
        if !self.started {
            return Err("function stream omitted Started".into());
        }
        match self.terminal {
            Some(Err(error)) => Err(error.into()),
            Some(Ok(response))
                if response.finish_reason == Some(FinishReason::ToolCall)
                    && !response.message.function_calls().is_empty()
                    && response.hosted_tool_outputs.is_empty() =>
            {
                Ok(response)
            }
            Some(Ok(_)) | None => {
                Err("function stream omitted its authoritative function-call completion".into())
            }
        }
    }

    pub fn finish(self) -> Result<GenerateResponse, CaseFailure> {
        let response = self.finish_observation()?;
        GenerationEvidence::completed(&response)?;
        Ok(response)
    }

    pub fn finish_structure(self) -> Result<GenerateResponse, String> {
        if !self.started || !self.saw_text {
            return Err("stream omitted Started or actual text deltas".to_owned());
        }
        match self.terminal {
            Some(Ok(response)) => Ok(response),
            Some(Err(_)) | None => Err("stream ended without Completed".to_owned()),
        }
    }
}

pub struct BatchEvidence;

impl BatchEvidence {
    pub fn completed(job: &BatchJob, expected_id: &str) -> Result<(), String> {
        if job.id != expected_id || expected_id.is_empty() {
            return Err("batch response returned the wrong job identity".to_owned());
        }
        if job.status != BatchStatus::Completed {
            return Err(format!(
                "batch did not complete successfully: {:?}",
                job.status
            ));
        }
        Ok(())
    }

    pub fn rows(rows: &[BatchResult], expected: &[(String, String)]) -> Result<(), String> {
        if rows.len() != expected.len() || expected.is_empty() {
            return Err("batch result count differs from nonempty expected inventory".to_owned());
        }
        let mut seen = BTreeSet::new();
        for row in rows {
            let BatchResult::Succeeded { custom_id, output } = row else {
                return Err("positive batch contained an unsuccessful row".to_owned());
            };
            if !seen.insert(custom_id) {
                return Err("batch duplicated a custom_id".to_owned());
            }
            let (_, answer) = expected
                .iter()
                .find(|(id, _)| id == custom_id)
                .ok_or_else(|| "batch returned an unexpected custom_id".to_owned())?;
            GenerationEvidence::exact_text(output, answer, false)?;
        }
        if expected.iter().any(|(id, _)| !seen.contains(id)) {
            return Err("batch omitted an expected custom_id".to_owned());
        }
        Ok(())
    }
}

pub struct ErrorEvidence;

impl ErrorEvidence {
    pub fn identity_token(message: &str, expected: &str) -> bool {
        if expected.is_empty() {
            return false;
        }
        message.match_indices(expected).any(|(start, _)| {
            let before = &message[..start];
            let after = &message[start + expected.len()..];
            let prefix = before
                .rsplit(char::is_whitespace)
                .next()
                .unwrap_or_default();
            if prefix.contains("://") {
                return false;
            }
            let left = match before.chars().next_back() {
                None => true,
                Some(character)
                    if character.is_whitespace()
                        || matches!(character, '`' | '\'' | '"' | '(' | '[' | '{') =>
                {
                    true
                }
                Some('=') => {
                    let key = prefix
                        .trim_start_matches(['\'', '"', '(', '[', '{'])
                        .strip_suffix('=')
                        .unwrap_or_default();
                    !key.is_empty()
                        && key
                            .chars()
                            .all(|character| character.is_ascii_alphanumeric() || character == '_')
                }
                Some(_) => false,
            };
            let boundary = |character: char| {
                character.is_whitespace()
                    || matches!(
                        character,
                        '`' | '\'' | '"' | ')' | ']' | '}' | ',' | ';' | '!'
                    )
            };
            let right = match after.chars().next() {
                None => true,
                Some(character) if boundary(character) => true,
                Some('.' | ':') => after[1..].chars().next().is_none_or(boundary),
                Some(_) => false,
            };
            left && right
        })
    }

    pub fn not_found(
        error: &SdkError,
        expected_operation: Operation,
        resource: &str,
        codes: &[Option<&str>],
    ) -> Result<(), String> {
        let details = error.native_details();
        let recognized = |value: &str| !value.is_empty() && codes.contains(&Some(value));
        let code_matches = details.code.as_deref().is_none_or(recognized);
        let type_matches = details.error_type.as_deref().is_none_or(|value| {
            recognized(value) || matches!(value, "error" | "invalid_request_error")
        });
        let status_matches = details.native_status.as_deref().is_none_or(recognized);
        let reported_http_matches = details
            .reported_http_status
            .is_none_or(|value| value == 404);
        let rpc_matches = details
            .rpc_code
            .is_none_or(|value| value == 5 && codes.contains(&Some("NOT_FOUND")));
        let native_signal = [
            details.code.as_deref(),
            details.error_type.as_deref(),
            details.native_status.as_deref(),
        ]
        .into_iter()
        .flatten()
        .any(recognized)
            || (details.reported_http_status == Some(404) && codes.contains(&Some("404")))
            || (details.rpc_code == Some(5) && codes.contains(&Some("NOT_FOUND")));
        let native_fields_consistent =
            code_matches && type_matches && status_matches && reported_http_matches && rpc_matches;
        let context = error.context();
        let operation_matches = matches!(
            expected_operation,
            Operation::Generate | Operation::GenerateStream
        ) && context.operation == expected_operation;
        let http_rejection = error.http_status() == Some(404)
            && context.phase == ErrorPhase::ResponseHeaders
            && operation_matches;
        let stream_rejection = error.http_status().is_none()
            && operation_matches
            && context.operation == Operation::GenerateStream
            && context.phase == ErrorPhase::Stream
            && native_signal;
        let classification_matches = error.kind() == ErrorKind::NotFound
            && native_fields_consistent
            && (native_signal || (http_rejection && codes.contains(&None)));
        let message = details.message.as_deref().unwrap_or_default();
        let message_matches = Self::identity_token(message, resource);
        let native_resources = details
            .details
            .iter()
            .filter_map(|detail| match detail {
                NativeDetail::Resource { name, .. } => Some(name.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        let identity_matches = !details.truncated
            && !resource.is_empty()
            && if native_resources.is_empty() {
                message_matches
            } else {
                native_resources.iter().all(|name| *name == resource)
            };
        if !classification_matches || !(http_rejection || stream_rejection) || !identity_matches {
            return Err(format!(
                "missing-resource evidence rejected: classification_matches={classification_matches}, operation_matches={operation_matches}, http_rejection={http_rejection}, stream_rejection={stream_rejection}, code_matches={code_matches}, type_matches={type_matches}, status_matches={status_matches}, reported_http_matches={reported_http_matches}, rpc_matches={rpc_matches}, native_signal={native_signal}, structured_identity_present={}, identity_matches={identity_matches}, message_identity_matches={message_matches}, native_details_truncated={}; error={error:?}",
                !native_resources.is_empty(),
                details.truncated,
            ));
        }
        Ok(())
    }
}
