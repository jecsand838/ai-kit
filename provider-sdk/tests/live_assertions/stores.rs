use provider_sdk::SdkError;
use provider_sdk::error::{
    ErrorKind, ErrorPhase, JsonErrorCategory, NativeDetail, NativeErrorDetails, Operation,
    ResponseDetails, ResponseViolation,
};
use provider_sdk::files::{FileObject, FileUploadRequest};
use provider_sdk::generation::GenerateResponse;
use provider_sdk::hosted::{HostedToolOutput, HostedToolStatus};
use serde_json::{Value, json};

use super::{ErrorEvidence, GenerationEvidence};
use crate::hosted_assertions::{FileSearchContract, HostedAnswer, HostedEvidence, HostedProvider};
use crate::live_failures::CaseFailure;

/// Exact ownership of one populated store and its SDK-uploaded source file.
pub struct StoreFixture {
    display_name: String,
    state: StoreState,
    input: StoreInput,
    source_observations: Vec<Value>,
    file_search_contract: FileSearchContract,
    file_search_observations: Vec<Value>,
}

enum StoreState {
    Ready,
    CreateAttempted,
    Owned {
        name: String,
        preflight: bool,
    },
    DeleteAttempted {
        name: String,
        preflight: bool,
    },
    Deleted {
        name: String,
        preflight: bool,
    },
    Absent {
        name: String,
        lookup: SdkError,
        checked_at_unix_ms: u128,
    },
}

enum StoreInput {
    Empty,
    UploadAttempted,
    Uploaded { id: String },
    ImportAttempted { id: String },
    Importing { id: String, operation: String },
    Imported { id: String, operation: String },
    ImportFailed { id: String, operation: String },
    DeleteAttempted { id: String },
    Deleted { id: String },
}

impl StoreFixture {
    pub fn new(display_name: String, file_search_contract: FileSearchContract) -> Self {
        Self {
            display_name,
            state: StoreState::Ready,
            input: StoreInput::Empty,
            source_observations: Vec::new(),
            file_search_contract,
            file_search_observations: Vec::new(),
        }
    }

    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    pub fn begin_create(&mut self) -> Result<(), SdkError> {
        if !matches!(self.state, StoreState::Ready) {
            return Err(self.state_error("store creation may be attempted only once"));
        }
        self.state = StoreState::CreateAttempted;
        Ok(())
    }

    pub fn accept_created(&mut self, body: &[u8]) -> Result<(), SdkError> {
        if !matches!(self.state, StoreState::CreateAttempted) {
            return Err(self.state_error("store creation acknowledgement is out of order"));
        }
        let value = self.decode(body)?;
        let name = value.get("name").and_then(Value::as_str).ok_or_else(|| {
            self.response_error(ResponseViolation::MissingField { field: "name" }, body)
        })?;
        let valid = name.strip_prefix("fileSearchStores/").is_some_and(|id| {
            !id.is_empty()
                && id.len() <= 40
                && id
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        });
        if !valid {
            return Err(
                self.response_error(ResponseViolation::InvalidField { field: "name" }, body)
            );
        }
        self.state = StoreState::Owned {
            name: name.to_owned(),
            preflight: false,
        };
        Ok(())
    }

    pub fn target(&self) -> Result<&str, SdkError> {
        match &self.state {
            StoreState::Owned { name, .. }
            | StoreState::DeleteAttempted { name, .. }
            | StoreState::Deleted { name, .. }
            | StoreState::Absent { name, .. } => Ok(name),
            StoreState::Ready | StoreState::CreateAttempted => {
                Err(self.state_error("store has no acknowledged owned identity"))
            }
        }
    }

    pub fn begin_upload(&mut self) -> Result<(), SdkError> {
        if !matches!(
            self.state,
            StoreState::Owned {
                preflight: false,
                ..
            }
        ) || !matches!(self.input, StoreInput::Empty)
        {
            return Err(self.input_error("source upload requires one fresh owned store"));
        }
        self.input = StoreInput::UploadAttempted;
        Ok(())
    }

    pub fn accept_uploaded(&mut self, id: String) -> Result<(), SdkError> {
        if !matches!(self.input, StoreInput::UploadAttempted) {
            return Err(self.input_error("source upload acknowledgement is out of order"));
        }
        if !id.strip_prefix("files/").is_some_and(|suffix| {
            !suffix.is_empty()
                && suffix.len() <= 40
                && suffix
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        }) {
            return Err(self.input_error("source upload returned an invalid lifecycle identity"));
        }
        self.input = StoreInput::Uploaded { id };
        Ok(())
    }

    /// Retain the actual SDK metadata before checking its fixture contract.
    pub fn observe_source_file(&mut self, request: &FileUploadRequest, file: &FileObject) {
        self.source_observations.push(json!({
            "stage": if self.source_observations.is_empty() {
                "upload_acknowledged"
            } else {
                "readiness_lookup"
            },
            "request": {
                "file_name": request.file_name(),
                "media_type": request.media_type(),
                "size_bytes": request.data().len(),
            },
            "file": file,
        }));
    }

    pub fn begin_import(&mut self) -> Result<String, SdkError> {
        if !matches!(
            self.state,
            StoreState::Owned {
                preflight: false,
                ..
            }
        ) {
            return Err(self.input_error("import requires an undeleted owned store"));
        }
        let StoreInput::Uploaded { id } = &self.input else {
            return Err(self.input_error("import requires an acknowledged source and runs once"));
        };
        let id = id.clone();
        self.input = StoreInput::ImportAttempted { id: id.clone() };
        Ok(id)
    }

    /// Validates only native readiness; SDK retrieval must still prove the secret.
    pub fn accept_import(&mut self, body: &[u8]) -> Result<bool, SdkError> {
        let (id, expected) = match &self.input {
            StoreInput::ImportAttempted { id } => (id.clone(), None),
            StoreInput::Importing { id, operation } => (id.clone(), Some(operation.clone())),
            _ => return Err(self.input_error("import acknowledgement is out of order")),
        };
        if !matches!(
            self.state,
            StoreState::Owned {
                preflight: false,
                ..
            }
        ) {
            return Err(self.input_error("import acknowledgement requires an undeleted store"));
        }
        let value = self.decode(body)?;
        let operation = value.get("name").and_then(Value::as_str).ok_or_else(|| {
            self.response_error(ResponseViolation::MissingField { field: "name" }, body)
        })?;
        let prefix = format!("{}/operations/", self.target()?);
        if !operation.strip_prefix(&prefix).is_some_and(|suffix| {
            !suffix.is_empty()
                && suffix
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        }) {
            return Err(
                self.response_error(ResponseViolation::InvalidField { field: "name" }, body)
            );
        }
        if expected
            .as_deref()
            .is_some_and(|expected| expected != operation)
        {
            return Err(
                self.response_error(ResponseViolation::IdentityMismatch { field: "name" }, body)
            );
        }
        // Retain an acknowledged operation before validating the remainder.
        self.input = StoreInput::Importing {
            id: id.clone(),
            operation: operation.into(),
        };
        let done = match value.get("done") {
            None | Some(Value::Null) => false,
            Some(Value::Bool(done)) => *done,
            Some(_) => {
                return Err(
                    self.response_error(ResponseViolation::InvalidField { field: "done" }, body)
                );
            }
        };
        let error = value.get("error").filter(|value| !value.is_null());
        let response = value.get("response").filter(|value| !value.is_null());
        if error.is_some_and(|error| !error.is_object())
            || response.is_some_and(|response| !response.is_object())
            || (error.is_some() && response.is_some())
            || (!done && error.is_some())
        {
            return Err(
                self.response_error(ResponseViolation::InvalidField { field: "result" }, body)
            );
        }
        if let Some(error) = error {
            self.input = StoreInput::ImportFailed {
                id,
                operation: operation.into(),
            };
            return Err(SdkError::new(
                ErrorKind::ProviderRejected,
                Operation::Unspecified,
                ErrorPhase::Normalize,
            )
            .with_correlation_id(self.display_name())
            .with_request_target(operation)
            .with_native_details(NativeErrorDetails {
                rpc_code: error.get("code").and_then(Value::as_i64),
                message: error
                    .get("message")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                body: Some(String::from_utf8_lossy(body).into_owned()),
                ..Default::default()
            }));
        }
        if done {
            // The generic Operation schema permits a terminal omitted result.
            // Do not invent or require a document identity; retrieval proves it.
            self.input = StoreInput::Imported {
                id,
                operation: operation.into(),
            };
        }
        // Import acknowledgements can carry a provisional response object.
        // Only the done flag grants readiness; pending payloads cannot do so.
        Ok(done)
    }

    pub fn import_operation(&self) -> Result<&str, SdkError> {
        match &self.input {
            StoreInput::Importing { operation, .. } => Ok(operation),
            _ => Err(self.input_error("there is no pending acknowledged import to poll")),
        }
    }

    pub fn accept_preflight(
        &mut self,
        response: &GenerateResponse,
        secret: &str,
    ) -> Result<(), CaseFailure> {
        if !matches!(
            self.state,
            StoreState::Owned {
                preflight: false,
                ..
            }
        ) || !matches!(self.input, StoreInput::Imported { .. })
        {
            return Err(self
                .state_error("preflight requires an undeleted store and completed import")
                .into());
        }
        let mut observation = self.file_search_contract.observation(response);
        observation["stage"] = json!("preflight");
        self.file_search_observations.push(observation);
        let mut failures = Vec::new();
        if let Err(error) = HostedEvidence::assess(
            response,
            Some(HostedAnswer::Marker(secret)),
            HostedEvidence::file_search(
                response,
                HostedProvider::Gemini,
                self.file_search_contract,
            ),
        ) {
            failures.push(error.into());
        }
        for output in &response.hosted_tool_outputs {
            let status = match output {
                HostedToolOutput::WebSearch { status, .. }
                | HostedToolOutput::FileSearch { status, .. }
                | HostedToolOutput::UrlContext { status, .. } => status,
                HostedToolOutput::CodeExecution {
                    status, results, ..
                } => {
                    for result in results {
                        if let Some(HostedToolStatus::Failed { error }) = &result.status {
                            failures.push(CaseFailure::Remote(error.clone()));
                        }
                    }
                    status
                }
            };
            match status {
                Some(HostedToolStatus::Failed { error }) => {
                    failures.push(CaseFailure::Remote(error.clone()))
                }
                Some(HostedToolStatus::Incomplete) => {
                    failures.push("preflight returned incomplete hosted activity".into())
                }
                Some(HostedToolStatus::InProgress | HostedToolStatus::Completed) | None => {}
            }
        }
        CaseFailure::collected("owned-store SDK retrieval preflight", failures)?;
        let StoreState::Owned { preflight, .. } = &mut self.state else {
            return Err(self
                .state_error("store preflight lost its owned store")
                .into());
        };
        *preflight = true;
        Ok(())
    }

    pub fn begin_input_delete(&mut self) -> Option<String> {
        let id = match &self.input {
            StoreInput::Uploaded { id }
            | StoreInput::ImportAttempted { id }
            | StoreInput::Importing { id, .. }
            | StoreInput::Imported { id, .. }
            | StoreInput::ImportFailed { id, .. } => id.clone(),
            StoreInput::Empty
            | StoreInput::UploadAttempted
            | StoreInput::DeleteAttempted { .. }
            | StoreInput::Deleted { .. } => return None,
        };
        self.input = StoreInput::DeleteAttempted { id: id.clone() };
        Some(id)
    }

    pub fn accept_input_deleted(&mut self) -> Result<(), SdkError> {
        let StoreInput::DeleteAttempted { id } = &self.input else {
            return Err(self.input_error("source deletion acknowledgement is out of order"));
        };
        self.input = StoreInput::Deleted { id: id.clone() };
        Ok(())
    }

    pub fn finish_input(&self) -> Result<(), SdkError> {
        match self.input {
            StoreInput::Empty | StoreInput::Deleted { .. } => Ok(()),
            StoreInput::UploadAttempted => {
                Err(self.input_error("source upload remains uncertain; identity is unknown"))
            }
            StoreInput::DeleteAttempted { .. } => {
                Err(self
                    .input_error("source deletion remains uncertain; mutation will not be retried"))
            }
            _ => Err(self.input_error("owned source file was not deleted")),
        }
    }

    pub fn observation(&self) -> Value {
        let (store_state, preflight) = match &self.state {
            StoreState::Ready => ("not_created", false),
            StoreState::CreateAttempted => ("create_uncertain", false),
            StoreState::Owned { preflight, .. } => ("owned", *preflight),
            StoreState::DeleteAttempted { preflight, .. } => ("delete_uncertain", *preflight),
            StoreState::Deleted { preflight, .. } => ("deleted", *preflight),
            StoreState::Absent { .. } => ("lookup_absent", true),
        };
        let (source_state, source, operation) = match &self.input {
            StoreInput::Empty => ("not_uploaded", None, None),
            StoreInput::UploadAttempted => ("upload_uncertain", None, None),
            StoreInput::Uploaded { id } => ("uploaded", Some(id), None),
            StoreInput::ImportAttempted { id } => ("import_uncertain", Some(id), None),
            StoreInput::Importing { id, operation } => {
                ("import_pending", Some(id), Some(operation))
            }
            StoreInput::Imported { id, operation } => {
                ("import_completed", Some(id), Some(operation))
            }
            StoreInput::ImportFailed { id, operation } => {
                ("import_failed", Some(id), Some(operation))
            }
            StoreInput::DeleteAttempted { id } => ("delete_uncertain", Some(id), None),
            StoreInput::Deleted { id } => ("deleted", Some(id), None),
        };
        let absence = match &self.state {
            StoreState::Absent {
                lookup,
                checked_at_unix_ms,
                ..
            } => Some(json!({
                "checked_at_unix_ms": checked_at_unix_ms, "lookup": CaseFailure::Sdk(lookup.clone()).evidence(),
                "retrieval_revocation_proven": false,
            })),
            _ => None,
        };
        json!({
            "store": self.target().ok(), "store_state": store_state,
            "retrieval_preflight": preflight, "source": source,
            "source_state": source_state, "import_operation": operation,
            "source_file_observations": self.source_observations,
            "file_search_observations": self.file_search_observations,
            "management_absence": absence,
        })
    }

    fn input_error(&self, rule: &'static str) -> SdkError {
        let mut error = SdkError::new(
            ErrorKind::InvalidState,
            Operation::Unspecified,
            ErrorPhase::Cleanup,
        )
        .with_correlation_id(self.display_name())
        .with_native_details(NativeErrorDetails {
            message: Some(rule.into()),
            ..Default::default()
        });
        match &self.input {
            StoreInput::Uploaded { id }
            | StoreInput::ImportAttempted { id }
            | StoreInput::Importing { id, .. }
            | StoreInput::Imported { id, .. }
            | StoreInput::ImportFailed { id, .. }
            | StoreInput::DeleteAttempted { id }
            | StoreInput::Deleted { id } => error = error.with_request_target(id),
            StoreInput::Empty | StoreInput::UploadAttempted => {}
        }
        error
    }

    /// Marks the exact mutation before the caller awaits; cleanup uses this same gate.
    pub fn begin_delete(&mut self) -> Result<Option<String>, SdkError> {
        let StoreState::Owned { name, preflight } = &self.state else {
            return Ok(None);
        };
        let name = name.clone();
        self.state = StoreState::DeleteAttempted {
            name: name.clone(),
            preflight: *preflight,
        };
        Ok(Some(name))
    }

    pub fn accept_deleted(&mut self, body: &[u8]) -> Result<(), SdkError> {
        let StoreState::DeleteAttempted { name, preflight } = &self.state else {
            return Err(self.state_error("store deletion acknowledgement is out of order"));
        };
        if !body.iter().all(u8::is_ascii_whitespace) {
            let value = self.decode(body)?;
            if !value.as_object().is_some_and(serde_json::Map::is_empty) {
                return Err(self.response_error(
                    ResponseViolation::ProtocolViolation {
                        rule: "store deletion must acknowledge an empty response",
                    },
                    body,
                ));
            }
        }
        self.state = StoreState::Deleted {
            name: name.clone(),
            preflight: *preflight,
        };
        Ok(())
    }

    pub fn negative_target(&self) -> Result<&str, SdkError> {
        match &self.state {
            StoreState::Deleted {
                name,
                preflight: true,
            } => Ok(name),
            StoreState::Absent { name, .. } => Ok(name),
            _ => Err(self.state_error(
                "negative store proof requires successful preflight and acknowledged deletion",
            )),
        }
    }

    /// A deletion acknowledgement is not an absence check or retrieval revocation.
    pub fn accept_absence(&mut self, lookup: SdkError) -> Result<(), CaseFailure> {
        let StoreState::Deleted {
            name,
            preflight: true,
        } = &self.state
        else {
            return Err(self
                .state_error("absence lookup requires proven retrieval and acknowledged deletion")
                .into());
        };
        let details = lookup.native_details();
        let body = details.body.as_deref().ok_or_else(|| {
            CaseFailure::from("store lookup omitted its native response body")
                .with_sdk(lookup.clone())
        })?;
        let native = self
            .decode(body.as_bytes())
            .map_err(|error| CaseFailure::from(error).with_sdk(lookup.clone()))?;
        let valid = lookup.kind() == ErrorKind::ProviderRejected
            && lookup.context().operation == Operation::Unspecified
            && lookup.context().phase == ErrorPhase::ResponseHeaders
            && lookup.request_target() == Some(name.as_str())
            && lookup.http_status() == Some(404)
            && !details.truncated
            && details.omitted_details == 0
            && lookup.cause().is_none()
            && lookup.supplementary().is_empty()
            && native.pointer("/error/code").and_then(Value::as_u64) == Some(404)
            && native.pointer("/error/status").and_then(Value::as_str) == Some("NOT_FOUND")
            && native
                .pointer("/error/message")
                .and_then(Value::as_str)
                .is_some_and(|message| ErrorEvidence::identity_token(message, name));
        if !valid {
            return Err(CaseFailure::from(
                "exact owned-store lookup did not prove native 404/NOT_FOUND absence",
            )
            .with_sdk(lookup));
        }
        let checked_at_unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| CaseFailure::from("store lookup timestamp precedes the Unix epoch"))?
            .as_millis();
        self.state = StoreState::Absent {
            name: name.clone(),
            lookup,
            checked_at_unix_ms,
        };
        Ok(())
    }

    /// Reports a provider observation; an accepted answer never proves rejection.
    pub fn consistency_probe(
        &mut self,
        result: Result<GenerateResponse, CaseFailure>,
        operation: Operation,
        secret: &str,
    ) -> Result<Value, CaseFailure> {
        if !matches!(self.state, StoreState::Absent { .. })
            || secret.is_empty()
            || !matches!(operation, Operation::Generate | Operation::GenerateStream)
        {
            return Err(
                "store consistency probe requires verified absence and a nonempty secret".into(),
            );
        }
        match result {
            Ok(response) => {
                let mut observation = self.file_search_contract.observation(&response);
                observation["stage"] = json!("post_delete");
                self.file_search_observations.push(observation.clone());
                GenerationEvidence::completed(&response)?;
                let grounded = response
                    .hosted_tool_outputs
                    .iter()
                    .any(|output| matches!(output, HostedToolOutput::FileSearch { .. }));
                if !response.hosted_tool_outputs.is_empty() {
                    HostedEvidence::file_search(
                        &response,
                        HostedProvider::Gemini,
                        self.file_search_contract,
                    )?;
                }
                let answer_contract = HostedAnswer::Marker(secret).verify(&response);
                let marker_returned = answer_contract.is_ok();
                // Post-deletion observation separates an observed secret from
                // the requested answer format. Preflight still requires both.
                let marker_observed = GenerationEvidence::text(&response)?
                    .split(|character: char| {
                        !character.is_alphanumeric() && !matches!(character, '_' | '-')
                    })
                    .filter(|token| *token == secret)
                    .count()
                    == 1;
                if observation["unpaired_file_search_declaration_observed"] == true
                    && !marker_observed
                {
                    return Err(
                        "unpaired File Search declaration requires exactly one complete observed marker"
                            .into(),
                    );
                }
                observation["outcome"] = json!(if grounded && marker_returned {
                    "retrieval_persists"
                } else {
                    "completed_without_rejection"
                });
                observation["native_rejection_observed"] = json!(false);
                observation["retrieval_revocation_proven"] = json!(false);
                observation["marker_returned"] = json!(marker_returned);
                observation["marker_observed"] = json!(marker_observed);
                observation["answer_contract_error"] = json!(answer_contract.err());
                observation["retrieval_evidence"] = json!(grounded);
                Ok(observation)
            }
            Err(CaseFailure::Sdk(error)) => {
                self.rejection(&error, operation)
                    .map_err(|failure| CaseFailure::from(failure).with_sdk(error.clone()))?;
                Ok(json!({
                    "outcome": "native_rejection_observed",
                    "native_rejection_observed": true,
                    "retrieval_revocation_proven": false,
                    "error": CaseFailure::Sdk(error).evidence(),
                }))
            }
            Err(failure) => Err(failure),
        }
    }

    pub fn rejection(&self, error: &SdkError, operation: Operation) -> Result<(), String> {
        if !matches!(self.state, StoreState::Absent { .. }) {
            return Err("deleted-store probe requires verified native lookup absence".into());
        }
        if error.cause().is_some()
            || !error.supplementary().is_empty()
            || error.native_details().omitted_details != 0
        {
            return Err("deleted-store rejection contains additional or omitted failures".into());
        }
        let target = self
            .negative_target()
            .map_err(|error| format!("deleted-store fixture history is incomplete: {error:?}"))?;
        if error.kind() == ErrorKind::NotFound {
            return ErrorEvidence::not_found(
                error,
                operation,
                target,
                &[Some("NOT_FOUND"), Some("404")],
            );
        }
        let details = error.native_details();
        let code_matches = details
            .code
            .as_deref()
            .is_none_or(|code| code == "INVALID_ARGUMENT");
        let type_matches = details
            .error_type
            .as_deref()
            .is_none_or(|kind| kind == "error" || kind == "INVALID_ARGUMENT");
        let canonical_matches = details
            .native_status
            .as_deref()
            .is_none_or(|status| status == "INVALID_ARGUMENT");
        let rpc_matches = details.rpc_code.is_none_or(|code| code == 3);
        let reported_matches = details
            .reported_http_status
            .is_none_or(|status| status == 400);
        let native_signal = details.native_status.as_deref() == Some("INVALID_ARGUMENT")
            || details.rpc_code == Some(3)
            || (details.reported_http_status == Some(400)
                && details.code.as_deref() == Some("INVALID_ARGUMENT"));
        let operation_matches =
            matches!(operation, Operation::Generate | Operation::GenerateStream)
                && error.context().operation == operation;
        let opening = error.context().phase == ErrorPhase::ResponseHeaders
            && error.http_status() == Some(400);
        let in_stream = operation == Operation::GenerateStream
            && error.context().phase == ErrorPhase::Stream
            && error.http_status().is_none();
        let resources = details
            .details
            .iter()
            .filter_map(|detail| match detail {
                NativeDetail::Resource { name, .. } => Some(name.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        let identity = if resources.is_empty() {
            details
                .message
                .as_deref()
                .is_some_and(|message| ErrorEvidence::identity_token(message, target))
        } else {
            resources.iter().all(|name| *name == target)
        };
        let native_consistent =
            code_matches && type_matches && canonical_matches && rpc_matches && reported_matches;
        if error.kind() != ErrorKind::InvalidRequest
            || !operation_matches
            || !(opening || in_stream)
            || !native_signal
            || !native_consistent
            || !identity
            || details.truncated
            || details.omitted_details != 0
        {
            return Err(format!(
                "deleted-store rejection unproven: invalid_request={}, operation_matches={operation_matches}, opening={opening}, in_stream={in_stream}, native_signal={native_signal}, native_consistent={native_consistent}, native_identity={identity}, native_truncated={}, omitted_details={}",
                error.kind() == ErrorKind::InvalidRequest,
                details.truncated,
                details.omitted_details
            ));
        }
        Ok(())
    }

    pub fn finish(&self) -> Result<(), SdkError> {
        match self.state {
            StoreState::Ready | StoreState::Deleted { .. } | StoreState::Absent { .. } => Ok(()),
            StoreState::CreateAttempted => {
                Err(self.state_error("store creation remains uncertain; owned identity is unknown"))
            }
            StoreState::Owned { .. } => Err(self.state_error("owned store was not deleted")),
            StoreState::DeleteAttempted { .. } => {
                Err(self
                    .state_error("store deletion remains uncertain; mutation will not be retried"))
            }
        }
    }

    fn state_error(&self, rule: &'static str) -> SdkError {
        let mut error = SdkError::new(
            ErrorKind::InvalidState,
            Operation::Unspecified,
            ErrorPhase::Cleanup,
        )
        .with_correlation_id(self.display_name.clone())
        .with_native_details(NativeErrorDetails {
            message: Some(rule.to_owned()),
            ..Default::default()
        });
        match &self.state {
            StoreState::Owned { name, .. }
            | StoreState::DeleteAttempted { name, .. }
            | StoreState::Deleted { name, .. }
            | StoreState::Absent { name, .. } => error = error.with_request_target(name.clone()),
            StoreState::Ready | StoreState::CreateAttempted => {}
        }
        error
    }

    fn response_error(&self, reason: ResponseViolation, body: &[u8]) -> SdkError {
        SdkError::new(
            ErrorKind::InvalidResponse(reason),
            Operation::Unspecified,
            ErrorPhase::Decode,
        )
        .with_correlation_id(self.display_name.clone())
        .with_native_details(NativeErrorDetails {
            body: Some(String::from_utf8_lossy(body).into_owned()),
            ..Default::default()
        })
    }

    fn decode(&self, body: &[u8]) -> Result<Value, SdkError> {
        if body.len() > 65_536 {
            return Err(self
                .response_error(
                    ResponseViolation::LimitExceeded { limit: 65_536 },
                    &body[..65_536],
                )
                .with_response_details(ResponseDetails::Size {
                    observed: body.len(),
                }));
        }
        serde_json::from_slice(body).map_err(|error| {
            self.response_error(ResponseViolation::MalformedJson, body)
                .with_response_details(ResponseDetails::Json {
                    line: error.line(),
                    column: error.column(),
                    category: match error.classify() {
                        serde_json::error::Category::Io => JsonErrorCategory::Io,
                        serde_json::error::Category::Syntax => JsonErrorCategory::Syntax,
                        serde_json::error::Category::Data => JsonErrorCategory::Data,
                        serde_json::error::Category::Eof => JsonErrorCategory::Eof,
                    },
                })
        })
    }
}
