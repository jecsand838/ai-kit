use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;

use providers_kit::KitError;
use providers_kit::batch::{BatchInput, BatchJob, BatchResult, BatchStatus};
use providers_kit::error::{
    ClassificationBasis, ErrorKind, ErrorPhase, NativeDetail, NativeErrorDetails, Operation,
    RemoteFailure,
};
use providers_kit::files::{FileObject, FilePage, FileState, FileUploadRequest};
use providers_kit::generation::{
    ContentPart, FileReference, GenerateRequest, GenerateResponse, GenerationConfig, Message, Role,
};
use serde_json::{Value, json};

use super::{BatchEvidence, ErrorEvidence, GenerationEvidence};
use crate::hosted_assertions::{HostedAnswer, HostedProvider};
use crate::live_failures::CaseFailure;

impl BatchEvidence {
    /// Checks an owned fixture lookup before a model may consume the file.
    pub fn fixture_file_ready(
        observed: &FileObject,
        uploaded: &FileObject,
        request: &FileUploadRequest,
        provider: HostedProvider,
    ) -> Result<bool, CaseFailure> {
        if uploaded.file_name.as_deref() != Some(request.file_name()) {
            return Err("fixture upload returned a different file name".into());
        }
        if uploaded.id.trim().is_empty() {
            return Err("fixture upload returned an empty resource id".into());
        }
        if observed.id != uploaded.id {
            return Err("fixture file readiness changed the uploaded resource identity".into());
        }
        if let Some(FileState::Failed { code, message }) = &observed.state {
            return Err(CaseFailure::Remote(RemoteFailure::new(
                ErrorKind::ProviderRejected,
                NativeErrorDetails {
                    code: code.clone(),
                    message: message.clone(),
                    ..Default::default()
                },
            )?)
            .context("owned fixture file failed processing"));
        }
        if matches!(provider, HostedProvider::Gemini) {
            let bytes = u64::try_from(request.data().len())
                .map_err(|_| "fixture upload size does not fit the public file size")?;
            for (stage, file) in [("upload", uploaded), ("readiness", observed)] {
                if file.file_name.as_deref() != Some(request.file_name())
                    || file.media_type.as_deref() != Some(request.media_type())
                    || file.size_bytes != Some(bytes)
                {
                    return Err(format!(
                        "Gemini fixture {stage} metadata does not match the requested name, MIME type, and byte length"
                    )
                    .into());
                }
                if !matches!(
                    &file.input_reference,
                    Some(FileReference::Url { url, media_type })
                        if url.scheme() == "https"
                            && url.has_host()
                            && url.username().is_empty()
                            && url.password().is_none()
                            && media_type.as_deref() == Some(request.media_type())
                ) {
                    return Err(format!(
                        "Gemini fixture {stage} lacks a usable HTTPS input reference with matching MIME type"
                    )
                    .into());
                }
            }
            if observed.input_reference != uploaded.input_reference {
                return Err("Gemini fixture readiness changed the returned input reference".into());
            }
            return Ok(matches!(observed.state, Some(FileState::Active)));
        }
        Ok(!matches!(observed.state, Some(FileState::Processing)))
    }
}

/// Test-only retention policy for an exact output of an owned, completed Gemini batch.
pub struct GeminiOutputRetention {
    batch_id: String,
    file_id: String,
    completed_at: String,
    deletion_error: Option<KitError>,
}

impl GeminiOutputRetention {
    pub fn from_metadata(owned_job: &str, metadata: &Value) -> Option<Self> {
        if !owned_job.starts_with("batches/")
            || owned_job.len() <= 8
            || metadata.get("name")?.as_str()? != owned_job
            || !matches!(
                metadata.pointer("/metadata/state")?.as_str()?,
                "JOB_STATE_SUCCEEDED" | "BATCH_STATE_SUCCEEDED"
            )
            || metadata.get("error").is_some_and(|value| !value.is_null())
        {
            return None;
        }
        let file_id = metadata.pointer("/response/responsesFile")?.as_str()?;
        let suffix = file_id.strip_prefix("files/")?;
        if suffix.len() <= 40
            || !suffix
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            || suffix.starts_with('-')
            || suffix.ends_with('-')
        {
            return None;
        }
        let completed_at = metadata.pointer("/metadata/endTime")?.as_str()?;
        if completed_at.trim().is_empty() {
            return None;
        }
        Some(Self {
            batch_id: owned_job.to_owned(),
            file_id: file_id.to_owned(),
            completed_at: completed_at.to_owned(),
            deletion_error: None,
        })
    }

    pub fn defer(&mut self, error: KitError) -> Result<(), KitError> {
        let native = error.native_details();
        if self.is_deferred()
            || error.kind() != ErrorKind::InvalidRequest
            || error.context().operation != Operation::DeleteFile
            || error.context().phase != ErrorPhase::ResponseHeaders
            || error.http_status() != Some(400)
            || error.request_target() != Some(self.file_id.as_str())
            || native.native_status.as_deref() != Some("INVALID_ARGUMENT")
            || native.reported_http_status != Some(400)
            || native.message.as_deref()
                != Some(
                    "* DeleteFileRequest.name: File ID (name excluding 'files/') cannot be more than 40 characters.\n",
                )
            || native.truncated
            || native.omitted_details != 0
            || !native.details.is_empty()
            || native.rpc_code.is_some()
            || native.code.is_some()
            || native.error_type.is_some()
            || native.outcome.is_some()
            || native.parameter.is_some()
            || native.execution_exit_code.is_some()
            || error.cause().is_some()
            || !error.supplementary().is_empty()
        {
            return Err(error);
        }
        self.deletion_error = Some(error);
        Ok(())
    }

    pub fn is_deferred(&self) -> bool {
        self.deletion_error.is_some()
    }

    pub fn evidence(&self) -> Value {
        json!({
            "id": self.file_id,
            "batch_id": self.batch_id,
            "completed_at": self.completed_at,
            "cleanup_state": if self.is_deferred() { "awaiting_provider_expiration" } else { "deletion_pending" },
            "deleted": false,
            "expiration_verified": false,
            "provider_retention_days": 42,
            "retention_source": "https://ai.google.dev/gemini-api/docs/batch-api#retrieving-results",
            "deletion_error": self.deletion_error.as_ref().map(|error| CaseFailure::from(error.clone()).evidence()),
        })
    }
}

enum InventoryState {
    Scanning { found: bool },
    Complete { found: bool },
    Invalid,
}

/// Proof of a complete public file-list traversal for one exact owned resource.
pub struct FileInventoryEvidence {
    id: String,
    cursors: BTreeSet<String>,
    state: InventoryState,
}

impl FileInventoryEvidence {
    pub fn new(id: &str) -> Self {
        Self {
            id: id.to_owned(),
            cursors: BTreeSet::new(),
            state: InventoryState::Scanning { found: false },
        }
    }

    pub fn observe(&mut self, page: &FilePage) -> Result<Option<String>, String> {
        let InventoryState::Scanning { found } = self.state else {
            self.state = InventoryState::Invalid;
            return Err("file inventory received a page after completion or failure".into());
        };
        let found = found || page.files.iter().any(|file| file.id == self.id);
        match &page.next_cursor {
            Some(cursor) => {
                if cursor.trim().is_empty() || !self.cursors.insert(cursor.clone()) {
                    self.state = InventoryState::Invalid;
                    return Err("file inventory returned an empty or repeated cursor".into());
                }
                self.state = InventoryState::Scanning { found };
                Ok(Some(cursor.clone()))
            }
            None => {
                self.state = InventoryState::Complete { found };
                Ok(None)
            }
        }
    }

    pub fn observe_result(
        &mut self,
        result: Result<FilePage, CaseFailure>,
    ) -> Result<Option<String>, CaseFailure> {
        match result {
            Ok(page) => self.observe(&page).map_err(CaseFailure::from),
            Err(error) => {
                self.state = InventoryState::Invalid;
                Err(error)
            }
        }
    }

    pub fn contains(&self, expected_id: &str) -> Result<bool, String> {
        if expected_id != self.id || self.id.is_empty() {
            return Err("file inventory proof belongs to a different resource".into());
        }
        match self.state {
            InventoryState::Complete { found } => Ok(found),
            InventoryState::Scanning { .. } | InventoryState::Invalid => {
                Err("file inventory did not complete a valid traversal".into())
            }
        }
    }
}

enum DeletedFileState {
    Uploaded,
    UnaryReadable,
    BatchReadable,
    Visible,
    Deleted,
    Absent,
    ControlVerified,
}

/// Case-owned identity plus the prerequisite evidence for deleted-file input rejection.
pub struct DeletedFileEvidence {
    id: String,
    reference: FileReference,
    state: DeletedFileState,
}

impl DeletedFileEvidence {
    pub fn new(id: String, reference: FileReference) -> Self {
        Self {
            id,
            reference,
            state: DeletedFileState::Uploaded,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn read_request(&self) -> Result<GenerateRequest, KitError> {
        GenerateRequest::new(vec![Message::user(vec![
            ContentPart::Text { text: "What is the confirmation code in the attached delivery confirmation record? Return one line in the form Answer: <confirmation code>. Do not decorate that field; put any explanation on other lines.".into() },
            ContentPart::File { reference: self.reference.clone() },
        ])?])?.with_generation(GenerationConfig::default().with_max_output_tokens(NonZeroU32::new(4096).unwrap()))
    }

    pub fn preflight(
        &mut self,
        response: &GenerateResponse,
        expected_marker: &str,
    ) -> Result<(), String> {
        if !matches!(self.state, DeletedFileState::Uploaded) {
            return Err("file preflight was attempted out of order".into());
        }
        Self::readable(response, expected_marker)?;
        self.state = DeletedFileState::UnaryReadable;
        Ok(())
    }

    pub fn batch_preflight(
        &mut self,
        job: &BatchJob,
        job_id: &str,
        rows: &[BatchResult],
        row_id: &str,
        expected_marker: &str,
    ) -> Result<(), String> {
        if !matches!(self.state, DeletedFileState::UnaryReadable) {
            return Err("batch file preflight requires successful unary readability first".into());
        }
        BatchEvidence::completed(job, job_id)?;
        let [BatchResult::Succeeded { custom_id, output }] = rows else {
            return Err("batch file preflight requires exactly one successful row".into());
        };
        if row_id.is_empty() || custom_id != row_id {
            return Err("batch file preflight changed its exact row identity".into());
        }
        Self::readable(output, expected_marker)?;
        self.state = DeletedFileState::BatchReadable;
        Ok(())
    }

    fn readable(response: &GenerateResponse, marker: &str) -> Result<(), String> {
        GenerationEvidence::completed(response)?;
        if !response.hosted_tool_outputs.is_empty() {
            return Err("file readability unexpectedly used a hosted tool".into());
        }
        HostedAnswer::Marker(marker).verify(response)
    }

    pub fn visible(&mut self, inventory: &FileInventoryEvidence) -> Result<(), String> {
        if !matches!(self.state, DeletedFileState::BatchReadable) {
            return Err("file visibility requires unary and batch same-file readability".into());
        }
        if !inventory.contains(&self.id)? {
            return Err("owned file was absent from its pre-deletion inventory".into());
        }
        self.state = DeletedFileState::Visible;
        Ok(())
    }

    /// Called only after the Kit has acknowledged deletion of this exact resource.
    pub fn deletion_acknowledged(&mut self) -> Result<(), String> {
        if !matches!(self.state, DeletedFileState::Visible) {
            return Err("file deletion requires successful preflight and visibility".into());
        }
        self.state = DeletedFileState::Deleted;
        Ok(())
    }

    pub fn absent(&mut self, inventory: &FileInventoryEvidence) -> Result<bool, String> {
        if !matches!(self.state, DeletedFileState::Deleted) {
            return Err("file absence requires acknowledged Kit deletion".into());
        }
        if inventory.contains(&self.id)? {
            return Ok(false);
        }
        self.state = DeletedFileState::Absent;
        Ok(true)
    }

    pub fn mixed_rows<'a>(
        &mut self,
        rows: &'a [BatchResult],
        success: (&str, &str),
        failure_id: &str,
    ) -> Result<&'a RemoteFailure, String> {
        if !matches!(self.state, DeletedFileState::Absent) {
            return Err("mixed rows require proven owned-file deletion and absence".into());
        }
        if rows.len() != 2
            || success.0.is_empty()
            || failure_id.is_empty()
            || success.0 == failure_id
        {
            return Err("mixed batch requires exactly two distinct correlated rows".into());
        }
        let mut good = None;
        let mut bad = None;
        for row in rows {
            match row {
                BatchResult::Succeeded { custom_id, output }
                    if custom_id == success.0 && good.is_none() =>
                {
                    good = Some(output.as_ref());
                }
                BatchResult::Failed { custom_id, error }
                    if custom_id == failure_id && bad.is_none() =>
                {
                    bad = Some(error);
                }
                _ => {
                    return Err(
                        "mixed batch returned duplicate, unexpected, or unsuccessful control rows"
                            .into(),
                    );
                }
            }
        }
        let good = good.ok_or("mixed batch omitted its successful text control")?;
        let bad = bad.ok_or("mixed batch omitted its correlated failed file row")?;
        GenerationEvidence::exact_text(good, success.1, false)?;
        self.state = DeletedFileState::ControlVerified;
        Ok(bad)
    }

    pub fn openai(&self, error: &RemoteFailure) -> Result<(), String> {
        let details = error.native_details();
        self.check(
            matches!(
                error.kind(),
                ErrorKind::NotFound | ErrorKind::InvalidRequest | ErrorKind::ProviderRejected
            ) && (matches!(
                details.code.as_deref(),
                Some("file_not_found" | "invalid_file" | "invalid_file_id")
            ) || matches!(
                details.error_type.as_deref(),
                Some("invalid_request_error" | "not_found_error")
            )),
            error,
        )
    }

    pub fn anthropic(&self, error: &RemoteFailure) -> Result<(), String> {
        let details = error.native_details();
        self.check(
            matches!(
                (details.error_type.as_deref(), error.kind()),
                (Some("not_found_error"), ErrorKind::NotFound)
                    | (Some("invalid_request_error"), ErrorKind::InvalidRequest)
            ),
            error,
        )
    }

    pub fn gemini(&self, error: &RemoteFailure) -> Result<(), String> {
        let details = error.native_details();
        // Native batch errors need not echo the input URI. This observed bare
        // RPC result qualifies only after same-file readability, deletion,
        // complete absence, and the exact mixed-row control have been proved.
        if matches!(self.state, DeletedFileState::ControlVerified)
            && error.kind() == ErrorKind::PermissionDenied
            && error.classification_basis() == ClassificationBasis::RpcCode
            && details.rpc_code == Some(7)
            && details.message.as_deref() == Some("The caller does not have permission")
            && !details.truncated
            && details.omitted_details == 0
            && details.details.is_empty()
            && details.code.is_none()
            && details.error_type.is_none()
            && details.native_status.is_none()
            && details.reported_http_status.is_none()
            && details.parameter.is_none()
            && details.outcome.is_none()
            && details.execution_exit_code.is_none()
        {
            return Ok(());
        }
        let expected = match (details.rpc_code, details.native_status.as_deref()) {
            (Some(3), None | Some("INVALID_ARGUMENT")) | (None, Some("INVALID_ARGUMENT")) => {
                Some(ErrorKind::InvalidRequest)
            }
            (Some(5), None | Some("NOT_FOUND")) | (None, Some("NOT_FOUND")) => {
                Some(ErrorKind::NotFound)
            }
            (Some(7), None | Some("PERMISSION_DENIED")) | (None, Some("PERMISSION_DENIED")) => {
                Some(ErrorKind::PermissionDenied)
            }
            _ => None,
        };
        self.check(expected == Some(error.kind()), error)
    }

    fn check(&self, relevant_code: bool, error: &RemoteFailure) -> Result<(), String> {
        if !matches!(self.state, DeletedFileState::ControlVerified) {
            return Err(
                "deleted-file rejection lacks the complete prerequisite and control proof".into(),
            );
        }
        let details = error.native_details();
        if !relevant_code || details.truncated {
            return Err(
                "deleted-file input rejection remains unproven: unexpected native row category"
                    .into(),
            );
        }
        let reference = match &self.reference {
            FileReference::Provider { id, .. } => id.as_str(),
            FileReference::Url { url, .. } => url.as_str(),
        };
        let names = details
            .details
            .iter()
            .filter_map(|detail| match detail {
                NativeDetail::Resource { name, .. } => Some(name.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        let message = details.message.as_deref().unwrap_or_default();
        let identified = if names.is_empty() {
            ErrorEvidence::identity_token(message, &self.id)
                || ErrorEvidence::identity_token(message, reference)
        } else {
            names
                .iter()
                .all(|name| *name == self.id || *name == reference)
        };
        if !identified {
            return Err("deleted-file input rejection remains unproven: diagnostic omitted the exact owned file ID or input URI".into());
        }
        Ok(())
    }
}

#[derive(Debug)]
enum PublicationState {
    Prepared,
    PublicationUncertain,
    Published { job_id: String },
    Rejected,
}

/// Exact ownership and publication obligations for one closed batch fixture.
pub struct InlinePublication {
    expected: BTreeMap<String, Value>,
    state: PublicationState,
    upload_attempted: bool,
    input_id: Option<String>,
}

impl InlinePublication {
    pub fn new(model: &str, rows: &[BatchInput]) -> Result<Self, String> {
        let mut expected = BTreeMap::new();
        if model.is_empty() || rows.is_empty() {
            return Err("batch ownership requires model and rows".into());
        }
        for row in rows {
            let request = row.request();
            if request.messages().len() != 1
                || !request.tools().is_empty()
                || request.reasoning().is_some()
                || request.response_format().is_some()
                || request.generation()
                    != &GenerationConfig::default()
                        .with_max_output_tokens(NonZeroU32::new(4096).unwrap())
            {
                return Err(
                    "batch cleanup ownership only supports the closed strict fixture request"
                        .into(),
                );
            }
            let message = &request.messages()[0];
            if message.role() != Role::User
                || !message.function_calls().is_empty()
                || !message.function_results().is_empty()
                || message.continuation().is_some()
            {
                return Err("batch cleanup ownership requires one ordinary user message".into());
            }
            let mut content = Vec::new();
            for part in message.content() {
                content.push(match part {
                    ContentPart::Text { text } => json!({"type":"input_text","text":text}),
                    ContentPart::File {
                        reference: FileReference::Provider { id, .. },
                    } => json!({"type":"input_file","file_id":id}),
                    ContentPart::File {
                        reference: FileReference::Url { url, .. },
                    } => json!({"type":"input_file","file_url":url}),
                    _ => return Err(
                        "batch cleanup ownership encountered content outside the fixture contract"
                            .into(),
                    ),
                });
            }
            let value = json!({"custom_id":row.custom_id(),"method":"POST","url":"/v1/responses","body":{
                "model":model,"input":[{"type":"message","role":"user","content":content}],
                "max_output_tokens":4096,"include":["reasoning.encrypted_content"],"store":false
            }});
            if expected.insert(row.custom_id().to_owned(), value).is_some() {
                return Err("batch ownership has duplicate row IDs".into());
            }
        }
        Ok(Self {
            expected,
            state: PublicationState::Prepared,
            upload_attempted: false,
            input_id: None,
        })
    }

    pub fn from_prepared_jsonl(model: &str, bytes: &[u8]) -> Result<Self, String> {
        let expected =
            Self::parse_input(bytes).ok_or("prepared batch input is not unique JSONL")?;
        if expected.is_empty()
            || expected.values().any(|row| {
                row.get("method").and_then(Value::as_str) != Some("POST")
                    || row.get("url").and_then(Value::as_str) != Some("/v1/responses")
                    || row.pointer("/body/model").and_then(Value::as_str) != Some(model)
            })
        {
            return Err("prepared native batch input changed model or request shape".into());
        }
        Ok(Self {
            expected,
            state: PublicationState::Prepared,
            upload_attempted: false,
            input_id: None,
        })
    }

    /// Bind a known ordinary source before its dependent batch is admitted.
    pub fn bind_source(
        &self,
        source: &FileObject,
        owner: usize,
        files: &mut BTreeMap<String, Option<usize>>,
    ) -> Result<(), String> {
        if !matches!(self.state, PublicationState::Prepared)
            || source.id.trim().is_empty()
            || self.input_id.as_deref() == Some(source.id.as_str())
            || !matches!(files.get(&source.id), Some(None))
        {
            return Err("batch source must be known, unassigned, distinct from its input, and bound before publication".into());
        }
        let reference = source
            .input_reference
            .as_ref()
            .ok_or("batch source lacks its exact generation input reference")?;
        let referenced = self.expected.values().any(|row| {
            row.pointer("/body/input/0/content")
                .and_then(Value::as_array)
                .is_some_and(|parts| {
                    parts.iter().any(|part| {
                        part.get("type").and_then(Value::as_str) == Some("input_file")
                            && match reference {
                                FileReference::Provider { id, .. } => {
                                    part.get("file_id").and_then(Value::as_str) == Some(id.as_str())
                                }
                                FileReference::Url { url, .. } => {
                                    part.get("file_url").and_then(Value::as_str)
                                        == Some(url.as_str())
                                }
                            }
                    })
                })
        });
        if !referenced {
            return Err("batch source reference is not used by this exact publication".into());
        }
        // All validation precedes the sole mutation. The JSONL input identity
        // remains owned independently by input_id and its discovery state.
        *files
            .get_mut(&source.id)
            .ok_or("batch source ownership disappeared before binding")? = Some(owner);
        Ok(())
    }

    pub fn upload_started(&mut self) -> Result<(), String> {
        if !matches!(self.state, PublicationState::Prepared) || self.upload_attempted {
            return Err("batch fixture upload cannot be replayed".into());
        }
        self.upload_attempted = true;
        Ok(())
    }

    pub fn uploaded_input(&mut self, id: &str) -> Result<(), String> {
        if !matches!(self.state, PublicationState::Prepared) {
            return Err("native input upload completed after publication began".into());
        }
        self.set_input(id)
    }

    pub fn begin(&mut self) -> Result<(), String> {
        if !matches!(self.state, PublicationState::Prepared) {
            return Err("batch submission cannot be replayed".into());
        }
        self.state = PublicationState::PublicationUncertain;
        Ok(())
    }

    pub fn accepted(&mut self, job_id: &str) -> Result<(), String> {
        if !matches!(self.state, PublicationState::PublicationUncertain) || job_id.trim().is_empty()
        {
            return Err("batch publication did not return one exact owned job ID".into());
        }
        self.state = PublicationState::Published {
            job_id: job_id.to_owned(),
        };
        Ok(())
    }

    pub fn rejected(&mut self) -> Result<(), String> {
        if !matches!(self.state, PublicationState::PublicationUncertain) {
            return Err("batch rejection has no admitted submission".into());
        }
        self.state = PublicationState::Rejected;
        Ok(())
    }

    /// Keeps the returned Kit failure authoritative while classifying ownership.
    pub fn submission_failed(&mut self, error: KitError) -> CaseFailure {
        // Local validation has not submitted a job. Once sending begins, only
        // the same affirmative header rejections as the native batch owner
        // release file dependencies; other failures leave publication unknown.
        let phase = error.context().phase;
        let rejected = matches!(
            (error.kind(), phase),
            (ErrorKind::Configuration, ErrorPhase::Configuration)
                | (
                    ErrorKind::InvalidInput | ErrorKind::Unsupported,
                    ErrorPhase::InputValidation
                )
                | (ErrorKind::Closed, ErrorPhase::Lifecycle)
        ) || phase == ErrorPhase::RequestBuild
            || (phase == ErrorPhase::ResponseHeaders
                && matches!(error.http_status(), Some(400 | 422)));
        let primary = CaseFailure::from(error);
        if rejected && let Err(cleanup) = self.rejected() {
            CaseFailure::Group {
                label: "batch submission and ownership classification failures",
                failures: vec![primary, CaseFailure::from(cleanup)],
            }
        } else {
            primary
        }
    }

    pub fn publication_uncertain(&self) -> bool {
        matches!(self.state, PublicationState::PublicationUncertain)
    }
    pub fn needs_input_discovery(&self) -> bool {
        self.upload_attempted && self.input_id.is_none()
    }
    pub fn input_id(&self) -> Option<&str> {
        self.input_id.as_deref()
    }
    pub fn job_id(&self) -> Option<&str> {
        match &self.state {
            PublicationState::Published { job_id } => Some(job_id),
            _ => None,
        }
    }

    pub fn capture_input(&mut self, job_id: &str, file_id: &str) -> Result<(), String> {
        if self.job_id() != Some(job_id) {
            return Err("input artifact metadata belongs to another submission".into());
        }
        self.set_input(file_id)
    }

    pub fn discover_input(&mut self, file_id: &str, bytes: &[u8]) -> Result<(), String> {
        if !self.needs_input_discovery() || !self.owns_input(bytes) {
            return Err("input bytes do not prove this unresolved submission's ownership".into());
        }
        self.set_input(file_id)
    }

    fn set_input(&mut self, id: &str) -> Result<(), String> {
        if id.trim().is_empty() || self.input_id.as_deref().is_some_and(|known| known != id) {
            return Err("batch input artifact identity changed".into());
        }
        self.input_id = Some(id.to_owned());
        Ok(())
    }

    pub fn can_delete_input(&self, job_terminal: bool) -> bool {
        match self.state {
            PublicationState::Prepared | PublicationState::Rejected => true,
            PublicationState::Published { .. } => job_terminal,
            PublicationState::PublicationUncertain => false,
        }
    }

    pub fn owns_input(&self, bytes: &[u8]) -> bool {
        Self::parse_input(bytes).is_some_and(|rows| rows == self.expected)
    }

    fn parse_input(bytes: &[u8]) -> Option<BTreeMap<String, Value>> {
        let text = std::str::from_utf8(bytes).ok()?;
        let mut rows = BTreeMap::new();
        for line in text.lines() {
            let row: Value = serde_json::from_str(line).ok()?;
            let id = row.get("custom_id")?.as_str()?.to_owned();
            if id.is_empty() || rows.insert(id, row).is_some() {
                return None;
            }
        }
        Some(rows)
    }
}

#[derive(Clone, Copy, Debug)]
enum CleanupState {
    Tracked,
    Terminal,
    ArtifactsCaptured,
    DeletionUncertain,
    Deleted,
}

/// Cleanup obligations for one batch ID retained by the case's ownership map.
#[derive(Clone, Copy, Debug)]
pub struct BatchCleanup {
    state: CleanupState,
}

impl BatchCleanup {
    pub fn new() -> Self {
        Self {
            state: CleanupState::Tracked,
        }
    }

    pub fn needs_status(&self) -> bool {
        matches!(self.state, CleanupState::Tracked)
    }

    pub fn needs_artifacts(&self) -> bool {
        matches!(self.state, CleanupState::Terminal)
    }

    pub fn deletion_uncertain(&self) -> bool {
        matches!(self.state, CleanupState::DeletionUncertain)
    }

    pub fn terminal(&mut self, job: &BatchJob, expected_id: &str) -> Result<(), String> {
        if !matches!(self.state, CleanupState::Tracked)
            || job.id != expected_id
            || expected_id.is_empty()
            || matches!(job.status, BatchStatus::Queued | BatchStatus::Running)
        {
            return Err("cleanup terminal proof lacked the exact owned terminal job".into());
        }
        self.state = CleanupState::Terminal;
        Ok(())
    }

    pub fn artifacts_captured(&mut self) -> Result<(), String> {
        if !matches!(self.state, CleanupState::Terminal) {
            return Err("batch artifact capture requires proven terminal ownership".into());
        }
        self.state = CleanupState::ArtifactsCaptured;
        Ok(())
    }

    /// Record uncertainty before awaiting the native prerequisite request.
    pub fn begin_delete(&mut self) -> Result<(), String> {
        if !matches!(self.state, CleanupState::ArtifactsCaptured) {
            return Err(
                "batch deletion requires captured artifacts from an owned terminal job".into(),
            );
        }
        self.state = CleanupState::DeletionUncertain;
        Ok(())
    }

    pub fn deletion_acknowledged(&mut self, body: &[u8]) -> Result<(), String> {
        if !matches!(self.state, CleanupState::DeletionUncertain) {
            return Err("batch deletion acknowledgement lacked its owned request".into());
        }
        if !body.is_empty() {
            let value: serde_json::Value = serde_json::from_slice(body)
                .map_err(|_| "native Gemini batch deletion returned invalid JSON".to_owned())?;
            if !value.as_object().is_some_and(serde_json::Map::is_empty) {
                return Err("native Gemini batch deletion returned neither an empty body nor an empty object".into());
            }
        }
        self.state = CleanupState::Deleted;
        Ok(())
    }
}
