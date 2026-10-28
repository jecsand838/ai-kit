use crate::live_failures::CaseFailure;
use providers_kit::error::{
    ErrorKind, ErrorPhase, NativeErrorDetails, Operation, ResponseDetails, ResponseViolation,
};
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::time::Duration;
use tokio::time::Instant;

use providers_kit::batch::{
    BatchInput, BatchJob, BatchListRequest, BatchResult, BatchStatus, BatchSubmitRequest,
};
use providers_kit::files::{FileListRequest, FileObject, FileUploadRequest};
use providers_kit::generation::GenerateRequest;
use providers_kit::{KitError, ModelBackend, ProviderBackend};
use reqwest::{Client, Method};
use serde_json::{Value, json};

use super::{LiveProvider, Mode, Service};
use crate::hosted_assertions::HostedProvider;
use crate::live_assertions::{
    BatchCleanup, BatchEvidence, DeletedFileEvidence, FileInventoryEvidence, GeminiOutputRetention,
    GenerationEvidence, InlinePublication,
};

const MAX_RESULT_BYTES: NonZeroUsize = NonZeroUsize::new(4 * 1024 * 1024).unwrap();
const PAGE_SIZE: NonZeroUsize = NonZeroUsize::new(100).unwrap();

enum BatchCase {
    Inline,
    ProviderFile,
    Mixed,
    Cancellation,
    Missing,
    Bounded,
    UnsupportedFile,
    UnsupportedAll,
}

struct BatchFixture<'a> {
    live: &'a LiveProvider,
    completion_deadline: Option<Instant>,
    client: Client,
    nonce: String,
    jobs: BTreeMap<String, BatchCleanup>,
    files: BTreeMap<String, Option<usize>>,
    file_observations: Vec<Value>,
    gemini_outputs: BTreeMap<String, GeminiOutputRetention>,
    publications: Vec<InlinePublication>,
    job_publications: BTreeMap<String, usize>,
    cleanup_errors: Vec<CaseFailure>,
}

impl BatchCase {
    async fn run(self, service: Service) {
        let live = LiveProvider::new(service).expect("strict-live provider configuration");
        let completion_deadline = match (&self, service) {
            (Self::Inline | Self::Mixed | Self::Bounded, Service::Anthropic) => {
                Some(Instant::now() + Duration::from_secs(24 * 60 * 60))
            }
            _ => None,
        };
        let result = match BatchFixture::new(&live, completion_deadline) {
            Ok(mut fixture) => {
                let result = match self {
                    Self::Inline => fixture.positive(false, false).await,
                    Self::ProviderFile => fixture.positive(true, false).await,
                    Self::Bounded => fixture.positive(false, true).await,
                    Self::Mixed => fixture.mixed().await,
                    Self::Cancellation => fixture.cancellation().await,
                    Self::Missing => fixture.missing().await,
                    Self::UnsupportedFile => fixture.unsupported_file().await,
                    Self::UnsupportedAll => fixture.unsupported_all().await,
                };
                fixture.finish(result).await
            }
            Err(error) => Err(error),
        };
        live.finish(result).await;
    }
}

impl<'a> BatchFixture<'a> {
    fn new(
        live: &'a LiveProvider,
        completion_deadline: Option<Instant>,
    ) -> Result<Self, CaseFailure> {
        let client = Client::builder()
            .timeout(Duration::from_secs(60))
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map_err(|error| {
                CaseFailure::from(Self::native_transport(error, ErrorPhase::Configuration))
                    .context("batch fixture HTTP client")
            })?;
        Ok(Self {
            live,
            completion_deadline,
            client,
            nonce: LiveProvider::nonce(),
            jobs: BTreeMap::new(),
            files: BTreeMap::new(),
            file_observations: Vec::new(),
            gemini_outputs: BTreeMap::new(),
            publications: Vec::new(),
            job_publications: BTreeMap::new(),
            cleanup_errors: Vec::new(),
        })
    }

    fn prompt(marker: &str) -> String {
        format!("Reply with exactly {marker}. Do not add punctuation or other text.")
    }

    fn row(id: &str, marker: &str) -> Result<BatchInput, CaseFailure> {
        BatchInput::new(id, LiveProvider::text_request(&Self::prompt(marker))?)
            .map_err(|error| CaseFailure::from(error).context("batch row validation"))
    }

    async fn submit(
        &mut self,
        request: &BatchSubmitRequest,
        source: Option<&FileObject>,
    ) -> Result<BatchJob, CaseFailure> {
        if self
            .completion_deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err("shared batch completion deadline expired before submission".into());
        }
        let owner = match request {
            BatchSubmitRequest::Inline { rows } => {
                let owner = self.publications.len();
                self.publications
                    .push(InlinePublication::new(self.live.default_model(), rows)?);
                owner
            }
            BatchSubmitRequest::ProviderFile { file_id } => self
                .files
                .get(file_id)
                .copied()
                .flatten()
                .ok_or("provider-file submission lacks an exact owned input")?,
        };
        if let Some(source) = source {
            self.publications[owner].bind_source(source, owner, &mut self.files)?;
        }
        let model = self.live.model()?;
        if matches!(self.live.service(), Service::OpenAi)
            && matches!(request, BatchSubmitRequest::Inline { .. })
        {
            self.publications[owner].upload_started()?;
        }
        self.publications[owner].begin()?;
        let job = match model.submit_batch(request).await {
            Ok(job) => job,
            Err(error) => {
                return Err(self.publications[owner]
                    .submission_failed(error)
                    .context("batch submission failed before required row evidence"));
            }
        };
        if self.jobs.contains_key(&job.id) {
            return Err("batch submission reused a case-owned job ID".into());
        }
        self.publications[owner].accepted(&job.id)?;
        self.job_publications.insert(job.id.clone(), owner);
        self.jobs.insert(job.id.clone(), BatchCleanup::new());
        self.write_ownership(Some(owner)).map_err(|error| {
            CaseFailure::from(format!(
                "accepted batch ownership checkpoint failed: {:?}",
                error.kind()
            ))
            .with_owner(job.id.clone())
        })?;
        Ok(job)
    }

    async fn completed(&mut self, id: &str) -> Result<BatchJob, CaseFailure> {
        let started = Instant::now();
        let deadline = self
            .completion_deadline
            .unwrap_or_else(|| started + self.completion_timeout());
        let poll_interval = match self.live.service() {
            Service::OpenAi => Duration::from_secs(60),
            Service::Anthropic | Service::Gemini | Service::Ollama => Duration::from_secs(5),
        };
        tokio::time::timeout_at(deadline, async {
            loop {
                let job = self
                    .live
                    .provider()
                    .batch_status(id.to_owned())
                    .await
                    .map_err(|error| {
                        CaseFailure::from(error).context("batch completion polling")
                    })?;
                if job.id != id {
                    return Err("batch completion returned a different resource".into());
                }
                match job.status {
                    BatchStatus::Queued | BatchStatus::Running => {
                        tokio::time::sleep(poll_interval).await;
                    }
                    BatchStatus::Completed => {
                        BatchEvidence::completed(&job, id)?;
                        self.record_terminal(&job, id)?;
                        return Ok(job);
                    }
                    BatchStatus::Failed { .. } | BatchStatus::Cancelled | BatchStatus::Expired => {
                        return Err(Self::job_failure(
                            &job,
                            "batch did not complete successfully",
                        ));
                    }
                }
            }
        })
        .await
        .map_err(|_| {
            CaseFailure::from(format!(
                "batch unfinished within the remaining {}-second completion budget; coverage failed",
                deadline.saturating_duration_since(started).as_secs()
            ))
            .with_owner(id.to_owned())
        })?
    }

    async fn listed(&self, id: &str) -> Result<(), CaseFailure> {
        let mut request = BatchListRequest::default().with_limit(PAGE_SIZE);
        let mut cursors = BTreeSet::new();
        for _ in 0..20 {
            let page = self
                .live
                .provider()
                .list_batches(request)
                .await
                .map_err(|error| CaseFailure::from(error).context("batch list"))?;
            if page.jobs.iter().any(|job| job.id == id) {
                return Ok(());
            }
            let Some(cursor) = page.next_cursor else {
                break;
            };
            if !cursors.insert(cursor.clone()) {
                return Err("batch list repeated its cursor".into());
            }
            request = BatchListRequest::default()
                .with_limit(PAGE_SIZE)
                .with_cursor(cursor)
                .map_err(|error| CaseFailure::from(error).context("batch list cursor"))?;
        }
        Err("bounded batch listing omitted the submitted job".into())
    }

    async fn results(&self, id: &str) -> Result<Vec<BatchResult>, CaseFailure> {
        let rows = self
            .live
            .provider()
            .batch_results(id.to_owned(), MAX_RESULT_BYTES)
            .await
            .map_err(|error| CaseFailure::from(error).context("public batch results"))?;
        self.live.capture_batch(id, &rows);
        Ok(rows)
    }

    async fn positive(&mut self, provider_file: bool, bounded: bool) -> Result<(), CaseFailure> {
        let expected = vec![
            (
                format!("good-first-{}", self.nonce),
                format!("batch-first-{}", self.nonce),
            ),
            (
                format!("good-second-{}", self.nonce),
                format!("batch-second-{}", self.nonce),
            ),
        ];
        let request = if provider_file {
            let file_id = self.input_file(&expected).await?;
            BatchSubmitRequest::ProviderFile { file_id }
        } else {
            BatchSubmitRequest::Inline {
                rows: expected
                    .iter()
                    .map(|(id, marker)| Self::row(id, marker))
                    .collect::<Result<Vec<_>, _>>()?,
            }
        };
        let job = self.submit(&request, None).await?;
        self.completed(&job.id).await?;
        self.listed(&job.id).await?;
        let rows = self.results(&job.id).await?;
        Self::row_assertion(BatchEvidence::rows(&rows, &expected), &rows)?;
        if bounded {
            match self
                .live
                .provider()
                .batch_results(job.id.clone(), NonZeroUsize::MIN)
                .await
            {
                Err(error)
                    if error.kind()
                        == ErrorKind::InvalidResponse(ResponseViolation::LimitExceeded {
                            limit: 1,
                        }) => {}
                Err(error) => {
                    return Err(CaseFailure::from(
                        "one-byte result limit returned the wrong error",
                    )
                    .with_sdk(error));
                }
                Ok(rows) => {
                    self.live.capture_batch(&job.id, &rows);
                    return Err("one-byte result limit accepted real batch output".into());
                }
            }
        }
        Ok(())
    }

    async fn uploaded(
        &mut self,
        name: String,
        media_type: &str,
        bytes: Vec<u8>,
    ) -> Result<FileObject, CaseFailure> {
        let request = FileUploadRequest::new(name, media_type, bytes)
            .map_err(|error| CaseFailure::from(error).context("fixture file validation"))?;
        let file = self
            .live
            .provider()
            .upload_file(request.clone())
            .await
            .map_err(|error| CaseFailure::from(error).context("fixture upload"))?;
        self.files.insert(file.id.clone(), None);
        self.file_observations.push(json!({
            "stage": "upload_acknowledged",
            "request": {
                "file_name": request.file_name(),
                "media_type": request.media_type(),
                "size_bytes": request.data().len(),
            },
            "file": file,
        }));
        let id = file.id.clone();
        let uploaded = file;
        let provider = match self.live.service() {
            Service::OpenAi => HostedProvider::OpenAi,
            Service::Anthropic => HostedProvider::Anthropic,
            Service::Gemini => HostedProvider::Gemini,
            Service::Ollama => return Err("Ollama has no batch upload fixture".into()),
        };
        // Validate the acknowledgment before looking up its exact owned ID.
        // An ACTIVE acknowledgment does not replace the subsequent Kit lookup.
        BatchEvidence::fixture_file_ready(&uploaded, &uploaded, &request, provider)?;
        tokio::time::timeout(Duration::from_secs(90), async {
            loop {
                let file = self
                    .live
                    .provider()
                    .file_info(id.clone())
                    .await
                    .map_err(|error| CaseFailure::from(error).context("fixture file readiness"))?;
                self.file_observations.push(json!({
                    "stage": "readiness_lookup",
                    "requested_id": id,
                    "file": file,
                }));
                if BatchEvidence::fixture_file_ready(&file, &uploaded, &request, provider)? {
                    return Ok(file);
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        })
        .await
        .map_err(|_| CaseFailure::from("fixture file readiness timed out"))?
    }

    async fn deleted_reference(
        &mut self,
    ) -> Result<(DeletedFileEvidence, GenerateRequest), CaseFailure> {
        // This secret is independent of the case nonce used for every public identity.
        let marker = LiveProvider::nonce();
        let file = self
            .uploaded(
                format!("batch-deleted-{}.txt", self.nonce),
                "text/plain",
                format!("Record: delivery confirmation\nConfirmation code: {marker}\n")
                    .into_bytes(),
            )
            .await?;
        let reference = file
            .input_reference
            .clone()
            .ok_or("uploaded fixture lacked an input reference")?;
        let mut deleted = DeletedFileEvidence::new(file.id.clone(), reference);
        let request = deleted.read_request()?;
        let response = self.live.generate(&request, Mode::Unary).await?;
        deleted.preflight(&response, &marker)?;
        let preflight_id = format!("readability-{}", self.nonce);
        let submitted = self
            .submit(
                &BatchSubmitRequest::Inline {
                    rows: vec![BatchInput::new(preflight_id.clone(), request.clone())?],
                },
                Some(&file),
            )
            .await?;
        let job = self.completed(&submitted.id).await?;
        let rows = self.results(&job.id).await?;
        Self::row_assertion(
            deleted.batch_preflight(&job, &submitted.id, &rows, &preflight_id, &marker),
            &rows,
        )?;
        let visible = self.file_inventory(deleted.id()).await?;
        deleted.visible(&visible)?;
        self.live
            .provider()
            .delete_file(deleted.id().to_owned())
            .await
            .map_err(|error| {
                CaseFailure::from(error.with_request_target(deleted.id().to_owned()))
                    .context("delete owned negative fixture")
                    .with_owner(deleted.id().to_owned())
            })?;
        deleted.deletion_acknowledged()?;
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                let inventory = self.file_inventory(deleted.id()).await?;
                if deleted.absent(&inventory)? { return Ok::<(), CaseFailure>(()); }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }).await.map_err(|_| CaseFailure::from("file deletion was acknowledged but complete-list absence was not established within 60 seconds"))??;
        self.files.remove(deleted.id());
        Ok((deleted, request))
    }

    async fn file_inventory(&self, id: &str) -> Result<FileInventoryEvidence, CaseFailure> {
        tokio::time::timeout(Duration::from_secs(60), async {
            let mut inventory = FileInventoryEvidence::new(id);
            let mut request = FileListRequest::default().with_limit(PAGE_SIZE);
            for _ in 0..100 {
                let page = self.live.provider().list_files(request).await
                    .map_err(|error| CaseFailure::from(error).context("owned-file inventory"));
                let Some(cursor) = inventory.observe_result(page)? else { return Ok(inventory); };
                request = FileListRequest::default().with_limit(PAGE_SIZE).with_cursor(cursor)?;
            }
            Err(CaseFailure::from("owned-file inventory exceeded 100 pages without completing; absence remains unproven"))
        }).await.map_err(|_| CaseFailure::from("owned-file inventory exceeded its 60-second deadline"))?
    }

    async fn mixed(&mut self) -> Result<(), CaseFailure> {
        let (mut deleted, request) = self.deleted_reference().await.map_err(|error| {
            error.context(
                "deleted-file fixture prerequisite failed; row-error coverage remains unproven",
            )
        })?;
        let good_id = format!("good-{}", self.nonce);
        let bad_id = format!("bad-{}", self.nonce);
        let marker = format!("mixed-ok-{}", self.nonce);
        let job = self
            .submit(
                &BatchSubmitRequest::Inline {
                    rows: vec![
                        Self::row(&good_id, &marker)?,
                        BatchInput::new(bad_id.clone(), request)?,
                    ],
                },
                None,
            )
            .await?;
        self.completed(&job.id).await?;
        let rows = self.results(&job.id).await?;
        let error = match deleted.mixed_rows(&rows, (&good_id, &marker), &bad_id) {
            Ok(error) => error,
            Err(error) => return Self::row_assertion(Err(error), &rows),
        };
        let result = match self.live.service() {
            Service::OpenAi => deleted.openai(error),
            Service::Anthropic => deleted.anthropic(error),
            Service::Gemini => deleted.gemini(error),
            Service::Ollama => Err("Ollama has no batch file input".to_owned()),
        };
        result.map_err(|message| CaseFailure::Group {
            label: "deleted-file row assertion and accepted outcome",
            failures: vec![
                CaseFailure::from(message),
                CaseFailure::BatchRow {
                    custom_id: bad_id,
                    error: error.clone(),
                },
            ],
        })
    }

    fn row_assertion(result: Result<(), String>, rows: &[BatchResult]) -> Result<(), CaseFailure> {
        result.map_err(|message| {
            let mut failures = vec![CaseFailure::from(message)];
            for row in rows {
                if let BatchResult::Failed { custom_id, error } = row {
                    failures.push(CaseFailure::BatchRow {
                        custom_id: custom_id.clone(),
                        error: error.clone(),
                    });
                }
            }
            CaseFailure::Group {
                label: "batch assertion and accepted row failures",
                failures,
            }
        })
    }

    fn job_failure(job: &BatchJob, message: &'static str) -> CaseFailure {
        let mut failure = CaseFailure::from(message);
        if let BatchStatus::Failed { errors } = &job.status {
            for error in errors {
                failure = failure.with_remote(error.clone());
            }
        }
        failure.with_owner(job.id.clone())
    }

    fn completion_timeout(&self) -> Duration {
        match self.live.service() {
            Service::OpenAi => Duration::from_secs(24 * 60 * 60),
            Service::Anthropic | Service::Gemini | Service::Ollama => Duration::from_secs(30 * 60),
        }
    }

    fn cancellation_timeout(&self) -> Duration {
        match self.live.service() {
            Service::OpenAi => Duration::from_secs(11 * 60),
            Service::Anthropic | Service::Gemini | Service::Ollama => Duration::from_secs(3 * 60),
        }
    }

    async fn terminal(&self, id: &str, deadline: Duration) -> Result<BatchJob, CaseFailure> {
        tokio::time::timeout(deadline, async {
            loop {
                let job = self
                    .live
                    .provider()
                    .batch_status(id.to_owned())
                    .await
                    .map_err(|error| CaseFailure::from(error).context("batch terminal polling"))?;
                if job.id != id {
                    return Err("batch polling changed resource identity".into());
                }
                if matches!(job.status, BatchStatus::Queued | BatchStatus::Running) {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                } else {
                    return Ok(job);
                }
            }
        })
        .await
        .map_err(|_| {
            CaseFailure::from(format!(
                "batch did not settle within {} seconds",
                deadline.as_secs()
            ))
            .with_owner(id.to_owned())
        })?
    }

    async fn cancellation(&mut self) -> Result<(), CaseFailure> {
        let expected = (0..8)
            .map(|index| {
                (
                    format!("cancel-{}-{index}", self.nonce),
                    format!("cancel-result-{}-{index}", self.nonce),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let rows = expected
            .iter()
            .map(|(id, marker)| Self::row(id, marker))
            .collect::<Result<Vec<_>, _>>()?;
        let job = self
            .submit(&BatchSubmitRequest::Inline { rows }, None)
            .await?;
        let cancelled = match self.live.provider().cancel_batch(job.id.clone()).await {
            Ok(cancelled) => cancelled,
            Err(error) => {
                let mut failures =
                    vec![CaseFailure::from(error).context("cancellation was not accepted")];
                if matches!(self.live.service(), Service::OpenAi | Service::Gemini) {
                    match self.live.provider().batch_status(job.id.clone()).await {
                        Err(error) => failures
                            .push(CaseFailure::from(error).context("cancellation status refresh")),
                        Ok(refreshed) if refreshed.id != job.id => {
                            failures.push("cancellation refresh changed job identity".into())
                        }
                        Ok(refreshed) if matches!(refreshed.status, BatchStatus::Completed) => {
                            let expected = expected.into_iter().collect::<Vec<_>>();
                            match self.results(&job.id).await {
                                Ok(rows) => {
                                    if let Err(error) = Self::row_assertion(
                                        BatchEvidence::rows(&rows, &expected),
                                        &rows,
                                    ) {
                                        failures.push(error);
                                    }
                                }
                                Err(error) => failures.push(error),
                            }
                            failures.push(
                                "job completed but cancellation effect remains unproven".into(),
                            );
                        }
                        Ok(refreshed) => {
                            if matches!(refreshed.status, BatchStatus::Failed { .. }) {
                                failures.push(Self::job_failure(
                                    &refreshed,
                                    "cancelled job returned failed terminal data",
                                ));
                            }
                        }
                    }
                }
                return CaseFailure::collected("cancellation and completion evidence", failures);
            }
        };
        if cancelled.id != job.id {
            return Err("cancellation changed resource identity".into());
        }
        let terminal = self.terminal(&job.id, self.cancellation_timeout()).await?;
        match self.live.service() {
            Service::OpenAi | Service::Gemini => {
                if matches!(terminal.status, BatchStatus::Completed) {
                    let expected = expected.into_iter().collect::<Vec<_>>();
                    let rows = self.results(&job.id).await?;
                    Self::row_assertion(BatchEvidence::rows(&rows, &expected), &rows)?;
                    return Err("batch completed with verified rows before cancellation took effect; cancellation effect remains unproved".into());
                }
                if !matches!(terminal.status, BatchStatus::Cancelled) {
                    return Err(Self::job_failure(
                        &terminal,
                        "cancellation effect was not proved by terminal state",
                    ));
                }
            }
            Service::Anthropic => {
                BatchEvidence::completed(&terminal, &job.id)?;
                let rows = self.results(&job.id).await?;
                let mut seen = BTreeSet::new();
                let mut cancelled_rows = 0;
                for row in rows {
                    match row {
                        BatchResult::Cancelled { custom_id }
                            if expected.contains_key(&custom_id)
                                && seen.insert(custom_id.clone()) =>
                        {
                            cancelled_rows += 1
                        }
                        BatchResult::Succeeded { custom_id, output }
                            if expected.contains_key(&custom_id)
                                && seen.insert(custom_id.clone()) =>
                        {
                            let marker = expected.get(&custom_id).ok_or_else(|| {
                                "cancellation row lost expected marker".to_owned()
                            })?;
                            GenerationEvidence::exact_text(&output, marker, false)?;
                        }
                        BatchResult::Failed { custom_id, error } => {
                            return Err(CaseFailure::BatchRow { custom_id, error }
                                .context("cancellation returned a failed row"));
                        }
                        _ => {
                            return Err(
                                "cancellation returned failed, expired, duplicate, or unknown rows"
                                    .into(),
                            );
                        }
                    }
                }
                if seen.len() != expected.len() {
                    return Err("cancellation results omitted submitted rows".into());
                }
                if cancelled_rows == 0 {
                    return Err("all Anthropic rows completed before cancellation; cancellation effect remains unproved".into());
                }
            }
            Service::Ollama => {
                return Err("Ollama cancellation must use the unsupported case".into());
            }
        }
        Ok(())
    }

    fn record_terminal(&mut self, job: &BatchJob, expected_id: &str) -> Result<(), CaseFailure> {
        self.jobs
            .get_mut(expected_id)
            .ok_or_else(|| "terminal batch was not owned by this case".to_owned())?
            .terminal(job, expected_id)
            .map_err(CaseFailure::from)
    }

    async fn deleted_gemini_batch(&mut self) -> Result<String, CaseFailure> {
        if self.live.service() != Service::Gemini {
            return Err("native batch deletion prerequisite is Gemini-only".into());
        }
        let row = Self::row(
            &format!("missing-{}", self.nonce),
            &format!("owned-{}", self.nonce),
        )?;
        let job = self
            .submit(&BatchSubmitRequest::Inline { rows: vec![row] }, None)
            .await?;
        // This fixture needs a valid server-issued terminal operation, not
        // successful generation. Positive completion assertions stay separate.
        let terminal = self.terminal(&job.id, self.completion_timeout()).await?;
        self.record_terminal(&terminal, &job.id)?;
        // Capture cleanup identities before deleting the only native metadata owner.
        // This read supplies no batch-result evidence.
        self.batch_artifacts(&job.id)
            .await
            .map_err(|error| error.with_owner(job.id.clone()))?;
        let request = self.native(Method::DELETE, &format!("v1beta/{}", job.id))?;
        self.jobs
            .get_mut(&job.id)
            .ok_or("owned Gemini deletion lost its cleanup record")?
            .begin_delete()?;
        // Exactly one native prerequisite request. The state above remains uncertain
        // if sending, reading, or decoding fails; cleanup never retries this deletion.
        let (_, body) = self
            .native_bytes(request)
            .await
            .map_err(|error| error.with_owner(job.id.clone()))?;
        self.jobs
            .get_mut(&job.id)
            .ok_or("owned Gemini deletion lost its cleanup record")?
            .deletion_acknowledged(&body)
            .map_err(|message| {
                CaseFailure::from(message)
                    .with_sdk(
                        KitError::new(
                            ErrorKind::InvalidResponse(ResponseViolation::ProtocolViolation {
                                rule: "native batch deletion must return an empty acknowledgment",
                            }),
                            Operation::Unspecified,
                            ErrorPhase::Normalize,
                        )
                        .with_request_target(job.id.clone())
                        .with_native_details(NativeErrorDetails {
                            body: Some(String::from_utf8_lossy(&body).into_owned()),
                            ..Default::default()
                        }),
                    )
                    .with_owner(job.id.clone())
            })?;
        Ok(job.id)
    }

    async fn missing(&mut self) -> Result<(), CaseFailure> {
        let id = match self.live.service() {
            Service::OpenAi => format!("batch_{}", self.nonce),
            Service::Anthropic => format!("msgbatch_{}", self.nonce),
            Service::Gemini => self.deleted_gemini_batch().await?,
            Service::Ollama => {
                return Err("Ollama missing resources use unsupported coverage".into());
            }
        };
        let status = tokio::time::timeout(
            Duration::from_secs(180),
            self.live.provider().batch_status(id.clone()),
        )
        .await
        .map_err(|_| CaseFailure::from("missing batch status exceeded 180 seconds"))
        .and_then(|result| Self::not_found(result, Operation::BatchStatus));
        let results = tokio::time::timeout(
            Duration::from_secs(180),
            self.live
                .provider()
                .batch_results(id.clone(), MAX_RESULT_BYTES),
        )
        .await
        .map_err(|_| CaseFailure::from("missing batch results exceeded 180 seconds"))
        .and_then(|result| {
            if let Ok(rows) = &result {
                self.live.capture_batch(&id, rows);
            }
            Self::not_found(result, Operation::BatchResults)
        });
        let cancellation = tokio::time::timeout(
            Duration::from_secs(180),
            self.live.provider().cancel_batch(id),
        )
        .await
        .map_err(|_| CaseFailure::from("missing batch cancellation exceeded 180 seconds"))
        .and_then(|result| Self::not_found(result, Operation::CancelBatch));
        let failures = [status, results, cancellation]
            .into_iter()
            .filter_map(Result::err)
            .collect::<Vec<_>>();
        CaseFailure::collected("missing batch resource probes", failures)
    }

    fn not_found<T>(result: Result<T, KitError>, operation: Operation) -> Result<(), CaseFailure> {
        match result {
            Err(error)
                if error.kind() == ErrorKind::NotFound
                    && error.http_status() == Some(404)
                    && error.context().operation == operation
                    && error.context().phase == ErrorPhase::ResponseHeaders =>
            {
                Ok(())
            }
            Err(error) => Err(CaseFailure::from(format!(
                "{operation:?} missing resource returned the wrong error"
            ))
            .with_sdk(error)),
            Ok(_) => Err(format!("{operation:?} succeeded for a nonexistent resource").into()),
        }
    }

    fn unsupported<T>(result: Result<T, KitError>, operation: &str) -> Result<(), CaseFailure> {
        match result {
            Err(ref error) if error.kind() == ErrorKind::Unsupported => Ok(()),
            Err(error) => Err(CaseFailure::from(format!(
                "{operation} returned the wrong unsupported error"
            ))
            .with_sdk(error)),
            Ok(_) => Err(format!("{operation} unexpectedly succeeded").into()),
        }
    }

    async fn unsupported_file(&self) -> Result<(), CaseFailure> {
        Self::unsupported(
            self.live
                .model()?
                .submit_batch(&BatchSubmitRequest::ProviderFile {
                    file_id: format!("file_{}", self.nonce),
                })
                .await,
            "provider-file batch",
        )
    }

    async fn unsupported_all(&self) -> Result<(), CaseFailure> {
        let request = BatchSubmitRequest::Inline {
            rows: vec![Self::row(&self.nonce, &self.nonce)?],
        };
        Self::unsupported(
            self.live.model()?.submit_batch(&request).await,
            "inline batch",
        )?;
        self.unsupported_file().await?;
        Self::unsupported(
            self.live.provider().batch_status(self.nonce.clone()).await,
            "batch_status",
        )?;
        Self::unsupported(
            self.live.provider().cancel_batch(self.nonce.clone()).await,
            "cancel_batch",
        )?;
        Self::unsupported(
            self.live
                .provider()
                .batch_results(self.nonce.clone(), MAX_RESULT_BYTES)
                .await,
            "batch_results",
        )?;
        Self::unsupported(
            self.live
                .provider()
                .list_batches(BatchListRequest::default())
                .await,
            "list_batches",
        )
    }

    async fn input_file(&mut self, expected: &[(String, String)]) -> Result<String, CaseFailure> {
        let mut bytes = Vec::new();
        for (id, marker) in expected {
            let row = match self.live.service() {
                Service::OpenAi => {
                    json!({"custom_id": id, "method": "POST", "url": "/v1/responses", "body": {"model": self.live.default_model(), "input": Self::prompt(marker), "max_output_tokens": 4096}})
                }
                Service::Gemini => {
                    json!({"key": id, "request": {"contents": [{"role": "user", "parts": [{"text": Self::prompt(marker)}]}], "generationConfig": {"maxOutputTokens": 4096}}})
                }
                Service::Anthropic | Service::Ollama => {
                    return Err("provider-file fixture requested for unsupported provider".into());
                }
            };
            serde_json::to_writer(&mut bytes, &row).map_err(|error| {
                CaseFailure::from(
                    KitError::new(
                        ErrorKind::InvalidInput,
                        Operation::Unspecified,
                        ErrorPhase::RequestBuild,
                    )
                    .with_native_details(NativeErrorDetails {
                        message: Some(error.to_string()),
                        ..Default::default()
                    }),
                )
                .context("batch fixture JSONL")
            })?;
            bytes.push(b'\n');
        }
        let owner = self.publications.len();
        let publication = match self.live.service() {
            Service::OpenAi => {
                InlinePublication::from_prepared_jsonl(self.live.default_model(), &bytes)?
            }
            Service::Gemini => InlinePublication::new(
                self.live.default_model(),
                &expected
                    .iter()
                    .map(|(id, marker)| Self::row(id, marker))
                    .collect::<Result<Vec<_>, _>>()?,
            )?,
            Service::Anthropic | Service::Ollama => {
                return Err("unsupported native batch input".into());
            }
        };
        self.publications.push(publication);
        self.publications[owner].upload_started()?;
        match self.live.service() {
            Service::OpenAi => {
                let name = format!("batch-owned-{}.jsonl", self.nonce);
                let part = reqwest::multipart::Part::bytes(bytes.clone())
                    .file_name(name.clone())
                    .mime_str("application/jsonl")
                    .map_err(|error| {
                        CaseFailure::from(Self::native_transport(error, ErrorPhase::RequestBuild))
                            .context("batch fixture multipart")
                    })?;
                let form = reqwest::multipart::Form::new()
                    .text("purpose", "batch")
                    .part("file", part);
                let request = self.native(Method::POST, "files")?.multipart(form);
                let value = self.native_json(request).await?;
                let id = value
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|id| id.starts_with("file-") && id.len() > 5)
                    .ok_or_else(|| "native batch upload omitted its file id".to_owned())?
                    .to_owned();
                self.publications[owner].uploaded_input(&id)?;
                self.files.insert(id.clone(), Some(owner));
                if value.get("filename").and_then(Value::as_str) != Some(name.as_str())
                    || value.get("purpose").and_then(Value::as_str) != Some("batch")
                {
                    return Err(
                        "native batch upload returned a different file name or purpose".into(),
                    );
                }
                let downloaded = self
                    .live
                    .provider()
                    .download_file(id.clone(), MAX_RESULT_BYTES)
                    .await
                    .map_err(|error| {
                        CaseFailure::from(error).context("public batch-input download")
                    })?;
                if downloaded != bytes {
                    return Err(
                        "public batch-input download changed the exact uploaded JSONL bytes".into(),
                    );
                }
                match self
                    .live
                    .provider()
                    .download_file(id.clone(), NonZeroUsize::MIN)
                    .await
                {
                    Err(error)
                        if error.kind()
                            == ErrorKind::InvalidResponse(ResponseViolation::LimitExceeded {
                                limit: 1,
                            }) => {}
                    Err(error) => {
                        return Err(CaseFailure::from(
                            "one-byte batch-input download returned the wrong error",
                        )
                        .with_sdk(error));
                    }
                    Ok(_) => {
                        return Err(
                            "one-byte batch-input download accepted the JSONL fixture".into()
                        );
                    }
                }
                Ok(id)
            }
            Service::Gemini => {
                let id = self
                    .uploaded(
                        format!("batch-owned-{}.jsonl", self.nonce),
                        "application/jsonl",
                        bytes,
                    )
                    .await?
                    .id;
                self.publications[owner].uploaded_input(&id)?;
                self.files.insert(id.clone(), Some(owner));
                Ok(id)
            }
            Service::Anthropic | Service::Ollama => Err("unsupported native batch input".into()),
        }
    }

    fn native(&self, method: Method, path: &str) -> Result<reqwest::RequestBuilder, CaseFailure> {
        let (base_name, key_name, expected_base) = match self.live.service() {
            Service::OpenAi => (
                "TEST_OPENAI_BASE_URL",
                "TEST_OPENAI_API_KEY",
                "https://api.openai.com/v1",
            ),
            Service::Gemini => (
                "TEST_GEMINI_BASE_URL",
                "TEST_GEMINI_API_KEY",
                "https://generativelanguage.googleapis.com",
            ),
            Service::Anthropic | Service::Ollama => {
                return Err("native batch fixture helper is limited to OpenAI/Gemini".into());
            }
        };
        let base = LiveProvider::required(base_name)?;
        if base.trim_end_matches('/') != expected_base {
            return Err("native fixture requires the exact public provider endpoint".into());
        }
        let request = self
            .client
            .request(method, format!("{expected_base}/{path}"));
        let key = LiveProvider::required(key_name)?;
        Ok(match self.live.service() {
            Service::OpenAi => request.bearer_auth(key),
            Service::Gemini => request.header("x-goog-api-key", key),
            Service::Anthropic | Service::Ollama => {
                return Err("unexpected native fixture service".into());
            }
        })
    }

    fn native_transport(error: reqwest::Error, phase: ErrorPhase) -> KitError {
        let timed_out = error.is_timeout();
        let kind = if timed_out {
            ErrorKind::Timeout
        } else {
            ErrorKind::Transport
        };
        let details = NativeErrorDetails {
            message: Some(error.without_url().to_string()),
            ..Default::default()
        };
        let error = KitError::new(kind, Operation::Unspecified, phase).with_native_details(details);
        if timed_out {
            error.with_timeout(Duration::from_secs(60))
        } else {
            error
        }
    }

    async fn native_bytes(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<(String, Vec<u8>), CaseFailure> {
        let request = request
            .build()
            .map_err(|error| Self::native_transport(error, ErrorPhase::RequestBuild))?;
        let target = request.url().to_string();
        let mut response = self.client.execute(request).await.map_err(|error| {
            Self::native_transport(error, ErrorPhase::RequestSend)
                .with_request_target(target.clone())
        })?;
        let status = response.status();
        let primary = (!status.is_success()).then(|| {
            KitError::new(
                ErrorKind::ProviderRejected,
                Operation::Unspecified,
                ErrorPhase::ResponseHeaders,
            )
            .with_http_status(status.as_u16())
            .with_request_target(target.clone())
        });
        let mut bytes = Vec::new();
        loop {
            match response.chunk().await {
                Ok(Some(chunk)) => {
                    let observed = bytes.len().saturating_add(chunk.len());
                    if observed > MAX_RESULT_BYTES.get() {
                        bytes.extend_from_slice(&chunk[..MAX_RESULT_BYTES.get() - bytes.len()]);
                        let details = NativeErrorDetails {
                            body: Some(String::from_utf8_lossy(&bytes).into_owned()),
                            truncated: true,
                            ..Default::default()
                        };
                        let error = KitError::new(
                            ErrorKind::InvalidResponse(ResponseViolation::LimitExceeded {
                                limit: MAX_RESULT_BYTES.get(),
                            }),
                            Operation::Unspecified,
                            ErrorPhase::ReadBody,
                        )
                        .with_request_target(target.clone())
                        .with_response_details(ResponseDetails::Size { observed })
                        .with_native_details(details.clone());
                        return Err(match primary {
                            Some(primary) => primary
                                .with_native_details(details)
                                .with_supplementary(error),
                            None => error,
                        }
                        .into());
                    }
                    bytes.extend_from_slice(&chunk);
                }
                Ok(None) => break,
                Err(error) => {
                    let error = Self::native_transport(error, ErrorPhase::ReadBody)
                        .with_request_target(target.clone());
                    let body = NativeErrorDetails {
                        body: Some(String::from_utf8_lossy(&bytes).into_owned()),
                        truncated: true,
                        ..Default::default()
                    };
                    let mut details = error.native_details().clone();
                    details.body = body.body.clone();
                    details.truncated = true;
                    let error = error.with_native_details(details).with_response_details(
                        ResponseDetails::Size {
                            observed: bytes.len(),
                        },
                    );
                    return Err(match primary {
                        Some(primary) => {
                            primary.with_native_details(body).with_supplementary(error)
                        }
                        None => error,
                    }
                    .into());
                }
            }
        }
        if let Some(error) = primary {
            return Err(error
                .with_native_details(NativeErrorDetails {
                    body: Some(String::from_utf8_lossy(&bytes).into_owned()),
                    ..Default::default()
                })
                .into());
        }
        Ok((target, bytes))
    }

    async fn native_json(&self, request: reqwest::RequestBuilder) -> Result<Value, CaseFailure> {
        let (target, bytes) = self.native_bytes(request).await?;
        serde_json::from_slice(&bytes).map_err(|error| {
            KitError::new(
                ErrorKind::InvalidResponse(ResponseViolation::MalformedJson),
                Operation::Unspecified,
                ErrorPhase::Decode,
            )
            .with_request_target(target)
            .with_response_details(ResponseDetails::Json {
                category: match error.classify() {
                    serde_json::error::Category::Io => providers_kit::error::JsonErrorCategory::Io,
                    serde_json::error::Category::Syntax => {
                        providers_kit::error::JsonErrorCategory::Syntax
                    }
                    serde_json::error::Category::Data => {
                        providers_kit::error::JsonErrorCategory::Data
                    }
                    serde_json::error::Category::Eof => {
                        providers_kit::error::JsonErrorCategory::Eof
                    }
                },
                line: error.line(),
                column: error.column(),
            })
            .with_native_details(NativeErrorDetails {
                message: Some(error.to_string()),
                body: Some(String::from_utf8_lossy(&bytes).into_owned()),
                ..Default::default()
            })
            .into()
        })
    }

    async fn batch_artifacts(&mut self, id: &str) -> Result<(), CaseFailure> {
        if !self.jobs.get(id).is_some_and(BatchCleanup::needs_artifacts) {
            return Err("batch artifact discovery requires an owned terminal job".into());
        }
        let owner = *self
            .job_publications
            .get(id)
            .ok_or("artifact discovery lost submission ownership")?;
        match self.live.service() {
            Service::OpenAi => {
                let value = self
                    .native_json(self.native(Method::GET, &format!("batches/{id}"))?)
                    .await?;
                if value.get("id").and_then(Value::as_str) != Some(id) {
                    return Err("cleanup metadata changed batch identity".into());
                }
                let input = value
                    .get("input_file_id")
                    .and_then(Value::as_str)
                    .filter(|file| file.starts_with("file-") && file.len() > 5)
                    .ok_or_else(|| "cleanup metadata omitted the owned input file".to_owned())?;
                self.publications[owner].capture_input(id, input)?;
                self.files.insert(input.to_owned(), Some(owner));
                for field in ["input_file_id", "output_file_id", "error_file_id"] {
                    match value.get(field) {
                        None | Some(Value::Null) => {}
                        Some(Value::String(file))
                            if file.starts_with("file-") && file.len() > 5 =>
                        {
                            self.files.insert(file.clone(), Some(owner));
                        }
                        _ => {
                            return Err(
                                format!("cleanup metadata contained invalid {field}").into()
                            );
                        }
                    }
                }
            }
            Service::Gemini => {
                let value = self
                    .native_json(self.native(Method::GET, &format!("v1beta/{id}"))?)
                    .await?;
                if value.get("name").and_then(Value::as_str) != Some(id) {
                    return Err("cleanup metadata changed Gemini batch identity".into());
                }
                if let Some(file) = value.pointer("/response/responsesFile") {
                    match file {
                        Value::Null => {}
                        Value::String(file) if file.starts_with("files/") && file.len() > 6 => {
                            if self.files.contains_key(file) {
                                return Err(
                                    "Gemini output reused an already owned file identity".into()
                                );
                            }
                            self.files.insert(file.clone(), Some(owner));
                            if let Some(output) = GeminiOutputRetention::from_metadata(id, &value) {
                                self.gemini_outputs.insert(file.clone(), output);
                            }
                        }
                        _ => {
                            return Err(
                                "cleanup metadata contained an invalid Gemini output file".into()
                            );
                        }
                    }
                }
            }
            Service::Anthropic | Service::Ollama => {}
        }
        self.jobs
            .get_mut(id)
            .ok_or("batch artifact discovery lost its owned job")?
            .artifacts_captured()
            .map_err(CaseFailure::from)
    }

    async fn partial_openai_inputs(&mut self) -> Result<(), CaseFailure> {
        if !matches!(self.live.service(), Service::OpenAi)
            || !self
                .publications
                .iter()
                .any(InlinePublication::needs_input_discovery)
        {
            return Ok(());
        }
        let mut request = FileListRequest::default().with_limit(PAGE_SIZE);
        let mut cursors = BTreeSet::new();
        let mut inspection_errors = Vec::new();
        for _ in 0..20 {
            let page = self.live.provider().list_files(request).await?;
            for file in page.files {
                let native_name = format!("batch-owned-{}.jsonl", self.nonce);
                if self.files.contains_key(&file.id)
                    || !matches!(file.file_name.as_deref(), Some(name) if name == "batch-input.jsonl" || name == native_name)
                {
                    continue;
                }
                let bytes = match self
                    .live
                    .provider()
                    .download_file(file.id.clone(), MAX_RESULT_BYTES)
                    .await
                {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        inspection_errors.push(
                            CaseFailure::from(error.with_request_target(file.id))
                                .context("unresolved input candidate could not be inspected"),
                        );
                        continue;
                    }
                };
                let matching = self
                    .publications
                    .iter()
                    .enumerate()
                    .filter_map(|(owner, publication)| {
                        publication.owns_input(&bytes).then_some(owner)
                    })
                    .collect::<Vec<_>>();
                let [owner] = matching.as_slice() else {
                    continue;
                };
                self.files.insert(file.id.clone(), Some(*owner));
                if self.publications[*owner].needs_input_discovery() {
                    self.publications[*owner].discover_input(&file.id, &bytes)?;
                }
            }
            let Some(cursor) = page.next_cursor else {
                let unresolved = self
                    .publications
                    .iter()
                    .enumerate()
                    .filter_map(|(owner, publication)| {
                        publication.needs_input_discovery().then_some(owner)
                    })
                    .collect::<Vec<_>>();
                if unresolved.is_empty() {
                    return Ok(());
                }
                inspection_errors.push(CaseFailure::from(format!("partial OpenAI input ownership remains unresolved for submissions {unresolved:?}; case nonce {}", self.nonce)));
                return CaseFailure::collected(
                    "partial-upload ownership remains unproven",
                    inspection_errors,
                );
            };
            if cursor.trim().is_empty() || !cursors.insert(cursor.clone()) {
                return Err("partial-upload inventory repeated or omitted a cursor".into());
            }
            request = FileListRequest::default()
                .with_limit(PAGE_SIZE)
                .with_cursor(cursor)?;
        }
        Err("partial-upload cleanup exceeded bounded file inventory".into())
    }

    async fn cleanup(&mut self) {
        for (id, state) in self.jobs.clone() {
            if state.deletion_uncertain() {
                continue;
            }
            if state.needs_status() {
                let status = self.live.provider().batch_status(id.clone()).await;
                let terminal = match status {
                    Ok(job) if job.id == id => {
                        if matches!(job.status, BatchStatus::Queued | BatchStatus::Running) {
                            None
                        } else {
                            Some(job)
                        }
                    }
                    Ok(_) => {
                        self.cleanup_errors.push(
                            CaseFailure::from("cleanup status changed batch identity")
                                .with_owner(id.clone()),
                        );
                        continue;
                    }
                    Err(error) => {
                        self.cleanup_errors.push(
                            CaseFailure::from(error.with_request_target(id.clone()))
                                .context("cleanup status")
                                .with_owner(id.clone()),
                        );
                        None
                    }
                };
                let terminal = match terminal {
                    Some(job) => job,
                    None => {
                        if let Err(error) = self.live.provider().cancel_batch(id.clone()).await {
                            self.cleanup_errors.push(
                                CaseFailure::from(error.with_request_target(id.clone()))
                                    .context("cleanup cancellation")
                                    .with_owner(id.clone()),
                            );
                        }
                        match self.terminal(&id, self.cancellation_timeout()).await {
                            Ok(job) => job,
                            Err(error) => {
                                self.cleanup_errors.push(error.with_owner(id.clone()));
                                continue;
                            }
                        }
                    }
                };
                if let Err(error) = self.record_terminal(&terminal, &id) {
                    self.cleanup_errors.push(error.with_owner(id.clone()));
                    continue;
                }
            }
            if self
                .jobs
                .get(&id)
                .is_some_and(BatchCleanup::needs_artifacts)
                && let Err(error) = self.batch_artifacts(&id).await
            {
                self.cleanup_errors.push(error.with_owner(id.clone()));
            }
        }
        if let Err(error) = self.partial_openai_inputs().await {
            self.cleanup_errors.push(error);
        }
        for (id, owner) in self.files.clone() {
            let can_delete = owner.is_none_or(|owner| {
                let publication = &self.publications[owner];
                let terminal = publication.job_id().is_some_and(|job| {
                    self.jobs
                        .get(job)
                        .is_some_and(|state| !state.needs_status())
                });
                publication.can_delete_input(terminal)
            });
            if !can_delete {
                self.cleanup_errors.push(
                    CaseFailure::from(format!(
                        "owned file retained for uncertain or unsettled submission {owner:?}"
                    ))
                    .with_owner(id),
                );
                continue;
            }
            match self.live.provider().delete_file(id.clone()).await {
                Ok(()) => {
                    self.files.remove(&id);
                }
                Err(error) => {
                    let error = error.with_request_target(id.clone());
                    let result = match self.gemini_outputs.get_mut(&id) {
                        Some(output) => output.defer(error),
                        None => Err(error),
                    };
                    if let Err(error) = result {
                        self.cleanup_errors.push(
                            CaseFailure::from(error)
                                .context("delete exact owned fixture")
                                .with_owner(id),
                        );
                    }
                }
            }
        }
    }

    fn write_ownership(&self, accepted_submission: Option<usize>) -> std::io::Result<()> {
        let retained_outputs = self
            .gemini_outputs
            .values()
            .filter(|output| output.is_deferred())
            .map(GeminiOutputRetention::evidence)
            .collect::<Vec<_>>();
        let directory = match std::env::var_os("PROVIDERS_KIT_EVIDENCE_DIR") {
            Some(directory) => std::path::PathBuf::from(directory),
            None if accepted_submission.is_none()
                && self.jobs.is_empty()
                && retained_outputs.is_empty()
                && self.file_observations.is_empty() =>
            {
                return Ok(());
            }
            None => {
                let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../target/providers-kit-retained-outputs")
                    .join(uuid::Uuid::new_v4().to_string());
                let mut builder = std::fs::DirBuilder::new();
                builder.recursive(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::DirBuilderExt;
                    builder.mode(0o700);
                }
                builder.create(&directory)?;
                directory
            }
        };
        let thread = std::thread::current();
        let case = thread.name().unwrap_or("unnamed_case").replace(':', "_");
        let path = directory.join(match accepted_submission {
            Some(owner) => format!("{case}.{}.submission-{owner}.ownership.json", self.nonce),
            None => format!("{case}.ownership.json"),
        });
        let jobs = self
            .jobs
            .iter()
            .map(|(id, state)| {
                json!({
                    "id": id,
                    "submission": self.job_publications.get(id),
                    "cleanup_state": format!("{state:?}"),
                    "status_pending": state.needs_status(),
                    "artifact_discovery_pending": state.needs_artifacts(),
                    "deletion_uncertain": state.deletion_uncertain(),
                })
            })
            .collect::<Vec<_>>();
        let residual_files = self
            .files
            .iter()
            .map(|(id, owner)| json!({
                "id": id, "submission": owner,
                "cleanup_state": if self.gemini_outputs.get(id).is_some_and(GeminiOutputRetention::is_deferred) {
                    "awaiting_provider_expiration"
                } else { "unresolved" },
            }))
            .collect::<Vec<_>>();
        let submissions = self
            .publications
            .iter()
            .enumerate()
            .map(|(owner, publication)| {
                json!({
                    "submission": owner,
                    "job_id": publication.job_id(),
                    "input_id": publication.input_id(),
                    "publication_uncertain": publication.publication_uncertain(),
                    "input_discovery_pending": publication.needs_input_discovery(),
                })
            })
            .collect::<Vec<_>>();
        let evidence = json!({
            "case": case,
            "provider": format!("{:?}", self.live.service()),
            "snapshot_stage": match accepted_submission {
                Some(_) => "accepted_before_completion_wait",
                None => "after_batch_cleanup_before_provider_shutdown",
            },
            "jobs": jobs,
            "residual_files": residual_files,
            "retained_outputs": retained_outputs,
            "submissions": submissions,
            "file_observations": self.file_observations,
        });
        CaseFailure::write_required_observation(
            &path,
            evidence,
            &LiveProvider::evidence_secrets(),
        )?;
        if accepted_submission.is_some() {
            eprintln!(
                "batch accepted; private ownership checkpoint: {}",
                path.display()
            );
        }
        if !retained_outputs.is_empty() {
            eprintln!(
                "Gemini generated-output cleanup deferred under the retention exception; {} output(s) remain undeleted, expiration unverified; ownership evidence: {}",
                retained_outputs.len(),
                path.display()
            );
        }
        Ok(())
    }

    async fn finish(mut self, primary: Result<(), CaseFailure>) -> Result<(), CaseFailure> {
        let deadline = self.cancellation_timeout() + Duration::from_secs(180);
        if tokio::time::timeout(deadline, self.cleanup())
            .await
            .is_err()
        {
            self.cleanup_errors
                .push("aggregate batch cleanup deadline expired".into());
        }
        for (id, state) in &self.jobs {
            let message = if state.needs_status() {
                Some("owned batch remains unsettled")
            } else if state.needs_artifacts() {
                Some("owned terminal batch artifact discovery remains incomplete")
            } else if state.deletion_uncertain() {
                Some(
                    "native deletion of the owned terminal job remains uncertain; it will not be repeated",
                )
            } else {
                None
            };
            if let Some(message) = message {
                self.cleanup_errors
                    .push(CaseFailure::from(message).with_owner(id.clone()));
            }
        }
        for (id, owner) in &self.files {
            if self
                .gemini_outputs
                .get(id)
                .is_some_and(GeminiOutputRetention::is_deferred)
            {
                continue;
            }
            self.cleanup_errors.push(
                CaseFailure::from(format!(
                    "exact owned file remains in cleanup inventory for submission {owner:?}"
                ))
                .with_owner(id.clone()),
            );
        }
        for (owner, publication) in self.publications.iter().enumerate() {
            if publication.publication_uncertain() || publication.needs_input_discovery() {
                let failure = CaseFailure::from(format!(
                    "submission {owner} remains unresolved: publication_uncertain={}, input_discovery_pending={}, case nonce={}",
                    publication.publication_uncertain(),
                    publication.needs_input_discovery(),
                    self.nonce
                ));
                self.cleanup_errors.push(match publication.input_id() {
                    Some(id) => failure.with_owner(id.to_owned()),
                    None => failure,
                });
            }
        }
        if let Err(error) = self.write_ownership(None) {
            self.cleanup_errors
                .push(format!("private batch ownership export failed: {:?}", error.kind()).into());
        }
        CaseFailure::combine(
            primary,
            CaseFailure::collected("batch cleanup failures", self.cleanup_errors),
        )
    }
}

#[tokio::test]
async fn openai_inline_batch_completes() {
    BatchCase::Inline.run(Service::OpenAi).await;
}
#[tokio::test]
async fn anthropic_inline_batch_completes() {
    BatchCase::Inline.run(Service::Anthropic).await;
}
#[tokio::test]
async fn gemini_inline_batch_completes() {
    BatchCase::Inline.run(Service::Gemini).await;
}
#[tokio::test]
async fn openai_provider_file_batch_completes() {
    BatchCase::ProviderFile.run(Service::OpenAi).await;
}
#[tokio::test]
async fn gemini_provider_file_batch_completes() {
    BatchCase::ProviderFile.run(Service::Gemini).await;
}
#[tokio::test]
async fn openai_batch_preserves_mixed_row_outcomes() {
    BatchCase::Mixed.run(Service::OpenAi).await;
}
#[tokio::test]
async fn anthropic_batch_preserves_mixed_row_outcomes() {
    BatchCase::Mixed.run(Service::Anthropic).await;
}
#[tokio::test]
async fn gemini_batch_preserves_mixed_row_outcomes() {
    BatchCase::Mixed.run(Service::Gemini).await;
}
#[tokio::test]
async fn openai_batch_cancellation_has_effect() {
    BatchCase::Cancellation.run(Service::OpenAi).await;
}
#[tokio::test]
async fn anthropic_batch_cancellation_has_effect() {
    BatchCase::Cancellation.run(Service::Anthropic).await;
}
#[tokio::test]
async fn gemini_batch_cancellation_has_effect() {
    BatchCase::Cancellation.run(Service::Gemini).await;
}
#[tokio::test]
async fn openai_batch_missing_resources_fail() {
    BatchCase::Missing.run(Service::OpenAi).await;
}
#[tokio::test]
async fn anthropic_batch_missing_resources_fail() {
    BatchCase::Missing.run(Service::Anthropic).await;
}
#[tokio::test]
async fn gemini_batch_missing_resources_fail() {
    BatchCase::Missing.run(Service::Gemini).await;
}
#[tokio::test]
async fn openai_batch_result_limit_is_enforced() {
    BatchCase::Bounded.run(Service::OpenAi).await;
}
#[tokio::test]
async fn anthropic_batch_result_limit_is_enforced() {
    BatchCase::Bounded.run(Service::Anthropic).await;
}
#[tokio::test]
async fn gemini_batch_result_limit_is_enforced() {
    BatchCase::Bounded.run(Service::Gemini).await;
}
#[tokio::test]
async fn anthropic_provider_file_batch_is_unsupported() {
    BatchCase::UnsupportedFile.run(Service::Anthropic).await;
}
#[tokio::test]
async fn ollama_batch_operations_are_unsupported() {
    BatchCase::UnsupportedAll.run(Service::Ollama).await;
}
