use std::io::Write;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use providers_kit::batch::{
    BatchJob, BatchListRequest, BatchPage, BatchResult, BatchSubmitRequest,
};
use providers_kit::config::ProviderKind;
use providers_kit::embedding::{EmbedRequest, EmbedResponse};
use providers_kit::error::ErrorKind;
use providers_kit::files::{FileListRequest, FileObject, FilePage, FileUploadRequest};
use providers_kit::generation::{GenerateEventStream, GenerateRequest, GenerateResponse};
use providers_kit::models::{ModelListRequest, ModelPage, ModelSummary};
use providers_kit::runtime::HealthStatus;
use providers_kit::session::SessionConfig;
use providers_kit::{KitError, Model, ModelBackend, ModelId, Provider, ProviderBackend};

use crate::caller_shell::CallerShellObserver;
use crate::custom_provider_evidence::{AnthropicEvidence, CloudPhase};

#[derive(Clone, Copy, Debug)]
pub(crate) enum ScenarioStage {
    Setup,
    PlainGeneration,
    FunctionContinuation,
    ParallelTools,
    CallerCancellation,
    Shell,
    Approval,
    RuntimeLoss,
    Streaming,
}

#[derive(Clone, Copy)]
pub(crate) struct ConfiguredProvider {
    kind: &'static str,
    request_timeout_ms: u64,
    connect_timeout_ms: u64,
}

impl ConfiguredProvider {
    pub(crate) fn new(
        kind: &ProviderKind,
        request_timeout: Duration,
        connect_timeout: Duration,
    ) -> Self {
        let kind = match kind {
            ProviderKind::OpenAi(_) => "openai",
            ProviderKind::Anthropic(_) => "anthropic",
            ProviderKind::Gemini(_) => "gemini",
            ProviderKind::Ollama(_) => "ollama",
        };
        Self {
            kind,
            request_timeout_ms: request_timeout.as_millis().min(u128::from(u64::MAX)) as u64,
            connect_timeout_ms: connect_timeout.as_millis().min(u128::from(u64::MAX)) as u64,
        }
    }
}

pub(crate) struct GenerationDiagnostics {
    stage: Mutex<ScenarioStage>,
    ordinal: AtomicUsize,
    shell: Arc<CallerShellObserver>,
}

impl GenerationDiagnostics {
    pub(crate) fn new(shell: Arc<CallerShellObserver>) -> Self {
        Self {
            stage: Mutex::new(ScenarioStage::Setup),
            ordinal: AtomicUsize::new(0),
            shell,
        }
    }

    pub(crate) fn enter(&self, stage: ScenarioStage) {
        *self.stage.lock().unwrap_or_else(|error| error.into_inner()) = stage;
    }

    pub(crate) fn begin(
        self: &Arc<Self>,
        model: &ModelId,
        configured: ConfiguredProvider,
    ) -> GenerationProbe {
        GenerationProbe {
            diagnostics: Arc::clone(self),
            stage: *self.stage.lock().unwrap_or_else(|error| error.into_inner()),
            ordinal: self.ordinal.fetch_add(1, Ordering::AcqRel) + 1,
            model: model.clone(),
            configured,
            started: Instant::now(),
        }
    }
}

pub(crate) struct GenerationProbe {
    diagnostics: Arc<GenerationDiagnostics>,
    stage: ScenarioStage,
    ordinal: usize,
    model: ModelId,
    configured: ConfiguredProvider,
    started: Instant,
}

impl GenerationProbe {
    pub(crate) fn observe_stream(
        self,
        stream: GenerateEventStream<<Model as ModelBackend>::Continuation>,
    ) -> GenerateEventStream<<Model as ModelBackend>::Continuation> {
        GenerateEventStream::new(stream.inspect(move |item| {
            if let Err(error) = item {
                self.report(error);
            }
        }))
    }

    pub(crate) fn failure(&self, error: &KitError) -> serde_json::Value {
        let mut nodes = Vec::new();
        let mut next = Some(error);
        while let Some(error) = next.filter(|_| nodes.len() < 8) {
            nodes.push(NativeFailure::project(error));
            next = error.cause();
        }
        let model = self.model.as_str();
        let model = if model.len() <= 128
            && model.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'/' | b':')
            }) {
            model
        } else {
            "<redacted-model>"
        };
        serde_json::json!({
            "version": 1,
            "stage": format!("{:?}", self.stage),
            "model": model,
            "provider_kind": self.configured.kind,
            "configured_request_timeout_ms": self.configured.request_timeout_ms,
            "configured_connect_timeout_ms": self.configured.connect_timeout_ms,
            "generation_ordinal": self.ordinal,
            "elapsed_ms": self.started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
            "shell": self.diagnostics.shell.snapshot(),
            "cause_chain": nodes,
            "cause_chain_truncated": next.is_some(),
        })
    }

    fn report(&self, error: &KitError) {
        // Serialize only the fixed projection, never a native error or payload.
        if let Ok(encoded) = serde_json::to_vec(&self.failure(error))
            && encoded.len() <= 8 * 1024
        {
            let mut stderr = std::io::stderr().lock();
            let _ = stderr.write_all(b"AGENTS_KIT_CUSTOM_PROVIDER_FAILURE=");
            let _ = stderr.write_all(&encoded);
            let _ = stderr.write_all(b"\n");
        }
    }
}

struct NativeFailure;

impl NativeFailure {
    fn project(error: &KitError) -> serde_json::Value {
        let native = error.native_details();
        let message = native.message.as_deref().unwrap_or("");
        let mut limit = message.len().min(2048);
        while !message.is_char_boundary(limit) {
            limit -= 1;
        }
        let inspected = message[..limit].to_ascii_lowercase();
        let indicators = [
            (
                "dns",
                ["dns error", "dns resolution", "failed to lookup address"]
                    .iter()
                    .any(|needle| inspected.contains(needle)),
            ),
            (
                "tls",
                ["tls", "certificate", "ssl"]
                    .iter()
                    .any(|needle| inspected.contains(needle)),
            ),
            (
                "connection_refused",
                inspected.contains("connection refused"),
            ),
            ("connection_reset", inspected.contains("connection reset")),
            (
                "connection_closed",
                [
                    "connection closed",
                    "connection aborted",
                    "unexpected eof",
                    "broken pipe",
                ]
                .iter()
                .any(|needle| inspected.contains(needle)),
            ),
            ("proxy", inspected.contains("proxy")),
            (
                "http2",
                inspected.contains("http2") || inspected.contains("http/2"),
            ),
            (
                "timeout",
                inspected.contains("timed out") || inspected.contains("timeout"),
            ),
        ];
        let kind = match error.kind() {
            ErrorKind::Configuration => "configuration",
            ErrorKind::InvalidInput => "invalid_input",
            ErrorKind::Unsupported => "unsupported",
            ErrorKind::Transport => "transport",
            ErrorKind::Timeout => "timeout",
            ErrorKind::DeadlineExceeded => "deadline_exceeded",
            ErrorKind::Closed => "closed",
            ErrorKind::InvalidState => "invalid_state",
            ErrorKind::InvalidResponse(_) => "invalid_response",
            ErrorKind::InvalidRequest => "invalid_request",
            ErrorKind::Authentication => "authentication",
            ErrorKind::PermissionDenied => "permission_denied",
            ErrorKind::NotFound => "not_found",
            ErrorKind::Conflict => "conflict",
            ErrorKind::RequestTooLarge => "request_too_large",
            ErrorKind::FailedPrecondition => "failed_precondition",
            ErrorKind::ResourceExhausted => "resource_exhausted",
            ErrorKind::RateLimited => "rate_limited",
            ErrorKind::QuotaExceeded => "quota_exceeded",
            ErrorKind::Billing => "billing",
            ErrorKind::ServiceUnavailable => "service_unavailable",
            ErrorKind::Internal => "internal",
            ErrorKind::Cancelled => "cancelled",
            ErrorKind::ProviderRejected => "provider_rejected",
        };
        serde_json::json!({
            "kind": kind,
            "operation": format!("{:?}", error.context().operation),
            "phase": format!("{:?}", error.context().phase),
            "http_status": error.http_status(),
            "timeout_ms": error.timeout().map(|timeout| timeout.as_millis().min(u128::from(u64::MAX)) as u64),
            "reported_http_status": native.reported_http_status,
            "rpc_code": native.rpc_code,
            "native_message_indicators": indicators.into_iter().filter_map(|(name, present)| present.then_some(name)).collect::<Vec<_>>(),
            "message_inspection_truncated": limit != message.len(),
            "native_message_present": native.message.is_some(),
            "native_body_present": native.body.is_some(),
            "native_url_present": native.url.is_some(),
            "native_truncated": native.truncated,
            "diagnostics_truncated": error.diagnostics_truncated(),
            "supplementary_count": error.supplementary().len(),
        })
    }
}

#[derive(Clone)]
enum ProviderObservation {
    Scenario {
        diagnostics: Arc<GenerationDiagnostics>,
        configured: ConfiguredProvider,
    },
    Anthropic {
        evidence: Arc<Mutex<AnthropicEvidence>>,
        phase: CloudPhase,
    },
}

pub(crate) struct ObservedProvider {
    inner: Provider,
    observation: ProviderObservation,
}

impl ObservedProvider {
    pub(crate) fn new(
        inner: Provider,
        diagnostics: Arc<GenerationDiagnostics>,
        configured: ConfiguredProvider,
    ) -> Self {
        Self {
            inner,
            observation: ProviderObservation::Scenario {
                diagnostics,
                configured,
            },
        }
    }

    pub(crate) fn anthropic(inner: Provider, evidence: Arc<Mutex<AnthropicEvidence>>) -> Self {
        let phase = evidence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .provider_phase();
        Self {
            inner,
            observation: ProviderObservation::Anthropic { evidence, phase },
        }
    }
}

impl ProviderBackend for ObservedProvider {
    type Model = ObservedModel;

    fn model(&self, id: ModelId) -> Result<Self::Model, KitError> {
        self.inner.model(id).map(|inner| ObservedModel {
            inner,
            observation: self.observation.clone(),
        })
    }

    async fn list_models(&self, request: ModelListRequest) -> Result<ModelPage, KitError> {
        self.inner.list_models(request).await
    }

    async fn model_info(&self, id: ModelId) -> Result<ModelSummary, KitError> {
        self.inner.model_info(id).await
    }

    async fn upload_file(&self, request: FileUploadRequest) -> Result<FileObject, KitError> {
        self.inner.upload_file(request).await
    }

    async fn list_files(&self, request: FileListRequest) -> Result<FilePage, KitError> {
        self.inner.list_files(request).await
    }

    async fn file_info(&self, id: String) -> Result<FileObject, KitError> {
        self.inner.file_info(id).await
    }

    async fn delete_file(&self, id: String) -> Result<(), KitError> {
        self.inner.delete_file(id).await
    }

    async fn download_file(
        &self,
        id: String,
        max_bytes: NonZeroUsize,
    ) -> Result<Vec<u8>, KitError> {
        self.inner.download_file(id, max_bytes).await
    }

    async fn batch_status(&self, id: String) -> Result<BatchJob, KitError> {
        self.inner.batch_status(id).await
    }

    async fn cancel_batch(&self, id: String) -> Result<BatchJob, KitError> {
        self.inner.cancel_batch(id).await
    }

    async fn batch_results(
        &self,
        id: String,
        max_bytes: NonZeroUsize,
    ) -> Result<Vec<BatchResult<<Model as ModelBackend>::Continuation>>, KitError> {
        self.inner.batch_results(id, max_bytes).await
    }

    async fn list_batches(&self, request: BatchListRequest) -> Result<BatchPage, KitError> {
        self.inner.list_batches(request).await
    }

    async fn health(&self) -> Result<HealthStatus, KitError> {
        self.inner.health().await
    }

    async fn shutdown(&self) -> Result<(), KitError> {
        let result = self.inner.shutdown().await;
        if let ProviderObservation::Anthropic { evidence, phase } = &self.observation {
            evidence
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .shutdown(*phase, &result);
        }
        result
    }
}

pub(crate) struct ObservedModel {
    inner: Model,
    observation: ProviderObservation,
}

impl ModelBackend for ObservedModel {
    type Continuation = <Model as ModelBackend>::Continuation;
    type Session = <Model as ModelBackend>::Session;

    fn model_id(&self) -> &ModelId {
        self.inner.model_id()
    }

    async fn generate(
        &self,
        request: &GenerateRequest<Self::Continuation>,
    ) -> Result<GenerateResponse<Self::Continuation>, KitError> {
        match &self.observation {
            ProviderObservation::Scenario {
                diagnostics,
                configured,
            } => {
                let probe = diagnostics.begin(self.model_id(), *configured);
                let result = self.inner.generate(request).await;
                if let Err(error) = &result {
                    probe.report(error);
                }
                result
            }
            ProviderObservation::Anthropic { evidence, phase } => {
                let generation = evidence
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .begin(*phase, self.model_id(), request, false);
                let result = self.inner.generate(request).await;
                let mut evidence = evidence
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                match &result {
                    Ok(response) => evidence.completion(generation, response),
                    Err(error) => evidence.acquisition_error(generation, error),
                }
                result
            }
        }
    }

    async fn generate_stream(
        &self,
        request: &GenerateRequest<Self::Continuation>,
    ) -> Result<GenerateEventStream<Self::Continuation>, KitError> {
        match &self.observation {
            ProviderObservation::Scenario {
                diagnostics,
                configured,
            } => {
                let probe = diagnostics.begin(self.model_id(), *configured);
                match self.inner.generate_stream(request).await {
                    Ok(stream) => Ok(probe.observe_stream(stream)),
                    Err(error) => {
                        probe.report(&error);
                        Err(error)
                    }
                }
            }
            ProviderObservation::Anthropic { evidence, phase } => {
                let generation = evidence
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .begin(*phase, self.model_id(), request, true);
                match self.inner.generate_stream(request).await {
                    Ok(stream) => Ok(AnthropicEvidence::inspect_stream(
                        evidence, generation, stream,
                    )),
                    Err(error) => {
                        evidence
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .acquisition_error(generation, &error);
                        Err(error)
                    }
                }
            }
        }
    }

    async fn embed(&self, request: &EmbedRequest) -> Result<EmbedResponse, KitError> {
        self.inner.embed(request).await
    }

    async fn submit_batch(
        &self,
        request: &BatchSubmitRequest<Self::Continuation>,
    ) -> Result<BatchJob, KitError> {
        self.inner.submit_batch(request).await
    }

    async fn open_session(&self, config: SessionConfig) -> Result<Self::Session, KitError> {
        self.inner.open_session(config).await
    }
}
