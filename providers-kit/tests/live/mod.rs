use crate::live_failures::CaseFailure;
mod batches;
mod current_models;
mod generation;
mod hosted;
mod ollama_settings;
mod resources;
mod stores;

use std::env;
use std::num::NonZeroU32;
use std::sync::Mutex;
use std::time::Duration;

use futures_util::StreamExt;
use providers_kit::batch::BatchResult;
use providers_kit::config::{
    AnthropicConfig, ApiKey, GeminiConfig, GeminiServiceTier, OllamaConfig, OpenAiConfig,
    ProviderKind, RetryPolicy, TransportConfig,
};
use providers_kit::generation::{
    GenerateEvent, GenerateRequest, GenerateResponse, GenerationConfig, Message,
};
use providers_kit::models::{ModelBackend, ModelId};
use providers_kit::{KitError, Model, Provider, ProviderBackend, ProviderConfig};
use url::Url;
use uuid::Uuid;

use crate::hosted_assertions::{FileSearchContract, HostedProvider};
use crate::live_assertions::{
    FileSearchCapture, FileSearchStage, GenerationEvidence, NegativeStreamEvidence,
    SessionTurnEvidence, StreamEvidence,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Service {
    OpenAi,
    Anthropic,
    Gemini,
    Ollama,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Mode {
    Unary,
    Streaming,
}

enum StreamCompletion {
    RequireSuccessfulGeneration,
    RequireFunctionCall,
    InspectHostedResponse { capture: Option<FileSearchCapture> },
}

pub(super) struct LiveProvider {
    provider: Provider,
    service: Service,
    default_model: String,
    evidence_failures: Mutex<Vec<CaseFailure>>,
    session_observation: Option<(serde_json::Value, Vec<SessionTurnEvidence>)>,
}

impl LiveProvider {
    pub(super) fn new(service: Service) -> Result<Self, CaseFailure> {
        let kind = match service {
            Service::OpenAi => ProviderKind::OpenAi(
                OpenAiConfig::new(Self::key("TEST_OPENAI_API_KEY")?)
                    .with_base_url(Self::url("TEST_OPENAI_BASE_URL")?),
            ),
            Service::Anthropic => ProviderKind::Anthropic(
                AnthropicConfig::new(Self::key("TEST_ANTHROPIC_API_KEY")?)
                    .with_base_url(Self::url("TEST_ANTHROPIC_BASE_URL")?),
            ),
            Service::Gemini => {
                let tier = match Self::required("TEST_GEMINI_SERVICE_TIER")?.as_str() {
                    "standard" => GeminiServiceTier::Standard,
                    "flex" => GeminiServiceTier::Flex,
                    "priority" => GeminiServiceTier::Priority,
                    _ => return Err("invalid TEST_GEMINI_SERVICE_TIER".to_owned().into()),
                };
                ProviderKind::Gemini(
                    GeminiConfig::new(Self::key("TEST_GEMINI_API_KEY")?)
                        .with_base_url(Self::url("TEST_GEMINI_BASE_URL")?)
                        .with_service_tier(tier),
                )
            }
            Service::Ollama => ProviderKind::Ollama(
                OllamaConfig::local()
                    .with_base_url(Self::url("TEST_OLLAMA_BASE_URL")?)
                    .map_err(Self::error)?,
            ),
        };
        let model_variable = match service {
            Service::OpenAi => "TEST_OPENAI_MODEL_DEFAULT",
            Service::Anthropic => "TEST_ANTHROPIC_MODEL_DEFAULT",
            Service::Gemini => "TEST_GEMINI_MODEL_DEFAULT",
            Service::Ollama => "TEST_OLLAMA_MODEL_DEFAULT",
        };
        // Read all required construction inputs before creating a lifecycle owner.
        let default_model = Self::required(model_variable)?;
        let transport = TransportConfig::new(
            Duration::from_secs(180),
            Duration::from_secs(30),
            Duration::from_secs(90),
            RetryPolicy::default(),
        )
        .map_err(Self::error)?;
        let provider =
            Provider::from_config(ProviderConfig::new(kind, transport)).map_err(Self::error)?;
        Ok(Self {
            provider,
            service,
            default_model,
            evidence_failures: Mutex::new(Vec::new()),
            session_observation: None,
        })
    }

    pub(super) fn provider(&self) -> &Provider {
        &self.provider
    }
    pub(super) fn service(&self) -> Service {
        self.service
    }
    pub(super) fn default_model(&self) -> &str {
        &self.default_model
    }

    pub(super) fn model(&self) -> Result<Model, CaseFailure> {
        self.selected(&self.default_model)
    }

    fn selected(&self, id: &str) -> Result<Model, CaseFailure> {
        let model = self
            .provider
            .model(ModelId::new(id).map_err(Self::error)?)
            .map_err(Self::error)?;
        if model.model_id().as_str() != id {
            return Err("selected model changed the caller's exact model ID"
                .to_owned()
                .into());
        }
        Ok(model)
    }

    pub(super) fn nonce() -> String {
        format!("strict-{}", Uuid::new_v4().simple())
    }

    pub(super) fn required(name: &str) -> Result<String, CaseFailure> {
        env::var(name)
            .ok()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                CaseFailure::from(format!("missing required strict-live variable: {name}"))
            })
    }

    fn key(name: &str) -> Result<ApiKey, CaseFailure> {
        ApiKey::new(Self::required(name)?).map_err(Self::error)
    }

    fn url(name: &str) -> Result<Url, CaseFailure> {
        Url::parse(&Self::required(name)?)
            .map_err(|_| CaseFailure::from(format!("invalid URL in {name}")))
    }

    pub(super) fn text_request(text: &str) -> Result<GenerateRequest, CaseFailure> {
        GenerateRequest::new(vec![Message::user_text(text).map_err(Self::error)?])
            .map_err(Self::error)?
            .with_generation(Self::generation_config())
            .map_err(Self::error)
    }

    fn generation_config() -> GenerationConfig {
        GenerationConfig::default()
            .with_max_output_tokens(NonZeroU32::new(4096).expect("nonzero fixed output budget"))
    }

    pub(super) async fn generate(
        &self,
        request: &GenerateRequest,
        mode: Mode,
    ) -> Result<GenerateResponse, CaseFailure> {
        let model = self.model()?;
        self.generate_on(&model, request, mode).await
    }

    pub(super) async fn generate_hosted(
        &self,
        request: &GenerateRequest,
        mode: Mode,
    ) -> Result<GenerateResponse, CaseFailure> {
        let model = self.model()?;
        self.generate_with_completion(
            &model,
            request,
            mode,
            StreamCompletion::InspectHostedResponse { capture: None },
        )
        .await
    }

    pub(super) async fn generate_file_search(
        &self,
        request: &GenerateRequest,
        mode: Mode,
        stage: FileSearchStage,
    ) -> Result<GenerateResponse, CaseFailure> {
        if self.service != Service::Gemini || !matches!(mode, Mode::Streaming) {
            return self.generate_hosted(request, mode).await;
        }
        let contract = FileSearchContract::for_request(
            HostedProvider::Gemini,
            providers_kit::error::Operation::GenerateStream,
            self.default_model(),
        );
        if contract != FileSearchContract::GeminiStreaming {
            return self.generate_hosted(request, mode).await;
        }
        let directory = CaseFailure::required_observation_directory(
            env::var_os("PROVIDERS_KIT_EVIDENCE_DIR").map(std::path::PathBuf::from),
        )?;
        let model = self.model()?;
        self.generate_with_completion(
            &model,
            request,
            mode,
            StreamCompletion::InspectHostedResponse {
                capture: Some(FileSearchCapture {
                    contract,
                    selected_model: self.default_model.clone(),
                    stage,
                    directory,
                    secrets: Self::evidence_secrets(),
                }),
            },
        )
        .await
    }

    async fn generate_on(
        &self,
        model: &Model,
        request: &GenerateRequest,
        mode: Mode,
    ) -> Result<GenerateResponse, CaseFailure> {
        self.generate_with_completion(
            model,
            request,
            mode,
            StreamCompletion::RequireSuccessfulGeneration,
        )
        .await
    }

    async fn generate_with_completion(
        &self,
        model: &Model,
        request: &GenerateRequest,
        mode: Mode,
        mut completion: StreamCompletion,
    ) -> Result<GenerateResponse, CaseFailure> {
        // Retain an in-stream error until EOF, including across the deadline.
        let mut evidence = StreamEvidence::new();
        if let StreamCompletion::InspectHostedResponse { capture } = &mut completion
            && let Some(capture) = capture.take()
        {
            evidence = evidence.with_file_search_capture(capture);
        }
        let outcome = tokio::time::timeout(Duration::from_secs(180), async {
            match mode {
                Mode::Unary => {
                    let response = model.generate(request).await.map_err(Self::error)?;
                    self.capture_response("generation-unary", &response);
                    Ok(Some(response))
                }
                Mode::Streaming => {
                    let mut stream = model.generate_stream(request).await.map_err(Self::error)?;
                    while let Some(item) = stream.next().await {
                        if let Ok(GenerateEvent::Completed(response)) = &item {
                            self.capture_response("generation-stream-completed", response);
                        }
                        evidence.observe_result(item)?;
                    }
                    Ok(None)
                }
            }
        })
        .await;
        // This runs before every operation outcome, including validation errors
        // and timeouts. Final case shutdown reports capture failures alongside
        // the primary failure and owned-resource cleanup.
        self.evidence_failures
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend(evidence.take_export_failures());
        let response =
            match outcome {
                Ok(Ok(Some(response))) => Ok(response),
                Ok(Ok(None)) => match completion {
                    StreamCompletion::RequireSuccessfulGeneration => evidence.finish(),
                    StreamCompletion::RequireFunctionCall => evidence.finish_function_call(),
                    StreamCompletion::InspectHostedResponse { .. } => evidence.finish_observation(),
                },
                Ok(Err(failure)) => Err(failure),
                Err(_) => Err(evidence
                    .interrupted("generation/stream exceeded 180 seconds before terminal EOF")),
            }?;
        GenerationEvidence::selected_model(model.model_id(), &response)?;
        Ok(response)
    }

    pub(super) async fn rejected(
        &self,
        model: &Model,
        request: &GenerateRequest,
        mode: Mode,
    ) -> Result<KitError, CaseFailure> {
        // The owner outlives the timed future, preserving a received native error
        // even when the service never terminates its accepted stream.
        let mut evidence = NegativeStreamEvidence::new();
        let outcome = tokio::time::timeout(Duration::from_secs(180), async {
            match mode {
                Mode::Unary => match model.generate(request).await {
                    Err(error) => Ok(Some(error)),
                    Ok(response) => {
                        self.capture_response("negative-unary-accepted", &response);
                        Err(CaseFailure::from(
                            "negative generation unexpectedly succeeded",
                        ))
                    }
                },
                Mode::Streaming => match model.generate_stream(request).await {
                    Err(error) => Ok(Some(error)),
                    Ok(mut stream) => {
                        while let Some(item) = stream.next().await {
                            if let Ok(GenerateEvent::Completed(response)) = &item {
                                self.capture_response("negative-stream-completed", response);
                            }
                            evidence.observe(item)?;
                        }
                        Ok(None)
                    }
                },
            }
        })
        .await;
        match outcome {
            Ok(Ok(Some(error))) => Ok(error),
            Ok(Ok(None)) => evidence.finish(),
            Ok(Err(failure)) => Err(failure),
            Err(_) => Err(evidence
                .interrupted("negative generation exceeded 180 seconds before terminal EOF")),
        }
    }

    pub(super) fn error(error: KitError) -> CaseFailure {
        // Kit formatting retains typed operation/phase/violation evidence and redacts
        // all native strings. Explicit native details are never copied into this log.
        CaseFailure::from(error)
    }

    fn capture_response(&self, stage: &'static str, response: &GenerateResponse) {
        let Some(directory) = env::var_os("PROVIDERS_KIT_EVIDENCE_DIR") else {
            return;
        };
        self.capture_observation(directory, stage, CaseFailure::response_evidence(response));
    }

    fn write_file_search_observation(
        &self,
        contract: FileSearchContract,
        response: &GenerateResponse,
        assessment: &Result<(), CaseFailure>,
    ) -> Result<(), CaseFailure> {
        let mut observation = contract.observation(response);
        let exclusion = observation["strict_file_search_only_requirement_met"].clone();
        observation["selected_model"] = serde_json::json!(self.default_model());
        observation["stage"] = serde_json::json!("retrieval");
        observation["retrieval_passed"] = serde_json::json!(assessment.is_ok());
        observation["failure"] =
            serde_json::json!(assessment.as_ref().err().map(CaseFailure::evidence));
        let path = Self::write_required_observation("file-search", observation)?;
        eprintln!(
            "Gemini File Search streaming: retrieval_passed={}; File Search-only exclusion={exclusion}; no code execution claim; evidence={}",
            assessment.is_ok(),
            path.display()
        );
        Ok(())
    }

    fn write_required_observation(
        stage: &'static str,
        observation: serde_json::Value,
    ) -> Result<std::path::PathBuf, CaseFailure> {
        let directory = CaseFailure::required_observation_directory(
            env::var_os("PROVIDERS_KIT_EVIDENCE_DIR").map(std::path::PathBuf::from),
        )?;
        let thread = std::thread::current();
        let case = thread.name().unwrap_or("unnamed_case").replace(':', "_");
        let path = directory.join(format!("{case}.{}.{stage}.json", Self::nonce()));
        CaseFailure::write_required_observation(&path, observation, &Self::evidence_secrets())
            .map_err(|error| {
                CaseFailure::from(format!(
                    "required {stage} observation failed: {:?}",
                    error.kind()
                ))
            })?;
        Ok(path)
    }

    fn capture_batch(&self, job_id: &str, rows: &[BatchResult]) {
        let Some(directory) = env::var_os("PROVIDERS_KIT_EVIDENCE_DIR") else {
            return;
        };
        self.capture_observation(
            directory,
            "batch-results",
            CaseFailure::batch_evidence(job_id, rows),
        );
    }

    fn capture_observation(
        &self,
        directory: std::ffi::OsString,
        stage: &'static str,
        observation: serde_json::Value,
    ) {
        let thread = std::thread::current();
        let case = thread.name().unwrap_or("unnamed_case").replace(':', "_");
        let path = std::path::PathBuf::from(directory).join(format!(
            "{case}.{stage}.{}.observation.json",
            Uuid::new_v4()
        ));
        if let Err(error) =
            CaseFailure::write_observation(&path, observation, &Self::evidence_secrets())
        {
            // Capture cannot replace a later assertion or Kit error. Keep it
            // until normal cleanup/shutdown completes, then report both.
            self.evidence_failures
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(format!("private observation export failed: {:?}", error.kind()).into());
        }
    }

    fn evidence_secrets() -> Vec<String> {
        [
            "TEST_OPENAI_API_KEY",
            "TEST_ANTHROPIC_API_KEY",
            "TEST_GEMINI_API_KEY",
        ]
        .iter()
        .filter_map(|name| env::var(name).ok())
        .collect()
    }

    fn export_session(
        mut observation: serde_json::Value,
        turns: Vec<SessionTurnEvidence>,
    ) -> Result<(), CaseFailure> {
        let directory = CaseFailure::required_observation_directory(
            env::var_os("PROVIDERS_KIT_EVIDENCE_DIR").map(std::path::PathBuf::from),
        )?;
        let thread = std::thread::current();
        let case = thread.name().unwrap_or("unnamed_case").replace(':', "_");
        let prefix = format!("{case}.{}", Self::nonce());
        let mut records = Vec::new();
        let mut failures = Vec::new();
        for (index, turn) in turns.into_iter().enumerate() {
            let (record, result) = turn.export(&directory, &format!("{prefix}.turn-{index}"));
            records.push(record);
            if let Err(error) = result {
                failures.push(error);
            }
        }
        observation["turns"] = serde_json::json!(records);
        let path = directory.join(format!("{prefix}.interactive-session.json"));
        match CaseFailure::write_required_observation(&path, observation, &Self::evidence_secrets())
        {
            Ok(()) => eprintln!("Interactive session evidence={}", path.display()),
            Err(error) => failures
                .push(format!("required session observation failed: {:?}", error.kind()).into()),
        }
        CaseFailure::collected("session evidence export failures", failures)
    }

    pub(super) async fn finish(mut self, result: Result<(), CaseFailure>) {
        let cleanup = tokio::time::timeout(Duration::from_secs(180), self.provider.shutdown())
            .await
            .map_err(|_| CaseFailure::from("provider shutdown exceeded 180 seconds"))
            .and_then(|result| result.map_err(Self::error));
        let mut grade = Ok(None);
        let export = match self.session_observation.take() {
            Some((mut observation, turns)) => {
                observation["provider_shutdown"] = serde_json::json!({
                    "succeeded":cleanup.is_ok(), "failure":cleanup.as_ref().err().map(CaseFailure::evidence)
                });
                if result.is_ok() && cleanup.is_ok() {
                    grade = (|| {
                        let thread = std::thread::current();
                        let case = thread.name().ok_or_else(|| {
                            CaseFailure::from("Realtime grade case identity missing")
                        })?;
                        let model = observation["selected_model"].as_str().ok_or_else(|| {
                            CaseFailure::from("Realtime grade model identity missing")
                        })?;
                        let receipt = SessionTurnEvidence::realtime_grade_receipt(
                            case,
                            model,
                            &Self::required("TEST_OPENAI_MODEL_INTERACTIVE")?,
                            &turns,
                        )
                        .map_err(CaseFailure::from)?;
                        let line = serde_json::to_string(&receipt).map_err(|_| {
                            CaseFailure::from("Realtime grade serialization failed")
                        })?;
                        if line.len() > 4096 {
                            return Err(CaseFailure::from(
                                "Realtime grade receipt exceeded 4096 bytes",
                            ));
                        }
                        Ok(Some(line))
                    })();
                }
                Self::export_session(observation, turns)
            }
            None => Ok(()),
        };
        let evidence = CaseFailure::collected(
            "private observation export failures",
            self.evidence_failures
                .into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        let (grade_line, grade_result) = match grade {
            Ok(line) => (line, Ok(())),
            Err(error) => (None, Err(error)),
        };
        if let Err(mut failure) = CaseFailure::combine(
            CaseFailure::combine(
                CaseFailure::combine(CaseFailure::combine(result, cleanup), export),
                evidence,
            ),
            grade_result,
        ) {
            if let Some(directory) = env::var_os("PROVIDERS_KIT_EVIDENCE_DIR") {
                let thread = std::thread::current();
                let case = thread.name().unwrap_or("unnamed_case").replace(':', "_");
                let path = std::path::PathBuf::from(directory).join(format!("{case}.json"));
                let secrets = Self::evidence_secrets();
                if let Err(error) = failure.write_evidence(&path, &secrets) {
                    failure = CaseFailure::Group {
                        label: "case and evidence export failures",
                        failures: vec![
                            failure,
                            format!("private evidence export failed: {:?}", error.kind()).into(),
                        ],
                    };
                }
            }
            panic!("{:?}: {failure}", self.service);
        }
        if let Some(line) = grade_line {
            eprintln!("\nPROVIDERS_KIT_REALTIME_GRADE={line}");
        }
    }
}
