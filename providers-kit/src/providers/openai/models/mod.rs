mod catalog;

use crate::KitError;
use crate::batch::{BatchJob, BatchSubmitRequest};
use crate::embedding::{EmbedRequest, EmbedResponse};
use crate::error::ValidationViolation;
use crate::generation::{
    GenerateEventStream, GenerateRequest, GenerateResponse, GenerationContinuation,
};
use crate::models::{ModelBackend, ModelId};
use crate::session::SessionConfig;

use super::batch_submission::OpenAiBatchSubmission;
use super::batches::render_batch_submission;
use super::client::OpenAiClient;
use super::embeddings::{build_embeddings_body, normalize_embeddings_response};
use super::responses::normalize_generate_response;
use super::session::OpenAiSession;

pub(crate) use catalog::{list_models, model_info};

/// One exact OpenAI model child.
#[derive(Clone)]
pub(crate) struct OpenAiModel {
    client: OpenAiClient,
    id: ModelId,
}

impl OpenAiModel {
    pub(crate) fn new(client: OpenAiClient, id: ModelId) -> Self {
        Self { client, id }
    }

    pub(super) fn api(&self) -> &super::api::OpenAiApiContext {
        self.client.api()
    }

    pub(in crate::providers::openai) fn validate_stateless_model(
        model: &ModelId,
    ) -> Result<(), KitError> {
        if matches!(
            model.as_str(),
            "gpt-audio-1.5"
                | "gpt-audio-mini"
                | "gpt-audio-mini-2025-12-15"
                | "gpt-4o-audio-preview"
                | "gpt-4o-audio-preview-2025-06-03"
        ) {
            return Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "model",
                    rule: "stateless audio is not supplied by this Kit",
                },
                "OpenAI stateless audio generation and batches are currently unsupported by this Kit",
            ));
        }
        Ok(())
    }

    pub(crate) fn prepare_owned_batch(
        &self,
        request: &BatchSubmitRequest<GenerationContinuation>,
    ) -> Result<OpenAiBatchSubmission, KitError> {
        OpenAiBatchSubmission::new(self.api().clone(), &self.id, request)
    }
}

impl ModelBackend for OpenAiModel {
    type Continuation = GenerationContinuation;
    type Session = OpenAiSession;

    fn model_id(&self) -> &ModelId {
        &self.id
    }

    async fn generate(&self, request: &GenerateRequest) -> Result<GenerateResponse, KitError> {
        let body = Self::build_response_body(&self.id, request.as_view(), false)?;
        let raw = self.client.responses_create(body).await?;
        raw.normalize(|raw| normalize_generate_response(Some(&self.id), &raw))
    }

    async fn generate_stream(
        &self,
        request: &GenerateRequest,
    ) -> Result<GenerateEventStream, KitError> {
        let body = Self::build_response_body(&self.id, request.as_view(), true)?;
        self.client.responses_stream(self.id.clone(), body).await
    }

    async fn embed(&self, request: &EmbedRequest) -> Result<EmbedResponse, KitError> {
        let (body, input_count) = build_embeddings_body(self.id.as_str(), request)?;
        let raw = self.client.embeddings_create(body).await?;
        raw.normalize(|raw| normalize_embeddings_response(raw, input_count))
    }

    async fn submit_batch(
        &self,
        request: &BatchSubmitRequest<GenerationContinuation>,
    ) -> Result<BatchJob, KitError> {
        let plan = render_batch_submission(&self.id, request)?;
        self.client.batch_submit(plan).await
    }

    async fn open_session(&self, config: SessionConfig) -> Result<OpenAiSession, KitError> {
        OpenAiSession::open(self, config).await
    }
}
