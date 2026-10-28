mod catalog;

use crate::KitError;
use crate::batch::{BatchJob, BatchSubmitRequest};
use crate::core::HostedToolMode;
use crate::embedding::{EmbedRequest, EmbedResponse};
use crate::generation::{
    GenerateEventStream, GenerateRequest, GenerateResponse, GenerationContinuation,
};
use crate::models::{ModelBackend, ModelId};

use super::batches::render_batch_submission;
use super::client::GeminiClient;
use super::embeddings::{build_embed_request, normalize_embed_response};
use super::generate_content::{build_generate_body, normalize_generate_response};
use super::reasoning::GeminiThinkingProfile;

pub(crate) use catalog::{list_models, model_info};

/// One exact Gemini model child.
#[derive(Clone)]
pub(crate) struct GeminiModel {
    client: GeminiClient,
    id: ModelId,
}

impl GeminiModel {
    pub(crate) fn new(client: GeminiClient, id: ModelId) -> Result<Self, KitError> {
        client
            .api()
            .exact_model_id("Gemini model selection", id.as_str())?;
        Ok(Self { client, id })
    }

    fn thinking(&self) -> GeminiThinkingProfile {
        GeminiThinkingProfile::for_model(self.id.as_str())
    }
}

impl ModelBackend for GeminiModel {
    type Continuation = GenerationContinuation;
    type Session = std::convert::Infallible;

    fn model_id(&self) -> &ModelId {
        &self.id
    }

    async fn generate(&self, request: &GenerateRequest) -> Result<GenerateResponse, KitError> {
        let body = build_generate_body(
            self.client.api(),
            &self.id,
            self.thinking(),
            request.as_view(),
            HostedToolMode::Generate,
        )?;
        let raw = self.client.generate_content(self.id.as_str(), body).await?;
        raw.normalize(|raw| normalize_generate_response(&self.id, raw))
    }

    async fn generate_stream(
        &self,
        request: &GenerateRequest,
    ) -> Result<GenerateEventStream, KitError> {
        let body = build_generate_body(
            self.client.api(),
            &self.id,
            self.thinking(),
            request.as_view(),
            HostedToolMode::GenerateStream,
        )?;
        self.client.stream_generate_content(&self.id, body).await
    }

    async fn embed(&self, request: &EmbedRequest) -> Result<EmbedResponse, KitError> {
        let request = build_embed_request(self.id.as_str(), request)?;
        let response = self.client.embed_content(request).await?;
        normalize_embed_response(response)
    }

    async fn submit_batch(
        &self,
        request: &BatchSubmitRequest<GenerationContinuation>,
    ) -> Result<BatchJob, KitError> {
        let (path, body) =
            render_batch_submission(self.client.api(), &self.id, self.thinking(), request)?;
        self.client.batch_generate_content(path, body).await
    }
}
