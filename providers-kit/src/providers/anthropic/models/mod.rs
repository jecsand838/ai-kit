mod catalog;

use crate::KitError;
use crate::batch::{BatchJob, BatchSubmitRequest};
use crate::generation::{
    GenerateEventStream, GenerateRequest, GenerateResponse, GenerationContinuation,
};
use crate::models::{ModelBackend, ModelId};

use super::batches::render_batch_submission;
use super::client::AnthropicClient;
use super::messages::{ClaudeThinkingProfile, build_message_body, normalize_generate_response};

pub(crate) use catalog::{list_models, model_info};

/// One exact Anthropic model child.
#[derive(Clone)]
pub(crate) struct AnthropicModel {
    client: AnthropicClient,
    id: ModelId,
}

impl AnthropicModel {
    pub(crate) fn new(client: AnthropicClient, id: ModelId) -> Self {
        Self { client, id }
    }

    fn thinking(&self) -> ClaudeThinkingProfile {
        ClaudeThinkingProfile::for_model(self.id.as_str())
    }
}

impl ModelBackend for AnthropicModel {
    type Continuation = GenerationContinuation;
    type Session = std::convert::Infallible;

    fn model_id(&self) -> &ModelId {
        &self.id
    }

    async fn generate(&self, request: &GenerateRequest) -> Result<GenerateResponse, KitError> {
        let view = request.as_view();
        let body = build_message_body(self.id.as_str(), self.thinking(), view, false)?;
        let raw = self.client.messages_create(body).await?;
        raw.normalize(|raw| normalize_generate_response(Some(&self.id), &raw))
    }

    async fn generate_stream(
        &self,
        request: &GenerateRequest,
    ) -> Result<GenerateEventStream, KitError> {
        let view = request.as_view();
        let body = build_message_body(self.id.as_str(), self.thinking(), view, true)?;
        self.client.messages_stream(self.id.clone(), body).await
    }

    async fn submit_batch(
        &self,
        request: &BatchSubmitRequest<GenerationContinuation>,
    ) -> Result<BatchJob, KitError> {
        let body = render_batch_submission(self.id.as_str(), self.thinking(), request)?;
        self.client.batch_submit(body).await
    }
}
