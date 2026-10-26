mod catalog;

use crate::SdkError;
use crate::embedding::{EmbedRequest, EmbedResponse};
use crate::generation::{
    GenerateEventStream, GenerateRequest, GenerateResponse, GenerationContinuation,
};
use crate::models::{ModelBackend, ModelId};

use std::convert::Infallible;

use super::chat::{build_chat_body, normalize_chat_response};
use super::client::OllamaClient;
use super::native::thinking::OllamaThinkingProfile;

pub(crate) use catalog::{list_models, model_info};

/// One exact Ollama model child.
#[derive(Clone)]
pub(crate) struct OllamaModel {
    client: OllamaClient,
    id: ModelId,
}

impl OllamaModel {
    pub(crate) fn new(client: OllamaClient, id: ModelId) -> Self {
        Self { client, id }
    }

    fn thinking(&self) -> OllamaThinkingProfile {
        match self.id.as_str() {
            "gpt-oss:20b" => OllamaThinkingProfile::GptOssLevel,
            "qwen3.5:9b-q4_K_M" | "gemma4:e4b-it-qat" => OllamaThinkingProfile::Boolean,
            _ => OllamaThinkingProfile::Unavailable,
        }
    }
}

impl ModelBackend for OllamaModel {
    type Continuation = GenerationContinuation;
    type Session = Infallible;

    fn model_id(&self) -> &ModelId {
        &self.id
    }

    async fn generate(&self, request: &GenerateRequest) -> Result<GenerateResponse, SdkError> {
        self.client
            .api()
            .validate_response_format(request.as_view().response_format)?;
        let built = build_chat_body(&self.id, self.thinking(), request.as_view(), false)?;
        let raw = self.client.chat(built.body).await?;
        raw.normalize(|raw| normalize_chat_response(&raw, built.model_id))
    }

    async fn generate_stream(
        &self,
        request: &GenerateRequest,
    ) -> Result<GenerateEventStream, SdkError> {
        self.client
            .api()
            .validate_response_format(request.as_view().response_format)?;
        let built = build_chat_body(&self.id, self.thinking(), request.as_view(), true)?;
        self.client.chat_stream(built.body).await
    }

    async fn embed(&self, request: &EmbedRequest) -> Result<EmbedResponse, SdkError> {
        let body = Self::build_embed_body(self.id.as_str(), request)?;
        let raw = self.client.embed(body).await?;
        raw.normalize(|raw| Self::normalize_embed_response(&raw, request))
    }
}
