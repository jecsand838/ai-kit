use std::sync::Arc;

use serde_json::Value;

use crate::SdkError;
use crate::batch::BatchJob;
use crate::generation::GenerateEventStream;
use crate::models::ModelId;
use crate::transport::HttpResponse;

use super::api::GeminiApiContext;
use super::embeddings::{GeminiEmbedRequest, GeminiEmbedResponse};

#[derive(Clone)]
pub(crate) struct GeminiClient {
    api: Arc<GeminiApiContext>,
}

impl GeminiClient {
    pub(crate) fn new(api: Arc<GeminiApiContext>) -> Self {
        Self { api }
    }

    pub(crate) fn api(&self) -> &GeminiApiContext {
        self.api.as_ref()
    }

    pub(crate) async fn generate_content(
        &self,
        model: &str,
        body: Value,
    ) -> Result<HttpResponse<Value>, SdkError> {
        super::generate_content::create_raw_generate_content(self.api(), model, body).await
    }

    pub(crate) async fn stream_generate_content(
        &self,
        model: &ModelId,
        body: Value,
    ) -> Result<GenerateEventStream, SdkError> {
        super::streaming::start_generate_stream(self.api.as_ref().clone(), model.clone(), body)
            .await
    }

    pub(crate) async fn embed_content(
        &self,
        request: GeminiEmbedRequest,
    ) -> Result<GeminiEmbedResponse, SdkError> {
        super::embeddings::send_embed_request(self.api(), request).await
    }

    pub(crate) async fn batch_generate_content(
        &self,
        path: String,
        body: Value,
    ) -> Result<BatchJob, SdkError> {
        super::batches::submit_rendered_batch(self.api(), path, body).await
    }
}
