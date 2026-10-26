use std::sync::Arc;

use serde_json::Value;

use crate::SdkError;
use crate::batch::BatchJob;
use crate::generation::GenerateEventStream;
use crate::models::ModelId;
use crate::transport::HttpResponse;

use super::api::OpenAiApiContext;
use super::batches::OpenAiBatchSubmitPlan;
use super::wire::OpenAiEmbeddingsResponse;

#[derive(Clone)]
pub(crate) struct OpenAiClient {
    api: Arc<OpenAiApiContext>,
}

impl OpenAiClient {
    pub(crate) fn new(api: Arc<OpenAiApiContext>) -> Self {
        Self { api }
    }

    pub(crate) fn api(&self) -> &OpenAiApiContext {
        self.api.as_ref()
    }

    pub(crate) async fn responses_create(
        &self,
        body: Value,
    ) -> Result<HttpResponse<Value>, SdkError> {
        super::responses::create_raw_response(self.api(), body).await
    }

    pub(crate) async fn responses_stream(
        &self,
        selected_model: ModelId,
        body: Value,
    ) -> Result<GenerateEventStream, SdkError> {
        super::streaming::start_generate_stream(self.api.as_ref().clone(), selected_model, body)
            .await
    }

    pub(crate) async fn embeddings_create(
        &self,
        body: Value,
    ) -> Result<HttpResponse<OpenAiEmbeddingsResponse>, SdkError> {
        super::embeddings::create_raw_embeddings(self.api(), body).await
    }

    pub(crate) async fn batch_submit(
        &self,
        plan: OpenAiBatchSubmitPlan,
    ) -> Result<BatchJob, SdkError> {
        super::batches::submit_rendered_batch(self.api(), plan).await
    }
}
