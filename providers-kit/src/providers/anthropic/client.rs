use std::sync::Arc;

use serde_json::Value;

use crate::KitError;
use crate::batch::BatchJob;
use crate::generation::GenerateEventStream;
use crate::models::ModelId;
use crate::transport::HttpResponse;

use super::api::AnthropicApiContext;

#[derive(Clone)]
pub(crate) struct AnthropicClient {
    api: Arc<AnthropicApiContext>,
}

impl AnthropicClient {
    pub(crate) fn new(api: Arc<AnthropicApiContext>) -> Self {
        Self { api }
    }

    pub(crate) fn api(&self) -> &AnthropicApiContext {
        self.api.as_ref()
    }

    pub(crate) async fn messages_create(
        &self,
        body: Value,
    ) -> Result<HttpResponse<Value>, KitError> {
        super::messages::create_raw_message(self.api(), body).await
    }

    pub(crate) async fn messages_stream(
        &self,
        selected_model: ModelId,
        body: Value,
    ) -> Result<GenerateEventStream, KitError> {
        super::streaming::start_generate_stream(self.api.as_ref().clone(), selected_model, body)
            .await
    }

    pub(crate) async fn batch_submit(&self, body: Value) -> Result<BatchJob, KitError> {
        super::batches::submit_rendered_batch(self.api(), body).await
    }
}
