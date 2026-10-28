use std::sync::Arc;

use serde_json::Value;

use crate::KitError;
use crate::generation::GenerateEventStream;
use crate::transport::HttpResponse;

use super::api::OllamaApiContext;

#[derive(Clone)]
pub(crate) struct OllamaClient {
    api: Arc<OllamaApiContext>,
}

impl OllamaClient {
    pub(crate) fn new(api: Arc<OllamaApiContext>) -> Self {
        Self { api }
    }

    pub(crate) fn api(&self) -> &OllamaApiContext {
        self.api.as_ref()
    }

    pub(crate) async fn chat(&self, body: Value) -> Result<HttpResponse<Value>, KitError> {
        super::chat::create_raw_chat(self.api(), body).await
    }

    pub(crate) async fn chat_stream(&self, body: Value) -> Result<GenerateEventStream, KitError> {
        super::streaming::start_generate_stream(self.api.as_ref().clone(), body).await
    }

    pub(crate) async fn embed(&self, body: Value) -> Result<HttpResponse<Value>, KitError> {
        self.create_raw_embed(body).await
    }
}
