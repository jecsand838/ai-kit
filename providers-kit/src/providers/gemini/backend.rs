use std::num::NonZeroUsize;

use reqwest::Method;

use crate::KitError;
use crate::batch::{BatchJob, BatchListRequest, BatchPage, BatchResult};
use crate::config::GeminiConfig;
use crate::config::TransportConfig;
use crate::files::{FileListRequest, FileObject, FilePage, FileUploadRequest};
use crate::generation::GenerationContinuation;
use crate::models::ModelId;
use crate::models::{ModelListRequest, ModelPage, ModelSummary};
use crate::provider::ProviderBackend;
use crate::runtime::HealthStatus;

use super::api::{GEMINI_REQUESTS, GeminiApiContext};
use super::client::GeminiClient;
use super::models::GeminiModel;

#[derive(Clone)]
pub(crate) struct GeminiBackend {
    client: GeminiClient,
}

impl GeminiBackend {
    pub(crate) fn from_config(
        config: GeminiConfig,
        transport: TransportConfig,
    ) -> Result<Self, KitError> {
        Ok(Self {
            client: GeminiClient::new(GeminiApiContext::shared(config, transport)?),
        })
    }
}

impl ProviderBackend for GeminiBackend {
    type Model = GeminiModel;

    fn model(&self, id: ModelId) -> Result<GeminiModel, KitError> {
        GeminiModel::new(self.client.clone(), id)
    }

    async fn list_models(&self, request: ModelListRequest) -> Result<ModelPage, KitError> {
        super::models::list_models(self.client.api(), request).await
    }

    async fn model_info(&self, id: ModelId) -> Result<ModelSummary, KitError> {
        super::models::model_info(self.client.api(), id).await
    }

    async fn upload_file(&self, request: FileUploadRequest) -> Result<FileObject, KitError> {
        super::files::upload_file(self.client.api(), request).await
    }

    async fn list_files(&self, request: FileListRequest) -> Result<FilePage, KitError> {
        super::files::list_files(self.client.api(), request).await
    }

    async fn file_info(&self, id: String) -> Result<FileObject, KitError> {
        super::files::file_info(self.client.api(), id).await
    }

    async fn delete_file(&self, id: String) -> Result<(), KitError> {
        super::files::delete_file(self.client.api(), id).await
    }

    async fn download_file(
        &self,
        id: String,
        max_bytes: NonZeroUsize,
    ) -> Result<Vec<u8>, KitError> {
        super::files::download_file(self.client.api(), id, max_bytes).await
    }

    async fn batch_status(&self, id: String) -> Result<BatchJob, KitError> {
        super::batches::batch_status(self.client.api(), id).await
    }

    async fn cancel_batch(&self, id: String) -> Result<BatchJob, KitError> {
        super::batches::cancel_batch(self.client.api(), id).await
    }

    async fn batch_results(
        &self,
        id: String,
        max_bytes: NonZeroUsize,
    ) -> Result<Vec<BatchResult<GenerationContinuation>>, KitError> {
        super::batches::batch_results(self.client.api(), id, max_bytes).await
    }

    async fn list_batches(&self, request: BatchListRequest) -> Result<BatchPage, KitError> {
        super::batches::list_batches(self.client.api(), request).await
    }

    async fn health(&self) -> Result<HealthStatus, KitError> {
        let api = self.client.api();
        HealthStatus::from_probe(GEMINI_REQUESTS.json::<serde_json::Value>(
            "v1beta/models",
            api.request(Method::GET, "v1beta/models")?,
            api.transport().retryable_execution(),
        ))
        .await
    }

    async fn shutdown(&self) -> Result<(), KitError> {
        Ok(())
    }
}
