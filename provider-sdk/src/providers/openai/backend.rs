use std::num::NonZeroUsize;

use reqwest::Method;

use crate::SdkError;
use crate::batch::{BatchJob, BatchListRequest, BatchPage, BatchResult};
use crate::config::OpenAiConfig;
use crate::config::TransportConfig;
use crate::error::{ErrorPhase, ResponseViolation};
use crate::files::{FileListRequest, FileObject, FilePage, FileUploadRequest};
use crate::generation::GenerationContinuation;
use crate::models::ModelId;
use crate::models::{ModelListRequest, ModelPage, ModelSummary};
use crate::provider::ProviderBackend;
use crate::runtime::HealthStatus;
use crate::transport::CheckedRequestSpec;

use super::api::{OPENAI_REQUESTS, OpenAiApiContext};
use super::client::OpenAiClient;
use super::models::OpenAiModel;

#[derive(Clone)]
pub(crate) struct OpenAiBackend {
    client: OpenAiClient,
}

impl OpenAiBackend {
    pub(crate) fn from_config(
        config: OpenAiConfig,
        transport: TransportConfig,
    ) -> Result<Self, SdkError> {
        Ok(Self {
            client: OpenAiClient::new(OpenAiApiContext::shared(config, transport)?),
        })
    }
}

impl ProviderBackend for OpenAiBackend {
    type Model = OpenAiModel;

    fn model(&self, id: ModelId) -> Result<OpenAiModel, SdkError> {
        Ok(OpenAiModel::new(self.client.clone(), id))
    }

    async fn list_models(&self, request: ModelListRequest) -> Result<ModelPage, SdkError> {
        super::models::list_models(self.client.api(), request).await
    }

    async fn model_info(&self, id: ModelId) -> Result<ModelSummary, SdkError> {
        super::models::model_info(self.client.api(), id).await
    }

    async fn upload_file(&self, request: FileUploadRequest) -> Result<FileObject, SdkError> {
        super::files::upload_file(self.client.api(), request).await
    }

    async fn list_files(&self, request: FileListRequest) -> Result<FilePage, SdkError> {
        super::files::list_files(self.client.api(), request).await
    }

    async fn file_info(&self, id: String) -> Result<FileObject, SdkError> {
        super::files::file_info(self.client.api(), id).await
    }

    async fn delete_file(&self, id: String) -> Result<(), SdkError> {
        super::files::delete_file(self.client.api(), id).await
    }

    async fn download_file(
        &self,
        id: String,
        max_bytes: NonZeroUsize,
    ) -> Result<Vec<u8>, SdkError> {
        super::files::download_file(self.client.api(), id, max_bytes).await
    }

    async fn batch_status(&self, id: String) -> Result<BatchJob, SdkError> {
        let api = self.client.api();
        let batch = api
            .fetch_batch(&id, api.transport().retryable_execution())
            .await?;
        batch.normalize(|batch| {
            if batch.id != id {
                return Err(
                    SdkError::invalid_response(ResponseViolation::IdentityMismatch { field: "id" })
                        .at_phase(ErrorPhase::Normalize)
                        .with_message(
                            "OpenAI batch lookup returned a different batch id".to_string(),
                        )
                        .with_diagnostic(CheckedRequestSpec::diagnostic_json(&batch)),
                );
            }
            batch.into_job()
        })
    }

    async fn cancel_batch(&self, id: String) -> Result<BatchJob, SdkError> {
        super::batches::cancel_batch(self.client.api(), id).await
    }

    async fn batch_results(
        &self,
        id: String,
        max_bytes: NonZeroUsize,
    ) -> Result<Vec<BatchResult<GenerationContinuation>>, SdkError> {
        super::batches::batch_results(self.client.api(), id, max_bytes).await
    }

    async fn list_batches(&self, request: BatchListRequest) -> Result<BatchPage, SdkError> {
        super::batches::list_batches(self.client.api(), request).await
    }

    async fn health(&self) -> Result<HealthStatus, SdkError> {
        let api = self.client.api();
        HealthStatus::from_probe(OPENAI_REQUESTS.json::<serde_json::Value>(
            "models",
            api.request(Method::GET, "models")?,
            api.transport().retryable_execution(),
        ))
        .await
    }

    async fn shutdown(&self) -> Result<(), SdkError> {
        Ok(())
    }
}
