use std::time::Instant;

use reqwest::Method;

use crate::KitError;
use crate::config::OllamaConfig;
use crate::config::TransportConfig;
use crate::error::{ErrorPhase, ResponseViolation};
use crate::models::ModelId;
use crate::models::{ModelListRequest, ModelPage, ModelSummary};
use crate::provider::ProviderBackend;
use crate::runtime::HealthStatus;
use crate::transport::CheckedRequestSpec;

use super::api::{OLLAMA_REQUESTS, OllamaApiContext};
use super::client::OllamaClient;
use super::models::OllamaModel;
use super::wire::OllamaVersionResponse;

#[derive(Clone)]
pub(crate) struct OllamaBackend {
    client: OllamaClient,
}

impl OllamaBackend {
    pub(crate) fn from_config(
        config: OllamaConfig,
        transport: TransportConfig,
    ) -> Result<Self, KitError> {
        Ok(Self {
            client: OllamaClient::new(OllamaApiContext::shared(config, transport)?),
        })
    }
}

impl ProviderBackend for OllamaBackend {
    type Model = OllamaModel;

    fn model(&self, id: ModelId) -> Result<OllamaModel, KitError> {
        Ok(OllamaModel::new(self.client.clone(), id))
    }

    async fn list_models(&self, request: ModelListRequest) -> Result<ModelPage, KitError> {
        super::models::list_models(self.client.api(), request).await
    }

    async fn model_info(&self, id: ModelId) -> Result<ModelSummary, KitError> {
        super::models::model_info(self.client.api(), id).await
    }

    async fn health(&self) -> Result<HealthStatus, KitError> {
        let api = self.client.api();
        let start = Instant::now();
        let version = OLLAMA_REQUESTS
            .json::<OllamaVersionResponse>(
                "version",
                api.request(Method::GET, "version")?,
                api.transport().execution(),
            )
            .await?;
        version.normalize(|version| {
            if version.version.trim().is_empty() {
                return Err(KitError::invalid_response(ResponseViolation::InvalidField {
                    field: "version",
                })
                .at_phase(ErrorPhase::Normalize)
                .with_message("Ollama health response contained an empty version".to_string())
                .with_diagnostic(CheckedRequestSpec::diagnostic_json(&version)));
            }
            Ok(HealthStatus {
                latency: start.elapsed(),
            })
        })
    }

    async fn shutdown(&self) -> Result<(), KitError> {
        Ok(())
    }
}
