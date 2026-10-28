use std::sync::Arc;

use reqwest::Method;

use super::wire::OllamaErrorEnvelope;
use crate::KitError;
use crate::config::OllamaConfig;
use crate::config::TransportConfig;
use crate::error::ValidationViolation;
use crate::generation::ResponseFormat;
use crate::transport::{AuthScheme, CheckedRequestSpec, ProviderHttpContext};

#[derive(Clone)]
pub(crate) struct OllamaApiContext {
    transport: ProviderHttpContext,
}

pub(super) const OLLAMA_REQUESTS: CheckedRequestSpec =
    CheckedRequestSpec::new(OllamaErrorEnvelope::parse);

impl OllamaApiContext {
    pub(crate) fn validate_response_format(
        &self,
        format: Option<&ResponseFormat>,
    ) -> Result<(), KitError> {
        let url = self.transport.url("chat")?;
        if url.scheme() == "https"
            && url.host_str() == Some("ollama.com")
            && url.port_or_known_default() == Some(443)
            && matches!(
                format,
                Some(ResponseFormat::JsonObject | ResponseFormat::JsonSchema(_))
            )
        {
            return Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "response_format",
                    rule: "structured output unavailable on Ollama Cloud",
                },
                "Ollama Cloud does not support structured output",
            ));
        }
        Ok(())
    }

    pub(crate) fn shared(
        config: OllamaConfig,
        transport: TransportConfig,
    ) -> Result<Arc<Self>, KitError> {
        let auth = match config.api_key() {
            Some(key) => AuthScheme::Bearer(key.clone()),
            None => AuthScheme::None,
        };

        let transport = ProviderHttpContext::new(config.base_url().clone(), auth, transport)?;
        Ok(Arc::new(Self { transport }))
    }

    pub(crate) fn transport(&self) -> &ProviderHttpContext {
        &self.transport
    }

    pub(crate) fn request(
        &self,
        method: Method,
        path: &str,
    ) -> Result<reqwest::RequestBuilder, KitError> {
        self.transport.request(method, path)
    }
}
