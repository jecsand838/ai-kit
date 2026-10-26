use std::sync::Arc;

use reqwest::Method;
use serde_json::Value;

use super::wire::OpenAiErrorEnvelope;
use crate::SdkError;
use crate::config::OpenAiConfig;
use crate::config::TransportConfig;
use crate::error::ValidationViolation;
use crate::transport::{AuthMode, AuthScheme, CheckedRequestSpec, ProviderHttpContext};

#[derive(Clone)]
pub(crate) struct OpenAiApiContext {
    config: OpenAiConfig,
    transport: ProviderHttpContext,
}

pub(super) const OPENAI_REQUESTS: CheckedRequestSpec =
    CheckedRequestSpec::new(OpenAiErrorEnvelope::parse);

impl OpenAiApiContext {
    pub(crate) fn shared(
        config: OpenAiConfig,
        transport: TransportConfig,
    ) -> Result<Arc<Self>, SdkError> {
        let transport = ProviderHttpContext::new(
            config.base_url().clone(),
            AuthScheme::Bearer(config.api_key().clone()),
            transport,
        )?;

        Ok(Arc::new(Self { config, transport }))
    }

    pub(crate) fn transport(&self) -> &ProviderHttpContext {
        &self.transport
    }

    pub(crate) fn request(
        &self,
        method: Method,
        path: &str,
    ) -> Result<reqwest::RequestBuilder, SdkError> {
        Ok(self.apply_scope_headers(self.transport.request(method, path)?))
    }

    pub(crate) fn live_request(&self, model: &str) -> Result<http::Request<()>, SdkError> {
        let mut request = self
            .transport
            .websocket_request("realtime", AuthMode::Apply, |url| {
                url.query_pairs_mut().append_pair("model", model);
            })?;
        self.insert_scope_headers(request.headers_mut())?;
        Ok(request)
    }

    fn apply_scope_headers(&self, mut builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(organization) = self.config.organization() {
            builder = builder.header("OpenAI-Organization", organization);
        }
        if let Some(project) = self.config.project() {
            builder = builder.header("OpenAI-Project", project);
        }
        builder
    }

    fn insert_scope_headers(&self, headers: &mut http::HeaderMap) -> Result<(), SdkError> {
        if let Some(organization) = self.config.organization() {
            headers.insert(
                http::HeaderName::from_static("openai-organization"),
                http::HeaderValue::from_str(organization).map_err(|err| {
                    SdkError::config(
                        ValidationViolation::InvalidFormat {
                            field: "credentials.organization",
                            rule: "valid HTTP header value",
                        },
                        format!("invalid OpenAI organization header: {err}"),
                    )
                })?,
            );
        }
        if let Some(project) = self.config.project() {
            headers.insert(
                http::HeaderName::from_static("openai-project"),
                http::HeaderValue::from_str(project).map_err(|err| {
                    SdkError::config(
                        ValidationViolation::InvalidFormat {
                            field: "credentials.project",
                            rule: "valid HTTP header value",
                        },
                        format!("invalid OpenAI project header: {err}"),
                    )
                })?,
            );
        }
        Ok(())
    }
}

pub(crate) fn insert_non_null(
    target: &mut serde_json::Map<String, Value>,
    key: &str,
    value: Option<Value>,
) {
    if let Some(value) = value
        && !value.is_null()
    {
        target.insert(key.to_string(), value);
    }
}

pub(crate) fn epoch_to_string(value: Option<i64>) -> Option<String> {
    value.map(|value| value.to_string())
}
