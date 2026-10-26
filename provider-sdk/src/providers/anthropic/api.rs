use std::sync::Arc;

use reqwest::Method;
use url::Url;

use super::wire::AnthropicErrorEnvelope;
use crate::SdkError;
use crate::config::TransportConfig;
use crate::config::{AnthropicConfig, DEFAULT_ANTHROPIC_VERSION};
use crate::error::ErrorKind;
use crate::transport::{AuthMode, AuthScheme, CheckedRequestSpec, ProviderHttpContext};

#[derive(Clone)]
pub(crate) struct AnthropicApiContext {
    transport: ProviderHttpContext,
}

pub(super) const ANTHROPIC_REQUESTS: CheckedRequestSpec =
    CheckedRequestSpec::new(AnthropicErrorEnvelope::parse);

impl AnthropicApiContext {
    pub(crate) fn shared(
        config: AnthropicConfig,
        transport: TransportConfig,
    ) -> Result<Arc<Self>, SdkError> {
        let transport = ProviderHttpContext::new(
            config.base_url().clone(),
            AuthScheme::Header {
                name: "x-api-key".to_string(),
                value: config.api_key().clone(),
                prefix: None,
            },
            transport,
        )?;

        Ok(Arc::new(Self { transport }))
    }

    pub(super) fn classify_error(kind: &str) -> ErrorKind {
        match kind {
            "invalid_request_error" => ErrorKind::InvalidRequest,
            "authentication_error" => ErrorKind::Authentication,
            "permission_error" => ErrorKind::PermissionDenied,
            "not_found_error" => ErrorKind::NotFound,
            "request_too_large" => ErrorKind::RequestTooLarge,
            "rate_limit_error" => ErrorKind::RateLimited,
            "api_error" => ErrorKind::Internal,
            "billing_error" => ErrorKind::Billing,
            "conflict_error" => ErrorKind::Conflict,
            "timeout_error" => ErrorKind::DeadlineExceeded,
            "overloaded_error" => ErrorKind::ServiceUnavailable,
            _ => ErrorKind::ProviderRejected,
        }
    }

    pub(crate) fn transport(&self) -> &ProviderHttpContext {
        &self.transport
    }

    pub(crate) fn request(
        &self,
        method: Method,
        path: &str,
    ) -> Result<reqwest::RequestBuilder, SdkError> {
        Ok(self.decorate_request(self.transport.request(method, path)?))
    }

    pub(crate) fn request_absolute(&self, method: Method, url: Url) -> reqwest::RequestBuilder {
        let auth = if self.transport.is_same_origin(&url) {
            AuthMode::Apply
        } else {
            AuthMode::Skip
        };
        self.decorate_request(self.transport.request_absolute(method, url, auth))
    }

    fn decorate_request(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        builder.header("anthropic-version", DEFAULT_ANTHROPIC_VERSION)
    }
}
