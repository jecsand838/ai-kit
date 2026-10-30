use reqwest::{Client, Method, RequestBuilder, Url};
use std::time::Duration;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use crate::KitError;
use crate::config::{RetryPolicy, TransportConfig};
use crate::error::{ErrorKind, ErrorPhase, Operation, ValidationViolation};
use crate::transport::auth::AuthScheme;

use super::execution::RequestExecution;

const PROVIDER_USER_AGENT: &str = "providers-kit/0.0.2";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AuthMode {
    Apply,
    Skip,
}

#[derive(Clone)]
pub(crate) struct ProviderHttpContext {
    client: Client,
    base_url: Url,
    auth: AuthScheme,
    connect_timeout: Duration,
    timeout: Duration,
    retry: RetryPolicy,
}

impl ProviderHttpContext {
    pub(crate) fn new(
        base_url: Url,
        auth: AuthScheme,
        transport: TransportConfig,
    ) -> Result<Self, KitError> {
        if !matches!(base_url.scheme(), "http" | "https")
            || base_url.cannot_be_a_base()
            || !base_url.username().is_empty()
            || base_url.password().is_some()
            || base_url.query().is_some()
            || base_url.fragment().is_some()
        {
            return Err(KitError::config(
                ValidationViolation::InvalidFormat {
                    field: "base_url",
                    rule: "HTTP(S) base without credentials, query, or fragment",
                },
                "provider base URL must be an HTTP(S) base without credentials, query, or fragment",
            ));
        }
        let client = Client::builder()
            .user_agent(PROVIDER_USER_AGENT)
            .connect_timeout(transport.connect_timeout())
            .pool_idle_timeout(transport.pool_idle_timeout())
            .build()
            .map_err(|err| {
                KitError::new(
                    ErrorKind::Configuration,
                    Operation::Configure,
                    ErrorPhase::RequestBuild,
                )
                .with_message(format!("failed to build HTTP client: {err}"))
            })?;
        Ok(Self {
            client,
            base_url,
            auth,
            connect_timeout: transport.connect_timeout(),
            timeout: transport.request_timeout(),
            retry: transport.retry().clone(),
        })
    }

    pub(crate) fn url(&self, path: &str) -> Result<Url, KitError> {
        let mut base = self.base_url.clone();
        if !base.path().ends_with('/') {
            let normalized = if base.path().is_empty() {
                "/".to_string()
            } else {
                format!("{}/", base.path().trim_end_matches('/'))
            };
            base.set_path(&normalized);
        }
        let normalized_path = path.trim_start_matches('/');
        base.join(normalized_path).map_err(|err| {
            KitError::new(
                ErrorKind::Configuration,
                Operation::Configure,
                ErrorPhase::RequestBuild,
            )
            .with_message(format!("failed to join provider url path {path}: {err}"))
        })
    }

    pub(crate) fn request(&self, method: Method, path: &str) -> Result<RequestBuilder, KitError> {
        let url = self.url(path)?;
        Ok(self.request_absolute(method, url, AuthMode::Apply))
    }

    pub(crate) fn request_absolute(
        &self,
        method: Method,
        url: Url,
        auth: AuthMode,
    ) -> RequestBuilder {
        let builder = self.client.request(method, url);
        match auth {
            AuthMode::Apply => self.auth.apply(builder),
            AuthMode::Skip => builder,
        }
    }

    pub(crate) fn is_same_origin(&self, url: &Url) -> bool {
        self.base_url.origin() == url.origin()
    }

    pub(crate) fn url_with_query(
        &self,
        path: &str,
        decorate: impl FnOnce(&mut Url),
    ) -> Result<Url, KitError> {
        let mut url = self.url(path)?;
        decorate(&mut url);
        Ok(url)
    }

    pub(crate) fn websocket_url(
        &self,
        path: &str,
        decorate: impl FnOnce(&mut Url),
    ) -> Result<Url, KitError> {
        let mut url = self.url(path)?;
        let scheme = match url.scheme() {
            "https" => "wss",
            "http" => "ws",
            other => other,
        }
        .to_string();
        url.set_scheme(&scheme).map_err(|_| {
            KitError::new(
                ErrorKind::Configuration,
                Operation::Configure,
                ErrorPhase::RequestBuild,
            )
            .with_message("failed to convert provider URL into a websocket URL")
        })?;
        decorate(&mut url);
        Ok(url)
    }

    pub(crate) fn websocket_request(
        &self,
        path: &str,
        auth: AuthMode,
        decorate: impl FnOnce(&mut Url),
    ) -> Result<http::Request<()>, KitError> {
        let url = self.websocket_url(path, decorate)?;
        let mut request = url.as_str().into_client_request().map_err(|error| {
            KitError::new(
                ErrorKind::Configuration,
                Operation::Configure,
                ErrorPhase::RequestBuild,
            )
            .with_message(format!(
                "failed to build provider WebSocket request: {error}"
            ))
        })?;
        if auth == AuthMode::Apply {
            self.auth.apply_headers(request.headers_mut())?;
        }
        Ok(request)
    }

    pub(crate) fn timeout(&self) -> Duration {
        self.timeout
    }

    pub(crate) fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    pub(crate) fn execution(&self) -> RequestExecution {
        RequestExecution::fail_fast(self.timeout)
    }

    pub(crate) fn retryable_execution(&self) -> RequestExecution {
        RequestExecution::retryable(self.timeout, self.retry.clone())
    }
}
