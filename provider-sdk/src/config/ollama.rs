use url::Url;

use super::ApiKey;
use crate::SdkError;
use crate::error::ValidationViolation;

const DEFAULT_OLLAMA_LOCAL_BASE_URL: &str = "http://localhost:11434/api";
const DEFAULT_OLLAMA_CLOUD_BASE_URL: &str = "https://ollama.com/api";

/// Ollama local or cloud configuration.
///
/// Construction is passive and does not verify a running local or cloud service.
///
/// # Examples
///
/// ```
/// use provider_sdk::config::{ApiKey, OllamaConfig};
/// let config = OllamaConfig::local();
/// let _config = config.with_base_url("http://127.0.0.1:11434/api".parse().unwrap())?;
///
/// # Ok::<(), provider_sdk::SdkError>(())
/// ```
///
#[derive(Clone)]
pub struct OllamaConfig {
    base_url: Url,
    api_key: Option<ApiKey>,
}

impl OllamaConfig {
    /// Creates an unauthenticated local Ollama configuration.
    ///
    /// ```
    /// let config = provider_sdk::config::OllamaConfig::local();
    /// let _ = config;
    /// ```
    #[must_use]
    pub fn local() -> Self {
        Self {
            base_url: Url::parse(DEFAULT_OLLAMA_LOCAL_BASE_URL)
                .expect("crate-owned local Ollama base URL should parse"),
            api_key: None,
        }
    }

    /// Creates an authenticated local or proxy configuration.
    ///
    /// # Examples
    ///
    /// ```
    /// use provider_sdk::config::{ApiKey, OllamaConfig};
    /// let config = OllamaConfig::local_with_key(ApiKey::new("example-key")?);
    /// let _config = config.with_base_url("http://127.0.0.1:11434/api".parse().unwrap())?;
    ///
    /// # Ok::<(), provider_sdk::SdkError>(())
    /// ```
    ///
    #[must_use]
    pub fn local_with_key(api_key: ApiKey) -> Self {
        Self {
            base_url: Url::parse(DEFAULT_OLLAMA_LOCAL_BASE_URL)
                .expect("crate-owned local Ollama base URL should parse"),
            api_key: Some(api_key),
        }
    }

    /// Creates an Ollama Cloud configuration.
    ///
    /// # Examples
    ///
    /// ```
    /// use provider_sdk::config::{ApiKey, OllamaConfig};
    /// let config = OllamaConfig::cloud(ApiKey::new("example-key")?);
    /// let _config = config.with_base_url("http://127.0.0.1:11434/api".parse().unwrap())?;
    ///
    /// # Ok::<(), provider_sdk::SdkError>(())
    /// ```
    ///
    #[must_use]
    pub fn cloud(api_key: ApiKey) -> Self {
        Self {
            base_url: Url::parse(DEFAULT_OLLAMA_CLOUD_BASE_URL)
                .expect("crate-owned Ollama Cloud base URL should parse"),
            api_key: Some(api_key),
        }
    }

    /// Replaces the API base URL.
    ///
    /// # Errors
    ///
    /// Returns a configuration error unless the URL path is exactly `/api`. The URL is not contacted or checked for endpoint capabilities here.
    ///
    /// # Examples
    ///
    /// ```
    /// use provider_sdk::config::OllamaConfig;
    /// assert!(OllamaConfig::local().with_base_url("http://localhost:11434/".parse().unwrap()).is_err());
    /// let _config = OllamaConfig::local().with_base_url("http://localhost:11434/api".parse().unwrap())?;
    ///
    /// # Ok::<(), provider_sdk::SdkError>(())
    /// ```
    ///
    pub fn with_base_url(mut self, base_url: Url) -> Result<Self, SdkError> {
        if base_url.path() != "/api" {
            return Err(SdkError::config(
                ValidationViolation::InvalidValue {
                    field: "base_url.path",
                    rule: "exact /api path",
                },
                "Ollama base URL must point at the exact /api path",
            ));
        }
        self.base_url = base_url;
        Ok(self)
    }

    pub(crate) fn base_url(&self) -> &Url {
        &self.base_url
    }

    pub(crate) fn api_key(&self) -> Option<&ApiKey> {
        self.api_key.as_ref()
    }
}
