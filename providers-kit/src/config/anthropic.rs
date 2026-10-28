use url::Url;

use super::ApiKey;

const DEFAULT_ANTHROPIC_BASE_URL: &str = "https://api.anthropic.com";
pub(crate) const DEFAULT_ANTHROPIC_VERSION: &str = "2023-06-01";

/// Anthropic credentials and endpoint configuration.
///
/// ```
/// let config = providers_kit::config::AnthropicConfig::new(
///     providers_kit::config::ApiKey::new("anthropic-test")?,
/// );
/// let _ = config;
///
/// # Ok::<(), providers_kit::KitError>(())
/// ```
#[derive(Clone)]
pub struct AnthropicConfig {
    api_key: ApiKey,
    base_url: Url,
}

impl AnthropicConfig {
    /// Creates a configuration for the public Anthropic endpoint.
    ///
    /// This only constructs configuration; provider operations perform network I/O.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::config::{ApiKey, AnthropicConfig};
    /// let config = AnthropicConfig::new(ApiKey::new("example-key")?);
    /// let _owned_configuration = config;
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    #[must_use]
    pub fn new(api_key: ApiKey) -> Self {
        Self {
            api_key,
            base_url: Url::parse(DEFAULT_ANTHROPIC_BASE_URL)
                .expect("crate-owned Anthropic base URL should parse"),
        }
    }

    /// Replaces the API base URL.
    ///
    /// This only constructs configuration; provider operations perform network I/O.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::config::{ApiKey, AnthropicConfig};
    /// let config = AnthropicConfig::new(ApiKey::new("example-key")?);
    /// let config = config.with_base_url("https://example.com/api".parse().unwrap());
    /// let _owned_configuration = config;
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    #[must_use]
    pub fn with_base_url(mut self, base_url: Url) -> Self {
        self.base_url = base_url;
        self
    }

    pub(crate) fn api_key(&self) -> &ApiKey {
        &self.api_key
    }

    pub(crate) fn base_url(&self) -> &Url {
        &self.base_url
    }
}
