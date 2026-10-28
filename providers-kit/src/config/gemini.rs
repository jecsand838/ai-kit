use url::Url;

use super::ApiKey;

const DEFAULT_GEMINI_BASE_URL: &str = "https://generativelanguage.googleapis.com";

/// Gemini request service tier.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum GeminiServiceTier {
    /// Standard request processing.
    #[default]
    Standard,
    /// Flexible lower-priority processing.
    Flex,
    /// Priority processing.
    Priority,
}

/// Gemini credentials and endpoint configuration.
///
/// ```
/// let config = providers_kit::config::GeminiConfig::new(
///     providers_kit::config::ApiKey::new("gemini-test")?,
/// );
/// let _ = config;
///
/// # Ok::<(), providers_kit::KitError>(())
/// ```
#[derive(Clone)]
pub struct GeminiConfig {
    api_key: ApiKey,
    base_url: Url,
    service_tier: GeminiServiceTier,
}

impl GeminiConfig {
    /// Creates a configuration for the public Gemini endpoint.
    ///
    /// This only constructs configuration; provider operations perform network I/O.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::config::{ApiKey, GeminiConfig};
    /// let config = GeminiConfig::new(ApiKey::new("example-key")?);
    /// let _owned_configuration = config;
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    #[must_use]
    pub fn new(api_key: ApiKey) -> Self {
        Self {
            api_key,
            base_url: Url::parse(DEFAULT_GEMINI_BASE_URL)
                .expect("crate-owned Gemini base URL should parse"),
            service_tier: GeminiServiceTier::Standard,
        }
    }

    /// Replaces the API base URL.
    ///
    /// This only constructs configuration; provider operations perform network I/O.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::config::{ApiKey, GeminiConfig};
    /// let config = GeminiConfig::new(ApiKey::new("example-key")?);
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

    /// Replaces the request service tier.
    ///
    /// The selected endpoint/model determines support; configuration does not probe it.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::config::{ApiKey, GeminiConfig, GeminiServiceTier};
    /// let _config = GeminiConfig::new(ApiKey::new("example-key")?)
    ///     .with_service_tier(GeminiServiceTier::Standard);
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    #[must_use]
    pub fn with_service_tier(mut self, service_tier: GeminiServiceTier) -> Self {
        self.service_tier = service_tier;
        self
    }

    pub(crate) fn api_key(&self) -> &ApiKey {
        &self.api_key
    }

    pub(crate) fn base_url(&self) -> &Url {
        &self.base_url
    }

    pub(crate) fn service_tier(&self) -> GeminiServiceTier {
        self.service_tier
    }
}
