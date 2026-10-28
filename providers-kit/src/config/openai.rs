use reqwest::header::HeaderValue;
use url::Url;

use super::ApiKey;
use crate::KitError;
use crate::error::ValidationViolation;

const DEFAULT_OPENAI_BASE_URL: &str = "https://api.openai.com/v1";

/// OpenAI credentials and endpoint configuration.
///
/// ```
/// let config = providers_kit::config::OpenAiConfig::new(
///     providers_kit::config::ApiKey::new("sk-test")?,
/// );
/// let _ = config;
///
/// # Ok::<(), providers_kit::KitError>(())
/// ```
#[derive(Clone)]
pub struct OpenAiConfig {
    api_key: ApiKey,
    base_url: Url,
    organization: Option<String>,
    project: Option<String>,
}

impl OpenAiConfig {
    /// Creates a configuration for the public OpenAI endpoint.
    ///
    /// This only constructs configuration; provider operations perform network I/O.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::config::{ApiKey, OpenAiConfig};
    /// let config = OpenAiConfig::new(ApiKey::new("example-key")?);
    /// let _owned_configuration = config;
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    #[must_use]
    pub fn new(api_key: ApiKey) -> Self {
        Self {
            api_key,
            base_url: Url::parse(DEFAULT_OPENAI_BASE_URL)
                .expect("crate-owned OpenAI base URL should parse"),
            organization: None,
            project: None,
        }
    }

    /// Replaces the API base URL.
    ///
    /// This only constructs configuration; provider operations perform network I/O.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::config::{ApiKey, OpenAiConfig};
    /// let config = OpenAiConfig::new(ApiKey::new("example-key")?);
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

    /// Sets the optional organization header.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for an empty or padded value, or one that cannot be represented as an HTTP header value.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::config::{ApiKey, OpenAiConfig};
    /// let config = OpenAiConfig::new(ApiKey::new("example-key")?);
    /// assert!(config.clone().with_organization(" padded ").is_err());
    /// let _config = config.with_organization("team-1")?;
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn with_organization(mut self, organization: impl Into<String>) -> Result<Self, KitError> {
        self.organization = Some(Self::scope(
            "credentials.organization",
            organization.into(),
        )?);
        Ok(self)
    }

    /// Sets the optional project header.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for an empty or padded value, or one that cannot be represented as an HTTP header value.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::config::{ApiKey, OpenAiConfig};
    /// let config = OpenAiConfig::new(ApiKey::new("example-key")?);
    /// assert!(config.clone().with_project(" padded ").is_err());
    /// let _config = config.with_project("team-1")?;
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn with_project(mut self, project: impl Into<String>) -> Result<Self, KitError> {
        self.project = Some(Self::scope("credentials.project", project.into())?);
        Ok(self)
    }

    pub(crate) fn api_key(&self) -> &ApiKey {
        &self.api_key
    }

    pub(crate) fn base_url(&self) -> &Url {
        &self.base_url
    }

    pub(crate) fn organization(&self) -> Option<&str> {
        self.organization.as_deref()
    }

    pub(crate) fn project(&self) -> Option<&str> {
        self.project.as_deref()
    }

    fn scope(label: &'static str, value: String) -> Result<String, KitError> {
        if value.is_empty() || value.trim() != value || HeaderValue::try_from(&value).is_err() {
            Err(KitError::config(
                ValidationViolation::InvalidFormat {
                    field: label,
                    rule: "nonblank, unpadded HTTP header value",
                },
                format!("{label} must be a nonblank, unpadded HTTP header value"),
            ))
        } else {
            Ok(value)
        }
    }
}
