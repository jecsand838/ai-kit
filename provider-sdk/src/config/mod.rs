//! Provider credentials, endpoints, and shared transport policy.
//!
//! Build a [`ProviderConfig`] from one [`ProviderKind`] and [`TransportConfig`].
//! Configuration constructors are passive; [`crate::Provider::from_config`] builds
//! the selected clients, and subsequent operations check real service behavior.
//! [`ApiKey`] rejects invalid HTTP-header values and redacts its Debug output.
//! [`RetryPolicy`] applies only to operations whose existing semantics permit replay.

mod anthropic;
mod credentials;
mod gemini;
mod ollama;
mod openai;
mod retry;
mod transport;

pub use anthropic::AnthropicConfig;
pub(crate) use anthropic::DEFAULT_ANTHROPIC_VERSION;
pub use credentials::ApiKey;
pub use gemini::{GeminiConfig, GeminiServiceTier};
pub use ollama::OllamaConfig;
pub use openai::OpenAiConfig;
pub use retry::RetryPolicy;
pub use transport::TransportConfig;

/// Concrete provider configuration selected for [`ProviderConfig`].
///
/// ```
/// let kind = provider_sdk::config::ProviderKind::Ollama(
///     provider_sdk::config::OllamaConfig::local(),
/// );
/// assert!(matches!(kind, provider_sdk::config::ProviderKind::Ollama(_)));
/// ```
#[derive(Clone)]
pub enum ProviderKind {
    /// OpenAI API configuration.
    OpenAi(OpenAiConfig),
    /// Anthropic API configuration.
    Anthropic(AnthropicConfig),
    /// Gemini API configuration.
    Gemini(GeminiConfig),
    /// Ollama local or cloud configuration.
    Ollama(OllamaConfig),
}

/// Provider construction input.
///
/// Model selection is intentionally absent. Callers select an exact
/// [`crate::ModelId`] after constructing the provider.
///
/// ```
/// let config = provider_sdk::ProviderConfig::new(
///     provider_sdk::config::ProviderKind::Ollama(
///         provider_sdk::config::OllamaConfig::local(),
///     ),
///     provider_sdk::config::TransportConfig::default(),
/// );
/// let _ = config;
/// ```
#[derive(Clone)]
pub struct ProviderConfig {
    kind: ProviderKind,
    transport: TransportConfig,
}

impl ProviderConfig {
    /// Combines one concrete provider config with shared transport policy.
    ///
    /// ```
    /// let config = provider_sdk::ProviderConfig::new(
    ///     provider_sdk::config::ProviderKind::Ollama(
    ///         provider_sdk::config::OllamaConfig::local(),
    ///     ),
    ///     provider_sdk::config::TransportConfig::default(),
    /// );
    /// let _ = config;
    /// ```
    #[must_use]
    pub fn new(kind: ProviderKind, transport: TransportConfig) -> Self {
        Self { kind, transport }
    }

    pub(crate) fn into_parts(self) -> (ProviderKind, TransportConfig) {
        (self.kind, self.transport)
    }
}
