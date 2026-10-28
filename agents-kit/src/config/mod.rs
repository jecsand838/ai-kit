//! Validated passive configuration for Agent construction.
//!
//! Constructing these values performs no process startup or network I/O.
//! [`crate::AgentBuilder::build`] owns that work. Choose one [`BackendConfig`],
//! set lifecycle and prompt admission bounds, then resupply integrations when
//! reopening an existing Agent. Workspace and prompt defaults remain separate.

mod codex;
mod custom;
mod opencode;

pub use codex::CodexConfig;
pub use custom::{CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
pub use opencode::OpenCodeConfig;

pub(crate) use codex::CodexMode;
pub(crate) use opencode::OpenCodeMode;

use std::num::NonZeroUsize;
use std::time::Duration;

use crate::KitError;

/// Chooses one backend and carries only that backend's passive configuration.
#[derive(Clone, Debug)]
pub enum BackendConfig {
    /// Uses a Codex app server.
    Codex(
        /// The Codex connection configuration.
        CodexConfig,
    ),
    /// Uses an OpenCode server.
    OpenCode(
        /// The OpenCode process or attachment configuration.
        OpenCodeConfig,
    ),
    /// Uses the provider-driven Custom backend.
    Custom(
        /// Passive Custom backend policy configuration.
        CustomAgentConfig,
    ),
}

/// Portable agent-wide runtime configuration.
///
/// Workspace model, reasoning-effort, and knowledge defaults belong to
/// [`crate::workspace::WorkspaceDefaults`], not this type.
///
/// # Examples
///
/// ```
/// use std::num::NonZeroUsize;
/// use std::time::Duration;
/// use agents_kit::{
///     config::{AgentConfig, CodexConfig},
///     KitError,
/// };
///
/// let agent_config = || -> Result<AgentConfig, KitError> {
///     let backend = CodexConfig::spawn(
///         Duration::from_secs(15),
///         Duration::from_secs(60),
///     )?;
///     AgentConfig::new(
///         backend.into(),
///         Duration::from_secs(60),
///         NonZeroUsize::new(1).expect("one is nonzero"),
///     )?
///     .with_display_name("documentation-agent")
/// };
/// let config = agent_config()?;
/// assert_eq!(config.display_name(), Some("documentation-agent"));
/// # Ok::<(), KitError>(())
/// ```
#[derive(Clone, Debug)]
pub struct AgentConfig {
    backend: BackendConfig,
    display_name: Option<String>,
    instructions: Option<String>,
    tracing_enabled: bool,
    lifecycle_timeout: Duration,
    prompt_limit: NonZeroUsize,
    default_prompt_timeout: Option<Duration>,
}

impl AgentConfig {
    /// Creates agent-wide configuration with tracing disabled and no optional text.
    ///
    /// # Errors
    /// Returns [`crate::error::ErrorKind::InvalidConfig`] when the lifecycle timeout is zero or the selected backend configuration is invalid.
    ///
    /// ```
    /// use agents_kit::config::{AgentConfig, CodexConfig};
    /// use std::{num::NonZeroUsize, time::Duration};
    /// let config = AgentConfig::new(CodexConfig::spawn(Duration::from_secs(15),
    ///     Duration::from_secs(60))?.into(), Duration::from_secs(30), NonZeroUsize::MIN)?;
    /// assert_eq!(config.lifecycle_timeout(), Duration::from_secs(30));
    /// assert_eq!(config.prompt_limit().get(), 1);
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn new(
        backend: BackendConfig,
        lifecycle_timeout: Duration,
        prompt_limit: NonZeroUsize,
    ) -> Result<Self, KitError> {
        let config = Self {
            backend,
            display_name: None,
            instructions: None,
            tracing_enabled: false,
            lifecycle_timeout,
            prompt_limit,
            default_prompt_timeout: None,
        };
        config.validate()?;
        Ok(config)
    }

    /// Sets a nonempty, unpadded display name.
    ///
    /// # Errors
    /// Returns [`crate::error::ErrorKind::InvalidConfig`] for an empty or padded display name.
    ///
    /// ```
    /// use agents_kit::config::{AgentConfig, CodexConfig};
    /// use std::{num::NonZeroUsize, time::Duration};
    /// let config = AgentConfig::new(CodexConfig::spawn(Duration::from_secs(15),
    ///     Duration::from_secs(60))?.into(), Duration::from_secs(30), NonZeroUsize::MIN)?;
    /// let config = config.with_display_name("worker")?;
    /// assert_eq!(config.display_name(), Some("worker"));
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn with_display_name(mut self, display_name: impl Into<String>) -> Result<Self, KitError> {
        let display_name = display_name.into();
        Self::validate_optional_text("agent display name", Some(&display_name))?;
        self.display_name = Some(display_name);
        Ok(self)
    }

    /// Sets nonempty, unpadded agent-level instructions.
    ///
    /// # Errors
    /// Returns [`crate::error::ErrorKind::InvalidConfig`] for empty or padded instructions.
    ///
    /// ```
    /// use agents_kit::config::{AgentConfig, CodexConfig};
    /// use std::{num::NonZeroUsize, time::Duration};
    /// let config = AgentConfig::new(CodexConfig::spawn(Duration::from_secs(15),
    ///     Duration::from_secs(60))?.into(), Duration::from_secs(30), NonZeroUsize::MIN)?;
    /// let config = config.with_instructions("Use concise answers.")?;
    /// assert_eq!(config.instructions(), Some("Use concise answers."));
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn with_instructions(mut self, instructions: impl Into<String>) -> Result<Self, KitError> {
        let instructions = instructions.into();
        Self::validate_optional_text("agent instructions", Some(&instructions))?;
        self.instructions = Some(instructions);
        Ok(self)
    }

    /// Enables or disables runtime tracing for this agent.
    ///
    /// ```
    /// use agents_kit::config::{AgentConfig, CodexConfig};
    /// use std::{num::NonZeroUsize, time::Duration};
    /// let config = AgentConfig::new(CodexConfig::spawn(Duration::from_secs(15),
    ///     Duration::from_secs(60))?.into(), Duration::from_secs(30), NonZeroUsize::MIN)?;
    /// let config = config.with_tracing_enabled(true);
    /// assert!(config.tracing_enabled());
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn with_tracing_enabled(mut self, tracing_enabled: bool) -> Self {
        self.tracing_enabled = tracing_enabled;
        self
    }

    /// Sets the default foreground prompt timeout used when a request omits one.
    ///
    /// # Errors
    /// Returns [`crate::error::ErrorKind::InvalidConfig`] when the timeout is zero.
    ///
    /// ```
    /// use agents_kit::config::{AgentConfig, CodexConfig};
    /// use std::{num::NonZeroUsize, time::Duration};
    /// let config = AgentConfig::new(CodexConfig::spawn(Duration::from_secs(15),
    ///     Duration::from_secs(60))?.into(), Duration::from_secs(30), NonZeroUsize::MIN)?;
    /// let config = config.with_default_prompt_timeout(Duration::from_secs(45))?;
    /// assert_eq!(config.default_prompt_timeout(), Some(Duration::from_secs(45)));
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn with_default_prompt_timeout(mut self, timeout: Duration) -> Result<Self, KitError> {
        if timeout.is_zero() {
            return Err(KitError::invalid_config(
                "agent default prompt timeout must be greater than zero",
            ));
        }
        self.default_prompt_timeout = Some(timeout);
        Ok(self)
    }

    /// Returns the selected backend configuration.
    ///
    /// ```
    /// use agents_kit::config::{AgentConfig, CodexConfig};
    /// use std::{num::NonZeroUsize, time::Duration};
    /// let config = AgentConfig::new(CodexConfig::spawn(Duration::from_secs(15),
    ///     Duration::from_secs(60))?.into(), Duration::from_secs(30), NonZeroUsize::MIN)?;
    /// assert!(matches!(config.backend(), agents_kit::config::BackendConfig::Codex(_)));
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn backend(&self) -> &BackendConfig {
        &self.backend
    }

    /// Returns the configured display name, when present.
    ///
    /// ```
    /// use agents_kit::config::{AgentConfig, CodexConfig};
    /// use std::{num::NonZeroUsize, time::Duration};
    /// let config = AgentConfig::new(CodexConfig::spawn(Duration::from_secs(15),
    ///     Duration::from_secs(60))?.into(), Duration::from_secs(30), NonZeroUsize::MIN)?;
    /// assert_eq!(config.display_name(), None);
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn display_name(&self) -> Option<&str> {
        self.display_name.as_deref()
    }

    /// Returns the configured agent-level instructions, when present.
    ///
    /// ```
    /// use agents_kit::config::{AgentConfig, CodexConfig};
    /// use std::{num::NonZeroUsize, time::Duration};
    /// let config = AgentConfig::new(CodexConfig::spawn(Duration::from_secs(15),
    ///     Duration::from_secs(60))?.into(), Duration::from_secs(30), NonZeroUsize::MIN)?;
    /// assert_eq!(config.instructions(), None);
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn instructions(&self) -> Option<&str> {
        self.instructions.as_deref()
    }

    /// Returns whether runtime tracing is enabled.
    ///
    /// ```
    /// use agents_kit::config::{AgentConfig, CodexConfig};
    /// use std::{num::NonZeroUsize, time::Duration};
    /// let config = AgentConfig::new(CodexConfig::spawn(Duration::from_secs(15),
    ///     Duration::from_secs(60))?.into(), Duration::from_secs(30), NonZeroUsize::MIN)?;
    /// assert!(!config.tracing_enabled());
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn tracing_enabled(&self) -> bool {
        self.tracing_enabled
    }

    /// Returns the bound used for agent lifecycle and recovery work.
    ///
    /// ```
    /// use agents_kit::config::{AgentConfig, CodexConfig};
    /// use std::{num::NonZeroUsize, time::Duration};
    /// let config = AgentConfig::new(CodexConfig::spawn(Duration::from_secs(15),
    ///     Duration::from_secs(60))?.into(), Duration::from_secs(30), NonZeroUsize::MIN)?;
    /// assert_eq!(config.lifecycle_timeout(), Duration::from_secs(30));
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn lifecycle_timeout(&self) -> Duration {
        self.lifecycle_timeout
    }

    /// Returns the maximum number of concurrently admitted prompts for the agent.
    ///
    /// ```
    /// use agents_kit::config::{AgentConfig, CodexConfig};
    /// use std::{num::NonZeroUsize, time::Duration};
    /// let config = AgentConfig::new(CodexConfig::spawn(Duration::from_secs(15),
    ///     Duration::from_secs(60))?.into(), Duration::from_secs(30), NonZeroUsize::MIN)?;
    /// assert_eq!(config.prompt_limit().get(), 1);
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn prompt_limit(&self) -> NonZeroUsize {
        self.prompt_limit
    }

    /// Returns the default foreground prompt timeout, when configured.
    #[must_use]
    ///
    /// ```
    /// use agents_kit::config::{AgentConfig, CodexConfig};
    /// use std::{num::NonZeroUsize, time::Duration};
    /// let config = AgentConfig::new(CodexConfig::spawn(Duration::from_secs(15),
    ///     Duration::from_secs(60))?.into(), Duration::from_secs(30), NonZeroUsize::MIN)?;
    /// assert_eq!(config.default_prompt_timeout(), None);
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn default_prompt_timeout(&self) -> Option<Duration> {
        self.default_prompt_timeout
    }

    pub(crate) fn validate(&self) -> Result<(), KitError> {
        if self.lifecycle_timeout.is_zero() {
            return Err(KitError::invalid_config(
                "agent lifecycle timeout must be greater than zero",
            ));
        }
        if self
            .default_prompt_timeout
            .is_some_and(|timeout| timeout.is_zero())
        {
            return Err(KitError::invalid_config(
                "agent default prompt timeout must be greater than zero",
            ));
        }
        Self::validate_optional_text("agent display name", self.display_name.as_deref())?;
        Self::validate_optional_text("agent instructions", self.instructions.as_deref())?;
        match &self.backend {
            BackendConfig::Codex(config) => config.validate(),
            BackendConfig::OpenCode(config) => config.validate(),
            BackendConfig::Custom(config) => config.validate(),
        }
    }

    fn validate_optional_text(label: &str, value: Option<&str>) -> Result<(), KitError> {
        if value.is_some_and(|value| value.is_empty() || value.trim() != value) {
            return Err(KitError::invalid_config(format!(
                "{label} must be nonempty and unpadded when present"
            )));
        }
        Ok(())
    }
}

impl From<CodexConfig> for BackendConfig {
    fn from(config: CodexConfig) -> Self {
        Self::Codex(config)
    }
}

impl From<OpenCodeConfig> for BackendConfig {
    fn from(config: OpenCodeConfig) -> Self {
        Self::OpenCode(config)
    }
}

impl From<CustomAgentConfig> for BackendConfig {
    fn from(config: CustomAgentConfig) -> Self {
        Self::Custom(config)
    }
}
