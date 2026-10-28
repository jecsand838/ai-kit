use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use crate::KitError;

/// Selects how an agent connects to a Codex app server.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use agents_kit::{
///     config::{BackendConfig, CodexConfig},
///     KitError,
/// };
///
/// let spawned_codex = || -> Result<BackendConfig, KitError> {
///     let codex = CodexConfig::spawn(
///         Duration::from_secs(15),
///         Duration::from_secs(60),
///     )?;
///     Ok(codex.into())
/// };
/// let backend = spawned_codex()?;
/// assert!(matches!(backend, BackendConfig::Codex(_)));
/// # Ok::<(), KitError>(())
/// ```
#[derive(Clone, Debug)]
pub struct CodexConfig {
    mode: CodexMode,
}

#[derive(Clone, Debug)]
pub(crate) enum CodexMode {
    Spawn {
        startup_timeout: Duration,
        request_timeout: Duration,
    },
    Tcp {
        address: SocketAddr,
        connect_timeout: Duration,
        request_timeout: Duration,
    },
    Unix {
        path: PathBuf,
        connect_timeout: Duration,
        request_timeout: Duration,
    },
}

impl CodexConfig {
    /// Configures a Kit-owned Codex child process.
    ///
    /// Configuration is passive; the child starts during Agent build.
    ///
    /// # Errors
    /// Returns [`crate::error::ErrorKind::InvalidConfig`] when:
    /// Either timeout is zero.
    ///
    /// ```
    /// use agents_kit::config::CodexConfig;
    /// use std::time::Duration;
    /// let config = CodexConfig::spawn(Duration::from_secs(15), Duration::from_secs(60))?;
    /// assert!(CodexConfig::spawn(Duration::ZERO, Duration::from_secs(60)).is_err());
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn spawn(startup_timeout: Duration, request_timeout: Duration) -> Result<Self, KitError> {
        Self::validate_timeout("Codex startup timeout", startup_timeout)?;
        Self::validate_timeout("Codex request timeout", request_timeout)?;
        Ok(Self {
            mode: CodexMode::Spawn {
                startup_timeout,
                request_timeout,
            },
        })
    }

    /// Configures exclusive attachment to a caller-managed TCP app server.
    ///
    /// Constructing this value does not open a socket.
    ///
    /// # Errors
    /// Returns [`crate::error::ErrorKind::InvalidConfig`] when:
    /// The port or either timeout is zero.
    ///
    /// ```
    /// use agents_kit::config::CodexConfig;
    /// use std::time::Duration;
    /// let config = CodexConfig::tcp("127.0.0.1:4500".parse().unwrap(),
    ///     Duration::from_secs(10), Duration::from_secs(60))?;
    /// assert!(CodexConfig::tcp("127.0.0.1:0".parse().unwrap(),
    ///     Duration::from_secs(10), Duration::from_secs(60)).is_err());
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn tcp(
        address: SocketAddr,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self, KitError> {
        if address.port() == 0 {
            return Err(KitError::invalid_config(
                "Codex TCP address must use a nonzero port",
            ));
        }
        Self::validate_timeout("Codex connect timeout", connect_timeout)?;
        Self::validate_timeout("Codex request timeout", request_timeout)?;
        Ok(Self {
            mode: CodexMode::Tcp {
                address,
                connect_timeout,
                request_timeout,
            },
        })
    }

    /// Configures exclusive attachment through a Unix-domain socket.
    ///
    /// The path is not opened until Agent build. Availability of Unix sockets depends on the host.
    ///
    /// # Errors
    /// Returns [`crate::error::ErrorKind::InvalidConfig`] when:
    /// The path is empty or either timeout is zero.
    ///
    /// ```
    /// use agents_kit::config::CodexConfig;
    /// use std::time::Duration;
    /// let config = CodexConfig::unix("codex.sock".into(), Duration::from_secs(10), Duration::from_secs(60))?;
    /// assert!(CodexConfig::unix("".into(), Duration::from_secs(10), Duration::from_secs(60)).is_err());
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn unix(
        path: PathBuf,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self, KitError> {
        if path.as_os_str().is_empty() {
            return Err(KitError::invalid_config(
                "Codex Unix socket path must not be empty",
            ));
        }
        Self::validate_timeout("Codex connect timeout", connect_timeout)?;
        Self::validate_timeout("Codex request timeout", request_timeout)?;
        Ok(Self {
            mode: CodexMode::Unix {
                path,
                connect_timeout,
                request_timeout,
            },
        })
    }

    pub(crate) fn mode(&self) -> &CodexMode {
        &self.mode
    }

    pub(crate) fn validate(&self) -> Result<(), KitError> {
        match &self.mode {
            CodexMode::Spawn {
                startup_timeout,
                request_timeout,
            } => {
                Self::validate_timeout("Codex startup timeout", *startup_timeout)?;
                Self::validate_timeout("Codex request timeout", *request_timeout)
            }
            CodexMode::Tcp {
                address,
                connect_timeout,
                request_timeout,
            } => {
                if address.port() == 0 {
                    return Err(KitError::invalid_config(
                        "Codex TCP address must use a nonzero port",
                    ));
                }
                Self::validate_timeout("Codex connect timeout", *connect_timeout)?;
                Self::validate_timeout("Codex request timeout", *request_timeout)
            }
            CodexMode::Unix {
                path,
                connect_timeout,
                request_timeout,
            } => {
                if path.as_os_str().is_empty() {
                    return Err(KitError::invalid_config(
                        "Codex Unix socket path must not be empty",
                    ));
                }
                Self::validate_timeout("Codex connect timeout", *connect_timeout)?;
                Self::validate_timeout("Codex request timeout", *request_timeout)
            }
        }
    }

    fn validate_timeout(label: &str, timeout: Duration) -> Result<(), KitError> {
        if timeout.is_zero() {
            return Err(KitError::invalid_config(format!(
                "{label} must be greater than zero"
            )));
        }
        Ok(())
    }
}

impl CodexMode {
    pub(crate) fn request_timeout(&self) -> Duration {
        match self {
            Self::Spawn {
                request_timeout, ..
            }
            | Self::Tcp {
                request_timeout, ..
            }
            | Self::Unix {
                request_timeout, ..
            } => *request_timeout,
        }
    }
}
