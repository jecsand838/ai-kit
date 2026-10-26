use std::ffi::OsString;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use crate::SdkError;

const MIN_IDLE_STREAM_TIMEOUT: Duration = Duration::from_secs(30);

/// Selects SDK-owned spawning or exclusive attachment for OpenCode.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use agent_sdk::{
///     config::{BackendConfig, OpenCodeConfig},
///     SdkError,
/// };
///
/// let external_opencode = || -> Result<BackendConfig, SdkError> {
///     let opencode = OpenCodeConfig::external(
///         "http://127.0.0.1:4096",
///         Duration::from_secs(10),
///         Duration::from_secs(60),
///         Duration::from_secs(30),
///     )?;
///     Ok(opencode.into())
/// };
/// let backend = external_opencode()?;
/// assert!(matches!(backend, BackendConfig::OpenCode(_)));
/// # Ok::<(), SdkError>(())
/// ```
#[derive(Clone, Debug)]
pub struct OpenCodeConfig {
    mode: OpenCodeMode,
}

#[derive(Clone, Debug)]
pub(crate) enum OpenCodeMode {
    Spawn {
        executable: PathBuf,
        arguments: Vec<OsString>,
        address: SocketAddr,
        startup_timeout: Duration,
        request_timeout: Duration,
        idle_stream_timeout: Duration,
    },
    External {
        base_url: reqwest::Url,
        connect_timeout: Duration,
        request_timeout: Duration,
        idle_stream_timeout: Duration,
    },
}

impl OpenCodeConfig {
    /// Configures an SDK-owned OpenCode process on a loopback address.
    ///
    /// Extra arguments may not replace the SDK-owned serve, hostname, or port routing.
    ///
    /// This creates passive configuration; it does not start the executable.
    ///
    /// # Errors
    /// Returns [`crate::error::ErrorKind::InvalidConfig`] when:
    /// The executable is empty, the address is not loopback with a nonzero port, arguments replace owned routing, startup/request timeout is zero, or stream silence is below thirty seconds.
    ///
    /// ```
    /// use agent_sdk::config::OpenCodeConfig;
    /// use std::time::Duration;
    /// let config = OpenCodeConfig::spawn("opencode".into(), vec![],
    ///     "127.0.0.1:4096".parse().unwrap(), Duration::from_secs(15),
    ///     Duration::from_secs(60), Duration::from_secs(30))?;
    /// let backend: agent_sdk::config::BackendConfig = config.into();
    /// assert!(matches!(backend, agent_sdk::config::BackendConfig::OpenCode(_)));
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn spawn(
        executable: PathBuf,
        arguments: Vec<OsString>,
        address: SocketAddr,
        startup_timeout: Duration,
        request_timeout: Duration,
        idle_stream_timeout: Duration,
    ) -> Result<Self, SdkError> {
        if executable.as_os_str().is_empty() {
            return Err(SdkError::invalid_config(
                "OpenCode executable path must not be empty",
            ));
        }
        if !address.ip().is_loopback() || address.port() == 0 {
            return Err(SdkError::invalid_config(
                "OpenCode spawn address must be loopback with a nonzero port",
            ));
        }
        for argument in &arguments {
            if Self::replaces_owned_routing(argument) {
                return Err(SdkError::invalid_config(
                    "OpenCode extra arguments must not replace serve, hostname, or port",
                ));
            }
        }
        Self::validate_timeout("OpenCode startup timeout", startup_timeout)?;
        Self::validate_timeout("OpenCode request timeout", request_timeout)?;
        Self::validate_idle_timeout(idle_stream_timeout)?;
        Ok(Self {
            mode: OpenCodeMode::Spawn {
                executable,
                arguments,
                address,
                startup_timeout,
                request_timeout,
                idle_stream_timeout,
            },
        })
    }

    /// Configures exclusive attachment to a caller-managed OpenCode server.
    ///
    /// The base URL must be credential-free HTTP or HTTPS. Caller-owned server
    /// configuration is not mutated by this attachment.
    ///
    /// This creates passive configuration without contacting the server. A path prefix is retained with a trailing slash.
    ///
    /// # Errors
    /// Returns [`crate::error::ErrorKind::InvalidConfig`] when:
    /// The URL is malformed or not credential-free HTTP(S), has query/fragment components, a connect/request timeout is zero, or stream silence is below thirty seconds.
    ///
    /// ```
    /// use agent_sdk::config::OpenCodeConfig;
    /// use std::time::Duration;
    /// let config = OpenCodeConfig::external("http://127.0.0.1:4096",
    ///     Duration::from_secs(10), Duration::from_secs(60), Duration::from_secs(30))?;
    /// assert!(OpenCodeConfig::external("http://127.0.0.1:4096?token=example",
    ///     Duration::from_secs(10), Duration::from_secs(60), Duration::from_secs(30)).is_err());
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn external(
        base_url: impl AsRef<str>,
        connect_timeout: Duration,
        request_timeout: Duration,
        idle_stream_timeout: Duration,
    ) -> Result<Self, SdkError> {
        let mut base_url = reqwest::Url::parse(base_url.as_ref())
            .map_err(|_| SdkError::invalid_config("OpenCode base URL is invalid"))?;
        if !matches!(base_url.scheme(), "http" | "https")
            || !base_url.username().is_empty()
            || base_url.password().is_some()
            || base_url.query().is_some()
            || base_url.fragment().is_some()
        {
            return Err(SdkError::invalid_config(
                "OpenCode base URL must be credential-free canonical HTTP or HTTPS",
            ));
        }
        base_url.set_query(None);
        base_url.set_fragment(None);
        if !base_url.path().ends_with('/') {
            let path = format!("{}/", base_url.path());
            base_url.set_path(&path);
        }
        Self::validate_timeout("OpenCode connect timeout", connect_timeout)?;
        Self::validate_timeout("OpenCode request timeout", request_timeout)?;
        Self::validate_idle_timeout(idle_stream_timeout)?;
        Ok(Self {
            mode: OpenCodeMode::External {
                base_url,
                connect_timeout,
                request_timeout,
                idle_stream_timeout,
            },
        })
    }

    pub(crate) fn mode(&self) -> &OpenCodeMode {
        &self.mode
    }

    pub(crate) fn validate(&self) -> Result<(), SdkError> {
        match &self.mode {
            OpenCodeMode::Spawn {
                executable,
                arguments,
                address,
                startup_timeout,
                request_timeout,
                idle_stream_timeout,
            } => {
                if executable.as_os_str().is_empty()
                    || !address.ip().is_loopback()
                    || address.port() == 0
                    || arguments.iter().any(Self::replaces_owned_routing)
                {
                    return Err(SdkError::invalid_config(
                        "OpenCode spawn configuration is invalid",
                    ));
                }
                Self::validate_timeout("OpenCode startup timeout", *startup_timeout)?;
                Self::validate_timeout("OpenCode request timeout", *request_timeout)?;
                Self::validate_idle_timeout(*idle_stream_timeout)
            }
            OpenCodeMode::External {
                base_url,
                connect_timeout,
                request_timeout,
                idle_stream_timeout,
            } => {
                if !matches!(base_url.scheme(), "http" | "https")
                    || !base_url.username().is_empty()
                    || base_url.password().is_some()
                {
                    return Err(SdkError::invalid_config(
                        "OpenCode external configuration is invalid",
                    ));
                }
                Self::validate_timeout("OpenCode connect timeout", *connect_timeout)?;
                Self::validate_timeout("OpenCode request timeout", *request_timeout)?;
                Self::validate_idle_timeout(*idle_stream_timeout)
            }
        }
    }

    fn validate_timeout(label: &str, timeout: Duration) -> Result<(), SdkError> {
        if timeout.is_zero() {
            return Err(SdkError::invalid_config(format!(
                "{label} must be greater than zero"
            )));
        }
        Ok(())
    }

    fn validate_idle_timeout(timeout: Duration) -> Result<(), SdkError> {
        if timeout < MIN_IDLE_STREAM_TIMEOUT {
            return Err(SdkError::invalid_config(
                "OpenCode idle stream timeout must be at least thirty seconds",
            ));
        }
        Ok(())
    }

    fn replaces_owned_routing(argument: &OsString) -> bool {
        let argument = argument.to_string_lossy();
        matches!(argument.as_ref(), "serve" | "--" | "--hostname" | "--port")
            || argument.starts_with("--hostname=")
            || argument.starts_with("--port=")
    }
}

impl OpenCodeMode {
    pub(crate) fn base_url(&self) -> reqwest::Url {
        match self {
            Self::Spawn { address, .. } => reqwest::Url::parse(&format!("http://{address}/"))
                .expect("validated SocketAddr always forms an HTTP URL"),
            Self::External { base_url, .. } => base_url.clone(),
        }
    }

    pub(crate) fn request_timeout(&self) -> Duration {
        match self {
            Self::Spawn {
                request_timeout, ..
            }
            | Self::External {
                request_timeout, ..
            } => *request_timeout,
        }
    }

    pub(crate) fn idle_stream_timeout(&self) -> Duration {
        match self {
            Self::Spawn {
                idle_stream_timeout,
                ..
            }
            | Self::External {
                idle_stream_timeout,
                ..
            } => *idle_stream_timeout,
        }
    }

    pub(crate) fn connect_timeout(&self) -> Option<Duration> {
        match self {
            Self::External {
                connect_timeout, ..
            } => Some(*connect_timeout),
            Self::Spawn { .. } => None,
        }
    }
}
