//! MCP client construction and independently owned server lifecycle.
//!
//! [`McpClientConfig`](crate::mcp::McpClientConfig) selects stdio or Streamable HTTP plus naming and interaction
//! policy. Construction is passive; [`crate::Toolset::mcp`] connects. The optional
//! `mcp-server` feature adds a server whose consuming shutdown settles serving
//! work while the caller retains separate toolset shutdown authority.
//!
//! # Invoke an explicitly configured service
//!
//! This example requires a running Streamable HTTP server at the configured URL,
//! with an `echo` tool accepting `{"text":"hello"}` and returning that text.
//! MCP invocation arguments must be JSON objects. Supply any
//! required authentication through the explicit header map. Namespaces affect public
//! selection; they do not rename the native tool. Never infer a server endpoint from
//! a Toolset. A request timeout bounds calls, not a whole application scenario.
//!
//! ```no_run
//! use std::time::Duration;
//! use tool_sdk::{Toolset, ToolsetBackend, ToolHandlerBackend, ToolError};
//! use tool_sdk::declaration::ToolName;
//! use tool_sdk::execution::{ToolContent, ToolHandlerInput, ToolHandlerOutput, ToolInvocation};
//! use tool_sdk::mcp::{McpClientConfig, McpConnectionConfig};
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), ToolError> {
//! let config = McpClientConfig::new(McpConnectionConfig::StreamableHttp {
//!     endpoint: "http://127.0.0.1:8000/mcp".parse().unwrap(),
//!     headers: Default::default(),
//! }).with_request_timeout(Duration::from_secs(30))?;
//! let tools = Toolset::mcp(config).await?;
//! let work = async {
//!     let name = ToolName::new("echo")?;
//!     let inventory = tools.list_tools().await?;
//!     assert!(inventory.iter().any(|info| info.name == name));
//!     let mut handler = tools.tool_handler(name).await?;
//!     let output = handler.step(ToolHandlerInput::Invoke(ToolInvocation {
//!         call_id: Some("owned-call".into()), arguments: serde_json::json!({"text":"hello"}),
//!     })).await?;
//!     let ToolHandlerOutput::Completed(result) = output else {
//!         return Err(ToolError::Protocol { message: "configured echo unexpectedly interacted".into() });
//!     };
//!     assert!(!result.is_error);
//!     assert!(matches!(result.content.as_slice(), [ToolContent::Text { text, .. }] if text == "hello"));
//!     Ok::<(), ToolError>(())
//! }.await;
//! let cleanup = tools.shutdown().await;
//! work?;
//! cleanup?;
//! # Ok(())
//! # }
//! ```
//!
//! With `mcp-server`, the `McpServer` type's complete example starts a separately
//! owned loopback server, invokes it through a client, then settles the client,
//! server, and underlying Toolset in that order. Without that feature only client
//! construction is available. A server does not own the caller's underlying source.

#[cfg(feature = "mcp-server")]
use std::net::SocketAddr;
use std::{collections::BTreeMap, fmt, path::PathBuf, time::Duration};

use serde::{Deserialize, Serialize};
use url::Url;

use crate::{ToolError, name::ToolName};

#[cfg(feature = "mcp-server")]
pub use crate::sources::mcp::server::McpServer;

/// Connection to one MCP server.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "transport", rename_all = "snake_case", deny_unknown_fields)]
pub enum McpConnectionConfig {
    /// Spawns an MCP server and communicates over standard input/output.
    Stdio {
        /// Executable to spawn.
        program: String,
        /// Arguments passed to the executable.
        args: Vec<String>,
        /// Optional working directory for the child process.
        cwd: Option<PathBuf>,
        /// Explicit environment entries supplied to the child process.
        env: BTreeMap<String, String>,
    },
    /// Connects to a Streamable HTTP MCP endpoint.
    StreamableHttp {
        /// Absolute MCP endpoint URL.
        endpoint: Url,
        /// Request headers, including caller-owned authentication when needed.
        headers: BTreeMap<String, String>,
    },
}

impl fmt::Debug for McpConnectionConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stdio {
                program,
                args,
                cwd,
                env,
            } => formatter
                .debug_struct("McpConnectionConfig::Stdio")
                .field("program", program)
                .field("argument_count", &args.len())
                .field("has_working_directory", &cwd.is_some())
                .field("environment_count", &env.len())
                .finish(),
            Self::StreamableHttp { endpoint, headers } => formatter
                .debug_struct("McpConnectionConfig::StreamableHttp")
                .field("endpoint_scheme", &endpoint.scheme())
                .field("endpoint_host", &endpoint.host_str())
                .field("endpoint_port", &endpoint.port_or_known_default())
                .field("header_count", &headers.len())
                .finish(),
        }
    }
}

/// Validated MCP client Toolset configuration.
/// Configuration construction does not connect or spawn a process; [`crate::Toolset::mcp`]
/// performs that work. Discovered names may be namespaced without exposing native routing.
///
/// # Examples
///
/// ```
/// use tool_sdk::mcp::{McpClientConfig, McpConnectionConfig};
/// let connection = McpConnectionConfig::StreamableHttp { endpoint: "http://127.0.0.1:8000/mcp".parse().unwrap(), headers: Default::default() };
/// let config = McpClientConfig::new(connection);
/// let value = serde_json::to_value(&config).unwrap();
/// assert_eq!(value["request_timeout_secs"], 30);
/// assert_eq!(value["form_interactions"], false);
/// ```
///
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpClientConfig {
    connection: McpConnectionConfig,
    namespace: Option<ToolName>,
    request_timeout_secs: u64,
    form_interactions: bool,
    validate_forms: bool,
}

impl McpClientConfig {
    /// Creates a client configuration with a 30-second request timeout.
    ///
    /// # Examples
    ///
    /// ```
    /// use tool_sdk::mcp::{McpClientConfig, McpConnectionConfig};
    /// let connection = McpConnectionConfig::StreamableHttp { endpoint: "http://127.0.0.1:8000/mcp".parse().unwrap(), headers: Default::default() };
    /// let config = McpClientConfig::new(connection);
    /// let value = serde_json::to_value(&config).unwrap();
    /// assert_eq!(value["request_timeout_secs"], 30);
    /// assert_eq!(value["form_interactions"], false);
    /// ```
    ///
    #[must_use]
    pub fn new(connection: McpConnectionConfig) -> Self {
        Self {
            connection,
            namespace: None,
            request_timeout_secs: 30,
            form_interactions: false,
            validate_forms: false,
        }
    }

    /// Applies a caller-visible namespace to discovered names.
    ///
    /// # Errors
    ///
    /// Returns [`ToolError::InvalidConfiguration`] if the namespace is not a valid [`ToolName`].
    ///
    /// # Examples
    ///
    /// ```
    /// use tool_sdk::mcp::{McpClientConfig, McpConnectionConfig};
    /// let connection = McpConnectionConfig::StreamableHttp { endpoint: "http://127.0.0.1:8000/mcp".parse().unwrap(), headers: Default::default() };
    /// let config = McpClientConfig::new(connection);
    /// let config = config.with_namespace("search")?;
    /// assert_eq!(serde_json::to_value(config).unwrap()["namespace"], "search");
    ///
    /// # Ok::<(), tool_sdk::ToolError>(())
    /// ```
    ///
    pub fn with_namespace<N>(mut self, namespace: N) -> Result<Self, ToolError>
    where
        N: TryInto<ToolName>,
        N::Error: fmt::Display,
    {
        self.namespace =
            Some(
                namespace
                    .try_into()
                    .map_err(|error| ToolError::InvalidConfiguration {
                        message: error.to_string(),
                    })?,
            );
        Ok(self)
    }

    /// Sets the nonzero per-request timeout.
    ///
    /// # Errors
    ///
    /// Returns [`ToolError::InvalidConfiguration`] for zero. Positive subsecond values become one second; other values use whole seconds.
    ///
    /// # Examples
    ///
    /// ```
    /// use tool_sdk::mcp::{McpClientConfig, McpConnectionConfig};
    /// let connection = McpConnectionConfig::StreamableHttp { endpoint: "http://127.0.0.1:8000/mcp".parse().unwrap(), headers: Default::default() };
    /// let config = McpClientConfig::new(connection);
    /// assert!(config.clone().with_request_timeout(std::time::Duration::ZERO).is_err());
    /// let config = config.with_request_timeout(std::time::Duration::from_secs(5))?;
    /// assert_eq!(serde_json::to_value(config).unwrap()["request_timeout_secs"], 5);
    ///
    /// # Ok::<(), tool_sdk::ToolError>(())
    /// ```
    ///
    pub fn with_request_timeout(mut self, timeout: Duration) -> Result<Self, ToolError> {
        if timeout.is_zero() {
            return Err(ToolError::InvalidConfiguration {
                message: "MCP request timeout must be greater than zero".to_owned(),
            });
        }
        self.request_timeout_secs = timeout.as_secs().max(1);
        Ok(self)
    }

    /// Enables form interactions and selects response schema validation.
    /// Passing false still enables forms. Replies must remain JSON objects; only schema
    /// constraints are relaxed.
    ///
    /// # Examples
    ///
    /// ```
    /// use tool_sdk::mcp::{McpClientConfig, McpConnectionConfig};
    /// let connection = McpConnectionConfig::StreamableHttp { endpoint: "http://127.0.0.1:8000/mcp".parse().unwrap(), headers: Default::default() };
    /// let config = McpClientConfig::new(connection);
    /// let config = config.with_form_validation(false);
    /// let value = serde_json::to_value(config).unwrap();
    /// assert_eq!(value["form_interactions"], true);
    /// assert_eq!(value["validate_forms"], false);
    /// ```
    ///
    #[must_use]
    pub fn with_form_validation(mut self, enabled: bool) -> Self {
        self.form_interactions = true;
        self.validate_forms = enabled;
        self
    }

    pub(crate) fn connection(&self) -> &McpConnectionConfig {
        &self.connection
    }

    pub(crate) fn namespace(&self) -> Option<&ToolName> {
        self.namespace.as_ref()
    }

    pub(crate) fn request_timeout(&self) -> Duration {
        Duration::from_secs(self.request_timeout_secs)
    }

    pub(crate) fn validates_forms(&self) -> bool {
        self.validate_forms
    }

    pub(crate) fn supports_forms(&self) -> bool {
        self.form_interactions
    }
}

/// Policy for resolving interactions while serving a Toolset over MCP.
#[cfg(feature = "mcp-server")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpServerInteractionPolicy {
    /// Use MCP elicitation for form requests when the client advertises support.
    ElicitFormIfClientSupports,
    /// Reject an interaction-requesting call with an MCP request error.
    #[default]
    RejectWithToolError,
}

/// Validated independent MCP server configuration.
/// Available with the `mcp-server` feature. Constructing this value does not bind a socket.
///
/// # Examples
///
/// ```
/// use tool_sdk::mcp::McpServerConfig;
/// let config = McpServerConfig::new("127.0.0.1:8000".parse().unwrap(), "/mcp", "page")?;
/// assert!(serde_json::to_value(&config).unwrap().is_object());
///
/// # Ok::<(), tool_sdk::ToolError>(())
/// ```
///
#[cfg(feature = "mcp-server")]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerConfig {
    bind: SocketAddr,
    endpoint_path: String,
    cursor_prefix: String,
    interaction_policy: McpServerInteractionPolicy,
    shutdown_timeout_secs: u64,
}

#[cfg(feature = "mcp-server")]
impl McpServerConfig {
    /// Creates a validated loopback server configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ToolError::InvalidConfiguration`] unless the address is loopback with a nonzero port, the endpoint starts with `/` and contains no query or fragment, and the cursor prefix is nonblank.
    ///
    /// # Examples
    ///
    /// ```
    /// use tool_sdk::mcp::McpServerConfig;
    /// let config = McpServerConfig::new("127.0.0.1:8000".parse().unwrap(), "/mcp", "page")?;
    /// let value = serde_json::to_value(&config).unwrap();
    /// assert_eq!(value["shutdown_timeout_secs"], 5);
    /// assert_eq!(value["interaction_policy"], "reject_with_tool_error");
    ///
    /// # Ok::<(), tool_sdk::ToolError>(())
    /// ```
    ///
    pub fn new(
        bind: SocketAddr,
        endpoint_path: impl Into<String>,
        cursor_prefix: impl Into<String>,
    ) -> Result<Self, ToolError> {
        let config = Self {
            bind,
            endpoint_path: endpoint_path.into(),
            cursor_prefix: cursor_prefix.into(),
            interaction_policy: McpServerInteractionPolicy::default(),
            shutdown_timeout_secs: 5,
        };
        config.validate()?;
        Ok(config)
    }

    /// Selects how the server handles tool-requested caller interactions.
    ///
    /// # Examples
    ///
    /// ```
    /// use tool_sdk::mcp::McpServerConfig;
    /// let config = McpServerConfig::new("127.0.0.1:8000".parse().unwrap(), "/mcp", "page")?;
    /// let config = config.with_interaction_policy(tool_sdk::mcp::McpServerInteractionPolicy::ElicitFormIfClientSupports);
    /// let value = serde_json::to_value(&config).unwrap();
    /// assert_eq!(value["shutdown_timeout_secs"], 5);
    /// assert_eq!(value["interaction_policy"], "elicit_form_if_client_supports");
    ///
    /// # Ok::<(), tool_sdk::ToolError>(())
    /// ```
    ///
    #[must_use]
    pub fn with_interaction_policy(mut self, policy: McpServerInteractionPolicy) -> Self {
        self.interaction_policy = policy;
        self
    }

    /// Sets the nonzero deadline for joining server shutdown.
    ///
    /// # Errors
    ///
    /// Returns [`ToolError::InvalidConfiguration`] for zero. Positive subsecond values become one second; other values use whole seconds. This bounds observation and never discards retained cleanup.
    ///
    /// # Examples
    ///
    /// ```
    /// use tool_sdk::mcp::McpServerConfig;
    /// let config = McpServerConfig::new("127.0.0.1:8000".parse().unwrap(), "/mcp", "page")?;
    /// assert!(config.clone().with_shutdown_timeout(std::time::Duration::ZERO).is_err());
    /// let config = config.with_shutdown_timeout(std::time::Duration::from_secs(10))?;
    /// let value = serde_json::to_value(&config).unwrap();
    /// assert_eq!(value["shutdown_timeout_secs"], 10);
    /// assert_eq!(value["interaction_policy"], "reject_with_tool_error");
    ///
    /// # Ok::<(), tool_sdk::ToolError>(())
    /// ```
    ///
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Result<Self, ToolError> {
        if timeout.is_zero() {
            return Err(ToolError::InvalidConfiguration {
                message: "MCP server shutdown timeout must be greater than zero".to_owned(),
            });
        }
        self.shutdown_timeout_secs = timeout.as_secs().max(1);
        Ok(self)
    }

    pub(crate) fn validate(&self) -> Result<(), ToolError> {
        if !self.bind.ip().is_loopback() || self.bind.port() == 0 {
            return Err(ToolError::InvalidConfiguration {
                message: "MCP server requires a loopback address with a nonzero port".to_owned(),
            });
        }
        if self.endpoint_path.is_empty()
            || !self.endpoint_path.starts_with('/')
            || self.endpoint_path.contains('?')
            || self.endpoint_path.contains('#')
        {
            return Err(ToolError::InvalidConfiguration {
                message: "MCP endpoint path must be absolute and contain no query or fragment"
                    .to_owned(),
            });
        }
        if self.cursor_prefix.trim().is_empty() {
            return Err(ToolError::InvalidConfiguration {
                message: "MCP pagination cursor prefix must not be empty".to_owned(),
            });
        }
        Ok(())
    }

    pub(crate) fn bind(&self) -> SocketAddr {
        self.bind
    }

    pub(crate) fn endpoint_path(&self) -> &str {
        &self.endpoint_path
    }

    pub(crate) fn cursor_prefix(&self) -> &str {
        &self.cursor_prefix
    }

    pub(crate) fn interaction_policy(&self) -> McpServerInteractionPolicy {
        self.interaction_policy
    }

    pub(crate) fn shutdown_timeout(&self) -> Duration {
        Duration::from_secs(self.shutdown_timeout_secs)
    }
}
