use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{ApiKey, StorageError};

/// Selects the Qdrant deployment mode.
///
/// Available with the `qdrant` feature. An absent API key means unauthenticated
/// access; present keys use the validated [`ApiKey`] wrapper.
///
/// # Examples
///
/// ```
/// use storage_sdk::database::config::QdrantModeConfig;
/// let mode = QdrantModeConfig::RemoteRest { endpoint: "http://127.0.0.1:6333".to_owned(),
///     api_key: None };
/// assert!(matches!(mode, QdrantModeConfig::RemoteRest { api_key: None, .. }));
/// # Ok::<(), storage_sdk::StorageError>(())
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QdrantModeConfig {
    /// Uses qdrant-edge rooted at `path`.
    Edge {
        /// Parent directory for qdrant-edge state.
        path: PathBuf,
    },
    /// Connects to a remote REST endpoint.
    RemoteRest {
        /// REST endpoint.
        endpoint: String,
        /// Optional API key.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        api_key: Option<ApiKey>,
    },
}

/// Qdrant connection and field-selection configuration.
///
/// Available with the `qdrant` feature. Constructors select `vector` as the
/// default named dense-vector field and do not open a connection. Validate via
/// [`DatabaseConfig::validate`](crate::database::config::DatabaseConfig::validate).
///
/// # Examples
///
/// ```
/// use storage_sdk::database::config::{DatabaseConfig, QdrantConfig};
/// let config = QdrantConfig::remote_rest("http://127.0.0.1:6333");
/// assert_eq!(config.default_vector_name.as_deref(), Some("vector"));
/// DatabaseConfig::qdrant(config).validate()?;
/// # Ok::<(), storage_sdk::StorageError>(())
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QdrantConfig {
    /// Deployment mode.
    pub mode: QdrantModeConfig,
    /// Default named dense-vector field.
    pub default_vector_name: Option<String>,
}

impl QdrantConfig {
    /// Builds an embedded qdrant-edge configuration.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::database::config::{DatabaseConfig, QdrantConfig, QdrantModeConfig};
    /// let config = QdrantConfig::edge("data/qdrant");
    /// assert!(matches!(config.mode, QdrantModeConfig::Edge { .. }));
    /// DatabaseConfig::qdrant(config).validate()?;
    /// # Ok::<(), storage_sdk::StorageError>(())
    /// ```
    #[must_use]
    pub fn edge(path: impl Into<PathBuf>) -> Self {
        Self {
            mode: QdrantModeConfig::Edge { path: path.into() },
            default_vector_name: Some("vector".into()),
        }
    }

    /// Builds a remote Qdrant REST configuration.
    ///
    /// The endpoint must use HTTP or HTTPS when validated. Constructing this value
    /// does not contact the endpoint; the default API key is absent.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::database::config::{DatabaseConfig, QdrantConfig, QdrantModeConfig};
    /// let config = QdrantConfig::remote_rest("http://127.0.0.1:6333");
    /// assert!(matches!(config.mode, QdrantModeConfig::RemoteRest { api_key: None, .. }));
    /// DatabaseConfig::qdrant(config).validate()?;
    /// # Ok::<(), storage_sdk::StorageError>(())
    /// ```
    #[must_use]
    pub fn remote_rest(endpoint: impl Into<String>) -> Self {
        Self {
            mode: QdrantModeConfig::RemoteRest {
                endpoint: endpoint.into(),
                api_key: None,
            },
            default_vector_name: Some("vector".into()),
        }
    }

    /// Builds a remote Qdrant REST configuration with API-key authentication.
    ///
    /// Constructing this value performs no network I/O. The endpoint must use HTTP
    /// or HTTPS when validated. Credential contents are redacted from `Debug`.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::database::config::{DatabaseConfig, QdrantConfig, ApiKey};
    /// let key = ApiKey::from_string("example-key")?;
    /// let config = QdrantConfig::remote_rest_with_api_key("http://127.0.0.1:6333", key);
    /// assert!(!format!("{config:?}").contains("example-key"));
    /// DatabaseConfig::qdrant(config).validate()?;
    /// # Ok::<(), storage_sdk::StorageError>(())
    /// ```
    #[must_use]
    pub fn remote_rest_with_api_key(endpoint: impl Into<String>, api_key: ApiKey) -> Self {
        Self {
            mode: QdrantModeConfig::RemoteRest {
                endpoint: endpoint.into(),
                api_key: Some(api_key),
            },
            default_vector_name: Some("vector".into()),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), StorageError> {
        match &self.mode {
            QdrantModeConfig::Edge { path } => validate_path(path)?,
            QdrantModeConfig::RemoteRest { endpoint, .. } => validate_endpoint(endpoint)?,
        }
        if self
            .default_vector_name
            .as_deref()
            .is_some_and(|name| name.trim().is_empty())
        {
            return Err(StorageError::InvalidConfig(
                "QdrantConfig.default_vector_name must not be empty when configured".into(),
            ));
        }
        Ok(())
    }
}

fn validate_path(path: &Path) -> Result<(), StorageError> {
    if path.as_os_str().is_empty() {
        return Err(StorageError::InvalidConfig(
            "Qdrant edge path must not be empty".into(),
        ));
    }
    Ok(())
}

fn validate_endpoint(endpoint: &str) -> Result<(), StorageError> {
    let parsed = url::Url::parse(endpoint).map_err(|error| {
        StorageError::InvalidConfig(format!("Qdrant endpoint must be a valid URL: {error}"))
    })?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(StorageError::InvalidConfig(
            "Qdrant endpoint must use http or https".into(),
        ));
    }
    Ok(())
}
