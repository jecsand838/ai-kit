use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{Password, StorageError};

/// Selects the SurrealDB deployment mode.
///
/// # Examples
///
/// ```
/// let mode = storage_sdk::database::config::SurrealModeConfig::RemoteWs {
///     endpoint: "ws://127.0.0.1:8000".into(),
/// };
/// assert!(matches!(mode, storage_sdk::database::config::SurrealModeConfig::RemoteWs { .. }));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SurrealModeConfig {
    /// Uses the in-memory engine.
    Mem,
    /// Uses the embedded RocksDB engine rooted at `path`.
    RocksDb {
        /// Filesystem path that stores the RocksDB state.
        path: PathBuf,
    },
    /// Uses the embedded SurrealKV engine rooted at `path`.
    SurrealKv {
        /// Filesystem path that stores the SurrealKV state.
        path: PathBuf,
    },
    /// Connects to a remote websocket endpoint.
    RemoteWs {
        /// Websocket endpoint such as `ws://127.0.0.1:8000`.
        endpoint: String,
    },
    /// Connects to a remote HTTP endpoint.
    RemoteHttp {
        /// HTTP endpoint such as `http://127.0.0.1:8000`.
        endpoint: String,
    },
}

/// SurrealDB-specific backend configuration.
///
/// Text-search field selection is request-owned through
/// [`ScopedQuery::text_field`](crate::database::query::ScopedQuery::text_field)
/// on the request's text options; this config does not provide a text-field
/// fallback.
///
/// # Examples
///
/// ```
/// let config = storage_sdk::database::config::SurrealConfig::default();
/// assert!(matches!(config.mode, storage_sdk::database::config::SurrealModeConfig::Mem));
/// assert_eq!(config.namespace.as_deref(), Some("test"));
/// let request = storage_sdk::database::query::TextSearchRequest {
///     query: storage_sdk::database::query::ScopedQuery::text(
///         storage_sdk::database::StorageLocator::new("documents"),
///     ).text_field("body"),
///     text: "rust".to_owned(),
/// };
/// assert_eq!(request.query.options.text_field.as_deref(), Some("body"));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SurrealConfig {
    /// Deployment mode used to connect to SurrealDB.
    pub mode: SurrealModeConfig,
    /// Default native namespace used when the locator does not provide one.
    pub namespace: Option<String>,
    /// Default database used when the locator does not provide one.
    pub database: Option<String>,
    /// Optional root username for remote or embedded engines that require auth.
    pub username: Option<String>,
    /// Optional root password for remote or embedded engines that require auth.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<Password>,
}

impl SurrealConfig {
    pub(crate) fn validate(&self) -> Result<(), StorageError> {
        match &self.mode {
            SurrealModeConfig::Mem => {}
            SurrealModeConfig::RocksDb { path } => validate_non_empty_path("mode.path", path)?,
            SurrealModeConfig::SurrealKv { path } => validate_non_empty_path("mode.path", path)?,
            SurrealModeConfig::RemoteWs { endpoint } => {
                validate_endpoint("mode.endpoint", endpoint, &["ws", "wss"])?
            }
            SurrealModeConfig::RemoteHttp { endpoint } => {
                validate_endpoint("mode.endpoint", endpoint, &["http", "https"])?
            }
        }

        validate_optional_non_empty("namespace", self.namespace.as_deref())?;
        validate_optional_non_empty("database", self.database.as_deref())?;
        validate_optional_non_empty("username", self.username.as_deref())?;

        Ok(())
    }
}

impl Default for SurrealConfig {
    fn default() -> Self {
        Self {
            mode: SurrealModeConfig::Mem,
            namespace: Some("test".into()),
            database: Some("test".into()),
            username: None,
            password: None,
        }
    }
}

fn validate_non_empty_path(field: &str, path: &Path) -> Result<(), StorageError> {
    if path.as_os_str().is_empty() {
        return Err(StorageError::InvalidConfig(format!(
            "{field} must not be empty"
        )));
    }
    Ok(())
}

fn validate_optional_non_empty(field: &str, value: Option<&str>) -> Result<(), StorageError> {
    if let Some(value) = value
        && value.trim().is_empty()
    {
        return Err(StorageError::InvalidConfig(format!(
            "{field} must not be empty when configured"
        )));
    }
    Ok(())
}

fn validate_endpoint(field: &str, endpoint: &str, schemes: &[&str]) -> Result<(), StorageError> {
    validate_optional_non_empty(field, Some(endpoint))?;
    let parsed = url::Url::parse(endpoint).map_err(|err| {
        StorageError::InvalidConfig(format!("{field} must be a valid URL: {err}"))
    })?;
    if !schemes.iter().any(|scheme| parsed.scheme() == *scheme) {
        return Err(StorageError::InvalidConfig(format!(
            "{field} must use one of the following schemes: {}",
            schemes.join(", ")
        )));
    }
    Ok(())
}
