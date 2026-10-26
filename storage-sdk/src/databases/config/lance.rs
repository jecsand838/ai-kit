use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::StorageError;

/// LanceDB embedded configuration.
///
/// This configuration accepts local filesystem directories and `memory://`.
/// Native remote routes and `file:` URLs are rejected at validation. Construction
/// does not open the database or create a directory.
///
/// # Examples
///
/// ```
/// use storage_sdk::database::config::{DatabaseConfig, LanceConfig};
/// let config = LanceConfig::embedded_path("memory://");
/// assert_eq!(config.default_vector_column.as_deref(), Some("vector"));
/// DatabaseConfig::lance(config).validate()?;
/// # Ok::<(), storage_sdk::StorageError>(())
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LanceConfig {
    /// Filesystem directory used for embedded tables, or `memory://`.
    /// Native remote and `file:` URI routes are not accepted.
    pub path: PathBuf,
    /// Default vector column used when a query omits its vector field.
    pub default_vector_column: Option<String>,
}

impl LanceConfig {
    /// Builds an embedded LanceDB configuration rooted at `path`.
    ///
    /// The default vector column is `vector`. The path is checked by
    /// [`DatabaseConfig::validate`](crate::database::config::DatabaseConfig::validate)
    /// and [`crate::Database::connect`], not by this infallible constructor.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::database::config::{DatabaseConfig, LanceConfig};
    /// let config = LanceConfig::embedded_path("data/documents");
    /// assert_eq!(config.path, std::path::PathBuf::from("data/documents"));
    /// DatabaseConfig::lance(config).validate()?;
    /// # Ok::<(), storage_sdk::StorageError>(())
    /// ```
    #[must_use]
    pub fn embedded_path(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            default_vector_column: Some("vector".into()),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), StorageError> {
        if self.path.as_os_str().is_empty() {
            return Err(StorageError::InvalidConfig(
                "LanceConfig.path must not be empty".into(),
            ));
        }
        let path = self.uri_string();
        let bytes = path.as_bytes();
        let windows_drive = bytes.len() >= 3
            && bytes[0].is_ascii_alphabetic()
            && bytes[1] == b':'
            && matches!(bytes[2], b'/' | b'\\');
        if path != "memory://" && !windows_drive && url::Url::parse(&path).is_ok() {
            return Err(StorageError::InvalidConfig(
                "LanceConfig.path accepts filesystem paths or memory://, not native URI routes"
                    .into(),
            ));
        }
        if self
            .default_vector_column
            .as_deref()
            .is_some_and(|column| column.trim().is_empty())
        {
            return Err(StorageError::InvalidConfig(
                "LanceConfig.default_vector_column must not be empty when configured".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn uri_string(&self) -> String {
        self.path.to_string_lossy().into_owned()
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}
