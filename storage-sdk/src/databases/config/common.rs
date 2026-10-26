use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{CapabilitySet, StorageError, StorageLocator};

/// Shared policy applied to every built-in database connection.
///
/// Defaults allow writes, require no optional capabilities, and use a 15-second
/// connect timeout, a 30-second operation deadline, and two retries for safe
/// operations. The operation deadline includes internal preflight and retry
/// work. Mutations are not automatically replayed after uncertain completion.
/// Configuration is validated by [`DatabaseConfig::validate`](crate::database::config::DatabaseConfig::validate) or
/// [`crate::Database::connect`]; merely constructing this value performs no I/O.
///
/// # Examples
///
/// ```
/// use storage_sdk::database::config::{CommonConfig, DatabaseConfig, LanceConfig};
/// let common = CommonConfig { request_timeout_ms: 5_000, read_only: true,
///     ..Default::default() };
/// let config = DatabaseConfig::lance(LanceConfig::embedded_path("memory://"))
///     .with_common(common);
/// config.validate()?;
/// assert!(config.common().read_only);
/// # Ok::<(), storage_sdk::StorageError>(())
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommonConfig {
    /// Default locator used to fill omitted request scope.
    pub default_locator: Option<StorageLocator>,
    /// Connection timeout in milliseconds.
    pub connect_timeout_ms: u64,
    /// Portable operation timeout in milliseconds.
    pub request_timeout_ms: u64,
    /// Maximum retry count for retry-safe operations.
    pub max_retries: u32,
    /// Rejects mutating operations when set.
    pub read_only: bool,
    /// Optional retained capabilities required at connection time.
    pub required_capabilities: CapabilitySet,
}

impl Default for CommonConfig {
    fn default() -> Self {
        Self {
            default_locator: None,
            connect_timeout_ms: 15_000,
            request_timeout_ms: 30_000,
            max_retries: 2,
            read_only: false,
            required_capabilities: CapabilitySet::empty(),
        }
    }
}

impl CommonConfig {
    pub(crate) fn validate(&self) -> Result<(), StorageError> {
        if self.connect_timeout_ms == 0 {
            return Err(StorageError::InvalidConfig(
                "connect_timeout_ms must be greater than zero".into(),
            ));
        }
        if self.request_timeout_ms == 0 {
            return Err(StorageError::InvalidConfig(
                "request_timeout_ms must be greater than zero".into(),
            ));
        }
        if let Some(locator) = &self.default_locator {
            locator.validate_for_config("default_locator")?;
        }
        Ok(())
    }

    pub(crate) fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.connect_timeout_ms)
    }

    pub(crate) fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }

    #[cfg(any(feature = "surrealdb", feature = "qdrant"))]
    pub(crate) fn retries(&self) -> usize {
        self.max_retries as usize
    }

    pub(crate) fn ensure_mutable(&self, operation: &'static str) -> Result<(), StorageError> {
        if self.read_only {
            return Err(StorageError::backend_op(
                operation,
                format!(
                    "operation `{operation}` is disabled because the database is configured read-only"
                ),
            ));
        }
        Ok(())
    }
}
