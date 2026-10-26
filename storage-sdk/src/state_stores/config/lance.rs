use std::{path::PathBuf, time::Duration};

use serde::{Deserialize, Serialize};

use crate::StateStoreError;

/// Embedded Lance state storage. Memory belongs to one connected store;
/// directory connections share persisted state through native atomic commits.
///
/// # Examples
///
/// ```
/// use storage_sdk::state::config::LanceStateStoreLocation;
/// let location = LanceStateStoreLocation::Memory;
/// assert!(matches!(location, LanceStateStoreLocation::Memory));
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LanceStateStoreLocation {
    /// Process-local storage, shared by clones of the connected handle.
    Memory,
    /// A local directory, which may also contain Lance Database collections.
    Directory(PathBuf),
}

/// Configuration for the embedded Lance StateStore.
///
/// The timeout bounds admission and observation. An admitted commit retains
/// authority after its observer times out or is dropped. Native filesystem
/// calls may delay the final owner's joining cleanup beyond that timeout.
///
/// # Examples
///
/// ```
/// use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
/// let config = StateStoreConfig::Lance(LanceStateStoreConfig::memory());
/// config.validate()?;
/// # Ok::<(), storage_sdk::StateStoreError>(())
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LanceStateStoreConfig {
    location: LanceStateStoreLocation,
    operation_timeout_ms: u64,
}

impl LanceStateStoreConfig {
    /// Selects process-local memory with a 30-second observation timeout.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::state::config::LanceStateStoreConfig;
    /// assert_eq!(LanceStateStoreConfig::memory().operation_timeout(),
    ///     std::time::Duration::from_secs(30));
    /// ```
    #[must_use]
    pub fn memory() -> Self {
        Self {
            location: LanceStateStoreLocation::Memory,
            operation_timeout_ms: 30_000,
        }
    }

    /// Selects a local directory. No filesystem I/O occurs until connection.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::state::config::LanceStateStoreConfig;
    /// let config = LanceStateStoreConfig::directory("application-data");
    /// config.validate()?;
    /// # Ok::<(), storage_sdk::StateStoreError>(())
    /// ```
    #[must_use]
    pub fn directory(path: impl Into<PathBuf>) -> Self {
        Self {
            location: LanceStateStoreLocation::Directory(path.into()),
            operation_timeout_ms: 30_000,
        }
    }

    /// Sets the admission and observation timeout, without expiring transactions.
    ///
    /// # Errors
    ///
    /// Rejects sub-millisecond, zero, overflowing, or unrepresentable deadlines.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::state::config::LanceStateStoreConfig;
    /// let config = LanceStateStoreConfig::memory()
    ///     .with_operation_timeout(std::time::Duration::from_secs(2))?;
    /// assert_eq!(config.operation_timeout(), std::time::Duration::from_secs(2));
    /// # Ok::<(), storage_sdk::StateStoreError>(())
    /// ```
    pub fn with_operation_timeout(mut self, timeout: Duration) -> Result<Self, StateStoreError> {
        self.operation_timeout_ms = u64::try_from(timeout.as_millis()).map_err(|_| {
            StateStoreError::invalid_config("Lance state operation timeout exceeds milliseconds")
        })?;
        self.validate()?;
        Ok(self)
    }

    /// Borrows the selected storage location.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::state::config::{LanceStateStoreConfig, LanceStateStoreLocation};
    /// assert_eq!(LanceStateStoreConfig::memory().location(), &LanceStateStoreLocation::Memory);
    /// ```
    #[must_use]
    pub fn location(&self) -> &LanceStateStoreLocation {
        &self.location
    }

    /// Returns the admission and observation timeout.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::state::config::LanceStateStoreConfig;
    /// assert_eq!(LanceStateStoreConfig::memory().operation_timeout().as_secs(), 30);
    /// ```
    #[must_use]
    pub fn operation_timeout(&self) -> Duration {
        Duration::from_millis(self.operation_timeout_ms)
    }

    /// Checks local configuration without opening storage.
    ///
    /// # Errors
    ///
    /// Rejects empty, NUL-containing, non-UTF-8, or URI directory paths, and
    /// zero or unrepresentable observation timeouts. External URIs are outside
    /// this embedded backend's supported scope.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::state::config::LanceStateStoreConfig;
    /// assert!(LanceStateStoreConfig::directory("").validate().is_err());
    /// LanceStateStoreConfig::memory().validate()?;
    /// # Ok::<(), storage_sdk::StateStoreError>(())
    /// ```
    pub fn validate(&self) -> Result<(), StateStoreError> {
        if self.operation_timeout_ms == 0
            || std::time::Instant::now()
                .checked_add(self.operation_timeout())
                .is_none()
        {
            return Err(StateStoreError::invalid_config(
                "Lance state operation timeout must be positive and fit the clock range",
            ));
        }
        if let LanceStateStoreLocation::Directory(path) = &self.location {
            let value = path.to_str().ok_or_else(|| {
                StateStoreError::invalid_config("Lance state directory must be valid UTF-8")
            })?;
            if value.is_empty() || value.contains('\0') || value.contains("://") {
                return Err(StateStoreError::invalid_config(
                    "Lance state directory must be a nonempty local filesystem path without NUL",
                ));
            }
        }
        Ok(())
    }
}
