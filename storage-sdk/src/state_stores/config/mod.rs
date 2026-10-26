//! Built-in state-store configuration.

mod lance;
mod postgres;
mod redb;
#[cfg(feature = "sqlite")]
mod sqlite;

use serde::{Deserialize, Serialize};

use crate::StateStoreError;

pub use lance::{LanceStateStoreConfig, LanceStateStoreLocation};
pub use postgres::{PostgresStateStoreConfig, PostgresStateStoreTlsMode};
pub use redb::{RedbStateStoreConfig, RedbStateStoreLocation};
#[cfg(feature = "sqlite")]
pub use sqlite::{SqliteStateStoreConfig, SqliteStateStoreLocation};

/// Selects and configures one built-in state-store implementation.
///
/// Lance is always compiled. Redb, PostgreSQL, and SQLite/Turso require
/// `redb`, `postgres`, and `sqlite`. These values contain configuration only; pass
/// one to [`crate::StateStore::connect`] to obtain a shared store.
///
/// # Examples
///
/// ```
/// use storage_sdk::state::config::{StateStoreConfig, LanceStateStoreConfig};
/// let config = StateStoreConfig::lance(LanceStateStoreConfig::memory());
/// config.validate()?;
/// # Ok::<(), storage_sdk::StateStoreError>(())
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StateStoreConfig {
    /// Opens the embedded Lance state store.
    Lance(LanceStateStoreConfig),
    /// Opens the embedded Redb state store.
    Redb(RedbStateStoreConfig),
    /// Opens the PostgreSQL state store.
    Postgres(PostgresStateStoreConfig),
    /// Opens the embedded SQLite/Turso state store.
    #[cfg(feature = "sqlite")]
    Sqlite(SqliteStateStoreConfig),
}

impl StateStoreConfig {
    /// Selects the embedded Lance StateStore.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// let config = StateStoreConfig::lance(LanceStateStoreConfig::memory());
    /// config.validate()?;
    /// # Ok::<(), storage_sdk::StateStoreError>(())
    /// ```
    #[must_use]
    pub fn lance(config: LanceStateStoreConfig) -> Self {
        Self::Lance(config)
    }

    /// Selects Redb.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::state::config::{StateStoreConfig, RedbStateStoreConfig};
    /// let config = StateStoreConfig::redb(RedbStateStoreConfig::memory());
    /// assert!(matches!(config, StateStoreConfig::Redb(_)));
    /// config.validate()?;
    /// # Ok::<(), storage_sdk::StateStoreError>(())
    /// ```
    #[must_use]
    pub fn redb(config: RedbStateStoreConfig) -> Self {
        Self::Redb(config)
    }

    /// Selects PostgreSQL.
    ///
    /// This constructs configuration without connecting to PostgreSQL.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::state::config::{StateStoreConfig, PostgresStateStoreConfig};
    /// let config = StateStoreConfig::postgres(PostgresStateStoreConfig::localhost("app", "user"));
    /// config.validate()?;
    /// # Ok::<(), storage_sdk::StateStoreError>(())
    /// ```
    #[must_use]
    pub fn postgres(config: PostgresStateStoreConfig) -> Self {
        Self::Postgres(config)
    }

    /// Selects SQLite/Turso.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::state::config::{StateStoreConfig, SqliteStateStoreConfig};
    /// let config = StateStoreConfig::sqlite(SqliteStateStoreConfig::memory());
    /// config.validate()?;
    /// # Ok::<(), storage_sdk::StateStoreError>(())
    /// ```
    #[cfg(feature = "sqlite")]
    #[must_use]
    pub fn sqlite(config: SqliteStateStoreConfig) -> Self {
        Self::Sqlite(config)
    }

    /// Validates the selected backend configuration.
    ///
    /// This performs local checks only and does not establish connectivity.
    ///
    /// # Errors
    ///
    /// Returns [`StateStoreError::InvalidConfig`] when the selected configuration
    /// has an invalid location, connection setting, timeout, or cache bound. The
    /// selected backend configuration's `validate` method describes its bounds.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::state::config::{StateStoreConfig, RedbStateStoreConfig};
    /// let config = StateStoreConfig::redb(RedbStateStoreConfig::memory());
    /// config.validate()?;
    /// # Ok::<(), storage_sdk::StateStoreError>(())
    /// ```
    pub fn validate(&self) -> Result<(), StateStoreError> {
        match self {
            Self::Lance(config) => config.validate(),
            Self::Redb(config) => config.validate(),
            Self::Postgres(config) => config.validate(),
            #[cfg(feature = "sqlite")]
            Self::Sqlite(config) => config.validate(),
        }
    }
}

impl From<LanceStateStoreConfig> for StateStoreConfig {
    fn from(config: LanceStateStoreConfig) -> Self {
        Self::Lance(config)
    }
}

impl From<RedbStateStoreConfig> for StateStoreConfig {
    fn from(config: RedbStateStoreConfig) -> Self {
        Self::Redb(config)
    }
}

impl From<PostgresStateStoreConfig> for StateStoreConfig {
    fn from(config: PostgresStateStoreConfig) -> Self {
        Self::Postgres(config)
    }
}

#[cfg(feature = "sqlite")]
impl From<SqliteStateStoreConfig> for StateStoreConfig {
    fn from(config: SqliteStateStoreConfig) -> Self {
        Self::Sqlite(config)
    }
}
