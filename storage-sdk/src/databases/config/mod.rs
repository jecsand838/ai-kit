//! Built-in database configuration.

mod common;
mod lance;
#[cfg(feature = "qdrant")]
mod qdrant;
mod surreal;

use serde::{Deserialize, Serialize};

use crate::StorageError;

pub use common::CommonConfig;
pub use lance::LanceConfig;
#[cfg(feature = "qdrant")]
pub use qdrant::{QdrantConfig, QdrantModeConfig};
pub use surreal::{SurrealConfig, SurrealModeConfig};

/// Selects and configures one built-in database implementation.
///
/// Shared portable connection policy is carried directly by each variant, so
/// construction has one configuration owner rather than a wrapper around a
/// second backend-selection enum.
///
/// LanceDB is always available. SurrealDB requires `surrealdb` and Qdrant
/// requires `qdrant`. Configuration values do not open backend resources.
/// These constructors create data only; [`crate::Database::connect`] opens it.
///
/// # Examples
///
/// ```
/// use storage_sdk::database::config::{DatabaseConfig, LanceConfig};
/// let config = DatabaseConfig::lance(LanceConfig::embedded_path("memory://"));
/// config.validate()?;
/// assert_eq!(config.common().request_timeout_ms, 30_000);
/// # Ok::<(), storage_sdk::StorageError>(())
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DatabaseConfig {
    /// Opens LanceDB.
    Lance {
        /// LanceDB-specific settings.
        config: LanceConfig,
        /// Shared portable connection policy.
        common: CommonConfig,
    },
    /// Opens SurrealDB.
    Surreal {
        /// SurrealDB-specific settings.
        config: SurrealConfig,
        /// Shared portable connection policy.
        common: CommonConfig,
    },
    /// Opens Qdrant.
    #[cfg(feature = "qdrant")]
    Qdrant {
        /// Qdrant-specific settings.
        config: QdrantConfig,
        /// Shared portable connection policy.
        common: CommonConfig,
    },
}

impl DatabaseConfig {
    /// Selects LanceDB with default shared connection policy.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::database::config::{DatabaseConfig, LanceConfig};
    /// let config = DatabaseConfig::lance(LanceConfig::embedded_path("memory://"));
    /// config.validate()?;
    /// # Ok::<(), storage_sdk::StorageError>(())
    /// ```
    #[must_use]
    pub fn lance(config: LanceConfig) -> Self {
        Self::Lance {
            config,
            common: CommonConfig::default(),
        }
    }

    /// Selects SurrealDB with default shared connection policy.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::database::config::{DatabaseConfig, SurrealConfig};
    /// let config = DatabaseConfig::surreal(SurrealConfig::default());
    /// config.validate()?;
    /// # Ok::<(), storage_sdk::StorageError>(())
    /// ```
    #[must_use]
    pub fn surreal(config: SurrealConfig) -> Self {
        Self::Surreal {
            config,
            common: CommonConfig::default(),
        }
    }

    /// Selects Qdrant with default shared connection policy.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::database::config::{DatabaseConfig, QdrantConfig};
    /// let config = DatabaseConfig::qdrant(QdrantConfig::remote_rest("http://127.0.0.1:6333"));
    /// config.validate()?;
    /// # Ok::<(), storage_sdk::StorageError>(())
    /// ```
    #[cfg(feature = "qdrant")]
    #[must_use]
    pub fn qdrant(config: QdrantConfig) -> Self {
        Self::Qdrant {
            config,
            common: CommonConfig::default(),
        }
    }

    /// Replaces the shared connection policy carried by this variant.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::database::config::{CommonConfig, DatabaseConfig, LanceConfig};
    /// let config = DatabaseConfig::lance(LanceConfig::embedded_path("memory://"))
    ///     .with_common(CommonConfig { read_only: true, ..Default::default() });
    /// assert!(config.common().read_only);
    /// # Ok::<(), storage_sdk::StorageError>(())
    /// ```
    #[must_use]
    pub fn with_common(self, common: CommonConfig) -> Self {
        match self {
            Self::Lance { config, .. } => Self::Lance { config, common },
            Self::Surreal { config, .. } => Self::Surreal { config, common },
            #[cfg(feature = "qdrant")]
            Self::Qdrant { config, .. } => Self::Qdrant { config, common },
        }
    }

    /// Borrows the shared connection policy.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::database::config::{DatabaseConfig, LanceConfig};
    /// let config = DatabaseConfig::lance(LanceConfig::embedded_path("memory://"));
    /// assert_eq!(config.common().connect_timeout_ms, 15_000);
    /// # Ok::<(), storage_sdk::StorageError>(())
    /// ```
    #[must_use]
    pub fn common(&self) -> &CommonConfig {
        match self {
            Self::Lance { common, .. } | Self::Surreal { common, .. } => common,
            #[cfg(feature = "qdrant")]
            Self::Qdrant { common, .. } => common,
        }
    }

    /// Validates shared policy and backend-specific settings.
    ///
    /// This checks configuration without opening a connection or proving service
    /// availability or request-specific capability support.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::InvalidConfig`] for zero timeouts, invalid default
    /// locator fragments, or invalid selected backend settings, such as an empty
    /// embedded path, unsupported URI route, or wrong remote URL scheme.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::database::config::{DatabaseConfig, LanceConfig};
    /// let valid = DatabaseConfig::lance(LanceConfig::embedded_path("memory://"));
    /// valid.validate()?;
    /// let invalid = DatabaseConfig::lance(LanceConfig::embedded_path("s3://bucket/data"));
    /// assert!(invalid.validate().is_err());
    /// # Ok::<(), storage_sdk::StorageError>(())
    /// ```
    pub fn validate(&self) -> Result<(), StorageError> {
        self.common().validate()?;
        match self {
            Self::Lance { config, .. } => config.validate(),
            Self::Surreal { config, .. } => config.validate(),
            #[cfg(feature = "qdrant")]
            Self::Qdrant { config, .. } => config.validate(),
        }
    }
}

impl From<LanceConfig> for DatabaseConfig {
    fn from(config: LanceConfig) -> Self {
        Self::lance(config)
    }
}

impl From<SurrealConfig> for DatabaseConfig {
    fn from(config: SurrealConfig) -> Self {
        Self::surreal(config)
    }
}

#[cfg(feature = "qdrant")]
impl From<QdrantConfig> for DatabaseConfig {
    fn from(config: QdrantConfig) -> Self {
        Self::qdrant(config)
    }
}
