use std::{
    fmt,
    path::{Path, PathBuf},
    time::Duration,
};

use serde::{
    Deserialize, Deserializer, Serialize, Serializer, de,
    ser::{SerializeMap, SerializeStruct},
};

use crate::StateStoreError;

const DEFAULT_OPERATION_TIMEOUT_MS: u64 = 5_000;

/// Local SQLite-compatible state store location.
///
/// SQLite/Turso StateStore support is local embedded storage behind the
/// opt-in `sqlite` feature. File locations should point at local disk, not
/// a network filesystem.
///
/// # Examples
///
/// ```rust
/// let location = storage_kit::state::config::SqliteStateStoreLocation::Memory;
/// if !location.is_memory() {
///     return Err("expected memory location".into());
/// }
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub enum SqliteStateStoreLocation {
    /// Uses a process-local in-memory database.
    Memory,
    /// Uses a caller-selected local database file.
    File(PathBuf),
}

impl SqliteStateStoreLocation {
    /// Returns true for an in-memory location.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let location = storage_kit::state::config::SqliteStateStoreLocation::Memory;
    /// if !location.is_memory() {
    ///     return Err("expected memory location".into());
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn is_memory(&self) -> bool {
        match self {
            Self::Memory => true,
            Self::File(_) => false,
        }
    }

    pub(crate) fn path_for_turso(&self) -> Result<&str, StateStoreError> {
        match self {
            Self::Memory => Ok(":memory:"),
            Self::File(path) => Self::validate_file_path(path),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), StateStoreError> {
        match self {
            Self::Memory => Ok(()),
            Self::File(path) => Self::validate_file_path(path).map(|_| ()),
        }
    }

    fn validate_file_path(path: &Path) -> Result<&str, StateStoreError> {
        let value = path.to_str().ok_or_else(|| {
            StateStoreError::invalid_config("SQLite state store file path must be valid UTF-8")
        })?;
        if value.is_empty() {
            return Err(StateStoreError::invalid_config(
                "SQLite state store file path must not be empty",
            ));
        }
        if value.chars().any(|character| character == '\0') {
            return Err(StateStoreError::invalid_config(
                "SQLite state store file path must not contain NUL",
            ));
        }
        Ok(value)
    }

    fn from_json_value(value: &serde_json::Value) -> Result<Self, String> {
        if value.as_str() == Some("Memory") {
            return Ok(Self::Memory);
        }
        let object = value.as_object().ok_or_else(|| {
            "SqliteStateStoreLocation must be `Memory` or `{ File: ... }`".to_owned()
        })?;
        if object.len() != 1 {
            return Err(
                "SqliteStateStoreLocation object must contain exactly one variant".to_owned(),
            );
        }
        let file = object
            .get("File")
            .ok_or_else(|| "SqliteStateStoreLocation object must contain `File`".to_owned())?;
        let path = file
            .as_str()
            .ok_or_else(|| "SqliteStateStoreLocation.File must be a string".to_owned())?;
        Ok(Self::File(PathBuf::from(path)))
    }
}

impl Clone for SqliteStateStoreLocation {
    fn clone(&self) -> Self {
        match self {
            Self::Memory => Self::Memory,
            Self::File(path) => Self::File(path.clone()),
        }
    }
}

impl fmt::Debug for SqliteStateStoreLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Memory => f.write_str("Memory"),
            Self::File(path) => f.debug_tuple("File").field(path).finish(),
        }
    }
}

impl PartialEq for SqliteStateStoreLocation {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Memory, Self::Memory) => true,
            (Self::File(left), Self::File(right)) => left == right,
            _ => false,
        }
    }
}

impl Eq for SqliteStateStoreLocation {}

impl Serialize for SqliteStateStoreLocation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Memory => serializer.serialize_str("Memory"),
            Self::File(path) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("File", path)?;
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for SqliteStateStoreLocation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        Self::from_json_value(&value).map_err(de::Error::custom)
    }
}

/// SQLite-compatible local state store configuration.
///
/// This config selects the Turso-backed local StateStore implementation when
/// the `sqlite` feature is enabled. It is intended for embedded local state;
/// PostgreSQL should be used for remote durable StateStore deployments.
///
/// # Examples
///
/// ```rust
/// let config = storage_kit::state::config::SqliteStateStoreConfig::memory()
///     .with_operation_timeout(std::time::Duration::from_millis(250));
/// let state_config = storage_kit::state::config::StateStoreConfig::sqlite(config.clone());
///
/// assert!(config.location().is_memory());
/// assert_eq!(config.operation_timeout().as_millis(), 250);
/// assert_eq!(state_config.validate(), Ok(()));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct SqliteStateStoreConfig {
    location: SqliteStateStoreLocation,
    operation_timeout_ms: u64,
}

impl SqliteStateStoreConfig {
    /// Builds an in-memory SQLite-compatible state store config.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let config = storage_kit::state::config::SqliteStateStoreConfig::memory();
    /// if !config.location().is_memory() {
    ///     return Err("expected memory location".into());
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn memory() -> Self {
        Self {
            location: SqliteStateStoreLocation::Memory,
            operation_timeout_ms: DEFAULT_OPERATION_TIMEOUT_MS,
        }
    }

    /// Builds a file-backed SQLite-compatible state store config.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let config = storage_kit::state::config::SqliteStateStoreConfig::file("target/state_store_docs.db");
    /// if config.location().is_memory() {
    ///     return Err("expected file location".into());
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn file(path: impl Into<PathBuf>) -> Self {
        Self {
            location: SqliteStateStoreLocation::File(path.into()),
            operation_timeout_ms: DEFAULT_OPERATION_TIMEOUT_MS,
        }
    }

    /// Replaces the operation timeout.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let config = storage_kit::state::config::SqliteStateStoreConfig::memory()
    ///     .with_operation_timeout(std::time::Duration::from_millis(250));
    /// if config.operation_timeout().as_millis() != 250 {
    ///     return Err("unexpected timeout".into());
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn with_operation_timeout(mut self, timeout: Duration) -> Self {
        let millis = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
        self.operation_timeout_ms = millis;
        self
    }

    /// Returns the configured operation timeout.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let config = storage_kit::state::config::SqliteStateStoreConfig::memory();
    /// if config.operation_timeout().as_secs() != 5 {
    ///     return Err("unexpected timeout".into());
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn operation_timeout(&self) -> Duration {
        Duration::from_millis(self.operation_timeout_ms)
    }

    /// Returns the local SQLite location.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let config = storage_kit::state::config::SqliteStateStoreConfig::memory();
    /// if !config.location().is_memory() {
    ///     return Err("expected memory location".into());
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn location(&self) -> &SqliteStateStoreLocation {
        &self.location
    }

    /// Validates location and timeout settings.
    ///
    /// # Errors
    ///
    /// Returns an invalid-config error when the timeout is zero or the file
    /// path cannot be passed to Turso as a valid UTF-8 path.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let config = storage_kit::state::config::SqliteStateStoreConfig::memory();
    /// config.validate()?;
    /// # Ok::<(), storage_kit::StateStoreError>(())
    /// ```
    pub fn validate(&self) -> Result<(), StateStoreError> {
        if self.operation_timeout_ms == 0 {
            return Err(StateStoreError::invalid_config(
                "SQLite state store operation_timeout must be greater than zero",
            ));
        }
        self.location.validate()
    }

    pub(crate) fn path_for_turso(&self) -> Result<&str, StateStoreError> {
        self.location.path_for_turso()
    }

    pub(crate) fn operation_timeout_ms(&self) -> u64 {
        self.operation_timeout_ms
    }

    pub(crate) fn from_json_value(value: &serde_json::Value) -> Result<Self, String> {
        let object = value
            .as_object()
            .ok_or_else(|| "SqliteStateStoreConfig must be an object".to_owned())?;
        let location = object
            .get("location")
            .ok_or_else(|| "SqliteStateStoreConfig.location is required".to_owned())
            .and_then(SqliteStateStoreLocation::from_json_value)?;
        let timeout = object
            .get("operation_timeout_ms")
            .ok_or_else(|| "SqliteStateStoreConfig.operation_timeout_ms is required".to_owned())?
            .as_u64()
            .ok_or_else(|| {
                "SqliteStateStoreConfig.operation_timeout_ms must be a u64".to_owned()
            })?;
        Ok(Self {
            location,
            operation_timeout_ms: timeout,
        })
    }
}

impl Clone for SqliteStateStoreConfig {
    fn clone(&self) -> Self {
        Self {
            location: self.location.clone(),
            operation_timeout_ms: self.operation_timeout_ms,
        }
    }
}

impl fmt::Debug for SqliteStateStoreConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteStateStoreConfig")
            .field("location", &self.location)
            .field("operation_timeout_ms", &self.operation_timeout_ms)
            .finish()
    }
}

impl Serialize for SqliteStateStoreConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SqliteStateStoreConfig", 2)?;
        state.serialize_field("location", &self.location)?;
        state.serialize_field("operation_timeout_ms", &self.operation_timeout_ms)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for SqliteStateStoreConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        Self::from_json_value(&value).map_err(de::Error::custom)
    }
}
