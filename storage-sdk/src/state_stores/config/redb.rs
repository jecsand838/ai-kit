use std::{
    ffi::OsStr,
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
const DEFAULT_CACHE_SIZE_BYTES: usize = 64 * 1024 * 1024;

/// Local Redb StateStore location.
///
/// Redb is an opt-in embedded StateStore backend. Memory locations
/// are process-local. File locations persist embedded control-plane state to a
/// caller-selected local path.
///
/// # Examples
///
/// ```rust
/// let memory = storage_sdk::state::config::RedbStateStoreLocation::Memory;
/// let file = storage_sdk::state::config::RedbStateStoreLocation::File(
///     std::env::temp_dir().join("storage-sdk-state.redb"),
/// );
/// let _ = (memory, file);
/// ```
pub enum RedbStateStoreLocation {
    /// Uses a process-local Redb in-memory backend.
    Memory,
    /// Uses a caller-selected local Redb database file.
    File(PathBuf),
}

impl RedbStateStoreLocation {
    /// Returns true for an in-memory Redb location.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let location = storage_sdk::state::config::RedbStateStoreLocation::Memory;
    /// if !location.is_memory() {
    ///     return Err(storage_sdk::StateStoreError::InvalidRequest(
    ///         "expected memory location".to_owned(),
    ///     ));
    /// }
    /// # Ok::<(), storage_sdk::StateStoreError>(())
    /// ```
    #[must_use]
    pub fn is_memory(&self) -> bool {
        match self {
            Self::Memory => true,
            Self::File(_) => false,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), StateStoreError> {
        match self {
            Self::Memory => Ok(()),
            Self::File(path) => RedbStateStorePath::new(path).validate(),
        }
    }

    fn from_json_value(value: &serde_json::Value) -> Result<Self, String> {
        if value.as_str() == Some("Memory") {
            return Ok(Self::Memory);
        }
        let object = value.as_object().ok_or_else(|| {
            "RedbStateStoreLocation must be `Memory` or `{ File: ... }`".to_owned()
        })?;
        if object.len() != 1 {
            return Err(
                "RedbStateStoreLocation object must contain exactly one variant".to_owned(),
            );
        }
        let file = object
            .get("File")
            .ok_or_else(|| "RedbStateStoreLocation object must contain `File`".to_owned())?;
        let path = file
            .as_str()
            .ok_or_else(|| "RedbStateStoreLocation.File must be a string".to_owned())?;
        Ok(Self::File(PathBuf::from(path)))
    }
}

impl Clone for RedbStateStoreLocation {
    fn clone(&self) -> Self {
        match self {
            Self::Memory => Self::Memory,
            Self::File(path) => Self::File(path.clone()),
        }
    }
}

impl fmt::Debug for RedbStateStoreLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Memory => f.write_str("Memory"),
            Self::File(path) => f.debug_tuple("File").field(path).finish(),
        }
    }
}

impl PartialEq for RedbStateStoreLocation {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Memory, Self::Memory) => true,
            (Self::File(left), Self::File(right)) => left == right,
            _ => false,
        }
    }
}

impl Eq for RedbStateStoreLocation {}

impl Serialize for RedbStateStoreLocation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Memory => serializer.serialize_str("Memory"),
            Self::File(path) => {
                let value = path.to_str().ok_or_else(|| {
                    serde::ser::Error::custom(
                        "RedbStateStoreLocation.File must be valid UTF-8 to serialize as JSON",
                    )
                })?;
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("File", value)?;
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for RedbStateStoreLocation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        Self::from_json_value(&value).map_err(de::Error::custom)
    }
}

/// Redb embedded StateStore configuration.
///
/// This always-available config selects the Redb StateStore backend, which
/// requires the `redb` feature. Use
/// [`Self::memory`] for process-local embedded state and
/// [`Self::file`] for persisted embedded state.
///
/// # Examples
///
/// ```rust
/// let config = storage_sdk::state::config::RedbStateStoreConfig::memory();
/// let state_config = storage_sdk::state::config::StateStoreConfig::redb(config);
/// let _ = state_config;
/// ```
pub struct RedbStateStoreConfig {
    location: RedbStateStoreLocation,
    operation_timeout_ms: u64,
    cache_size_bytes: usize,
}

impl RedbStateStoreConfig {
    /// Builds an in-memory Redb StateStore config.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let config = storage_sdk::state::config::RedbStateStoreConfig::memory();
    /// if !config.location().is_memory() {
    ///     return Err(storage_sdk::StateStoreError::InvalidRequest(
    ///         "expected memory location".to_owned(),
    ///     ));
    /// }
    /// # Ok::<(), storage_sdk::StateStoreError>(())
    /// ```
    #[must_use]
    pub fn memory() -> Self {
        Self {
            location: RedbStateStoreLocation::Memory,
            operation_timeout_ms: DEFAULT_OPERATION_TIMEOUT_MS,
            cache_size_bytes: DEFAULT_CACHE_SIZE_BYTES,
        }
    }

    /// Builds a file-backed Redb StateStore config.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let path = std::env::temp_dir().join("storage-sdk-state.redb");
    /// let config = storage_sdk::state::config::RedbStateStoreConfig::file(path);
    /// let _ = config;
    /// ```
    #[must_use]
    pub fn file(path: impl Into<PathBuf>) -> Self {
        Self {
            location: RedbStateStoreLocation::File(path.into()),
            operation_timeout_ms: DEFAULT_OPERATION_TIMEOUT_MS,
            cache_size_bytes: DEFAULT_CACHE_SIZE_BYTES,
        }
    }

    /// Replaces the operation timeout.
    ///
    /// # Errors
    ///
    /// Returns an invalid-config error when the timeout is zero or cannot be
    /// represented as milliseconds.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let config = storage_sdk::state::config::RedbStateStoreConfig::memory()
    ///     .with_operation_timeout(std::time::Duration::from_secs(2))?;
    /// let _ = config;
    /// # Ok::<(), storage_sdk::StateStoreError>(())
    /// ```
    pub fn with_operation_timeout(mut self, timeout: Duration) -> Result<Self, StateStoreError> {
        let millis = u64::try_from(timeout.as_millis()).map_err(|err| {
            StateStoreError::invalid_config({
                let mut message = String::from("Redb state store operation_timeout overflow: ");
                message.push_str(&err.to_string());
                message
            })
        })?;
        if millis == 0 {
            return Err(StateStoreError::invalid_config(
                "Redb state store operation_timeout must be greater than zero",
            ));
        }
        self.operation_timeout_ms = millis;
        Ok(self)
    }

    /// Replaces the Redb cache size in bytes.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let config = storage_sdk::state::config::RedbStateStoreConfig::memory()
    ///     .with_cache_size_bytes(64 * 1024 * 1024);
    /// let _ = config;
    /// ```
    #[must_use]
    pub fn with_cache_size_bytes(mut self, bytes: usize) -> Self {
        self.cache_size_bytes = bytes;
        self
    }

    /// Returns the configured operation timeout.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let timeout = storage_sdk::state::config::RedbStateStoreConfig::memory().operation_timeout();
    /// let _ = timeout;
    /// ```
    #[must_use]
    pub fn operation_timeout(&self) -> Duration {
        Duration::from_millis(self.operation_timeout_ms)
    }

    /// Returns the configured Redb cache size in bytes.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let bytes = storage_sdk::state::config::RedbStateStoreConfig::memory().cache_size_bytes();
    /// let _ = bytes;
    /// ```
    #[must_use]
    pub fn cache_size_bytes(&self) -> usize {
        self.cache_size_bytes
    }

    /// Returns the configured Redb location.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let config = storage_sdk::state::config::RedbStateStoreConfig::memory();
    /// let location = config.location();
    /// let _ = location;
    /// ```
    #[must_use]
    pub fn location(&self) -> &RedbStateStoreLocation {
        &self.location
    }

    /// Validates Redb location, timeout, and cache settings.
    ///
    /// # Errors
    ///
    /// Returns an invalid-config error when a numeric setting is zero or a
    /// file path is not valid for Redb construction.
    ///
    /// # Examples
    ///
    /// ```rust
    /// storage_sdk::state::config::RedbStateStoreConfig::memory().validate()?;
    /// # Ok::<(), storage_sdk::StateStoreError>(())
    /// ```
    pub fn validate(&self) -> Result<(), StateStoreError> {
        if self.operation_timeout_ms == 0 {
            return Err(StateStoreError::invalid_config(
                "Redb state store operation_timeout must be greater than zero",
            ));
        }
        if self.cache_size_bytes == 0 {
            return Err(StateStoreError::invalid_config(
                "Redb state store cache_size_bytes must be greater than zero",
            ));
        }
        self.location.validate()
    }

    #[cfg(feature = "redb")]
    pub(crate) fn operation_timeout_ms(&self) -> u64 {
        self.operation_timeout_ms
    }

    pub(crate) fn from_json_value(value: &serde_json::Value) -> Result<Self, String> {
        let object = value
            .as_object()
            .ok_or_else(|| "RedbStateStoreConfig must be an object".to_owned())?;
        let location = object
            .get("location")
            .ok_or_else(|| "RedbStateStoreConfig.location is required".to_owned())
            .and_then(RedbStateStoreLocation::from_json_value)?;
        let operation_timeout_ms = object
            .get("operation_timeout_ms")
            .ok_or_else(|| "RedbStateStoreConfig.operation_timeout_ms is required".to_owned())?
            .as_u64()
            .ok_or_else(|| "RedbStateStoreConfig.operation_timeout_ms must be a u64".to_owned())?;
        let cache_size_u64 = object
            .get("cache_size_bytes")
            .ok_or_else(|| "RedbStateStoreConfig.cache_size_bytes is required".to_owned())?
            .as_u64()
            .ok_or_else(|| "RedbStateStoreConfig.cache_size_bytes must be a u64".to_owned())?;
        let cache_size_bytes = usize::try_from(cache_size_u64).map_err(|err| {
            let mut message = String::from("RedbStateStoreConfig.cache_size_bytes overflow: ");
            message.push_str(&err.to_string());
            message
        })?;
        Ok(Self {
            location,
            operation_timeout_ms,
            cache_size_bytes,
        })
    }
}

impl Clone for RedbStateStoreConfig {
    fn clone(&self) -> Self {
        Self {
            location: self.location.clone(),
            operation_timeout_ms: self.operation_timeout_ms,
            cache_size_bytes: self.cache_size_bytes,
        }
    }
}

impl fmt::Debug for RedbStateStoreConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedbStateStoreConfig")
            .field("location", &self.location)
            .field("operation_timeout_ms", &self.operation_timeout_ms)
            .field("cache_size_bytes", &self.cache_size_bytes)
            .finish()
    }
}

impl PartialEq for RedbStateStoreConfig {
    fn eq(&self, other: &Self) -> bool {
        self.location == other.location
            && self.operation_timeout_ms == other.operation_timeout_ms
            && self.cache_size_bytes == other.cache_size_bytes
    }
}

impl Eq for RedbStateStoreConfig {}

impl Serialize for RedbStateStoreConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("RedbStateStoreConfig", 3)?;
        state.serialize_field("location", &self.location)?;
        state.serialize_field("operation_timeout_ms", &self.operation_timeout_ms)?;
        state.serialize_field("cache_size_bytes", &self.cache_size_bytes)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for RedbStateStoreConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        Self::from_json_value(&value).map_err(de::Error::custom)
    }
}

struct RedbStateStorePath<'a> {
    path: &'a Path,
}

impl<'a> RedbStateStorePath<'a> {
    fn new(path: &'a Path) -> Self {
        Self { path }
    }

    fn validate(&self) -> Result<(), StateStoreError> {
        if self.path.as_os_str().is_empty() {
            return Err(StateStoreError::invalid_config(
                "Redb state store file path must not be empty",
            ));
        }
        if self.contains_nul(self.path.as_os_str()) {
            return Err(StateStoreError::invalid_config(
                "Redb state store file path must not contain NUL",
            ));
        }
        Ok(())
    }

    fn contains_nul(&self, value: &OsStr) -> bool {
        value.as_encoded_bytes().contains(&0)
    }
}
