use std::{fmt, time::Duration};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de, ser::SerializeStruct};

use crate::{Password, StateStoreError};

const DEFAULT_POSTGRES_PORT: u16 = 5432;
const DEFAULT_OPERATION_TIMEOUT_MS: u64 = 5_000;

/// TLS behavior for the initial PostgreSQL StateStore backend.
///
/// The first Postgres backend intentionally supports only `NoTls`.
///
/// # Examples
///
/// ```rust
/// let tls = storage_kit::state::config::PostgresStateStoreTlsMode::Disable;
/// if tls != storage_kit::state::config::PostgresStateStoreTlsMode::Disable {
///     return Err("unexpected TLS mode".into());
/// }
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub enum PostgresStateStoreTlsMode {
    /// Connect with `tokio_postgres::NoTls`.
    Disable,
}

impl Clone for PostgresStateStoreTlsMode {
    fn clone(&self) -> Self {
        *self
    }
}

impl Copy for PostgresStateStoreTlsMode {}

impl fmt::Debug for PostgresStateStoreTlsMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disable => f.write_str("Disable"),
        }
    }
}

impl PartialEq for PostgresStateStoreTlsMode {
    fn eq(&self, other: &Self) -> bool {
        matches!((self, other), (Self::Disable, Self::Disable))
    }
}

impl Eq for PostgresStateStoreTlsMode {}

impl Serialize for PostgresStateStoreTlsMode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Disable => serializer.serialize_str("Disable"),
        }
    }
}

impl<'de> Deserialize<'de> for PostgresStateStoreTlsMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match String::deserialize(deserializer)?.as_str() {
            "Disable" => Ok(Self::Disable),
            other => Err(de::Error::unknown_variant(other, &["Disable"])),
        }
    }
}

/// PostgreSQL StateStore configuration.
///
/// PostgreSQL StateStore support requires the `postgres` feature and is intended
/// for remote durable control-plane state. This always-available config stays
/// pure data until it is passed
/// to [`crate::state::config::StateStoreConfig::postgres`] and opened through
/// [`crate::StateStore::connect`]. Password bytes are only exposed at the
/// final authentication boundary inside the private PostgreSQL backend.
///
/// # Examples
///
/// ```rust
/// let password = storage_kit::state::config::Password::from_string("state-store-secret")?;
/// let config = storage_kit::state::config::PostgresStateStoreConfig::localhost(
///     "storage_kit_state",
///     "storage_kit",
/// )
/// .with_password(password)
/// .with_port(15432)
/// .with_operation_timeout(std::time::Duration::from_secs(2))?;
/// let state_config = storage_kit::state::config::StateStoreConfig::postgres(config.clone());
///
/// assert_eq!(config.host(), "127.0.0.1");
/// assert_eq!(config.port(), 15432);
/// assert_eq!(config.tls_mode(), storage_kit::state::config::PostgresStateStoreTlsMode::Disable);
/// assert!(!format!("{config:?}").contains("state-store-secret"));
/// assert_eq!(state_config.validate(), Ok(()));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct PostgresStateStoreConfig {
    host: String,
    port: u16,
    database: String,
    username: String,
    password: Option<Password>,
    tls_mode: PostgresStateStoreTlsMode,
    operation_timeout_ms: u64,
}

impl PostgresStateStoreConfig {
    /// Builds a PostgreSQL StateStore config.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let config = storage_kit::state::config::PostgresStateStoreConfig::new(
    ///     "127.0.0.1",
    ///     "storage_kit_state",
    ///     "storage_kit",
    /// );
    /// if config.host() != "127.0.0.1" {
    ///     return Err("unexpected host".into());
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn new(
        host: impl Into<String>,
        database: impl Into<String>,
        username: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port: DEFAULT_POSTGRES_PORT,
            database: database.into(),
            username: username.into(),
            password: None,
            tls_mode: PostgresStateStoreTlsMode::Disable,
            operation_timeout_ms: DEFAULT_OPERATION_TIMEOUT_MS,
        }
    }

    /// Builds a PostgreSQL StateStore config for `127.0.0.1`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let config = storage_kit::state::config::PostgresStateStoreConfig::localhost(
    ///     "storage_kit_state",
    ///     "storage_kit",
    /// );
    /// if config.host() != "127.0.0.1" {
    ///     return Err("unexpected host".into());
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn localhost(database: impl Into<String>, username: impl Into<String>) -> Self {
        Self::new("127.0.0.1", database, username)
    }

    /// Replaces the PostgreSQL server port.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let config = storage_kit::state::config::PostgresStateStoreConfig::localhost("db", "user")
    ///     .with_port(15432);
    /// if config.port() != 15432 {
    ///     return Err("unexpected port".into());
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Sets the password used by PostgreSQL authentication.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let password = storage_kit::state::config::Password::from_string("secret")?;
    /// let config = storage_kit::state::config::PostgresStateStoreConfig::localhost("db", "user")
    ///     .with_password(password);
    /// if config.password().is_none() {
    ///     return Err("expected password".into());
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn with_password(mut self, password: Password) -> Self {
        self.password = Some(password);
        self
    }

    /// Clears any configured password.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let password = storage_kit::state::config::Password::from_string("secret")?;
    /// let config = storage_kit::state::config::PostgresStateStoreConfig::localhost("db", "user")
    ///     .with_password(password)
    ///     .without_password();
    /// if config.password().is_some() {
    ///     return Err("unexpected password".into());
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn without_password(mut self) -> Self {
        self.password = None;
        self
    }

    /// Replaces the per-operation timeout.
    ///
    /// # Errors
    ///
    /// Returns an invalid-config error when the timeout cannot be represented
    /// as milliseconds.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let config = storage_kit::state::config::PostgresStateStoreConfig::localhost("db", "user")
    ///     .with_operation_timeout(std::time::Duration::from_millis(250))?;
    /// if config.operation_timeout().as_millis() != 250 {
    ///     return Err("unexpected timeout".into());
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn with_operation_timeout(mut self, timeout: Duration) -> Result<Self, StateStoreError> {
        self.operation_timeout_ms = u64::try_from(timeout.as_millis()).map_err(|err| {
            StateStoreError::invalid_config({
                let mut message =
                    String::from("PostgreSQL state store operation_timeout overflow: ");
                message.push_str(&err.to_string());
                message
            })
        })?;
        Ok(self)
    }

    /// Returns the configured host.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let config = storage_kit::state::config::PostgresStateStoreConfig::new(
    ///     "db.internal",
    ///     "storage_kit_state",
    ///     "storage_kit",
    /// );
    /// assert_eq!(config.host(), "db.internal");
    /// ```
    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the configured port.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let config = storage_kit::state::config::PostgresStateStoreConfig::localhost("db", "user")
    ///     .with_port(15432);
    /// assert_eq!(config.port(), 15432);
    /// ```
    #[must_use]
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Returns the configured database name.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let config = storage_kit::state::config::PostgresStateStoreConfig::localhost(
    ///     "storage_kit_state",
    ///     "storage_kit",
    /// );
    /// assert_eq!(config.database(), "storage_kit_state");
    /// ```
    #[must_use]
    pub fn database(&self) -> &str {
        &self.database
    }

    /// Returns the configured username.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let config = storage_kit::state::config::PostgresStateStoreConfig::localhost(
    ///     "storage_kit_state",
    ///     "storage_kit",
    /// );
    /// assert_eq!(config.username(), "storage_kit");
    /// ```
    #[must_use]
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Returns the configured password, if present.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let password = storage_kit::state::config::Password::from_string("secret")?;
    /// let config = storage_kit::state::config::PostgresStateStoreConfig::localhost("db", "user")
    ///     .with_password(password);
    /// assert!(config.password().is_some());
    /// # Ok::<(), storage_kit::StorageError>(())
    /// ```
    #[must_use]
    pub fn password(&self) -> Option<&Password> {
        self.password.as_ref()
    }

    /// Returns the configured TLS mode.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let config = storage_kit::state::config::PostgresStateStoreConfig::localhost("db", "user");
    /// assert_eq!(
    ///     config.tls_mode(),
    ///     storage_kit::state::config::PostgresStateStoreTlsMode::Disable
    /// );
    /// ```
    #[must_use]
    pub fn tls_mode(&self) -> PostgresStateStoreTlsMode {
        self.tls_mode
    }

    /// Returns the configured operation timeout.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let config = storage_kit::state::config::PostgresStateStoreConfig::localhost("db", "user")
    ///     .with_operation_timeout(std::time::Duration::from_millis(750))?;
    /// assert_eq!(config.operation_timeout().as_millis(), 750);
    /// # Ok::<(), storage_kit::StateStoreError>(())
    /// ```
    #[must_use]
    pub fn operation_timeout(&self) -> Duration {
        Duration::from_millis(self.operation_timeout_ms)
    }

    #[cfg(feature = "postgres")]
    pub(crate) fn operation_timeout_ms(&self) -> u64 {
        self.operation_timeout_ms
    }

    /// Validates PostgreSQL StateStore configuration.
    ///
    /// # Errors
    ///
    /// Returns an invalid-config error when text fields, port, or operation
    /// timeout are unusable.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let config = storage_kit::state::config::PostgresStateStoreConfig::localhost("db", "user");
    /// config.validate()?;
    /// let invalid = storage_kit::state::config::PostgresStateStoreConfig::new("", "db", "user");
    /// assert!(matches!(
    ///     invalid.validate(),
    ///     Err(storage_kit::StateStoreError::InvalidConfig(_))
    /// ));
    /// # Ok::<(), storage_kit::StateStoreError>(())
    /// ```
    pub fn validate(&self) -> Result<(), StateStoreError> {
        PostgresConfigText::new("PostgreSQL state store host", &self.host).validate()?;
        PostgresConfigText::new("PostgreSQL state store database", &self.database).validate()?;
        PostgresConfigText::new("PostgreSQL state store username", &self.username).validate()?;
        if self.port == 0 {
            return Err(StateStoreError::invalid_config(
                "PostgreSQL state store port must be greater than zero",
            ));
        }
        if self.operation_timeout_ms == 0 {
            return Err(StateStoreError::invalid_config(
                "PostgreSQL state store operation_timeout must be greater than zero",
            ));
        }
        Ok(())
    }

    pub(crate) fn from_json_value(value: &serde_json::Value) -> Result<Self, String> {
        let object = value
            .as_object()
            .ok_or_else(|| "PostgresStateStoreConfig must be an object".to_owned())?;
        let host = required_string(object, "host")?;
        let port = required_u64(object, "port")
            .and_then(|value| u16::try_from(value).map_err(|err| err.to_string()))?;
        let database = required_string(object, "database")?;
        let username = required_string(object, "username")?;
        let password = match object.get("password") {
            Some(value) if !value.is_null() => {
                Some(Password::deserialize(value).map_err(|err| err.to_string())?)
            }
            _ => None,
        };
        let tls_mode = match object.get("tls_mode") {
            Some(value) => {
                PostgresStateStoreTlsMode::deserialize(value).map_err(|err| err.to_string())?
            }
            None => PostgresStateStoreTlsMode::Disable,
        };
        let operation_timeout_ms = required_u64(object, "operation_timeout_ms")?;
        Ok(Self {
            host,
            port,
            database,
            username,
            password,
            tls_mode,
            operation_timeout_ms,
        })
    }
}

impl Clone for PostgresStateStoreConfig {
    fn clone(&self) -> Self {
        Self {
            host: self.host.clone(),
            port: self.port,
            database: self.database.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            tls_mode: self.tls_mode,
            operation_timeout_ms: self.operation_timeout_ms,
        }
    }
}

impl fmt::Debug for PostgresStateStoreConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresStateStoreConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "[redacted]"))
            .field("tls_mode", &self.tls_mode)
            .field("operation_timeout_ms", &self.operation_timeout_ms)
            .finish()
    }
}

impl Serialize for PostgresStateStoreConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("PostgresStateStoreConfig", 7)?;
        state.serialize_field("host", &self.host)?;
        state.serialize_field("port", &self.port)?;
        state.serialize_field("database", &self.database)?;
        state.serialize_field("username", &self.username)?;
        state.serialize_field("password", &self.password)?;
        state.serialize_field("tls_mode", &self.tls_mode)?;
        state.serialize_field("operation_timeout_ms", &self.operation_timeout_ms)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for PostgresStateStoreConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        Self::from_json_value(&value).map_err(de::Error::custom)
    }
}

struct PostgresConfigText<'a> {
    label: &'static str,
    value: &'a str,
}

impl<'a> PostgresConfigText<'a> {
    fn new(label: &'static str, value: &'a str) -> Self {
        Self { label, value }
    }

    fn validate(&self) -> Result<(), StateStoreError> {
        if self.value.is_empty() {
            return Err(StateStoreError::invalid_config(format!(
                "{} must not be empty",
                self.label
            )));
        }
        if self.value.trim() != self.value {
            return Err(StateStoreError::invalid_config(format!(
                "{} must not contain leading or trailing whitespace",
                self.label
            )));
        }
        if self
            .value
            .chars()
            .any(|character| character == '\0' || character.is_control())
        {
            return Err(StateStoreError::invalid_config(format!(
                "{} must not contain NUL or control characters",
                self.label
            )));
        }
        Ok(())
    }
}

fn required_string(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &'static str,
) -> Result<String, String> {
    object
        .get(field)
        .ok_or_else(|| required_message(field))
        .and_then(|value| {
            value
                .as_str()
                .map(ToOwned::to_owned)
                .ok_or_else(|| type_message(field, "a string"))
        })
}

fn required_u64(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &'static str,
) -> Result<u64, String> {
    object
        .get(field)
        .ok_or_else(|| required_message(field))
        .and_then(|value| value.as_u64().ok_or_else(|| type_message(field, "a u64")))
}

fn required_message(field: &str) -> String {
    let mut message = String::from("PostgresStateStoreConfig.");
    message.push_str(field);
    message.push_str(" is required");
    message
}

fn type_message(field: &str, expected: &str) -> String {
    let mut message = String::from("PostgresStateStoreConfig.");
    message.push_str(field);
    message.push_str(" must be ");
    message.push_str(expected);
    message
}
