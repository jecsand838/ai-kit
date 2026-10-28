use std::fmt;

use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::StorageError;

/// Validated password used by storage backend configuration.
///
/// A present password must be non-empty and not whitespace-only. Leading and
/// trailing whitespace are preserved because passwords are opaque caller-owned
/// values. Use this wrapper for SurrealDB root passwords and PostgreSQL
/// StateStore authentication.
///
/// # Examples
///
/// ```
/// let password = storage_kit::state::config::Password::from_string("postgres secret")?;
/// let config = storage_kit::state::config::PostgresStateStoreConfig::localhost("db", "user")
///     .with_password(password.clone());
///
/// assert!(!format!("{password:?}").contains("postgres secret"));
/// assert!(config.password().is_some());
/// # Ok::<(), storage_kit::StorageError>(())
/// ```
#[derive(Clone)]
pub struct Password {
    secret: SecretString,
}

impl Password {
    /// Validates and stores a password.
    ///
    /// # Errors
    /// Returns a configuration error when the secret is empty or whitespace-only.
    ///
    /// # Examples
    ///
    /// ```
    /// let password = storage_kit::state::config::Password::new(secrecy::SecretString::new("secret".into()))?;
    /// let _ = password;
    /// # Ok::<(), storage_kit::StorageError>(())
    /// ```
    pub fn new(secret: SecretString) -> Result<Self, StorageError> {
        reject_empty_secret("password", &secret)?;
        Ok(Self { secret })
    }

    /// Validates and stores a password from an owned string-like value.
    ///
    /// # Errors
    /// Returns a configuration error when the value is empty or whitespace-only.
    ///
    /// # Examples
    ///
    /// ```
    /// let password = storage_kit::state::config::Password::from_string("secret")?;
    /// let _ = password;
    /// # Ok::<(), storage_kit::StorageError>(())
    /// ```
    pub fn from_string(secret: impl Into<String>) -> Result<Self, StorageError> {
        Self::new(SecretString::new(secret.into().into()))
    }

    #[cfg(any(feature = "postgres", feature = "surrealdb"))]
    pub(crate) fn as_secret(&self) -> &SecretString {
        &self.secret
    }
}

impl fmt::Debug for Password {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Password")
            .field("secret", &"[redacted]")
            .finish()
    }
}

impl Serialize for Password {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.secret.expose_secret())
    }
}

impl<'de> Deserialize<'de> for Password {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::from_string(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

fn reject_empty_secret(label: &str, secret: &SecretString) -> Result<(), StorageError> {
    if secret.expose_secret().trim().is_empty() {
        return Err(StorageError::InvalidConfig({
            let mut message = String::from(label);
            message.push_str(" must not be empty");
            message
        }));
    }
    Ok(())
}
