use std::fmt;

use reqwest::header::HeaderValue;
use secrecy::{ExposeSecret, SecretString};

use crate::KitError;
use crate::error::ValidationViolation;

/// Validated API key used by provider configuration.
///
/// The key is a nonempty, syntax-valid secret; construction neither authenticates
/// it nor proves provider permissions. Empty values,
/// whitespace-only values, and values with leading or trailing whitespace are
/// rejected at construction. Values must also be valid HTTP header values;
/// embedded control characters such as newlines are rejected.
///
/// # Examples
/// ```rust
/// let key = providers_kit::config::ApiKey::new("sk-test")?;
/// assert!(!format!("{key:?}").contains("sk-test"));
///
/// # Ok::<(), providers_kit::KitError>(())
/// ```
#[derive(Clone)]
pub struct ApiKey {
    secret: SecretString,
}

impl ApiKey {
    /// Validates and stores an API key.
    ///
    /// # Errors
    /// Returns a configuration error when the secret is empty, whitespace-only,
    /// contains leading or trailing whitespace, or is not a valid HTTP header value.
    ///
    /// # Examples
    /// ```rust
    /// let key = providers_kit::config::ApiKey::new("sk-test")?;
    /// assert!(!format!("{key:?}").contains("sk-test"));
    /// assert!(providers_kit::config::ApiKey::new(" padded ").is_err());
    /// assert!(providers_kit::config::ApiKey::new("invalid\nheader").is_err());
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    pub fn new(secret: impl Into<String>) -> Result<Self, KitError> {
        let secret = SecretString::new(secret.into().into());
        let exposed = secret.expose_secret();
        if exposed.trim().is_empty() {
            return Err(KitError::config(
                ValidationViolation::Empty {
                    field: "credentials.api_key",
                },
                "API key must not be empty",
            ));
        }
        if exposed != exposed.trim() {
            return Err(KitError::config(
                ValidationViolation::InvalidFormat {
                    field: "credentials.api_key",
                    rule: "must be unpadded",
                },
                "API key must not contain leading or trailing whitespace",
            ));
        }
        if HeaderValue::try_from(exposed).is_err() {
            return Err(KitError::config(
                ValidationViolation::InvalidFormat {
                    field: "credentials.api_key",
                    rule: "valid HTTP header value",
                },
                "API key must be a valid HTTP header value",
            ));
        }
        Ok(Self { secret })
    }

    pub(crate) fn as_secret(&self) -> &SecretString {
        &self.secret
    }
}

impl fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApiKey")
            .field("secret", &"<redacted>")
            .finish()
    }
}
