use std::time::Duration;

use crate::SdkError;
use crate::config::RetryPolicy;
use crate::error::ValidationViolation;

/// Shared HTTP transport settings used by provider backends.
///
/// These settings feed the transport-owned checked-request layer. Replayable
/// requests can use [`RetryPolicy`] within the same logical request deadline,
/// while non-replayable requests still use the same transport defaults without
/// retrying.
///
/// # Examples
/// ```rust
/// let transport = provider_sdk::config::TransportConfig::default();
/// let _ = transport;
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransportConfig {
    request_timeout: Duration,
    connect_timeout: Duration,
    pool_idle_timeout: Duration,
    retry: RetryPolicy,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(120),
            connect_timeout: Duration::from_secs(15),
            pool_idle_timeout: Duration::from_secs(90),
            retry: RetryPolicy::default(),
        }
    }
}

impl TransportConfig {
    /// Creates validated transport timing and retry policy.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when any timeout is zero or cannot fit the monotonic clock range. Configuration does not authorize retries for non-replayable operations.
    ///
    /// # Examples
    ///
    /// ```
    /// use provider_sdk::config::{RetryPolicy, TransportConfig};
    /// use std::time::Duration;
    /// let transport = TransportConfig::new(Duration::from_secs(60), Duration::from_secs(5), Duration::from_secs(30), RetryPolicy::default())?;
    /// assert_ne!(transport, TransportConfig::default());
    /// assert!(TransportConfig::new(Duration::ZERO, Duration::from_secs(5), Duration::from_secs(30), RetryPolicy::default()).is_err());
    ///
    /// # Ok::<(), provider_sdk::SdkError>(())
    /// ```
    ///
    pub fn new(
        request_timeout: Duration,
        connect_timeout: Duration,
        pool_idle_timeout: Duration,
        retry: RetryPolicy,
    ) -> Result<Self, SdkError> {
        let now = std::time::Instant::now();
        for (field, timeout) in [
            ("transport.request_timeout", request_timeout),
            ("transport.connect_timeout", connect_timeout),
            ("transport.pool_idle_timeout", pool_idle_timeout),
        ] {
            if timeout.is_zero() {
                return Err(SdkError::config(
                    ValidationViolation::OutOfRange {
                        field,
                        rule: "must be positive",
                    },
                    "transport timeouts must be non-zero",
                ));
            }
            if now.checked_add(timeout).is_none() {
                return Err(SdkError::config(
                    ValidationViolation::OutOfRange {
                        field,
                        rule: "must fit the monotonic clock range",
                    },
                    "transport timeout exceeds the supported clock range",
                ));
            }
        }
        Ok(Self {
            request_timeout,
            connect_timeout,
            pool_idle_timeout,
            retry,
        })
    }

    #[must_use]
    /// Returns the total deadline for one logical HTTP request.
    pub(crate) const fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    #[must_use]
    /// Returns the TCP/TLS connection-establishment bound.
    pub(crate) const fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    #[must_use]
    /// Returns the idle pooled-connection retention bound.
    pub(crate) const fn pool_idle_timeout(&self) -> Duration {
        self.pool_idle_timeout
    }

    #[must_use]
    /// Returns the replay-safe retry policy.
    pub(crate) const fn retry(&self) -> &RetryPolicy {
        &self.retry
    }
}
