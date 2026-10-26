use std::time::Duration;

use crate::SdkError;
use crate::error::ValidationViolation;

/// Exponential-backoff settings for replayable provider HTTP operations.
///
/// The policy applies only to request shapes that the SDK marks safe to replay,
/// and every retry still runs inside the same shared logical request deadline.
///
/// # Examples
/// ```rust
/// let policy = provider_sdk::config::RetryPolicy::default();
/// let _ = policy;
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    max_retries: u32,
    initial_backoff: Duration,
    max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_millis(250),
            max_backoff: Duration::from_secs(4),
        }
    }
}

impl RetryPolicy {
    /// Creates validated replay-safe retry policy.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when retries are enabled with zero initial backoff, or the maximum backoff is shorter than the initial backoff.
    ///
    /// # Examples
    ///
    /// ```
    /// use provider_sdk::config::RetryPolicy;
    /// use std::time::Duration;
    /// let disabled = RetryPolicy::new(0, Duration::ZERO, Duration::ZERO)?;
    /// assert_ne!(disabled, RetryPolicy::default());
    /// assert!(RetryPolicy::new(1, Duration::ZERO, Duration::from_secs(1)).is_err());
    ///
    /// # Ok::<(), provider_sdk::SdkError>(())
    /// ```
    ///
    pub fn new(
        max_retries: u32,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) -> Result<Self, SdkError> {
        if max_retries > 0 && initial_backoff.is_zero() {
            return Err(SdkError::config(
                ValidationViolation::OutOfRange {
                    field: "retry.initial_backoff",
                    rule: "positive when retries are enabled",
                },
                "retry initial backoff must be non-zero when retries are enabled",
            ));
        }
        if max_backoff < initial_backoff {
            return Err(SdkError::config(
                ValidationViolation::InvalidValue {
                    field: "retry.max_backoff",
                    rule: "at least initial_backoff",
                },
                "retry maximum backoff must not be shorter than the initial backoff",
            ));
        }
        Ok(Self {
            max_retries,
            initial_backoff,
            max_backoff,
        })
    }

    pub(crate) fn delay_for_attempt(&self, attempt: u32) -> Option<Duration> {
        if attempt >= self.max_retries {
            return None;
        }
        let multiplier = 2_u32.saturating_pow(attempt);
        Some(
            self.initial_backoff
                .saturating_mul(multiplier)
                .min(self.max_backoff),
        )
    }
}
