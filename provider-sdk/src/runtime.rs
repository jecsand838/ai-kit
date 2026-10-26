//! Provider health observations.
//!
//! [`HealthStatus`] reports successful probe latency. A successful probe does not
//! establish support or success for another model, file, batch, or session call.
//! Shared shutdown belongs to [`crate::ProviderBackend::shutdown`].

use std::future::Future;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::SdkError;

/// Successful provider health-probe result.
///
/// # Examples
///
/// ```
/// let health = provider_sdk::runtime::HealthStatus { latency: std::time::Duration::from_millis(12) };
/// assert_eq!(health.latency.as_millis(), 12);
/// ```
///
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthStatus {
    /// End-to-end probe latency.
    pub latency: Duration,
}

impl HealthStatus {
    pub(crate) async fn from_probe<T, F>(probe: F) -> Result<Self, SdkError>
    where
        F: Future<Output = Result<T, SdkError>>,
    {
        let start = Instant::now();
        probe.await?;
        Ok(Self {
            latency: start.elapsed(),
        })
    }
}
