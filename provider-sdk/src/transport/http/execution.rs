use std::time::Duration;

use reqwest::Response;
use reqwest::header::RETRY_AFTER;
use tokio::time::Instant;

use crate::SdkError;
use crate::config::RetryPolicy;
use crate::error::{ErrorKind, ErrorPhase, Operation, ValidationViolation};

#[derive(Clone, Debug)]
pub(crate) struct RequestExecution {
    timeout: Duration,
    deadline: Option<Instant>,
    retry: Option<RetryPolicy>,
    latest_rejection: Option<SdkError>,
}

impl RequestExecution {
    pub(crate) fn fail_fast(timeout: Duration) -> Self {
        Self {
            timeout,
            deadline: None,
            retry: None,
            latest_rejection: None,
        }
    }

    pub(crate) fn retryable(timeout: Duration, policy: RetryPolicy) -> Self {
        Self {
            timeout,
            deadline: None,
            retry: Some(policy),
            latest_rejection: None,
        }
    }

    pub(crate) fn timeout(&self) -> Duration {
        self.timeout
    }

    pub(crate) fn through_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    pub(crate) fn record_rejection(&mut self, error: SdkError) {
        self.latest_rejection = Some(error);
    }

    pub(crate) fn attach_rejection(&mut self, error: SdkError) -> SdkError {
        if error.kind() != ErrorKind::Timeout {
            return error;
        }
        match self.latest_rejection.take() {
            Some(rejection) => error.with_supplementary(rejection),
            None => error,
        }
    }

    pub(crate) fn deadline(&self) -> Result<Instant, SdkError> {
        if let Some(deadline) = self.deadline {
            return Ok(deadline);
        }
        Instant::now().checked_add(self.timeout).ok_or_else(|| {
            SdkError::config(
                ValidationViolation::OutOfRange {
                    field: "transport.timeout",
                    rule: "must fit the monotonic clock range",
                },
                "request timeout exceeds the supported clock range",
            )
        })
    }

    pub(crate) fn remaining_timeout(&self, deadline: Instant) -> Result<Duration, SdkError> {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| self.timeout_error())?;
        if remaining.is_zero() {
            return Err(self.timeout_error());
        }
        Ok(remaining)
    }

    pub(crate) fn timeout_error(&self) -> SdkError {
        SdkError::new(
            ErrorKind::Timeout,
            Operation::Unspecified,
            ErrorPhase::RequestSend,
        )
        .with_timeout(self.timeout)
    }

    pub(crate) fn retry_delay_for_response(
        &self,
        attempt: u32,
        response: &Response,
    ) -> Option<Duration> {
        let policy = self.retry.as_ref()?;
        let fallback_delay = policy.delay_for_attempt(attempt)?;
        let status = response.status().as_u16();
        if !matches!(status, 408 | 409 | 429 | 500 | 502 | 503 | 504 | 529) {
            return None;
        }
        if status == 429 {
            return response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .map(Duration::from_secs)
                .or(Some(fallback_delay));
        }
        Some(fallback_delay)
    }

    pub(crate) fn retry_delay_for_error(
        &self,
        attempt: u32,
        err: &reqwest::Error,
    ) -> Option<Duration> {
        if err.is_connect() || err.is_request() {
            return self.retry.as_ref()?.delay_for_attempt(attempt);
        }
        None
    }
}
