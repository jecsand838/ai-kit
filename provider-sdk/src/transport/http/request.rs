use std::error::Error as _;

use url::Url;

use crate::SdkError;
use crate::error::{ErrorKind, ErrorPhase, NativeErrorDetails, Operation};

use super::execution::RequestExecution;

impl RequestExecution {
    pub(crate) fn transport_error(
        path: &str,
        timeout: std::time::Duration,
        error: reqwest::Error,
    ) -> SdkError {
        let url = error.url().map(Self::redacted_url);
        let original_url = error.url().map(ToString::to_string);
        let kind = if error.is_timeout() {
            ErrorKind::Timeout
        } else {
            ErrorKind::Transport
        };
        let phase = if error.is_builder() {
            ErrorPhase::RequestBuild
        } else {
            ErrorPhase::RequestSend
        };
        let redact = |message: String| match (&original_url, &url) {
            (Some(raw), Some(redacted)) => message.replace(raw, redacted),
            _ => message,
        };
        let mut sources = Vec::new();
        let mut source = error.source();
        while let Some(error) = source {
            sources.push(redact(error.to_string()));
            source = error.source();
        }
        let mut cause = None;
        for message in sources.into_iter().rev() {
            let mut child = SdkError::new(ErrorKind::Transport, Operation::Unspecified, phase)
                .with_message(message);
            if let Some(next) = cause {
                child = child.with_cause(next);
            }
            cause = Some(child);
        }
        let mut retained = SdkError::new(kind, Operation::Unspecified, phase).with_native_details(
            NativeErrorDetails {
                message: Some(redact(error.to_string())),
                url,
                ..Default::default()
            },
        );
        if kind == ErrorKind::Timeout {
            retained = retained.with_timeout(timeout);
        }
        if let Some(status) = error.status() {
            retained = retained.with_http_status(status.as_u16());
        }
        if let Some(cause) = cause {
            retained = retained.with_cause(cause);
        }
        retained.with_request_target(path)
    }

    fn redacted_url(url: &Url) -> String {
        let mut redacted = url.clone();
        let _ = redacted.set_password(None);
        let _ = redacted.set_username("");
        redacted.set_query(None);
        redacted.set_fragment(None);
        redacted.to_string()
    }
}
