use std::num::NonZeroUsize;

use futures_util::StreamExt;
use reqwest::{RequestBuilder, Response};
use serde::de::DeserializeOwned;

use crate::SdkError;
use crate::error::{
    ClassificationBasis, ErrorKind, ErrorPhase, NativeErrorDetails, Operation, RemoteFailure,
    ResponseDetails, ResponseViolation,
};

use super::execution::RequestExecution;

const CONTROL_BODY_LIMIT: NonZeroUsize = NonZeroUsize::new(32 * 1024 * 1024).unwrap();
const ERROR_BODY_LIMIT: usize = 64 * 1024;
const TRUNCATION_MARKER: &str = "\n[response body truncated]";

pub(crate) struct HttpResponse<T> {
    value: T,
    metadata: NativeErrorDetails,
}

impl<T> HttpResponse<T> {
    pub(crate) fn value(&self) -> &T {
        &self.value
    }

    pub(crate) fn into_value(self) -> T {
        self.value
    }

    pub(crate) fn attach_error(&self, error: SdkError) -> SdkError {
        CheckedRequestSpec::attach_metadata(error, &self.metadata)
    }

    pub(crate) fn try_map<U>(
        self,
        normalize: impl FnOnce(T) -> Result<U, SdkError>,
    ) -> Result<HttpResponse<U>, SdkError> {
        let value = normalize(self.value)
            .map_err(|error| CheckedRequestSpec::attach_metadata(error, &self.metadata))?;
        Ok(HttpResponse {
            value,
            metadata: self.metadata,
        })
    }

    pub(crate) fn normalize<U>(
        self,
        normalize: impl FnOnce(T) -> Result<U, SdkError>,
    ) -> Result<U, SdkError> {
        self.try_map(normalize).map(HttpResponse::into_value)
    }
}

impl HttpResponse<Response> {
    pub(crate) fn from_response(response: Response) -> Self {
        Self {
            metadata: CheckedRequestSpec::response_metadata(&response),
            value: response,
        }
    }
}

struct DiagnosticBody {
    body: String,
    truncated: bool,
    failure: Option<SdkError>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CheckedRequestSpec {
    parse_error: fn(&str) -> Result<Option<RemoteFailure>, SdkError>,
}

impl CheckedRequestSpec {
    pub(crate) const fn new(
        parse_error: fn(&str) -> Result<Option<RemoteFailure>, SdkError>,
    ) -> Self {
        Self { parse_error }
    }

    /// Precise HTTP rejection remains authoritative; documented machine categories
    /// may refine only broad invalid-request, throttling, or unknown statuses.
    pub(crate) fn classify_http_failure(
        failure: RemoteFailure,
        status: u16,
    ) -> Result<RemoteFailure, SdkError> {
        // Accepted work can fail inside a successful row envelope. The reported
        // success status remains metadata and cannot classify that failure.
        if (200..300).contains(&status) {
            return Ok(failure);
        }
        let kind = failure.kind();
        let http_kind = ErrorKind::from_http_status(status);
        let refined = match status {
            // FAILED_PRECONDITION has a documented HTTP 400 mapping.
            400 if kind == ErrorKind::FailedPrecondition
                && failure.classification_basis() == ClassificationBasis::CanonicalStatus =>
            {
                true
            }
            400 | 422 => {
                matches!(
                    failure.classification_basis(),
                    ClassificationBasis::NativeCode | ClassificationBasis::NativeReason
                ) && matches!(
                    kind,
                    ErrorKind::Authentication
                        | ErrorKind::PermissionDenied
                        | ErrorKind::NotFound
                        | ErrorKind::Billing
                        | ErrorKind::QuotaExceeded
                        | ErrorKind::FailedPrecondition
                        | ErrorKind::Unsupported
                        | ErrorKind::RequestTooLarge
                        | ErrorKind::ResourceExhausted
                )
            }
            429 => matches!(
                kind,
                ErrorKind::QuotaExceeded
                    | ErrorKind::Billing
                    | ErrorKind::ResourceExhausted
                    | ErrorKind::RateLimited
            ),
            _ => http_kind == ErrorKind::ProviderRejected && kind != ErrorKind::ProviderRejected,
        };
        if refined || (kind == http_kind && kind != ErrorKind::ProviderRejected) {
            return Ok(failure);
        }
        RemoteFailure::from_native(
            http_kind,
            ClassificationBasis::HttpStatus { status },
            failure.native_details().clone(),
        )
    }

    pub(crate) fn refine_http_error(self, error: SdkError) -> SdkError {
        let Some(status) = error.http_status() else {
            return error;
        };
        let error = if error.remote_failure().is_none() {
            match RemoteFailure::from_native(
                ErrorKind::from_http_status(status),
                ClassificationBasis::HttpStatus { status },
                error.native_details().clone(),
            ) {
                Ok(failure) => error.with_remote_failure(failure),
                Err(failure) => return error.with_supplementary(failure),
            }
        } else {
            error
        };
        let Some(body) = error.native_details().body.as_deref() else {
            return error;
        };
        let parsed = match (self.parse_error)(body) {
            Ok(Some(failure)) => failure,
            Ok(None) => {
                return error.with_supplementary(
                    SdkError::invalid_response(ResponseViolation::ProtocolViolation {
                        rule: "unrecognized provider error envelope",
                    })
                    .at_phase(ErrorPhase::Decode),
                );
            }
            Err(failure) => return error.with_supplementary(failure),
        };
        let original = error.native_details();
        let mut details = parsed.native_details().clone();
        details.body = original.body.clone();
        details.request_id = details.request_id.or_else(|| original.request_id.clone());
        details.retry_after = details.retry_after.or_else(|| original.retry_after.clone());
        details.url = details.url.or_else(|| original.url.clone());
        details.truncated |= original.truncated;
        let classified =
            RemoteFailure::from_native(parsed.kind(), parsed.classification_basis(), details)
                .and_then(|failure| Self::classify_http_failure(failure, status));
        match classified {
            Ok(failure) => error.with_remote_failure(failure),
            Err(failure) => error.with_supplementary(failure),
        }
    }

    pub(crate) async fn json<T>(
        self,
        path: &str,
        request: RequestBuilder,
        mut execution: RequestExecution,
    ) -> Result<HttpResponse<T>, SdkError>
    where
        T: DeserializeOwned,
    {
        let path = Self::diagnostic_path(path);
        let response = self.checked(path, request, &mut execution).await?;
        let mut metadata = Self::response_metadata(&response);
        let bytes = self
            .read_bounded_body(path, response, &mut execution, CONTROL_BODY_LIMIT.get())
            .await
            .map_err(|error| Self::attach_metadata(error, &metadata))?;
        let diagnostic = Self::diagnostic_body(&bytes);
        metadata.body = diagnostic.body;
        metadata.truncated = diagnostic.truncated;
        let value = serde_json::from_slice(&bytes).map_err(|err| {
            let violation = ResponseViolation::from_json_error(&err, "response");
            let error = SdkError::invalid_response(violation)
                .with_response_details(ResponseDetails::json(&err))
                .at_phase(ErrorPhase::Decode)
                .with_message(format!("failed to decode response from {path}: {err}"))
                .with_diagnostic(Some(Self::diagnostic_body(&bytes)));
            Self::attach_metadata(error, &metadata)
        })?;
        Ok(HttpResponse { value, metadata })
    }

    pub(crate) async fn bytes(
        self,
        path: &str,
        request: RequestBuilder,
        execution: RequestExecution,
    ) -> Result<HttpResponse<Vec<u8>>, SdkError> {
        self.bytes_with_limit(path, request, execution, CONTROL_BODY_LIMIT.get())
            .await
    }

    pub(crate) async fn bytes_with_limit(
        self,
        path: &str,
        request: RequestBuilder,
        mut execution: RequestExecution,
        max_bytes: usize,
    ) -> Result<HttpResponse<Vec<u8>>, SdkError> {
        let path = Self::diagnostic_path(path);
        let response = self.checked(path, request, &mut execution).await?;
        let mut metadata = Self::response_metadata(&response);
        let value = self
            .read_bounded_body(path, response, &mut execution, max_bytes)
            .await
            .map_err(|error| Self::attach_metadata(error, &metadata))?;
        let diagnostic = Self::diagnostic_body(&value);
        metadata.body = diagnostic.body;
        metadata.truncated = diagnostic.truncated;
        Ok(HttpResponse { value, metadata })
    }

    pub(crate) async fn text(
        self,
        path: &str,
        request: RequestBuilder,
        mut execution: RequestExecution,
    ) -> Result<HttpResponse<String>, SdkError> {
        let path = Self::diagnostic_path(path);
        let response = self.checked(path, request, &mut execution).await?;
        let mut metadata = Self::response_metadata(&response);
        let bytes = self
            .read_bounded_body(path, response, &mut execution, CONTROL_BODY_LIMIT.get())
            .await
            .map_err(|error| Self::attach_metadata(error, &metadata))?;
        let diagnostic = Self::diagnostic_body(&bytes);
        metadata.body = diagnostic.body;
        metadata.truncated = diagnostic.truncated;
        let value = String::from_utf8(bytes).map_err(|err| {
            let error = SdkError::invalid_response(ResponseViolation::InvalidUtf8)
                .at_phase(ErrorPhase::Decode)
                .with_message(format!("response from {path} was not valid UTF-8: {err}"))
                .with_diagnostic(Some(Self::diagnostic_body(err.as_bytes())));
            Self::attach_metadata(error, &metadata)
        })?;
        Ok(HttpResponse { value, metadata })
    }

    pub(crate) async fn response(
        self,
        path: &str,
        request: RequestBuilder,
        mut execution: RequestExecution,
    ) -> Result<Response, SdkError> {
        let path = Self::diagnostic_path(path);
        self.checked(path, request, &mut execution).await
    }

    async fn checked(
        self,
        path: &str,
        request: RequestBuilder,
        execution: &mut RequestExecution,
    ) -> Result<Response, SdkError> {
        let timeout = execution.timeout();
        let deadline = execution.deadline()?;
        let mut attempts = 0;
        let mut request = request;
        loop {
            let remaining = execution
                .remaining_timeout(deadline)
                .map_err(|error| execution.attach_rejection(error))?;
            let retry_request = request.try_clone();
            let (current, delay, rejected) = match request.timeout(remaining).send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        return Ok(response);
                    }
                    let delay = execution.retry_delay_for_response(attempts, &response);
                    let error = match self.ensure_response_success(path, response, timeout).await {
                        Ok(response) => return Ok(response),
                        Err(error) => error,
                    };
                    // Headers already established a rejection. A failed diagnostic read
                    // cannot replace it with a synthesized retry/deadline failure.
                    if error.supplementary().iter().any(|failure| {
                        failure.kind() == ErrorKind::Timeout
                            && failure.context().phase == ErrorPhase::ReadBody
                    }) {
                        return Err(error);
                    }
                    (error, delay, true)
                }
                Err(error) => {
                    let delay = if error.is_timeout() && !error.is_connect() {
                        None
                    } else {
                        execution.retry_delay_for_error(attempts, &error)
                    };
                    let error = RequestExecution::transport_error(path, timeout, error);
                    (error, delay, false)
                }
            };
            let (Some(delay), Some(next_request)) = (delay, retry_request) else {
                return Err(execution.attach_rejection(current));
            };
            let remaining = match execution.remaining_timeout(deadline) {
                Ok(remaining) => remaining,
                Err(error) => {
                    return Err(if rejected {
                        error.with_supplementary(current)
                    } else {
                        let error = error.with_cause(current);
                        execution.attach_rejection(error)
                    });
                }
            };
            // Merely declining another attempt is not an elapsed deadline.
            if delay >= remaining {
                return Err(execution.attach_rejection(current));
            }
            tokio::time::sleep(delay).await;
            if rejected {
                execution.record_rejection(current);
                execution
                    .remaining_timeout(deadline)
                    .map_err(|error| execution.attach_rejection(error))?;
            } else if let Err(error) = execution.remaining_timeout(deadline) {
                let error = error.with_cause(current);
                return Err(execution.attach_rejection(error));
            }
            attempts += 1;
            request = next_request;
        }
    }

    async fn ensure_response_success(
        self,
        path: &str,
        response: Response,
        timeout: std::time::Duration,
    ) -> Result<Response, SdkError> {
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }

        let metadata = Self::response_metadata(&response);
        // Construct the authoritative rejection before awaiting diagnostic I/O.
        let failure = RemoteFailure::from_native(
            ErrorKind::from_http_status(status.as_u16()),
            ClassificationBasis::HttpStatus {
                status: status.as_u16(),
            },
            metadata,
        )?;
        let mut error =
            SdkError::from_remote(failure, Operation::Unspecified, ErrorPhase::ResponseHeaders)
                .with_http_status(status.as_u16());
        let diagnostic = self.read_diagnostic_body(path, response, timeout).await;
        error = error.with_diagnostic(Some(NativeErrorDetails {
            body: Some(if diagnostic.truncated {
                format!("{}{TRUNCATION_MARKER}", diagnostic.body)
            } else {
                diagnostic.body
            }),
            truncated: diagnostic.truncated,
            ..Default::default()
        }));
        if let Some(failure) = diagnostic.failure {
            error = error.with_supplementary(failure);
        }
        error = self.refine_http_error(error);
        Err(error)
    }

    async fn read_bounded_body(
        self,
        path: &str,
        response: Response,
        execution: &mut RequestExecution,
        max_bytes: usize,
    ) -> Result<Vec<u8>, SdkError> {
        let limit = u64::try_from(max_bytes).unwrap_or(u64::MAX);
        if let Some(length) = response.content_length().filter(|length| *length > limit) {
            return Err(
                SdkError::invalid_response(ResponseViolation::LimitExceeded { limit: max_bytes })
                    .with_response_details(ResponseDetails::Size {
                        observed: usize::try_from(length).unwrap_or(usize::MAX),
                    })
                    .at_phase(ErrorPhase::ReadBody)
                    .with_message(format!(
                        "response from {path} exceeded the {max_bytes}-byte limit"
                    )),
            );
        }

        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|err| {
                let error = RequestExecution::transport_error(path, execution.timeout(), err)
                    .at_phase(ErrorPhase::ReadBody);
                execution.attach_rejection(error)
            })?;
            let next_len = body.len().checked_add(chunk.len()).ok_or_else(|| {
                SdkError::invalid_response(ResponseViolation::LimitExceeded { limit: max_bytes })
                    .with_response_details(ResponseDetails::Size {
                        observed: usize::MAX,
                    })
                    .at_phase(ErrorPhase::ReadBody)
                    .with_message(format!(
                        "response from {path} exceeded the {max_bytes}-byte limit"
                    ))
            })?;
            if next_len > max_bytes {
                return Err(
                    SdkError::invalid_response(ResponseViolation::LimitExceeded {
                        limit: max_bytes,
                    })
                    .with_response_details(ResponseDetails::Size { observed: next_len })
                    .at_phase(ErrorPhase::ReadBody)
                    .with_message(format!(
                        "response from {path} exceeded the {max_bytes}-byte limit"
                    )),
                );
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }

    async fn read_diagnostic_body(
        self,
        path: &str,
        response: Response,
        timeout: std::time::Duration,
    ) -> DiagnosticBody {
        let mut body = Vec::new();
        let mut truncated = response
            .content_length()
            .is_some_and(|length| length > ERROR_BODY_LIMIT as u64);
        let mut stream = response.bytes_stream();
        let mut failure = None;
        while body.len() < ERROR_BODY_LIMIT {
            let Some(chunk) = stream.next().await else {
                break;
            };
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    failure = Some(
                        RequestExecution::transport_error(path, timeout, error)
                            .at_phase(ErrorPhase::ReadBody),
                    );
                    break;
                }
            };
            let remaining = ERROR_BODY_LIMIT - body.len();
            if chunk.len() > remaining {
                body.extend_from_slice(&chunk[..remaining]);
                truncated = true;
                break;
            }
            body.extend_from_slice(&chunk);
        }
        if body.len() == ERROR_BODY_LIMIT && !truncated && failure.is_none() {
            match stream.next().await {
                Some(Ok(_)) => truncated = true,
                Some(Err(error)) => {
                    failure = Some(
                        RequestExecution::transport_error(path, timeout, error)
                            .at_phase(ErrorPhase::ReadBody),
                    );
                }
                None => {}
            }
        }
        let (body, truncated) = Self::bounded_lossy_text(&body, truncated || failure.is_some());
        DiagnosticBody {
            body,
            truncated,
            failure,
        }
    }

    fn response_metadata(response: &Response) -> NativeErrorDetails {
        NativeErrorDetails {
            request_id: response
                .headers()
                .get("x-request-id")
                .or_else(|| response.headers().get("request-id"))
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
            retry_after: response
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
            url: Some(Self::diagnostic_path(response.url().as_str()).to_owned()),
            ..NativeErrorDetails::default()
        }
    }

    fn attach_metadata(error: SdkError, metadata: &NativeErrorDetails) -> SdkError {
        let mut details = error.native_details().clone();
        details.request_id = details.request_id.or_else(|| metadata.request_id.clone());
        details.retry_after = details.retry_after.or_else(|| metadata.retry_after.clone());
        details.url = details.url.or_else(|| metadata.url.clone());
        if details.body.is_none() {
            details.body = metadata.body.clone();
            details.truncated |= metadata.truncated;
        }
        error.with_native_capture(details)
    }

    fn diagnostic_path(path: &str) -> &str {
        let path = path.split_once('?').map_or(path, |(path, _)| path);
        let path = path.split_once('#').map_or(path, |(path, _)| path);
        path.split_once("://").map_or(path, |(_, authority)| {
            authority.find('/').map_or("/", |index| &authority[index..])
        })
    }

    pub(crate) fn diagnostic_body(bytes: &[u8]) -> NativeErrorDetails {
        let source_truncated = bytes.len() > ERROR_BODY_LIMIT;
        let prefix = &bytes[..bytes.len().min(ERROR_BODY_LIMIT)];
        let (body, truncated) = Self::bounded_lossy_text(prefix, source_truncated);
        let body = if truncated {
            format!("{body}{TRUNCATION_MARKER}")
        } else {
            body
        };
        NativeErrorDetails {
            body: Some(body),
            truncated,
            ..NativeErrorDetails::default()
        }
    }

    fn bounded_lossy_text(bytes: &[u8], source_truncated: bool) -> (String, bool) {
        let mut body = String::from_utf8_lossy(bytes).into_owned();
        let truncated = source_truncated || body.len() > ERROR_BODY_LIMIT;
        if truncated {
            let limit = ERROR_BODY_LIMIT.saturating_sub(TRUNCATION_MARKER.len());
            let mut end = body.len().min(limit);
            while !body.is_char_boundary(end) {
                end -= 1;
            }
            body.truncate(end);
        }
        (body, truncated)
    }

    pub(crate) fn diagnostic_json<T>(value: &T) -> Option<NativeErrorDetails>
    where
        T: serde::Serialize + ?Sized,
    {
        serde_json::to_vec(value)
            .ok()
            .map(|bytes| Self::diagnostic_body(&bytes))
    }
}
