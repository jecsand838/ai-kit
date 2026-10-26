use std::time::Duration;

use reqwest::{Client, Method, header::HeaderValue};
use secrecy::ExposeSecret;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::time::{Instant, timeout_at};
use url::Url;

use crate::{ApiKey, CommonConfig, StorageError};

#[derive(Clone, Debug)]
pub(crate) struct RestQdrantClient {
    base_url: String,
    api_key: Option<ApiKey>,
    client: Client,
    retry_policy: RetryPolicy,
}

#[derive(Debug, Deserialize)]
struct QdrantRestEnvelope<T> {
    result: T,
}

impl RestQdrantClient {
    pub(crate) fn new(
        base_url: impl Into<String>,
        api_key: Option<ApiKey>,
        common: &CommonConfig,
    ) -> Result<Self, StorageError> {
        let base_url = normalize_rest_base_url(base_url.into())?;
        Self::from_normalized_base_url(base_url, api_key, common)
    }

    fn from_normalized_base_url(
        base_url: String,
        api_key: Option<ApiKey>,
        common: &CommonConfig,
    ) -> Result<Self, StorageError> {
        let client = Client::builder()
            .connect_timeout(common.connect_timeout())
            .timeout(common.request_timeout())
            .build()
            .map_err(|err| StorageError::backend_op("rest_client", err.to_string()))?;
        Ok(Self {
            base_url,
            api_key,
            client,
            retry_policy: RetryPolicy::from_common(common),
        })
    }

    async fn request_json(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
        retry_mode: RetryMode,
    ) -> Result<Value, StorageError> {
        self.send_json(method, path, body, retry_mode).await
    }

    async fn request_result<T>(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
        retry_mode: RetryMode,
    ) -> Result<T, StorageError>
    where
        T: DeserializeOwned,
    {
        let raw = self.request_json(method, path, body, retry_mode).await?;
        serde_json::from_value::<QdrantRestEnvelope<T>>(raw)
            .map(|envelope| envelope.result)
            .map_err(|err| {
                StorageError::Serialization(format!("invalid Qdrant REST response envelope: {err}"))
            })
    }

    pub(crate) async fn request_read_result<T>(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<T, StorageError>
    where
        T: DeserializeOwned,
    {
        self.request_result(method, path, body, RetryMode::Read)
            .await
    }

    pub(crate) async fn request_read_result_value(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, StorageError> {
        self.request_read_result(method, path, body).await
    }

    pub(crate) async fn request_read_result_array(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Vec<Value>, StorageError> {
        match self.request_read_result_value(method, path, body).await? {
            Value::Array(values) => Ok(values),
            other => Err(StorageError::Serialization(format!(
                "Qdrant REST result must be an array, got {other}"
            ))),
        }
    }

    pub(crate) async fn request_operation_ack(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, StorageError> {
        self.request_json(method, path, body, RetryMode::None).await
    }

    pub(crate) async fn request_applied_mutation(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<(), StorageError> {
        let separator = if path.contains('?') { '&' } else { '?' };
        let path = format!("{path}{separator}wait=true");
        let result: Value = self
            .request_result(method, &path, body, RetryMode::None)
            .await?;
        match result.get("status").and_then(Value::as_str) {
            Some("completed") => Ok(()),
            status => Err(StorageError::backend_op(
                "rest_mutation",
                format!("Qdrant did not confirm applied completion: {status:?}"),
            )),
        }
    }

    pub(crate) async fn request_read_envelope_value(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, StorageError> {
        self.request_json(method, path, body, RetryMode::Read).await
    }

    async fn send_json(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
        retry_mode: RetryMode,
    ) -> Result<Value, StorageError> {
        let method_label = method.to_string();
        let path = normalize_request_path(path);
        let url = format!("{}{}", self.base_url.trim_end_matches('/'), path);
        let deadline = self.retry_policy.deadline()?;
        let mut attempt = 0usize;
        loop {
            let request = self.build_request(method.clone(), &url, body.as_ref());
            match timeout_at(deadline, request.send()).await {
                Err(_) => {
                    return Err(self.deadline_error(&method_label, &path, attempt, retry_mode));
                }
                Ok(Ok(response)) => {
                    let retry = self
                        .retry_policy
                        .decision_for_status(retry_mode, attempt, deadline, &response);
                    if let RetryDecision::RetryAfter(delay) = retry {
                        let _ = timeout_at(deadline, response.bytes()).await;
                        attempt += 1;
                        self.sleep_before_retry(
                            deadline,
                            delay,
                            &method_label,
                            &path,
                            attempt,
                            retry_mode,
                        )
                        .await?;
                        continue;
                    }
                    return timeout_at(
                        deadline,
                        self.decode_response(
                            response,
                            &method_label,
                            &path,
                            body.as_ref(),
                            attempt,
                            retry_mode,
                        ),
                    )
                    .await
                    .map_err(|_| self.deadline_error(&method_label, &path, attempt, retry_mode))?;
                }
                Ok(Err(err)) => {
                    if let RetryDecision::RetryAfter(delay) = self
                        .retry_policy
                        .decision_for_error(retry_mode, attempt, deadline, &err)
                    {
                        attempt += 1;
                        self.sleep_before_retry(
                            deadline,
                            delay,
                            &method_label,
                            &path,
                            attempt,
                            retry_mode,
                        )
                        .await?;
                        continue;
                    }
                    return Err(StorageError::backend_op(
                        "rest_request",
                        format!(
                            "{} {} {} failed after {} attempt(s): {}",
                            method_label,
                            path,
                            retry_mode.label(),
                            attempt + 1,
                            rest_error_class(&err)
                        ),
                    ));
                }
            }
        }
    }

    async fn sleep_before_retry(
        &self,
        deadline: Instant,
        delay: Duration,
        method_label: &str,
        path: &str,
        attempts_started: usize,
        retry_mode: RetryMode,
    ) -> Result<(), StorageError> {
        timeout_at(deadline, tokio::time::sleep(delay))
            .await
            .map_err(|_| {
                self.deadline_error(
                    method_label,
                    path,
                    attempts_started.saturating_sub(1),
                    retry_mode,
                )
            })
    }

    fn deadline_error(
        &self,
        method_label: &str,
        path: &str,
        attempt: usize,
        retry_mode: RetryMode,
    ) -> StorageError {
        StorageError::backend_op(
            "rest_request",
            format!(
                "{} {} {} failed after {} attempt(s): timed out after {:?}",
                method_label,
                path,
                retry_mode.label(),
                attempt + 1,
                self.retry_policy.request_timeout
            ),
        )
    }

    fn build_request(
        &self,
        method: Method,
        url: &str,
        body: Option<&Value>,
    ) -> reqwest::RequestBuilder {
        let mut request = self.client.request(method, url);
        if let Some(key) = &self.api_key {
            request = request.header("api-key", key.as_secret().expose_secret());
        }
        if let Some(body) = body {
            request = request.json(body);
        }
        request
    }

    async fn decode_response(
        &self,
        response: reqwest::Response,
        method_label: &str,
        path: &str,
        body: Option<&Value>,
        attempt: usize,
        retry_mode: RetryMode,
    ) -> Result<Value, StorageError> {
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|err| StorageError::backend_op("rest_body", err.to_string()))?;

        if !status.is_success() {
            return Err(StorageError::backend_op(
                "rest_request",
                format!(
                    "{} {} {} failed after {} attempt(s): http {}: {}; body={}",
                    method_label,
                    path,
                    retry_mode.label(),
                    attempt + 1,
                    status.as_u16(),
                    text,
                    body.map(Value::to_string).unwrap_or_else(|| "null".into())
                ),
            ));
        }

        parse_success_json_body(&text)
    }
}

#[derive(Clone, Debug)]
struct RetryPolicy {
    max_retries: usize,
    base_delay: Duration,
    max_delay: Duration,
    request_timeout: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RetryMode {
    Read,
    None,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RetryDecision {
    RetryAfter(Duration),
    DoNotRetry,
}

impl RetryPolicy {
    fn from_common(common: &CommonConfig) -> Self {
        Self {
            max_retries: common.retries(),
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(2),
            request_timeout: common.request_timeout(),
        }
    }

    fn deadline(&self) -> Result<Instant, StorageError> {
        Instant::now()
            .checked_add(self.request_timeout)
            .ok_or_else(|| {
                StorageError::backend_op(
                    "rest_request",
                    format!(
                        "Qdrant REST request timeout {:?} cannot be represented as a Tokio deadline",
                        self.request_timeout
                    ),
                )
            })
    }

    fn decision_for_status(
        &self,
        retry_mode: RetryMode,
        attempt: usize,
        deadline: Instant,
        response: &reqwest::Response,
    ) -> RetryDecision {
        if !self.can_retry(retry_mode, attempt)
            || !matches!(
                response.status().as_u16(),
                408 | 429 | 500 | 502 | 503 | 504 | 529
            )
        {
            return RetryDecision::DoNotRetry;
        }
        self.delay(
            attempt,
            deadline,
            response.headers().get(reqwest::header::RETRY_AFTER),
        )
    }

    fn decision_for_error(
        &self,
        retry_mode: RetryMode,
        attempt: usize,
        deadline: Instant,
        err: &reqwest::Error,
    ) -> RetryDecision {
        if self.can_retry(retry_mode, attempt) && (err.is_connect() || err.is_timeout()) {
            self.delay(attempt, deadline, None)
        } else {
            RetryDecision::DoNotRetry
        }
    }

    fn can_retry(&self, retry_mode: RetryMode, attempt: usize) -> bool {
        retry_mode == RetryMode::Read && attempt < self.max_retries
    }

    fn delay(
        &self,
        attempt: usize,
        deadline: Instant,
        retry_after: Option<&HeaderValue>,
    ) -> RetryDecision {
        let scale = 1u32.checked_shl(attempt as u32).unwrap_or(u32::MAX);
        let backoff = self.base_delay.saturating_mul(scale).min(self.max_delay);
        let delay = retry_after_delay(retry_after).unwrap_or(backoff);
        let Some(wake_at) = Instant::now().checked_add(delay) else {
            return RetryDecision::DoNotRetry;
        };
        if wake_at > deadline {
            RetryDecision::DoNotRetry
        } else {
            RetryDecision::RetryAfter(delay)
        }
    }
}

impl RetryMode {
    fn label(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::None => "operation",
        }
    }
}

fn retry_after_delay(header: Option<&HeaderValue>) -> Option<Duration> {
    let value = header?.to_str().ok()?.trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let retry_at = chrono::DateTime::parse_from_rfc2822(value).ok()?;
    retry_at
        .with_timezone(&chrono::Utc)
        .signed_duration_since(chrono::Utc::now())
        .to_std()
        .ok()
}

fn rest_error_class(err: &reqwest::Error) -> String {
    if err.is_timeout() {
        format!("timeout: {err}")
    } else if err.is_connect() {
        format!("connect: {err}")
    } else if err.is_decode() {
        format!("decode: {err}")
    } else if let Some(status) = err.status() {
        format!("http {}: {err}", status.as_u16())
    } else {
        err.to_string()
    }
}

fn parse_success_json_body(text: &str) -> Result<Value, StorageError> {
    if text.trim().is_empty() {
        return Err(StorageError::Serialization(
            "Qdrant successful REST response must include a JSON body".into(),
        ));
    }

    serde_json::from_str::<Value>(text).map_err(|err| {
        StorageError::Serialization(format!(
            "Qdrant successful REST response must be valid JSON: {err}; body={text}"
        ))
    })
}

pub(crate) fn normalize_rest_base_url(raw: String) -> Result<String, StorageError> {
    finish_base_url(parse_base_url(raw)?)
}

fn parse_base_url(raw: String) -> Result<Url, StorageError> {
    let candidate = raw.trim().to_string();
    if candidate.is_empty() {
        return Err(StorageError::InvalidConfig(
            "Qdrant endpoint cannot be empty".into(),
        ));
    }
    if !candidate.contains("://") {
        return Err(StorageError::InvalidConfig(format!(
            "Qdrant endpoint `{candidate}` must include a URL scheme"
        )));
    }

    Url::parse(&candidate).map_err(|err| {
        StorageError::InvalidConfig(format!("invalid Qdrant endpoint `{candidate}`: {err}"))
    })
}

fn finish_base_url(mut url: Url) -> Result<String, StorageError> {
    if url.path().is_empty() {
        url.set_path("");
    }
    Ok(url.to_string().trim_end_matches('/').to_string())
}

fn normalize_request_path(path: &str) -> String {
    if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    }
}
