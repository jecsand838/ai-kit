//! Replay-safe batch reads through the supplied adapter; not live-service evidence.
use std::collections::VecDeque;
use std::future::Future;
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::FutureExt;
use providers_kit::batch::{BatchInput, BatchResult, BatchStatus, BatchSubmitRequest};
use providers_kit::config::{
    ApiKey, GeminiConfig, OpenAiConfig, ProviderConfig, ProviderKind, RetryPolicy, TransportConfig,
};
use providers_kit::error::{ErrorKind, ErrorPhase, Operation, ResponseViolation};
use providers_kit::generation::{ContentPart, FinishReason};
use providers_kit::{ModelBackend, Provider, ProviderBackend};
use serde_json::json;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use url::Url;

use crate::support::scripted_http::{RecordedRequest, ScriptedResponse};
use crate::{WireProvider, WireRequest};

/// One socket/task owner, with cancellation available during every external wait.
pub(super) struct BatchServer {
    url: Url,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<(), String>>>,
}

pub(super) enum BatchReply {
    Complete(ScriptedResponse),
    DisconnectBeforeHeaders,
    Gated {
        response: ScriptedResponse,
        entered: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    },
    Incomplete {
        status: u16,
        body: &'static str,
    },
}

impl BatchReply {
    async fn write(self, stream: &mut TcpStream) -> Result<(), String> {
        match self {
            // The owner has already read and recorded the complete request.
            // Returning drops this accepted socket without a response.
            Self::DisconnectBeforeHeaders => Ok(()),
            Self::Complete(response) => response
                .write(stream)
                .await
                .map_err(|error| error.to_string()),
            Self::Gated {
                response,
                entered,
                release,
            } => {
                let _ = entered.send(());
                release.await.map_err(|error| error.to_string())?;
                response
                    .write(stream)
                    .await
                    .map_err(|error| error.to_string())
            }
            Self::Incomplete { status, body } => {
                // Valid headers establish status; the held connection cannot finish its body.
                let response = format!(
                    "HTTP/1.1 {status} fixture\r\nContent-Type: application/json\r\nContent-Length: 4096\r\nx-request-id: private-held-body\r\nConnection: close\r\n\r\n{body}"
                );
                stream
                    .write_all(response.as_bytes())
                    .await
                    .map_err(|error| error.to_string())?;
                stream.flush().await.map_err(|error| error.to_string())?;
                std::future::pending().await
            }
        }
    }
}

impl BatchServer {
    const ID: &str = "batches/retry-owned";
    const PATH: &str = "/v1beta/batches/retry-owned";
    const MODEL: &str = "models/gemini-2.5-flash";
    const WATCHDOG: Duration = Duration::from_secs(10);

    pub(super) async fn start(replies: Vec<BatchReply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        let (stop, mut stopping) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut replies = VecDeque::from(replies);
            loop {
                let accepted = tokio::select! {
                    _ = &mut stopping => return Ok(()),
                    accepted = listener.accept() => accepted,
                };
                let (mut stream, _) = accepted.map_err(|error| error.to_string())?;
                let received = tokio::select! {
                    _ = &mut stopping => return Ok(()),
                    received = tokio::time::timeout(Self::WATCHDOG, RecordedRequest::read(&mut stream)) => received,
                };
                let request = received
                    .map_err(|error| error.to_string())?
                    .map_err(|error| error.to_string())?;
                captured.lock().unwrap().push(request);
                let response = replies.pop_front().unwrap_or_else(|| BatchReply::Complete(
                    ScriptedResponse::error(500, r#"{"error":{"code":500,"status":"INTERNAL","message":"unexpected extra request"}}"#),
                ));
                tokio::select! {
                    _ = &mut stopping => return Ok(()),
                    written = tokio::time::timeout(Self::WATCHDOG, response.write(&mut stream)) => {
                        written.map_err(|error| error.to_string())??;
                    }
                }
            }
        });
        Self {
            url,
            requests,
            stop: Some(stop),
            task: Some(task),
        }
    }

    fn provider(&self, max_retries: u32, timeout: Duration) -> Provider {
        self.provider_with_backoff(max_retries, timeout, Duration::from_millis(10))
    }

    fn provider_with_backoff(
        &self,
        max_retries: u32,
        timeout: Duration,
        backoff: Duration,
    ) -> Provider {
        Self::configured_provider(
            ProviderKind::Gemini(
                GeminiConfig::new(ApiKey::new("fixture-key").unwrap())
                    .with_base_url(self.url.clone()),
            ),
            max_retries,
            timeout,
            backoff,
        )
    }

    pub(super) fn openai_provider(
        &self,
        max_retries: u32,
        timeout: Duration,
        backoff: Duration,
    ) -> Provider {
        Self::configured_provider(
            ProviderKind::OpenAi(
                OpenAiConfig::new(ApiKey::new("fixture-key").unwrap())
                    .with_base_url(self.url.join("v1").unwrap()),
            ),
            max_retries,
            timeout,
            backoff,
        )
    }

    fn configured_provider(
        kind: ProviderKind,
        max_retries: u32,
        timeout: Duration,
        backoff: Duration,
    ) -> Provider {
        Provider::from_config(ProviderConfig::new(
            kind,
            TransportConfig::new(
                timeout,
                Duration::from_secs(1),
                Duration::from_secs(1),
                RetryPolicy::new(max_retries, backoff, backoff).unwrap(),
            )
            .unwrap(),
        ))
        .unwrap()
    }

    fn rejected(status: u16, request_id: &str) -> BatchReply {
        let canonical = match status {
            400 => "INVALID_ARGUMENT",
            401 => "UNAUTHENTICATED",
            403 => "PERMISSION_DENIED",
            404 => "NOT_FOUND",
            503 => "UNAVAILABLE",
            _ => panic!("unsupported fixture status"),
        };
        BatchReply::Complete(ScriptedResponse::error(status, &json!({"error":{"code":status,"status":canonical,"message":"private-retry-evidence"}}).to_string()).with_header("x-request-id", request_id))
    }

    fn operation(id: &str, state: &str) -> ScriptedResponse {
        ScriptedResponse::json(&json!({"name":id,"metadata":{"state":state}}).to_string())
    }

    fn succeeded() -> BatchReply {
        BatchReply::Complete(Self::operation(Self::ID, "JOB_STATE_SUCCEEDED"))
    }

    pub(super) async fn observe<T>(future: impl Future<Output = T>) -> Result<T, String> {
        match tokio::time::timeout(Self::WATCHDOG, AssertUnwindSafe(future).catch_unwind()).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(panic)) => Err(format!(
                "operation panicked: {:?}",
                panic
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| panic.downcast_ref::<&str>().copied())
            )),
            Err(error) => Err(format!("operation watchdog: {error}")),
        }
    }

    pub(super) async fn finish(
        mut self,
        provider: &Provider,
    ) -> (Vec<RecordedRequest>, Vec<String>) {
        let mut errors = Vec::new();
        if let other @ (Ok(Err(_)) | Err(_)) = Self::observe(provider.shutdown()).await {
            errors.push(format!("provider shutdown: {other:?}"));
        }
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(mut task) = self.task.take() {
            match tokio::time::timeout(Self::WATCHDOG, &mut task).await {
                Ok(Ok(Ok(()))) => {}
                Ok(other) => errors.push(format!("server join: {other:?}")),
                Err(error) => {
                    errors.push(format!("server join watchdog: {error}"));
                    task.abort();
                    let joined = task.await;
                    eprintln!("BATCH_READ aborted server join={joined:?}");
                }
            }
        }
        let requests = self.requests.lock().unwrap().clone();
        eprintln!(
            "BATCH_READ cleanup requests={} errors={errors:?}",
            requests.len()
        );
        (requests, errors)
    }

    fn assert_reads(requests: &[RecordedRequest], expected: usize) {
        assert_eq!(requests.len(), expected, "exact replay bound");
        for request in requests {
            assert_eq!(request.method, "GET");
            assert_eq!(
                request.path,
                Self::PATH,
                "retry retains exact operation identity"
            );
            assert!(request.body.is_empty());
        }
    }
}

impl Drop for BatchServer {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[tokio::test]
async fn gemini_batch_status_retries_503_for_the_same_operation() {
    let server = BatchServer::start(vec![
        BatchServer::rejected(503, "private-first"),
        BatchServer::succeeded(),
    ])
    .await;
    let provider = server.provider(2, Duration::from_secs(3));
    let observed = BatchServer::observe(provider.batch_status(BatchServer::ID.into())).await;
    let (requests, cleanup) = server.finish(&provider).await;
    let job = observed
        .unwrap()
        .expect("transient operation GET should recover");
    assert_eq!(job.id, BatchServer::ID);
    assert_eq!(job.status, BatchStatus::Completed);
    BatchServer::assert_reads(&requests, 2);
    assert!(cleanup.is_empty(), "{cleanup:?}");
}

#[tokio::test]
async fn gemini_batch_results_retries_503_and_preserves_correlated_output() {
    let output = json!({"name":BatchServer::ID,"metadata":{"state":"JOB_STATE_SUCCEEDED"},"response":{"inlinedResponses":{"inlinedResponses":[{"metadata":{"custom_id":"row-exact"},"response":{"modelVersion":BatchServer::MODEL,"candidates":[{"content":{"role":"model","parts":[{"text":"exact-batch-answer"}]},"finishReason":"STOP"}]}}]}}});
    let server = BatchServer::start(vec![
        BatchServer::rejected(503, "private-first"),
        BatchReply::Complete(ScriptedResponse::json(&output.to_string())),
    ])
    .await;
    let provider = server.provider(2, Duration::from_secs(3));
    let observed = BatchServer::observe(
        provider.batch_results(BatchServer::ID.into(), NonZeroUsize::new(16_384).unwrap()),
    )
    .await;
    let (requests, cleanup) = server.finish(&provider).await;
    let rows = observed
        .unwrap()
        .expect("transient results GET should recover");
    assert_eq!(rows.len(), 1);
    match &rows[0] {
        BatchResult::Succeeded { custom_id, output } => {
            assert_eq!(custom_id, "row-exact");
            assert_eq!(output.model.as_str(), BatchServer::MODEL);
            assert_eq!(output.finish_reason, Some(FinishReason::Stop));
            assert_eq!(
                output.message.content(),
                &[ContentPart::Text {
                    text: "exact-batch-answer".into()
                }]
            );
        }
        other => panic!("expected correlated successful output, got {other:?}"),
    }
    BatchServer::assert_reads(&requests, 2);
    assert!(cleanup.is_empty(), "{cleanup:?}");
}

#[tokio::test]
async fn gemini_batch_read_exhausts_exact_retry_bound_and_retains_last_rejection() {
    let server = BatchServer::start(vec![
        BatchServer::rejected(503, "private-first"),
        BatchServer::rejected(503, "private-second"),
        BatchServer::rejected(503, "private-last"),
        BatchServer::succeeded(),
    ])
    .await;
    let provider = server.provider(2, Duration::from_secs(3));
    let observed = BatchServer::observe(provider.batch_status(BatchServer::ID.into())).await;
    let (requests, cleanup) = server.finish(&provider).await;
    let error = observed
        .unwrap()
        .expect_err("exhaustion must retain rejection");
    assert_eq!(error.kind(), ErrorKind::ServiceUnavailable);
    assert_eq!(error.http_status(), Some(503));
    assert_eq!(error.context().operation, Operation::BatchStatus);
    assert_eq!(
        error.native_details().request_id.as_deref(),
        Some("private-last")
    );
    BatchServer::assert_reads(&requests, 3);
    assert!(cleanup.is_empty(), "{cleanup:?}");
}

#[tokio::test]
async fn gemini_batch_read_zero_retries_makes_one_attempt() {
    let server = BatchServer::start(vec![
        BatchServer::rejected(503, "private-first"),
        BatchServer::succeeded(),
    ])
    .await;
    let provider = server.provider(0, Duration::from_secs(3));
    let observed = BatchServer::observe(provider.batch_status(BatchServer::ID.into())).await;
    let (requests, cleanup) = server.finish(&provider).await;
    let error = observed.unwrap().unwrap_err();
    assert_eq!(error.http_status(), Some(503));
    assert_eq!(
        error.native_details().request_id.as_deref(),
        Some("private-first")
    );
    BatchServer::assert_reads(&requests, 1);
    assert!(cleanup.is_empty(), "{cleanup:?}");
}

#[tokio::test]
async fn gemini_batch_read_permanent_rejections_are_not_retried() {
    for (status, kind) in [
        (400, ErrorKind::InvalidRequest),
        (401, ErrorKind::Authentication),
        (403, ErrorKind::PermissionDenied),
        (404, ErrorKind::NotFound),
    ] {
        let server = BatchServer::start(vec![
            BatchServer::rejected(status, "private-permanent"),
            BatchServer::succeeded(),
        ])
        .await;
        let provider = server.provider(2, Duration::from_secs(3));
        let observed = BatchServer::observe(provider.batch_status(BatchServer::ID.into())).await;
        let (requests, cleanup) = server.finish(&provider).await;
        let error = observed.unwrap().unwrap_err();
        assert_eq!(error.kind(), kind, "HTTP {status}");
        assert_eq!(error.http_status(), Some(status));
        BatchServer::assert_reads(&requests, 1);
        assert!(cleanup.is_empty(), "{cleanup:?}");
    }
}

#[tokio::test]
async fn gemini_batch_read_invalid_success_is_not_retried() {
    for (reply, violation, phase) in [
        (
            ScriptedResponse::json("{not-json"),
            ResponseViolation::MalformedJson,
            ErrorPhase::Decode,
        ),
        (
            BatchServer::operation("batches/other", "JOB_STATE_SUCCEEDED"),
            ResponseViolation::IdentityMismatch { field: "name" },
            ErrorPhase::Normalize,
        ),
    ] {
        let server =
            BatchServer::start(vec![BatchReply::Complete(reply), BatchServer::succeeded()]).await;
        let provider = server.provider(2, Duration::from_secs(3));
        let observed = BatchServer::observe(provider.batch_status(BatchServer::ID.into())).await;
        let (requests, cleanup) = server.finish(&provider).await;
        let error = observed.unwrap().unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidResponse(violation));
        assert_eq!(error.context().phase, phase);
        BatchServer::assert_reads(&requests, 1);
        assert!(cleanup.is_empty(), "{cleanup:?}");
    }
}

#[tokio::test]
async fn gemini_batch_read_rejected_body_timeout_keeps_http_rejection_primary() {
    let server = BatchServer::start(vec![BatchReply::Incomplete {
        status: 503,
        body: r#"{"error":{"code":503,"status":"UNAVAILABLE","message":"private-held"}}"#,
    }])
    .await;
    let provider = server.provider(2, Duration::from_millis(500));
    let observed = BatchServer::observe(provider.batch_status(BatchServer::ID.into())).await;
    let (requests, cleanup) = server.finish(&provider).await;
    let error = observed.unwrap().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::ServiceUnavailable);
    assert_eq!(error.http_status(), Some(503));
    assert_eq!(error.context().phase, ErrorPhase::ResponseHeaders);
    assert_eq!(
        error.native_details().request_id.as_deref(),
        Some("private-held-body")
    );
    assert!(
        error
            .supplementary()
            .iter()
            .any(|failure| failure.kind() == ErrorKind::Timeout
                && failure.context().phase == ErrorPhase::ReadBody)
    );
    BatchServer::assert_reads(&requests, 1);
    assert!(cleanup.is_empty(), "{cleanup:?}");
}

#[tokio::test]
async fn gemini_batch_read_body_timeout_after_retry_retains_prior_rejection() {
    let server = BatchServer::start(vec![
        BatchServer::rejected(503, "private-first"),
        BatchReply::Incomplete {
            status: 200,
            body: r#"{"name":"batches/retry-owned""#,
        },
    ])
    .await;
    let provider = server.provider(2, Duration::from_millis(500));
    let observed = BatchServer::observe(provider.batch_status(BatchServer::ID.into())).await;
    let (requests, cleanup) = server.finish(&provider).await;
    let error = observed.unwrap().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert_eq!(error.context().operation, Operation::BatchStatus);
    assert_eq!(error.context().phase, ErrorPhase::ReadBody);
    assert_eq!(error.supplementary().len(), 1);
    let rejection = &error.supplementary()[0];
    assert_eq!(rejection.kind(), ErrorKind::ServiceUnavailable);
    assert_eq!(rejection.http_status(), Some(503));
    assert_eq!(
        rejection.native_details().request_id.as_deref(),
        Some("private-first")
    );
    assert!(!format!("{error:?}").contains("private-first"));
    BatchServer::assert_reads(&requests, 2);
    assert!(cleanup.is_empty(), "{cleanup:?}");
}

#[tokio::test]
async fn gemini_batch_read_backoff_outside_deadline_keeps_current_rejection() {
    let server = BatchServer::start(vec![
        BatchServer::rejected(503, "private-first"),
        BatchServer::succeeded(),
    ])
    .await;
    let provider =
        server.provider_with_backoff(2, Duration::from_millis(500), Duration::from_secs(2));
    let observed = BatchServer::observe(provider.batch_status(BatchServer::ID.into())).await;
    let (requests, cleanup) = server.finish(&provider).await;
    let error = observed.unwrap().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::ServiceUnavailable);
    assert_eq!(error.http_status(), Some(503));
    assert_eq!(error.context().phase, ErrorPhase::ResponseHeaders);
    assert_eq!(
        error.native_details().request_id.as_deref(),
        Some("private-first")
    );
    BatchServer::assert_reads(&requests, 1);
    assert!(cleanup.is_empty(), "{cleanup:?}");
}

#[tokio::test]
async fn gemini_batch_submission_503_never_replays_post() {
    let server = BatchServer::start(vec![
        BatchServer::rejected(503, "private-submit"),
        BatchServer::succeeded(),
    ])
    .await;
    let provider = server.provider(2, Duration::from_secs(3));
    let model = WireProvider::Gemini.model(&provider, BatchServer::MODEL);
    let request = BatchSubmitRequest::Inline {
        rows: vec![BatchInput::new("row-exact", WireRequest::user("probe")).unwrap()],
    };
    let observed = BatchServer::observe(model.submit_batch(&request)).await;
    let (requests, cleanup) = server.finish(&provider).await;
    let error = observed.unwrap().unwrap_err();
    assert_eq!(error.http_status(), Some(503));
    assert_eq!(error.context().operation, Operation::SubmitBatch);
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(
        requests[0].path,
        "/v1beta/models/gemini-2.5-flash:batchGenerateContent"
    );
    assert!(cleanup.is_empty(), "{cleanup:?}");
}

#[tokio::test]
async fn gemini_batch_cancellation_post_is_not_retried() {
    let server = BatchServer::start(vec![
        BatchServer::rejected(503, "private-cancel"),
        BatchServer::succeeded(),
    ])
    .await;
    let provider = server.provider(2, Duration::from_secs(3));
    let observed = BatchServer::observe(provider.cancel_batch(BatchServer::ID.into())).await;
    let (requests, cleanup) = server.finish(&provider).await;
    let error = observed.unwrap().unwrap_err();
    assert_eq!(error.http_status(), Some(503));
    assert_eq!(error.context().operation, Operation::CancelBatch);
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, format!("{}:cancel", BatchServer::PATH));
    assert!(cleanup.is_empty(), "{cleanup:?}");
}

#[tokio::test]
async fn gemini_batch_cancellation_retries_only_the_followup_read() {
    let server = BatchServer::start(vec![
        BatchReply::Complete(ScriptedResponse::json("{}")),
        BatchServer::rejected(503, "private-cancel-read"),
        BatchReply::Complete(BatchServer::operation(
            BatchServer::ID,
            "JOB_STATE_CANCELLED",
        )),
    ])
    .await;
    let provider = server.provider(2, Duration::from_secs(3));
    let observed = BatchServer::observe(provider.cancel_batch(BatchServer::ID.into())).await;
    let (requests, cleanup) = server.finish(&provider).await;
    let job = observed
        .unwrap()
        .expect("cancel acknowledgement survives transient read failure");
    assert_eq!(job.id, BatchServer::ID);
    assert_eq!(job.status, BatchStatus::Cancelled);
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, format!("{}:cancel", BatchServer::PATH));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&requests[0].body).unwrap(),
        json!({})
    );
    BatchServer::assert_reads(&requests[1..], 2);
    assert!(cleanup.is_empty(), "{cleanup:?}");
}

#[tokio::test]
async fn gemini_batch_read_observer_loss_releases_admission_for_shutdown() {
    let (entered, entry) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let server = BatchServer::start(vec![BatchReply::Gated {
        response: BatchServer::operation(BatchServer::ID, "JOB_STATE_SUCCEEDED"),
        entered,
        release: released,
    }])
    .await;
    let provider = server.provider(2, Duration::from_secs(3));
    let mut read = Box::pin(provider.batch_status(BatchServer::ID.into()));
    let observed = BatchServer::observe(async {
        tokio::select! {
            result = &mut read => Err(format!("read ended before held response entry: {result:?}")),
            result = entry => result.map_err(|error| error.to_string()),
        }
    })
    .await;
    drop(read);
    // The held socket remains unready. Shutdown must not depend on a discarded GET.
    let (requests, cleanup) = server.finish(&provider).await;
    drop(release);
    let after_shutdown = provider.batch_status(BatchServer::ID.into()).await;
    observed.unwrap().unwrap();
    assert_eq!(after_shutdown.unwrap_err().kind(), ErrorKind::Closed);
    BatchServer::assert_reads(&requests, 1);
    assert!(cleanup.is_empty(), "{cleanup:?}");
}

#[tokio::test]
async fn gemini_batch_read_shutdown_waiter_loss_still_drains_admitted_read() {
    let (entered, entry) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let server = BatchServer::start(vec![BatchReply::Gated {
        response: BatchServer::operation(BatchServer::ID, "JOB_STATE_SUCCEEDED"),
        entered,
        release: released,
    }])
    .await;
    let provider = server.provider(2, Duration::from_secs(3));
    let mut read = Box::pin(provider.batch_status(BatchServer::ID.into()));
    let observed = BatchServer::observe(async {
        tokio::select! {
            result = &mut read => Err(format!("read ended before held response entry: {result:?}")),
            result = entry => result.map_err(|error| error.to_string()),
        }
    })
    .await;
    // This poll starts shutdown and discards only its observer; accepted work stays owned.
    let early_shutdown = provider.shutdown().now_or_never();
    let release_result = release.send(());
    let completed = BatchServer::observe(read).await;
    let (requests, cleanup) = server.finish(&provider).await;
    let after_shutdown = provider.batch_status(BatchServer::ID.into()).await;
    observed.unwrap().unwrap();
    assert!(
        early_shutdown.is_none(),
        "shutdown completed before the accepted read: {early_shutdown:?}"
    );
    assert!(release_result.is_ok());
    assert_eq!(completed.unwrap().unwrap().status, BatchStatus::Completed);
    assert_eq!(after_shutdown.unwrap_err().kind(), ErrorKind::Closed);
    BatchServer::assert_reads(&requests, 1);
    assert!(cleanup.is_empty(), "{cleanup:?}");
}
