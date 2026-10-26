//! Public batch lifecycle regressions. These fixtures are not live-provider evidence.
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::FutureExt;
use provider_sdk::batch::{
    BatchInput, BatchJob, BatchListRequest, BatchStatus, BatchSubmitRequest,
};
use provider_sdk::config::{
    ApiKey, OpenAiConfig, ProviderConfig, ProviderKind, RetryPolicy, TransportConfig,
};
use provider_sdk::error::{ErrorKind, ErrorPhase, NativeDetail, Operation, ResponseViolation};
use provider_sdk::{ModelBackend, Provider, ProviderBackend, SdkError};
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;
use url::Url;

use super::gemini_batch::{BatchReply, BatchServer};
use crate::support::scripted_http::{RecordedRequest, ScriptedResponse};
use crate::{WireProvider, WireRequest};

#[tokio::test]
async fn openai_batch_status_retries_disconnect_before_headers_for_same_id() {
    let server = BatchServer::start(vec![
        BatchReply::DisconnectBeforeHeaders,
        BatchReply::Complete(ScriptedResponse::json(
            r#"{"id":"batch_retry_owned","status":"completed"}"#,
        )),
    ])
    .await;
    let provider = server.openai_provider(2, Duration::from_secs(3), Duration::from_millis(10));
    let observed = BatchServer::observe(provider.batch_status("batch_retry_owned".into())).await;
    let (requests, cleanup) = server.finish(&provider).await;
    let job = observed
        .unwrap()
        .expect("a transient status GET may recover");
    assert_eq!(job.id, "batch_retry_owned");
    assert_eq!(job.status, BatchStatus::Completed);
    BatchFixture::assert_status_reads(&requests, 2);
    assert!(cleanup.is_empty(), "{cleanup:?}");
}

#[tokio::test]
async fn openai_batch_status_retries_503_for_same_id() {
    let server = BatchServer::start(vec![
        BatchReply::Complete(ScriptedResponse::error(
            503,
            r#"{"error":{"code":"server_error","message":"status unavailable"}}"#,
        )),
        BatchReply::Complete(ScriptedResponse::json(
            r#"{"id":"batch_retry_owned","status":"completed"}"#,
        )),
    ])
    .await;
    let provider = server.openai_provider(2, Duration::from_secs(3), Duration::from_millis(10));
    let observed = BatchServer::observe(provider.batch_status("batch_retry_owned".into())).await;
    let (requests, cleanup) = server.finish(&provider).await;
    let job = observed
        .unwrap()
        .expect("a transient status GET may recover");
    assert_eq!(job.id, "batch_retry_owned");
    assert_eq!(job.status, BatchStatus::Completed);
    BatchFixture::assert_status_reads(&requests, 2);
    assert!(cleanup.is_empty(), "{cleanup:?}");
}

#[tokio::test]
async fn openai_batch_status_zero_and_exhausted_retries_preserve_final_failure() {
    for retries in [0, 2] {
        for disconnected in [false, true] {
            let mut replies = (0..=retries)
                .map(|attempt| {
                    if disconnected {
                        BatchReply::DisconnectBeforeHeaders
                    } else {
                        BatchFixture::status_rejection(503, &format!("private-{attempt}"))
                    }
                })
                .collect::<Vec<_>>();
            replies.push(BatchReply::Complete(BatchFixture::status_reply()));
            let server = BatchServer::start(replies).await;
            let provider =
                server.openai_provider(retries, Duration::from_secs(3), Duration::from_millis(10));
            let observed =
                BatchServer::observe(provider.batch_status("batch_retry_owned".into())).await;
            let (requests, cleanup) = server.finish(&provider).await;
            let error = observed
                .unwrap()
                .expect_err("retry bound must stop before success");
            assert_eq!(error.context().operation, Operation::BatchStatus);
            if disconnected {
                assert_eq!(error.kind(), ErrorKind::Transport);
                assert_eq!(error.context().phase, ErrorPhase::RequestSend);
                assert_eq!(error.http_status(), None);
                assert!(error.cause().is_some());
            } else {
                assert_eq!(error.kind(), ErrorKind::ServiceUnavailable);
                assert_eq!(error.http_status(), Some(503));
                assert_eq!(
                    error.native_details().request_id.as_deref(),
                    Some(format!("private-{retries}").as_str())
                );
            }
            BatchFixture::assert_status_reads(&requests, retries as usize + 1);
            assert!(cleanup.is_empty(), "{cleanup:?}");
        }
    }
}

#[tokio::test]
async fn openai_batch_status_permanent_and_invalid_responses_are_not_retried() {
    for (reply, kind, phase) in [
        (
            BatchFixture::status_rejection(400, "private-permanent"),
            ErrorKind::InvalidRequest,
            ErrorPhase::ResponseHeaders,
        ),
        (
            BatchFixture::status_rejection(401, "private-permanent"),
            ErrorKind::Authentication,
            ErrorPhase::ResponseHeaders,
        ),
        (
            BatchFixture::status_rejection(403, "private-permanent"),
            ErrorKind::PermissionDenied,
            ErrorPhase::ResponseHeaders,
        ),
        (
            BatchFixture::status_rejection(404, "private-permanent"),
            ErrorKind::NotFound,
            ErrorPhase::ResponseHeaders,
        ),
        (
            BatchReply::Complete(ScriptedResponse::json("{not-json")),
            ErrorKind::InvalidResponse(ResponseViolation::MalformedJson),
            ErrorPhase::Decode,
        ),
        (
            BatchReply::Complete(ScriptedResponse::json(
                r#"{"id":"batch_other","status":"completed"}"#,
            )),
            ErrorKind::InvalidResponse(ResponseViolation::IdentityMismatch { field: "id" }),
            ErrorPhase::Normalize,
        ),
    ] {
        let server = BatchServer::start(vec![
            reply,
            BatchReply::Complete(BatchFixture::status_reply()),
        ])
        .await;
        let provider = server.openai_provider(2, Duration::from_secs(3), Duration::from_millis(10));
        let observed =
            BatchServer::observe(provider.batch_status("batch_retry_owned".into())).await;
        let (requests, cleanup) = server.finish(&provider).await;
        let error = observed
            .unwrap()
            .expect_err("invalid status must remain failure");
        assert_eq!(error.kind(), kind);
        assert_eq!(error.context().operation, Operation::BatchStatus);
        assert_eq!(error.context().phase, phase);
        BatchFixture::assert_status_reads(&requests, 1);
        assert!(cleanup.is_empty(), "{cleanup:?}");
    }
}

#[tokio::test]
async fn openai_batch_status_body_timeouts_preserve_primary_and_prior_rejection() {
    for success_headers in [false, true] {
        let mut replies = Vec::new();
        if success_headers {
            replies.push(BatchFixture::status_rejection(503, "private-prior"));
        }
        replies.push(BatchReply::Incomplete {
            status: if success_headers { 200 } else { 503 },
            body: if success_headers {
                r#"{"id":"batch_retry_owned""#
            } else {
                r#"{"error":{"message":"held rejection"}}"#
            },
        });
        let server = BatchServer::start(replies).await;
        let provider =
            server.openai_provider(2, Duration::from_millis(500), Duration::from_millis(10));
        let observed =
            BatchServer::observe(provider.batch_status("batch_retry_owned".into())).await;
        let (requests, cleanup) = server.finish(&provider).await;
        let error = observed.unwrap().expect_err("body cannot finish");
        assert_eq!(error.context().operation, Operation::BatchStatus);
        if success_headers {
            assert_eq!(error.kind(), ErrorKind::Timeout);
            assert_eq!(error.context().phase, ErrorPhase::ReadBody);
            assert_eq!(error.supplementary().len(), 1);
            let prior = &error.supplementary()[0];
            assert_eq!(prior.kind(), ErrorKind::ServiceUnavailable);
            assert_eq!(prior.http_status(), Some(503));
            assert_eq!(
                prior.native_details().request_id.as_deref(),
                Some("private-prior")
            );
        } else {
            assert_eq!(error.kind(), ErrorKind::ServiceUnavailable);
            assert_eq!(error.http_status(), Some(503));
            assert_eq!(error.context().phase, ErrorPhase::ResponseHeaders);
            assert!(
                error
                    .supplementary()
                    .iter()
                    .any(|failure| failure.kind() == ErrorKind::Timeout
                        && failure.context().phase == ErrorPhase::ReadBody)
            );
        }
        BatchFixture::assert_status_reads(&requests, if success_headers { 2 } else { 1 });
        assert!(cleanup.is_empty(), "{cleanup:?}");
    }
}

#[tokio::test]
async fn openai_batch_status_retry_keeps_one_absolute_deadline() {
    let (entered, entry) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let server = BatchServer::start(vec![
        BatchFixture::status_rejection(503, "private-prior"),
        BatchReply::Gated {
            response: BatchFixture::status_reply(),
            entered,
            release: released,
        },
    ])
    .await;
    let provider = server.openai_provider(2, Duration::from_secs(2), Duration::from_millis(1500));
    let mut read = Box::pin(provider.batch_status("batch_retry_owned".into()));
    let observed = BatchServer::observe(async {
        tokio::select! {
            result = &mut read => Err(format!("status ended before retry entered: {result:?}")),
            entered = entry => {
                entered.map_err(|error| error.to_string())?;
                // Retry has consumed 1.5s of the original 2s budget. A restarted
                // 2s budget would still be pending at this independently held boundary.
                tokio::time::timeout(Duration::from_secs(1), &mut read)
                    .await.map_err(|error| error.to_string())
            },
        }
    })
    .await;
    drop(read);
    let (requests, cleanup) = server.finish(&provider).await;
    drop(release);
    let error = observed
        .unwrap()
        .expect("original deadline must expire while retry is held")
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert_eq!(error.context().operation, Operation::BatchStatus);
    assert_eq!(error.context().phase, ErrorPhase::RequestSend);
    assert_eq!(error.supplementary().len(), 1);
    assert_eq!(
        error.supplementary()[0]
            .native_details()
            .request_id
            .as_deref(),
        Some("private-prior")
    );
    BatchFixture::assert_status_reads(&requests, 2);
    assert!(cleanup.is_empty(), "{cleanup:?}");
}

#[tokio::test]
async fn openai_batch_status_backoff_outside_budget_keeps_current_failure() {
    for disconnected in [false, true] {
        let first = if disconnected {
            BatchReply::DisconnectBeforeHeaders
        } else {
            BatchFixture::status_rejection(503, "private-current")
        };
        let server = BatchServer::start(vec![
            first,
            BatchReply::Complete(BatchFixture::status_reply()),
        ])
        .await;
        let provider =
            server.openai_provider(2, Duration::from_millis(500), Duration::from_secs(2));
        let observed =
            BatchServer::observe(provider.batch_status("batch_retry_owned".into())).await;
        let (requests, cleanup) = server.finish(&provider).await;
        let error = observed.unwrap().unwrap_err();
        assert_eq!(error.context().operation, Operation::BatchStatus);
        assert_eq!(
            error.kind(),
            if disconnected {
                ErrorKind::Transport
            } else {
                ErrorKind::ServiceUnavailable
            }
        );
        if disconnected {
            assert_eq!(error.context().phase, ErrorPhase::RequestSend);
            assert_eq!(error.http_status(), None);
            assert!(error.cause().is_some());
        } else {
            assert_eq!(error.http_status(), Some(503));
            assert_eq!(
                error.native_details().request_id.as_deref(),
                Some("private-current")
            );
        }
        BatchFixture::assert_status_reads(&requests, 1);
        assert!(cleanup.is_empty(), "{cleanup:?}");
    }
}

#[tokio::test]
async fn openai_batch_status_retry_observer_and_shutdown_waiter_loss_settle() {
    for discard_shutdown_waiter in [false, true] {
        let (entered, entry) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let server = BatchServer::start(vec![
            BatchFixture::status_rejection(503, "private-prior"),
            BatchReply::Gated {
                response: BatchFixture::status_reply(),
                entered,
                release: released,
            },
        ])
        .await;
        let provider = server.openai_provider(2, Duration::from_secs(3), Duration::from_millis(10));
        let mut read = Box::pin(provider.batch_status("batch_retry_owned".into()));
        let entered = BatchServer::observe(async {
            tokio::select! {
                result = &mut read => Err(format!("status ended before held retry: {result:?}")),
                entered = entry => entered.map_err(|error| error.to_string()),
            }
        })
        .await;
        if discard_shutdown_waiter {
            let early_shutdown = provider.shutdown().now_or_never();
            let released = release.send(());
            let completed = BatchServer::observe(read).await;
            let (requests, cleanup) = server.finish(&provider).await;
            let after = provider.batch_status("batch_retry_owned".into()).await;
            entered.unwrap().unwrap();
            assert!(
                early_shutdown.is_none(),
                "shutdown must drain admitted retry"
            );
            assert!(released.is_ok());
            assert_eq!(completed.unwrap().unwrap().status, BatchStatus::Completed);
            assert_eq!(after.unwrap_err().kind(), ErrorKind::Closed);
            BatchFixture::assert_status_reads(&requests, 2);
            assert!(cleanup.is_empty(), "{cleanup:?}");
        } else {
            drop(read);
            let (requests, cleanup) = server.finish(&provider).await;
            drop(release);
            let after = provider.batch_status("batch_retry_owned".into()).await;
            entered.unwrap().unwrap();
            assert_eq!(after.unwrap_err().kind(), ErrorKind::Closed);
            BatchFixture::assert_status_reads(&requests, 2);
            assert!(cleanup.is_empty(), "{cleanup:?}");
        }
    }
}

#[tokio::test]
async fn openai_batch_status_opt_in_leaves_other_batch_operations_fail_fast() {
    for operation in [
        Operation::BatchResults,
        Operation::ListBatches,
        Operation::CancelBatch,
    ] {
        let server = BatchServer::start(vec![
            BatchFixture::status_rejection(503, "private-first"),
            BatchReply::Complete(BatchFixture::status_reply()),
        ])
        .await;
        let provider = server.openai_provider(2, Duration::from_secs(3), Duration::from_millis(10));
        let observed = BatchServer::observe(async {
            match operation {
                Operation::BatchResults => provider
                    .batch_results("batch_retry_owned".into(), NonZeroUsize::new(4096).unwrap())
                    .await
                    .map(|_| ()),
                Operation::ListBatches => provider
                    .list_batches(BatchListRequest::default())
                    .await
                    .map(|_| ()),
                Operation::CancelBatch => provider
                    .cancel_batch("batch_retry_owned".into())
                    .await
                    .map(|_| ()),
                _ => unreachable!("only three explicitly selected operations"),
            }
        })
        .await;
        let (requests, cleanup) = server.finish(&provider).await;
        let error = observed
            .unwrap()
            .expect_err("only status opts into retries");
        assert_eq!(error.kind(), ErrorKind::ServiceUnavailable);
        assert_eq!(error.http_status(), Some(503));
        assert_eq!(error.context().operation, operation);
        assert_eq!(requests.len(), 1);
        let (method, path) = match operation {
            Operation::BatchResults => ("GET", "/v1/batches/batch_retry_owned"),
            Operation::ListBatches => ("GET", "/v1/batches"),
            Operation::CancelBatch => ("POST", "/v1/batches/batch_retry_owned/cancel"),
            _ => unreachable!("only three explicitly selected operations"),
        };
        assert_eq!(requests[0].method, method);
        assert_eq!(requests[0].path, path);
        assert!(cleanup.is_empty(), "{cleanup:?}");
    }
}

#[derive(Clone, Copy)]
enum Creation {
    Rejected,
    Accepted,
    Disconnected,
    Status(u16),
    MalformedAcceptance,
}

#[derive(Clone, Copy)]
enum HeldPhase {
    Upload,
    Creation,
}

struct BatchFixture {
    url: Url,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Vec<String>>>,
    creation_seen: oneshot::Receiver<()>,
    release_creation: Option<oneshot::Sender<()>>,
    creation_answered: oneshot::Receiver<()>,
    replies: Arc<AtomicUsize>,
    replied: Arc<Notify>,
}

struct BatchObservation {
    result: Result<BatchJob, SdkError>,
    requests: Vec<RecordedRequest>,
    cleanup: String,
    cleanup_succeeded: bool,
}

impl BatchFixture {
    const FILE: &str = "file-owned-batch-input";
    const MODEL: &str = "gpt-4.1";

    fn status_reply() -> ScriptedResponse {
        ScriptedResponse::json(r#"{"id":"batch_retry_owned","status":"completed"}"#)
    }

    fn status_rejection(status: u16, request_id: &str) -> BatchReply {
        BatchReply::Complete(
            ScriptedResponse::error(status, r#"{"error":{"message":"status unavailable"}}"#)
                .with_header("x-request-id", request_id),
        )
    }

    fn assert_status_reads(requests: &[RecordedRequest], expected: usize) {
        assert_eq!(requests.len(), expected, "exact status retry bound");
        for request in requests {
            assert_eq!(request.method, "GET");
            assert_eq!(request.path, "/v1/batches/batch_retry_owned");
            assert!(request.body.is_empty());
            assert_eq!(
                request, &requests[0],
                "replay preserves the complete request"
            );
        }
    }

    async fn start(
        models: &[&str],
        creation: Creation,
        upload_rejected: bool,
        delete_rejected: bool,
        hold: bool,
    ) -> Self {
        let rows = models.iter().enumerate().map(|(index, model)| {
            json!({"custom_id":format!("row-{index}"),"method":"POST","url":"/v1/responses","body":{"model":model,"input":"fixture"}}).to_string()
        }).collect::<Vec<_>>().join("\n") + "\n";
        Self::start_rows(
            rows,
            creation,
            upload_rejected,
            delete_rejected,
            hold.then_some(HeldPhase::Creation),
        )
        .await
    }

    async fn start_rows(
        rows: String,
        creation: Creation,
        upload_rejected: bool,
        delete_rejected: bool,
        hold: Option<HeldPhase>,
    ) -> Self {
        Self::start_timed(
            rows,
            creation,
            upload_rejected,
            delete_rejected,
            hold,
            Duration::ZERO,
        )
        .await
    }

    async fn start_timed(
        rows: String,
        creation: Creation,
        upload_rejected: bool,
        delete_rejected: bool,
        hold: Option<HeldPhase>,
        response_delay: Duration,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("http://{}/v1", listener.local_addr().unwrap())).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stored = Arc::clone(&requests);
        let (stop, mut stopping) = oneshot::channel();
        let (seen, creation_seen) = oneshot::channel();
        let (release_creation, released) = oneshot::channel();
        let (answered, creation_answered) = oneshot::channel();
        let replies = Arc::new(AtomicUsize::new(0));
        let replied = Arc::new(Notify::new());
        let reply_count = Arc::clone(&replies);
        let reply_changed = Arc::clone(&replied);
        let task = tokio::spawn(async move {
            let mut uploads = 0_usize;
            let mut errors = Vec::new();
            let mut seen = Some(seen);
            let mut released = Some(released);
            let mut answered = Some(answered);
            loop {
                let connection = tokio::select! {
                    _ = &mut stopping => break,
                    result = listener.accept() => result,
                };
                let (mut stream, _) = match connection {
                    Ok(connection) => connection,
                    Err(error) => {
                        errors.push(format!("accept: {error}"));
                        break;
                    }
                };
                let request = tokio::select! {
                    _ = &mut stopping => break,
                    result = tokio::time::timeout(Duration::from_secs(5), RecordedRequest::read(&mut stream)) => result,
                };
                let request = match request {
                    Ok(Ok(request)) => request,
                    other => {
                        errors.push(format!("read: {other:?}"));
                        break;
                    }
                };
                stored.lock().unwrap().push(request.clone());
                let creating = request.method == "POST" && request.path == "/v1/batches";
                let observed = match hold {
                    Some(HeldPhase::Upload) => {
                        request.method == "POST" && request.path == "/v1/files"
                    }
                    Some(HeldPhase::Creation) | None => creating,
                };
                if observed {
                    if let Some(seen) = seen.take() {
                        let _ = seen.send(());
                    }
                    if hold.is_some()
                        && let Some(released) = released.take()
                    {
                        tokio::select! { _ = &mut stopping => break, _ = released => {} }
                    }
                }
                let reply = match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/v1/files") if upload_rejected => ScriptedResponse::error(400, r#"{"error":{"type":"invalid_request_error","code":"upload_rejected","message":"upload rejected"}}"#),
                    ("POST", "/v1/files") => {
                        let id = if uploads == 0 { Self::FILE.to_owned() } else { format!("{}-{uploads}", Self::FILE) };
                        uploads += 1;
                        ScriptedResponse::json(&json!({"id":id,"object":"file","bytes":rows.len(),"created_at":1,"filename":"batch-input.jsonl","purpose":"batch","status":"processed"}).to_string())
                    }
                    ("GET", "/v1/files/file-owned-batch-input") => ScriptedResponse::json(&json!({"id":Self::FILE,"object":"file","bytes":rows.len(),"created_at":1,"filename":"batch-input.jsonl","purpose":"batch","status":"processed"}).to_string()),
                    ("GET", "/v1/files/file-owned-batch-input/content") => ScriptedResponse::chunks(200, "application/jsonl", vec![rows.as_bytes().to_vec()]),
                    ("POST", "/v1/batches") => {
                        match creation {
                            Creation::Status(status) => ScriptedResponse::error(status, r#"{"error":{"message":"uncertain creation"}}"#),
                            Creation::MalformedAcceptance => ScriptedResponse::json(r#"{"id":"batch_owned","status":"unrecognized"}"#),
                            Creation::Rejected => ScriptedResponse::error(400, r#"{"error":{"type":"invalid_request_error","code":"batch_rejected","message":"authoritative batch rejection"}}"#),
                            Creation::Accepted => {
                                let input: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
                                let suffix = input["input_file_id"].as_str().unwrap().strip_prefix(Self::FILE).unwrap();
                                ScriptedResponse::json(&json!({"id":format!("batch_owned{suffix}"),"status":"validating"}).to_string())
                            },
                            Creation::Disconnected => { if let Some(answered) = answered.take() { let _ = answered.send(()); } continue; }
                        }
                    }
                    ("DELETE", "/v1/files/file-owned-batch-input") if delete_rejected => ScriptedResponse::error(403, r#"{"error":{"type":"permission_error","code":"delete_rejected","message":"cleanup rejected"}}"#),
                    ("DELETE", path) if path.starts_with("/v1/files/file-owned-batch-input") => ScriptedResponse::json(&json!({"id":path.strip_prefix("/v1/files/").unwrap(),"deleted":true}).to_string()),
                    _ => { errors.push(format!("unexpected request: {} {}", request.method, request.path)); ScriptedResponse::error(404, r#"{"error":{"message":"unexpected fixture request"}}"#) }
                };
                if !response_delay.is_zero() && request.method == "POST" {
                    tokio::select! {
                        _ = &mut stopping => break,
                        _ = tokio::time::sleep(response_delay) => {}
                    }
                }
                let written =
                    tokio::time::timeout(Duration::from_secs(5), reply.write(&mut stream)).await;
                let cancelled_peer = (hold.is_some() || !response_delay.is_zero())
                    && matches!(&written, Ok(Err(error)) if error.downcast_ref::<std::io::Error>().is_some_and(|error| matches!(error.kind(), std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset)));
                if !matches!(written, Ok(Ok(()))) && !cancelled_peer {
                    errors.push(format!("write: {written:?}"));
                }
                reply_count.fetch_add(1, Ordering::Release);
                reply_changed.notify_waiters();
                if observed && let Some(answered) = answered.take() {
                    let _ = answered.send(());
                }
            }
            errors
        });
        Self {
            url,
            requests,
            stop: Some(stop),
            task: Some(task),
            creation_seen,
            release_creation: Some(release_creation),
            creation_answered,
            replies,
            replied,
        }
    }

    async fn wait_replies(&self, count: usize) {
        loop {
            let notified = self.replied.notified();
            if self.replies.load(Ordering::Acquire) >= count {
                return;
            }
            notified.await;
        }
    }

    fn inline() -> BatchSubmitRequest {
        BatchSubmitRequest::Inline {
            rows: vec![BatchInput::new("row-0", WireRequest::user("fixture")).unwrap()],
        }
    }

    fn borrowed() -> BatchSubmitRequest {
        BatchSubmitRequest::ProviderFile {
            file_id: Self::FILE.into(),
        }
    }

    async fn finish(mut self) -> (Vec<RecordedRequest>, String, bool) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let mut task = self.task.take().unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(5), &mut task).await;
        let (diagnostics, successful) = match outcome {
            Ok(result) => {
                let successful = matches!(&result, Ok(errors) if errors.is_empty());
                (format!("fixture={result:?}"), successful)
            }
            Err(error) => {
                task.abort();
                let joined = task.await;
                (
                    format!("fixture deadline={error}; joined={joined:?}"),
                    false,
                )
            }
        };
        let requests = self.requests.lock().unwrap().clone();
        (requests, diagnostics, successful)
    }

    async fn observe(self, request: BatchSubmitRequest) -> BatchObservation {
        self.observe_bounded(request, Duration::from_secs(10)).await
    }

    async fn observe_bounded(
        self,
        request: BatchSubmitRequest,
        deadline: Duration,
    ) -> BatchObservation {
        let provider = WireProvider::OpenAi.build(self.url.clone());
        let model = WireProvider::OpenAi.model(&provider, Self::MODEL);
        assert_eq!(model.model_id().as_str(), Self::MODEL);
        let result = tokio::time::timeout(deadline, model.submit_batch(&request)).await;
        let shutdown = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
        let (requests, fixture, fixture_succeeded) = self.finish().await;
        let cleanup = format!("shutdown={shutdown:?}; {fixture}; operation={result:?}");
        let cleanup_succeeded = matches!(shutdown, Ok(Ok(()))) && fixture_succeeded;
        BatchObservation {
            result: result.expect("bounded submit; cleanup attempted"),
            requests,
            cleanup,
            cleanup_succeeded,
        }
    }
}

impl Drop for BatchFixture {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

impl BatchObservation {
    fn cleanup_succeeded(&self) {
        assert!(self.cleanup_succeeded, "{}", self.cleanup);
    }
    fn deleted(&self) -> Vec<&RecordedRequest> {
        self.requests
            .iter()
            .filter(|request| request.method == "DELETE")
            .collect()
    }

    fn primary_rejection(&self) -> &SdkError {
        let error = self
            .result
            .as_ref()
            .expect_err("definitive provider rejection");
        assert_eq!(
            error.context().operation,
            Operation::SubmitBatch,
            "{}",
            self.cleanup
        );
        assert_eq!(error.http_status(), Some(400), "{}", self.cleanup);
        assert_eq!(error.kind(), ErrorKind::InvalidRequest, "{}", self.cleanup);
        assert_eq!(
            error.native_details().code.as_deref(),
            Some("batch_rejected"),
            "{}",
            self.cleanup
        );
        error
    }

    fn owned_deleted_after_rejection(&self) {
        self.primary_rejection();
        assert_eq!(
            self.deleted().len(),
            1,
            "owned input must be deleted exactly once: {:?}; {}",
            self.requests,
            self.cleanup
        );
        assert_eq!(
            self.deleted()[0].path,
            format!("/v1/files/{}", BatchFixture::FILE)
        );
        let create = self
            .requests
            .iter()
            .position(|request| request.path == "/v1/batches")
            .unwrap();
        let delete = self
            .requests
            .iter()
            .position(|request| request.method == "DELETE")
            .unwrap();
        assert!(create < delete);
        self.cleanup_succeeded();
    }
}

impl BatchFixture {
    fn assert_claim_or_report(
        submit: &Result<BatchJob, SdkError>,
        shutdown: &Result<(), SdkError>,
        diagnostics: &str,
    ) {
        match (submit, shutdown) {
            (Ok(job), Ok(())) => assert_eq!(job.id, "batch_owned", "{diagnostics}"),
            (Err(observed), Err(reported)) => {
                assert_eq!(observed.kind(), ErrorKind::InvalidState, "{diagnostics}");
                assert_eq!(reported.kind(), ErrorKind::InvalidState, "{diagnostics}");
                for resource in [Self::FILE, "batch_owned"] {
                    assert!(
                        Self::error_mentions(observed, resource)
                            && Self::error_mentions(reported, resource),
                        "one terminal authority retains {resource}: {diagnostics}"
                    );
                }
            }
            _ => panic!("receipt claim and shutdown report must agree: {diagnostics}"),
        }
    }

    fn error_mentions(error: &SdkError, resource: &str) -> bool {
        error
            .request_target()
            .is_some_and(|target| target.contains(resource))
            || error
                .native_details()
                .message
                .as_deref()
                .is_some_and(|message| message.contains(resource))
            || error.native_details().details.iter().any(
                |detail| matches!(detail, NativeDetail::Resource { name, .. } if name == resource),
            )
            || error
                .cause()
                .is_some_and(|cause| Self::error_mentions(cause, resource))
            || error
                .supplementary()
                .iter()
                .any(|error| Self::error_mentions(error, resource))
    }

    async fn reject_mismatched_models(models: &[&str]) {
        let observed = BatchFixture::start(models, Creation::Accepted, false, false, false)
            .await
            .observe(BatchFixture::borrowed())
            .await;
        let error = observed
            .result
            .as_ref()
            .expect_err("every native row must match the exact selected Model");
        assert!(
            matches!(error.kind(), ErrorKind::InvalidInput),
            "{}",
            observed.cleanup
        );
        assert_eq!(error.context().operation, Operation::SubmitBatch);
        assert!(
            !observed
                .requests
                .iter()
                .any(|request| request.method == "POST"),
            "validation must precede durable creation"
        );
        assert!(
            observed.deleted().is_empty(),
            "borrowed resource must not be deleted"
        );
        observed.cleanup_succeeded();
    }
}

#[tokio::test]
async fn batch_ownership_upload_and_creation_share_one_request_deadline() {
    for (budget, accepted) in [
        (Duration::from_secs(1), false),
        (Duration::from_secs(3), true),
    ] {
        // Both actual native stages fit an independent one-second budget, but
        // their combined duration exceeds it. No elapsed-time upper assertion
        // or assumption about the scheduling order supplies the oracle.
        let fixture = BatchFixture::start_timed(
            String::new(),
            Creation::Accepted,
            false,
            false,
            None,
            Duration::from_millis(600),
        )
        .await;
        let provider = Provider::from_config(ProviderConfig::new(
            ProviderKind::OpenAi(
                OpenAiConfig::new(ApiKey::new("wire-key").unwrap())
                    .with_base_url(fixture.url.clone()),
            ),
            TransportConfig::new(
                budget,
                Duration::from_secs(1),
                Duration::from_secs(1),
                RetryPolicy::default(),
            )
            .unwrap(),
        ))
        .unwrap();
        let model = WireProvider::OpenAi.model(&provider, BatchFixture::MODEL);
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            model.submit_batch(&BatchFixture::inline()),
        )
        .await;
        let shutdown = tokio::time::timeout(Duration::from_secs(5), provider.shutdown()).await;
        let (requests, fixture, joined) = fixture.finish().await;
        let diagnostics =
            format!("budget={budget:?}; result={result:?}; shutdown={shutdown:?}; {fixture}");
        assert!(joined && matches!(shutdown, Ok(Ok(()))), "{diagnostics}");
        let result = result.expect(&diagnostics);
        if accepted {
            assert_eq!(
                result.expect(&diagnostics).id,
                "batch_owned",
                "{diagnostics}"
            );
        } else {
            let error = result.expect_err("upload and create must not reset the original deadline");
            assert_eq!(error.kind(), ErrorKind::Timeout, "{diagnostics}");
            assert_eq!(
                error.context().operation,
                Operation::SubmitBatch,
                "{diagnostics}"
            );
            assert!(
                BatchFixture::error_mentions(&error, BatchFixture::FILE),
                "uncertain creation must retain known input: {diagnostics}"
            );
        }
        assert_eq!(
            requests
                .iter()
                .map(|request| (request.method.as_str(), request.path.as_str()))
                .collect::<Vec<_>>(),
            vec![("POST", "/v1/files"), ("POST", "/v1/batches")],
            "both native stages execute without deleting uncertain or accepted input: {diagnostics}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_ownership_concurrent_shutdown_seal_linearizes_admission() {
    for iteration in 0..8 {
        let fixture = BatchFixture::start(
            &[BatchFixture::MODEL],
            Creation::Accepted,
            false,
            false,
            false,
        )
        .await;
        let provider = WireProvider::OpenAi.build(fixture.url.clone());
        let model = WireProvider::OpenAi.model(&provider, BatchFixture::MODEL);
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let submit_start = Arc::clone(&barrier);
        let mut submit = tokio::spawn(async move {
            submit_start.wait().await;
            model.submit_batch(&BatchFixture::inline()).await
        });
        let close_start = Arc::clone(&barrier);
        let closing = provider.clone();
        let mut close = tokio::spawn(async move {
            close_start.wait().await;
            closing.shutdown().await
        });
        barrier.wait().await;
        let outcomes = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(&mut submit, &mut close)
        })
        .await;
        if outcomes.is_err() {
            submit.abort();
            close.abort();
            let _ = tokio::join!(submit, close);
        }
        let replay = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
        let (requests, cleanup, fixture_succeeded) = fixture.finish().await;
        let diagnostics =
            format!("iteration={iteration}; outcomes={outcomes:?}; replay={replay:?}; {cleanup}");
        assert!(fixture_succeeded, "{diagnostics}");
        let (submitted, closed) = outcomes.expect("bounded racing owners");
        let submitted = submitted.expect("submission owner joined");
        let closed = closed.expect("shutdown owner joined");
        let replay = replay.expect("bounded replay");
        assert_eq!(
            closed.as_ref().map_err(SdkError::kind),
            replay.as_ref().map_err(SdkError::kind),
            "{diagnostics}"
        );
        if submitted
            .as_ref()
            .is_err_and(|error| error.kind() == ErrorKind::Closed)
        {
            assert!(
                closed.is_ok() && requests.is_empty(),
                "sealed admission starts no native work: {diagnostics}"
            );
        } else {
            BatchFixture::assert_claim_or_report(&submitted, &closed, &diagnostics);
            assert_eq!(
                requests
                    .iter()
                    .filter(|request| request.method == "POST" && request.path == "/v1/files")
                    .count(),
                1,
                "{diagnostics}"
            );
            assert_eq!(
                requests
                    .iter()
                    .filter(|request| request.method == "POST" && request.path == "/v1/batches")
                    .count(),
                1,
                "{diagnostics}"
            );
        }
        assert!(
            !requests.iter().any(|request| request.method == "DELETE"),
            "{diagnostics}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_ownership_shutdown_and_live_observer_share_terminal_authority() {
    // Control each authority first, then allow the two public observations to
    // race. Native wire barriers establish the same accepted creation in all modes.
    for mode in 0..3 {
        let mut fixture = BatchFixture::start(
            &[BatchFixture::MODEL],
            Creation::Accepted,
            false,
            false,
            true,
        )
        .await;
        let provider = WireProvider::OpenAi.build(fixture.url.clone());
        let model = WireProvider::OpenAi.model(&provider, BatchFixture::MODEL);
        let request = BatchFixture::inline();
        let mut observer = Box::pin(model.submit_batch(&request));
        let mut premature = None;
        let entered = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                entered = &mut fixture.creation_seen => entered.map_err(|error| error.to_string()),
                result = &mut observer => {
                    let message = format!("submission settled before held creation: {result:?}");
                    premature = Some(result);
                    Err(message)
                },
            }
        })
        .await;
        let release = fixture.release_creation.take().unwrap().send(());
        let answered =
            tokio::time::timeout(Duration::from_secs(5), &mut fixture.creation_answered).await;
        let (submitted, closed) = if let Some(premature) = premature {
            (
                Ok(premature),
                tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await,
            )
        } else {
            match mode {
                0 => {
                    let submitted =
                        tokio::time::timeout(Duration::from_secs(10), &mut observer).await;
                    let closed =
                        tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
                    (submitted, closed)
                }
                1 => {
                    let closed =
                        tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
                    let submitted =
                        tokio::time::timeout(Duration::from_secs(10), &mut observer).await;
                    (submitted, closed)
                }
                _ => {
                    let start = tokio::sync::Barrier::new(2);
                    tokio::join!(
                        async {
                            start.wait().await;
                            tokio::time::timeout(Duration::from_secs(10), &mut observer).await
                        },
                        async {
                            start.wait().await;
                            tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await
                        },
                    )
                }
            }
        };
        drop(observer);
        let replay = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
        let (requests, cleanup, fixture_succeeded) = fixture.finish().await;
        let diagnostics = format!(
            "mode={mode}; entered={entered:?}; release={release:?}; answered={answered:?}; submitted={submitted:?}; closed={closed:?}; replay={replay:?}; {cleanup}"
        );
        assert!(
            matches!(entered, Ok(Ok(()))) && matches!(answered, Ok(Ok(()))) && fixture_succeeded,
            "{diagnostics}"
        );
        let submitted = submitted.expect("bounded observation after native acceptance");
        let closed = closed.expect("shutdown owns accepted work independently of observer polling");
        let replay = replay.expect("bounded shutdown replay");
        BatchFixture::assert_claim_or_report(&submitted, &closed, &diagnostics);
        if mode == 0 {
            assert!(
                submitted.is_ok() && closed.is_ok(),
                "claim-first control: {diagnostics}"
            );
        }
        if mode == 1 {
            assert!(
                submitted.is_err() && closed.is_err(),
                "report-first control: {diagnostics}"
            );
        }
        assert_eq!(
            closed.as_ref().map_err(SdkError::kind),
            replay.as_ref().map_err(SdkError::kind),
            "{diagnostics}"
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.method == "POST" && request.path == "/v1/files")
                .count(),
            1,
            "{diagnostics}"
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.method == "POST" && request.path == "/v1/batches")
                .count(),
            1,
            "{diagnostics}"
        );
        assert!(
            !requests.iter().any(|request| request.method == "DELETE"),
            "{diagnostics}"
        );
    }
}

#[tokio::test]
async fn batch_file_validation_rejects_malformed_or_ambiguous_binding_before_creation() {
    let valid = json!({"custom_id":"row-0","method":"POST","url":"/v1/responses","body":{"model":BatchFixture::MODEL,"input":"fixture"}});
    let mut cases = vec![
        String::new(),
        "{}".to_owned(),
        "null".to_owned(),
        "[".to_owned(),
        format!("{valid}\n\n{valid}"),
        format!("{valid}\n{valid}"),
    ];
    for (pointer, value) in [
        ("/body/model", serde_json::Value::Null),
        ("/body/model", json!(7)),
        ("/body/model", json!("gpt-4.1 ")),
        ("/method", json!("GET")),
        ("/url", json!("/v1/embeddings")),
        ("/custom_id", json!("")),
    ] {
        let mut row = valid.clone();
        *row.pointer_mut(pointer).unwrap() = value;
        cases.push(row.to_string());
    }
    cases.push(r#"{"custom_id":"row-0","method":"POST","url":"/v1/responses","body":{"model":"gpt-4.1-mini","model":"gpt-4.1"}}"#.to_owned());
    for rows in cases {
        let observed =
            BatchFixture::start_rows(rows.clone(), Creation::Accepted, false, false, None)
                .await
                .observe(BatchFixture::borrowed())
                .await;
        let error = observed
            .result
            .as_ref()
            .expect_err(&format!("invalid binding must reject: {rows}"));
        assert_eq!(
            error.kind(),
            ErrorKind::InvalidInput,
            "{rows}: {}",
            observed.cleanup
        );
        assert_eq!(error.context().operation, Operation::SubmitBatch);
        assert_eq!(error.context().phase, ErrorPhase::InputValidation);
        assert!(
            !observed
                .requests
                .iter()
                .any(|request| request.method == "POST" || request.method == "DELETE"),
            "{rows}: {:?}",
            observed.requests
        );
        observed.cleanup_succeeded();
    }
}

#[tokio::test]
async fn batch_file_validation_preserves_crlf_unknown_options_and_original_file() {
    let rows = format!(
        "{}\r\n{}\r\n",
        json!({"custom_id":"first","method":"POST","url":"/v1/responses","body":{"model":BatchFixture::MODEL,"input":"fixture","future_native_option":{"nested":true}}}),
        json!({"custom_id":"last","method":"POST","url":"/v1/responses","body":{"model":BatchFixture::MODEL,"input":"fixture"}})
    );
    let observed = BatchFixture::start_rows(rows, Creation::Accepted, false, false, None)
        .await
        .observe(BatchFixture::borrowed())
        .await;
    assert!(observed.result.is_ok(), "{}", observed.cleanup);
    let posts = observed
        .requests
        .iter()
        .filter(|request| request.method == "POST")
        .collect::<Vec<_>>();
    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0].path, "/v1/batches");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&posts[0].body).unwrap()["input_file_id"],
        BatchFixture::FILE
    );
    assert!(observed.deleted().is_empty());
    observed.cleanup_succeeded();
}

#[tokio::test]
async fn reaudit_openai_provider_file_rejects_model_mismatch() {
    BatchFixture::reject_mismatched_models(&["gpt-4.1-mini"]).await;
}

#[tokio::test]
async fn reaudit_openai_provider_file_rejects_later_row_model_mismatch() {
    BatchFixture::reject_mismatched_models(&[BatchFixture::MODEL, "gpt-4.1-mini"]).await;
}

#[tokio::test]
async fn reaudit_openai_provider_file_matching_model_remains_supported() {
    let observed = BatchFixture::start(
        &[BatchFixture::MODEL, BatchFixture::MODEL],
        Creation::Accepted,
        false,
        false,
        false,
    )
    .await
    .observe(BatchFixture::borrowed())
    .await;
    assert_eq!(
        observed
            .result
            .as_ref()
            .expect("matching provider files remain supported")
            .id,
        "batch_owned"
    );
    let creations = observed
        .requests
        .iter()
        .filter(|request| request.method == "POST" && request.path == "/v1/batches")
        .collect::<Vec<_>>();
    assert_eq!(creations.len(), 1);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&creations[0].body).unwrap()["input_file_id"],
        BatchFixture::FILE
    );
    assert!(
        !observed
            .requests
            .iter()
            .any(|request| request.method == "POST" && request.path == "/v1/files")
    );
    assert!(observed.deleted().is_empty());
    observed.cleanup_succeeded();
}

#[tokio::test]
async fn reaudit_openai_inline_rejection_deletes_owned_input() {
    BatchFixture::start(
        &[BatchFixture::MODEL],
        Creation::Rejected,
        false,
        false,
        false,
    )
    .await
    .observe(BatchFixture::inline())
    .await
    .owned_deleted_after_rejection();
}

#[tokio::test]
async fn reaudit_openai_inline_cleanup_failure_preserves_primary() {
    let observed = BatchFixture::start(
        &[BatchFixture::MODEL],
        Creation::Rejected,
        false,
        true,
        false,
    )
    .await
    .observe(BatchFixture::inline())
    .await;
    let error = observed.primary_rejection();
    assert!(
        error
            .supplementary()
            .iter()
            .any(|cleanup| cleanup.context().phase == ErrorPhase::Cleanup
                && cleanup.http_status() == Some(403)
                && cleanup
                    .request_target()
                    .is_some_and(|target| target.ends_with(BatchFixture::FILE))),
        "primary must retain actionable cleanup failure for the owned input: {}",
        observed.cleanup
    );
    observed.owned_deleted_after_rejection();
}

#[tokio::test]
async fn reaudit_openai_inline_creation_controls_preserve_resource_ownership() {
    let upload = BatchFixture::start(
        &[BatchFixture::MODEL],
        Creation::Rejected,
        true,
        false,
        false,
    )
    .await
    .observe(BatchFixture::inline())
    .await;
    assert!(upload.result.is_err());
    assert!(upload.deleted().is_empty());
    assert!(
        !upload
            .requests
            .iter()
            .any(|request| request.path == "/v1/batches")
    );
    let accepted = BatchFixture::start(
        &[BatchFixture::MODEL],
        Creation::Accepted,
        false,
        false,
        false,
    )
    .await
    .observe(BatchFixture::inline())
    .await;
    assert!(accepted.result.is_ok());
    assert!(
        accepted.deleted().is_empty(),
        "accepted job still owns input"
    );
    let borrowed = BatchFixture::start(
        &[BatchFixture::MODEL],
        Creation::Rejected,
        false,
        false,
        false,
    )
    .await
    .observe(BatchFixture::borrowed())
    .await;
    borrowed.primary_rejection();
    assert!(
        borrowed.deleted().is_empty(),
        "rejection does not transfer borrowed file ownership"
    );
    let ambiguous = BatchFixture::start(
        &[BatchFixture::MODEL],
        Creation::Disconnected,
        false,
        false,
        false,
    )
    .await
    .observe(BatchFixture::inline())
    .await;
    let error = ambiguous
        .result
        .as_ref()
        .expect_err("lost creation response");
    assert_eq!(error.context().operation, Operation::SubmitBatch);
    assert_eq!(error.kind(), ErrorKind::Transport);
    let upload_position = ambiguous
        .requests
        .iter()
        .position(|request| request.method == "POST" && request.path == "/v1/files")
        .expect("input upload occurred");
    let creation_position = ambiguous
        .requests
        .iter()
        .position(|request| request.method == "POST" && request.path == "/v1/batches")
        .expect("creation reached remote before response was lost");
    assert!(upload_position < creation_position);
    assert!(
        ambiguous.deleted().is_empty(),
        "lost response does not prove the durable job was rejected"
    );
    for observation in [&upload, &accepted, &borrowed, &ambiguous] {
        observation.cleanup_succeeded();
    }
}

#[tokio::test]
async fn batch_payload_row_bound_rejects_before_upload() {
    let request = BatchSubmitRequest::Inline {
        rows: (0..50_001)
            .map(|index| {
                BatchInput::new(format!("row-{index}"), WireRequest::user("fixture")).unwrap()
            })
            .collect(),
    };
    let observed = BatchFixture::start(
        &[BatchFixture::MODEL],
        Creation::Accepted,
        false,
        false,
        false,
    )
    .await
    .observe_bounded(request, Duration::from_secs(60))
    .await;
    let error = observed
        .result
        .as_ref()
        .expect_err("row ceiling applies before upload");
    assert_eq!(
        error.kind(),
        ErrorKind::InvalidInput,
        "{}",
        observed.cleanup
    );
    assert_eq!(error.context().operation, Operation::SubmitBatch);
    assert!(
        observed.requests.is_empty(),
        "row bound must reject before any I/O"
    );
    observed.cleanup_succeeded();
}

#[tokio::test]
async fn batch_payload_byte_bound_rejects_before_upload() {
    let request = BatchSubmitRequest::Inline {
        rows: vec![
            BatchInput::new(
                "row-0",
                WireRequest::user(&"x".repeat(200 * 1024 * 1024 + 1)),
            )
            .unwrap(),
        ],
    };
    let observed = BatchFixture::start(
        &[BatchFixture::MODEL],
        Creation::Accepted,
        false,
        false,
        false,
    )
    .await
    .observe_bounded(request, Duration::from_secs(90))
    .await;
    let error = observed
        .result
        .as_ref()
        .expect_err("byte ceiling applies before upload");
    assert_eq!(
        error.kind(),
        ErrorKind::InvalidInput,
        "{}",
        observed.cleanup
    );
    assert_eq!(error.context().operation, Operation::SubmitBatch);
    assert!(
        observed.requests.is_empty(),
        "byte bound must reject before any I/O"
    );
    observed.cleanup_succeeded();
}

#[tokio::test]
async fn batch_payload_near_ceiling_remains_admissible() {
    let bytes = 200 * 1024 * 1024 - 8192;
    let request = BatchSubmitRequest::Inline {
        rows: vec![BatchInput::new("row-0", WireRequest::user(&"x".repeat(bytes))).unwrap()],
    };
    let observed = BatchFixture::start(
        &[BatchFixture::MODEL],
        Creation::Accepted,
        false,
        false,
        false,
    )
    .await
    .observe_bounded(request, Duration::from_secs(90))
    .await;
    assert!(
        observed.result.is_ok(),
        "largest supported single payload fits aggregate admission: {}",
        observed.cleanup
    );
    let uploads = observed
        .requests
        .iter()
        .filter(|request| request.method == "POST" && request.path == "/v1/files")
        .collect::<Vec<_>>();
    assert_eq!(uploads.len(), 1);
    assert!(
        uploads[0].body.len() > bytes,
        "real large JSONL must cross upload boundary"
    );
    assert!(
        uploads[0].body.len() < 200 * 1024 * 1024,
        "the selected payload is under the input bound including native wrapping"
    );
    assert!(observed.deleted().is_empty());
    observed.cleanup_succeeded();
}

#[tokio::test]
async fn batch_ownership_unclaimed_receipts_are_bounded_and_retained_for_shutdown() {
    let fixture = BatchFixture::start(
        &[BatchFixture::MODEL],
        Creation::Accepted,
        false,
        false,
        false,
    )
    .await;
    let provider = WireProvider::OpenAi.build(fixture.url.clone());
    let model = WireProvider::OpenAi.model(&provider, BatchFixture::MODEL);
    let request = BatchFixture::inline();
    let exercise = tokio::time::timeout(Duration::from_secs(20), async {
        for index in 0..64 {
            let mut observer = Box::pin(model.submit_batch(&request));
            if let std::task::Poll::Ready(result) = futures_util::poll!(observer.as_mut()) {
                return Err(format!(
                    "unobserved submission {index} settled early: {result:?}"
                ));
            }
            fixture.wait_replies((index + 1) * 2).await;
            drop(observer);
        }
        Ok(tokio::time::timeout(Duration::from_millis(250), model.submit_batch(&request)).await)
    })
    .await;
    let shutdown = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
    let (requests, cleanup, fixture_succeeded) = fixture.finish().await;
    let diagnostics = format!("exercise={exercise:?}; shutdown={shutdown:?}; {cleanup}");
    assert!(
        matches!(exercise, Ok(Ok(Ok(Err(ref error)))) if error.kind() == ErrorKind::InvalidState),
        "receipt exhaustion must reject before I/O: {diagnostics}"
    );
    assert!(
        matches!(shutdown, Ok(Err(_))) && fixture_succeeded,
        "unclaimed receipts remain reportable: {diagnostics}"
    );
    let error = shutdown.as_ref().unwrap().as_ref().unwrap_err();
    for index in 0..64 {
        let suffix = if index == 0 {
            String::new()
        } else {
            format!("-{index}")
        };
        for resource in [
            format!("{}{suffix}", BatchFixture::FILE),
            format!("batch_owned{suffix}"),
        ] {
            assert!(error.native_details().details.iter().any(|detail| matches!(detail, NativeDetail::Resource { name, .. } if name == &resource)), "each exact service-observed resource survives bounded receipt reporting: {resource}; {diagnostics}");
        }
    }
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method == "POST" && request.path == "/v1/files")
            .count(),
        64,
        "{diagnostics}"
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method == "POST" && request.path == "/v1/batches")
            .count(),
        64,
        "{diagnostics}"
    );
    assert!(
        !requests.iter().any(|request| request.method == "DELETE"),
        "accepted inputs cannot be deleted: {diagnostics}"
    );
}

#[tokio::test]
async fn batch_payload_aggregate_admission_rejects_before_io_and_releases_bytes() {
    // Exercise the byte budget independently of the task count. Run serially:
    // each real native request carries 150 MiB, below the single-input ceiling.
    let mut fixture = BatchFixture::start_rows(
        String::new(),
        Creation::Accepted,
        false,
        false,
        Some(HeldPhase::Upload),
    )
    .await;
    let provider = WireProvider::OpenAi.build(fixture.url.clone());
    let model = WireProvider::OpenAi.model(&provider, BatchFixture::MODEL);
    let request = BatchSubmitRequest::Inline {
        rows: vec![
            BatchInput::new(
                "large-row",
                WireRequest::user(&"x".repeat(150 * 1024 * 1024)),
            )
            .unwrap(),
        ],
    };
    let mut observers = (0..2)
        .map(|_| Box::pin(model.submit_batch(&request)))
        .collect::<Vec<_>>();
    let mut initially_pending = true;
    for observer in &mut observers {
        initially_pending &= futures_util::poll!(observer.as_mut()).is_pending();
    }
    let mut admitted = Box::pin(futures_util::future::join_all(observers));
    let mut premature = None;
    let entered = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::select! {
            entered = &mut fixture.creation_seen => entered.map_err(|error| error.to_string()),
            completed = &mut admitted => {
                premature = Some(completed);
                Err("admitted requests settled before the held upload".to_owned())
            }
        }
    })
    .await;
    let overflow =
        tokio::time::timeout(Duration::from_secs(15), model.submit_batch(&request)).await;
    let release = fixture.release_creation.take().unwrap().send(());
    let accepted = match premature {
        Some(completed) => Ok(completed),
        None => tokio::time::timeout(Duration::from_secs(90), admitted).await,
    };
    let reused = tokio::time::timeout(Duration::from_secs(90), model.submit_batch(&request)).await;
    let shutdown = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
    let (requests, cleanup, fixture_succeeded) = fixture.finish().await;
    let diagnostics = format!(
        "entered={entered:?}; overflow={overflow:?}; release={release:?}; accepted={accepted:?}; reused={reused:?}; shutdown={shutdown:?}; {cleanup}"
    );
    assert!(initially_pending, "upload is held: {diagnostics}");
    assert!(
        matches!(overflow, Ok(Err(ref error)) if error.kind() == ErrorKind::InvalidState && error.context().operation == Operation::SubmitBatch),
        "aggregate budget must reject before upload: {diagnostics}"
    );
    assert!(
        matches!(entered, Ok(Ok(())))
            && matches!(&accepted, Ok(results) if results.len() == 2 && results.iter().all(Result::is_ok)),
        "{diagnostics}"
    );
    assert!(
        matches!(reused, Ok(Ok(_))) && matches!(shutdown, Ok(Ok(()))) && fixture_succeeded,
        "capacity must be reusable after settlement: {diagnostics}"
    );
    let uploads = requests
        .iter()
        .filter(|request| request.method == "POST" && request.path == "/v1/files")
        .collect::<Vec<_>>();
    assert_eq!(uploads.len(), 3, "overflow must not upload: {diagnostics}");
    assert!(
        uploads
            .iter()
            .all(|request| request.body.len() > 150 * 1024 * 1024),
        "all admitted inputs are real large uploads: {diagnostics}"
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method == "POST" && request.path == "/v1/batches")
            .count(),
        3,
        "{diagnostics}"
    );
    assert!(
        !requests.iter().any(|request| request.method == "DELETE"),
        "accepted inputs remain owned by jobs: {diagnostics}"
    );
}

#[tokio::test]
async fn batch_ownership_inflight_bound_rejects_before_io_and_releases_capacity() {
    let mut fixture = BatchFixture::start(
        &[BatchFixture::MODEL],
        Creation::Accepted,
        false,
        false,
        true,
    )
    .await;
    let provider = WireProvider::OpenAi.build(fixture.url.clone());
    let model = WireProvider::OpenAi.model(&provider, BatchFixture::MODEL);
    let request = BatchFixture::inline();
    let mut observers = (0..16)
        .map(|_| Box::pin(model.submit_batch(&request)))
        .collect::<Vec<_>>();
    let mut initially_pending = true;
    for observer in &mut observers {
        initially_pending &= futures_util::poll!(observer.as_mut()).is_pending();
    }
    let entered = tokio::time::timeout(Duration::from_secs(10), &mut fixture.creation_seen).await;
    let overflow =
        tokio::time::timeout(Duration::from_millis(250), model.submit_batch(&request)).await;
    let release = fixture.release_creation.take().unwrap().send(());
    let accepted = tokio::time::timeout(
        Duration::from_secs(10),
        futures_util::future::join_all(observers),
    )
    .await;
    let reused = tokio::time::timeout(Duration::from_secs(10), model.submit_batch(&request)).await;
    let shutdown = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
    let (requests, cleanup, fixture_succeeded) = fixture.finish().await;
    let diagnostics = format!(
        "entered={entered:?}; overflow={overflow:?}; release={release:?}; accepted={accepted:?}; reused={reused:?}; shutdown={shutdown:?}; {cleanup}"
    );
    assert!(initially_pending, "native create is held; {diagnostics}");
    assert!(
        matches!(overflow, Ok(Err(ref error)) if error.kind() == ErrorKind::InvalidState && error.context().operation == Operation::SubmitBatch),
        "bounded admission must fail before native I/O: {diagnostics}"
    );
    assert!(
        matches!(entered, Ok(Ok(())))
            && matches!(&accepted, Ok(results) if results.len() == 16 && results.iter().all(Result::is_ok)),
        "{diagnostics}"
    );
    assert!(
        matches!(reused, Ok(Ok(_))) && matches!(shutdown, Ok(Ok(()))) && fixture_succeeded,
        "{diagnostics}"
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method == "POST" && request.path == "/v1/files")
            .count(),
        17,
        "rejected overflow never uploads: {diagnostics}"
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method == "POST" && request.path == "/v1/batches")
            .count(),
        17,
        "only admitted submissions create: {diagnostics}"
    );
}

#[tokio::test]
async fn batch_ownership_uncertain_http_and_malformed_acceptance_retain_input() {
    for creation in [
        Creation::Status(408),
        Creation::Status(409),
        Creation::Status(429),
        Creation::Status(500),
        Creation::Status(503),
        Creation::MalformedAcceptance,
    ] {
        let observed = BatchFixture::start(&[BatchFixture::MODEL], creation, false, false, false)
            .await
            .observe(BatchFixture::inline())
            .await;
        assert!(observed.result.is_err(), "{}", observed.cleanup);
        assert!(
            observed
                .requests
                .iter()
                .any(|request| request.method == "POST" && request.path == "/v1/files"),
            "{}",
            observed.cleanup
        );
        assert_eq!(
            observed
                .requests
                .iter()
                .filter(|request| request.method == "POST" && request.path == "/v1/batches")
                .count(),
            1,
            "creation must remain fail-fast: {}",
            observed.cleanup
        );
        assert!(
            observed.deleted().is_empty(),
            "uncertain admission cannot authorize deletion: {}",
            observed.cleanup
        );
        let error = observed.result.as_ref().unwrap_err();
        assert!(
            BatchFixture::error_mentions(error, BatchFixture::FILE),
            "caller needs the retained owned input identity: {}",
            observed.cleanup
        );
        observed.cleanup_succeeded();
    }
}

#[tokio::test]
async fn batch_ownership_unobserved_acceptance_is_reported_by_shutdown() {
    let mut fixture = BatchFixture::start(
        &[BatchFixture::MODEL],
        Creation::Accepted,
        false,
        false,
        true,
    )
    .await;
    let provider = WireProvider::OpenAi.build(fixture.url.clone());
    let model = WireProvider::OpenAi.model(&provider, BatchFixture::MODEL);
    let request = BatchFixture::inline();
    let mut observer = Box::pin(model.submit_batch(&request));
    let entered = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            result = &mut observer => panic!("held create settled early: {result:?}"),
            entered = &mut fixture.creation_seen => entered,
        }
    })
    .await;
    let released = fixture.release_creation.take().unwrap().send(());
    let answered =
        tokio::time::timeout(Duration::from_secs(5), &mut fixture.creation_answered).await;
    // The native response is delivered while the operation observer is unpolled.
    // Dropping it cannot make send-success equivalent to receipt consumption.
    drop(observer);
    let shutdown = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
    let replay = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
    let (requests, cleanup, fixture_succeeded) = fixture.finish().await;
    let diagnostics = format!(
        "entered={entered:?}; release={released:?}; answered={answered:?}; shutdown={shutdown:?}; replay={replay:?}; {cleanup}"
    );
    assert!(
        matches!(entered, Ok(Ok(()))) && matches!(answered, Ok(Ok(()))) && fixture_succeeded,
        "{diagnostics}"
    );
    let error = shutdown
        .expect("bounded shutdown")
        .expect_err("unclaimed accepted work must be reported");
    let replay = replay
        .expect("bounded replay")
        .expect_err("same unclaimed terminal result");
    for resource in [BatchFixture::FILE, "batch_owned"] {
        assert!(
            BatchFixture::error_mentions(&error, resource),
            "resource={resource}; {diagnostics}"
        );
        assert!(
            BatchFixture::error_mentions(&replay, resource),
            "replayed resource={resource}; {diagnostics}"
        );
    }
    assert_eq!(error.kind(), replay.kind());
    assert!(
        !requests.iter().any(|request| request.method == "DELETE"),
        "accepted input is retained; {diagnostics}"
    );
}

#[tokio::test]
async fn batch_ownership_runtime_loss_wakes_observer_without_shutdown() {
    let mut fixture = BatchFixture::start_rows(
        format!("{}\n", json!({"custom_id":"row-0","method":"POST","url":"/v1/responses","body":{"model":BatchFixture::MODEL,"input":"fixture"}})),
        Creation::Accepted, false, false, Some(HeldPhase::Upload),
    ).await;
    let provider = WireProvider::OpenAi.build(fixture.url.clone());
    let model = WireProvider::OpenAi.model(&provider, BatchFixture::MODEL);
    let (_, placeholder) = oneshot::channel();
    let entered = std::mem::replace(&mut fixture.creation_seen, placeholder);
    let (transferred, observer) = oneshot::channel();
    let (stopped, runtime_stopped) = oneshot::channel();
    let owner = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let mut operation = Box::pin(async move {
                model.submit_batch(&BatchFixture::inline()).await
            });
            tokio::select! {
                result = &mut operation => panic!("operation settled before held upload: {result:?}"),
                entered = entered => {
                    entered.expect("actual upload reached fixture");
                    assert!(transferred.send(operation).is_ok());
                }
            }
        });
        // Runtime destruction cannot guarantee remote cleanup. It must still
        // publish a local terminal failure to an observer retained elsewhere.
        drop(runtime);
        let _ = stopped.send(());
    });
    let observed =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(10), async {
            let operation = observer.await.map_err(|_| "observer transfer failed")?;
            runtime_stopped
                .await
                .map_err(|_| "native task runtime did not stop")?;
            Ok::<_, &str>(operation.await)
        }))
        .catch_unwind()
        .await;
    let release = fixture.release_creation.take().unwrap().send(());
    let answered =
        tokio::time::timeout(Duration::from_secs(5), &mut fixture.creation_answered).await;
    let shutdown = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
    // Stopping the fixture also releases its upload-entry sender on setup
    // failure, so the dedicated runtime cannot remain stuck at that barrier.
    let (requests, cleanup, fixture_succeeded) = fixture.finish().await;
    let joined = owner.join();
    let diagnostics = format!(
        "operation={observed:?}; release={release:?}; answered={answered:?}; shutdown={shutdown:?}; owner={joined:?}; {cleanup}"
    );
    assert!(matches!(observed, Ok(Ok(Ok(Err(_))))), "{diagnostics}");
    assert!(matches!(answered, Ok(Ok(()))), "{diagnostics}");
    assert!(
        shutdown.is_ok() && joined.is_ok() && fixture_succeeded,
        "{diagnostics}"
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method == "POST" && request.path == "/v1/files")
            .count(),
        1,
        "{diagnostics}"
    );
    assert!(
        !requests
            .iter()
            .any(|request| request.path == "/v1/batches" || request.method == "DELETE"),
        "uncertain upload identity cannot be guessed: {diagnostics}"
    );
}

#[tokio::test]
async fn batch_ownership_cancelled_upload_deletes_only_unsubmitted_input() {
    let mut fixture = BatchFixture::start_rows(
        String::new(),
        Creation::Accepted,
        false,
        false,
        Some(HeldPhase::Upload),
    )
    .await;
    let provider = WireProvider::OpenAi.build(fixture.url.clone());
    let model = WireProvider::OpenAi.model(&provider, BatchFixture::MODEL);
    let mut submit = tokio::spawn(async move { model.submit_batch(&BatchFixture::inline()).await });
    let reached = tokio::time::timeout(Duration::from_secs(10), &mut fixture.creation_seen).await;
    submit.abort();
    let caller = tokio::time::timeout(Duration::from_secs(5), &mut submit).await;
    fixture.release_creation.take().unwrap().send(()).ok();
    let answered =
        tokio::time::timeout(Duration::from_secs(5), &mut fixture.creation_answered).await;
    let shutdown = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
    let (requests, diagnostics, fixture_succeeded) = fixture.finish().await;
    assert!(
        matches!(reached, Ok(Ok(()))),
        "upload barrier: {reached:?}; shutdown: {shutdown:?}; {diagnostics}"
    );
    assert!(
        matches!(caller, Ok(Err(ref error)) if error.is_cancelled()),
        "caller: {caller:?}"
    );
    assert!(
        matches!(answered, Ok(Ok(()))),
        "upload answered: {answered:?}; {diagnostics}"
    );
    assert!(shutdown.is_ok(), "shutdown bounded: {shutdown:?}");
    assert!(
        !requests.iter().any(|request| request.path == "/v1/batches"),
        "abandoned upload must not start batch: {requests:?}"
    );
    let deleted = requests
        .iter()
        .filter(|request| request.method == "DELETE")
        .collect::<Vec<_>>();
    assert_eq!(
        deleted.len(),
        1,
        "known uploaded input must retain cleanup ownership: {requests:?}; {diagnostics}"
    );
    assert_eq!(deleted[0].path, format!("/v1/files/{}", BatchFixture::FILE));
    assert!(fixture_succeeded, "{diagnostics}");
}

#[tokio::test]
async fn reaudit_openai_inline_dropped_submission_retains_cleanup_ownership() {
    for creation in [Creation::Accepted, Creation::Rejected] {
        let mut fixture =
            BatchFixture::start(&[BatchFixture::MODEL], creation, false, false, true).await;
        let provider = WireProvider::OpenAi.build(fixture.url.clone());
        let model = WireProvider::OpenAi.model(&provider, BatchFixture::MODEL);
        let mut submit =
            tokio::spawn(async move { model.submit_batch(&BatchFixture::inline()).await });
        let reached =
            tokio::time::timeout(Duration::from_secs(10), &mut fixture.creation_seen).await;
        let before_release = fixture.requests.lock().unwrap().clone();
        submit.abort();
        let caller = tokio::time::timeout(Duration::from_secs(5), &mut submit).await;
        fixture.release_creation.take().unwrap().send(()).ok();
        let answered =
            tokio::time::timeout(Duration::from_secs(5), &mut fixture.creation_answered).await;
        let shutdown = tokio::time::timeout(Duration::from_secs(10), provider.shutdown()).await;
        let (requests, diagnostics, fixture_succeeded) = fixture.finish().await;
        assert!(
            matches!(reached, Ok(Ok(()))),
            "create barrier={reached:?}; caller={caller:?}; shutdown={shutdown:?}; {diagnostics}"
        );
        assert!(
            matches!(caller, Ok(Err(ref error)) if error.is_cancelled()),
            "caller must be joined: {caller:?}"
        );
        assert!(
            matches!(answered, Ok(Ok(()))),
            "response barrier={answered:?}; {diagnostics}"
        );
        assert!(
            shutdown.is_ok(),
            "shutdown deadline: {shutdown:?}; {diagnostics}"
        );
        assert!(
            !before_release
                .iter()
                .any(|request| request.method == "DELETE"),
            "pending creation has uncertain acceptance"
        );
        let deleted = requests
            .iter()
            .filter(|request| request.method == "DELETE")
            .collect::<Vec<_>>();
        match creation {
            Creation::Rejected => {
                assert_eq!(
                    deleted.len(),
                    1,
                    "dropping caller must retain cleanup ownership through definitive rejection: {requests:?}; shutdown={shutdown:?}; {diagnostics}"
                );
                assert_eq!(deleted[0].path, format!("/v1/files/{}", BatchFixture::FILE));
            }
            Creation::Accepted => assert!(
                deleted.is_empty(),
                "still-running accepted batch needs input"
            ),
            Creation::Disconnected | Creation::Status(_) | Creation::MalformedAcceptance => {
                unreachable!()
            }
        }
        assert!(fixture_succeeded, "fixture cleanup: {diagnostics}");
    }
}
