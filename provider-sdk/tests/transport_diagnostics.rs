use std::panic::AssertUnwindSafe;
use std::time::{Duration, Instant};

use futures_util::FutureExt;

use provider_sdk::config::{
    AnthropicConfig, ApiKey, OpenAiConfig, ProviderKind, RetryPolicy, TransportConfig,
};
use provider_sdk::error::{ErrorKind, ErrorPhase, Operation, ResponseViolation};
use provider_sdk::models::ModelListRequest;
use provider_sdk::{Provider, ProviderBackend, ProviderConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use url::Url;

struct DeadlineServer {
    url: Url,
    admitted: Option<oneshot::Receiver<()>>,
    release: Option<oneshot::Sender<()>>,
    connection_release: Option<std::sync::mpsc::Sender<()>>,
    task: tokio::task::JoinHandle<usize>,
}

impl DeadlineServer {
    const STALLED_SUCCESS: &'static [u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 500\r\nContent-Type: application/json\r\nx-request-id: successful-headers\r\nConnection: close\r\n\r\n{\"data\":";
    const STALLED_REJECTION: &'static [u8] = b"HTTP/1.1 429 Rejected\r\nContent-Length: 500\r\nContent-Type: application/json\r\nx-request-id: received-rejection\r\nConnection: close\r\n\r\n{\"error\":";

    async fn start(rejections: Vec<(u16, &'static str)>, final_prefix: &'static [u8]) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let (admitted_sender, admitted) = oneshot::channel();
        let (release, release_receiver) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut count = 0;
            let serve = async {
                for (status, code) in rejections {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    Self::read_headers(&mut stream).await;
                    count += 1;
                    Self::reject(&mut stream, status, code).await;
                }
                let (mut stream, _) = listener.accept().await.unwrap();
                Self::read_headers(&mut stream).await;
                count += 1;
                if !final_prefix.is_empty() {
                    stream.write_all(final_prefix).await.unwrap();
                    stream.flush().await.unwrap();
                }
                let _ = admitted_sender.send(());
                std::future::pending::<()>().await;
                drop(stream);
            };
            tokio::select! {
                () = serve => {},
                _ = release_receiver => {},
            }
            count
        });
        Self {
            url,
            admitted: Some(admitted),
            release: Some(release),
            connection_release: None,
            task,
        }
    }

    async fn hold_connections(rejections: Vec<(u16, &'static str)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!(
            "http://localhost:{}",
            listener.local_addr().unwrap().port()
        ))
        .unwrap();
        let (admitted_sender, admitted) = oneshot::channel();
        let (release, release_receiver) = oneshot::channel();
        let (connection_release, connection_receiver) = std::sync::mpsc::channel();
        let task = tokio::spawn(async move {
            let mut count = 0;
            let mut blocker = None;
            let serve = AssertUnwindSafe(async {
                let (last, earlier) = match rejections.split_last() {
                    Some((last, earlier)) => (Some(*last), earlier),
                    None => (None, [].as_slice()),
                };
                for &(status, code) in earlier {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    Self::read_headers(&mut stream).await;
                    count += 1;
                    Self::reject(&mut stream, status, code).await;
                }
                let pending = if let Some((status, code)) = last {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    Self::read_headers(&mut stream).await;
                    count += 1;
                    Some((stream, status, code))
                } else {
                    None
                };
                let (started, worker_started) = oneshot::channel();
                blocker = Some(tokio::task::spawn_blocking(move || {
                    let _ = started.send(());
                    connection_receiver.recv_timeout(Duration::from_secs(20))
                }));
                worker_started.await.unwrap();
                if let Some((mut stream, status, code)) = pending {
                    Self::reject(&mut stream, status, code).await;
                }
                let _ = admitted_sender.send(());
                // A connection succeeding despite the gate is observable as an
                // unexpected successful request, rather than a second stalled body.
                loop {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    Self::read_headers(&mut stream).await;
                    count += 1;
                    stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"data\":[]}").await.unwrap();
                    stream.shutdown().await.unwrap();
                }
            }).catch_unwind();
            let outcome = tokio::select! {
                outcome = serve => Some(outcome),
                _ = release_receiver => None,
            };
            // Keep the blocking handle outside the cancellable server future.
            let joined = match blocker {
                Some(blocker) => Some(blocker.await),
                None => None,
            };
            if let Some(Err(panic)) = outcome {
                std::panic::resume_unwind(panic);
            }
            if let Some(joined) = joined {
                joined
                    .expect("connection gate task joins")
                    .expect("connection gate is released");
            }
            count
        });
        Self {
            url,
            admitted: Some(admitted),
            release: Some(release),
            connection_release: Some(connection_release),
            task,
        }
    }

    async fn reject(stream: &mut TcpStream, status: u16, code: &str) {
        let body = serde_json::json!({"error": {
            "code": code, "message": "private rejection"
        }})
        .to_string();
        stream.write_all(format!("HTTP/1.1 {status} Rejected\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        stream.shutdown().await.unwrap();
    }

    async fn read_headers(stream: &mut TcpStream) {
        let mut bytes = Vec::new();
        while !bytes.windows(4).any(|part| part == b"\r\n\r\n") {
            let mut chunk = [0; 1024];
            let count = stream.read(&mut chunk).await.unwrap();
            assert_ne!(count, 0);
            bytes.extend_from_slice(&chunk[..count]);
            assert!(bytes.len() <= 16_384);
        }
    }

    fn provider(&self) -> Provider {
        Provider::from_config(ProviderConfig::new(
            ProviderKind::OpenAi(
                OpenAiConfig::new(ApiKey::new("fixture-key").unwrap())
                    .with_base_url(self.url.clone()),
            ),
            TransportConfig::new(
                Duration::from_secs(2),
                Duration::from_secs(1),
                Duration::from_secs(1),
                RetryPolicy::default(),
            )
            .unwrap(),
        ))
        .unwrap()
    }

    async fn admitted(&mut self) -> bool {
        tokio::time::timeout(Duration::from_secs(5), self.admitted.take().unwrap())
            .await
            .is_ok_and(|result| result.is_ok())
    }

    async fn finish(mut self) -> usize {
        if let Some(release) = self.connection_release.take() {
            let _ = release.send(());
        }
        let _ = self.release.take().unwrap().send(());
        self.task.await.unwrap()
    }
}

#[derive(Clone, Copy)]
enum ConnectionTimeoutCase {
    OneRejection,
    LatestRejection,
    NoRejection,
    ExhaustedRetries,
}

impl ConnectionTimeoutCase {
    fn run(self) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(self.check());
    }

    async fn check(self) {
        let (rejections, timeout, retries, backoff) = match self {
            Self::OneRejection => (vec![(503, "server_error")], 3, 3, 1),
            Self::LatestRejection => (
                vec![(429, "insufficient_quota"), (503, "server_error")],
                5,
                3,
                1,
            ),
            Self::NoRejection => (vec![], 3, 3, 4),
            Self::ExhaustedRetries => (vec![(503, "server_error")], 3, 1, 1),
        };
        let received = rejections.len();
        let timeout = Duration::from_secs(timeout);
        let mut server = DeadlineServer::hold_connections(rejections).await;
        let provider = Provider::from_config(ProviderConfig::new(
            ProviderKind::OpenAi(
                OpenAiConfig::new(ApiKey::new("fixture-key").unwrap())
                    .with_base_url(server.url.clone()),
            ),
            TransportConfig::new(
                timeout,
                Duration::from_millis(400),
                Duration::from_secs(1),
                RetryPolicy::new(
                    retries,
                    Duration::from_secs(backoff),
                    Duration::from_secs(4),
                )
                .unwrap(),
            )
            .unwrap(),
        ));
        let provider = match provider {
            Ok(provider) => provider,
            Err(error) => {
                server.finish().await;
                panic!("fixture provider construction failed: {error}");
            }
        };
        let outcome = AssertUnwindSafe(async {
            // Without an earlier request, establish the DNS gate before admission.
            if received == 0 {
                assert!(server.admitted().await, "connection gate starts");
            }
            let started = Instant::now();
            let result = tokio::time::timeout(
                Duration::from_secs(8),
                provider.list_models(ModelListRequest::default()),
            )
            .await;
            let elapsed = started.elapsed();
            if received != 0 {
                assert!(server.admitted().await, "gate precedes the last rejection");
            }
            let error = result
                .expect("request observation is bounded")
                .expect_err("blocked DNS must cause a connection timeout");
            assert!(
                elapsed < timeout,
                "connection failure must precede the logical deadline: {elapsed:?}"
            );
            assert_eq!(error.kind(), ErrorKind::Timeout);
            assert_eq!(error.context().operation, Operation::ListModels);
            assert_eq!(error.context().phase, ErrorPhase::RequestSend);
            assert_eq!(error.timeout(), Some(timeout));
            assert_eq!(error.http_status(), None);
            assert_eq!(
                error
                    .cause()
                    .expect("actual transport cause survives")
                    .kind(),
                ErrorKind::Transport
            );
            assert_eq!(
                error.supplementary().len(),
                usize::from(received != 0),
                "connection timeout after {elapsed:?} of {timeout:?} logical budget retains the last rejection"
            );
            if received != 0 {
                let rejection = &error.supplementary()[0];
                assert_eq!(rejection.kind(), ErrorKind::ServiceUnavailable);
                assert_eq!(rejection.http_status(), Some(503));
                assert_eq!(rejection.context().operation, Operation::ListModels);
                assert_eq!(rejection.context().phase, ErrorPhase::ResponseHeaders);
                assert!(
                    rejection.supplementary().is_empty(),
                    "keep the latest rejection, not attempt history"
                );
                assert!(
                    rejection
                        .native_details()
                        .body
                        .as_deref()
                        .unwrap()
                        .contains("private rejection")
                );
            }
            for formatted in [error.to_string(), format!("{error:?}")] {
                assert!(!formatted.contains("private rejection"));
                assert!(!formatted.contains("fixture-key"));
            }
        })
        .catch_unwind()
        .await;
        // Release and join the blocking task even when the runtime assertion fails.
        // Attempt both independent cleanup owners before rethrowing that assertion.
        let finished = AssertUnwindSafe(server.finish()).catch_unwind().await;
        let shutdown = provider.shutdown().await;
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
        let requests = match finished {
            Ok(requests) => requests,
            Err(panic) => std::panic::resume_unwind(panic),
        };
        shutdown.expect("provider shuts down");
        assert_eq!(
            requests, received,
            "timed-out DNS must not issue another HTTP request"
        );
    }
}

#[test]
fn connect_timeout_preserves_rejection_when_backoff_cannot_fit() {
    ConnectionTimeoutCase::OneRejection.run();
}

#[test]
fn connect_timeout_preserves_only_latest_rejection_when_backoff_cannot_fit() {
    ConnectionTimeoutCase::LatestRejection.run();
}

#[test]
fn connect_timeout_without_rejection_has_no_supplementary_failure() {
    ConnectionTimeoutCase::NoRejection.run();
}

#[test]
fn connect_timeout_preserves_rejection_when_retries_are_exhausted() {
    ConnectionTimeoutCase::ExhaustedRetries.run();
}

#[tokio::test]
async fn diagnostic_resolution_later_deadline_preserves_only_latest_rejection() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut server = DeadlineServer::start(
            vec![(429, "insufficient_quota"), (503, "server_error")],
            b"",
        )
        .await;
        let provider = server.provider();
        let (result, admitted) = tokio::join!(
            tokio::time::timeout(
                Duration::from_secs(8),
                provider.list_models(ModelListRequest::default())
            ),
            server.admitted()
        );
        provider.shutdown().await.unwrap();
        assert_eq!(
            server.finish().await,
            3,
            "two rejected attempts precede the stalled attempt"
        );
        assert!(admitted, "the final request must reach the fixture");
        let result = result.expect("request observation remains bounded");
        let error = result.expect_err("stalled retry exhausts logical deadline");
        assert_eq!(error.kind(), ErrorKind::Timeout);
        assert_eq!(error.context().operation, Operation::ListModels);
        assert_eq!(error.context().phase, ErrorPhase::RequestSend);
        assert!(
            error.cause().is_some(),
            "the genuine transport timeout cause is retained"
        );
        assert_eq!(error.supplementary().len(), 1);
        let rejection = &error.supplementary()[0];
        assert_eq!(rejection.kind(), ErrorKind::ServiceUnavailable);
        assert_eq!(rejection.http_status(), Some(503));
        assert_eq!(rejection.context().phase, ErrorPhase::ResponseHeaders);
        assert!(
            rejection.supplementary().is_empty(),
            "attempt history does not accumulate"
        );
        assert!(!format!("{error:?}").contains("private rejection"));
    })
    .await
    .expect("deadline proof is bounded");
}

#[tokio::test]
async fn diagnostic_resolution_received_rejection_remains_primary_on_body_timeout() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut server = DeadlineServer::start(Vec::new(), DeadlineServer::STALLED_REJECTION).await;
        let provider = server.provider();
        let (result, admitted) = tokio::join!(
            tokio::time::timeout(
                Duration::from_secs(8),
                provider.list_models(ModelListRequest::default())
            ),
            server.admitted()
        );
        provider.shutdown().await.unwrap();
        assert_eq!(server.finish().await, 1);
        assert!(admitted, "the final request must reach the fixture");
        let result = result.expect("request observation remains bounded");
        let error = result.expect_err("received rejection cannot become local timeout");
        assert_eq!(error.kind(), ErrorKind::ResourceExhausted);
        assert_eq!(error.http_status(), Some(429));
        assert_eq!(
            error.native_details().request_id.as_deref(),
            Some("received-rejection")
        );
        assert_eq!(error.context().phase, ErrorPhase::ResponseHeaders);
        assert!(
            error
                .supplementary()
                .iter()
                .any(|child| child.kind() == ErrorKind::Timeout
                    && child.context().phase == ErrorPhase::ReadBody)
        );
    })
    .await
    .expect("rejected body timeout proof is bounded");
}

#[tokio::test]
async fn successful_body_timeout_retains_only_latest_rejection() {
    for rejections in [
        vec![(503, "server_error")],
        vec![(429, "insufficient_quota"), (503, "server_error")],
    ] {
        let attempts = rejections.len() + 1;
        let mut server = DeadlineServer::start(rejections, DeadlineServer::STALLED_SUCCESS).await;
        let provider = server.provider();
        let (result, admitted) = tokio::join!(
            tokio::time::timeout(
                Duration::from_secs(8),
                provider.list_models(ModelListRequest::default())
            ),
            server.admitted()
        );
        let shutdown = provider.shutdown().await;
        let requests = server.finish().await;
        shutdown.unwrap();
        assert!(admitted, "the final request must reach the fixture");
        let result = result.expect("request observation remains bounded");
        assert_eq!(requests, attempts);
        let error = result.expect_err("incomplete successful body must time out");
        assert_eq!(error.kind(), ErrorKind::Timeout);
        assert_eq!(error.context().operation, Operation::ListModels);
        assert_eq!(error.context().phase, ErrorPhase::ReadBody);
        assert_eq!(error.timeout(), Some(Duration::from_secs(2)));
        assert_eq!(error.http_status(), None);
        assert!(
            error.cause().is_some(),
            "retain the genuine body timeout cause"
        );
        assert_eq!(
            error.native_details().request_id.as_deref(),
            Some("successful-headers")
        );
        assert_eq!(error.supplementary().len(), 1);
        let rejection = &error.supplementary()[0];
        assert_eq!(rejection.kind(), ErrorKind::ServiceUnavailable);
        assert_eq!(rejection.http_status(), Some(503));
        assert_eq!(rejection.context().operation, Operation::ListModels);
        assert_eq!(rejection.context().phase, ErrorPhase::ResponseHeaders);
        assert!(
            rejection
                .native_details()
                .body
                .as_deref()
                .unwrap()
                .contains("private rejection")
        );
        assert!(rejection.supplementary().is_empty());
        for formatted in [error.to_string(), format!("{error:?}")] {
            assert!(!formatted.contains("private rejection"));
            assert!(!formatted.contains("successful-headers"));
            assert!(!formatted.contains("fixture-key"));
        }
    }
}

#[tokio::test]
async fn successful_body_timeout_without_rejection_has_no_supplementary_failure() {
    let mut server = DeadlineServer::start(Vec::new(), DeadlineServer::STALLED_SUCCESS).await;
    let provider = server.provider();
    let (result, admitted) = tokio::join!(
        tokio::time::timeout(
            Duration::from_secs(8),
            provider.list_models(ModelListRequest::default())
        ),
        server.admitted()
    );
    let shutdown = provider.shutdown().await;
    let requests = server.finish().await;
    shutdown.unwrap();
    assert!(admitted, "the final request must reach the fixture");
    let result = result.expect("request observation remains bounded");
    assert_eq!(requests, 1);
    let error = result.expect_err("the body remains incomplete");
    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert_eq!(error.context().phase, ErrorPhase::ReadBody);
    assert!(error.cause().is_some());
    assert!(error.supplementary().is_empty());
}

#[tokio::test]
async fn completed_retry_body_discards_prior_rejection() {
    for (prefix, valid) in [
        (b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\nContent-Type: application/json\r\nx-request-id: completed-body\r\nConnection: close\r\n\r\n{\"data\":[]}".as_slice(), true),
        (b"HTTP/1.1 200 OK\r\nContent-Length: 7\r\nContent-Type: application/json\r\nx-request-id: completed-body\r\nConnection: close\r\n\r\ninvalid".as_slice(), false),
    ] {
        let mut server = DeadlineServer::start(vec![(503, "server_error")], prefix).await;
        let provider = server.provider();
        let (result, admitted) = tokio::join!(
            tokio::time::timeout(Duration::from_secs(8), provider.list_models(ModelListRequest::default())),
            server.admitted()
        );
        let shutdown = provider.shutdown().await;
        let requests = server.finish().await;
        shutdown.unwrap();
        assert!(admitted, "the final request must reach the fixture");
        let result = result.expect("request observation remains bounded");
        assert_eq!(requests, 2);
        if valid {
            assert!(result.is_ok(), "complete model list: {result:?}");
        } else {
            let error = result.expect_err("malformed JSON must remain a decode failure");
            assert_eq!(error.kind(), ErrorKind::InvalidResponse(ResponseViolation::MalformedJson));
            assert_eq!(error.context().phase, ErrorPhase::Decode);
            assert_eq!(error.context().operation, Operation::ListModels);
            assert_eq!(error.native_details().request_id.as_deref(), Some("completed-body"));
            assert!(error.supplementary().is_empty());
        }
    }
}

#[tokio::test]
async fn anthropic_health_finishes_at_successful_headers() {
    let mut server =
        DeadlineServer::start(vec![(503, "server_error")], DeadlineServer::STALLED_SUCCESS).await;
    let provider = Provider::from_config(ProviderConfig::new(
        ProviderKind::Anthropic(
            AnthropicConfig::new(ApiKey::new("fixture-key").unwrap())
                .with_base_url(server.url.clone()),
        ),
        TransportConfig::new(
            Duration::from_secs(2),
            Duration::from_secs(1),
            Duration::from_secs(1),
            RetryPolicy::default(),
        )
        .unwrap(),
    ))
    .unwrap();
    let (result, admitted) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(8), provider.health()),
        server.admitted()
    );
    let shutdown = provider.shutdown().await;
    let requests = server.finish().await;
    shutdown.unwrap();
    assert!(admitted, "the final request must reach the fixture");
    let result = result.expect("request observation remains bounded");
    assert_eq!(requests, 2);
    assert!(
        result.is_ok(),
        "successful headers establish health: {result:?}"
    );
}
