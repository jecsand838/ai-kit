#![cfg(feature = "qdrant")]

use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use serde_json::{Value, json};
use storage_sdk::{
    Database, DatabaseBackend, StorageError,
    database::{
        StorageLocator,
        config::{CommonConfig, DatabaseConfig, QdrantConfig},
        data::{
            DataRecord, DataRequest, DataResponse, DeleteRequest, DenseVector, FilterExpr,
            InsertRequest, RecordId, UpdateAssignment, UpdateRequest,
        },
        index::{CreateIndexRequest, IndexKind, IndexRequest, IndexResponse, WaitForIndexRequest},
    },
};

// This fixture proves wire/completion interpretation only. It is deliberately
// outside strict_live and is not evidence that a real Qdrant service works.
struct RecordedRequest {
    line: String,
    body: Value,
    body_len: usize,
}

enum CompletionReply {
    Fixed {
        body: Value,
        delay: Duration,
    },
    DelayedBody {
        body: Value,
        delay: Duration,
    },
    Replacement {
        status: &'static str,
        delay: Duration,
    },
}

impl CompletionReply {
    fn response(&self, request_line: &str) -> (Value, Duration) {
        match self {
            Self::Fixed { body, delay } | Self::DelayedBody { body, delay } => {
                (body.clone(), *delay)
            }
            Self::Replacement { status, delay } => {
                if request_line.starts_with("GET ") {
                    (
                        json!({"result":{"config":{"params":{"vectors":{"vector":{"size":2,"distance":"Cosine"}}}}},"status":"ok"}),
                        *delay,
                    )
                } else if request_line.contains("/points/delete") {
                    (
                        json!({"result":{"operation_id":7,"status":"completed"},"status":"ok","time":0.001}),
                        Duration::ZERO,
                    )
                } else {
                    (
                        json!({"result":{"operation_id":7,"status":status},"status":"ok","time":0.001}),
                        *delay,
                    )
                }
            }
        }
    }
}

struct CompletionServer {
    address: SocketAddr,
    stopping: Arc<AtomicBool>,
    worker: Option<JoinHandle<Result<Vec<RecordedRequest>, String>>>,
}

impl CompletionServer {
    fn start(status: &'static str) -> Self {
        Self::start_exchange(status, false)
    }

    fn start_exchange(status: &'static str, overwrite: bool) -> Self {
        Self::start_timed(status, overwrite, Duration::ZERO)
    }

    fn start_timed(status: &'static str, overwrite: bool, response_delay: Duration) -> Self {
        let reply = if overwrite {
            CompletionReply::Replacement {
                status,
                delay: response_delay,
            }
        } else {
            CompletionReply::Fixed {
                body: json!({"result":{"operation_id":7,"status":status},"status":"ok","time":0.001}),
                delay: response_delay,
            }
        };
        Self::start_reply(reply)
    }

    fn start_reply(reply: CompletionReply) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind completion fixture");
        listener.set_nonblocking(true).expect("nonblocking accept");
        let address = listener.local_addr().expect("fixture address");
        let stopping = Arc::new(AtomicBool::new(false));
        let worker_stopping = Arc::clone(&stopping);
        let worker = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut requests = Vec::new();
            loop {
                let mut stream = loop {
                    if worker_stopping.load(Ordering::Acquire) {
                        return Ok(requests);
                    }
                    if Instant::now() >= deadline {
                        return Err("fixture request deadline elapsed".to_owned());
                    }
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(2));
                        }
                        Err(error) => return Err(error.to_string()),
                    }
                };
                // BSD accepted sockets may inherit the nonblocking listener mode.
                stream
                    .set_nonblocking(false)
                    .map_err(|error| error.to_string())?;
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .map_err(|error| error.to_string())?;
                stream
                    .set_write_timeout(Some(Duration::from_secs(2)))
                    .map_err(|error| error.to_string())?;
                let mut bytes = Vec::new();
                let mut buffer = [0_u8; 4096];
                let header_end = loop {
                    let count = stream
                        .read(&mut buffer)
                        .map_err(|error| error.to_string())?;
                    if count == 0 {
                        return Err("request ended before headers".to_owned());
                    }
                    bytes.extend_from_slice(&buffer[..count]);
                    if bytes.len() > 64 * 1024 {
                        return Err("request exceeded fixture bound".to_owned());
                    }
                    if let Some(position) =
                        bytes.windows(4).position(|window| window == b"\r\n\r\n")
                    {
                        break position + 4;
                    }
                };
                let headers =
                    std::str::from_utf8(&bytes[..header_end]).map_err(|error| error.to_string())?;
                let request_line = headers
                    .lines()
                    .next()
                    .ok_or("missing request line")?
                    .to_owned();
                if headers.lines().any(|line| {
                    line.split_once(':')
                        .is_some_and(|(name, _)| name.eq_ignore_ascii_case("transfer-encoding"))
                }) {
                    return Err(
                        "fixture expects content-length framing, not transfer-encoding".to_owned(),
                    );
                }
                let length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>())
                    })
                    .transpose()
                    .map_err(|error| error.to_string())?
                    .unwrap_or(0);
                if length > 64 * 1024 {
                    return Err("body exceeded fixture bound".to_owned());
                }
                while bytes.len() < header_end + length {
                    let count = stream
                        .read(&mut buffer)
                        .map_err(|error| error.to_string())?;
                    if count == 0 {
                        return Err("request ended before body".to_owned());
                    }
                    bytes.extend_from_slice(&buffer[..count]);
                }
                let body = if length == 0 {
                    Value::Null
                } else {
                    serde_json::from_slice(&bytes[header_end..header_end + length])
                        .map_err(|error| error.to_string())?
                };
                let (response, response_delay) = reply.response(&request_line);
                let response = response.to_string();
                let headers = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    response.len()
                );
                if matches!(reply, CompletionReply::DelayedBody { .. }) {
                    stream
                        .write_all(headers.as_bytes())
                        .map_err(|error| error.to_string())?;
                }
                let respond_at = Instant::now() + response_delay;
                while Instant::now() < respond_at {
                    if worker_stopping.load(Ordering::Acquire) {
                        requests.push(RecordedRequest {
                            line: request_line,
                            body,
                            body_len: bytes.len() - header_end,
                        });
                        return Ok(requests);
                    }
                    std::thread::sleep(Duration::from_millis(2));
                }
                if !matches!(reply, CompletionReply::DelayedBody { .. }) {
                    stream
                        .write_all(headers.as_bytes())
                        .map_err(|error| error.to_string())?;
                }
                stream
                    .write_all(response.as_bytes())
                    .map_err(|error| error.to_string())?;
                requests.push(RecordedRequest {
                    line: request_line,
                    body,
                    body_len: bytes.len() - header_end,
                });
                // Keep accepting until the public operation settles so a replay
                // remains observable instead of hitting a closed fixture port.
                if requests.len() > 4 {
                    return Err("unexpected mutation replay".into());
                }
            }
        });
        Self {
            address,
            stopping,
            worker: Some(worker),
        }
    }

    fn finish(mut self) -> Vec<RecordedRequest> {
        self.stopping.store(true, Ordering::Release);
        self.worker
            .take()
            .expect("owned fixture thread")
            .join()
            .expect("fixture thread panicked")
            .expect("complete native request")
    }

    async fn exercise(status: &'static str, delete: bool, check_wait: bool) {
        let server = Self::start(status);
        let database = Database::connect(DatabaseConfig::qdrant(QdrantConfig::remote_rest(
            format!("http://{}", server.address),
        )))
        .await
        .expect("connect supplied REST adapter");
        let locator = StorageLocator::new("completion");
        let filter = FilterExpr::eq("marker", "selected");
        let request = if delete {
            DataRequest::Delete(DeleteRequest { locator, filter })
        } else {
            DataRequest::Update(UpdateRequest {
                locator,
                filter: Some(filter),
                assignments: vec![UpdateAssignment {
                    column: "status".to_owned(),
                    value: json!("updated"),
                }],
            })
        };
        let result = tokio::time::timeout(Duration::from_secs(5), database.data(request)).await;
        let requests = server.finish();
        assert_eq!(requests.len(), 1, "one non-replayed mutation request");
        let RecordedRequest {
            line: request_line,
            body,
            ..
        } = &requests[0];
        let result = result.expect("adapter operation must settle within fixture watchdog");
        assert!(body.get("filter").is_some_and(Value::is_object));
        if check_wait {
            let path = request_line
                .split_whitespace()
                .nth(1)
                .expect("HTTP request target");
            let url = url::Url::parse(&format!("http://fixture{path}")).expect("request URL");
            assert!(
                url.query_pairs()
                    .any(|(key, value)| key == "wait" && value == "true"),
                "successful mutation must request native applied completion: {request_line}"
            );
        }
        match status {
            "completed" => assert!(
                matches!(result, Ok(DataResponse::Mutation(_))),
                "applied mutation must succeed: {result:?}"
            ),
            "acknowledged" => assert!(
                result.is_err(),
                "queued acknowledgement is not completed portable mutation: {result:?}"
            ),
            _ => panic!("unsupported fixture status"),
        }
    }
    async fn overwrite(status: &'static str) {
        let server = Self::start_exchange(status, true);
        let database = Database::connect(DatabaseConfig::qdrant(QdrantConfig::remote_rest(
            format!("http://{}", server.address),
        )))
        .await
        .expect("connect supplied REST adapter");
        let record =
            DataRecord::new(RecordId::U64(7)).vector("vector", DenseVector::F32(vec![1.0, 0.0]));
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            database.data(DataRequest::Insert(InsertRequest::overwrite(
                StorageLocator::new("completion"),
                vec![record],
            ))),
        )
        .await;
        let requests = server.finish();
        let result = result.expect("bounded overwrite operation");
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.line.starts_with("PUT "))
                .count(),
            1,
            "one upsert, never replayed"
        );
        let RecordedRequest {
            line: upsert_line,
            body: upsert_body,
            ..
        } = requests
            .iter()
            .find(|request| request.line.starts_with("PUT "))
            .expect("upsert request");
        assert_eq!(
            upsert_body.get("points"),
            Some(&json!([{"id":7,"vector":{"vector":[1.0,0.0]},"payload":{}}]))
        );
        assert!(
            upsert_body
                .get("update_mode")
                .is_none_or(|mode| mode == "upsert"),
            "native default or explicit upsert preserves intended replacement semantics"
        );
        for RecordedRequest { line, .. } in requests
            .iter()
            .filter(|request| !request.line.starts_with("GET "))
        {
            let target = line.split_whitespace().nth(1).expect("request target");
            let url = url::Url::parse(&format!("http://fixture{target}")).expect("mutation URL");
            assert!(
                url.query_pairs()
                    .any(|(key, value)| key == "wait" && value == "true"),
                "mutation must await applied completion: {line}"
            );
            let expected_path = if line == upsert_line {
                "/collections/completion/points"
            } else {
                "/collections/completion/points/delete"
            };
            assert_eq!(url.path(), expected_path);
        }
        let deletions = requests
            .iter()
            .filter(|request| request.line.contains("/points/delete"))
            .collect::<Vec<_>>();
        match status {
            "acknowledged" => {
                assert!(
                    result.is_err(),
                    "unconfirmed replacement must fail: {result:?}"
                );
                assert!(
                    deletions.is_empty(),
                    "unconfirmed upsert must never prune old rows"
                );
            }
            "completed" => {
                assert!(result.is_ok(), "confirmed replacement succeeds: {result:?}");
                assert_eq!(
                    deletions.len(),
                    1,
                    "confirmed replacement removes only other rows"
                );
                assert_eq!(
                    deletions[0].body,
                    json!({"filter":{"must_not":[{"has_id":[7]}]}})
                );
                let upsert = requests
                    .iter()
                    .position(|request| request.line.starts_with("PUT "))
                    .expect("upsert");
                let deletion = requests
                    .iter()
                    .position(|request| request.line.contains("/points/delete"))
                    .expect("prune");
                assert!(upsert < deletion, "replace only after applied upsert");
            }
            _ => panic!("unsupported overwrite status"),
        }
    }
    async fn replacement_deadline() {
        let server = Self::start_timed("completed", true, Duration::from_millis(1200));
        let database = Database::connect(
            DatabaseConfig::qdrant(QdrantConfig::remote_rest(format!(
                "http://{}",
                server.address
            )))
            .with_common(CommonConfig {
                request_timeout_ms: 2000,
                max_retries: 0,
                ..Default::default()
            }),
        )
        .await
        .expect("connect supplied REST adapter");
        let result = tokio::time::timeout(
            Duration::from_millis(3500),
            database.data(DataRequest::Insert(InsertRequest::overwrite(
                StorageLocator::new("completion"),
                vec![
                    DataRecord::new(RecordId::U64(7))
                        .vector("vector", DenseVector::F32(vec![1.0, 0.0])),
                ],
            ))),
        )
        .await;
        let requests = server.finish();
        let result = result.expect("outer fixture watchdog is not the portable deadline");
        assert!(
            result.is_err(),
            "read-only preflight must not restart replacement budget: {result:?}"
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.line.starts_with("GET "))
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.line.starts_with("PUT "))
                .count(),
            1
        );
        assert!(
            requests
                .iter()
                .all(|request| !request.line.contains("/points/delete")),
            "unconfirmed timed-out upsert must not prune"
        );
    }
}

impl Drop for CompletionServer {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum IndexMutation {
    Create,
    Drop,
}

impl IndexMutation {
    async fn observe(self, response: Value) -> (Result<IndexResponse, StorageError>, url::Url) {
        let server = CompletionServer::start_reply(CompletionReply::Fixed {
            body: response,
            delay: Duration::ZERO,
        });
        let database = Database::connect(DatabaseConfig::qdrant(QdrantConfig::remote_rest(
            format!("http://{}", server.address),
        )))
        .await
        .expect("connect supplied REST adapter");
        let locator = StorageLocator::new("completion");
        let request = match self {
            Self::Create => IndexRequest::Create(CreateIndexRequest {
                locator,
                columns: vec!["body".to_owned()],
                kind: IndexKind::FullText,
                wait_timeout_ms: None,
            }),
            Self::Drop => IndexRequest::Drop {
                locator,
                name: "body".to_owned(),
            },
        };
        let observed = tokio::time::timeout(Duration::from_secs(5), database.index(request)).await;
        let requests = server.finish();
        eprintln!(
            "index {self:?}: fixture joined; {} request(s); result={observed:?}",
            requests.len()
        );
        let result = observed.expect("index operation must settle before fixture watchdog");
        assert_eq!(requests.len(), 1, "one mutation, never replayed");
        let request = &requests[0];
        let mut line = request.line.split_whitespace();
        let method = line.next().expect("HTTP method");
        let target = line.next().expect("HTTP target");
        let url = url::Url::parse(&format!("http://fixture{target}")).expect("index request URL");
        match self {
            Self::Create => {
                assert_eq!(method, "PUT");
                assert_eq!(url.path(), "/collections/completion/index");
                assert_eq!(
                    request.body,
                    json!({"field_name":"body","field_schema":{"type":"text","tokenizer":"word"}})
                );
                assert!(request.body_len > 0);
            }
            Self::Drop => {
                assert_eq!(method, "DELETE");
                assert_eq!(url.path(), "/collections/completion/index/body");
                assert_eq!(
                    request.body_len, 0,
                    "DELETE must be bodyless, not JSON null"
                );
            }
        }
        (result, url)
    }

    async fn accepts_completed(self) {
        let (result, url) = self
            .observe(json!({"result":{"operation_id":7,"status":"completed"},"status":"ok"}))
            .await;
        assert_eq!(
            result.expect("confirmed index mutation succeeds"),
            IndexResponse::Completed
        );
        let wait_values = url
            .query_pairs()
            .filter(|(key, _)| key == "wait")
            .map(|(_, value)| value.into_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            wait_values,
            ["true"],
            "index mutation must request applied completion exactly once: {url}"
        );
    }

    async fn rejects_unconfirmed(self, response: Value) {
        let (result, _) = self.observe(response).await;
        assert!(
            matches!(
                result,
                Err(StorageError::BackendOperation { .. } | StorageError::Serialization(_))
            ),
            "an HTTP-success envelope without confirmed completion must fail: {result:?}"
        );
    }
}

#[derive(Clone, Copy, Debug)]
enum IndexWait {
    Ready,
    Missing,
    RequestedDeadline,
    RequestedBodyDeadline,
    CommonDeadline,
    InvalidDeadline,
}

impl IndexWait {
    async fn exercise(self) {
        let body = match self {
            Self::Missing => json!({"result":{"payload_schema":{}},"status":"ok"}),
            Self::Ready
            | Self::RequestedDeadline
            | Self::RequestedBodyDeadline
            | Self::CommonDeadline
            | Self::InvalidDeadline => {
                json!({"result":{"payload_schema":{"body":{"data_type":"text","points":1}}},"status":"ok"})
            }
        };
        let delay = match self {
            Self::RequestedDeadline | Self::RequestedBodyDeadline | Self::CommonDeadline => {
                Duration::from_millis(1200)
            }
            Self::Ready | Self::Missing | Self::InvalidDeadline => Duration::ZERO,
        };
        let reply = match self {
            Self::RequestedBodyDeadline => CompletionReply::DelayedBody { body, delay },
            Self::Ready
            | Self::Missing
            | Self::RequestedDeadline
            | Self::CommonDeadline
            | Self::InvalidDeadline => CompletionReply::Fixed { body, delay },
        };
        let server = CompletionServer::start_reply(reply);
        let request_timeout_ms = match self {
            Self::CommonDeadline => 100,
            Self::Ready
            | Self::Missing
            | Self::RequestedDeadline
            | Self::RequestedBodyDeadline
            | Self::InvalidDeadline => 2000,
        };
        let timeout_ms = match self {
            Self::RequestedDeadline | Self::RequestedBodyDeadline => 100,
            Self::InvalidDeadline => 0,
            Self::Ready | Self::Missing | Self::CommonDeadline => 1000,
        };
        let database = Database::connect(
            DatabaseConfig::qdrant(QdrantConfig::remote_rest(format!(
                "http://{}",
                server.address
            )))
            .with_common(CommonConfig {
                request_timeout_ms,
                max_retries: 0,
                ..Default::default()
            }),
        )
        .await
        .expect("connect supplied REST adapter");
        let started = Instant::now();
        let observed = tokio::time::timeout(
            Duration::from_secs(5),
            database.index(IndexRequest::WaitFor(WaitForIndexRequest {
                locator: StorageLocator::new("completion"),
                index_names: vec!["body".to_owned()],
                timeout_ms,
            })),
        )
        .await;
        let elapsed = started.elapsed();
        let requests = server.finish();
        eprintln!(
            "index wait {self:?}: fixture joined; {} request(s); elapsed={elapsed:?}; result={observed:?}",
            requests.len()
        );
        let result = observed.expect("fixture watchdog is not a successful portable deadline");
        match self {
            Self::InvalidDeadline => {
                assert!(
                    matches!(result, Err(StorageError::InvalidRequest(_))),
                    "zero wait budget must be rejected: {result:?}"
                );
                assert!(requests.is_empty(), "invalid input must not reach Qdrant");
            }
            Self::Ready
            | Self::Missing
            | Self::RequestedDeadline
            | Self::RequestedBodyDeadline
            | Self::CommonDeadline => {
                assert_eq!(requests.len(), 1, "one collection observation");
                assert_eq!(requests[0].line, "GET /collections/completion HTTP/1.1");
                assert_eq!(requests[0].body_len, 0);
                match self {
                    Self::Ready => {
                        assert_eq!(result.expect("ready index"), IndexResponse::Completed)
                    }
                    Self::Missing => assert!(
                        matches!(result, Err(StorageError::BackendOperation { .. })),
                        "missing index is not ready: {result:?}"
                    ),
                    Self::RequestedDeadline
                    | Self::RequestedBodyDeadline
                    | Self::CommonDeadline => {
                        let Err(StorageError::BackendOperation { operation, message }) = result
                        else {
                            panic!(
                                "the owning deadline must fail before a delayed ready response: {result:?}"
                            );
                        };
                        match self {
                            Self::RequestedDeadline | Self::RequestedBodyDeadline => {
                                assert_eq!(operation, "wait_for_index");
                                assert!(
                                    message.contains("timed out"),
                                    "requested timeout evidence must remain explicit: {message}"
                                );
                            }
                            Self::CommonDeadline => {
                                assert!(
                                    matches!(operation.as_str(), "index" | "rest_request"),
                                    "CommonConfig deadline owner: {operation}"
                                );
                                assert!(
                                    message.contains("timed out") || message.contains("timeout:"),
                                    "CommonConfig timeout evidence must remain explicit: {message}"
                                );
                            }
                            Self::Ready | Self::Missing | Self::InvalidDeadline => unreachable!(),
                        }
                        // A generous scheduling allowance distinguishes either 100 ms
                        // owner budget from the valid response delayed by 1,200 ms.
                        assert!(
                            elapsed < Duration::from_secs(1),
                            "deadline did not bound native observation: {elapsed:?}"
                        );
                    }
                    Self::InvalidDeadline => unreachable!(),
                }
            }
        }
    }
}

#[tokio::test]
async fn update_requests_applied_completion() {
    CompletionServer::exercise("completed", false, true).await;
}
#[tokio::test]
async fn delete_requests_applied_completion() {
    CompletionServer::exercise("completed", true, true).await;
}
#[tokio::test]
async fn update_rejects_queued_acknowledgement() {
    CompletionServer::exercise("acknowledged", false, false).await;
}
#[tokio::test]
async fn delete_rejects_queued_acknowledgement() {
    CompletionServer::exercise("acknowledged", true, false).await;
}
#[tokio::test]
async fn update_accepts_completed_result() {
    CompletionServer::exercise("completed", false, false).await;
}
#[tokio::test]
async fn delete_accepts_completed_result() {
    CompletionServer::exercise("completed", true, false).await;
}

#[tokio::test]
async fn overwrite_never_prunes_after_unconfirmed_upsert() {
    CompletionServer::overwrite("acknowledged").await;
}
#[tokio::test]
async fn overwrite_prunes_only_after_confirmed_upsert() {
    CompletionServer::overwrite("completed").await;
}

#[tokio::test]
async fn overwrite_preflight_and_mutation_share_original_deadline() {
    CompletionServer::replacement_deadline().await;
}

#[tokio::test]
async fn index_create_requests_and_accepts_applied_completion() {
    IndexMutation::Create.accepts_completed().await;
}

#[tokio::test]
async fn index_drop_requests_and_accepts_applied_completion() {
    IndexMutation::Drop.accepts_completed().await;
}

#[tokio::test]
async fn index_create_rejects_queued_acknowledgement() {
    IndexMutation::Create
        .rejects_unconfirmed(
            json!({"result":{"operation_id":7,"status":"acknowledged"},"status":"ok"}),
        )
        .await;
}

#[tokio::test]
async fn index_create_rejects_missing_result() {
    IndexMutation::Create
        .rejects_unconfirmed(json!({"status":"ok"}))
        .await;
}

#[tokio::test]
async fn index_create_rejects_missing_completion_status() {
    IndexMutation::Create
        .rejects_unconfirmed(json!({"result":{"operation_id":7},"status":"ok"}))
        .await;
}

#[tokio::test]
async fn index_create_rejects_malformed_completion_status() {
    IndexMutation::Create
        .rejects_unconfirmed(
            json!({"result":{"operation_id":7,"status":{"completed":true}},"status":"ok"}),
        )
        .await;
}

#[tokio::test]
async fn index_create_rejects_unknown_completion_status() {
    IndexMutation::Create
        .rejects_unconfirmed(json!({"result":{"operation_id":7,"status":"accepted"},"status":"ok"}))
        .await;
}

#[tokio::test]
async fn index_drop_rejects_queued_acknowledgement() {
    IndexMutation::Drop
        .rejects_unconfirmed(
            json!({"result":{"operation_id":7,"status":"acknowledged"},"status":"ok"}),
        )
        .await;
}

#[tokio::test]
async fn index_drop_rejects_missing_result() {
    IndexMutation::Drop
        .rejects_unconfirmed(json!({"status":"ok"}))
        .await;
}

#[tokio::test]
async fn index_drop_rejects_missing_completion_status() {
    IndexMutation::Drop
        .rejects_unconfirmed(json!({"result":{"operation_id":7},"status":"ok"}))
        .await;
}

#[tokio::test]
async fn index_drop_rejects_malformed_completion_status() {
    IndexMutation::Drop
        .rejects_unconfirmed(
            json!({"result":{"operation_id":7,"status":{"completed":true}},"status":"ok"}),
        )
        .await;
}

#[tokio::test]
async fn index_drop_rejects_unknown_completion_status() {
    IndexMutation::Drop
        .rejects_unconfirmed(json!({"result":{"operation_id":7,"status":"accepted"},"status":"ok"}))
        .await;
}

#[tokio::test]
async fn index_wait_accepts_ready_inventory() {
    IndexWait::Ready.exercise().await;
}

#[tokio::test]
async fn index_wait_rejects_missing_index() {
    IndexWait::Missing.exercise().await;
}

#[tokio::test]
async fn index_wait_enforces_requested_deadline() {
    IndexWait::RequestedDeadline.exercise().await;
}

#[tokio::test]
async fn index_wait_retains_shorter_common_deadline() {
    IndexWait::CommonDeadline.exercise().await;
}

#[tokio::test]
async fn index_wait_rejects_zero_deadline_before_request() {
    IndexWait::InvalidDeadline.exercise().await;
}

#[tokio::test]
async fn index_wait_requested_deadline_includes_response_body() {
    IndexWait::RequestedBodyDeadline.exercise().await;
}
