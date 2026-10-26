#![cfg(feature = "surrealdb")]

use std::{
    collections::BTreeMap,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use storage_sdk::{
    Database, DatabaseBackend, StorageError,
    database::{
        StorageLocator,
        catalog::{CatalogRequest, CatalogResponse},
        config::{CommonConfig, DatabaseConfig, SurrealConfig, SurrealModeConfig},
        data::{DataRecord, DataRequest, DataResponse, FilterExpr, ReplacePartitionRequest},
    },
};
use surrealdb::types::{Array, ErrorDetails, Object, QueryError, SurrealValue, Value};

// Native RPC normalization/deadline proof only, outside strict_live. This fixture
// uses the pinned SDK's public FlatBuffers codec and typed native error values.
// It cannot establish the availability or transactional behavior of a service.
#[derive(Clone, Copy)]
enum Scenario {
    Deadline,
    RetrySuccess,
    RetryExhausted { max_retries: u32 },
    ContextDeadline,
    BackoffDeadline,
    TransportLoss,
    IndeterminateCommit,
    UntypedConflict,
    PrimaryError,
    NotExecutedOnly,
}

#[derive(Clone, Copy)]
enum Operation {
    ReplacePartition,
    DropCollection,
}

impl Operation {
    async fn execute(
        self,
        database: &Database,
        locator: StorageLocator,
    ) -> Result<(), StorageError> {
        match self {
            Self::ReplacePartition => {
                let request = ReplacePartitionRequest::new(
                    locator,
                    FilterExpr::eq("partition", "a"),
                    "marker",
                    vec![
                        DataRecord::without_id()
                            .json_field("partition", serde_json::json!("a"))
                            .json_field("marker", serde_json::json!("record")),
                    ],
                )
                .expect("valid replacement");
                let response = database
                    .data(DataRequest::ReplacePartition(request))
                    .await?;
                assert!(matches!(response, DataResponse::Mutation(_)));
            }
            Self::DropCollection => {
                let response = database
                    .catalog(CatalogRequest::DropCollection {
                        locator,
                        ignore_missing: false,
                    })
                    .await?;
                assert_eq!(response, CatalogResponse::Completed);
            }
        }
        Ok(())
    }
}

struct ConflictServer {
    address: SocketAddr,
    stopping: Arc<AtomicBool>,
    attempts: Arc<AtomicUsize>,
    worker: Option<JoinHandle<Result<(), String>>>,
}

impl ConflictServer {
    fn start(scenario: Scenario) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind native RPC fixture");
        listener
            .set_nonblocking(true)
            .expect("nonblocking listener");
        let address = listener.local_addr().expect("fixture address");
        let stopping = Arc::new(AtomicBool::new(false));
        let attempts = Arc::new(AtomicUsize::new(0));
        let worker_stopping = Arc::clone(&stopping);
        let worker_attempts = Arc::clone(&attempts);
        let worker = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(15);
            while !worker_stopping.load(Ordering::Acquire) && Instant::now() < deadline {
                match listener.accept() {
                    Ok((stream, _)) => {
                        Self::reply(stream, scenario, &worker_attempts, &worker_stopping)?;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(2));
                    }
                    Err(error) => return Err(error.to_string()),
                }
            }
            Ok(())
        });
        Self {
            address,
            stopping,
            attempts,
            worker: Some(worker),
        }
    }

    fn request(stream: &mut TcpStream) -> Result<(String, Vec<u8>), String> {
        // Accepted sockets may inherit the listener's nonblocking mode on BSD.
        // The owned worker intentionally uses bounded blocking stream I/O.
        stream
            .set_nonblocking(false)
            .map_err(|error| error.to_string())?;
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .map_err(|error| error.to_string())?;
        stream
            .set_write_timeout(Some(Duration::from_secs(3)))
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
            if bytes.len() > 128 * 1024 {
                return Err("request exceeded fixture bound".to_owned());
            }
            if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break position + 4;
            }
        };
        let headers =
            std::str::from_utf8(&bytes[..header_end]).map_err(|error| error.to_string())?;
        let line = headers
            .lines()
            .next()
            .ok_or("missing HTTP request line")?
            .to_owned();
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
        if length > 128 * 1024 {
            return Err("request body exceeded fixture bound".to_owned());
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
        Ok((line, bytes[header_end..header_end + length].to_vec()))
    }

    fn statement(error: Option<(ErrorDetails, &str)>) -> Value {
        let mut row = BTreeMap::from([
            ("time".to_owned(), Value::String("0ns".to_owned())),
            ("type".to_owned(), Value::None),
        ]);
        match error {
            None => {
                row.insert("status".to_owned(), Value::String("OK".to_owned()));
                row.insert("result".to_owned(), Value::None);
            }
            Some((details, message)) => {
                let Value::Object(details) = details.into_value() else {
                    panic!("native error details are an object")
                };
                row.extend(details.into_inner());
                row.insert("status".to_owned(), Value::String("ERR".to_owned()));
                row.insert("result".to_owned(), Value::String(message.to_owned()));
            }
        }
        Value::Object(Object::from(row))
    }

    fn reply(
        mut stream: TcpStream,
        scenario: Scenario,
        attempts: &AtomicUsize,
        stopping: &AtomicBool,
    ) -> Result<(), String> {
        let (line, bytes) = Self::request(&mut stream)?;
        let response = if line.starts_with("GET /health ") {
            Vec::new()
        } else {
            let Value::Object(request) =
                surrealdb::types::decode::<Value>(&bytes).map_err(|error| error.to_string())?
            else {
                return Err("native RPC request must be an object".to_owned());
            };
            let method = match request.get("method") {
                Some(Value::String(method)) => method.as_str(),
                _ => return Err("RPC method is missing".to_owned()),
            };
            let mutation = match request.get("params") {
                Some(Value::Array(params)) => {
                    matches!(params.first(), Some(Value::String(sql)) if sql.contains("BEGIN TRANSACTION") && sql.contains("DELETE "))
                }
                _ => false,
            };
            if matches!(scenario, Scenario::ContextDeadline)
                && method == "use"
                && matches!(request.get("params"), Some(Value::Array(params)) if params.first() == Some(&Value::String("deadline_scoped".to_owned())))
            {
                let until = Instant::now() + Duration::from_millis(700);
                while Instant::now() < until && !stopping.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(2));
                }
            }
            let result = if mutation {
                let attempt = attempts.fetch_add(1, Ordering::AcqRel) + 1;
                if matches!(scenario, Scenario::TransportLoss) {
                    // The request was accepted; closing before its response
                    // leaves commit authority unknown and must not be replayed.
                    return Ok(());
                }
                if matches!(scenario, Scenario::Deadline | Scenario::ContextDeadline) {
                    if attempt == 1 {
                        // Spend most of the configured operation deadline on an
                        // exact retry-safe conflict, then hold the next response.
                        let until = Instant::now() + Duration::from_millis(1200);
                        while Instant::now() < until && !stopping.load(Ordering::Acquire) {
                            std::thread::sleep(Duration::from_millis(2));
                        }
                    } else {
                        while !stopping.load(Ordering::Acquire) {
                            std::thread::sleep(Duration::from_millis(2));
                        }
                        return Ok(());
                    }
                }
                let conflict = Self::statement(Some((
                    ErrorDetails::Query(Some(QueryError::TransactionConflict)),
                    "native-conflict-marker",
                )));
                let not_executed = Self::statement(Some((
                    ErrorDetails::Query(Some(QueryError::NotExecuted)),
                    "not-executed-marker",
                )));
                let statements = match scenario {
                    Scenario::Deadline
                    | Scenario::ContextDeadline
                    | Scenario::BackoffDeadline
                    | Scenario::RetryExhausted { .. } => vec![not_executed, conflict],
                    Scenario::RetrySuccess if attempt == 1 => vec![not_executed, conflict],
                    Scenario::RetrySuccess => vec![Self::statement(None)],
                    Scenario::PrimaryError => vec![
                        conflict,
                        Self::statement(Some((ErrorDetails::Internal, "primary-error-marker"))),
                    ],
                    Scenario::NotExecutedOnly => vec![not_executed],
                    Scenario::IndeterminateCommit => vec![Self::statement(Some((
                        ErrorDetails::Query(None),
                        "native-unknown-commit-marker",
                    )))],
                    Scenario::UntypedConflict => vec![Self::statement(Some((
                        ErrorDetails::Internal,
                        "Query not executed: Transaction conflict: This transaction can be retried",
                    )))],
                    Scenario::TransportLoss => unreachable!("lost transport has no response"),
                };
                Value::Array(Array::from(statements))
            } else if method == "query" {
                Value::Array(Array::from(vec![Self::statement(None)]))
            } else if method == "version" {
                Value::String("surrealdb-3.3.0".to_owned())
            } else {
                Value::None
            };
            let mut envelope = BTreeMap::from([("result".to_owned(), result)]);
            for key in ["id", "session"] {
                if let Some(value) = request.get(key) {
                    envelope.insert(key.to_owned(), value.clone());
                }
            }
            surrealdb::types::encode(&Value::Object(Object::from(envelope)))
                .map_err(|error| error.to_string())?
        };
        write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/surrealdb\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", response.len()).map_err(|error| error.to_string())?;
        stream
            .write_all(&response)
            .map_err(|error| error.to_string())
    }

    fn finish(mut self) -> Result<usize, String> {
        self.stopping.store(true, Ordering::Release);
        self.worker
            .take()
            .ok_or("missing owned RPC worker")?
            .join()
            .map_err(|_| "fixture thread panicked".to_owned())??;
        Ok(self.attempts.load(Ordering::Acquire))
    }

    async fn exercise(operation: Operation, scenario: Scenario) {
        let server = Self::start(scenario);
        let config = DatabaseConfig::surreal(SurrealConfig {
            mode: SurrealModeConfig::RemoteHttp {
                endpoint: format!("http://{}", server.address),
            },
            namespace: Some("deadline".to_owned()),
            database: Some("proof".to_owned()),
            username: None,
            password: None,
        })
        .with_common(CommonConfig {
            request_timeout_ms: if matches!(scenario, Scenario::BackoffDeadline) {
                200
            } else {
                2000
            },
            max_retries: match scenario {
                Scenario::RetryExhausted { max_retries } => max_retries,
                Scenario::BackoffDeadline => 1000,
                _ => 8,
            },
            ..CommonConfig::default()
        });
        let database = match tokio::time::timeout(Duration::from_secs(5), Database::connect(config))
            .await
        {
            Ok(Ok(database)) => database,
            Ok(Err(error)) => {
                let cleanup = server.finish();
                panic!("supplied Surreal HTTP adapter: {error:?}; fixture cleanup: {cleanup:?}");
            }
            Err(error) => {
                let cleanup = server.finish();
                panic!("fixture connection bound: {error:?}; fixture cleanup: {cleanup:?}");
            }
        };
        let locator = if matches!(scenario, Scenario::ContextDeadline) {
            StorageLocator::new("records").with_namespace(["deadline_scoped"])
        } else {
            StorageLocator::new("records")
        };
        let outcome = tokio::time::timeout(
            Duration::from_millis(if matches!(scenario, Scenario::BackoffDeadline) {
                800
            } else {
                2600
            }),
            operation.execute(&database, locator),
        )
        .await;
        drop(database);
        let cleanup = server.finish(); // Unblock and join before asserting, including timeout.
        let result = outcome.unwrap_or_else(|error| {
            panic!("one operation deadline must not restart after a conflict: {error:?}; fixture cleanup: {cleanup:?}")
        });
        let attempts = cleanup.unwrap_or_else(|error| {
            panic!("native RPC fixture failed: {error}; operation: {result:?}")
        });
        match scenario {
            Scenario::Deadline | Scenario::ContextDeadline => {
                assert_eq!(
                    attempts, 2,
                    "the test must observe an actual conflict retry"
                );
                assert!(
                    matches!(result, Err(StorageError::BackendOperation { message, .. }) if message.contains("timed out")),
                    "the original operation deadline must be reported"
                );
            }
            Scenario::RetrySuccess => {
                assert_eq!(attempts, 2);
                assert!(
                    result.is_ok(),
                    "exact conflict plus not-executed statements may retry: {result:?}"
                );
            }
            Scenario::RetryExhausted { max_retries } => {
                assert_eq!(attempts, max_retries as usize + 1);
                assert!(
                    matches!(result, Err(StorageError::BackendOperation { message, .. }) if message.contains("native-conflict-marker")),
                    "exhaustion must retain the final native conflict"
                );
            }
            Scenario::BackoffDeadline => {
                assert!(attempts > 1, "must retry before exhausting the deadline");
                assert!(attempts < 1001, "deadline must interrupt retry backoff");
                assert!(
                    matches!(result, Err(StorageError::BackendOperation { message, .. }) if message.contains("timed out")),
                    "the original deadline must include retry backoff"
                );
            }
            Scenario::IndeterminateCommit | Scenario::UntypedConflict => {
                assert_eq!(
                    attempts, 1,
                    "only typed rollback conflicts establish retry safety"
                );
                assert!(
                    result.is_err(),
                    "uncertain native completion cannot succeed"
                );
            }
            Scenario::TransportLoss => {
                assert_eq!(
                    attempts, 1,
                    "uncertain native completion must not be replayed"
                );
                assert!(result.is_err(), "lost response cannot report completion");
            }
            Scenario::PrimaryError => {
                assert_eq!(attempts, 1, "a primary error cannot be replayed");
                assert!(
                    matches!(result, Err(StorageError::BackendOperation { message, .. }) if message.contains("primary-error-marker")),
                    "retain the primary statement error"
                );
            }
            Scenario::NotExecutedOnly => {
                assert_eq!(
                    attempts, 1,
                    "not-executed alone does not establish retry safety"
                );
                assert!(result.is_err());
            }
        }
    }
}

impl Drop for ConflictServer {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[tokio::test]
async fn partition_retry_preserves_original_operation_deadline() {
    ConflictServer::exercise(Operation::ReplacePartition, Scenario::Deadline).await;
}
#[tokio::test]
async fn exact_conflict_with_rolled_back_statements_can_retry() {
    ConflictServer::exercise(Operation::ReplacePartition, Scenario::RetrySuccess).await;
}
#[tokio::test]
async fn conflict_does_not_hide_nonretryable_primary_error() {
    ConflictServer::exercise(Operation::ReplacePartition, Scenario::PrimaryError).await;
}
#[tokio::test]
async fn not_executed_without_conflict_is_not_replayed() {
    ConflictServer::exercise(Operation::ReplacePartition, Scenario::NotExecutedOnly).await;
}

#[tokio::test]
async fn collection_drop_retries_exact_rolled_back_conflict() {
    ConflictServer::exercise(Operation::DropCollection, Scenario::RetrySuccess).await;
}

#[tokio::test]
async fn collection_drop_preserves_retry_limit_and_final_conflict() {
    for max_retries in [0, 2] {
        ConflictServer::exercise(
            Operation::DropCollection,
            Scenario::RetryExhausted { max_retries },
        )
        .await;
    }
}

#[tokio::test]
async fn collection_drop_does_not_replay_mixed_error() {
    ConflictServer::exercise(Operation::DropCollection, Scenario::PrimaryError).await;
}

#[tokio::test]
async fn collection_drop_does_not_replay_not_executed_alone() {
    ConflictServer::exercise(Operation::DropCollection, Scenario::NotExecutedOnly).await;
}

#[tokio::test]
async fn collection_drop_does_not_replay_uncertain_transport_completion() {
    ConflictServer::exercise(Operation::DropCollection, Scenario::TransportLoss).await;
}

#[tokio::test]
async fn collection_drop_preserves_original_deadline_across_queries() {
    ConflictServer::exercise(Operation::DropCollection, Scenario::Deadline).await;
}

#[tokio::test]
async fn collection_drop_deadline_includes_context_selection() {
    ConflictServer::exercise(Operation::DropCollection, Scenario::ContextDeadline).await;
}

#[tokio::test]
async fn collection_drop_deadline_includes_retry_backoff() {
    ConflictServer::exercise(Operation::DropCollection, Scenario::BackoffDeadline).await;
}

#[tokio::test]
async fn collection_drop_does_not_replay_indeterminate_commit() {
    ConflictServer::exercise(Operation::DropCollection, Scenario::IndeterminateCommit).await;
}

#[tokio::test]
async fn collection_drop_does_not_infer_conflict_from_message() {
    ConflictServer::exercise(Operation::DropCollection, Scenario::UntypedConflict).await;
}
