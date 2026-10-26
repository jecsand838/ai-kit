mod codex_release;
mod lance_state_support;
mod native_cleanup;

use lance_state_support::LanceStateDirectory;
use native_cleanup::NativeCleanup;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use agent_sdk::config::{AgentConfig, CodexConfig};
use agent_sdk::conversation::NewConversation;
use agent_sdk::error::{DeadlineKind, ErrorDetail, ErrorKind, ErrorPhase, Operation};
use agent_sdk::notification::{ConversationNotificationKind, NotificationResolution, ProcessState};
use agent_sdk::prompt::{PromptEvent, PromptRequest, StructuredOutputRequest};
use agent_sdk::workspace::{
    DeleteWorkspaceRequest, NewWorkspace, UpdateWorkspaceRequest, WorkspaceContext,
    WorkspaceDefaults, WorkspaceExecutionPolicy, WorkspaceNetworkAccess, WorkspaceRoot,
    WorkspaceRootAccess, WorkspaceSandboxMode,
};
use agent_sdk::{Agent, AgentBuilder, AgentStorage, Conversation, ConversationBackend, Workspace};
use agent_sdk::{AgentBackend, WorkspaceBackend};
use futures_util::{FutureExt, SinkExt, StreamExt};
use serde_json::{Value, json};
use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_sdk::state::record::{
    StateKey, StateListRequest, StateListResponse, StateRecord, StateRecordSnapshot,
};
use storage_sdk::{
    StateStore, StateStoreBackend, StateStoreError, StateTransaction, StateTransactionBackend,
};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

// Protocol fixture only: the selected rust-v0.163.0 archive's App Server v2
// AgentMessageDeltaNotification supplies threadId, turnId, itemId and delta.
// This drives the real SDK WebSocket adapter; it is not live Codex evidence.
struct CodexPeer {
    address: SocketAddr,
    events: mpsc::Sender<Value>,
    turns: mpsc::Receiver<String>,
    responses: mpsc::Receiver<Value>,
    interrupts: Arc<AtomicUsize>,
    identity: Arc<std::sync::Mutex<Value>>,
    features: Arc<std::sync::Mutex<Value>>,
    models: Arc<std::sync::Mutex<Value>>,
    requests: Arc<std::sync::Mutex<Vec<Value>>>,
    hold_initialization: Arc<AtomicBool>,
    initializations: Arc<AtomicUsize>,
    thread_starts: Arc<AtomicUsize>,
    rejections: Arc<std::sync::Mutex<BTreeMap<String, Value>>>,
    resume_overrides: Arc<std::sync::Mutex<serde_json::Map<String, Value>>>,
    history: Arc<std::sync::Mutex<Value>>,
    history_override: Arc<std::sync::Mutex<Option<Value>>>,
    held_history: Arc<std::sync::Mutex<Option<oneshot::Receiver<()>>>>,
    history_entered: Arc<tokio::sync::Notify>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

enum FirstStart {
    Ordinary,
    PaginatedEmpty,
    Materialization(Value),
    DeferredInterruptTerminal,
    ThreadObservation(oneshot::Receiver<()>),
    RecoveredTurn {
        status: String,
        error: Value,
        items: Vec<Value>,
    },
    Held(oneshot::Receiver<()>),
    EarlyDeltas {
        count: usize,
        release: Option<oneshot::Receiver<()>>,
        turn: Option<String>,
    },
}

impl CodexPeer {
    async fn start() -> Self {
        Self::start_with(FirstStart::Ordinary).await
    }

    async fn start_with(mut first_start: FirstStart) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("protocol listener");
        let address = listener.local_addr().expect("listener address");
        let (events, mut inbound) = mpsc::channel::<Value>(64);
        let (accepted, turns) = mpsc::channel(16);
        let (replied, responses) = mpsc::channel(16);
        let interrupts = Arc::new(AtomicUsize::new(0));
        let observed_interrupts = Arc::clone(&interrupts);
        let features = Arc::new(std::sync::Mutex::new(json!({"data":[
            {"name":"enable_fanout","enabled":false}, {"name":"multi_agent","enabled":false},
            {"name":"multi_agent_v2","enabled":false}],"nextCursor":null})));
        let reported_features = Arc::clone(&features);
        let models = Arc::new(std::sync::Mutex::new(
            json!({"data":[{"model":"selected-model","displayName":"Selected","hidden":false,
            "supportedReasoningEfforts":[{"reasoningEffort":"medium"}],"defaultReasoningEffort":"medium",
            "supportsPersonality":true,"isDefault":true}],"nextCursor":null}),
        ));
        let reported_models = Arc::clone(&models);
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed_requests = Arc::clone(&requests);
        let identity = Arc::new(std::sync::Mutex::new(json!("codex/0.163.0")));
        let reported_identity = Arc::clone(&identity);
        let hold_initialization = Arc::new(AtomicBool::new(false));
        let held_initialization = Arc::clone(&hold_initialization);
        let initializations = Arc::new(AtomicUsize::new(0));
        let observed_initializations = Arc::clone(&initializations);
        let thread_starts = Arc::new(AtomicUsize::new(0));
        let observed_thread_starts = Arc::clone(&thread_starts);
        let rejections = Arc::new(std::sync::Mutex::new(BTreeMap::<String, Value>::new()));
        let request_rejections = Arc::clone(&rejections);
        let resume_overrides = Arc::new(std::sync::Mutex::new(serde_json::Map::new()));
        let reported_resume_overrides = Arc::clone(&resume_overrides);
        let history = Arc::new(std::sync::Mutex::new(Value::Null));
        let observed_history = Arc::clone(&history);
        let history_override = Arc::new(std::sync::Mutex::new(None::<Value>));
        let overridden_history = Arc::clone(&history_override);
        let held_history = Arc::new(std::sync::Mutex::new(None::<oneshot::Receiver<()>>));
        let held_history_response = Arc::clone(&held_history);
        let history_entered = Arc::new(tokio::sync::Notify::new());
        let entered_history = Arc::clone(&history_entered);
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let connection = tokio::select! {
                connection = listener.accept() => connection.expect("protocol connection").0,
                _ = &mut stopped => return,
            };
            let mut socket = tokio_tungstenite::accept_async(connection)
                .await
                .expect("WebSocket handshake");
            let mut thread = Value::Null;
            let mut policy = Value::Null;
            let mut next_turn = 0;
            let mut defer_interrupt_terminal = false;
            let paginated = matches!(first_start, FirstStart::PaginatedEmpty);
            let mut materialized = false;
            let mut loaded = true;
            loop {
                tokio::select! {
                    _ = &mut stopped => return,
                    event = inbound.recv() => {
                        let Some(event) = event else { break };
                        if event["method"] == "turn/completed" && !defer_interrupt_terminal {
                            thread["turns"] = json!([event["params"]["turn"].clone()]);
                        }
                        if defer_interrupt_terminal && event["method"] == "turn/completed" {
                            let terminal = &event["params"]["turn"];
                            if let Some(turn) = thread["turns"].as_array_mut().unwrap().iter_mut().find(|turn| turn["id"] == terminal["id"])
                                && turn["status"] == "inProgress" {
                                turn["status"] = terminal["status"].clone();
                                turn["error"] = terminal["error"].clone();
                            }
                        }
                        if defer_interrupt_terminal && event["method"] == "item/completed" {
                            let turn_id = &event["params"]["turnId"];
                            if let Some(turn) = thread["turns"].as_array_mut().unwrap().iter_mut().find(|turn| turn["id"] == *turn_id)
                                && turn["status"] == "inProgress" {
                                turn["items"].as_array_mut().unwrap().push(event["params"]["item"].clone());
                            }
                        }
                        *observed_history.lock().unwrap() = thread.clone();
                        if socket.send(Message::Text(event.to_string().into())).await.is_err() { break }
                    }
                    frame = socket.next() => {
                        let Some(Ok(Message::Text(frame))) = frame else { break };
                        let request: Value = serde_json::from_str(&frame).expect("JSON-RPC request");
                        let Some(method) = request["method"].as_str() else {
                            replied.send(request).await.expect("approval response observer");
                            continue;
                        };
                        if method == "initialized" { continue }
                        if method == "initialize" {
                            observed_initializations.fetch_add(1, Ordering::SeqCst);
                            if held_initialization.load(Ordering::SeqCst) { continue }
                        }
                        observed_requests.lock().unwrap().push(request.clone());
                        let rejection = request_rejections.lock().expect("fixture rejections").get(method).cloned();
                        if let Some(error) = rejection {
                            let response = json!({"id":request["id"],"error":error});
                            if socket.send(Message::Text(response.to_string().into())).await.is_err() { break }
                            continue;
                        }
                        let params = &request["params"];
                        if method == "thread/read" && params["includeTurns"] == true
                            && let FirstStart::Materialization(reply) = &first_start
                        {
                            let mut response = json!({"id":request["id"]});
                            if let Some(error) = reply.get("error") {
                                response["error"] = error.clone();
                            } else {
                                let mut read = thread.clone();
                                for (field, value) in reply.as_object().unwrap() {
                                    if value.is_null() { read.as_object_mut().unwrap().remove(field); }
                                    else { read[field] = value.clone(); }
                                }
                                response["result"] = json!({"thread":read});
                            }
                            if socket.send(Message::Text(response.to_string().into())).await.is_err() { break }
                            continue;
                        }
                        if method == "thread/resume" && paginated && !loaded && !materialized {
                            let response = json!({"id":request["id"],"error":{"code":-32600,"message":"thread rollout has not been persisted"}});
                            if socket.send(Message::Text(response.to_string().into())).await.is_err() { break }
                            continue;
                        }
                        if method == "thread/read" && params["threadId"] == "child-thread" && params["includeTurns"] == true {
                            let response = json!({"id":request["id"],"error":{"code":-32600,"message":"ephemeral child has no readable durable history"}});
                            if socket.send(Message::Text(response.to_string().into())).await.is_err() { break }
                            continue;
                        }
                        let mut result = match method {
                            "initialize" => json!({"userAgent":reported_identity.lock().unwrap().clone()}),
                            "thread/start" => {
                                observed_thread_starts.fetch_add(1, Ordering::SeqCst);
                                thread = json!({"id":"stream-thread", "cwd":params["cwd"], "parentThreadId":null,
                                    "threadSource":params["threadSource"], "turns":[]});
                                policy = json!({"cwd":params["cwd"], "runtimeWorkspaceRoots":params["runtimeWorkspaceRoots"],
                                    "sandbox":{"type":"dangerFullAccess"}, "activePermissionProfile":{"id":":danger-full-access","extends":null}});
                                if matches!(&first_start, FirstStart::ThreadObservation(_)) {
                                    let FirstStart::ThreadObservation(release) = std::mem::replace(&mut first_start, FirstStart::Ordinary) else {
                                        unreachable!("thread observation was selected");
                                    };
                                    let event = json!({"method":"thread/started","params":{"thread":thread.clone()}});
                                    socket.send(Message::Text(event.to_string().into())).await.expect("same-thread creation notification");
                                    tokio::select! {
                                        result = release => result.expect("release original thread/start response"),
                                        _ = &mut stopped => return,
                                    }
                                }
                                let mut response = policy.clone();
                                response["thread"] = thread.clone();
                                response
                            }
                            "thread/read" => {
                                if let Some(history) = overridden_history.lock().unwrap().as_ref() {
                                    json!({"thread":history})
                                } else if params["threadId"] == "child-thread" {
                                    let mut child = thread.clone();
                                    child["id"] = json!("child-thread");
                                    child["parentThreadId"] = json!("stream-thread");
                                    child["turns"] = json!([{"id":"child-turn","status":"inProgress","itemsView":"full","items":[],"error":null}]);
                                    json!({"thread":child})
                                } else if params["threadId"] == "foreign-thread" {
                                    json!({"thread":{"id":"foreign-thread","parentThreadId":null,"turns":[]}})
                                } else {
                                    json!({"thread":thread})
                                }
                            }
                            "thread/resume" => {
                                loaded = true;
                                let mut response = policy.clone();
                                response.as_object_mut().unwrap().extend(reported_resume_overrides.lock().unwrap().clone());
                                response["thread"] = thread.clone();
                                response
                            }
                            "thread/name/set" => { materialized = true; json!({}) },
                            "thread/delete" | "thread/backgroundTerminals/clean" => json!({}),
                            "thread/list" | "thread/backgroundTerminals/list" => json!({"data":[],"nextCursor":null}),
                            "thread/unsubscribe" => { loaded = false; json!({"status":"unsubscribed"}) },
                            "experimentalFeature/list" => reported_features.lock().unwrap().clone(),
                            "model/list" => reported_models.lock().unwrap().clone(),
                            "turn/start" => {
                                next_turn += 1;
                                let id = format!("turn-{next_turn}");
                                let mut turn = json!({"id":id,"status":"inProgress","items":[],"error":null});
                                if defer_interrupt_terminal || matches!(&first_start, FirstStart::DeferredInterruptTerminal) {
                                    defer_interrupt_terminal = true;
                                    turn["itemsView"] = json!("full");
                                    turn["items"] = json!([{"type":"userMessage","id":format!("user-{id}"),"clientId":params["clientUserMessageId"]}]);
                                    thread["turns"].as_array_mut().unwrap().push(turn.clone());
                                } else {
                                    thread["turns"] = json!([turn.clone()]);
                                }
                                *observed_history.lock().unwrap() = thread.clone();
                                accepted.send(id.clone()).await.expect("turn observer");
                                if next_turn == 1 {
                                    match std::mem::replace(&mut first_start, FirstStart::Ordinary) {
                                        FirstStart::Ordinary | FirstStart::PaginatedEmpty | FirstStart::Materialization(_) => {}
                                        FirstStart::DeferredInterruptTerminal => {}
                                        FirstStart::ThreadObservation(_) => panic!("thread observation must be consumed before a turn starts"),
                                        FirstStart::RecoveredTurn { status, error, mut items } => {
                                            items.insert(0, json!({"type":"userMessage","clientId":params["clientUserMessageId"]}));
                                            thread["turns"] = json!([{"id":id,"status":status,"itemsView":"full",
                                                "items":items,"error":error}]);
                                            turn = Value::Null;
                                        }
                                        FirstStart::Held(release) => {
                                            tokio::select! {
                                                result = release => result.expect("release original native start"),
                                                _ = &mut stopped => return,
                                            }
                                        }
                                        FirstStart::EarlyDeltas { count, release, turn: observed_turn } => {
                                            for index in 0..count {
                                                let event = json!({"method":"item/agentMessage/delta","params":{
                                                    "threadId":"stream-thread","turnId":observed_turn.as_deref().unwrap_or(&id),"itemId":"early-answer",
                                                    "delta":format!("early {index}")}});
                                                socket.send(Message::Text(event.to_string().into())).await.expect("early native delta");
                                            }
                                            if let Some(release) = release {
                                                tokio::select! {
                                                    result = release => result.expect("release receipt after early output"),
                                                    _ = &mut stopped => return,
                                                }
                                            }
                                        }
                                    }
                                }
                                json!({"turn":turn})
                            }
                            "turn/interrupt" => {
                                observed_interrupts.fetch_add(1, Ordering::SeqCst);
                                let turn = thread["turns"].as_array_mut().unwrap().iter_mut()
                                    .find(|turn| turn["id"] == params["turnId"]).expect("exact interrupted turn");
                                turn["status"] = json!("interrupted");
                                json!({})
                            }
                            method => panic!("unhandled fixture request {method}"),
                        };
                        *observed_history.lock().unwrap() = thread.clone();
                        if method == "thread/read" && params["includeTurns"] == true {
                            let release = held_history_response.lock().unwrap().take();
                            if let Some(release) = release {
                                entered_history.notify_one();
                                tokio::select! {
                                    result = release => { if result.is_err() { return } },
                                    _ = &mut stopped => return,
                                }
                            }
                        }
                        if method == "thread/read" {
                            result["readState"] = json!({"firstUnread":{"type":"turn","turnId":"earlier-turn"},"revision":"opaque"});
                        } else if method == "thread/list" {
                            result["readStates"] = json!({});
                        }
                        let response = json!({"id":request["id"],"result":result});
                        if socket.send(Message::Text(response.to_string().into())).await.is_err() { break }
                        if method == "turn/interrupt" && !defer_interrupt_terminal {
                            let event = json!({"method":"turn/completed","params":{"threadId":"stream-thread","turn":thread["turns"][0]}});
                            if socket.send(Message::Text(event.to_string().into())).await.is_err() { break }
                        }
                    }
                }
            }
            // Keep the fixture task alive until explicit cleanup, including
            // after SDK transport drop, so public runtime task counts cannot
            // mistake a finished peer for a remaining SDK task.
            let _ = stopped.await;
        });
        Self {
            address,
            events,
            turns,
            responses,
            interrupts,
            identity,
            features,
            models,
            requests,
            hold_initialization,
            initializations,
            thread_starts,
            rejections,
            resume_overrides,
            history,
            history_override,
            held_history,
            history_entered,
            stop: Some(stop),
            task: Some(task),
        }
    }

    async fn turn(&mut self) -> String {
        tokio::time::timeout(Duration::from_secs(3), self.turns.recv())
            .await
            .expect("native acceptance progress")
            .expect("native accepted turn")
    }

    fn reject(&self, method: &str, code: i64, message: &str) {
        self.rejections
            .lock()
            .expect("fixture rejections")
            .insert(method.to_owned(), json!({"code":code,"message":message}));
    }

    async fn request(&self, id: i64, method: &str, params: Value) {
        self.events
            .send(json!({"id":id,"method":method,"params":params}))
            .await
            .expect("fixture request source");
    }

    async fn notify(&self, method: &str, params: Value) {
        self.events
            .send(json!({"method":method,"params":params}))
            .await
            .expect("fixture event source");
    }

    async fn complete(&self, turn: &str, text: &str) {
        self.notify(
            "item/completed",
            json!({"threadId":"stream-thread","turnId":turn,"completedAtMs":1,
            "item":{"id":"answer","type":"agentMessage","text":text,"phase":"final_answer"}}),
        )
        .await;
        self.notify("turn/completed", json!({"threadId":"stream-thread", "turn":{"id":turn,"status":"completed","items":[],"error":null}})).await;
    }

    async fn fail(&self, turn: &str, info: Value) {
        self.notify(
            "turn/completed",
            json!({"threadId":"stream-thread", "turn":{
            "id":turn,"status":"failed","items":[],"error":{
                "message":"secret-token native-private-path", "codexErrorInfo":info,
                "additionalDetails":"provider-private-id"}}}),
        )
        .await;
    }

    async fn shutdown(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(mut task) = self.task.take() {
            match tokio::time::timeout(Duration::from_secs(3), &mut task).await {
                Ok(result) => result.expect("fixture task"),
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    panic!("fixture join bound");
                }
            }
        }
    }
}

impl Drop for CodexPeer {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

#[derive(Clone)]
struct WaiterFaultStore {
    inner: StateStore,
    fault: WaiterFault,
    armed: Arc<AtomicBool>,
    attempts: Arc<AtomicUsize>,
}

#[derive(Clone)]
enum WaiterFault {
    InteractionPublication {
        point: InteractionFaultPoint,
        source: StorageFaultSource,
        owner_task: Arc<std::sync::Mutex<Option<tokio::task::Id>>>,
        failures: usize,
        rollback_failure: bool,
        rollbacks: Arc<AtomicUsize>,
    },
    RollbackFailure,
    HeldClear {
        entered: Arc<tokio::sync::Semaphore>,
        release: Arc<tokio::sync::Semaphore>,
        observation_error: bool,
    },
    NotificationReceipt {
        observation: bool,
        cleanup_failure: bool,
        observe_next: Arc<AtomicBool>,
        restore_prefix: Arc<std::sync::Mutex<Option<Value>>>,
        sequence_gap: Arc<AtomicBool>,
        entered: Arc<tokio::sync::Semaphore>,
        release: Arc<tokio::sync::Semaphore>,
        committed: Arc<AtomicUsize>,
    },
    Storage {
        point: StorageFaultPoint,
        source: StorageFaultSource,
        scope: StorageFaultScope,
        skip_transactions: usize,
        transactions: Arc<AtomicUsize>,
    },
    Retry,
    ForeignOwner,
    TerminalRecord {
        patch: Value,
        captured: Arc<std::sync::Mutex<Option<Value>>>,
    },
    TimeoutIntent {
        failures: usize,
    },
    ThreadCreationObservation {
        case: CreationObservationCase,
        reconciled: Arc<tokio::sync::Semaphore>,
        entered: Arc<tokio::sync::Semaphore>,
        release: Arc<[tokio::sync::Semaphore; 2]>,
        committed: Arc<tokio::sync::Semaphore>,
        conflicts: Arc<AtomicUsize>,
    },
    CreationFailure {
        point: StorageFaultPoint,
        source: StorageFaultSource,
        cleanup_failure: bool,
        native_starts: Arc<AtomicUsize>,
        rollbacks: Arc<AtomicUsize>,
    },
    HeldCreation {
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Semaphore>,
    },
    HeldCommit {
        waiter: &'static str,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Semaphore>,
        completed: Arc<AtomicUsize>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CreationObservationCase {
    EventWins,
    ForegroundWins,
    DifferentThread,
    ChangedObligation,
    UnchangedObligation,
    ReadbackUnavailable,
}

struct WaiterFaultTransaction {
    inner: StateTransaction,
    owner: WaiterFaultStore,
    held_waiter: bool,
    held_clear: bool,
    held_notification: bool,
    held_creation_observation: Option<usize>,
    creation_load_failed: bool,
    storage_selected: bool,
    interaction_staged: bool,
    publication_fault: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InteractionFaultPoint {
    Begin,
    PromptLoad,
    Journal,
    Continuation,
    Commit,
    HeldStaging,
    HeldCommit,
    AmbiguousCommit,
    StaleDuplicate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StorageFaultPoint {
    Begin,
    Load,
    Put,
    Delete,
    Commit,
}

impl StorageFaultPoint {
    fn phase(self) -> ErrorPhase {
        match self {
            Self::Begin | Self::Put | Self::Delete => ErrorPhase::StateWrite,
            Self::Load => ErrorPhase::StateRead,
            Self::Commit => ErrorPhase::StateCommit,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StorageFaultSource {
    Unavailable,
    TimedOut,
    Conflict,
}

impl StorageFaultSource {
    fn error(self) -> StateStoreError {
        match self {
            Self::Unavailable => {
                StateStoreError::unavailable("secret-storage-operation", "secret-storage-message")
            }
            Self::TimedOut => StateStoreError::timed_out("secret-storage-operation", 37),
            Self::Conflict => StateStoreError::transaction_conflict(
                "secret-storage-operation",
                "secret-storage-message",
            ),
        }
    }
}

#[derive(Clone)]
enum StorageFaultScope {
    Any,
    Task(Option<tokio::task::Id>),
}

impl StorageFaultScope {
    fn includes_current_task(&self) -> bool {
        match self {
            Self::Any => true,
            Self::Task(expected) => *expected == tokio::task::try_id(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StorageFaultOperation {
    Prompt,
    PromptStream,
    Abort,
    CloseConversation,
    UpdateWorkspace,
    DeleteWorkspace,
}

impl StorageFaultOperation {
    fn operation(self) -> Operation {
        match self {
            Self::Prompt | Self::PromptStream => Operation::Prompt,
            Self::Abort => Operation::Abort,
            Self::CloseConversation => Operation::CloseConversation,
            Self::UpdateWorkspace => Operation::UpdateWorkspace,
            Self::DeleteWorkspace => Operation::DeleteWorkspace,
        }
    }

    fn retries_conflict(self) -> bool {
        matches!(
            self,
            Self::Prompt | Self::PromptStream | Self::Abort | Self::CloseConversation
        )
    }

    async fn assert_fault(self, point: StorageFaultPoint, source: StorageFaultSource) {
        let store = WaiterFaultStore {
            inner: StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
                .await
                .expect("fault store"),
            fault: WaiterFault::Storage {
                point,
                source,
                // An active prompt independently polls its state. Fault the
                // caller's abort transaction, not that competing prompt owner.
                scope: if self == Self::Abort {
                    StorageFaultScope::Task(tokio::task::try_id())
                } else {
                    StorageFaultScope::Any
                },
                // Public close first checks its idle prompt through abort. Select
                // the close mutation itself, rather than that preceding owner.
                skip_transactions: usize::from(self == Self::CloseConversation),
                transactions: Arc::new(AtomicUsize::new(0)),
            },
            armed: Arc::new(AtomicBool::new(false)),
            attempts: Arc::new(AtomicUsize::new(0)),
        };
        let CodexFixture {
            mut peer,
            agent,
            workspace,
            conversation,
            root,
        } = CodexFixture::build_with_store(CodexPeer::start().await, store.clone()).await;
        let mut conversation = Some(conversation);
        let mut active = None;
        let observed = std::panic::AssertUnwindSafe(async {
            let before = workspace.info().await.expect("workspace before fault");
            let conversation_id = conversation.as_ref().expect("conversation").id().clone();
            if self == Self::Abort {
                let stream = conversation.as_ref().expect("conversation")
                    .prompt_stream(PromptRequest::new("active cancellation target").unwrap())
                    .await.expect("active prompt");
                let turn = peer.turn().await;
                active = Some((stream, turn));
                let key = StateKey::new("agent_sdk_codex_prompts_v1", conversation_id.as_str()).unwrap();
                tokio::time::timeout(Duration::from_secs(3), async {
                    loop {
                        if store.inner.load(key.clone()).await.unwrap().is_some_and(|record| {
                            record.record.value["state"]["state"] == "running"
                        }) {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                }).await.expect("accepted prompt must settle before fault admission");
            }
            store.armed.store(true, Ordering::SeqCst);
            let succeeds = source == StorageFaultSource::Conflict && self.retries_conflict();
            let result = tokio::time::timeout(Duration::from_secs(3), async {
                match self {
                    Self::Prompt => {
                        let mut prompt = Box::pin(conversation.as_ref().unwrap()
                            .prompt(PromptRequest::new("phase probe").unwrap()));
                        if succeeds {
                            let turn = tokio::select! {
                                turn = peer.turn() => turn,
                                result = &mut prompt => panic!("conflict must retry admission: {result:?}"),
                            };
                            peer.complete(&turn, "accepted once").await;
                        }
                        prompt.await.map(|response| assert_eq!(response.output(), "accepted once"))
                    }
                    Self::PromptStream => {
                        let mut stream = conversation.as_ref().unwrap()
                            .prompt_stream(PromptRequest::new("stream phase probe").unwrap()).await?;
                        if succeeds {
                            let turn = peer.turn().await;
                            peer.complete(&turn, "accepted once").await;
                        }
                        match stream.recv().await {
                            Some(Err(error)) => Err(error),
                            Some(Ok(PromptEvent::Completed(response))) => {
                                assert_eq!(response.output(), "accepted once");
                                Ok(())
                            }
                            event => panic!("expected a terminal phase result: {event:?}"),
                        }
                    }
                    Self::Abort => conversation.as_ref().unwrap().abort().await,
                    Self::CloseConversation => conversation.take().unwrap().close().await,
                    Self::UpdateWorkspace => agent.update_workspace(UpdateWorkspaceRequest::new(
                        before.id().clone(), before.revision(), "replacement", before.context().clone(),
                        before.defaults().clone(),
                    ).unwrap()).await.map(|_| ()),
                    Self::DeleteWorkspace => agent.delete_workspace(DeleteWorkspaceRequest::new(
                        before.id().clone(), before.revision(),
                    ).unwrap()).await,
                }
            }).await.expect("public operation must settle after a definite injected failure");
            store.armed.store(false, Ordering::SeqCst);
            assert_eq!(store.attempts.load(Ordering::SeqCst), 1, "{self:?} {point:?}");
            if let Some((mut stream, turn)) = active.take() {
                if !succeeds {
                    assert_eq!(peer.interrupts.load(Ordering::SeqCst), 0,
                        "uncommitted cancellation cannot dispatch a native interrupt");
                    peer.complete(&turn, "original prompt completed").await;
                }
                let terminal = tokio::time::timeout(Duration::from_secs(3), stream.recv())
                    .await.expect("accepted prompt settlement");
                if succeeds {
                    assert!(matches!(terminal, Some(Err(error)) if error.kind() == ErrorKind::Cancelled));
                    assert_eq!(peer.interrupts.load(Ordering::SeqCst), 1);
                } else {
                    assert!(matches!(terminal, Some(Ok(PromptEvent::Completed(response)))
                        if response.output() == "original prompt completed"));
                }
            }
            let after = agent.workspace_info(before.id().clone()).await.expect("workspace remains committed");
            assert_eq!(after.revision(), before.revision(), "failed mutation cannot advance revision");
            assert_eq!(after.name(), before.name());
            let conversations = workspace.list_conversations().await.expect("durable conversation inventory");
            assert_eq!(conversations.len(), usize::from(!(self == Self::CloseConversation && succeeds)));
            if !conversations.is_empty() {
                assert_eq!(conversations[0].id(), &conversation_id);
            }
            assert!(matches!(peer.turns.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
                "failed admission and conflict retries cannot duplicate native work");
            if succeeds {
                result.expect("one exact conflict must preserve the permitted retry");
            } else {
                let error = result.expect_err("injected dependency failure");
                assert_eq!(error.context().operation, self.operation(), "{self:?} {point:?}");
                assert_eq!(error.context().phase, point.phase(), "{self:?} {point:?}: {error:?}");
                match source {
                    StorageFaultSource::Unavailable => {
                        assert_eq!(error.kind(), ErrorKind::Integration);
                        assert_eq!(error.detail(), Some(&ErrorDetail::ServiceUnavailable));
                    }
                    StorageFaultSource::TimedOut => {
                        assert_eq!(error.kind(), ErrorKind::Timeout);
                        assert_eq!(error.detail(), Some(&ErrorDetail::DeadlineElapsed {
                            kind: DeadlineKind::Operation, timeout: Some(Duration::from_millis(37)),
                        }));
                    }
                    StorageFaultSource::Conflict => {
                        assert_eq!(error.kind(), ErrorKind::Conflict);
                        assert_eq!(error.detail(), Some(&ErrorDetail::TransactionConflict));
                    }
                }
                assert!(error.cause().is_none());
                assert!(error.supplementary().is_empty());
                assert!(!format!("{error} {error:?}").contains("secret-storage"));
            }
        }).catch_unwind().await;
        store.armed.store(false, Ordering::SeqCst);
        if let Some((stream, turn)) = active.take() {
            peer.complete(&turn, "cleanup terminal").await;
            drop(stream);
        }
        drop(conversation);
        drop(workspace);
        let stopped = tokio::time::timeout(Duration::from_secs(4), agent.shutdown()).await;
        peer.shutdown().await;
        let removed = std::fs::remove_dir_all(root);
        if let Err(panic) = observed {
            std::panic::resume_unwind(panic);
        }
        stopped
            .expect("Agent shutdown bound")
            .expect("Agent shutdown");
        removed.expect("fixture directory cleanup");
    }
}

impl WaiterFaultStore {
    async fn assert_interaction_publication(
        point: InteractionFaultPoint,
        source: StorageFaultSource,
        rollback_failure: bool,
        failures: usize,
    ) {
        let raw = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let armed = Arc::new(AtomicBool::new(false));
        let attempts = Arc::new(AtomicUsize::new(0));
        let rollbacks = Arc::new(AtomicUsize::new(0));
        let store = Self {
            inner: raw.clone(),
            armed: Arc::clone(&armed),
            attempts: Arc::clone(&attempts),
            fault: WaiterFault::InteractionPublication {
                point,
                source,
                owner_task: Arc::new(std::sync::Mutex::new(None)),
                failures,
                rollback_failure,
                rollbacks: Arc::clone(&rollbacks),
            },
        };
        let mut fixture = CodexFixture::build_with_store(CodexPeer::start().await, store).await;
        let observed = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(4), async {
            let mut notifications = fixture.conversation.notification_stream().unwrap();
            let mut stream = fixture.conversation.prompt_stream(PromptRequest::new("approval publication").unwrap()).await.unwrap();
            let turn = fixture.peer.turn().await;
            let prompt_key = StateKey::new("agent_sdk_codex_prompts_v1", fixture.conversation.id().as_str()).unwrap();
            loop {
                if raw.load(prompt_key.clone()).await.unwrap().unwrap().record.value["state"]["turn_id"] == turn { break; }
                tokio::task::yield_now().await;
            }
            armed.store(true, Ordering::SeqCst);
            let callback = json!("exact-publication-callback");
            fixture.peer.events.send(json!({"id":callback,"method":"item/commandExecution/requestApproval","params":{
                "threadId":"stream-thread","turnId":turn,"itemId":"running-command","approvalId":"distinct-approval",
                "kind":"writeStdin","startedAtMs":1,"availableDecisions":["accept","cancel"]}})).await.unwrap();
            let succeeds = source == StorageFaultSource::Conflict && !rollback_failure && failures != usize::MAX
                && point != InteractionFaultPoint::HeldStaging;
            let committed = succeeds || point == InteractionFaultPoint::AmbiguousCommit;
            let notification = if point == InteractionFaultPoint::AmbiguousCommit {
                let response = fixture.peer.responses.recv().await.unwrap();
                assert_eq!(response["id"], callback);
                assert_eq!(response["error"]["code"], -32603);
                None
            } else { tokio::select! {
                notification = notifications.recv() => {
                    assert!(succeeds, "non-conflict or failed rollback must not be retried");
                    Some(notification.unwrap().unwrap())
                }
                response = fixture.peer.responses.recv() => {
                    let response = response.unwrap();
                    assert!(!succeeds, "exact precommit conflict must reconcile, not reject: {response}");
                    assert_eq!(response["id"], callback);
                    assert_eq!(response["error"]["code"], -32603);
                    assert!(!response.to_string().contains("secret"));
                    None
                }
            } };
            armed.store(false, Ordering::SeqCst);
            let attempted = attempts.load(Ordering::SeqCst);
            if failures == usize::MAX {
                assert!((2..=121).contains(&attempted), "conflict attempts must remain bounded: {attempted}");
            } else {
                assert_eq!(attempted, if succeeds && point != InteractionFaultPoint::HeldCommit { failures + 1 } else { 1 }, "{point:?}");
            }
            let failed_attempts = if succeeds { failures } else { attempted };
            assert_eq!(rollbacks.load(Ordering::SeqCst), if matches!(point, InteractionFaultPoint::Begin | InteractionFaultPoint::Commit | InteractionFaultPoint::HeldCommit | InteractionFaultPoint::AmbiguousCommit) { 0 } else { failed_attempts },
                "failed staging must explicitly settle rollback before retry or rejection");
            let continuations = raw.list(StateListRequest::new("agent_sdk_codex_continuations_v1").unwrap()).await.unwrap();
            assert_eq!(continuations.records.len(), usize::from(committed));
            let journal_key = StateKey::new("agent_sdk_conversation_notifications_v1", fixture.conversation.id().as_str()).unwrap();
            let journal = raw.load(journal_key).await.unwrap().unwrap();
            let pending = journal.record.value["pending"].as_array().unwrap();
            assert_eq!(pending.iter().filter(|entry| entry["kind"]["kind"] == "interaction").count(), usize::from(committed));
            if let Some(notification) = notification {
                assert!(matches!(notification.kind(), ConversationNotificationKind::Interaction { title, .. } if title == "Terminal input approval"));
                let continuation = &continuations.records[0].record.value;
                assert_eq!(continuation["request_id"], callback);
                assert_eq!(continuation["notification_id"], json!(notification.id()));
                drop(notifications);
                notifications = fixture.conversation.notification_stream().unwrap();
                let replay = notifications.recv().await.unwrap().unwrap();
                assert_eq!(replay.id(), notification.id());
                assert_eq!(replay.kind(), notification.kind());
                let resolution = fixture.conversation.resolve_notification(notification.id().clone(), NotificationResolution::Allow).await.unwrap_err();
                assert_eq!(resolution.kind(), ErrorKind::Conflict, "enqueue ambiguity remains fenced");
                assert_eq!(fixture.peer.responses.recv().await.unwrap(), json!({"id":callback,"result":{"decision":"accept"}}));
                let repeated = fixture.conversation.resolve_notification(notification.id().clone(), NotificationResolution::Allow).await.unwrap_err();
                assert_eq!(repeated.kind(), ErrorKind::Conflict);
            } else if !committed {
                assert!(tokio::time::timeout(Duration::from_millis(30), notifications.recv()).await.is_err());
            }
            fixture.peer.complete(&turn, "exact approval terminal").await;
            assert!(matches!(stream.recv().await, Some(Ok(PromptEvent::Completed(response))) if response.output() == "exact approval terminal"));
            assert!(stream.recv().await.is_none());
            assert!(fixture.peer.responses.try_recv().is_err(), "no duplicate native reply");
        })).catch_unwind().await;
        armed.store(false, Ordering::SeqCst);
        let mut cleanup = NativeCleanup::new(Duration::from_secs(4));
        cleanup.settle(fixture.conversation.close()).await;
        cleanup.settle(fixture.workspace.close()).await;
        cleanup.settle(fixture.agent.shutdown()).await;
        cleanup
            .settle(async {
                fixture.peer.shutdown().await;
                Ok::<(), agent_sdk::SdkError>(())
            })
            .await;
        cleanup
            .settle(async { std::fs::remove_dir_all(fixture.root) })
            .await;
        cleanup.finish(observed);
    }

    fn publication_failure(&self, current: InteractionFaultPoint) -> Option<StateStoreError> {
        let WaiterFault::InteractionPublication {
            point,
            source,
            owner_task,
            failures,
            ..
        } = &self.fault
        else {
            return None;
        };
        if current != *point || !self.armed.load(Ordering::SeqCst) {
            return None;
        }
        if matches!(
            point,
            InteractionFaultPoint::Begin | InteractionFaultPoint::PromptLoad
        ) {
            let mut owner = owner_task.lock().unwrap();
            if owner.is_none() || *owner != tokio::task::try_id() {
                return None;
            }
            *owner = None;
        }
        if self.attempts.fetch_add(1, Ordering::SeqCst) < *failures {
            Some(source.error())
        } else {
            None
        }
    }

    fn storage_failure(
        &self,
        current: StorageFaultPoint,
        selected: bool,
    ) -> Option<StateStoreError> {
        let WaiterFault::Storage { point, source, .. } = &self.fault else {
            return None;
        };
        if selected && current == *point && self.armed.swap(false, Ordering::SeqCst) {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            Some(source.error())
        } else {
            None
        }
    }
}

#[tokio::test]
async fn codex_interaction_publication_reconciles_journal_conflict() {
    WaiterFaultStore::assert_interaction_publication(
        InteractionFaultPoint::Journal,
        StorageFaultSource::Conflict,
        false,
        1,
    )
    .await;
}

#[tokio::test]
async fn codex_interaction_publication_reconciles_continuation_conflict() {
    WaiterFaultStore::assert_interaction_publication(
        InteractionFaultPoint::Continuation,
        StorageFaultSource::Conflict,
        false,
        1,
    )
    .await;
}

#[tokio::test]
async fn codex_interaction_publication_reconciles_commit_conflict() {
    WaiterFaultStore::assert_interaction_publication(
        InteractionFaultPoint::Commit,
        StorageFaultSource::Conflict,
        false,
        1,
    )
    .await;
}

#[tokio::test]
async fn codex_interaction_publication_does_not_retry_other_failures() {
    for source in [
        StorageFaultSource::Unavailable,
        StorageFaultSource::TimedOut,
    ] {
        WaiterFaultStore::assert_interaction_publication(
            InteractionFaultPoint::Journal,
            source,
            false,
            1,
        )
        .await;
        WaiterFaultStore::assert_interaction_publication(
            InteractionFaultPoint::Commit,
            source,
            false,
            1,
        )
        .await;
    }
    WaiterFaultStore::assert_interaction_publication(
        InteractionFaultPoint::Journal,
        StorageFaultSource::Conflict,
        true,
        1,
    )
    .await;
}

impl StateStoreBackend for WaiterFaultStore {
    type Transaction = WaiterFaultTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        if key.namespace().as_str() == "agent_sdk_codex_thread_creations_v2"
            && let WaiterFault::ThreadCreationObservation {
                case,
                conflicts,
                reconciled,
                ..
            } = &self.fault
            && conflicts.load(Ordering::SeqCst) > 0
        {
            let snapshot = if *case == CreationObservationCase::ReadbackUnavailable
                && self.armed.swap(false, Ordering::SeqCst)
            {
                Err(StateStoreError::unavailable(
                    "load",
                    "injected creation readback failure",
                ))
            } else {
                self.inner.load(key).await
            };
            reconciled.add_permits(1);
            return snapshot;
        }
        if key.namespace().as_str() == "agent_sdk_conversation_notifications_v1"
            && let WaiterFault::NotificationReceipt {
                observe_next,
                entered,
                release,
                ..
            } = &self.fault
            && observe_next.swap(false, Ordering::SeqCst)
        {
            entered.add_permits(1);
            release.acquire().await.unwrap().forget();
        }
        let notification = key.namespace().as_str() == "agent_sdk_conversation_notifications_v1";
        let mut snapshot = self.inner.load(key).await?;
        if notification
            && let WaiterFault::NotificationReceipt { restore_prefix, .. } = &self.fault
            && let Some(prefix) = restore_prefix.lock().unwrap().take()
            && let Some(snapshot) = &mut snapshot
        {
            Arc::make_mut(&mut snapshot.record.value)["pending"]
                .as_array_mut()
                .unwrap()
                .insert(0, prefix);
        }
        if notification
            && let WaiterFault::NotificationReceipt { sequence_gap, .. } = &self.fault
            && sequence_gap.swap(false, Ordering::SeqCst)
            && let Some(snapshot) = &mut snapshot
        {
            let value = Arc::make_mut(&mut snapshot.record.value);
            value["next_sequence"] = json!(value["next_sequence"].as_u64().unwrap() + 1);
        }
        Ok(snapshot)
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        let continuation = request.namespace().as_str() == "agent_sdk_codex_continuations_v1";
        let mut response = self.inner.list(request).await?;
        if continuation
            && self.armed.load(Ordering::SeqCst)
            && let WaiterFault::InteractionPublication { owner_task, .. } = &self.fault
        {
            *owner_task.lock().unwrap() = tokio::task::try_id();
        }
        if continuation
            && self.armed.load(Ordering::SeqCst)
            && matches!(
                &self.fault,
                WaiterFault::InteractionPublication {
                    point: InteractionFaultPoint::StaleDuplicate,
                    ..
                }
            )
        {
            for snapshot in &mut response.records {
                Arc::make_mut(&mut snapshot.record.value)["state"] = json!("pending");
            }
            self.attempts.fetch_add(1, Ordering::SeqCst);
        }
        Ok(response)
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        if let Some(error) = self.publication_failure(InteractionFaultPoint::Begin) {
            return Err(error);
        }
        let storage_selected = match &self.fault {
            WaiterFault::Storage {
                scope,
                skip_transactions,
                transactions,
                ..
            } if self.armed.load(Ordering::SeqCst) && scope.includes_current_task() => {
                transactions.fetch_add(1, Ordering::SeqCst) >= *skip_transactions
            }
            _ => false,
        };
        if let Some(error) = self.storage_failure(StorageFaultPoint::Begin, storage_selected) {
            return Err(error);
        }
        Ok(WaiterFaultTransaction {
            inner: self.inner.begin_transaction().await?,
            owner: self.clone(),
            held_waiter: false,
            held_clear: false,
            held_notification: false,
            held_creation_observation: None,
            creation_load_failed: false,
            storage_selected,
            interaction_staged: false,
            publication_fault: false,
        })
    }
}

impl StateTransactionBackend for WaiterFaultTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        if let Some(error) = self
            .owner
            .storage_failure(StorageFaultPoint::Load, self.storage_selected)
        {
            return Err(error);
        }
        let creation = key.namespace().as_str() == "agent_sdk_codex_thread_creations_v2";
        let prompt = key.namespace().as_str() == "agent_sdk_codex_prompts_v1";
        if prompt
            && let Some(error) = self
                .owner
                .publication_failure(InteractionFaultPoint::PromptLoad)
        {
            self.publication_fault = true;
            return Err(error);
        }
        let mut snapshot = self.inner.load(key).await?;
        if creation
            && let WaiterFault::CreationFailure {
                point,
                source,
                native_starts,
                ..
            } = &self.owner.fault
            && *point == StorageFaultPoint::Load
            && native_starts.load(Ordering::SeqCst) == 1
            && self.owner.armed.swap(false, Ordering::SeqCst)
        {
            self.creation_load_failed = true;
            self.owner.attempts.fetch_add(1, Ordering::SeqCst);
            return Err(source.error());
        }
        if prompt
            && matches!(&self.owner.fault, WaiterFault::ForeignOwner)
            && self.owner.armed.load(Ordering::SeqCst)
            && let Some(snapshot) = &mut snapshot
            && snapshot.record.value["waiter"] == "delivering"
        {
            self.owner.attempts.fetch_add(1, Ordering::SeqCst);
            Arc::make_mut(&mut snapshot.record.value)["client_message_id"] =
                json!("foreign-generation");
        }
        Ok(snapshot)
    }

    async fn put(&mut self, mut record: StateRecord) -> Result<(), StateStoreError> {
        let journal_interaction = record.key.namespace().as_str()
            == "agent_sdk_conversation_notifications_v1"
            && record.value["pending"].as_array().is_some_and(|pending| {
                pending
                    .iter()
                    .any(|entry| entry["kind"]["kind"] == "interaction")
            });
        let continuation = record.key.namespace().as_str() == "agent_sdk_codex_continuations_v1";
        self.interaction_staged |= continuation;
        if continuation
            && self
                .owner
                .publication_failure(InteractionFaultPoint::HeldStaging)
                .is_some()
        {
            self.publication_fault = true;
            tokio::time::sleep(Duration::from_millis(3100)).await;
        }
        let point = if journal_interaction {
            Some(InteractionFaultPoint::Journal)
        } else if continuation {
            Some(InteractionFaultPoint::Continuation)
        } else {
            None
        };
        if let Some(error) = point.and_then(|point| self.owner.publication_failure(point)) {
            self.publication_fault = true;
            return Err(error);
        }
        if record.key.namespace().as_str() == "agent_sdk_codex_thread_creations_v2"
            && record.value["state"]["native_thread_id"] == "stream-thread"
            && let WaiterFault::ThreadCreationObservation {
                case,
                entered,
                release,
                ..
            } = &self.owner.fault
            && self.owner.armed.load(Ordering::SeqCst)
        {
            let position = self.owner.attempts.fetch_add(1, Ordering::SeqCst);
            if position < release.len() {
                if position == 0 {
                    match case {
                        CreationObservationCase::DifferentThread => {
                            Arc::make_mut(&mut record.value)["state"]["native_thread_id"] =
                                json!("other-thread");
                        }
                        CreationObservationCase::ChangedObligation => {
                            Arc::make_mut(&mut record.value)["native_name"] =
                                json!("different-obligation");
                        }
                        CreationObservationCase::UnchangedObligation => {
                            record = StateRecord::new(
                                StateKey::new("fixture_receipts", "unrelated_commit").unwrap(),
                                json!(true),
                            );
                        }
                        CreationObservationCase::EventWins
                        | CreationObservationCase::ForegroundWins
                        | CreationObservationCase::ReadbackUnavailable => {}
                    }
                }
                self.inner.put(record).await?;
                self.held_creation_observation = Some(position);
                entered.add_permits(1);
                release[position]
                    .acquire()
                    .await
                    .expect("release staged creation observation")
                    .forget();
                return Ok(());
            }
        }
        self.held_notification |= record.key.namespace().as_str()
            == "agent_sdk_conversation_notifications_v1"
            && matches!(self.owner.fault, WaiterFault::NotificationReceipt { .. })
            && self.owner.armed.load(Ordering::SeqCst);
        if self.held_notification
            && let WaiterFault::NotificationReceipt {
                cleanup_failure: true,
                ..
            } = &self.owner.fault
            && self.owner.attempts.fetch_add(1, Ordering::SeqCst) == 0
        {
            return Err(StateStoreError::transaction_conflict(
                "put",
                "injected receipt conflict before rollback failure",
            ));
        }
        if let Some(error) = self
            .owner
            .storage_failure(StorageFaultPoint::Put, self.storage_selected)
        {
            return Err(error);
        }
        if record.key.namespace().as_str() == "agent_sdk_codex_prompts_v1"
            && record.value["state"]["state"] == "terminal"
            && let WaiterFault::TerminalRecord { patch, captured } = &self.owner.fault
            && self.owner.armed.swap(false, Ordering::SeqCst)
        {
            *captured.lock().unwrap() = Some(record.value.as_ref().clone());
            let value = Arc::make_mut(&mut record.value);
            for (key, replacement) in patch.as_object().unwrap() {
                if key == "version" && replacement.is_null() {
                    value.as_object_mut().unwrap().remove(key);
                } else {
                    value[key] = replacement.clone();
                }
            }
            self.owner.attempts.fetch_add(1, Ordering::SeqCst);
        }
        if record.key.namespace().as_str() == "agent_sdk_codex_prompts_v1"
            && record.value["waiter"] == "timed_out"
            && let WaiterFault::TimeoutIntent { failures } = &self.owner.fault
            && self.owner.armed.load(Ordering::SeqCst)
            && self.owner.attempts.fetch_add(1, Ordering::SeqCst) < *failures
        {
            return Err(StateStoreError::transaction_conflict(
                "put",
                "injected timeout intent conflict",
            ));
        }
        if let WaiterFault::HeldCommit { waiter, .. } = &self.owner.fault {
            self.held_waiter |= record.key.namespace().as_str() == "agent_sdk_codex_prompts_v1"
                && record.value["waiter"].as_str() == Some(*waiter);
        }
        if record.key.namespace().as_str() == "agent_sdk_codex_prompts_v1"
            && record.value["state"]["state"] == "prepared"
            && let WaiterFault::HeldCreation { entered, release } = &self.owner.fault
            && self.owner.armed.swap(false, Ordering::SeqCst)
        {
            self.inner.put(record).await?;
            entered.notify_one();
            release
                .acquire()
                .await
                .expect("release held caller transaction")
                .forget();
            return Ok(());
        }
        if record.key.namespace().as_str() == "agent_sdk_codex_prompts_v1"
            && record.value["waiter"] == "lost"
            && matches!(&self.owner.fault, WaiterFault::Retry)
            && self.owner.armed.load(Ordering::SeqCst)
        {
            self.owner.attempts.fetch_add(1, Ordering::SeqCst);
            return Err(StateStoreError::transaction_conflict(
                "put",
                "injected terminal waiter conflict",
            ));
        }
        self.inner.put(record).await
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        if let Some(error) = self
            .owner
            .storage_failure(StorageFaultPoint::Delete, self.storage_selected)
        {
            return Err(error);
        }
        self.held_clear |= key.namespace().as_str() == "agent_sdk_codex_prompts_v1"
            && matches!(self.owner.fault, WaiterFault::HeldClear { .. })
            && self.owner.armed.load(Ordering::SeqCst);
        self.inner.delete(key).await
    }

    async fn commit(self) -> Result<(), StateStoreError> {
        if self.interaction_staged
            && self
                .owner
                .publication_failure(InteractionFaultPoint::HeldCommit)
                .is_some()
        {
            tokio::time::sleep(Duration::from_millis(3100)).await;
            return self.inner.commit().await;
        }
        if self.interaction_staged
            && let Some(error) = self
                .owner
                .publication_failure(InteractionFaultPoint::AmbiguousCommit)
        {
            self.inner.commit().await?;
            return Err(error);
        }
        if self.interaction_staged
            && let Some(error) = self
                .owner
                .publication_failure(InteractionFaultPoint::Commit)
        {
            self.inner.rollback().await?;
            return Err(error);
        }
        if self.held_clear
            && let WaiterFault::HeldClear {
                entered,
                release,
                observation_error,
            } = &self.owner.fault
            && self.owner.armed.swap(false, Ordering::SeqCst)
        {
            self.inner.commit().await?;
            self.owner.attempts.fetch_add(1, Ordering::SeqCst);
            entered.add_permits(1);
            release.acquire().await.unwrap().forget();
            return if *observation_error {
                Err(StateStoreError::unavailable(
                    "commit",
                    "owned clear observation failed after publication",
                ))
            } else {
                Ok(())
            };
        }

        if let WaiterFault::CreationFailure {
            point,
            source,
            native_starts,
            ..
        } = &self.owner.fault
            && *point == StorageFaultPoint::Commit
            && native_starts.load(Ordering::SeqCst) == 1
            && self.owner.armed.swap(false, Ordering::SeqCst)
        {
            self.owner.attempts.fetch_add(1, Ordering::SeqCst);
            self.inner.rollback().await?;
            return Err(source.error());
        }
        if let Some(position) = self.held_creation_observation
            && let WaiterFault::ThreadCreationObservation {
                case,
                reconciled,
                committed,
                conflicts,
                ..
            } = &self.owner.fault
        {
            let result = self.inner.commit().await;
            if result.is_ok() {
                committed.add_permits(1);
                if position == 1 && *case == CreationObservationCase::ForegroundWins {
                    reconciled.acquire().await.unwrap().forget();
                }
            }
            if matches!(&result, Err(StateStoreError::TransactionConflict { .. })) {
                conflicts.fetch_add(1, Ordering::SeqCst);
            }
            return result;
        }
        if self.held_notification
            && let WaiterFault::NotificationReceipt {
                observation,
                cleanup_failure: false,
                observe_next,
                entered,
                release,
                committed,
                ..
            } = &self.owner.fault
            && self.owner.armed.swap(false, Ordering::SeqCst)
        {
            self.inner.commit().await?;
            committed.fetch_add(1, Ordering::SeqCst);
            if *observation {
                observe_next.store(true, Ordering::SeqCst);
            } else {
                entered.add_permits(1);
                release.acquire().await.unwrap().forget();
            }
            return Ok(());
        }

        if let Some(error) = self
            .owner
            .storage_failure(StorageFaultPoint::Commit, self.storage_selected)
        {
            self.inner.rollback().await?;
            return Err(error);
        }
        if self.held_waiter
            && let WaiterFault::HeldCommit {
                entered,
                release,
                completed,
                ..
            } = &self.owner.fault
            && self.owner.armed.swap(false, Ordering::SeqCst)
        {
            self.owner.attempts.fetch_add(1, Ordering::SeqCst);
            entered.notify_one();
            release
                .acquire()
                .await
                .expect("release admitted waiter commit")
                .forget();
            self.inner.commit().await?;
            completed.fetch_add(1, Ordering::SeqCst);
            return Ok(());
        }
        self.inner.commit().await
    }

    async fn rollback(self) -> Result<(), StateStoreError> {
        self.inner.rollback().await?;
        if matches!(self.owner.fault, WaiterFault::RollbackFailure)
            && self.owner.armed.swap(false, Ordering::SeqCst)
        {
            self.owner.attempts.fetch_add(1, Ordering::SeqCst);
            return Err(StateStoreError::unavailable(
                "rollback",
                "native observation rollback failed",
            ));
        }

        if self.publication_fault
            && let WaiterFault::InteractionPublication {
                rollback_failure,
                rollbacks,
                ..
            } = &self.owner.fault
        {
            rollbacks.fetch_add(1, Ordering::SeqCst);
            if *rollback_failure {
                return Err(StateStoreError::unavailable(
                    "secret-rollback",
                    "secret-cleanup",
                ));
            }
        }
        if self.creation_load_failed
            && let WaiterFault::CreationFailure {
                cleanup_failure,
                rollbacks,
                ..
            } = &self.owner.fault
        {
            rollbacks.fetch_add(1, Ordering::SeqCst);
            if *cleanup_failure {
                return Err(StateStoreError::unavailable(
                    "rollback",
                    "injected creation rollback failure",
                ));
            }
        }
        if self.held_notification
            && let WaiterFault::NotificationReceipt {
                cleanup_failure: true,
                ..
            } = &self.owner.fault
            && self.owner.attempts.load(Ordering::SeqCst) == 1
        {
            return Err(StateStoreError::unavailable(
                "rollback",
                "injected receipt rollback unavailable",
            ));
        }
        Ok(())
    }
}

struct CodexFixture<S: StateStoreBackend = StateStore> {
    peer: CodexPeer,
    agent: Agent<S>,
    workspace: Workspace<S>,
    conversation: Conversation<S>,
    root: PathBuf,
}

impl CodexFixture {
    async fn build() -> Self {
        let peer = CodexPeer::start().await;
        Self::build_with_peer(peer).await
    }

    async fn build_with_peer(peer: CodexPeer) -> Self {
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("fixture store");
        Self::build_with_store(peer, store).await
    }

    async fn assert_amendment_decision(decision: Value, approvable: bool) {
        let mut fixture = Self::build().await;
        let mut notifications = fixture
            .conversation
            .notification_stream()
            .expect("approval notifications");
        let mut stream = fixture
            .conversation
            .prompt_stream(PromptRequest::new("approve command").expect("prompt"))
            .await
            .expect("prompt admission");
        let turn = fixture.peer.turn().await;
        fixture
            .peer
            .request(
                19,
                "item/commandExecution/requestApproval",
                json!({
                    "threadId":"stream-thread", "turnId":turn, "itemId":"command",
                    "startedAtMs":1, "command":"printf approved",
                    "availableDecisions":["accept", decision, "cancel"]
                }),
            )
            .await;
        let observed =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(3), async {
                tokio::select! {
                    response = fixture.peer.responses.recv() => (None, None, response),
                    notification = notifications.recv() => {
                        let notification = notification
                            .expect("approval delivery remains open")
                            .expect("approval notification");
                        let resolution = fixture.conversation.resolve_notification(
                            notification.id().clone(), NotificationResolution::Allow,
                        ).await;
                        let response = fixture.peer.responses.recv().await;
                        (Some(notification), Some(resolution), response)
                    }
                }
            }))
            .catch_unwind()
            .await;
        if matches!(&observed, Ok(Ok((Some(_), _, _)))) {
            fixture
                .peer
                .notify(
                    "serverRequest/resolved",
                    json!({"threadId":"stream-thread", "requestId":19}),
                )
                .await;
        }
        fixture.peer.complete(&turn, "approved terminal").await;
        let terminal = tokio::time::timeout(Duration::from_secs(3), stream.recv()).await;
        drop(notifications);
        fixture.shutdown().await;

        let (notification, resolution, response) = match observed {
            Ok(observed) => observed.expect("approval response bound"),
            Err(panic) => std::panic::resume_unwind(panic),
        };
        if approvable {
            assert_eq!(
                response,
                Some(json!({"id":19,"result":{"decision":"accept"}})),
                "a pinned optional amendment must not reject the approval request"
            );
            assert!(matches!(
                notification
                    .as_ref()
                    .map(|notification| notification.kind()),
                Some(ConversationNotificationKind::Interaction {
                    response_schema: None,
                    ..
                })
            ));
            assert!(resolution.is_some_and(|result| {
                result
                    .as_ref()
                    .map_or_else(|error| error.kind() == ErrorKind::Conflict, |()| true)
            }));
        } else {
            let response = response.expect("malformed approval receives a native error");
            assert_eq!(response["id"], 19);
            assert_eq!(response["error"]["code"], -32602);
            assert!(response.get("result").is_none());
            assert!(
                notification.is_none(),
                "malformed approval must not publish an interaction"
            );
            assert!(resolution.is_none());
        }
        assert!(matches!(
            terminal,
            Ok(Some(Ok(PromptEvent::Completed(response)))) if response.output() == "approved terminal"
        ));
    }
}

impl<S: StateStoreBackend> CodexFixture<S> {
    async fn build_with_store(peer: CodexPeer, store: S) -> Self {
        let config = AgentConfig::new(
            CodexConfig::tcp(peer.address, Duration::from_secs(3), Duration::from_secs(3))
                .expect("Codex config")
                .into(),
            Duration::from_secs(3),
            NonZeroUsize::new(1).expect("one prompt"),
        )
        .expect("Agent config");
        let agent = AgentBuilder::create(config, AgentStorage::new(store))
            .build()
            .await
            .expect("Codex fixture Agent");
        let root =
            std::env::temp_dir().join(format!("agent-sdk-codex-stream-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).expect("fixture directory");
        let root = root.canonicalize().expect("canonical fixture root");
        let context = WorkspaceContext::new(
            root.clone(),
            vec![WorkspaceRoot::new(root.clone(), WorkspaceRootAccess::ReadWrite).expect("root")],
            WorkspaceExecutionPolicy::new(
                WorkspaceSandboxMode::FullAccess,
                WorkspaceNetworkAccess::Enabled,
            ),
        )
        .expect("context");
        let workspace = agent
            .new_workspace(
                NewWorkspace::new("protocol-stream", context, WorkspaceDefaults::empty())
                    .expect("workspace request"),
            )
            .await
            .expect("workspace");
        let conversation = workspace
            .new_conversation(
                NewConversation::new(None, None, Vec::new()).expect("conversation request"),
            )
            .await
            .expect("conversation");
        Self {
            peer,
            agent,
            workspace,
            conversation,
            root,
        }
    }

    async fn settle(self) -> Result<(), agent_sdk::SdkError> {
        let conversation = self.conversation.close().await;
        let workspace = self.workspace.close().await;
        let agent = self.agent.shutdown().await;
        self.peer.shutdown().await;
        std::fs::remove_dir_all(self.root).expect("fixture directory cleanup");
        conversation.and(workspace).and(agent)
    }

    async fn shutdown(self) {
        self.settle().await.expect("fixture cleanup");
    }

    async fn revoke(self, baseline: usize) {
        drop(self.conversation);
        drop(self.workspace);
        drop(self.agent);
        let runtime = tokio::runtime::Handle::current();
        tokio::time::timeout(Duration::from_secs(3), async {
            while runtime.metrics().num_alive_tasks() > baseline {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("revoked native runtime must retain no owned tasks");
        self.peer.shutdown().await;
        std::fs::remove_dir_all(self.root).expect("fixture directory cleanup");
    }

    async fn finish_next_prompt(&mut self) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let mut prompt = Box::pin(
                self.conversation
                    .prompt(PromptRequest::new("next prompt").expect("prompt")),
            );
            let turn = tokio::select! {
                turn = self.peer.turn() => Some(turn),
                result = &mut prompt => {
                    assert!(matches!(result, Err(error) if error.kind() == ErrorKind::Busy), "only definite preadmission Busy can be retried");
                    None
                }
                _ = tokio::time::sleep_until(deadline) => panic!("retained generation did not settle"),
            };
            if let Some(turn) = turn {
                assert_eq!(turn, "turn-2", "observation loss cannot replay native work");
                self.peer.complete(&turn, "next terminal").await;
                let response = tokio::time::timeout_at(deadline, &mut prompt)
                    .await
                    .expect("next prompt deadline")
                    .expect("next terminal");
                assert_eq!(response.output(), "next terminal");
                return;
            }
            tokio::task::yield_now().await;
        }
    }
}

#[tokio::test]
async fn codex_initialization_timeout_identifies_server_verification() {
    let peer = CodexPeer::start().await;
    peer.hold_initialization.store(true, Ordering::SeqCst);
    let config = AgentConfig::new(
        CodexConfig::tcp(
            peer.address,
            Duration::from_secs(2),
            Duration::from_millis(100),
        )
        .expect("native config")
        .into(),
        Duration::from_secs(2),
        NonZeroUsize::new(1).unwrap(),
    )
    .unwrap();
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        AgentBuilder::create(config, AgentStorage::new(store)).build(),
    )
    .await;
    let initialized = peer.initializations.load(Ordering::SeqCst);
    let error = match result {
        Ok(Err(error)) => Some(error),
        Ok(Ok(agent)) => {
            let _ = agent.shutdown().await;
            None
        }
        Err(_) => None,
    };
    peer.shutdown().await;
    assert_eq!(
        initialized, 1,
        "the request reached server verification exactly once"
    );
    let error = error.expect("stalled initialization must fail within its deadline");
    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert_eq!(error.context().operation, Operation::Build);
    assert_eq!(error.context().phase, ErrorPhase::ServerVerification);
}

#[tokio::test]
async fn codex_process_delivery_reconciles_ambiguous_approval_at_prompt_terminal() {
    let mut fixture = CodexFixture::build().await;
    let observed =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(3), async {
            let mut receiver = fixture
                .conversation
                .notification_stream()
                .expect("process notification receiver");
            let mut stream = fixture
                .conversation
                .prompt_stream(PromptRequest::new("one native process").expect("prompt"))
                .await
                .expect("process prompt admission");
            let turn = fixture.peer.turn().await;
            fixture
                .peer
                .notify(
                    "item/started",
                    json!({
                        "threadId":"stream-thread", "turnId":turn, "startedAtMs":1,
                        "item":{"id":"command", "type":"commandExecution", "status":"inProgress",
                            "command":"printf process; exit 7", "cwd":fixture.root,
                            "commandActions":[], "aggregatedOutput":null, "exitCode":null}
                    }),
                )
                .await;
            let running = receiver
                .recv()
                .await
                .expect("running delivery")
                .expect("running item");
            assert!(matches!(
                running.kind(),
                ConversationNotificationKind::Process {
                    state: ProcessState::Running,
                    ..
                }
            ));
            drop(receiver);
            receiver = fixture
                .conversation
                .notification_stream()
                .expect("replay receiver");
            let replay = receiver
                .recv()
                .await
                .expect("running replay")
                .expect("replayed item");
            assert_eq!(replay.id(), running.id());
            assert_eq!(replay.kind(), running.kind());

            fixture
                .peer
                .request(
                    29,
                    "item/commandExecution/requestApproval",
                    json!({
                        "threadId":"stream-thread", "turnId":turn, "itemId":"command",
                        "startedAtMs":1, "command":"printf process; exit 7",
                        "availableDecisions":["accept", "cancel"]
                    }),
                )
                .await;
            let approval = receiver
                .recv()
                .await
                .expect("approval delivery")
                .expect("approval item");
            assert!(matches!(
                approval.kind(),
                ConversationNotificationKind::Interaction {
                    response_schema: None,
                    ..
                }
            ));
            assert!(
                matches!(
                    fixture
                        .conversation
                        .resolve_notification(approval.id().clone(), NotificationResolution::Allow,)
                        .await,
                    Err(error) if error.kind() == ErrorKind::Conflict
                ),
                "native enqueue cannot prove acceptance"
            );
            assert_eq!(
                fixture.peer.responses.recv().await,
                Some(json!({"id":29,"result":{"decision":"accept"}}))
            );
            fixture
                .peer
                .notify(
                    "serverRequest/resolved",
                    json!({
                        "threadId":"stream-thread", "requestId":29
                    }),
                )
                .await;
            fixture
                .peer
                .notify(
                    "item/completed",
                    json!({
                        "threadId":"stream-thread", "turnId":turn, "completedAtMs":2,
                        "item":{"id":"command", "type":"commandExecution", "status":"failed",
                            "command":"printf process; exit 7", "cwd":fixture.root,
                            "commandActions":[], "aggregatedOutput":"process", "exitCode":7}
                    }),
                )
                .await;
            assert!(
                matches!(receiver.recv().await, Some(Err(error)) if error.kind() == ErrorKind::Conflict),
                "ambiguous approval must block later process delivery"
            );
            assert!(
                receiver.recv().await.is_none(),
                "conflicted receiver must stay fused"
            );
            assert!(
                matches!(
                    fixture.conversation.notification_stream(),
                    Err(error) if error.kind() == ErrorKind::Conflict
                ),
                "receiver acquisition cannot bypass owning reconciliation"
            );
            assert!(
                matches!(
                    fixture
                        .conversation
                        .resolve_notification(approval.id().clone(), NotificationResolution::Allow,)
                        .await,
                    Err(error) if error.kind() == ErrorKind::Conflict
                ),
                "ambiguous response must never be resent"
            );

            fixture.peer.complete(&turn, "process_DONE").await;
            assert!(matches!(stream.recv().await,
                Some(Ok(PromptEvent::Completed(response))) if response.output() == "process_DONE"));
            assert!(stream.recv().await.is_none());
            assert!(
                receiver.recv().await.is_none(),
                "reconciliation does not revive the old receiver"
            );
            receiver = fixture
                .conversation
                .notification_stream()
                .expect("reconciled receiver");
            let exited = receiver
                .recv()
                .await
                .expect("retained exit delivery")
                .expect("retained exit item");
            assert_ne!(exited.id(), running.id());
            assert_ne!(exited.id(), approval.id());
            match (running.kind(), exited.kind()) {
                (
                    ConversationNotificationKind::Process {
                        label: running_label,
                        ..
                    },
                    ConversationNotificationKind::Process {
                        label,
                        state: ProcessState::Exited { exit_code },
                    },
                ) => {
                    assert_eq!(label, running_label);
                    assert_eq!(*exit_code, Some(7));
                }
                _ => panic!("retained process exit must follow the original running item"),
            }
            drop(receiver);
            receiver = fixture
                .conversation
                .notification_stream()
                .expect("exit replay receiver");
            let replay = receiver
                .recv()
                .await
                .expect("exit replay")
                .expect("replayed exit item");
            assert_eq!(replay.id(), exited.id());
            assert_eq!(replay.kind(), exited.kind());
            drop(receiver);
            drop(stream);
            fixture.finish_next_prompt().await;
        }))
        .catch_unwind()
        .await;

    // Preserve a failed assertion until every fixture owner has been joined.
    let CodexFixture {
        conversation,
        workspace,
        agent,
        mut peer,
        root,
    } = fixture;
    let conversation_closed = conversation.close().await;
    let workspace_closed = workspace.close().await;
    let agent_stopped = agent.shutdown().await;
    if let Some(stop) = peer.stop.take() {
        let _ = stop.send(());
    }
    let peer_joined = if let Some(mut task) = peer.task.take() {
        let joined = tokio::time::timeout(Duration::from_secs(3), &mut task).await;
        if joined.is_err() {
            task.abort();
            let _ = task.await;
        }
        Some(joined)
    } else {
        None
    };
    let extra_response = peer.responses.try_recv();
    drop(peer);
    let directory_removed = std::fs::remove_dir_all(root);
    match observed {
        Ok(result) => result.expect("process ambiguity proof must remain bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    conversation_closed.expect("conversation cleanup");
    workspace_closed.expect("workspace cleanup");
    agent_stopped.expect("Agent cleanup");
    peer_joined
        .expect("owned peer task")
        .expect("peer join bound")
        .expect("peer task");
    directory_removed.expect("fixture directory cleanup");
    assert!(
        matches!(
            extra_response,
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
        ),
        "approval continuation must have exactly one native response"
    );
}

// Pinned rust-v0.163.0: app-server-protocol/schema/typescript/v2/
// CommandExecutionApprovalDecision.ts keeps enum payload field names in snake_case.
#[tokio::test]
async fn codex_command_approval_available_decision_execpolicy_amendment() {
    CodexFixture::assert_amendment_decision(
        json!({
            "acceptWithExecpolicyAmendment": { "execpolicy_amendment": ["printf"] }
        }),
        true,
    )
    .await;
}

#[tokio::test]
async fn codex_command_approval_available_decision_network_policy_amendment() {
    CodexFixture::assert_amendment_decision(
        json!({
            "applyNetworkPolicyAmendment": {
                "network_policy_amendment": { "host":"example.com", "action":"allow" }
            }
        }),
        true,
    )
    .await;
}

#[tokio::test]
async fn codex_command_approval_available_decision_rejects_malformed_amendments() {
    let mut failures = Vec::new();
    for (case, decision) in [
        (
            "execpolicy/missing",
            json!({"acceptWithExecpolicyAmendment": {}}),
        ),
        (
            "execpolicy/wrong-type",
            json!({"acceptWithExecpolicyAmendment": {"execpolicy_amendment": "printf"}}),
        ),
        (
            "execpolicy/wrong-spelling",
            json!({"acceptWithExecpolicyAmendment": {"execpolicyAmendment": ["printf"]}}),
        ),
        (
            "network/missing",
            json!({"applyNetworkPolicyAmendment": {}}),
        ),
        (
            "network/wrong-type",
            json!({"applyNetworkPolicyAmendment": {"network_policy_amendment": "example.com"}}),
        ),
        (
            "network/wrong-spelling",
            json!({"applyNetworkPolicyAmendment": {
                "networkPolicyAmendment": {"host":"example.com", "action":"allow"}
            }}),
        ),
    ] {
        eprintln!("checking malformed amendment {case}: {decision}");
        // The fixture joins its owned Agent and peer before its assertions.
        // Exercise every malformed case even when an earlier one is accepted.
        if std::panic::AssertUnwindSafe(CodexFixture::assert_amendment_decision(
            decision.clone(),
            false,
        ))
        .catch_unwind()
        .await
        .is_err()
        {
            failures.push((case, decision));
        }
    }
    assert!(
        failures.is_empty(),
        "malformed amendment cases failed: {failures:?}"
    );
}

#[tokio::test]
async fn codex_file_read_rejection_never_exposes_native_error_prose() {
    let fixture = CodexFixture::build().await;
    fixture.peer.reject(
        "fs/readFile",
        -32603,
        "secret-token native-private-path provider-private-id",
    );
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        fixture.conversation.read_file(PathBuf::from("answer.txt")),
    )
    .await;
    fixture.shutdown().await;
    let error = result
        .expect("read-file deadline")
        .expect_err("native rejection");
    assert_eq!(error.kind(), ErrorKind::Integration);
    assert_eq!(error.detail(), Some(&ErrorDetail::RemoteInternal));
    assert_eq!(error.context().operation, Operation::ReadFile);
    assert_eq!(error.context().phase, ErrorPhase::ResponseRead);
    for text in [
        error.message().to_owned(),
        error.to_string(),
        format!("{error:?}"),
    ] {
        assert!(!text.contains("secret-token"));
        assert!(!text.contains("native-private-path"));
        assert!(!text.contains("provider-private-id"));
    }
}

#[tokio::test]
async fn codex_typed_terminal_failures_survive_unary_stream_and_durable_delivery() {
    let cases = [
        (json!("rateLimitExceeded"), ErrorDetail::RateLimited, None),
        (
            json!("flexUnavailable"),
            ErrorDetail::ServiceUnavailable,
            None,
        ),
        (
            json!("misalignmentPolicyViolation"),
            ErrorDetail::PolicyRejected,
            None,
        ),
        (json!("tooManyDenials"), ErrorDetail::PolicyRejected, None),
        (
            json!("future-private-error"),
            ErrorDetail::RemoteRejected,
            None,
        ),
        (
            json!({"future-private-error":{"secret-token":true}}),
            ErrorDetail::RemoteRejected,
            None,
        ),
        (
            json!("contextWindowExceeded"),
            ErrorDetail::ContextLimit,
            None,
        ),
        (
            json!("sessionBudgetExceeded"),
            ErrorDetail::SessionLimit,
            None,
        ),
        (
            json!("usageLimitExceeded"),
            ErrorDetail::ResourceExhausted,
            None,
        ),
        (
            json!("serverOverloaded"),
            ErrorDetail::ServiceUnavailable,
            None,
        ),
        (json!("cyberPolicy"), ErrorDetail::PolicyRejected, None),
        (
            json!({"httpConnectionFailed":{"httpStatusCode":503}}),
            ErrorDetail::TransportFailure,
            Some(503),
        ),
        (
            json!({"responseStreamConnectionFailed":{"httpStatusCode":502}}),
            ErrorDetail::TransportFailure,
            Some(502),
        ),
        (
            json!("internalServerError"),
            ErrorDetail::RemoteInternal,
            None,
        ),
        (
            json!("unauthorized"),
            ErrorDetail::AuthenticationRejected,
            None,
        ),
        (json!("badRequest"), ErrorDetail::InvalidRequest, None),
        (
            json!("threadRollbackFailed"),
            ErrorDetail::RemoteRejected,
            None,
        ),
        (json!("sandboxError"), ErrorDetail::RemoteRejected, None),
        (
            json!({"responseStreamDisconnected":{"httpStatusCode":500}}),
            ErrorDetail::TransportFailure,
            Some(500),
        ),
        (
            json!({"responseTooManyFailedAttempts":{"httpStatusCode":429}}),
            ErrorDetail::RemoteRejected,
            Some(429),
        ),
        (
            json!({"activeTurnNotSteerable":{"turnKind":"review"}}),
            ErrorDetail::PreconditionFailed,
            None,
        ),
        (json!("other"), ErrorDetail::RemoteRejected, None),
        (Value::Null, ErrorDetail::RemoteRejected, None),
    ];
    for (info, detail, status) in cases {
        for streaming in [false, true] {
            let mut fixture = CodexFixture::build().await;
            let observed = std::panic::AssertUnwindSafe(async {
                if streaming {
                    let mut stream = fixture.conversation.prompt_stream(PromptRequest::new("fail").unwrap()).await.unwrap();
                    let turn = fixture.peer.turn().await;
                    fixture.peer.notify("item/agentMessage/delta", json!({"threadId":"stream-thread","turnId":turn,"itemId":"answer","delta":"partial"})).await;
                    assert!(matches!(tokio::time::timeout(Duration::from_secs(3), stream.recv()).await.unwrap(), Some(Ok(PromptEvent::OutputDelta { .. }))));
                    fixture.peer.fail(&turn, info.clone()).await;
                    let result = tokio::time::timeout(Duration::from_secs(3), stream.recv()).await.unwrap();
                    let error = result.expect("terminal event").expect_err("failed turn cannot complete");
                    assert!(tokio::time::timeout(Duration::from_secs(3), stream.recv()).await.unwrap().is_none());
                    error
                } else {
                    let mut prompt = Box::pin(fixture.conversation.prompt(PromptRequest::new("fail").unwrap()));
                    let turn = tokio::select! {
                        turn = fixture.peer.turn() => turn,
                        result = &mut prompt => panic!("early completion: {result:?}"),
                    };
                    fixture.peer.fail(&turn, info.clone()).await;
                    tokio::time::timeout(Duration::from_secs(3), prompt).await.unwrap().expect_err("failed turn cannot complete")
                }
            }).catch_unwind().await;
            let cleanup = fixture.settle().await;
            let error = match observed {
                Ok(error) => error,
                Err(panic) => std::panic::resume_unwind(panic),
            };
            assert_eq!(error.kind(), ErrorKind::Integration, "{info}");
            assert_eq!(error.context().operation, Operation::Prompt);
            assert_eq!(error.context().phase, ErrorPhase::ModelGeneration);
            assert_eq!(error.detail(), Some(&detail), "{info}");
            assert_eq!(error.http_status(), status, "{info}");
            cleanup.expect("fixture cleanup");
            assert!(!format!("{error} {error:?} {}", error.message()).contains("secret-token"));
        }
    }
}

#[tokio::test]
async fn codex_recovered_terminal_failure_retains_typed_history_error() {
    let peer = CodexPeer::start_with(FirstStart::RecoveredTurn { status: "failed".to_owned(), items: vec![], error: json!({
        "message":"secret-token", "codexErrorInfo":{"httpConnectionFailed":{"httpStatusCode":503}},
        "additionalDetails":"native-private-path"
    }) })
    .await;
    let fixture = CodexFixture::build_with_peer(peer).await;
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        fixture
            .conversation
            .prompt(PromptRequest::new("recover").unwrap()),
    )
    .await;
    fixture.shutdown().await;
    let error = result
        .expect("recovery bound")
        .expect_err("authoritative failed history");
    assert_eq!(error.kind(), ErrorKind::Integration);
    assert_eq!(error.detail(), Some(&ErrorDetail::TransportFailure));
    assert_eq!(error.http_status(), Some(503));
    assert_eq!(error.context().operation, Operation::Prompt);
    assert_eq!(error.context().phase, ErrorPhase::ModelGeneration);
    assert!(!format!("{error} {error:?}").contains("secret-token"));
}

#[tokio::test]
async fn codex_retry_advisory_is_not_a_terminal_or_a_replay_request() {
    let mut fixture = CodexFixture::build().await;
    let mut stream = fixture
        .conversation
        .prompt_stream(PromptRequest::new("retry advisory").unwrap())
        .await
        .unwrap();
    let turn = fixture.peer.turn().await;
    fixture.peer.notify("error", json!({"threadId":"stream-thread","turnId":turn,"willRetry":true,
        "error":{"message":"secret-token","codexErrorInfo":"serverOverloaded","additionalDetails":null}})).await;
    let premature = tokio::time::timeout(Duration::from_millis(30), stream.recv()).await;
    fixture.peer.complete(&turn, "complete once").await;
    let terminal = tokio::time::timeout(Duration::from_secs(3), stream.recv()).await;
    let eof = tokio::time::timeout(Duration::from_secs(3), stream.recv()).await;
    let replay = fixture.peer.turns.try_recv();
    fixture.shutdown().await;
    assert!(premature.is_err());
    assert!(
        matches!(terminal.unwrap(), Some(Ok(PromptEvent::Completed(response))) if response.output() == "complete once")
    );
    assert!(eof.unwrap().is_none());
    assert!(matches!(replay, Err(mpsc::error::TryRecvError::Empty)));
}

#[tokio::test]
async fn codex_abort_storage_load_failure_preserves_detector_phase() {
    StorageFaultOperation::Abort
        .assert_fault(StorageFaultPoint::Load, StorageFaultSource::Unavailable)
        .await;
}

#[tokio::test]
async fn codex_prompt_storage_failures_preserve_detector_phase() {
    for point in [
        StorageFaultPoint::Begin,
        StorageFaultPoint::Load,
        StorageFaultPoint::Put,
        StorageFaultPoint::Commit,
    ] {
        StorageFaultOperation::Prompt
            .assert_fault(point, StorageFaultSource::Unavailable)
            .await;
    }
    StorageFaultOperation::PromptStream
        .assert_fault(StorageFaultPoint::Load, StorageFaultSource::Unavailable)
        .await;
}

#[tokio::test]
async fn codex_abort_storage_failures_preserve_detector_phase() {
    for point in [
        StorageFaultPoint::Begin,
        StorageFaultPoint::Put,
        StorageFaultPoint::Commit,
    ] {
        StorageFaultOperation::Abort
            .assert_fault(point, StorageFaultSource::Unavailable)
            .await;
    }
}

#[tokio::test]
async fn codex_close_storage_failures_preserve_detector_phase() {
    for point in [
        StorageFaultPoint::Begin,
        StorageFaultPoint::Load,
        StorageFaultPoint::Put,
        StorageFaultPoint::Delete,
        StorageFaultPoint::Commit,
    ] {
        StorageFaultOperation::CloseConversation
            .assert_fault(point, StorageFaultSource::Unavailable)
            .await;
    }
}

#[tokio::test]
async fn codex_close_storage_deadlines_preserve_source_and_detector_phase() {
    for point in [StorageFaultPoint::Begin, StorageFaultPoint::Commit] {
        StorageFaultOperation::CloseConversation
            .assert_fault(point, StorageFaultSource::TimedOut)
            .await;
    }
}

#[tokio::test]
async fn codex_workspace_storage_failures_preserve_detector_phase() {
    for point in [
        StorageFaultPoint::Begin,
        StorageFaultPoint::Load,
        StorageFaultPoint::Put,
        StorageFaultPoint::Commit,
    ] {
        StorageFaultOperation::UpdateWorkspace
            .assert_fault(point, StorageFaultSource::Unavailable)
            .await;
    }
    for point in [
        StorageFaultPoint::Begin,
        StorageFaultPoint::Load,
        StorageFaultPoint::Put,
        StorageFaultPoint::Delete,
        StorageFaultPoint::Commit,
    ] {
        StorageFaultOperation::DeleteWorkspace
            .assert_fault(point, StorageFaultSource::Unavailable)
            .await;
    }
}

#[tokio::test]
async fn codex_storage_phase_mapping_preserves_conflict_ownership() {
    for operation in [
        StorageFaultOperation::Prompt,
        StorageFaultOperation::PromptStream,
        StorageFaultOperation::Abort,
        StorageFaultOperation::CloseConversation,
        StorageFaultOperation::UpdateWorkspace,
        StorageFaultOperation::DeleteWorkspace,
    ] {
        operation
            .assert_fault(StorageFaultPoint::Commit, StorageFaultSource::Conflict)
            .await;
    }
}

#[tokio::test]
async fn codex_storage_phase_mapping_preserves_committed_workspace_publication() {
    let CodexFixture {
        peer,
        agent,
        workspace,
        conversation,
        root,
    } = CodexFixture::build().await;
    let observed = std::panic::AssertUnwindSafe(async {
        let before = workspace.info().await.unwrap();
        let replacement = agent
            .update_workspace(
                UpdateWorkspaceRequest::new(
                    before.id().clone(),
                    before.revision(),
                    "committed replacement",
                    before.context().clone(),
                    before.defaults().clone(),
                )
                .unwrap(),
            )
            .await
            .expect("committed workspace update");
        assert_eq!(replacement.revision(), before.revision() + 1);
        assert_eq!(replacement.name(), "committed replacement");
        assert_eq!(
            workspace.info().await.expect_err("stale workspace").kind(),
            ErrorKind::Conflict
        );
        peer.reject("thread/delete", -32603, "secret native cleanup failure");
        agent
            .delete_workspace(
                DeleteWorkspaceRequest::new(replacement.id().clone(), replacement.revision())
                    .unwrap(),
            )
            .await
            .expect("committed deletion remains successful after native cleanup failure");
        assert_eq!(
            agent
                .workspace_info(before.id().clone())
                .await
                .expect_err("deleted workspace")
                .kind(),
            ErrorKind::NotFound
        );
        assert!(agent.list_workspaces().await.unwrap().is_empty());
    })
    .catch_unwind()
    .await;
    drop(conversation);
    drop(workspace);
    let stopped = tokio::time::timeout(Duration::from_secs(4), agent.shutdown()).await;
    peer.shutdown().await;
    let removed = std::fs::remove_dir_all(root);
    if let Err(panic) = observed {
        std::panic::resume_unwind(panic);
    }
    stopped
        .expect("Agent shutdown bound")
        .expect("Agent shutdown");
    removed.expect("fixture directory cleanup");
}

#[tokio::test]
async fn codex_persisted_failure_snapshot_restores_safe_diagnostics_and_rejects_old_versions() {
    let leaf = json!({"kind":"integration","context":{"operation":"prompt","phase":"cleanup"},
        "detail":{"kind":"deadline_elapsed","deadline":"lifecycle","timeout":{"seconds":1,"nanoseconds":25}},
        "http_status":null,"cause":null,"supplementary":[],"truncated":false});
    let failure = json!({"kind":"integration","context":{"operation":"prompt","phase":"model_generation"},
        "detail":{"kind":"transport_failure"},"http_status":503,"cause":leaf,"supplementary":[leaf],"truncated":true});
    let terminal_state = json!({"state":"terminal","turn_id":"turn-1","outcome":{"outcome":"failure","error":failure}});
    for version in [json!(4), json!(3), json!(5), Value::Null] {
        let raw = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let captured = Arc::new(std::sync::Mutex::new(None));
        let store = WaiterFaultStore {
            inner: raw.clone(),
            fault: WaiterFault::TerminalRecord {
                patch: json!({"version":version,"state":terminal_state}),
                captured: Arc::clone(&captured),
            },
            armed: Arc::new(AtomicBool::new(false)),
            attempts: Arc::new(AtomicUsize::new(0)),
        };
        let peer = CodexPeer::start().await;
        let baseline = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        let mut fixture = CodexFixture::build_with_store(peer, store.clone()).await;
        let observed = std::panic::AssertUnwindSafe(async {
            let mut stream = fixture
                .conversation
                .prompt_stream(PromptRequest::new("recorded error").unwrap())
                .await
                .unwrap();
            let turn = fixture.peer.turn().await;
            store.armed.store(true, Ordering::SeqCst);
            fixture
                .peer
                .fail(
                    &turn,
                    json!({"httpConnectionFailed":{"httpStatusCode":503}}),
                )
                .await;
            let terminal = tokio::time::timeout(Duration::from_secs(5), stream.recv()).await;
            let eof = tokio::time::timeout(Duration::from_secs(3), stream.recv()).await;
            (terminal, eof)
        })
        .catch_unwind()
        .await;
        store.armed.store(false, Ordering::SeqCst);
        let prompt_key = StateKey::new(
            "agent_sdk_codex_prompts_v1",
            fixture.conversation.id().as_str(),
        )
        .unwrap();
        fixture.revoke(baseline).await;
        let retained = tokio::time::timeout(Duration::from_secs(3), raw.load(prompt_key))
            .await
            .expect("record observation bound")
            .expect("record observation");
        let (terminal, eof) = match observed {
            Ok(value) => value,
            Err(panic) => std::panic::resume_unwind(panic),
        };
        let original = captured
            .lock()
            .unwrap()
            .clone()
            .expect("terminal snapshot captured");
        assert_eq!(original["version"], 4);
        assert!(!original.to_string().contains("secret-token"));
        let error = terminal
            .expect("terminal deadline")
            .expect("terminal event")
            .expect_err("failed record");
        assert!(eof.expect("EOF deadline").is_none());
        if version == 4 {
            assert_eq!(error.kind(), ErrorKind::Integration);
            assert_eq!(error.detail(), Some(&ErrorDetail::TransportFailure));
            assert_eq!(error.http_status(), Some(503));
            assert_eq!(error.context().phase, ErrorPhase::ModelGeneration);
            assert!(error.diagnostics_truncated());
            assert_eq!(error.cause().unwrap().context().phase, ErrorPhase::Cleanup);
            assert_eq!(
                error.cause().unwrap().detail(),
                Some(&ErrorDetail::DeadlineElapsed {
                    kind: agent_sdk::error::DeadlineKind::Lifecycle,
                    timeout: Some(Duration::new(1, 25))
                })
            );
            assert_eq!(error.supplementary().len(), 1);
            assert_eq!(
                error.supplementary()[0].context().phase,
                ErrorPhase::Cleanup
            );
        } else {
            assert_eq!(error.kind(), ErrorKind::InvalidState);
            assert_eq!(error.context().phase, ErrorPhase::StateRead);
            let retained = retained.expect("unsupported record must not be deleted or repaired");
            assert_eq!(retained.record.value["state"], terminal_state);
            if version.is_null() {
                assert!(retained.record.value.get("version").is_none());
            } else {
                assert_eq!(retained.record.value["version"], version);
            }
        }
    }
}

#[tokio::test]
async fn codex_failed_turn_keeps_primary_diagnostics_when_waiter_cleanup_fails() {
    for interrupted in [false, true] {
        let raw = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let store = WaiterFaultStore {
            inner: raw,
            fault: WaiterFault::Retry,
            armed: Arc::new(AtomicBool::new(false)),
            attempts: Arc::new(AtomicUsize::new(0)),
        };
        let peer = CodexPeer::start().await;
        let baseline = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        let mut fixture = CodexFixture::build_with_store(peer, store.clone()).await;
        let observed = std::panic::AssertUnwindSafe(async {
        let mut stream = fixture
            .conversation
            .prompt_stream(PromptRequest::new("failed cleanup").unwrap())
            .await
            .unwrap();
        let turn = fixture.peer.turn().await;
        store.armed.store(true, Ordering::SeqCst);
        fixture.peer.notify("turn/completed", json!({"threadId":"stream-thread", "turn":{
            "id":turn,"status":if interrupted { "interrupted" } else { "failed" },"items":[],
            "error":{"message":"secret-token","codexErrorInfo":{"httpConnectionFailed":{"httpStatusCode":503}}}
        }})).await;
        let terminal = tokio::time::timeout(Duration::from_secs(5), stream.recv()).await;
        let eof = tokio::time::timeout(Duration::from_secs(3), stream.recv()).await;
        (terminal, eof)
    })
    .catch_unwind()
    .await;
        store.armed.store(false, Ordering::SeqCst);
        fixture.revoke(baseline).await;
        let (terminal, eof) = match observed {
            Ok(value) => value,
            Err(panic) => std::panic::resume_unwind(panic),
        };
        let error = terminal
            .expect("terminal deadline")
            .expect("terminal event")
            .expect_err("native failure");
        assert!(eof.expect("EOF deadline").is_none());
        assert!(store.attempts.load(Ordering::SeqCst) > 0);
        let primary = if interrupted {
            assert_eq!(error.kind(), ErrorKind::Cancelled);
            assert_eq!(error.context().phase, ErrorPhase::Cancellation);
            error
                .cause()
                .expect("cleanup must preserve interruption cause")
        } else {
            &error
        };
        assert_eq!(primary.kind(), ErrorKind::Integration);
        assert_eq!(primary.detail(), Some(&ErrorDetail::TransportFailure));
        assert_eq!(primary.http_status(), Some(503));
        assert_eq!(primary.context().phase, ErrorPhase::ModelGeneration);
        assert!(!format!("{error:?}").contains("secret-token"));
        assert_eq!(error.supplementary().len(), 1);
        assert_eq!(error.supplementary()[0].kind(), ErrorKind::Timeout);
        assert_eq!(
            error.supplementary()[0].context().phase,
            ErrorPhase::Cleanup
        );
    }
}

#[tokio::test]
async fn codex_protocol_fixture_preserves_ordinary_terminal_prompt() {
    let mut fixture = CodexFixture::build().await;
    let mut prompt = Box::pin(
        fixture
            .conversation
            .prompt(PromptRequest::new("ordinary").expect("prompt")),
    );
    let turn = tokio::select! {
        turn = fixture.peer.turn() => turn,
        result = &mut prompt => panic!("prompt ended before peer acceptance: {result:?}"),
    };
    fixture.peer.complete(&turn, "ordinary terminal").await;
    let response = tokio::time::timeout(Duration::from_secs(3), &mut prompt)
        .await
        .expect("terminal bound")
        .expect("ordinary response");
    assert_eq!(response.output(), "ordinary terminal");
    drop(prompt);
    fixture.shutdown().await;
}

#[tokio::test]
async fn codex_prompt_stream_preserves_native_deltas_and_authoritative_terminal() {
    CodexFixture::build().await.stream_case().await;
}

#[tokio::test]
async fn lance_state_memory_codex_stream_preserves_deltas_and_terminal() {
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .expect("Lance memory store");
    CodexFixture::build_with_store(CodexPeer::start().await, store)
        .await
        .stream_case()
        .await;
}

#[tokio::test]
async fn lance_state_directory_codex_stream_preserves_deltas_and_terminal() {
    let directory = LanceStateDirectory::new();
    let store = StateStore::connect(directory.config())
        .await
        .expect("Lance directory store");
    CodexFixture::build_with_store(CodexPeer::start().await, store)
        .await
        .stream_case()
        .await;
}

impl CodexFixture {
    async fn stream_case(self) {
        let mut fixture = self;
        let schema = StructuredOutputRequest::new(json!({"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],"additionalProperties":false})).expect("schema");
        let mut stream = fixture
            .conversation
            .prompt_stream(
                PromptRequest::new("stream")
                    .expect("prompt")
                    .with_structured_output(schema),
            )
            .await
            .expect("native streaming supported");
        let turn = fixture.peer.turn().await;
        fixture.peer.notify("item/reasoning/textDelta", json!({"threadId":"stream-thread","turnId":turn,"itemId":"reasoning","delta":"private reasoning"})).await;
        let text = "🦀".repeat(5_000);
        fixture
            .peer
            .notify(
                "item/agentMessage/delta",
                json!({"threadId":"stream-thread","turnId":turn,"itemId":"answer","delta":text}),
            )
            .await;
        let mut observed = String::new();
        while observed.len() < text.len() {
            let Some(Ok(PromptEvent::OutputDelta { text: fragment })) =
                tokio::time::timeout(Duration::from_secs(3), stream.recv())
                    .await
                    .expect("real delta before completion")
            else {
                panic!("expected real delta")
            };
            assert!(fragment.len() <= 16 * 1024);
            observed.push_str(&fragment);
        }
        assert_eq!(observed, text);
        assert!(
            tokio::time::timeout(Duration::from_millis(25), stream.recv())
                .await
                .is_err()
        );
        fixture
            .peer
            .complete(&turn, "{\"answer\":\"authoritative\"}")
            .await;
        let Some(Ok(PromptEvent::Completed(response))) =
            tokio::time::timeout(Duration::from_secs(3), stream.recv())
                .await
                .expect("terminal bound")
        else {
            panic!("authoritative terminal")
        };
        assert_eq!(response.output(), "{\"answer\":\"authoritative\"}");
        assert_eq!(
            response.structured_output(),
            Some(&json!({"answer":"authoritative"}))
        );
        assert!(stream.recv().await.is_none());
        fixture.shutdown().await;
    }
}

#[tokio::test]
async fn codex_prompt_stream_drop_and_overflow_preserve_accepted_work() {
    for drop_observer in [true, false] {
        let mut fixture = CodexFixture::build().await;
        let mut stream = Some(
            fixture
                .conversation
                .prompt_stream(PromptRequest::new("accepted work").expect("prompt"))
                .await
                .expect("stream admission"),
        );
        let turn = fixture.peer.turn().await;
        if drop_observer {
            drop(stream.take());
        }
        for index in 0..128 {
            fixture.peer.notify("item/agentMessage/delta", json!({"threadId":"stream-thread","turnId":turn,"itemId":"answer","delta":format!("part {index}")})).await;
        }
        assert!(matches!(
            fixture
                .conversation
                .prompt(PromptRequest::new("cannot steal generation").expect("prompt"))
                .await,
            Err(error) if error.kind() == ErrorKind::Busy
        ));
        assert_eq!(fixture.peer.interrupts.load(Ordering::SeqCst), 0);
        fixture.peer.complete(&turn, "accepted terminal").await;
        fixture.finish_next_prompt().await;
        if let Some(mut stream) = stream {
            let mut fragments = 0;
            loop {
                match tokio::time::timeout(Duration::from_secs(3), stream.recv())
                    .await
                    .expect("bounded observation")
                {
                    Some(Ok(PromptEvent::OutputDelta { .. })) => fragments += 1,
                    Some(Err(error)) if error.kind() == ErrorKind::Integration => break,
                    outcome => panic!("overflow cannot claim completion: {outcome:?}"),
                }
            }
            assert!(fragments <= 64);
            assert!(stream.recv().await.is_none());
        }
        assert_eq!(fixture.peer.interrupts.load(Ordering::SeqCst), 0);
        fixture.shutdown().await;
    }
}

#[tokio::test]
async fn codex_prompt_stream_abort_settles_exact_generation_before_new_output() {
    let mut fixture = CodexFixture::build().await;
    let mut stream = fixture
        .conversation
        .prompt_stream(PromptRequest::new("abort").expect("prompt"))
        .await
        .expect("stream admission");
    let old_turn = fixture.peer.turn().await;
    fixture.peer.notify("item/agentMessage/delta", json!({"threadId":"stream-thread","turnId":old_turn,"itemId":"answer","delta":"old partial"})).await;
    assert!(
        matches!(tokio::time::timeout(Duration::from_secs(3), stream.recv()).await.expect("partial deadline"),
        Some(Ok(PromptEvent::OutputDelta { text })) if text == "old partial")
    );
    fixture.conversation.abort().await.expect("explicit abort");
    assert_eq!(fixture.peer.interrupts.load(Ordering::SeqCst), 1);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), stream.recv())
            .await
            .expect("abort terminal deadline"),
        Some(Err(error)) if error.kind() == ErrorKind::Cancelled
    ));
    assert!(stream.recv().await.is_none());
    let mut next = fixture
        .conversation
        .prompt_stream(PromptRequest::new("new generation").expect("prompt"))
        .await
        .expect("next stream");
    let new_turn = fixture.peer.turn().await;
    assert_eq!(new_turn, "turn-2");
    fixture.peer.notify("item/agentMessage/delta", json!({"threadId":"stream-thread","turnId":old_turn,"itemId":"old-item","delta":"stale output"})).await;
    fixture.peer.notify("item/agentMessage/delta", json!({"threadId":"stream-thread","turnId":new_turn,"itemId":"new-item","delta":"new output"})).await;
    assert!(
        matches!(tokio::time::timeout(Duration::from_secs(3), next.recv()).await.expect("new delta deadline"),
        Some(Ok(PromptEvent::OutputDelta { text })) if text == "new output")
    );
    fixture.peer.complete(&new_turn, "new terminal").await;
    assert!(
        matches!(tokio::time::timeout(Duration::from_secs(3), next.recv()).await.expect("new terminal deadline"),
        Some(Ok(PromptEvent::Completed(response))) if response.output() == "new terminal")
    );
    assert!(next.recv().await.is_none());
    assert_eq!(fixture.peer.interrupts.load(Ordering::SeqCst), 1);
    fixture.shutdown().await;
}

#[tokio::test]
async fn codex_prompt_stream_delayed_abort_terminal_preserves_settled_generation() {
    let peer = CodexPeer::start_with(FirstStart::DeferredInterruptTerminal).await;
    let mut fixture = CodexFixture::build_with_peer(peer).await;
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(10),
        async {
            let mut stream = fixture
                .conversation
                .prompt_stream(PromptRequest::new("reconcile interrupted turn").expect("prompt"))
                .await
                .expect("stream admission");
            let old_turn = fixture.peer.turn().await;
            fixture
                .peer
                .notify(
                    "item/agentMessage/delta",
                    json!({
                        "threadId":"stream-thread", "turnId":old_turn,
                        "itemId":"answer", "delta":"accepted partial"
                    }),
                )
                .await;
            assert!(matches!(stream.recv().await,
                Some(Ok(PromptEvent::OutputDelta { text })) if text == "accepted partial"));

            // The peer acknowledges interruption and exposes its exact terminal
            // history, but sends no terminal event until the public owner settles.
            fixture
                .conversation
                .abort()
                .await
                .expect("receipt-reconciled abort");
            assert_eq!(fixture.peer.interrupts.load(Ordering::SeqCst), 1);
            assert!(matches!(stream.recv().await,
                Some(Err(error)) if error.kind() == ErrorKind::Cancelled));
            assert!(stream.recv().await.is_none());

            fixture
                .peer
                .notify(
                    "turn/completed",
                    json!({
                        "threadId":"stream-thread", "turn":{
                            "id":old_turn, "status":"interrupted", "items":[], "error":null
                        }
                    }),
                )
                .await;
            // A request queued behind the event proves the same event owner
            // processed it before the following prompt. No sleep is an oracle.
            fixture
                .peer
                .request(71, "currentTime/read", json!({"threadId":"stream-thread"}))
                .await;
            let response =
                tokio::time::timeout(Duration::from_secs(3), fixture.peer.responses.recv())
                    .await
                    .expect("delayed terminal must leave the native event owner responsive")
                    .expect("native response owner remains open");
            assert_eq!(response["id"], 71);
            assert!(response["result"]["currentTimeAt"].is_i64());
            assert!(response.get("error").is_none());
            fixture.finish_next_prompt().await;
            assert_eq!(fixture.peer.interrupts.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.peer.thread_starts.load(Ordering::SeqCst), 1);
        },
    )
    .await;
    let mut cleanup = NativeCleanup::new(Duration::from_secs(3));
    cleanup.settle(fixture.conversation.close()).await;
    cleanup.settle(fixture.workspace.close()).await;
    cleanup.settle(fixture.agent.shutdown()).await;
    // Complete the peer's own bounded stop/abort/join before recording its
    // result. An outer observation timeout must not drop its joining owner.
    let peer_cleanup = std::panic::AssertUnwindSafe(fixture.peer.shutdown())
        .catch_unwind()
        .await;
    cleanup
        .settle(async {
            if let Err(panic) = peer_cleanup {
                std::panic::resume_unwind(panic);
            }
            Ok::<(), std::io::Error>(())
        })
        .await;
    cleanup
        .settle(async { std::fs::remove_dir_all(fixture.root) })
        .await;
    cleanup.finish(observed);
}

#[tokio::test]
async fn codex_prompt_stream_deadline_interrupts_once_and_retains_generation() {
    let mut fixture = CodexFixture::build().await;
    let mut stream = fixture
        .conversation
        .prompt_stream(
            PromptRequest::new("deadline")
                .expect("prompt")
                .with_timeout(Duration::from_millis(250))
                .expect("timeout"),
        )
        .await
        .expect("stream admission");
    let turn = fixture.peer.turn().await;
    fixture.peer.notify("item/agentMessage/delta", json!({"threadId":"stream-thread","turnId":turn,"itemId":"answer","delta":"before timeout"})).await;
    assert!(
        matches!(tokio::time::timeout(Duration::from_secs(3), stream.recv()).await.expect("partial deadline"),
        Some(Ok(PromptEvent::OutputDelta { text })) if text == "before timeout")
    );
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), stream.recv())
            .await
            .expect("prompt deadline remains active after returning stream"),
        Some(Err(error)) if error.kind() == ErrorKind::Timeout
    ));
    assert!(stream.recv().await.is_none());
    fixture.finish_next_prompt().await;
    assert_eq!(fixture.peer.interrupts.load(Ordering::SeqCst), 1);
    fixture.shutdown().await;
}

#[tokio::test]
async fn codex_prompt_stream_deadline_survives_held_start_receipt_without_replay() {
    let (release, held) = oneshot::channel();
    let peer = CodexPeer::start_with(FirstStart::Held(held)).await;
    let mut fixture = CodexFixture::build_with_peer(peer).await;
    let mut stream = fixture
        .conversation
        .prompt_stream(
            PromptRequest::new("held start receipt")
                .expect("prompt")
                .with_timeout(Duration::from_millis(250))
                .expect("timeout"),
        )
        .await
        .expect("stream admission");
    assert_eq!(fixture.peer.turn().await, "turn-1");
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), stream.recv())
            .await
            .expect("observer deadline includes native acceptance setup"),
        Some(Err(error)) if error.kind() == ErrorKind::Timeout
    ));
    assert!(stream.recv().await.is_none());
    assert!(matches!(
        fixture
            .conversation
            .prompt(PromptRequest::new("cannot replace ambiguous acceptance").expect("prompt"))
            .await,
        Err(error) if error.kind() == ErrorKind::Busy
    ));
    assert_eq!(fixture.peer.interrupts.load(Ordering::SeqCst), 0);
    release.send(()).expect("original start owner remains live");
    fixture.finish_next_prompt().await;
    assert_eq!(fixture.peer.interrupts.load(Ordering::SeqCst), 1);
    fixture.shutdown().await;
}

#[tokio::test]
async fn codex_prompt_stream_deadline_does_not_park_its_own_held_store_transaction() {
    let raw = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .expect("held transaction store");
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let store = WaiterFaultStore {
        inner: raw,
        fault: WaiterFault::HeldCreation {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        },
        armed: Arc::new(AtomicBool::new(true)),
        attempts: Arc::new(AtomicUsize::new(0)),
    };
    let mut fixture = CodexFixture::build_with_store(CodexPeer::start().await, store).await;
    let mut stream = fixture
        .conversation
        .prompt_stream(
            PromptRequest::new("held caller storage")
                .expect("prompt")
                .with_timeout(Duration::from_millis(250))
                .expect("timeout"),
        )
        .await
        .expect("stream admission");
    tokio::time::timeout(Duration::from_secs(1), entered.notified())
        .await
        .expect("prepared write owns its transaction");
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), stream.recv())
            .await
            .expect("observer timeout cannot await a mutation behind its own parked transaction"),
        Some(Err(error)) if error.kind() == ErrorKind::Timeout
    ));
    assert!(stream.recv().await.is_none());
    assert!(matches!(
        fixture.peer.turns.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    release.add_permits(1);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut next = loop {
        match tokio::time::timeout_at(
            deadline,
            fixture
                .conversation
                .prompt_stream(PromptRequest::new("after released transaction").expect("prompt")),
        )
        .await
        .expect("retained preparation cleanup bound")
        {
            Ok(stream) => break stream,
            Err(error) if error.kind() == ErrorKind::Busy => tokio::task::yield_now().await,
            Err(error) => panic!("expired preparation must settle before reuse: {error:?}"),
        }
    };
    let turn = fixture.peer.turn().await;
    assert_eq!(
        turn, "turn-1",
        "expired local preparation never dispatches native work"
    );
    fixture
        .peer
        .complete(&turn, "healthy after storage release")
        .await;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), next.recv()).await.expect("next terminal"),
        Some(Ok(PromptEvent::Completed(response))) if response.output() == "healthy after storage release"
    ));
    assert!(next.recv().await.is_none());
    assert_eq!(fixture.peer.interrupts.load(Ordering::SeqCst), 0);
    fixture.shutdown().await;
}

#[tokio::test]
async fn codex_prompt_stream_early_delta_waits_for_exact_receipt_after_cancelled_recv() {
    let (release, held) = oneshot::channel();
    let peer = CodexPeer::start_with(FirstStart::EarlyDeltas {
        count: 1,
        release: Some(held),
        turn: None,
    })
    .await;
    let mut fixture = CodexFixture::build_with_peer(peer).await;
    let mut stream = fixture
        .conversation
        .prompt_stream(PromptRequest::new("one early native fragment").expect("prompt"))
        .await
        .expect("stream admission");
    let turn = fixture.peer.turn().await;
    // Cancellation of this pending read must retain both the admission gate
    // and the native fragment, without exposing unproven source ownership.
    assert!(
        tokio::time::timeout(Duration::from_millis(25), stream.recv())
            .await
            .is_err()
    );
    assert_eq!(fixture.peer.interrupts.load(Ordering::SeqCst), 0);
    release
        .send(())
        .expect("original native request still owns its response");
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), stream.recv()).await.expect("exact receipt releases retained output"),
        Some(Ok(PromptEvent::OutputDelta { text })) if text == "early 0"
    ));
    fixture
        .peer
        .complete(&turn, "authoritative native terminal")
        .await;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), stream.recv()).await.expect("native terminal after gated text"),
        Some(Ok(PromptEvent::Completed(response))) if response.output() == "authoritative native terminal"
    ));
    assert!(stream.recv().await.is_none());
    fixture.finish_next_prompt().await;
    assert_eq!(fixture.peer.interrupts.load(Ordering::SeqCst), 0);
    fixture.shutdown().await;
}

#[tokio::test]
async fn codex_prompt_stream_early_wrong_turn_is_discarded_after_exact_receipt() {
    let (release, held) = oneshot::channel();
    let peer = CodexPeer::start_with(FirstStart::EarlyDeltas {
        count: 1,
        release: Some(held),
        turn: Some("foreign-turn".to_owned()),
    })
    .await;
    let mut fixture = CodexFixture::build_with_peer(peer).await;
    let mut stream = fixture
        .conversation
        .prompt_stream(PromptRequest::new("exact early owner").expect("prompt"))
        .await
        .expect("stream admission");
    let turn = fixture.peer.turn().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(25), stream.recv())
            .await
            .is_err(),
        "unproven output must remain gated"
    );
    release.send(()).expect("original request remains owned");
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), stream.recv())
            .await
            .expect("mismatched gate must end observation"),
        Some(Err(error)) if error.kind() == ErrorKind::Integration
    ));
    assert!(
        stream.recv().await.is_none(),
        "foreign queued text must never become visible"
    );
    fixture
        .peer
        .complete(&turn, "owned original work continues")
        .await;
    fixture.finish_next_prompt().await;
    assert_eq!(fixture.peer.interrupts.load(Ordering::SeqCst), 0);
    fixture.shutdown().await;
}

#[tokio::test]
async fn codex_prompt_stream_unresolved_gate_does_not_hide_runtime_revocation() {
    let (_release, held) = oneshot::channel();
    let peer = CodexPeer::start_with(FirstStart::EarlyDeltas {
        count: 1,
        release: Some(held),
        turn: None,
    })
    .await;
    let baseline = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();
    let mut fixture = CodexFixture::build_with_peer(peer).await;
    let mut stream = fixture
        .conversation
        .prompt_stream(PromptRequest::new("gated runtime drop").expect("prompt"))
        .await
        .expect("stream admission");
    fixture.peer.turn().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(25), stream.recv())
            .await
            .is_err()
    );
    fixture.revoke(baseline).await;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), stream.recv())
            .await
            .expect("reserved error cannot await gate admission"),
        Some(Err(error)) if error.kind() == ErrorKind::Closed
    ));
    assert!(stream.recv().await.is_none());
}

#[tokio::test]
async fn codex_prompt_stream_pre_receipt_flood_preserves_transport_and_accepted_work() {
    // The pin submits UserInput before constructing TurnStartResponse. Native
    // output may therefore precede the response that proves the accepted turn.
    let (release, receipt) = oneshot::channel();
    let peer = CodexPeer::start_with(FirstStart::EarlyDeltas {
        count: 128,
        release: Some(receipt),
        turn: None,
    })
    .await;
    let mut fixture = CodexFixture::build_with_peer(peer).await;
    let mut stream = fixture
        .conversation
        .prompt_stream(PromptRequest::new("early native output").expect("prompt"))
        .await
        .expect("stream admission");
    let turn = fixture.peer.turn().await;
    let observed = tokio::time::timeout(Duration::from_secs(1), async {
        let mut fragments = 0;
        loop {
            match stream.recv().await {
                Some(Ok(PromptEvent::OutputDelta { .. })) => fragments += 1,
                Some(Err(error)) if error.kind() == ErrorKind::Integration => break fragments,
                event => panic!("lost exact early ownership cannot claim completion: {event:?}"),
            }
        }
    })
    .await
    .expect("native reader must drain before the start receipt behind its output");
    assert_eq!(
        observed, 0,
        "unproven flood text must never cross the receipt gate"
    );
    assert!(stream.recv().await.is_none());
    release
        .send(())
        .expect("release original receipt after bounded overflow");
    fixture
        .peer
        .complete(&turn, "accepted despite observation loss")
        .await;
    fixture.finish_next_prompt().await;
    assert_eq!(fixture.peer.interrupts.load(Ordering::SeqCst), 0);
    fixture.shutdown().await;
}

#[tokio::test]
async fn codex_prompt_stream_requires_exact_root_thread_and_turn() {
    let mut fixture = CodexFixture::build().await;
    let mut stream = fixture
        .conversation
        .prompt_stream(PromptRequest::new("thread isolation").expect("prompt"))
        .await
        .expect("stream admission");
    let turn = fixture.peer.turn().await;
    fixture
        .peer
        .notify(
            "item/agentMessage/delta",
            json!({"threadId":"stream-thread","turnId":turn,"itemId":"root","delta":"root first"}),
        )
        .await;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), stream.recv()).await.expect("root delta"),
        Some(Ok(PromptEvent::OutputDelta { text })) if text == "root first"
    ));
    fixture.peer.notify("item/agentMessage/delta", json!({"threadId":"child-thread","turnId":turn,"itemId":"child","delta":"child must not bleed"})).await;
    fixture
        .peer
        .notify(
            "item/agentMessage/delta",
            json!({"threadId":"stream-thread","turnId":turn,"itemId":"root","delta":"root second"}),
        )
        .await;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), stream.recv()).await.expect("isolated root delta"),
        Some(Ok(PromptEvent::OutputDelta { text })) if text == "root second"
    ));
    fixture.peer.complete(&turn, "root terminal").await;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), stream.recv()).await.expect("root terminal"),
        Some(Ok(PromptEvent::Completed(response))) if response.output() == "root terminal"
    ));
    assert!(stream.recv().await.is_none());
    fixture.shutdown().await;
}

impl WaiterFaultStore {
    async fn assert_timeout_intent(failures: usize) {
        let raw = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("timeout intent store");
        let store = Self {
            inner: raw.clone(),
            fault: WaiterFault::TimeoutIntent { failures },
            armed: Arc::new(AtomicBool::new(true)),
            attempts: Arc::new(AtomicUsize::new(0)),
        };
        let peer = CodexPeer::start().await;
        let baseline = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        let mut fixture = CodexFixture::build_with_store(peer, store.clone()).await;
        let mut notifications = fixture
            .conversation
            .notification_stream()
            .expect("notifications");
        let mut stream = fixture
            .conversation
            .prompt_stream(
                PromptRequest::new("timeout intent ownership")
                    .expect("prompt")
                    .with_timeout(Duration::from_millis(100))
                    .expect("deadline"),
            )
            .await
            .expect("stream admission");
        assert_eq!(fixture.peer.turn().await, "turn-1");
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), stream.recv())
                .await
                .expect("bounded timeout"),
            Some(Err(error)) if error.kind() == ErrorKind::Timeout
        ));
        assert!(stream.recv().await.is_none());
        if failures == 1 {
            tokio::time::timeout(Duration::from_secs(4), async {
                while fixture.peer.interrupts.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("transient intent failure must retry before exact native cancellation");
            assert!(store.attempts.load(Ordering::SeqCst) > 1);
            assert_eq!(fixture.peer.interrupts.load(Ordering::SeqCst), 1);
            drop(notifications);
            fixture.finish_next_prompt().await;
            fixture.shutdown().await;
        } else {
            assert!(tokio::time::timeout(Duration::from_secs(4), notifications.recv()).await
                .expect("permanent intent failure must invalidate observation before native completion")
                .is_none_or(|result| result.is_err_and(|error| error.kind() == ErrorKind::Closed)));
            let attempts = store.attempts.load(Ordering::SeqCst);
            assert!(attempts > 1);
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert_eq!(
                store.attempts.load(Ordering::SeqCst),
                attempts,
                "no timeout-intent retries may be admitted after its lifecycle bound"
            );
            assert_eq!(
                fixture.peer.interrupts.load(Ordering::SeqCst),
                0,
                "failed durable intent cannot authorize native cancellation"
            );
            let record = raw
                .load(
                    StateKey::new(
                        "agent_sdk_codex_prompts_v1",
                        fixture.conversation.id().as_str(),
                    )
                    .expect("key"),
                )
                .await
                .expect("retained state")
                .expect("original generation remains recovery owned");
            assert_eq!(record.record.value["waiter"], "waiting");
            drop(notifications);
            fixture.revoke(baseline).await;
        }
    }

    async fn assert_admitted_commit(waiter: &'static str) {
        let raw = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("held commit store");
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let store = Self {
            inner: raw.clone(),
            fault: WaiterFault::HeldCommit {
                waiter,
                entered: Arc::clone(&entered),
                release: Arc::clone(&release),
                completed: Arc::clone(&completed),
            },
            armed: Arc::new(AtomicBool::new(true)),
            attempts: Arc::new(AtomicUsize::new(0)),
        };
        let peer = CodexPeer::start().await;
        let baseline = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        let mut fixture = CodexFixture::build_with_store(peer, store.clone()).await;
        let request = PromptRequest::new("hold original waiter commit").expect("prompt");
        let request = if waiter == "timed_out" {
            request
                .with_timeout(Duration::from_millis(100))
                .expect("prompt deadline")
        } else {
            request
        };
        let mut stream = fixture
            .conversation
            .prompt_stream(request)
            .await
            .expect("stream admission");
        let turn = fixture.peer.turn().await;
        if waiter == "lost" {
            fixture
                .peer
                .complete(&turn, "native terminal before cleanup")
                .await;
        }
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("original commit entered");
        let held_at = tokio::time::Instant::now();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(5), stream.recv())
                .await
                .expect("observer error remains bounded while admitted commit is held"),
            Some(Err(error)) if error.kind() == ErrorKind::Timeout
        ));
        assert!(stream.recv().await.is_none());
        // Hold past both the user observation deadline and the configured
        // lifecycle retry bound. The admitted commit itself must survive.
        tokio::time::sleep_until(held_at + Duration::from_millis(3_250)).await;
        assert_eq!(completed.load(Ordering::SeqCst), 0);
        release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(1), async {
            while completed.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("deadline must retain and poll the exact admitted commit");
        assert_eq!(completed.load(Ordering::SeqCst), 1);
        assert_eq!(store.attempts.load(Ordering::SeqCst), 1);
        if waiter == "lost" {
            let retained = raw
                .load(
                    StateKey::new(
                        "agent_sdk_codex_prompts_v1",
                        fixture.conversation.id().as_str(),
                    )
                    .expect("key"),
                )
                .await
                .expect("durable commit receipt")
                .expect("cleanup remains recovery owned after its deadline");
            assert_eq!(retained.record.value["waiter"], "lost");
        }
        fixture.revoke(baseline).await;
    }

    async fn assert_terminal(fault: WaiterFault) {
        for primary_failure in [false, true] {
            let raw = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
                .await
                .expect("waiter fault store");
            let store = WaiterFaultStore {
                inner: raw.clone(),
                fault: fault.clone(),
                armed: Arc::new(AtomicBool::new(false)),
                attempts: Arc::new(AtomicUsize::new(0)),
            };
            let peer = CodexPeer::start().await;
            let runtime = tokio::runtime::Handle::current();
            let baseline = runtime.metrics().num_alive_tasks();
            let mut fixture = CodexFixture::build_with_store(peer, store.clone()).await;
            let request = PromptRequest::new("terminal cleanup").expect("prompt");
            let request = if primary_failure {
                request.with_structured_output(
                    StructuredOutputRequest::new(json!({"type":"object"})).expect("schema"),
                )
            } else {
                request
            };
            let mut stream = fixture
                .conversation
                .prompt_stream(request)
                .await
                .expect("stream");
            let turn = fixture.peer.turn().await;
            store.armed.store(true, Ordering::SeqCst);
            fixture.peer.complete(&turn, "terminal text").await;
            let terminal = tokio::time::timeout(Duration::from_secs(5), stream.recv())
                .await
                .expect("terminal settlement bound");
            assert!(
                store.attempts.load(Ordering::SeqCst) > 0,
                "fault reached waiter settlement"
            );
            let record = raw
                .load(
                    StateKey::new(
                        "agent_sdk_codex_prompts_v1",
                        fixture.conversation.id().as_str(),
                    )
                    .expect("key"),
                )
                .await
                .expect("retained prompt")
                .expect("durable failed cleanup");
            assert_eq!(record.record.value["waiter"], "delivering");
            if primary_failure {
                assert!(
                    matches!(terminal, Some(Err(error)) if error.kind() == ErrorKind::Protocol),
                    "primary output failure must survive cleanup failure"
                );
            } else if matches!(&fault, WaiterFault::ForeignOwner) {
                assert!(
                    matches!(terminal, Some(Err(error)) if error.kind() == ErrorKind::Conflict),
                    "definite ownership failure cannot claim Completed"
                );
            } else {
                assert!(
                    matches!(terminal, Some(Err(error)) if error.kind() == ErrorKind::Timeout),
                    "unsettled terminal waiter cannot claim Completed"
                );
            }
            assert!(stream.recv().await.is_none());
            store.armed.store(false, Ordering::SeqCst);
            // The failed runtime retains durable reconciliation evidence. Releasing
            // its public handles must still join/abort its accepted task ownership.
            drop(fixture.conversation);
            drop(fixture.workspace);
            drop(fixture.agent);
            tokio::time::timeout(Duration::from_secs(3), async {
                while runtime.metrics().num_alive_tasks() > baseline {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("failed terminal cannot detach native tasks");
            fixture.peer.shutdown().await;
            std::fs::remove_dir_all(fixture.root).expect("fixture directory cleanup");
        }
    }
}

#[tokio::test]
async fn codex_prompt_stream_terminal_deadline_retains_admitted_waiter_commit() {
    WaiterFaultStore::assert_admitted_commit("lost").await;
}

#[tokio::test]
async fn codex_prompt_stream_retries_transient_timeout_intent_before_native_abort() {
    WaiterFaultStore::assert_timeout_intent(1).await;
}

#[tokio::test]
async fn codex_prompt_stream_permanent_timeout_intent_failure_closes_runtime_observation() {
    WaiterFaultStore::assert_timeout_intent(usize::MAX).await;
}

#[tokio::test]
async fn codex_prompt_stream_deadline_retains_admitted_timeout_intent_commit() {
    WaiterFaultStore::assert_admitted_commit("timed_out").await;
}

#[tokio::test]
async fn codex_prompt_stream_waiter_retry_exhaustion_preserves_primary_error() {
    WaiterFaultStore::assert_terminal(WaiterFault::Retry).await;
}

#[tokio::test]
async fn codex_prompt_stream_definite_waiter_failure_cannot_claim_completed() {
    WaiterFaultStore::assert_terminal(WaiterFault::ForeignOwner).await;
}

#[tokio::test]
async fn codex_prompt_stream_last_handle_drop_releases_owned_tasks() {
    let peer = CodexPeer::start().await;
    let handle = tokio::runtime::Handle::current();
    let baseline = handle.metrics().num_alive_tasks();
    let mut fixture = CodexFixture::build_with_peer(peer).await;
    let mut stream = fixture
        .conversation
        .prompt_stream(PromptRequest::new("owned work").expect("prompt"))
        .await
        .expect("stream admission");
    let turn = fixture.peer.turn().await;
    fixture
        .peer
        .notify(
            "item/agentMessage/delta",
            json!({"threadId":"stream-thread","turnId":turn,"itemId":"answer","delta":"accepted"}),
        )
        .await;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), stream.recv())
            .await
            .expect("partial deadline"),
        Some(Ok(PromptEvent::OutputDelta { .. }))
    ));
    assert!(
        handle.metrics().num_alive_tasks() > baseline,
        "accepted SDK work is independently alive"
    );
    let CodexFixture {
        peer,
        agent,
        workspace,
        conversation,
        root,
    } = fixture;
    drop(conversation);
    drop(workspace);
    drop(agent);
    tokio::time::timeout(Duration::from_secs(3), async {
        while handle.metrics().num_alive_tasks() > baseline {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("last native runtime handle cannot detach accepted prompt or transport tasks");
    assert!(
        matches!(
            tokio::time::timeout(Duration::from_secs(3), stream.recv())
                .await
                .expect("revoked observer deadline"),
            Some(Err(_))
        ),
        "runtime drop cannot claim Completed"
    );
    assert!(stream.recv().await.is_none());
    peer.shutdown().await;
    std::fs::remove_dir_all(root).expect("fixture directory cleanup");
}

#[tokio::test]
async fn codex_prompt_stream_malformed_native_delta_fails_observation() {
    let mut fixture = CodexFixture::build().await;
    let mut stream = fixture
        .conversation
        .prompt_stream(PromptRequest::new("malformed delta").expect("prompt"))
        .await
        .expect("native streaming supported");
    let turn = fixture.peer.turn().await;
    fixture
        .peer
        .notify(
            "item/agentMessage/delta",
            json!({"threadId":"stream-thread","turnId":turn,"itemId":"answer","delta":4}),
        )
        .await;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), stream.recv())
            .await
            .expect("malformed event bound"),
        Some(Err(error)) if error.kind() == ErrorKind::Protocol
    ));
    assert!(stream.recv().await.is_none());
    // Malformed protocol revokes the live runtime; graceful close cannot claim
    // success after the connection-wide owning error.
    drop(fixture.conversation);
    drop(fixture.workspace);
    let _ = fixture.agent.shutdown().await;
    fixture.peer.shutdown().await;
    std::fs::remove_dir_all(fixture.root).expect("fixture directory cleanup");
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum NotificationReceiptCase {
    CancelledCommit,
    LaterPublication,
    ResurrectedIdentity,
    SequenceGap,
    RollbackFailure,
}

impl WaiterFaultStore {
    async fn interrupted_notification_acknowledgment(case: NotificationReceiptCase) {
        let inner = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let committed = Arc::new(AtomicUsize::new(0));
        let observe_next = Arc::new(AtomicBool::new(false));
        let restore_prefix = Arc::new(std::sync::Mutex::new(None));
        let sequence_gap = Arc::new(AtomicBool::new(false));
        let store = Self {
            inner,
            fault: WaiterFault::NotificationReceipt {
                observation: case != NotificationReceiptCase::CancelledCommit,
                cleanup_failure: case == NotificationReceiptCase::RollbackFailure,
                observe_next: Arc::clone(&observe_next),
                restore_prefix: Arc::clone(&restore_prefix),
                sequence_gap: Arc::clone(&sequence_gap),
                entered: Arc::clone(&entered),
                release: Arc::clone(&release),
                committed: Arc::clone(&committed),
            },
            armed: Arc::new(AtomicBool::new(false)),
            attempts: Arc::new(AtomicUsize::new(0)),
        };
        let peer = CodexPeer::start().await;
        let mut fixture = CodexFixture::build_with_store(peer, store.clone()).await;
        let mut receiver = fixture.conversation.notification_stream().unwrap();
        let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(10), async {
            let mut stream = fixture.conversation.prompt_stream(PromptRequest::new("notification receipt owner").unwrap()).await.unwrap();
            let turn = fixture.peer.turn().await;
            fixture.peer.notify("item/started", json!({
                "threadId":"stream-thread", "turnId":turn, "startedAtMs":1,
                "item":{"id":"receipt-command", "type":"commandExecution", "status":"inProgress",
                    "command":"printf receipt", "cwd":fixture.root,
                    "commandActions":[], "aggregatedOutput":null, "exitCode":null}
            })).await;
            let running = receiver.recv().await.unwrap().unwrap();
            assert!(matches!(running.kind(), ConversationNotificationKind::Process {state: ProcessState::Running, ..}));
            let key = StateKey::new("agent_sdk_conversation_notifications_v1", fixture.conversation.id().as_str()).unwrap();
            let prefix = store.inner.load(key).await.unwrap().unwrap().record.value["pending"][0].clone();
            store.armed.store(true, Ordering::SeqCst);
            if case == NotificationReceiptCase::RollbackFailure {
                let received = tokio::time::timeout(Duration::from_secs(1), receiver.recv()).await;
                store.armed.store(false, Ordering::SeqCst);
                fixture.peer.complete(&turn, "notification terminal").await;
                assert!(matches!(stream.recv().await, Some(Ok(PromptEvent::Completed(response))) if response.output() == "notification terminal"));
                assert!(stream.recv().await.is_none());
                assert_eq!(store.attempts.load(Ordering::SeqCst), 1, "a failed rollback cannot authorize another receipt write");
                let error = received.expect("receipt rollback failure must terminate").expect("one terminal failure").expect_err("receipt conflict and cleanup uncertainty must survive");
                assert_eq!(error.kind(), ErrorKind::Conflict);
                assert_eq!(error.detail(), Some(&ErrorDetail::TransactionConflict));
                assert_eq!(error.context().operation, Operation::NotificationStream);
                assert!(error.supplementary().iter().any(|cleanup| cleanup.context().phase == ErrorPhase::Cleanup && cleanup.detail() == Some(&ErrorDetail::ServiceUnavailable)));
                assert!(receiver.recv().await.is_none());
                return;
            }
            {
                let receive = receiver.recv();
                tokio::pin!(receive);
                tokio::select! {
                    item = &mut receive => panic!("only running process has been published: {item:?}"),
                    permit = entered.acquire() => permit.unwrap().forget(),
                }
                // Drop the observer after the real acknowledgment applied, either
                // before its commit reply or during the subsequent exact read.
            }
            assert_eq!(committed.load(Ordering::SeqCst), 1);
            release.add_permits(1);
            fixture.peer.notify("item/completed", json!({
                "threadId":"stream-thread", "turnId":turn, "completedAtMs":2,
                "item":{"id":"receipt-command", "type":"commandExecution", "status":"completed",
                    "command":"printf receipt", "cwd":fixture.root,
                    "commandActions":[], "aggregatedOutput":"receipt", "exitCode":0}
            })).await;
            fixture.peer.complete(&turn, "notification terminal").await;
            assert!(matches!(stream.recv().await, Some(Ok(PromptEvent::Completed(response))) if response.output() == "notification terminal"));
            assert!(stream.recv().await.is_none());
            if matches!(case, NotificationReceiptCase::ResurrectedIdentity | NotificationReceiptCase::SequenceGap) {
                if case == NotificationReceiptCase::ResurrectedIdentity {
                    *restore_prefix.lock().unwrap() = Some(prefix);
                } else {
                    sequence_gap.store(true, Ordering::SeqCst);
                }
                let error = receiver.recv().await.expect("contradiction must terminate once").expect_err("receipt observation must preserve exact identities and publication sequence");
                assert_eq!(error.kind(), ErrorKind::InvalidState);
                assert!(receiver.recv().await.is_none());
                return;
            }
            let exited = receiver.recv().await.expect("the committed later item must survive").expect("receipt reconciliation must not reject legitimate journal advancement");
            assert_ne!(exited.id(), running.id());
            assert!(matches!(exited.kind(), ConversationNotificationKind::Process {state: ProcessState::Exited {exit_code: Some(0)}, ..}));
            assert_eq!(committed.load(Ordering::SeqCst), 1);
        }).await;
        store.armed.store(false, Ordering::SeqCst);
        observe_next.store(false, Ordering::SeqCst);
        restore_prefix.lock().unwrap().take();
        sequence_gap.store(false, Ordering::SeqCst);
        release.add_permits(1);
        let CodexFixture {
            conversation,
            workspace,
            agent,
            mut peer,
            root,
        } = fixture;
        let mut cleanup = NativeCleanup::new(Duration::from_secs(4));
        cleanup.settle(conversation.close()).await;
        cleanup
            .settle(async {
                while let Some(item) = receiver.recv().await {
                    item?;
                }
                Ok::<(), agent_sdk::SdkError>(())
            })
            .await;
        cleanup.settle(workspace.close()).await;
        cleanup.settle(agent.shutdown()).await;
        cleanup
            .settle(async {
                if let Some(stop) = peer.stop.take() {
                    let _ = stop.send(());
                }
                if let Some(mut task) = peer.task.take() {
                    let joined = tokio::time::timeout(Duration::from_secs(3), &mut task).await;
                    if joined.is_err() {
                        task.abort();
                        let _ = task.await;
                    }
                    joined
                        .expect("receipt fixture joins")
                        .expect("receipt fixture remains healthy");
                }
                Ok::<(), agent_sdk::SdkError>(())
            })
            .await;
        cleanup
            .settle(async { std::fs::remove_dir_all(root) })
            .await;
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn codex_notification_ack_commit_survives_cancelled_observer() {
    WaiterFaultStore::interrupted_notification_acknowledgment(
        NotificationReceiptCase::CancelledCommit,
    )
    .await;
}

#[tokio::test]
async fn codex_notification_ack_observation_allows_later_publication() {
    WaiterFaultStore::interrupted_notification_acknowledgment(
        NotificationReceiptCase::LaterPublication,
    )
    .await;
}

#[tokio::test]
async fn codex_notification_ack_observation_rejects_resurrected_identity() {
    WaiterFaultStore::interrupted_notification_acknowledgment(
        NotificationReceiptCase::ResurrectedIdentity,
    )
    .await;
}

#[tokio::test]
async fn codex_notification_ack_observation_rejects_unaccounted_sequence_gap() {
    WaiterFaultStore::interrupted_notification_acknowledgment(NotificationReceiptCase::SequenceGap)
        .await;
}

#[tokio::test]
async fn codex_notification_ack_conflict_preserves_rollback_failure() {
    WaiterFaultStore::interrupted_notification_acknowledgment(
        NotificationReceiptCase::RollbackFailure,
    )
    .await;
}

impl CreationObservationCase {
    async fn assert_observation(self) {
        let raw = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("real Lance fixture store");
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new([
            tokio::sync::Semaphore::new(0),
            tokio::sync::Semaphore::new(0),
        ]);
        let committed = Arc::new(tokio::sync::Semaphore::new(0));
        let reconciled = Arc::new(tokio::sync::Semaphore::new(0));
        let conflicts = Arc::new(AtomicUsize::new(0));
        let attempts = Arc::new(AtomicUsize::new(0));
        let store = WaiterFaultStore {
            inner: raw,
            fault: WaiterFault::ThreadCreationObservation {
                case: self,
                reconciled: Arc::clone(&reconciled),
                entered: Arc::clone(&entered),
                release: Arc::clone(&release),
                committed: Arc::clone(&committed),
                conflicts: Arc::clone(&conflicts),
            },
            armed: Arc::new(AtomicBool::new(true)),
            attempts: Arc::clone(&attempts),
        };
        let (respond, response) = oneshot::channel();
        let mut respond = Some(respond);
        let peer = CodexPeer::start_with(FirstStart::ThreadObservation(response)).await;
        let thread_starts = Arc::clone(&peer.thread_starts);
        let config = AgentConfig::new(
            CodexConfig::tcp(peer.address, Duration::from_secs(3), Duration::from_secs(3))
                .unwrap()
                .into(),
            Duration::from_secs(3),
            NonZeroUsize::MIN,
        )
        .unwrap();
        let agent = AgentBuilder::create(config, AgentStorage::new(store))
            .build()
            .await
            .unwrap();
        let cwd = std::env::current_dir().unwrap();
        let context = WorkspaceContext::new(
            cwd.clone(),
            vec![WorkspaceRoot::new(cwd, WorkspaceRootAccess::ReadWrite).unwrap()],
            WorkspaceExecutionPolicy::new(
                WorkspaceSandboxMode::FullAccess,
                WorkspaceNetworkAccess::Enabled,
            ),
        )
        .unwrap();
        let workspace = agent
            .new_workspace(
                NewWorkspace::new("creation conflict", context, WorkspaceDefaults::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let mut conversation = None;
        let observed = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(10),
            async {
                let (creation, ()) = tokio::join!(
                    workspace
                        .new_conversation(NewConversation::new(None, None, Vec::new()).unwrap()),
                    async {
                        // The notification alone can stage until the original native
                        // response is released. Thus the first writer is the event.
                        entered.acquire().await.unwrap().forget();
                        respond
                            .take()
                            .unwrap()
                            .send(())
                            .expect("original thread/start response");
                        entered.acquire().await.unwrap().forget();
                        let winner = usize::from(self == Self::ForegroundWins);
                        release[winner].add_permits(1);
                        committed.acquire().await.unwrap().forget();
                        release[1 - winner].add_permits(1);
                    },
                );
                assert_eq!(
                    attempts.load(Ordering::SeqCst),
                    2,
                    "both original-snapshot observation writes staged"
                );
                assert_eq!(
                    conflicts.load(Ordering::SeqCst),
                    1,
                    "the foreground receives the original real Lance commit conflict"
                );
                assert_eq!(
                    thread_starts.load(Ordering::SeqCst),
                    1,
                    "creation must never replay thread/start"
                );
                if !matches!(self, Self::EventWins | Self::ForegroundWins) {
                    let error = match creation {
                        Ok(created) => {
                            conversation = Some(created);
                            panic!(
                                "mismatched or unreadable obligation must not reconcile: {self:?}"
                            );
                        }
                        Err(error) => error,
                    };
                    assert_eq!(error.context().operation, Operation::NewConversation);
                    if self == Self::ReadbackUnavailable {
                        assert_eq!(error.detail(), Some(&ErrorDetail::TransactionConflict));
                        assert_eq!(error.context().phase, ErrorPhase::StateCommit);
                        assert!(
                            error
                                .supplementary()
                                .iter()
                                .any(|failure| failure.context().phase == ErrorPhase::StateRead
                                    && failure.detail() == Some(&ErrorDetail::ServiceUnavailable))
                        );
                    } else if matches!(self, Self::DifferentThread | Self::ChangedObligation) {
                        // Invalidation is the outer publication failure. Its
                        // original persistence failure must still own cleanup.
                        assert_eq!(error.kind(), ErrorKind::Closed);
                        assert_eq!(error.context().phase, ErrorPhase::Publication);
                        assert_eq!(error.supplementary().len(), 1);
                        let primary = &error.supplementary()[0];
                        assert_eq!(primary.kind(), ErrorKind::Integration);
                        assert_eq!(primary.detail(), Some(&ErrorDetail::TransactionConflict));
                        assert_eq!(primary.context().phase, ErrorPhase::StateCommit);
                        assert_eq!(primary.supplementary().len(), 1);
                        let cleanup = &primary.supplementary()[0];
                        assert_eq!(cleanup.kind(), ErrorKind::Conflict);
                        assert_eq!(cleanup.detail(), None);
                        assert_eq!(cleanup.context().phase, ErrorPhase::Cleanup);
                    } else {
                        assert_eq!(error.kind(), ErrorKind::Integration);
                        assert_eq!(error.detail(), Some(&ErrorDetail::TransactionConflict));
                        assert_eq!(error.context().phase, ErrorPhase::StateCommit);
                        assert!(error.supplementary().is_empty());
                    }
                    return;
                }
                let created = conversation.insert(creation.expect(
                    "one matching native creation must reconcile its observation commit conflict",
                ));
                assert_eq!(workspace.list_conversations().await.unwrap().len(), 1);
                assert_eq!(
                    workspace
                        .conversation_info(created.id())
                        .await
                        .unwrap()
                        .id(),
                    &created.id()
                );
            },
        )
        .await;
        // Settle admitted work even when a witness or the expected success fails.
        if let Some(respond) = respond {
            let _ = respond.send(());
        }
        release[0].add_permits(1);
        release[1].add_permits(1);
        reconciled.add_permits(1);
        let mut cleanup = NativeCleanup::new(Duration::from_secs(10));
        if let Some(conversation) = conversation {
            cleanup.settle(conversation.close()).await;
        }
        if matches!(self, Self::DifferentThread | Self::ChangedObligation) {
            cleanup
                .settle(async {
                    let error = workspace
                        .close()
                        .await
                        .expect_err("failed ownership cleanup invalidates this workspace");
                    assert_eq!(error.kind(), ErrorKind::Closed);
                    Ok::<(), std::convert::Infallible>(())
                })
                .await;
        } else {
            cleanup.settle(workspace.close()).await;
        }
        cleanup.settle(agent.shutdown()).await;
        cleanup
            .settle(async {
                peer.shutdown().await;
                Ok::<(), std::convert::Infallible>(())
            })
            .await;
        cleanup.finish(observed);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_creation_reconciles_same_thread_observation_commit_conflict() {
    CreationObservationCase::EventWins
        .assert_observation()
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_creation_reconciles_notification_after_foreground_commit() {
    CreationObservationCase::ForegroundWins
        .assert_observation()
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_creation_rejects_different_thread_after_commit_conflict() {
    CreationObservationCase::DifferentThread
        .assert_observation()
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_creation_rejects_changed_obligation_after_commit_conflict() {
    CreationObservationCase::ChangedObligation
        .assert_observation()
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_creation_rejects_unchanged_obligation_after_commit_conflict() {
    CreationObservationCase::UnchangedObligation
        .assert_observation()
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_creation_preserves_conflict_when_readback_fails() {
    CreationObservationCase::ReadbackUnavailable
        .assert_observation()
        .await;
}

impl WaiterFaultStore {
    async fn assert_creation_failure(
        point: StorageFaultPoint,
        source: StorageFaultSource,
        cleanup_failure: bool,
    ) {
        let peer = CodexPeer::start().await;
        let starts = Arc::clone(&peer.thread_starts);
        let rollbacks = Arc::new(AtomicUsize::new(0));
        let attempts = Arc::new(AtomicUsize::new(0));
        let store = Self {
            inner: StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
                .await
                .unwrap(),
            fault: WaiterFault::CreationFailure {
                point,
                source,
                cleanup_failure,
                native_starts: Arc::clone(&starts),
                rollbacks: Arc::clone(&rollbacks),
            },
            armed: Arc::new(AtomicBool::new(true)),
            attempts: Arc::clone(&attempts),
        };
        let config = AgentConfig::new(
            CodexConfig::tcp(peer.address, Duration::from_secs(3), Duration::from_secs(3))
                .unwrap()
                .into(),
            Duration::from_secs(3),
            NonZeroUsize::MIN,
        )
        .unwrap();
        let agent = AgentBuilder::create(config, AgentStorage::new(store))
            .build()
            .await
            .unwrap();
        let cwd = std::env::current_dir().unwrap();
        let context = WorkspaceContext::new(
            cwd.clone(),
            vec![WorkspaceRoot::new(cwd, WorkspaceRootAccess::ReadWrite).unwrap()],
            WorkspaceExecutionPolicy::new(
                WorkspaceSandboxMode::FullAccess,
                WorkspaceNetworkAccess::Enabled,
            ),
        )
        .unwrap();
        let workspace = agent
            .new_workspace(
                NewWorkspace::new("creation load", context, WorkspaceDefaults::empty()).unwrap(),
            )
            .await
            .unwrap();
        let observed = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(10),
            async {
                let error = match workspace
                    .new_conversation(NewConversation::new(None, None, Vec::new()).unwrap())
                    .await
                {
                    Ok(conversation) => {
                        conversation.close().await.unwrap();
                        panic!("injected observation failure must not succeed");
                    }
                    Err(error) => error,
                };
                assert_eq!(
                    starts.load(Ordering::SeqCst),
                    1,
                    "never replay native creation"
                );
                assert_eq!(attempts.load(Ordering::SeqCst), 1);
                assert_eq!(
                    rollbacks.load(Ordering::SeqCst),
                    usize::from(point == StorageFaultPoint::Load),
                    "failed read must settle its transaction"
                );
                assert_eq!(error.context().operation, Operation::NewConversation);
                assert_eq!(error.context().phase, point.phase());
                match source {
                    StorageFaultSource::Unavailable => {
                        assert_eq!(error.detail(), Some(&ErrorDetail::ServiceUnavailable))
                    }
                    StorageFaultSource::TimedOut => {
                        assert_eq!(error.kind(), ErrorKind::Timeout);
                        assert!(matches!(
                            error.detail(),
                            Some(ErrorDetail::DeadlineElapsed { .. })
                        ));
                    }
                    StorageFaultSource::Conflict => {
                        assert_eq!(error.detail(), Some(&ErrorDetail::TransactionConflict))
                    }
                }
                if cleanup_failure {
                    assert!(
                        error
                            .supplementary()
                            .iter()
                            .any(|failure| failure.context().phase == ErrorPhase::Cleanup
                                && failure.detail() == Some(&ErrorDetail::ServiceUnavailable))
                    );
                }
            },
        )
        .await;
        let mut cleanup = NativeCleanup::new(Duration::from_secs(10));
        cleanup.settle(workspace.close()).await;
        cleanup.settle(agent.shutdown()).await;
        cleanup
            .settle(async {
                peer.shutdown().await;
                Ok::<(), std::convert::Infallible>(())
            })
            .await;
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn codex_creation_preserves_unavailable_load_and_settles_transaction() {
    WaiterFaultStore::assert_creation_failure(
        StorageFaultPoint::Load,
        StorageFaultSource::Unavailable,
        false,
    )
    .await;
}

#[tokio::test]
async fn codex_creation_preserves_timeout_load_and_settles_transaction() {
    WaiterFaultStore::assert_creation_failure(
        StorageFaultPoint::Load,
        StorageFaultSource::TimedOut,
        false,
    )
    .await;
}

#[tokio::test]
async fn codex_creation_preserves_load_conflict_and_rollback_failure() {
    WaiterFaultStore::assert_creation_failure(
        StorageFaultPoint::Load,
        StorageFaultSource::Conflict,
        true,
    )
    .await;
}

#[tokio::test]
async fn codex_creation_preserves_nonconflict_commit_failure_without_replay() {
    WaiterFaultStore::assert_creation_failure(
        StorageFaultPoint::Commit,
        StorageFaultSource::Unavailable,
        false,
    )
    .await;
}

impl CodexPeer {
    async fn settle_dropped(self, root: PathBuf, cleanup: &mut NativeCleanup, baseline: usize) {
        cleanup
            .settle(async {
                tokio::time::timeout(Duration::from_secs(3), async {
                    while tokio::runtime::Handle::current()
                        .metrics()
                        .num_alive_tasks()
                        > baseline
                    {
                        tokio::task::yield_now().await;
                    }
                })
                .await
            })
            .await;
        // Keep the peer outside the bounded SDK observation. Even a failed SDK
        // join observation must continue to the peer's owned stop/abort/join.
        let peer_cleanup = std::panic::AssertUnwindSafe(self.shutdown())
            .catch_unwind()
            .await;
        cleanup
            .settle(async {
                if let Err(panic) = peer_cleanup {
                    std::panic::resume_unwind(panic);
                }
                Ok::<(), std::io::Error>(())
            })
            .await;
        cleanup
            .settle(async { std::fs::remove_dir_all(root) })
            .await;
    }
}

#[derive(Clone, Copy, Debug)]
enum HistoricalTerminalCase {
    SeveralSettled,
    NextActive,
    ChangedOwner,
    DifferentOutcome,
    MatchingCause,
    DifferentCause,
    MissingHistory,
    DuplicateTurn,
    MissingClient,
    DuplicateClient,
    IncompleteHistory,
    IncompleteOther,
    ForeignHistory,
    ReadFailure,
    RollbackFailure,
    OrphanContinuation,
    MissingActive,
}

impl HistoricalTerminalCase {
    fn accepted(self) -> bool {
        matches!(
            self,
            Self::SeveralSettled | Self::NextActive | Self::ChangedOwner | Self::MatchingCause
        )
    }

    async fn run(self) {
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let peer = CodexPeer::start_with(FirstStart::DeferredInterruptTerminal).await;
        let baseline = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        let controlled = WaiterFaultStore {
            inner: store.clone(),
            fault: WaiterFault::RollbackFailure,
            armed: Arc::new(AtomicBool::new(false)),
            attempts: Arc::new(AtomicUsize::new(0)),
        };
        let mut fixture = CodexFixture::build_with_store(peer, controlled.clone()).await;
        let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(20), async {
            let count = if matches!(self, Self::SeveralSettled) { 3 } else { 1 };
            let mut previous = Vec::new();
            let mut orphan = None;
            let mut notifications = fixture.conversation.notification_stream().unwrap();
            for index in 0..count {
                let mut stream = fixture.conversation.prompt_stream(PromptRequest::new(format!("generation {index}")).unwrap()).await.unwrap();
                let turn = fixture.peer.turn().await;
                fixture.peer.notify("item/agentMessage/delta", json!({"threadId":"stream-thread","turnId":turn,"itemId":"answer","delta":"accepted"})).await;
                assert!(matches!(stream.recv().await, Some(Ok(PromptEvent::OutputDelta { text })) if text == "accepted"));
                if matches!(self, Self::OrphanContinuation) {
                    fixture.peer.request(92, "item/commandExecution/requestApproval", json!({
                        "threadId":"stream-thread", "turnId":turn, "itemId":"command",
                        "startedAtMs":1, "command":"printf approved",
                        "availableDecisions":["accept", "cancel"]
                    })).await;
                    let notification = notifications.recv().await.unwrap().unwrap();
                    assert!(matches!(notification.kind(), ConversationNotificationKind::Interaction { .. }));
                    let mut page = store.list(StateListRequest::new("agent_sdk_codex_continuations_v1").unwrap()).await.unwrap();
                    assert_eq!(page.records.len(), 1, "save the real outstanding native continuation");
                    let record = page.records.pop().unwrap().record;
                    assert_eq!(record.value["turn_id"], turn);
                    orphan = Some(record);
                }
                fixture.conversation.abort().await.expect("exact history reconciles interruption");
                assert!(matches!(stream.recv().await, Some(Err(error)) if error.kind() == ErrorKind::Cancelled));
                assert!(stream.recv().await.is_none());
                previous.push(turn);
            }
            if let Some(orphan) = orphan {
                assert!(store.load(orphan.key.clone()).await.unwrap().is_none(), "owned terminal cleanup removed the actual continuation");
                let mut transaction = store.begin_transaction().await.unwrap();
                transaction.put(orphan).await.unwrap();
                transaction.commit().await.unwrap();
            }
            drop(notifications);
            let mut current = None;
            if matches!(self, Self::NextActive | Self::MissingActive) {
                let mut stream = fixture.conversation.prompt_stream(PromptRequest::new("still active generation").unwrap()).await.unwrap();
                let turn = fixture.peer.turn().await;
                fixture.peer.notify("item/agentMessage/delta", json!({"threadId":"stream-thread","turnId":turn,"itemId":"active","delta":"current accepted"})).await;
                assert!(matches!(stream.recv().await, Some(Ok(PromptEvent::OutputDelta { text })) if text == "current accepted"));
                if matches!(self, Self::MissingActive) {
                    let key = StateKey::new("agent_sdk_codex_prompts_v1", fixture.conversation.id().as_str()).unwrap();
                    let mut transaction = store.begin_transaction().await.unwrap();
                    assert!(transaction.load(key.clone()).await.unwrap().is_some(), "corruption removes an actual live generation");
                    transaction.delete(key).await.unwrap();
                    transaction.commit().await.unwrap();
                    // Native terminal evidence is complete and valid; the SDK
                    // owner has neither settled nor begun clearing this row.
                    previous = vec![turn.clone()];
                }
                current = Some((stream, turn));
            }
            let mut history = fixture.peer.history.lock().unwrap().clone();
            match self {
                Self::MatchingCause | Self::DifferentCause => history["turns"][0]["error"] = json!({"message":"private-history", "codexErrorInfo":"rateLimitExceeded"}),
                Self::MissingHistory => history["turns"] = json!([]),
                Self::DuplicateTurn => { let turn = history["turns"][0].clone(); history["turns"].as_array_mut().unwrap().push(turn); },
                Self::MissingClient => { history["turns"][0]["items"][0].as_object_mut().unwrap().remove("clientId"); },
                Self::DuplicateClient => { let user = history["turns"][0]["items"][0].clone(); history["turns"][0]["items"].as_array_mut().unwrap().push(user); },
                Self::IncompleteHistory => history["turns"][0]["itemsView"] = json!("summary"),
                Self::IncompleteOther => history["turns"].as_array_mut().unwrap().push(json!({"id":"other-terminal","status":"interrupted","itemsView":"summary","error":null,"items":[]})),
                Self::ForeignHistory => history["id"] = json!("different-thread"),
                Self::ReadFailure => fixture.peer.reject("thread/read", -32603, "test history unavailable"),
                Self::RollbackFailure => controlled.armed.store(true, Ordering::SeqCst),
                Self::MissingActive => history["turns"][1]["status"] = json!("interrupted"),
                Self::SeveralSettled | Self::NextActive | Self::ChangedOwner | Self::DifferentOutcome | Self::OrphanContinuation => {},
            }
            if matches!(self, Self::MatchingCause | Self::DifferentCause | Self::MissingHistory | Self::DuplicateTurn | Self::MissingClient | Self::DuplicateClient | Self::IncompleteHistory | Self::IncompleteOther | Self::ForeignHistory | Self::MissingActive) {
                *fixture.peer.history_override.lock().unwrap() = Some(history);
            }
            let release_history = if matches!(self, Self::ChangedOwner) {
                let (release, held) = oneshot::channel();
                *fixture.peer.held_history.lock().unwrap() = Some(held);
                Some(release)
            } else { None };
            for turn in &previous {
                let status = if matches!(self, Self::DifferentOutcome) { "completed" } else { "interrupted" };
                let error = match self {
                    Self::MatchingCause => json!({"message":"private-live", "codexErrorInfo":"rateLimitExceeded"}),
                    Self::DifferentCause => json!({"message":"private-live", "codexErrorInfo":"flexUnavailable"}),
                    _ => Value::Null,
                };
                fixture.peer.notify("turn/completed", json!({"threadId":"stream-thread","turn":{"id":turn,"status":status,"items":[],"error":error}})).await;
            }
            if let Some(release_history) = release_history {
                fixture.peer.history_entered.notified().await;
                let mut prompt = Box::pin(fixture.conversation.prompt(PromptRequest::new("admit while historical read is held").unwrap()));
                let key = StateKey::new("agent_sdk_codex_prompts_v1", fixture.conversation.id().as_str()).unwrap();
                // Poll the real new prompt until its durable acceptance intent
                // exists. Its native request waits behind the held read response.
                loop {
                    tokio::select! {
                        result = &mut prompt => panic!("new prompt settled before dispatch release: {result:?}"),
                        snapshot = store.load(key.clone()) => {
                            if let Some(snapshot) = snapshot.unwrap()
                                && snapshot.record.value["state"]["state"] == "may_be_accepted" {
                                break;
                            }
                            tokio::task::yield_now().await;
                        }
                    }
                }
                release_history.send(()).expect("release original history observation");
                let turn = tokio::select! {
                    turn = fixture.peer.turn() => turn,
                    result = &mut prompt => panic!("new prompt lost to historical terminal: {result:?}"),
                };
                assert_eq!(turn, "turn-2");
                fixture.peer.complete(&turn, "changed owner exact result").await;
                let response = prompt.await.expect("new generation survives history owner change");
                assert_eq!(response.output(), "changed owner exact result");
            }
            if self.accepted() {
                fixture.peer.request(83, "currentTime/read", json!({"threadId":"stream-thread"})).await;
                let response = fixture.peer.responses.recv().await.expect("historical terminals keep event owner responsive");
                assert_eq!(response["id"], 83);
                assert!(response["result"]["currentTimeAt"].is_i64());
                if let Some((mut stream, turn)) = current.take() {
                    fixture.peer.complete(&turn, "current exact answer").await;
                    assert!(matches!(stream.recv().await, Some(Ok(PromptEvent::Completed(answer))) if answer.output() == "current exact answer"));
                    assert!(stream.recv().await.is_none());
                } else if !matches!(self, Self::ChangedOwner) {
                    let mut stream = fixture.conversation.prompt_stream(PromptRequest::new("after historical terminals").unwrap()).await.unwrap();
                    let turn = fixture.peer.turn().await;
                    assert_eq!(turn, format!("turn-{}", count + 1));
                    fixture.peer.complete(&turn, "subsequent exact answer").await;
                    assert!(matches!(stream.recv().await, Some(Ok(PromptEvent::Completed(answer))) if answer.output() == "subsequent exact answer"));
                    assert!(stream.recv().await.is_none());
                }
                assert_eq!(fixture.peer.interrupts.load(Ordering::SeqCst), count);
                assert_eq!(fixture.peer.thread_starts.load(Ordering::SeqCst), 1);
            } else {
                tokio::time::timeout(Duration::from_secs(3), async {
                    loop {
                        match fixture.agent.info().await {
                            Err(error) if error.kind() == ErrorKind::Closed => break,
                            Err(error) => panic!("unexpected hierarchy observation: {error:?}"),
                            Ok(_) => tokio::task::yield_now().await,
                        }
                    }
                }).await.expect("historical event invalidates hierarchy before shutdown");
                assert_eq!(fixture.peer.thread_starts.load(Ordering::SeqCst), 1);
                drop(current.take());
            }
        }).await;
        let mut cleanup = NativeCleanup::new(Duration::from_secs(10));
        if self.accepted() {
            cleanup.settle(fixture.conversation.close()).await;
            cleanup.settle(fixture.workspace.close()).await;
            cleanup.settle(fixture.agent.shutdown()).await;
        } else {
            cleanup
                .settle(async {
                    let error = fixture
                        .agent
                        .shutdown()
                        .await
                        .expect_err("contradictory historical terminal must fail closed");
                    assert!(
                        matches!(
                            error.kind(),
                            ErrorKind::InvalidState | ErrorKind::Protocol | ErrorKind::Integration
                        ),
                        "{self:?}: {error:?}"
                    );
                    assert_eq!(error.context().operation, Operation::Shutdown);
                    if matches!(self, Self::OrphanContinuation) {
                        assert_eq!(error.kind(), ErrorKind::InvalidState);
                        assert_eq!(
                            error.message(),
                            "historical prompt still owns an interaction continuation"
                        );
                    }
                    if matches!(self, Self::ReadFailure) {
                        assert_eq!(error.kind(), ErrorKind::Integration);
                        assert_eq!(error.detail(), Some(&ErrorDetail::RemoteInternal));
                        assert_eq!(error.context().phase, ErrorPhase::ResponseRead);
                    }
                    if matches!(self, Self::RollbackFailure) {
                        assert_eq!(error.kind(), ErrorKind::Integration);
                        assert_eq!(error.detail(), Some(&ErrorDetail::ServiceUnavailable));
                        assert_eq!(error.context().phase, ErrorPhase::Cleanup);
                        assert_eq!(controlled.attempts.load(Ordering::SeqCst), 1);
                    }
                    Ok::<(), std::io::Error>(())
                })
                .await;
            drop(fixture.conversation);
            drop(fixture.workspace);
        }
        fixture
            .peer
            .settle_dropped(fixture.root, &mut cleanup, baseline)
            .await;
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn codex_historical_terminals_survive_three_compacted_generations() {
    HistoricalTerminalCase::SeveralSettled.run().await;
}
#[tokio::test]
async fn codex_historical_terminal_preserves_next_active_generation() {
    HistoricalTerminalCase::NextActive.run().await;
}
#[tokio::test]
async fn codex_historical_terminal_revalidates_owner_changed_during_native_read() {
    HistoricalTerminalCase::ChangedOwner.run().await;
}
#[tokio::test]
async fn codex_historical_terminal_rejects_changed_outcome() {
    HistoricalTerminalCase::DifferentOutcome.run().await;
}
#[tokio::test]
async fn codex_historical_terminal_rejects_missing_native_turn() {
    HistoricalTerminalCase::MissingHistory.run().await;
}
#[tokio::test]
async fn codex_historical_terminal_rejects_duplicate_native_turn() {
    HistoricalTerminalCase::DuplicateTurn.run().await;
}
#[tokio::test]
async fn codex_historical_terminal_rejects_missing_native_generation() {
    HistoricalTerminalCase::MissingClient.run().await;
}
#[tokio::test]
async fn codex_historical_terminal_rejects_duplicate_native_generation() {
    HistoricalTerminalCase::DuplicateClient.run().await;
}
#[tokio::test]
async fn codex_historical_terminal_rejects_incomplete_native_history() {
    HistoricalTerminalCase::IncompleteHistory.run().await;
}
#[tokio::test]
async fn codex_historical_terminal_rejects_incomplete_other_native_turn() {
    HistoricalTerminalCase::IncompleteOther.run().await;
}
#[tokio::test]
async fn codex_historical_terminal_rejects_foreign_native_history() {
    HistoricalTerminalCase::ForeignHistory.run().await;
}
#[tokio::test]
async fn codex_historical_terminal_preserves_native_read_failure() {
    HistoricalTerminalCase::ReadFailure.run().await;
}
#[tokio::test]
async fn codex_historical_terminal_does_not_mask_missing_active_prompt() {
    HistoricalTerminalCase::MissingActive.run().await;
}

impl WaiterFaultStore {
    async fn assert_late_terminal_during_owned_clear(observation_error: bool) {
        let raw = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let store = Self {
            inner: raw.clone(),
            fault: WaiterFault::HeldClear {
                entered: Arc::clone(&entered),
                release: Arc::clone(&release),
                observation_error,
            },
            armed: Arc::new(AtomicBool::new(false)),
            attempts: Arc::new(AtomicUsize::new(0)),
        };
        let peer = CodexPeer::start_with(FirstStart::DeferredInterruptTerminal).await;
        let baseline = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        let mut fixture = CodexFixture::build_with_store(peer, store.clone()).await;
        let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(15), async {
            let mut stream = fixture.conversation.prompt_stream(PromptRequest::new("clear ownership").unwrap()).await.unwrap();
            let turn = fixture.peer.turn().await;
            fixture.peer.notify("item/agentMessage/delta", json!({"threadId":"stream-thread","turnId":turn,"itemId":"answer","delta":"accepted"})).await;
            assert!(matches!(stream.recv().await, Some(Ok(PromptEvent::OutputDelta { text })) if text == "accepted"));
            store.armed.store(true, Ordering::SeqCst);
            let mut abort = Box::pin(fixture.conversation.abort());
            tokio::select! {
                result = &mut abort => result.expect("abort receipt"),
                permit = entered.acquire() => { permit.unwrap().forget(); },
            }
            // The wrapped transaction has performed the real Lance delete and
            // retains the consuming commit observer. Native fact handling must
            // wait for this exact cleanup owner, not mistake absence for loss.
            let key = StateKey::new("agent_sdk_codex_prompts_v1", fixture.conversation.id().as_str()).unwrap();
            while store.attempts.load(Ordering::SeqCst) == 0 { tokio::task::yield_now().await; }
            assert!(raw.load(key).await.unwrap().is_none());
            let (release_history, held) = oneshot::channel();
            *fixture.peer.held_history.lock().unwrap() = Some(held);
            fixture.peer.notify("turn/completed", json!({"threadId":"stream-thread","turn":{"id":turn,"status":"interrupted","items":[],"error":null}})).await;
            fixture.peer.history_entered.notified().await;
            fixture.peer.request(91, "currentTime/read", json!({"threadId":"stream-thread"})).await;
            release_history.send(()).unwrap();
            assert!(tokio::time::timeout(Duration::from_millis(50), fixture.peer.responses.recv()).await.is_err(), "event owner must retain the clearing boundary");
            release.add_permits(1);
            // The original abort observer may already have completed; dropping
            // its observation never revokes accepted work or repolls a result.
            drop(abort);
            assert!(matches!(stream.recv().await, Some(Err(error)) if error.kind() == ErrorKind::Cancelled));
            assert!(stream.recv().await.is_none());
            let response = fixture.peer.responses.recv().await.expect("terminal owner resumes after cleanup");
            assert_eq!(response["id"], 91);
            assert!(response["result"]["currentTimeAt"].is_i64());
            assert_eq!(store.attempts.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.peer.interrupts.load(Ordering::SeqCst), 1);
            fixture.finish_next_prompt().await;
        }).await;
        release.add_permits(1);
        store.armed.store(false, Ordering::SeqCst);
        let mut cleanup = NativeCleanup::new(Duration::from_secs(10));
        cleanup.settle(fixture.conversation.close()).await;
        cleanup.settle(fixture.workspace.close()).await;
        cleanup.settle(fixture.agent.shutdown()).await;
        fixture
            .peer
            .settle_dropped(fixture.root, &mut cleanup, baseline)
            .await;
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn codex_historical_terminal_waits_for_owned_clear_commit() {
    WaiterFaultStore::assert_late_terminal_during_owned_clear(false).await;
}
#[tokio::test]
async fn codex_historical_terminal_waits_for_uncertain_clear_reconciliation() {
    WaiterFaultStore::assert_late_terminal_during_owned_clear(true).await;
}

#[tokio::test]
async fn codex_historical_terminal_rollback_failure_vetoes_reconciliation() {
    HistoricalTerminalCase::RollbackFailure.run().await;
}

#[tokio::test]
async fn codex_historical_terminal_rejects_orphaned_matching_continuation() {
    HistoricalTerminalCase::OrphanContinuation.run().await;
}

#[tokio::test]
async fn codex_selected_release_initialization_rejects_old_and_malformed_identity() {
    for (identity, expected) in [
        (json!("codex/0.162.1"), ErrorKind::InvalidConfig),
        (json!("codex/0.144.1"), ErrorKind::InvalidConfig),
        (Value::Null, ErrorKind::Protocol),
        (json!("invalid"), ErrorKind::Protocol),
    ] {
        let peer = CodexPeer::start().await;
        *peer.identity.lock().unwrap() = identity;
        let config = AgentConfig::new(
            CodexConfig::tcp(peer.address, Duration::from_secs(3), Duration::from_secs(3))
                .unwrap()
                .into(),
            Duration::from_secs(3),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let result = AgentBuilder::create(config, AgentStorage::new(store))
            .build()
            .await;
        let error = match result {
            Err(error) => Some(error),
            Ok(agent) => {
                agent.shutdown().await.unwrap();
                None
            }
        };
        let starts = peer.thread_starts.load(Ordering::SeqCst);
        peer.shutdown().await;
        assert_eq!(starts, 0);
        assert_eq!(
            error.expect("invalid runtime identity must fail").kind(),
            expected
        );
    }
}

#[tokio::test]
async fn codex_interrupted_error_preserves_cancellation_and_redacted_cause() {
    for info in [
        Value::Null,
        json!("rateLimitExceeded"),
        json!("misalignmentPolicyViolation"),
    ] {
        let mut fixture = CodexFixture::build().await;
        let mut stream = fixture
            .conversation
            .prompt_stream(PromptRequest::new("interrupted").unwrap())
            .await
            .unwrap();
        let turn = fixture.peer.turn().await;
        let error = if info.is_null() {
            Value::Null
        } else {
            json!({"message":"secret-token", "codexErrorInfo":info, "misalignment":{
                "errorType":"private", "detailedExplanation":"secret-token", "steer":{"message":"secret-token"}, "reviewTarget":"secret-token"
            }})
        };
        fixture
            .peer
            .notify(
                "turn/completed",
                json!({"threadId":"stream-thread","turn":{
                    "id":turn,"status":"interrupted","items":[],"error":error
                }}),
            )
            .await;
        let result = tokio::time::timeout(Duration::from_secs(3), stream.recv()).await;
        fixture.shutdown().await;
        let error = result
            .unwrap()
            .unwrap()
            .expect_err("interruption cannot complete");
        assert_eq!(error.kind(), ErrorKind::Cancelled);
        assert_eq!(error.context().phase, ErrorPhase::Cancellation);
        if info.is_null() {
            assert!(error.cause().is_none());
        } else {
            let cause = error
                .cause()
                .expect("interruption retains normalized native cause");
            assert_eq!(cause.kind(), ErrorKind::Integration);
            assert_eq!(
                cause.detail(),
                Some(&if info == "rateLimitExceeded" {
                    ErrorDetail::RateLimited
                } else {
                    ErrorDetail::PolicyRejected
                })
            );
        }
        assert!(!format!("{error:?} {error}").contains("secret-token"));
    }
}

#[tokio::test]
async fn codex_input_blocking_policy_uses_selected_field() {
    for (blocking, timeout, expected) in [
        (None, Value::Null, None),
        (Some(Value::Null), json!(100), None),
        (Some(json!(true)), json!(100), None),
        (Some(json!(false)), Value::Null, Some(-32601)),
        (Some(json!(false)), json!(100), Some(-32601)),
        (Some(json!("false")), Value::Null, Some(-32602)),
        (Some(json!(true)), json!("100"), Some(-32602)),
    ] {
        let mut fixture = CodexFixture::build().await;
        let observed = std::panic::AssertUnwindSafe(async {
            let mut notifications = fixture.conversation.notification_stream().unwrap();
            let mut stream = fixture.conversation.prompt_stream(PromptRequest::new("input").unwrap()).await.unwrap();
            let turn = fixture.peer.turn().await;
            let mut params = json!({"threadId":"stream-thread","turnId":turn,"itemId":"input",
                "autoResolutionMs":timeout,"questions":[{"id":"choice","header":"Choice","question":"First line\nSecond line","isOther":false,"isSecret":false,"options":[{"label":"First\nOption","description":"description"}]}]});
            if let Some(blocking) = blocking { params["isBlocking"] = blocking; }
            fixture.peer.events.send(json!({"id":"input-callback","method":"item/tool/requestUserInput","params":params})).await.unwrap();
            let response = tokio::time::timeout(Duration::from_secs(3), async {
                tokio::select! {
                    response = fixture.peer.responses.recv() => response.unwrap(),
                    notification = notifications.recv() => {
                        let notification = notification.unwrap().unwrap();
                        assert!(expected.is_none(), "unsupported input published an actionable notification");
                        let ConversationNotificationKind::Interaction { body, response_schema, .. } = notification.kind() else { panic!("expected input"); };
                        assert!(body.contains("First line\nSecond line"));
                        assert_eq!(response_schema.as_ref().unwrap()["properties"]["answer"]["enum"], json!(["First\nOption"]));
                        let resolution = fixture.conversation.resolve_notification(notification.id().clone(), NotificationResolution::Submit { content: json!({"answer":"First\nOption"}) }).await;
                        assert!(matches!(resolution, Err(error) if error.kind() == ErrorKind::Conflict));
                        fixture.peer.responses.recv().await.unwrap()
                    }
                }
            }).await.unwrap();
            assert_eq!(response["id"], "input-callback");
            if let Some(code) = expected {
                assert_eq!(response["error"]["code"], code);
                assert!(tokio::time::timeout(Duration::from_millis(20), notifications.recv()).await.is_err());
            } else {
                assert_eq!(response["result"], json!({"answers":{"choice":{"answers":["First\nOption"]}}}));
            }
            fixture.peer.complete(&turn, "input settled").await;
            assert!(matches!(tokio::time::timeout(Duration::from_secs(3), stream.recv()).await.unwrap(), Some(Ok(PromptEvent::Completed(_)))));
        }).catch_unwind().await;
        let cleanup = fixture.settle().await;
        if let Err(panic) = observed {
            std::panic::resume_unwind(panic);
        }
        cleanup.unwrap();
    }
}

#[tokio::test]
async fn codex_stdin_approval_preserves_callback_and_one_shot_decisions() {
    for (kind, title, resolution, decision) in [
        (
            None,
            Some("Command approval"),
            NotificationResolution::Allow,
            "accept",
        ),
        (
            Some(json!("command")),
            Some("Command approval"),
            NotificationResolution::Allow,
            "accept",
        ),
        (
            Some(json!("writeStdin")),
            Some("Terminal input approval"),
            NotificationResolution::Allow,
            "accept",
        ),
        (
            Some(json!("writeStdin")),
            Some("Terminal input approval"),
            NotificationResolution::Deny {
                reason: "Denied".to_owned(),
            },
            "cancel",
        ),
        (
            Some(json!("writeStdin")),
            Some("Terminal input approval"),
            NotificationResolution::Dismiss {
                reason: "Dismissed".to_owned(),
            },
            "cancel",
        ),
        (
            Some(json!("unknown")),
            None,
            NotificationResolution::Allow,
            "",
        ),
        (Some(Value::Null), None, NotificationResolution::Allow, ""),
        (Some(json!(1)), None, NotificationResolution::Allow, ""),
    ] {
        let mut fixture = CodexFixture::build().await;
        let observed = std::panic::AssertUnwindSafe(async {
            let mut notifications = fixture.conversation.notification_stream().unwrap();
            let mut stream = fixture.conversation.prompt_stream(PromptRequest::new("approval").unwrap()).await.unwrap();
            let turn = fixture.peer.turn().await;
            let mut params = json!({"threadId":"stream-thread","turnId":turn,"itemId":"terminal","approvalId":"callback","startedAtMs":1,"availableDecisions":["accept","cancel"]});
            if let Some(kind) = kind { params["kind"] = kind; }
            fixture.peer.request(71, "item/commandExecution/requestApproval", params).await;
            let response = tokio::time::timeout(Duration::from_secs(3), async {
                tokio::select! {
                    response = fixture.peer.responses.recv() => response.unwrap(),
                    notification = notifications.recv() => {
                        let notification = notification.unwrap().unwrap();
                        let ConversationNotificationKind::Interaction { title: actual, .. } = notification.kind() else { panic!("expected approval"); };
                        assert_eq!(Some(actual.as_str()), title);
                        let result = fixture.conversation.resolve_notification(notification.id().clone(), resolution).await;
                        assert!(matches!(result, Err(error) if error.kind() == ErrorKind::Conflict));
                        fixture.peer.responses.recv().await.unwrap()
                    }
                }
            }).await.unwrap();
            assert_eq!(response["id"], 71);
            if title.is_some() { assert_eq!(response["result"], json!({"decision":decision})); }
            else { assert_eq!(response["error"]["code"], -32602); }
            fixture.peer.complete(&turn, "settled").await;
            assert!(matches!(tokio::time::timeout(Duration::from_secs(3), stream.recv()).await.unwrap(), Some(Ok(PromptEvent::Completed(_)))));
        }).catch_unwind().await;
        let cleanup = fixture.settle().await;
        if let Err(panic) = observed {
            std::panic::resume_unwind(panic);
        }
        cleanup.unwrap();
    }
}

#[tokio::test]
async fn codex_reasoning_overrides_reset_across_model_changes_and_reopen() {
    use agent_sdk::model::{ModelSelector, ReasoningEffort};

    let peer = CodexPeer::start().await;
    *peer.models.lock().unwrap() = json!({"data":[
        {"model":"selected-model","displayName":"Selected","hidden":false,
         "supportedReasoningEfforts":[{"reasoningEffort":"low"},{"reasoningEffort":"medium"},{"reasoningEffort":"high"}],
         "defaultReasoningEffort":"medium","supportsPersonality":false,"isDefault":true},
        {"model":"alternate-model","displayName":"Alternate","hidden":false,
         "supportedReasoningEfforts":[{"reasoningEffort":"low"},{"reasoningEffort":"high"},{"reasoningEffort":"ultra"}],
         "defaultReasoningEffort":"low","supportsPersonality":false,"isDefault":false}
    ],"nextCursor":null});
    let CodexFixture {
        mut peer,
        agent,
        workspace,
        conversation,
        root,
    } = CodexFixture::build_with_peer(peer).await;
    let workspace_id = workspace.id().clone();
    let conversation_id = conversation.id().clone();
    let mut workspace = Some(workspace);
    let mut conversation = Some(conversation);
    let observed = std::panic::AssertUnwindSafe(async {
        let selections = [
            (None, Some("high"), None, Some("high")),
            (None, None, None, Some("medium")),
            (Some("alternate-model"), Some("ultra"), Some("alternate-model"), Some("ultra")),
            (Some("alternate-model"), None, None, Some("low")),
            (None, None, Some("selected-model"), Some("medium")),
        ];
        for (model, effort, expected_model, expected_effort) in selections {
            let mut request = PromptRequest::new("select exact effort").unwrap();
            if let Some(model) = model {
                request = request.with_model(ModelSelector::new(model).unwrap());
            }
            if let Some(effort) = effort {
                request = request.with_reasoning_effort(ReasoningEffort::new(effort).unwrap());
            }
            let mut stream = conversation.as_ref().unwrap().prompt_stream(request).await.unwrap();
            let turn = peer.turn().await;
            {
                let requests = peer.requests.lock().unwrap();
                let params = &requests.iter().rev().find(|request| request["method"] == "turn/start").unwrap()["params"];
                assert_eq!(params.get("model").and_then(Value::as_str), expected_model);
                assert_eq!(params.get("effort").and_then(Value::as_str), expected_effort);
                assert!(params.get("personality").is_none());
            }
            peer.complete(&turn, "exact selection output").await;
            assert!(matches!(stream.recv().await, Some(Ok(PromptEvent::Completed(response))) if response.output() == "exact selection output"));
            assert!(stream.recv().await.is_none());
        }
        workspace.take().unwrap().close().await.unwrap();
        drop(conversation.take());
        let reopened = workspace.insert(agent.get_workspace(workspace_id).await.unwrap());
        conversation = Some(reopened.get_conversation(conversation_id).await.unwrap());
        let mut stream = conversation.as_ref().unwrap().prompt_stream(PromptRequest::new("reopened default").unwrap()).await.unwrap();
        let turn = peer.turn().await;
        {
            let requests = peer.requests.lock().unwrap();
            let params = &requests.iter().rev().find(|request| request["method"] == "turn/start").unwrap()["params"];
            assert!(params.get("model").is_none());
            assert!(params.get("effort").is_none());
        }
        peer.complete(&turn, "reopened default output").await;
        assert!(matches!(stream.recv().await, Some(Ok(PromptEvent::Completed(response))) if response.output() == "reopened default output"));
        assert_eq!(peer.thread_starts.load(Ordering::SeqCst), 1);
    }).catch_unwind().await;
    let mut cleanup = NativeCleanup::new(Duration::from_secs(10));
    if let Some(conversation) = conversation {
        cleanup.settle(conversation.close()).await;
    }
    if let Some(workspace) = workspace {
        cleanup.settle(workspace.close()).await;
    }
    cleanup.settle(agent.shutdown()).await;
    peer.shutdown().await;
    cleanup
        .settle(async { std::fs::remove_dir_all(root) })
        .await;
    cleanup.finish(observed.map(Ok));
}

#[tokio::test]
async fn codex_reasoning_rejects_unadvertised_effort_and_unsourced_reset() {
    use agent_sdk::model::{ModelSelector, ReasoningEffort};

    let mut fixture = CodexFixture::build().await;
    let observed = std::panic::AssertUnwindSafe(async {
        let error = fixture.conversation.prompt(
            PromptRequest::new("unadvertised effort").unwrap()
                .with_reasoning_effort(ReasoningEffort::new("ultra").unwrap()),
        ).await.expect_err("unadvertised effort must fail before native acceptance");
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(!fixture.peer.requests.lock().unwrap().iter().any(|request| request["method"] == "turn/start"));

        let mut stream = fixture.conversation.prompt_stream(
            PromptRequest::new("supported explicit selection").unwrap()
                .with_model(ModelSelector::new("selected-model").unwrap())
                .with_reasoning_effort(ReasoningEffort::medium()),
        ).await.unwrap();
        let turn = fixture.peer.turn().await;
        fixture.peer.complete(&turn, "accepted").await;
        assert!(matches!(stream.recv().await, Some(Ok(PromptEvent::Completed(response))) if response.output() == "accepted"));
        fixture.peer.models.lock().unwrap()["data"][0]["isDefault"] = json!(false);
        let error = fixture.conversation.prompt(PromptRequest::new("unsourced reset").unwrap()).await
            .expect_err("omission cannot preserve a sticky override without a sourced default");
        assert_eq!(error.kind(), ErrorKind::Unsupported);
        assert_eq!(fixture.peer.requests.lock().unwrap().iter().filter(|request| request["method"] == "turn/start").count(), 1);
    }).catch_unwind().await;
    let cleanup = fixture.settle().await;
    if let Err(panic) = observed {
        std::panic::resume_unwind(panic);
    }
    cleanup.unwrap();
}

#[tokio::test]
async fn codex_retired_personality_cannot_mutate_native_state() {
    let mut fixture = CodexFixture::build().await;
    let observed = std::panic::AssertUnwindSafe(async {
        let models = fixture.agent.list_models().await.unwrap();
        assert_eq!(models[0].selector().as_str(), "selected-model");
        assert!(
            models[0].personalities().is_empty(),
            "deprecated source flag cannot advertise a personality"
        );
        assert!(models.iter().all(|model| model.personalities().is_empty()));
        let workspace_info = fixture.workspace.info().await.unwrap();
        for name in ["none", "friendly", "pragmatic"] {
            let personality = agent_sdk::model::PersonalitySelector::new(name).unwrap();
            let before = fixture.peer.requests.lock().unwrap().len();
            let error = fixture
                .conversation
                .prompt(
                    PromptRequest::new("unsupported")
                        .unwrap()
                        .with_personality(personality.clone()),
                )
                .await
                .expect_err("retired personality");
            assert_eq!(error.kind(), ErrorKind::Unsupported);
            assert_eq!(
                fixture.peer.requests.lock().unwrap().len(),
                before,
                "personality must fail before native mutation"
            );
            assert!(
                fixture.peer.turns.try_recv().is_err(),
                "personality must fail before native turn"
            );
            let result = fixture
                .workspace
                .new_conversation(
                    NewConversation::new(
                        None,
                        Some(WorkspaceDefaults::empty().with_personality(personality.clone())),
                        vec![],
                    )
                    .unwrap(),
                )
                .await;
            let error = match result {
                Err(error) => error,
                Ok(conversation) => {
                    conversation.close().await.unwrap();
                    panic!("retired defaults were accepted");
                }
            };
            assert_eq!(error.kind(), ErrorKind::Unsupported);
            assert_eq!(fixture.peer.requests.lock().unwrap().len(), before);
            let defaults = WorkspaceDefaults::empty().with_personality(personality);
            let created = fixture
                .agent
                .new_workspace(
                    NewWorkspace::new(
                        "retired defaults",
                        workspace_info.context().clone(),
                        defaults.clone(),
                    )
                    .unwrap(),
                )
                .await;
            let error = match created {
                Err(error) => error,
                Ok(workspace) => {
                    workspace.close().await.unwrap();
                    panic!("unsupported workspace personality accepted")
                }
            };
            assert_eq!(error.kind(), ErrorKind::Unsupported);
            let update = UpdateWorkspaceRequest::new(
                workspace_info.id().clone(),
                workspace_info.revision(),
                "retired defaults",
                workspace_info.context().clone(),
                defaults,
            )
            .unwrap();
            assert!(
                matches!(fixture.agent.check_workspace_update(&update).await,
                Err(error) if error.kind() == ErrorKind::Unsupported)
            );
            assert!(matches!(fixture.agent.update_workspace(update).await,
                Err(error) if error.kind() == ErrorKind::Unsupported));
            assert_eq!(
                fixture.workspace.info().await.unwrap().revision(),
                workspace_info.revision()
            );
            assert_eq!(fixture.peer.requests.lock().unwrap().len(), before);
        }
    })
    .catch_unwind()
    .await;
    fixture.finish_release(observed, None).await;
}

#[tokio::test]
async fn codex_persisted_personality_intent_rejects_reopen_before_connection() {
    for namespace in [
        "agent_sdk_workspaces_v1",
        "agent_sdk_conversations_v1",
        "agent_sdk_codex_native_selection_v1",
    ] {
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let CodexFixture {
            mut peer,
            agent,
            workspace,
            conversation,
            root,
        } = CodexFixture::build_with_store(CodexPeer::start().await, store.clone()).await;
        let agent_id = agent.id();
        let config = AgentConfig::new(
            CodexConfig::tcp(peer.address, Duration::from_secs(3), Duration::from_secs(3))
                .unwrap()
                .into(),
            Duration::from_secs(3),
            NonZeroUsize::MIN,
        )
        .unwrap();
        let mut agent = Some(agent);
        let mut workspace = Some(workspace);
        let mut conversation = Some(conversation);
        let observed = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(10),
            async {
                // Obtain a real persisted selection before simulating the prior
                // release's explicit personality in its unchanged durable format.
                let mut prompt = conversation
                    .as_ref()
                    .unwrap()
                    .prompt_stream(PromptRequest::new("select model").unwrap().with_model(
                        agent_sdk::model::ModelSelector::new("selected-model").unwrap(),
                    ))
                    .await
                    .unwrap();
                let turn = peer.turn().await;
                peer.complete(&turn, "selected").await;
                assert!(matches!(
                    prompt.recv().await,
                    Some(Ok(PromptEvent::Completed(_)))
                ));
                workspace.take().unwrap().close().await.unwrap();
                drop(conversation.take());
                agent.take().unwrap().shutdown().await.unwrap();
                let mut page = store
                    .list(StateListRequest::new(namespace).unwrap())
                    .await
                    .unwrap();
                assert_eq!(page.records.len(), 1, "one durable intent in {namespace}");
                let key = page.records.pop().unwrap().record.key;
                let mut transaction = store.begin_transaction().await.unwrap();
                let mut record = transaction
                    .load(key.clone())
                    .await
                    .unwrap()
                    .expect("existing durable intent")
                    .record;
                let value = Arc::make_mut(&mut record.value);
                let field = if namespace == "agent_sdk_codex_native_selection_v1" {
                    "selection"
                } else {
                    "defaults"
                };
                if value[field].is_null() {
                    value[field] =
                        json!({"model":null,"reasoning_effort":null,"knowledge_policy":null});
                }
                value[field]["personality"] = json!("friendly");
                transaction.put(record.clone()).await.unwrap();
                transaction.commit().await.unwrap();
                let before = peer.initializations.load(Ordering::SeqCst);
                let result =
                    AgentBuilder::reopen(config, AgentStorage::new(store.clone()), agent_id)
                        .build()
                        .await;
                let error = match result {
                    Err(error) => error,
                    Ok(reopened) => {
                        agent = Some(reopened);
                        panic!("persisted personality was silently discarded");
                    }
                };
                assert_eq!(error.kind(), ErrorKind::Unsupported, "{namespace}");
                assert_eq!(
                    peer.initializations.load(Ordering::SeqCst),
                    before,
                    "rejection precedes native attachment"
                );
                assert_eq!(
                    store.load(key).await.unwrap().unwrap().record.value,
                    record.value,
                    "caller intent is preserved"
                );
            },
        )
        .await;
        let mut cleanup = NativeCleanup::new(Duration::from_secs(10));
        if let Some(conversation) = conversation {
            cleanup.settle(conversation.close()).await;
        }
        if let Some(workspace) = workspace {
            cleanup.settle(workspace.close()).await;
        }
        if let Some(agent) = agent {
            cleanup.settle(agent.shutdown()).await;
        }
        peer.shutdown().await;
        cleanup
            .settle(async { std::fs::remove_dir_all(root) })
            .await;
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn codex_foreground_requires_active_controls_without_retired_fanout() {
    let peer = CodexPeer::start().await;
    *peer.features.lock().unwrap() = json!({"data":[{"name":"multi_agent","enabled":false},{"name":"multi_agent_v2","enabled":false}],"nextCursor":null});
    let fixture = CodexFixture::build_with_peer(peer).await;
    let requests = fixture.peer.requests.lock().unwrap().clone();
    fixture.shutdown().await;
    let start = requests
        .iter()
        .find(|request| request["method"] == "thread/start")
        .unwrap();
    assert_eq!(start["params"]["config"]["features.multi_agent"], false);
    assert_eq!(start["params"]["config"]["features.multi_agent_v2"], false);
    assert!(
        start["params"]["config"]
            .get("features.enable_fanout")
            .is_none()
    );
}

impl CodexFixture {
    fn release_notifications() -> Vec<(&'static str, Value, Option<ErrorKind>, &'static str)> {
        vec![
            (
                "thread/readState/changed",
                json!({"threadId":"stream-thread","readState":{"firstUnread":null,"revision":"opaque-revision"}}),
                None,
                "readState",
            ),
            (
                "thread/reverted",
                json!({"threadId":"stream-thread"}),
                Some(ErrorKind::InvalidState),
                "threadId",
            ),
            (
                "thread/queue/changed",
                json!({"threadId":"stream-thread"}),
                Some(ErrorKind::Unsupported),
                "threadId",
            ),
            (
                "thread/attachment/updated",
                json!({"threadId":"stream-thread","attachmentType":"reference","identityKey":"key","attachmentId":"attachment","operation":"created"}),
                None,
                "operation",
            ),
            (
                "thread/prediction/updated",
                json!({"threadId":"stream-thread","sourceTurnId":"turn-1","result":{"type":"completed","text":"prediction"}}),
                Some(ErrorKind::Unsupported),
                "result",
            ),
            (
                "project/changed",
                json!({"projectId":"project","changeType":"updated"}),
                None,
                "changeType",
            ),
            (
                "thread/project/updated",
                json!({"threadId":"stream-thread","projectId":"project"}),
                None,
                "projectId",
            ),
            (
                "thread/environment/connected",
                json!({"threadId":"stream-thread","environmentId":"environment"}),
                None,
                "environmentId",
            ),
            (
                "thread/environment/disconnected",
                json!({"threadId":"stream-thread","environmentId":"environment"}),
                None,
                "environmentId",
            ),
            (
                "autoApprovalReview/strictReviewRequired",
                json!({"threadId":"stream-thread","turnId":"turn-1","startedAtMs":1}),
                None,
                "startedAtMs",
            ),
            (
                "rawResponse/completed",
                json!({"threadId":"stream-thread","turnId":"turn-1","responseId":"response","usage":null,"usageMetadata":null}),
                None,
                "responseId",
            ),
            (
                "mcpServer/event/stream/notification",
                json!({"subscriptionId":"subscription","notification":{"method":"updated","params":{}}}),
                Some(ErrorKind::Unsupported),
                "notification",
            ),
            (
                "account/gatewayOAuth/changed",
                json!({"authUrl":"https://private.invalid","providerId":"provider","status":"started","error":null}),
                None,
                "status",
            ),
            (
                "modelProvider/authRecoveryStarted",
                json!({"threadId":"stream-thread","turnId":"turn-1","provider":"provider","message":"secret-token"}),
                None,
                "message",
            ),
            (
                "modelProvider/authRecoveryCompleted",
                json!({"threadId":"stream-thread","turnId":"turn-1","provider":"provider","message":"secret-token"}),
                None,
                "message",
            ),
            (
                "thread/realtime/item/started",
                json!({"threadId":"stream-thread","item":{"id":"item","realtimeSessionId":"session","type":"realtimeSessionStarted"}}),
                Some(ErrorKind::Unsupported),
                "item",
            ),
            (
                "thread/realtime/item/transcript/delta",
                json!({"threadId":"stream-thread","itemId":"item","delta":"not portable output"}),
                Some(ErrorKind::Unsupported),
                "delta",
            ),
            (
                "thread/realtime/item/completed",
                json!({"threadId":"stream-thread","item":{"id":"item","realtimeSessionId":"session","type":"realtimeSessionClosed","outcome":"ended"}}),
                Some(ErrorKind::Unsupported),
                "item",
            ),
        ]
    }

    async fn assert_release_event(method: &str, params: Value, expected: Option<ErrorKind>) {
        let mut fixture = Self::build().await;
        let observed = std::panic::AssertUnwindSafe(async {
            let mut notifications = fixture.conversation.notification_stream().unwrap();
            let mut stream = fixture.conversation.prompt_stream(PromptRequest::new("event").unwrap()).await.unwrap();
            let turn = fixture.peer.turn().await;
            fixture.peer.notify("item/agentMessage/delta", json!({"threadId":"stream-thread","turnId":turn,"itemId":"answer","delta":"accepted"})).await;
            assert!(matches!(tokio::time::timeout(Duration::from_secs(3), stream.recv()).await.unwrap(), Some(Ok(PromptEvent::OutputDelta { .. }))));
            if expected.is_none() {
                fixture.peer.notify("item/completed", json!({"threadId":"stream-thread","turnId":turn,"completedAtMs":1,
                    "item":{"id":"answer","type":"agentMessage","text":"actual terminal","phase":"final_answer"}})).await;
                fixture.peer.notify(method, params.clone()).await;
            }
            fixture.peer.notify(method, params).await;
            if let Some(expected) = expected {
                let error = tokio::time::timeout(Duration::from_secs(3), stream.recv()).await.expect("selected event must terminate honestly").unwrap().expect_err("event must not complete");
                assert_eq!(error.kind(), expected, "{method}");
                assert!(!format!("{error:?}").contains("secret-token"));
            } else {
                assert!(tokio::time::timeout(Duration::from_millis(25), stream.recv()).await.is_err(), "metadata must not complete or emit output");
                assert!(tokio::time::timeout(Duration::from_millis(25), notifications.recv()).await.is_err(), "private metadata cannot invent public notifications");
                fixture.peer.notify("turn/completed", json!({"threadId":"stream-thread","turn":{"id":turn,"status":"completed","items":[],"error":null}})).await;
                assert!(matches!(tokio::time::timeout(Duration::from_secs(3), stream.recv()).await.unwrap(), Some(Ok(PromptEvent::Completed(response))) if response.output() == "actual terminal"));
                assert!(stream.recv().await.is_none());
                assert!(tokio::time::timeout(Duration::from_millis(25), notifications.recv()).await.is_err(), "repeated metadata cannot publish after completion");
            }
        }).catch_unwind().await;
        let cleanup = fixture.settle().await;
        if let Err(panic) = observed {
            std::panic::resume_unwind(panic);
        }
        if expected.is_none() {
            cleanup.unwrap();
        }
    }
}

#[tokio::test]
async fn codex_selected_notification_additions_have_explicit_outcomes() {
    for (method, params, expected, _) in CodexFixture::release_notifications() {
        CodexFixture::assert_release_event(method, params, expected).await;
    }
}

#[tokio::test]
async fn codex_selected_notification_additions_reject_malformed_payloads() {
    for (method, mut params, _, field) in CodexFixture::release_notifications() {
        params[field] = json!([]);
        CodexFixture::assert_release_event(method, params, Some(ErrorKind::Protocol)).await;
    }
}

#[tokio::test]
async fn codex_read_state_positions_remain_private_until_authoritative_terminal() {
    for read_state in [
        json!({"revision":"opaque / revision"}),
        json!({"firstUnread":null,"revision":""}),
        json!({"firstUnread":{"type":"threadStart"},"revision":"not-a-number"}),
        json!({"firstUnread":{"type":"turn","turnId":"earlier-turn"},"revision":"historical"}),
    ] {
        CodexFixture::assert_release_event(
            "thread/readState/changed",
            json!({"threadId":"stream-thread","readState":read_state}),
            None,
        )
        .await;
    }
}

#[tokio::test]
async fn codex_read_state_rejects_malformed_nested_payloads() {
    for read_state in [
        None,
        Some(Value::Null),
        Some(json!([])),
        Some(json!("receipt")),
        Some(json!({})),
        Some(json!({"revision":null})),
        Some(json!({"revision":1})),
        Some(json!({"revision":"opaque","firstUnread":false})),
        Some(json!({"revision":"opaque","firstUnread":[]})),
        Some(json!({"revision":"opaque","firstUnread":{}})),
        Some(json!({"revision":"opaque","firstUnread":{"type":null}})),
        Some(json!({"revision":"opaque","firstUnread":{"type":"unknown"}})),
        Some(json!({"revision":"opaque","firstUnread":{"type":"turn"}})),
        Some(json!({"revision":"opaque","firstUnread":{"type":"turn","turnId":null}})),
        Some(json!({"revision":"opaque","firstUnread":{"type":"turn","turnId":1}})),
        Some(json!({"revision":"opaque","firstUnread":{"type":"turn","turnId":""}})),
    ] {
        let mut params = json!({"threadId":"stream-thread"});
        if let Some(read_state) = read_state {
            params["readState"] = read_state;
        }
        CodexFixture::assert_release_event(
            "thread/readState/changed",
            params,
            Some(ErrorKind::Protocol),
        )
        .await;
    }
}

#[tokio::test]
async fn codex_read_state_requires_valid_thread_identity() {
    for thread_id in [
        None,
        Some(Value::Null),
        Some(json!(1)),
        Some(json!("")),
        Some(json!(" thread ")),
    ] {
        let mut params = json!({"readState":{"revision":"opaque"}});
        if let Some(thread_id) = thread_id {
            params["threadId"] = thread_id;
        }
        CodexFixture::assert_release_event(
            "thread/readState/changed",
            params,
            Some(ErrorKind::Protocol),
        )
        .await;
    }
}

#[tokio::test]
async fn codex_async_agent_items_are_not_portable_final_answers() {
    for (field, value, expected) in [
        ("delivery", json!("async"), ErrorKind::Unsupported),
        (
            "questions",
            json!([{"title":"First line\nSecond line","options":["Option\nOne"]}]),
            ErrorKind::Unsupported,
        ),
        ("delivery", json!("unknown"), ErrorKind::Protocol),
        ("questions", json!([{"title":9}]), ErrorKind::Protocol),
    ] {
        let mut item =
            json!({"type":"agentMessage","id":"answer","phase":"final_answer","text":"not final"});
        item[field] = value;
        CodexFixture::assert_release_event(
            "item/completed",
            json!({"threadId":"stream-thread","turnId":"turn-1","completedAtMs":1,"item":item}),
            Some(expected),
        )
        .await;
    }
}

#[tokio::test]
async fn codex_function_output_is_private_non_answer_data() {
    CodexFixture::assert_release_event("item/completed", json!({"threadId":"stream-thread","turnId":"turn-1","completedAtMs":1,
        "item":{"type":"functionCallOutput","id":"output","name":"lookup","namespace":null,"output":[{"type":"input_text","text":"not final"}]}}), None).await;
}

impl CodexFixture {
    async fn recovered_result(
        status: &str,
        error: Value,
        items: Vec<Value>,
    ) -> Result<agent_sdk::prompt::PromptResponse, agent_sdk::SdkError> {
        let peer = CodexPeer::start_with(FirstStart::RecoveredTurn {
            status: status.to_owned(),
            error,
            items,
        })
        .await;
        let fixture = Self::build_with_peer(peer).await;
        let observed = tokio::time::timeout(
            Duration::from_secs(3),
            fixture
                .conversation
                .prompt(PromptRequest::new("recover selected items").unwrap()),
        )
        .await;
        let cleanup = fixture.settle().await;
        let result = observed.expect("bounded history recovery");
        if result.is_ok() {
            cleanup.unwrap();
        }
        result
    }
}

#[tokio::test]
async fn codex_history_async_items_match_live_rejection() {
    for (field, value, expected) in [
        ("delivery", json!("async"), ErrorKind::Unsupported),
        ("questions", json!([]), ErrorKind::Unsupported),
        (
            "questions",
            json!([{"title":"private question", "options":null}]),
            ErrorKind::Unsupported,
        ),
        ("delivery", json!("unknown"), ErrorKind::Protocol),
        ("questions", json!([{"title":9}]), ErrorKind::Protocol),
        (
            "questions",
            json!([{"title":"question", "extra":true}]),
            ErrorKind::Protocol,
        ),
    ] {
        let mut item =
            json!({"type":"agentMessage","text":"not a final answer","phase":"final_answer"});
        item[field] = value;
        let error = CodexFixture::recovered_result("completed", Value::Null, vec![item])
            .await
            .expect_err("async history must not become a final answer");
        assert_eq!(error.kind(), expected);
    }
}

#[tokio::test]
async fn codex_function_output_shapes_match_live_and_history() {
    for output in [
        json!("private output"),
        json!([]),
        json!([
            {"type":"input_text","text":"private output"},
            {"type":"input_image","image_url":"data:private","detail":"original"},
            {"type":"input_image","file_id":"private-file"},
            {"type":"input_audio","audio_url":"private-audio"},
            {"type":"encrypted_content","encrypted_content":"private-ciphertext"}
        ]),
    ] {
        let item = json!({"type":"functionCallOutput","id":"output","name":"lookup","namespace":null,"output":output});
        for (method, time) in [
            ("item/started", "startedAtMs"),
            ("item/completed", "completedAtMs"),
        ] {
            let mut params = json!({"threadId":"stream-thread","turnId":"turn-1","item":item});
            params[time] = json!(1);
            CodexFixture::assert_release_event(method, params, None).await;
        }
        let answer = json!({"type":"agentMessage","text":"exact final", "phase":"final_answer", "delivery":null,"questions":null});
        let response = CodexFixture::recovered_result("completed", Value::Null, vec![answer, item])
            .await
            .unwrap();
        assert_eq!(response.output(), "exact final");
    }
    for item in [
        json!({"type":"functionCallOutput","name":"lookup","output":"text"}),
        json!({"type":"functionCallOutput","id":"output","output":"text"}),
        json!({"type":"functionCallOutput","id":"output","name":"lookup","namespace":4,"output":"text"}),
        json!({"type":"functionCallOutput","id":"output","name":"lookup","output":{}}),
        json!({"type":"functionCallOutput","id":"output","name":"lookup","output":[{"type":"input_image"}]}),
        json!({"type":"functionCallOutput","id":"output","name":"lookup","output":[{"type":"input_text","text":4}]}),
    ] {
        CodexFixture::assert_release_event(
            "item/completed",
            json!({"threadId":"stream-thread","turnId":"turn-1","completedAtMs":1,"item":item}),
            Some(ErrorKind::Protocol),
        )
        .await;
        assert_eq!(
            CodexFixture::recovered_result("completed", Value::Null, vec![item])
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Protocol
        );
    }
}

#[tokio::test]
async fn codex_permission_locations_reject_nonlocal_without_actionable_notification() {
    let mut cases = vec![
        (json!("file://remote-host/share/project"), -32601),
        (json!("file:///C:/project"), -32601),
        (json!("relative/path"), -32601),
        (json!(4), -32602),
    ];
    if std::path::MAIN_SEPARATOR == '/' {
        // The wire convention is Windows for any two leading separators,
        // including spellings that this host also considers absolute.
        for cwd in [
            r"C:\project",
            r"//remote-host/share/project",
            r"/\remote-host/share/project",
            r"\/remote-host/share/project",
            r"\\remote-host\share\project",
            r"\\?\C:\project",
            r"\\.\NUL",
        ] {
            cases.push((json!(cwd), -32601));
        }
        for cwd in ["//", "//server", "//server/", "///server/share"] {
            cases.push((json!(cwd), -32602));
        }
    }
    for (cwd, expected) in cases {
        let mut fixture = CodexFixture::build().await;
        let observed = std::panic::AssertUnwindSafe(async {
            let mut notifications = fixture.conversation.notification_stream().unwrap();
            let mut stream = fixture.conversation.prompt_stream(PromptRequest::new("permission location").unwrap()).await.unwrap();
            let turn = fixture.peer.turn().await;
            fixture.peer.request(75, "item/permissions/requestApproval", json!({"threadId":"stream-thread","turnId":turn,"itemId":"permissions","startedAtMs":1,"cwd":cwd,"permissions":{}})).await;
            let response = tokio::time::timeout(Duration::from_secs(3), fixture.peer.responses.recv()).await.unwrap().unwrap();
            assert_eq!(response["id"], 75);
            assert_eq!(response["error"]["code"], expected, "{cwd}");
            assert!(tokio::time::timeout(Duration::from_millis(25), notifications.recv()).await.is_err());
            fixture.peer.complete(&turn, "settled").await;
            assert!(matches!(stream.recv().await, Some(Ok(PromptEvent::Completed(_)))));
        }).catch_unwind().await;
        let cleanup = fixture.settle().await;
        if let Err(panic) = observed {
            std::panic::resume_unwind(panic);
        }
        cleanup.unwrap();
    }
}

#[tokio::test]
async fn codex_known_error_shapes_cannot_escape_as_unknown_errors() {
    for error in [
        json!({"message":"secret-token", "codexErrorInfo":{"rateLimitExceeded":{"invalid":true}}}),
        json!({"message":"secret-token", "codexErrorInfo":{"httpConnectionFailed":{"httpStatusCode":"503"}}}),
        json!({"message":"secret-token", "codexErrorInfo":{"rateLimitExceeded":null,"future":null}}),
        json!({"message":"secret-token", "codexErrorInfo":12}),
        json!({"message":"secret-token", "codexErrorInfo":[]}),
        json!({"message":"secret-token", "codexErrorInfo":"misalignmentPolicyViolation", "misalignment":{"steer":{}}}),
        json!({"message":"secret-token", "misalignment":{"reviewTarget":42}}),
        json!({"message":"secret-token", "misalignment":{"errorType":[]}}),
        json!({"message":"secret-token", "misalignment":{"detailedExplanation":false}}),
    ] {
        CodexFixture::assert_release_event("turn/completed", json!({"threadId":"stream-thread","turn":{"id":"turn-1","status":"failed","items":[],"error":error}}), Some(ErrorKind::Protocol)).await;
        let failure = CodexFixture::recovered_result("failed", error, vec![])
            .await
            .unwrap_err();
        assert_eq!(failure.kind(), ErrorKind::Protocol);
        assert!(!format!("{failure:?}").contains("secret-token"));
    }
}

#[tokio::test]
async fn codex_interrupted_history_preserves_normalized_cause() {
    let error = CodexFixture::recovered_result(
        "interrupted",
        json!({"message":"secret-token", "codexErrorInfo":"rateLimitExceeded"}),
        vec![],
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Cancelled);
    assert_eq!(error.context().phase, ErrorPhase::Cancellation);
    let cause = error.cause().expect("history retains native cause");
    assert_eq!(cause.detail(), Some(&ErrorDetail::RateLimited));
    assert_eq!(cause.context().phase, ErrorPhase::ModelGeneration);
    assert!(!format!("{error:?}").contains("secret-token"));
}

#[tokio::test]
async fn codex_delayed_interruption_requires_matching_normalized_history_cause() {
    HistoricalTerminalCase::MatchingCause.run().await;
    HistoricalTerminalCase::DifferentCause.run().await;
}

#[tokio::test]
async fn codex_shared_item_callbacks_preserve_distinct_rpc_ids_and_no_resend() {
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let peer = CodexPeer::start().await;
    let mut fixture = CodexFixture::build_with_store(peer, store.clone()).await;
    let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(10), async {
        let mut notifications = fixture.conversation.notification_stream().unwrap();
        let mut stream = fixture.conversation.prompt_stream(PromptRequest::new("shared item").unwrap()).await.unwrap();
        let turn = fixture.peer.turn().await;
        for id in [json!(71), json!("71")] {
            fixture.peer.events.send(json!({"id":id,"method":"item/commandExecution/requestApproval", "params":{
                "threadId":"stream-thread","turnId":turn,"itemId":"same-item", "kind":"writeStdin",
                "startedAtMs":1, "availableDecisions":["accept","cancel"]
            }})).await.unwrap();
        }
        let notification = notifications.recv().await.unwrap().unwrap();
        loop {
            let page = store.list(StateListRequest::new("agent_sdk_codex_continuations_v1").unwrap()).await.unwrap();
            if page.records.len() == 2 { break; }
            tokio::task::yield_now().await;
        }
        let result = fixture.conversation.resolve_notification(notification.id().clone(), NotificationResolution::Allow).await;
        assert!(matches!(result, Err(error) if error.kind() == ErrorKind::Conflict));
        let response = fixture.peer.responses.recv().await.unwrap();
        assert_eq!(response["id"], json!(71));
        assert_eq!(response["result"], json!({"decision":"accept"}));
        fixture.peer.notify("serverRequest/resolved", json!({"threadId":"stream-thread", "requestId":71})).await;
        let duplicate = fixture.conversation.resolve_notification(notification.id().clone(), NotificationResolution::Allow).await;
        assert!(matches!(duplicate, Err(error) if error.kind() == ErrorKind::Conflict));
        assert!(tokio::time::timeout(Duration::from_millis(25), fixture.peer.responses.recv()).await.is_err());
        fixture.conversation.abort().await.unwrap();
        assert!(matches!(stream.recv().await, Some(Err(error)) if error.kind() == ErrorKind::Cancelled));
        assert!(store.list(StateListRequest::new("agent_sdk_codex_continuations_v1").unwrap()).await.unwrap().records.is_empty(), "terminal lifecycle settles both distinct callbacks");
        let late = fixture.conversation.resolve_notification(notification.id().clone(), NotificationResolution::Allow).await;
        assert!(matches!(late, Err(error) if matches!(error.kind(), ErrorKind::Conflict | ErrorKind::NotFound)));
        assert!(tokio::time::timeout(Duration::from_millis(25), fixture.peer.responses.recv()).await.is_err());
    }).await;
    let cleanup = fixture.settle().await;
    NativeCleanup::new(Duration::from_secs(10)).finish(observed);
    cleanup.unwrap();
}

#[tokio::test]
async fn codex_added_notifications_cannot_adopt_foreign_thread_ownership() {
    for (method, mut params, expected, _) in CodexFixture::release_notifications() {
        if params.get("threadId").is_some() {
            params["threadId"] = json!("foreign-thread");
            // Passive metadata and foreign history replacement do not acquire an
            // owner. Unsupported execution still requires an existing owner.
            let foreign_error =
                (expected == Some(ErrorKind::Unsupported)).then_some(ErrorKind::InvalidState);
            CodexFixture::release_event(method, params, foreign_error).await;
        }
    }
}

#[tokio::test]
async fn codex_error_bearing_interruption_allows_exact_reopen() {
    let peer = CodexPeer::start_with(FirstStart::DeferredInterruptTerminal).await;
    let CodexFixture {
        mut peer,
        agent,
        workspace,
        conversation,
        root,
    } = CodexFixture::build_with_peer(peer).await;
    let workspace_id = workspace.id().clone();
    let conversation_id = conversation.id().clone();
    let mut workspace = Some(workspace);
    let mut conversation = Some(conversation);
    let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(10), async {
        let mut stream = conversation.as_ref().unwrap().prompt_stream(PromptRequest::new("interrupt then reopen").unwrap()).await.unwrap();
        let turn = peer.turn().await;
        peer.notify("turn/completed", json!({"threadId":"stream-thread","turn":{
            "id":turn,"status":"interrupted","items":[],"error":{"message":"private-native-message","codexErrorInfo":"tooManyDenials"}
        }})).await;
        let error = stream.recv().await.unwrap().unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Cancelled);
        assert_eq!(error.cause().unwrap().detail(), Some(&ErrorDetail::PolicyRejected));
        workspace.take().unwrap().close().await.unwrap();
        drop(conversation.take());
        let reopened = workspace.insert(agent.get_workspace(workspace_id).await.unwrap());
        let reopened = conversation.insert(reopened.get_conversation(conversation_id).await.unwrap());
        let mut next = reopened.prompt_stream(PromptRequest::new("reopened").unwrap()).await.unwrap();
        let next_turn = peer.turn().await;
        peer.complete(&next_turn, "exact reopened output").await;
        assert!(matches!(next.recv().await, Some(Ok(PromptEvent::Completed(response))) if response.output() == "exact reopened output"));
        assert_eq!(peer.thread_starts.load(Ordering::SeqCst), 1, "reopen retains native identity");
        {
            let requests = peer.requests.lock().unwrap();
            let start = &requests.iter().find(|request| request["method"] == "thread/start").unwrap()["params"];
            let resumes = requests.iter().filter(|request| request["method"] == "thread/resume").collect::<Vec<_>>();
            assert_eq!(resumes.len(), 1);
            let resume = &resumes[0]["params"];
            assert_eq!(resume["threadId"], "stream-thread");
            assert_eq!(resume["cwd"], json!(root));
            assert_eq!(resume["runtimeWorkspaceRoots"], json!([root]));
            assert_eq!(resume["permissions"], ":danger-full-access");
            assert_eq!(resume["excludeTurns"], true);
            for field in ["cwd", "runtimeWorkspaceRoots", "permissions", "config"] {
                assert_eq!(resume[field], start[field], "resume must preserve {field}");
            }
            assert_eq!(resume["config"]["features.multi_agent"], false);
            assert_eq!(resume["config"]["features.multi_agent_v2"], false);
            assert_eq!(resume["config"]["suppress_unstable_features_warning"], true);
            assert!(resume.get("sandbox").is_none());
            let lists = requests.iter().filter(|request| request["method"] == "thread/list").collect::<Vec<_>>();
            assert!(!lists.is_empty(), "reopen cleanup inspects native descendants");
            assert!(lists.iter().all(|request| request["params"].get("excludedThreadIds").is_none()));
        }
        assert!(peer.requests.lock().unwrap().iter().any(|request| request["method"] == "thread/read" && request["params"]["threadId"] == "stream-thread"));
    }).await;
    let mut cleanup = NativeCleanup::new(Duration::from_secs(10));
    if let Some(conversation) = conversation {
        cleanup.settle(conversation.close()).await;
    }
    if let Some(workspace) = workspace {
        cleanup.settle(workspace.close()).await;
    }
    cleanup.settle(agent.shutdown()).await;
    peer.shutdown().await;
    cleanup
        .settle(async { std::fs::remove_dir_all(root) })
        .await;
    cleanup.finish(observed);
}

#[tokio::test]
async fn codex_reopen_rejects_changed_effective_policy_before_publication() {
    for overrides in [
        json!({"sandbox":{"type":"readOnly","networkAccess":false}}),
        json!({"sandbox":{"type":"externalSandbox","networkAccess":"restricted"}}),
        json!({"activePermissionProfile":{"id":":read-only","extends":null}}),
        json!({"activePermissionProfile":{"id":":danger-full-access","extends":"other-profile"}}),
        json!({"activePermissionProfile":null}),
    ] {
        let CodexFixture {
            peer,
            agent,
            workspace,
            conversation,
            root,
        } = CodexFixture::build().await;
        let workspace_id = workspace.id().clone();
        let conversation_id = conversation.id().clone();
        let mut workspace = Some(workspace);
        let mut conversation = Some(conversation);
        let observed = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(10),
            async {
                workspace.take().unwrap().close().await.unwrap();
                drop(conversation.take());
                *peer.resume_overrides.lock().unwrap() = overrides.as_object().unwrap().clone();
                let reopened = workspace.insert(agent.get_workspace(workspace_id).await.unwrap());
                match reopened.get_conversation(conversation_id).await {
                    Err(error) => assert_eq!(error.kind(), ErrorKind::InvalidState),
                    Ok(handle) => {
                        conversation = Some(handle);
                        panic!("mismatched effective policy must reject before handle publication");
                    }
                }
                assert_eq!(peer.thread_starts.load(Ordering::SeqCst), 1);
                let requests = peer.requests.lock().unwrap();
                assert_eq!(
                    requests
                        .iter()
                        .filter(|request| request["method"] == "thread/resume")
                        .count(),
                    1
                );
                assert!(
                    !requests
                        .iter()
                        .any(|request| request["method"] == "turn/start")
                );
            },
        )
        .await;
        peer.resume_overrides.lock().unwrap().clear();
        let mut cleanup = NativeCleanup::new(Duration::from_secs(10));
        if let Some(conversation) = conversation {
            cleanup.settle(conversation.close()).await;
        }
        if let Some(workspace) = workspace {
            cleanup.settle(workspace.close()).await;
        }
        cleanup.settle(agent.shutdown()).await;
        peer.shutdown().await;
        cleanup
            .settle(async { std::fs::remove_dir_all(root) })
            .await;
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn codex_shutdown_rejection_is_private_and_does_not_replay_resume() {
    let CodexFixture {
        peer,
        agent,
        workspace,
        conversation,
        root,
    } = CodexFixture::build().await;
    let workspace_id = workspace.id().clone();
    let conversation_id = conversation.id().clone();
    let mut workspace = Some(workspace);
    let mut conversation = Some(conversation);
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(10),
        async {
            workspace.take().unwrap().close().await.unwrap();
            drop(conversation.take());
            peer.rejections.lock().unwrap().insert(
                "thread/resume".to_owned(),
                json!({
                    "code":-32600,
                    "message":"secret-token native-private-path retry after reconnecting",
                    "data":{"reason":"serverShuttingDown"}
                }),
            );
            let reopened = workspace.insert(agent.get_workspace(workspace_id).await.unwrap());
            let error = match reopened.get_conversation(conversation_id).await {
                Err(error) => error,
                Ok(handle) => {
                    conversation = Some(handle);
                    panic!("shutdown rejection must not publish a conversation");
                }
            };
            assert_eq!(error.kind(), ErrorKind::Integration);
            assert_eq!(error.detail(), Some(&ErrorDetail::InvalidRequest));
            for text in [
                error.message().to_owned(),
                error.to_string(),
                format!("{error:?}"),
            ] {
                for private in [
                    "secret-token",
                    "native-private-path",
                    "retry after reconnecting",
                    "serverShuttingDown",
                ] {
                    assert!(!text.contains(private));
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
            let requests = peer.requests.lock().unwrap();
            assert_eq!(
                requests
                    .iter()
                    .filter(|request| request["method"] == "thread/resume")
                    .count(),
                1
            );
            assert!(
                !requests
                    .iter()
                    .any(|request| request["method"] == "turn/start")
            );
            assert_eq!(peer.thread_starts.load(Ordering::SeqCst), 1);
        },
    )
    .await;
    peer.rejections.lock().unwrap().clear();
    let mut cleanup = NativeCleanup::new(Duration::from_secs(10));
    if let Some(conversation) = conversation {
        cleanup.settle(conversation.close()).await;
    }
    if let Some(workspace) = workspace {
        cleanup.settle(workspace.close()).await;
    }
    cleanup.settle(agent.shutdown()).await;
    peer.shutdown().await;
    cleanup
        .settle(async { std::fs::remove_dir_all(root) })
        .await;
    cleanup.finish(observed);
}

#[tokio::test]
async fn codex_active_foreground_controls_remain_required_and_typed() {
    for (entries, expected) in [
        (
            json!([{"name":"multi_agent","enabled":false}]),
            ErrorKind::Protocol,
        ),
        (
            json!([{"name":"multi_agent","enabled":false},{"name":"multi_agent_v2","enabled":true}]),
            ErrorKind::Unsupported,
        ),
        (
            json!([{"name":"multi_agent","enabled":false},{"name":"multi_agent_v2","enabled":"false"}]),
            ErrorKind::Protocol,
        ),
        (
            json!([{"name":"multi_agent","enabled":false},{"name":"multi_agent","enabled":false},{"name":"multi_agent_v2","enabled":false}]),
            ErrorKind::Protocol,
        ),
    ] {
        let CodexFixture {
            peer,
            agent,
            workspace,
            conversation,
            root,
        } = CodexFixture::build().await;
        let workspace_id = workspace.id().clone();
        let mut workspace = Some(workspace);
        *peer.features.lock().unwrap() = json!({"data":entries,"nextCursor":null});
        let observed = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(10),
            async {
                let id = conversation.id().clone();
                workspace.take().unwrap().close().await.unwrap();
                let workspace = workspace.insert(agent.get_workspace(workspace_id).await.unwrap());
                let result = workspace.get_conversation(id).await;
                let error = match result {
                    Err(error) => error,
                    Ok(conversation) => {
                        conversation.close().await.unwrap();
                        panic!("invalid active controls must fail");
                    }
                };
                assert_eq!(error.kind(), expected);
            },
        )
        .await;
        *peer.features.lock().unwrap() = json!({"data":[{"name":"multi_agent","enabled":false},{"name":"multi_agent_v2","enabled":false}],"nextCursor":null});
        let mut cleanup = NativeCleanup::new(Duration::from_secs(10));
        drop(conversation);
        if let Some(workspace) = workspace {
            cleanup.settle(workspace.close()).await;
        }
        cleanup.settle(agent.shutdown()).await;
        peer.shutdown().await;
        cleanup
            .settle(async { std::fs::remove_dir_all(root) })
            .await;
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn codex_partial_answer_phase_never_replaces_the_final_answer() {
    let partial = json!({"type":"agentMessage","id":"partial","text":"partial only","phase":"partial_answer"});
    CodexFixture::assert_release_event(
        "item/completed",
        json!({"threadId":"stream-thread","turnId":"turn-1","completedAtMs":1,"item":partial}),
        None,
    )
    .await;
    let final_answer =
        json!({"type":"agentMessage","id":"final","text":"exact final","phase":"final_answer"});
    let response = CodexFixture::recovered_result(
        "completed",
        Value::Null,
        vec![final_answer, partial.clone()],
    )
    .await
    .unwrap();
    assert_eq!(response.output(), "exact final");
    let error = CodexFixture::recovered_result("completed", Value::Null, vec![partial])
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::NotYetImplemented);
}

#[tokio::test]
async fn codex_interaction_publication_stops_repeated_conflicts_at_deadline() {
    WaiterFaultStore::assert_interaction_publication(
        InteractionFaultPoint::Journal,
        StorageFaultSource::Conflict,
        false,
        usize::MAX,
    )
    .await;
}

#[tokio::test]
async fn codex_interaction_publication_reconciles_begin_and_prompt_load_conflicts() {
    for point in [
        InteractionFaultPoint::Begin,
        InteractionFaultPoint::PromptLoad,
    ] {
        WaiterFaultStore::assert_interaction_publication(
            point,
            StorageFaultSource::Conflict,
            false,
            1,
        )
        .await;
    }
}

#[tokio::test]
async fn codex_interaction_publication_deadline_rejects_late_staging() {
    WaiterFaultStore::assert_interaction_publication(
        InteractionFaultPoint::HeldStaging,
        StorageFaultSource::Conflict,
        false,
        1,
    )
    .await;
}

#[tokio::test]
async fn codex_interaction_publication_deadline_retains_admitted_commit() {
    WaiterFaultStore::assert_interaction_publication(
        InteractionFaultPoint::HeldCommit,
        StorageFaultSource::Conflict,
        false,
        1,
    )
    .await;
}

#[tokio::test]
async fn codex_interaction_publication_does_not_retry_ambiguous_commit() {
    WaiterFaultStore::assert_interaction_publication(
        InteractionFaultPoint::AmbiguousCommit,
        StorageFaultSource::Unavailable,
        false,
        1,
    )
    .await;
}

#[tokio::test]
async fn codex_interaction_publication_reloads_settled_duplicate_in_transaction() {
    let raw = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let attempts = Arc::new(AtomicUsize::new(0));
    let store = WaiterFaultStore {
        inner: raw.clone(),
        armed: Arc::clone(&armed),
        attempts: Arc::clone(&attempts),
        fault: WaiterFault::InteractionPublication {
            point: InteractionFaultPoint::StaleDuplicate,
            source: StorageFaultSource::Conflict,
            owner_task: Arc::new(std::sync::Mutex::new(None)),
            failures: 1,
            rollback_failure: false,
            rollbacks: Arc::new(AtomicUsize::new(0)),
        },
    };
    let mut fixture = CodexFixture::build_with_store(CodexPeer::start().await, store).await;
    let observed = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(3), async {
        let mut notifications = fixture.conversation.notification_stream().unwrap();
        let mut stream = fixture.conversation.prompt_stream(PromptRequest::new("duplicate approval").unwrap()).await.unwrap();
        let turn = fixture.peer.turn().await;
        let request = json!({"id":71,"method":"item/commandExecution/requestApproval","params":{
            "threadId":"stream-thread","turnId":turn,"itemId":"command","startedAtMs":1,"availableDecisions":["accept","cancel"]}});
        fixture.peer.events.send(request.clone()).await.unwrap();
        let notification = notifications.recv().await.unwrap().unwrap();
        assert_eq!(fixture.conversation.resolve_notification(notification.id().clone(), NotificationResolution::Allow).await.unwrap_err().kind(), ErrorKind::Conflict);
        assert_eq!(fixture.peer.responses.recv().await.unwrap(), json!({"id":71,"result":{"decision":"accept"}}));
        let before = raw.list(StateListRequest::new("agent_sdk_codex_continuations_v1").unwrap()).await.unwrap();
        assert_eq!(before.records.len(), 1);
        assert_eq!(before.records[0].record.value["state"], "ambiguous");
        // A list snapshot may precede the resolution commit. Transactional load
        // still observes the current ambiguous continuation and resolving journal.
        armed.store(true, Ordering::SeqCst);
        fixture.peer.events.send(request).await.unwrap();
        let response = tokio::time::timeout(Duration::from_millis(250), fixture.peer.responses.recv()).await
            .expect("settled duplicate must receive rejection despite stale pending inventory").unwrap();
        assert_eq!(response["id"], 71);
        assert_eq!(response["error"]["code"], -32602);
        armed.store(false, Ordering::SeqCst);
        assert_eq!(attempts.load(Ordering::SeqCst), 1, "logical identity conflicts must not retry");
        let after = raw.list(StateListRequest::new("agent_sdk_codex_continuations_v1").unwrap()).await.unwrap();
        assert_eq!(after.records, before.records, "duplicate cannot mutate settled continuation");
        fixture.peer.complete(&turn, "duplicate safely rejected").await;
        assert!(matches!(stream.recv().await, Some(Ok(PromptEvent::Completed(response))) if response.output() == "duplicate safely rejected"));
        assert!(stream.recv().await.is_none());
        assert!(fixture.peer.responses.try_recv().is_err());
    })).catch_unwind().await;
    armed.store(false, Ordering::SeqCst);
    let mut cleanup = NativeCleanup::new(Duration::from_secs(4));
    cleanup.settle(fixture.conversation.close()).await;
    cleanup.settle(fixture.workspace.close()).await;
    cleanup.settle(fixture.agent.shutdown()).await;
    cleanup
        .settle(async {
            fixture.peer.shutdown().await;
            Ok::<(), agent_sdk::SdkError>(())
        })
        .await;
    cleanup
        .settle(async { std::fs::remove_dir_all(fixture.root) })
        .await;
    cleanup.finish(observed);
}

#[tokio::test]
async fn codex_release_interrupted_error_retains_cancelled_primary_and_safe_cause() {
    for recovered in [false, true] {
        for streaming in [false, true] {
            let native_error = json!({"message":"secret-token", "codexErrorInfo":"tooManyDenials",
                "misalignment":{"errorType":"private-classification", "detailedExplanation":"secret-token", "steer":{"message":"secret-token"}}});
            let peer = CodexPeer::start_with(if recovered {
                FirstStart::RecoveredTurn {
                    status: "interrupted".into(),
                    error: native_error.clone(),
                    items: vec![],
                }
            } else {
                FirstStart::Ordinary
            })
            .await;
            let mut fixture = CodexFixture::build_with_peer(peer).await;
            let observed = std::panic::AssertUnwindSafe(async {
                let error = if streaming {
                    let mut stream = fixture.conversation.prompt_stream(PromptRequest::new("interrupt").unwrap()).await.unwrap();
                    let turn = fixture.peer.turn().await;
                    if !recovered {
                        fixture.peer.notify("turn/completed", json!({"threadId":"stream-thread","turn":{
                            "id":turn,"status":"interrupted","items":[],"error":native_error}})).await;
                    }
                    let error = tokio::time::timeout(Duration::from_secs(3), stream.recv()).await.unwrap().unwrap().unwrap_err();
                    assert!(stream.recv().await.is_none());
                    error
                } else {
                    let mut prompt = Box::pin(fixture.conversation.prompt(PromptRequest::new("interrupt").unwrap()));
                    let turn = tokio::select! { turn = fixture.peer.turn() => turn, result = &mut prompt => panic!("early {result:?}") };
                    if !recovered {
                        fixture.peer.notify("turn/completed", json!({"threadId":"stream-thread","turn":{
                            "id":turn,"status":"interrupted","items":[],"error":native_error}})).await;
                    }
                    tokio::time::timeout(Duration::from_secs(3), prompt).await.unwrap().unwrap_err()
                };

            assert_eq!(error.kind(), ErrorKind::Cancelled, "recovered={recovered}, streaming={streaming}");
            assert_eq!(error.context().operation, Operation::Prompt);
            assert_eq!(error.context().phase, ErrorPhase::Cancellation);
            let cause = error.cause().expect("native cancellation cause");
            assert_eq!(cause.kind(), ErrorKind::Integration);
            assert_eq!(cause.context().operation, Operation::Prompt);
            assert_eq!(cause.context().phase, ErrorPhase::ModelGeneration);
            assert_eq!(cause.detail(), Some(&ErrorDetail::PolicyRejected));
            assert_eq!(cause.http_status(), None);
            assert!(!format!("{error} {error:?}").contains("secret-token"));
            }).catch_unwind().await;
            fixture.finish_release(observed, None).await;
        }
    }
}
