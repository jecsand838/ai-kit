mod native_cleanup;
mod opencode_abort_reconciliation;
mod opencode_cwd_routing;
mod opencode_file_paths;
mod opencode_pending_cancellation;
mod opencode_prepared_cancellation;
mod opencode_provider_inventory;
mod opencode_recovered_waiter;
mod opencode_release;
mod opencode_terminal_reconciliation;
mod opencode_workspace_delete;

use native_cleanup::NativeCleanup;
use opencode_workspace_delete::WorkspaceDeleteCommitFault;

use agent_sdk::error::{DeadlineKind, ErrorDetail, ErrorKind, ErrorPhase, Operation};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::time::Duration;

use agent_sdk::config::{AgentConfig, OpenCodeConfig};
use agent_sdk::conversation::{ConversationBackend, NewConversation};
use agent_sdk::identity::ConversationId;
use agent_sdk::knowledge::KnowledgeSource;
use agent_sdk::notification::{ConversationNotificationKind, NotificationResolution};
use agent_sdk::prompt::{PromptEvent, PromptRequest, PromptStream, StructuredOutputRequest};
use agent_sdk::workspace::{
    DeleteWorkspaceRequest, NewWorkspace, WorkspaceContext, WorkspaceDefaults,
    WorkspaceExecutionPolicy, WorkspaceNetworkAccess, WorkspaceRoot, WorkspaceRootAccess,
    WorkspaceSandboxMode,
};
use agent_sdk::{AgentBackend, WorkspaceBackend};
use agent_sdk::{AgentBuilder, AgentStorage, SdkError};
use storage_sdk::database::CapabilitySet;
use storage_sdk::database::catalog::{CatalogRequest, CatalogResponse};
use storage_sdk::database::data::{DataRequest, DataResponse};
use storage_sdk::database::index::{IndexRequest, IndexResponse};
use storage_sdk::database::query::{QueryRequest, QueryResponse};
use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_sdk::state::record::{
    StateKey, StateListRequest, StateListResponse, StateRecord, StateRecordSnapshot,
};
use storage_sdk::{
    DatabaseBackend, StateStore, StateStoreBackend, StateStoreError, StateTransaction,
    StateTransactionBackend, StorageError,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, Semaphore, broadcast, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};

struct OpenCodeEventProbe {
    endpoint: String,
    stats: Arc<OpenCodeEventStats>,
    disconnect: watch::Sender<u64>,
    events: broadcast::Sender<String>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

#[derive(Clone, Copy, Debug)]
enum RetainedCloseCase {
    QuestionGone,
    Question,
    Permissions,
    OrdinaryQuestionObserved,
    OrdinaryQuestionClosed,
    OrdinaryQuestionCloseObserverLost,
    ChangedQuestion(RetainedIdentityChange),
    ChangedPermissions(RetainedIdentityChange),
}

#[derive(Clone, Copy, Debug)]
enum RetainedRejection {
    Acknowledged,
    AcknowledgedWithoutEvent,
    HeldAcknowledgedAfterDisconnectWithoutEvent,
    Held,
    HeldDroppedObserver,
    Unavailable,
    LostWithEvent,
    LostWithoutEvent,
    Missing,
    ContradictoryAnswer,
}

#[derive(Clone, Copy, Debug)]
enum RetainedIdentityChange {
    Session,
    Message,
    Call,
    Schema,
    MissingOrigin,
    ForeignPermission,
    MixedPermissionDirectory,
    OrdinaryCompletion,
}

impl RetainedIdentityChange {
    fn assert_closed_descendant(error: &SdkError, operation: Operation) {
        assert_eq!(error.kind(), ErrorKind::Closed);
        assert_eq!(error.context().operation, operation);
        assert_eq!(error.context().phase, ErrorPhase::Lifecycle);
        assert!(error.detail().is_none());
        assert!(error.cause().is_none());
        assert!(error.http_status().is_none());
        assert!(error.supplementary().is_empty());
        assert!(!error.diagnostics_truncated());
    }

    fn assert_native_isolation_shutdown(error: &SdkError) {
        // The original ownership/routing conflict must survive bounded cleanup.
        // A stopped event command receiver retains its precise Lifecycle phase;
        // this does not authorize a Closed primary or arbitrary shutdown errors.
        let mut pending = vec![(error, true)];
        let mut conflicts = 0;
        while let Some((error, primary)) = pending.pop() {
            assert_eq!(error.context().operation, Operation::Shutdown);
            assert!(error.cause().is_none());
            assert!(error.http_status().is_none());
            assert!(!error.diagnostics_truncated());
            match error.kind() {
                ErrorKind::Conflict => {
                    assert_eq!(error.context().phase, ErrorPhase::Cleanup);
                    assert!(error.detail().is_none());
                    conflicts += 1;
                }
                ErrorKind::Timeout => {
                    assert_eq!(error.context().phase, ErrorPhase::Cleanup);
                    assert_eq!(
                        error.detail(),
                        Some(&ErrorDetail::DeadlineElapsed {
                            kind: DeadlineKind::Operation,
                            timeout: None,
                        })
                    );
                }
                ErrorKind::Closed => {
                    assert!(
                        !primary,
                        "event-channel closure cannot replace the isolation failure"
                    );
                    assert_eq!(error.context().phase, ErrorPhase::Lifecycle);
                    assert!(error.detail().is_none());
                    assert!(error.supplementary().is_empty());
                }
                other => {
                    panic!("unexpected native isolation cleanup failure {other:?}: {error:?}")
                }
            }
            pending.extend(error.supplementary().iter().map(|error| (error, false)));
        }
        assert!(
            conflicts > 0,
            "shutdown must preserve the original native isolation conflict: {error:?}"
        );
    }

    fn change(self, request: &mut serde_json::Value, permission: bool) {
        match self {
            Self::Session => request["sessionID"] = serde_json::json!("ses_foreign"),
            Self::Message => {
                request["tool"]["messageID"] = serde_json::json!("msg_000000000099abcdefghijklmn")
            }
            Self::Call => request["tool"]["callID"] = serde_json::json!("call_foreign"),
            Self::Schema if permission => {
                request["metadata"]["command"] =
                    serde_json::json!("changed immutable permission metadata")
            }
            Self::Schema => request["questions"][0]["custom"] = serde_json::json!(false),
            Self::MissingOrigin => {
                request.as_object_mut().unwrap().remove("tool");
            }
            Self::ForeignPermission | Self::MixedPermissionDirectory | Self::OrdinaryCompletion => {
            }
        }
    }
}

struct OpenCodeEventStats {
    cwd_routing: Mutex<Option<Arc<opencode_cwd_routing::CwdRoutingFixture>>>,
    requests: AtomicUsize,
    active: AtomicUsize,
    maximum: AtomicUsize,
    subscriptions: AtomicUsize,
    history: AtomicUsize,
    history_failures: AtomicUsize,
    pending_scans: AtomicUsize,
    status_requests: AtomicUsize,
    unavailable_status: AtomicBool,
    rejected_status_failures: AtomicUsize,
    busy_status_responses: AtomicUsize,
    lose_stream_response: AtomicBool,
    lose_abort_response: AtomicBool,
    reject_abort_without_effects: AtomicBool,
    messages: AtomicUsize,
    question_replies: AtomicUsize,
    question_rejections: AtomicUsize,
    permission_rejections: AtomicUsize,
    pending_permissions: Mutex<Vec<serde_json::Value>>,
    mixed_permission_directories: Mutex<BTreeSet<String>>,
    retained_permissions: AtomicBool,
    retained_parallel_questions: AtomicBool,
    pending_peer_question: AtomicBool,
    held_second_permission: AtomicBool,
    retained_identity_change: Mutex<Option<RetainedIdentityChange>>,
    pending_cleanup_faults: Mutex<Option<PromptFaultStore>>,
    retained_rejection: Mutex<RetainedRejection>,
    rejection_entered: Semaphore,
    rejection_release: Semaphore,
    aborts: AtomicUsize,
    abort_authority: Mutex<Option<Arc<PromptFaultState>>>,
    aborts_without_dispatch_commit: AtomicUsize,
    deletes: AtomicUsize,
    inventory_responses: AtomicUsize,
    invalid_session_reads: AtomicUsize,
    next_session: AtomicUsize,
    native_active: AtomicBool,
    active_on_structured: AtomicBool,
    block_abort: AtomicBool,
    question_on_prompt: AtomicBool,
    pending_question: AtomicBool,
    block_history: AtomicBool,
    session_event_before_binding: AtomicBool,
    stall_capability_headers: AtomicBool,
    stall_capability_body: AtomicBool,
    stall_event_headers: AtomicBool,
    block_tool_inventory: AtomicBool,
    capability_requests: AtomicUsize,
    history_release: Notify,
    listener_pause: Notify,
    listener_paused: Semaphore,
    listener_resume: Semaphore,
    listener_resumed: Semaphore,
    question_reply_redirect: Mutex<Option<String>>,
    lose_question_reply_response: AtomicBool,
    pause_before_reply_redirect: AtomicBool,
    question_reply_redirects: AtomicUsize,
    question_reply_acceptances: AtomicUsize,
    question_reply_status: AtomicUsize,
    session_event_history: Semaphore,
    event_headers_release: Notify,
    tool_inventory_blocked: Semaphore,
    tool_inventory_release: Semaphore,
    inventory_response: Semaphore,
    events: broadcast::Sender<String>,
    sessions: Mutex<BTreeMap<String, ProbeSession>>,
    history_rows: Mutex<Vec<serde_json::Value>>,
    stream_fixture: AtomicBool,
    retained_question_history: AtomicBool,
    retain_question_after_abort: AtomicBool,
    retained_question_close_history: AtomicBool,
    retained_question_close_rows: AtomicUsize,
    prompt_release: Semaphore,
    prompt_parent: Mutex<Option<serde_json::Value>>,
    withhold_stream_completion: AtomicBool,
    invalid_stream_response: AtomicBool,
    malformed_stream_response: AtomicBool,
    terminal_error: Mutex<Option<serde_json::Value>>,
    provider_status: AtomicUsize,
    release: Mutex<opencode_release::ReleaseFixture>,
    tool_models: Mutex<Vec<(String, String)>>,
    reject_abort: AtomicBool,
    defer_stream_input: AtomicBool,
    prompt_responses: AtomicUsize,
    connections: AtomicUsize,
}

struct ActiveEventConnection {
    stats: Arc<OpenCodeEventStats>,
}

struct ProbeConnection {
    stats: Arc<OpenCodeEventStats>,
}

struct ClientSetupDatabase {
    released: Option<oneshot::Sender<()>>,
    calls: Arc<AtomicUsize>,
}

impl DatabaseBackend for ClientSetupDatabase {
    fn capabilities(&self) -> CapabilitySet {
        self.calls.fetch_add(1, Ordering::SeqCst);
        CapabilitySet::empty()
    }

    async fn catalog(&self, _: CatalogRequest) -> Result<CatalogResponse, StorageError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(StorageError::unsupported_capability(
            "catalog",
            "unused startup database",
        ))
    }

    async fn data(&self, _: DataRequest) -> Result<DataResponse, StorageError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(StorageError::unsupported_capability(
            "data",
            "unused startup database",
        ))
    }

    async fn query(&self, _: QueryRequest) -> Result<QueryResponse, StorageError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(StorageError::unsupported_capability(
            "query",
            "unused startup database",
        ))
    }

    async fn index(&self, _: IndexRequest) -> Result<IndexResponse, StorageError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(StorageError::unsupported_capability(
            "index",
            "unused startup database",
        ))
    }
}

impl Drop for ClientSetupDatabase {
    fn drop(&mut self) {
        if let Some(released) = self.released.take() {
            let _ = released.send(());
        }
    }
}

struct ClientSetupBlocker {
    release: Option<std::sync::mpsc::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl ClientSetupBlocker {
    async fn occupy() -> Self {
        let (release, held) = std::sync::mpsc::channel();
        let (entered, ready) = oneshot::channel();
        let task = tokio::task::spawn_blocking(move || {
            let _ = entered.send(());
            match held.recv_timeout(Duration::from_secs(20)) {
                Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    panic!("the test must release its finite blocking worker");
                }
            }
        });
        let mut blocker = Self {
            release: Some(release),
            task: Some(task),
        };
        if !matches!(
            tokio::time::timeout(Duration::from_secs(5), ready).await,
            Ok(Ok(()))
        ) {
            let _ = blocker.join().await;
            panic!("the test blocking worker must become occupied");
        }
        blocker
    }

    async fn join(&mut self) -> Result<(), tokio::task::JoinError> {
        drop(self.release.take());
        match self.task.take() {
            Some(task) => task.await,
            None => Ok(()),
        }
    }
}

#[derive(Clone, Copy)]
enum ClientSetupScenario {
    ObservedExpiry,
    DroppedSuccess,
    DroppedExpiry,
}

impl ClientSetupScenario {
    fn run(self) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .expect("client setup test runtime must build")
            .block_on(self.assert_ownership());
    }

    async fn assert_ownership(self) {
        use futures_util::FutureExt;

        let probe = OpenCodeEventProbe::start(0).await;
        let storage = OpenCodeEventProbe::storage().await;
        let mut blocker = ClientSetupBlocker::occupy().await;
        let (released, mut resource_released) = oneshot::channel();
        let database_calls = Arc::new(AtomicUsize::new(0));
        let lifecycle_timeout = Duration::from_secs(3);
        let mut build = Some(tokio::spawn({
            let config = probe.config(Duration::from_secs(1), lifecycle_timeout);
            let storage = storage.clone();
            let database = ClientSetupDatabase {
                released: Some(released),
                calls: Arc::clone(&database_calls),
            };
            async move {
                AgentBuilder::create(config, storage)
                    .with_knowledge_database(database)
                    .build()
                    .await
            }
        }));
        let mut unexpected_agent = None;
        let mut reopened = None;
        let mut resource_observed = false;
        let outcome = std::panic::AssertUnwindSafe(async {
            let identity = tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let inventory = storage
                        .list_agents()
                        .await
                        .expect("durable setup inventory");
                    if let Some(info) = inventory.first() {
                        assert_eq!(inventory.len(), 1, "setup creates one durable identity");
                        break info.id().clone();
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("durable setup must complete while the blocking worker is occupied");

            let competing = AgentBuilder::reopen(
                probe.config(Duration::from_secs(1), lifecycle_timeout),
                storage.clone(),
                identity.clone(),
            )
            .build()
            .await;
            let busy = match competing {
                Err(error) => error,
                Ok(agent) => {
                    reopened = Some(agent);
                    panic!("accepted construction must retain its live identity claim");
                }
            };
            assert_eq!(busy.kind(), ErrorKind::Busy);
            assert_eq!(busy.context().operation, Operation::Reopen);
            assert_eq!(
                probe.stats.requests.load(Ordering::SeqCst),
                0,
                "queued client setup must not begin native verification"
            );

            if !matches!(self, Self::ObservedExpiry) {
                let observer = build.take().expect("build observer remains owned");
                observer.abort();
                match observer.await {
                    Err(error) if error.is_cancelled() => {}
                    Ok(Ok(agent)) => {
                        unexpected_agent = Some(agent);
                        panic!("the build observer must remain pending until setup can run");
                    }
                    Ok(Err(_)) | Err(_) => {
                        panic!("only the caller's build observer must be cancelled");
                    }
                }
                let competing = AgentBuilder::reopen(
                    probe.config(Duration::from_secs(1), lifecycle_timeout),
                    storage.clone(),
                    identity.clone(),
                )
                .build()
                .await;
                let busy = match competing {
                    Err(error) => error,
                    Ok(agent) => {
                        reopened = Some(agent);
                        panic!("caller loss cannot release accepted construction's identity claim");
                    }
                };
                assert_eq!(busy.kind(), ErrorKind::Busy);
                assert_eq!(busy.context().operation, Operation::Reopen);
            }

            if matches!(self, Self::ObservedExpiry | Self::DroppedExpiry) {
                // Waiting for a resource release is also a reactor-progress check.
                // The finite setup owner must retain the resource beyond expiry
                // while its blocking execution context remains occupied.
                let release = tokio::time::timeout(
                    lifecycle_timeout + Duration::from_millis(100),
                    &mut resource_released,
                )
                .await;
                resource_observed = release.is_ok();
                assert!(
                    release.is_err(),
                    "expiry cannot detach the accepted setup owner"
                );
                assert_eq!(probe.stats.requests.load(Ordering::SeqCst), 0);
            }

            blocker
                .join()
                .await
                .expect("the occupied blocking worker must join");
            if let Some(mut observer) = build.take() {
                let result = tokio::time::timeout(Duration::from_secs(5), &mut observer).await;
                if result.is_err() {
                    build = Some(observer);
                    panic!("queued setup must settle after its execution context is released");
                }
                let error = match result.unwrap().expect("build observer must join") {
                    Err(error) => error,
                    Ok(agent) => {
                        unexpected_agent = Some(agent);
                        panic!("expired setup cannot publish an Agent");
                    }
                };
                assert_eq!(error.kind(), ErrorKind::Timeout);
                assert_eq!(error.context().operation, Operation::Build);
                assert_eq!(error.context().phase, ErrorPhase::ClientSetup);
                assert_eq!(
                    error.detail(),
                    Some(&ErrorDetail::DeadlineElapsed {
                        kind: DeadlineKind::Lifecycle,
                        timeout: Some(lifecycle_timeout),
                    })
                );
                assert!(error.cause().is_none());
                assert!(error.supplementary().is_empty());
            }

            let release =
                tokio::time::timeout(Duration::from_secs(5), &mut resource_released).await;
            resource_observed = release.is_ok();
            release
                .expect("accepted setup must release its consumed database")
                .expect("consumed database must report its drop");
            if matches!(self, Self::DroppedSuccess) {
                probe.wait_for(1, 0, 0).await;
            } else {
                assert_eq!(probe.stats.requests.load(Ordering::SeqCst), 0);
                assert_eq!(probe.stats.subscriptions.load(Ordering::SeqCst), 0);
            }
            assert_eq!(
                database_calls.load(Ordering::SeqCst),
                0,
                "the resource lifetime probe must not add database work to empty-Agent startup"
            );
            let inventory = storage
                .list_agents()
                .await
                .expect("retained durable identity");
            assert_eq!(inventory.len(), 1);
            assert_eq!(inventory[0].id(), &identity);

            // The consumed-resource drop wakes this current-thread runtime only
            // after the owner's synchronous terminal tail releases its claim.
            // Observe cleanup, then perform exactly one public reopen attempt.
            reopened = Some(
                AgentBuilder::reopen(
                    probe.config(Duration::from_secs(1), lifecycle_timeout),
                    storage.clone(),
                    identity,
                )
                .build()
                .await
                .expect("completed setup ownership must release its identity claim"),
            );
        })
        .catch_unwind()
        .await;

        let blocker_cleanup = blocker.join().await;
        if let Some(observer) = build.take()
            && let Ok(Ok(agent)) = observer.await
        {
            unexpected_agent = Some(agent);
        }
        let unexpected_cleanup = match unexpected_agent.take() {
            Some(agent) => agent.shutdown().await,
            None => Ok(()),
        };
        let resource_cleanup = if resource_observed {
            Ok(())
        } else {
            tokio::time::timeout(Duration::from_secs(5), &mut resource_released)
                .await
                .map_err(|_| "the retained setup owner must release its database during cleanup")
                .and_then(|released| {
                    released.map_err(|_| "the consumed startup database must report release")
                })
        };
        let reopened_cleanup = match reopened.take() {
            Some(agent) => agent.shutdown().await,
            None => Ok(()),
        };
        let stats = Arc::clone(&probe.stats);
        probe.shutdown().await;
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
        blocker_cleanup.expect("the test blocking worker must be joined");
        unexpected_cleanup.expect("an unexpectedly published Agent must shut down");
        resource_cleanup.expect("the retained setup owner must settle before probe teardown");
        reopened_cleanup.expect("the reopened Agent must shut down");
        assert_eq!(stats.active.load(Ordering::SeqCst), 0);
        assert_eq!(stats.connections.load(Ordering::SeqCst), 0);
    }
}

struct ProbeRequest {
    model: Option<(String, String)>,
    method: String,
    path: String,
    directory: Option<String>,
    body: Vec<u8>,
}

#[derive(Clone)]
struct ProbeSession {
    correlation: String,
    directory: String,
}

enum ProbeStreamActivity {
    Disconnect,
    Event(Result<String, broadcast::error::RecvError>),
    Closed,
}

const OPENCODE_PROMPT_NAMESPACE: &str = "agent_sdk_opencode_prompts_v1";
const OPENCODE_CONTINUATION_NAMESPACE: &str = "agent_sdk_opencode_continuations_v1";
const OPENCODE_CATALOG_NAMESPACE: &str = "agent_sdk_opencode_catalogs_v1";
const OPENCODE_EVENT_PROGRESS_NAMESPACE: &str = "agent_sdk_opencode_event_progress_v1";
const OPENCODE_SESSION_NAMESPACE: &str = "agent_sdk_opencode_sessions_v1";
const OPENCODE_CLEANUP_NAMESPACE: &str = "agent_sdk_opencode_cleanup_v1";
const CONVERSATION_NAMESPACE: &str = "agent_sdk_conversations_v1";
const WORKSPACE_NAMESPACE: &str = "agent_sdk_workspaces_v1";

#[repr(u8)]
#[derive(Clone, Copy, Eq, PartialEq)]
enum PromptCommitFault {
    None,
    Conflict,
    Unavailable,
    BlockThenCommit,
    UnavailableAfterCommit,
    BlockThenConflict,
    BlockThenUnavailableAfterCommit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PromptPersistenceFault {
    None,
    Terminal,
    TerminalOnce,
    TerminalCommitConflictOnce,
    TerminalCommitUnavailableOnce,
    TerminalCommitReceiptLost,
    ResponseLoss,
    ResponseLossOnce,
    ResponseLossClaim,
    ResponseLossClaimOnce,
    ResponseLossSettle,
    ResponseLossSettleOnce,
    WaiterLoss,
    WaiterLossOnce,
    TimeoutOnce,
    Timeout,
    DispatchOnce,
    Dispatch,
    AbortOutcomeOnce,
    AbortOutcome,
    AbortOutcomeCommitConflictOnce,
    AbortOutcomeCommitReceiptLost,
    AbortOutcomeCommitUnavailableOnce,
    ReopenCommitConflictOnce,
    AbortOutcomeCommitConflict,
    AdmissionOnce,
    Admission,
    AdmissionConflictOnce,
    AdmissionConflict,
    AdmissionCommitConflictOnce,
    AdmissionCommitUnavailableOnce,
    AdmissionCommitReceiptLost,
    CancelRequestOnce,
    CancelRequest,
    CancelRequestLoadConflictOnce,
    CancelRequestConflictOnce,
    CancelRequestConflict,
    CancelRequestCommitConflictOnce,
    CancelRequestCommitReceiptLost,
    CancelRequestCommitUnavailableOnce,
    CancelRequestCommitReceiptAdvanced,
    CancelRequestForeign,
    CancelRequestTerminal,
    NativeCleanupLoadConflictOnce,
    NativeCleanupLoadThenBeginConflict,
    NativeCleanupLoadConflict,
    NativeCleanupConflictOnce,
    NativeCleanupCommitConflictOnce,
    NativeCleanupCommitReceiptLost,
    NativeCleanupCommitUnavailableOnce,
    CancelClaimOnce,
    CancelClaim,
    CancelClaimCommitConflictOnce,
    CancelClaimCommitUnavailableOnce,
    CancelClaimCommitReceiptLost,
    CancelClaimCommitHeld,
    CancelDispatchOnce,
    CancelDispatch,
    CancelDispatchCommitUnavailableOnce,
    CancelDispatchCommitReceiptLost,
    PreDispatchCleanupOnce,
    PreDispatchCleanup,
    IdleRepairOnce,
    IdleRepair,
}

impl PromptPersistenceFault {
    fn is_native_cleanup(self) -> bool {
        matches!(
            self,
            Self::NativeCleanupLoadConflictOnce
                | Self::NativeCleanupLoadThenBeginConflict
                | Self::NativeCleanupLoadConflict
                | Self::NativeCleanupConflictOnce
                | Self::NativeCleanupCommitConflictOnce
                | Self::NativeCleanupCommitReceiptLost
                | Self::NativeCleanupCommitUnavailableOnce
        )
    }

    fn is_cancellation_request(self) -> bool {
        matches!(
            self,
            Self::CancelRequestLoadConflictOnce
                | Self::CancelRequestConflictOnce
                | Self::CancelRequestConflict
                | Self::CancelRequestCommitConflictOnce
                | Self::CancelRequestCommitReceiptLost
                | Self::CancelRequestCommitUnavailableOnce
                | Self::CancelRequestCommitReceiptAdvanced
                | Self::CancelRequestForeign
                | Self::CancelRequestTerminal
        )
    }

    fn targets(self, record: &StateRecord) -> bool {
        if record.key.namespace().as_str() != OPENCODE_PROMPT_NAMESPACE {
            return false;
        }
        let phase = record
            .value
            .pointer("/phase/phase")
            .and_then(serde_json::Value::as_str);
        let waiter = record.value["waiter"].as_str();
        let cancellation = record
            .value
            .pointer("/cancellation/state")
            .and_then(serde_json::Value::as_str);
        let lost = matches!(
            phase,
            Some("response_lost_before_admission" | "response_lost_after_admission")
        );
        match self {
            Self::None
            | Self::ReopenCommitConflictOnce
            | Self::NativeCleanupLoadConflictOnce
            | Self::NativeCleanupLoadThenBeginConflict
            | Self::NativeCleanupLoadConflict
            | Self::NativeCleanupConflictOnce
            | Self::NativeCleanupCommitConflictOnce
            | Self::NativeCleanupCommitReceiptLost
            | Self::NativeCleanupCommitUnavailableOnce
            | Self::CancelRequestLoadConflictOnce
            | Self::CancelRequestForeign
            | Self::CancelRequestTerminal => false,
            Self::Terminal
            | Self::TerminalOnce
            | Self::TerminalCommitConflictOnce
            | Self::TerminalCommitUnavailableOnce
            | Self::TerminalCommitReceiptLost => {
                phase == Some("terminal") && waiter == Some("waiting")
            }
            Self::ResponseLoss | Self::ResponseLossOnce => lost && waiter == Some("waiting"),
            Self::ResponseLossClaim | Self::ResponseLossClaimOnce => {
                lost && waiter == Some("delivering")
            }
            Self::ResponseLossSettle | Self::ResponseLossSettleOnce => {
                lost && waiter == Some("lost")
            }
            Self::WaiterLoss | Self::WaiterLossOnce => {
                phase == Some("admitted") && waiter == Some("lost")
            }
            Self::TimeoutOnce | Self::Timeout => waiter == Some("timed_out"),
            Self::DispatchOnce | Self::Dispatch => {
                phase == Some("dispatched_awaiting_admission")
                    && cancellation == Some("not_requested")
            }
            Self::AbortOutcomeOnce
            | Self::AbortOutcome
            | Self::AbortOutcomeCommitConflictOnce
            | Self::AbortOutcomeCommitConflict
            | Self::AbortOutcomeCommitReceiptLost
            | Self::AbortOutcomeCommitUnavailableOnce => cancellation == Some("accepted"),
            Self::AdmissionOnce
            | Self::Admission
            | Self::AdmissionConflictOnce
            | Self::AdmissionConflict
            | Self::AdmissionCommitConflictOnce
            | Self::AdmissionCommitUnavailableOnce
            | Self::AdmissionCommitReceiptLost => {
                phase == Some("admitted") && cancellation == Some("not_requested")
            }
            Self::CancelRequestOnce
            | Self::CancelRequest
            | Self::CancelRequestConflictOnce
            | Self::CancelRequestConflict
            | Self::CancelRequestCommitConflictOnce
            | Self::CancelRequestCommitReceiptLost
            | Self::CancelRequestCommitUnavailableOnce
            | Self::CancelRequestCommitReceiptAdvanced => {
                phase == Some("prepared") && cancellation == Some("requested")
            }
            Self::CancelClaimOnce
            | Self::CancelClaim
            | Self::CancelClaimCommitConflictOnce
            | Self::CancelClaimCommitUnavailableOnce
            | Self::CancelClaimCommitReceiptLost
            | Self::CancelClaimCommitHeld => cancellation == Some("reserved"),
            Self::CancelDispatchOnce
            | Self::CancelDispatch
            | Self::CancelDispatchCommitUnavailableOnce
            | Self::CancelDispatchCommitReceiptLost => cancellation == Some("dispatching"),
            Self::PreDispatchCleanupOnce | Self::PreDispatchCleanup => {
                matches!(phase, Some("prepared" | "dispatched_awaiting_admission"))
                    && cancellation == Some("not_requested")
            }
            Self::IdleRepairOnce | Self::IdleRepair => {
                phase == Some("response_lost_after_admission") && waiter == Some("lost")
            }
        }
    }

    fn fails_once(self) -> bool {
        matches!(
            self,
            Self::TerminalOnce
                | Self::TerminalCommitConflictOnce
                | Self::TerminalCommitUnavailableOnce
                | Self::TerminalCommitReceiptLost
                | Self::TimeoutOnce
                | Self::ResponseLossOnce
                | Self::ResponseLossClaimOnce
                | Self::ResponseLossSettleOnce
                | Self::WaiterLossOnce
                | Self::DispatchOnce
                | Self::AbortOutcomeOnce
                | Self::AbortOutcomeCommitConflictOnce
                | Self::AbortOutcomeCommitReceiptLost
                | Self::AbortOutcomeCommitUnavailableOnce
                | Self::ReopenCommitConflictOnce
                | Self::AdmissionOnce
                | Self::AdmissionConflictOnce
                | Self::AdmissionCommitConflictOnce
                | Self::AdmissionCommitUnavailableOnce
                | Self::AdmissionCommitReceiptLost
                | Self::CancelRequestOnce
                | Self::CancelRequestLoadConflictOnce
                | Self::CancelRequestConflictOnce
                | Self::CancelRequestCommitConflictOnce
                | Self::CancelRequestCommitReceiptLost
                | Self::CancelRequestCommitUnavailableOnce
                | Self::CancelRequestCommitReceiptAdvanced
                | Self::CancelRequestForeign
                | Self::CancelRequestTerminal
                | Self::CancelClaimOnce
                | Self::CancelClaimCommitConflictOnce
                | Self::CancelClaimCommitUnavailableOnce
                | Self::CancelClaimCommitReceiptLost
                | Self::CancelClaimCommitHeld
                | Self::CancelDispatchOnce
                | Self::CancelDispatchCommitUnavailableOnce
                | Self::CancelDispatchCommitReceiptLost
                | Self::PreDispatchCleanupOnce
                | Self::IdleRepairOnce
        )
    }

    fn is_commit(self) -> bool {
        matches!(
            self,
            Self::AbortOutcomeCommitConflictOnce
                | Self::ReopenCommitConflictOnce
                | Self::AbortOutcomeCommitConflict
                | Self::AbortOutcomeCommitReceiptLost
                | Self::AbortOutcomeCommitUnavailableOnce
                | Self::CancelRequestCommitConflictOnce
                | Self::CancelRequestCommitReceiptLost
                | Self::CancelRequestCommitUnavailableOnce
                | Self::CancelRequestCommitReceiptAdvanced
                | Self::AdmissionCommitConflictOnce
                | Self::AdmissionCommitUnavailableOnce
                | Self::AdmissionCommitReceiptLost
                | Self::TerminalCommitConflictOnce
                | Self::TerminalCommitUnavailableOnce
                | Self::TerminalCommitReceiptLost
                | Self::CancelClaimCommitConflictOnce
                | Self::CancelClaimCommitUnavailableOnce
                | Self::CancelClaimCommitReceiptLost
                | Self::CancelClaimCommitHeld
                | Self::CancelDispatchCommitUnavailableOnce
                | Self::CancelDispatchCommitReceiptLost
        )
    }

    fn is_deletion(self) -> bool {
        matches!(
            self,
            Self::PreDispatchCleanupOnce
                | Self::PreDispatchCleanup
                | Self::IdleRepairOnce
                | Self::IdleRepair
        )
    }

    fn retained_cancellation(self) -> &'static str {
        match self {
            Self::CancelClaimOnce | Self::CancelClaim => "requested",
            Self::CancelDispatchOnce | Self::CancelDispatch => "reserved",
            Self::AbortOutcomeOnce | Self::AbortOutcome => "dispatching",
            _ => panic!("only an admitted cancellation fault has this expectation"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PreparedCancelFault {
    None,
    BeginConflictOnce,
    LoadConflictOnce,
    LoadConflict,
    DeleteConflictOnce,
    PutConflictOnce,
    CommitConflictOnce,
    CommitConflict,
    CommitReceiptLost,
    CommitReceiptHeld,
    CommitUnavailableOnce,
    CommitUnavailableForeignReceipt,
    CommitUnavailableMissingReceipt,
    CommitUnavailableMalformedReceipt,
    CommitReceiptLostReadUnavailableOnce,
    CommitUnavailableReadUnavailableOnce,
    CommitUnavailableReadHeld,
    InitialMissing,
    InitialForeign,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PromptCleanupStage {
    Prepared,
    AwaitNonPreparedRollback,
    AwaitWaiterRollback,
    RecoveredWaiter,
    AbortReconciliation,
    TerminalMutation,
    AwaitEventSettlement { continuation_key: StateKey },
    TerminalEvent,
}

#[derive(Clone, Debug)]
struct PreparedCancelReceipt {
    task: Option<tokio::task::Id>,
    key: StateKey,
    fault: PreparedCancelFault,
    requested_fault: PreparedCancelFault,
    stage: PromptCleanupStage,
    enabled: bool,
    active: bool,
    awaiting_rollback: bool,
    uncertain_commit: bool,
    foreign_receipt: bool,
    unavailable_receipt: bool,
    hold_receipt: bool,
    attempted_record: Option<StateRecord>,
    exact_receipts: usize,
    intent_record: Option<StateRecord>,
    before: Option<StateRecordSnapshot>,
    event_before: Option<StateRecordSnapshot>,
    event_record: Option<StateRecord>,
    event_commits: usize,
    activation_reads: usize,
    activation_rollbacks: usize,
    begins: usize,
    reads: usize,
    puts: usize,
    writes_after_commit: usize,
    deletes: usize,
    deletes_after_commit: usize,
    commit_attempts: usize,
    commits: usize,
    rollbacks: usize,
    rollback_failures: usize,
    faults: usize,
    receipt_reads: usize,
    receipt_read_failures: usize,
    absent_receipts: usize,
    accesses_before_rollback: usize,
}

#[derive(Clone)]
struct ReconciliationObserver {
    task: tokio::task::Id,
    key: StateKey,
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
}

impl PreparedCancelReceipt {
    fn new(
        fault: PreparedCancelFault,
        task: Option<tokio::task::Id>,
        key: StateKey,
        stage: PromptCleanupStage,
    ) -> Self {
        Self {
            task,
            key,
            fault,
            requested_fault: fault,
            stage,
            enabled: true,
            active: false,
            awaiting_rollback: false,
            uncertain_commit: false,
            foreign_receipt: false,
            unavailable_receipt: false,
            hold_receipt: false,
            attempted_record: None,
            exact_receipts: 0,
            intent_record: None,
            before: None,
            event_before: None,
            event_record: None,
            event_commits: 0,
            activation_reads: 0,
            activation_rollbacks: 0,
            begins: 0,
            reads: 0,
            puts: 0,
            writes_after_commit: 0,
            deletes: 0,
            deletes_after_commit: 0,
            commit_attempts: 0,
            commits: 0,
            rollbacks: 0,
            rollback_failures: 0,
            faults: 0,
            receipt_reads: 0,
            receipt_read_failures: 0,
            absent_receipts: 0,
            accesses_before_rollback: 0,
        }
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkspaceCreatePhase {
    None,
    Begin,
    Load,
    Put,
    Commit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConversationIntentPhase {
    Begin,
    WorkspaceLoad,
    RoutingLoad,
    ConversationLoad,
    BindingLoad,
    ConversationPut,
    BindingPut,
    Commit,
}

struct ConversationIntentFault {
    phase: ConversationIntentPhase,
    fault: PromptCommitFault,
    remaining: usize,
    injections: usize,
    begins: usize,
    rollbacks: usize,
    awaiting_rollback: bool,
    fail_rollback: bool,
    stale_workspace: bool,
    late_commit: bool,
    reject_create: bool,
    reject_ingest: bool,
    fail_discard: bool,
    discard_attempts: usize,
    fail_forget_read: bool,
    creation_task: Option<tokio::task::Id>,
    discard_commits: usize,
    forget_reads: usize,
    commit_entered: Arc<Semaphore>,
    commit_release: Arc<Semaphore>,
    active: bool,
    records: BTreeMap<String, StateRecord>,
}

impl ConversationIntentFault {
    fn new(phase: ConversationIntentPhase, fault: PromptCommitFault, remaining: usize) -> Self {
        Self {
            phase,
            fault,
            remaining,
            injections: 0,
            begins: 0,
            rollbacks: 0,
            awaiting_rollback: false,
            fail_rollback: false,
            stale_workspace: false,
            late_commit: false,
            reject_create: false,
            reject_ingest: false,
            fail_discard: false,
            discard_attempts: 0,
            fail_forget_read: false,
            creation_task: None,
            discard_commits: 0,
            forget_reads: 0,
            commit_entered: Arc::new(Semaphore::new(0)),
            commit_release: Arc::new(Semaphore::new(0)),
            active: true,
            records: BTreeMap::new(),
        }
    }

    async fn exercise(self) {
        let phase = self.phase;
        let fault = self.fault;
        let remaining = self.remaining;
        let fail_rollback = self.fail_rollback;
        let stale_workspace = self.stale_workspace;
        let late_commit = self.late_commit;
        let reject_create = self.reject_create;
        let reject_ingest = self.reject_ingest;
        let fail_discard = self.fail_discard;
        let fail_forget_read = self.fail_forget_read;
        let commit_entered = self.commit_entered.clone();
        let commit_release = self.commit_release.clone();
        let probe = OpenCodeEventProbe::start(0).await;
        let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
        let config = probe.config(Duration::from_secs(10), Duration::from_secs(5));
        let storage = faults.storage();
        let agent = AgentBuilder::create(config.clone(), storage.clone())
            .build()
            .await
            .expect("intent fault Agent must build");
        let agent_id = agent.id();
        let workspace = agent
            .new_workspace(OpenCodeEventProbe::workspace_request())
            .await
            .expect("intent fault workspace must build");
        let sessions = probe.stats.next_session.load(Ordering::SeqCst);
        let config_reads = {
            let mut release = probe.stats.release.lock().unwrap();
            if reject_create {
                // Reuse create_session's existing definite config-read rejection;
                // no native creation POST or session effect may occur.
                release.config_error = Some(serde_json::json!({"message":"WIRE_SECRET"}));
            }
            release.config_reads
        };
        *faults.state.conversation_intent.lock().unwrap() = Some(self);
        let knowledge_sources = if reject_ingest {
            // The existing unconfigured knowledge path rejects nonempty ingestion
            // after intent commit, before any native conversation creation.
            vec![
                KnowledgeSource::text("intent-source", "Intent source", "original source").unwrap(),
            ]
        } else {
            Vec::new()
        };
        let mut creation = Box::pin(
            workspace
                .new_conversation(NewConversation::new(None, None, knowledge_sources).unwrap()),
        );
        let early = if late_commit {
            tokio::select! {
                entered = commit_entered.acquire() => entered.unwrap().forget(),
                result = &mut creation => panic!("creation settled before its held intent commit: {}", result.is_ok()),
            }
            let early = tokio::time::timeout(Duration::from_millis(5_200), &mut creation)
                .await
                .ok();
            commit_release.add_permits(1);
            early
        } else {
            None
        };
        let pending_until_commit = early.is_none();
        let result = match early {
            Some(result) => Ok(result),
            None => tokio::time::timeout(Duration::from_secs(7), &mut creation).await,
        };
        drop(creation);
        let receipt = faults
            .state
            .conversation_intent
            .lock()
            .unwrap()
            .take()
            .unwrap();
        let mut durable = Vec::new();
        for record in receipt.records.values() {
            durable.push((
                record.clone(),
                faults.inner.load(record.key.clone()).await.unwrap(),
            ));
        }
        let creates = probe.stats.next_session.load(Ordering::SeqCst) - sessions;
        let rejected_config_reads = {
            let mut release = probe.stats.release.lock().unwrap();
            let reads = release.config_reads - config_reads;
            if reject_create {
                release.config_error = None;
            }
            reads
        };
        let result = result.map(|result| result.map(drop));
        drop(workspace);
        agent
            .shutdown()
            .await
            .expect("intent fault Agent must settle its owners");
        let reopened = if (late_commit || fail_forget_read) && !fail_discard {
            match AgentBuilder::reopen(config, storage, agent_id)
                .build()
                .await
            {
                Ok(agent) => agent.shutdown().await,
                Err(error) => Err(error),
            }
        } else {
            Ok(())
        };
        probe.shutdown().await;

        assert!(
            pending_until_commit,
            "an admitted intent commit must settle before deadline reporting"
        );
        reopened.expect("a known pre-native late intent must be discarded so reopen can finish");
        let result = result.expect("intent retries must retain the original lifecycle deadline");
        let succeeds = fault == PromptCommitFault::Conflict
            && remaining != usize::MAX
            && !fail_rollback
            && !stale_workspace
            && !reject_create
            && !reject_ingest;
        assert_eq!(
            creates,
            usize::from(succeeds),
            "native create must run exactly once after intent success and never before: {phase:?}"
        );
        if succeeds {
            result.expect("definite local intent conflicts must retry the same creation");
            assert_eq!(receipt.injections, remaining, "{phase:?}");
            assert_eq!(receipt.begins, remaining + 1, "{phase:?}");
            assert_eq!(durable.len(), 2);
            for (record, stored) in durable {
                let stored = stored.expect("successful intent must remain durable");
                if record.key.namespace().as_str() == CONVERSATION_NAMESPACE {
                    assert_eq!(stored.record, record);
                    assert_eq!(stored.revision.get(), 1);
                } else {
                    assert_eq!(stored.record.value["state"]["phase"], "bound");
                    assert_eq!(
                        stored.record.value["state"]["correlation"],
                        record.value["state"]["correlation"]
                    );
                    assert_eq!(stored.revision.get(), 2);
                }
            }
        } else {
            let error = result.expect_err("non-retryable intent failures must remain errors");
            assert_eq!(error.context().operation, Operation::NewConversation);
            if reject_create || reject_ingest {
                assert_eq!(
                    rejected_config_reads,
                    usize::from(reject_create),
                    "native prerequisite rejection occurs only after successful ingestion"
                );
                assert_eq!(receipt.begins, 1);
                assert_eq!(receipt.injections, 0);
                assert_eq!(
                    receipt.discard_attempts, 1,
                    "discard is attempted once without replay"
                );
                assert_eq!(durable.len(), 2);
                let primary = if fail_discard {
                    assert_eq!(error.kind(), ErrorKind::Closed);
                    assert_eq!(error.context().phase, ErrorPhase::Publication);
                    let [primary] = error.supplementary() else {
                        panic!("revoked publication must preserve creation failure: {error:?}");
                    };
                    primary
                } else {
                    &error
                };
                if reject_ingest {
                    assert_eq!(primary.kind(), ErrorKind::Unsupported);
                    assert_eq!(primary.detail(), None);
                    assert_eq!(primary.http_status(), None);
                    assert_eq!(
                        primary.context().phase,
                        if fail_discard {
                            ErrorPhase::Unspecified
                        } else {
                            ErrorPhase::Admission
                        }
                    );
                } else {
                    assert_eq!(primary.kind(), ErrorKind::InvalidInput);
                    assert_eq!(primary.detail(), Some(&ErrorDetail::InvalidRequest));
                    assert_eq!(primary.http_status(), Some(400));
                    assert_eq!(primary.context().phase, ErrorPhase::ResponseRead);
                }
                assert!(!format!("{error:?}").contains("WIRE_SECRET"));
                if fail_discard {
                    let [cleanup] = primary.supplementary() else {
                        panic!("discard failure must supplement creation rejection: {error:?}");
                    };
                    assert_eq!(cleanup.kind(), ErrorKind::Integration);
                    assert_eq!(cleanup.context().phase, ErrorPhase::Cleanup);
                } else if fail_forget_read {
                    assert_eq!(
                        receipt.discard_commits, 1,
                        "intent discard must commit first"
                    );
                    assert_eq!(
                        receipt.forget_reads, 1,
                        "one owned cleanup read fails without retry"
                    );
                    let [cleanup] = primary.supplementary() else {
                        panic!(
                            "knowledge settlement must supplement creation rejection: {error:?}"
                        );
                    };
                    assert_eq!(cleanup.kind(), ErrorKind::Integration);
                    assert_eq!(cleanup.detail(), Some(&ErrorDetail::ServiceUnavailable));
                    assert_eq!(cleanup.context().phase, ErrorPhase::StateRead);
                    assert!(!format!("{error:?}").contains("PRIVATE_FORGET_SETTLEMENT"));
                } else {
                    assert!(primary.supplementary().is_empty());
                }
            } else if late_commit {
                assert_eq!(receipt.begins, 1);
                assert_eq!(receipt.injections, 0);
                assert_eq!(durable.len(), 2);
                assert_eq!(receipt.discard_attempts, 1);
                if fail_discard {
                    assert_eq!(error.kind(), ErrorKind::Closed);
                    assert_eq!(error.context().phase, ErrorPhase::Publication);
                    let [timeout] = error.supplementary() else {
                        panic!("publication failure must retain the operation deadline: {error:?}");
                    };
                    assert_eq!(timeout.kind(), ErrorKind::Timeout);
                    assert_eq!(timeout.context().phase, ErrorPhase::StateCommit);
                    let [cleanup] = timeout.supplementary() else {
                        panic!("failed intent discard must supplement timeout: {error:?}");
                    };
                    assert_eq!(cleanup.kind(), ErrorKind::Integration);
                    assert_eq!(cleanup.context().phase, ErrorPhase::Cleanup);
                } else {
                    assert_eq!(error.kind(), ErrorKind::Timeout);
                    assert_eq!(error.context().phase, ErrorPhase::StateCommit);
                    assert!(error.supplementary().is_empty());
                }
            } else if remaining == usize::MAX {
                assert_eq!(error.kind(), ErrorKind::Timeout);
                assert_eq!(error.context().phase, ErrorPhase::StateWrite);
                assert!(error.cause().is_none());
                let [conflict] = error.supplementary() else {
                    panic!("deadline must retain the last definite conflict: {error:?}");
                };
                assert_eq!(conflict.kind(), ErrorKind::Conflict);
                assert_eq!(conflict.detail(), Some(&ErrorDetail::TransactionConflict));
                assert_eq!(conflict.context().phase, ErrorPhase::StateWrite);
                assert!(
                    receipt.injections > 1,
                    "persistent conflicts must actually retry"
                );
            } else if fail_rollback {
                assert_eq!(
                    error.kind(),
                    if fault == PromptCommitFault::Conflict {
                        ErrorKind::Conflict
                    } else {
                        ErrorKind::Integration
                    }
                );
                if fault == PromptCommitFault::Conflict {
                    assert_eq!(error.detail(), Some(&ErrorDetail::TransactionConflict));
                }
                let [cleanup] = error.supplementary() else {
                    panic!("rollback failure must supplement the original conflict: {error:?}");
                };
                assert_eq!(cleanup.kind(), ErrorKind::Integration);
                assert_eq!(cleanup.context().phase, ErrorPhase::Cleanup);
                assert_eq!(receipt.begins, 1, "failed rollback must prohibit retry");
            } else if stale_workspace {
                assert_eq!(error.kind(), ErrorKind::Conflict);
                assert_ne!(error.detail(), Some(&ErrorDetail::TransactionConflict));
                assert_eq!(receipt.begins, 1, "a logical conflict must not be retried");
            } else {
                assert_eq!(error.kind(), ErrorKind::Integration);
                assert_eq!(receipt.injections, 1);
                assert_eq!(
                    receipt.begins, 1,
                    "an uncertain failure must not be replayed"
                );
            }
            for (record, stored) in durable {
                if fault == PromptCommitFault::UnavailableAfterCommit
                    || ((late_commit || reject_create || reject_ingest) && fail_discard)
                {
                    let stored =
                        stored.expect("uncertain intent commit must preserve durable state");
                    assert_eq!(stored.record, record);
                    assert_eq!(stored.revision.get(), 1);
                } else {
                    assert!(
                        stored.is_none(),
                        "failed intent must not publish partial state"
                    );
                }
            }
        }
        let expected_rollbacks = if matches!(
            phase,
            ConversationIntentPhase::Begin | ConversationIntentPhase::Commit
        ) {
            0
        } else if stale_workspace {
            1
        } else {
            receipt.injections
        };
        assert_eq!(
            receipt.rollbacks, expected_rollbacks,
            "each admitted read/write failure must explicitly settle before return/retry: {phase:?}"
        );
    }

    fn before(&mut self, phase: ConversationIntentPhase) -> Result<bool, StateStoreError> {
        if !self.active {
            if (self.late_commit || self.reject_create || self.reject_ingest)
                && phase == ConversationIntentPhase::Begin
            {
                self.discard_attempts += 1;
                if self.fail_discard {
                    return Err(StateStoreError::unavailable(
                        "begin",
                        "injected intent discard failure",
                    ));
                }
            }
            return Ok(false);
        }
        if phase == ConversationIntentPhase::Begin {
            if self.fail_forget_read {
                self.creation_task = Some(tokio::task::try_id().expect("creation owns its task"));
            }
            assert!(
                !self.awaiting_rollback,
                "a retry must await explicit rollback"
            );
            self.begins += 1;
        }
        if self.phase != phase || self.remaining == 0 {
            return Ok(false);
        }
        if self.remaining != usize::MAX {
            self.remaining -= 1;
        }
        self.injections += 1;
        self.awaiting_rollback = !matches!(
            phase,
            ConversationIntentPhase::Begin | ConversationIntentPhase::Commit
        );
        match self.fault {
            PromptCommitFault::Conflict => Err(StateStoreError::transaction_conflict(
                "conversation_intent",
                "injected intent serialization conflict",
            )),
            PromptCommitFault::Unavailable => Err(StateStoreError::unavailable(
                "conversation_intent",
                "injected intent unavailability",
            )),
            PromptCommitFault::UnavailableAfterCommit => Ok(true),
            _ => panic!("intent injection requires an explicit fault"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BoundSessionFault {
    None,
    Conflicts(usize),
    Unavailable,
    StagingRollbackFailure,
}

struct PromptFaultState {
    mode: AtomicU8,
    target: AtomicUsize,
    prompt_commits: AtomicUsize,
    waiter_fault: AtomicU8,
    waiter_fault_attempts: AtomicUsize,
    block_waiter_commit: AtomicBool,
    persistence_fault: Mutex<PromptPersistenceFault>,
    persistence_foreign: AtomicBool,
    persistence_conflict: AtomicBool,
    persistence_rollback_failure: AtomicBool,
    persistence_foreign_next: AtomicBool,
    persistence_attempts: AtomicUsize,
    native_cleanup_armed: AtomicBool,
    native_cleanup_receipt: Mutex<Option<opencode_pending_cancellation::NativeCleanupReceipt>>,
    prepared_cancel_receipt: Mutex<Option<PreparedCancelReceipt>>,
    reconciliation_observer: Mutex<Option<ReconciliationObserver>>,
    admission_writes: AtomicUsize,
    terminal_writes: AtomicUsize,
    cancellation_request_task: Mutex<Option<tokio::task::Id>>,
    cancellation_request_writes: AtomicUsize,
    cancellation_request_commits: AtomicUsize,
    cancellation_request_reads_entered: Semaphore,
    cancellation_request_admission_committed: Semaphore,
    cancellation_request_advanced: AtomicBool,
    cancellation_request_uncommitted: AtomicBool,
    cancellation_request_advance_observed: Semaphore,
    cancellation_claim_writes: AtomicUsize,
    cancellation_claim_owners: Mutex<Vec<serde_json::Value>>,
    cancellation_dispatch_writes: AtomicUsize,
    cancellation_dispatch_owners: Mutex<Vec<serde_json::Value>>,
    cancellation_dispatch_receipts: Mutex<Vec<serde_json::Value>>,
    cancellation_outcome_records: Mutex<Vec<StateRecord>>,
    cancellation_outcome_writes: AtomicUsize,
    cancellation_outcome_parents: Mutex<Vec<serde_json::Value>>,
    fail_unchanged_prompt_rollback: AtomicBool,
    unchanged_prompt_rollback_failures: AtomicUsize,
    unchanged_prompt_rollback_entered: Semaphore,
    fail_prompt_observation: AtomicBool,
    prompt_observation_failures: AtomicUsize,
    block_prompt_load: AtomicBool,
    prompt_load_blocked: Semaphore,
    prompt_load_release: Semaphore,
    blocked: Semaphore,
    release: Semaphore,
    block_event_commit: AtomicBool,
    obscure_event_commit: AtomicU8,
    event_observation_failures_after_commit: AtomicUsize,
    event_observation_countdown: AtomicUsize,
    event_observation_failures: AtomicUsize,
    event_commit_blocked: Semaphore,
    event_commit_release: Semaphore,
    event_commit_conflicts: AtomicUsize,
    event_commit_attempts: AtomicUsize,
    event_catalog_receipt: Mutex<Option<oneshot::Sender<Result<(), StateStoreError>>>>,
    event_catalog_task: Mutex<Option<tokio::task::Id>>,
    event_catalog_peers_release: Semaphore,
    event_catalog_load_blocked: Semaphore,
    event_catalog_load_release: Semaphore,
    event_catalog_load_conflicts: AtomicUsize,
    event_catalog_load_attempts: AtomicUsize,
    fail_continuation_after_commit: AtomicBool,
    continuation_load_countdown: AtomicUsize,
    continuation_transaction_conflicts: AtomicUsize,
    continuation_put_fault: AtomicU8,
    continuation_commit_conflict: AtomicBool,
    continuation_failures: AtomicUsize,
    obscure_continuation_reconciliation_commit: AtomicBool,
    ambiguous_continuation_reconciliations: AtomicUsize,
    block_workspace_load: AtomicBool,
    block_transaction_begin: AtomicBool,
    transaction_begin_blocked: Semaphore,
    transaction_begin_release: Semaphore,
    bound_session_fault: Mutex<BoundSessionFault>,
    bound_session_commit_attempts: AtomicUsize,
    bound_session_writes: AtomicUsize,
    bound_session_conflicts: AtomicUsize,
    block_bound_session_commit: AtomicBool,
    obscure_bound_session_commit: AtomicBool,
    bound_session_observation_failures_after_commit: AtomicUsize,
    bound_session_observation_countdown: AtomicUsize,
    bound_session_observation_failures: AtomicUsize,
    bound_session_commit_blocked: Semaphore,
    bound_session_commit_release: Semaphore,
    creation_cleanup_observation_failures_after_commit: AtomicUsize,
    creation_cleanup_observation_countdown: AtomicUsize,
    creation_cleanup_observation_failures: AtomicUsize,
    workspace_delete_commit_fault: Mutex<WorkspaceDeleteCommitFault>,
    ambiguous_workspace_deletes: AtomicUsize,
    workspace_delete_commit_attempts: AtomicUsize,
    workspace_delete_observation_failures_after_commit: AtomicUsize,
    workspace_delete_observation_countdown: AtomicUsize,
    workspace_delete_observation_failures: AtomicUsize,
    workspace_create_phase: AtomicU8,
    workspace_create_fault: AtomicU8,
    workspace_create_remaining: AtomicUsize,
    workspace_create_injections: AtomicUsize,
    workspace_create_loads: AtomicUsize,
    workspace_create_key: Mutex<Option<StateKey>>,
    workspace_create_record: Mutex<Option<StateRecord>>,
    conversation_intent: Mutex<Option<ConversationIntentFault>>,
}

#[derive(Clone)]
struct PromptFaultStore {
    inner: StateStore,
    state: Arc<PromptFaultState>,
}

struct PromptFaultTransaction {
    inner: StateTransaction,
    faults: PromptFaultStore,
    wrote_prompt: bool,
    commit_fault: PromptPersistenceFault,
    wrote_event: bool,
    event_catalog_receipt: Option<oneshot::Sender<Result<(), StateStoreError>>>,
    wrote_continuation: bool,
    wrote_bound_session: bool,
    inserted_cleanup: bool,
    deleted_workspace: bool,
    wrote_workspace: bool,
    conversation_intent: bool,
    held_waiter_commit: bool,
    failed_prompt_read: bool,
    cancellation_request_read_conflict: bool,
    cancellation_request_task: bool,
    cancellation_request_record: Option<StateRecord>,
    failed_persistence: bool,
    failed_native_cleanup: bool,
    native_cleanup_retry_task: Option<tokio::task::Id>,
    wrote_native_cleanup: bool,
    read_unrequested_admission: bool,
    read_missing_prompt: bool,
    dispatch_record: Option<serde_json::Value>,
    prepared_cancel_selected: bool,
    prepared_cancel_read: bool,
    prepared_cancel_activation_read: bool,
    prepared_cancel_event: Option<StateRecord>,
    prepared_cancel_deleted: bool,
    recovered_waiter_written: bool,
    prepared_cancel_intent: Option<StateRecord>,
}

impl PromptCommitFault {
    fn from_byte(value: u8) -> Self {
        match value {
            1 => Self::Conflict,
            2 => Self::Unavailable,
            3 => Self::BlockThenCommit,
            4 => Self::UnavailableAfterCommit,
            5 => Self::BlockThenConflict,
            6 => Self::BlockThenUnavailableAfterCommit,
            _ => Self::None,
        }
    }
}

impl PromptFaultStore {
    fn new(inner: StateStore) -> Self {
        Self {
            inner,
            state: Arc::new(PromptFaultState {
                mode: AtomicU8::new(PromptCommitFault::None as u8),
                target: AtomicUsize::new(0),
                prompt_commits: AtomicUsize::new(0),
                waiter_fault: AtomicU8::new(0),
                waiter_fault_attempts: AtomicUsize::new(0),
                block_waiter_commit: AtomicBool::new(false),
                persistence_fault: Mutex::new(PromptPersistenceFault::None),
                persistence_foreign: AtomicBool::new(false),
                persistence_conflict: AtomicBool::new(false),
                persistence_rollback_failure: AtomicBool::new(false),
                persistence_foreign_next: AtomicBool::new(false),
                persistence_attempts: AtomicUsize::new(0),
                native_cleanup_armed: AtomicBool::new(false),
                native_cleanup_receipt: Mutex::new(None),
                prepared_cancel_receipt: Mutex::new(None),
                reconciliation_observer: Mutex::new(None),
                admission_writes: AtomicUsize::new(0),
                terminal_writes: AtomicUsize::new(0),
                cancellation_request_task: Mutex::new(None),
                cancellation_request_writes: AtomicUsize::new(0),
                cancellation_request_commits: AtomicUsize::new(0),
                cancellation_request_reads_entered: Semaphore::new(0),
                cancellation_request_admission_committed: Semaphore::new(0),
                cancellation_request_advanced: AtomicBool::new(false),
                cancellation_request_uncommitted: AtomicBool::new(false),
                cancellation_request_advance_observed: Semaphore::new(0),
                cancellation_claim_writes: AtomicUsize::new(0),
                cancellation_claim_owners: Mutex::new(Vec::new()),
                cancellation_dispatch_writes: AtomicUsize::new(0),
                cancellation_dispatch_owners: Mutex::new(Vec::new()),
                cancellation_dispatch_receipts: Mutex::new(Vec::new()),
                cancellation_outcome_records: Mutex::new(Vec::new()),
                cancellation_outcome_writes: AtomicUsize::new(0),
                cancellation_outcome_parents: Mutex::new(Vec::new()),
                fail_unchanged_prompt_rollback: AtomicBool::new(false),
                unchanged_prompt_rollback_failures: AtomicUsize::new(0),
                unchanged_prompt_rollback_entered: Semaphore::new(0),
                fail_prompt_observation: AtomicBool::new(false),
                prompt_observation_failures: AtomicUsize::new(0),
                block_prompt_load: AtomicBool::new(false),
                prompt_load_blocked: Semaphore::new(0),
                prompt_load_release: Semaphore::new(0),
                blocked: Semaphore::new(0),
                release: Semaphore::new(0),
                block_event_commit: AtomicBool::new(false),
                obscure_event_commit: AtomicU8::new(PromptCommitFault::None as u8),
                event_observation_failures_after_commit: AtomicUsize::new(0),
                event_observation_countdown: AtomicUsize::new(0),
                event_observation_failures: AtomicUsize::new(0),
                event_commit_blocked: Semaphore::new(0),
                event_commit_release: Semaphore::new(0),
                event_commit_conflicts: AtomicUsize::new(0),
                event_commit_attempts: AtomicUsize::new(0),
                event_catalog_receipt: Mutex::new(None),
                event_catalog_task: Mutex::new(None),
                event_catalog_peers_release: Semaphore::new(0),
                event_catalog_load_blocked: Semaphore::new(0),
                event_catalog_load_release: Semaphore::new(0),
                event_catalog_load_conflicts: AtomicUsize::new(0),
                event_catalog_load_attempts: AtomicUsize::new(0),
                fail_continuation_after_commit: AtomicBool::new(false),
                continuation_load_countdown: AtomicUsize::new(0),
                continuation_transaction_conflicts: AtomicUsize::new(0),
                continuation_put_fault: AtomicU8::new(PromptCommitFault::None as u8),
                continuation_commit_conflict: AtomicBool::new(false),
                continuation_failures: AtomicUsize::new(0),
                obscure_continuation_reconciliation_commit: AtomicBool::new(false),
                ambiguous_continuation_reconciliations: AtomicUsize::new(0),
                block_workspace_load: AtomicBool::new(false),
                block_transaction_begin: AtomicBool::new(false),
                transaction_begin_blocked: Semaphore::new(0),
                transaction_begin_release: Semaphore::new(0),
                bound_session_fault: Mutex::new(BoundSessionFault::None),
                bound_session_commit_attempts: AtomicUsize::new(0),
                bound_session_writes: AtomicUsize::new(0),
                bound_session_conflicts: AtomicUsize::new(0),
                block_bound_session_commit: AtomicBool::new(false),
                obscure_bound_session_commit: AtomicBool::new(false),
                bound_session_observation_failures_after_commit: AtomicUsize::new(0),
                bound_session_observation_countdown: AtomicUsize::new(0),
                bound_session_observation_failures: AtomicUsize::new(0),
                bound_session_commit_blocked: Semaphore::new(0),
                bound_session_commit_release: Semaphore::new(0),
                creation_cleanup_observation_failures_after_commit: AtomicUsize::new(0),
                creation_cleanup_observation_countdown: AtomicUsize::new(0),
                creation_cleanup_observation_failures: AtomicUsize::new(0),
                workspace_delete_commit_fault: Mutex::new(WorkspaceDeleteCommitFault::None),
                ambiguous_workspace_deletes: AtomicUsize::new(0),
                workspace_delete_commit_attempts: AtomicUsize::new(0),
                workspace_delete_observation_failures_after_commit: AtomicUsize::new(0),
                workspace_delete_observation_countdown: AtomicUsize::new(0),
                workspace_delete_observation_failures: AtomicUsize::new(0),
                workspace_create_phase: AtomicU8::new(WorkspaceCreatePhase::None as u8),
                workspace_create_fault: AtomicU8::new(PromptCommitFault::None as u8),
                workspace_create_remaining: AtomicUsize::new(0),
                workspace_create_injections: AtomicUsize::new(0),
                workspace_create_loads: AtomicUsize::new(0),
                workspace_create_key: Mutex::new(None),
                workspace_create_record: Mutex::new(None),
                conversation_intent: Mutex::new(None),
            }),
        }
    }

    fn persistence_fault(&self) -> PromptPersistenceFault {
        *self
            .state
            .persistence_fault
            .lock()
            .expect("persistence fault")
    }

    fn arm_prepared_cancel(
        &self,
        fault: PreparedCancelFault,
        task: tokio::task::Id,
        key: StateKey,
    ) {
        assert_eq!(key.namespace().as_str(), OPENCODE_PROMPT_NAMESPACE);
        let mut receipt = self.state.prepared_cancel_receipt.lock().unwrap();
        assert!(
            receipt.is_none(),
            "each case owns one prepared-cancellation observation"
        );
        *receipt = Some(PreparedCancelReceipt::new(
            fault,
            Some(task),
            key,
            PromptCleanupStage::Prepared,
        ));
    }

    fn arm_reconciliation_observer(&self, task: tokio::task::Id, key: StateKey) {
        let mut current = self.state.reconciliation_observer.lock().unwrap();
        assert!(current.is_none(), "one exact overlapping observer per case");
        *current = Some(ReconciliationObserver {
            task,
            key,
            entered: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
        });
    }

    async fn wait_reconciliation_observer(&self) {
        let observer = self
            .state
            .reconciliation_observer
            .lock()
            .unwrap()
            .clone()
            .expect("the selected overlapping observer must be armed");
        observer.entered.acquire().await.unwrap().forget();
    }

    fn release_reconciliation_observer(&self) {
        if let Some(observer) = self.state.reconciliation_observer.lock().unwrap().as_ref() {
            observer.release.add_permits(1);
        }
    }

    fn arm_abort_reconciliation(
        &self,
        fault: PreparedCancelFault,
        task: tokio::task::Id,
        key: StateKey,
    ) {
        self.arm_prepared_cancel(fault, task, key);
        self.state
            .prepared_cancel_receipt
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .stage = PromptCleanupStage::AwaitNonPreparedRollback;
    }

    fn arm_recovered_waiter(
        &self,
        fault: PreparedCancelFault,
        task: tokio::task::Id,
        key: StateKey,
    ) {
        self.arm_abort_reconciliation(fault, task, key);
        self.state
            .prepared_cancel_receipt
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .stage = PromptCleanupStage::AwaitWaiterRollback;
    }

    fn arm_terminal_reconciliation_task(
        &self,
        fault: PreparedCancelFault,
        task: tokio::task::Id,
        key: StateKey,
    ) {
        self.arm_prepared_cancel(fault, task, key);
        self.state
            .prepared_cancel_receipt
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .stage = PromptCleanupStage::TerminalMutation;
    }

    fn arm_terminal_delivery_cleanup(&self, fault: PreparedCancelFault, key: StateKey) {
        assert_eq!(key.namespace().as_str(), OPENCODE_PROMPT_NAMESPACE);
        let mut receipt = self.state.prepared_cancel_receipt.lock().unwrap();
        assert!(
            receipt.is_none(),
            "one terminal-delivery observation per case"
        );
        *receipt = Some(PreparedCancelReceipt::new(
            fault,
            None,
            key,
            PromptCleanupStage::TerminalMutation,
        ));
    }

    fn arm_terminal_event_reconciliation(
        &self,
        fault: PreparedCancelFault,
        prompt_key: StateKey,
        continuation_key: StateKey,
    ) {
        assert_eq!(prompt_key.namespace().as_str(), OPENCODE_PROMPT_NAMESPACE);
        assert_eq!(
            continuation_key.namespace().as_str(),
            OPENCODE_CONTINUATION_NAMESPACE
        );
        assert_ne!(
            fault,
            PreparedCancelFault::BeginConflictOnce,
            "event activation does not identify intervening transaction admission"
        );
        let mut receipt = self.state.prepared_cancel_receipt.lock().unwrap();
        assert!(
            receipt.is_none(),
            "each case owns one prompt-cleanup observation"
        );
        *receipt = Some(PreparedCancelReceipt::new(
            fault,
            None,
            prompt_key,
            PromptCleanupStage::AwaitEventSettlement { continuation_key },
        ));
    }

    fn disarm_prepared_cancel(&self) {
        if let Some(receipt) = self.state.prepared_cancel_receipt.lock().unwrap().as_mut() {
            receipt.enabled = false;
        }
        self.state
            .persistence_rollback_failure
            .store(false, Ordering::SeqCst);
    }

    fn prepared_cancel_receipt(&self) -> PreparedCancelReceipt {
        self.state
            .prepared_cancel_receipt
            .lock()
            .unwrap()
            .as_ref()
            .expect("prepared cancellation must be armed")
            .clone()
    }

    async fn observe_prepared_cancel_receipt(
        &self,
        key: &StateKey,
        snapshot: &mut Option<StateRecordSnapshot>,
    ) -> Result<(), StateStoreError> {
        let hold = {
            let mut guard = self.state.prepared_cancel_receipt.lock().unwrap();
            if let Some(receipt) = guard.as_mut()
                && receipt.enabled
                && receipt.uncertain_commit
                && receipt.key == *key
                && receipt.task.is_some()
                && tokio::task::try_id() == receipt.task
            {
                receipt.receipt_reads += 1;
                if receipt.unavailable_receipt {
                    receipt.unavailable_receipt = false;
                    receipt.receipt_read_failures += 1;
                    return Err(StateStoreError::unavailable(
                        "load",
                        "injected prompt cleanup receipt read failure",
                    ));
                }
                receipt.absent_receipts += usize::from(snapshot.is_none());
                if receipt.foreign_receipt
                    && let Some(snapshot) = snapshot
                {
                    Arc::make_mut(&mut snapshot.record.value)["message_id"] =
                        serde_json::json!("prompt_foreign_prepared_cancel_receipt");
                }
                if receipt.requested_fault == PreparedCancelFault::CommitUnavailableMissingReceipt {
                    *snapshot = None;
                } else if receipt.requested_fault
                    == PreparedCancelFault::CommitUnavailableMalformedReceipt
                    && let Some(snapshot) = snapshot
                {
                    Arc::make_mut(&mut snapshot.record.value)["phase"] =
                        serde_json::json!("invalid");
                }
                if receipt.attempted_record.as_ref().is_some_and(|expected| {
                    snapshot
                        .as_ref()
                        .is_some_and(|snapshot| snapshot.record == *expected)
                }) {
                    receipt.exact_receipts += 1;
                    if receipt.stage == PromptCleanupStage::RecoveredWaiter {
                        receipt.active = false;
                    }
                }
                std::mem::take(&mut receipt.hold_receipt)
            } else {
                false
            }
        };
        if hold {
            self.state.blocked.add_permits(1);
            let _released = self
                .state
                .release
                .acquire()
                .await
                .expect("uncertain receipt observation must be released");
        }
        Ok(())
    }

    fn arm_native_cleanup_receipt(&self) {
        if self.persistence_fault().is_native_cleanup() {
            self.state
                .native_cleanup_armed
                .store(true, Ordering::SeqCst);
        }
    }

    fn record_native_cleanup_dispatch(&self, request_id: &str) {
        if let Some(receipt) = self.state.native_cleanup_receipt.lock().unwrap().as_mut()
            && receipt
                .before
                .record
                .value
                .pointer("/request/question/id")
                .or_else(|| {
                    receipt
                        .before
                        .record
                        .value
                        .pointer("/request/permission/id")
                })
                .and_then(serde_json::Value::as_str)
                == Some(request_id)
        {
            receipt.dispatch_readbacks.push(receipt.readbacks);
            receipt.dispatch_before_commit += usize::from(receipt.commits == 0);
        }
    }

    fn observe_native_cleanup_receipt(&self, snapshot: Option<&StateRecordSnapshot>) {
        if !self.state.native_cleanup_armed.load(Ordering::SeqCst) {
            return;
        }
        let mut receipt = self.state.native_cleanup_receipt.lock().unwrap();
        if let Some(receipt) = receipt.as_mut()
            && receipt.committed_receipt_lost
            && snapshot.is_some_and(|snapshot| receipt.attempt.as_ref() == Some(&snapshot.record))
        {
            receipt.readbacks += 1;
        }
    }

    fn observe_cancellation_request_advance(&self, snapshot: Option<&StateRecordSnapshot>) {
        let selected = self
            .state
            .cancellation_request_task
            .lock()
            .unwrap()
            .is_some_and(|id| tokio::task::try_id() == Some(id));
        if selected
            && self
                .state
                .cancellation_request_advanced
                .load(Ordering::SeqCst)
            && snapshot.is_some_and(|snapshot| {
                snapshot.record.key.namespace().as_str() == OPENCODE_PROMPT_NAMESPACE
                    && snapshot.record.value["phase"]["phase"] == "terminal"
                    && snapshot.record.value["phase"]["outcome"]["outcome"] == "cancelled"
                    && snapshot.record.value["cancellation"]["state"] == "unnecessary"
            })
        {
            self.state
                .cancellation_request_advanced
                .store(false, Ordering::SeqCst);
            self.state
                .cancellation_request_advance_observed
                .add_permits(1);
        }
    }

    fn set_persistence_fault(&self, fault: PromptPersistenceFault) {
        *self
            .state
            .persistence_fault
            .lock()
            .expect("persistence fault") = fault;
    }

    fn storage(&self) -> AgentStorage<PromptFaultStore> {
        AgentStorage::new(self.clone())
    }

    fn before_workspace_create(
        &self,
        phase: WorkspaceCreatePhase,
    ) -> Result<bool, StateStoreError> {
        if self.state.workspace_create_phase.load(Ordering::SeqCst) != phase as u8
            || self
                .state
                .workspace_create_remaining
                .try_update(
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    |remaining| match remaining {
                        0 => None,
                        usize::MAX => Some(usize::MAX),
                        _ => Some(remaining - 1),
                    },
                )
                .is_err()
        {
            return Ok(false);
        }
        self.state
            .workspace_create_injections
            .fetch_add(1, Ordering::SeqCst);
        let operation = match phase {
            WorkspaceCreatePhase::None => unreachable!("a workspace fault must have a phase"),
            WorkspaceCreatePhase::Begin => "begin",
            WorkspaceCreatePhase::Load => "load",
            WorkspaceCreatePhase::Put => "put",
            WorkspaceCreatePhase::Commit => "commit",
        };
        match PromptCommitFault::from_byte(self.state.workspace_create_fault.load(Ordering::SeqCst))
        {
            PromptCommitFault::Conflict => Err(StateStoreError::transaction_conflict(
                operation,
                "injected workspace creation conflict",
            )),
            PromptCommitFault::Unavailable => Err(StateStoreError::unavailable(
                operation,
                "injected workspace creation failure",
            )),
            PromptCommitFault::UnavailableAfterCommit => Ok(true),
            _ => unreachable!("workspace creation requires an explicitly selected fault"),
        }
    }

    fn arm(&self, fault: PromptCommitFault, target: usize) {
        assert!(fault != PromptCommitFault::None);
        assert!(target > 0);
        self.state.prompt_commits.store(0, Ordering::SeqCst);
        self.state.mode.store(fault as u8, Ordering::SeqCst);
        self.state.target.store(target, Ordering::SeqCst);
    }

    async fn before_commit(&self, wrote_prompt: bool) -> PromptCommitFault {
        if !wrote_prompt || self.state.target.load(Ordering::SeqCst) == 0 {
            return PromptCommitFault::None;
        }
        let ordinal = self.state.prompt_commits.fetch_add(1, Ordering::SeqCst) + 1;
        if ordinal != self.state.target.load(Ordering::SeqCst) {
            return PromptCommitFault::None;
        }
        let fault = PromptCommitFault::from_byte(
            self.state
                .mode
                .swap(PromptCommitFault::None as u8, Ordering::SeqCst),
        );
        if matches!(
            fault,
            PromptCommitFault::BlockThenCommit
                | PromptCommitFault::BlockThenConflict
                | PromptCommitFault::BlockThenUnavailableAfterCommit
        ) {
            self.state.blocked.add_permits(1);
            self.state
                .release
                .acquire()
                .await
                .expect("prompt fault release must remain open")
                .forget();
        }
        fault
    }

    async fn wait_until_blocked(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.state.blocked.acquire())
            .await
            .expect("prompt commit must reach its deterministic block")
            .expect("prompt fault block must remain open")
            .forget();
    }

    fn release(&self) {
        self.state.release.add_permits(1);
    }

    fn block_next_workspace_load(&self) {
        self.state
            .block_workspace_load
            .store(true, Ordering::SeqCst);
    }

    fn block_next_event_commit(&self) {
        self.state.block_event_commit.store(true, Ordering::SeqCst);
    }

    fn obscure_next_event_commit(&self) {
        self.state.obscure_event_commit.store(
            PromptCommitFault::UnavailableAfterCommit as u8,
            Ordering::SeqCst,
        );
    }

    fn fail_next_event_commit_observation(&self) {
        self.state
            .event_observation_failures_after_commit
            .store(1, Ordering::SeqCst);
    }

    fn fail_event_commit_observations(&self, failures: usize) {
        assert!(failures > 0, "at least one event observation must fail");
        self.state
            .event_observation_failures_after_commit
            .store(failures, Ordering::SeqCst);
    }

    fn event_observation_failures(&self) -> usize {
        self.state.event_observation_failures.load(Ordering::SeqCst)
    }

    async fn wait_for_event_observation_failures(&self, failures: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.event_observation_failures() < failures {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("event commit observation failure must become visible");
    }

    fn obscure_next_continuation_reconciliation_commit(&self) {
        self.state
            .obscure_continuation_reconciliation_commit
            .store(true, Ordering::SeqCst);
    }

    fn ambiguous_continuation_reconciliations(&self) -> usize {
        self.state
            .ambiguous_continuation_reconciliations
            .load(Ordering::SeqCst)
    }

    fn block_next_transaction_begin(&self) {
        self.state
            .block_transaction_begin
            .store(true, Ordering::SeqCst);
    }

    async fn wait_until_transaction_begin_blocked(&self) {
        tokio::time::timeout(
            Duration::from_secs(5),
            self.state.transaction_begin_blocked.acquire(),
        )
        .await
        .expect("catalog binding must reach its transaction boundary")
        .expect("catalog transaction fault block must remain open")
        .forget();
    }

    fn release_transaction_begin(&self) {
        self.state.transaction_begin_release.add_permits(1);
    }

    fn block_next_bound_session_commit(&self) {
        assert!(
            !self
                .state
                .block_bound_session_commit
                .swap(true, Ordering::SeqCst),
            "only one bound-session commit may be blocked"
        );
    }

    fn obscure_next_bound_session_commit(&self) {
        self.state
            .obscure_bound_session_commit
            .store(true, Ordering::SeqCst);
    }

    fn fail_next_bound_session_commit_observation(&self) {
        self.state
            .bound_session_observation_failures_after_commit
            .store(1, Ordering::SeqCst);
    }

    fn fail_next_close_commit_observation(&self) {
        self.state
            .bound_session_observation_countdown
            .store(1, Ordering::SeqCst);
    }

    fn fail_bound_session_commit_observation_until_released(&self) {
        self.state
            .bound_session_observation_failures_after_commit
            .store(usize::MAX, Ordering::SeqCst);
    }

    fn release_bound_session_commit_observation(&self) {
        self.state
            .bound_session_observation_countdown
            .store(0, Ordering::SeqCst);
    }

    fn bound_session_observation_failures(&self) -> usize {
        self.state
            .bound_session_observation_failures
            .load(Ordering::SeqCst)
    }

    async fn wait_until_bound_session_commit_blocked(&self) {
        tokio::time::timeout(
            Duration::from_secs(5),
            self.state.bound_session_commit_blocked.acquire(),
        )
        .await
        .expect("bound-session commit must reach its deterministic block")
        .expect("bound-session commit block must remain open")
        .forget();
    }

    fn release_bound_session_commit(&self) {
        self.state.bound_session_commit_release.add_permits(1);
    }

    fn fail_creation_cleanup_observation_until_released(&self) {
        self.state
            .creation_cleanup_observation_failures_after_commit
            .store(usize::MAX, Ordering::SeqCst);
    }

    fn release_creation_cleanup_observation(&self) {
        self.state
            .creation_cleanup_observation_countdown
            .store(0, Ordering::SeqCst);
    }

    fn creation_cleanup_observation_failures(&self) -> usize {
        self.state
            .creation_cleanup_observation_failures
            .load(Ordering::SeqCst)
    }

    fn obscure_next_workspace_delete_commit(&self) {
        *self.state.workspace_delete_commit_fault.lock().unwrap() =
            WorkspaceDeleteCommitFault::UnavailableAfterCommit;
    }

    fn ambiguous_workspace_deletes(&self) -> usize {
        self.state
            .ambiguous_workspace_deletes
            .load(Ordering::SeqCst)
    }

    fn fail_next_workspace_delete_commit_observation(&self) {
        self.state
            .workspace_delete_observation_failures_after_commit
            .store(1, Ordering::SeqCst);
    }

    fn fail_workspace_delete_commit_observation_until_released(&self) {
        self.state
            .workspace_delete_observation_failures_after_commit
            .store(usize::MAX, Ordering::SeqCst);
    }

    fn release_workspace_delete_commit_observation(&self) {
        self.state
            .workspace_delete_observation_countdown
            .store(0, Ordering::SeqCst);
    }

    fn workspace_delete_commit_attempts(&self) -> usize {
        self.state
            .workspace_delete_commit_attempts
            .load(Ordering::SeqCst)
    }

    fn workspace_delete_observation_failures(&self) -> usize {
        self.state
            .workspace_delete_observation_failures
            .load(Ordering::SeqCst)
    }

    async fn before_bound_session_commit(&self, wrote_bound_session: bool) {
        if wrote_bound_session {
            self.state
                .bound_session_commit_attempts
                .fetch_add(1, Ordering::SeqCst);
            let conflict = {
                let mut fault = self.state.bound_session_fault.lock().unwrap();
                match *fault {
                    BoundSessionFault::Conflicts(remaining) if remaining > 0 => {
                        *fault = BoundSessionFault::Conflicts(remaining - 1);
                        true
                    }
                    _ => false,
                }
            };
            if conflict {
                let count = self
                    .state
                    .bound_session_conflicts
                    .fetch_add(1, Ordering::SeqCst)
                    + 1;
                let mut competing = self.inner.begin_transaction().await.unwrap();
                competing
                    .put(StateRecord::new(
                        StateKey::new("bound_session_conflict_probe", "receipt").unwrap(),
                        serde_json::json!({"attempt": count}),
                    ))
                    .await
                    .unwrap();
                competing
                    .commit()
                    .await
                    .expect("the competing Lance commit must publish before binding");
            }
        }
        if wrote_bound_session
            && self
                .state
                .block_bound_session_commit
                .swap(false, Ordering::SeqCst)
        {
            self.state.bound_session_commit_blocked.add_permits(1);
            self.state
                .bound_session_commit_release
                .acquire()
                .await
                .expect("bound-session commit release must remain open")
                .forget();
        }
    }

    async fn wait_until_event_commit_blocked(&self) {
        tokio::time::timeout(
            Duration::from_secs(5),
            self.state.event_commit_blocked.acquire(),
        )
        .await
        .expect("event progress commit must reach its deterministic block")
        .expect("event progress fault block must remain open")
        .forget();
    }

    fn release_event_commit(&self) {
        self.state.block_event_commit.store(false, Ordering::SeqCst);
        self.state.event_commit_release.add_permits(1);
    }

    fn conflict_event_commits(&self, conflicts: usize) {
        assert!(conflicts > 0);
        self.state.event_commit_attempts.store(0, Ordering::SeqCst);
        self.state
            .event_commit_conflicts
            .store(conflicts, Ordering::SeqCst);
    }

    fn clear_event_commit_conflicts(&self) {
        self.state.event_commit_conflicts.store(0, Ordering::SeqCst);
    }

    fn event_commit_attempts(&self) -> usize {
        self.state.event_commit_attempts.load(Ordering::SeqCst)
    }

    async fn wait_for_event_commit_attempts(&self, attempts: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.event_commit_attempts() < attempts {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("event transaction retries must make bounded progress");
    }

    fn conflict_event_catalog_loads(&self, conflicts: usize) {
        assert!(conflicts > 0);
        self.state
            .event_catalog_load_attempts
            .store(0, Ordering::SeqCst);
        self.state
            .event_catalog_load_conflicts
            .store(conflicts, Ordering::SeqCst);
    }

    fn block_next_event_catalog_load(&self) -> oneshot::Receiver<Result<(), StateStoreError>> {
        let (sender, receiver) = oneshot::channel();
        assert!(
            self.state
                .event_catalog_receipt
                .lock()
                .unwrap()
                .replace(sender)
                .is_none(),
            "only one event catalog load may be blocked"
        );
        receiver
    }

    fn release_event_catalog_peers(&self) {
        self.state.event_catalog_task.lock().unwrap().take();
        self.state.event_catalog_peers_release.add_permits(1);
    }

    async fn wait_until_event_catalog_load_blocked(&self) {
        tokio::time::timeout(
            Duration::from_secs(5),
            self.state.event_catalog_load_blocked.acquire(),
        )
        .await
        .expect("event catalog load must reach its deterministic block")
        .expect("event catalog load block must remain open")
        .forget();
    }

    fn release_event_catalog_load(&self) {
        self.state.event_catalog_load_release.add_permits(1);
    }

    fn event_catalog_load_attempts(&self) -> usize {
        self.state
            .event_catalog_load_attempts
            .load(Ordering::SeqCst)
    }

    fn before_event_catalog_load(&self, key: &StateKey) -> bool {
        if key.namespace().as_str() != OPENCODE_CATALOG_NAMESPACE {
            return false;
        }
        self.state
            .event_catalog_load_attempts
            .fetch_add(1, Ordering::SeqCst);
        self.state
            .event_catalog_load_conflicts
            .try_update(
                Ordering::SeqCst,
                Ordering::SeqCst,
                |remaining| match remaining {
                    0 => None,
                    usize::MAX => Some(usize::MAX),
                    _ => Some(remaining - 1),
                },
            )
            .is_ok()
    }

    fn fail_next_post_commit_continuation_reconciliation(&self) {
        self.state
            .fail_continuation_after_commit
            .store(true, Ordering::SeqCst);
    }

    fn conflict_next_continuation_transaction_load(&self) {
        self.state
            .continuation_transaction_conflicts
            .store(1, Ordering::SeqCst);
    }

    fn continuation_failures(&self) -> usize {
        self.state.continuation_failures.load(Ordering::SeqCst)
    }

    fn finish_commit(&self, wrote_continuation: bool) {
        if wrote_continuation
            && self
                .state
                .fail_continuation_after_commit
                .swap(false, Ordering::SeqCst)
        {
            self.state
                .continuation_load_countdown
                .store(1, Ordering::SeqCst);
        }
    }

    async fn before_event_commit(&self, wrote_event: bool) -> bool {
        if !wrote_event {
            return false;
        }
        self.state
            .event_commit_attempts
            .fetch_add(1, Ordering::SeqCst);
        if self.state.block_event_commit.load(Ordering::SeqCst) {
            self.state.event_commit_blocked.add_permits(1);
            self.state
                .event_commit_release
                .acquire()
                .await
                .expect("event progress fault release must remain open")
                .forget();
        }
        self.state
            .event_commit_conflicts
            .try_update(
                Ordering::SeqCst,
                Ordering::SeqCst,
                |remaining| match remaining {
                    0 => None,
                    usize::MAX => Some(usize::MAX),
                    _ => Some(remaining - 1),
                },
            )
            .is_ok()
    }

    fn prompt_key(conversation_id: &ConversationId) -> StateKey {
        StateKey::new(OPENCODE_PROMPT_NAMESPACE, conversation_id.as_str())
            .expect("OpenCode prompt state key must be valid")
    }

    async fn prompt_snapshot(
        &self,
        conversation_id: &ConversationId,
    ) -> Option<StateRecordSnapshot> {
        self.inner
            .load(Self::prompt_key(conversation_id))
            .await
            .expect("OpenCode prompt state must be readable")
    }

    async fn restore_prompt(&self, record: StateRecord) {
        let mut transaction = self
            .inner
            .begin_transaction()
            .await
            .expect("OpenCode prompt restoration must begin");
        transaction
            .put(record)
            .await
            .expect("OpenCode prompt restoration must stage");
        transaction
            .commit()
            .await
            .expect("OpenCode prompt restoration must commit");
    }

    async fn delete_prompt(&self, conversation_id: &ConversationId) {
        let mut transaction = self
            .inner
            .begin_transaction()
            .await
            .expect("OpenCode prompt deletion must begin");
        transaction
            .delete(Self::prompt_key(conversation_id))
            .await
            .expect("OpenCode prompt deletion must stage");
        transaction
            .commit()
            .await
            .expect("OpenCode prompt deletion must commit");
    }
}

impl StateStoreBackend for PromptFaultStore {
    type Transaction = PromptFaultTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        {
            let mut receipt = self.state.conversation_intent.lock().unwrap();
            if let Some(receipt) = receipt.as_mut().filter(|receipt| {
                receipt.fail_forget_read
                    && !receipt.active
                    && receipt.discard_commits == 1
                    && receipt.forget_reads == 0
                    && receipt.creation_task.is_some()
                    && receipt.creation_task == tokio::task::try_id()
            }) {
                // Only this accepted creation's post-discard read is faulted.
                // No private key spelling or unrelated owner read selects it.
                receipt.forget_reads += 1;
                return Err(StateStoreError::unavailable(
                    "load",
                    "PRIVATE_FORGET_SETTLEMENT",
                ));
            }
        }
        let namespace = key.namespace().as_str();
        if namespace == OPENCODE_PROMPT_NAMESPACE
            && self.state.fail_prompt_observation.load(Ordering::SeqCst)
        {
            self.state
                .prompt_observation_failures
                .fetch_add(1, Ordering::SeqCst);
            return Err(StateStoreError::unavailable(
                "load",
                "PRIVATE_PROMPT_OBSERVATION_FAILURE",
            ));
        }
        let event_failure = namespace == OPENCODE_EVENT_PROGRESS_NAMESPACE
            && self
                .state
                .event_observation_countdown
                .try_update(
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    |remaining| match remaining {
                        0 => None,
                        usize::MAX => Some(usize::MAX),
                        _ => Some(remaining - 1),
                    },
                )
                .is_ok();
        let bound_session_failure = namespace == OPENCODE_SESSION_NAMESPACE
            && self
                .state
                .bound_session_observation_countdown
                .try_update(
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    |remaining| match remaining {
                        0 => None,
                        usize::MAX => Some(usize::MAX),
                        _ => Some(remaining - 1),
                    },
                )
                .is_ok();
        let continuation_failure = namespace == OPENCODE_CONTINUATION_NAMESPACE
            && self
                .state
                .continuation_load_countdown
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok_and(|remaining| remaining == 1);
        let workspace_delete_failure = namespace == WORKSPACE_NAMESPACE
            && self
                .state
                .workspace_delete_observation_countdown
                .try_update(
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    |remaining| match remaining {
                        0 => None,
                        usize::MAX => Some(usize::MAX),
                        _ => Some(remaining - 1),
                    },
                )
                .is_ok();
        let creation_cleanup_failure = matches!(
            namespace,
            CONVERSATION_NAMESPACE | OPENCODE_SESSION_NAMESPACE | OPENCODE_CLEANUP_NAMESPACE
        ) && self
            .state
            .creation_cleanup_observation_countdown
            .try_update(
                Ordering::SeqCst,
                Ordering::SeqCst,
                |remaining| match remaining {
                    0 => None,
                    usize::MAX => Some(usize::MAX),
                    _ => Some(remaining - 1),
                },
            )
            .is_ok();
        if event_failure
            || bound_session_failure
            || continuation_failure
            || workspace_delete_failure
            || creation_cleanup_failure
        {
            if event_failure {
                self.state
                    .event_observation_failures
                    .fetch_add(1, Ordering::SeqCst);
            }
            if bound_session_failure {
                self.state
                    .bound_session_observation_failures
                    .fetch_add(1, Ordering::SeqCst);
            }
            if continuation_failure {
                self.state
                    .continuation_failures
                    .fetch_add(1, Ordering::SeqCst);
            }
            if workspace_delete_failure {
                self.state
                    .workspace_delete_observation_failures
                    .fetch_add(1, Ordering::SeqCst);
            }
            if creation_cleanup_failure {
                self.state
                    .creation_cleanup_observation_failures
                    .fetch_add(1, Ordering::SeqCst);
            }
            return Err(StateStoreError::unavailable(
                "load",
                "injected post-commit exact observation failure",
            ));
        }
        if key.namespace().as_str() == WORKSPACE_NAMESPACE
            && self
                .state
                .block_workspace_load
                .swap(false, Ordering::SeqCst)
        {
            self.state.blocked.add_permits(1);
            self.state
                .release
                .acquire()
                .await
                .expect("workspace load release must remain open")
                .forget();
        }
        let mut snapshot = self.inner.load(key.clone()).await?;
        self.observe_prepared_cancel_receipt(&key, &mut snapshot)
            .await?;
        self.observe_cancellation_request_advance(snapshot.as_ref());
        self.observe_native_cleanup_receipt(snapshot.as_ref());
        Ok(snapshot)
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        let response = self.inner.list(request).await?;
        for snapshot in &response.records {
            self.observe_native_cleanup_receipt(Some(snapshot));
        }
        Ok(response)
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        let prepared_cancel_selected = {
            let mut receipt = self.state.prepared_cancel_receipt.lock().unwrap();
            if let Some(receipt) = receipt.as_mut()
                && receipt.enabled
                && receipt.active
                && receipt.stage != PromptCleanupStage::TerminalMutation
                && receipt.task.is_some()
                && tokio::task::try_id() == receipt.task
            {
                receipt.begins += 1;
                receipt.accesses_before_rollback += usize::from(receipt.awaiting_rollback);
                if !matches!(
                    receipt.stage,
                    PromptCleanupStage::AwaitNonPreparedRollback
                        | PromptCleanupStage::AwaitWaiterRollback
                ) && receipt.fault == PreparedCancelFault::BeginConflictOnce
                {
                    receipt.fault = PreparedCancelFault::None;
                    receipt.faults += 1;
                    return Err(StateStoreError::transaction_conflict(
                        "begin_transaction",
                        "injected prepared cancellation admission conflict",
                    ));
                }
                true
            } else {
                false
            }
        };
        let selected_begin = {
            let mut receipt = self.state.native_cleanup_receipt.lock().unwrap();
            receipt.as_mut().is_some_and(|receipt| {
                if self.state.native_cleanup_armed.load(Ordering::SeqCst)
                    && receipt.begin_conflict_task.is_some()
                    && receipt.begin_conflict_task == tokio::task::try_id()
                {
                    assert!(
                        receipt.rollback_confirmed,
                        "the selected retry cannot begin before rollback settles"
                    );
                    receipt.begin_conflict_task = None;
                    receipt.begin_conflicts += 1;
                    true
                } else {
                    false
                }
            })
        };
        if selected_begin {
            self.state
                .persistence_attempts
                .fetch_add(1, Ordering::SeqCst);
            return Err(StateStoreError::transaction_conflict(
                "begin_transaction",
                "injected native cleanup retry admission conflict",
            ));
        }
        if let Some(intent) = self.state.conversation_intent.lock().unwrap().as_mut() {
            intent.before(ConversationIntentPhase::Begin)?;
        }
        self.before_workspace_create(WorkspaceCreatePhase::Begin)?;
        let cancellation_request_task = self
            .state
            .cancellation_request_task
            .lock()
            .unwrap()
            .is_some_and(|id| tokio::task::try_id() == Some(id));
        let catalog_peer = self
            .state
            .event_catalog_task
            .lock()
            .unwrap()
            .is_some_and(|owner| tokio::task::try_id() != Some(owner));
        if catalog_peer {
            // Only the selected event owner may publish until its catalog and
            // same-stream sentinel are confirmed. Drop keeps this latch open.
            drop(
                self.state
                    .event_catalog_peers_release
                    .acquire()
                    .await
                    .expect("catalog peer publication gate must remain open"),
            );
        }
        if !cancellation_request_task
            && self
                .state
                .block_transaction_begin
                .swap(false, Ordering::SeqCst)
        {
            self.state.transaction_begin_blocked.add_permits(1);
            self.state
                .transaction_begin_release
                .acquire()
                .await
                .expect("catalog transaction release must remain open")
                .forget();
        }
        let cancellation_request_read_conflict = cancellation_request_task
            && self.persistence_fault() == PromptPersistenceFault::CancelRequestLoadConflictOnce;
        if cancellation_request_task && self.persistence_fault().is_cancellation_request() {
            self.state.cancellation_request_reads_entered.add_permits(1);
            // The selected abort must snapshot the actual published admission.
            // Lance does not serialize transaction begin behind another writer.
            drop(
                self.state
                    .cancellation_request_admission_committed
                    .acquire()
                    .await
                    .expect("prompt admission publication gate must remain open"),
            );
        }
        if cancellation_request_read_conflict {
            self.set_persistence_fault(PromptPersistenceFault::None);
        }
        Ok(PromptFaultTransaction {
            inner: self.inner.begin_transaction().await?,
            faults: self.clone(),
            wrote_prompt: false,
            commit_fault: PromptPersistenceFault::None,
            wrote_event: false,
            event_catalog_receipt: None,
            wrote_continuation: false,
            wrote_bound_session: false,
            inserted_cleanup: false,
            deleted_workspace: false,
            wrote_workspace: false,
            conversation_intent: false,
            held_waiter_commit: false,
            failed_prompt_read: false,
            cancellation_request_read_conflict,
            cancellation_request_task,
            cancellation_request_record: None,
            failed_persistence: false,
            failed_native_cleanup: false,
            native_cleanup_retry_task: None,
            wrote_native_cleanup: false,
            read_unrequested_admission: false,
            read_missing_prompt: false,
            dispatch_record: None,
            prepared_cancel_selected,
            prepared_cancel_read: false,
            prepared_cancel_activation_read: false,
            prepared_cancel_event: None,
            prepared_cancel_deleted: false,
            recovered_waiter_written: false,
            prepared_cancel_intent: None,
        })
    }
}

impl StateTransactionBackend for PromptFaultTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        {
            let mut intent = self.faults.state.conversation_intent.lock().unwrap();
            if let Some(intent) = intent.as_mut().filter(|intent| intent.active) {
                let phase = match key.namespace().as_str() {
                    WORKSPACE_NAMESPACE => Some(ConversationIntentPhase::WorkspaceLoad),
                    "agent_sdk_opencode_workspace_routing_v1" => {
                        Some(ConversationIntentPhase::RoutingLoad)
                    }
                    CONVERSATION_NAMESPACE => Some(ConversationIntentPhase::ConversationLoad),
                    OPENCODE_SESSION_NAMESPACE => Some(ConversationIntentPhase::BindingLoad),
                    _ => None,
                };
                if let Some(phase) = phase {
                    self.conversation_intent = true;
                    intent.before(phase)?;
                }
            }
        }
        let stale_workspace = key.namespace().as_str() == WORKSPACE_NAMESPACE
            && self
                .faults
                .state
                .conversation_intent
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|intent| intent.active && intent.stale_workspace);
        if stale_workspace {
            let mut snapshot = self.inner.load(key).await?;
            let value = Arc::make_mut(&mut snapshot.as_mut().unwrap().record.value);
            value["revision"] = serde_json::json!(value["revision"].as_u64().unwrap() + 1);
            return Ok(snapshot);
        }
        let observer = self
            .faults
            .state
            .reconciliation_observer
            .lock()
            .unwrap()
            .as_ref()
            .filter(|observer| tokio::task::try_id() == Some(observer.task) && observer.key == key)
            .cloned();
        if let Some(observer) = observer {
            // This is an actual second caller's transaction read. Its delivery
            // is held only after the backing Store completes the same operation.
            let result = self.inner.load(key).await;
            observer.entered.add_permits(1);
            let _released = observer.release.acquire().await.unwrap();
            return result;
        }
        let selected_prepared_cancel = self.prepared_cancel_selected
            && self
                .faults
                .state
                .prepared_cancel_receipt
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|receipt| receipt.key == key);
        if selected_prepared_cancel {
            let mut snapshot = self.inner.load(key.clone()).await?;
            {
                let mut receipt = self.faults.state.prepared_cancel_receipt.lock().unwrap();
                let receipt = receipt.as_mut().unwrap();
                if matches!(
                    receipt.stage,
                    PromptCleanupStage::AwaitNonPreparedRollback
                        | PromptCleanupStage::AwaitWaiterRollback
                ) {
                    assert!(
                        snapshot.as_ref().is_some_and(|snapshot| {
                            snapshot.record.value["phase"]["phase"] != "prepared"
                                && receipt.intent_record.as_ref().is_some_and(|intent| {
                                    snapshot.record.value["message_id"]
                                        == intent.value["message_id"]
                                })
                        }),
                        "reconciliation activation requires the real nonprepared generation"
                    );
                    receipt.activation_reads += 1;
                    self.prepared_cancel_activation_read = true;
                    return Ok(snapshot);
                }
            }
            self.prepared_cancel_read = true;
            {
                let mut receipt = self.faults.state.prepared_cancel_receipt.lock().unwrap();
                let receipt = receipt.as_mut().unwrap();
                receipt.reads += 1;
                if receipt.before.is_none() {
                    receipt.before = snapshot.clone();
                }
                if matches!(
                    receipt.fault,
                    PreparedCancelFault::LoadConflictOnce | PreparedCancelFault::LoadConflict
                ) {
                    if receipt.fault == PreparedCancelFault::LoadConflictOnce {
                        receipt.fault = PreparedCancelFault::None;
                    }
                    receipt.faults += 1;
                    receipt.awaiting_rollback = true;
                    return Err(StateStoreError::transaction_conflict(
                        "load",
                        "injected prepared cancellation read conflict",
                    ));
                }
                match receipt.fault {
                    PreparedCancelFault::InitialMissing => {
                        receipt.fault = PreparedCancelFault::None;
                        receipt.faults += 1;
                        snapshot = None;
                    }
                    PreparedCancelFault::InitialForeign => {
                        receipt.fault = PreparedCancelFault::None;
                        receipt.faults += 1;
                        let snapshot = snapshot.as_mut().expect("the real prompt is retained");
                        Arc::make_mut(&mut snapshot.record.value)["message_id"] =
                            serde_json::json!("prompt_foreign_prepared_cancel_initial");
                    }
                    _ => {}
                }
                if receipt.stage == PromptCleanupStage::Prepared
                    && !receipt.uncertain_commit
                    && snapshot.as_ref().is_none_or(|snapshot| {
                        snapshot.record.value["phase"]["phase"] != "prepared"
                            || receipt.intent_record.as_ref().is_some_and(|intent| {
                                snapshot.record.value["message_id"] != intent.value["message_id"]
                            })
                    })
                {
                    // Only this read-only prepared check is selected. Later
                    // cancellation claim/dispatch transactions have other owners.
                    receipt.active = false;
                }
            }
            self.faults
                .observe_prepared_cancel_receipt(&key, &mut snapshot)
                .await?;
            return Ok(snapshot);
        }
        let selected_cleanup = self
            .faults
            .state
            .native_cleanup_armed
            .load(Ordering::SeqCst)
            && self
                .faults
                .state
                .native_cleanup_receipt
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|receipt| receipt.before.record.key == key);
        if selected_cleanup {
            let mut receipt = self.faults.state.native_cleanup_receipt.lock().unwrap();
            let receipt = receipt.as_mut().unwrap();
            if receipt.awaiting_rollback && !receipt.rollback_confirmed {
                receipt.reads_before_rollback += 1;
            }
        }
        let cleanup_fault = self.faults.persistence_fault();
        if selected_cleanup
            && matches!(
                cleanup_fault,
                PromptPersistenceFault::NativeCleanupLoadConflictOnce
                    | PromptPersistenceFault::NativeCleanupLoadThenBeginConflict
                    | PromptPersistenceFault::NativeCleanupLoadConflict
            )
        {
            let before = self
                .inner
                .load(key)
                .await?
                .expect("the admitted native interaction must retain its continuation");
            {
                let mut receipt = self.faults.state.native_cleanup_receipt.lock().unwrap();
                let receipt = receipt.as_mut().unwrap();
                receipt.before = before;
                receipt.awaiting_rollback = true;
                receipt.rollback_confirmed = false;
            }
            if cleanup_fault == PromptPersistenceFault::NativeCleanupLoadThenBeginConflict {
                self.native_cleanup_retry_task = Some(
                    tokio::task::try_id()
                        .expect("the selected native cleanup transaction must have an owned task"),
                );
            }
            if cleanup_fault != PromptPersistenceFault::NativeCleanupLoadConflict {
                self.faults
                    .set_persistence_fault(PromptPersistenceFault::None);
            }
            self.faults
                .state
                .persistence_attempts
                .fetch_add(1, Ordering::SeqCst);
            self.failed_native_cleanup = true;
            return Err(StateStoreError::transaction_conflict(
                "load",
                "injected native cleanup preparation conflict",
            ));
        }
        if self.cancellation_request_read_conflict
            && key.namespace().as_str() == OPENCODE_PROMPT_NAMESPACE
        {
            self.cancellation_request_read_conflict = false;
            self.failed_persistence = true;
            self.faults
                .state
                .persistence_attempts
                .fetch_add(1, Ordering::SeqCst);
            return Err(StateStoreError::transaction_conflict(
                "load",
                "injected cancellation intent read conflict",
            ));
        }
        if key.namespace().as_str() == OPENCODE_PROMPT_NAMESPACE
            && self
                .faults
                .state
                .block_prompt_load
                .swap(false, Ordering::SeqCst)
        {
            self.faults.state.prompt_load_blocked.add_permits(1);
            tokio::time::timeout(
                Duration::from_secs(8),
                self.faults.state.prompt_load_release.acquire(),
            )
            .await
            .expect("the test must release its held prompt read")
            .expect("prompt read release must remain open")
            .forget();
            self.failed_prompt_read = true;
            return Err(StateStoreError::unavailable(
                "load",
                "PRIVATE_REOPEN_READ_FAILURE",
            ));
        }
        if key.namespace().as_str() == WORKSPACE_NAMESPACE
            && self
                .faults
                .state
                .workspace_create_phase
                .load(Ordering::SeqCst)
                != WorkspaceCreatePhase::None as u8
        {
            self.faults
                .state
                .workspace_create_loads
                .fetch_add(1, Ordering::SeqCst);
            {
                let mut first = self.faults.state.workspace_create_key.lock().unwrap();
                assert_eq!(first.get_or_insert_with(|| key.clone()), &key);
            }
            self.faults
                .before_workspace_create(WorkspaceCreatePhase::Load)?;
        }
        if key.namespace().as_str() == OPENCODE_CATALOG_NAMESPACE {
            let selected = self
                .faults
                .state
                .event_catalog_receipt
                .lock()
                .unwrap()
                .take();
            if let Some(receipt) = selected {
                self.event_catalog_receipt = Some(receipt);
                *self.faults.state.event_catalog_task.lock().unwrap() = Some(
                    tokio::task::try_id().expect("catalog application has an owned event task"),
                );
                self.faults.state.event_catalog_load_blocked.add_permits(1);
                self.faults
                    .state
                    .event_catalog_load_release
                    .acquire()
                    .await
                    .expect("event catalog load release must remain open")
                    .forget();
            }
        }
        if self.faults.before_event_catalog_load(&key) {
            return Err(StateStoreError::transaction_conflict(
                "load",
                "injected event catalog conflict",
            ));
        }
        if key.namespace().as_str() == OPENCODE_CONTINUATION_NAMESPACE
            && self
                .faults
                .state
                .continuation_transaction_conflicts
                .try_update(
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    |remaining| match remaining {
                        0 => None,
                        usize::MAX => Some(usize::MAX),
                        _ => Some(remaining - 1),
                    },
                )
                .is_ok()
        {
            self.faults.state.blocked.add_permits(1);
            return Err(StateStoreError::transaction_conflict(
                "load",
                "injected continuation transaction conflict",
            ));
        }
        let fail = key.namespace().as_str() == OPENCODE_CONTINUATION_NAMESPACE
            && self
                .faults
                .state
                .continuation_load_countdown
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok_and(|remaining| remaining == 1);
        if fail {
            self.faults
                .state
                .continuation_failures
                .fetch_add(1, Ordering::SeqCst);
            return Err(StateStoreError::unavailable(
                "load",
                "injected post-commit continuation reconciliation failure",
            ));
        }
        let is_prompt = key.namespace().as_str() == OPENCODE_PROMPT_NAMESPACE;
        let mut snapshot = self.inner.load(key).await?;
        self.read_missing_prompt |= is_prompt && snapshot.is_none();
        self.faults
            .observe_cancellation_request_advance(snapshot.as_ref());
        let request_fault = self.faults.persistence_fault();
        if self.cancellation_request_task
            && is_prompt
            && matches!(
                request_fault,
                PromptPersistenceFault::CancelRequestForeign
                    | PromptPersistenceFault::CancelRequestTerminal
            )
        {
            self.faults
                .set_persistence_fault(PromptPersistenceFault::None);
            self.faults
                .state
                .persistence_attempts
                .fetch_add(1, Ordering::SeqCst);
            let value = Arc::make_mut(
                &mut snapshot
                    .as_mut()
                    .expect("the held prompt admission must publish its record")
                    .record
                    .value,
            );
            if request_fault == PromptPersistenceFault::CancelRequestForeign {
                value["message_id"] = serde_json::json!("prompt_other_generation");
            } else {
                value["phase"] =
                    serde_json::json!({"phase":"terminal", "outcome":{"outcome":"cancelled"}});
                value["cancellation"] = serde_json::json!({"state":"unnecessary"});
            }
        }
        self.read_unrequested_admission |= is_prompt
            && snapshot.as_ref().is_some_and(|snapshot| {
                snapshot.record.value["phase"]["phase"] == "admitted"
                    && snapshot.record.value["cancellation"]["state"] == "not_requested"
            });
        if is_prompt
            && self
                .faults
                .state
                .persistence_foreign_next
                .swap(false, Ordering::SeqCst)
            && let Some(snapshot) = &mut snapshot
        {
            Arc::make_mut(&mut snapshot.record.value)["message_id"] =
                serde_json::json!("prompt_foreign_persistence");
        }
        if is_prompt
            && let Some(snapshot) = &mut snapshot
            && snapshot
                .record
                .value
                .pointer("/phase/phase")
                .and_then(serde_json::Value::as_str)
                == Some("terminal")
        {
            let expected_waiter = match self.faults.state.waiter_fault.load(Ordering::SeqCst) {
                1 => Some("waiting"),
                2 => Some("delivering"),
                3 => Some("lost"),
                _ => None,
            };
            if expected_waiter.is_some()
                && snapshot.record.value["waiter"].as_str() == expected_waiter
            {
                self.faults
                    .state
                    .waiter_fault_attempts
                    .fetch_add(1, Ordering::SeqCst);
                // A schema-valid record for another generation is a definite
                // ownership failure; it must not be treated as retryable I/O.
                Arc::make_mut(&mut snapshot.record.value)["message_id"] =
                    serde_json::json!("prompt_other_generation");
            }
        }
        self.faults
            .observe_native_cleanup_receipt(snapshot.as_ref());
        Ok(snapshot)
    }

    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        {
            let mut intent = self.faults.state.conversation_intent.lock().unwrap();
            if let Some(intent) = intent.as_mut().filter(|intent| intent.active) {
                let phase = match record.key.namespace().as_str() {
                    CONVERSATION_NAMESPACE => Some(ConversationIntentPhase::ConversationPut),
                    OPENCODE_SESSION_NAMESPACE => Some(ConversationIntentPhase::BindingPut),
                    _ => None,
                };
                if let Some(phase) = phase {
                    assert_eq!(
                        intent
                            .records
                            .entry(record.key.namespace().as_str().to_owned())
                            .or_insert_with(|| record.clone()),
                        &record,
                        "all retries must retain the exact portable identity and native correlation",
                    );
                    intent.before(phase)?;
                }
            }
        }
        let selected_event = self
            .faults
            .state
            .prepared_cancel_receipt
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|receipt| {
                receipt.enabled && matches!(
                    &receipt.stage,
                    PromptCleanupStage::AwaitEventSettlement { continuation_key }
                        if *continuation_key == record.key && record.value["state"] == "accepted"
                )
            });
        if selected_event {
            let before = self
                .inner
                .load(record.key.clone())
                .await?
                .expect("selected native answer must update the real admitted continuation");
            assert_eq!(before.record.value["state"], "pending");
            assert_eq!(before.record.value["request"], record.value["request"]);
            assert_eq!(
                before.record.value["generation_id"],
                record.value["generation_id"]
            );
            self.prepared_cancel_event = Some(record.clone());
            self.faults
                .state
                .prepared_cancel_receipt
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .event_before = Some(before);
        }
        {
            let mut receipt = self.faults.state.prepared_cancel_receipt.lock().unwrap();
            if let Some(receipt) = receipt.as_mut()
                && receipt.enabled
                && receipt.key == record.key
                && receipt.commits != 0
                && (receipt.stage != PromptCleanupStage::RecoveredWaiter
                    || receipt.attempted_record.as_ref() == Some(&record))
            {
                receipt.writes_after_commit += 1;
            }
            if let Some(receipt) = receipt.as_ref()
                && receipt.enabled
                && receipt.intent_record.is_none()
                && receipt.key == record.key
                && receipt.task.is_some()
                && tokio::task::try_id() == receipt.task
                && record.value["cancellation"]["state"] == "requested"
            {
                self.prepared_cancel_intent = Some(record.clone());
            }
        }
        if self.prepared_cancel_selected && self.prepared_cancel_read {
            let mut guard = self.faults.state.prepared_cancel_receipt.lock().unwrap();
            let receipt = guard.as_mut().unwrap();
            if receipt.key == record.key {
                if receipt.stage == PromptCleanupStage::RecoveredWaiter {
                    let before = receipt
                        .before
                        .as_ref()
                        .expect("actual recovered prompt must be read");
                    let mut expected = (*before.record.value).clone();
                    assert_eq!(expected["phase"]["phase"], "admitted");
                    assert_eq!(expected["cancellation"]["state"], "requested");
                    assert_eq!(expected["waiter"], "waiting");
                    expected["waiter"] = serde_json::json!("lost");
                    assert_eq!(
                        *record.value, expected,
                        "selected update changes only the actual ended waiter"
                    );
                    receipt
                        .attempted_record
                        .get_or_insert_with(|| record.clone());
                    self.recovered_waiter_written = true;
                }
                receipt.puts += 1;
                receipt.accesses_before_rollback += usize::from(receipt.awaiting_rollback);
                if receipt.stage == PromptCleanupStage::RecoveredWaiter
                    && receipt.fault == PreparedCancelFault::PutConflictOnce
                {
                    receipt.fault = PreparedCancelFault::None;
                    receipt.faults += 1;
                    receipt.awaiting_rollback = true;
                    return Err(StateStoreError::transaction_conflict(
                        "put",
                        "injected recovered waiter update conflict",
                    ));
                }
            }
        }
        let selected_cleanup = self
            .faults
            .state
            .native_cleanup_armed
            .load(Ordering::SeqCst)
            && self
                .faults
                .state
                .native_cleanup_receipt
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|receipt| receipt.before.record.key == record.key);
        if selected_cleanup {
            let before = self
                .inner
                .load(record.key.clone())
                .await?
                .expect("native cleanup may mutate only its existing admitted continuation");
            if before.record != record
                || self
                    .faults
                    .state
                    .native_cleanup_receipt
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_some_and(|receipt| receipt.attempt.is_some())
            {
                self.wrote_native_cleanup = true;
                let first = {
                    let mut receipt = self.faults.state.native_cleanup_receipt.lock().unwrap();
                    let receipt = receipt.as_mut().unwrap();
                    receipt.writes += 1;
                    let first = receipt.attempt.is_none();
                    if first {
                        receipt.before = before;
                        receipt.attempt = Some(record.clone());
                    } else if receipt.attempt.as_ref() == Some(&record) {
                        receipt.repeated_attempts += 1;
                    }
                    if receipt.awaiting_rollback && !receipt.rollback_confirmed {
                        receipt.writes_before_rollback += 1;
                    }
                    first
                };
                let fault = self.faults.persistence_fault();
                if first && fault.is_native_cleanup() {
                    if fault == PromptPersistenceFault::NativeCleanupConflictOnce {
                        self.faults
                            .set_persistence_fault(PromptPersistenceFault::None);
                        self.faults
                            .state
                            .persistence_attempts
                            .fetch_add(1, Ordering::SeqCst);
                        self.faults
                            .state
                            .native_cleanup_receipt
                            .lock()
                            .unwrap()
                            .as_mut()
                            .unwrap()
                            .awaiting_rollback = true;
                        self.failed_native_cleanup = true;
                        return Err(StateStoreError::transaction_conflict(
                            "put",
                            "injected native cleanup preparation conflict",
                        ));
                    }
                    self.commit_fault = fault;
                }
            }
        }
        let mut persistence = self.faults.persistence_fault();
        if persistence.is_cancellation_request() && !self.cancellation_request_task {
            persistence = PromptPersistenceFault::None;
        }
        if self.cancellation_request_task
            && PromptPersistenceFault::CancelRequestOnce.targets(&record)
        {
            let writes = self
                .faults
                .state
                .cancellation_request_writes
                .fetch_add(1, Ordering::SeqCst)
                + 1;
            if writes > 1
                && self
                    .faults
                    .state
                    .cancellation_request_uncommitted
                    .load(Ordering::SeqCst)
            {
                self.faults.state.blocked.add_permits(1);
            }
            if persistence == PromptPersistenceFault::CancelRequestCommitReceiptAdvanced {
                self.cancellation_request_record = Some(record.clone());
            }
        }
        if PromptPersistenceFault::AdmissionOnce.targets(&record) {
            self.faults
                .state
                .admission_writes
                .fetch_add(1, Ordering::SeqCst);
        }
        if PromptPersistenceFault::TerminalOnce.targets(&record) {
            self.faults
                .state
                .terminal_writes
                .fetch_add(1, Ordering::SeqCst);
        }
        if PromptPersistenceFault::CancelClaimOnce.targets(&record) {
            self.faults
                .state
                .cancellation_claim_writes
                .fetch_add(1, Ordering::SeqCst);
            let mut owners = self.faults.state.cancellation_claim_owners.lock().unwrap();
            if owners.len() < 4 {
                owners.push(serde_json::json!({"owner": record.value["cancellation"]["owner"], "generation": record.value["message_id"]}));
            }
        }
        if PromptPersistenceFault::CancelDispatchOnce.targets(&record) {
            self.dispatch_record = Some(
                serde_json::json!({"owner": record.value["cancellation"]["owner"], "generation": record.value["message_id"]}),
            );
            self.faults
                .state
                .cancellation_dispatch_writes
                .fetch_add(1, Ordering::SeqCst);
            let mut owners = self
                .faults
                .state
                .cancellation_dispatch_owners
                .lock()
                .unwrap();
            if owners.len() < 4 {
                owners.push(serde_json::json!({"owner": record.value["cancellation"]["owner"], "generation": record.value["message_id"]}));
            }
        }
        if PromptPersistenceFault::AbortOutcomeOnce.targets(&record) {
            self.faults
                .state
                .cancellation_outcome_records
                .lock()
                .unwrap()
                .push(record.clone());
            self.faults
                .state
                .cancellation_outcome_writes
                .fetch_add(1, Ordering::SeqCst);
            let mut parents = self
                .faults
                .state
                .cancellation_outcome_parents
                .lock()
                .unwrap();
            if parents.len() < 4 {
                parents.push(serde_json::json!({
                    "generation": record.value["message_id"],
                    "request_part": record.value["request_part_id"],
                }));
            }
        }
        if persistence.is_commit() && persistence.targets(&record) {
            self.commit_fault = persistence;
        }
        if !persistence.is_deletion() && !persistence.is_commit() && persistence.targets(&record) {
            self.faults
                .state
                .persistence_attempts
                .fetch_add(1, Ordering::SeqCst);
            if persistence.fails_once() {
                self.faults
                    .set_persistence_fault(PromptPersistenceFault::None);
            }
            if self.faults.state.persistence_foreign.load(Ordering::SeqCst) {
                self.faults
                    .state
                    .persistence_foreign_next
                    .store(true, Ordering::SeqCst);
            }
            self.failed_persistence = true;
            return Err(
                if matches!(
                    persistence,
                    PromptPersistenceFault::CancelRequestConflictOnce
                        | PromptPersistenceFault::CancelRequestConflict
                        | PromptPersistenceFault::AdmissionConflictOnce
                        | PromptPersistenceFault::AdmissionConflict
                ) || self
                    .faults
                    .state
                    .persistence_conflict
                    .load(Ordering::SeqCst)
                {
                    StateStoreError::transaction_conflict(
                        "put",
                        "private rolled-back transition conflict",
                    )
                } else {
                    StateStoreError::unavailable("put", "private persistence failure")
                },
            );
        }
        self.held_waiter_commit |= record.key.namespace().as_str() == OPENCODE_PROMPT_NAMESPACE
            && record
                .value
                .pointer("/phase/phase")
                .and_then(serde_json::Value::as_str)
                == Some("terminal")
            && record.value["waiter"] == "lost"
            && self
                .faults
                .state
                .block_waiter_commit
                .swap(false, Ordering::SeqCst);
        if record.key.namespace().as_str() == OPENCODE_PROMPT_NAMESPACE
            && record.value["waiter"] == "lost"
            && self.faults.state.waiter_fault.load(Ordering::SeqCst) == 4
        {
            self.faults
                .state
                .waiter_fault_attempts
                .fetch_add(1, Ordering::SeqCst);
            return Err(StateStoreError::unavailable(
                "put",
                "private OpenCode backend detail",
            ));
        }
        if record.key.namespace().as_str() == WORKSPACE_NAMESPACE {
            self.wrote_workspace = true;
            if self
                .faults
                .state
                .workspace_create_phase
                .load(Ordering::SeqCst)
                != WorkspaceCreatePhase::None as u8
            {
                {
                    let mut first = self.faults.state.workspace_create_record.lock().unwrap();
                    assert_eq!(first.get_or_insert_with(|| record.clone()), &record);
                }
                self.faults
                    .before_workspace_create(WorkspaceCreatePhase::Put)?;
            }
        }
        if record.key.namespace().as_str() == OPENCODE_CONTINUATION_NAMESPACE {
            match PromptCommitFault::from_byte(
                self.faults
                    .state
                    .continuation_put_fault
                    .swap(PromptCommitFault::None as u8, Ordering::SeqCst),
            ) {
                PromptCommitFault::Conflict => {
                    return Err(StateStoreError::transaction_conflict(
                        "put",
                        "injected continuation settlement conflict",
                    ));
                }
                PromptCommitFault::Unavailable => {
                    return Err(StateStoreError::unavailable(
                        "put",
                        "injected continuation settlement failure",
                    ));
                }
                _ => {}
            }
        }
        self.wrote_prompt |= record.key.namespace().as_str() == OPENCODE_PROMPT_NAMESPACE;
        self.wrote_event |= record.key.namespace().as_str() == OPENCODE_EVENT_PROGRESS_NAMESPACE;
        self.wrote_continuation |=
            record.key.namespace().as_str() == OPENCODE_CONTINUATION_NAMESPACE;
        let writes_bound_session = record.key.namespace().as_str() == OPENCODE_SESSION_NAMESPACE
            && record
                .value
                .pointer("/state/phase")
                .and_then(serde_json::Value::as_str)
                == Some("bound");
        self.wrote_bound_session |= writes_bound_session;
        if writes_bound_session {
            self.faults
                .state
                .bound_session_writes
                .fetch_add(1, Ordering::SeqCst);
        }
        if writes_bound_session
            && *self.faults.state.bound_session_fault.lock().unwrap()
                == BoundSessionFault::StagingRollbackFailure
        {
            return Err(StateStoreError::transaction_conflict(
                "put",
                "bound-session staging conflict",
            ));
        }
        self.inserted_cleanup |= record.key.namespace().as_str() == OPENCODE_CLEANUP_NAMESPACE;
        self.inner.put(record).await
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        let select_terminal = self
            .faults
            .state
            .prepared_cancel_receipt
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|receipt| {
                receipt.enabled
                    && receipt.stage == PromptCleanupStage::TerminalMutation
                    && receipt.key == key
                    && (receipt.task.is_none() || tokio::task::try_id() == receipt.task)
            });
        if select_terminal {
            let snapshot = self.inner.load(key.clone()).await?;
            assert!(
                snapshot.as_ref().is_some_and(|snapshot| {
                    snapshot.record.value["phase"]["phase"] == "terminal"
                }),
                "terminal cleanup must target a real terminal record"
            );
            let mut receipt = self.faults.state.prepared_cancel_receipt.lock().unwrap();
            let receipt = receipt.as_mut().unwrap();
            if receipt.task.is_none() {
                assert!(
                    snapshot.as_ref().is_some_and(|snapshot| {
                        matches!(
                            snapshot.record.value["waiter"].as_str(),
                            Some("lost" | "delivered")
                        )
                    }),
                    "live terminal cleanup follows durable waiter settlement"
                );
                receipt.task =
                    Some(tokio::task::try_id().expect("terminal delivery has an owned task"));
            }
            if receipt.before.is_none() {
                receipt.before = snapshot;
            }
            receipt.reads += 1;
            self.prepared_cancel_selected = true;
            self.prepared_cancel_read = true;
        }
        {
            let mut receipt = self.faults.state.prepared_cancel_receipt.lock().unwrap();
            if let Some(receipt) = receipt.as_mut()
                && receipt.enabled
                && receipt.key == key
                && receipt.commits != 0
                && receipt.stage != PromptCleanupStage::RecoveredWaiter
            {
                receipt.deletes_after_commit += 1;
            }
        }
        let waiter_cleanup = self
            .faults
            .state
            .prepared_cancel_receipt
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|receipt| {
                receipt.enabled
                    && receipt.key == key
                    && receipt.stage == PromptCleanupStage::RecoveredWaiter
            });
        if waiter_cleanup {
            self.faults
                .state
                .prepared_cancel_receipt
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .deletes += 1;
        }
        if self.prepared_cancel_selected && !waiter_cleanup {
            let mut receipt = self.faults.state.prepared_cancel_receipt.lock().unwrap();
            let receipt = receipt.as_mut().unwrap();
            if receipt.key == key {
                receipt.deletes += 1;
                receipt.accesses_before_rollback += usize::from(receipt.awaiting_rollback);
                self.prepared_cancel_deleted = true;
                if receipt.fault == PreparedCancelFault::DeleteConflictOnce {
                    receipt.fault = PreparedCancelFault::None;
                    receipt.faults += 1;
                    receipt.awaiting_rollback = true;
                    return Err(StateStoreError::transaction_conflict(
                        "delete",
                        "injected prepared cancellation deletion conflict",
                    ));
                }
            }
        }
        let persistence = self.faults.persistence_fault();
        if persistence == PromptPersistenceFault::ReopenCommitConflictOnce
            && key.namespace().as_str() == OPENCODE_PROMPT_NAMESPACE
        {
            self.commit_fault = persistence;
        }
        if persistence.is_deletion()
            && key.namespace().as_str() == OPENCODE_PROMPT_NAMESPACE
            && let Some(snapshot) = self.inner.load(key.clone()).await?
            && persistence.targets(&snapshot.record)
        {
            self.faults
                .state
                .persistence_attempts
                .fetch_add(1, Ordering::SeqCst);
            if persistence.fails_once() {
                self.faults
                    .set_persistence_fault(PromptPersistenceFault::None);
            }
            if self.faults.state.persistence_foreign.load(Ordering::SeqCst) {
                self.faults
                    .state
                    .persistence_foreign_next
                    .store(true, Ordering::SeqCst);
            }
            return Err(StateStoreError::unavailable(
                "delete",
                "private persistence failure",
            ));
        }
        self.wrote_prompt |= key.namespace().as_str() == OPENCODE_PROMPT_NAMESPACE;
        self.wrote_event |= key.namespace().as_str() == OPENCODE_EVENT_PROGRESS_NAMESPACE;
        self.wrote_continuation |= key.namespace().as_str() == OPENCODE_CONTINUATION_NAMESPACE;
        self.deleted_workspace |= key.namespace().as_str() == WORKSPACE_NAMESPACE;
        self.inner.delete(key).await
    }

    async fn commit(self) -> Result<(), StateStoreError> {
        if self.conversation_intent {
            let held = {
                let mut intent = self.faults.state.conversation_intent.lock().unwrap();
                let intent = intent.as_mut().unwrap();
                intent
                    .late_commit
                    .then(|| (intent.commit_entered.clone(), intent.commit_release.clone()))
            };
            if let Some((entered, release)) = held {
                entered.add_permits(1);
                release.acquire().await.unwrap().forget();
            }
            let injected = self
                .faults
                .state
                .conversation_intent
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .before(ConversationIntentPhase::Commit);
            let obscure = match injected {
                Ok(obscure) => obscure,
                Err(error) => {
                    self.inner.rollback().await?;
                    return Err(error);
                }
            };
            self.inner.commit().await?;
            self.faults
                .state
                .conversation_intent
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .active = false;
            return if obscure {
                Err(StateStoreError::unavailable(
                    "commit",
                    "injected intent commit receipt loss",
                ))
            } else {
                Ok(())
            };
        }
        let faults = self.faults.clone();
        if self.prepared_cancel_deleted || self.recovered_waiter_written {
            let fault = {
                let mut receipt = faults.state.prepared_cancel_receipt.lock().unwrap();
                let receipt = receipt.as_mut().unwrap();
                receipt.commit_attempts += 1;
                let fault = receipt.fault;
                if fault != PreparedCancelFault::CommitConflict {
                    receipt.fault = PreparedCancelFault::None;
                }
                fault
            };
            if matches!(
                fault,
                PreparedCancelFault::CommitConflictOnce
                    | PreparedCancelFault::CommitConflict
                    | PreparedCancelFault::CommitReceiptLost
                    | PreparedCancelFault::CommitReceiptHeld
                    | PreparedCancelFault::CommitUnavailableOnce
                    | PreparedCancelFault::CommitUnavailableForeignReceipt
                    | PreparedCancelFault::CommitUnavailableMissingReceipt
                    | PreparedCancelFault::CommitUnavailableMalformedReceipt
                    | PreparedCancelFault::CommitReceiptLostReadUnavailableOnce
                    | PreparedCancelFault::CommitUnavailableReadUnavailableOnce
                    | PreparedCancelFault::CommitUnavailableReadHeld
            ) {
                if matches!(
                    fault,
                    PreparedCancelFault::CommitReceiptLost
                        | PreparedCancelFault::CommitReceiptHeld
                        | PreparedCancelFault::CommitReceiptLostReadUnavailableOnce
                ) {
                    self.inner.commit().await?;
                } else {
                    self.inner.rollback().await?;
                }
                {
                    let mut receipt = faults.state.prepared_cancel_receipt.lock().unwrap();
                    let receipt = receipt.as_mut().unwrap();
                    receipt.faults += 1;
                    if matches!(
                        fault,
                        PreparedCancelFault::CommitConflictOnce
                            | PreparedCancelFault::CommitConflict
                    ) {
                        receipt.rollbacks += 1;
                        receipt.awaiting_rollback = false;
                    } else {
                        receipt.uncertain_commit = true;
                        receipt.foreign_receipt =
                            fault == PreparedCancelFault::CommitUnavailableForeignReceipt;
                        receipt.hold_receipt =
                            fault == PreparedCancelFault::CommitUnavailableReadHeld;
                        receipt.unavailable_receipt = matches!(
                            fault,
                            PreparedCancelFault::CommitReceiptLostReadUnavailableOnce
                                | PreparedCancelFault::CommitUnavailableReadUnavailableOnce
                        );
                        receipt.commits += usize::from(matches!(
                            fault,
                            PreparedCancelFault::CommitReceiptLost
                                | PreparedCancelFault::CommitReceiptHeld
                                | PreparedCancelFault::CommitReceiptLostReadUnavailableOnce
                        ));
                        if fault == PreparedCancelFault::CommitReceiptHeld {
                            receipt.active = false;
                        }
                    }
                }
                if fault == PreparedCancelFault::CommitReceiptHeld {
                    faults.state.blocked.add_permits(1);
                    faults
                        .state
                        .release
                        .acquire()
                        .await
                        .expect("prepared cancellation receipt gate must be released")
                        .forget();
                    return Ok(());
                }
                return Err(
                    if matches!(
                        fault,
                        PreparedCancelFault::CommitConflictOnce
                            | PreparedCancelFault::CommitConflict
                    ) {
                        StateStoreError::transaction_conflict(
                            "commit",
                            "injected rejected prepared cancellation commit",
                        )
                    } else {
                        StateStoreError::unavailable(
                            "commit",
                            "injected unknown prepared cancellation receipt",
                        )
                    },
                );
            }
        }
        if self.commit_fault.is_native_cleanup() {
            faults
                .state
                .persistence_attempts
                .fetch_add(1, Ordering::SeqCst);
            faults.set_persistence_fault(PromptPersistenceFault::None);
            if self.commit_fault == PromptPersistenceFault::NativeCleanupCommitReceiptLost {
                let committed = self.inner.commit().await;
                eprintln!(
                    "native cleanup selected lost receipt: committed={}, definite_conflict={}",
                    committed.is_ok(),
                    matches!(&committed, Err(StateStoreError::TransactionConflict { .. })),
                );
                committed?;
                {
                    let mut receipt = faults.state.native_cleanup_receipt.lock().unwrap();
                    let receipt = receipt.as_mut().unwrap();
                    receipt.committed_receipt_lost = true;
                    receipt.commits += 1;
                }
                return Err(StateStoreError::unavailable(
                    "commit",
                    "injected lost native cleanup receipt",
                ));
            }
            self.inner.rollback().await?;
            if self.commit_fault == PromptPersistenceFault::NativeCleanupCommitConflictOnce {
                {
                    let mut receipt = faults.state.native_cleanup_receipt.lock().unwrap();
                    let receipt = receipt.as_mut().unwrap();
                    receipt.rollback_attempts += 1;
                    receipt.rollback_confirmed = true;
                }
                return Err(StateStoreError::transaction_conflict(
                    "commit",
                    "injected rejected native cleanup commit",
                ));
            }
            assert_eq!(
                self.commit_fault,
                PromptPersistenceFault::NativeCleanupCommitUnavailableOnce
            );
            return Err(StateStoreError::unavailable(
                "commit",
                "injected unknown native cleanup outcome",
            ));
        }
        if self.commit_fault != PromptPersistenceFault::None {
            faults
                .state
                .persistence_attempts
                .fetch_add(1, Ordering::SeqCst);
            if self.commit_fault != PromptPersistenceFault::AbortOutcomeCommitConflict {
                faults.set_persistence_fault(PromptPersistenceFault::None);
            }
            if self.commit_fault == PromptPersistenceFault::CancelClaimCommitHeld {
                self.inner.commit().await?;
                faults.state.blocked.add_permits(1);
                faults
                    .state
                    .release
                    .acquire()
                    .await
                    .expect("the committed cancellation claim remains owned until released")
                    .forget();
                return Ok(());
            }
            if matches!(
                self.commit_fault,
                PromptPersistenceFault::AbortOutcomeCommitConflictOnce
                    | PromptPersistenceFault::ReopenCommitConflictOnce
            ) {
                let mut other = faults.inner.begin_transaction().await?;
                other
                    .put(StateRecord::new(
                        StateKey::new("abort-outcome-witness", "intervening-commit").unwrap(),
                        serde_json::json!({"published": true}),
                    ))
                    .await?;
                other.commit().await?;
                let result = self.inner.commit().await;
                assert!(
                    matches!(result, Err(StateStoreError::TransactionConflict { .. })),
                    "the fixture must forward a supplied Lance transaction conflict: {result:?}"
                );
                return result;
            }
            if matches!(
                self.commit_fault,
                PromptPersistenceFault::AbortOutcomeCommitReceiptLost
                    | PromptPersistenceFault::CancelRequestCommitReceiptLost
                    | PromptPersistenceFault::CancelRequestCommitReceiptAdvanced
                    | PromptPersistenceFault::AdmissionCommitReceiptLost
                    | PromptPersistenceFault::TerminalCommitReceiptLost
                    | PromptPersistenceFault::CancelClaimCommitReceiptLost
                    | PromptPersistenceFault::CancelDispatchCommitReceiptLost
            ) {
                self.inner.commit().await?;
                if matches!(
                    self.commit_fault,
                    PromptPersistenceFault::CancelRequestCommitReceiptLost
                        | PromptPersistenceFault::CancelRequestCommitReceiptAdvanced
                ) {
                    faults
                        .state
                        .cancellation_request_commits
                        .fetch_add(1, Ordering::SeqCst);
                }
                if self.commit_fault == PromptPersistenceFault::CancelRequestCommitReceiptAdvanced {
                    let expected = self
                        .cancellation_request_record
                        .expect("advanced receipt owns its committed intent record");
                    let mut transaction = faults.inner.begin_transaction().await?;
                    let mut snapshot = transaction
                        .load(expected.key.clone())
                        .await?
                        .expect("committed intent remains present while its foreground is held");
                    assert_eq!(
                        snapshot.record, expected,
                        "the test advances exactly its committed intent"
                    );
                    let value = Arc::make_mut(&mut snapshot.record.value);
                    value["phase"] =
                        serde_json::json!({"phase":"terminal", "outcome":{"outcome":"cancelled"}});
                    value["cancellation"] = serde_json::json!({"state":"unnecessary"});
                    transaction.put(snapshot.record).await?;
                    transaction.commit().await?;
                    faults
                        .state
                        .cancellation_request_advanced
                        .store(true, Ordering::SeqCst);
                }
                if let Some(record) = self.dispatch_record {
                    faults
                        .state
                        .cancellation_dispatch_receipts
                        .lock()
                        .unwrap()
                        .push(record);
                }
                return Err(StateStoreError::unavailable(
                    "commit",
                    "private lost transition receipt",
                ));
            }
            self.inner.rollback().await?;
            if self.commit_fault == PromptPersistenceFault::CancelRequestCommitUnavailableOnce {
                faults
                    .state
                    .cancellation_request_uncommitted
                    .store(true, Ordering::SeqCst);
            }
            return Err(
                if matches!(
                    self.commit_fault,
                    PromptPersistenceFault::AbortOutcomeCommitConflictOnce
                        | PromptPersistenceFault::AbortOutcomeCommitConflict
                        | PromptPersistenceFault::CancelRequestCommitConflictOnce
                        | PromptPersistenceFault::AdmissionCommitConflictOnce
                        | PromptPersistenceFault::TerminalCommitConflictOnce
                        | PromptPersistenceFault::CancelClaimCommitConflictOnce
                ) {
                    StateStoreError::transaction_conflict(
                        "commit",
                        "private rolled-back transition conflict",
                    )
                } else {
                    StateStoreError::unavailable("commit", "private unobserved transition outcome")
                },
            );
        }
        if self.held_waiter_commit {
            faults.state.blocked.add_permits(1);
            faults
                .state
                .release
                .acquire()
                .await
                .expect("terminal commit release")
                .forget();
        }
        let wrote_prompt = self.wrote_prompt;
        let wrote_event = self.wrote_event;
        let wrote_continuation = self.wrote_continuation;
        let wrote_bound_session = self.wrote_bound_session;
        let inserted_cleanup = self.inserted_cleanup;
        let deleted_workspace = self.deleted_workspace;
        let obscure_workspace_create =
            self.wrote_workspace && faults.before_workspace_create(WorkspaceCreatePhase::Commit)?;
        if wrote_continuation
            && faults
                .state
                .continuation_commit_conflict
                .swap(false, Ordering::SeqCst)
        {
            return Err(StateStoreError::transaction_conflict(
                "commit",
                "injected continuation settlement conflict",
            ));
        }
        let workspace_delete_fault = if deleted_workspace {
            faults
                .state
                .workspace_delete_commit_attempts
                .fetch_add(1, Ordering::SeqCst);
            std::mem::replace(
                &mut *faults.state.workspace_delete_commit_fault.lock().unwrap(),
                WorkspaceDeleteCommitFault::None,
            )
        } else {
            WorkspaceDeleteCommitFault::None
        };
        if workspace_delete_fault == WorkspaceDeleteCommitFault::TimeoutBeforeCommit {
            self.inner.rollback().await?;
            return Err(StateStoreError::timed_out(
                "PRIVATE_WORKSPACE_DELETE_COMMIT",
                77,
            ));
        }
        faults
            .before_bound_session_commit(wrote_bound_session)
            .await;
        if wrote_bound_session
            && *faults.state.bound_session_fault.lock().unwrap() == BoundSessionFault::Unavailable
        {
            self.inner.rollback().await?;
            return Err(StateStoreError::unavailable(
                "commit",
                "bound-session uncertain receipt",
            ));
        }
        let event_conflict = faults.before_event_commit(wrote_event).await;
        let prompt_fault = if event_conflict {
            PromptCommitFault::None
        } else {
            faults
                .before_commit(wrote_prompt || (inserted_cleanup && !deleted_workspace))
                .await
        };
        let result = if event_conflict {
            self.inner.rollback().await?;
            Err(StateStoreError::transaction_conflict(
                "commit",
                "injected event conflict",
            ))
        } else if wrote_event
            && faults.state.obscure_event_commit.load(Ordering::SeqCst)
                == PromptCommitFault::Unavailable as u8
        {
            self.inner.rollback().await?;
            Err(StateStoreError::unavailable(
                "commit",
                "injected unapplied event-progress commit",
            ))
        } else {
            match prompt_fault {
                PromptCommitFault::Conflict => {
                    self.inner.rollback().await?;
                    Err(StateStoreError::transaction_conflict(
                        "commit",
                        "injected prompt conflict",
                    ))
                }
                PromptCommitFault::Unavailable => {
                    self.inner.rollback().await?;
                    Err(StateStoreError::unavailable(
                        "commit",
                        "injected prompt unavailability",
                    ))
                }
                PromptCommitFault::BlockThenConflict => {
                    self.inner.rollback().await?;
                    Err(StateStoreError::transaction_conflict(
                        "commit",
                        "injected late prompt conflict",
                    ))
                }
                PromptCommitFault::None
                | PromptCommitFault::BlockThenCommit
                | PromptCommitFault::UnavailableAfterCommit
                | PromptCommitFault::BlockThenUnavailableAfterCommit => self.inner.commit().await,
            }
        };
        if let Some(receipt) = self.event_catalog_receipt {
            let _ = receipt.send(result.clone());
        }
        let committed = result.is_ok();
        if committed {
            {
                let mut receipt = faults.state.conversation_intent.lock().unwrap();
                if let Some(receipt) = receipt.as_mut().filter(|receipt| {
                    receipt.fail_forget_read
                        && !receipt.active
                        && receipt.discard_attempts == 1
                        && receipt.discard_commits == 0
                        && receipt.creation_task.is_some()
                        && receipt.creation_task == tokio::task::try_id()
                }) {
                    receipt.discard_commits += 1;
                }
            }
            if prompt_fault == PromptCommitFault::BlockThenCommit {
                faults
                    .state
                    .cancellation_request_admission_committed
                    .add_permits(1);
            }
            if let Some(record) = self.prepared_cancel_event {
                assert!(
                    wrote_event,
                    "terminal cleanup activation requires actual event-progress commit"
                );
                let mut receipt = faults.state.prepared_cancel_receipt.lock().unwrap();
                let receipt = receipt.as_mut().unwrap();
                assert!(matches!(
                    receipt.stage,
                    PromptCleanupStage::AwaitEventSettlement { .. }
                ));
                receipt.task = Some(tokio::task::try_id().expect("native event has an owned task"));
                receipt.event_record = Some(record);
                receipt.event_commits += 1;
                receipt.stage = PromptCleanupStage::TerminalEvent;
                receipt.active = true;
            }
            if let Some(intent) = self.prepared_cancel_intent {
                let mut receipt = faults.state.prepared_cancel_receipt.lock().unwrap();
                let receipt = receipt.as_mut().unwrap();
                assert!(receipt.intent_record.is_none());
                receipt.intent_record = Some(intent);
                receipt.active = true;
            }
            if self.prepared_cancel_deleted || self.recovered_waiter_written {
                let mut receipt = faults.state.prepared_cancel_receipt.lock().unwrap();
                let receipt = receipt.as_mut().unwrap();
                receipt.commits += 1;
                receipt.active = false;
            }
            if self.wrote_native_cleanup {
                faults
                    .state
                    .native_cleanup_receipt
                    .lock()
                    .unwrap()
                    .as_mut()
                    .unwrap()
                    .commits += 1;
            }
            if let Some(record) = self.dispatch_record {
                faults
                    .state
                    .cancellation_dispatch_receipts
                    .lock()
                    .unwrap()
                    .push(record);
            }
            faults.finish_commit(wrote_continuation);
        }
        if committed && obscure_workspace_create {
            Err(StateStoreError::unavailable(
                "commit",
                "injected ambiguous workspace creation commit",
            ))
        } else if committed
            && matches!(
                prompt_fault,
                PromptCommitFault::UnavailableAfterCommit
                    | PromptCommitFault::BlockThenUnavailableAfterCommit
            )
        {
            if inserted_cleanup {
                let failures = faults
                    .state
                    .creation_cleanup_observation_failures_after_commit
                    .swap(0, Ordering::SeqCst);
                if failures != 0 {
                    faults
                        .state
                        .creation_cleanup_observation_countdown
                        .store(failures, Ordering::SeqCst);
                }
            }
            Err(StateStoreError::unavailable(
                "commit",
                "injected ambiguous prompt commit",
            ))
        } else if committed
            && deleted_workspace
            && matches!(
                workspace_delete_fault,
                WorkspaceDeleteCommitFault::UnavailableAfterCommit
                    | WorkspaceDeleteCommitFault::TimeoutAfterCommit
            )
        {
            faults
                .state
                .ambiguous_workspace_deletes
                .fetch_add(1, Ordering::SeqCst);
            let failures = faults
                .state
                .workspace_delete_observation_failures_after_commit
                .swap(0, Ordering::SeqCst);
            if failures != 0 {
                faults
                    .state
                    .workspace_delete_observation_countdown
                    .store(failures, Ordering::SeqCst);
            }
            if workspace_delete_fault == WorkspaceDeleteCommitFault::TimeoutAfterCommit {
                Err(StateStoreError::timed_out(
                    "PRIVATE_WORKSPACE_DELETE_COMMIT",
                    77,
                ))
            } else {
                Err(StateStoreError::unavailable(
                    "commit",
                    "injected ambiguous workspace delete commit",
                ))
            }
        } else if committed
            && wrote_bound_session
            && faults
                .state
                .obscure_bound_session_commit
                .swap(false, Ordering::SeqCst)
        {
            let failures = faults
                .state
                .bound_session_observation_failures_after_commit
                .swap(0, Ordering::SeqCst);
            if failures != 0 {
                faults
                    .state
                    .bound_session_observation_countdown
                    .store(failures, Ordering::SeqCst);
            }
            Err(StateStoreError::unavailable(
                "commit",
                "injected ambiguous bound-session commit",
            ))
        } else if committed
            && wrote_continuation
            && !wrote_prompt
            && !wrote_event
            && faults
                .state
                .obscure_continuation_reconciliation_commit
                .swap(false, Ordering::SeqCst)
        {
            faults
                .state
                .ambiguous_continuation_reconciliations
                .fetch_add(1, Ordering::SeqCst);
            Err(StateStoreError::unavailable(
                "commit",
                "injected ambiguous continuation reconciliation commit",
            ))
        } else if committed
            && wrote_event
            && faults
                .state
                .obscure_event_commit
                .compare_exchange(
                    PromptCommitFault::UnavailableAfterCommit as u8,
                    PromptCommitFault::None as u8,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
        {
            let failures = faults
                .state
                .event_observation_failures_after_commit
                .swap(0, Ordering::SeqCst);
            if failures != 0 {
                faults
                    .state
                    .event_observation_countdown
                    .store(failures, Ordering::SeqCst);
            }
            Err(StateStoreError::unavailable(
                "commit",
                "injected ambiguous event-progress commit",
            ))
        } else {
            result
        }
    }

    async fn rollback(self) -> Result<(), StateStoreError> {
        if self.conversation_intent {
            self.inner.rollback().await?;
            let mut intent = self.faults.state.conversation_intent.lock().unwrap();
            let intent = intent.as_mut().unwrap();
            intent.rollbacks += 1;
            if intent.fail_rollback {
                return Err(StateStoreError::unavailable(
                    "rollback",
                    "injected intent rollback receipt loss",
                ));
            }
            intent.awaiting_rollback = false;
            return Ok(());
        }
        let prepared_cancel = self.prepared_cancel_selected && self.prepared_cancel_read;
        if prepared_cancel {
            self.faults
                .state
                .prepared_cancel_receipt
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .rollbacks += 1;
        }
        self.inner.rollback().await?;
        if self.wrote_bound_session
            && *self.faults.state.bound_session_fault.lock().unwrap()
                == BoundSessionFault::StagingRollbackFailure
        {
            return Err(StateStoreError::unavailable(
                "rollback",
                "bound-session rollback receipt lost",
            ));
        }
        if self.prepared_cancel_activation_read {
            let mut receipt = self.faults.state.prepared_cancel_receipt.lock().unwrap();
            let receipt = receipt.as_mut().unwrap();
            assert!(matches!(
                receipt.stage,
                PromptCleanupStage::AwaitNonPreparedRollback
                    | PromptCleanupStage::AwaitWaiterRollback
            ));
            receipt.activation_rollbacks += 1;
            // This is a one-time boundary. Later same-task/key attempts remain
            // selected whether the owner retries locally or returns to Abort.
            receipt.stage = if receipt.stage == PromptCleanupStage::AwaitWaiterRollback {
                PromptCleanupStage::RecoveredWaiter
            } else {
                PromptCleanupStage::AbortReconciliation
            };
        }
        if prepared_cancel {
            let failed = self
                .faults
                .state
                .persistence_rollback_failure
                .swap(false, Ordering::SeqCst);
            {
                let mut receipt = self.faults.state.prepared_cancel_receipt.lock().unwrap();
                let receipt = receipt.as_mut().unwrap();
                receipt.rollback_failures += usize::from(failed);
                if !failed
                    && !receipt.awaiting_rollback
                    && !self.prepared_cancel_deleted
                    && !self.wrote_prompt
                    && receipt.stage == PromptCleanupStage::AbortReconciliation
                    && matches!(
                        receipt.requested_fault,
                        PreparedCancelFault::None
                            | PreparedCancelFault::BeginConflictOnce
                            | PreparedCancelFault::LoadConflictOnce
                    )
                {
                    // This successful read-only reconciliation has ended. A
                    // later cancellation claim/dispatch is a different owner.
                    receipt.active = false;
                }
                receipt.awaiting_rollback = failed;
            }
            if failed {
                return Err(StateStoreError::unavailable(
                    "rollback",
                    "injected prepared cancellation rollback receipt failure",
                ));
            }
        }
        if self.failed_native_cleanup {
            let failed = self
                .faults
                .state
                .persistence_rollback_failure
                .swap(false, Ordering::SeqCst);
            {
                let mut receipt = self.faults.state.native_cleanup_receipt.lock().unwrap();
                let receipt = receipt.as_mut().unwrap();
                receipt.rollback_attempts += 1;
                receipt.rollback_confirmed = !failed;
                if !failed {
                    receipt.begin_conflict_task = self.native_cleanup_retry_task;
                }
            }
            if failed {
                return Err(StateStoreError::unavailable(
                    "rollback",
                    "injected native cleanup rollback receipt failure",
                ));
            }
        }
        if (self.read_unrequested_admission || self.read_missing_prompt)
            && !self.wrote_prompt
            && self
                .faults
                .state
                .fail_unchanged_prompt_rollback
                .swap(false, Ordering::SeqCst)
        {
            self.faults
                .state
                .unchanged_prompt_rollback_failures
                .fetch_add(1, Ordering::SeqCst);
            self.faults
                .state
                .unchanged_prompt_rollback_entered
                .add_permits(1);
            return Err(StateStoreError::unavailable(
                "rollback",
                "injected unchanged prompt rollback failure",
            ));
        }
        if self.failed_persistence
            && self
                .faults
                .state
                .persistence_rollback_failure
                .swap(false, Ordering::SeqCst)
        {
            return Err(StateStoreError::unavailable(
                "rollback",
                "injected mutation cleanup failure",
            ));
        }
        if self.failed_prompt_read {
            Err(StateStoreError::unavailable(
                "rollback",
                "PRIVATE_REOPEN_ROLLBACK_FAILURE",
            ))
        } else {
            Ok(())
        }
    }
}

impl OpenCodeEventProbe {
    async fn start(history_failures: usize) -> Self {
        let mut listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("event probe must bind");
        let address = listener
            .local_addr()
            .expect("event probe address must exist");
        let (events, _) = broadcast::channel(2_048);
        let stats = Arc::new(OpenCodeEventStats {
            cwd_routing: Mutex::new(None),
            requests: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
            subscriptions: AtomicUsize::new(0),
            history: AtomicUsize::new(0),
            history_failures: AtomicUsize::new(history_failures),
            pending_scans: AtomicUsize::new(0),
            status_requests: AtomicUsize::new(0),
            unavailable_status: AtomicBool::new(false),
            rejected_status_failures: AtomicUsize::new(0),
            busy_status_responses: AtomicUsize::new(0),
            lose_stream_response: AtomicBool::new(false),
            lose_abort_response: AtomicBool::new(false),
            reject_abort_without_effects: AtomicBool::new(false),
            messages: AtomicUsize::new(0),
            question_replies: AtomicUsize::new(0),
            question_rejections: AtomicUsize::new(0),
            permission_rejections: AtomicUsize::new(0),
            pending_permissions: Mutex::new(Vec::new()),
            mixed_permission_directories: Mutex::new(BTreeSet::new()),
            retained_permissions: AtomicBool::new(false),
            retained_parallel_questions: AtomicBool::new(false),
            pending_peer_question: AtomicBool::new(false),
            held_second_permission: AtomicBool::new(false),
            retained_identity_change: Mutex::new(None),
            pending_cleanup_faults: Mutex::new(None),
            retained_rejection: Mutex::new(RetainedRejection::Acknowledged),
            rejection_entered: Semaphore::new(0),
            rejection_release: Semaphore::new(0),
            aborts: AtomicUsize::new(0),
            abort_authority: Mutex::new(None),
            aborts_without_dispatch_commit: AtomicUsize::new(0),
            deletes: AtomicUsize::new(0),
            inventory_responses: AtomicUsize::new(0),
            invalid_session_reads: AtomicUsize::new(0),
            next_session: AtomicUsize::new(0),
            native_active: AtomicBool::new(false),
            active_on_structured: AtomicBool::new(false),
            block_abort: AtomicBool::new(false),
            question_on_prompt: AtomicBool::new(false),
            pending_question: AtomicBool::new(false),
            block_history: AtomicBool::new(false),
            session_event_before_binding: AtomicBool::new(false),
            stall_capability_headers: AtomicBool::new(false),
            stall_capability_body: AtomicBool::new(false),
            stall_event_headers: AtomicBool::new(false),
            block_tool_inventory: AtomicBool::new(false),
            capability_requests: AtomicUsize::new(0),
            history_release: Notify::new(),
            listener_pause: Notify::new(),
            listener_paused: Semaphore::new(0),
            listener_resume: Semaphore::new(0),
            listener_resumed: Semaphore::new(0),
            question_reply_redirect: Mutex::new(None),
            lose_question_reply_response: AtomicBool::new(false),
            pause_before_reply_redirect: AtomicBool::new(false),
            question_reply_redirects: AtomicUsize::new(0),
            question_reply_acceptances: AtomicUsize::new(0),
            question_reply_status: AtomicUsize::new(200),
            session_event_history: Semaphore::new(0),
            event_headers_release: Notify::new(),
            tool_inventory_blocked: Semaphore::new(0),
            tool_inventory_release: Semaphore::new(0),
            inventory_response: Semaphore::new(0),
            events: events.clone(),
            sessions: Mutex::new(BTreeMap::new()),
            history_rows: Mutex::new(Vec::new()),
            stream_fixture: AtomicBool::new(false),
            retained_question_history: AtomicBool::new(false),
            retain_question_after_abort: AtomicBool::new(false),
            retained_question_close_history: AtomicBool::new(false),
            retained_question_close_rows: AtomicUsize::new(0),
            prompt_release: Semaphore::new(0),
            prompt_parent: Mutex::new(None),
            withhold_stream_completion: AtomicBool::new(false),
            invalid_stream_response: AtomicBool::new(false),
            malformed_stream_response: AtomicBool::new(false),
            terminal_error: Mutex::new(None),
            provider_status: AtomicUsize::new(200),
            release: Mutex::new(opencode_release::ReleaseFixture::default()),
            tool_models: Mutex::new(Vec::new()),
            reject_abort: AtomicBool::new(false),
            defer_stream_input: AtomicBool::new(false),
            prompt_responses: AtomicUsize::new(0),
            connections: AtomicUsize::new(0),
        });
        let task_stats = Arc::clone(&stats);
        let (disconnect, _) = watch::channel(0);
        let task_disconnect = disconnect.clone();
        let task_events = events.clone();
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else {
                            break;
                        };
                        let stats = Arc::clone(&task_stats);
                        let disconnect = task_disconnect.subscribe();
                        let events = task_events.subscribe();
                        connections.spawn(async move {
                            stats.connections.fetch_add(1, Ordering::SeqCst);
                            let _connection = ProbeConnection { stats: Arc::clone(&stats) };
                            Self::serve(stream, stats, disconnect, events).await;
                        });
                    }
                    _ = &mut stopped => break,
                    _ = task_stats.listener_pause.notified() => {
                        // Existing SSE/task ownership survives; no new HTTP
                        // connection can enter after this actual listener drop.
                        drop(listener);
                        task_stats.listener_paused.add_permits(1);
                        tokio::select! {
                            _ = &mut stopped => break,
                            permit = task_stats.listener_resume.acquire() => {
                                permit.expect("probe listener resume must remain owned").forget();
                            }
                        }
                        listener = TcpListener::bind(address).await
                            .expect("the same owned probe endpoint must restore");
                        task_stats.listener_resumed.add_permits(1);
                    }
                }
                while connections.try_join_next().is_some() {}
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        });
        Self {
            endpoint: format!("http://{address}"),
            stats,
            disconnect,
            events,
            stop: Some(stop),
            task: Some(task),
        }
    }

    async fn serve(
        mut stream: TcpStream,
        stats: Arc<OpenCodeEventStats>,
        mut disconnect: watch::Receiver<u64>,
        mut events: broadcast::Receiver<String>,
    ) {
        let Some(request) = Self::request(&mut stream).await else {
            return;
        };
        stats.requests.fetch_add(1, Ordering::SeqCst);
        let cwd_routing = stats.cwd_routing.lock().unwrap().clone();
        if let Some(routing) = &cwd_routing {
            routing.record(&request);
        }
        if request.method == "POST" && request.path == "/question/que_probe/reject" {
            // Count a forbidden rejection without changing the existing 404 response.
            stats.question_rejections.fetch_add(1, Ordering::SeqCst);
        }
        match request.path.as_str() {
            "/path" => {
                if let Some(routing) = &cwd_routing {
                    routing.reply(&mut stream, &request).await;
                } else {
                    let directory = request
                        .directory
                        .as_deref()
                        .unwrap_or(env!("CARGO_MANIFEST_DIR"));
                    Self::json(
                        &mut stream,
                        &serde_json::json!({"directory": directory}).to_string(),
                    )
                    .await;
                }
            }
            "/global/health" => {
                let health = stats
                    .release
                    .lock()
                    .unwrap()
                    .health
                    .clone()
                    .unwrap_or_else(|| serde_json::json!({"healthy":true,"version":"1.19.3"}));
                Self::json(&mut stream, &health.to_string()).await;
            }
            "/experimental/capabilities" => {
                stats.capability_requests.fetch_add(1, Ordering::SeqCst);
                if stats.stall_capability_headers.swap(false, Ordering::SeqCst) {
                    let mut closed = [0_u8; 1];
                    let _ = stream.read(&mut closed).await;
                    return;
                }
                if stats.stall_capability_body.swap(false, Ordering::SeqCst) {
                    let response = concat!(
                        "HTTP/1.1 200 OK\r\n",
                        "content-type: application/json\r\n",
                        "content-length: 29\r\n",
                        "connection: close\r\n\r\n",
                    );
                    if stream.write_all(response.as_bytes()).await.is_ok() {
                        let mut closed = [0_u8; 1];
                        let _ = stream.read(&mut closed).await;
                    }
                    return;
                }
                Self::json(&mut stream, r#"{"backgroundSubagents":false}"#).await;
            }
            "/config" => {
                let (error, config) = {
                    let mut release = stats.release.lock().unwrap();
                    release.config_reads += 1;
                    (release.config_error.clone(), release.config.clone())
                };
                if let Some(error) = error {
                    let body = error.to_string();
                    let response = format!(
                        "HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                } else {
                    let config = config.unwrap_or_else(|| serde_json::json!({"share":"disabled"}));
                    Self::json(&mut stream, &config.to_string()).await;
                }
            }
            "/provider" => {
                let status = stats.provider_status.load(Ordering::SeqCst);
                if status != 200 {
                    let body = r#"{"message":"WIRE_SECRET","resource":"PRIVATE_PROVIDER"}"#;
                    let response = format!(
                        "HTTP/1.1 {status} Probe\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                    return;
                }
                let body = stats.release.lock().unwrap().inventory();
                Self::json(&mut stream, &body.to_string()).await;
            }
            "/mcp" => Self::json(&mut stream, "{}").await,
            "/experimental/tool/ids" => Self::json(&mut stream, r#"["read"]"#).await,
            "/experimental/tool" => {
                if let Some(model) = request.model {
                    stats.tool_models.lock().unwrap().push(model);
                }
                if stats.block_tool_inventory.swap(false, Ordering::SeqCst) {
                    stats.tool_inventory_blocked.add_permits(1);
                    stats
                        .tool_inventory_release
                        .acquire()
                        .await
                        .expect("tool inventory release must remain open")
                        .forget();
                }
                Self::json(
                    &mut stream,
                    r#"[{"id":"read","description":"Read a file","parameters":{"type":"object","properties":{},"additionalProperties":false}}]"#,
                )
                .await;
            }
            "/session" if request.method == "POST" => {
                let value: serde_json::Value = serde_json::from_slice(&request.body)
                    .expect("session request must contain JSON");
                stats
                    .release
                    .lock()
                    .unwrap()
                    .session_requests
                    .push(value.clone());
                let correlation = value
                    .pointer("/metadata/agent-sdk.creation-correlation")
                    .and_then(serde_json::Value::as_str)
                    .expect("session request must contain its creation correlation")
                    .to_owned();
                let directory = if let Some(routing) = &cwd_routing {
                    routing.native_directory(
                        request
                            .directory
                            .as_deref()
                            .expect("session routing must be explicit"),
                    )
                } else if matches!(
                    *stats.retained_identity_change.lock().unwrap(),
                    Some(RetainedIdentityChange::MixedPermissionDirectory)
                ) {
                    request
                        .directory
                        .clone()
                        .expect("mixed-directory session must retain its requested routing")
                } else {
                    env!("CARGO_MANIFEST_DIR").to_owned()
                };
                let ordinal = stats.next_session.fetch_add(1, Ordering::SeqCst);
                let session_id = if ordinal == 0 {
                    "ses_probe".to_owned()
                } else {
                    format!("ses_probe_{ordinal}")
                };
                stats
                    .sessions
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .insert(
                        session_id.clone(),
                        ProbeSession {
                            correlation: correlation.clone(),
                            directory: directory.clone(),
                        },
                    );
                if session_id == "ses_probe"
                    && stats.session_event_before_binding.load(Ordering::SeqCst)
                {
                    let info = serde_json::json!({
                        "id": &session_id,
                        "slug": "probe",
                        "projectID": "prj_probe",
                        "directory": &directory,
                        "title": "probe",
                        "version": "1.19.3",
                        "metadata": {"agent-sdk.creation-correlation": &correlation},
                        "time": {"created": 1, "updated": 1}
                    });
                    let id = "evt_probe_session_created_before_binding";
                    *stats
                        .history_rows
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                        vec![serde_json::json!({
                            "id": id,
                            "aggregate_id": &session_id,
                            "seq": 0,
                            "type": "session.created.1",
                            "data": {"sessionID": &session_id, "info": &info}
                        })];
                    let live = serde_json::json!({
                        "directory": &directory,
                        "payload": {
                            "id": id,
                            "type": "session.created",
                            "properties": {"sessionID": &session_id, "info": info}
                        }
                    });
                    stats
                        .events
                        .send(live.to_string())
                        .expect("the pre-binding event subscriber must remain owned");
                    tokio::time::timeout(
                        Duration::from_secs(5),
                        stats.session_event_history.acquire(),
                    )
                    .await
                    .expect("pre-binding event must enter same-stream history")
                    .expect("pre-binding history gate must remain open")
                    .forget();
                }
                let body = serde_json::json!({
                    "id": session_id,
                    "parentID": null,
                    "share": null,
                    "metadata": {"agent-sdk.creation-correlation": correlation}
                })
                .to_string();
                Self::json(&mut stream, &body).await;
            }
            "/session/ses_probe_1/message"
                if request.method == "POST"
                    && stats.retained_question_history.load(Ordering::SeqCst) =>
            {
                let value = serde_json::from_slice(&request.body).unwrap();
                Self::retained_secondary_structured_prompt(&mut stream, &stats, value).await;
            }
            "/session/ses_probe/message" if request.method == "POST" => {
                let value: serde_json::Value = serde_json::from_slice(&request.body)
                    .expect("message request must contain JSON");
                for field in ["variant", "reasoning_effort", "personality"] {
                    assert!(
                        value.get(field).is_none(),
                        "portable selectors must not invent native {field}"
                    );
                }
                let correlation = stats.release.lock().unwrap().correlation;
                if let Some(correlation) = correlation {
                    correlation.respond(&mut stream, &stats, value).await;
                    return;
                }
                if stats.retained_question_history.load(Ordering::SeqCst) {
                    Self::retained_question_prompt(&mut stream, &stats, value).await;
                    return;
                }
                if value.get("format").is_none() {
                    stats.messages.fetch_add(1, Ordering::SeqCst);
                    stats.native_active.store(true, Ordering::SeqCst);
                    let part_id = value
                        .pointer("/parts/0/id")
                        .and_then(serde_json::Value::as_str)
                        .expect("message request must contain its request-part identity");
                    let parent_id = "msg_000000000003abcdefghijklmn";
                    let message = serde_json::json!({
                        "directory": env!("CARGO_MANIFEST_DIR"),
                        "payload": {
                            "id": "evt_probe_plain_message",
                            "type": "message.updated",
                            "properties": {
                                "sessionID": "ses_probe",
                                "info": {
                                    "id": parent_id,
                                    "sessionID": "ses_probe",
                                    "role": "user",
                                    "time": {"created": 1.0},
                                    "agent": "agent",
                                    "model": {
                                        "modelID": "model",
                                        "providerID": "provider"
                                    }
                                }
                            }
                        }
                    });
                    let _ = stats.events.send(message.to_string());
                    let part = serde_json::json!({
                        "directory": env!("CARGO_MANIFEST_DIR"),
                        "payload": {
                            "id": "evt_probe_plain_part",
                            "type": "message.part.updated",
                            "properties": {
                                "sessionID": "ses_probe",
                                "part": {
                                    "id": part_id,
                                    "sessionID": "ses_probe",
                                    "messageID": parent_id,
                                    "type": "text",
                                    "text": "plain probe"
                                },
                                "time": 1.0
                            }
                        }
                    });
                    if !stats.defer_stream_input.load(Ordering::SeqCst) {
                        let _ = stats.events.send(part.to_string());
                    }
                    if stats.stream_fixture.load(Ordering::SeqCst) {
                        *stats.prompt_parent.lock().unwrap() = Some(serde_json::json!({
                            "info": message["payload"]["properties"]["info"],
                            "parts": [part["payload"]["properties"]["part"]]
                        }));
                        *stats.history_rows.lock().unwrap() = vec![
                            serde_json::json!({
                                "id": "evt_probe_plain_message",
                                "aggregate_id": "ses_probe", "seq": 0,
                                "type": "message.updated.1",
                                "data": message["payload"]["properties"]
                            }),
                            serde_json::json!({
                                "id": "evt_probe_plain_part",
                                "aggregate_id": "ses_probe", "seq": 1,
                                "type": "message.part.updated.1",
                                "data": part["payload"]["properties"]
                            }),
                        ];
                    }
                    let busy = serde_json::json!({
                        "directory": env!("CARGO_MANIFEST_DIR"),
                        "payload": {
                            "id": "evt_probe_busy",
                            "type": "session.status",
                            "properties": {
                                "sessionID": "ses_probe",
                                "status": {"type": "busy"}
                            }
                        }
                    });
                    let _ = stats.events.send(busy.to_string());
                    if stats.question_on_prompt.load(Ordering::SeqCst) {
                        let question = serde_json::json!({
                            "directory": env!("CARGO_MANIFEST_DIR"),
                            "payload": {
                                "id": "evt_probe_question",
                                "type": "question.asked",
                                "properties": {
                                    "id": "que_probe",
                                    "sessionID": "ses_probe",
                                    "questions": [{
                                        "question": "Continue?",
                                        "header": "Probe",
                                        "options": [],
                                        "multiple": false,
                                        "custom": true
                                    }]
                                }
                            }
                        });
                        let _ = stats.events.send(question.to_string());
                    }
                    if stats.stream_fixture.load(Ordering::SeqCst) {
                        stats.prompt_release.acquire().await.unwrap().forget();
                        if stats.lose_stream_response.load(Ordering::SeqCst) {
                            // The accepted request loses its transport without a
                            // fabricated assistant terminal or native idle event.
                            stats.prompt_responses.fetch_add(1, Ordering::SeqCst);
                            return;
                        }
                        stats.native_active.store(false, Ordering::SeqCst);
                        if !stats.withhold_stream_completion.load(Ordering::SeqCst) {
                            let event = serde_json::json!({
                                "directory": env!("CARGO_MANIFEST_DIR"),
                                "payload": {
                                    "id": "evt_stream_completed", "type": "message.updated",
                                    "properties": {"sessionID": "ses_probe", "info":
                                        OpenCodeStreamCase::assistant(OpenCodeStreamCase::ASSISTANT, parent_id)
                                    }
                                }
                            });
                            let _ = stats.events.send(event.to_string());
                        }
                        let mut body = serde_json::json!({
                            "info": OpenCodeStreamCase::assistant(OpenCodeStreamCase::ASSISTANT, parent_id),
                            "parts": [{
                                "id": "prt_stream_text", "sessionID": "ses_probe",
                                "messageID": OpenCodeStreamCase::ASSISTANT, "type": "text",
                                "text": "authoritative HTTP answer"
                            }]
                        });
                        if let Some(error) = stats.terminal_error.lock().unwrap().clone() {
                            body["info"]["error"] = error;
                        }
                        if stats.invalid_stream_response.load(Ordering::SeqCst) {
                            body["info"]["id"] = serde_json::json!("msg_invalid");
                        }
                        if stats.malformed_stream_response.load(Ordering::SeqCst) {
                            Self::json(&mut stream, "{").await;
                        } else {
                            Self::json(&mut stream, &body.to_string()).await;
                        }
                        stats.prompt_responses.fetch_add(1, Ordering::SeqCst);
                    } else {
                        std::future::pending::<()>().await;
                    }
                    return;
                }
                if stats.active_on_structured.load(Ordering::SeqCst) {
                    stats.native_active.store(true, Ordering::SeqCst);
                    let busy = serde_json::json!({
                        "directory": env!("CARGO_MANIFEST_DIR"),
                        "payload": {
                            "id": "evt_probe_structured_busy",
                            "type": "session.status",
                            "properties": {
                                "sessionID": "ses_probe",
                                "status": {"type": "busy"}
                            }
                        }
                    });
                    let _ = stats.events.send(busy.to_string());
                }
                let part_id = value
                    .pointer("/parts/0/id")
                    .and_then(serde_json::Value::as_str)
                    .expect("message request must contain its request-part identity");
                let parent_id = "msg_000000000001abcdefghijklmn";
                let response_id = "msg_000000000002abcdefghijklmn";
                *stats
                    .history_rows
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = vec![
                    serde_json::json!({
                        "id": "evt_probe_parent_0",
                        "aggregate_id": "ses_probe",
                        "seq": 0,
                        "type": "message.updated.1",
                        "data": {
                            "sessionID": "ses_probe",
                            "info": {
                                "id": parent_id,
                                "sessionID": "ses_probe",
                                "role": "user",
                                "time": {"created": 1.0},
                                "agent": "agent",
                                "model": {"modelID": "model", "providerID": "provider"}
                            }
                        }
                    }),
                    serde_json::json!({
                        "id": "evt_probe_parent_1",
                        "aggregate_id": "ses_probe",
                        "seq": 1,
                        "type": "message.part.updated.1",
                        "data": {
                            "sessionID": "ses_probe",
                            "part": {
                                "id": part_id,
                                "sessionID": "ses_probe",
                                "messageID": parent_id,
                                "type": "text",
                                "text": "structured probe"
                            },
                            "time": 1.0
                        }
                    }),
                ];
                let body = serde_json::json!({
                    "info": {
                        "id": response_id,
                        "sessionID": "ses_probe",
                        "role": "assistant",
                        "time": {"created": 1, "completed": 2},
                        "parentID": parent_id,
                        "modelID": "model",
                        "providerID": "provider",
                        "mode": "build",
                        "agent": "agent",
                        "path": {
                            "cwd": env!("CARGO_MANIFEST_DIR"),
                            "root": env!("CARGO_MANIFEST_DIR")
                        },
                        "cost": 0,
                        "tokens": {
                            "input": 0,
                            "output": 0,
                            "reasoning": 0,
                            "cache": {"read": 0, "write": 0}
                        },
                        "structured": {"ack": "STRUCTURED_ACK"}
                    },
                    "parts": [{
                        "id": "prt_probe_output",
                        "sessionID": "ses_probe",
                        "messageID": response_id,
                        "type": "text",
                        "text": "structured"
                    }]
                })
                .to_string();
                Self::json(&mut stream, &body).await;
            }
            path if stats.retained_question_history.load(Ordering::SeqCst)
                && request.method == "GET"
                && path.starts_with("/session/ses_probe/message/") =>
            {
                let message = path.strip_prefix("/session/ses_probe/message/").unwrap();
                // Native message reads return the current projection of immutable
                // updates. Keep both generations available for exact tool →
                // assistant → user/input-part ownership checks.
                let response = {
                    let rows = stats.history_rows.lock().unwrap();
                    let mut info = None;
                    let mut parts = BTreeMap::new();
                    for row in rows.iter() {
                        if row["type"] == "message.updated.1"
                            && row["data"]["info"]["id"] == message
                        {
                            info = Some(row["data"]["info"].clone());
                        }
                        if row["type"] == "message.part.updated.1"
                            && row["data"]["part"]["messageID"] == message
                        {
                            let part = row["data"]["part"].clone();
                            parts.insert(part["id"].as_str().unwrap().to_owned(), part);
                        }
                    }
                    info.map(|info| serde_json::json!({"info": info, "parts": parts.into_values().collect::<Vec<_>>()}))
                };
                match response {
                    Some(response) => Self::json(&mut stream, &response.to_string()).await,
                    None => {
                        let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n").await;
                    }
                }
            }
            path if request.method == "GET"
                && (path == "/session/ses_probe/message/msg_000000000003abcdefghijklmn"
                    || (path.starts_with("/session/ses_probe/message/")
                        && stats.release.lock().unwrap().correlation.is_some())) =>
            {
                let parent = stats.prompt_parent.lock().unwrap().clone().unwrap();
                Self::json(&mut stream, &parent.to_string()).await;
            }
            path if request.method == "DELETE"
                && path
                    .strip_prefix("/session/")
                    .is_some_and(|id| id != "status" && !id.is_empty() && !id.contains('/')) =>
            {
                let session_id = path
                    .strip_prefix("/session/")
                    .expect("exact session delete path must contain an identity");
                stats.deletes.fetch_add(1, Ordering::SeqCst);
                stats
                    .sessions
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .remove(session_id);
                if stats.retained_question_history.load(Ordering::SeqCst) {
                    stats
                        .history_rows
                        .lock()
                        .unwrap()
                        .retain(|row| row["aggregate_id"] != session_id);
                }
                Self::json(&mut stream, "true").await;
            }
            path if request.method == "GET"
                && path
                    .strip_prefix("/session/")
                    .is_some_and(|id| id != "status" && !id.is_empty() && !id.contains('/')) =>
            {
                let session_id = path
                    .strip_prefix("/session/")
                    .expect("exact session read path must contain an identity");
                let session = stats
                    .sessions
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .get(session_id)
                    .cloned()
                    .expect("session must have been created");
                let invalid = stats
                    .invalid_session_reads
                    .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                        remaining.checked_sub(1)
                    })
                    .is_ok();
                let permission = (!invalid).then(
                    || serde_json::json!([{"permission": "*", "pattern": "*", "action": "allow"}]),
                );
                let body = serde_json::json!({
                    "id": session_id,
                    "directory": session.directory,
                    "parentID": null,
                    "share": null,
                    "metadata": {"agent-sdk.creation-correlation": session.correlation},
                    "permission": permission
                })
                .to_string();
                Self::json(&mut stream, &body).await;
            }
            "/session/ses_probe/abort" if request.method == "POST" => {
                if stats
                    .abort_authority
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_some_and(|authority| {
                        authority
                            .cancellation_dispatch_receipts
                            .lock()
                            .unwrap()
                            .is_empty()
                    })
                {
                    stats
                        .aborts_without_dispatch_commit
                        .fetch_add(1, Ordering::SeqCst);
                }
                stats.aborts.fetch_add(1, Ordering::SeqCst);
                if stats.lose_abort_response.load(Ordering::SeqCst) {
                    // The actual abort request entered, but no response or
                    // native effect establishes acceptance for its caller.
                    return;
                }
                if stats.reject_abort_without_effects.load(Ordering::SeqCst) {
                    Self::json(&mut stream, "false").await;
                    return;
                }
                if stats.block_abort.load(Ordering::SeqCst) {
                    std::future::pending::<()>().await;
                    return;
                }
                let was_active = stats.native_active.swap(false, Ordering::SeqCst);
                if stats.retained_question_history.load(Ordering::SeqCst) && was_active {
                    if !stats.retain_question_after_abort.load(Ordering::SeqCst) {
                        stats.pending_question.store(false, Ordering::SeqCst);
                    }
                    Self::retained_question_abort(&stats);
                    stats.prompt_release.add_permits(1);
                }
                let idle = serde_json::json!({
                    "directory": env!("CARGO_MANIFEST_DIR"),
                    "payload": {
                        "id": "evt_probe_idle",
                        "type": "session.idle",
                        "properties": {"sessionID": "ses_probe"}
                    }
                });
                let publication =
                    stats
                        .pending_cleanup_faults
                        .lock()
                        .unwrap()
                        .clone()
                        .filter(|faults| {
                            stats.malformed_stream_response.load(Ordering::SeqCst)
                                && faults.persistence_fault()
                                    == PromptPersistenceFault::NativeCleanupCommitReceiptLost
                        });
                let body = if stats.reject_abort.load(Ordering::SeqCst) {
                    "false"
                } else {
                    "true"
                };
                if let Some(faults) = publication {
                    // This scenario needs an actual committed idle preparation.
                    // Publish the same native idle event before the abort reply
                    // releases that writer; no event or native outcome changes.
                    stats
                        .events
                        .send(idle.to_string())
                        .expect("the idle event owner remains connected");
                    tokio::time::timeout(Duration::from_secs(3), async {
                        loop {
                            let progress = faults
                                .inner
                                .list(
                                    StateListRequest::new(OPENCODE_EVENT_PROGRESS_NAMESPACE)
                                        .unwrap(),
                                )
                                .await
                                .unwrap();
                            if progress.records.iter().any(|record| {
                                record.record.value["recent"]
                                    .as_array()
                                    .is_some_and(|recent| {
                                        recent.iter().any(|event| {
                                            event["source"] == "normal"
                                                && event["id"] == idle["payload"]["id"]
                                                && event["event_type"] == idle["payload"]["type"]
                                                && event["data"] == idle["payload"]["properties"]
                                        })
                                    })
                            }) {
                                break;
                            }
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .expect("the exact idle event must publish before the selected abort reply");
                    Self::json(&mut stream, body).await;
                } else {
                    Self::json(&mut stream, body).await;
                    tokio::task::yield_now().await;
                    let _ = stats.events.send(idle.to_string());
                }
            }
            "/session/status" => {
                stats.status_requests.fetch_add(1, Ordering::SeqCst);
                if stats.unavailable_status.load(Ordering::SeqCst) {
                    Self::unavailable(&mut stream).await;
                    if stats.aborts.load(Ordering::SeqCst) > 0 {
                        stats
                            .rejected_status_failures
                            .fetch_add(1, Ordering::SeqCst);
                    }
                    return;
                }
                let body = if stats.native_active.load(Ordering::SeqCst) {
                    r#"{"ses_probe":{"type":"busy"}}"#
                } else {
                    "{}"
                };
                Self::json(&mut stream, body).await;
                if body != "{}" {
                    stats.busy_status_responses.fetch_add(1, Ordering::SeqCst);
                }
            }
            "/experimental/session" => {
                let sessions = stats
                    .sessions
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone();
                let body = serde_json::Value::Array(
                    sessions
                        .into_iter()
                        .map(|(id, session)| {
                            serde_json::json!({
                                "id": id,
                                "directory": session.directory,
                                "parentID": null,
                                "metadata": {
                                    "agent-sdk.creation-correlation": session.correlation
                                }
                            })
                        })
                        .collect(),
                )
                .to_string();
                Self::json(&mut stream, &body).await;
                stats.inventory_responses.fetch_add(1, Ordering::SeqCst);
                stats.inventory_response.add_permits(1);
            }
            "/sync/history" => {
                stats.history.fetch_add(1, Ordering::SeqCst);
                if stats
                    .session_event_before_binding
                    .swap(false, Ordering::SeqCst)
                {
                    stats.session_event_history.add_permits(1);
                }
                if stats.block_history.load(Ordering::SeqCst) {
                    stats.history_release.notified().await;
                }
                let failure = stats.subscriptions.load(Ordering::SeqCst) > 0
                    && stats
                        .history_failures
                        .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                            if remaining == 0 {
                                None
                            } else {
                                Some(remaining - 1)
                            }
                        })
                        .is_ok();
                if failure {
                    Self::unavailable(&mut stream).await;
                } else {
                    let mut rows = stats
                        .history_rows
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .clone();
                    if stats.retained_question_history.load(Ordering::SeqCst) {
                        let known: BTreeMap<String, u64> =
                            serde_json::from_slice(&request.body).unwrap();
                        rows.retain(|row| {
                            known
                                .get(row["aggregate_id"].as_str().unwrap())
                                .is_none_or(|sequence| row["seq"].as_u64().unwrap() > *sequence)
                        });
                        if stats
                            .retained_question_close_history
                            .swap(false, Ordering::SeqCst)
                        {
                            stats
                                .retained_question_close_rows
                                .store(rows.len(), Ordering::SeqCst);
                            for row in &rows {
                                stats.events.send(serde_json::json!({
                                    "directory": env!("CARGO_MANIFEST_DIR"),
                                    "payload": {"id": row["id"], "type": "sync", "syncEvent": {
                                        "id": row["id"], "aggregateID": row["aggregate_id"],
                                        "seq": row["seq"], "type": row["type"], "data": row["data"]
                                    }}
                                }).to_string()).unwrap();
                            }
                        }
                    }
                    Self::json(&mut stream, &serde_json::Value::Array(rows).to_string()).await;
                }
            }
            "/permission" => {
                stats.pending_scans.fetch_add(1, Ordering::SeqCst);
                let mut pending = stats.pending_permissions.lock().unwrap().clone();
                if matches!(
                    *stats.retained_identity_change.lock().unwrap(),
                    Some(RetainedIdentityChange::MixedPermissionDirectory)
                ) {
                    let directory = request
                        .directory
                        .as_deref()
                        .expect("pending inventory requires native routing");
                    let primary = env!("CARGO_MANIFEST_DIR");
                    let secondary = PathBuf::from(primary).join("tests");
                    if stats.aborts.load(Ordering::SeqCst) == 0 {
                        if directory != primary {
                            pending.clear();
                        }
                    } else {
                        let selected = if directory == primary {
                            "per_retained_0"
                        } else {
                            assert_eq!(PathBuf::from(directory), secondary);
                            "per_retained_1"
                        };
                        pending.retain(|request| request["id"] == selected);
                        if !pending.is_empty() {
                            stats
                                .mixed_permission_directories
                                .lock()
                                .unwrap()
                                .insert(directory.to_owned());
                        }
                    }
                }
                if let Some(change) = *stats.retained_identity_change.lock().unwrap() {
                    for request in &mut pending {
                        change.change(request, true);
                    }
                }
                if stats.aborts.load(Ordering::SeqCst) > 0
                    && pending
                        .iter()
                        .any(|request| request["id"] == "per_retained_0")
                    && let Some(faults) = stats.pending_cleanup_faults.lock().unwrap().as_ref()
                {
                    faults.arm_native_cleanup_receipt();
                }
                Self::json(&mut stream, &serde_json::Value::Array(pending).to_string()).await;
            }
            path if request.method == "POST"
                && path.starts_with("/permission/per_retained_")
                && path.ends_with("/reply") =>
            {
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&request.body).unwrap()["reply"],
                    "reject"
                );
                if let Some(faults) = stats.pending_cleanup_faults.lock().unwrap().as_ref() {
                    faults.record_native_cleanup_dispatch(path.split('/').nth(2).unwrap());
                }
                stats.permission_rejections.fetch_add(1, Ordering::SeqCst);
                let mode = *stats.retained_rejection.lock().unwrap();
                stats.rejection_entered.add_permits(1);
                if matches!(
                    mode,
                    RetainedRejection::Held
                        | RetainedRejection::HeldDroppedObserver
                        | RetainedRejection::HeldAcknowledgedAfterDisconnectWithoutEvent
                ) {
                    stats.rejection_release.acquire().await.unwrap().forget();
                }
                let id = path
                    .strip_prefix("/permission/")
                    .unwrap()
                    .strip_suffix("/reply")
                    .unwrap();
                // Pinned Permission.reject cancels every pending request in the
                // selected request's session, including parallel tool requests.
                let cancelled = {
                    let mut pending = stats.pending_permissions.lock().unwrap();
                    let session = pending
                        .iter()
                        .find(|value| value["id"] == id)
                        .map(|value| value["sessionID"].clone());
                    session.map(|session| {
                        let cancelled = pending
                            .iter()
                            .filter(|value| value["sessionID"] == session)
                            .cloned()
                            .collect::<Vec<_>>();
                        pending.retain(|value| value["sessionID"] != session);
                        cancelled
                    })
                };
                let Some(cancelled) = cancelled else {
                    stream.write_all(b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n").await.unwrap();
                    return;
                };
                for request in cancelled {
                    stats.events.send(serde_json::json!({
                        "directory": env!("CARGO_MANIFEST_DIR"),
                        "payload": {"id": format!("evt_rejected_{}", request["id"].as_str().unwrap()),
                            "type": "permission.replied", "properties": {
                                "sessionID": request["sessionID"], "requestID": request["id"], "reply": "reject"
                            }}
                    }).to_string()).unwrap();
                }
                Self::json(&mut stream, "true").await;
            }
            "/question/que_probe/reply" if request.method == "POST" => {
                stats.question_replies.fetch_add(1, Ordering::SeqCst);
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&request.body).unwrap(),
                    serde_json::json!({"answers": [["Yes"]]})
                );
                let status = stats.question_reply_status.load(Ordering::SeqCst);
                if status != 200 {
                    let reason = match status {
                        400 => "Bad Request",
                        404 => "Not Found",
                        _ => panic!("unsupported question reply fixture status"),
                    };
                    stream.write_all(format!(
                        "HTTP/1.1 {status} {reason}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                    ).as_bytes()).await.unwrap();
                    return;
                }
                // The selected answer has been consumed. Losing its response
                // or following a redirect cannot undo this recorded effect.
                stats
                    .question_reply_acceptances
                    .fetch_add(1, Ordering::SeqCst);
                let redirect = stats.question_reply_redirect.lock().unwrap().clone();
                if let Some(location) = redirect {
                    if stats.pause_before_reply_redirect.load(Ordering::SeqCst) {
                        stats.listener_pause.notify_one();
                        stats.listener_paused.acquire().await.unwrap().forget();
                    }
                    stream.write_all(format!(
                        "HTTP/1.1 307 Temporary Redirect\r\nlocation: {location}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                    ).as_bytes()).await.unwrap();
                    stats
                        .question_reply_redirects
                        .fetch_add(1, Ordering::SeqCst);
                    return;
                }
                if stats.lose_question_reply_response.load(Ordering::SeqCst) {
                    return;
                }
                Self::json(&mut stream, "true").await;
            }
            path if matches!(
                path,
                "/question/que_probe_pending/reject" | "/question/que_probe_peer/reject"
            ) && request.method == "POST"
                && stats.retained_question_history.load(Ordering::SeqCst) =>
            {
                // Native Question.reject removes exactly this pending identity,
                // publishes rejection and settles its original deferred ask. The
                // optional wire failures distinguish effect from its receipt.
                if let Some(faults) = stats.pending_cleanup_faults.lock().unwrap().as_ref() {
                    faults.record_native_cleanup_dispatch(path.split('/').nth(2).unwrap());
                }
                stats.question_rejections.fetch_add(1, Ordering::SeqCst);
                let mode = *stats.retained_rejection.lock().unwrap();
                stats.rejection_entered.add_permits(1);
                if matches!(
                    mode,
                    RetainedRejection::Held
                        | RetainedRejection::HeldDroppedObserver
                        | RetainedRejection::HeldAcknowledgedAfterDisconnectWithoutEvent
                ) {
                    stats.rejection_release.acquire().await.unwrap().forget();
                }
                if matches!(mode, RetainedRejection::Unavailable) {
                    Self::unavailable(&mut stream).await;
                    return;
                }
                let (id, existed) = if path == "/question/que_probe_pending/reject" {
                    (
                        "que_probe_pending",
                        stats.pending_question.swap(false, Ordering::SeqCst),
                    )
                } else {
                    (
                        "que_probe_peer",
                        stats.pending_peer_question.swap(false, Ordering::SeqCst),
                    )
                };
                if !existed || matches!(mode, RetainedRejection::Missing) {
                    stream.write_all(b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n").await.unwrap();
                    return;
                }
                if !matches!(
                    mode,
                    RetainedRejection::LostWithoutEvent
                        | RetainedRejection::AcknowledgedWithoutEvent
                        | RetainedRejection::HeldAcknowledgedAfterDisconnectWithoutEvent
                ) {
                    let (kind, properties) = if matches!(
                        mode,
                        RetainedRejection::ContradictoryAnswer
                    ) {
                        (
                            "question.replied",
                            serde_json::json!({"sessionID": "ses_probe", "requestID": id, "answers": [["Yes"]]}),
                        )
                    } else {
                        (
                            "question.rejected",
                            serde_json::json!({"sessionID": "ses_probe", "requestID": id}),
                        )
                    };
                    stats
                        .events
                        .send(
                            serde_json::json!({
                                "directory": env!("CARGO_MANIFEST_DIR"),
                                "payload": {"id": if id == "que_probe_pending" { "evt_retained_question_rejected" } else { "evt_retained_question_peer_rejected" }, "type": kind,
                                    "properties": properties}
                            })
                            .to_string(),
                        )
                        .unwrap();
                }
                if !matches!(
                    mode,
                    RetainedRejection::LostWithEvent
                        | RetainedRejection::LostWithoutEvent
                        | RetainedRejection::ContradictoryAnswer
                ) {
                    Self::json(&mut stream, "true").await;
                }
            }
            "/question" => {
                stats.pending_scans.fetch_add(1, Ordering::SeqCst);
                let body = if stats.pending_question.load(Ordering::SeqCst)
                    || stats.pending_peer_question.load(Ordering::SeqCst)
                {
                    let mut questions = serde_json::json!([{
                        "id": if stats.lose_stream_response.load(Ordering::SeqCst) { "que_probe" } else { "que_probe_pending" },
                        "sessionID": "ses_probe",
                        "questions": [{
                            "question": "Continue?",
                            "header": "Probe",
                            "options": [],
                            "multiple": false,
                            "custom": true
                        }]
                    }]);
                    if stats.retained_question_history.load(Ordering::SeqCst) {
                        questions[0]["tool"] = serde_json::json!({
                            "messageID": "msg_000000000006abcdefghijklmn",
                            "callID": "call_retained_question"
                        });
                    }
                    if stats.pending_peer_question.load(Ordering::SeqCst) {
                        let mut peer = questions[0].clone();
                        peer["id"] = serde_json::json!("que_probe_peer");
                        peer["tool"]["callID"] = serde_json::json!("call_retained_question_peer");
                        questions.as_array_mut().unwrap().push(peer);
                    }
                    if !stats.pending_question.load(Ordering::SeqCst) {
                        questions.as_array_mut().unwrap().remove(0);
                    }
                    if let Some(change) = *stats.retained_identity_change.lock().unwrap() {
                        for question in questions.as_array_mut().unwrap() {
                            change.change(question, false);
                        }
                    }
                    if stats.aborts.load(Ordering::SeqCst) > 0
                        && let Some(faults) = stats.pending_cleanup_faults.lock().unwrap().as_ref()
                    {
                        faults.arm_native_cleanup_receipt();
                    }
                    questions.to_string()
                } else {
                    "[]".to_owned()
                };
                Self::json(&mut stream, &body).await;
            }
            "/global/event" => {
                stats.subscriptions.fetch_add(1, Ordering::SeqCst);
                let active = stats.active.fetch_add(1, Ordering::SeqCst) + 1;
                stats.maximum.fetch_max(active, Ordering::SeqCst);
                let _active = ActiveEventConnection {
                    stats: Arc::clone(&stats),
                };
                if stats.stall_event_headers.load(Ordering::SeqCst) {
                    let mut closed = [0_u8; 1];
                    tokio::select! {
                        _ = stats.event_headers_release.notified() => {}
                        _ = stream.read(&mut closed) => return,
                    }
                }
                let response = concat!(
                    "HTTP/1.1 200 OK\r\n",
                    "content-type: text/event-stream\r\n",
                    "cache-control: no-cache\r\n",
                    "connection: keep-alive\r\n",
                    "transfer-encoding: chunked\r\n\r\n",
                );
                if stream.write_all(response.as_bytes()).await.is_err() {
                    return;
                }
                let mut closed = [0_u8; 1];
                loop {
                    let activity = tokio::select! {
                        _ = disconnect.changed() => ProbeStreamActivity::Disconnect,
                        event = events.recv() => ProbeStreamActivity::Event(event),
                        _ = stream.read(&mut closed) => ProbeStreamActivity::Closed,
                    };
                    match activity {
                        ProbeStreamActivity::Disconnect | ProbeStreamActivity::Closed => break,
                        ProbeStreamActivity::Event(Ok(event)) => {
                            if Self::sse(&mut stream, &event).await.is_err() {
                                break;
                            }
                        }
                        ProbeStreamActivity::Event(Err(broadcast::error::RecvError::Lagged(_))) => {
                        }
                        ProbeStreamActivity::Event(Err(broadcast::error::RecvError::Closed)) => {
                            break;
                        }
                    }
                }
            }
            _ => {
                let response = concat!(
                    "HTTP/1.1 404 Not Found\r\n",
                    "content-length: 0\r\n",
                    "connection: close\r\n\r\n",
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        }
    }

    async fn request(stream: &mut TcpStream) -> Option<ProbeRequest> {
        let mut request = Vec::new();
        let mut chunk = [0_u8; 1_024];
        let header_end = loop {
            let read = stream.read(&mut chunk).await.ok()?;
            if read == 0 {
                return None;
            }
            request.extend_from_slice(&chunk[..read]);
            if request.len() > 64 * 1_024 {
                return None;
            }
            if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                break end + 4;
            }
        };
        let headers = std::str::from_utf8(&request[..header_end]).ok()?;
        let length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0);
        let mut request_line = headers.lines().next()?.split_whitespace();
        let method = request_line.next()?.to_owned();
        let target = request_line.next()?.to_owned();
        while request.len() < header_end + length {
            let read = stream.read(&mut chunk).await.ok()?;
            if read == 0 {
                return None;
            }
            request.extend_from_slice(&chunk[..read]);
        }
        let url = reqwest::Url::parse(&format!("http://localhost{target}")).ok()?;
        let query: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
        Some(ProbeRequest {
            model: query
                .get("provider")
                .zip(query.get("model"))
                .map(|(provider, model)| (provider.clone(), model.clone())),
            method,
            path: target.split('?').next()?.to_owned(),
            directory: url
                .query_pairs()
                .find_map(|(name, value)| (name == "directory").then(|| value.into_owned())),
            body: request[header_end..header_end + length].to_vec(),
        })
    }

    async fn json(stream: &mut TcpStream, body: &str) {
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = stream.write_all(response.as_bytes()).await;
    }

    async fn unavailable(stream: &mut TcpStream) {
        let response = concat!(
            "HTTP/1.1 503 Service Unavailable\r\n",
            "content-length: 0\r\n",
            "connection: close\r\n\r\n",
        );
        let _ = stream.write_all(response.as_bytes()).await;
    }

    async fn sse(stream: &mut TcpStream, event: &str) -> std::io::Result<()> {
        let payload = format!("data: {event}\n\n");
        let chunk = format!("{:X}\r\n{payload}\r\n", payload.len());
        stream.write_all(chunk.as_bytes()).await
    }

    fn disconnect_current(&self) {
        self.disconnect.send_modify(|generation| *generation += 1);
    }

    // Pinned OpenCode v1.19.3 event-v2-bridge.ts publishes a normal event and
    // its same-ID durable sync event; handlers/sync.ts reads retained EventTable
    // rows. This mode preserves both prompt generations instead of replacing
    // the first rows; their sync frames wait for the explicit close history
    // response, exercising overlap of history with the same-ID live facts.
    fn retain_question_event(stats: &OpenCodeEventStats, kind: &str, data: serde_json::Value) {
        let mut rows = stats.history_rows.lock().unwrap();
        let sequence = rows.len();
        let id = format!("evt_retained_question_{sequence}");
        rows.push(serde_json::json!({
            "id": id, "aggregate_id": "ses_probe", "seq": sequence,
            "type": format!("{kind}.1"), "data": data
        }));
        let normal = serde_json::json!({
            "directory": env!("CARGO_MANIFEST_DIR"),
            "payload": {"id": id, "type": kind, "properties": data}
        });
        stats.events.send(normal.to_string()).unwrap();
    }

    async fn retained_secondary_structured_prompt(
        stream: &mut TcpStream,
        stats: &OpenCodeEventStats,
        request: serde_json::Value,
    ) {
        assert!(request.get("format").is_some());
        assert!(stats.sessions.lock().unwrap().contains_key("ses_probe_1"));
        assert_eq!(stats.messages.fetch_add(1, Ordering::SeqCst), 1);
        let session = "ses_probe_1";
        let parent = "msg_000000000020abcdefghijklmn";
        let assistant = "msg_000000000021abcdefghijklmn";
        let user = serde_json::json!({
            "id": parent, "sessionID": session, "role": "user", "time": {"created": 1},
            "agent": "agent", "model": {"modelID": "model", "providerID": "provider"}
        });
        let input = serde_json::json!({
            "id": request["parts"][0]["id"], "sessionID": session, "messageID": parent,
            "type": "text", "text": request["parts"][0]["text"]
        });
        {
            let mut rows = stats.history_rows.lock().unwrap();
            assert!(!rows.iter().any(|row| row["aggregate_id"] == session));
            // B's parent is available only through its own immutable history,
            // after its actual HTTP request. A's history remains unchanged.
            rows.extend([
                serde_json::json!({"id": "evt_scoped_structured_user", "aggregate_id": session,
                    "seq": 0, "type": "message.updated.1", "data": {"sessionID": session, "info": user}}),
                serde_json::json!({"id": "evt_scoped_structured_input", "aggregate_id": session,
                    "seq": 1, "type": "message.part.updated.1", "data": {"sessionID": session, "part": input, "time": 1}}),
            ]);
        }
        let mut info = OpenCodeStreamCase::assistant(assistant, parent);
        info["sessionID"] = serde_json::json!(session);
        info["structured"] = serde_json::json!({"ack": "STRUCTURED_ACK"});
        Self::json(
            stream,
            &serde_json::json!({"info": info, "parts": [{
                "id": "prt_scoped_structured_output", "sessionID": session,
                "messageID": assistant, "type": "text", "text": "structured"
            }]})
            .to_string(),
        )
        .await;
        stats.prompt_responses.fetch_add(1, Ordering::SeqCst);
    }

    async fn retained_question_prompt(
        stream: &mut TcpStream,
        stats: &OpenCodeEventStats,
        request: serde_json::Value,
    ) {
        assert!(request.get("format").is_none());
        let ordinal = stats.messages.fetch_add(1, Ordering::SeqCst);
        assert!(
            ordinal < 2,
            "the fixture permits exactly two native submissions"
        );
        // Creation-time ordering permits a User identity greater than its Assistant.
        // All retained-origin controls still verify the exact session, tool and request part.
        let parent = format!(
            "msg_00000000000{}abcdefghijklmn",
            if ordinal == 0 { 9 } else { 7 }
        );
        let assistant = format!("msg_00000000000{}abcdefghijklmn", ordinal * 2 + 6);
        let input = serde_json::json!({
            "id": request["parts"][0]["id"], "sessionID": "ses_probe",
            "messageID": parent, "type": "text", "text": request["parts"][0]["text"]
        });
        let user = serde_json::json!({
            "id": parent, "sessionID": "ses_probe", "role": "user",
            "time": {"created": 1}, "agent": "agent",
            "model": {"modelID": "model", "providerID": "provider"}
        });
        *stats.prompt_parent.lock().unwrap() =
            Some(serde_json::json!({"info": user, "parts": [input]}));
        stats.native_active.store(true, Ordering::SeqCst);
        Self::retain_question_event(
            stats,
            "message.updated",
            serde_json::json!({"sessionID": "ses_probe", "info": user}),
        );
        Self::retain_question_event(
            stats,
            "message.part.updated",
            serde_json::json!({"sessionID": "ses_probe", "part": input, "time": 1}),
        );
        stats
            .events
            .send(
                serde_json::json!({
                    "directory": env!("CARGO_MANIFEST_DIR"),
                    "payload": {"id": format!("evt_retained_busy_{ordinal}"),
                        "type": "session.status", "properties": {
                            "sessionID": "ses_probe", "status": {"type": "busy"}
                        }}
                })
                .to_string(),
            )
            .unwrap();
        let mut info = OpenCodeStreamCase::assistant(&assistant, &parent);
        if ordinal == 0 {
            info["time"].as_object_mut().unwrap().remove("completed");
            Self::retain_question_event(
                stats,
                "message.updated",
                serde_json::json!({"sessionID": "ses_probe", "info": info}),
            );
            let permission = stats.retained_permissions.load(Ordering::SeqCst);
            for ordinal in
                0..if permission || stats.retained_parallel_questions.load(Ordering::SeqCst) {
                    2
                } else {
                    1
                }
            {
                let call = if permission {
                    format!("call_retained_permission_{ordinal}")
                } else if ordinal == 0 {
                    "call_retained_question".to_owned()
                } else {
                    "call_retained_question_peer".to_owned()
                };
                let part = if permission {
                    format!("prt_retained_permission_{ordinal}")
                } else if ordinal == 0 {
                    "prt_retained_question".to_owned()
                } else {
                    "prt_retained_question_peer".to_owned()
                };
                Self::retain_question_event(
                    stats,
                    "message.part.updated",
                    serde_json::json!({
                        "sessionID": "ses_probe", "time": 1, "part": {
                            "id": part, "sessionID": "ses_probe", "messageID": assistant,
                            "type": "tool", "callID": call,
                            "tool": if permission { "bash" } else { "question" },
                            "state": {"status": "running", "input": {}, "time": {"start": 1}}
                        }
                    }),
                );
                let request = if permission {
                    serde_json::json!({
                        "id": format!("per_retained_{ordinal}"), "sessionID": "ses_probe",
                        "permission": "bash", "patterns": [format!("echo permission_{ordinal}")],
                        "metadata": {"command": format!("echo permission_{ordinal}")}, "always": ["echo *"],
                        "tool": {"messageID": assistant, "callID": call}
                    })
                } else {
                    serde_json::json!({
                        "id": if ordinal == 0 { "que_probe_pending" } else { "que_probe_peer" }, "sessionID": "ses_probe",
                        "tool": {"messageID": assistant, "callID": call},
                        "questions": [{"question": "Continue?", "header": "Probe",
                            "options": [], "multiple": false, "custom": true}]
                    })
                };
                if permission {
                    stats
                        .pending_permissions
                        .lock()
                        .unwrap()
                        .push(request.clone());
                } else if ordinal == 0 {
                    stats.pending_question.store(true, Ordering::SeqCst);
                } else {
                    stats.pending_peer_question.store(true, Ordering::SeqCst);
                }
                stats.events.send(serde_json::json!({
                    "directory": env!("CARGO_MANIFEST_DIR"),
                    "payload": {"id": format!("evt_retained_request_{ordinal}"),
                        "type": if permission { "permission.asked" } else { "question.asked" },
                        "properties": request}
                }).to_string()).unwrap();
            }
            stats.prompt_release.acquire().await.unwrap().forget();
            info = OpenCodeStreamCase::assistant(&assistant, &parent);
            if !matches!(
                *stats.retained_identity_change.lock().unwrap(),
                Some(RetainedIdentityChange::OrdinaryCompletion)
            ) {
                info["error"] = serde_json::json!({
                    "name": "MessageAbortedError", "data": {"message": "The operation was aborted"}
                });
            }
        } else {
            if stats.held_second_permission.load(Ordering::SeqCst) {
                info["time"].as_object_mut().unwrap().remove("completed");
                Self::retain_question_event(
                    stats,
                    "message.updated",
                    serde_json::json!({"sessionID": "ses_probe", "info": info}),
                );
                Self::retain_question_event(
                    stats,
                    "message.part.updated",
                    serde_json::json!({
                        "sessionID": "ses_probe", "time": 3, "part": {
                            "id": "prt_retained_later", "sessionID": "ses_probe", "messageID": assistant,
                            "type": "tool", "callID": "call_retained_later", "tool": "bash",
                            "state": {"status": "running", "input": {}, "time": {"start": 3}}
                        }
                    }),
                );
                let request = serde_json::json!({
                    "id": "per_retained_later", "sessionID": "ses_probe", "permission": "bash",
                    "patterns": ["echo later"], "metadata": {"command": "echo later"}, "always": ["echo *"],
                    "tool": {"messageID": assistant, "callID": "call_retained_later"}
                });
                stats
                    .pending_permissions
                    .lock()
                    .unwrap()
                    .push(request.clone());
                stats.events.send(serde_json::json!({"directory": env!("CARGO_MANIFEST_DIR"), "payload": {
                    "id": "evt_retained_later_permission", "type": "permission.asked", "properties": request
                }}).to_string()).unwrap();
                stats.prompt_release.acquire().await.unwrap().forget();
                info = OpenCodeStreamCase::assistant(&assistant, &parent);
            }
            Self::retain_question_event(
                stats,
                "message.updated",
                serde_json::json!({"sessionID": "ses_probe", "info": info}),
            );
            stats.native_active.store(false, Ordering::SeqCst);
        }
        // The first generation became idle in the abort route. Its delayed
        // HTTP writer must not overwrite the second generation's busy state.
        let parts = if ordinal == 0 {
            serde_json::json!([])
        } else {
            serde_json::json!([{"id": "prt_retained_answer", "sessionID": "ses_probe",
                "messageID": assistant, "type": "text", "text": "RETAINED_HISTORY_REUSE"}])
        };
        let body = serde_json::json!({"info": info, "parts": parts});
        if ordinal == 0 && stats.malformed_stream_response.load(Ordering::SeqCst) {
            // The independent native history records cancelled tool/assistant
            // work before the final HTTP response is lost. A later session-abort
            // refusal does not revoke that exact native terminal evidence.
            let faults = stats.pending_cleanup_faults.lock().unwrap().clone();
            let publication = faults
                .filter(|faults| {
                    faults.persistence_fault()
                        == PromptPersistenceFault::NativeCleanupCommitReceiptLost
                })
                .map(|faults| (faults, stats.history_rows.lock().unwrap().len()));
            Self::retained_question_abort(stats);
            stats.native_active.store(false, Ordering::SeqCst);
            if let Some((faults, first_terminal)) = publication {
                let terminal_events = stats.history_rows.lock().unwrap()[first_terminal..].to_vec();
                assert!(!terminal_events.is_empty());
                // The lost-receipt case needs a successful native preparation.
                // Settle these exact Normal events before HTTP loss starts the
                // competing foreground cleanup; sync history stays withheld.
                tokio::time::timeout(Duration::from_secs(3), async {
                    loop {
                        let progress = faults
                            .inner
                            .list(StateListRequest::new(OPENCODE_EVENT_PROGRESS_NAMESPACE).unwrap())
                            .await
                            .unwrap();
                        if progress.records.iter().any(|record| {
                            record.record.value["recent"]
                                .as_array()
                                .is_some_and(|recent| {
                                    terminal_events.iter().all(|terminal| {
                                        recent.iter().any(|event| {
                                            event["source"] == "normal"
                                                && event["id"] == terminal["id"]
                                                && event["data"] == terminal["data"]
                                        })
                                    })
                                })
                        }) {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("the exact terminal events must publish before lost-receipt cleanup");
            }
            Self::json(stream, "{").await;
        } else {
            Self::json(stream, &body.to_string()).await;
        }
        stats.prompt_responses.fetch_add(1, Ordering::SeqCst);
    }

    fn retained_question_abort(stats: &OpenCodeEventStats) {
        // The selected processor cleanup terminalizes the outstanding question tool and
        // assistant. Question.ask removes its pending entry when its own deferred
        // wait settles; session abort does not prove that separate wait settled.
        // Historical Native1.17.18 captured both terminal tool/assistant facts and the same
        // still-pending question after reuse. This is not target-runtime evidence;
        // no rejection event is fabricated.
        let permission = stats.retained_permissions.load(Ordering::SeqCst);
        let ordinary = matches!(
            *stats.retained_identity_change.lock().unwrap(),
            Some(RetainedIdentityChange::OrdinaryCompletion)
        );
        for ordinal in 0..if permission || stats.retained_parallel_questions.load(Ordering::SeqCst)
        {
            2
        } else {
            1
        } {
            Self::retain_question_event(
                stats,
                "message.part.updated",
                serde_json::json!({
                    "sessionID": "ses_probe", "time": 2, "part": {
                        "id": if permission { format!("prt_retained_permission_{ordinal}") } else if ordinal == 0 { "prt_retained_question".to_owned() } else { "prt_retained_question_peer".to_owned() },
                        "sessionID": "ses_probe", "messageID": "msg_000000000006abcdefghijklmn",
                        "type": "tool",
                        "callID": if permission { format!("call_retained_permission_{ordinal}") } else if ordinal == 0 { "call_retained_question".to_owned() } else { "call_retained_question_peer".to_owned() },
                        "tool": if permission { "bash" } else { "question" },
                        "state": {"status": "error", "input": {}, "error": "Tool execution aborted",
                            "metadata": {"interrupted": true}, "time": {"start": 1, "end": 2}}
                    }
                }),
            );
        }
        let mut info = OpenCodeStreamCase::assistant(
            "msg_000000000006abcdefghijklmn",
            "msg_000000000009abcdefghijklmn",
        );
        if !ordinary {
            info["error"] = serde_json::json!({
                "name": "MessageAbortedError", "data": {"message": "The operation was aborted"}
            });
        }
        Self::retain_question_event(
            stats,
            "message.updated",
            serde_json::json!({"sessionID": "ses_probe", "info": info}),
        );
    }

    fn fail_history(&self) {
        self.stats
            .history_failures
            .store(usize::MAX, Ordering::SeqCst);
    }

    fn invalidate_next_session_read(&self) {
        self.invalidate_session_reads(1);
    }

    fn invalidate_session_reads(&self, reads: usize) {
        assert!(reads > 0, "at least one exact session read must be invalid");
        self.stats
            .invalid_session_reads
            .store(reads, Ordering::SeqCst);
    }

    fn block_history(&self) {
        self.stats.block_history.store(true, Ordering::SeqCst);
    }

    fn block_next_tool_inventory(&self) {
        self.stats
            .block_tool_inventory
            .store(true, Ordering::SeqCst);
    }

    async fn wait_until_tool_inventory_blocked(&self) {
        tokio::time::timeout(
            Duration::from_secs(5),
            self.stats.tool_inventory_blocked.acquire(),
        )
        .await
        .expect("tool discovery must reach its deterministic block")
        .expect("tool inventory block must remain open")
        .forget();
    }

    fn release_tool_inventory(&self) {
        self.stats.tool_inventory_release.add_permits(1);
    }

    fn emit_next_session_before_binding(&self) {
        assert!(
            !self
                .stats
                .session_event_before_binding
                .swap(true, Ordering::SeqCst),
            "only one pre-binding session event may be armed"
        );
    }

    fn release_history(&self) {
        self.stats.block_history.store(false, Ordering::SeqCst);
        self.stats.history_release.notify_waiters();
    }

    fn stall_event_headers(&self) {
        self.stats.stall_event_headers.store(true, Ordering::SeqCst);
    }

    fn stall_next_capability_headers(&self) {
        self.stats
            .stall_capability_headers
            .store(true, Ordering::SeqCst);
    }

    fn stall_next_capability_body(&self) {
        self.stats
            .stall_capability_body
            .store(true, Ordering::SeqCst);
    }

    async fn wait_for_capability_requests(&self, requests: usize) {
        let progress = tokio::time::timeout(Duration::from_secs(5), async {
            while self.stats.capability_requests.load(Ordering::SeqCst) < requests {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            progress.is_ok(),
            "capability verification must reach its deterministic stall: requests={}",
            self.stats.capability_requests.load(Ordering::SeqCst),
        );
    }

    fn request_question_on_prompt(&self) {
        self.stats.question_on_prompt.store(true, Ordering::SeqCst);
    }

    fn retain_pending_question(&self) {
        self.stats.pending_question.store(true, Ordering::SeqCst);
    }

    fn mark_native_idle(&self) {
        self.stats.native_active.store(false, Ordering::SeqCst);
    }

    fn mark_native_active(&self) {
        self.stats.native_active.store(true, Ordering::SeqCst);
    }

    fn retain_active_structured_prompt(&self) {
        self.stats
            .active_on_structured
            .store(true, Ordering::SeqCst);
        self.stats.block_abort.store(true, Ordering::SeqCst);
    }

    fn emit_overlap(&self, count: usize) {
        for index in 0..count {
            let event = serde_json::json!({
                "directory": env!("CARGO_MANIFEST_DIR"),
                "payload": {
                    "id": format!("evt_probe_overlap_{index}"),
                    "type": "probe.future",
                    "properties": {}
                }
            });
            self.events
                .send(event.to_string())
                .expect("the overlap event subscriber must remain owned");
        }
    }

    fn emit_durable(&self, id: &str, sequence: u64) {
        let event = serde_json::json!({
            "directory": env!("CARGO_MANIFEST_DIR"),
            "payload": {
                "id": id,
                "type": "sync",
                "syncEvent": {
                    "id": id,
                    "aggregateID": "ses_probe",
                    "seq": sequence,
                    "type": "probe.future.1",
                    "data": {}
                }
            }
        });
        self.events
            .send(event.to_string())
            .expect("the durable event subscriber must remain owned");
    }

    fn set_startup_history_event(&self, id: &str) {
        *self
            .stats
            .history_rows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = vec![serde_json::json!({
            "id": id,
            "aggregate_id": "ses_probe",
            "seq": 0,
            "type": "probe.future.1",
            "data": {}
        })];
    }

    fn emit_normal(&self, id: &str) {
        let event = serde_json::json!({
            "directory": env!("CARGO_MANIFEST_DIR"),
            "payload": {
                "id": id,
                "type": "probe.future",
                "properties": {}
            }
        });
        self.events
            .send(event.to_string())
            .expect("the normal event subscriber must remain owned");
    }

    fn emit_catalog_invalidation(&self, id: &str) {
        let event = serde_json::json!({
            "directory": env!("CARGO_MANIFEST_DIR"),
            "payload": {
                "id": id,
                "type": "mcp.tools.changed",
                "properties": { "server": "probe" }
            }
        });
        self.events
            .send(event.to_string())
            .expect("the catalog event subscriber must remain owned");
    }

    fn emit_gap(&self, first_sequence: u64, label: &str) {
        let second_sequence = first_sequence
            .checked_add(1)
            .expect("gap sequence must remain bounded");
        let first_id = format!("evt_probe_gap_{label}_{first_sequence}");
        let second_id = format!("evt_probe_gap_{label}_{second_sequence}");
        let first = serde_json::json!({
            "id": first_id,
            "aggregate_id": "ses_probe",
            "seq": first_sequence,
            "type": "probe.future.1",
            "data": {}
        });
        let second = serde_json::json!({
            "id": &second_id,
            "aggregate_id": "ses_probe",
            "seq": second_sequence,
            "type": "probe.future.1",
            "data": {}
        });
        *self
            .stats
            .history_rows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = vec![first, second];
        self.emit_durable(&second_id, second_sequence);
    }

    fn config(&self, request_timeout: Duration, lifecycle_timeout: Duration) -> AgentConfig {
        let backend = OpenCodeConfig::external(
            &self.endpoint,
            Duration::from_secs(1),
            request_timeout,
            Duration::from_secs(30),
        )
        .expect("OpenCode probe config must be valid");
        AgentConfig::new(
            backend.into(),
            lifecycle_timeout,
            NonZeroUsize::new(1).expect("one prompt must be nonzero"),
        )
        .expect("OpenCode Agent config must be valid")
    }

    async fn storage() -> AgentStorage {
        AgentStorage::new(Self::memory_store().await)
    }

    async fn memory_store() -> StateStore {
        StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("event ownership state store must build")
    }

    async fn wait_for(&self, subscriptions: usize, history: usize, active: usize) {
        let progress = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if self.stats.subscriptions.load(Ordering::SeqCst) >= subscriptions
                    && self.stats.history.load(Ordering::SeqCst) >= history
                    && self.stats.active.load(Ordering::SeqCst) == active
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            progress.is_ok(),
            "event ownership must make bounded progress: subscriptions={}, history={}, active={}, expected=({subscriptions}, {history}, {active})",
            self.stats.subscriptions.load(Ordering::SeqCst),
            self.stats.history.load(Ordering::SeqCst),
            self.stats.active.load(Ordering::SeqCst),
        );
    }

    async fn wait_for_synchronization(&self, history: usize, pending_scans: usize) {
        let progress = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if self.stats.history.load(Ordering::SeqCst) >= history
                    && self.stats.pending_scans.load(Ordering::SeqCst) >= pending_scans
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            progress.is_ok(),
            "event synchronization must complete: subscriptions={}, history={}, pending_scans={}, active={}",
            self.stats.subscriptions.load(Ordering::SeqCst),
            self.stats.history.load(Ordering::SeqCst),
            self.stats.pending_scans.load(Ordering::SeqCst),
            self.stats.active.load(Ordering::SeqCst),
        );
    }

    async fn wait_for_messages(&self, messages: usize) {
        let progress = tokio::time::timeout(Duration::from_secs(5), async {
            while self.stats.messages.load(Ordering::SeqCst) < messages {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            progress.is_ok(),
            "prompt dispatch must make bounded progress: messages={}, aborts={}, native_active={}",
            self.stats.messages.load(Ordering::SeqCst),
            self.stats.aborts.load(Ordering::SeqCst),
            self.stats.native_active.load(Ordering::SeqCst),
        );
    }

    async fn wait_for_history(&self, history: usize) {
        let progress = tokio::time::timeout(Duration::from_secs(5), async {
            while self.stats.history.load(Ordering::SeqCst) < history {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            progress.is_ok(),
            "history must make bounded progress: subscriptions={}, history={}, active={}",
            self.stats.subscriptions.load(Ordering::SeqCst),
            self.stats.history.load(Ordering::SeqCst),
            self.stats.active.load(Ordering::SeqCst),
        );
    }

    async fn wait_for_inventory_responses(&self, inventory_responses: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.stats.inventory_responses.load(Ordering::SeqCst) < inventory_responses {
                self.stats
                    .inventory_response
                    .acquire()
                    .await
                    .expect("inventory-response barrier must remain open")
                    .forget();
            }
        })
        .await
        .expect("native inventory responses must make bounded progress");
    }

    async fn wait_for_status_requests(&self, status_requests: usize) {
        let progress = tokio::time::timeout(Duration::from_secs(5), async {
            while self.stats.status_requests.load(Ordering::SeqCst) < status_requests {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            progress.is_ok(),
            "native status observation must make bounded progress: requests={}, messages={}, aborts={}",
            self.stats.status_requests.load(Ordering::SeqCst),
            self.stats.messages.load(Ordering::SeqCst),
            self.stats.aborts.load(Ordering::SeqCst),
        );
    }

    fn workspace_request() -> NewWorkspace {
        Self::workspace_request_with_defaults(WorkspaceDefaults::empty())
    }

    fn workspace_request_with_defaults(defaults: WorkspaceDefaults) -> NewWorkspace {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let context = WorkspaceContext::new(
            root.clone(),
            vec![
                WorkspaceRoot::new(root, WorkspaceRootAccess::ReadWrite)
                    .expect("event probe workspace root must be valid"),
            ],
            WorkspaceExecutionPolicy::new(
                WorkspaceSandboxMode::FullAccess,
                WorkspaceNetworkAccess::Enabled,
            ),
        )
        .expect("event probe workspace context must be valid");
        NewWorkspace::new("event-probe", context, defaults)
            .expect("event probe workspace must be valid")
    }

    fn structured_request(timeout: Duration) -> PromptRequest {
        let structured = StructuredOutputRequest::new(serde_json::json!({
            "type": "object",
            "properties": {
                "ack": {"type": "string", "const": "STRUCTURED_ACK"}
            },
            "required": ["ack"],
            "additionalProperties": false
        }))
        .expect("structured probe schema must be valid");
        PromptRequest::new("return structured probe output")
            .expect("structured probe prompt must be valid")
            .with_structured_output(structured)
            .with_timeout(timeout)
            .expect("structured probe timeout must be valid")
    }

    async fn assistant_history_replay(
        self,
        live_sync: bool,
        stable_conflict: bool,
        promotion_fault: PromptCommitFault,
    ) {
        use futures_util::FutureExt;

        let faults = PromptFaultStore::new(Self::memory_store().await);
        let storage = faults.storage();
        let mut agent = None;
        let outcome = std::panic::AssertUnwindSafe(async {
            agent = Some(
                AgentBuilder::create(
                    self.config(Duration::from_secs(5), Duration::from_secs(3)),
                    storage.clone(),
                )
                .build()
                .await
                .expect("the Assistant replay Agent must build"),
            );
            let agent_id = agent.as_ref().unwrap().id();
            let workspace = agent
                .as_ref()
                .unwrap()
                .new_workspace(Self::workspace_request())
                .await
                .expect("the Assistant replay workspace must be created");
            let conversation = workspace
                .new_conversation(NewConversation::new(None, None, Vec::new()).unwrap())
                .await
                .expect("the Assistant replay conversation must be created");
            let key = StateKey::new(OPENCODE_EVENT_PROGRESS_NAMESPACE, agent_id.as_str()).unwrap();
            let original = serde_json::json!({
                "sessionID": "ses_probe",
                "info": {
                    "id": "msg_probe_assistant", "sessionID": "ses_probe",
                    "role": "assistant", "parentID": "msg_probe_user",
                    "modelID": "probe", "providerID": "probe", "mode": "build",
                    "agent": "build", "path": {"cwd": "/probe", "root": "/probe"},
                    "time": {"created": 1}, "cost": 0,
                    "tokens": {"input": 0, "output": 0, "reasoning": 0,
                               "cache": {"read": 0, "write": 0}},
                    "summary": false, "variant": "probe",
                    "probe_extra": {"ordered": [1, 2]}
                }
            });
            // The selected publisher encodes history before queueing the same
            // Assistant object for live serialization. These six fields are
            // mutated in place by processor/prompt after updateMessage returns.
            let mut live = original.clone();
            live["info"]["time"]["completed"] = serde_json::json!(2);
            live["info"]["cost"] = serde_json::json!(1);
            live["info"]["tokens"]["output"] = serde_json::json!(3);
            live["info"]["finish"] = serde_json::json!("stop");
            live["info"]["error"] = serde_json::json!({
                "name": "MessageAbortedError", "data": {"message": "probe cancellation"}
            });
            live["info"]["structured"] = serde_json::json!({"answer": "probe"});
            let id = "evt_probe_assistant_replay";
            let mut envelope = serde_json::json!({
                "directory": env!("CARGO_MANIFEST_DIR"),
                "payload": if live_sync {
                    serde_json::json!({"id": id, "type": "sync", "syncEvent": {
                        "id": id, "aggregateID": "ses_probe", "seq": 0,
                        "type": "message.updated.1", "data": live
                    }})
                } else {
                    serde_json::json!({"id": id, "type": "message.updated", "properties": live})
                }
            });
            self.events.send(envelope.to_string()).unwrap();
            let initial = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let snapshot = faults.inner.load(key.clone()).await.unwrap().unwrap();
                    if snapshot.record.value["recent"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|row| row["id"] == id && row["data"] == live)
                    {
                        break snapshot;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the original live Assistant must be durably remembered");
            assert_eq!(
                initial.record.value["high_water"]["ses_probe"],
                if live_sync {
                    serde_json::json!(0)
                } else {
                    serde_json::Value::Null
                }
            );

            // A contradictory observation from that SAME live form is rejected,
            // not excused by the cross-form mutable-field rule.
            let history = self.stats.history.load(Ordering::SeqCst);
            let pending = self.stats.pending_scans.load(Ordering::SeqCst);
            let data = if live_sync {
                &mut envelope["payload"]["syncEvent"]["data"]
            } else {
                &mut envelope["payload"]["properties"]
            };
            data["info"]["cost"] = serde_json::json!(4);
            self.events.send(envelope.to_string()).unwrap();
            self.wait_for_synchronization(history + 1, pending + 2)
                .await;
            assert_eq!(
                faults.inner.load(key.clone()).await.unwrap().unwrap(),
                initial
            );
            conversation
                .info()
                .await
                .expect("rejected live content must preserve its owner");
            drop(conversation);
            drop(workspace);
            agent.take().unwrap().shutdown().await.unwrap();
            let before = faults.inner.load(key.clone()).await.unwrap().unwrap();
            let mut history_data = original.clone();
            if stable_conflict {
                history_data["info"]["parentID"] = serde_json::json!("msg_probe_other_user");
            }
            *self.stats.history_rows.lock().unwrap() = vec![serde_json::json!({
                "id": id, "aggregate_id": "ses_probe", "seq": 0,
                "type": "message.updated.1", "data": history_data
            })];
            let attempts = faults.event_commit_attempts();
            faults
                .state
                .obscure_event_commit
                .store(promotion_fault as u8, Ordering::SeqCst);
            let reopened = AgentBuilder::reopen(
                self.config(Duration::from_secs(5), Duration::from_secs(3)),
                storage.clone(),
                agent_id.clone(),
            )
            .build()
            .await;
            if stable_conflict || promotion_fault == PromptCommitFault::Unavailable {
                let error = match reopened {
                    Ok(unexpected) => {
                        agent = Some(unexpected);
                        panic!("unproven replay cannot publish readiness");
                    }
                    Err(error) => error,
                };
                assert!(matches!((error).kind(), ErrorKind::Timeout), "{error:?}");
                assert_eq!(
                    faults.inner.load(key.clone()).await.unwrap().unwrap(),
                    before
                );
                if stable_conflict {
                    assert_eq!(faults.event_commit_attempts(), attempts);
                } else {
                    assert!(
                        faults.event_commit_attempts() > attempts,
                        "the history promotion must reach a real rolled-back transaction"
                    );
                }
                return;
            }
            agent = Some(reopened.expect("immutable history must reconcile the live projection"));
            let promoted = faults.inner.load(key.clone()).await.unwrap().unwrap();
            let remembered = promoted.record.value["recent"]
                .as_array()
                .unwrap()
                .iter()
                .find(|row| row["id"] == id)
                .unwrap();
            assert_eq!(remembered["data"], original);
            assert_eq!(remembered["aggregate_id"], "ses_probe");
            assert_eq!(remembered["sequence"], 0);
            assert_eq!(promoted.record.value["high_water"]["ses_probe"], 0);
            assert_eq!(
                faults.event_commit_attempts(),
                attempts + 1,
                "a lost applied receipt must be reconciled without replaying promotion"
            );

            if live_sync && promotion_fault == PromptCommitFault::None {
                // An ambiguous later live observation must confirm the retained
                // History identity, not compare the receipt to weaker input.
                let history = self.stats.history.load(Ordering::SeqCst);
                let subscriptions = self.stats.subscriptions.load(Ordering::SeqCst);
                let attempts = faults.event_commit_attempts();
                faults.obscure_next_event_commit();
                self.events
                    .send(
                        serde_json::json!({
                            "directory": env!("CARGO_MANIFEST_DIR"),
                            "payload": {"id": id, "type": "message.updated", "properties": live}
                        })
                        .to_string(),
                    )
                    .unwrap();
                let sentinel = "evt_probe_after_history_receipt";
                self.emit_normal(sentinel);
                let settled = tokio::time::timeout(Duration::from_secs(3), async {
                    loop {
                        let snapshot = faults.inner.load(key.clone()).await.unwrap().unwrap();
                        if snapshot.record.value["recent"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|row| row["id"] == sentinel)
                        {
                            break snapshot;
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("the following same-stream event must settle after the original receipt");
                let retained = settled.record.value["recent"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|row| row["id"] == id)
                    .unwrap();
                assert_eq!(retained, remembered);
                assert_eq!(
                    settled.record.value["high_water"],
                    promoted.record.value["high_water"]
                );
                assert_eq!(
                    faults.event_commit_attempts(),
                    attempts + 2,
                    "the live observation and sentinel each commit once"
                );
                assert_eq!(
                    self.stats.history.load(Ordering::SeqCst),
                    history,
                    "a confirmed retained-history receipt must not trigger recovery"
                );
                assert_eq!(
                    self.stats.subscriptions.load(Ordering::SeqCst),
                    subscriptions
                );

                // Unknown future normal types must not inherit the selected
                // Assistant exception merely by sharing its canonical prefix.
                let history = self.stats.history.load(Ordering::SeqCst);
                let pending = self.stats.pending_scans.load(Ordering::SeqCst);
                let mut unknown = live.clone();
                unknown["info"]["cost"] = serde_json::json!("not-a-number");
                self.events
                    .send(
                        serde_json::json!({
                            "directory": env!("CARGO_MANIFEST_DIR"),
                            "payload": {
                                "id": id, "type": "message.updated.999",
                                "properties": unknown
                            }
                        })
                        .to_string(),
                    )
                    .unwrap();
                self.wait_for_synchronization(history + 1, pending + 2)
                    .await;
                let recovered = faults.inner.load(key.clone()).await.unwrap().unwrap();
                let retained = recovered.record.value["recent"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|row| row["id"] == id)
                    .unwrap();
                assert_eq!(retained, remembered);
                assert_eq!(
                    recovered.record.value["high_water"],
                    promoted.record.value["high_water"]
                );
                assert_eq!(
                    self.stats.subscriptions.load(Ordering::SeqCst),
                    subscriptions,
                    "contradictory unknown content must recover on the same stream"
                );
            }

            // Once history is the retained authority, a changed history copy is
            // a real contradiction even in a field that can change on live data.
            agent.take().unwrap().shutdown().await.unwrap();
            let before = faults.inner.load(key.clone()).await.unwrap().unwrap();
            self.stats.history_rows.lock().unwrap()[0]["data"]["info"]["cost"] =
                serde_json::json!(5);
            let reopened = AgentBuilder::reopen(
                self.config(Duration::from_secs(5), Duration::from_secs(3)),
                storage.clone(),
                agent_id.clone(),
            )
            .build()
            .await;
            let error = match reopened {
                Ok(unexpected) => {
                    agent = Some(unexpected);
                    panic!("same-history content contradictions must fail");
                }
                Err(error) => error,
            };
            assert!(matches!((error).kind(), ErrorKind::Timeout), "{error:?}");
            assert_eq!(
                faults.inner.load(key.clone()).await.unwrap().unwrap(),
                before
            );
            if !live_sync {
                // The old private epoch cannot silently supply invented origin.
                self.stats.history_rows.lock().unwrap()[0]["data"] = original;
                let mut obsolete = before.record.clone();
                Arc::make_mut(&mut obsolete.value)["version"] = serde_json::json!(3);
                let mut transaction = faults.inner.begin_transaction().await.unwrap();
                transaction.put(obsolete).await.unwrap();
                transaction.commit().await.unwrap();
                let obsolete = faults.inner.load(key.clone()).await.unwrap().unwrap();
                let reopened = AgentBuilder::reopen(
                    self.config(Duration::from_secs(5), Duration::from_secs(3)),
                    storage.clone(),
                    agent_id,
                )
                .build()
                .await;
                let unchanged = faults.inner.load(key.clone()).await.unwrap().unwrap();
                let mut transaction = faults.inner.begin_transaction().await.unwrap();
                transaction.put(before.record).await.unwrap();
                transaction.commit().await.unwrap();
                let error = match reopened {
                    Ok(unexpected) => {
                        agent = Some(unexpected);
                        panic!("event progress without provenance must be rejected");
                    }
                    Err(error) => error,
                };
                assert!(
                    matches!((error).kind(), ErrorKind::InvalidState),
                    "{error:?}"
                );
                assert_eq!(unchanged, obsolete);
            }
        })
        .catch_unwind()
        .await;
        faults
            .state
            .obscure_event_commit
            .store(PromptCommitFault::None as u8, Ordering::SeqCst);
        let cleanup = if let Some(agent) = agent.take() {
            agent.shutdown().await
        } else {
            Ok(())
        };
        let stats = Arc::clone(&self.stats);
        self.shutdown().await;
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
        cleanup.expect("the retained Assistant replay Agent must shut down");
        assert_eq!(stats.active.load(Ordering::SeqCst), 0);
        assert_eq!(stats.connections.load(Ordering::SeqCst), 0);
        assert_eq!(stats.maximum.load(Ordering::SeqCst), 1);
        assert_eq!(stats.messages.load(Ordering::SeqCst), 0);
        assert_eq!(stats.question_replies.load(Ordering::SeqCst), 0);
        assert_eq!(stats.next_session.load(Ordering::SeqCst), 1);
    }

    async fn shutdown(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.await.expect("event probe task must join");
        }
    }
}

impl Drop for ActiveEventConnection {
    fn drop(&mut self) {
        self.stats.active.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Drop for ProbeConnection {
    fn drop(&mut self) {
        self.stats.connections.fetch_sub(1, Ordering::SeqCst);
    }
}

// Protocol-fixture evidence checked against the checksum-pinned v1.19.3 archive:
// packages/schema/src/v1/session.ts (PartDelta),
// packages/opencode/src/session/processor.ts (text-start/text-delta/text-end), and
// packages/opencode/src/session/session.ts (updatePartDelta non-durable publish).
// This is neither an executed OpenCode model nor strict-live evidence. The final
// HTTP text deliberately differs from the transient text: native plugins may
// change text at text-end, so a delta accumulator cannot supply terminal truth.
struct OpenCodeStreamCase {
    probe: OpenCodeEventProbe,
    agent: agent_sdk::Agent<PromptFaultStore>,
    workspace: agent_sdk::Workspace<PromptFaultStore>,
    conversation: Arc<agent_sdk::Conversation<PromptFaultStore>>,
    faults: PromptFaultStore,
    runtime_baseline: usize,
}

impl OpenCodeStreamCase {
    const PARENT: &str = "msg_000000000003abcdefghijklmn";
    const ASSISTANT: &str = "msg_000000000004abcdefghijklmn";

    fn assert_reconciliation_timeout(
        error: &SdkError,
        operation: Operation,
        storage_phase: ErrorPhase,
    ) {
        assert_eq!(error.kind(), ErrorKind::Timeout);
        assert_eq!(error.context().operation, operation);
        assert_eq!(
            error.context().phase,
            ErrorPhase::Reconciliation,
            "{error:?}"
        );
        assert_eq!(
            error.detail(),
            Some(&ErrorDetail::DeadlineElapsed {
                kind: DeadlineKind::Operation,
                timeout: None,
            })
        );
        assert!(
            error.cause().is_none(),
            "an earlier attempt is not the timer's cause: {error:?}"
        );
        assert!(std::error::Error::source(error).is_none());
        let [attempt] = error.supplementary() else {
            panic!("the last failed attempt must remain supplementary: {error:?}");
        };
        assert_eq!(attempt.kind(), ErrorKind::Integration);
        assert_eq!(attempt.context().operation, operation);
        assert_eq!(attempt.context().phase, storage_phase);
        assert_eq!(attempt.detail(), Some(&ErrorDetail::ServiceUnavailable));
        assert!(attempt.cause().is_none());
        assert!(!format!("{error} {error:?}").contains("PRIVATE_"));
    }

    async fn verify_recovered_abort_evidence(fault: Option<PromptPersistenceFault>) {
        use futures_util::FutureExt;

        let case = Self::start().await;
        let outcome = std::panic::AssertUnwindSafe(async {
            let mut stream = case
                .stream_with_admission_fault(PromptPersistenceFault::Admission)
                .await;
            let terminal = tokio::time::timeout(Duration::from_secs(7), stream.recv())
                .await
                .expect("admission failure must release foreground ownership")
                .expect("the stream must publish its terminal failure")
                .expect_err("failed admission cannot publish completion");
            assert_eq!(terminal.kind(), ErrorKind::Timeout);
            assert!(stream.recv().await.is_none());
            case.faults
                .state
                .persistence_attempts
                .store(0, Ordering::SeqCst);
            case.faults
                .set_persistence_fault(fault.unwrap_or(PromptPersistenceFault::None));
            case.faults
                .state
                .fail_prompt_observation
                .store(fault.is_none(), Ordering::SeqCst);
            let error = tokio::time::timeout(Duration::from_secs(7), case.conversation.abort())
                .await
                .expect("recovered abort observation must remain bounded")
                .expect_err("unavailable recovery state cannot settle cancellation");
            let failures = if fault.is_some() {
                case.faults
                    .state
                    .persistence_attempts
                    .load(Ordering::SeqCst)
            } else {
                case.faults
                    .state
                    .prompt_observation_failures
                    .load(Ordering::SeqCst)
            };
            assert!(failures > 0, "the storage failure must be observed");
            assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 0);
            assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 1);
            assert!(
                stream.recv().await.is_none(),
                "recovery must not replay a stream terminal"
            );
            Self::assert_reconciliation_timeout(
                &error,
                Operation::Abort,
                if fault.is_some() {
                    ErrorPhase::StateWrite
                } else {
                    ErrorPhase::StateRead
                },
            );
            assert!(error.supplementary()[0].supplementary().is_empty());
        })
        .catch_unwind()
        .await;
        case.faults
            .set_persistence_fault(PromptPersistenceFault::None);
        case.faults
            .state
            .fail_prompt_observation
            .store(false, Ordering::SeqCst);
        // Restore storage, then settle the same still-active native generation.
        // Releasing its response first would make the peer report natural completion
        // after the original foreground observer has already been lost.
        let cancellation =
            tokio::time::timeout(Duration::from_secs(7), case.conversation.abort()).await;
        case.release();
        let Self {
            agent,
            workspace,
            conversation,
            probe,
            ..
        } = case;
        drop(conversation);
        drop(workspace);
        let cleanup = agent.shutdown().await;
        probe.shutdown().await;
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
        cancellation
            .expect("restored cancellation cleanup must remain bounded")
            .expect("restored storage must permit cleanup of the original generation");
        cleanup.expect("the recovered Agent must settle after storage is restored");
    }

    async fn assert_persistence_retries_stopped(&self, started: tokio::time::Instant) {
        tokio::time::sleep_until(started + Duration::from_secs(6)).await;
        let attempts = self
            .faults
            .state
            .persistence_attempts
            .load(Ordering::SeqCst);
        assert!(attempts > 1, "transient persistence failures must retry");
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            self.faults
                .state
                .persistence_attempts
                .load(Ordering::SeqCst),
            attempts,
            "no new persistence attempt may begin after its lifecycle deadline"
        );
    }

    async fn wait_for_admission(&self) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if self
                    .faults
                    .prompt_snapshot(&self.conversation.id())
                    .await
                    .is_some_and(|record| {
                        record
                            .record
                            .value
                            .pointer("/phase/phase")
                            .and_then(serde_json::Value::as_str)
                            == Some("admitted")
                    })
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("exact input and active status must be durably admitted");
    }

    async fn start() -> Self {
        Self::start_with_prompt_limit(NonZeroUsize::new(1).unwrap(), Duration::from_secs(15)).await
    }

    async fn start_with_prompt_limit(
        prompt_limit: NonZeroUsize,
        request_timeout: Duration,
    ) -> Self {
        let mut probe = OpenCodeEventProbe::start(0).await;
        probe.stats.stream_fixture.store(true, Ordering::SeqCst);
        let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
        let runtime_baseline = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        let config = probe.config(request_timeout, Duration::from_secs(5));
        let config = AgentConfig::new(
            config.backend().clone(),
            config.lifecycle_timeout(),
            prompt_limit,
        )
        .unwrap();
        let agent = AgentBuilder::create(config, faults.storage()).build().await;
        let agent = match agent {
            Ok(agent) => agent,
            Err(primary) => {
                // Keep the protocol peer's native task outside a timed cleanup
                // future so timeout cannot discard its only join handle.
                let mut task = probe.task.take().expect("the probe owns its server task");
                let mut joined = false;
                let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
                cleanup
                    .settle(async {
                        probe.shutdown().await;
                        let result = (&mut task).await;
                        joined = true;
                        result.expect("the event probe must join after failed Agent construction");
                        Ok::<(), SdkError>(())
                    })
                    .await;
                if !joined {
                    task.abort();
                    cleanup
                        .settle(async {
                            match (&mut task).await {
                                Ok(()) => {}
                                Err(error) => assert!(
                                    error.is_cancelled(),
                                    "event probe task panicked: {error}"
                                ),
                            }
                            Ok::<(), SdkError>(())
                        })
                        .await;
                }
                let diagnostic = format!("stream fixture Agent must build: {primary:?}");
                eprintln!("{diagnostic}");
                return cleanup.finish(Err(Box::new(diagnostic)));
            }
        };
        let workspace = agent
            .new_workspace(OpenCodeEventProbe::workspace_request())
            .await
            .unwrap();
        let conversation = Arc::new(
            workspace
                .new_conversation(NewConversation::new(None, None, Vec::new()).unwrap())
                .await
                .unwrap(),
        );
        Self {
            probe,
            agent,
            workspace,
            conversation,
            faults,
            runtime_baseline,
        }
    }

    async fn revoke(self) {
        let Self {
            probe,
            agent,
            workspace,
            conversation,
            faults,
            runtime_baseline,
        } = self;
        drop(conversation);
        drop(workspace);
        drop(agent);
        let runtime = tokio::runtime::Handle::current();
        let drainage = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(3),
            async {
                while runtime.metrics().num_alive_tasks()
                    > runtime_baseline + probe.stats.connections.load(Ordering::SeqCst)
                {
                    tokio::task::yield_now().await;
                }
            },
        )
        .await;
        drop(faults);
        let mut cleanup = NativeCleanup::new(Duration::from_secs(3));
        cleanup
            .settle(async {
                probe.shutdown().await;
                Ok::<(), SdkError>(())
            })
            .await;
        cleanup.finish(drainage);
    }

    async fn verify_waiter_failure(mode: u8) {
        for primary_failure in [false, true] {
            let case = Self::start().await;
            case.probe
                .stats
                .invalid_stream_response
                .store(primary_failure, Ordering::SeqCst);
            case.faults.state.waiter_fault.store(mode, Ordering::SeqCst);
            let mut stream = case.stream().await;
            case.begin_text();
            case.release();
            let terminal = tokio::time::timeout(Duration::from_secs(7), stream.recv())
                .await
                .expect("terminal cleanup must have a lifecycle bound");
            assert!(
                case.faults
                    .state
                    .waiter_fault_attempts
                    .load(Ordering::SeqCst)
                    > 0,
                "selected waiter phase must reach its fault"
            );
            let retained = case
                .faults
                .prompt_snapshot(&case.conversation.id())
                .await
                .expect("failed cleanup remains recoverable");
            assert_eq!(
                retained.record.value["waiter"],
                match mode {
                    1 => "waiting",
                    2 | 4 => "delivering",
                    3 => "lost",
                    _ => panic!("unknown waiter fault"),
                }
            );
            if primary_failure {
                assert!(
                    matches!(
                        (terminal)
                            .as_ref()
                            .map(|result| result.as_ref().map_err(SdkError::kind)),
                        Some(Err(ErrorKind::Protocol))
                    ),
                    "primary Protocol must survive cleanup failure: {terminal:?}"
                );
            } else if mode == 4 {
                assert!(
                    matches!(
                        (terminal)
                            .as_ref()
                            .map(|result| result.as_ref().map_err(SdkError::kind)),
                        Some(Err(ErrorKind::Timeout))
                    ),
                    "permanent I/O cannot claim Completed: {terminal:?}"
                );
            } else {
                assert!(
                    matches!(
                        (terminal)
                            .as_ref()
                            .map(|result| result.as_ref().map_err(SdkError::kind)),
                        Some(Err(ErrorKind::Conflict))
                    ),
                    "definite wrong owner cannot claim Completed: {terminal:?}"
                );
            }
            let Some(Err(error)) = terminal else {
                panic!("failure expected")
            };
            let message = error.to_string();
            assert!(
                !message.contains("OpenCode")
                    && !message.contains("prompt_other_generation")
                    && !message.contains("private"),
                "stream errors must use the ordinary caller boundary: {message}"
            );
            assert!(stream.recv().await.is_none());
            case.faults.state.waiter_fault.store(0, Ordering::SeqCst);
            case.revoke().await;
        }
    }

    async fn verify_terminal_cleanup_commit(fault: PreparedCancelFault) {
        let case = Self::start().await;
        case.probe.request_question_on_prompt();
        let observed = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(20),
            async {
                let mut notifications = case.conversation.notification_stream().unwrap();
                let mut stream = case.stream().await;
                let interaction = loop {
                    tokio::select! {
                        item = notifications.recv() => {
                            let item = item.unwrap().unwrap();
                            if matches!(item.kind(), ConversationNotificationKind::Interaction { .. }) {
                                break item;
                            }
                        }
                        item = stream.recv() => panic!("the question must precede completion: {item:?}"),
                    }
                };
                case.wait_for_admission().await;
                case.conversation.resolve_notification(
                    interaction.id().clone(),
                    NotificationResolution::Submit {
                        content: serde_json::json!({"answers": [["Yes"]]}),
                    },
                ).await.expect("the native question must settle once before terminal cleanup");
                let continuation_key = StateKey::new(
                    OPENCODE_CONTINUATION_NAMESPACE,
                    format!("{}:que_probe", case.conversation.id().as_str()),
                ).unwrap();
                let accepted = case.faults.inner.load(continuation_key.clone()).await.unwrap().unwrap();
                assert_eq!(accepted.record.value["state"], "accepted");
                assert_eq!(accepted.record.value["notification_id"], interaction.id().as_str());
                case.faults.arm_terminal_delivery_cleanup(
                    fault,
                    PromptFaultStore::prompt_key(&case.conversation.id()),
                );
                case.begin_text();
                let released = tokio::time::Instant::now();
                case.release();
                let terminal = tokio::time::timeout(Duration::from_secs(7), stream.recv())
                    .await.expect("terminal cleanup shares the existing five-second lifecycle bound")
                    .expect("the stream publishes one terminal result");
                let receipt = case.faults.prepared_cancel_receipt();
                assert!(receipt.faults > 0, "the terminal deletion must encounter the selected real commit fault");
                let before = receipt.before.as_ref().expect("the fault observes the durable terminal record");
                assert_eq!(before.record.value["phase"]["phase"], "terminal");
                assert_eq!(before.record.value["phase"]["outcome"]["outcome"], "success");
                assert_eq!(before.record.value["message_id"], accepted.record.value["generation_id"]);
                assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 1,
                    "retrying storage cleanup cannot dispatch another native prompt");
                assert_eq!(case.probe.stats.question_replies.load(Ordering::SeqCst), 1,
                    "retrying storage cleanup cannot repeat the accepted native tool answer");
                assert_eq!(case.probe.stats.question_rejections.load(Ordering::SeqCst), 0);
                assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 0);
                assert_eq!(case.faults.inner.load(continuation_key).await.unwrap(), Some(accepted),
                    "terminal deletion must preserve the exact accepted continuation receipt");
                match fault {
                    PreparedCancelFault::CommitConflictOnce => {
                        let PromptEvent::Completed(response) = terminal.expect(
                            "a definite rejected cleanup commit must be re-authored without native replay",
                        ) else {
                            panic!("the native terminal response must remain authoritative");
                        };
                        assert_eq!(response.output(), "authoritative HTTP answer");
                        assert_eq!(receipt.faults, 1);
                        assert_eq!(receipt.commit_attempts, 2);
                        assert_eq!(receipt.deletes, 2);
                        assert_eq!(receipt.rollbacks, 1);
                        assert_eq!(receipt.commits, 1);
                        assert_eq!(receipt.accesses_before_rollback, 0);
                        case.settled().await;
                    }
                    PreparedCancelFault::CommitConflict => {
                        let error = terminal.expect_err("persistent conflicts must exhaust the original deadline");
                        assert_eq!(error.kind(), ErrorKind::Timeout);
                        assert!(receipt.commit_attempts > 1, "deadline evidence requires actual bounded retries");
                        assert_eq!(receipt.commits, 0);
                        assert_eq!(receipt.rollbacks, receipt.commit_attempts);
                        assert_eq!(case.faults.prompt_snapshot(&case.conversation.id()).await, Some(before.clone()));
                        tokio::time::sleep_until(released + Duration::from_secs(6)).await;
                        let attempts = case.faults.prepared_cancel_receipt().commit_attempts;
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        assert_eq!(case.faults.prepared_cancel_receipt().commit_attempts, attempts,
                            "no new cleanup transaction may begin after terminal observation expires");
                    }
                    other => panic!("unsupported terminal cleanup test fault: {other:?}"),
                }
                assert!(stream.recv().await.is_none());
                let duplicate = tokio::time::timeout(Duration::from_millis(100), async {
                    while let Some(item) = notifications.recv().await {
                        let item = item.unwrap();
                        if matches!(item.kind(), ConversationNotificationKind::Interaction { .. }) {
                            return item;
                        }
                    }
                    panic!("the conversation remains open");
                }).await;
                assert!(duplicate.is_err(), "storage retries cannot republish a tool interaction");
                if fault == PreparedCancelFault::CommitConflictOnce {
                    case.faults.disarm_prepared_cancel();
                    let following = case.conversation.prompt(
                        OpenCodeEventProbe::structured_request(Duration::from_secs(2)),
                    ).await.expect("successful cleanup releases capacity for a new authored prompt");
                    assert_eq!(following.structured_output().and_then(|value| value.get("ack"))
                        .and_then(serde_json::Value::as_str), Some("STRUCTURED_ACK"));
                    assert_eq!(case.probe.stats.question_replies.load(Ordering::SeqCst), 1);
                }
            },
        ).await;
        case.faults.disarm_prepared_cancel();
        case.finish_observed(observed).await;
    }

    async fn verify_abort_persistence(fault: PromptPersistenceFault) {
        let case = OpenCodeStreamCase::start().await;
        let mut stream = case.stream().await;
        case.begin_text();
        case.delta(
            "evt_abort_outcome_ready",
            OpenCodeStreamCase::ASSISTANT,
            "prt_stream_text",
            "text",
            "accepted",
        );
        assert!(matches!(
            OpenCodeStreamCase::next(&mut stream).await,
            Ok(PromptEvent::OutputDelta { .. })
        ));
        case.wait_for_admission().await;
        case.faults.set_persistence_fault(fault);
        let started = tokio::time::Instant::now();
        let (abort, terminal) = tokio::time::timeout(Duration::from_secs(7), async {
            tokio::join!(case.conversation.abort(), stream.recv())
        })
        .await
        .expect("abort settlement and stream observation must both be bounded");
        let attempts = case
            .faults
            .state
            .persistence_attempts
            .load(Ordering::SeqCst);
        assert!(
            attempts > 0,
            "the native abort reached its exact outcome persistence"
        );
        assert_eq!(
            case.probe.stats.aborts.load(Ordering::SeqCst),
            usize::from(
                fault.fails_once()
                    || matches!(
                        fault,
                        PromptPersistenceFault::AbortOutcomeOnce
                            | PromptPersistenceFault::AbortOutcome
                    )
            ),
            "cancellation cannot dispatch without its durable claim or resend during persistence"
        );
        assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 1);
        if fault.fails_once() {
            assert!(abort.is_ok(), "{abort:?}");
            assert!(
                matches!(
                    (terminal)
                        .as_ref()
                        .map(|result| result.as_ref().map_err(SdkError::kind)),
                    Some(Err(ErrorKind::Cancelled))
                ),
                "{terminal:?}"
            );
            assert_eq!(attempts, 1);
            assert!(stream.recv().await.is_none());
            case.settled().await;
            case.shutdown().await;
            return;
        }
        assert!(
            matches!(
                (abort).as_ref().map_err(SdkError::kind),
                Err(ErrorKind::Timeout)
            ),
            "{abort:?}"
        );
        assert!(
            matches!(
                (terminal)
                    .as_ref()
                    .map(|result| result.as_ref().map_err(SdkError::kind)),
                Some(Err(ErrorKind::Timeout))
            ),
            "{terminal:?}"
        );
        assert!(stream.recv().await.is_none());
        tokio::time::sleep_until(started + Duration::from_secs(6)).await;
        let attempts = case
            .faults
            .state
            .persistence_attempts
            .load(Ordering::SeqCst);
        assert!(attempts > 1);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            case.faults
                .state
                .persistence_attempts
                .load(Ordering::SeqCst),
            attempts,
            "abort outcome retry admission must stop while recovery retains the record"
        );
        let retained = case
            .faults
            .prompt_snapshot(&case.conversation.id())
            .await
            .expect("uncommitted abort outcome must retain its exact generation");
        assert_eq!(
            retained
                .record
                .value
                .pointer("/cancellation/state")
                .and_then(serde_json::Value::as_str),
            Some(fault.retained_cancellation())
        );
        assert_ne!(retained.record.value["waiter"], "delivered");
        case.faults
            .set_persistence_fault(PromptPersistenceFault::None);
        case.release();
        case.revoke().await;
    }

    async fn verify_pre_dispatch_cleanup(fault: PromptPersistenceFault, foreign: bool) {
        let case = OpenCodeStreamCase::start().await;
        case.probe.mark_native_active();
        case.faults.set_persistence_fault(fault);
        case.faults
            .state
            .persistence_foreign
            .store(foreign, Ordering::SeqCst);
        let started = tokio::time::Instant::now();
        let mut stream = case
            .conversation
            .prompt_stream(OpenCodeStreamCase::request())
            .await
            .unwrap();
        let terminal = tokio::time::timeout(Duration::from_secs(7), stream.recv())
            .await
            .expect("pre-dispatch cleanup must end observation despite lower unavailability");
        assert!(
            matches!(
                (terminal)
                    .as_ref()
                    .map(|result| result.as_ref().map_err(SdkError::kind)),
                Some(Err(ErrorKind::Busy))
            ),
            "native busy rejection must survive cleanup failure: {terminal:?}"
        );
        assert!(stream.recv().await.is_none());
        assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 0);
        assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 0);
        assert!(
            case.faults
                .state
                .persistence_attempts
                .load(Ordering::SeqCst)
                > 0
        );
        if fault.fails_once() {
            case.settled().await;
            case.probe.mark_native_idle();
            case.shutdown().await;
            return;
        }
        if !foreign {
            case.assert_persistence_retries_stopped(started).await;
        }
        assert!(
            case.faults
                .prompt_snapshot(&case.conversation.id())
                .await
                .is_some()
        );
        case.faults
            .set_persistence_fault(PromptPersistenceFault::None);
        case.faults
            .state
            .persistence_foreign_next
            .store(false, Ordering::SeqCst);
        case.probe.mark_native_idle();
        case.revoke().await;
    }

    async fn verify_persistence_failure(
        fault: PromptPersistenceFault,
        foreign: bool,
        primary: bool,
    ) {
        let case = Self::start().await;
        let ambiguous = !matches!(fault, PromptPersistenceFault::Terminal);
        case.probe
            .stats
            .malformed_stream_response
            .store(ambiguous, Ordering::SeqCst);
        case.probe
            .stats
            .invalid_stream_response
            .store(primary && !ambiguous, Ordering::SeqCst);
        case.faults.set_persistence_fault(fault);
        case.faults
            .state
            .persistence_foreign
            .store(foreign, Ordering::SeqCst);
        let mut stream = case.stream().await;
        case.begin_text();
        // Synchronize through one actual observed delta so the native request is
        // durably admitted before the selected terminal/response-loss fault.
        case.delta(
            "evt_persistence_ready",
            Self::ASSISTANT,
            "prt_stream_text",
            "text",
            "accepted",
        );
        Self::next(&mut stream).await.unwrap();
        let released = tokio::time::Instant::now();
        case.release();
        let terminal = tokio::time::timeout(Duration::from_secs(7), stream.recv())
            .await
            .expect("native persistence failure without caller timeout must be bounded");
        assert!(
            case.faults
                .state
                .persistence_attempts
                .load(Ordering::SeqCst)
                > 0,
            "exact persistence phase reached"
        );
        if primary || ambiguous {
            assert!(
                matches!(
                    (terminal)
                        .as_ref()
                        .map(|result| result.as_ref().map_err(SdkError::kind)),
                    Some(Err(ErrorKind::Protocol))
                ),
                "primary native protocol failure must survive persistence: {terminal:?}"
            );
        } else if foreign {
            assert!(
                matches!(
                    (terminal)
                        .as_ref()
                        .map(|result| result.as_ref().map_err(SdkError::kind)),
                    Some(Err(ErrorKind::InvalidState))
                ),
                "foreign terminal generation cannot complete: {terminal:?}"
            );
        } else {
            assert!(
                matches!(
                    (terminal)
                        .as_ref()
                        .map(|result| result.as_ref().map_err(SdkError::kind)),
                    Some(Err(ErrorKind::Timeout))
                ),
                "uncommitted terminal cannot complete: {terminal:?}"
            );
        }
        let Some(Err(error)) = terminal else {
            panic!("failure required")
        };
        assert!(!error.to_string().contains("private") && !error.to_string().contains("OpenCode"));
        assert!(stream.recv().await.is_none());
        if !foreign {
            tokio::time::sleep_until(released + Duration::from_secs(6)).await;
            let settled_attempts = case
                .faults
                .state
                .persistence_attempts
                .load(Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(150)).await;
            assert_eq!(
                case.faults
                    .state
                    .persistence_attempts
                    .load(Ordering::SeqCst),
                settled_attempts,
                "terminal observation must not leave unbounded persistence retry admission"
            );
        }
        let retained = case
            .faults
            .prompt_snapshot(&case.conversation.id())
            .await
            .expect("failed persistence remains durable recovery state");
        assert_ne!(retained.record.value["waiter"], "delivered");
        assert_eq!(
            case.probe.stats.messages.load(Ordering::SeqCst),
            1,
            "ambiguous HTTP is never replayed"
        );
        case.faults
            .set_persistence_fault(PromptPersistenceFault::None);
        case.faults
            .state
            .persistence_foreign_next
            .store(false, Ordering::SeqCst);
        case.revoke().await;
    }

    fn request() -> PromptRequest {
        PromptRequest::new("plain probe").unwrap()
    }

    async fn stream(&self) -> PromptStream {
        let stream = tokio::time::timeout(
            Duration::from_secs(5),
            self.conversation.prompt_stream(Self::request()),
        )
        .await
        .expect("prompt_stream must return before the gated HTTP terminal")
        .expect("native OpenCode incremental output must be supported");
        self.probe.wait_for_messages(1).await;
        stream
    }

    async fn stream_with_admission_fault(&self, fault: PromptPersistenceFault) -> PromptStream {
        self.probe
            .stats
            .defer_stream_input
            .store(true, Ordering::SeqCst);
        let event_commits = self
            .faults
            .state
            .event_commit_attempts
            .load(Ordering::SeqCst);
        self.faults.set_persistence_fault(fault);
        let stream = self.stream().await;
        // Isolate the foreground's status reconciliation from the event
        // transaction, which can also persist admission. Initial busy arrives
        // without exact input-part evidence. Deliver that part later, then a
        // stale idle signal while authoritative HTTP status remains busy.
        self.faults
            .wait_for_event_commit_attempts(event_commits + 2)
            .await;
        let part = self
            .probe
            .stats
            .prompt_parent
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()["parts"][0]
            .clone();
        self.emit(
            "evt_deferred_input_part",
            "message.part.updated",
            serde_json::json!({"sessionID": "ses_probe", "part": part, "time": 1.0}),
        );
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(snapshot) = self.faults.prompt_snapshot(&self.conversation.id()).await
                    && snapshot.record.value["input_observed"] == true
                {
                    let phase = snapshot
                        .record
                        .value
                        .pointer("/phase/phase")
                        .and_then(serde_json::Value::as_str);
                    if fault.fails_once() && phase == Some("admitted") {
                        assert_eq!(
                            self.faults
                                .state
                                .persistence_attempts
                                .load(Ordering::SeqCst),
                            1,
                            "only the foreground may have retried this isolated admission fault"
                        );
                    } else {
                        assert_eq!(phase, Some("dispatched_awaiting_admission"));
                    }
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the exact deferred input must precede foreground admission");
        self.emit(
            "evt_stale_idle_for_status_reconciliation",
            "session.idle",
            serde_json::json!({"sessionID": "ses_probe"}),
        );
        stream
    }

    fn assistant(id: &str, parent: &str) -> serde_json::Value {
        serde_json::json!({
            "id": id, "sessionID": "ses_probe", "role": "assistant",
            "time": {"created": 1, "completed": 2}, "parentID": parent,
            "modelID": "model", "providerID": "provider", "mode": "build",
            "agent": "agent", "path": {
                "cwd": env!("CARGO_MANIFEST_DIR"), "root": env!("CARGO_MANIFEST_DIR")
            }, "cost": 0, "tokens": {
                "input": 0, "output": 0, "reasoning": 0, "cache": {"read": 0, "write": 0}
            }
        })
    }

    fn emit(&self, id: &str, kind: &str, properties: serde_json::Value) {
        self.probe
            .events
            .send(
                serde_json::json!({
                    "directory": env!("CARGO_MANIFEST_DIR"),
                    "payload": {"id": id, "type": kind, "properties": properties}
                })
                .to_string(),
            )
            .unwrap();
    }

    fn message(&self, id: &str, parent: &str) {
        let mut info = Self::assistant(id, parent);
        info["time"].as_object_mut().unwrap().remove("completed");
        self.emit(
            &format!("evt_stream_message_{id}"),
            "message.updated",
            serde_json::json!({"sessionID": "ses_probe", "info": info}),
        );
    }

    fn part(&self, message: &str, id: &str, kind: &str) {
        self.emit(
            &format!("evt_stream_part_{id}"),
            "message.part.updated",
            serde_json::json!({"sessionID": "ses_probe", "time": 1.0, "part": {
                "id": id, "sessionID": "ses_probe", "messageID": message,
                "type": kind, "text": "", "time": {"start": 1}
            }}),
        );
    }

    fn delta(&self, id: &str, message: &str, part: &str, field: &str, text: &str) {
        self.emit(
            id,
            "message.part.delta",
            serde_json::json!({
                "sessionID": "ses_probe", "messageID": message, "partID": part,
                "field": field, "delta": text
            }),
        );
    }

    fn begin_text(&self) {
        self.message(Self::ASSISTANT, Self::PARENT);
        self.part(Self::ASSISTANT, "prt_stream_text", "text");
    }

    async fn next(stream: &mut PromptStream) -> Result<PromptEvent, SdkError> {
        tokio::time::timeout(Duration::from_secs(5), stream.recv())
            .await
            .expect("the gated native event must make bounded progress")
            .expect("one native event or explicit observation error must arrive")
    }

    fn release(&self) {
        self.probe.stats.prompt_release.add_permits(1);
    }

    fn completion(&self, message: &str, parent: &str) {
        self.emit(
            &format!("evt_stream_completed_{message}"),
            "message.updated",
            serde_json::json!({
                "sessionID": "ses_probe", "info": Self::assistant(message, parent)
            }),
        );
    }

    async fn http_responded(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.probe.stats.prompt_responses.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("independent HTTP terminal must be delivered");
    }

    async fn settled(&self) {
        let key =
            StateKey::new(OPENCODE_PROMPT_NAMESPACE, self.conversation.id().as_str()).unwrap();
        let settled = tokio::time::timeout(Duration::from_secs(5), async {
            while self.faults.inner.load(key.clone()).await.unwrap().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        let retained = if settled.is_err() {
            self.faults.inner.load(key).await.unwrap().map(|snapshot| {
                serde_json::json!({
                    "phase": snapshot.record.value["phase"]["phase"],
                    "waiter": snapshot.record.value["waiter"],
                    "cancellation": snapshot.record.value["cancellation"],
                })
            })
        } else {
            None
        };
        assert!(
            settled.is_ok(),
            "accepted generation must durably settle despite observer loss; retained ownership: {retained:?}"
        );
    }

    async fn shutdown(self) {
        self.agent
            .shutdown()
            .await
            .expect("Agent must join native prompt and event tasks");
        // SDK task join closes its socket; the independent protocol peer still
        // needs to observe EOF before its connection guard is released.
        self.probe.wait_for(1, 0, 0).await;
        assert_eq!(self.probe.stats.maximum.load(Ordering::SeqCst), 1);
        drop(self.conversation);
        drop(self.workspace);
        self.probe.shutdown().await;
    }
}

#[tokio::test]
async fn opencode_portable_effort_and_personality_reject_before_native_mutation() {
    use agent_sdk::model::{PersonalitySelector, ReasoningEffort};
    use futures_util::FutureExt;

    let case = OpenCodeStreamCase::start().await;
    let observed = std::panic::AssertUnwindSafe(async {
        let models = case.agent.list_models().await.unwrap();
        assert!(!models.is_empty());
        assert!(
            models
                .iter()
                .all(|model| model.reasoning_efforts().is_empty()
                    && model.personalities().is_empty())
        );
        let messages = case.probe.stats.messages.load(Ordering::SeqCst);
        let sessions = case.probe.stats.next_session.load(Ordering::SeqCst);
        for name in [
            "minimal",
            "low",
            "medium",
            "high",
            "maximum",
            "ultra",
            "native-variant",
        ] {
            let effort = ReasoningEffort::new(name).unwrap();
            let error = case
                .conversation
                .prompt(OpenCodeStreamCase::request().with_reasoning_effort(effort.clone()))
                .await
                .expect_err("native variants are not portable reasoning selections");
            assert_eq!(error.kind(), ErrorKind::Unsupported);
            let result = case
                .workspace
                .new_conversation(
                    NewConversation::new(
                        None,
                        Some(WorkspaceDefaults::new(None, Some(effort), None).unwrap()),
                        Vec::new(),
                    )
                    .unwrap(),
                )
                .await;
            let error = match result {
                Err(error) => error,
                Ok(conversation) => {
                    conversation.close().await.unwrap();
                    panic!("unsupported conversation defaults reached native creation");
                }
            };
            assert_eq!(error.kind(), ErrorKind::Unsupported);
            assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), messages);
            assert_eq!(
                case.probe.stats.next_session.load(Ordering::SeqCst),
                sessions
            );
        }
        for name in ["none", "friendly", "build"] {
            let personality = PersonalitySelector::new(name).unwrap();
            let error = case
                .conversation
                .prompt(OpenCodeStreamCase::request().with_personality(personality.clone()))
                .await
                .expect_err("native agent names are not portable personalities");
            assert_eq!(error.kind(), ErrorKind::Unsupported);
            let result = case
                .workspace
                .new_conversation(
                    NewConversation::new(
                        None,
                        Some(WorkspaceDefaults::empty().with_personality(personality)),
                        Vec::new(),
                    )
                    .unwrap(),
                )
                .await;
            let error = match result {
                Err(error) => error,
                Ok(conversation) => {
                    conversation.close().await.unwrap();
                    panic!("unsupported personality defaults reached native creation");
                }
            };
            assert_eq!(error.kind(), ErrorKind::Unsupported);
            assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), messages);
            assert_eq!(
                case.probe.stats.next_session.load(Ordering::SeqCst),
                sessions
            );
        }
        let mut stream = case.stream().await;
        case.release();
        assert!(matches!(
            OpenCodeStreamCase::next(&mut stream).await,
            Ok(PromptEvent::Completed(_))
        ));
        assert!(stream.recv().await.is_none());
    })
    .catch_unwind()
    .await;
    case.release();
    case.shutdown().await;
    if let Err(panic) = observed {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn opencode_workspace_creation_rejects_unsupported_defaults_before_commit() {
    use agent_sdk::model::{PersonalitySelector, ReasoningEffort};
    use futures_util::FutureExt;

    let case = OpenCodeStreamCase::start().await;
    let observed = std::panic::AssertUnwindSafe(async {
        let original = case.agent.list_workspaces().await.unwrap();
        let messages = case.probe.stats.messages.load(Ordering::SeqCst);
        let sessions = case.probe.stats.next_session.load(Ordering::SeqCst);
        for defaults in [
            WorkspaceDefaults::new(None, Some(ReasoningEffort::high()), None).unwrap(),
            WorkspaceDefaults::empty()
                .with_personality(PersonalitySelector::new("friendly").unwrap()),
        ] {
            let result = case
                .agent
                .new_workspace(OpenCodeEventProbe::workspace_request_with_defaults(
                    defaults,
                ))
                .await;
            let error = match result {
                Err(error) => error,
                Ok(workspace) => {
                    workspace.close().await.unwrap();
                    panic!("unsupported defaults reached workspace publication");
                }
            };
            assert_eq!(error.kind(), ErrorKind::Unsupported);
            assert_eq!(error.context().operation, Operation::NewWorkspace);
            assert_eq!(case.agent.list_workspaces().await.unwrap(), original);
            assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), messages);
            assert_eq!(
                case.probe.stats.next_session.load(Ordering::SeqCst),
                sessions
            );
        }
        let valid = case
            .agent
            .new_workspace(OpenCodeEventProbe::workspace_request())
            .await
            .unwrap();
        let info = valid.info().await.unwrap();
        assert_eq!(info.revision(), 1);
        assert_eq!(info.context(), original[0].context());
        assert_eq!(info.defaults(), &WorkspaceDefaults::empty());
        valid.close().await.unwrap();
        let mut stream = case.stream().await;
        case.release();
        assert!(matches!(
            OpenCodeStreamCase::next(&mut stream).await,
            Ok(PromptEvent::Completed(_))
        ));
        assert!(stream.recv().await.is_none());
    })
    .catch_unwind()
    .await;
    case.release();
    case.shutdown().await;
    if let Err(panic) = observed {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn opencode_workspace_update_preflight_rejects_unsupported_defaults() {
    use agent_sdk::model::{PersonalitySelector, ReasoningEffort};
    use agent_sdk::workspace::{UpdateWorkspaceRequest, WorkspaceUpdateReadiness};
    use futures_util::FutureExt;

    let case = OpenCodeStreamCase::start().await;
    let observed = std::panic::AssertUnwindSafe(async {
        let original = case.workspace.info().await.unwrap();
        let messages = case.probe.stats.messages.load(Ordering::SeqCst);
        let sessions = case.probe.stats.next_session.load(Ordering::SeqCst);
        for defaults in [
            WorkspaceDefaults::new(None, Some(ReasoningEffort::high()), None).unwrap(),
            WorkspaceDefaults::empty()
                .with_personality(PersonalitySelector::new("friendly").unwrap()),
        ] {
            let request = UpdateWorkspaceRequest::new(
                original.id().clone(),
                original.revision(),
                "unsupported replacement",
                original.context().clone(),
                defaults,
            )
            .unwrap();
            let error = case
                .agent
                .check_workspace_update(&request)
                .await
                .expect_err("unsupported defaults cannot be ready for publication");
            assert_eq!(error.kind(), ErrorKind::Unsupported);
            assert_eq!(error.context().operation, Operation::CheckWorkspaceUpdate);
            assert_eq!(case.workspace.info().await.unwrap(), original);
            assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), messages);
            assert_eq!(
                case.probe.stats.next_session.load(Ordering::SeqCst),
                sessions
            );
        }
        let valid = UpdateWorkspaceRequest::new(
            original.id().clone(),
            original.revision(),
            "valid replacement",
            original.context().clone(),
            original.defaults().clone(),
        )
        .unwrap();
        assert_eq!(
            case.agent.check_workspace_update(&valid).await.unwrap(),
            WorkspaceUpdateReadiness::Ready
        );
        assert_eq!(case.workspace.info().await.unwrap(), original);
        let mut stream = case.stream().await;
        case.release();
        assert!(matches!(
            OpenCodeStreamCase::next(&mut stream).await,
            Ok(PromptEvent::Completed(_))
        ));
        assert!(stream.recv().await.is_none());
    })
    .catch_unwind()
    .await;
    case.release();
    case.shutdown().await;
    if let Err(panic) = observed {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn opencode_workspace_update_rejects_unsupported_defaults_before_commit() {
    use agent_sdk::model::{PersonalitySelector, ReasoningEffort};
    use agent_sdk::workspace::UpdateWorkspaceRequest;
    use futures_util::FutureExt;

    let case = OpenCodeStreamCase::start().await;
    let observed = std::panic::AssertUnwindSafe(async {
        let original = case.workspace.info().await.unwrap();
        let messages = case.probe.stats.messages.load(Ordering::SeqCst);
        let sessions = case.probe.stats.next_session.load(Ordering::SeqCst);
        for defaults in [
            WorkspaceDefaults::new(None, Some(ReasoningEffort::high()), None).unwrap(),
            WorkspaceDefaults::empty()
                .with_personality(PersonalitySelector::new("friendly").unwrap()),
        ] {
            let request = UpdateWorkspaceRequest::new(
                original.id().clone(),
                original.revision(),
                "unsupported replacement",
                original.context().clone(),
                defaults,
            )
            .unwrap();
            let error = case
                .agent
                .update_workspace(request)
                .await
                .expect_err("unsupported defaults cannot commit a workspace revision");
            assert_eq!(error.kind(), ErrorKind::Unsupported);
            assert_eq!(error.context().operation, Operation::UpdateWorkspace);
            assert_eq!(case.workspace.info().await.unwrap(), original);
            assert_eq!(
                case.agent
                    .workspace_info(original.id().clone())
                    .await
                    .unwrap(),
                original
            );
            assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), messages);
            assert_eq!(
                case.probe.stats.next_session.load(Ordering::SeqCst),
                sessions
            );
        }
        let accepted = case
            .agent
            .update_workspace(
                UpdateWorkspaceRequest::new(
                    original.id().clone(),
                    original.revision(),
                    "valid replacement",
                    original.context().clone(),
                    original.defaults().clone(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(accepted.id(), original.id());
        assert_eq!(accepted.revision(), original.revision() + 1);
        assert_eq!(accepted.name(), "valid replacement");
        assert_eq!(accepted.context(), original.context());
        assert_eq!(accepted.defaults(), original.defaults());
        assert_eq!(
            case.agent
                .workspace_info(original.id().clone())
                .await
                .unwrap(),
            accepted
        );
        assert_eq!(
            case.workspace.info().await.unwrap_err().kind(),
            ErrorKind::Conflict
        );
    })
    .catch_unwind()
    .await;
    case.release();
    case.shutdown().await;
    if let Err(panic) = observed {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn opencode_terminal_authentication_preserves_safe_classification() {
    let cases = [
        (
            serde_json::json!({"name":"ProviderAuthError","data":{"providerID":"PRIVATE_PROVIDER","message":"WIRE_SECRET"}}),
            ErrorKind::Integration,
            ErrorDetail::AuthenticationRejected,
            None,
        ),
        (
            serde_json::json!({"name":"MessageOutputLengthError","data":{}}),
            ErrorKind::Integration,
            ErrorDetail::OutputLimit,
            None,
        ),
        (
            serde_json::json!({"name":"StructuredOutputError","data":{"message":"WIRE_SECRET","retries":2}}),
            ErrorKind::Protocol,
            ErrorDetail::StructuredOutputMismatch,
            None,
        ),
        (
            serde_json::json!({"name":"ContextOverflowError","data":{"message":"WIRE_SECRET","responseBody":"PRIVATE_BODY"}}),
            ErrorKind::Integration,
            ErrorDetail::ContextLimit,
            None,
        ),
        (
            serde_json::json!({"name":"ContentFilterError","data":{"message":"WIRE_SECRET"}}),
            ErrorKind::Integration,
            ErrorDetail::ContentFiltered,
            None,
        ),
        (
            serde_json::json!({"name":"APIError","data":{"message":"WIRE_SECRET","statusCode":429,"isRetryable":true,"responseHeaders":{"authorization":"PRIVATE_HEADER"},"responseBody":"PRIVATE_BODY"}}),
            ErrorKind::Integration,
            ErrorDetail::ResourceExhausted,
            Some(429),
        ),
        (
            serde_json::json!({"name":"APIError","data":{"message":"WIRE_SECRET","statusCode":504,"isRetryable":true}}),
            ErrorKind::Integration,
            ErrorDetail::RemoteDeadlineExceeded,
            Some(504),
        ),
    ];
    for (native, kind, detail, status) in cases {
        for streamed in [false, true] {
            let case = OpenCodeStreamCase::start().await;
            *case.probe.stats.terminal_error.lock().unwrap() = Some(native.clone());
            let (error, eof) = if streamed {
                let mut stream = case.stream().await;
                case.release();
                let terminal = tokio::time::timeout(Duration::from_secs(5), stream.recv()).await;
                let eof = tokio::time::timeout(Duration::from_secs(3), stream.recv()).await;
                let error = match terminal {
                    Ok(Some(Err(error))) => Ok(error),
                    _ => Err("stream must publish one terminal error, never Completed"),
                };
                (error, matches!(eof, Ok(None)))
            } else {
                let conversation = Arc::clone(&case.conversation);
                let mut task = tokio::spawn(async move {
                    conversation.prompt(OpenCodeStreamCase::request()).await
                });
                case.probe.wait_for_messages(1).await;
                case.release();
                let result = tokio::time::timeout(Duration::from_secs(5), &mut task).await;
                if result.is_err() {
                    task.abort();
                    let _ = task.await;
                }
                (
                    match result {
                        Ok(Ok(Err(error))) => Ok(error),
                        _ => Err("typed terminal rejection must not become success"),
                    },
                    true,
                )
            };
            case.shutdown().await;
            let error = error.expect("terminal rejection must settle before cleanup");
            assert!(eof, "a failed stream must terminate");
            assert_eq!(error.kind(), kind, "{error:?}");
            assert_eq!(error.detail(), Some(&detail));
            assert_eq!(error.http_status(), status);
            assert_eq!(error.context().operation, Operation::Prompt);
            assert_eq!(error.context().phase, ErrorPhase::CompletionValidation);
            let rendered = format!("{error:?} {error}");
            for secret in [
                "WIRE_SECRET",
                "PRIVATE_PROVIDER",
                "PRIVATE_BODY",
                "PRIVATE_HEADER",
                "OpenCode",
            ] {
                assert!(!rendered.contains(secret), "{rendered}");
            }
        }
    }
}

#[tokio::test]
async fn opencode_http_authentication_preserves_safe_classification() {
    let cases = [
        (401, ErrorDetail::AuthenticationRejected),
        (403, ErrorDetail::PermissionDenied),
        (429, ErrorDetail::ResourceExhausted),
        (500, ErrorDetail::RemoteInternal),
        (503, ErrorDetail::ServiceUnavailable),
        (504, ErrorDetail::RemoteDeadlineExceeded),
    ];
    let case = OpenCodeStreamCase::start().await;
    let mut results = Vec::new();
    for (status, detail) in cases {
        case.probe
            .stats
            .provider_status
            .store(status, Ordering::SeqCst);
        results.push((status, detail, case.agent.list_models().await));
    }
    case.probe
        .stats
        .provider_status
        .store(200, Ordering::SeqCst);
    case.shutdown().await;
    for (status, detail, result) in results {
        let error = result.expect_err("HTTP rejection cannot become successful inventory");
        assert_eq!(error.kind(), ErrorKind::Integration);
        assert_eq!(error.detail(), Some(&detail));
        assert_eq!(error.http_status(), Some(status as u16));
        assert_eq!(error.context().operation, Operation::ListModels);
        assert_eq!(error.context().phase, ErrorPhase::ResponseRead);
        let rendered = format!("{error:?} {error}");
        assert!(!rendered.contains("WIRE_SECRET") && !rendered.contains("PRIVATE_PROVIDER"));
    }
}

#[tokio::test]
async fn opencode_prompt_stream_fixture_ordinary_prompt_baseline() {
    let case = OpenCodeStreamCase::start().await;
    let conversation = Arc::clone(&case.conversation);
    let task =
        tokio::spawn(async move { conversation.prompt(OpenCodeStreamCase::request()).await });
    case.probe.wait_for_messages(1).await;
    case.begin_text();
    case.delta(
        "evt_stream_first",
        OpenCodeStreamCase::ASSISTANT,
        "prt_stream_text",
        "text",
        "transient",
    );
    assert!(!task.is_finished(), "HTTP terminal remains gated");
    case.release();
    let response = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(response.output(), "authoritative HTTP answer");
    case.settled().await;
    case.shutdown().await;
}

#[tokio::test]
async fn opencode_prompt_stream_delivers_owned_text_before_http_terminal_and_filters_other_fields()
{
    let case = OpenCodeStreamCase::start().await;
    let mut stream = case.stream().await;
    case.begin_text();
    case.part(
        OpenCodeStreamCase::ASSISTANT,
        "prt_stream_reasoning",
        "reasoning",
    );
    case.message("msg_other_assistant", "msg_other_parent");
    case.part("msg_other_assistant", "prt_other", "text");
    for flag in ["synthetic", "ignored"] {
        let part_id = format!("prt_stream_{flag}");
        let mut part = serde_json::json!({
            "id": part_id, "sessionID": "ses_probe", "messageID": OpenCodeStreamCase::ASSISTANT,
            "type": "text", "text": "", "time": {"start": 1}
        });
        part[flag] = serde_json::json!(true);
        case.emit(
            &format!("evt_part_{flag}"),
            "message.part.updated",
            serde_json::json!({"sessionID": "ses_probe", "time": 1.0, "part": part}),
        );
        case.delta(
            &format!("evt_delta_{flag}"),
            OpenCodeStreamCase::ASSISTANT,
            &part_id,
            "text",
            "HIDDEN TEXT SECRET",
        );
    }
    case.delta(
        "evt_stream_user",
        OpenCodeStreamCase::PARENT,
        "prt_stream_text",
        "text",
        "USER SECRET",
    );
    case.delta(
        "evt_stream_reasoning",
        OpenCodeStreamCase::ASSISTANT,
        "prt_stream_reasoning",
        "text",
        "REASONING SECRET",
    );
    case.delta(
        "evt_stream_other",
        "msg_other_assistant",
        "prt_other",
        "text",
        "OTHER PROMPT SECRET",
    );
    case.delta(
        "evt_stream_unknown_part",
        OpenCodeStreamCase::ASSISTANT,
        "prt_unknown",
        "text",
        "UNKNOWN PART SECRET",
    );
    case.delta(
        "evt_stream_field",
        OpenCodeStreamCase::ASSISTANT,
        "prt_stream_text",
        "metadata",
        "METADATA SECRET",
    );
    case.delta(
        "evt_stream_first",
        OpenCodeStreamCase::ASSISTANT,
        "prt_stream_text",
        "text",
        "héllo ",
    );
    assert_eq!(
        OpenCodeStreamCase::next(&mut stream).await.unwrap(),
        PromptEvent::OutputDelta {
            text: "héllo ".into()
        }
    );
    assert!(case.probe.stats.native_active.load(Ordering::SeqCst));
    case.delta(
        "evt_stream_first",
        OpenCodeStreamCase::ASSISTANT,
        "prt_stream_text",
        "text",
        "héllo ",
    );
    case.delta(
        "evt_stream_second",
        OpenCodeStreamCase::ASSISTANT,
        "prt_stream_text",
        "text",
        "world",
    );
    assert_eq!(
        OpenCodeStreamCase::next(&mut stream).await.unwrap(),
        PromptEvent::OutputDelta {
            text: "world".into()
        }
    );
    case.release();
    let PromptEvent::Completed(response) = OpenCodeStreamCase::next(&mut stream).await.unwrap()
    else {
        panic!("HTTP terminal must complete once")
    };
    assert_eq!(response.output(), "authoritative HTTP answer");
    assert!(stream.recv().await.is_none());
    case.settled().await;
    case.shutdown().await;
}

#[tokio::test]
async fn opencode_prompt_stream_observer_drop_keeps_accepted_http_and_generation_owned() {
    let case = OpenCodeStreamCase::start().await;
    let mut stream = case.stream().await;
    case.begin_text();
    case.delta(
        "evt_stream_first",
        OpenCodeStreamCase::ASSISTANT,
        "prt_stream_text",
        "text",
        "observed",
    );
    OpenCodeStreamCase::next(&mut stream).await.unwrap();
    drop(stream);
    assert!(matches!(
        (case
            .conversation
            .prompt(OpenCodeStreamCase::request())
            .await)
            .as_ref()
            .map_err(SdkError::kind),
        Err(ErrorKind::Busy)
    ));
    assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 0);
    case.release();
    case.settled().await;
    assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 0);
    case.shutdown().await;
}

#[tokio::test]
async fn opencode_prompt_stream_disconnect_ends_observation_without_replaying_http() {
    let case = OpenCodeStreamCase::start().await;
    let mut stream = case.stream().await;
    case.begin_text();
    case.delta(
        "evt_stream_first",
        OpenCodeStreamCase::ASSISTANT,
        "prt_stream_text",
        "text",
        "before gap",
    );
    OpenCodeStreamCase::next(&mut stream).await.unwrap();
    case.probe.disconnect_current();
    assert!(matches!(
        (OpenCodeStreamCase::next(&mut stream).await)
            .as_ref()
            .map_err(SdkError::kind),
        Err(ErrorKind::Integration)
    ));
    assert!(stream.recv().await.is_none());
    assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 1);
    assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 0);
    case.release();
    case.settled().await;
    case.shutdown().await;
}

#[tokio::test]
async fn opencode_prompt_stream_malformed_delta_ends_observation_with_safe_error() {
    let case = OpenCodeStreamCase::start().await;
    let mut stream = case.stream().await;
    case.begin_text();
    case.delta(
        "evt_stream_first",
        OpenCodeStreamCase::ASSISTANT,
        "prt_stream_text",
        "text",
        "before malformed",
    );
    OpenCodeStreamCase::next(&mut stream).await.unwrap();
    case.emit(
        "evt_stream_malformed",
        "message.part.delta",
        serde_json::json!({
            "sessionID": "ses_probe", "messageID": OpenCodeStreamCase::ASSISTANT,
            "partID": "prt_stream_text", "field": "text", "delta": {"secret": "WIRE SECRET"}
        }),
    );
    let error = OpenCodeStreamCase::next(&mut stream)
        .await
        .expect_err("malformed typed delta must interrupt observation");
    assert!(matches!(
        (error).kind(),
        ErrorKind::Integration | ErrorKind::Protocol
    ));
    assert!(!format!("{error:?}").contains("WIRE SECRET"));
    case.release();
    case.settled().await;
    case.shutdown().await;
}

#[tokio::test]
async fn opencode_prompt_stream_abort_settles_the_same_generation_and_stream() {
    let case = OpenCodeStreamCase::start().await;
    let mut stream = case.stream().await;
    case.begin_text();
    case.delta(
        "evt_stream_first",
        OpenCodeStreamCase::ASSISTANT,
        "prt_stream_text",
        "text",
        "before abort",
    );
    OpenCodeStreamCase::next(&mut stream).await.unwrap();
    case.conversation.abort().await.unwrap();
    assert!(matches!(
        (OpenCodeStreamCase::next(&mut stream).await)
            .as_ref()
            .map_err(SdkError::kind),
        Err(ErrorKind::Cancelled)
    ));
    assert!(stream.recv().await.is_none());
    assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 1);
    case.settled().await;
    case.shutdown().await;
}

#[tokio::test]
async fn opencode_prompt_stream_overflow_is_bounded_and_does_not_abort_accepted_work() {
    let case = OpenCodeStreamCase::start().await;
    let mut stream = case.stream().await;
    case.begin_text();
    case.delta(
        "evt_stream_ready",
        OpenCodeStreamCase::ASSISTANT,
        "prt_stream_text",
        "text",
        "ready",
    );
    OpenCodeStreamCase::next(&mut stream).await.unwrap();
    let commits = case
        .faults
        .state
        .event_commit_attempts
        .load(Ordering::SeqCst);
    for index in 0..70 {
        case.delta(
            &format!("evt_stream_overflow_{index}"),
            OpenCodeStreamCase::ASSISTANT,
            "prt_stream_text",
            "text",
            "chunk",
        );
    }
    case.faults
        .wait_for_event_commit_attempts(commits + 70)
        .await;
    let mut fragments = 0;
    loop {
        match OpenCodeStreamCase::next(&mut stream).await {
            Ok(PromptEvent::OutputDelta { text }) => {
                assert_eq!(text, "chunk");
                fragments += 1;
                assert!(
                    fragments <= 64,
                    "the public observation queue must remain bounded"
                );
            }
            Err(error) if error.kind() == ErrorKind::Integration => break,
            event => panic!("overflow must end observation explicitly: {event:?}"),
        }
    }
    assert!(stream.recv().await.is_none());
    assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 0);
    case.release();
    case.settled().await;
    case.shutdown().await;
}

#[tokio::test]
async fn opencode_prompt_stream_sequence_gap_interrupts_even_when_same_connection_recovers() {
    let case = OpenCodeStreamCase::start().await;
    let mut stream = case.stream().await;
    case.begin_text();
    case.delta(
        "evt_stream_ready",
        OpenCodeStreamCase::ASSISTANT,
        "prt_stream_text",
        "text",
        "ready",
    );
    OpenCodeStreamCase::next(&mut stream).await.unwrap();
    // Durable position has not advanced through the already-seen normal parent
    // events. This sequence cannot be applied contiguously without history.
    case.probe.emit_gap(2, "prompt_stream");
    assert!(matches!(
        (OpenCodeStreamCase::next(&mut stream).await)
            .as_ref()
            .map_err(SdkError::kind),
        Err(ErrorKind::Integration)
    ));
    assert_eq!(case.probe.stats.subscriptions.load(Ordering::SeqCst), 1);
    case.release();
    case.settled().await;
    case.shutdown().await;
}

#[tokio::test]
async fn opencode_prompt_stream_cancelled_recv_does_not_lose_the_next_fragment() {
    let case = OpenCodeStreamCase::start().await;
    let mut stream = case.stream().await;
    case.begin_text();
    assert!(
        tokio::time::timeout(Duration::from_millis(25), stream.recv())
            .await
            .is_err()
    );
    case.delta(
        "evt_stream_after_recv_drop",
        OpenCodeStreamCase::ASSISTANT,
        "prt_stream_text",
        "text",
        "still observed",
    );
    assert_eq!(
        OpenCodeStreamCase::next(&mut stream).await.unwrap(),
        PromptEvent::OutputDelta {
            text: "still observed".into()
        }
    );
    case.release();
    assert!(matches!(
        OpenCodeStreamCase::next(&mut stream).await.unwrap(),
        PromptEvent::Completed(_)
    ));
    case.settled().await;
    case.shutdown().await;
}

#[tokio::test]
async fn opencode_prompt_stream_http_terminal_waits_for_its_late_live_completion_marker() {
    let case = OpenCodeStreamCase::start().await;
    case.probe
        .stats
        .withhold_stream_completion
        .store(true, Ordering::SeqCst);
    let mut stream = case.stream().await;
    case.begin_text();
    case.delta(
        "evt_stream_before_http",
        OpenCodeStreamCase::ASSISTANT,
        "prt_stream_text",
        "text",
        "before HTTP",
    );
    OpenCodeStreamCase::next(&mut stream).await.unwrap();
    case.release();
    case.http_responded().await;
    case.completion("msg_unrelated_completed", OpenCodeStreamCase::PARENT);
    assert!(
        tokio::time::timeout(Duration::from_millis(25), stream.recv())
            .await
            .is_err(),
        "another Assistant completion cannot close this HTTP result's observation"
    );
    case.delta(
        "evt_stream_late_delta",
        OpenCodeStreamCase::ASSISTANT,
        "prt_stream_text",
        "text",
        "late transport delta",
    );
    assert_eq!(
        OpenCodeStreamCase::next(&mut stream).await.unwrap(),
        PromptEvent::OutputDelta {
            text: "late transport delta".into()
        }
    );
    case.completion(OpenCodeStreamCase::ASSISTANT, OpenCodeStreamCase::PARENT);
    let PromptEvent::Completed(response) = OpenCodeStreamCase::next(&mut stream).await.unwrap()
    else {
        panic!("owned marker unlocks only the authoritative HTTP result")
    };
    assert_eq!(response.output(), "authoritative HTTP answer");
    assert!(stream.recv().await.is_none());
    case.settled().await;
    case.shutdown().await;
}

#[tokio::test]
async fn opencode_prompt_stream_early_live_completion_cannot_replace_http_authority() {
    let case = OpenCodeStreamCase::start().await;
    case.probe
        .stats
        .withhold_stream_completion
        .store(true, Ordering::SeqCst);
    let mut stream = case.stream().await;
    case.begin_text();
    case.delta(
        "evt_stream_early_delta",
        OpenCodeStreamCase::ASSISTANT,
        "prt_stream_text",
        "text",
        "transient",
    );
    OpenCodeStreamCase::next(&mut stream).await.unwrap();
    case.completion(OpenCodeStreamCase::ASSISTANT, OpenCodeStreamCase::PARENT);
    assert!(
        tokio::time::timeout(Duration::from_millis(25), stream.recv())
            .await
            .is_err(),
        "SSE completion metadata is not a terminal PromptResponse"
    );
    assert_eq!(case.probe.stats.prompt_responses.load(Ordering::SeqCst), 0);
    case.release();
    let PromptEvent::Completed(response) = OpenCodeStreamCase::next(&mut stream).await.unwrap()
    else {
        panic!("already observed marker cannot strand HTTP terminal")
    };
    assert_eq!(response.output(), "authoritative HTTP answer");
    case.settled().await;
    case.shutdown().await;
}

#[tokio::test]
async fn opencode_prompt_stream_missing_live_marker_has_a_bounded_observation_deadline() {
    let case = OpenCodeStreamCase::start().await;
    case.probe
        .stats
        .withhold_stream_completion
        .store(true, Ordering::SeqCst);
    let mut stream = case.stream().await;
    case.begin_text();
    case.delta(
        "evt_stream_before_missing_marker",
        OpenCodeStreamCase::ASSISTANT,
        "prt_stream_text",
        "text",
        "partial",
    );
    OpenCodeStreamCase::next(&mut stream).await.unwrap();
    case.release();
    case.http_responded().await;
    let event = tokio::time::timeout(Duration::from_secs(8), stream.recv())
        .await
        .expect("missing transient completion evidence must not hang the accepted owner");
    assert!(
        matches!(
            (event)
                .as_ref()
                .map(|result| result.as_ref().map_err(SdkError::kind)),
            Some(Err(ErrorKind::Timeout))
        ),
        "missing marker cannot turn a potentially incomplete observation into Completed: {event:?}"
    );
    assert!(stream.recv().await.is_none());
    assert_eq!(
        case.probe.stats.aborts.load(Ordering::SeqCst),
        0,
        "HTTP terminal work is already complete and must not be aborted or replayed"
    );
    assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 1);
    case.settled().await;
    case.shutdown().await;
}

#[tokio::test]
async fn opencode_prompt_stream_last_handle_drop_releases_owned_tasks() {
    let case = OpenCodeStreamCase::start().await;
    let mut stream = case.stream().await;
    case.begin_text();
    case.delta(
        "evt_stream_before_drop",
        OpenCodeStreamCase::ASSISTANT,
        "prt_stream_text",
        "text",
        "accepted",
    );
    OpenCodeStreamCase::next(&mut stream).await.unwrap();
    let OpenCodeStreamCase {
        probe,
        agent,
        workspace,
        conversation,
        faults,
        runtime_baseline,
    } = case;
    let runtime = tokio::runtime::Handle::current();
    assert!(
        runtime.metrics().num_alive_tasks()
            > runtime_baseline + probe.stats.connections.load(Ordering::SeqCst),
        "accepted SDK tasks must exist independently of fixture connection tasks"
    );
    drop(conversation);
    drop(workspace);
    drop(agent);
    // This current-thread runtime cannot switch between these two reads. The
    // fixture's connection guards count its live HTTP/SSE tasks separately;
    // the initial baseline includes the listener and caller-retained storage.
    tokio::time::timeout(Duration::from_secs(3), async {
        while runtime.metrics().num_alive_tasks()
            > runtime_baseline + probe.stats.connections.load(Ordering::SeqCst)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("last native runtime handle cannot leave a prompt or transport task detached");
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), stream.recv())
            .await
            .expect("revoked output observation must terminate"),
        Some(Err(_))
    ));
    assert!(stream.recv().await.is_none());
    drop(faults);
    probe.shutdown().await;
}

#[tokio::test]
async fn opencode_prompt_stream_deadline_keeps_polling_its_held_storage_commit() {
    let case = OpenCodeStreamCase::start().await;
    case.faults.arm(PromptCommitFault::BlockThenCommit, 1);
    let mut stream = case
        .conversation
        .prompt_stream(
            OpenCodeStreamCase::request()
                .with_timeout(Duration::from_millis(100))
                .unwrap(),
        )
        .await
        .unwrap();
    case.faults.wait_until_blocked().await;
    // Observation must meet its own deadline even while an accepted lower
    // commit is held. Releasing that commit afterward must still advance the
    // same retained core future and timeout intent without native dispatch.
    let terminal = tokio::time::timeout(Duration::from_secs(1), stream.recv())
        .await
        .expect("observer deadline cannot wait for its held storage commit");
    assert!(matches!(
        (terminal)
            .as_ref()
            .map(|result| result.as_ref().map_err(SdkError::kind)),
        Some(Err(ErrorKind::Timeout))
    ));
    case.faults.release();
    assert!(stream.recv().await.is_none());
    assert_eq!(
        case.probe.stats.messages.load(Ordering::SeqCst),
        0,
        "expired pre-dispatch work must not call the native prompt"
    );
    case.settled().await;
    case.shutdown().await;
}

#[tokio::test]
async fn opencode_prompt_stream_claim_failure_is_terminal_and_preserves_primary_error() {
    OpenCodeStreamCase::verify_waiter_failure(1).await;
}

#[tokio::test]
async fn opencode_prompt_stream_settlement_failure_is_terminal_and_preserves_primary_error() {
    OpenCodeStreamCase::verify_waiter_failure(2).await;
}

#[tokio::test]
async fn opencode_prompt_stream_clear_failure_is_terminal_and_preserves_primary_error() {
    OpenCodeStreamCase::verify_waiter_failure(3).await;
}

#[tokio::test]
async fn opencode_prompt_stream_cleanup_retry_is_bounded_and_preserves_primary_error() {
    OpenCodeStreamCase::verify_waiter_failure(4).await;
}

#[tokio::test]
async fn opencode_prompt_stream_terminal_cleanup_commit_conflict_recomputes_without_replay() {
    OpenCodeStreamCase::verify_terminal_cleanup_commit(PreparedCancelFault::CommitConflictOnce)
        .await;
}

#[tokio::test]
async fn opencode_prompt_stream_terminal_cleanup_conflicts_keep_original_deadline() {
    OpenCodeStreamCase::verify_terminal_cleanup_commit(PreparedCancelFault::CommitConflict).await;
}

#[tokio::test]
async fn opencode_prompt_stream_terminal_deadline_preserves_its_admitted_commit() {
    for primary_failure in [false, true] {
        let case = OpenCodeStreamCase::start().await;
        case.probe
            .stats
            .invalid_stream_response
            .store(primary_failure, Ordering::SeqCst);
        case.probe
            .stats
            .withhold_stream_completion
            .store(true, Ordering::SeqCst);
        case.faults
            .state
            .block_waiter_commit
            .store(true, Ordering::SeqCst);
        let mut stream = case.stream().await;
        case.begin_text();
        // The primary-error path skips the normal response's completion-marker
        // wait. Observe a same-stream delta after that marker before holding the
        // writer, so this tests observer timeout rather than event-owner expiry.
        case.completion(OpenCodeStreamCase::ASSISTANT, OpenCodeStreamCase::PARENT);
        case.delta(
            "evt_terminal_waiter_ready",
            OpenCodeStreamCase::ASSISTANT,
            "prt_stream_text",
            "text",
            "accepted before terminal hold",
        );
        assert_eq!(
            OpenCodeStreamCase::next(&mut stream).await.unwrap(),
            PromptEvent::OutputDelta {
                text: "accepted before terminal hold".into()
            }
        );
        case.release();
        case.faults.wait_until_blocked().await;
        let terminal = tokio::time::timeout(Duration::from_secs(7), stream.recv())
            .await
            .expect("terminal observation is bounded while commit remains held");
        if primary_failure {
            assert!(
                matches!(
                    (terminal)
                        .as_ref()
                        .map(|result| result.as_ref().map_err(SdkError::kind)),
                    Some(Err(ErrorKind::Protocol))
                ),
                "execution failure must survive a held cleanup commit: {terminal:?}"
            );
        } else {
            assert!(
                matches!(
                    (terminal)
                        .as_ref()
                        .map(|result| result.as_ref().map_err(SdkError::kind)),
                    Some(Err(ErrorKind::Timeout))
                ),
                "held cleanup cannot claim Completed: {terminal:?}"
            );
        }
        assert!(stream.recv().await.is_none());
        case.faults.release();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let retained = case
                    .faults
                    .prompt_snapshot(&case.conversation.id())
                    .await
                    .expect("late cleanup commit remains recoverable");
                if retained.record.value["waiter"] == "lost" {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the admitted waiter commit must survive observer timeout");
        assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 1);
        assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 0);
        case.revoke().await;
    }
}

#[tokio::test]
async fn opencode_prompt_stream_owned_removal_revokes_text_but_unrelated_removal_does_not() {
    // Pinned OpenCode v1.19.3: packages/schema/src/v1/session.ts
    // retains message/part IDs; session/session.ts removeMessage/removePart
    // publishes the removal.
    // These are actual SSE frames, not a claim of executing the pinned server.
    for (remove_parent, remove_part) in [(false, false), (false, true), (true, false), (true, true)]
    {
        let case = OpenCodeStreamCase::start().await;
        let mut stream = case.stream().await;
        case.begin_text();
        case.message("msg_unrelated_removed", "msg_unrelated_parent");
        case.part("msg_unrelated_removed", "prt_unrelated_removed", "text");
        for (kind, fields) in [
            (
                "message.part.removed",
                serde_json::json!({
                    "sessionID": "ses_probe", "messageID": "msg_unrelated_removed",
                    "partID": "prt_unrelated_removed"
                }),
            ),
            (
                "message.removed",
                serde_json::json!({
                    "sessionID": "ses_probe", "messageID": "msg_unrelated_removed"
                }),
            ),
        ] {
            case.emit(&format!("evt_unrelated_{kind}"), kind, fields);
        }
        case.delta(
            "evt_after_unrelated_removal",
            OpenCodeStreamCase::ASSISTANT,
            "prt_stream_text",
            "text",
            "still owned",
        );
        assert_eq!(
            OpenCodeStreamCase::next(&mut stream).await.unwrap(),
            PromptEvent::OutputDelta {
                text: "still owned".into()
            }
        );
        let kind = if remove_part {
            "message.part.removed"
        } else {
            "message.removed"
        };
        let message = if remove_parent {
            OpenCodeStreamCase::PARENT
        } else {
            OpenCodeStreamCase::ASSISTANT
        };
        let mut fields = serde_json::json!({
            "sessionID": "ses_probe", "messageID": message
        });
        if remove_part {
            fields["partID"] = if remove_parent {
                case.probe
                    .stats
                    .prompt_parent
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()["parts"][0]["id"]
                    .clone()
            } else {
                serde_json::json!("prt_stream_text")
            };
        }
        case.emit("evt_owned_removal", kind, fields);
        case.delta(
            "evt_after_owned_removal",
            OpenCodeStreamCase::ASSISTANT,
            "prt_stream_text",
            "text",
            "REMOVED OWNER SECRET",
        );
        assert!(
            OpenCodeStreamCase::next(&mut stream).await.is_err(),
            "removal of an observed owner must interrupt rather than leak stale output"
        );
        assert!(stream.recv().await.is_none());
        case.release();
        case.settled().await;
        assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 0);
        case.shutdown().await;
    }
}

#[tokio::test]
async fn opencode_prompt_stream_terminal_persistence_retry_is_bounded_and_preserves_primary() {
    for primary in [false, true] {
        OpenCodeStreamCase::verify_persistence_failure(
            PromptPersistenceFault::Terminal,
            false,
            primary,
        )
        .await;
    }
}

#[tokio::test]
async fn opencode_prompt_stream_terminal_persistence_foreign_owner_preserves_primary() {
    for primary in [false, true] {
        OpenCodeStreamCase::verify_persistence_failure(
            PromptPersistenceFault::Terminal,
            true,
            primary,
        )
        .await;
    }
}

#[tokio::test]
async fn opencode_prompt_stream_ambiguous_response_persistence_preserves_primary_and_is_bounded() {
    for foreign in [false, true] {
        OpenCodeStreamCase::verify_persistence_failure(
            PromptPersistenceFault::ResponseLoss,
            foreign,
            true,
        )
        .await;
    }
}

#[tokio::test]
async fn opencode_prompt_stream_ambiguous_response_delivery_preserves_primary_and_is_bounded() {
    for fault in [
        PromptPersistenceFault::ResponseLossClaim,
        PromptPersistenceFault::ResponseLossSettle,
    ] {
        for foreign in [false, true] {
            OpenCodeStreamCase::verify_persistence_failure(fault, foreign, true).await;
        }
    }
}

#[tokio::test]
async fn opencode_prompt_stream_timeout_intent_retries_once_then_aborts_exact_generation() {
    let case = OpenCodeStreamCase::start().await;
    case.faults
        .set_persistence_fault(PromptPersistenceFault::TimeoutOnce);
    let mut stream = case
        .conversation
        .prompt_stream(
            OpenCodeStreamCase::request()
                .with_timeout(Duration::from_millis(200))
                .unwrap(),
        )
        .await
        .unwrap();
    case.probe.wait_for_messages(1).await;
    assert!(matches!(
        (tokio::time::timeout(Duration::from_secs(1), stream.recv())
            .await
            .unwrap())
        .as_ref()
        .map(|result| result.as_ref().map_err(SdkError::kind)),
        Some(Err(ErrorKind::Timeout))
    ));
    tokio::time::timeout(Duration::from_secs(3), async {
        while case.probe.stats.aborts.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("one failed timeout intent must retry and reach its exact native abort");
    assert_eq!(
        case.faults
            .state
            .persistence_attempts
            .load(Ordering::SeqCst),
        1
    );
    assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 1);
    case.release();
    case.settled().await;
    case.shutdown().await;
}

#[tokio::test]
async fn opencode_prompt_stream_timeout_intent_retries_with_a_bound_and_retains_native_work() {
    let case = OpenCodeStreamCase::start().await;
    case.faults
        .set_persistence_fault(PromptPersistenceFault::Timeout);
    let mut stream = case
        .conversation
        .prompt_stream(
            OpenCodeStreamCase::request()
                .with_timeout(Duration::from_millis(200))
                .unwrap(),
        )
        .await
        .unwrap();
    case.probe.wait_for_messages(1).await;
    assert!(matches!(
        (tokio::time::timeout(Duration::from_secs(1), stream.recv())
            .await
            .unwrap())
        .as_ref()
        .map(|result| result.as_ref().map_err(SdkError::kind)),
        Some(Err(ErrorKind::Timeout))
    ));
    tokio::time::sleep(Duration::from_secs(6)).await;
    let attempts = case
        .faults
        .state
        .persistence_attempts
        .load(Ordering::SeqCst);
    assert!(
        attempts > 1,
        "retryable timeout intent cannot be silently discarded"
    );
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        case.faults
            .state
            .persistence_attempts
            .load(Ordering::SeqCst),
        attempts,
        "timeout intent retry admission must stop at its lifecycle bound"
    );
    assert_eq!(
        case.probe.stats.aborts.load(Ordering::SeqCst),
        0,
        "uncommitted cancellation intent cannot authorize native abort"
    );
    assert!(
        case.faults
            .prompt_snapshot(&case.conversation.id())
            .await
            .is_some()
    );
    case.faults
        .set_persistence_fault(PromptPersistenceFault::None);
    case.release();
    case.settled().await;
    case.shutdown().await;
}

#[tokio::test]
async fn opencode_prompt_stream_dispatch_persistence_retry_admission_is_bounded() {
    for fault in [
        PromptPersistenceFault::DispatchOnce,
        PromptPersistenceFault::Dispatch,
    ] {
        let case = OpenCodeStreamCase::start().await;
        case.faults.set_persistence_fault(fault);
        let started = tokio::time::Instant::now();
        // Unlike stream(), this deliberately does not wait for an HTTP POST:
        // dispatch must first durably succeed, and this fault rejects that write.
        let mut stream = case
            .conversation
            .prompt_stream(OpenCodeStreamCase::request())
            .await
            .expect("local stream admission");
        if fault.fails_once() {
            case.probe.wait_for_messages(1).await;
            assert_eq!(
                case.faults
                    .state
                    .persistence_attempts
                    .load(Ordering::SeqCst),
                1
            );
            case.begin_text();
            case.release();
            assert!(matches!(
                OpenCodeStreamCase::next(&mut stream).await,
                Ok(PromptEvent::Completed(_))
            ));
            assert!(stream.recv().await.is_none());
            case.settled().await;
            case.shutdown().await;
            continue;
        }
        let terminal = tokio::time::timeout(Duration::from_secs(7), stream.recv())
            .await
            .expect("dispatch persistence without a caller timeout must terminate observation");
        assert!(
            matches!(
                (terminal)
                    .as_ref()
                    .map(|result| result.as_ref().map_err(SdkError::kind)),
                Some(Err(ErrorKind::Timeout))
            ),
            "{terminal:?}"
        );
        assert!(stream.recv().await.is_none());
        tokio::time::sleep_until(started + Duration::from_secs(6)).await;
        let attempts = case
            .faults
            .state
            .persistence_attempts
            .load(Ordering::SeqCst);
        assert!(attempts > 1, "transient dispatch failures must retry");
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            case.faults
                .state
                .persistence_attempts
                .load(Ordering::SeqCst),
            attempts,
            "dispatch retry admission must stop after the lifecycle budget"
        );
        assert_eq!(
            case.probe.stats.messages.load(Ordering::SeqCst),
            0,
            "a rejected dispatch write never authorizes a native request"
        );
        assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 0);
        case.settled().await;
        case.faults
            .set_persistence_fault(PromptPersistenceFault::None);
        let mut retry = case.stream().await;
        case.begin_text();
        case.release();
        assert!(matches!(
            OpenCodeStreamCase::next(&mut retry).await,
            Ok(PromptEvent::Completed(_))
        ));
        assert!(retry.recv().await.is_none());
        case.settled().await;
        assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 1);
        case.shutdown().await;
    }
}

#[tokio::test]
async fn opencode_prompt_stream_dispatch_deadline_retains_its_admitted_commit() {
    let case = OpenCodeStreamCase::start().await;
    case.faults.arm(PromptCommitFault::BlockThenCommit, 2);
    let mut stream = case
        .conversation
        .prompt_stream(OpenCodeStreamCase::request())
        .await
        .expect("local stream admission");
    case.faults.wait_until_blocked().await;
    let prepared = case
        .faults
        .prompt_snapshot(&case.conversation.id())
        .await
        .expect("the held dispatch follows its durable prepared record");
    assert_eq!(
        prepared
            .record
            .value
            .pointer("/phase/phase")
            .and_then(serde_json::Value::as_str),
        Some("prepared")
    );
    let terminal = tokio::time::timeout(Duration::from_secs(7), stream.recv())
        .await
        .expect("dispatch observer must time out before its admitted commit is released");
    assert!(
        matches!(
            (terminal)
                .as_ref()
                .map(|result| result.as_ref().map_err(SdkError::kind)),
            Some(Err(ErrorKind::Timeout))
        ),
        "{terminal:?}"
    );
    assert!(stream.recv().await.is_none());
    assert_eq!(
        case.faults.state.prompt_commits.load(Ordering::SeqCst),
        2,
        "deadline must not drop the held commit and start a competing cleanup"
    );
    assert_eq!(
        case.faults.prompt_snapshot(&case.conversation.id()).await,
        Some(prepared),
        "prepared state remains unchanged while dispatch commit is held"
    );
    assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 0);
    case.faults.release();
    case.settled().await;
    assert_eq!(
        case.probe.stats.messages.load(Ordering::SeqCst),
        0,
        "late dispatch persistence must settle without a late native POST"
    );
    assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 0);
    let mut retry = case.stream().await;
    case.begin_text();
    case.release();
    assert!(matches!(
        OpenCodeStreamCase::next(&mut retry).await,
        Ok(PromptEvent::Completed(_))
    ));
    assert!(retry.recv().await.is_none());
    case.settled().await;
    case.shutdown().await;
}

#[tokio::test]
async fn opencode_prompt_stream_abort_outcome_retry_admission_is_bounded() {
    for fault in [
        PromptPersistenceFault::AbortOutcomeOnce,
        PromptPersistenceFault::AbortOutcome,
    ] {
        OpenCodeStreamCase::verify_abort_persistence(fault).await;
    }
}

#[tokio::test]
async fn opencode_prompt_stream_cancellation_claim_retries_are_bounded() {
    for fault in [
        PromptPersistenceFault::CancelClaimOnce,
        PromptPersistenceFault::CancelClaim,
    ] {
        OpenCodeStreamCase::verify_abort_persistence(fault).await;
    }
}

#[tokio::test]
async fn opencode_prompt_stream_cancellation_dispatch_retries_are_bounded() {
    for fault in [
        PromptPersistenceFault::CancelDispatchOnce,
        PromptPersistenceFault::CancelDispatch,
    ] {
        OpenCodeStreamCase::verify_abort_persistence(fault).await;
    }
}

impl OpenCodeStreamCase {
    async fn verify_admission_persistence(fault: PromptPersistenceFault) {
        let case = OpenCodeStreamCase::start().await;
        let outcome = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(15), async {
            let started = tokio::time::Instant::now();
            let mut stream = case.stream_with_admission_fault(fault).await;
            if fault == PromptPersistenceFault::AdmissionCommitUnavailableOnce {
                let terminal = tokio::time::timeout(Duration::from_secs(3), stream.recv())
                    .await
                    .expect("an unconfirmed commit must not resume the mutation")
                    .expect("the stream publishes the original commit error")
                    .expect_err("unchanged durable state cannot confirm commit");
                assert_eq!(terminal.kind(), ErrorKind::Integration);
                assert_eq!(terminal.context().phase, ErrorPhase::StateCommit);
                assert_eq!(terminal.detail(), Some(&ErrorDetail::ServiceUnavailable));
                assert!(stream.recv().await.is_none());
                assert_eq!(
                    case.faults.state.admission_writes.load(Ordering::SeqCst),
                    1,
                    "an ambiguous commit must not repeat the mutation"
                );
                assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 1);
                return;
            }
            if fault.fails_once() {
                case.wait_for_admission().await;
                assert_eq!(
                    case.faults
                        .state
                        .persistence_attempts
                        .load(Ordering::SeqCst),
                    1
                );
                case.begin_text();
                case.release();
                assert!(matches!(
                    OpenCodeStreamCase::next(&mut stream).await,
                    Ok(PromptEvent::Completed(_))
                ));
                case.settled().await;
                if fault == PromptPersistenceFault::AdmissionCommitReceiptLost {
                    assert_eq!(
                        case.faults.state.admission_writes.load(Ordering::SeqCst),
                        1,
                        "confirmed lost commit receipt must not replay an already applied mutation"
                    );
                }
                assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 1);
                return;
            }
            let terminal = tokio::time::timeout(Duration::from_secs(7), stream.recv())
                .await
                .expect("native admission persistence cannot leave stream observation unbounded");
            assert!(
                matches!(
                    (terminal)
                        .as_ref()
                        .map(|result| result.as_ref().map_err(SdkError::kind)),
                    Some(Err(ErrorKind::Timeout))
                ),
                "{terminal:?}"
            );
            assert!(stream.recv().await.is_none());
            case.assert_persistence_retries_stopped(started).await;
            let retained = case
                .faults
                .prompt_snapshot(&case.conversation.id())
                .await
                .unwrap();
            assert_eq!(
                retained
                    .record
                    .value
                    .pointer("/phase/phase")
                    .and_then(serde_json::Value::as_str),
                Some("dispatched_awaiting_admission")
            );
            assert_eq!(retained.record.value["input_observed"], true);
            assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 1);
            assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 0);
        }).await;
        case.finish_observed(outcome).await;
    }
}

#[tokio::test]
async fn opencode_prompt_stream_admission_persistence_retries_are_bounded() {
    for fault in [
        PromptPersistenceFault::AdmissionOnce,
        PromptPersistenceFault::Admission,
    ] {
        OpenCodeStreamCase::verify_admission_persistence(fault).await;
    }
}

#[tokio::test]
async fn opencode_prompt_admission_exact_transaction_conflicts_recompute_without_replay() {
    OpenCodeStreamCase::verify_admission_persistence(PromptPersistenceFault::AdmissionConflictOnce)
        .await;
}

#[tokio::test]
async fn opencode_prompt_admission_permanent_transaction_conflict_keeps_original_deadline() {
    OpenCodeStreamCase::verify_admission_persistence(PromptPersistenceFault::AdmissionConflict)
        .await;
}

#[tokio::test]
async fn opencode_prompt_stream_reserved_cancellation_retries_are_bounded() {
    for fault in [
        PromptPersistenceFault::CancelRequestOnce,
        PromptPersistenceFault::CancelRequest,
    ] {
        let case = OpenCodeStreamCase::start().await;
        let observed = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(15),
            async {
                case.faults.arm(PromptCommitFault::BlockThenCommit, 1);
                case.faults.set_persistence_fault(fault);
                let mut stream = case
                    .conversation
                    .prompt_stream(OpenCodeStreamCase::request())
                    .await
                    .unwrap();
                case.faults.wait_until_blocked().await;
                case.faults.block_next_transaction_begin();
                let mut abort = Box::pin(case.conversation.abort());
                std::future::poll_fn(|context| {
                    assert!(
                        abort.as_mut().poll(context).is_pending(),
                        "abort must join the held start"
                    );
                    std::task::Poll::Ready(())
                })
                .await;
                tokio::select! {
                    biased;
                    () = case.faults.wait_until_transaction_begin_blocked() => {}
                    result = &mut abort => panic!("reserved abort ended before its storage boundary: {result:?}"),
                }
                let started = tokio::time::Instant::now();
                // Hold the competing foreground at its next transaction boundary.
                // The already waiting abort must snapshot the actual prompt commit
                // before either owner can remove the reserved generation.
                case.faults.block_next_transaction_begin();
                case.faults.release();
                let admission = async {
                    drop(
                        tokio::time::timeout(
                            Duration::from_secs(3),
                            case.faults
                                .state
                                .cancellation_request_admission_committed
                                .acquire(),
                        )
                        .await
                        .expect("reserved cancellation requires a real prompt commit")
                        .expect("prompt publication receipt remains owned"),
                    );
                    case.faults.release_transaction_begin();
                    case.faults.wait_until_transaction_begin_blocked().await;
                    tokio::time::timeout(Duration::from_secs(3), async {
                        while case
                            .faults
                            .state
                            .persistence_attempts
                            .load(Ordering::SeqCst)
                            == 0
                        {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .expect("the selected cancellation must reach its injected write failure");
                    case.faults.release_transaction_begin();
                };
                let ((), aborted, terminal) = tokio::time::timeout(Duration::from_secs(7), async {
                    tokio::join!(admission, &mut abort, stream.recv())
                })
                .await
                .expect("reserved cancellation persistence cannot leave accepted work unbounded");
                drop(abort);
                assert!(
                    case.faults
                        .state
                        .persistence_attempts
                        .load(Ordering::SeqCst)
                        > 0
                );
                assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 0);
                assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 0);
                if fault.fails_once() {
                    assert!(aborted.is_ok(), "{aborted:?}");
                    assert!(
                        matches!(
                            (terminal)
                                .as_ref()
                                .map(|result| result.as_ref().map_err(SdkError::kind)),
                            Some(Err(ErrorKind::Cancelled))
                        ),
                        "{terminal:?}"
                    );
                    assert!(stream.recv().await.is_none());
                    assert_eq!(
                        case.faults
                            .state
                            .persistence_attempts
                            .load(Ordering::SeqCst),
                        1
                    );
                    case.settled().await;
                    return;
                }
                assert!(
                    matches!(
                        (aborted).as_ref().map_err(SdkError::kind),
                        Err(ErrorKind::Timeout)
                    ),
                    "{aborted:?}"
                );
                assert!(
                    matches!(
                        (terminal)
                            .as_ref()
                            .map(|result| result.as_ref().map_err(SdkError::kind)),
                        Some(Err(ErrorKind::Timeout))
                    ),
                    "{terminal:?}"
                );
                assert!(stream.recv().await.is_none());
                case.assert_persistence_retries_stopped(started).await;
                let retained = case
                    .faults
                    .prompt_snapshot(&case.conversation.id())
                    .await
                    .unwrap();
                assert_eq!(
                    retained
                        .record
                        .value
                        .pointer("/phase/phase")
                        .and_then(serde_json::Value::as_str),
                    Some("prepared")
                );
                assert_eq!(
                    retained
                        .record
                        .value
                        .pointer("/cancellation/state")
                        .and_then(serde_json::Value::as_str),
                    Some("not_requested")
                );
                let error = aborted
                    .expect_err("persistent cancellation intent failure must remain an error");
                OpenCodeStreamCase::assert_reconciliation_timeout(
                    &error,
                    Operation::Abort,
                    ErrorPhase::StateWrite,
                );
                assert!(error.supplementary()[0].supplementary().is_empty());
            },
        )
        .await;
        case.faults.release();
        case.faults
            .state
            .block_transaction_begin
            .store(false, Ordering::SeqCst);
        case.faults.release_transaction_begin();
        case.faults.release_transaction_begin();
        case.faults
            .set_persistence_fault(PromptPersistenceFault::None);
        if !matches!(&observed, Ok(Ok(()))) {
            case.finish_observed(observed).await;
            continue;
        }
        if fault.fails_once() {
            case.shutdown().await;
        } else {
            case.revoke().await;
        }
    }
}

#[tokio::test]
async fn opencode_prompt_stream_pre_dispatch_cleanup_preserves_primary_and_bounds_retries() {
    for fault in [
        PromptPersistenceFault::PreDispatchCleanupOnce,
        PromptPersistenceFault::PreDispatchCleanup,
    ] {
        OpenCodeStreamCase::verify_pre_dispatch_cleanup(fault, false).await;
    }
}

#[tokio::test]
async fn opencode_prompt_stream_pre_dispatch_foreign_cleanup_preserves_primary_error() {
    OpenCodeStreamCase::verify_pre_dispatch_cleanup(
        PromptPersistenceFault::PreDispatchCleanup,
        true,
    )
    .await;
}

#[tokio::test]
async fn opencode_prompt_stream_idle_response_loss_repair_bounds_retry_admission() {
    for fault in [
        PromptPersistenceFault::IdleRepairOnce,
        PromptPersistenceFault::IdleRepair,
    ] {
        let case = OpenCodeStreamCase::start().await;
        case.probe
            .stats
            .malformed_stream_response
            .store(true, Ordering::SeqCst);
        case.probe.stats.reject_abort.store(true, Ordering::SeqCst);
        case.faults.set_persistence_fault(fault);
        let mut stream = case.stream().await;
        case.begin_text();
        case.delta(
            "evt_idle_repair_ready",
            OpenCodeStreamCase::ASSISTANT,
            "prt_stream_text",
            "text",
            "accepted",
        );
        OpenCodeStreamCase::next(&mut stream).await.unwrap();
        case.wait_for_admission().await;
        let started = tokio::time::Instant::now();
        case.release();
        assert!(matches!(
            (OpenCodeStreamCase::next(&mut stream).await)
                .as_ref()
                .map_err(SdkError::kind),
            Err(ErrorKind::Protocol)
        ));
        assert!(stream.recv().await.is_none());
        tokio::time::timeout(Duration::from_secs(3), async {
            while case
                .faults
                .state
                .persistence_attempts
                .load(Ordering::SeqCst)
                == 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("lost HTTP plus a rejected abort and native idle must reach repair");
        assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 1);
        assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 1);
        if fault.fails_once() {
            case.settled().await;
            assert_eq!(
                case.faults
                    .state
                    .persistence_attempts
                    .load(Ordering::SeqCst),
                1
            );
            case.shutdown().await;
            continue;
        }
        case.assert_persistence_retries_stopped(started).await;
        let retained = case
            .faults
            .prompt_snapshot(&case.conversation.id())
            .await
            .unwrap();
        assert_eq!(
            retained
                .record
                .value
                .pointer("/phase/phase")
                .and_then(serde_json::Value::as_str),
            Some("response_lost_after_admission")
        );
        assert_eq!(retained.record.value["waiter"], "lost");
        case.faults
            .set_persistence_fault(PromptPersistenceFault::None);
        case.revoke().await;
    }
}

#[tokio::test]
async fn opencode_reconnect_replaces_the_sole_event_stream_and_shutdown_joins_it() {
    let probe = OpenCodeEventProbe::start(1).await;
    let agent = AgentBuilder::create(
        probe.config(Duration::from_secs(1), Duration::from_secs(3)),
        OpenCodeEventProbe::storage().await,
    )
    .build()
    .await
    .expect("OpenCode Agent must build against the event probe");

    probe.wait_for(1, 3, 1).await;
    probe.disconnect_current();
    probe.wait_for(2, 4, 1).await;
    assert_eq!(
        probe.stats.maximum.load(Ordering::SeqCst),
        1,
        "OpenCode must drop a failed SSE response before opening its replacement"
    );

    probe.fail_history();
    probe.disconnect_current();
    probe.wait_for(3, 5, 1).await;
    tokio::time::timeout(Duration::from_secs(2), agent.shutdown())
        .await
        .expect("OpenCode stop must interrupt history retry/backoff")
        .expect("OpenCode Agent must shut down");
    probe.wait_for(3, 5, 0).await;
    probe.shutdown().await;
}

#[tokio::test]
async fn opencode_history_retries_reuse_one_stream_and_one_startup_deadline() {
    let probe = OpenCodeEventProbe::start(usize::MAX).await;
    let config = probe.config(Duration::from_secs(10), Duration::from_secs(4));
    let storage = OpenCodeEventProbe::storage().await;
    let started = tokio::time::Instant::now();
    let build = tokio::spawn(async move { AgentBuilder::create(config, storage).build().await });
    probe.wait_for(1, 2, 1).await;
    let error = tokio::time::timeout(Duration::from_secs(7), build)
        .await
        .expect("OpenCode startup must retain its original bounded deadline")
        .expect("OpenCode startup observer task must join")
        .map(|_| ())
        .expect_err("permanent history unavailability must prevent readiness");
    let elapsed = started.elapsed();
    assert!(
        matches!((error).kind(), ErrorKind::Timeout),
        "the construction episode must end at its lifecycle deadline: {error:?}"
    );
    assert!(
        elapsed < Duration::from_secs(6),
        "history retry/backoff must not receive a fresh nested timeout: elapsed={elapsed:?}"
    );
    probe.wait_for(1, 2, 0).await;
    assert_eq!(
        probe.stats.maximum.load(Ordering::SeqCst),
        1,
        "retryable history failures must retain their healthy current stream"
    );
    probe.shutdown().await;
}

#[tokio::test]
async fn opencode_startup_deadline_stops_a_stalled_event_connection() {
    let probe = OpenCodeEventProbe::start(0).await;
    probe.stall_event_headers();
    let config = probe.config(Duration::from_secs(10), Duration::from_secs(4));
    let storage = OpenCodeEventProbe::storage().await;
    let started = tokio::time::Instant::now();
    let build = tokio::spawn(async move { AgentBuilder::create(config, storage).build().await });
    probe.wait_for(1, 0, 1).await;
    let error = tokio::time::timeout(Duration::from_secs(7), build)
        .await
        .expect("stalled OpenCode event establishment must remain lifecycle-bounded")
        .expect("stalled OpenCode startup observer task must join")
        .map(|_| ())
        .expect_err("a stalled OpenCode event response cannot become ready");
    assert!(
        matches!((error).kind(), ErrorKind::Timeout),
        "stalled event establishment must end at the episode deadline: {error:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(6),
        "the request timeout must not replace the shorter startup episode deadline"
    );
    probe.wait_for(1, 0, 0).await;
    assert_eq!(probe.stats.maximum.load(Ordering::SeqCst), 1);
    probe.shutdown().await;
    assert_eq!(error.context().operation, Operation::Build);
    assert_eq!(error.context().phase, ErrorPhase::EventReady);
}

#[tokio::test]
async fn opencode_startup_persistent_commit_observation_is_aborted_and_joined() {
    let probe = OpenCodeEventProbe::start(0).await;
    let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
    let storage = faults.storage();
    let agent = AgentBuilder::create(
        probe.config(Duration::from_secs(10), Duration::from_secs(5)),
        storage.clone(),
    )
    .build()
    .await
    .expect("OpenCode Agent must build before the startup-observation reopen");
    let agent_id = agent.id();
    let workspace = agent
        .new_workspace(OpenCodeEventProbe::workspace_request())
        .await
        .expect("OpenCode startup-observation workspace must be created");
    let conversation = workspace
        .new_conversation(
            NewConversation::new(None, None, Vec::new())
                .expect("OpenCode startup-observation conversation request must be valid"),
        )
        .await
        .expect("OpenCode startup-observation conversation must be created");
    drop(conversation);
    drop(workspace);
    agent
        .shutdown()
        .await
        .expect("the seeded OpenCode Agent must shut down before reopen");

    probe.set_startup_history_event("evt_probe_startup_persistent_observation");
    let setup_attempts = faults.event_commit_attempts();
    let subscriptions = probe.stats.subscriptions.load(Ordering::SeqCst);
    faults.block_next_event_commit();
    faults.obscure_next_event_commit();
    faults.fail_event_commit_observations(usize::MAX);
    let config = probe.config(Duration::from_secs(10), Duration::from_secs(3));
    let mut build = tokio::spawn(async move {
        AgentBuilder::reopen(config, storage, agent_id)
            .build()
            .await
    });
    tokio::select! {
        () = faults.wait_until_event_commit_blocked() => {}
        result = &mut build => {
            panic!(
                "reopen returned before startup event mutation admission: {:?}",
                result.map(|result| result.map(|_| ()))
            );
        }
    }
    assert_eq!(faults.event_commit_attempts(), setup_attempts + 1);
    faults.release_event_commit();
    faults.wait_for_event_observation_failures(2).await;

    let error = tokio::time::timeout(Duration::from_secs(8), &mut build)
        .await
        .expect("startup failure cleanup must retain a final bounded authority")
        .expect("startup observer task must join")
        .map(|_| ())
        .expect_err("persistently unobservable startup progress cannot publish an Agent");
    assert!(
        matches!((error).kind(), ErrorKind::Timeout),
        "startup must preserve its deadline classification: {error:?}"
    );
    assert_eq!(
        faults.event_commit_attempts(),
        setup_attempts + 1,
        "ambiguous startup progress must never replay its admitted mutation"
    );
    probe
        .wait_for(
            subscriptions + 1,
            probe.stats.history.load(Ordering::SeqCst),
            0,
        )
        .await;
    assert_eq!(probe.stats.maximum.load(Ordering::SeqCst), 1);
    probe.shutdown().await;
}

#[tokio::test]
async fn opencode_construction_timeout_identifies_inventory_verification() {
    let probe = OpenCodeEventProbe::start(0).await;
    probe.block_history();
    let config = probe.config(Duration::from_secs(10), Duration::from_secs(2));
    let storage = OpenCodeEventProbe::storage().await;
    let mut build =
        tokio::spawn(async move { AgentBuilder::create(config, storage).build().await });
    let result = tokio::time::timeout(Duration::from_secs(5), &mut build).await;
    if result.is_err() {
        build.abort();
        let _ = build.await;
    }
    probe.release_history();
    let error = match result {
        Ok(Ok(Err(error))) => Some(error),
        Ok(Ok(Ok(agent))) => {
            let _ = agent.shutdown().await;
            None
        }
        _ => None,
    };
    let history_requests = probe.stats.history.load(Ordering::SeqCst);
    let subscriptions = probe.stats.subscriptions.load(Ordering::SeqCst);
    probe.shutdown().await;
    let error = error.expect("stalled inventory must prevent Agent publication");
    assert_eq!(history_requests, 1, "startup must reach inventory history");
    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert_eq!(error.context().operation, Operation::Build);
    assert_eq!(error.context().phase, ErrorPhase::InventoryVerification);
    assert_eq!(
        subscriptions, 0,
        "inventory expiry cannot start event delivery"
    );
}

#[test]
fn opencode_queued_client_setup_expiry_preserves_identity_for_reopen() {
    ClientSetupScenario::ObservedExpiry.run();
}

#[test]
fn opencode_queued_client_setup_dropped_caller_cleans_unclaimed_success() {
    ClientSetupScenario::DroppedSuccess.run();
}

#[test]
fn opencode_queued_client_setup_dropped_caller_retains_expired_owner() {
    ClientSetupScenario::DroppedExpiry.run();
}

#[tokio::test]
async fn opencode_construction_deadline_bounds_stalled_capability_headers_and_body() {
    let probe = OpenCodeEventProbe::start(0).await;

    probe.stall_next_capability_headers();
    let config = probe.config(Duration::from_secs(10), Duration::from_secs(4));
    let storage = OpenCodeEventProbe::storage().await;
    let started = tokio::time::Instant::now();
    let build = tokio::spawn(async move { AgentBuilder::create(config, storage).build().await });
    probe.wait_for_capability_requests(1).await;
    let error = tokio::time::timeout(Duration::from_secs(7), build)
        .await
        .expect("stalled capability headers must remain construction-bounded")
        .expect("capability-header build observer must join")
        .map(|_| ())
        .expect_err("stalled capability headers must prevent Agent publication");
    assert!(
        matches!((error).kind(), ErrorKind::Timeout),
        "stalled capability headers must retain Timeout: {error:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(6),
        "the request timeout must not replace the Agent construction deadline"
    );

    let headers_error = error;
    probe.stall_next_capability_body();
    let config = probe.config(Duration::from_secs(10), Duration::from_secs(4));
    let storage = OpenCodeEventProbe::storage().await;
    let started = tokio::time::Instant::now();
    let build = tokio::spawn(async move { AgentBuilder::create(config, storage).build().await });
    probe.wait_for_capability_requests(2).await;
    let error = tokio::time::timeout(Duration::from_secs(7), build)
        .await
        .expect("stalled capability body must remain construction-bounded")
        .expect("capability-body build observer must join")
        .map(|_| ())
        .expect_err("a stalled capability body must prevent Agent publication");
    assert!(
        matches!((error).kind(), ErrorKind::Timeout),
        "a stalled capability body must retain Timeout: {error:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(6),
        "response-body waiting must consume the original construction deadline"
    );
    assert_eq!(
        probe.stats.subscriptions.load(Ordering::SeqCst),
        0,
        "capability expiry must prevent event-owner publication"
    );
    assert_eq!(
        probe.stats.history.load(Ordering::SeqCst),
        0,
        "capability expiry must prevent history synchronization"
    );

    probe.shutdown().await;
    for error in [headers_error, error] {
        assert_eq!(error.context().operation, Operation::Build);
        assert_eq!(error.context().phase, ErrorPhase::ServerVerification);
    }
}

#[tokio::test]
async fn opencode_late_ambiguous_setup_commit_remains_reopenable_recovery_state() {
    let probe = OpenCodeEventProbe::start(0).await;
    let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
    let storage = faults.storage();
    faults.block_next_event_commit();
    faults.obscure_next_event_commit();

    let mut build = tokio::spawn({
        let config = probe.config(Duration::from_secs(2), Duration::from_secs(1));
        let storage = storage.clone();
        async move { AgentBuilder::create(config, storage).build().await }
    });
    faults.wait_until_event_commit_blocked().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(1_200), &mut build)
            .await
            .is_err(),
        "an admitted setup transaction must remain owned past construction expiry"
    );

    faults.release_event_commit();
    let error = tokio::time::timeout(Duration::from_secs(2), build)
        .await
        .expect("late setup settlement must remain observable")
        .expect("late setup build task must join")
        .map(|_| ())
        .expect_err("a late setup commit cannot publish an Agent");
    assert!(
        matches!((error).kind(), ErrorKind::Timeout),
        "a confirmed late setup commit must retain Timeout: {error:?}"
    );
    assert_eq!(faults.event_commit_attempts(), 1);
    assert_eq!(
        probe.stats.capability_requests.load(Ordering::SeqCst),
        0,
        "late durable setup cannot authorize capability verification"
    );
    assert_eq!(
        probe.stats.subscriptions.load(Ordering::SeqCst),
        0,
        "late durable setup cannot publish the event owner"
    );
    assert_eq!(probe.stats.history.load(Ordering::SeqCst), 0);

    let durable = storage
        .list_agents()
        .await
        .expect("late committed setup state must remain discoverable");
    assert_eq!(
        durable.len(),
        1,
        "setup must commit exactly one durable Agent"
    );
    let reopened = AgentBuilder::reopen(
        probe.config(Duration::from_secs(2), Duration::from_secs(3)),
        storage,
        durable[0].id().clone(),
    )
    .build()
    .await
    .expect("the complete late setup commit must reopen without repair");
    reopened
        .shutdown()
        .await
        .expect("the recovered Agent must shut down");
    probe.shutdown().await;
}

#[derive(Clone, Copy)]
enum BoundSessionCreationCase {
    ConflictOnce,
    ConflictDeadline,
    ChangedObligation,
    UncertainCommit,
    FailedRollback,
    ReopenConflict,
}

impl BoundSessionCreationCase {
    async fn verify(self) {
        let probe = OpenCodeEventProbe::start(0).await;
        let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
        let storage = faults.storage();
        let config = probe.config(Duration::from_secs(2), Duration::from_secs(1));
        let mut original = None;
        let mut recovered = None;
        let mut creations = JoinSet::new();
        let mut constructions = JoinSet::new();
        let mut restore = None;
        let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(30), async {
        constructions.spawn({
            let config = config.clone();
            let storage = storage.clone();
            async move { AgentBuilder::create(config, storage).build().await }
        });
        original = Some(constructions.join_next().await.unwrap().unwrap().unwrap());
        let agent_id = original.as_ref().unwrap().id();
        let workspace = original.as_ref().unwrap()
            .new_workspace(OpenCodeEventProbe::workspace_request())
            .await
            .unwrap();
        let workspace_id = workspace.id();
        *faults.state.bound_session_fault.lock().unwrap() = match self {
            Self::ConflictOnce => BoundSessionFault::Conflicts(1),
            Self::ConflictDeadline => BoundSessionFault::Conflicts(usize::MAX),
            Self::ChangedObligation => BoundSessionFault::None,
            Self::UncertainCommit | Self::ReopenConflict => BoundSessionFault::Unavailable,
            Self::FailedRollback => BoundSessionFault::StagingRollbackFailure,
        };
        if matches!(self, Self::ChangedObligation) {
            faults.block_next_bound_session_commit();
        }
        let started = tokio::time::Instant::now();
        creations.spawn(async move {
            let result = workspace
                .new_conversation(NewConversation::new(None, None, Vec::new()).unwrap())
                .await;
            (workspace, result)
        });
        let changed = if matches!(self, Self::ChangedObligation) {
            faults.wait_until_bound_session_commit_blocked().await;
            let records = faults
                .inner
                .list(StateListRequest::new(OPENCODE_SESSION_NAMESPACE).unwrap())
                .await
                .unwrap();
            let [original] = records.records.as_slice() else {
                panic!("exactly one Creating obligation must exist")
            };
            assert_eq!(original.record.value["state"]["phase"], "creating");
            let mut different = original.record.clone();
            let old = different.value["workspace_revision"].as_u64().unwrap();
            Arc::make_mut(&mut different.value)["workspace_revision"] = serde_json::json!(old + 1);
            restore = Some(original.record.clone());
            let mut competing = faults.inner.begin_transaction().await.unwrap();
            competing.put(different.clone()).await.unwrap();
            competing.commit().await.unwrap();
            faults.release_bound_session_commit();
            Some((original.record.clone(), different))
        } else {
            None
        };
        let (workspace, result) = tokio::time::timeout(Duration::from_secs(8), creations.join_next())
            .await
            .expect("the original creation deadline must bound safe binding retries")
            .expect("one creation remains owned")
            .expect("the owned creation must join");
        let conversation = match self {
            Self::ConflictOnce => {
                let conversation = result.expect(
                    "a proven binding conflict must recompute only the exact validated binding",
                );
                assert_eq!(
                    faults
                        .state
                        .bound_session_commit_attempts
                        .load(Ordering::SeqCst),
                    2
                );
                assert_eq!(
                    faults.state.bound_session_conflicts.load(Ordering::SeqCst),
                    1
                );
                Some(conversation)
            }
            Self::ConflictDeadline => {
                let error = result
                    .map(|_| ())
                    .expect_err("persistent conflict cannot publish a conversation");
                assert_eq!(error.kind(), ErrorKind::Timeout, "{error:?}");
                assert!(
                    error
                        .supplementary()
                        .iter()
                        .any(|error| error.kind() == ErrorKind::Conflict
                            && error.detail() == Some(&ErrorDetail::TransactionConflict)),
                    "last exact conflict remains diagnostic evidence: {error:?}"
                );
                assert!(
                    faults
                        .state
                        .bound_session_commit_attempts
                        .load(Ordering::SeqCst)
                        > 1
                );
                assert!(started.elapsed() < Duration::from_secs(5));
                None
            }
            Self::ChangedObligation => {
                let error = result
                    .map(|_| ())
                    .expect_err("changed creation authority cannot be overwritten");
                assert_eq!(error.kind(), ErrorKind::InvalidState, "{error:?}");
                let (original, different) = changed.unwrap();
                let actual = faults
                    .inner
                    .load(different.key.clone())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(actual.record, different);
                assert_eq!(
                    faults
                        .state
                        .bound_session_commit_attempts
                        .load(Ordering::SeqCst),
                    1
                );
                // Restore only this deliberate external invalidation so normal
                // owner shutdown and same-ID recovery remain independently testable.
                let mut transaction = faults.inner.begin_transaction().await.unwrap();
                transaction.put(original).await.unwrap();
                transaction.commit().await.unwrap();
                restore = None;
                None
            }
            Self::UncertainCommit | Self::ReopenConflict => {
                let error = result
                    .map(|_| ())
                    .expect_err("uncertain consuming commit permits observation only");
                assert_eq!(error.kind(), ErrorKind::Integration, "{error:?}");
                assert_eq!(
                    faults
                        .state
                        .bound_session_commit_attempts
                        .load(Ordering::SeqCst),
                    1
                );
                None
            }
            Self::FailedRollback => {
                assert_eq!(faults.state.bound_session_writes.load(Ordering::SeqCst), 1,
                    "failed rollback must veto every further binding staging attempt");
                let error = result
                    .map(|_| ())
                    .expect_err("failed rollback vetoes fresh binding attempts");
                assert_eq!(error.kind(), ErrorKind::Conflict, "{error:?}");
                assert_eq!(error.detail(), Some(&ErrorDetail::TransactionConflict));
                assert!(
                    error
                        .supplementary()
                        .iter()
                        .any(|error| error.context().phase == ErrorPhase::Cleanup),
                    "{error:?}"
                );
                assert_eq!(
                    faults
                        .state
                        .bound_session_commit_attempts
                        .load(Ordering::SeqCst),
                    0
                );
                None
            }
        };
        assert_eq!(
            probe.stats.next_session.load(Ordering::SeqCst),
            1,
            "binding retry never repeats native session creation"
        );
        assert_eq!(
            probe.stats.messages.load(Ordering::SeqCst),
            0,
            "binding never dispatches a native prompt"
        );
        assert_eq!(probe.stats.deletes.load(Ordering::SeqCst), 0);
        *faults.state.bound_session_fault.lock().unwrap() = BoundSessionFault::None;
        let conversation_id = conversation.as_ref().map(ConversationBackend::id);
        drop(conversation);
        drop(workspace);
        original.take().unwrap()
            .shutdown()
            .await
            .expect("original creation owner must settle before reconstruction");
        if matches!(self, Self::ReopenConflict) {
            let before = faults
                .state
                .bound_session_commit_attempts
                .load(Ordering::SeqCst);
            *faults.state.bound_session_fault.lock().unwrap() = BoundSessionFault::Conflicts(1);
            constructions.spawn({
                let config = config.clone();
                let storage = storage.clone();
                let id = agent_id.clone();
                async move { AgentBuilder::reopen(config, storage, id).build().await }
            });
            let error = match constructions.join_next().await.unwrap().unwrap() {
                Ok(agent) => {
                    recovered = Some(agent);
                    panic!("Reopen binding keeps its single-attempt conflict behavior");
                }
                Err(error) => error,
            };
            assert_eq!(error.kind(), ErrorKind::Conflict, "{error:?}");
            assert_eq!(error.detail(), Some(&ErrorDetail::TransactionConflict));
            assert_eq!(
                faults
                    .state
                    .bound_session_commit_attempts
                    .load(Ordering::SeqCst),
                before + 1
            );
            *faults.state.bound_session_fault.lock().unwrap() = BoundSessionFault::None;
        }
        constructions.spawn({
            let config = config.clone();
            let storage = storage.clone();
            let id = agent_id.clone();
            async move { AgentBuilder::reopen(config, storage, id).build().await }
        });
        recovered = Some(constructions.join_next().await.unwrap().unwrap().expect(
            "one same-ID reconstruction must bind retained Creating authority without native replay",
        ));
        assert_eq!(recovered.as_ref().unwrap().id(), agent_id);
        let workspace = recovered.as_ref().unwrap().get_workspace(workspace_id).await.unwrap();
        let inventory = workspace.list_conversations().await.unwrap();
        assert_eq!(inventory.len(), 1);
        if let Some(id) = conversation_id {
            assert_eq!(inventory[0].id(), &id);
        }
        let conversation = workspace
            .get_conversation(inventory[0].id().clone())
            .await
            .unwrap();
        assert_eq!(probe.stats.next_session.load(Ordering::SeqCst), 1);
        assert_eq!(probe.stats.messages.load(Ordering::SeqCst), 0);
        assert_eq!(probe.stats.deletes.load(Ordering::SeqCst), 0);
        drop(conversation);
        drop(workspace);
        }).await;
        // Release only this case's fixture boundaries before joining every
        // admitted construction. An outer supervised budget may classify a
        // stuck owner as incomplete; dropping its observer never detaches it.
        *faults.state.bound_session_fault.lock().unwrap() = BoundSessionFault::None;
        faults.release_bound_session_commit();
        faults.release_bound_session_commit_observation();
        let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
        while let Some(result) = creations.join_next().await {
            cleanup
                .settle(async {
                    let (workspace, conversation) =
                        result.map_err(|error| format!("creation owner failed: {error}"))?;
                    drop(conversation);
                    drop(workspace);
                    Ok::<(), String>(())
                })
                .await;
        }
        if let Some(record) = restore {
            cleanup
                .settle(async {
                    let mut transaction = faults.inner.begin_transaction().await?;
                    transaction.put(record).await?;
                    transaction.commit().await
                })
                .await;
        }
        if let Some(agent) = original.take() {
            cleanup.settle(agent.shutdown()).await;
        }
        if let Some(agent) = recovered.take() {
            cleanup.settle(agent.shutdown()).await;
        }
        while let Some(result) = constructions.join_next().await {
            cleanup
                .settle(async {
                    match result.map_err(|error| format!("construction owner failed: {error}"))? {
                        Ok(agent) => agent
                            .shutdown()
                            .await
                            .map_err(|error| format!("construction cleanup failed: {error:?}")),
                        Err(error) => Err(format!("unobserved construction failed: {error:?}")),
                    }
                })
                .await;
        }
        cleanup
            .settle(async {
                probe.shutdown().await;
                Ok::<(), SdkError>(())
            })
            .await;
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn opencode_bound_creation_real_conflict_retries_only_binding() {
    BoundSessionCreationCase::ConflictOnce.verify().await;
}

#[tokio::test]
async fn opencode_bound_creation_conflicts_retain_original_deadline_and_failure() {
    BoundSessionCreationCase::ConflictDeadline.verify().await;
}

#[tokio::test]
async fn opencode_bound_creation_changed_obligation_is_not_overwritten() {
    BoundSessionCreationCase::ChangedObligation.verify().await;
}

#[tokio::test]
async fn opencode_bound_creation_uncertain_commit_is_not_replayed() {
    BoundSessionCreationCase::UncertainCommit.verify().await;
}

#[tokio::test]
async fn opencode_bound_creation_failed_rollback_vetoes_retry() {
    BoundSessionCreationCase::FailedRollback.verify().await;
}

#[tokio::test]
async fn opencode_bound_creation_reopen_conflict_remains_single_attempt() {
    BoundSessionCreationCase::ReopenConflict.verify().await;
}

#[tokio::test]
async fn opencode_late_bound_conversation_commit_remains_accepted_and_reopenable() {
    let probe = OpenCodeEventProbe::start(0).await;
    let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
    let storage = faults.storage();
    let config = probe.config(Duration::from_secs(1), Duration::from_secs(3));
    let agent = AgentBuilder::create(config.clone(), storage.clone())
        .build()
        .await
        .expect("OpenCode Agent must build before late conversation completion");
    let agent_id = agent.id();
    let workspace = agent
        .new_workspace(OpenCodeEventProbe::workspace_request())
        .await
        .expect("OpenCode workspace must build before late conversation completion");
    let workspace_id = workspace.id();

    faults.block_next_bound_session_commit();
    faults.obscure_next_bound_session_commit();
    let mut creation = tokio::spawn(async move {
        let result = workspace
            .new_conversation(
                NewConversation::new(None, None, Vec::new())
                    .expect("late completion conversation request must be valid"),
            )
            .await;
        (workspace, result)
    });
    faults.wait_until_bound_session_commit_blocked().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(3_200), &mut creation)
            .await
            .is_err(),
        "an admitted conversation completion must remain owned past its lifecycle deadline"
    );
    faults.release_bound_session_commit();

    let (workspace, conversation) = tokio::time::timeout(Duration::from_secs(4), creation)
        .await
        .expect("late conversation completion must remain observable")
        .expect("late conversation completion task must join");
    let conversation =
        conversation.expect("a confirmed bound conversation remains accepted after its deadline");
    let conversation_id = conversation.id();
    assert_eq!(faults.bound_session_observation_failures(), 0);
    assert_eq!(
        probe.stats.deletes.load(Ordering::SeqCst),
        0,
        "confirmed conversation binding must not enter native cleanup"
    );
    assert!(
        workspace
            .list_conversations()
            .await
            .expect("durable conversation inventory must remain readable")
            .iter()
            .any(|info| info.id() == &conversation_id),
        "the late confirmed binding must remain in durable inventory"
    );

    drop(conversation);
    drop(workspace);
    agent
        .shutdown()
        .await
        .expect("the original Agent must shut down without deleting accepted state");
    let reopened = AgentBuilder::reopen(config, storage, agent_id)
        .build()
        .await
        .expect("the late confirmed conversation must remain reopenable");
    let workspace = reopened
        .get_workspace(workspace_id)
        .await
        .expect("the durable workspace must reopen");
    let conversation = workspace
        .get_conversation(conversation_id)
        .await
        .expect("the late confirmed conversation must reopen by typed identity");
    assert_eq!(
        probe.stats.deletes.load(Ordering::SeqCst),
        0,
        "reopen must observe the original native session without cleanup deletion"
    );

    drop(conversation);
    drop(workspace);
    reopened
        .shutdown()
        .await
        .expect("the reopened Agent must shut down");
    probe.shutdown().await;
}

#[tokio::test]
async fn opencode_ambiguous_bound_conversation_retries_exact_observation() {
    let probe = OpenCodeEventProbe::start(0).await;
    let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
    let storage = faults.storage();
    let config = probe.config(Duration::from_secs(1), Duration::from_secs(3));
    let agent = AgentBuilder::create(config.clone(), storage.clone())
        .build()
        .await
        .expect("OpenCode Agent must build before ambiguous conversation completion");
    let agent_id = agent.id();
    let workspace = agent
        .new_workspace(OpenCodeEventProbe::workspace_request())
        .await
        .expect("OpenCode workspace must build before ambiguous conversation completion");
    let workspace_id = workspace.id();

    faults.obscure_next_bound_session_commit();
    faults.fail_next_bound_session_commit_observation();
    let conversation = workspace
        .new_conversation(
            NewConversation::new(None, None, Vec::new())
                .expect("ambiguous completion conversation request must be valid"),
        )
        .await
        .expect("the applied binding must survive one failed exact observation");
    let conversation_id = conversation.id();
    assert_eq!(faults.bound_session_observation_failures(), 1);
    assert_eq!(
        probe.stats.deletes.load(Ordering::SeqCst),
        0,
        "an ambiguously confirmed binding must not enter native cleanup"
    );

    drop(conversation);
    drop(workspace);
    agent
        .shutdown()
        .await
        .expect("the original Agent must shut down after ambiguous completion");
    let reopened = AgentBuilder::reopen(config, storage, agent_id)
        .build()
        .await
        .expect("the ambiguously confirmed conversation must remain reopenable");
    let workspace = reopened
        .get_workspace(workspace_id)
        .await
        .expect("the durable workspace must reopen");
    let conversation = workspace
        .get_conversation(conversation_id)
        .await
        .expect("the ambiguously confirmed conversation must reopen");
    assert_eq!(probe.stats.deletes.load(Ordering::SeqCst), 0);

    drop(conversation);
    drop(workspace);
    reopened
        .shutdown()
        .await
        .expect("the reopened Agent must shut down");
    probe.shutdown().await;
}

#[tokio::test]
async fn opencode_bound_conversation_observation_expiry_remains_recovery_owned() {
    let probe = OpenCodeEventProbe::start(0).await;
    let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
    let storage = faults.storage();
    let config = probe.config(Duration::from_secs(10), Duration::from_secs(3));
    let agent = AgentBuilder::create(config.clone(), storage.clone())
        .build()
        .await
        .expect("OpenCode Agent must build before recovery-owned completion");
    let agent_id = agent.id();
    let workspace = agent
        .new_workspace(OpenCodeEventProbe::workspace_request())
        .await
        .expect("OpenCode workspace must build before recovery-owned completion");
    let workspace_id = workspace.id();

    faults.obscure_next_bound_session_commit();
    faults.fail_bound_session_commit_observation_until_released();
    let error = match workspace
        .new_conversation(
            NewConversation::new(None, None, Vec::new())
                .expect("recovery-owned conversation request must be valid"),
        )
        .await
    {
        Ok(_) => panic!("unobservable committed binding must remain recovery-owned"),
        Err(error) => error,
    };
    assert!(
        matches!((error).kind(), ErrorKind::Busy),
        "observation expiry must return typed recovery ownership: {error:?}"
    );
    assert!(
        faults.bound_session_observation_failures() > 1,
        "completion must retry exact observation until its lifecycle deadline"
    );
    assert_eq!(
        probe.stats.deletes.load(Ordering::SeqCst),
        0,
        "recovery-required completion must not delete the ambiguously committed native session"
    );

    faults.release_bound_session_commit_observation();
    let durable = workspace
        .list_conversations()
        .await
        .expect("the recovery-owned conversation inventory must become readable");
    assert_eq!(durable.len(), 1);
    let conversation_id = durable[0].id().clone();
    let conversation = workspace
        .get_conversation(conversation_id.clone())
        .await
        .expect("the fully committed conversation must remain recoverable");

    drop(conversation);
    drop(workspace);
    agent
        .shutdown()
        .await
        .expect("the original Agent must shut down after recovery observation");
    let reopened = AgentBuilder::reopen(config, storage, agent_id)
        .build()
        .await
        .expect("the recovery-owned conversation must remain reopenable");
    let workspace = reopened
        .get_workspace(workspace_id)
        .await
        .expect("the durable workspace must reopen");
    let conversation = workspace
        .get_conversation(conversation_id)
        .await
        .expect("the recovery-owned conversation must reopen by typed identity");
    assert_eq!(probe.stats.deletes.load(Ordering::SeqCst), 0);

    drop(conversation);
    drop(workspace);
    reopened
        .shutdown()
        .await
        .expect("the reopened Agent must shut down");
    probe.shutdown().await;
}

#[tokio::test]
async fn opencode_shutdown_stops_a_stalled_reconnect_connection() {
    let probe = OpenCodeEventProbe::start(0).await;
    let agent = AgentBuilder::create(
        probe.config(Duration::from_secs(5), Duration::from_secs(5)),
        OpenCodeEventProbe::storage().await,
    )
    .build()
    .await
    .expect("OpenCode Agent must build before the stalled reconnect");
    let history = probe.stats.history.load(Ordering::SeqCst);

    probe.stall_event_headers();
    probe.disconnect_current();
    probe.wait_for(2, history, 1).await;
    tokio::time::timeout(Duration::from_secs(3), agent.shutdown())
        .await
        .expect("OpenCode shutdown must interrupt stalled reconnect establishment")
        .expect("OpenCode Agent must shut down during stalled reconnect establishment");
    probe.wait_for(2, history, 0).await;
    assert_eq!(
        probe.stats.maximum.load(Ordering::SeqCst),
        1,
        "stalled reconnect establishment must not overlap event subscriptions"
    );
    probe.shutdown().await;
}

#[tokio::test]
async fn opencode_conversation_intent_retries_only_definite_transaction_conflicts() {
    for phase in [
        ConversationIntentPhase::Begin,
        ConversationIntentPhase::WorkspaceLoad,
        ConversationIntentPhase::RoutingLoad,
        ConversationIntentPhase::ConversationLoad,
        ConversationIntentPhase::BindingLoad,
        ConversationIntentPhase::ConversationPut,
        ConversationIntentPhase::BindingPut,
        ConversationIntentPhase::Commit,
    ] {
        ConversationIntentFault::new(phase, PromptCommitFault::Conflict, 2)
            .exercise()
            .await;
    }
}

#[tokio::test]
async fn opencode_conversation_intent_preserves_non_conflict_and_uncertain_failures() {
    for (phase, fault) in [
        (
            ConversationIntentPhase::Begin,
            PromptCommitFault::Unavailable,
        ),
        (
            ConversationIntentPhase::WorkspaceLoad,
            PromptCommitFault::Unavailable,
        ),
        (
            ConversationIntentPhase::RoutingLoad,
            PromptCommitFault::Unavailable,
        ),
        (
            ConversationIntentPhase::ConversationPut,
            PromptCommitFault::Unavailable,
        ),
        (
            ConversationIntentPhase::BindingPut,
            PromptCommitFault::Unavailable,
        ),
        (
            ConversationIntentPhase::Commit,
            PromptCommitFault::Unavailable,
        ),
        (
            ConversationIntentPhase::Commit,
            PromptCommitFault::UnavailableAfterCommit,
        ),
    ] {
        ConversationIntentFault::new(phase, fault, 2)
            .exercise()
            .await;
    }
}

#[tokio::test]
async fn opencode_conversation_intent_conflicts_share_one_deadline() {
    ConversationIntentFault::new(
        ConversationIntentPhase::BindingPut,
        PromptCommitFault::Conflict,
        usize::MAX,
    )
    .exercise()
    .await;
}

#[tokio::test]
async fn opencode_conversation_intent_rollback_failure_stops_retry_and_preserves_primary() {
    for fault in [PromptCommitFault::Conflict, PromptCommitFault::Unavailable] {
        let mut intent =
            ConversationIntentFault::new(ConversationIntentPhase::BindingPut, fault, 2);
        intent.fail_rollback = true;
        intent.exercise().await;
    }
}

#[tokio::test]
async fn opencode_conversation_intent_late_commit_discards_before_reopen_without_native_create() {
    let mut intent =
        ConversationIntentFault::new(ConversationIntentPhase::Commit, PromptCommitFault::None, 0);
    intent.late_commit = true;
    intent.exercise().await;
}

#[tokio::test]
async fn opencode_conversation_intent_late_discard_failure_preserves_timeout() {
    let mut intent =
        ConversationIntentFault::new(ConversationIntentPhase::Commit, PromptCommitFault::None, 0);
    intent.late_commit = true;
    intent.fail_discard = true;
    intent.exercise().await;
}

#[tokio::test]
async fn opencode_conversation_creation_rejection_preserves_discard_failure() {
    for fail_discard in [false, true] {
        let mut intent = ConversationIntentFault::new(
            ConversationIntentPhase::Commit,
            PromptCommitFault::None,
            0,
        );
        intent.reject_create = true;
        intent.fail_discard = fail_discard;
        intent.exercise().await;
    }
}

#[tokio::test]
async fn opencode_conversation_ingestion_rejection_preserves_discard_failure() {
    for fail_discard in [false, true] {
        let mut intent = ConversationIntentFault::new(
            ConversationIntentPhase::Commit,
            PromptCommitFault::None,
            0,
        );
        intent.reject_ingest = true;
        intent.fail_discard = fail_discard;
        intent.exercise().await;
    }
}

#[tokio::test]
async fn opencode_conversation_creation_rejection_preserves_forget_read_failure() {
    let mut intent =
        ConversationIntentFault::new(ConversationIntentPhase::Commit, PromptCommitFault::None, 0);
    intent.reject_create = true;
    intent.fail_forget_read = true;
    intent.exercise().await;
}

#[tokio::test]
async fn opencode_conversation_ingestion_rejection_preserves_forget_read_failure() {
    let mut intent =
        ConversationIntentFault::new(ConversationIntentPhase::Commit, PromptCommitFault::None, 0);
    intent.reject_ingest = true;
    intent.fail_forget_read = true;
    intent.exercise().await;
}

#[tokio::test]
async fn opencode_conversation_intent_stale_workspace_is_not_retryable() {
    let mut intent = ConversationIntentFault::new(
        ConversationIntentPhase::WorkspaceLoad,
        PromptCommitFault::Conflict,
        0,
    );
    intent.stale_workspace = true;
    intent.exercise().await;
}

#[tokio::test]
async fn opencode_workspace_creation_retries_only_definite_transaction_conflicts() {
    for (phase, fault, remaining) in [
        (WorkspaceCreatePhase::Begin, PromptCommitFault::Conflict, 2),
        (WorkspaceCreatePhase::Load, PromptCommitFault::Conflict, 2),
        (WorkspaceCreatePhase::Put, PromptCommitFault::Conflict, 2),
        (WorkspaceCreatePhase::Commit, PromptCommitFault::Conflict, 2),
        (
            WorkspaceCreatePhase::Load,
            PromptCommitFault::Conflict,
            usize::MAX,
        ),
        (WorkspaceCreatePhase::Put, PromptCommitFault::Unavailable, 2),
        (
            WorkspaceCreatePhase::Commit,
            PromptCommitFault::UnavailableAfterCommit,
            2,
        ),
    ] {
        let probe = OpenCodeEventProbe::start(0).await;
        let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
        let agent = AgentBuilder::create(
            probe.config(Duration::from_secs(10), Duration::from_secs(5)),
            faults.storage(),
        )
        .build()
        .await
        .expect("workspace creation Agent must build before faults are armed");
        faults
            .state
            .workspace_create_fault
            .store(fault as u8, Ordering::SeqCst);
        faults
            .state
            .workspace_create_remaining
            .store(remaining, Ordering::SeqCst);
        faults
            .state
            .workspace_create_phase
            .store(phase as u8, Ordering::SeqCst);
        let sessions = probe.stats.next_session.load(Ordering::SeqCst);
        let result = tokio::time::timeout(
            Duration::from_secs(7),
            agent.new_workspace(OpenCodeEventProbe::workspace_request()),
        )
        .await
        .expect("workspace conflicts must remain within the original lifecycle deadline");
        assert_eq!(
            probe.stats.next_session.load(Ordering::SeqCst),
            sessions,
            "workspace creation and its retries must not create native sessions"
        );
        let created = if remaining == usize::MAX {
            assert!(matches!(
                (result).as_ref().map_err(SdkError::kind),
                Err(ErrorKind::Timeout)
            ));
            None
        } else if fault != PromptCommitFault::Conflict {
            assert!(matches!(
                (result).as_ref().map_err(SdkError::kind),
                Err(ErrorKind::Integration)
            ));
            None
        } else {
            Some(result.expect("definite workspace conflicts must retry the same creation"))
        };
        let injections = faults
            .state
            .workspace_create_injections
            .load(Ordering::SeqCst);
        let loads = faults.state.workspace_create_loads.load(Ordering::SeqCst);
        if remaining == usize::MAX {
            assert!(
                injections > 1,
                "persistent conflicts must actually be retried"
            );
            assert_eq!(loads, injections);
        } else if fault == PromptCommitFault::Conflict {
            assert_eq!(
                injections, 2,
                "both selected-phase conflicts must be consumed"
            );
            assert_eq!(
                faults
                    .state
                    .workspace_create_remaining
                    .load(Ordering::SeqCst),
                0
            );
            assert_eq!(
                loads,
                if phase == WorkspaceCreatePhase::Begin {
                    1
                } else {
                    3
                }
            );
        } else {
            assert_eq!(injections, 1, "non-conflict failures must not be retried");
            assert_eq!(
                loads, 1,
                "non-conflict failures must not start another creation"
            );
            assert_eq!(
                faults
                    .state
                    .workspace_create_remaining
                    .load(Ordering::SeqCst),
                1
            );
        }
        let inventory = agent
            .list_workspaces()
            .await
            .expect("workspace inventory must remain readable");
        let committed = created.is_some() || fault == PromptCommitFault::UnavailableAfterCommit;
        assert_eq!(inventory.len(), usize::from(committed));
        let key = faults
            .state
            .workspace_create_key
            .lock()
            .unwrap()
            .clone()
            .unwrap();
        let stored = faults
            .inner
            .load(key)
            .await
            .expect("exact attempted workspace key must remain readable");
        if committed {
            let stored = stored.expect("the one committed workspace must remain durable");
            assert_eq!(
                stored.revision.get(),
                1,
                "retries must not duplicate a committed write"
            );
            assert_eq!(
                Some(stored.record),
                *faults.state.workspace_create_record.lock().unwrap()
            );
            assert_eq!(inventory[0].revision(), 1);
            if let Some(workspace) = created {
                assert_eq!(workspace.id(), inventory[0].id().clone());
                drop(workspace);
            }
        } else {
            assert!(
                stored.is_none(),
                "failed local transactions must not publish a workspace"
            );
        }
        agent
            .shutdown()
            .await
            .expect("workspace creation Agent must settle all tasks");
        probe.wait_for(1, 0, 0).await;
        probe.shutdown().await;
    }
}

#[tokio::test]
async fn opencode_interaction_settlement_retries_only_local_transaction_conflicts() {
    for (load_conflicts, put_fault, commit_conflict, event_wins) in [
        (1, PromptCommitFault::None, false, false),
        (0, PromptCommitFault::Conflict, false, false),
        (0, PromptCommitFault::None, true, false),
        (0, PromptCommitFault::Unavailable, false, false),
        (usize::MAX, PromptCommitFault::None, false, false),
        (1, PromptCommitFault::None, false, true),
    ] {
        let probe = OpenCodeEventProbe::start(0).await;
        probe.request_question_on_prompt();
        let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
        let agent = AgentBuilder::create(
            probe.config(Duration::from_secs(10), Duration::from_secs(5)),
            faults.storage(),
        )
        .build()
        .await
        .expect("interaction settlement Agent must build");
        let workspace = agent
            .new_workspace(OpenCodeEventProbe::workspace_request())
            .await
            .expect("interaction settlement workspace must open");
        let conversation = workspace
            .new_conversation(NewConversation::new(None, None, Vec::new()).unwrap())
            .await
            .expect("interaction settlement conversation must open");
        let mut receiver = conversation.notification_stream().unwrap();
        let mut prompt = Box::pin(
            conversation.prompt(PromptRequest::new("ask the settlement probe question").unwrap()),
        );
        let interaction = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                tokio::select! {
                    notification = receiver.recv() => {
                        let notification = notification.unwrap().unwrap();
                        if matches!(notification.kind(), ConversationNotificationKind::Interaction { .. }) {
                            break notification;
                        }
                    }
                    result = &mut prompt => panic!("prompt ended before interaction: {result:?}"),
                }
            }
        })
        .await
        .expect("interaction admission must remain bounded");
        drop(prompt);
        faults
            .state
            .continuation_transaction_conflicts
            .store(load_conflicts, Ordering::SeqCst);
        faults
            .state
            .continuation_put_fault
            .store(put_fault as u8, Ordering::SeqCst);
        faults
            .state
            .continuation_commit_conflict
            .store(commit_conflict, Ordering::SeqCst);
        let mut resolution = Box::pin(conversation.resolve_notification(
            interaction.id().clone(),
            NotificationResolution::Submit {
                content: serde_json::json!({"answers": [["Yes"]]}),
            },
        ));
        if event_wins {
            tokio::select! {
                biased;
                _ = faults.wait_until_blocked() => {}
                result = &mut resolution => panic!("settlement did not reach its conflict: {result:?}"),
            }
            probe.events.send(serde_json::json!({
                "directory": env!("CARGO_MANIFEST_DIR"),
                "payload": {
                    "id": "evt_probe_replied",
                    "type": "question.replied",
                    "properties": {"sessionID": "ses_probe", "requestID": "que_probe", "answers": [["Yes"]]}
                }
            }).to_string()).expect("native reply event subscriber must remain owned");
            let key = StateKey::new(
                OPENCODE_CONTINUATION_NAMESPACE,
                format!("{}:que_probe", conversation.id().as_str()),
            )
            .unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    if faults
                        .inner
                        .load(key.clone())
                        .await
                        .unwrap()
                        .is_some_and(|record| record.record.value["state"] == "accepted")
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("native event acceptance must commit before caller settlement resumes");
        }
        let result = tokio::time::timeout(Duration::from_secs(7), &mut resolution)
            .await
            .expect("settlement conflicts must remain within the existing lifecycle deadline");
        let continuation_state = faults
            .inner
            .load(
                StateKey::new(
                    OPENCODE_CONTINUATION_NAMESPACE,
                    format!("{}:que_probe", conversation.id().as_str()),
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .map(|record| record.record.value["state"].clone());
        let notification_state = faults
            .inner
            .load(
                StateKey::new(
                    "agent_sdk_conversation_notifications_v1",
                    conversation.id().as_str(),
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .and_then(|record| {
                record.record.value["pending"].as_array().and_then(|items| {
                    items
                        .iter()
                        .find(|item| item["id"].as_str() == Some(interaction.id().as_str()))
                        .map(|item| item["state"].clone())
                })
            });
        eprintln!(
            "settlement row load_conflicts={load_conflicts} put_fault={} commit_conflict={commit_conflict} event_wins={event_wins}: result={result:?}, continuation={continuation_state:?}, notification={notification_state:?}",
            put_fault as u8
        );
        if load_conflicts == usize::MAX {
            assert!(
                matches!(
                    (result).as_ref().map_err(SdkError::kind),
                    Err(ErrorKind::Timeout)
                ),
                "{result:?}"
            );
        } else if matches!(put_fault, PromptCommitFault::Unavailable) {
            assert!(
                matches!(
                    (result).as_ref().map_err(SdkError::kind),
                    Err(ErrorKind::Integration)
                ),
                "{result:?}"
            );
        } else {
            result.expect("definite transaction conflicts must reconcile accepted interaction");
            conversation
                .resolve_notification(
                    interaction.id().clone(),
                    NotificationResolution::Submit {
                        content: serde_json::json!({"answers": [["Yes"]]}),
                    },
                )
                .await
                .expect_err("a durably accepted interaction must not be resubmitted");
        }
        let requires_native_reconciliation =
            load_conflicts == usize::MAX || matches!(put_fault, PromptCommitFault::Unavailable);
        if requires_native_reconciliation {
            assert_eq!(continuation_state, Some(serde_json::json!("pending")));
            let notification_state = notification_state
                .as_ref()
                .expect("the selected unresolved interaction retains its actual notification");
            assert_eq!(notification_state["state"], "resolving");
            assert_eq!(notification_state["detail"]["acceptance"], "unconfirmed");
        }
        assert_eq!(
            probe.stats.question_replies.load(Ordering::SeqCst),
            1,
            "storage settlement must never replay the accepted native reply"
        );
        if load_conflicts != usize::MAX {
            assert_eq!(
                faults
                    .state
                    .continuation_transaction_conflicts
                    .load(Ordering::SeqCst),
                0
            );
        }
        assert_eq!(
            faults.state.continuation_put_fault.load(Ordering::SeqCst),
            PromptCommitFault::None as u8
        );
        assert!(
            !faults
                .state
                .continuation_commit_conflict
                .load(Ordering::SeqCst)
        );
        drop(resolution);
        faults
            .state
            .continuation_transaction_conflicts
            .store(0, Ordering::SeqCst);
        if requires_native_reconciliation {
            // The accepted native answer outlives failed local settlement.
            // Reconcile its exact event before asking Abort to drain the prompt;
            // cancellation has no authority to relabel the Resolving owner.
            probe.events.send(serde_json::json!({
                "directory": env!("CARGO_MANIFEST_DIR"),
                "payload": {
                    "id": "evt_probe_replied_after_settlement_failure",
                    "type": "question.replied",
                    "properties": {"sessionID": "ses_probe", "requestID": "que_probe", "answers": [["Yes"]]}
                }
            }).to_string()).expect("the accepted answer's event subscriber remains owned");
            let key = StateKey::new(
                OPENCODE_CONTINUATION_NAMESPACE,
                format!("{}:que_probe", conversation.id().as_str()),
            )
            .unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    if faults
                        .inner
                        .load(key.clone())
                        .await
                        .unwrap()
                        .is_some_and(|record| record.record.value["state"] == "accepted")
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the exact late native acceptance must commit before teardown");
        }
        assert_eq!(probe.stats.question_replies.load(Ordering::SeqCst), 1);
        assert_eq!(probe.stats.question_rejections.load(Ordering::SeqCst), 0);
        conversation
            .abort()
            .await
            .expect("the admitted generation must drain after settlement");
        drop(receiver);
        drop(conversation);
        drop(workspace);
        agent
            .shutdown()
            .await
            .expect("settlement Agent must shut down");
        probe.shutdown().await;
    }
}

#[tokio::test]
async fn opencode_caller_settlement_retains_commit_and_receipt_failures_without_replay() {
    let case = OpenCodeStreamCase::start().await;
    let stats = Arc::clone(&case.probe.stats);
    case.probe.request_question_on_prompt();
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(25),
        async {
            let mut receiver = case.conversation.notification_stream().unwrap();
            let mut output = case.stream().await;
            let interaction = loop {
                tokio::select! {
                    item = receiver.recv() => {
                        let item = item.unwrap().unwrap();
                        if matches!(item.kind(), ConversationNotificationKind::Interaction { .. }) {
                            break item;
                        }
                    }
                    item = output.recv() => panic!("question must precede terminal output: {item:?}"),
                }
            };
            case.wait_for_admission().await;
            let key = StateKey::new(
                OPENCODE_CONTINUATION_NAMESPACE,
                format!("{}:que_probe", case.conversation.id().as_str()),
            ).unwrap();
            let pending = case.faults.inner.load(key.clone()).await.unwrap().unwrap();
            assert_eq!(pending.record.value["state"], "pending");
            assert_eq!(pending.record.value["notification_id"], interaction.id().as_str());

            // The peer acknowledges this answer without emitting a replied event.
            // Lose the real settlement commit receipt, then fail its exact readback.
            case.faults.obscure_next_continuation_reconciliation_commit();
            case.faults.fail_next_post_commit_continuation_reconciliation();
            let error = tokio::time::timeout(
                Duration::from_secs(7),
                case.conversation.resolve_notification(
                    interaction.id().clone(),
                    NotificationResolution::Submit {
                        content: serde_json::json!({"answers": [["Yes"]]}),
                    },
                ),
            ).await.expect("caller settlement must retain its lifecycle bound")
                .expect_err("a failed receipt read cannot confirm the observed settlement");
            assert_eq!(case.faults.ambiguous_continuation_reconciliations(), 1);
            assert_eq!(case.faults.continuation_failures(), 1);
            assert_eq!(stats.question_replies.load(Ordering::SeqCst), 1);
            assert_eq!(stats.question_rejections.load(Ordering::SeqCst), 0);
            assert_eq!(stats.aborts.load(Ordering::SeqCst), 0);
            assert_eq!(stats.messages.load(Ordering::SeqCst), 1);

            // Both complete facts survive; their root/supplementary ordering is
            // not a separate caller-settlement contract.
            let mut failures = vec![&error];
            let mut commit = false;
            let mut read = false;
            while let Some(failure) = failures.pop() {
                assert!(!failure.diagnostics_truncated(), "{error:?}");
                if matches!(failure.context().phase, ErrorPhase::StateCommit | ErrorPhase::StateRead) {
                    assert_eq!(failure.kind(), ErrorKind::Integration, "{error:?}");
                    assert_eq!(failure.context().operation, Operation::ResolveNotification, "{error:?}");
                    assert_eq!(failure.detail(), Some(&ErrorDetail::ServiceUnavailable), "{error:?}");
                    commit |= failure.context().phase == ErrorPhase::StateCommit;
                    read |= failure.context().phase == ErrorPhase::StateRead;
                }
                failures.extend(failure.cause());
                failures.extend(failure.supplementary());
            }
            assert!(commit && read, "both settlement failures must survive: {error:?}");

            let accepted = case.faults.inner.load(key.clone()).await.unwrap().unwrap();
            let mut expected = (*pending.record.value).clone();
            expected["state"] = serde_json::json!("accepted");
            assert_eq!(*accepted.record.value, expected);
            assert_eq!(accepted.revision.get(), pending.revision.get() + 1);
            let notifications = case.faults.inner.load(StateKey::new(
                "agent_sdk_conversation_notifications_v1",
                case.conversation.id().as_str(),
            ).unwrap()).await.unwrap().unwrap();
            let items = notifications.record.value["pending"].as_array().unwrap();
            assert!(items.iter().all(|item| item["state"]["state"] != "resolving"));
            if let Some(item) = items.iter().find(|item| item["id"].as_str() == Some(interaction.id().as_str())) {
                assert_eq!(item["state"]["state"], "terminal");
                assert!(item["state"]["detail"]["outcome"].get("accepted").is_some());
            }

            let duplicate = case.conversation.resolve_notification(
                interaction.id().clone(),
                NotificationResolution::Submit {
                    content: serde_json::json!({"answers": [["Yes"]]}),
                },
            ).await.expect_err("the actually committed answer cannot be resubmitted");
            assert!(matches!(duplicate.kind(), ErrorKind::Conflict | ErrorKind::NotFound), "{duplicate:?}");
            assert_eq!(case.faults.inner.load(key).await.unwrap(), Some(accepted));
            assert_eq!(stats.question_replies.load(Ordering::SeqCst), 1);
            assert_eq!(case.faults.ambiguous_continuation_reconciliations(), 1);
            assert_eq!(case.faults.continuation_failures(), 1);
        },
    ).await;
    // Disable any unconsumed setup fault and release the held native response
    // before independently revoking and draining the existing fixture owners.
    case.faults
        .state
        .obscure_continuation_reconciliation_commit
        .store(false, Ordering::SeqCst);
    case.faults
        .state
        .fail_continuation_after_commit
        .store(false, Ordering::SeqCst);
    case.faults
        .state
        .continuation_load_countdown
        .store(0, Ordering::SeqCst);
    case.release();
    let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
    cleanup
        .settle(async {
            case.revoke().await;
            Ok::<(), SdkError>(())
        })
        .await;
    cleanup
        .settle(async {
            assert_eq!(stats.active.load(Ordering::SeqCst), 0);
            assert_eq!(stats.connections.load(Ordering::SeqCst), 0);
            assert_eq!(
                stats.question_replies.load(Ordering::SeqCst),
                1,
                "joined cleanup cannot replay the accepted answer"
            );
            assert_eq!(
                stats.messages.load(Ordering::SeqCst),
                1,
                "settlement cannot start another prompt"
            );
            Ok::<(), SdkError>(())
        })
        .await;
    cleanup.finish(observed);
}

#[tokio::test]
async fn opencode_abort_joiners_share_dispatch_and_settle_continuations_first() {
    let mut probe = OpenCodeEventProbe::start(0).await;
    probe.request_question_on_prompt();
    let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
    let agent = AgentBuilder::create(
        probe.config(Duration::from_secs(1), Duration::from_secs(3)),
        faults.storage(),
    )
    .build()
    .await
    .expect("OpenCode Agent must build against the abort probe");
    let workspace = agent
        .new_workspace(OpenCodeEventProbe::workspace_request())
        .await
        .expect("OpenCode abort workspace must be created");
    let conversation = workspace
        .new_conversation(
            NewConversation::new(None, None, Vec::new())
                .expect("OpenCode abort conversation request must be valid"),
        )
        .await
        .expect("OpenCode abort conversation must be created");
    let mut notifications = conversation
        .notification_stream()
        .expect("OpenCode abort notification receiver must be claimed");
    let status_requests = probe.stats.status_requests.load(Ordering::SeqCst);
    let mut prompt = Box::pin(
        conversation.prompt(
            PromptRequest::new("ask the abort probe question")
                .expect("OpenCode abort prompt must be valid")
                .with_timeout(Duration::from_secs(5))
                .expect("OpenCode abort prompt timeout must be valid"),
        ),
    );
    let interaction = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            tokio::select! {
                notification = notifications.recv() => {
                    let notification = notification
                        .expect("OpenCode notification delivery must remain open")
                        .expect("OpenCode interaction notification must succeed");
                    if matches!(
                        notification.kind(),
                        ConversationNotificationKind::Interaction { .. }
                    ) {
                        break notification;
                    }
                }
                result = &mut prompt => {
                    panic!("OpenCode prompt terminalized before its continuation: {result:?}")
                }
            }
        }
    })
    .await
    .expect("OpenCode interaction must be delivered within the prompt deadline");
    probe.wait_for_messages(1).await;
    probe.wait_for_status_requests(status_requests + 2).await;
    let history = probe.stats.history.load(Ordering::SeqCst);
    let pending_scans = probe.stats.pending_scans.load(Ordering::SeqCst);
    let pending = faults
        .inner
        .load(
            StateKey::new(
                OPENCODE_CONTINUATION_NAMESPACE,
                format!("{}:que_probe", conversation.id().as_str()),
            )
            .expect("the delivered continuation has its exact durable identity"),
        )
        .await
        .expect("the delivered continuation must remain readable")
        .expect("the delivered continuation must remain durable before Abort");
    assert_eq!(pending.record.value["state"], "pending");
    assert_eq!(
        pending.record.value["notification_id"],
        interaction.id().as_str()
    );
    faults.obscure_next_continuation_reconciliation_commit();
    faults.fail_next_post_commit_continuation_reconciliation();
    // The accepted Abort owns the next synchronization after its foreground
    // writes settle. An unrelated eager gap would introduce another allowed
    // Lance transaction conflict, outside this shared-dispatch assertion.

    let (owner, joiner) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(conversation.abort(), conversation.abort())
    })
    .await
    .expect("OpenCode abort owner and joiner must settle within one lifecycle deadline");
    if owner.is_err() || joiner.is_err() {
        let primary = format!(
            "abort joiners must preserve one successful settlement: owner={owner:?}; joiner={joiner:?}; messages={}; aborts={}; native_active={}",
            probe.stats.messages.load(Ordering::SeqCst),
            probe.stats.aborts.load(Ordering::SeqCst),
            probe.stats.native_active.load(Ordering::SeqCst),
        );
        let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
        cleanup.settle(async {
            match tokio::time::timeout(Duration::from_secs(2), &mut prompt).await {
                Ok(Ok(_)) => eprintln!("abort-joiners prompt evidence=unexpected_success"),
                Ok(Err(error)) => eprintln!("abort-joiners prompt error={error:?}"),
                Err(_) => eprintln!("abort-joiners prompt observation=deadline"),
            }
            let mut delivered = 0_usize;
            let observation = tokio::time::timeout(Duration::from_secs(2), async {
                while delivered < 16 {
                    match notifications.recv().await {
                        Some(Err(error)) => {
                            eprintln!("abort-joiners notification error={error:?}");
                            break;
                        }
                        Some(Ok(_)) => delivered += 1,
                        None => break,
                    }
                }
            }).await;
            eprintln!("abort-joiners notification snapshot: delivered={delivered}; observation_complete={}", observation.is_ok());
            if let Some(snapshot) = faults.prompt_snapshot(&conversation.id()).await {
                eprintln!("abort-joiners durable snapshot={}", serde_json::json!({
                    "revision":snapshot.revision.get(),
                    "phase":snapshot.record.value["phase"]["phase"].as_str(),
                    "cancellation":snapshot.record.value["cancellation"]["state"].as_str(),
                    "waiter":snapshot.record.value["waiter"].as_str(),
                }));
            }
            Ok::<(), SdkError>(())
        }).await;
        // Both Abort observers above have joined; shutdown retains any prompt
        // work whose observation did not finish within the diagnostic bound.
        drop(prompt);
        drop(notifications);
        drop(conversation);
        drop(workspace);
        cleanup.settle(agent.shutdown()).await;
        let mut server = probe
            .task
            .take()
            .expect("the protocol peer retains its owned server");
        let mut joined = false;
        cleanup
            .settle(async {
                probe.shutdown().await;
                let result = (&mut server).await;
                joined = true;
                result.expect("the protocol server must join after failed Abort");
                Ok::<(), SdkError>(())
            })
            .await;
        if !joined {
            server.abort();
            if let Err(error) = server.await
                && !error.is_cancelled()
            {
                cleanup.settle(async { Err::<(), _>(error) }).await;
            }
        }
        return cleanup.finish(Err(Box::new(primary)));
    }
    owner.unwrap_or_else(|error| {
        panic!(
            "OpenCode abort owner must observe completion: {error:?}; messages={}, aborts={}, native_active={}, history={}, pending_scans={}",
            probe.stats.messages.load(Ordering::SeqCst),
            probe.stats.aborts.load(Ordering::SeqCst),
            probe.stats.native_active.load(Ordering::SeqCst),
            probe.stats.history.load(Ordering::SeqCst),
            probe.stats.pending_scans.load(Ordering::SeqCst),
        )
    });
    joiner.unwrap_or_else(|error| {
        panic!(
            "OpenCode abort joiner must observe the same completion: {error:?}; messages={}, aborts={}, native_active={}",
            probe.stats.messages.load(Ordering::SeqCst),
            probe.stats.aborts.load(Ordering::SeqCst),
            probe.stats.native_active.load(Ordering::SeqCst),
        )
    });
    assert_eq!(
        probe.stats.aborts.load(Ordering::SeqCst),
        1,
        "one prompt generation must dispatch exactly one native abort"
    );
    assert!(
        probe.stats.history.load(Ordering::SeqCst) > history,
        "Abort settlement must complete native history synchronization"
    );
    assert!(
        probe.stats.pending_scans.load(Ordering::SeqCst) >= pending_scans + 2,
        "Abort settlement must reconcile native permission and question inventories"
    );
    assert_eq!(
        faults.continuation_failures(),
        1,
        "Settling must resume after the injected post-commit reconciliation failure"
    );
    assert_eq!(
        faults.ambiguous_continuation_reconciliations(),
        1,
        "an applied lost-continuation commit must be confirmed exactly once"
    );
    let prompt_error = tokio::time::timeout(Duration::from_secs(2), &mut prompt)
        .await
        .expect("the aborted prompt waiter must terminate")
        .expect_err("the aborted prompt cannot report success");
    assert!(
        matches!((prompt_error).kind(), ErrorKind::Cancelled),
        "accepted native abort must preserve portable cancellation: {prompt_error:?}"
    );
    let resolution_error = conversation
        .resolve_notification(
            interaction.id().clone(),
            NotificationResolution::Submit {
                content: serde_json::json!({"answers": [["Yes"]]}),
            },
        )
        .await
        .expect_err("abort completion must follow continuation settlement");
    assert!(
        matches!(
            (resolution_error).kind(),
            ErrorKind::Conflict | ErrorKind::NotFound | ErrorKind::Closed
        ),
        "settled continuation must reject a later resolution: {resolution_error:?}"
    );
    conversation
        .abort()
        .await
        .expect("a later abort must observe the released idle reservation");
    assert_eq!(probe.stats.aborts.load(Ordering::SeqCst), 1);

    drop(prompt);
    drop(notifications);
    drop(conversation);
    drop(workspace);
    agent
        .shutdown()
        .await
        .expect("OpenCode Agent must shut down after abort reconciliation");
    probe.shutdown().await;
}

#[tokio::test]
async fn opencode_absent_prompt_reconciliation_releases_its_generation() {
    let probe = OpenCodeEventProbe::start(0).await;
    let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
    let agent = AgentBuilder::create(
        probe.config(Duration::from_secs(1), Duration::from_secs(3)),
        faults.storage(),
    )
    .build()
    .await
    .expect("OpenCode Agent must build against the absent-prompt probe");
    let workspace = agent
        .new_workspace(OpenCodeEventProbe::workspace_request())
        .await
        .expect("OpenCode absent-prompt workspace must be created");
    let conversation = workspace
        .new_conversation(
            NewConversation::new(None, None, Vec::new())
                .expect("OpenCode absent-prompt conversation request must be valid"),
        )
        .await
        .expect("OpenCode absent-prompt conversation must be created");
    let conversation_id = conversation.id();
    let mut prompt = Box::pin(
        conversation.prompt(
            PromptRequest::new("hold the absent-prompt probe open")
                .expect("OpenCode absent-prompt request must be valid")
                .with_timeout(Duration::from_secs(5))
                .expect("OpenCode absent-prompt timeout must be valid"),
        ),
    );
    tokio::select! {
        _ = probe.wait_for_messages(1) => {}
        result = &mut prompt => panic!("prompt ended before its durable record was removed: {result:?}"),
    }
    assert!(faults.prompt_snapshot(&conversation_id).await.is_some());
    faults.delete_prompt(&conversation_id).await;

    tokio::time::timeout(Duration::from_secs(3), conversation.abort())
        .await
        .expect("absent prompt reconciliation must remain bounded")
        .expect("an absent durable prompt must settle and release its reservation");
    let prompt_error = tokio::time::timeout(Duration::from_secs(2), &mut prompt)
        .await
        .expect("the prompt waiter must observe its removed durable state")
        .expect_err("a removed durable prompt cannot complete successfully");
    assert!(
        matches!(
            (prompt_error).kind(),
            ErrorKind::InvalidState
                | ErrorKind::Integration
                | ErrorKind::Closed
                | ErrorKind::Cancelled
        ),
        "the prompt must preserve an exact terminal failure: {prompt_error:?}"
    );
    conversation
        .abort()
        .await
        .expect("a second abort must observe no retained generation");
    probe.mark_native_idle();
    let following = conversation
        .prompt(OpenCodeEventProbe::structured_request(Duration::from_secs(
            2,
        )))
        .await
        .expect("a later prompt must reuse the released reservation");
    assert_eq!(
        following
            .structured_output()
            .and_then(|value| value.get("ack"))
            .and_then(serde_json::Value::as_str),
        Some("STRUCTURED_ACK")
    );

    drop(prompt);
    drop(conversation);
    drop(workspace);
    agent
        .shutdown()
        .await
        .expect("OpenCode Agent must shut down after absent-prompt reconciliation");
    probe.shutdown().await;
}

#[tokio::test]
async fn opencode_recovered_prompt_commit_classifies_faults_and_publishes_after_commit() {
    let probe = OpenCodeEventProbe::start(0).await;
    let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
    let agent = AgentBuilder::create(
        probe.config(Duration::from_secs(1), Duration::from_secs(3)),
        faults.storage(),
    )
    .build()
    .await
    .expect("OpenCode Agent must build against the recovered-prompt probe");
    let agent_id = agent.id();
    let workspace = agent
        .new_workspace(OpenCodeEventProbe::workspace_request())
        .await
        .expect("OpenCode recovered-prompt workspace must be created");
    let conversation = workspace
        .new_conversation(
            NewConversation::new(None, None, Vec::new())
                .expect("OpenCode recovered-prompt conversation request must be valid"),
        )
        .await
        .expect("OpenCode recovered-prompt conversation must be created");
    let conversation_id = conversation.id();
    let status_requests = probe.stats.status_requests.load(Ordering::SeqCst);
    faults.arm(PromptCommitFault::BlockThenCommit, 2);
    let prompt = tokio::spawn(async move {
        conversation
            .prompt(
                PromptRequest::new("capture the prepared recovery boundary")
                    .expect("OpenCode recovery prompt must be valid")
                    .with_timeout(Duration::from_secs(8))
                    .expect("OpenCode recovery prompt timeout must be valid"),
            )
            .await
    });
    faults.wait_until_blocked().await;
    let prepared = faults
        .prompt_snapshot(&conversation_id)
        .await
        .expect("the first prompt commit must expose its prepared record");
    assert_eq!(
        prepared.record.value.pointer("/phase/phase"),
        Some(&serde_json::Value::String("prepared".to_owned()))
    );
    assert_eq!(
        prepared.record.value.get("waiter"),
        Some(&serde_json::Value::String("waiting".to_owned()))
    );
    faults.release();
    probe.wait_for_messages(1).await;
    probe.wait_for_status_requests(status_requests + 2).await;
    drop(workspace);
    let (shutdown, prompt) = tokio::join!(agent.shutdown(), prompt);
    shutdown.expect("the capture Agent must settle its admitted prompt");
    let prompt_error = prompt
        .expect("the capture prompt task must join")
        .expect_err("shutdown must cancel the capture prompt");
    assert!(matches!((prompt_error).kind(), ErrorKind::Cancelled));

    faults.restore_prompt(prepared.record.clone()).await;
    probe.mark_native_active();
    faults.arm(PromptCommitFault::Conflict, 1);
    let conflict = AgentBuilder::reopen(
        probe.config(Duration::from_secs(1), Duration::from_secs(3)),
        faults.storage(),
        agent_id.clone(),
    )
    .build()
    .await
    .map(|_| ())
    .expect_err("a definite recovered-prompt conflict must escape reopen");
    assert!(
        matches!((conflict).kind(), ErrorKind::Conflict),
        "recovered prompt conflict must retain its exact classification: {conflict:?}"
    );
    assert_eq!(
        faults
            .prompt_snapshot(&conversation_id)
            .await
            .expect("conflicted prompt record must remain")
            .record
            .value
            .get("waiter"),
        Some(&serde_json::Value::String("waiting".to_owned())),
        "a failed commit cannot publish its recovered waiter mutation"
    );

    faults.restore_prompt(prepared.record.clone()).await;
    faults.arm(PromptCommitFault::BlockThenConflict, 1);
    let mut late_conflict = tokio::spawn({
        let config = probe.config(Duration::from_secs(1), Duration::from_secs(1));
        let storage = faults.storage();
        let agent_id = agent_id.clone();
        async move {
            AgentBuilder::reopen(config, storage, agent_id)
                .build()
                .await
        }
    });
    faults.wait_until_blocked().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(1_200), &mut late_conflict)
            .await
            .is_err(),
        "the admitted prompt conflict must remain owned past construction expiry"
    );
    faults.release();
    let late_conflict = tokio::time::timeout(Duration::from_secs(4), late_conflict)
        .await
        .expect("the late prompt conflict must remain observable")
        .expect("the late prompt-conflict task must join")
        .map(|_| ())
        .expect_err("a definite prompt conflict cannot publish an Agent");
    assert!(
        matches!((late_conflict).kind(), ErrorKind::Conflict),
        "deadline expiry cannot erase a definite transaction conflict: {late_conflict:?}"
    );

    faults.arm(PromptCommitFault::Unavailable, 1);
    let unavailable = AgentBuilder::reopen(
        probe.config(Duration::from_secs(1), Duration::from_secs(3)),
        faults.storage(),
        agent_id.clone(),
    )
    .build()
    .await
    .map(|_| ())
    .expect_err("a failed recovered-prompt commit must escape reopen");
    assert!(
        matches!((unavailable).kind(), ErrorKind::Integration),
        "recovered prompt storage failure must remain integration-scoped: {unavailable:?}"
    );
    assert_eq!(
        faults
            .prompt_snapshot(&conversation_id)
            .await
            .expect("failed prompt record must remain")
            .record
            .value
            .get("waiter"),
        Some(&serde_json::Value::String("waiting".to_owned()))
    );

    faults.arm(PromptCommitFault::UnavailableAfterCommit, 1);
    let ambiguous = AgentBuilder::reopen(
        probe.config(Duration::from_secs(1), Duration::from_secs(3)),
        faults.storage(),
        agent_id.clone(),
    )
    .build()
    .await
    .map(|_| ())
    .expect_err("an ambiguously committed recovered prompt must retain its exact outcome");
    assert!(
        matches!((ambiguous).kind(), ErrorKind::InvalidState),
        "exact durable recovery must preserve the postcommit classification: {ambiguous:?}"
    );
    assert_eq!(
        faults
            .prompt_snapshot(&conversation_id)
            .await
            .expect("ambiguously committed prompt record must remain")
            .record
            .value
            .get("waiter"),
        Some(&serde_json::Value::String("lost".to_owned()))
    );

    faults.restore_prompt(prepared.record.clone()).await;
    faults.arm(PromptCommitFault::BlockThenCommit, 1);
    let reopen = tokio::spawn({
        let config = probe.config(Duration::from_secs(1), Duration::from_secs(3));
        let storage = faults.storage();
        let agent_id = agent_id.clone();
        async move {
            AgentBuilder::reopen(config, storage, agent_id)
                .build()
                .await
        }
    });
    faults.wait_until_blocked().await;
    assert!(
        !reopen.is_finished(),
        "reopen observation must remain pending until recovered state commits"
    );
    assert_eq!(
        faults
            .prompt_snapshot(&conversation_id)
            .await
            .expect("blocked prompt record must remain visible")
            .record
            .value
            .get("waiter"),
        Some(&serde_json::Value::String("waiting".to_owned())),
        "the recovered waiter mutation must not become observable before commit"
    );
    faults.release();
    let committed = tokio::time::timeout(Duration::from_secs(4), reopen)
        .await
        .expect("reopen must resume after the recovered commit")
        .expect("reopen task must join")
        .map(|_| ())
        .expect_err("native work before dispatch must remain invalid after recovery commits");
    assert!(matches!((committed).kind(), ErrorKind::InvalidState));
    assert_eq!(
        faults
            .prompt_snapshot(&conversation_id)
            .await
            .expect("committed recovered prompt must remain")
            .record
            .value
            .get("waiter"),
        Some(&serde_json::Value::String("lost".to_owned())),
        "recovered state becomes observable only after its commit is released"
    );

    faults.restore_prompt(prepared.record.clone()).await;
    probe.mark_native_idle();
    faults.arm(PromptCommitFault::BlockThenCommit, 1);
    let mut late_reopen = tokio::spawn({
        let config = probe.config(Duration::from_secs(1), Duration::from_secs(1));
        let storage = faults.storage();
        let agent_id = agent_id.clone();
        async move {
            AgentBuilder::reopen(config, storage, agent_id)
                .build()
                .await
        }
    });
    faults.wait_until_blocked().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(1_200), &mut late_reopen)
            .await
            .is_err(),
        "an admitted prompt cleanup must remain owned after construction expiry"
    );
    faults.release();
    let late = tokio::time::timeout(Duration::from_secs(4), late_reopen)
        .await
        .expect("late prompt cleanup settlement must remain observable")
        .expect("late prompt cleanup task must join")
        .map(|_| ())
        .expect_err("late prompt cleanup cannot publish an Agent");
    assert!(
        matches!((late).kind(), ErrorKind::Timeout),
        "a confirmed late prompt cleanup must retain Timeout: {late:?}"
    );
    assert!(
        faults.prompt_snapshot(&conversation_id).await.is_none(),
        "the late committed prompt cleanup must remain durable recovery state"
    );

    faults.restore_prompt(prepared.record).await;
    faults.arm(PromptCommitFault::UnavailableAfterCommit, 1);
    let reopened = AgentBuilder::reopen(
        probe.config(Duration::from_secs(1), Duration::from_secs(3)),
        faults.storage(),
        agent_id,
    )
    .build()
    .await
    .expect("an exact-read ambiguous prompt cleanup must reopen successfully");
    assert!(
        faults.prompt_snapshot(&conversation_id).await.is_none(),
        "ambiguous prompt cleanup must be accepted only after exact deletion is observed"
    );
    reopened
        .shutdown()
        .await
        .expect("the recovered-prompt Agent must shut down");
    probe.shutdown().await;
}

#[tokio::test]
async fn opencode_assistant_normal_history_replay_preserves_origin_authority() {
    OpenCodeEventProbe::start(0)
        .await
        .assistant_history_replay(false, false, PromptCommitFault::None)
        .await;
}

#[tokio::test]
async fn opencode_assistant_sync_history_replay_preserves_origin_authority() {
    OpenCodeEventProbe::start(0)
        .await
        .assistant_history_replay(true, false, PromptCommitFault::None)
        .await;
}

#[tokio::test]
async fn opencode_assistant_history_replay_rejects_changed_parent() {
    OpenCodeEventProbe::start(0)
        .await
        .assistant_history_replay(true, true, PromptCommitFault::None)
        .await;
}

#[tokio::test]
async fn opencode_assistant_history_promotion_confirms_applied_receipt() {
    OpenCodeEventProbe::start(0)
        .await
        .assistant_history_replay(true, false, PromptCommitFault::UnavailableAfterCommit)
        .await;
}

#[tokio::test]
async fn opencode_assistant_history_promotion_rejects_unapplied_receipt() {
    OpenCodeEventProbe::start(0)
        .await
        .assistant_history_replay(true, false, PromptCommitFault::Unavailable)
        .await;
}

#[tokio::test]
async fn opencode_pre_binding_event_recovers_on_the_owned_stream() {
    let probe = OpenCodeEventProbe::start(0).await;
    let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
    let agent = AgentBuilder::create(
        probe.config(Duration::from_secs(5), Duration::from_secs(3)),
        faults.storage(),
    )
    .build()
    .await
    .expect("OpenCode Agent must build before the binding race");
    let workspace = agent
        .new_workspace(OpenCodeEventProbe::workspace_request())
        .await
        .expect("OpenCode binding-race workspace must be created");

    let history = probe.stats.history.load(Ordering::SeqCst);
    let pending_scans = probe.stats.pending_scans.load(Ordering::SeqCst);
    probe.block_history();
    probe.emit_next_session_before_binding();
    let conversation = workspace
        .new_conversation(
            NewConversation::new(None, None, Vec::new())
                .expect("OpenCode binding-race conversation request must be valid"),
        )
        .await
        .expect("the durable binding must admit the early native event");
    probe.release_history();
    probe
        .wait_for_synchronization(history + 1, pending_scans + 2)
        .await;
    let synchronized_history = probe.stats.history.load(Ordering::SeqCst);
    let attempts = faults.event_commit_attempts();
    assert!(
        attempts > 0,
        "same-stream history must apply the event after durable binding"
    );
    probe.emit_durable("evt_probe_after_binding", 1);
    faults.wait_for_event_commit_attempts(attempts + 1).await;
    assert_eq!(
        probe.stats.history.load(Ordering::SeqCst),
        synchronized_history,
        "the following contiguous event must not require another synchronization"
    );
    assert_eq!(
        probe.stats.subscriptions.load(Ordering::SeqCst),
        1,
        "binding admission must preserve the owned SSE response"
    );
    assert_eq!(probe.stats.maximum.load(Ordering::SeqCst), 1);
    conversation
        .info()
        .await
        .expect("same-stream admission recovery must keep the hierarchy live");

    drop(conversation);
    drop(workspace);
    agent
        .shutdown()
        .await
        .expect("OpenCode Agent must shut down after binding admission recovery");
    probe.shutdown().await;
}

#[tokio::test]
async fn opencode_pending_reconciliation_confirms_ambiguity_and_preserves_conflict() {
    let probe = OpenCodeEventProbe::start(0).await;
    let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
    let agent = AgentBuilder::create(
        probe.config(Duration::from_secs(1), Duration::from_secs(3)),
        faults.storage(),
    )
    .build()
    .await
    .expect("OpenCode Agent must build before pending reconciliation");
    let workspace = agent
        .new_workspace(OpenCodeEventProbe::workspace_request())
        .await
        .expect("OpenCode pending-reconciliation workspace must be created");
    let conversation = workspace
        .new_conversation(
            NewConversation::new(None, None, Vec::new())
                .expect("OpenCode pending-reconciliation conversation request must be valid"),
        )
        .await
        .expect("OpenCode pending-reconciliation conversation must be created");
    let mut notifications = conversation
        .notification_stream()
        .expect("OpenCode pending-reconciliation notification receiver must be claimed");

    probe.retain_pending_question();
    let history = probe.stats.history.load(Ordering::SeqCst);
    let pending_scans = probe.stats.pending_scans.load(Ordering::SeqCst);
    faults.obscure_next_continuation_reconciliation_commit();
    faults.fail_next_post_commit_continuation_reconciliation();
    probe.emit_gap(0, "pending_ambiguous_commit");
    probe
        .wait_for_synchronization(history + 1, pending_scans + 2)
        .await;
    let notification = tokio::time::timeout(Duration::from_secs(3), notifications.recv())
        .await
        .expect("confirmed pending reconciliation must publish within its lifecycle deadline")
        .expect("OpenCode notification delivery must remain open")
        .expect("OpenCode pending interaction notification must succeed");
    assert!(
        matches!(
            notification.kind(),
            ConversationNotificationKind::Interaction { .. }
        ),
        "confirmed pending reconciliation must publish its typed interaction"
    );
    assert_eq!(
        faults.continuation_failures(),
        1,
        "pending reconciliation must retry a transient exact-observation failure"
    );
    assert_eq!(
        faults.ambiguous_continuation_reconciliations(),
        1,
        "the ambiguous pending mutation must commit exactly once"
    );

    let history = probe.stats.history.load(Ordering::SeqCst);
    faults.conflict_next_continuation_transaction_load();
    probe.emit_gap(2, "pending_mutation_conflict");
    probe.wait_for(1, history + 1, 0).await;
    assert_eq!(
        probe.stats.subscriptions.load(Ordering::SeqCst),
        1,
        "pending mutation conflict must stop without reconnecting the healthy SSE response"
    );

    drop(notifications);
    drop(conversation);
    drop(workspace);
    let error = agent
        .shutdown()
        .await
        .expect_err("the stopped event owner must retain the pending mutation conflict");
    assert!(
        matches!((error).kind(), ErrorKind::Conflict),
        "pending mutation conflicts must retain Conflict at shutdown: {error:?}"
    );
    probe.shutdown().await;
}

#[tokio::test]
async fn opencode_catalog_invalidation_is_postcommit_and_applied_once() {
    let probe = OpenCodeEventProbe::start(0).await;
    let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
    let agent = AgentBuilder::create(
        probe.config(Duration::from_secs(2), Duration::from_secs(3)),
        faults.storage(),
    )
    .build()
    .await
    .expect("OpenCode Agent must build against the catalog probe");
    let model = agent
        .list_models()
        .await
        .expect("the catalog probe must expose its selected model")
        .into_iter()
        .next()
        .expect("the catalog probe must expose one model")
        .selector()
        .clone();
    let defaults = WorkspaceDefaults::new(Some(model), None, None)
        .expect("the catalog probe defaults must be valid");
    let workspace = Arc::new(
        agent
            .new_workspace(OpenCodeEventProbe::workspace_request_with_defaults(
                defaults,
            ))
            .await
            .expect("OpenCode catalog workspace must be created"),
    );
    assert_eq!(
        workspace
            .list_tools()
            .await
            .expect("the initial catalog must bind")
            .len(),
        1
    );

    faults.obscure_next_event_commit();
    faults.fail_next_event_commit_observation();
    probe.block_next_tool_inventory();
    let listing_workspace = Arc::clone(&workspace);
    let stale_listing = tokio::spawn(async move { listing_workspace.list_tools().await });
    probe.wait_until_tool_inventory_blocked().await;

    faults.block_next_event_commit();
    probe.emit_catalog_invalidation("evt_probe_catalog_postcommit");
    faults.wait_until_event_commit_blocked().await;
    let invalidation_attempt = faults.event_commit_attempts();

    faults.block_next_transaction_begin();
    probe.release_tool_inventory();
    faults.wait_until_transaction_begin_blocked().await;

    faults.release_event_commit();
    faults.wait_for_event_observation_failures(1).await;
    assert_eq!(
        faults.event_commit_attempts(),
        invalidation_attempt,
        "a failed exact observation must not replay the catalog mutation"
    );
    probe.emit_normal("evt_probe_catalog_postcommit_sentinel");
    faults
        .wait_for_event_commit_attempts(invalidation_attempt + 1)
        .await;
    for _ in 0..256 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        faults.event_commit_attempts(),
        invalidation_attempt + 1,
        "the catalog mutation and same-stream sentinel must each commit once"
    );
    faults.release_transaction_begin();
    let error = stale_listing
        .await
        .expect("the stale catalog listing task must join")
        .expect_err("an applied catalog event must invalidate the captured generation");
    assert!(
        matches!((error).kind(), ErrorKind::Conflict),
        "the stale catalog listing must retain its conflict classification: {error:?}"
    );
    assert_eq!(
        faults.event_observation_failures(),
        1,
        "catalog invalidation must survive one failed exact commit observation"
    );
    assert_eq!(
        workspace
            .list_tools()
            .await
            .expect("a fresh catalog listing must bind after invalidation")
            .len(),
        1
    );

    probe.block_next_tool_inventory();
    let listing_workspace = Arc::clone(&workspace);
    let duplicate_listing = tokio::spawn(async move { listing_workspace.list_tools().await });
    probe.wait_until_tool_inventory_blocked().await;
    let attempts = faults.event_commit_attempts();
    probe.emit_catalog_invalidation("evt_probe_catalog_postcommit");
    probe.emit_normal("evt_probe_catalog_duplicate_sentinel");
    faults.wait_for_event_commit_attempts(attempts + 2).await;
    probe.release_tool_inventory();
    assert_eq!(
        duplicate_listing
            .await
            .expect("the duplicate catalog listing task must join")
            .expect("a remembered duplicate must not invalidate the catalog")
            .len(),
        1
    );
    assert_eq!(
        probe.stats.subscriptions.load(Ordering::SeqCst),
        1,
        "catalog application and replay must retain the original SSE response"
    );
    assert_eq!(probe.stats.maximum.load(Ordering::SeqCst), 1);

    drop(workspace);
    agent
        .shutdown()
        .await
        .expect("OpenCode Agent must shut down after catalog invalidation");
    probe.shutdown().await;
}

#[tokio::test]
async fn opencode_late_applied_catalog_event_publishes_before_timeout() {
    let probe = OpenCodeEventProbe::start(0).await;
    let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
    let agent = AgentBuilder::create(
        probe.config(Duration::from_secs(2), Duration::from_secs(10)),
        faults.storage(),
    )
    .build()
    .await
    .expect("OpenCode Agent must build against the late catalog probe");
    let model = agent
        .list_models()
        .await
        .expect("the late catalog probe must expose its selected model")
        .into_iter()
        .next()
        .expect("the late catalog probe must expose one model")
        .selector()
        .clone();
    let defaults = WorkspaceDefaults::new(Some(model), None, None)
        .expect("the late catalog defaults must be valid");
    let workspace = Arc::new(
        agent
            .new_workspace(OpenCodeEventProbe::workspace_request_with_defaults(
                defaults,
            ))
            .await
            .expect("OpenCode late catalog workspace must be created"),
    );
    assert_eq!(
        workspace
            .list_tools()
            .await
            .expect("the initial late catalog must bind")
            .len(),
        1
    );
    let conversation = workspace
        .new_conversation(
            NewConversation::new(None, None, Vec::new())
                .expect("OpenCode late catalog conversation request must be valid"),
        )
        .await
        .expect("OpenCode late catalog conversation must be created");

    probe.block_next_tool_inventory();
    let listing_workspace = Arc::clone(&workspace);
    let mut stale_listing = tokio::spawn(async move { listing_workspace.list_tools().await });
    probe.wait_until_tool_inventory_blocked().await;
    faults.block_next_transaction_begin();
    probe.release_tool_inventory();
    faults.wait_until_transaction_begin_blocked().await;

    let history = probe.stats.history.load(Ordering::SeqCst);
    probe.block_history();
    let mut prompt = tokio::spawn(async move {
        conversation
            .prompt(OpenCodeEventProbe::structured_request(Duration::from_secs(
                2,
            )))
            .await
    });
    let mut prompt_joined = false;
    let mut listing_joined = false;
    let outcome = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(20),
        async {
            probe.wait_for_history(history + 1).await;
            let mut catalog_commit = faults.block_next_event_catalog_load();
            probe.emit_catalog_invalidation("evt_probe_catalog_late_applied");
            probe.release_history();
            faults.wait_until_event_catalog_load_blocked().await;

            assert!(
                tokio::time::timeout(Duration::from_secs(3), &mut catalog_commit).await.is_err(),
                "the selected catalog transaction must remain owned beyond the structured deadline"
            );
            assert!(
                !prompt.is_finished(),
                "the deliberately held timeout writer cannot finish before catalog publication"
            );

            let attempts = faults.event_commit_attempts();
            faults.release_event_catalog_load();
            tokio::time::timeout(Duration::from_secs(5), &mut catalog_commit).await
                .expect("the original catalog transaction must complete after release")
                .expect("catalog transaction ownership must not be lost")
                .expect("the original catalog transaction must actually publish");
            probe.emit_normal("evt_probe_catalog_late_applied_sentinel");
            faults.wait_for_event_commit_attempts(attempts + 2).await;
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let progress = faults.inner.list(StateListRequest::new(OPENCODE_EVENT_PROGRESS_NAMESPACE)
                        .expect("event progress namespace must be valid")).await
                        .expect("the exact sentinel receipt must be readable");
                    if progress.records.iter().any(|snapshot| snapshot.record.value["recent"].as_array()
                        .is_some_and(|events| events.iter().any(|event|
                            event["id"] == "evt_probe_catalog_late_applied_sentinel"
                            && event["source"] == "normal"
                            && event["event_type"] == "probe.future"
                            && event["data"] == serde_json::json!({})))) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            }).await.expect("the same-stream sentinel must actually publish before peer writers resume");
            faults.release_event_catalog_peers();
            let prompt_result = tokio::time::timeout(Duration::from_secs(5), &mut prompt)
                .await
                .expect("the structured command must settle after the admitted transaction");
            prompt_joined = true;
            let prompt_error = prompt_result.expect("the structured command task must join")
                .expect_err("the expired catalog application cannot publish structured success");
            assert!(
                matches!((prompt_error).kind(), ErrorKind::Timeout),
                "the structured command must retain its timeout classification: {prompt_error:?}"
            );
            assert_eq!(
                faults.event_commit_attempts(),
                attempts + 2,
                "the late catalog event and its same-stream sentinel must each commit once"
            );

            faults.release_transaction_begin();
            let listing_result = (&mut stale_listing).await;
            listing_joined = true;
            let listing_error = listing_result.expect("the late stale catalog listing task must join")
                .expect_err("a late applied catalog event must publish before timing out");
            assert!(
                matches!((listing_error).kind(), ErrorKind::Conflict),
                "late postcommit invalidation must retain its conflict classification: {listing_error:?}"
            );
            assert_eq!(probe.stats.subscriptions.load(Ordering::SeqCst), 1);
            assert_eq!(probe.stats.maximum.load(Ordering::SeqCst), 1);

        },
    ).await;
    probe.release_history();
    faults.release_event_catalog_load();
    faults.release_event_catalog_peers();
    faults.release_transaction_begin();
    let mut cleanup = NativeCleanup::new(Duration::from_secs(15));
    if !prompt_joined {
        cleanup
            .settle(async {
                let result = (&mut prompt).await;
                prompt_joined = true;
                result.map(|_| ())
            })
            .await;
        if !prompt_joined {
            prompt.abort();
            cleanup
                .settle(async {
                    match (&mut prompt).await {
                        Ok(_) => Ok(()),
                        Err(error) if error.is_cancelled() => Ok(()),
                        Err(error) => Err(error),
                    }
                })
                .await;
        }
    }
    if !listing_joined {
        cleanup
            .settle(async {
                let result = (&mut stale_listing).await;
                listing_joined = true;
                result.map(|_| ())
            })
            .await;
        if !listing_joined {
            stale_listing.abort();
            cleanup
                .settle(async {
                    match (&mut stale_listing).await {
                        Ok(_) => Ok(()),
                        Err(error) if error.is_cancelled() => Ok(()),
                        Err(error) => Err(error),
                    }
                })
                .await;
        }
    }
    drop(workspace);
    cleanup.settle(agent.shutdown()).await;
    cleanup
        .settle(async {
            probe.shutdown().await;
            Ok::<(), SdkError>(())
        })
        .await;
    cleanup.finish(outcome);
}

#[tokio::test]
async fn opencode_permanent_live_operation_conflict_stops_without_reconnect() {
    let probe = OpenCodeEventProbe::start(0).await;
    let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
    let agent = AgentBuilder::create(
        probe.config(Duration::from_secs(5), Duration::from_secs(3)),
        faults.storage(),
    )
    .build()
    .await
    .expect("OpenCode Agent must build before the live conflict");

    let history = probe.stats.history.load(Ordering::SeqCst);
    faults.conflict_event_catalog_loads(usize::MAX);
    let started = tokio::time::Instant::now();
    probe.emit_catalog_invalidation("evt_probe_permanent_live_conflict");
    probe.wait_for(1, history, 0).await;
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "live transaction retries must consume the original application deadline: elapsed={:?}",
        started.elapsed()
    );
    assert!(
        faults.event_catalog_load_attempts() > 1,
        "the operation-level transaction conflict must reach the event owner's retry boundary"
    );
    assert_eq!(
        probe.stats.subscriptions.load(Ordering::SeqCst),
        1,
        "live application failure must not reconnect a healthy SSE response"
    );
    assert_eq!(probe.stats.maximum.load(Ordering::SeqCst), 1);

    let attempts = faults.event_catalog_load_attempts();
    let error = agent
        .shutdown()
        .await
        .expect_err("the stopped event owner must retain its application failure");
    assert_eq!(error.kind(), ErrorKind::Timeout, "{error:?}");
    assert_eq!(
        faults.event_catalog_load_attempts(),
        attempts,
        "event transaction attempts must stop at the original application deadline"
    );
    probe.shutdown().await;
}

#[tokio::test]
async fn opencode_gap_and_finite_live_conflicts_reuse_the_event_stream() {
    let probe = OpenCodeEventProbe::start(0).await;
    let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
    let agent = AgentBuilder::create(
        probe.config(Duration::from_secs(1), Duration::from_secs(3)),
        faults.storage(),
    )
    .build()
    .await
    .expect("OpenCode Agent must build against the gap probe");
    let workspace = agent
        .new_workspace(OpenCodeEventProbe::workspace_request())
        .await
        .expect("OpenCode workspace must be created");
    let conversation = workspace
        .new_conversation(
            NewConversation::new(None, None, Vec::new())
                .expect("OpenCode probe conversation request must be valid"),
        )
        .await
        .expect("OpenCode probe conversation must be created");

    let history = probe.stats.history.load(Ordering::SeqCst);
    let pending_scans = probe.stats.pending_scans.load(Ordering::SeqCst);
    probe.emit_gap(0, "initial");
    probe
        .wait_for_synchronization(history + 1, pending_scans + 2)
        .await;
    assert_eq!(
        probe.stats.subscriptions.load(Ordering::SeqCst),
        1,
        "gap repair must synchronize history on the existing SSE response"
    );
    assert_eq!(
        probe.stats.maximum.load(Ordering::SeqCst),
        1,
        "gap repair must never overlap event subscriptions"
    );

    faults.conflict_event_commits(3);
    probe.emit_durable("evt_probe_finite_2", 2);
    faults.wait_for_event_commit_attempts(4).await;
    probe.emit_durable("evt_probe_finite_3", 3);
    faults.wait_for_event_commit_attempts(5).await;
    probe.emit_durable("evt_probe_following_4", 4);
    faults.wait_for_event_commit_attempts(6).await;
    assert_eq!(
        probe.stats.history.load(Ordering::SeqCst),
        history + 1,
        "finite live transaction conflicts must not become history recovery"
    );
    assert_eq!(
        probe.stats.subscriptions.load(Ordering::SeqCst),
        1,
        "finite live transaction conflicts must retain the current SSE response"
    );
    conversation
        .info()
        .await
        .expect("the conversation must remain usable after event conflict recovery");

    probe.block_history();
    let started = tokio::time::Instant::now();
    probe.emit_gap(5, "deadline");
    probe.wait_for_history(history + 2).await;
    assert_eq!(
        probe.stats.subscriptions.load(Ordering::SeqCst),
        1,
        "the noncontiguous live event must enter history on the current SSE response"
    );
    faults.conflict_event_commits(usize::MAX);
    probe.release_history();
    faults.wait_for_event_commit_attempts(2).await;
    probe.wait_for(1, history + 2, 0).await;
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "gap transaction retries must consume one lifecycle deadline: elapsed={:?}",
        started.elapsed()
    );
    let attempts = faults.event_commit_attempts();
    assert_eq!(probe.stats.subscriptions.load(Ordering::SeqCst), 1);
    assert_eq!(probe.stats.maximum.load(Ordering::SeqCst), 1);

    drop(conversation);
    drop(workspace);
    let shutdown = agent.shutdown().await;
    let settled_attempts = faults.event_commit_attempts();
    let stats = Arc::clone(&probe.stats);
    probe.shutdown().await;
    let error = shutdown.expect_err("expired gap recovery must retain its event-owner failure");
    eprintln!(
        "gap recovery shutdown: {error:?}; elapsed={:?} attempts={attempts} settled_attempts={settled_attempts} subscriptions={} history={} active={}",
        started.elapsed(),
        stats.subscriptions.load(Ordering::SeqCst),
        stats.history.load(Ordering::SeqCst),
        stats.active.load(Ordering::SeqCst),
    );
    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert_eq!(error.context().phase, ErrorPhase::EventSynchronization);
    assert!(
        error.cause().is_none(),
        "a failed attempt cannot cause the timer to elapse: {error:?}"
    );
    let [attempt] = error.supplementary() else {
        panic!("gap expiry must retain exactly its last failed recovery attempt: {error:?}");
    };
    assert_eq!(attempt.kind(), ErrorKind::Timeout);
    assert_eq!(attempt.context().phase, ErrorPhase::EventSynchronization);
    assert!(matches!(
        attempt.detail(),
        Some(ErrorDetail::DeadlineElapsed { .. })
    ));
    assert_eq!(
        settled_attempts, attempts,
        "event transaction attempts must stop when the gap episode expires"
    );
}

#[tokio::test]
async fn opencode_structured_parent_history_reuses_the_live_event_stream() {
    let probe = OpenCodeEventProbe::start(0).await;
    let agent = AgentBuilder::create(
        probe.config(Duration::from_secs(1), Duration::from_secs(3)),
        OpenCodeEventProbe::storage().await,
    )
    .build()
    .await
    .expect("OpenCode Agent must build against the structured-parent probe");
    let workspace = agent
        .new_workspace(OpenCodeEventProbe::workspace_request())
        .await
        .expect("OpenCode workspace must be created");
    let conversation = workspace
        .new_conversation(
            NewConversation::new(None, None, Vec::new())
                .expect("OpenCode probe conversation request must be valid"),
        )
        .await
        .expect("OpenCode probe conversation must be created");

    let history = probe.stats.history.load(Ordering::SeqCst);
    let pending_scans = probe.stats.pending_scans.load(Ordering::SeqCst);
    let response = conversation
        .prompt(OpenCodeEventProbe::structured_request(Duration::from_secs(2)))
        .await
        .unwrap_or_else(|error| {
            panic!(
                "structured prompt must recover its parent from history: {error:?}; subscriptions={}, history={}, pending_scans={}, active={}",
                probe.stats.subscriptions.load(Ordering::SeqCst),
                probe.stats.history.load(Ordering::SeqCst),
                probe.stats.pending_scans.load(Ordering::SeqCst),
                probe.stats.active.load(Ordering::SeqCst),
            )
        });
    assert_eq!(
        response
            .structured_output()
            .and_then(|value| value.get("ack"))
            .and_then(serde_json::Value::as_str),
        Some("STRUCTURED_ACK")
    );
    probe
        .wait_for_synchronization(history + 1, pending_scans + 2)
        .await;
    assert_eq!(
        probe.stats.subscriptions.load(Ordering::SeqCst),
        1,
        "structured-parent repair must synchronize history on the existing SSE response"
    );
    assert_eq!(
        probe.stats.maximum.load(Ordering::SeqCst),
        1,
        "structured-parent repair must never overlap event subscriptions"
    );

    drop(conversation);
    drop(workspace);
    agent
        .shutdown()
        .await
        .expect("OpenCode Agent must shut down after structured-parent repair");
    probe.wait_for(1, history + 1, 0).await;
    probe.shutdown().await;
}

#[tokio::test]
async fn opencode_structured_parent_deadline_preserves_the_healthy_live_stream() {
    let probe = OpenCodeEventProbe::start(0).await;
    let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
    let agent = AgentBuilder::create(
        probe.config(Duration::from_secs(1), Duration::from_secs(3)),
        faults.storage(),
    )
    .build()
    .await
    .expect("OpenCode Agent must build against the structured-timeout probe");
    let workspace = agent
        .new_workspace(OpenCodeEventProbe::workspace_request())
        .await
        .expect("OpenCode structured-timeout workspace must be created");
    let conversation = workspace
        .new_conversation(
            NewConversation::new(None, None, Vec::new())
                .expect("OpenCode structured-timeout conversation request must be valid"),
        )
        .await
        .expect("OpenCode structured-timeout conversation must be created");

    let history = probe.stats.history.load(Ordering::SeqCst);
    faults.conflict_event_commits(usize::MAX);
    let error = conversation
        .prompt(OpenCodeEventProbe::structured_request(
            Duration::from_millis(300),
        ))
        .await
        .expect_err("blocked structured-parent history must exhaust the caller deadline");
    assert!(
        matches!((error).kind(), ErrorKind::Timeout),
        "structured-parent command deadline must retain its timeout classification: {error:?}"
    );
    probe.wait_for_history(history + 1).await;
    faults.wait_for_event_commit_attempts(2).await;
    let attempts = faults.event_commit_attempts();
    faults.clear_event_commit_conflicts();

    let next_history = probe.stats.history.load(Ordering::SeqCst);
    let pending_scans = probe.stats.pending_scans.load(Ordering::SeqCst);
    probe.emit_gap(0, "structured_resume");
    probe
        .wait_for_synchronization(next_history + 1, pending_scans + 2)
        .await;
    assert_eq!(
        probe.stats.subscriptions.load(Ordering::SeqCst),
        1,
        "a structured command timeout must preserve the healthy SSE response"
    );
    assert_eq!(
        probe.stats.maximum.load(Ordering::SeqCst),
        1,
        "continued delivery after a structured timeout must not overlap subscriptions"
    );
    assert!(
        faults.event_commit_attempts() > attempts,
        "same-stream gap delivery must resume after the structured conflict clears"
    );

    drop(conversation);
    drop(workspace);
    agent
        .shutdown()
        .await
        .expect("OpenCode Agent must shut down after structured-timeout recovery");
    probe.wait_for(1, next_history + 1, 0).await;
    probe.shutdown().await;
}

#[tokio::test]
async fn opencode_queued_structured_parent_observes_its_own_absolute_deadline() {
    let probe = OpenCodeEventProbe::start(0).await;
    let agent = AgentBuilder::create(
        probe.config(Duration::from_secs(2), Duration::from_secs(3)),
        OpenCodeEventProbe::storage().await,
    )
    .build()
    .await
    .expect("OpenCode Agent must build against the queued-deadline probe");
    let workspace = agent
        .new_workspace(OpenCodeEventProbe::workspace_request())
        .await
        .expect("OpenCode queued-deadline workspace must be created");
    let conversation = workspace
        .new_conversation(
            NewConversation::new(None, None, Vec::new())
                .expect("OpenCode queued-deadline conversation request must be valid"),
        )
        .await
        .expect("OpenCode queued-deadline conversation must be created");

    let history = probe.stats.history.load(Ordering::SeqCst);
    let pending_scans = probe.stats.pending_scans.load(Ordering::SeqCst);
    probe.block_history();
    probe.emit_gap(0, "queued");
    probe.wait_for_history(history + 1).await;

    let started = tokio::time::Instant::now();
    let error = tokio::time::timeout(
        Duration::from_millis(1_200),
        conversation.prompt(OpenCodeEventProbe::structured_request(
            Duration::from_millis(400),
        )),
    )
    .await
    .expect("a queued synchronization must remain bounded by its own caller deadline")
    .expect_err("the queued structured-parent command must expire behind blocked gap repair");
    assert!(
        matches!((error).kind(), ErrorKind::Timeout),
        "the queued command must retain its timeout classification: {error:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "the short command must not inherit the earlier gap episode deadline: elapsed={:?}",
        started.elapsed()
    );
    assert_eq!(
        probe.stats.history.load(Ordering::SeqCst),
        history + 1,
        "the short command must expire while the earlier history request remains blocked"
    );
    assert_eq!(probe.stats.subscriptions.load(Ordering::SeqCst), 1);
    assert_eq!(probe.stats.maximum.load(Ordering::SeqCst), 1);

    probe.release_history();
    probe
        .wait_for_synchronization(history + 1, pending_scans + 2)
        .await;
    for _ in 0..256 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        probe.stats.history.load(Ordering::SeqCst),
        history + 1,
        "the owner must discard the abandoned queued command before starting more history"
    );
    assert_eq!(probe.stats.subscriptions.load(Ordering::SeqCst), 1);
    assert_eq!(probe.stats.maximum.load(Ordering::SeqCst), 1);

    drop(conversation);
    drop(workspace);
    agent
        .shutdown()
        .await
        .expect("OpenCode Agent must shut down after queued deadline expiration");
    probe.wait_for(1, history + 1, 0).await;
    probe.shutdown().await;
}

#[tokio::test]
async fn opencode_overlap_bound_stops_instead_of_resuming_the_live_stream() {
    let probe = OpenCodeEventProbe::start(0).await;
    let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
    let agent = AgentBuilder::create(
        probe.config(Duration::from_secs(2), Duration::from_secs(3)),
        faults.storage(),
    )
    .build()
    .await
    .expect("OpenCode Agent must build against the overlap probe");
    let workspace = agent
        .new_workspace(OpenCodeEventProbe::workspace_request())
        .await
        .expect("OpenCode overlap workspace must be created");
    let conversation = workspace
        .new_conversation(
            NewConversation::new(None, None, Vec::new())
                .expect("OpenCode overlap conversation request must be valid"),
        )
        .await
        .expect("OpenCode overlap conversation must be created");
    let history = probe.stats.history.load(Ordering::SeqCst);
    faults.block_next_event_commit();
    let prompt = tokio::spawn(async move {
        conversation
            .prompt(OpenCodeEventProbe::structured_request(Duration::from_secs(
                5,
            )))
            .await
    });
    faults.wait_until_event_commit_blocked().await;
    assert!(
        probe.stats.history.load(Ordering::SeqCst) > history,
        "the history response must complete before the event progress commit blocks"
    );
    probe.emit_overlap(1_025);
    for _ in 0..256 {
        tokio::task::yield_now().await;
    }
    faults.release_event_commit();
    let error = tokio::time::timeout(Duration::from_secs(7), prompt)
        .await
        .expect("overlap-bound failure must follow admitted event settlement")
        .expect("structured prompt task must join")
        .expect_err("bounded overlap overflow must invalidate event ownership");
    assert!(
        matches!((error).kind(), ErrorKind::Integration | ErrorKind::Closed),
        "overlap overflow must remain a terminal ownership failure: {error:?}"
    );
    probe.wait_for(1, history + 1, 0).await;
    assert_eq!(
        probe.stats.subscriptions.load(Ordering::SeqCst),
        1,
        "a Stop-class invariant failure must not be downgraded into reconnect/resume"
    );
    drop(workspace);
    let shutdown = tokio::time::timeout(Duration::from_secs(2), agent.shutdown()).await;
    probe.shutdown().await;
    let shutdown =
        shutdown.expect("OpenCode shutdown after event invalidation must remain bounded");
    assert!(
        shutdown.is_err(),
        "shutdown must preserve the event-owner invariant failure"
    );
}

#[tokio::test]
async fn opencode_command_stop_preserves_the_owned_terminal_error() {
    let probe = OpenCodeEventProbe::start(0).await;
    let agent = AgentBuilder::create(
        probe.config(Duration::from_secs(2), Duration::from_secs(3)),
        OpenCodeEventProbe::storage().await,
    )
    .build()
    .await
    .expect("Agent must build before command synchronization");
    let workspace = agent
        .new_workspace(OpenCodeEventProbe::workspace_request())
        .await
        .expect("command workspace");
    let conversation = workspace
        .new_conversation(
            NewConversation::new(None, None, Vec::new()).expect("conversation request"),
        )
        .await
        .expect("command conversation");
    let history = probe.stats.history.load(Ordering::SeqCst);
    *probe
        .stats
        .history_rows
        .lock()
        .expect("history fixture mutex") = vec![serde_json::json!({
        "id": "evt_probe_foreign_history",
        "aggregate_id": "ses_probe_foreign",
        "seq": 0,
        "type": "probe.future.1",
        "data": {}
    })];
    let close = tokio::time::timeout(Duration::from_secs(5), conversation.close()).await;
    let histories = probe.stats.history.load(Ordering::SeqCst);
    let subscriptions = probe.stats.subscriptions.load(Ordering::SeqCst);
    drop(workspace);
    let shutdown = tokio::time::timeout(Duration::from_secs(5), agent.shutdown()).await;
    probe.shutdown().await;

    let close = close
        .expect("command close remains bounded")
        .expect_err("the invalidated hierarchy cannot publish successful close");
    assert_eq!(close.kind(), ErrorKind::Closed);
    assert_eq!(
        histories,
        history + 1,
        "one command observes the foreign aggregate"
    );
    assert_eq!(
        subscriptions, 1,
        "terminal ownership failure cannot reconnect"
    );
    let shutdown = shutdown
        .expect("Agent shutdown remains bounded")
        .expect_err("shutdown retains the stopped event owner's error");
    assert_eq!(
        shutdown.kind(),
        ErrorKind::Conflict,
        "the foreign aggregate ownership failure must survive command delivery: {shutdown:?}"
    );
    assert_eq!(close.context().phase, ErrorPhase::Publication);
    let [command_failure] = close.supplementary() else {
        panic!("closed publication must retain the observed command failure: {close:?}");
    };
    assert_eq!(command_failure.kind(), ErrorKind::Conflict);
    assert_eq!(command_failure.detail(), shutdown.detail());
    assert_eq!(command_failure.http_status(), shutdown.http_status());
}

#[tokio::test]
async fn opencode_shutdown_rejects_late_prompt_task_admission() {
    let probe = OpenCodeEventProbe::start(0).await;
    let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
    let agent = AgentBuilder::create(
        probe.config(Duration::from_secs(1), Duration::from_secs(3)),
        faults.storage(),
    )
    .build()
    .await
    .expect("OpenCode Agent must build against the late-admission probe");
    let workspace = agent
        .new_workspace(OpenCodeEventProbe::workspace_request())
        .await
        .expect("OpenCode late-admission workspace must be created");
    let conversation = workspace
        .new_conversation(
            NewConversation::new(None, None, Vec::new())
                .expect("OpenCode late-admission conversation request must be valid"),
        )
        .await
        .expect("OpenCode late-admission conversation must be created");

    faults.block_next_workspace_load();
    let prompt = tokio::spawn(async move {
        conversation
            .prompt(OpenCodeEventProbe::structured_request(Duration::from_secs(
                8,
            )))
            .await
    });
    faults.wait_until_blocked().await;
    let shutdown = tokio::time::timeout(Duration::from_secs(5), agent.shutdown())
        .await
        .expect("OpenCode shutdown must remain bounded around admitted pre-task work");
    assert!(
        matches!(
            (shutdown).as_ref().map_err(SdkError::kind),
            Err(ErrorKind::Timeout)
        ),
        "shutdown must retain its hierarchy-quiescence deadline: {shutdown:?}"
    );

    faults.release();
    let error = tokio::time::timeout(Duration::from_secs(2), prompt)
        .await
        .expect("late prompt admission must be rejected after task sealing")
        .expect("late prompt observer task must join")
        .expect_err("sealed task admission cannot execute the prompt inline");
    assert!(
        matches!((error).kind(), ErrorKind::Closed),
        "sealed prompt ownership must publish closure: {error:?}"
    );
    assert_eq!(
        probe.stats.messages.load(Ordering::SeqCst),
        0,
        "rejected task admission must not dispatch a native prompt"
    );

    drop(workspace);
    probe.shutdown().await;
}

#[tokio::test]
async fn opencode_creation_cleanup_waits_for_exact_durable_handoff() {
    let probe = OpenCodeEventProbe::start(0).await;
    let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
    let config = probe.config(Duration::from_secs(1), Duration::from_secs(5));
    let agent = AgentBuilder::create(config.clone(), faults.storage())
        .build()
        .await
        .expect("OpenCode Agent must build before durable creation cleanup handoff");
    let agent_id = agent.id();
    let workspace = agent
        .new_workspace(OpenCodeEventProbe::workspace_request())
        .await
        .expect("OpenCode creation-cleanup workspace must be created");

    probe.invalidate_next_session_read();
    faults.arm(PromptCommitFault::UnavailableAfterCommit, 1);
    faults.fail_creation_cleanup_observation_until_released();
    let error = match workspace
        .new_conversation(
            NewConversation::new(None, None, Vec::new())
                .expect("OpenCode creation-cleanup conversation request must be valid"),
        )
        .await
    {
        Ok(_) => panic!("an invalid native session cannot publish a conversation"),
        Err(error) => error,
    };
    assert!(
        matches!((error).kind(), ErrorKind::Closed),
        "unobservable durable handoff must revoke the live runtime: {error:?}"
    );
    assert!(
        faults.creation_cleanup_observation_failures() > 1,
        "ambiguous handoff must retry only exact durable observation"
    );
    assert_eq!(
        probe.stats.deletes.load(Ordering::SeqCst),
        0,
        "native deletion cannot begin before the durable handoff is exactly confirmed"
    );
    assert!(
        matches!(
            (workspace.info().await).as_ref().map_err(SdkError::kind),
            Err(ErrorKind::Closed)
        ),
        "unobservable handoff must revoke the current hierarchy"
    );
    drop(workspace);
    drop(agent);
    faults.release_creation_cleanup_observation();

    let reopened = AgentBuilder::reopen(config, faults.storage(), agent_id)
        .build()
        .await
        .expect("the durable cleanup authority must reconcile on reopen");
    assert_eq!(
        probe.stats.deletes.load(Ordering::SeqCst),
        1,
        "reopen must settle the one durable cleanup authority exactly once"
    );
    reopened
        .shutdown()
        .await
        .expect("the cleanup-reconciled Agent must shut down");
    assert_eq!(
        probe.stats.deletes.load(Ordering::SeqCst),
        1,
        "settled cleanup must not be deleted again"
    );
    probe.shutdown().await;
}

#[tokio::test]
async fn opencode_unapplied_creation_cleanup_handoff_converges_in_one_reopen() {
    let probe = OpenCodeEventProbe::start(0).await;
    let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
    let config = probe.config(Duration::from_secs(1), Duration::from_secs(5));
    let agent = AgentBuilder::create(config.clone(), faults.storage())
        .build()
        .await
        .expect("OpenCode Agent must build before unapplied creation cleanup handoff");
    let agent_id = agent.id();
    let workspace = agent
        .new_workspace(OpenCodeEventProbe::workspace_request())
        .await
        .expect("OpenCode unapplied-handoff workspace must be created");

    probe.invalidate_session_reads(2);
    faults.arm(PromptCommitFault::Unavailable, 1);
    let error = match workspace
        .new_conversation(
            NewConversation::new(None, None, Vec::new())
                .expect("OpenCode unapplied-handoff conversation request must be valid"),
        )
        .await
    {
        Ok(_) => panic!("an invalid native session cannot publish a conversation"),
        Err(error) => error,
    };
    assert!(
        matches!((error).kind(), ErrorKind::Closed),
        "the failed durable handoff must revoke the live runtime: {error:?}"
    );
    assert_eq!(
        faults.state.prompt_commits.load(Ordering::SeqCst),
        1,
        "the failed handoff must make exactly one mutation attempt"
    );
    assert_eq!(
        probe.stats.deletes.load(Ordering::SeqCst),
        0,
        "native deletion cannot begin after an unapplied durable handoff"
    );
    drop(workspace);
    drop(agent);

    let reopened = AgentBuilder::reopen(config, faults.storage(), agent_id)
        .build()
        .await
        .expect("one reopen must hand off and settle the retained Creating authority");
    assert_eq!(
        faults.state.prompt_commits.load(Ordering::SeqCst),
        2,
        "reopen must retry the durable handoff exactly once"
    );
    assert_eq!(
        probe.stats.deletes.load(Ordering::SeqCst),
        1,
        "the confirmed handoff must settle native cleanup in the same reopen"
    );
    reopened
        .shutdown()
        .await
        .expect("the handoff-reconciled Agent must shut down");
    assert_eq!(
        probe.stats.deletes.load(Ordering::SeqCst),
        1,
        "settled creation cleanup must not be deleted again"
    );
    probe.shutdown().await;
}

#[tokio::test]
async fn opencode_ambiguous_workspace_delete_retries_exact_read_and_runs_postcommit_once() {
    let probe = OpenCodeEventProbe::start(0).await;
    let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
    let agent = AgentBuilder::create(
        probe.config(Duration::from_secs(1), Duration::from_secs(3)),
        faults.storage(),
    )
    .build()
    .await
    .expect("OpenCode Agent must build before ambiguous workspace deletion");
    let workspace = agent
        .new_workspace(OpenCodeEventProbe::workspace_request())
        .await
        .expect("OpenCode ambiguous-delete workspace must be created");
    let workspace_info = workspace
        .info()
        .await
        .expect("OpenCode ambiguous-delete workspace info must load");
    let conversation = workspace
        .new_conversation(
            NewConversation::new(None, None, Vec::new())
                .expect("OpenCode ambiguous-delete conversation request must be valid"),
        )
        .await
        .expect("OpenCode ambiguous-delete conversation must be created");
    let mut notifications = conversation
        .notification_stream()
        .expect("OpenCode ambiguous-delete receiver must be claimed");

    faults.obscure_next_workspace_delete_commit();
    faults.fail_next_workspace_delete_commit_observation();
    agent
        .delete_workspace(
            DeleteWorkspaceRequest::new(workspace_info.id().clone(), workspace_info.revision())
                .expect("OpenCode ambiguous workspace delete request must be valid"),
        )
        .await
        .expect("an exactly observed committed workspace delete must remain successful");
    assert_eq!(faults.ambiguous_workspace_deletes(), 1);
    assert_eq!(
        faults.workspace_delete_commit_attempts(),
        1,
        "ambiguous workspace deletion must make one mutation attempt"
    );
    assert_eq!(
        faults.workspace_delete_observation_failures(),
        1,
        "workspace deletion must retry its transient first exact-read failure"
    );
    assert_eq!(
        probe.stats.deletes.load(Ordering::SeqCst),
        1,
        "confirmed delete must run native postcommit cleanup exactly once"
    );
    assert!(matches!(
        (workspace.info().await).as_ref().map_err(SdkError::kind),
        Err(ErrorKind::Closed)
    ));
    assert!(matches!(
        (conversation.info().await).as_ref().map_err(SdkError::kind),
        Err(ErrorKind::Closed)
    ));
    let receiver_end = tokio::time::timeout(Duration::from_secs(2), notifications.recv())
        .await
        .expect("workspace delete must invalidate its notification receiver");
    assert!(
        receiver_end.is_none(),
        "workspace notification delivery must end after committed deletion: {receiver_end:?}"
    );

    agent
        .shutdown()
        .await
        .expect("the postcommit workspace-delete Agent must shut down");
    assert_eq!(
        probe.stats.deletes.load(Ordering::SeqCst),
        1,
        "shutdown must not replay settled workspace cleanup"
    );
    probe.shutdown().await;
}

#[tokio::test]
async fn opencode_unobservable_workspace_delete_revokes_live_ownership_until_reopen() {
    let probe = OpenCodeEventProbe::start(0).await;
    let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
    let config = probe.config(Duration::from_secs(1), Duration::from_secs(5));
    let agent = AgentBuilder::create(config.clone(), faults.storage())
        .build()
        .await
        .expect("OpenCode Agent must build before unobservable workspace deletion");
    let agent_id = agent.id();
    let workspace = agent
        .new_workspace(OpenCodeEventProbe::workspace_request())
        .await
        .expect("OpenCode unobservable-delete workspace must be created");
    let workspace_info = workspace
        .info()
        .await
        .expect("OpenCode unobservable-delete workspace info must load");
    let conversation = workspace
        .new_conversation(
            NewConversation::new(None, None, Vec::new())
                .expect("OpenCode unobservable-delete conversation request must be valid"),
        )
        .await
        .expect("OpenCode unobservable-delete conversation must be created");
    let mut notifications = conversation
        .notification_stream()
        .expect("OpenCode unobservable-delete receiver must be claimed");

    faults.obscure_next_workspace_delete_commit();
    faults.fail_workspace_delete_commit_observation_until_released();
    let error = agent
        .delete_workspace(
            DeleteWorkspaceRequest::new(workspace_info.id().clone(), workspace_info.revision())
                .expect("OpenCode unobservable workspace delete request must be valid"),
        )
        .await
        .expect_err("an unobservable workspace delete cannot publish success");
    assert!(
        matches!((error).kind(), ErrorKind::Integration),
        "unobservable deletion must preserve its integration classification: {error:?}"
    );
    assert_eq!(faults.ambiguous_workspace_deletes(), 1);
    assert_eq!(
        faults.workspace_delete_commit_attempts(),
        1,
        "recovery-owned workspace deletion must never replay its mutation"
    );
    assert!(faults.workspace_delete_observation_failures() > 1);
    assert_eq!(
        probe.stats.deletes.load(Ordering::SeqCst),
        0,
        "unconfirmed deletion must defer native postcommit cleanup to reopen"
    );
    assert!(matches!(
        (workspace.info().await).as_ref().map_err(SdkError::kind),
        Err(ErrorKind::Closed)
    ));
    assert!(matches!(
        (conversation.info().await).as_ref().map_err(SdkError::kind),
        Err(ErrorKind::Closed)
    ));
    assert!(
        tokio::time::timeout(Duration::from_secs(2), notifications.recv())
            .await
            .expect("unobservable delete must revoke notification delivery")
            .is_none()
    );
    drop(conversation);
    drop(workspace);
    drop(agent);
    faults.release_workspace_delete_commit_observation();

    let reopened = AgentBuilder::reopen(config, faults.storage(), agent_id)
        .build()
        .await
        .expect("durable reopen must decide the committed workspace delete");
    assert!(
        reopened
            .list_workspaces()
            .await
            .expect("reopened workspace inventory must load")
            .is_empty()
    );
    assert_eq!(
        probe.stats.deletes.load(Ordering::SeqCst),
        1,
        "reopen must settle the committed delete postcommit exactly once"
    );
    reopened
        .shutdown()
        .await
        .expect("workspace-delete recovery Agent must shut down");
    assert_eq!(probe.stats.deletes.load(Ordering::SeqCst), 1);
    probe.shutdown().await;
}

#[tokio::test]
async fn opencode_terminal_close_settles_a_late_ambiguous_commit() {
    let lifecycle_timeout = Duration::from_secs(5);
    let probe = OpenCodeEventProbe::start(0).await;
    let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
    let agent = AgentBuilder::create(
        probe.config(Duration::from_secs(10), lifecycle_timeout),
        faults.storage(),
    )
    .build()
    .await
    .expect("OpenCode Agent must build against the terminal-close probe");
    let workspace = agent
        .new_workspace(OpenCodeEventProbe::workspace_request())
        .await
        .expect("OpenCode terminal-close workspace must be created");
    let conversation = workspace
        .new_conversation(
            NewConversation::new(None, None, Vec::new())
                .expect("OpenCode terminal-close conversation request must be valid"),
        )
        .await
        .expect("OpenCode terminal-close conversation must be created");
    let conversation_id = conversation.id();
    let mut notifications = conversation
        .notification_stream()
        .expect("terminal-close receiver must be acquired before close");

    faults.arm(PromptCommitFault::BlockThenUnavailableAfterCommit, 1);
    let mut close = tokio::spawn(async move { conversation.close().await });
    faults.wait_until_blocked().await;
    assert!(
        tokio::time::timeout(lifecycle_timeout + Duration::from_millis(250), &mut close)
            .await
            .is_err(),
        "an admitted close transaction must retain ownership past its application deadline"
    );
    faults.fail_next_close_commit_observation();
    faults.release();
    tokio::time::timeout(Duration::from_secs(5), &mut close)
        .await
        .expect("late terminal close must remain bounded after commit release")
        .expect("terminal close observer task must join")
        .expect("an exact-confirmed terminal close must succeed even after deadline expiry");
    assert_eq!(
        faults.state.prompt_commits.load(Ordering::SeqCst),
        1,
        "ambiguous close reconciliation must never replay the mutation"
    );
    assert_eq!(
        faults.bound_session_observation_failures(),
        1,
        "late close reconciliation must retry its first transient exact-read failure"
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(5), notifications.recv())
            .await
            .expect("terminal receiver must observe postcommit closure")
            .is_none(),
        "the held receiver must end after exact close confirmation"
    );
    assert!(matches!(
        (workspace.get_conversation(conversation_id).await)
            .as_ref()
            .map_err(SdkError::kind),
        Err(ErrorKind::NotFound | ErrorKind::Closed)
    ));
    assert_eq!(probe.stats.subscriptions.load(Ordering::SeqCst), 1);
    assert_eq!(probe.stats.maximum.load(Ordering::SeqCst), 1);

    drop(workspace);
    agent
        .shutdown()
        .await
        .expect("OpenCode Agent must shut down after terminal close");
    probe.shutdown().await;
}

#[tokio::test]
async fn opencode_terminal_close_synchronizes_an_inflight_event_before_admission() {
    let probe = OpenCodeEventProbe::start(0).await;
    let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
    let agent = AgentBuilder::create(
        probe.config(Duration::from_secs(10), Duration::from_secs(5)),
        faults.storage(),
    )
    .build()
    .await
    .expect("OpenCode Agent must build against the close-synchronization probe");
    let workspace = agent
        .new_workspace(OpenCodeEventProbe::workspace_request())
        .await
        .expect("OpenCode close-synchronization workspace must be created");
    let conversation = workspace
        .new_conversation(
            NewConversation::new(None, None, Vec::new())
                .expect("OpenCode close-synchronization conversation request must be valid"),
        )
        .await
        .expect("OpenCode close-synchronization conversation must be created");

    let history = probe.stats.history.load(Ordering::SeqCst);
    let event_attempts = faults.event_commit_attempts();
    faults.block_next_event_commit();
    probe.emit_normal("evt_probe_before_terminal_close");
    faults.wait_until_event_commit_blocked().await;
    assert_eq!(faults.event_commit_attempts(), event_attempts + 1);

    probe.block_history();
    faults.arm(PromptCommitFault::BlockThenCommit, 1);
    let close = tokio::spawn(async move { conversation.close().await });
    faults.release_event_commit();
    probe.wait_for_history(history + 1).await;
    assert_eq!(
        faults.state.prompt_commits.load(Ordering::SeqCst),
        0,
        "close must not admit its transaction before event synchronization settles"
    );

    probe.release_history();
    faults.wait_until_blocked().await;
    assert_eq!(
        faults.state.prompt_commits.load(Ordering::SeqCst),
        1,
        "close must admit exactly one transaction after event synchronization"
    );
    faults.release();
    close
        .await
        .expect("terminal close task must join")
        .expect("terminal close must succeed after the in-flight event settles");

    drop(workspace);
    agent
        .shutdown()
        .await
        .expect("OpenCode Agent must shut down after synchronized terminal close");
    probe.shutdown().await;
}

#[tokio::test]
async fn opencode_terminal_close_waits_for_bounded_event_command_capacity() {
    let probe = OpenCodeEventProbe::start(0).await;
    let store = OpenCodeEventProbe::memory_store().await;
    let agent = AgentBuilder::create(
        probe.config(Duration::from_secs(5), Duration::from_secs(2)),
        AgentStorage::new(store.clone()),
    )
    .build()
    .await
    .expect("OpenCode Agent must build against the command-capacity probe");
    let workspace = agent
        .new_workspace(OpenCodeEventProbe::workspace_request())
        .await
        .expect("OpenCode command-capacity workspace must be created");
    let mut conversations = Vec::new();
    for _ in 0..19 {
        conversations.push(
            workspace
                .new_conversation(
                    NewConversation::new(None, None, Vec::new())
                        .expect("OpenCode command-capacity request must be valid"),
                )
                .await
                .expect("OpenCode command-capacity conversation must be created"),
        );
    }
    // The first native session owns the retained Question; eighteen closes
    // still exercise command pressure while one sibling remains open.
    let mut first = Some(conversations.remove(0));
    let sibling = conversations.pop().expect("one sibling remains open");
    let id = first.as_ref().unwrap().id();
    let mut notifications = first.as_ref().unwrap().notification_stream().unwrap();
    let mut sibling_notifications = sibling.notification_stream().unwrap();
    let mut closes = JoinSet::new();
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(20),
        async {
            probe.stats.retained_question_history.store(true, Ordering::SeqCst);
            probe.stats.pending_question.store(true, Ordering::SeqCst);
            *probe.stats.retained_identity_change.lock().unwrap() =
                Some(RetainedIdentityChange::MissingOrigin);
            probe.events.send(serde_json::json!({
                "directory": env!("CARGO_MANIFEST_DIR"),
                "payload": {"id": "evt_close_capacity_pending_question", "type": "question.asked", "properties": {
                    "id": "que_probe_pending", "sessionID": "ses_probe",
                    "questions": [{"question": "Continue?", "header": "Probe", "options": [], "multiple": false, "custom": true}]
                }}
            }).to_string()).unwrap();
            let interaction = tokio::time::timeout(Duration::from_secs(3), notifications.recv())
                .await.unwrap().unwrap().unwrap();
            assert!(matches!(interaction.kind(), ConversationNotificationKind::Interaction { .. }));
            let key = StateKey::new("agent_sdk_conversation_notifications_v1", id.as_str()).unwrap();
            let before = store.load(key.clone()).await.unwrap().unwrap();
            assert_eq!(before.record.value["lifecycle"], "open");
            assert_eq!(before.record.value["pending"].as_array().unwrap().len(), 1);
            let mut blocked = Box::pin(notifications.recv());
            assert!(tokio::time::timeout(Duration::from_millis(25), &mut blocked).await.is_err(),
                "an unresolved interaction must leave the following receive pending before Close");

            let history = probe.stats.history.load(Ordering::SeqCst);
            let inventory_responses = probe.stats.inventory_responses.load(Ordering::SeqCst);
            probe.block_history();
            let closing = first.take().unwrap();
            closes.spawn(async move { closing.close().await });
            probe.wait_for_history(history + 1).await;
            for conversation in conversations.drain(..) {
                closes.spawn(async move { conversation.close().await });
            }
            probe.wait_for_inventory_responses(inventory_responses + 18).await;
            let mut completed = 0;
            tokio::time::timeout(Duration::from_secs(5), async {
                while let Some(result) = closes.join_next().await {
                    let error = result.expect("terminal close task must join")
                        .expect_err("bounded event-command pressure must expire");
                    assert_eq!(error.kind(), ErrorKind::Timeout,
                        "bounded event-command pressure must expire as Timeout, never Busy: {error:?}");
                    assert_eq!(error.context().operation, Operation::CloseConversation);
                    assert_eq!(error.context().phase, ErrorPhase::EventSynchronization, "{error:?}");
                    completed += 1;
                }
            }).await.expect("all event-command capacity waiters must remain lifecycle bounded");
            assert_eq!(completed, 18);
            let failure = tokio::time::timeout(Duration::from_millis(250), &mut blocked)
                .await.expect("failed consuming Close must wake its already-blocked receiver before parent cleanup")
                .expect("failed Close must report receiver failure, not successful terminal EOF")
                .expect_err("failed Close must not deliver or acknowledge the unresolved interaction");
            drop(blocked);
            assert_eq!(failure.kind(), ErrorKind::Closed);
            assert_eq!(failure.context().operation, Operation::NotificationStream);
            assert_eq!(failure.context().phase, ErrorPhase::Lifecycle);
            assert!(tokio::time::timeout(Duration::from_millis(250), notifications.recv()).await.unwrap().is_none(),
                "the receiver failure is returned once");
            assert!(tokio::time::timeout(Duration::from_millis(25), sibling_notifications.recv()).await.is_err(),
                "one failed Close must not invalidate the sibling receiver");
            assert_eq!(store.load(key.clone()).await.unwrap(), Some(before.clone()),
                "receiver failure must preserve the exact unacknowledged durable backlog");
            assert!(probe.stats.pending_question.load(Ordering::SeqCst));
            assert_eq!(probe.stats.question_rejections.load(Ordering::SeqCst), 0);
            assert_eq!(probe.stats.deletes.load(Ordering::SeqCst), 0);

            probe.release_history();
            let reopened = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    match workspace.get_conversation(id.clone()).await {
                        Ok(reopened) => break reopened,
                        Err(error) if error.kind() == ErrorKind::Busy => tokio::task::yield_now().await,
                        Err(error) => panic!("failed precommit Close must preserve typed-id reopen: {error:?}"),
                    }
                }
            }).await.expect("the admitted event command must release its retained claim");
            assert_eq!(reopened.info().await.unwrap().id(), &id);
            assert_eq!(store.load(key).await.unwrap(), Some(before),
                "typed-id reopen must not erase or acknowledge the retained interaction");
            drop(reopened);
        },
    ).await;
    // Release every fixture gate and join every caller even when the receiver
    // assertion is red. This fixture settlement is not terminal Close success.
    probe.release_history();
    probe.stats.pending_question.store(false, Ordering::SeqCst);
    let _ = probe.events.send(
        serde_json::json!({
            "directory": env!("CARGO_MANIFEST_DIR"), "payload": {
                "id": "evt_close_capacity_cleanup_question", "type": "question.rejected",
                "properties": {"sessionID": "ses_probe", "requestID": "que_probe_pending"}
            }
        })
        .to_string(),
    );
    let mut cleanup = NativeCleanup::new(Duration::from_secs(5));
    cleanup
        .settle(async {
            while let Some(result) = closes.join_next().await {
                if let Err(error) = result.expect("Close caller must join during cleanup")
                    && error.kind() != ErrorKind::Timeout
                {
                    return Err(error);
                }
            }
            Ok::<(), SdkError>(())
        })
        .await;
    closes.abort_all();
    cleanup
        .settle(async {
            while let Some(result) = closes.join_next().await {
                if let Err(error) = result {
                    assert!(error.is_cancelled(), "Close caller panicked: {error}");
                }
            }
            Ok::<(), SdkError>(())
        })
        .await;
    drop(first);
    drop(conversations);
    drop(notifications);
    drop(sibling_notifications);
    drop(sibling);
    drop(workspace);
    cleanup.settle(agent.shutdown()).await;
    cleanup
        .settle(async {
            let stats = Arc::clone(&probe.stats);
            probe.shutdown().await;
            assert_eq!(stats.subscriptions.load(Ordering::SeqCst), 1);
            assert_eq!(stats.maximum.load(Ordering::SeqCst), 1);
            Ok::<(), SdkError>(())
        })
        .await;
    cleanup.finish(observed);
}

#[tokio::test]
async fn opencode_shutdown_joins_events_after_cleanup_deadline_expires() {
    let probe = OpenCodeEventProbe::start(0).await;
    let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
    let agent = AgentBuilder::create(
        probe.config(Duration::from_secs(10), Duration::from_secs(5)),
        faults.storage(),
    )
    .build()
    .await
    .expect("OpenCode Agent must build against the shutdown-cleanup probe");
    let workspace = agent
        .new_workspace(OpenCodeEventProbe::workspace_request())
        .await
        .expect("OpenCode shutdown-cleanup workspace must be created");
    let conversation = workspace
        .new_conversation(
            NewConversation::new(None, None, Vec::new())
                .expect("OpenCode shutdown-cleanup conversation request must be valid"),
        )
        .await
        .expect("OpenCode shutdown-cleanup conversation must be created");
    let history = probe.stats.history.load(Ordering::SeqCst);
    probe.wait_for(1, history, 1).await;

    faults.arm(PromptCommitFault::BlockThenCommit, 1);
    let close = tokio::spawn(async move { conversation.close().await });
    faults.wait_until_blocked().await;
    probe.block_history();
    faults.release();
    close
        .await
        .expect("terminal close task must join")
        .expect("terminal close must durably retain its blocked native cleanup");
    probe.wait_for_history(history + 1).await;
    probe.release_history();
    drop(workspace);

    faults.block_next_transaction_begin();
    let shutdown = agent.shutdown();
    tokio::pin!(shutdown);
    tokio::select! {
        () = faults.wait_until_transaction_begin_blocked() => {}
        result = &mut shutdown => {
            panic!("shutdown returned before the cleanup transaction stalled: {result:?}");
        }
    }
    let result = tokio::time::timeout(Duration::from_secs(8), &mut shutdown)
        .await
        .expect("OpenCode shutdown must remain bounded after cleanup exhausts its deadline");
    assert!(
        matches!(
            (result).as_ref().map_err(SdkError::kind),
            Err(ErrorKind::Timeout)
        ),
        "cleanup expiry must retain its deadline classification: {result:?}"
    );
    probe
        .wait_for(1, probe.stats.history.load(Ordering::SeqCst), 0)
        .await;
    assert_eq!(probe.stats.subscriptions.load(Ordering::SeqCst), 1);
    assert_eq!(probe.stats.maximum.load(Ordering::SeqCst), 1);

    faults.release_transaction_begin();
    probe.shutdown().await;
}

#[tokio::test]
async fn opencode_event_shutdown_closes_an_inflight_structured_observer() {
    let probe = OpenCodeEventProbe::start(0).await;
    let agent = AgentBuilder::create(
        probe.config(Duration::from_secs(2), Duration::from_secs(5)),
        OpenCodeEventProbe::storage().await,
    )
    .build()
    .await
    .expect("OpenCode Agent must build against the waiter-closure probe");
    let workspace = agent
        .new_workspace(OpenCodeEventProbe::workspace_request())
        .await
        .expect("OpenCode waiter-closure workspace must be created");
    let conversation = workspace
        .new_conversation(
            NewConversation::new(None, None, Vec::new())
                .expect("OpenCode waiter-closure conversation request must be valid"),
        )
        .await
        .expect("OpenCode waiter-closure conversation must be created");
    let history = probe.stats.history.load(Ordering::SeqCst);
    probe.block_history();
    probe.retain_active_structured_prompt();
    let prompt = tokio::spawn(async move {
        conversation
            .prompt(OpenCodeEventProbe::structured_request(Duration::from_secs(
                10,
            )))
            .await
    });
    probe.wait_for_history(history + 1).await;

    let shutdown = tokio::time::timeout(Duration::from_secs(7), agent.shutdown())
        .await
        .expect("event shutdown must remain bounded while synchronization is in flight");
    assert!(
        shutdown.is_err(),
        "the deliberately blocked native cancellation must exhaust shutdown"
    );
    let error = tokio::time::timeout(Duration::from_secs(2), prompt)
        .await
        .expect("closing event ownership must wake its structured observer")
        .expect("structured observer task must join")
        .expect_err("structured observer cannot survive event-owner shutdown");
    assert!(
        matches!(
            (error).kind(),
            ErrorKind::Closed | ErrorKind::Timeout | ErrorKind::Cancelled | ErrorKind::InvalidState
        ),
        "structured observer must preserve a terminal shutdown fact: {error:?}"
    );
    probe.wait_for(1, history + 1, 0).await;
    probe.release_history();
    drop(workspace);
    probe.shutdown().await;
}

#[tokio::test]
async fn opencode_prompt_stream_recovered_abort_outcome_retries_are_bounded() {
    for fault in [
        PromptPersistenceFault::AbortOutcomeOnce,
        PromptPersistenceFault::AbortOutcome,
    ] {
        let case = OpenCodeStreamCase::start().await;
        let mut stream = case
            .stream_with_admission_fault(PromptPersistenceFault::Admission)
            .await;
        let terminal = tokio::time::timeout(Duration::from_secs(7), stream.recv())
            .await
            .expect("foreground admission must relinquish its owner after its persistence bound");
        assert!(
            matches!(
                (terminal)
                    .as_ref()
                    .map(|result| result.as_ref().map_err(SdkError::kind)),
                Some(Err(ErrorKind::Timeout))
            ),
            "{terminal:?}"
        );
        assert!(stream.recv().await.is_none());
        let retained = case
            .faults
            .prompt_snapshot(&case.conversation.id())
            .await
            .unwrap();
        assert_eq!(retained.record.value["input_observed"], true);
        assert_eq!(
            retained
                .record
                .value
                .pointer("/phase/phase")
                .and_then(serde_json::Value::as_str),
            Some("dispatched_awaiting_admission")
        );
        assert!(case.probe.stats.native_active.load(Ordering::SeqCst));
        assert_eq!(case.probe.stats.prompt_responses.load(Ordering::SeqCst), 0);
        assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 0);
        case.faults
            .state
            .persistence_attempts
            .store(0, Ordering::SeqCst);
        case.faults.set_persistence_fault(fault);
        let started = tokio::time::Instant::now();
        let aborted = tokio::time::timeout(Duration::from_secs(7), case.conversation.abort())
            .await
            .expect("the recovery abort caller must retain its existing lifecycle deadline");
        assert_eq!(
            case.probe.stats.aborts.load(Ordering::SeqCst),
            1,
            "recovery persists the original native abort result without resending"
        );
        assert_eq!(
            case.probe.stats.messages.load(Ordering::SeqCst),
            1,
            "recovery cannot submit the failed foreground prompt again"
        );
        assert!(
            stream.recv().await.is_none(),
            "recovery cannot replay a stream terminal"
        );
        if fault.fails_once() {
            assert!(aborted.is_ok(), "{aborted:?}");
            assert_eq!(
                case.faults
                    .state
                    .persistence_attempts
                    .load(Ordering::SeqCst),
                1
            );
            case.settled().await;
            case.release();
            case.shutdown().await;
            continue;
        }
        assert!(
            matches!(
                (aborted).as_ref().map_err(SdkError::kind),
                Err(ErrorKind::Timeout)
            ),
            "{aborted:?}"
        );
        case.assert_persistence_retries_stopped(started).await;
        let unresolved = case
            .faults
            .prompt_snapshot(&case.conversation.id())
            .await
            .unwrap();
        assert_eq!(
            unresolved.record.value["message_id"], retained.record.value["message_id"],
            "failed recovery keeps the exact original generation"
        );
        assert_eq!(
            unresolved
                .record
                .value
                .pointer("/cancellation/state")
                .and_then(serde_json::Value::as_str),
            Some("dispatching"),
            "uncommitted native outcome cannot appear accepted"
        );
        assert_ne!(unresolved.record.value["waiter"], "delivered");
        assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 1);
        case.faults
            .set_persistence_fault(PromptPersistenceFault::None);
        case.release();
        case.revoke().await;
    }
}

#[tokio::test]
async fn opencode_response_loss_during_owned_cancel_claim_keeps_one_abort_and_joined_owner() {
    let request_timeout = Duration::from_secs(2);
    let case =
        OpenCodeStreamCase::start_with_prompt_limit(NonZeroUsize::new(1).unwrap(), request_timeout)
            .await;
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(20),
        async {
            let mut stream = case.stream().await;
            case.begin_text();
            case.delta(
                "evt_response_loss_claim_ready",
                OpenCodeStreamCase::ASSISTANT,
                "prt_stream_text",
                "text",
                "accepted",
            );
            assert!(matches!(
                OpenCodeStreamCase::next(&mut stream).await,
                Ok(PromptEvent::OutputDelta { .. })
            ));
            case.wait_for_admission().await;
            case.faults
                .set_persistence_fault(PromptPersistenceFault::CancelClaimCommitHeld);
            let mut abort = Box::pin(case.conversation.abort());
            tokio::select! {
                () = case.faults.wait_until_blocked() => {}
                result = &mut abort => panic!("Abort must retain the gated claim: {result:?}"),
            }
            let claimed = case
                .faults
                .prompt_snapshot(&case.conversation.id())
                .await
                .expect("the real claim commit must be durable before its receipt is released");
            assert_eq!(claimed.record.value["cancellation"]["state"], "reserved");
            assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 0);
            case.probe
                .stats
                .lose_stream_response
                .store(true, Ordering::SeqCst);
            case.release();
            case.http_responded().await;
            // The native transport has closed while the foreground is inside
            // an admitted StateStore commit. Keep that concrete receipt gated
            // through the native request's configured timeout, still within
            // the original longer cancellation deadline. The next HTTP poll
            // must observe Ready (EOF or elapsed request timeout); a socket
            // close alone would not prove that future's readiness.
            tokio::time::sleep(request_timeout).await;
            case.faults.release();
            abort
                .await
                .expect("the original accepted Abort must settle");
            let terminal = OpenCodeStreamCase::next(&mut stream)
                .await
                .expect_err("accepted cancellation cannot publish a completion");
            assert_eq!(terminal.kind(), ErrorKind::Cancelled);
            assert!(stream.recv().await.is_none());
            assert!(stream.recv().await.is_none());
            assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 1);
            assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 1);
            assert_eq!(case.probe.stats.prompt_responses.load(Ordering::SeqCst), 1);
            assert_eq!(
                case.faults
                    .state
                    .persistence_attempts
                    .load(Ordering::SeqCst),
                1,
                "the same committed claim is consumed once"
            );
            case.settled().await;
            case.conversation
                .info()
                .await
                .expect("the joined prompt owner must leave the conversation usable");
        },
    )
    .await;
    case.faults.release();
    case.finish_observed(observed).await;
}

#[tokio::test]
async fn opencode_recovered_abort_claim_timeout_keeps_supplementary_evidence() {
    OpenCodeStreamCase::verify_recovered_abort_evidence(Some(PromptPersistenceFault::CancelClaim))
        .await;
}

#[tokio::test]
async fn opencode_recovered_abort_dispatch_timeout_keeps_supplementary_evidence() {
    OpenCodeStreamCase::verify_recovered_abort_evidence(Some(
        PromptPersistenceFault::CancelDispatch,
    ))
    .await;
}

#[tokio::test]
async fn opencode_recovered_abort_read_timeout_keeps_supplementary_evidence() {
    OpenCodeStreamCase::verify_recovered_abort_evidence(None).await;
}

#[tokio::test]
async fn opencode_reopen_timeout_keeps_late_read_failure_supplementary() {
    use futures_util::FutureExt;

    let probe = OpenCodeEventProbe::start(0).await;
    let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
    let lifecycle = Duration::from_secs(3);
    let mut agent = Some(
        AgentBuilder::create(
            probe.config(Duration::from_secs(1), lifecycle),
            faults.storage(),
        )
        .build()
        .await
        .expect("reopen evidence fixture Agent must build"),
    );
    let mut reopen = None;
    let mut published = None;
    let outcome = std::panic::AssertUnwindSafe(async {
        let initial = agent.as_ref().unwrap();
        let identity = initial.id();
        let workspace = initial
            .new_workspace(OpenCodeEventProbe::workspace_request())
            .await
            .unwrap();
        let conversation = workspace
            .new_conversation(NewConversation::new(None, None, Vec::new()).unwrap())
            .await
            .unwrap();
        drop(conversation);
        drop(workspace);
        agent
            .take()
            .unwrap()
            .shutdown()
            .await
            .expect("idle Agent must shut down before reopen");

        faults.state.block_prompt_load.store(true, Ordering::SeqCst);
        reopen = Some(tokio::spawn({
            let config = probe.config(Duration::from_secs(1), lifecycle);
            let storage = faults.storage();
            let identity = identity.clone();
            async move {
                AgentBuilder::reopen(config, storage, identity)
                    .build()
                    .await
            }
        }));
        tokio::time::timeout(
            Duration::from_secs(5),
            faults.state.prompt_load_blocked.acquire(),
        )
        .await
        .expect("reopen must reach its storage read")
        .expect("prompt read observation gate must remain open")
        .forget();
        let early = tokio::time::timeout(
            lifecycle + Duration::from_millis(100),
            reopen.as_mut().unwrap(),
        )
        .await;
        if early.is_ok() {
            reopen.take();
        }
        match early {
            Err(_) => {}
            Ok(Ok(Ok(unexpected))) => {
                published = Some(unexpected);
                panic!("reopen cannot publish before the admitted read settles");
            }
            Ok(_) => panic!("the admitted reopen read must remain owned until release"),
        }
        faults.state.prompt_load_release.add_permits(1);
        let joined = tokio::time::timeout(Duration::from_secs(5), reopen.as_mut().unwrap()).await;
        if joined.is_ok() {
            reopen.take();
        }
        let error = match joined
            .expect("released read must settle reopen")
            .expect("reopen observer must join")
        {
            Err(error) => error,
            Ok(unexpected) => {
                published = Some(unexpected);
                panic!("expired reopen cannot publish an Agent");
            }
        };
        assert_eq!(probe.stats.messages.load(Ordering::SeqCst), 0);
        assert_eq!(probe.stats.aborts.load(Ordering::SeqCst), 0);
        published = Some(
            AgentBuilder::reopen(
                probe.config(Duration::from_secs(1), lifecycle),
                faults.storage(),
                identity,
            )
            .build()
            .await
            .expect("the failed reopen must release its identity claim"),
        );
        OpenCodeStreamCase::assert_reconciliation_timeout(
            &error,
            Operation::Reopen,
            ErrorPhase::StateRead,
        );
        let [rollback] = error.supplementary()[0].supplementary() else {
            panic!("the failed read must retain its rollback evidence: {error:?}");
        };
        assert_eq!(rollback.kind(), ErrorKind::Integration);
        assert_eq!(rollback.context().operation, Operation::Reopen);
        assert_eq!(rollback.context().phase, ErrorPhase::Cleanup);
        assert_eq!(rollback.detail(), Some(&ErrorDetail::ServiceUnavailable));
        assert!(rollback.cause().is_none());
        assert!(rollback.supplementary().is_empty());
    })
    .catch_unwind()
    .await;

    faults
        .state
        .block_prompt_load
        .store(false, Ordering::SeqCst);
    faults.state.prompt_load_release.add_permits(1);
    if let Some(task) = reopen.take()
        && let Ok(Ok(unexpected)) = task.await
    {
        published = Some(unexpected);
    }
    let initial_cleanup = match agent.take() {
        Some(initial) => initial.shutdown().await,
        None => Ok(()),
    };
    let published_cleanup = match published.take() {
        Some(reopened) => reopened.shutdown().await,
        None => Ok(()),
    };
    probe.shutdown().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
    initial_cleanup.expect("the initial Agent must shut down");
    published_cleanup.expect("the reopened Agent must shut down");
}

#[tokio::test]
async fn opencode_prompt_admission_commit_conflict_recomputes_without_replay() {
    OpenCodeStreamCase::verify_admission_persistence(
        PromptPersistenceFault::AdmissionCommitConflictOnce,
    )
    .await;
}

#[tokio::test]
async fn opencode_prompt_admission_ambiguous_uncommitted_transition_is_not_replayed() {
    OpenCodeStreamCase::verify_admission_persistence(
        PromptPersistenceFault::AdmissionCommitUnavailableOnce,
    )
    .await;
}

#[tokio::test]
async fn opencode_prompt_admission_lost_commit_receipt_is_exact_read_without_rewrite() {
    OpenCodeStreamCase::verify_admission_persistence(
        PromptPersistenceFault::AdmissionCommitReceiptLost,
    )
    .await;
}

impl OpenCodeStreamCase {
    async fn verify_lifecycle_conflict(fault: PromptPersistenceFault, rollback_failure: bool) {
        let case = Self::start().await;
        *case.probe.stats.abort_authority.lock().unwrap() = Some(Arc::clone(&case.faults.state));
        let terminal_transition = matches!(
            fault,
            PromptPersistenceFault::TerminalOnce
                | PromptPersistenceFault::TerminalCommitConflictOnce
                | PromptPersistenceFault::TerminalCommitUnavailableOnce
                | PromptPersistenceFault::TerminalCommitReceiptLost
        );
        let uncertain = matches!(
            fault,
            PromptPersistenceFault::TerminalCommitUnavailableOnce
                | PromptPersistenceFault::CancelClaimCommitUnavailableOnce
                | PromptPersistenceFault::CancelDispatchCommitUnavailableOnce
        );
        let receipt_lost = matches!(
            fault,
            PromptPersistenceFault::TerminalCommitReceiptLost
                | PromptPersistenceFault::CancelClaimCommitReceiptLost
                | PromptPersistenceFault::CancelDispatchCommitReceiptLost
        );
        let observed = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(15),
            async {
                let mut stream = case.stream().await;
                case.begin_text();
                case.delta(
                    "evt_lifecycle_conflict_ready",
                    Self::ASSISTANT,
                    "prt_stream_text",
                    "text",
                    "accepted",
                );
                assert!(matches!(
                    Self::next(&mut stream).await,
                    Ok(PromptEvent::OutputDelta { .. })
                ));
                case.wait_for_admission().await;
                case.faults.set_persistence_fault(fault);
                case.faults
                    .state
                    .persistence_conflict
                    .store(true, Ordering::SeqCst);
                case.faults
                    .state
                    .persistence_rollback_failure
                    .store(rollback_failure, Ordering::SeqCst);
                let result = if terminal_transition {
                    case.release();
                    tokio::time::timeout(Duration::from_secs(7), stream.recv())
                        .await
                        .expect("terminal persistence observation must remain bounded")
                } else {
                    let (abort, result) = tokio::time::timeout(Duration::from_secs(7), async {
                        tokio::join!(case.conversation.abort(), stream.recv())
                    })
                    .await
                    .expect("claim persistence and observer settlement must remain bounded");
                    abort.expect("the explicit abort must either retain its owner or recover under a fresh confirmed owner");
                    result
                }
                .expect("the prompt publishes one terminal outcome");
                assert_eq!(
                    case.faults
                        .state
                        .persistence_attempts
                        .load(Ordering::SeqCst),
                    1
                );
                assert_eq!(
                    case.probe.stats.messages.load(Ordering::SeqCst),
                    1,
                    "storage recomputation never resends native prompt work"
                );
                assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), usize::from(!terminal_transition));
                assert_eq!(case.probe.stats.aborts_without_dispatch_commit.load(Ordering::SeqCst), 0,
                    "native cancellation cannot arrive before a successful Dispatching commit");
                let writes = if terminal_transition {
                    case.faults.state.terminal_writes.load(Ordering::SeqCst)
                } else if matches!(
                    fault,
                    PromptPersistenceFault::CancelDispatchCommitUnavailableOnce
                        | PromptPersistenceFault::CancelDispatchCommitReceiptLost
                ) {
                    case.faults
                        .state
                        .cancellation_dispatch_writes
                        .load(Ordering::SeqCst)
                } else {
                    case.faults
                        .state
                        .cancellation_claim_writes
                        .load(Ordering::SeqCst)
                };
                if uncertain || rollback_failure {
                    if terminal_transition {
                        assert_eq!(writes, 1, "unconfirmed terminal authority cannot repeat the mutation");
                    } else {
                        let dispatch = case.faults.state.cancellation_dispatch_owners.lock().unwrap().clone();
                        let attempts = if fault == PromptPersistenceFault::CancelDispatchCommitUnavailableOnce {
                            dispatch.clone()
                        } else {
                            case.faults.state.cancellation_claim_owners.lock().unwrap().clone()
                        };
                        let [failed, recovered] = attempts.as_slice() else {
                            panic!("the failed owner may attempt once, followed by exactly one fresh recovery owner: {attempts:?}")
                        };
                        assert_eq!(failed["generation"], recovered["generation"]);
                        assert_ne!(failed["owner"], recovered["owner"], "same-owner mutation replay cannot substitute for recovery");
                        let receipts = case.faults.state.cancellation_dispatch_receipts.lock().unwrap().clone();
                        assert_eq!(receipts, vec![recovered.clone()], "only the fresh recovered owner's committed dispatch may authorize the first native abort");
                        if fault != PromptPersistenceFault::CancelDispatchCommitUnavailableOnce {
                            assert_eq!(dispatch, vec![recovered.clone()], "a failed reservation owner must never advance to dispatch");
                        }
                    }
                    let error =
                        result.expect_err("unconfirmed durable authority cannot publish success");
                    assert_eq!(error.context().operation, Operation::Prompt);
                    if uncertain {
                        assert_eq!(error.kind(), ErrorKind::Integration);
                        assert_eq!(error.context().phase, ErrorPhase::StateCommit);
                        assert_eq!(error.detail(), Some(&ErrorDetail::ServiceUnavailable));
                    } else {
                        assert_eq!(error.kind(), ErrorKind::Conflict);
                        assert_eq!(error.detail(), Some(&ErrorDetail::TransactionConflict));
                        assert!(
                            error
                                .supplementary()
                                .iter()
                                .any(|cleanup| cleanup.context().phase == ErrorPhase::Cleanup
                                    && cleanup.detail() == Some(&ErrorDetail::ServiceUnavailable)),
                            "failed rollback must remain visible: {error:?}"
                        );
                    }
                } else {
                    assert_eq!(
                        writes,
                        if receipt_lost { 1 } else { 2 },
                        "conflicts recompute; a confirmed lost receipt only reads back"
                    );
                    if terminal_transition {
                        let PromptEvent::Completed(response) =
                            result.expect("confirmed terminal persistence must complete")
                        else {
                            panic!("one authoritative completion is required")
                        };
                        assert_eq!(response.output(), "authoritative HTTP answer");
                    } else {
                        assert_eq!(
                            result
                                .expect_err("accepted abort cancels the prompt")
                                .kind(),
                            ErrorKind::Cancelled
                        );
                    }
                    case.settled().await;
                }
                assert!(stream.recv().await.is_none());
                if terminal_transition && (uncertain || rollback_failure) {
                    case.assert_repaired_native_idle().await;
                }
            },
        )
        .await;
        case.faults
            .state
            .persistence_rollback_failure
            .store(false, Ordering::SeqCst);
        case.finish_observed(observed).await;
    }

    async fn assert_repaired_native_idle(&self) {
        self.http_responded().await;
        let error = tokio::time::timeout(Duration::from_secs(7), self.conversation.abort())
            .await
            .expect("idle native repair must remain bounded")
            .expect_err(
                "idle native work without a durable response reports repaired invalid state",
            );
        assert_eq!(error.kind(), ErrorKind::InvalidState);
        assert_eq!(error.context().operation, Operation::Abort);
        assert_eq!(error.context().phase, ErrorPhase::Cancellation);
        assert_eq!(error.detail(), None);
        assert!(error.supplementary().is_empty());
        self.settled().await;
    }

    async fn finish_observed(
        self,
        observation: Result<Result<(), tokio::time::error::Elapsed>, Box<dyn std::any::Any + Send>>,
    ) {
        self.faults
            .set_persistence_fault(PromptPersistenceFault::None);
        self.faults
            .state
            .persistence_conflict
            .store(false, Ordering::SeqCst);
        let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
        cleanup.settle(self.conversation.abort()).await;
        self.release();
        let Self {
            agent,
            workspace,
            conversation,
            mut probe,
            ..
        } = self;
        drop(conversation);
        drop(workspace);
        cleanup.settle(agent.shutdown()).await;
        // Keep the server task outside the observation deadline. If graceful
        // stop cannot join, cancel and join the same owned task before exiting.
        let mut server = probe
            .task
            .take()
            .expect("the probe retains its server task");
        let mut joined = false;
        cleanup
            .settle(async {
                probe.shutdown().await;
                let result = (&mut server).await;
                joined = true;
                result.expect("the probe server must join");
                Ok::<(), SdkError>(())
            })
            .await;
        if !joined {
            server.abort();
            match server.await {
                Ok(()) => {}
                Err(error) if error.is_cancelled() => {}
                Err(error) => {
                    cleanup.settle(async { Err::<(), _>(error) }).await;
                }
            }
        }
        cleanup.finish(observation);
    }

    async fn verify_delivery_conflict(fault: PromptPersistenceFault) {
        let case = Self::start().await;
        let observed = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(15),
            async {
                case.probe
                    .stats
                    .malformed_stream_response
                    .store(true, Ordering::SeqCst);
                let mut stream = case.stream().await;
                case.begin_text();
                case.delta(
                    "evt_conflict_ready",
                    Self::ASSISTANT,
                    "prt_stream_text",
                    "text",
                    "accepted",
                );
                Self::next(&mut stream).await.unwrap();
                case.wait_for_admission().await;
                case.faults.set_persistence_fault(fault);
                case.faults
                    .state
                    .persistence_conflict
                    .store(true, Ordering::SeqCst);
                let started = tokio::time::Instant::now();
                case.release();
                let error = tokio::time::timeout(Duration::from_secs(7), stream.recv())
                    .await
                    .unwrap()
                    .expect("protocol failure must terminate observation")
                    .expect_err("malformed native output cannot succeed");
                assert_eq!(
                    error.kind(),
                    ErrorKind::Protocol,
                    "native failure stays primary: {error:?}"
                );
                assert!(stream.recv().await.is_none());
                if fault.fails_once() || fault == PromptPersistenceFault::None {
                    if fault == PromptPersistenceFault::None {
                        tokio::time::timeout(Duration::from_secs(3), async {
                            while case.probe.stats.aborts.load(Ordering::SeqCst) == 0 {
                                tokio::task::yield_now().await;
                            }
                        })
                        .await
                        .expect("native accepted cancellation must be dispatched");
                        assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 1);
                    }
                    assert_eq!(
                        case.faults
                            .state
                            .persistence_attempts
                            .load(Ordering::SeqCst),
                        usize::from(fault.fails_once())
                    );
                    case.settled().await;
                } else {
                    case.assert_persistence_retries_stopped(started).await;
                }
                assert_eq!(
                    case.probe.stats.messages.load(Ordering::SeqCst),
                    1,
                    "no native replay"
                );
                assert!(
                    case.probe.stats.aborts.load(Ordering::SeqCst)
                        <= usize::from(fault.fails_once() || fault == PromptPersistenceFault::None),
                    "settlement may cancel the original prompt once, never replay it"
                );
            },
        )
        .await;
        case.faults
            .set_persistence_fault(PromptPersistenceFault::None);
        case.faults
            .state
            .persistence_conflict
            .store(false, Ordering::SeqCst);
        // Failed persistence after a malformed terminal leaves response-loss state.
        // Exercise the same tracked Drop cleanup as the existing response-loss
        // contract, without treating a synthetic abort as terminal proof.
        let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
        cleanup
            .settle(async {
                case.revoke().await;
                Ok::<(), SdkError>(())
            })
            .await;
        cleanup.finish(observed);
    }

    async fn verify_dispatch_conflict(fault: PromptPersistenceFault) {
        let case = Self::start().await;
        let observed = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(15),
            async {
                case.faults.set_persistence_fault(fault);
                case.faults
                    .state
                    .persistence_conflict
                    .store(true, Ordering::SeqCst);
                let started = tokio::time::Instant::now();
                let mut stream = case
                    .conversation
                    .prompt_stream(Self::request())
                    .await
                    .unwrap();
                if fault.fails_once() {
                    case.probe.wait_for_messages(1).await;
                    case.begin_text();
                    case.release();
                    assert!(matches!(
                        Self::next(&mut stream).await,
                        Ok(PromptEvent::Completed(_))
                    ));
                    assert_eq!(
                        case.faults
                            .state
                            .persistence_attempts
                            .load(Ordering::SeqCst),
                        1
                    );
                    case.settled().await;
                } else {
                    let error = tokio::time::timeout(Duration::from_secs(7), stream.recv())
                        .await
                        .unwrap()
                        .expect("dispatch failure terminates observation")
                        .expect_err("uncommitted dispatch cannot succeed");
                    assert_eq!(error.kind(), ErrorKind::Timeout);
                    case.assert_persistence_retries_stopped(started).await;
                }
                assert!(stream.recv().await.is_none());
                assert_eq!(
                    case.probe.stats.messages.load(Ordering::SeqCst),
                    usize::from(fault.fails_once())
                );
                assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 0);
            },
        )
        .await;
        case.finish_observed(observed).await;
    }

    async fn verify_waiter_loss_conflict(fault: PromptPersistenceFault) {
        let case = Self::start().await;
        let mut caller = Some(tokio::spawn({
            let conversation = Arc::clone(&case.conversation);
            async move { conversation.prompt(Self::request()).await }
        }));
        let observed = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(15),
            async {
                case.probe.wait_for_messages(1).await;
                case.begin_text();
                case.wait_for_admission().await;
                case.faults.set_persistence_fault(fault);
                case.faults
                    .state
                    .persistence_conflict
                    .store(true, Ordering::SeqCst);
                let started = tokio::time::Instant::now();
                let caller = caller.take().unwrap();
                caller.abort();
                assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
                if fault.fails_once() {
                    tokio::time::timeout(Duration::from_secs(3), async {
                        loop {
                            if case
                                .faults
                                .prompt_snapshot(&case.conversation.id())
                                .await
                                .is_some_and(|snapshot| snapshot.record.value["waiter"] == "lost")
                            {
                                break;
                            }
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .expect("caller loss must durably settle after one conflict");
                    assert_eq!(
                        case.faults
                            .state
                            .persistence_attempts
                            .load(Ordering::SeqCst),
                        1
                    );
                    case.release();
                    case.settled().await;
                } else {
                    case.assert_persistence_retries_stopped(started).await;
                    let retained = case
                        .faults
                        .prompt_snapshot(&case.conversation.id())
                        .await
                        .unwrap();
                    assert_eq!(retained.record.value["waiter"], "waiting");
                }
                assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 1);
                assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 0);
            },
        )
        .await;
        if let Some(caller) = caller {
            caller.abort();
            let _ = caller.await;
        }
        case.finish_observed(observed).await;
    }
}

#[tokio::test]
async fn opencode_lifecycle_conflict_terminal_put_recomputes_without_native_replay() {
    OpenCodeStreamCase::verify_lifecycle_conflict(PromptPersistenceFault::TerminalOnce, false)
        .await;
}

#[tokio::test]
async fn opencode_lifecycle_conflict_terminal_failed_rollback_remains_visible() {
    OpenCodeStreamCase::verify_lifecycle_conflict(PromptPersistenceFault::TerminalOnce, true).await;
}

#[tokio::test]
async fn opencode_lifecycle_conflict_terminal_commit_recomputes_without_native_replay() {
    OpenCodeStreamCase::verify_lifecycle_conflict(
        PromptPersistenceFault::TerminalCommitConflictOnce,
        false,
    )
    .await;
}

#[tokio::test]
async fn opencode_lifecycle_conflict_terminal_uncertain_commit_does_not_repeat_mutation() {
    OpenCodeStreamCase::verify_lifecycle_conflict(
        PromptPersistenceFault::TerminalCommitUnavailableOnce,
        false,
    )
    .await;
}

#[tokio::test]
async fn opencode_lifecycle_conflict_terminal_lost_receipt_is_readback_only() {
    OpenCodeStreamCase::verify_lifecycle_conflict(
        PromptPersistenceFault::TerminalCommitReceiptLost,
        false,
    )
    .await;
}

#[tokio::test]
async fn opencode_lifecycle_conflict_cancel_claim_put_recomputes_before_one_abort() {
    OpenCodeStreamCase::verify_lifecycle_conflict(PromptPersistenceFault::CancelClaimOnce, false)
        .await;
}

#[tokio::test]
async fn opencode_lifecycle_conflict_cancel_claim_failed_rollback_remains_visible() {
    OpenCodeStreamCase::verify_lifecycle_conflict(PromptPersistenceFault::CancelClaimOnce, true)
        .await;
}

#[tokio::test]
async fn opencode_lifecycle_conflict_cancel_claim_commit_recomputes_before_one_abort() {
    OpenCodeStreamCase::verify_lifecycle_conflict(
        PromptPersistenceFault::CancelClaimCommitConflictOnce,
        false,
    )
    .await;
}

#[tokio::test]
async fn opencode_lifecycle_conflict_cancel_claim_uncertainty_requires_fresh_recovery_owner() {
    OpenCodeStreamCase::verify_lifecycle_conflict(
        PromptPersistenceFault::CancelClaimCommitUnavailableOnce,
        false,
    )
    .await;
}

#[tokio::test]
async fn opencode_lifecycle_conflict_cancel_claim_lost_receipt_is_readback_only() {
    OpenCodeStreamCase::verify_lifecycle_conflict(
        PromptPersistenceFault::CancelClaimCommitReceiptLost,
        false,
    )
    .await;
}

#[tokio::test]
async fn opencode_foreground_conflict_dispatch_retries_only_local_state() {
    OpenCodeStreamCase::verify_dispatch_conflict(PromptPersistenceFault::DispatchOnce).await;
}

#[tokio::test]
async fn opencode_response_loss_accepted_abort_settles_without_storage_fault() {
    OpenCodeStreamCase::verify_delivery_conflict(PromptPersistenceFault::None).await;
}

#[tokio::test]
async fn opencode_foreground_conflict_dispatch_permanent_conflict_keeps_deadline() {
    OpenCodeStreamCase::verify_dispatch_conflict(PromptPersistenceFault::Dispatch).await;
}

#[tokio::test]
async fn opencode_foreground_conflict_response_loss_retries_only_local_state() {
    OpenCodeStreamCase::verify_delivery_conflict(PromptPersistenceFault::ResponseLossOnce).await;
}

#[tokio::test]
async fn opencode_foreground_conflict_response_loss_permanent_conflict_keeps_deadline() {
    OpenCodeStreamCase::verify_delivery_conflict(PromptPersistenceFault::ResponseLoss).await;
}

#[tokio::test]
async fn opencode_foreground_conflict_delivery_claim_retries_only_local_state() {
    OpenCodeStreamCase::verify_delivery_conflict(PromptPersistenceFault::ResponseLossClaimOnce)
        .await;
}

#[tokio::test]
async fn opencode_foreground_conflict_delivery_claim_permanent_conflict_keeps_deadline() {
    OpenCodeStreamCase::verify_delivery_conflict(PromptPersistenceFault::ResponseLossClaim).await;
}

#[tokio::test]
async fn opencode_foreground_conflict_delivery_settlement_retries_only_local_state() {
    OpenCodeStreamCase::verify_delivery_conflict(PromptPersistenceFault::ResponseLossSettleOnce)
        .await;
}

#[tokio::test]
async fn opencode_foreground_conflict_delivery_settlement_permanent_conflict_keeps_deadline() {
    OpenCodeStreamCase::verify_delivery_conflict(PromptPersistenceFault::ResponseLossSettle).await;
}

#[tokio::test]
async fn opencode_foreground_conflict_caller_loss_retries_only_local_state() {
    OpenCodeStreamCase::verify_waiter_loss_conflict(PromptPersistenceFault::WaiterLossOnce).await;
}

#[tokio::test]
async fn opencode_foreground_conflict_caller_loss_permanent_conflict_keeps_deadline() {
    OpenCodeStreamCase::verify_waiter_loss_conflict(PromptPersistenceFault::WaiterLoss).await;
}

#[tokio::test]
async fn opencode_generation_conflict_with_rollback_failure_is_not_retried() {
    let case = OpenCodeStreamCase::start().await;
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(8),
        async {
            case.faults
                .set_persistence_fault(PromptPersistenceFault::DispatchOnce);
            case.faults
                .state
                .persistence_conflict
                .store(true, Ordering::SeqCst);
            case.faults
                .state
                .persistence_rollback_failure
                .store(true, Ordering::SeqCst);
            let mut stream = case
                .conversation
                .prompt_stream(OpenCodeStreamCase::request())
                .await
                .unwrap();
            let result = tokio::time::timeout(Duration::from_secs(3), stream.recv()).await;
            assert_eq!(
                case.probe.stats.messages.load(Ordering::SeqCst),
                0,
                "a failed rollback cannot be erased by retrying and dispatching native work"
            );
            let error = result
                .expect("local cleanup failure must settle observation")
                .expect("one terminal failure")
                .expect_err("failed cleanup cannot succeed");
            assert_eq!(error.kind(), ErrorKind::Conflict);
            assert_eq!(error.detail(), Some(&ErrorDetail::TransactionConflict));
            assert!(
                error
                    .supplementary()
                    .iter()
                    .any(|cleanup| cleanup.context().phase == ErrorPhase::Cleanup
                        && cleanup.detail() == Some(&ErrorDetail::ServiceUnavailable))
            );
            assert!(stream.recv().await.is_none());
            assert_eq!(
                case.faults
                    .state
                    .persistence_attempts
                    .load(Ordering::SeqCst),
                1
            );
        },
    )
    .await;
    case.faults
        .state
        .persistence_rollback_failure
        .store(false, Ordering::SeqCst);
    case.finish_observed(observed).await;
}

#[tokio::test]
async fn opencode_lifecycle_conflict_cancel_dispatch_uncertainty_requires_fresh_recovery_owner() {
    OpenCodeStreamCase::verify_lifecycle_conflict(
        PromptPersistenceFault::CancelDispatchCommitUnavailableOnce,
        false,
    )
    .await;
}

#[tokio::test]
async fn opencode_lifecycle_conflict_cancel_dispatch_lost_receipt_is_readback_only() {
    OpenCodeStreamCase::verify_lifecycle_conflict(
        PromptPersistenceFault::CancelDispatchCommitReceiptLost,
        false,
    )
    .await;
}

#[tokio::test]
async fn opencode_generation_unchanged_claim_rollback_failure_is_not_retried() {
    let case = OpenCodeStreamCase::start().await;
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(15),
        async {
            let mut stream = case.stream().await;
            case.begin_text();
            case.delta(
                "evt_noop_rollback_ready",
                OpenCodeStreamCase::ASSISTANT,
                "prt_stream_text",
                "text",
                "accepted",
            );
            assert!(matches!(
                OpenCodeStreamCase::next(&mut stream).await,
                Ok(PromptEvent::OutputDelta { .. })
            ));
            case.wait_for_admission().await;
            case.faults
                .state
                .fail_unchanged_prompt_rollback
                .store(true, Ordering::SeqCst);
            case.emit(
                "evt_noop_rollback_wakeup",
                "session.idle",
                serde_json::json!({"sessionID": "ses_probe"}),
            );
            tokio::time::timeout(
                Duration::from_secs(3),
                case.faults
                    .state
                    .unchanged_prompt_rollback_entered
                    .acquire(),
            )
            .await
            .expect("the active prompt must observe its unchanged cancellation claim")
            .unwrap()
            .forget();
            case.release();
            let error = tokio::time::timeout(Duration::from_secs(5), stream.recv())
                .await
                .unwrap()
                .expect("one terminal outcome")
                .expect_err("failed rollback cannot be erased by retrying the no-op claim");
            assert_eq!(error.kind(), ErrorKind::Integration);
            assert_eq!(error.context().phase, ErrorPhase::Cleanup);
            assert_eq!(error.detail(), Some(&ErrorDetail::ServiceUnavailable));
            assert_eq!(
                case.faults
                    .state
                    .unchanged_prompt_rollback_failures
                    .load(Ordering::SeqCst),
                1
            );
            assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 1);
            assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 0);
            assert!(stream.recv().await.is_none());
            case.assert_repaired_native_idle().await;
        },
    )
    .await;
    case.faults
        .state
        .fail_unchanged_prompt_rollback
        .store(false, Ordering::SeqCst);
    case.finish_observed(observed).await;
}

impl OpenCodeStreamCase {
    async fn verify_recovered_claim_receipt(fault: PromptPersistenceFault) {
        let case = Self::start().await;
        let observed = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(18),
            async {
                let mut stream = case
                    .stream_with_admission_fault(PromptPersistenceFault::Admission)
                    .await;
                let initial = tokio::time::timeout(Duration::from_secs(7), stream.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap_err();
                assert_eq!(initial.kind(), ErrorKind::Timeout);
                assert!(stream.recv().await.is_none());
                case.faults
                    .state
                    .persistence_attempts
                    .store(0, Ordering::SeqCst);
                case.faults.set_persistence_fault(fault);
                let recovered =
                    tokio::time::timeout(Duration::from_secs(7), case.conversation.abort())
                        .await
                        .expect("recovered claim settlement must retain its lifecycle deadline");
                assert_eq!(
                    case.faults
                        .state
                        .persistence_attempts
                        .load(Ordering::SeqCst),
                    1
                );
                assert_eq!(
                    case.faults
                        .state
                        .cancellation_claim_writes
                        .load(Ordering::SeqCst),
                    1,
                    "a recovered claim's uncertain commit cannot authorize another mutation"
                );
                assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 1);
                if fault == PromptPersistenceFault::CancelClaimCommitReceiptLost {
                    recovered.expect("exact same-owner committed claim permits cancellation");
                    assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 1);
                } else {
                    let error = recovered
                        .expect_err("unconfirmed recovered claim cannot dispatch cancellation");
                    assert_eq!(error.kind(), ErrorKind::Integration);
                    assert_eq!(error.context().phase, ErrorPhase::StateCommit);
                    assert_eq!(error.detail(), Some(&ErrorDetail::ServiceUnavailable));
                    assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 0);
                }
            },
        )
        .await;
        case.finish_observed(observed).await;
    }
}

#[tokio::test]
async fn opencode_recovered_claim_uncertain_commit_does_not_authorize_abort() {
    OpenCodeStreamCase::verify_recovered_claim_receipt(
        PromptPersistenceFault::CancelClaimCommitUnavailableOnce,
    )
    .await;
}

#[tokio::test]
async fn opencode_recovered_claim_lost_receipt_is_readback_only() {
    OpenCodeStreamCase::verify_recovered_claim_receipt(
        PromptPersistenceFault::CancelClaimCommitReceiptLost,
    )
    .await;
}

impl OpenCodeEventProbe {
    async fn verify_cancelled_question_close(case: RetainedCloseCase) {
        let (retain_native_question, permission, change) = match case {
            RetainedCloseCase::QuestionGone => (false, false, None),
            RetainedCloseCase::Question => (true, false, None),
            RetainedCloseCase::Permissions => (true, true, None),
            RetainedCloseCase::OrdinaryQuestionObserved
            | RetainedCloseCase::OrdinaryQuestionClosed
            | RetainedCloseCase::OrdinaryQuestionCloseObserverLost => (
                true,
                false,
                Some(RetainedIdentityChange::OrdinaryCompletion),
            ),
            RetainedCloseCase::ChangedQuestion(change) => (true, false, Some(change)),
            RetainedCloseCase::ChangedPermissions(change) => (true, true, Some(change)),
        };
        // This is a same-hierarchy causal control, not process-loss/reopen proof.
        // Its first cancelled generation remains in native immutable history while
        // the second generation completes and Conversation::close synchronizes.
        let probe = OpenCodeEventProbe::start(0).await;
        probe
            .stats
            .retained_question_history
            .store(true, Ordering::SeqCst);
        probe
            .stats
            .retain_question_after_abort
            .store(retain_native_question, Ordering::SeqCst);
        probe
            .stats
            .retained_permissions
            .store(permission, Ordering::SeqCst);
        let faults = PromptFaultStore::new(OpenCodeEventProbe::memory_store().await);
        // This current-thread test owns only its protocol peer and caller Store
        // before Agent construction. Relative drainage checks live work, not
        // private task identities or consumption of every native JoinHandle.
        let runtime_baseline = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        let mut agent = None;
        let mut workspace = None;
        let mut conversation = None;
        let mut secondary_workspace = None;
        let mut secondary_conversation = None;
        let mut closes = JoinSet::new();
        let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(30),
        async {
            agent = Some(
                AgentBuilder::create(
                    probe.config(Duration::from_secs(15), Duration::from_secs(5)),
                    faults.storage(),
                )
                .build()
                .await
                .expect("retained-history Agent must build"),
            );
            workspace = Some(
                agent
                    .as_ref()
                    .unwrap()
                    .new_workspace(OpenCodeEventProbe::workspace_request())
                    .await
                    .unwrap(),
            );
            conversation = Some(
                workspace
                    .as_ref()
                    .unwrap()
                    .new_conversation(NewConversation::new(None, None, Vec::new()).unwrap())
                    .await
                    .unwrap(),
            );
            if matches!(change, Some(RetainedIdentityChange::MixedPermissionDirectory)) {
                *probe.stats.retained_identity_change.lock().unwrap() = change;
                let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests");
                let context = WorkspaceContext::new(
                    directory.clone(),
                    vec![WorkspaceRoot::new(directory, WorkspaceRootAccess::ReadWrite).unwrap()],
                    WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::FullAccess, WorkspaceNetworkAccess::Enabled),
                ).unwrap();
                secondary_workspace = Some(agent.as_ref().unwrap().new_workspace(
                    NewWorkspace::new("other-permission-directory", context, WorkspaceDefaults::empty()).unwrap()
                ).await.unwrap());
                secondary_conversation = Some(secondary_workspace.as_ref().unwrap().new_conversation(
                    NewConversation::new(None, None, Vec::new()).unwrap()
                ).await.unwrap());
                let directories = probe.stats.sessions.lock().unwrap().values()
                    .map(|session| session.directory.clone()).collect::<BTreeSet<_>>();
                assert_eq!(directories, BTreeSet::from([
                    env!("CARGO_MANIFEST_DIR").to_owned(),
                    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests").to_str().unwrap().to_owned(),
                ]), "the routing guard requires two actual mapped native directories");
            }
            let current = conversation.as_ref().unwrap();
            let mut notifications = current.notification_stream().unwrap();
            let mut first = current
                .prompt_stream(PromptRequest::new("ask one question").unwrap())
                .await
                .unwrap();
            let interaction = loop {
                tokio::select! {
                    item = notifications.recv() => {
                        let item = item.expect("receiver must remain open").unwrap();
                        if matches!(item.kind(), ConversationNotificationKind::Interaction { .. }) {
                            break item;
                        }
                    }
                    result = first.recv() => {
                        panic!("the gated question cannot terminalize before cancellation: {result:?}");
                    }
                }
            };
            if permission {
                // Portable interactions are head-of-line; the second approval
                // cannot be delivered before the first settles. Observe both
                // admitted durable native requests through the caller Store.
                loop {
                    let records = faults.inner.list(StateListRequest::new(OPENCODE_CONTINUATION_NAMESPACE).unwrap()).await.unwrap();
                    if records.records.len() == 2 { break; }
                    tokio::task::yield_now().await;
                }
            }
            assert_eq!(probe.stats.pending_question.load(Ordering::SeqCst), !permission);
            assert_eq!(probe.stats.pending_permissions.lock().unwrap().len(), if permission { 2 } else { 0 });
            *probe.stats.retained_identity_change.lock().unwrap() = change;
            if matches!(change, Some(RetainedIdentityChange::ForeignPermission)) {
                let mut pending = probe.stats.pending_permissions.lock().unwrap();
                let mut foreign = pending[0].clone();
                foreign["id"] = serde_json::json!("per_retained_foreign");
                foreign["tool"]["callID"] = serde_json::json!("call_foreign_generation");
                foreign["tool"]["messageID"] = serde_json::json!("msg_000000000099abcdefghijklmn");
                pending.push(foreign);
            }
            if matches!(case, RetainedCloseCase::OrdinaryQuestionObserved | RetainedCloseCase::OrdinaryQuestionClosed | RetainedCloseCase::OrdinaryQuestionCloseObserverLost) {
                // Ordinary native completion is not cancellation authority. No
                // public abort or close has been invoked at this observation.
                probe.stats.native_active.store(false, Ordering::SeqCst);
                Self::retained_question_abort(&probe.stats);
                probe.stats.prompt_release.add_permits(1);
                let successful = loop {
                    if let Some(snapshot) = faults.prompt_snapshot(&current.id()).await
                        && snapshot.record.value["phase"]["phase"] == "terminal"
                        && snapshot.record.value["phase"]["outcome"]["outcome"] == "success"
                    {
                        let generation = snapshot.record.value["message_id"].as_str()
                            .expect("the committed prompt must retain its generation identity");
                        assert!(!generation.is_empty());
                        break generation.to_owned();
                    }
                    tokio::task::yield_now().await;
                };
                assert_eq!(probe.stats.messages.load(Ordering::SeqCst), 1);
                assert_eq!(probe.stats.aborts.load(Ordering::SeqCst), 0);
                if matches!(case, RetainedCloseCase::OrdinaryQuestionObserved) {
                    let history = probe.stats.history.load(Ordering::SeqCst);
                    let scans = probe.stats.pending_scans.load(Ordering::SeqCst);
                    let started = tokio::time::Instant::now();
                    probe.disconnect_current();
                    probe.wait_for_synchronization(history + 1, scans + 2).await;
                    // This is an effect-safety interval across the existing
                    // reconciliation budget; route counts are not success proof.
                    tokio::time::sleep_until(started + Duration::from_secs(6)).await;
                    assert_eq!(probe.stats.question_rejections.load(Ordering::SeqCst), 0);
                    assert_eq!(probe.stats.question_replies.load(Ordering::SeqCst), 0);
                    assert!(probe.stats.pending_question.load(Ordering::SeqCst));
                    let terminal = faults.prompt_snapshot(&current.id()).await.unwrap();
                    assert_eq!(terminal.record.value["message_id"], successful);
                    assert_eq!(terminal.record.value["phase"]["phase"], "terminal");
                    assert_eq!(terminal.record.value["phase"]["outcome"]["outcome"], "success");
                    current.prompt(PromptRequest::new("must not dispatch over pending work").unwrap())
                        .await.expect_err("unresolved ordinary interaction still fences prompt admission");
                    assert_eq!(probe.stats.messages.load(Ordering::SeqCst), 1);
                    assert_eq!(probe.stats.aborts.load(Ordering::SeqCst), 0);
                    return;
                }

                // Explicit terminal Close has independent selected-route authority
                // over an unchanged owned Pending request, even after Success.
                *probe.stats.retained_rejection.lock().unwrap() = RetainedRejection::Held;
                let id = current.id();
                let closing = conversation.take().unwrap();
                let close_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
                let close_observer = closes.spawn(async move { closing.close().await });
                tokio::select! {
                    biased;
                    permit = probe.stats.rejection_entered.acquire() => permit.unwrap().forget(),
                    result = closes.join_next() => panic!("explicit close must reject its owned Pending question: {result:?}"),
                }
                let terminal = faults.prompt_snapshot(&id).await.unwrap();
                assert_eq!(terminal.record.value["message_id"], successful);
                assert_eq!(terminal.record.value["phase"]["phase"], "terminal");
                assert_eq!(terminal.record.value["phase"]["outcome"]["outcome"], "success",
                    "interaction cleanup must not relabel an already committed prompt outcome");
                assert_eq!(probe.stats.question_rejections.load(Ordering::SeqCst), 1);
                assert_eq!(probe.stats.messages.load(Ordering::SeqCst), 1);
                assert_eq!(probe.stats.aborts.load(Ordering::SeqCst), 0);
                if matches!(case, RetainedCloseCase::OrdinaryQuestionCloseObserverLost) {
                    close_observer.abort();
                    let stopped = closes.join_next().await.unwrap()
                        .expect_err("only the public Close observer must be cancelled");
                    assert!(stopped.is_cancelled());
                    match workspace.as_ref().unwrap().get_conversation(id.clone()).await {
                        Ok(unexpected) => {
                            conversation = Some(unexpected);
                            panic!("native cleanup must retain conversation ownership after Close observer loss");
                        }
                        Err(error) => assert_eq!(error.kind(), ErrorKind::Busy),
                    }
                    assert!(probe.stats.pending_question.load(Ordering::SeqCst));
                    assert_eq!(probe.stats.question_rejections.load(Ordering::SeqCst), 1);
                    assert_eq!(probe.stats.messages.load(Ordering::SeqCst), 1);
                    assert_eq!(probe.stats.aborts.load(Ordering::SeqCst), 0);
                    probe.stats.rejection_release.add_permits(1);
                    // Native preparation was admitted, but terminal Close commit
                    // was not. Its retained claim releases only after cleanup.
                    let reopened = tokio::time::timeout_at(close_deadline, async {
                        loop {
                            match workspace.as_ref().unwrap().get_conversation(id.clone()).await {
                                Ok(reopened) => break reopened,
                                Err(error) if error.kind() == ErrorKind::Busy => tokio::task::yield_now().await,
                                Err(error) => panic!("pre-commit observer loss must remain reopenable after native settlement: {error:?}"),
                            }
                        }
                    }).await.expect("retained native cleanup must settle within its original lifecycle deadline");
                    conversation = Some(reopened);
                    assert!(!probe.stats.pending_question.load(Ordering::SeqCst));
                    assert_eq!(probe.stats.question_rejections.load(Ordering::SeqCst), 1);
                    assert_eq!(probe.stats.messages.load(Ordering::SeqCst), 1);
                    assert_eq!(probe.stats.aborts.load(Ordering::SeqCst), 0);
                    let closing = conversation.take().unwrap();
                    closes.spawn(async move { closing.close().await });
                } else {
                    probe.stats.rejection_release.add_permits(1);
                }
                closes.join_next().await.unwrap().expect("close observer must join")
                    .expect("explicit close must settle its owned ordinary-success question");
                assert!(!probe.stats.pending_question.load(Ordering::SeqCst));
                assert_eq!(probe.stats.question_rejections.load(Ordering::SeqCst), 1);
                assert_eq!(probe.stats.question_replies.load(Ordering::SeqCst), 0);
                assert_eq!(probe.stats.messages.load(Ordering::SeqCst), 1);
                while let Some(item) = notifications.recv().await {
                    assert!(!matches!(item.unwrap().kind(), ConversationNotificationKind::Interaction { .. }),
                        "the original interaction cannot be replayed during terminal close");
                }
                return;
            }
            let aborted = current.abort().await;
            if change.is_some() {
                // A cancelled portable interaction is not authority to reject a
                // changed origin/payload or a collateral foreign permission.
                // Consume the original failed close; do not retry it for luck.
                let id = current.id();
                let closed = conversation.take().unwrap().close().await;
                let prompt = faults.prompt_snapshot(&id).await.map(|snapshot| serde_json::json!({
                    "phase": snapshot.record.value["phase"], "cancellation": snapshot.record.value["cancellation"], "waiter": snapshot.record.value["waiter"]
                }));
                let states = faults.inner.list(StateListRequest::new(OPENCODE_CONTINUATION_NAMESPACE).unwrap()).await.unwrap().records.into_iter().map(|snapshot| snapshot.record.value["state"].clone()).collect::<Vec<_>>();
                eprintln!("native identity guard {change:?}: abort={aborted:?}, close={closed:?}, prompt={prompt:?}, continuation_states={states:?}");
                assert!(aborted.is_err() || closed.is_err(), "ambiguous native identity must remain an honest failure");
                assert_eq!(probe.stats.question_rejections.load(Ordering::SeqCst), 0);
                assert_eq!(probe.stats.permission_rejections.load(Ordering::SeqCst), 0);
                assert_eq!(probe.stats.question_replies.load(Ordering::SeqCst), 0);
                assert_eq!(probe.stats.messages.load(Ordering::SeqCst), 1);
                assert!(probe.stats.aborts.load(Ordering::SeqCst) <= 1, "the guarded path cannot repeat session cancellation");
                if matches!(change, Some(RetainedIdentityChange::MixedPermissionDirectory)) {
                    assert_eq!(*probe.stats.mixed_permission_directories.lock().unwrap(), BTreeSet::from([
                        env!("CARGO_MANIFEST_DIR").to_owned(),
                        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests").to_str().unwrap().to_owned(),
                    ]), "both distinct routed inventories must actually expose the collateral peers");
                    let pending = probe.stats.pending_permissions.lock().unwrap();
                    assert_eq!(pending.len(), 2);
                    assert!(pending.iter().all(|request| request["sessionID"] == "ses_probe"));
                    assert_eq!(pending.iter().map(|request| request["id"].as_str().unwrap()).collect::<BTreeSet<_>>(),
                        BTreeSet::from(["per_retained_0", "per_retained_1"]));
                }
                if matches!(change, Some(RetainedIdentityChange::ForeignPermission)) {
                    assert!(probe.stats.pending_permissions.lock().unwrap().iter().any(|request| request["id"] == "per_retained_foreign"), "the native session-wide side effect must not consume the foreign request");
                }
                return;
            }
            aborted.expect("native question abort must settle");
            let cancelled = first
                .recv()
                .await
                .expect("cancelled prompt must publish one terminal result")
                .expect_err("accepted abort cannot complete the prompt successfully");
            assert_eq!(cancelled.kind(), ErrorKind::Cancelled);
            assert!(first.recv().await.is_none());
            if permission {
                let pending = probe.stats.pending_permissions.lock().unwrap().len();
                let rejected = probe.stats.permission_rejections.load(Ordering::SeqCst);
                assert!(matches!((pending, rejected), (2, 0) | (0, 1)), "both original permission requests persist until one session-wide reject: {pending}/{rejected}");
            } else if retain_native_question {
                assert_eq!(
                    usize::from(probe.stats.pending_question.load(Ordering::SeqCst))
                        + probe.stats.question_rejections.load(Ordering::SeqCst),
                    1,
                    "the exact native request remains pending until its one selected rejection"
                );
            } else {
                assert!(!probe.stats.pending_question.load(Ordering::SeqCst));
                assert_eq!(probe.stats.question_rejections.load(Ordering::SeqCst), 0);
            }
            assert_eq!(probe.stats.aborts.load(Ordering::SeqCst), 1);
            let resolution = current
                .resolve_notification(
                    interaction.id().clone(),
                    if permission { NotificationResolution::Allow } else { NotificationResolution::Submit {
                        content: serde_json::json!({"answers": [["Yes"]]}),
                    } },
                )
                .await
                .expect_err("abort must settle the first question before reuse");
            assert!(matches!(
                resolution.kind(),
                ErrorKind::Conflict | ErrorKind::NotFound | ErrorKind::Closed
            ));
            let response = current
                .prompt(PromptRequest::new("return the reuse marker").unwrap())
                .await
                .expect("second prompt must complete after question cancellation");
            assert_eq!(response.output(), "RETAINED_HISTORY_REUSE");
            assert_eq!(probe.stats.messages.load(Ordering::SeqCst), 2);
            assert_eq!(probe.stats.history_rows.lock().unwrap().len(), if permission { 11 } else { 9 });
            let history = probe.stats.history.load(Ordering::SeqCst);
            probe
                .stats
                .retained_question_close_history
                .store(true, Ordering::SeqCst);
            faults.arm(PromptCommitFault::BlockThenCommit, 1);
            let closing = conversation.take().unwrap();
            closes.spawn(async move { closing.close().await });
            tokio::select! {
                biased;
                result = closes.join_next() => {
                    panic!("terminal close failed before durable admission: {result:?}");
                }
                permit = faults.state.blocked.acquire() => {
                    permit.expect("close commit entry must remain owned").forget();
                }
            }
            assert_eq!(faults.state.prompt_commits.load(Ordering::SeqCst), 1);
            assert!(probe.stats.history.load(Ordering::SeqCst) > history);
            assert!(
                probe.stats.retained_question_close_rows.load(Ordering::SeqCst) > 0,
                "explicit close must reconcile retained history overlapping delayed sync frames"
            );
            faults.release();
            closes
                .join_next()
                .await
                .unwrap()
                .expect("terminal close owner must join")
                .expect("retained question history must not stall terminal close");
            assert_eq!(faults.state.prompt_commits.load(Ordering::SeqCst), 1);
            while let Some(item) = notifications.recv().await {
                let item = item.expect("remaining close notifications must be valid");
                assert!(
                    !matches!(item.kind(), ConversationNotificationKind::Interaction { .. }),
                    "the settled first-generation interaction cannot replay after close"
                );
            }
            assert_eq!(probe.stats.messages.load(Ordering::SeqCst), 2);
            assert_eq!(probe.stats.aborts.load(Ordering::SeqCst), 1);
            assert_eq!(probe.stats.subscriptions.load(Ordering::SeqCst), 1);
            assert_eq!(probe.stats.maximum.load(Ordering::SeqCst), 1);
            assert!(!probe.stats.pending_question.load(Ordering::SeqCst));
            assert_eq!(probe.stats.question_rejections.load(Ordering::SeqCst), usize::from(retain_native_question && !permission));
            assert!(probe.stats.pending_permissions.lock().unwrap().is_empty());
            assert_eq!(probe.stats.permission_rejections.load(Ordering::SeqCst), usize::from(permission));
            assert_eq!(probe.stats.question_replies.load(Ordering::SeqCst), 0);
        },
    )
    .await;

        // Release every fixture gate even when the body times out or panics. A
        // consuming close remains in this JoinSet until joined, including cleanup.
        *probe.stats.retained_identity_change.lock().unwrap() = None;
        if change.is_some() {
            probe.stats.pending_question.store(false, Ordering::SeqCst);
            probe.stats.pending_permissions.lock().unwrap().clear();
        }
        if matches!(change, Some(RetainedIdentityChange::OrdinaryCompletion)) {
            // The guard has already recorded its unchanged native effect counts
            // and failed public observation. A later native rejection supplies
            // cleanup authority for the still-pending ordinary-success request;
            // merely removing its inventory entry does not invent acceptance.
            let _ = probe.events.send(
                serde_json::json!({
                    "directory": env!("CARGO_MANIFEST_DIR"), "payload": {
                        "id": "evt_guard_cleanup_question_rejected", "type": "question.rejected",
                        "properties": {"sessionID": "ses_probe", "requestID": "que_probe_pending"}
                    }
                })
                .to_string(),
            );
        }
        faults.release();
        probe.release_history();
        probe.stats.prompt_release.add_permits(2);
        probe.stats.rejection_release.add_permits(2);
        let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
        cleanup
            .settle(async {
                while let Some(result) = closes.join_next().await {
                    result.expect("retained terminal close task must join")?;
                }
                Ok::<(), SdkError>(())
            })
            .await;
        closes.abort_all();
        cleanup
            .settle(async {
                while let Some(result) = closes.join_next().await {
                    match result {
                        Ok(result) => result?,
                        Err(error) => {
                            assert!(error.is_cancelled(), "close owner panicked: {error}")
                        }
                    }
                }
                Ok::<(), SdkError>(())
            })
            .await;
        if let Some(current) = conversation.take() {
            cleanup.settle(current.abort()).await;
            cleanup.settle(current.close()).await;
        }
        if let Some(current) = secondary_conversation.take() {
            cleanup
                .settle(async {
                    match current.close().await {
                        Err(error)
                            if matches!(
                                change,
                                Some(RetainedIdentityChange::MixedPermissionDirectory)
                            ) =>
                        {
                            RetainedIdentityChange::assert_closed_descendant(
                                &error,
                                Operation::CloseConversation,
                            );
                            Ok(())
                        }
                        result => result,
                    }
                })
                .await;
        }
        if let Some(current) = secondary_workspace.take() {
            cleanup
                .settle(async {
                    match current.close().await {
                        Err(error)
                            if matches!(
                                change,
                                Some(RetainedIdentityChange::MixedPermissionDirectory)
                            ) =>
                        {
                            RetainedIdentityChange::assert_closed_descendant(
                                &error,
                                Operation::CloseWorkspace,
                            );
                            Ok(())
                        }
                        result => result,
                    }
                })
                .await;
        }
        if let Some(current) = workspace.take() {
            cleanup
                .settle(async {
                    let result = current.close().await;
                    if matches!(change, Some(RetainedIdentityChange::Session)) {
                        let error = result.expect_err(
                            "foreign session isolation must leave the workspace's runtime closed",
                        );
                        assert_eq!(error.kind(), ErrorKind::Closed);
                        assert_eq!(error.context().operation, Operation::CloseWorkspace);
                        assert_eq!(error.context().phase, ErrorPhase::Lifecycle);
                        eprintln!("expected foreign-session terminal workspace result: {error:?}");
                        Ok(())
                    } else if matches!(
                        change,
                        Some(RetainedIdentityChange::MixedPermissionDirectory)
                            | Some(RetainedIdentityChange::ForeignPermission)
                    ) {
                        if let Err(error) = result {
                            RetainedIdentityChange::assert_closed_descendant(
                                &error,
                                Operation::CloseWorkspace,
                            );
                        }
                        Ok(())
                    } else {
                        result
                    }
                })
                .await;
        }
        if let Some(current) = agent.take() {
            cleanup.settle(async {
                let result = current.shutdown().await;
                if matches!(change, Some(RetainedIdentityChange::Session)) {
                    let error = result.expect_err("shutdown must retain its precise foreign-session isolation failure after joining the runtime");
                    eprintln!("foreign-session terminal Agent result before classification: {error:?}");
                    RetainedIdentityChange::assert_native_isolation_shutdown(&error);
                    Ok(())
                } else if matches!(change, Some(RetainedIdentityChange::MixedPermissionDirectory)
                        | Some(RetainedIdentityChange::ForeignPermission)) {
                    let error = result.expect_err("shutdown must retain the native Permission isolation failure");
                    eprintln!("Permission isolation terminal Agent result before classification: {error:?}");
                    RetainedIdentityChange::assert_native_isolation_shutdown(&error);
                    Ok(())
                } else { result }
            }).await;
        }
        if matches!(
            change,
            Some(RetainedIdentityChange::MixedPermissionDirectory)
                | Some(RetainedIdentityChange::ForeignPermission)
                | Some(RetainedIdentityChange::Session)
        ) {
            cleanup
                .settle(async {
                    let runtime = tokio::runtime::Handle::current();
                    while runtime.metrics().num_alive_tasks()
                        > runtime_baseline + probe.stats.connections.load(Ordering::SeqCst)
                    {
                        tokio::task::yield_now().await;
                    }
                    Ok::<(), SdkError>(())
                })
                .await;
        }
        let stats = Arc::clone(&probe.stats);
        cleanup
            .settle(async {
                probe.shutdown().await;
                Ok::<(), SdkError>(())
            })
            .await;
        cleanup
            .settle(async {
                assert_eq!(stats.active.load(Ordering::SeqCst), 0);
                assert_eq!(stats.connections.load(Ordering::SeqCst), 0);
                Ok::<(), SdkError>(())
            })
            .await;
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn opencode_cancelled_question_trailing_history_allows_reuse_and_terminal_close() {
    OpenCodeEventProbe::verify_cancelled_question_close(RetainedCloseCase::QuestionGone).await;
}

#[tokio::test]
async fn opencode_cancelled_question_still_native_pending_allows_reuse_and_terminal_close() {
    OpenCodeEventProbe::verify_cancelled_question_close(RetainedCloseCase::Question).await;
}

impl OpenCodeStreamCase {
    async fn verify_abort_intent_conflict(fault: PromptPersistenceFault, rollback_failure: bool) {
        let case = Self::start().await;
        let mut aborts = JoinSet::new();
        let observed = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(15),
            async {
                // The first prompt commit is owned but held. The public abort
                // joins it before native dispatch and explicitly waits for its
                // commit receipt before capturing the Lance snapshot.
                case.faults.arm(PromptCommitFault::BlockThenCommit, 1);
                let mut stream = case.conversation.prompt_stream(Self::request()).await.unwrap();
                case.faults.wait_until_blocked().await;
                case.faults.set_persistence_fault(fault);
                case.faults.state.persistence_rollback_failure.store(rollback_failure, Ordering::SeqCst);
                let started = tokio::time::Instant::now();
                let (start, admitted) = oneshot::channel();
                let conversation = Arc::clone(&case.conversation);
                let task = aborts.spawn(async move {
                    admitted.await.expect("test must admit its owned public abort");
                    conversation.abort().await
                });
                *case.faults.state.cancellation_request_task.lock().unwrap() = Some(task.id());
                start.send(()).expect("the owned public abort must remain alive");
                tokio::time::timeout(Duration::from_secs(3), async {
                    tokio::select! {
                        biased;
                        entered = case.faults.state.cancellation_request_reads_entered.acquire() => {
                            entered.expect("the intent transaction observation gate must remain open").forget();
                        }
                        result = aborts.join_next() => panic!("abort must wait at its storage boundary while prompt admission is held: {result:?}"),
                    }
                }).await.expect("abort must reach its selected storage boundary before prompt admission is released");
                let hold_foreground = matches!(fault,
                    PromptPersistenceFault::CancelRequestConflict
                        | PromptPersistenceFault::CancelRequestCommitUnavailableOnce
                        | PromptPersistenceFault::CancelRequestCommitReceiptLost
                        | PromptPersistenceFault::CancelRequestCommitReceiptAdvanced);
                if hold_foreground {
                    case.faults.block_next_transaction_begin();
                }
                case.faults.release();
                if hold_foreground {
                    // The selected abort snapshots committed admission; before
                    // native dispatch the only competing writer is its prompt
                    // foreground, which must not independently confirm intent.
                    case.faults.wait_until_transaction_begin_blocked().await;
                }
                if fault == PromptPersistenceFault::CancelRequestCommitReceiptLost {
                    tokio::time::timeout(Duration::from_secs(3), async {
                        while case.faults.state.cancellation_request_commits.load(Ordering::SeqCst) == 0 {
                            tokio::task::yield_now().await;
                        }
                    }).await.expect("the lost receipt must follow an actual committed intent");
                    case.faults.release_transaction_begin();
                }
                if fault == PromptPersistenceFault::CancelRequestCommitReceiptAdvanced {
                    tokio::time::timeout(Duration::from_secs(3),
                        case.faults.state.cancellation_request_advance_observed.acquire())
                        .await.expect("the selected abort must observe the actually committed monotonic state")
                        .expect("the advanced receipt observation gate must remain open").forget();
                    case.faults.release_transaction_begin();
                }
                let result = tokio::time::timeout(Duration::from_secs(7), async {
                    if fault == PromptPersistenceFault::CancelRequestCommitUnavailableOnce {
                        tokio::select! {
                            result = aborts.join_next() => result,
                            repeated = case.faults.state.blocked.acquire() => {
                                repeated.expect("the repeated intent observation gate must remain open").forget();
                                panic!("an unknown uncommitted intent receipt must not blindly repeat the selected owner's write");
                            }
                        }
                    } else {
                        aborts.join_next().await
                    }
                }).await.expect("explicit abort intent observation must remain lifecycle bounded")
                    .expect("the test owns one abort task").expect("the public abort task must join");
                if fault == PromptPersistenceFault::CancelRequestConflict {
                    let error = result.expect_err("persistent exact conflict cannot publish successful cancellation");
                    assert_eq!(error.kind(), ErrorKind::Timeout);
                    assert_eq!(error.context().operation, Operation::Abort);
                    assert_eq!(error.context().phase, ErrorPhase::Reconciliation);
                    let [conflict] = error.supplementary() else {
                        panic!("the elapsed original deadline must retain its last exact conflict: {error:?}");
                    };
                    assert_eq!(conflict.kind(), ErrorKind::Conflict);
                    assert_eq!(conflict.detail(), Some(&ErrorDetail::TransactionConflict));
                    assert_eq!(conflict.context().phase, ErrorPhase::StateWrite);
                    case.assert_persistence_retries_stopped(started).await;
                    assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 0);
                    assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 0);
                    return;
                }
                assert_eq!(case.faults.state.persistence_attempts.load(Ordering::SeqCst), 1,
                    "the selected intent operation must consume exactly one injected failure");
                if fault == PromptPersistenceFault::CancelRequestCommitUnavailableOnce {
                    let error = result.expect_err("an unconfirmed intent commit must preserve uncertainty");
                    assert_eq!(error.kind(), ErrorKind::Integration);
                    assert_eq!(error.detail(), Some(&ErrorDetail::ServiceUnavailable));
                    assert_eq!(error.context().operation, Operation::Abort);
                    assert_eq!(error.context().phase, ErrorPhase::StateCommit);
                    assert_eq!(case.faults.state.cancellation_request_writes.load(Ordering::SeqCst), 1);
                    let record = case.faults.prompt_snapshot(&case.conversation.id()).await
                        .expect("the uncommitted intent must leave the original prepared generation");
                    assert_eq!(record.record.value["phase"]["phase"], "prepared");
                    assert_eq!(record.record.value["cancellation"]["state"], "not_requested");
                    assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 0);
                    assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 0);
                } else if rollback_failure {
                    let error = result.expect_err("failed rollback must stop this abort-intent owner");
                    assert_eq!(error.kind(), ErrorKind::Conflict);
                    assert_eq!(error.detail(), Some(&ErrorDetail::TransactionConflict));
                    assert_eq!(error.context().operation, Operation::Abort);
                    assert_eq!(error.context().phase, if fault == PromptPersistenceFault::CancelRequestLoadConflictOnce { ErrorPhase::StateRead } else { ErrorPhase::StateWrite });
                    assert!(error.supplementary().iter().any(|cleanup| cleanup.context().phase == ErrorPhase::Cleanup && cleanup.detail() == Some(&ErrorDetail::ServiceUnavailable)),
                        "the exact conflict must retain failed rollback evidence: {error:?}");
                    assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 0,
                        "an uncommitted intent cannot authorize native cancellation");
                    assert!(case.probe.stats.messages.load(Ordering::SeqCst) <= 1,
                        "a rejected cancellation cannot replay the original prompt");
                    assert_eq!(case.faults.state.cancellation_request_writes.load(Ordering::SeqCst), usize::from(fault != PromptPersistenceFault::CancelRequestLoadConflictOnce),
                        "a failed rollback cannot permit another intent write");
                } else {
                    result.expect("a definite intent conflict must recompute; a lost committed receipt must confirm the existing intent");
                    let writes = case.faults.state.cancellation_request_writes.load(Ordering::SeqCst);
                    match fault {
                        PromptPersistenceFault::CancelRequestCommitReceiptLost
                        | PromptPersistenceFault::CancelRequestCommitReceiptAdvanced => {
                            assert_eq!(case.faults.state.cancellation_request_commits.load(Ordering::SeqCst), 1,
                                "a lost receipt requires exactly one successful native intent commit");
                            assert_eq!(writes, 1,
                                "a lost committed intent receipt cannot rewrite intent");
                        }
                        PromptPersistenceFault::CancelRequestForeign
                        | PromptPersistenceFault::CancelRequestTerminal => assert_eq!(writes, 0,
                            "a foreign or terminal generation is a legal intent no-op"),
                        PromptPersistenceFault::CancelRequestLoadConflictOnce => assert!(writes <= 1,
                            "a retried read may observe intent independently committed by the foreground"),
                        _ => assert!((1..=2).contains(&writes),
                            "one failed write may be retried or observe the foreground's committed intent: {writes}"),
                    }
                    let terminal = tokio::time::timeout(Duration::from_secs(3), stream.recv()).await
                        .expect("the cancelled prepared prompt must settle its observer").expect("one terminal outcome is required")
                        .expect_err("accepted cancellation cannot publish completion");
                    assert_eq!(terminal.kind(), ErrorKind::Cancelled);
                    assert!(stream.recv().await.is_none());
                    assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 0,
                        "cancelling a reserved prompt must not dispatch native generation");
                    assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 0,
                        "a never-dispatched prompt needs no native abort");
                    case.settled().await;
                }
            },
        ).await;
        // Release admission even after a setup assertion fails, then let the
        // existing runtime owner settle the same generation during cleanup.
        case.faults.release();
        case.faults
            .state
            .cancellation_request_admission_committed
            .add_permits(1);
        case.faults
            .state
            .block_transaction_begin
            .store(false, Ordering::SeqCst);
        case.faults.release_transaction_begin();
        case.faults
            .state
            .cancellation_request_uncommitted
            .store(false, Ordering::SeqCst);
        case.faults
            .state
            .cancellation_request_advanced
            .store(false, Ordering::SeqCst);
        case.faults
            .state
            .persistence_rollback_failure
            .store(false, Ordering::SeqCst);
        case.faults
            .set_persistence_fault(PromptPersistenceFault::None);
        *case.faults.state.cancellation_request_task.lock().unwrap() = None;
        let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
        cleanup
            .settle(async {
                while let Some(result) = aborts.join_next().await {
                    result.expect("the owned public abort must join")?;
                }
                Ok::<(), SdkError>(())
            })
            .await;
        aborts.abort_all();
        cleanup
            .settle(async {
                while let Some(result) = aborts.join_next().await {
                    match result {
                        Ok(result) => result?,
                        Err(error) => assert!(error.is_cancelled(), "abort task panicked: {error}"),
                    }
                }
                Ok::<(), SdkError>(())
            })
            .await;
        cleanup.settle(case.conversation.abort()).await;
        case.release();
        let Self {
            agent,
            workspace,
            conversation,
            probe,
            ..
        } = case;
        drop(conversation);
        drop(workspace);
        cleanup.settle(agent.shutdown()).await;
        let stats = Arc::clone(&probe.stats);
        cleanup
            .settle(async {
                probe.shutdown().await;
                Ok::<(), SdkError>(())
            })
            .await;
        cleanup
            .settle(async {
                assert_eq!(stats.active.load(Ordering::SeqCst), 0);
                assert_eq!(stats.connections.load(Ordering::SeqCst), 0);
                Ok::<(), SdkError>(())
            })
            .await;
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn opencode_abort_intent_load_conflict_recomputes_before_native_dispatch() {
    OpenCodeStreamCase::verify_abort_intent_conflict(
        PromptPersistenceFault::CancelRequestLoadConflictOnce,
        false,
    )
    .await;
}

#[tokio::test]
async fn opencode_abort_intent_put_conflict_recomputes_before_native_dispatch() {
    OpenCodeStreamCase::verify_abort_intent_conflict(
        PromptPersistenceFault::CancelRequestConflictOnce,
        false,
    )
    .await;
}

#[tokio::test]
async fn opencode_abort_intent_commit_conflict_recomputes_before_native_dispatch() {
    OpenCodeStreamCase::verify_abort_intent_conflict(
        PromptPersistenceFault::CancelRequestCommitConflictOnce,
        false,
    )
    .await;
}

#[tokio::test]
async fn opencode_abort_intent_read_conflict_retains_failed_rollback_and_stops() {
    OpenCodeStreamCase::verify_abort_intent_conflict(
        PromptPersistenceFault::CancelRequestLoadConflictOnce,
        true,
    )
    .await;
}

#[tokio::test]
async fn opencode_abort_intent_write_conflict_retains_failed_rollback_and_stops() {
    OpenCodeStreamCase::verify_abort_intent_conflict(
        PromptPersistenceFault::CancelRequestConflictOnce,
        true,
    )
    .await;
}

#[tokio::test]
async fn opencode_abort_intent_lost_commit_receipt_preserves_idempotence() {
    OpenCodeStreamCase::verify_abort_intent_conflict(
        PromptPersistenceFault::CancelRequestCommitReceiptLost,
        false,
    )
    .await;
}

#[tokio::test]
async fn opencode_abort_intent_persistent_conflict_keeps_original_deadline() {
    OpenCodeStreamCase::verify_abort_intent_conflict(
        PromptPersistenceFault::CancelRequestConflict,
        false,
    )
    .await;
}

#[tokio::test]
async fn opencode_abort_intent_unknown_uncommitted_receipt_does_not_rewrite() {
    OpenCodeStreamCase::verify_abort_intent_conflict(
        PromptPersistenceFault::CancelRequestCommitUnavailableOnce,
        false,
    )
    .await;
}

#[tokio::test]
async fn opencode_abort_intent_lost_receipt_confirms_monotonic_terminal_state() {
    OpenCodeStreamCase::verify_abort_intent_conflict(
        PromptPersistenceFault::CancelRequestCommitReceiptAdvanced,
        false,
    )
    .await;
}

#[tokio::test]
async fn opencode_abort_intent_foreign_generation_remains_a_noop() {
    OpenCodeStreamCase::verify_abort_intent_conflict(
        PromptPersistenceFault::CancelRequestForeign,
        false,
    )
    .await;
}

#[tokio::test]
async fn opencode_abort_intent_terminal_generation_remains_a_noop() {
    OpenCodeStreamCase::verify_abort_intent_conflict(
        PromptPersistenceFault::CancelRequestTerminal,
        false,
    )
    .await;
}

#[tokio::test]
async fn opencode_cancelled_permission_still_native_pending_allows_reuse_and_terminal_close() {
    OpenCodeEventProbe::verify_cancelled_question_close(RetainedCloseCase::Permissions).await;
}

#[tokio::test]
async fn opencode_cancelled_question_changed_session_does_not_authorize_native_rejection() {
    OpenCodeEventProbe::verify_cancelled_question_close(RetainedCloseCase::ChangedQuestion(
        RetainedIdentityChange::Session,
    ))
    .await;
}

#[tokio::test]
async fn opencode_cancelled_question_changed_tool_message_does_not_authorize_native_rejection() {
    OpenCodeEventProbe::verify_cancelled_question_close(RetainedCloseCase::ChangedQuestion(
        RetainedIdentityChange::Message,
    ))
    .await;
}

#[tokio::test]
async fn opencode_cancelled_question_changed_tool_call_does_not_authorize_native_rejection() {
    OpenCodeEventProbe::verify_cancelled_question_close(RetainedCloseCase::ChangedQuestion(
        RetainedIdentityChange::Call,
    ))
    .await;
}

#[tokio::test]
async fn opencode_cancelled_question_changed_schema_does_not_authorize_native_rejection() {
    OpenCodeEventProbe::verify_cancelled_question_close(RetainedCloseCase::ChangedQuestion(
        RetainedIdentityChange::Schema,
    ))
    .await;
}

#[tokio::test]
async fn opencode_cancelled_question_missing_origin_does_not_authorize_native_rejection() {
    OpenCodeEventProbe::verify_cancelled_question_close(RetainedCloseCase::ChangedQuestion(
        RetainedIdentityChange::MissingOrigin,
    ))
    .await;
}

#[tokio::test]
async fn opencode_ordinary_question_reconnect_does_not_authorize_native_rejection() {
    OpenCodeEventProbe::verify_cancelled_question_close(
        RetainedCloseCase::OrdinaryQuestionObserved,
    )
    .await;
}

#[tokio::test]
async fn opencode_ordinary_question_explicit_close_rejects_pending_without_rewriting_success() {
    OpenCodeEventProbe::verify_cancelled_question_close(RetainedCloseCase::OrdinaryQuestionClosed)
        .await;
}

#[tokio::test]
async fn opencode_cancelled_permission_changed_metadata_does_not_authorize_native_rejection() {
    OpenCodeEventProbe::verify_cancelled_question_close(RetainedCloseCase::ChangedPermissions(
        RetainedIdentityChange::Schema,
    ))
    .await;
}

#[tokio::test]
async fn opencode_cancelled_permission_foreign_same_session_peer_does_not_authorize_native_rejection()
 {
    OpenCodeEventProbe::verify_cancelled_question_close(RetainedCloseCase::ChangedPermissions(
        RetainedIdentityChange::ForeignPermission,
    ))
    .await;
}

impl OpenCodeStreamCase {
    async fn verify_native_rejection_authority(permission: bool, mode: RetainedRejection) {
        let case = Self::start().await;
        case.probe
            .stats
            .retained_question_history
            .store(true, Ordering::SeqCst);
        case.probe
            .stats
            .retain_question_after_abort
            .store(true, Ordering::SeqCst);
        case.probe
            .stats
            .retained_permissions
            .store(permission, Ordering::SeqCst);
        *case.probe.stats.retained_rejection.lock().unwrap() = mode;
        let mut aborts = JoinSet::new();
        let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(25), async {
            let mut notifications = case.conversation.notification_stream().unwrap();
            let mut first = case.conversation.prompt_stream(PromptRequest::new("ask owned interaction").unwrap()).await.unwrap();
            loop {
                tokio::select! {
                    item = notifications.recv() => {
                        let item = item.unwrap().unwrap();
                        if matches!(item.kind(), ConversationNotificationKind::Interaction { .. }) { break; }
                    }
                    result = first.recv() => panic!("first prompt must reach its pending interaction: {result:?}"),
                }
            }
            if permission {
                loop {
                    let records = case.faults.inner.list(StateListRequest::new(OPENCODE_CONTINUATION_NAMESPACE).unwrap()).await.unwrap();
                    if records.records.len() == 2 { break; }
                    tokio::task::yield_now().await;
                }
            }
            let mut subscriptions_before_disconnect = None;
            let conversation = Arc::clone(&case.conversation);
            let original_observer = aborts.spawn(async move { conversation.abort().await });
            tokio::select! {
                biased;
                permit = case.probe.stats.rejection_entered.acquire() => permit.unwrap().forget(),
                result = aborts.join_next() => panic!("cancelled generation cannot be released before selected native interaction cleanup: {result:?}"),
            }
            if matches!(mode, RetainedRejection::Held
                        | RetainedRejection::HeldDroppedObserver
                        | RetainedRejection::HeldAcknowledgedAfterDisconnectWithoutEvent) {
                let busy = case.conversation.prompt(PromptRequest::new("must remain fenced").unwrap()).await.expect_err("pending native cancellation must retain the original generation's admission");
                assert_eq!(busy.kind(), ErrorKind::Busy);
                assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 1);
                if matches!(mode, RetainedRejection::HeldAcknowledgedAfterDisconnectWithoutEvent) {
                    assert!(!permission, "this exact transport control owns a question");
                    let subscriptions = case.probe.stats.subscriptions.load(Ordering::SeqCst);
                    let retired = subscriptions - case.probe.stats.active.load(Ordering::SeqCst);
                    subscriptions_before_disconnect = Some(subscriptions);
                    case.probe.disconnect_current();
                    tokio::time::timeout(Duration::from_secs(2), async {
                        while case.probe.stats.subscriptions.load(Ordering::SeqCst)
                            - case.probe.stats.active.load(Ordering::SeqCst) <= retired {
                            tokio::task::yield_now().await;
                        }
                    }).await.expect("the original native SSE connection must close before HTTP acknowledgement release");
                    assert!(case.probe.stats.pending_question.load(Ordering::SeqCst));
                    assert_eq!(case.probe.stats.question_rejections.load(Ordering::SeqCst), 1);
                }
                if matches!(mode, RetainedRejection::HeldDroppedObserver) {
                    original_observer.abort();
                    let dropped = aborts.join_next().await.unwrap().expect_err("only the owned caller observer must be cancelled");
                    assert!(dropped.is_cancelled());
                }
                let conversation = Arc::clone(&case.conversation);
                let (entered, waiting) = oneshot::channel();
                aborts.spawn(async move {
                    let pending = conversation.abort();
                    tokio::pin!(pending);
                    std::future::poll_fn(|context| {
                        match pending.as_mut().poll(context) {
                            std::task::Poll::Pending => std::task::Poll::Ready(()),
                            std::task::Poll::Ready(result) => panic!("second abort must remain pending with native rejection held: {result:?}"),
                        }
                    }).await;
                    entered.send(()).expect("test must retain its second pending abort observer");
                    pending.await
                });
                waiting.await.expect("second public abort must actually be polled while rejection is held");
                case.probe.stats.rejection_release.add_permits(1);
            }
            let confirmed = matches!(mode, RetainedRejection::Held
                        | RetainedRejection::HeldDroppedObserver
                        | RetainedRejection::HeldAcknowledgedAfterDisconnectWithoutEvent | RetainedRejection::AcknowledgedWithoutEvent | RetainedRejection::LostWithEvent);
            while let Some(result) = aborts.join_next().await {
                let result = result.expect("owned abort observers must join");
                if confirmed { result.expect("one authoritative native cancellation receipt must settle all observers"); }
                else { result.expect_err("uncertain or contradictory native outcome cannot become successful cancellation"); }
            }
            let terminal = first.recv().await.expect("the original observer must publish one terminal result");
            let terminal = terminal.expect_err("cancellation cannot manufacture a successful prompt completion");
            if confirmed {
                assert_eq!(terminal.kind(), ErrorKind::Cancelled, "confirmed cancellation must preserve the earlier complete execution error, including across a later SSE gap");
            }
            assert!(first.recv().await.is_none());
            assert_eq!(case.probe.stats.question_rejections.load(Ordering::SeqCst), usize::from(!permission));
            assert_eq!(case.probe.stats.permission_rejections.load(Ordering::SeqCst), usize::from(permission));
            assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 1);
            assert_eq!(case.probe.stats.question_replies.load(Ordering::SeqCst), 0);
            if confirmed {
                if let Some(previous) = subscriptions_before_disconnect {
                    tokio::time::timeout(Duration::from_secs(5), async {
                        while case.probe.stats.subscriptions.load(Ordering::SeqCst) <= previous
                            || case.probe.stats.active.load(Ordering::SeqCst) != 1
                        {
                            tokio::task::yield_now().await;
                        }
                    }).await.expect("confirmed native cancellation must survive event recovery without another rejection");
                    assert!(!case.probe.stats.pending_question.load(Ordering::SeqCst));
                    assert_eq!(case.probe.stats.question_rejections.load(Ordering::SeqCst), 1);
                }
                let response = case.conversation.prompt(PromptRequest::new("return the reuse marker").unwrap()).await.unwrap();
                assert_eq!(response.output(), "RETAINED_HISTORY_REUSE");
                assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 2);
            } else {
                let reuse = case.conversation.prompt(PromptRequest::new("must not dispatch while unresolved").unwrap()).await;
                assert!(reuse.is_err(), "uncertain cancellation must not release admission");
                assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 1);
                assert_eq!(case.probe.stats.question_rejections.load(Ordering::SeqCst), 1, "observing uncertainty must not resend the native reject");
            }
        }).await;
        // Supply an exact later native rejection only for fixture cleanup. It is
        // outside every public outcome/counter assertion and does not replay the
        // SDK's native request or turn an uncertain observation into a pass.
        case.probe.stats.rejection_release.add_permits(2);
        case.probe.stats.prompt_release.add_permits(2);
        case.probe
            .stats
            .pending_question
            .store(false, Ordering::SeqCst);
        let pending = std::mem::take(&mut *case.probe.stats.pending_permissions.lock().unwrap());
        if permission {
            for request in pending {
                let _ = case.probe.events.send(serde_json::json!({"directory": env!("CARGO_MANIFEST_DIR"), "payload": {
                    "id": format!("evt_cleanup_{}", request["id"].as_str().unwrap()), "type": "permission.replied",
                    "properties": {"sessionID": request["sessionID"], "requestID": request["id"], "reply": "reject"}
                }}).to_string());
            }
        } else {
            let _ = case.probe.events.send(
                serde_json::json!({"directory": env!("CARGO_MANIFEST_DIR"), "payload": {
                    "id": "evt_cleanup_question_rejected", "type": "question.rejected",
                    "properties": {"sessionID": "ses_probe", "requestID": "que_probe_pending"}
                }})
                .to_string(),
            );
        }
        let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
        cleanup
            .settle(async {
                while let Some(result) = aborts.join_next().await {
                    result.expect("abort observer must join")?;
                }
                Ok::<(), SdkError>(())
            })
            .await;
        aborts.abort_all();
        cleanup
            .settle(async {
                while let Some(result) = aborts.join_next().await {
                    match result {
                        Ok(result) => result?,
                        Err(error) => {
                            assert!(error.is_cancelled(), "abort observer panicked: {error}")
                        }
                    }
                }
                Ok::<(), SdkError>(())
            })
            .await;
        cleanup
            .settle(async {
                case.revoke().await;
                Ok::<(), SdkError>(())
            })
            .await;
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn opencode_cancelled_question_holds_generation_until_one_native_rejection_settles() {
    OpenCodeStreamCase::verify_native_rejection_authority(false, RetainedRejection::Held).await;
}

#[tokio::test]
async fn opencode_cancelled_permission_holds_generation_until_one_session_rejection_settles() {
    OpenCodeStreamCase::verify_native_rejection_authority(true, RetainedRejection::Held).await;
}

#[tokio::test]
async fn opencode_cancelled_question_lost_reject_response_uses_exact_rejection_event() {
    OpenCodeStreamCase::verify_native_rejection_authority(false, RetainedRejection::LostWithEvent)
        .await;
}

#[tokio::test]
async fn opencode_cancelled_question_unavailable_reject_retains_uncertainty_without_replay() {
    OpenCodeStreamCase::verify_native_rejection_authority(false, RetainedRejection::Unavailable)
        .await;
}

#[tokio::test]
async fn opencode_cancelled_question_lost_response_without_event_is_not_success() {
    OpenCodeStreamCase::verify_native_rejection_authority(
        false,
        RetainedRejection::LostWithoutEvent,
    )
    .await;
}

#[tokio::test]
async fn opencode_cancelled_question_missing_404_is_not_native_acknowledgement() {
    OpenCodeStreamCase::verify_native_rejection_authority(false, RetainedRejection::Missing).await;
}

#[tokio::test]
async fn opencode_cancelled_question_answer_event_cannot_confirm_rejection() {
    OpenCodeStreamCase::verify_native_rejection_authority(
        false,
        RetainedRejection::ContradictoryAnswer,
    )
    .await;
}

#[tokio::test]
async fn opencode_cancelled_question_acknowledged_response_does_not_require_delivered_event() {
    OpenCodeStreamCase::verify_native_rejection_authority(
        false,
        RetainedRejection::AcknowledgedWithoutEvent,
    )
    .await;
}

#[tokio::test]
async fn opencode_cancelled_question_dropped_abort_observer_preserves_one_native_rejection() {
    OpenCodeStreamCase::verify_native_rejection_authority(
        false,
        RetainedRejection::HeldDroppedObserver,
    )
    .await;
}

#[tokio::test]
async fn opencode_cancelled_permission_reappearing_during_later_generation_cannot_reject_new_work()
{
    let case = OpenCodeStreamCase::start().await;
    case.probe
        .stats
        .retained_question_history
        .store(true, Ordering::SeqCst);
    case.probe
        .stats
        .retain_question_after_abort
        .store(true, Ordering::SeqCst);
    case.probe
        .stats
        .retained_permissions
        .store(true, Ordering::SeqCst);
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(25),
        async {
            let mut notifications = case.conversation.notification_stream().unwrap();
            let mut first = case
                .conversation
                .prompt_stream(PromptRequest::new("first owned permissions").unwrap())
                .await
                .unwrap();
            loop {
                let item = notifications.recv().await.unwrap().unwrap();
                if matches!(
                    item.kind(),
                    ConversationNotificationKind::Interaction { .. }
                ) {
                    break;
                }
            }
            loop {
                let records = case
                    .faults
                    .inner
                    .list(StateListRequest::new(OPENCODE_CONTINUATION_NAMESPACE).unwrap())
                    .await
                    .unwrap();
                if records.records.len() == 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
            let old = case.probe.stats.pending_permissions.lock().unwrap()[0].clone();
            case.conversation
                .abort()
                .await
                .expect("the first generation must cancel its exact owned native resources");
            assert_eq!(
                first.recv().await.unwrap().unwrap_err().kind(),
                ErrorKind::Cancelled
            );
            assert!(first.recv().await.is_none());
            assert_eq!(
                case.probe
                    .stats
                    .permission_rejections
                    .load(Ordering::SeqCst),
                1
            );
            assert!(
                case.probe
                    .stats
                    .pending_permissions
                    .lock()
                    .unwrap()
                    .is_empty()
            );
            case.probe
                .stats
                .held_second_permission
                .store(true, Ordering::SeqCst);
            let _second = case
                .conversation
                .prompt_stream(PromptRequest::new("later held permission").unwrap())
                .await
                .unwrap();
            loop {
                let item = notifications.recv().await.unwrap().unwrap();
                if matches!(
                    item.kind(),
                    ConversationNotificationKind::Interaction { .. }
                ) {
                    break;
                }
            }
            assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 2);
            assert!(case.probe.stats.native_active.load(Ordering::SeqCst));
            assert!(
                case.probe
                    .stats
                    .pending_permissions
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|request| request["id"] == "per_retained_later")
            );
            // Contradictory native identity reappearance is intentionally injected
            // after original cleanup, while a genuinely later SDK prompt is active.
            // Only ordinary reconnect runs here; Close would authorize new cancellation.
            case.probe
                .stats
                .pending_permissions
                .lock()
                .unwrap()
                .push(old);
            let history = case.probe.stats.history.load(Ordering::SeqCst);
            let scans = case.probe.stats.pending_scans.load(Ordering::SeqCst);
            let started = tokio::time::Instant::now();
            case.probe.disconnect_current();
            case.probe
                .wait_for_synchronization(history + 1, scans + 2)
                .await;
            // Allow the existing five-second reconciliation budget to finish. This
            // is an effect-safety window, not a claim that scan counters mean success.
            tokio::time::sleep_until(started + Duration::from_secs(6)).await;
            assert_eq!(
                case.probe
                    .stats
                    .permission_rejections
                    .load(Ordering::SeqCst),
                1,
                "stale old cleanup cannot reject this session's newer native permission"
            );
            assert!(
                case.probe
                    .stats
                    .pending_permissions
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|request| request["id"] == "per_retained_later"),
                "session-wide collateral mutation must not consume newer work"
            );
            assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 1);
            assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 2);
            assert_eq!(case.probe.stats.subscriptions.load(Ordering::SeqCst), 2);
            assert_eq!(case.probe.stats.maximum.load(Ordering::SeqCst), 1);
        },
    )
    .await;
    case.probe
        .stats
        .native_active
        .store(false, Ordering::SeqCst);
    case.probe.stats.pending_permissions.lock().unwrap().clear();
    case.probe.stats.prompt_release.add_permits(2);
    let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
    cleanup
        .settle(async {
            case.revoke().await;
            Ok::<(), SdkError>(())
        })
        .await;
    cleanup.finish(observed);
}

#[tokio::test]
async fn opencode_ordinary_question_dropped_close_retains_native_cleanup_before_reopen() {
    OpenCodeEventProbe::verify_cancelled_question_close(
        RetainedCloseCase::OrdinaryQuestionCloseObserverLost,
    )
    .await;
}

#[tokio::test]
async fn opencode_cancelled_permission_mixed_inventory_directories_do_not_reject_collateral() {
    OpenCodeEventProbe::verify_cancelled_question_close(RetainedCloseCase::ChangedPermissions(
        RetainedIdentityChange::MixedPermissionDirectory,
    ))
    .await;
}

#[tokio::test]
async fn opencode_cancelled_question_http_ack_survives_sse_disconnect_without_rejected_event() {
    OpenCodeStreamCase::verify_native_rejection_authority(
        false,
        RetainedRejection::HeldAcknowledgedAfterDisconnectWithoutEvent,
    )
    .await;
}

#[tokio::test]
async fn opencode_idle_response_loss_retains_generation_until_native_question_cleanup() {
    let case = OpenCodeStreamCase::start().await;
    case.probe
        .stats
        .retained_question_history
        .store(true, Ordering::SeqCst);
    case.probe
        .stats
        .retain_question_after_abort
        .store(true, Ordering::SeqCst);
    case.probe
        .stats
        .malformed_stream_response
        .store(true, Ordering::SeqCst);
    case.probe.stats.reject_abort.store(true, Ordering::SeqCst);
    *case.probe.stats.retained_rejection.lock().unwrap() = RetainedRejection::Held;
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(25),
        async {
            let mut notifications = case.conversation.notification_stream().unwrap();
            let mut first = case.stream().await;
            loop {
                tokio::select! {
                    item = notifications.recv() => {
                        if matches!(item.unwrap().unwrap().kind(), ConversationNotificationKind::Interaction { .. }) {
                            break;
                        }
                    }
                    result = first.recv() => panic!("the native request must become pending before HTTP response loss: {result:?}"),
                }
            }
            case.wait_for_admission().await;
            case.release();
            let primary = OpenCodeStreamCase::next(&mut first).await.expect_err("malformed final HTTP must preserve its original failure");
            assert_eq!(primary.kind(), ErrorKind::Protocol);
            assert!(first.recv().await.is_none());
            tokio::time::timeout(Duration::from_secs(6), case.probe.stats.rejection_entered.acquire())
                .await.expect("idle response-loss repair must retain the original generation for selected native question cleanup")
                .unwrap().forget();
            assert!(case.probe.stats.pending_question.load(Ordering::SeqCst));
            assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 1);
            assert_eq!(case.probe.stats.question_rejections.load(Ordering::SeqCst), 1);
            assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 1);
            let busy = case.conversation.prompt(PromptRequest::new("must remain fenced while native cleanup is pending").unwrap())
                .await.expect_err("response loss cannot release admission before its native interaction settles");
            assert_eq!(busy.kind(), ErrorKind::Busy);
            case.probe.stats.rejection_release.add_permits(1);
            case.settled().await;
            let response = case.conversation.prompt(PromptRequest::new("return the reuse marker").unwrap()).await.unwrap();
            assert_eq!(response.output(), "RETAINED_HISTORY_REUSE");
            assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 2);
            assert_eq!(case.probe.stats.aborts.load(Ordering::SeqCst), 1);
            assert_eq!(case.probe.stats.question_rejections.load(Ordering::SeqCst), 1);
            assert_eq!(case.probe.stats.question_replies.load(Ordering::SeqCst), 0);
            assert!(!case.probe.stats.pending_question.load(Ordering::SeqCst));
        },
    ).await;
    case.probe.stats.rejection_release.add_permits(2);
    case.probe.stats.prompt_release.add_permits(2);
    case.probe
        .stats
        .malformed_stream_response
        .store(false, Ordering::SeqCst);
    case.probe.stats.reject_abort.store(false, Ordering::SeqCst);
    case.probe
        .stats
        .pending_question
        .store(false, Ordering::SeqCst);
    let _ = case.probe.events.send(
        serde_json::json!({"directory": env!("CARGO_MANIFEST_DIR"), "payload": {
            "id": "evt_response_loss_cleanup_question", "type": "question.rejected",
            "properties": {"sessionID": "ses_probe", "requestID": "que_probe_pending"}
        }})
        .to_string(),
    );
    let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
    cleanup
        .settle(async {
            case.revoke().await;
            Ok::<(), SdkError>(())
        })
        .await;
    cleanup.finish(observed);
}

#[tokio::test]
async fn opencode_owned_question_without_prompt_generation_can_be_explicitly_closed() {
    let OpenCodeStreamCase {
        probe,
        agent,
        workspace,
        conversation,
        faults,
        ..
    } = OpenCodeStreamCase::start().await;
    let mut agent = Some(agent);
    let mut workspace = Some(workspace);
    let mut conversation = Some(Arc::try_unwrap(conversation).ok().unwrap());
    let mut closes = JoinSet::new();
    probe
        .stats
        .retained_question_history
        .store(true, Ordering::SeqCst);
    probe.stats.pending_question.store(true, Ordering::SeqCst);
    // Missing tool origin is native-valid from first admission, not an identity
    // change. Close owns this unchanged request even without an SDK prompt.
    *probe.stats.retained_identity_change.lock().unwrap() =
        Some(RetainedIdentityChange::MissingOrigin);
    *probe.stats.retained_rejection.lock().unwrap() = RetainedRejection::Held;
    let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(15), async {
        let current = conversation.as_ref().unwrap();
        let id = current.id();
        let mut notifications = current.notification_stream().unwrap();
        probe.stats.events.send(serde_json::json!({
            "directory": env!("CARGO_MANIFEST_DIR"),
            "payload": {"id": "evt_inventory_question_without_prompt", "type": "question.asked", "properties": {
                "id": "que_probe_pending", "sessionID": "ses_probe",
                "questions": [{"question": "Continue?", "header": "Probe", "options": [], "multiple": false, "custom": true}]
            }}
        }).to_string()).unwrap();
        loop {
            let item = notifications.recv().await.unwrap().unwrap();
            if matches!(item.kind(), ConversationNotificationKind::Interaction { .. }) { break; }
        }
        assert!(faults.prompt_snapshot(&id).await.is_none());
        assert_eq!(probe.stats.messages.load(Ordering::SeqCst), 0);
        let closing = conversation.take().unwrap();
        closes.spawn(async move { closing.close().await });
        tokio::select! {
            biased;
            permit = probe.stats.rejection_entered.acquire() => permit.unwrap().forget(),
            result = closes.join_next() => panic!("explicit Close must reject its unchanged inventory request without inventing a prompt generation: {result:?}"),
        }
        assert_eq!(probe.stats.question_rejections.load(Ordering::SeqCst), 1);
        assert_eq!(probe.stats.aborts.load(Ordering::SeqCst), 0);
        assert_eq!(probe.stats.messages.load(Ordering::SeqCst), 0);
        assert!(faults.prompt_snapshot(&id).await.is_none());
        probe.stats.rejection_release.add_permits(1);
        closes.join_next().await.unwrap().expect("Close observer must join").expect("event-admitted request with matching inventory must settle before terminal Close");
        assert!(!probe.stats.pending_question.load(Ordering::SeqCst));
        assert_eq!(probe.stats.question_rejections.load(Ordering::SeqCst), 1);
        assert_eq!(probe.stats.question_replies.load(Ordering::SeqCst), 0);
        while let Some(item) = notifications.recv().await {
            assert!(!matches!(item.unwrap().kind(), ConversationNotificationKind::Interaction { .. }));
        }
    }).await;
    // Late exact native settlement is fixture cleanup only, after every effect
    // oracle; it cannot turn the observed Close failure into success.
    probe.stats.rejection_release.add_permits(2);
    probe.stats.pending_question.store(false, Ordering::SeqCst);
    let _ = probe.stats.events.send(serde_json::json!({
        "directory": env!("CARGO_MANIFEST_DIR"),
        "payload": {"id": "evt_inventory_question_cleanup", "type": "question.rejected", "properties": {"sessionID": "ses_probe", "requestID": "que_probe_pending"}}
    }).to_string());
    let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
    cleanup
        .settle(async {
            while let Some(result) = closes.join_next().await {
                result.expect("Close task must join")?;
            }
            Ok::<(), SdkError>(())
        })
        .await;
    closes.abort_all();
    cleanup
        .settle(async {
            while let Some(result) = closes.join_next().await {
                match result {
                    Ok(result) => result?,
                    Err(error) => assert!(error.is_cancelled()),
                }
            }
            Ok::<(), SdkError>(())
        })
        .await;
    if let Some(current) = conversation.take() {
        cleanup.settle(current.close()).await;
    }
    if let Some(current) = workspace.take() {
        cleanup.settle(current.close()).await;
    }
    if let Some(current) = agent.take() {
        cleanup.settle(current.shutdown()).await;
    }
    let stats = Arc::clone(&probe.stats);
    cleanup
        .settle(async {
            probe.shutdown().await;
            Ok::<(), SdkError>(())
        })
        .await;
    cleanup
        .settle(async {
            assert_eq!(stats.active.load(Ordering::SeqCst), 0);
            assert_eq!(stats.connections.load(Ordering::SeqCst), 0);
            Ok::<(), SdkError>(())
        })
        .await;
    cleanup.finish(observed);
}

#[tokio::test]
async fn opencode_close_settles_question_admitted_after_generation_cancelled() {
    let OpenCodeStreamCase {
        mut probe,
        agent,
        workspace,
        conversation,
        faults,
        ..
    } = OpenCodeStreamCase::start().await;
    let mut conversation = Some(Arc::try_unwrap(conversation).ok().unwrap());
    let mut closes = JoinSet::new();
    probe
        .stats
        .retained_question_history
        .store(true, Ordering::SeqCst);
    probe
        .stats
        .retain_question_after_abort
        .store(true, Ordering::SeqCst);
    *probe.stats.retained_rejection.lock().unwrap() = RetainedRejection::Held;
    let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(20), async {
        let current = conversation.as_ref().unwrap();
        let id = current.id();
        let mut notifications = current.notification_stream().unwrap();
        let mut output = current.prompt_stream(PromptRequest::new("ask one owned question").unwrap()).await.unwrap();
        let first_notification = loop {
            tokio::select! {
                item = notifications.recv() => {
                    let item = item.unwrap().unwrap();
                    if matches!(item.kind(), ConversationNotificationKind::Interaction { .. }) { break item.id().clone(); }
                }
                item = output.recv() => panic!("the first question must precede cancellation: {item:?}"),
            }
        };
        let first = faults.inner.list(StateListRequest::new(OPENCODE_CONTINUATION_NAMESPACE).unwrap()).await.unwrap().records;
        let [first] = first.as_slice() else { panic!("one original question must be durably admitted"); };
        assert_eq!(first.record.value.pointer("/request/question/id").and_then(serde_json::Value::as_str), Some("que_probe_pending"));
        let generation = first.record.value["generation_id"].as_str().expect("the original question must own a prompt generation").to_owned();
        let closing = conversation.take().unwrap();
        let close_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        closes.spawn(async move { closing.close().await });
        tokio::select! {
            biased;
            permit = probe.stats.rejection_entered.acquire() => permit.unwrap().forget(),
            result = closes.join_next() => panic!("Close must reach the original native rejection: {result:?}"),
        }
        let terminal = faults.prompt_snapshot(&id).await.expect("the held native obligation must retain its exact prompt");
        assert_eq!(terminal.record.value["message_id"], generation);
        assert_eq!(terminal.record.value["phase"]["phase"], "terminal");
        assert_eq!(terminal.record.value["phase"]["outcome"]["outcome"], "cancelled");
        assert_eq!(probe.stats.messages.load(Ordering::SeqCst), 1);
        assert_eq!(probe.stats.aborts.load(Ordering::SeqCst), 1);
        assert_eq!(probe.stats.question_rejections.load(Ordering::SeqCst), 1);

        // Delayed delivery of another genuine-shaped native question from the
        // same assistant does not create a new generation. The original input
        // and aborted assistant remain in immutable history. This establishes
        // a causal regression, not the historical live invocation's ordering.
        OpenCodeEventProbe::retain_question_event(&probe.stats, "message.part.updated", serde_json::json!({
            "sessionID": "ses_probe", "time": 2, "part": {
                "id": "prt_retained_question_peer", "sessionID": "ses_probe",
                "messageID": "msg_000000000006abcdefghijklmn", "type": "tool",
                "callID": "call_retained_question_peer", "tool": "question",
                "state": {"status": "error", "input": {}, "error": "Tool execution aborted",
                    "metadata": {"interrupted": true}, "time": {"start": 1, "end": 2}}
            }
        }));
        probe.stats.pending_peer_question.store(true, Ordering::SeqCst);
        probe.events.send(serde_json::json!({"directory": env!("CARGO_MANIFEST_DIR"), "payload": {
            "id": "evt_late_cancelled_question", "type": "question.asked", "properties": {
                "id": "que_probe_peer", "sessionID": "ses_probe",
                "tool": {"messageID": "msg_000000000006abcdefghijklmn", "callID": "call_retained_question_peer"},
                "questions": [{"question": "Continue?", "header": "Probe", "options": [], "multiple": false, "custom": true}]
            }
        }}).to_string()).unwrap();
        // The event owner can buffer this ordered overlap while Q1's reject is
        // held. Release Q1 before requiring Q2's committed admission.
        probe.stats.rejection_release.add_permits(1);
        let peer = tokio::time::timeout_at(close_deadline, async {
            loop {
                let records = faults.inner.list(StateListRequest::new(OPENCODE_CONTINUATION_NAMESPACE).unwrap()).await.unwrap().records;
                if let Some(peer) = records.into_iter().find(|record| record.record.value.pointer("/request/question/id").and_then(serde_json::Value::as_str) == Some("que_probe_peer")) {
                    break peer;
                }
                tokio::task::yield_now().await;
            }
        }).await.expect("the late native question must actually enter backing storage");
        assert_eq!(peer.record.value["generation_id"], generation);
        assert_eq!(peer.record.value.pointer("/request/question/sessionID").and_then(serde_json::Value::as_str), Some("ses_probe"));
        assert_eq!(peer.record.value.pointer("/request/question/tool/messageID").and_then(serde_json::Value::as_str), Some("msg_000000000006abcdefghijklmn"));
        assert_eq!(peer.record.value.pointer("/request/question/tool/callID").and_then(serde_json::Value::as_str), Some("call_retained_question_peer"));
        eprintln!("late question committed after terminal cancellation: state={:?}", peer.record.value["state"]);
        tokio::time::timeout_at(close_deadline, async {
            tokio::select! {
                biased;
                permit = probe.stats.rejection_entered.acquire() => permit.unwrap().forget(),
                result = closes.join_next() => panic!("Close must settle the newly admitted question before returning: {result:?}"),
            }
        }).await.expect("the late same-generation question must be rejected within the original Close deadline");
        assert!(!probe.stats.pending_question.load(Ordering::SeqCst));
        assert!(probe.stats.pending_peer_question.load(Ordering::SeqCst));
        assert_eq!(probe.stats.question_rejections.load(Ordering::SeqCst), 2);
        probe.stats.rejection_release.add_permits(1);
        tokio::time::timeout_at(close_deadline, closes.join_next()).await
            .expect("Close must retain its original lifecycle deadline").unwrap()
            .expect("Close observer must join").expect("all same-generation native obligations must settle");
        assert!(faults.prompt_snapshot(&id).await.is_none());
        assert!(!probe.stats.pending_peer_question.load(Ordering::SeqCst));
        assert_eq!(probe.stats.question_rejections.load(Ordering::SeqCst), 2);
        assert_eq!(probe.stats.question_replies.load(Ordering::SeqCst), 0);
        assert_eq!(probe.stats.messages.load(Ordering::SeqCst), 1);
        assert_eq!(probe.stats.aborts.load(Ordering::SeqCst), 1);
        let terminal = output.recv().await.expect("one prompt terminal").expect_err("cancelled work cannot complete successfully");
        assert_eq!(terminal.kind(), ErrorKind::Cancelled);
        assert!(output.recv().await.is_none());
        let mut delivered = BTreeSet::from([first_notification]);
        while let Some(item) = notifications.recv().await {
            let item = item.unwrap();
            assert!(delivered.insert(item.id().clone()), "drain cannot replay a notification");
        }
        assert!(notifications.recv().await.is_none());
    }).await;

    if !matches!(&observed, Ok(Ok(()))) {
        // Observe retained failure state before fixture settlement changes it.
        // This extra read neither renews Close's deadline nor replaces its
        // original failure; output is limited to identities and state tags.
        let retained = tokio::time::timeout(
            Duration::from_secs(1),
            faults
                .inner
                .list(StateListRequest::new(OPENCODE_CONTINUATION_NAMESPACE).unwrap()),
        )
        .await;
        match retained {
            Ok(Ok(records)) => eprintln!(
                "late question failure retained state: {}",
                serde_json::json!({
                    "continuation_count": records.records.len(),
                    "continuations": records.records.iter().take(2).map(|record| serde_json::json!({
                        "request_id": record.record.value.pointer("/request/question/id"),
                        "generation_id": record.record.value.get("generation_id"),
                        "state": record.record.value.get("state"),
                    })).collect::<Vec<_>>(),
                    "native_rejections": probe.stats.question_rejections.load(Ordering::SeqCst),
                    "first_native_pending": probe.stats.pending_question.load(Ordering::SeqCst),
                    "peer_native_pending": probe.stats.pending_peer_question.load(Ordering::SeqCst),
                })
            ),
            Ok(Err(error)) => eprintln!("late question failure state read failed: {error:?}"),
            Err(_) => eprintln!("late question failure state read reached its one-second bound"),
        }
    }

    // Independent fixture settlement after the oracle cannot change a failed
    // Close into a pass. Release every held socket and then join all owners.
    probe.stats.rejection_release.add_permits(4);
    probe.stats.prompt_release.add_permits(2);
    probe.release_history();
    for request in ["que_probe_pending", "que_probe_peer"] {
        let _ = probe.events.send(
            serde_json::json!({"directory": env!("CARGO_MANIFEST_DIR"), "payload": {
                "id": format!("evt_late_question_cleanup_{request}"), "type": "question.rejected",
                "properties": {"sessionID": "ses_probe", "requestID": request}
            }})
            .to_string(),
        );
    }
    probe.stats.pending_question.store(false, Ordering::SeqCst);
    probe
        .stats
        .pending_peer_question
        .store(false, Ordering::SeqCst);
    let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
    cleanup
        .settle(async {
            while let Some(result) = closes.join_next().await {
                result.expect("Close task must join")?;
            }
            Ok::<(), SdkError>(())
        })
        .await;
    closes.abort_all();
    cleanup
        .settle(async {
            while let Some(result) = closes.join_next().await {
                match result {
                    Ok(result) => result?,
                    Err(error) => assert!(error.is_cancelled()),
                }
            }
            Ok::<(), SdkError>(())
        })
        .await;
    if let Some(current) = conversation.take() {
        cleanup.settle(current.close()).await;
    }
    cleanup.settle(workspace.close()).await;
    cleanup.settle(agent.shutdown()).await;
    let stats = Arc::clone(&probe.stats);
    let mut peer_task = probe.task.take().expect("the native peer remains tracked");
    let mut peer_joined = false;
    cleanup
        .settle(async {
            probe.shutdown().await;
            let result = (&mut peer_task).await;
            peer_joined = true;
            result.expect("the native peer and every socket must join");
            Ok::<(), SdkError>(())
        })
        .await;
    if !peer_joined {
        peer_task.abort();
        cleanup
            .settle(async {
                match (&mut peer_task).await {
                    Ok(()) => {}
                    Err(error) => assert!(error.is_cancelled()),
                }
                Ok::<(), SdkError>(())
            })
            .await;
    }
    cleanup
        .settle(async {
            assert_eq!(stats.active.load(Ordering::SeqCst), 0);
            assert_eq!(stats.connections.load(Ordering::SeqCst), 0);
            Ok::<(), SdkError>(())
        })
        .await;
    cleanup.finish(observed);
}

#[tokio::test]
async fn opencode_last_handle_drop_preserves_known_execution_error_during_native_cleanup() {
    let mut case = Some(OpenCodeStreamCase::start().await);
    let mut aborts = JoinSet::new();
    let stats = Arc::clone(&case.as_ref().unwrap().probe.stats);
    let mut output = None;
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(25),
        async {
            let current = case.as_ref().unwrap();
            current.probe.stats.retained_question_history.store(true, Ordering::SeqCst);
            current.probe.stats.retain_question_after_abort.store(true, Ordering::SeqCst);
            *current.probe.stats.retained_rejection.lock().unwrap() = RetainedRejection::Held;
            let mut notifications = current.conversation.notification_stream().unwrap();
            let mut stream = current.conversation.prompt_stream(
                PromptRequest::new("ask one owned question").unwrap(),
            ).await.unwrap();
            loop {
                tokio::select! {
                    item = notifications.recv() => {
                        let item = item.unwrap().unwrap();
                        if matches!(item.kind(), ConversationNotificationKind::Interaction { .. }) {
                            break;
                        }
                    }
                    result = stream.recv() => panic!("question must enter before controlled cleanup: {result:?}"),
                }
            }
            drop(notifications);
            let conversation = Arc::clone(&current.conversation);
            let observer = aborts.spawn(async move { conversation.abort().await });
            tokio::select! {
                biased;
                permit = current.probe.stats.rejection_entered.acquire() => permit.unwrap().forget(),
                result = aborts.join_next() => panic!("known cancellation must retain its native cleanup: {result:?}"),
            }
            // A live generation cannot be recovered by the Abort caller. The
            // held native reject therefore follows the foreground's normalized
            // Cancelled result and durable terminalization, before publication.
            assert_eq!(current.probe.stats.question_rejections.load(Ordering::SeqCst), 1);
            assert_eq!(current.probe.stats.aborts.load(Ordering::SeqCst), 1);
            assert_eq!(current.probe.stats.messages.load(Ordering::SeqCst), 1);
            assert!(current.probe.stats.pending_question.load(Ordering::SeqCst));
            observer.abort();
            assert!(aborts.join_next().await.unwrap().expect_err("only the borrowed caller observer is cancelled").is_cancelled());
            output = Some(stream);
        },
    ).await;
    // Keep the native reject held only after its causal barrier was reached.
    // Failed setup instead releases every fixture gate before independent cleanup.
    let ready = matches!(&observed, Ok(Ok(())));
    if !ready && let Some(current) = case.as_ref() {
        current.probe.stats.rejection_release.add_permits(2);
        current.probe.stats.prompt_release.add_permits(2);
        current.probe.release_history();
    }
    aborts.abort_all();
    let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
    cleanup
        .settle(async {
            while let Some(result) = aborts.join_next().await {
                match result {
                    Ok(result) => result?,
                    Err(error) => assert!(error.is_cancelled(), "Abort observer panicked: {error}"),
                }
            }
            Ok::<(), SdkError>(())
        })
        .await;
    if let Some(current) = case.take() {
        // Revoke outside the setup observation deadline. The existing owner
        // drops all hierarchy handles, checks bounded relative task drainage,
        // and joins the peer before any terminal-error assertion.
        cleanup
            .settle(async {
                current.revoke().await;
                Ok::<(), SdkError>(())
            })
            .await;
    }
    if ready {
        cleanup.settle(async {
            let stream = output.as_mut().expect("the prepared output remains owned");
            let error = tokio::time::timeout(Duration::from_secs(3), stream.recv()).await
                .expect("last-handle revocation must end the output observation")
                .expect("one terminal outcome must be retained")
                .expect_err("known execution failure cannot become completion");
            eprintln!("known-error last-handle Drop terminal: {error:?}");
            assert_eq!(error.context().operation, Operation::Prompt);
            if error.kind() != ErrorKind::Cancelled {
                assert_eq!(error.kind(), ErrorKind::Closed);
                assert_eq!(error.context().phase, ErrorPhase::Publication,
                    "only a real hierarchy publication rejection can supersede the known execution error");
            }
            let mut pending = vec![&error];
            let mut original = false;
            while let Some(error) = pending.pop() {
                assert!(!error.diagnostics_truncated());
                assert_ne!(error.detail(), Some(&ErrorDetail::TaskTerminated));
                if error.kind() == ErrorKind::Cancelled {
                    assert_eq!(error.context().operation, Operation::Prompt);
                    assert_eq!(error.context().phase, ErrorPhase::CompletionValidation);
                    assert!(error.detail().is_none());
                    assert!(error.http_status().is_none());
                    assert!(error.cause().is_none());
                    original = true;
                }
                if let Some(cause) = error.cause() {
                    pending.push(cause);
                }
                pending.extend(error.supplementary());
            }
            assert!(original, "revoking the prompt owner must retain the complete earlier cancellation failure: {error:?}");
            assert!(stream.recv().await.is_none());
            assert_eq!(stats.question_rejections.load(Ordering::SeqCst), 1);
            assert_eq!(stats.aborts.load(Ordering::SeqCst), 1);
            assert_eq!(stats.messages.load(Ordering::SeqCst), 1);
            assert_eq!(stats.active.load(Ordering::SeqCst), 0);
            assert_eq!(stats.connections.load(Ordering::SeqCst), 0);

            Ok::<(), SdkError>(())
        }).await;
    }
    cleanup.finish(observed);
}

impl OpenCodeStreamCase {
    async fn verify_reopen_abort_outcome(fault: PromptPersistenceFault, rollback_failure: bool) {
        let mut original = Some(OpenCodeStreamCase::start().await);
        let stats = Arc::clone(&original.as_ref().unwrap().probe.stats);
        stats
            .retained_question_history
            .store(true, Ordering::SeqCst);
        stats
            .retain_question_after_abort
            .store(true, Ordering::SeqCst);
        *stats.retained_rejection.lock().unwrap() = RetainedRejection::Held;
        let faults = original.as_ref().unwrap().faults.clone();
        let baseline = original.as_ref().unwrap().runtime_baseline;
        let mut probe = None;
        let mut reopened_agent = None;
        let mut reopened_workspace = None;
        let mut reopened_conversation = None;
        let mut builds = JoinSet::new();
        let mut closes = JoinSet::new();
        let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(35),
        async {
            let current = original.as_ref().unwrap();
            let agent_id = current.agent.id();
            let workspace_id = current.workspace.id();
            let conversation_id = current.conversation.id();
            let config = current.probe.config(Duration::from_secs(15), Duration::from_secs(5));
            let mut notifications = current.conversation.notification_stream().unwrap();
            let mut output = current.stream().await;
            loop {
                tokio::select! {
                    item = notifications.recv() => {
                        if matches!(item.unwrap().unwrap().kind(), ConversationNotificationKind::Interaction { .. }) {
                            break;
                        }
                    }
                    item = output.recv() => panic!("the original question must be admitted before hierarchy loss: {item:?}"),
                }
            }
            current.wait_for_admission().await;
            let admitted = faults.prompt_snapshot(&conversation_id).await.unwrap();
            let generation = admitted.record.value["message_id"].as_str().filter(|value| !value.is_empty()).unwrap().to_owned();
            let request_part = admitted.record.value["request_part_id"].as_str().filter(|value| !value.is_empty()).unwrap().to_owned();
            let pending = faults.inner.list(StateListRequest::new(OPENCODE_CONTINUATION_NAMESPACE).unwrap()).await.unwrap().records;
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].record.value["state"], "pending");
            assert_eq!(pending[0].record.value.pointer("/request/question/id").and_then(serde_json::Value::as_str), Some("que_probe_pending"));
            assert!(stats.native_active.load(Ordering::SeqCst));
            assert_eq!(stats.aborts.load(Ordering::SeqCst), 0);
            assert_eq!(stats.question_rejections.load(Ordering::SeqCst), 0);
            drop(notifications);
            // Actual hierarchy loss leaves the caller's store and native peer
            // alive. No persisted record is fabricated or restored for reopen.
            let OpenCodeStreamCase { probe: native, agent, workspace, conversation, .. } = original.take().unwrap();
            probe = Some(native);
            drop(conversation);
            drop(workspace);
            drop(agent);
            let runtime = tokio::runtime::Handle::current();
            tokio::time::timeout(Duration::from_secs(3), async {
                while runtime.metrics().num_alive_tasks() > baseline + stats.connections.load(Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
            }).await.expect("the original hierarchy's tasks must end before a new owner is admitted");
            drop(output);
            let retained = faults.prompt_snapshot(&conversation_id).await.unwrap();
            assert_eq!(retained.record.value.pointer("/phase/phase").and_then(serde_json::Value::as_str), Some("admitted"));
            assert_eq!(retained.record.value["message_id"].as_str(), Some(generation.as_str()));
            assert_eq!(retained.record.value["request_part_id"].as_str(), Some(request_part.as_str()));
            assert_eq!(faults.inner.load(pending[0].record.key.clone()).await.unwrap().as_ref(), Some(&pending[0]));
            assert!(stats.native_active.load(Ordering::SeqCst));
            assert_eq!(stats.aborts.load(Ordering::SeqCst), 0);
            assert_eq!(stats.question_rejections.load(Ordering::SeqCst), 0);
            faults.state.persistence_attempts.store(0, Ordering::SeqCst);
            faults.state.cancellation_outcome_writes.store(0, Ordering::SeqCst);
            faults.state.cancellation_outcome_parents.lock().unwrap().clear();
            faults.set_persistence_fault(fault);
            faults.state.persistence_conflict.store(rollback_failure, Ordering::SeqCst);
            faults.state.persistence_rollback_failure.store(rollback_failure, Ordering::SeqCst);
            let storage = faults.storage();
            builds.spawn(async move { AgentBuilder::reopen(config, storage, agent_id).build().await });
            let persistent_conflict = matches!(fault, PromptPersistenceFault::AbortOutcomeCommitConflict | PromptPersistenceFault::AbortOutcomeCommitConflictOnce);
            if rollback_failure || persistent_conflict || fault == PromptPersistenceFault::AbortOutcomeCommitUnavailableOnce {
                let result = tokio::time::timeout(Duration::from_secs(7), builds.join_next()).await
                    .expect("outcome persistence must retain the original five-second lifecycle deadline")
                    .unwrap().expect("failed reopen task must join");
                let error = match result {
                    Ok(agent) => {
                        reopened_agent = Some(agent);
                        panic!("unconfirmed cancellation persistence cannot publish a reopened Agent");
                    }
                    Err(error) => error,
                };
                let attempts = faults.state.persistence_attempts.load(Ordering::SeqCst);
                if persistent_conflict {
                    assert_eq!(attempts, 1, "reopen preserves its single definite-conflict attempt");
                    assert_eq!(faults.state.cancellation_outcome_writes.load(Ordering::SeqCst), attempts);
                } else {
                    assert_eq!(attempts, 1, "the selected cancellation outcome fault must actually occur");
                    assert_eq!(faults.state.cancellation_outcome_writes.load(Ordering::SeqCst), 1,
                        "uncertain commit or failed rollback must veto mutation replay");
                }
                assert_eq!(error.context().operation, Operation::Reopen);
                if persistent_conflict {
                    assert_eq!(error.kind(), ErrorKind::Conflict);
                    assert_eq!(error.context().phase, ErrorPhase::StateCommit);
                    assert_eq!(error.detail(), Some(&ErrorDetail::TransactionConflict));
                } else if rollback_failure {
                    assert_eq!(error.kind(), ErrorKind::Conflict);
                    assert_eq!(error.context().phase, ErrorPhase::StateWrite);
                    assert_eq!(error.detail(), Some(&ErrorDetail::TransactionConflict));
                    assert!(!faults.state.persistence_rollback_failure.load(Ordering::SeqCst),
                        "the failed staging transaction must explicitly attempt rollback");
                    assert!(error.supplementary().iter().any(|cleanup|
                        cleanup.context().phase == ErrorPhase::Cleanup
                            && cleanup.detail() == Some(&ErrorDetail::ServiceUnavailable)),
                        "the original conflict must retain failed rollback evidence: {error:?}");
                } else {
                    assert_eq!(error.kind(), ErrorKind::Integration);
                    assert_eq!(error.context().phase, ErrorPhase::StateCommit);
                    assert_eq!(error.detail(), Some(&ErrorDetail::ServiceUnavailable));
                }
                assert_eq!(stats.aborts.load(Ordering::SeqCst), 1,
                    "native abort delivery must never be replayed after its observed result");
                assert_eq!(stats.messages.load(Ordering::SeqCst), 1);
                assert_eq!(stats.question_rejections.load(Ordering::SeqCst), 0,
                    "an unconfirmed cancellation outcome cannot authorize pending-question cleanup");
                assert_eq!(stats.question_replies.load(Ordering::SeqCst), 0);
                let retained = faults.prompt_snapshot(&conversation_id).await.unwrap();
                assert_eq!(retained.record.value["message_id"].as_str(), Some(generation.as_str()));
                assert_eq!(retained.record.value["request_part_id"].as_str(), Some(request_part.as_str()));
                assert_eq!(retained.record.value["cancellation"]["state"], "dispatching",
                    "an uncommitted outcome must retain its exact unresolved generation");
                assert!(stats.pending_question.load(Ordering::SeqCst));
                return;
            }
            tokio::time::timeout(Duration::from_secs(5), async {
                tokio::select! {
                    biased;
                    result = builds.join_next() => {
                        let result = result.unwrap().expect("reopen owner must not panic");
                        match result {
                            Ok(agent) => {
                                reopened_agent = Some(agent);
                                panic!("reopen published before the original pending native question was rejected");
                            }
                            Err(error) => panic!("reopen ended before the original pending native question was rejected: {error:?}"),
                        }
                    }
                    permit = stats.rejection_entered.acquire() => permit.unwrap().forget(),
                }
            }).await.expect("reopen must prepare native cleanup before publishing the Agent");
            assert_eq!(faults.state.persistence_attempts.load(Ordering::SeqCst), usize::from(fault != PromptPersistenceFault::None),
                "the selected outcome commit fault must actually occur");
            assert_eq!(faults.state.cancellation_outcome_writes.load(Ordering::SeqCst),
                if fault == PromptPersistenceFault::AbortOutcomeCommitConflictOnce { 2 } else { 1 },
                "definite conflicts recompute; a confirmed lost receipt only reads back");
            {
                let parents = faults.state.cancellation_outcome_parents.lock().unwrap();
                assert!(!parents.is_empty());
                for parent in parents.iter() {
                    assert_eq!(parent["generation"].as_str(), Some(generation.as_str()));
                    assert_eq!(parent["request_part"].as_str(), Some(request_part.as_str()));
                }
            }
            let held = faults.prompt_snapshot(&conversation_id).await
                .expect("accepted native abort cannot delete the pending cleanup's parent");
            assert_eq!(held.record.value["message_id"].as_str(), Some(generation.as_str()));
            assert_eq!(held.record.value["request_part_id"].as_str(), Some(request_part.as_str()));
            assert_eq!(stats.aborts.load(Ordering::SeqCst), 1);
            assert_eq!(stats.question_rejections.load(Ordering::SeqCst), 1);
            assert_eq!(stats.question_replies.load(Ordering::SeqCst), 0);
            assert_eq!(stats.messages.load(Ordering::SeqCst), 1);
            assert!(stats.pending_question.load(Ordering::SeqCst));
            if let Some(result) = builds.try_join_next() {
                match result.expect("reopen task must not panic while acknowledgment is held") {
                    Ok(agent) => {
                        reopened_agent = Some(agent);
                        panic!("reopen published while native acknowledgment was still held");
                    }
                    Err(error) => panic!("reopen failed while native acknowledgment was still held: {error:?}"),
                }
            }
            stats.rejection_release.add_permits(1);
            reopened_agent = Some(builds.join_next().await.unwrap().expect("reopen task must join").expect("accepted abort plus exact rejection must settle reopen"));
            reopened_workspace = Some(reopened_agent.as_ref().unwrap().get_workspace(workspace_id).await.unwrap());
            reopened_conversation = Some(reopened_workspace.as_ref().unwrap().get_conversation(conversation_id.clone()).await.unwrap());
            let conversation = reopened_conversation.as_ref().unwrap();
            conversation.abort().await.expect("later idle abort must observe the completed recovery");
            let response = conversation.prompt(PromptRequest::new("return the reuse marker").unwrap()).await.unwrap();
            assert_eq!(response.output(), "RETAINED_HISTORY_REUSE");
            assert_eq!(stats.messages.load(Ordering::SeqCst), 2);
            assert_eq!(stats.aborts.load(Ordering::SeqCst), 1);
            assert_eq!(stats.question_rejections.load(Ordering::SeqCst), 1);
            assert_eq!(stats.question_replies.load(Ordering::SeqCst), 0);
            assert!(!stats.pending_question.load(Ordering::SeqCst));
            let conversation = reopened_conversation.take().unwrap();
            closes.spawn(async move { conversation.close().await });
            closes.join_next().await.unwrap().expect("terminal Close task must join").expect("reopened conversation must close after native settlement");
        },
    ).await;
        // Every fixture release is after the captured result. Keep admitted build
        // and consuming Close tasks owned through independent teardown.
        faults.set_persistence_fault(PromptPersistenceFault::None);
        faults
            .state
            .persistence_conflict
            .store(false, Ordering::SeqCst);
        faults
            .state
            .persistence_rollback_failure
            .store(false, Ordering::SeqCst);
        stats.rejection_release.add_permits(4);
        stats.prompt_release.add_permits(4);
        stats.pending_question.store(false, Ordering::SeqCst);
        let _ = stats.events.send(
            serde_json::json!({"directory": env!("CARGO_MANIFEST_DIR"), "payload": {
                "id": "evt_reopen_question_cleanup", "type": "question.rejected",
                "properties": {"sessionID": "ses_probe", "requestID": "que_probe_pending"}
            }})
            .to_string(),
        );
        if let Some(native) = probe.as_ref() {
            native.release_history();
        }
        let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
        cleanup
            .settle(async {
                while let Some(result) = builds.join_next().await {
                    reopened_agent = Some(result.expect("reopen task must join during cleanup")?);
                }
                while let Some(result) = closes.join_next().await {
                    result.expect("Close task must join during cleanup")?;
                }
                Ok::<(), SdkError>(())
            })
            .await;
        builds.abort_all();
        closes.abort_all();
        cleanup
            .settle(async {
                while let Some(result) = builds.join_next().await {
                    match result {
                        Ok(Ok(agent)) => reopened_agent = Some(agent),
                        Ok(Err(error)) => return Err(error),
                        Err(error) => {
                            assert!(error.is_cancelled(), "reopen task panicked: {error}")
                        }
                    }
                }
                while let Some(result) = closes.join_next().await {
                    match result {
                        Ok(result) => result?,
                        Err(error) => assert!(error.is_cancelled(), "Close task panicked: {error}"),
                    }
                }
                Ok::<(), SdkError>(())
            })
            .await;
        drop(reopened_conversation.take());
        drop(reopened_workspace.take());
        drop(reopened_agent.take());
        if let Some(current) = original.take() {
            cleanup
                .settle(async {
                    current.revoke().await;
                    Ok::<(), SdkError>(())
                })
                .await;
        } else if let Some(native) = probe.take() {
            cleanup
                .settle(async {
                    let runtime = tokio::runtime::Handle::current();
                    while runtime.metrics().num_alive_tasks()
                        > baseline + stats.connections.load(Ordering::SeqCst)
                    {
                        tokio::task::yield_now().await;
                    }
                    Ok::<(), SdkError>(())
                })
                .await;
            cleanup
                .settle(async {
                    native.shutdown().await;
                    Ok::<(), SdkError>(())
                })
                .await;
        }
        cleanup
            .settle(async {
                assert_eq!(stats.active.load(Ordering::SeqCst), 0);
                assert_eq!(stats.connections.load(Ordering::SeqCst), 0);
                Ok::<(), SdkError>(())
            })
            .await;
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn opencode_reopen_admitted_question_retains_parent_until_native_rejection() {
    OpenCodeStreamCase::verify_reopen_abort_outcome(PromptPersistenceFault::None, false).await;
}

#[tokio::test]
async fn opencode_reopen_abort_outcome_commit_conflict_remains_single_attempt() {
    OpenCodeStreamCase::verify_reopen_abort_outcome(
        PromptPersistenceFault::AbortOutcomeCommitConflictOnce,
        false,
    )
    .await;
}

#[tokio::test]
async fn opencode_reopen_abort_outcome_persistent_conflict_is_not_retried() {
    OpenCodeStreamCase::verify_reopen_abort_outcome(
        PromptPersistenceFault::AbortOutcomeCommitConflict,
        false,
    )
    .await;
}

#[tokio::test]
async fn opencode_reopen_abort_outcome_lost_receipt_reads_back_without_rewrite() {
    OpenCodeStreamCase::verify_reopen_abort_outcome(
        PromptPersistenceFault::AbortOutcomeCommitReceiptLost,
        false,
    )
    .await;
}

#[tokio::test]
async fn opencode_reopen_abort_outcome_uncertain_commit_does_not_replay() {
    OpenCodeStreamCase::verify_reopen_abort_outcome(
        PromptPersistenceFault::AbortOutcomeCommitUnavailableOnce,
        false,
    )
    .await;
}

#[tokio::test]
async fn opencode_reopen_abort_outcome_failed_rollback_preserves_conflict() {
    OpenCodeStreamCase::verify_reopen_abort_outcome(PromptPersistenceFault::AbortOutcomeOnce, true)
        .await;
}

#[tokio::test]
async fn opencode_accepted_resolution_caller_loss_after_delete_releases_on_exact_late_event() {
    let case = OpenCodeStreamCase::start().await;
    let stats = Arc::clone(&case.probe.stats);
    stats
        .retained_question_history
        .store(true, Ordering::SeqCst);
    stats
        .retain_question_after_abort
        .store(true, Ordering::SeqCst);
    *stats.retained_identity_change.lock().unwrap() =
        Some(RetainedIdentityChange::OrdinaryCompletion);
    *stats.retained_rejection.lock().unwrap() = RetainedRejection::AcknowledgedWithoutEvent;
    let mut resolvers = JoinSet::new();
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(25),
        async {
            let runtime = tokio::runtime::Handle::current();
            let foreground_baseline = runtime.metrics().num_alive_tasks()
                - stats.connections.load(Ordering::SeqCst);
            let mut notifications = case.conversation.notification_stream().unwrap();
            let mut output = case
                .conversation
                .prompt_stream(PromptRequest::new("ask one question").unwrap())
                .await
                .unwrap();
            let interaction = loop {
                tokio::select! {
                    item = notifications.recv() => {
                        let item = item.unwrap().unwrap();
                        if matches!(item.kind(), ConversationNotificationKind::Interaction { .. }) {
                            break item;
                        }
                    }
                    item = output.recv() => panic!("question admission must precede terminal output: {item:?}"),
                }
            };
            let continuations = case.faults.inner.list(
                StateListRequest::new(OPENCODE_CONTINUATION_NAMESPACE).unwrap(),
            ).await.unwrap().records;
            let [pending] = continuations.as_slice() else {
                panic!("one exact owned question is required");
            };
            let pending = pending.clone();
            assert_eq!(pending.record.value["state"], "pending");
            assert_eq!(pending.record.value["notification_id"], interaction.id().as_str());
            assert!(stats.pending_question.load(Ordering::SeqCst));
            stats.native_active.store(false, Ordering::SeqCst);
            OpenCodeEventProbe::retained_question_abort(&stats);
            stats.prompt_release.add_permits(1);
            // The fixture's native-shaped Success is durable. Its unresolved question makes
            // the original public observation Busy; no success is manufactured.
            let terminal = tokio::time::timeout(Duration::from_secs(7), output.recv())
                .await.expect("ordinary retained question must finish its observation")
                .expect("the original stream must publish one terminal")
                .expect_err("Pending question prevents prompt-slot cleanup");
            assert_eq!(terminal.kind(), ErrorKind::Busy, "{terminal:?}");
            assert!(output.recv().await.is_none());
            drop(output);
            tokio::time::timeout(Duration::from_secs(3), async {
                while runtime.metrics().num_alive_tasks()
                    > foreground_baseline + stats.connections.load(Ordering::SeqCst)
                {
                    tokio::task::yield_now().await;
                }
            }).await.expect("the original foreground must end before resolution cleanup");
            let original = case.faults.prompt_snapshot(&case.conversation.id()).await.unwrap();
            assert_eq!(original.record.value["phase"]["phase"], "terminal");
            assert_eq!(original.record.value["phase"]["outcome"]["outcome"], "success");
            assert_eq!(original.record.value["cancellation"]["state"], "unnecessary");
            assert!(matches!(original.record.value["waiter"].as_str(), Some("delivered" | "lost" | "timed_out")));
            let generation = original.record.value["message_id"].as_str()
                .filter(|value| !value.is_empty()).expect("real prompt generation");
            assert!(!original.record.value["request_part_id"].as_str()
                .expect("real native request part").is_empty());
            assert_eq!(pending.record.value["generation_id"].as_str(), Some(generation));

            let conversation = Arc::clone(&case.conversation);
            let notification_id = interaction.id().clone();
            let (start, started) = oneshot::channel();
            let resolver = resolvers.spawn(async move {
                started.await.expect("resolver starts only after its exact task is selected");
                conversation.resolve_notification(
                    notification_id,
                    NotificationResolution::Dismiss { reason: "dismiss this owned question".to_owned() },
                ).await
            });
            case.faults.arm_terminal_reconciliation_task(
                PreparedCancelFault::CommitReceiptHeld,
                resolver.id(),
                PromptFaultStore::prompt_key(&case.conversation.id()),
            );
            start.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                tokio::select! {
                    biased;
                    result = resolvers.join_next() => panic!("resolution returned before its real deletion receipt was held: {result:?}"),
                    () = case.faults.wait_until_blocked() => {}
                }
            }).await.expect("accepted resolution must reach its held actual deletion receipt");
            // Read the real backing store directly. Test readbacks cannot be
            // mistaken for receipt observations by the selected SDK owner.
            assert!(case.faults.inner.load(original.record.key.clone()).await.unwrap().is_none());
            let accepted = case.faults.inner.load(pending.record.key.clone()).await.unwrap().unwrap();
            assert_eq!(accepted.record.value["state"], "accepted");
            assert_eq!(accepted.record.value["request"], pending.record.value["request"]);
            assert_eq!(accepted.record.value["generation_id"].as_str(), Some(generation));
            assert_eq!(accepted.record.value["notification_id"], pending.record.value["notification_id"]);
            assert_eq!(accepted.revision.get(), pending.revision.get() + 1);
            let receipt = case.faults.prepared_cancel_receipt();
            assert_eq!(receipt.before.as_ref().unwrap(), &original);
            assert_eq!(receipt.faults, 1);
            assert_eq!(receipt.deletes, 1);
            assert_eq!(receipt.commit_attempts, 1);
            assert_eq!(receipt.commits, 1);
            assert_eq!(receipt.deletes_after_commit, 0);
            assert_eq!(stats.messages.load(Ordering::SeqCst), 1);
            assert_eq!(stats.aborts.load(Ordering::SeqCst), 0);
            assert_eq!(stats.question_replies.load(Ordering::SeqCst), 0);
            assert_eq!(stats.question_rejections.load(Ordering::SeqCst), 1);
            assert!(!stats.pending_question.load(Ordering::SeqCst));

            resolver.abort();
            let cancelled = tokio::time::timeout(Duration::from_secs(3), resolvers.join_next_with_id())
                .await.expect("the dropped resolver observer must join").unwrap()
                .expect_err("only the selected resolver is cancelled");
            assert_eq!(cancelled.id(), resolver.id());
            assert!(cancelled.is_cancelled());
            case.faults.release();

            case.probe.events.send(serde_json::json!({
                "directory": env!("CARGO_MANIFEST_DIR"),
                "payload": {"id": "evt_caller_loss_question_rejected", "type": "question.rejected",
                    "properties": {"sessionID": "ses_probe", "requestID": "que_probe_pending"}}
            }).to_string()).unwrap();
            let sentinel = "evt_after_caller_loss_question_rejected";
            case.probe.events.send(serde_json::json!({
                "directory": env!("CARGO_MANIFEST_DIR"),
                "payload": {"id": sentinel, "type": "session.status",
                    "properties": {"sessionID": "ses_probe", "status": {"type": "idle"}}}
            }).to_string()).unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let progress = case.faults.inner.list(
                        StateListRequest::new(OPENCODE_EVENT_PROGRESS_NAMESPACE).unwrap(),
                    ).await.unwrap();
                    if progress.records.iter().any(|record| record.record.value["recent"].as_array()
                        .is_some_and(|recent| recent.iter().any(|event| event["id"] == sentinel)))
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            }).await.expect("the exact late event and its postcommit reconciliation must finish");
            assert_eq!(case.faults.inner.load(pending.record.key.clone()).await.unwrap().unwrap(), accepted);
            assert!(case.faults.inner.load(original.record.key.clone()).await.unwrap().is_none());
            let receipt = case.faults.prepared_cancel_receipt();
            assert_eq!(receipt.deletes, 1);
            assert_eq!(receipt.commit_attempts, 1);
            assert_eq!(receipt.commits, 1);
            assert_eq!(receipt.deletes_after_commit, 0);
            // No Abort or Close has run. The finished foreground cannot rescue
            // this slot; only the exact later terminal evidence has reconciled it.
            let reused = case.conversation.prompt(
                PromptRequest::new("return the reuse marker").unwrap(),
            ).await.expect("exact late acceptance and actual deletion must release prompt admission");
            assert_eq!(reused.output(), "RETAINED_HISTORY_REUSE");
            assert_eq!(stats.messages.load(Ordering::SeqCst), 2);
            assert_eq!(stats.aborts.load(Ordering::SeqCst), 0);
            assert_eq!(stats.question_replies.load(Ordering::SeqCst), 0);
            assert_eq!(stats.question_rejections.load(Ordering::SeqCst), 1);
        },
    ).await;

    case.faults.disarm_prepared_cancel();
    case.faults.release();
    case.probe.release_history();
    stats.prompt_release.add_permits(2);
    stats.rejection_release.add_permits(2);
    resolvers.abort_all();
    let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
    cleanup
        .settle(async {
            while let Some(result) = resolvers.join_next().await {
                match result {
                    Ok(result) => result?,
                    Err(error) => {
                        assert!(error.is_cancelled(), "resolver observer panicked: {error}")
                    }
                }
            }
            Ok::<(), SdkError>(())
        })
        .await;
    cleanup
        .settle(async {
            case.revoke().await;
            assert_eq!(stats.active.load(Ordering::SeqCst), 0);
            assert_eq!(stats.connections.load(Ordering::SeqCst), 0);
            Ok::<(), SdkError>(())
        })
        .await;
    cleanup.finish(observed);
}

#[tokio::test]
async fn opencode_stale_accepted_event_does_not_release_newer_unpersisted_prompt() {
    let case = OpenCodeStreamCase::start().await;
    let stats = Arc::clone(&case.probe.stats);
    stats
        .retained_question_history
        .store(true, Ordering::SeqCst);
    stats
        .retain_question_after_abort
        .store(true, Ordering::SeqCst);
    *stats.retained_identity_change.lock().unwrap() =
        Some(RetainedIdentityChange::OrdinaryCompletion);
    *stats.retained_rejection.lock().unwrap() = RetainedRejection::AcknowledgedWithoutEvent;
    let mut newer_output = None;
    let mut unexpected_output = None;
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(25),
        async {
            let runtime = tokio::runtime::Handle::current();
            let foreground_baseline = runtime.metrics().num_alive_tasks()
                - stats.connections.load(Ordering::SeqCst);
            let mut notifications = case.conversation.notification_stream().unwrap();
            let mut output = case
                .conversation
                .prompt_stream(PromptRequest::new("ask one question").unwrap())
                .await
                .unwrap();
            let interaction = loop {
                tokio::select! {
                    item = notifications.recv() => {
                        let item = item.unwrap().unwrap();
                        if matches!(item.kind(), ConversationNotificationKind::Interaction { .. }) {
                            break item;
                        }
                    }
                    item = output.recv() => panic!("question admission must precede terminal output: {item:?}"),
                }
            };
            let continuations = case.faults.inner.list(
                StateListRequest::new(OPENCODE_CONTINUATION_NAMESPACE).unwrap(),
            ).await.unwrap().records;
            let [pending] = continuations.as_slice() else {
                panic!("one exact owned question is required");
            };
            let pending = pending.clone();
            assert_eq!(pending.record.value["state"], "pending");
            assert_eq!(pending.record.value["notification_id"], interaction.id().as_str());
            assert!(stats.pending_question.load(Ordering::SeqCst));
            stats.native_active.store(false, Ordering::SeqCst);
            OpenCodeEventProbe::retained_question_abort(&stats);
            stats.prompt_release.add_permits(1);
            // The fixture's native-shaped Success is durable. Its unresolved question makes
            // the original public observation Busy; no success is manufactured.
            let terminal = tokio::time::timeout(Duration::from_secs(7), output.recv())
                .await.expect("ordinary retained question must finish its observation")
                .expect("the original stream must publish one terminal")
                .expect_err("Pending question prevents prompt-slot cleanup");
            assert_eq!(terminal.kind(), ErrorKind::Busy, "{terminal:?}");
            assert!(output.recv().await.is_none());
            drop(output);
            tokio::time::timeout(Duration::from_secs(3), async {
                while runtime.metrics().num_alive_tasks()
                    > foreground_baseline + stats.connections.load(Ordering::SeqCst)
                {
                    tokio::task::yield_now().await;
                }
            }).await.expect("the original foreground must end before resolution cleanup");
            let original = case.faults.prompt_snapshot(&case.conversation.id()).await.unwrap();
            assert_eq!(original.record.value["phase"]["phase"], "terminal");
            assert_eq!(original.record.value["phase"]["outcome"]["outcome"], "success");
            assert_eq!(original.record.value["cancellation"]["state"], "unnecessary");
            assert!(matches!(original.record.value["waiter"].as_str(), Some("delivered" | "lost" | "timed_out")));
            let generation = original.record.value["message_id"].as_str()
                .filter(|value| !value.is_empty()).expect("real prompt generation");
            assert!(!original.record.value["request_part_id"].as_str()
                .expect("real native request part").is_empty());
            assert_eq!(pending.record.value["generation_id"].as_str(), Some(generation));

            case.conversation.resolve_notification(
                interaction.id().clone(),
                NotificationResolution::Dismiss { reason: "dismiss only the original question".to_owned() },
            ).await.expect("normal accepted resolution must settle original cleanup");
            let accepted = case.faults.inner.load(pending.record.key.clone()).await.unwrap().unwrap();
            assert_eq!(accepted.record.value["state"], "accepted");
            assert_eq!(accepted.record.value["request"], pending.record.value["request"]);
            assert_eq!(accepted.record.value["generation_id"].as_str(), Some(generation));
            assert_eq!(accepted.record.value["notification_id"], pending.record.value["notification_id"]);
            assert_eq!(accepted.revision.get(), pending.revision.get() + 1);
            assert!(case.faults.inner.load(original.record.key.clone()).await.unwrap().is_none());
            assert_eq!(stats.messages.load(Ordering::SeqCst), 1);
            assert_eq!(stats.question_rejections.load(Ordering::SeqCst), 1);
            assert_eq!(stats.question_replies.load(Ordering::SeqCst), 0);
            assert_eq!(stats.aborts.load(Ordering::SeqCst), 0);
            assert!(!stats.pending_question.load(Ordering::SeqCst));

            // Drain preceding native events before the next-begin gate so it
            // belongs to the publicly admitted newer foreground.
            let ready = "evt_before_newer_prompt_begin_gate";
            case.probe.events.send(serde_json::json!({
                "directory": env!("CARGO_MANIFEST_DIR"),
                "payload": {"id": ready, "type": "session.status",
                    "properties": {"sessionID": "ses_probe", "status": {"type": "idle"}}}
            }).to_string()).unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let progress = case.faults.inner.list(
                        StateListRequest::new(OPENCODE_EVENT_PROGRESS_NAMESPACE).unwrap(),
                    ).await.unwrap();
                    if progress.records.iter().any(|record| record.record.value["recent"].as_array()
                        .is_some_and(|recent| recent.iter().any(|event| event["id"] == ready)))
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            }).await.expect("prior native events must finish before the newer begin gate");

            // This default probe emits no unsolicited events. Preflight uses
            // reads; the next transaction belongs to the newly admitted
            // foreground. Returning this public stream establishes registration
            // before its first durable transaction is allowed to begin.
            case.faults.block_next_transaction_begin();
            newer_output = Some(case.conversation.prompt_stream(
                PromptRequest::new("return the reuse marker").unwrap(),
            ).await.expect("newer prompt must be publicly admitted before its durable write"));
            case.faults.wait_until_transaction_begin_blocked().await;
            assert!(case.faults.inner.load(original.record.key.clone()).await.unwrap().is_none(),
                "the newer real registration has not written a prompt row");
            assert_eq!(stats.messages.load(Ordering::SeqCst), 1,
                "the held newer foreground cannot yet dispatch native work");

            case.probe.events.send(serde_json::json!({
                "directory": env!("CARGO_MANIFEST_DIR"),
                "payload": {"id": "evt_old_accepted_question_new_prompt", "type": "question.rejected",
                    "properties": {"sessionID": "ses_probe", "requestID": "que_probe_pending"}}
            }).to_string()).unwrap();
            let sentinel = "evt_after_old_accepted_question_new_prompt";
            case.probe.events.send(serde_json::json!({
                "directory": env!("CARGO_MANIFEST_DIR"),
                "payload": {"id": sentinel, "type": "session.status",
                    "properties": {"sessionID": "ses_probe", "status": {"type": "idle"}}}
            }).to_string()).unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let progress = case.faults.inner.list(
                        StateListRequest::new(OPENCODE_EVENT_PROGRESS_NAMESPACE).unwrap(),
                    ).await.unwrap();
                    if progress.records.iter().any(|record| record.record.value["recent"].as_array()
                        .is_some_and(|recent| recent.iter().any(|event| event["id"] == sentinel)))
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            }).await.expect("the stale exact acceptance must complete before checking the newer owner");
            assert_eq!(case.faults.inner.load(pending.record.key.clone()).await.unwrap().unwrap(), accepted);
            assert!(case.faults.inner.load(original.record.key.clone()).await.unwrap().is_none());
            let mut next = Box::pin(newer_output.as_mut().unwrap().recv());
            std::future::poll_fn(|context| match std::future::Future::poll(next.as_mut(), context) {
                std::task::Poll::Pending => std::task::Poll::Ready(()),
                std::task::Poll::Ready(item) => panic!("stale acceptance ended the newer observer: {item:?}"),
            }).await;
            drop(next);
            match case.conversation.prompt_stream(
                PromptRequest::new("must remain blocked by the newer prompt").unwrap(),
            ).await {
                Err(error) => assert_eq!(error.kind(), ErrorKind::Busy, "{error:?}"),
                Ok(stream) => {
                    unexpected_output = Some(stream);
                    panic!("stale acceptance released the newer prompt's admission");
                }
            }
            assert_eq!(stats.messages.load(Ordering::SeqCst), 1);
            assert_eq!(stats.question_rejections.load(Ordering::SeqCst), 1);
            assert_eq!(stats.question_replies.load(Ordering::SeqCst), 0);
            assert_eq!(stats.aborts.load(Ordering::SeqCst), 0);

            case.faults.release_transaction_begin();
            let completed = tokio::time::timeout(Duration::from_secs(7), async {
                loop {
                    match newer_output.as_mut().unwrap().recv().await.unwrap().unwrap() {
                        PromptEvent::OutputDelta { .. } => {}
                        PromptEvent::Completed(response) => break response,
                    }
                }
            }).await.expect("the unchanged newer prompt must finish after its own gate opens");
            assert_eq!(completed.output(), "RETAINED_HISTORY_REUSE");
            assert!(newer_output.as_mut().unwrap().recv().await.is_none());
            assert_eq!(stats.messages.load(Ordering::SeqCst), 2);
            assert_eq!(stats.question_rejections.load(Ordering::SeqCst), 1);
            assert_eq!(stats.question_replies.load(Ordering::SeqCst), 0);
            assert_eq!(stats.aborts.load(Ordering::SeqCst), 0);
            assert_eq!(case.faults.inner.load(pending.record.key.clone()).await.unwrap().unwrap(), accepted);
        },
    ).await;

    // Both public stream observations remain owned until independent fixture
    // cleanup. Releasing the real begin gate precedes revoking the hierarchy.
    case.faults.release_transaction_begin();
    case.faults.release();
    case.probe.release_history();
    stats.prompt_release.add_permits(2);
    stats.rejection_release.add_permits(2);
    drop(newer_output);
    drop(unexpected_output);
    let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
    cleanup
        .settle(async {
            case.revoke().await;
            assert_eq!(stats.active.load(Ordering::SeqCst), 0);
            assert_eq!(stats.connections.load(Ordering::SeqCst), 0);
            Ok::<(), SdkError>(())
        })
        .await;
    cleanup.finish(observed);
}

#[derive(Clone, Copy, Debug)]
enum QuestionReplyAdmission {
    InitialRefusal,
    ResponseLoss,
    RedirectedEffect,
    NoFollow,
    BadRequest,
    MissingRequest,
    ResponseLossSettlementFailure,
}

impl QuestionReplyAdmission {
    async fn verify(self) {
        let case = OpenCodeStreamCase::start().await;
        let stats = Arc::clone(&case.probe.stats);
        case.probe.request_question_on_prompt();
        let mut expected_requests = 0;
        let mut expected_effects = 0;
        let mut redirect_target = None;
        let mut redirect_stats = None;
        let observed = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(25),
            async {
                let mut receiver = case.conversation.notification_stream().unwrap();
                let mut output = case.stream().await;
                let interaction = loop {
                    tokio::select! {
                        item = receiver.recv() => {
                            let item = item.unwrap().unwrap();
                            if matches!(item.kind(), ConversationNotificationKind::Interaction { .. }) { break item; }
                        }
                        item = output.recv() => panic!("question must precede terminal output: {item:?}"),
                    }
                };
                case.wait_for_admission().await;
                let key = StateKey::new(OPENCODE_CONTINUATION_NAMESPACE,
                    format!("{}:que_probe", case.conversation.id().as_str())).unwrap();
                let pending = case.faults.inner.load(key.clone()).await.unwrap().unwrap();
                assert_eq!(pending.record.value["state"], "pending");
                assert_eq!(pending.record.value["notification_id"], interaction.id().as_str());
                let subscriptions = stats.subscriptions.load(Ordering::SeqCst);
                expected_requests = usize::from(!matches!(self, Self::InitialRefusal));
                expected_effects = usize::from(!matches!(self, Self::InitialRefusal | Self::BadRequest | Self::MissingRequest));
                match self {
                    Self::InitialRefusal => {
                        stats.listener_pause.notify_one();
                        tokio::time::timeout(Duration::from_secs(3), stats.listener_paused.acquire())
                            .await.expect("the owned listener must actually stop").unwrap().forget();
                        assert_eq!(stats.active.load(Ordering::SeqCst), 1,
                            "accepted SSE must survive listener suspension");
                    }
                    Self::ResponseLoss | Self::ResponseLossSettlementFailure => {
                        stats.lose_question_reply_response.store(true, Ordering::SeqCst);
                        if matches!(self, Self::ResponseLossSettlementFailure) {
                            case.faults.obscure_next_continuation_reconciliation_commit();
                            case.faults.fail_next_post_commit_continuation_reconciliation();
                        }
                    }
                    Self::RedirectedEffect => {
                        // The response handler first records the effect, then
                        // awaits actual listener drop before sending this redirect.
                        stats.pause_before_reply_redirect.store(true, Ordering::SeqCst);
                        *stats.question_reply_redirect.lock().unwrap() = Some(format!(
                            "{}/question/que_probe/reply?redirected=1", case.probe.endpoint));
                    }
                    Self::NoFollow => {
                        let target = OpenCodeEventProbe::start(0).await;
                        *stats.question_reply_redirect.lock().unwrap() = Some(format!(
                            "{}/question/que_probe/reply", target.endpoint));
                        redirect_stats = Some(Arc::clone(&target.stats));
                        redirect_target = Some(target);
                    }
                    Self::BadRequest => { stats.question_reply_status.store(400, Ordering::SeqCst); }
                    Self::MissingRequest => { stats.question_reply_status.store(404, Ordering::SeqCst); }
                }
                let resolution = NotificationResolution::Submit {
                    content: serde_json::json!({"answers": [["Yes"]]}),
                };
                let result = tokio::time::timeout(Duration::from_secs(7),
                    case.conversation.resolve_notification(interaction.id().clone(), resolution.clone()))
                    .await.expect("native resolution retains the existing lifecycle bound");
                if matches!(self, Self::InitialRefusal | Self::RedirectedEffect) {
                    stats.listener_resume.add_permits(1);
                    tokio::time::timeout(Duration::from_secs(3), stats.listener_resumed.acquire())
                        .await.expect("the same owned listener must restore").unwrap().forget();
                }
                let current = case.faults.inner.load(key.clone()).await.unwrap().unwrap();
                eprintln!("question reply admission {self:?}: result={result:?}; state={}; requests={}; effects={}; redirects={}; target_requests={:?}",
                    current.record.value["state"], stats.question_replies.load(Ordering::SeqCst),
                    stats.question_reply_acceptances.load(Ordering::SeqCst), stats.question_reply_redirects.load(Ordering::SeqCst),
                    redirect_stats.as_ref().map(|target| target.question_replies.load(Ordering::SeqCst)));
                let failure = result.expect_err("the selected missing or redirect receipt cannot be success");
                assert_eq!(failure.context().operation, Operation::ResolveNotification);
                match self {
                    Self::ResponseLossSettlementFailure => {
                        assert_eq!(case.faults.ambiguous_continuation_reconciliations(), 1);
                        assert_eq!(case.faults.continuation_failures(), 1);
                    }
                    Self::NoFollow => {
                        assert_eq!(failure.kind(), ErrorKind::Integration, "{failure:?}");
                        assert_eq!(failure.http_status(), Some(307), "{failure:?}");
                        assert_eq!(failure.context().phase, ErrorPhase::ResponseRead);
                        assert_eq!(failure.detail(), Some(&ErrorDetail::RemoteRejected));
                        assert_eq!(redirect_stats.as_ref().unwrap().question_replies.load(Ordering::SeqCst), 0);
                    }
                    Self::BadRequest | Self::MissingRequest => {
                        let (status, kind, detail) = if matches!(self, Self::BadRequest) {
                            (400, ErrorKind::InvalidInput, ErrorDetail::InvalidRequest)
                        } else {
                            (404, ErrorKind::NotFound, ErrorDetail::ResourceMissing)
                        };
                        assert_eq!(failure.kind(), kind, "{failure:?}");
                        assert_eq!(failure.http_status(), Some(status), "{failure:?}");
                        assert_eq!(failure.context().phase, ErrorPhase::ResponseRead);
                        assert_eq!(failure.detail(), Some(&detail));
                    }
                    Self::InitialRefusal | Self::ResponseLoss => {
                        assert_eq!(failure.kind(), ErrorKind::Integration, "{failure:?}");
                        assert_eq!(failure.context().phase, ErrorPhase::RequestSend, "{failure:?}");
                        assert_eq!(failure.detail(), Some(&ErrorDetail::TransportFailure));
                        assert!(failure.http_status().is_none());
                    }
                    Self::RedirectedEffect => {
                        // This row proves the acceptance fence, not a no-follow
                        // policy. The separate owned-target row proves no dispatch.
                        assert_eq!(stats.question_reply_redirects.load(Ordering::SeqCst), 1);
                    }
                }
                if matches!(self, Self::InitialRefusal) {
                    // Isolate initial send refusal from event-owner loss; other
                    // transport rows do not freeze subscription choreography.
                    assert_eq!(stats.subscriptions.load(Ordering::SeqCst), subscriptions);
                    assert_eq!(stats.active.load(Ordering::SeqCst), 1);
                }
                assert_eq!(stats.question_replies.load(Ordering::SeqCst), expected_requests);
                assert_eq!(stats.question_reply_acceptances.load(Ordering::SeqCst), expected_effects);
                let notification_key = StateKey::new("agent_sdk_conversation_notifications_v1",
                    case.conversation.id().as_str()).unwrap();
                let notifications = case.faults.inner.load(notification_key.clone()).await.unwrap().unwrap();
                let target = notifications.record.value["pending"].as_array().unwrap().iter()
                    .find(|item| item["id"].as_str() == Some(interaction.id().as_str())).unwrap();
                let mut expected = (*pending.record.value).clone();
                if matches!(self, Self::InitialRefusal | Self::BadRequest) {
                    stats.question_reply_status.store(200, Ordering::SeqCst);
                    let retried = case.conversation.resolve_notification(interaction.id().clone(), resolution).await;
                    eprintln!("question reply fresh explicit retry {self:?}: {retried:?}");
                    if retried.is_ok() { expected_requests += 1; expected_effects += 1; }
                    assert_eq!(*current.record.value, expected,
                        "proved preacceptance failure must restore the exact Pending continuation");
                    assert_eq!(target["state"]["state"], "pending");
                    retried.expect("only a fresh explicit caller resolution may send after preacceptance failure");
                    expected["state"] = serde_json::json!("accepted");
                    assert_eq!(*case.faults.inner.load(key).await.unwrap().unwrap().record.value, expected);
                    let common = case.faults.inner.load(notification_key).await.unwrap().unwrap();
                    assert!(common.record.value["pending"].as_array().unwrap().iter()
                        .all(|item| item["state"]["state"] != "resolving"));
                } else {
                    expected["state"] = serde_json::json!("ambiguous");
                    assert_eq!(*current.record.value, expected);
                    assert_eq!(current.revision.get(), pending.revision.get() + 1);
                    assert_eq!(target["state"]["state"], "resolving");
                    assert_eq!(target["state"]["detail"]["acceptance"], "ambiguous");
                    let duplicate = case.conversation.resolve_notification(interaction.id().clone(), resolution).await
                        .expect_err("uncertain acceptance cannot admit a second native answer");
                    assert!(matches!(duplicate.kind(), ErrorKind::Conflict | ErrorKind::Busy), "{duplicate:?}");
                    assert_eq!(case.faults.inner.load(key).await.unwrap(), Some(current));
                }
                assert_eq!(stats.question_replies.load(Ordering::SeqCst), expected_requests);
                assert_eq!(stats.question_reply_acceptances.load(Ordering::SeqCst), expected_effects);
                assert_eq!(stats.question_rejections.load(Ordering::SeqCst), 0);
                assert_eq!(stats.aborts.load(Ordering::SeqCst), 0);
                assert_eq!(stats.messages.load(Ordering::SeqCst), 1);
                if matches!(self, Self::ResponseLossSettlementFailure) {
                    // Both storage facts and the already observed native failure
                    // must survive; their root/supplementary order is not fixed.
                    let mut failures = vec![&failure];
                    let mut native = false;
                    let mut commit = false;
                    let mut read = false;
                    while let Some(error) = failures.pop() {
                        assert!(!error.diagnostics_truncated(), "{failure:?}");
                        if matches!(error.context().phase, ErrorPhase::RequestSend | ErrorPhase::StateCommit | ErrorPhase::StateRead) {
                            assert_eq!(error.kind(), ErrorKind::Integration, "{failure:?}");
                            assert_eq!(error.context().operation, Operation::ResolveNotification, "{failure:?}");
                            match error.context().phase {
                                ErrorPhase::RequestSend => {
                                    assert_eq!(error.detail(), Some(&ErrorDetail::TransportFailure), "{failure:?}");
                                    native = true;
                                }
                                ErrorPhase::StateCommit => {
                                    assert_eq!(error.detail(), Some(&ErrorDetail::ServiceUnavailable), "{failure:?}");
                                    commit = true;
                                }
                                ErrorPhase::StateRead => {
                                    assert_eq!(error.detail(), Some(&ErrorDetail::ServiceUnavailable), "{failure:?}");
                                    read = true;
                                }
                                _ => unreachable!(),
                            }
                        }
                        failures.extend(error.cause());
                        failures.extend(error.supplementary());
                    }
                    assert!(native && commit && read, "all three complete failures must survive: {failure:?}");
                }
            },
        ).await;
        stats.listener_resume.add_permits(1);
        stats
            .lose_question_reply_response
            .store(false, Ordering::SeqCst);
        stats
            .pause_before_reply_redirect
            .store(false, Ordering::SeqCst);
        stats.question_reply_status.store(200, Ordering::SeqCst);
        *stats.question_reply_redirect.lock().unwrap() = None;
        case.faults
            .state
            .obscure_continuation_reconciliation_commit
            .store(false, Ordering::SeqCst);
        case.faults
            .state
            .fail_continuation_after_commit
            .store(false, Ordering::SeqCst);
        case.faults
            .state
            .continuation_load_countdown
            .store(0, Ordering::SeqCst);
        case.release();
        let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
        if let Some(mut target) = redirect_target.take() {
            // Keep the peer's actual JoinHandle outside bounded observations.
            let mut task = target.task.take().unwrap();
            let mut joined = false;
            cleanup
                .settle(async {
                    target.shutdown().await;
                    (&mut task).await.expect("redirect target must join");
                    joined = true;
                    Ok::<(), SdkError>(())
                })
                .await;
            if !joined {
                task.abort();
                cleanup
                    .settle(async {
                        match (&mut task).await {
                            Ok(()) => {}
                            Err(error) => {
                                assert!(error.is_cancelled(), "redirect target panicked: {error}")
                            }
                        }
                        Ok::<(), SdkError>(())
                    })
                    .await;
            }
        }
        cleanup
            .settle(async {
                case.revoke().await;
                Ok::<(), SdkError>(())
            })
            .await;
        cleanup
            .settle(async {
                assert_eq!(stats.active.load(Ordering::SeqCst), 0);
                assert_eq!(stats.connections.load(Ordering::SeqCst), 0);
                assert_eq!(
                    stats.question_replies.load(Ordering::SeqCst),
                    expected_requests,
                    "joined cleanup cannot replay a native answer"
                );
                assert_eq!(
                    stats.question_reply_acceptances.load(Ordering::SeqCst),
                    expected_effects
                );
                assert_eq!(stats.question_rejections.load(Ordering::SeqCst), 0);
                assert_eq!(stats.aborts.load(Ordering::SeqCst), 0);
                assert_eq!(stats.messages.load(Ordering::SeqCst), 1);
                if let Some(target) = &redirect_stats {
                    assert_eq!(target.active.load(Ordering::SeqCst), 0);
                    assert_eq!(target.connections.load(Ordering::SeqCst), 0);
                    assert_eq!(
                        target.question_replies.load(Ordering::SeqCst),
                        0,
                        "joined redirect target must never receive the mutation"
                    );
                    assert_eq!(target.question_reply_acceptances.load(Ordering::SeqCst), 0);
                }
                Ok::<(), SdkError>(())
            })
            .await;
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn opencode_question_reply_initial_refusal_preserves_explicit_retry_authority() {
    QuestionReplyAdmission::InitialRefusal.verify().await;
}
#[tokio::test]
async fn opencode_question_reply_lost_response_preserves_ambiguous_acceptance() {
    QuestionReplyAdmission::ResponseLoss.verify().await;
}
#[tokio::test]
async fn opencode_question_reply_redirected_effect_cannot_become_preacceptance_refusal() {
    QuestionReplyAdmission::RedirectedEffect.verify().await;
}
#[tokio::test]
async fn opencode_question_reply_never_dispatches_to_a_redirect_target() {
    QuestionReplyAdmission::NoFollow.verify().await;
}
#[tokio::test]
async fn opencode_question_reply_bad_request_restores_pending() {
    QuestionReplyAdmission::BadRequest.verify().await;
}
#[tokio::test]
async fn opencode_question_reply_missing_request_preserves_uncertainty() {
    QuestionReplyAdmission::MissingRequest.verify().await;
}
#[tokio::test]
async fn opencode_question_reply_retains_native_and_settlement_failures() {
    QuestionReplyAdmission::ResponseLossSettlementFailure
        .verify()
        .await;
}

impl OpenCodeStreamCase {
    async fn verify_ambiguous_resolution_shutdown(late_event: bool) {
        let Self {
            probe,
            agent,
            workspace,
            conversation,
            faults,
            runtime_baseline,
        } = Self::start().await;
        let stats = Arc::clone(&probe.stats);
        stats
            .retained_question_history
            .store(true, Ordering::SeqCst);
        stats
            .retain_question_after_abort
            .store(true, Ordering::SeqCst);
        *stats.retained_identity_change.lock().unwrap() =
            Some(RetainedIdentityChange::OrdinaryCompletion);
        *stats.retained_rejection.lock().unwrap() = RetainedRejection::LostWithoutEvent;
        let mut owned_agent = Some(agent);
        let mut shutdowns = JoinSet::new();
        let observed = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(25),
            async {
                let runtime = tokio::runtime::Handle::current();
                let foreground_baseline = runtime.metrics().num_alive_tasks()
                    - stats.connections.load(Ordering::SeqCst);
                let mut receiver = conversation.notification_stream().unwrap();
                let mut output = conversation.prompt_stream(
                    PromptRequest::new("ask one question").unwrap(),
                ).await.unwrap();
                let interaction = loop {
                    tokio::select! {
                        item = receiver.recv() => {
                            let item = item.unwrap().unwrap();
                            if matches!(item.kind(), ConversationNotificationKind::Interaction { .. }) {
                                break item;
                            }
                        }
                        item = output.recv() => panic!("question must precede terminal output: {item:?}"),
                    }
                };
                let continuations = faults.inner.list(
                    StateListRequest::new(OPENCODE_CONTINUATION_NAMESPACE).unwrap(),
                ).await.unwrap().records;
                let [pending] = continuations.as_slice() else {
                    panic!("one exact owned question is required");
                };
                let pending = pending.clone();
                assert_eq!(pending.record.value["state"], "pending");
                assert_eq!(pending.record.value["notification_id"], interaction.id().as_str());
                stats.native_active.store(false, Ordering::SeqCst);
                OpenCodeEventProbe::retained_question_abort(&stats);
                stats.prompt_release.add_permits(1);
                // This fixture's native-shaped Success is durable, while its
                // unresolved question correctly makes the public terminal Busy.
                let terminal = tokio::time::timeout(Duration::from_secs(7), output.recv())
                    .await.expect("the original observation must finish")
                    .expect("the original stream must publish a terminal")
                    .expect_err("Pending question prevents prompt-slot cleanup");
                assert_eq!(terminal.kind(), ErrorKind::Busy, "{terminal:?}");
                assert!(output.recv().await.is_none());
                drop(output);
                tokio::time::timeout(Duration::from_secs(3), async {
                    while runtime.metrics().num_alive_tasks()
                        > foreground_baseline + stats.connections.load(Ordering::SeqCst)
                    {
                        tokio::task::yield_now().await;
                    }
                }).await.expect("the original foreground must end before resolution");
                let original = faults.prompt_snapshot(&conversation.id()).await.unwrap();
                assert_eq!(original.record.value["phase"]["phase"], "terminal");
                assert_eq!(original.record.value["phase"]["outcome"]["outcome"], "success");
                assert_eq!(original.record.value["cancellation"]["state"], "unnecessary");
                assert!(matches!(original.record.value["waiter"].as_str(), Some("delivered" | "lost" | "timed_out")));
                let generation = original.record.value["message_id"].as_str()
                    .filter(|value| !value.is_empty()).expect("real prompt generation");
                assert_eq!(pending.record.value["generation_id"].as_str(), Some(generation));
                assert!(!original.record.value["request_part_id"].as_str().unwrap().is_empty());

                let failure = conversation.resolve_notification(
                    interaction.id().clone(),
                    NotificationResolution::Dismiss { reason: "dismiss this owned question".to_owned() },
                ).await.expect_err("a consumed native rejection without a response or event is ambiguous");
                assert_eq!(failure.kind(), ErrorKind::Integration, "{failure:?}");
                assert_eq!(failure.context().operation, Operation::ResolveNotification);
                assert_eq!(failure.context().phase, ErrorPhase::RequestSend);
                assert_eq!(failure.detail(), Some(&ErrorDetail::TransportFailure));
                let ambiguous = faults.inner.load(pending.record.key.clone()).await.unwrap().unwrap();
                let mut expected = (*pending.record.value).clone();
                expected["state"] = serde_json::json!("ambiguous");
                assert_eq!(*ambiguous.record.value, expected);
                assert_eq!(ambiguous.revision.get(), pending.revision.get() + 1);
                let notification_key = StateKey::new(
                    "agent_sdk_conversation_notifications_v1", conversation.id().as_str(),
                ).unwrap();
                let notifications = faults.inner.load(notification_key.clone()).await.unwrap().unwrap();
                let unresolved = notifications.record.value["pending"].as_array().unwrap().iter()
                    .find(|item| item["id"].as_str() == Some(interaction.id().as_str())).unwrap();
                assert_eq!(unresolved["state"]["state"], "resolving");
                assert_eq!(unresolved["state"]["detail"]["acceptance"], "ambiguous");
                assert!(!stats.pending_question.load(Ordering::SeqCst));
                assert_eq!(stats.question_rejections.load(Ordering::SeqCst), 1);
                assert_eq!(stats.question_replies.load(Ordering::SeqCst), 0);
                assert_eq!(stats.aborts.load(Ordering::SeqCst), 0);

                if late_event {
                    probe.events.send(serde_json::json!({
                        "directory": env!("CARGO_MANIFEST_DIR"),
                        "payload": {"id": "evt_ambiguous_shutdown_rejected", "type": "question.rejected",
                            "properties": {"sessionID": "ses_probe", "requestID": "que_probe_pending"}}
                    }).to_string()).unwrap();
                    let sentinel = "evt_after_ambiguous_shutdown_rejected";
                    probe.events.send(serde_json::json!({
                        "directory": env!("CARGO_MANIFEST_DIR"),
                        "payload": {"id": sentinel, "type": "session.status",
                            "properties": {"sessionID": "ses_probe", "status": {"type": "idle"}}}
                    }).to_string()).unwrap();
                    tokio::time::timeout(Duration::from_secs(3), async {
                        loop {
                            let progress = faults.inner.list(
                                StateListRequest::new(OPENCODE_EVENT_PROGRESS_NAMESPACE).unwrap(),
                            ).await.unwrap();
                            if progress.records.iter().any(|record| record.record.value["recent"].as_array()
                                .is_some_and(|recent| recent.iter().any(|event| event["id"] == sentinel)))
                            {
                                break;
                            }
                            tokio::task::yield_now().await;
                        }
                    }).await.expect("the exact native event and following durable barrier must settle");
                    let accepted = faults.inner.load(pending.record.key.clone()).await.unwrap().unwrap();
                    expected["state"] = serde_json::json!("accepted");
                    assert_eq!(*accepted.record.value, expected);
                    assert_eq!(accepted.revision.get(), ambiguous.revision.get() + 1);
                    assert!(faults.inner.load(original.record.key.clone()).await.unwrap().is_none());
                    let common = faults.inner.load(notification_key.clone()).await.unwrap().unwrap();
                    let items = common.record.value["pending"].as_array().unwrap();
                    assert!(items.iter().all(|item| item["state"]["state"] != "resolving"));
                    if let Some(target) = items.iter().find(|item| item["id"].as_str() == Some(interaction.id().as_str())) {
                        assert_eq!(target["state"]["state"], "terminal");
                        assert_eq!(target["state"]["detail"]["outcome"], "reconciled_accepted");
                    }
                }

                // Observe the configured five-second Agent lifecycle to completion;
                // seven seconds is only the fixture watchdog, not a replacement budget.
                let current = owned_agent.take().unwrap();
                let started = tokio::time::Instant::now();
                shutdowns.spawn(async move { current.shutdown().await });
                let result = tokio::time::timeout(Duration::from_secs(7), shutdowns.join_next())
                    .await.expect("consuming Agent shutdown must remain bounded")
                    .unwrap().expect("the shutdown observer must join");
                eprintln!("ambiguous resolution shutdown: late_event={late_event}; elapsed={:?}; result={result:?}", started.elapsed());
                if late_event {
                    result.expect("exact native terminal evidence permits successful shutdown");
                } else {
                    let error = result.expect_err("native absence alone cannot settle ambiguous acceptance");
                    assert_eq!(error.context().operation, Operation::Shutdown, "{error:?}");
                    // The contract allows an owning error before expiry. Do not
                    // require waiting for a private settlement subdeadline or
                    // prescribe the detecting operations of supplementary errors.
                    assert_eq!(faults.inner.load(pending.record.key.clone()).await.unwrap(), Some(ambiguous));
                    let retained = faults.inner.load(original.record.key.clone()).await.unwrap().unwrap();
                    assert_eq!(retained.record.value["message_id"].as_str(), Some(generation));
                    assert_eq!(retained.record.value["phase"], original.record.value["phase"]);
                }
                let common = faults.inner.load(notification_key).await.unwrap().unwrap();
                let target = common.record.value["pending"].as_array().unwrap().iter()
                    .find(|item| item["id"].as_str() == Some(interaction.id().as_str()));
                if late_event {
                    if let Some(target) = target {
                        assert_eq!(target["state"]["state"], "terminal");
                        assert_eq!(target["state"]["detail"]["outcome"], "reconciled_accepted");
                    }
                } else {
                    let target = target.expect("unresolved acceptance remains durable for recovery");
                    assert_eq!(target["state"]["state"], "resolving");
                    assert_eq!(target["state"]["detail"]["acceptance"], "ambiguous");
                }
                assert_eq!(stats.active.load(Ordering::SeqCst), 0);
                assert_eq!(stats.question_rejections.load(Ordering::SeqCst), 1);
                assert_eq!(stats.question_replies.load(Ordering::SeqCst), 0);
                assert_eq!(stats.aborts.load(Ordering::SeqCst), 0);
                assert_eq!(stats.messages.load(Ordering::SeqCst), 1);
            },
        ).await;

        probe.release_history();
        stats.prompt_release.add_permits(2);
        stats.rejection_release.add_permits(2);
        if let Some(current) = owned_agent.take() {
            shutdowns.spawn(async move { current.shutdown().await });
        }
        let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
        cleanup
            .settle(async {
                while let Some(result) = shutdowns.join_next().await {
                    result.expect("cleanup shutdown task must join")?;
                }
                Ok::<(), SdkError>(())
            })
            .await;
        shutdowns.abort_all();
        cleanup
            .settle(async {
                while let Some(result) = shutdowns.join_next().await {
                    match result {
                        Ok(result) => result?,
                        Err(error) => {
                            assert!(error.is_cancelled(), "shutdown task panicked: {error}")
                        }
                    }
                }
                Ok::<(), SdkError>(())
            })
            .await;
        drop(conversation);
        drop(workspace);
        drop(faults);
        let runtime = tokio::runtime::Handle::current();
        cleanup
            .settle(async {
                while runtime.metrics().num_alive_tasks()
                    > runtime_baseline + stats.connections.load(Ordering::SeqCst)
                {
                    tokio::task::yield_now().await;
                }
                Ok::<(), SdkError>(())
            })
            .await;
        cleanup
            .settle(async {
                probe.shutdown().await;
                Ok::<(), SdkError>(())
            })
            .await;
        cleanup
            .settle(async {
                assert_eq!(stats.active.load(Ordering::SeqCst), 0);
                assert_eq!(stats.connections.load(Ordering::SeqCst), 0);
                assert_eq!(
                    stats.question_rejections.load(Ordering::SeqCst),
                    1,
                    "joined cleanup cannot repeat native resolution"
                );
                assert_eq!(stats.question_replies.load(Ordering::SeqCst), 0);
                assert_eq!(stats.aborts.load(Ordering::SeqCst), 0);
                assert_eq!(stats.messages.load(Ordering::SeqCst), 1);
                Ok::<(), SdkError>(())
            })
            .await;
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn opencode_ambiguous_resolution_shutdown_retains_unknown_acceptance_without_replay() {
    OpenCodeStreamCase::verify_ambiguous_resolution_shutdown(false).await;
}

#[tokio::test]
async fn opencode_ambiguous_resolution_shutdown_accepts_exact_late_native_evidence() {
    OpenCodeStreamCase::verify_ambiguous_resolution_shutdown(true).await;
}

#[derive(Clone, Copy, Debug)]
enum ResponseLossRecovery {
    Idle,
    QuietIdle,
    Active,
    Unavailable,
}

impl ResponseLossRecovery {
    async fn verify(self) {
        let case = OpenCodeStreamCase::start().await;
        let stats = Arc::clone(&case.probe.stats);
        let mut callers = JoinSet::new();
        let mut aborts = JoinSet::new();
        let mut unexpected_output = None;
        let mut expected_messages = 1;
        let observed = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(25),
            async {
                if !matches!(self, Self::QuietIdle) {
                    case.probe.request_question_on_prompt();
                    stats.pending_question.store(true, Ordering::SeqCst);
                }
                stats.lose_stream_response.store(true, Ordering::SeqCst);
                stats.reject_abort_without_effects.store(true, Ordering::SeqCst);
                let mut receiver = case.conversation.notification_stream().unwrap();
                let current = Arc::clone(&case.conversation);
                callers.spawn(async move { current.prompt(OpenCodeStreamCase::request()).await });
                let key = StateKey::new(OPENCODE_CONTINUATION_NAMESPACE,
                    format!("{}:que_probe", case.conversation.id().as_str())).unwrap();
                let pending = if matches!(self, Self::QuietIdle) {
                    case.wait_for_admission().await;
                    assert!(case.faults.inner.load(key.clone()).await.unwrap().is_none());
                    None
                } else {
                    let interaction = loop {
                        tokio::select! {
                            item = receiver.recv() => {
                                let item = item.unwrap().unwrap();
                                if matches!(item.kind(), ConversationNotificationKind::Interaction { .. }) { break item; }
                            }
                            result = callers.join_next() => panic!("the real Question must precede response loss: {result:?}"),
                        }
                    };
                    case.wait_for_admission().await;
                    let pending = case.faults.inner.load(key.clone()).await.unwrap().unwrap();
                    assert_eq!(pending.record.value["state"], "pending");
                    assert_eq!(pending.record.value["notification_id"], interaction.id().as_str());
                    Some(pending)
                };
                stats.unavailable_status.store(true, Ordering::SeqCst);
                case.release();
                let primary = tokio::time::timeout(Duration::from_secs(7), callers.join_next())
                    .await.expect("response loss must finish the original observer")
                    .unwrap().expect("prompt observer must join")
                    .expect_err("a lost accepted HTTP response cannot become success");
                assert_eq!(primary.kind(), ErrorKind::Integration, "{primary:?}");
                assert_eq!(primary.context().operation, Operation::Prompt);
                assert_eq!(primary.context().phase, ErrorPhase::RequestSend);
                assert_eq!(primary.detail(), Some(&ErrorDetail::TransportFailure));
                tokio::time::timeout(Duration::from_secs(3), async {
                    while stats.rejected_status_failures.load(Ordering::SeqCst) == 0 {
                        tokio::task::yield_now().await;
                    }
                }).await.expect("the rejected abort must be followed by an actual failed status response");
                let original = case.faults.prompt_snapshot(&case.conversation.id()).await.unwrap();
                assert_eq!(original.record.value["phase"]["phase"], "response_lost_after_admission");
                assert_eq!(original.record.value["cancellation"]["state"], "rejected");
                assert_eq!(original.record.value["waiter"], "delivered");
                if let Some(pending) = &pending {
                    assert_eq!(pending.record.value["generation_id"], original.record.value["message_id"]);
                }
                assert!(!original.record.value["request_part_id"].as_str().unwrap().is_empty());
                assert_eq!(case.faults.inner.load(key.clone()).await.unwrap(), pending.clone());
                assert_eq!(stats.aborts.load(Ordering::SeqCst), 1);

                // The existing exact-task prompt-read gate follows public Abort's
                // registration. Keep that observer attached while recovery runs,
                // so it must retain this generation's repaired outcome.
                let (start, admitted) = oneshot::channel();
                let current = Arc::clone(&case.conversation);
                let observer = aborts.spawn(async move {
                    admitted.await.unwrap();
                    current.abort().await
                });
                case.faults.arm_reconciliation_observer(observer.id(), original.record.key.clone());
                start.send(()).unwrap();
                tokio::time::timeout(Duration::from_secs(3), case.faults.wait_reconciliation_observer())
                    .await.expect("the Abort observer must attach before native recovery");
                // The entry is sufficient proof of registration; release the
                // real Lance transaction before reconnect needs to write.
                case.faults.release_reconciliation_observer();

                let status_before = stats.status_requests.load(Ordering::SeqCst);
                stats.native_active.store(matches!(self, Self::Active), Ordering::SeqCst);
                stats.unavailable_status.store(matches!(self, Self::Unavailable), Ordering::SeqCst);
                stats.pending_question.store(false, Ordering::SeqCst);
                // Model the recovered native inventory without inventing an idle,
                // answered, rejected, or completed SSE event. The original user
                // history remains available; the native Question is now absent.
                let history_before = stats.history.load(Ordering::SeqCst);
                let scans_before = stats.pending_scans.load(Ordering::SeqCst);
                let native_history = stats.history_rows.lock().unwrap().clone();
                case.probe.disconnect_current();
                let cancelled = if let Some(pending) = &pending {
                    let cancelled = tokio::time::timeout(Duration::from_secs(3), async {
                        loop {
                            let current = case.faults.inner.load(key.clone()).await.unwrap().unwrap();
                            if current.record.value["state"] == "cancelled" { break current; }
                            tokio::task::yield_now().await;
                        }
                    }).await.expect("real reconnect must cancel the exact lost Pending interaction");
                    let mut expected = (*pending.record.value).clone();
                    expected["state"] = serde_json::json!("cancelled");
                    assert_eq!(*cancelled.record.value, expected);
                    let journal_key = StateKey::new("agent_sdk_conversation_notifications_v1",
                        case.conversation.id().as_str()).unwrap();
                    let journal = case.faults.inner.load(journal_key).await.unwrap().unwrap();
                    let common = journal.record.value["pending"].as_array().unwrap().iter()
                        .find(|item| item["id"] == pending.record.value["notification_id"]).unwrap();
                    assert_eq!(common["state"]["state"], "terminal");
                    assert_eq!(common["state"]["detail"]["outcome"], "reconciled_cancelled");
                    Some(cancelled)
                } else {
                    // No continuation changes can wake the foreground in this
                    // variant: reconnect re-reads unchanged input history and
                    // empty native interaction inventories, without a status event.
                    case.probe.wait_for_synchronization(history_before + 1, scans_before + 2).await;
                    assert_eq!(*stats.history_rows.lock().unwrap(), native_history);
                    assert!(case.faults.inner.load(key.clone()).await.unwrap().is_none());
                    None
                };

                if matches!(self, Self::Active) {
                    let busy_before = stats.busy_status_responses.load(Ordering::SeqCst);
                    case.probe.events.send(serde_json::json!({
                        "directory": env!("CARGO_MANIFEST_DIR"), "payload": {
                            "id": "evt_recovered_prompt_still_busy", "type": "session.status",
                            "properties": {"sessionID": "ses_probe", "status": {"type": "busy"}}
                        }
                    }).to_string()).unwrap();
                    tokio::time::timeout(Duration::from_secs(3), async {
                        while stats.busy_status_responses.load(Ordering::SeqCst) <= busy_before {
                            tokio::task::yield_now().await;
                        }
                    }).await.expect("the actual busy status response must precede the negative oracle");
                    if let Some(result) = aborts.try_join_next() {
                        panic!("busy native work completed Abort before Idle: {result:?}");
                    }
                    // A cancelled interaction is not proof that the accepted
                    // prompt is idle. Keep actual busy status authoritative.
                    let retained = case.faults.prompt_snapshot(&case.conversation.id()).await.unwrap();
                    assert_eq!(retained.record.value["message_id"], original.record.value["message_id"]);
                    assert_eq!(retained.record.value["phase"], original.record.value["phase"]);
                    let busy = match case.conversation.prompt_stream(OpenCodeStreamCase::request()).await {
                        Err(error) => error,
                        Ok(output) => { unexpected_output = Some(output); panic!("active native work must retain public prompt admission"); }
                    };
                    assert_eq!(busy.kind(), ErrorKind::Busy);
                    stats.native_active.store(false, Ordering::SeqCst);
                    case.probe.events.send(serde_json::json!({
                        "directory": env!("CARGO_MANIFEST_DIR"), "payload": {
                            "id": "evt_recovered_prompt_became_idle", "type": "session.idle",
                            "properties": {"sessionID": "ses_probe"}
                        }
                    }).to_string()).unwrap();
                }
                let result = tokio::time::timeout(Duration::from_secs(7), aborts.join_next())
                    .await.expect("Abort retains its original lifecycle bound")
                    .unwrap().expect("Abort observer must join");
                eprintln!("response-loss recovery {self:?}: result={result:?}; status_before={status_before}; status_after={}; failed_status={}; aborts={}",
                    stats.status_requests.load(Ordering::SeqCst), stats.rejected_status_failures.load(Ordering::SeqCst),
                    stats.aborts.load(Ordering::SeqCst));
                let error = result.expect_err("rejected cancellation cannot manufacture successful completion");
                assert_eq!(error.context().operation, Operation::Abort);
                if matches!(self, Self::Unavailable) {
                    assert_eq!(error.kind(), ErrorKind::Timeout, "{error:?}");
                    let retained = case.faults.prompt_snapshot(&case.conversation.id()).await.unwrap();
                    assert_eq!(retained.record.value["message_id"], original.record.value["message_id"]);
                    assert_eq!(retained.record.value["phase"], original.record.value["phase"]);
                    assert_eq!(retained.record.value["cancellation"]["state"], "rejected");
                    let busy = match case.conversation.prompt_stream(OpenCodeStreamCase::request()).await {
                        Err(error) => error,
                        Ok(output) => { unexpected_output = Some(output); panic!("unavailable status cannot release the original prompt"); }
                    };
                    assert_eq!(busy.kind(), ErrorKind::Busy);
                } else {
                    assert_eq!(error.kind(), ErrorKind::InvalidState, "{error:?}");
                    assert_eq!(error.context().phase, ErrorPhase::Cancellation);
                    assert!(stats.status_requests.load(Ordering::SeqCst) > status_before,
                        "only a new actual status response can authorize idle repair");
                    case.settled().await;
                    assert!(case.faults.prompt_snapshot(&case.conversation.id()).await.is_none());
                    case.conversation.abort().await.expect("a truly later Abort is idle");
                    stats.lose_stream_response.store(false, Ordering::SeqCst);
                    stats.question_on_prompt.store(false, Ordering::SeqCst);
                    // The existing retained-history fixture's second generation
                    // appends fresh native identities and sequence positions.
                    stats.retained_question_history.store(true, Ordering::SeqCst);
                    let current = Arc::clone(&case.conversation);
                    expected_messages = 2;
                    callers.spawn(async move {
                        current.prompt(PromptRequest::new("return the reuse marker").unwrap()).await
                    });
                    let reused = tokio::time::timeout(Duration::from_secs(7), callers.join_next())
                        .await.expect("released prompt admission must support real reuse")
                        .unwrap().expect("reuse observer joins").expect("reuse must complete");
                    assert_eq!(reused.output(), "RETAINED_HISTORY_REUSE");
                }
                assert_eq!(case.faults.inner.load(key).await.unwrap(), cancelled);
                assert_eq!(stats.messages.load(Ordering::SeqCst), expected_messages);
                assert_eq!(stats.aborts.load(Ordering::SeqCst), 1);
                assert_eq!(stats.question_replies.load(Ordering::SeqCst), 0);
                assert_eq!(stats.question_rejections.load(Ordering::SeqCst), 0);
                // Keep the complete earlier prompt error authoritative; no
                // cleanup outcome may replace or synthesize its response.
                assert_eq!(primary.context().operation, Operation::Prompt);
                assert_eq!(primary.detail(), Some(&ErrorDetail::TransportFailure));
            },
        ).await;

        // Release all gates before independent owner drainage. This exact idle
        // event is cleanup only and cannot make the captured main oracle pass.
        stats.unavailable_status.store(false, Ordering::SeqCst);
        stats.native_active.store(false, Ordering::SeqCst);
        stats.pending_question.store(false, Ordering::SeqCst);
        case.faults.release_reconciliation_observer();
        case.release();
        let _ = case.probe.events.send(
            serde_json::json!({
                "directory": env!("CARGO_MANIFEST_DIR"), "payload": {
                    "id": "evt_response_loss_recovery_cleanup", "type": "session.idle",
                    "properties": {"sessionID": "ses_probe"}
                }
            })
            .to_string(),
        );
        drop(unexpected_output);
        let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
        cleanup
            .settle(async {
                callers.abort_all();
                aborts.abort_all();
                while let Some(result) = callers.join_next().await {
                    match result {
                        Ok(Err(error)) => eprintln!("late original prompt error: {error:?}"),
                        Ok(Ok(_)) => panic!("lost prompt unexpectedly succeeded during cleanup"),
                        Err(error) => {
                            assert!(error.is_cancelled(), "prompt task panicked: {error}")
                        }
                    }
                }
                while let Some(result) = aborts.join_next().await {
                    match result {
                        Ok(Err(error)) => eprintln!("late Abort result: {error:?}"),
                        Ok(Ok(())) => {}
                        Err(error) => assert!(error.is_cancelled(), "Abort task panicked: {error}"),
                    }
                }
                Ok::<(), SdkError>(())
            })
            .await;
        cleanup
            .settle(async {
                case.revoke().await;
                assert_eq!(stats.active.load(Ordering::SeqCst), 0);
                assert_eq!(stats.connections.load(Ordering::SeqCst), 0);
                assert_eq!(stats.messages.load(Ordering::SeqCst), expected_messages);
                assert_eq!(stats.aborts.load(Ordering::SeqCst), 1);
                assert_eq!(stats.question_replies.load(Ordering::SeqCst), 0);
                assert_eq!(stats.question_rejections.load(Ordering::SeqCst), 0);
                Ok::<(), SdkError>(())
            })
            .await;
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn opencode_response_loss_recovery_rechecks_idle_after_failed_status() {
    ResponseLossRecovery::Idle.verify().await;
}

#[tokio::test]
async fn opencode_response_loss_recovery_keeps_active_generation_until_idle() {
    ResponseLossRecovery::Active.verify().await;
}

#[tokio::test]
async fn opencode_response_loss_recovery_retains_generation_when_status_stays_unavailable() {
    ResponseLossRecovery::Unavailable.verify().await;
}

#[tokio::test]
async fn opencode_response_loss_recovery_rechecks_idle_after_quiet_reconnect() {
    ResponseLossRecovery::QuietIdle.verify().await;
}

#[derive(Clone, Copy, Debug)]
enum ReopenIdleBoundary {
    Admitted,
    AdmittedConflict,
    DeliveredAmbiguous,
    LostAmbiguous,
    DeliveredRejected,
    FailedRollback,
    MissingIdleRollback,
    MissingActiveRollback,
}

impl ReopenIdleBoundary {
    async fn verify(self) {
        let mut original = Some(OpenCodeStreamCase::start().await);
        let current = original.as_ref().unwrap();
        let stats = Arc::clone(&current.probe.stats);
        let faults = current.faults.clone();
        let baseline = current.runtime_baseline;
        let mut probe = None;
        let mut reopened_agent = None;
        let mut reopened_workspace = None;
        let mut reopened_conversation = None;
        let mut callers = JoinSet::new();
        let mut builds = JoinSet::new();
        let mut shutdowns = JoinSet::new();
        let observed = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(35),
            async {
                let current = original.as_ref().unwrap();
                let agent_id = current.agent.id();
                let workspace_id = current.workspace.id();
                let conversation_id = current.conversation.id();
                let config = current.probe.config(Duration::from_secs(15), Duration::from_secs(5));
                let missing = matches!(self, Self::MissingIdleRollback | Self::MissingActiveRollback);
                let response_loss = matches!(self, Self::DeliveredAmbiguous | Self::LostAmbiguous | Self::DeliveredRejected);
                let delivered = matches!(self, Self::DeliveredAmbiguous | Self::DeliveredRejected);
                let expected_aborts = usize::from(response_loss);
                stats.lose_stream_response.store(true, Ordering::SeqCst);
                stats.lose_abort_response.store(
                    matches!(self, Self::DeliveredAmbiguous | Self::LostAmbiguous), Ordering::SeqCst,
                );
                stats.reject_abort_without_effects.store(
                    matches!(self, Self::DeliveredRejected), Ordering::SeqCst,
                );
                let retained = if missing {
                    assert!(faults.prompt_snapshot(&conversation_id).await.is_none());
                    assert_eq!(stats.messages.load(Ordering::SeqCst), 0);
                    None
                } else {
                    let conversation = Arc::clone(&current.conversation);
                    let caller = callers.spawn(async move {
                        conversation.prompt(OpenCodeStreamCase::request()).await
                    });
                    current.wait_for_admission().await;
                    // Native Active and unavailable status keep the live owner from
                    // repairing the generation before the deliberate hierarchy loss.
                    stats.unavailable_status.store(true, Ordering::SeqCst);
                    if !delivered {
                        caller.abort();
                        assert!(callers.join_next().await.unwrap().expect_err(
                            "only the caller observer is cancelled",
                        ).is_cancelled());
                        tokio::time::timeout(Duration::from_secs(3), async {
                            loop {
                                let retained = faults.prompt_snapshot(&conversation_id).await.unwrap();
                                if retained.record.value["waiter"] == "lost" { break; }
                                tokio::task::yield_now().await;
                            }
                        }).await.expect("actual caller loss must be durably observed");
                    }
                    if response_loss {
                        current.release();
                        if delivered {
                            let primary = callers.join_next().await.unwrap()
                                .expect("the original prompt observer must join")
                                .expect_err("lost foreground bytes cannot produce prompt success");
                            assert_eq!(primary.kind(), ErrorKind::Integration, "{primary:?}");
                            assert_eq!(primary.context().operation, Operation::Prompt);
                            assert_eq!(primary.context().phase, ErrorPhase::RequestSend);
                            assert_eq!(primary.detail(), Some(&ErrorDetail::TransportFailure));
                        }
                    }
                    let expected_phase = if response_loss { "response_lost_after_admission" } else { "admitted" };
                    let expected_waiter = if delivered { "delivered" } else { "lost" };
                    let expected_cancel = match self {
                        Self::Admitted | Self::AdmittedConflict | Self::FailedRollback | Self::MissingIdleRollback | Self::MissingActiveRollback => "not_requested",
                        Self::DeliveredAmbiguous | Self::LostAmbiguous => "ambiguous",
                        Self::DeliveredRejected => "rejected",
                    };
                    let retained = tokio::time::timeout(Duration::from_secs(7), async {
                        loop {
                            let retained = faults.prompt_snapshot(&conversation_id).await.unwrap();
                            if retained.record.value["phase"]["phase"] == expected_phase
                                && retained.record.value["waiter"] == expected_waiter
                                && retained.record.value["cancellation"]["state"] == expected_cancel
                            { break retained; }
                            tokio::task::yield_now().await;
                        }
                    }).await.expect("the exact native response and observer boundary must be durable");
                    assert_eq!(retained.record.value["input_observed"], true);
                    assert_eq!(stats.messages.load(Ordering::SeqCst), 1);
                    assert_eq!(stats.aborts.load(Ordering::SeqCst), expected_aborts);
                    assert!(stats.native_active.load(Ordering::SeqCst));
                    assert!(!stats.pending_question.load(Ordering::SeqCst));
                    Some(retained)
                };

                // Keep only the caller-owned Store and protocol peer. Reopen
                // receives actual committed state; no record is restored or edited.
                let OpenCodeStreamCase { probe: native, agent, workspace, conversation, .. } = original.take().unwrap();
                probe = Some(native);
                drop(conversation);
                drop(workspace);
                drop(agent);
                let runtime = tokio::runtime::Handle::current();
                tokio::time::timeout(Duration::from_secs(3), async {
                    while runtime.metrics().num_alive_tasks() > baseline + stats.connections.load(Ordering::SeqCst) {
                        tokio::task::yield_now().await;
                    }
                }).await.expect("the original hierarchy must release its tasks before reopen");
                assert_eq!(faults.prompt_snapshot(&conversation_id).await.as_ref(), retained.as_ref());
                if !response_loss && !missing {
                    // Finish only the abandoned fixture HTTP connection after
                    // its SDK owner ended; send neither terminal bytes nor SSE.
                    stats.prompt_release.add_permits(1);
                    tokio::time::timeout(Duration::from_secs(3), async {
                        while stats.prompt_responses.load(Ordering::SeqCst) != 1 {
                            tokio::task::yield_now().await;
                        }
                    }).await.expect("the abandoned peer request must finish before a new prompt");
                }
                if matches!(self, Self::MissingActiveRollback) {
                    probe.as_ref().unwrap().mark_native_active();
                } else {
                    probe.as_ref().unwrap().mark_native_idle();
                }
                stats.unavailable_status.store(false, Ordering::SeqCst);
                if matches!(self, Self::FailedRollback | Self::MissingIdleRollback | Self::MissingActiveRollback) {
                    faults.state.fail_unchanged_prompt_rollback.store(true, Ordering::SeqCst);
                }
                if matches!(self, Self::AdmittedConflict) {
                    faults.set_persistence_fault(PromptPersistenceFault::ReopenCommitConflictOnce);
                    let storage = faults.storage();
                    let conflict_config = config.clone();
                    let conflict_id = agent_id.clone();
                    builds.spawn(async move {
                        AgentBuilder::reopen(conflict_config, storage, conflict_id).build().await
                    });
                    let error = match builds.join_next().await.unwrap()
                        .expect("the rejected reconstruction must join")
                    {
                        Ok(agent) => {
                            reopened_agent = Some(agent);
                            panic!("a real repair commit conflict must escape this single Reopen attempt");
                        }
                        Err(error) => error,
                    };
                    assert_eq!(error.kind(), ErrorKind::Conflict, "{error:?}");
                    assert_eq!(error.context().operation, Operation::Reopen);
                    assert_eq!(error.context().phase, ErrorPhase::StateCommit);
                    assert_eq!(error.detail(), Some(&ErrorDetail::TransactionConflict));
                    assert!(error.supplementary().is_empty(), "reconstruction cleanup must settle: {error:?}");
                    assert!(!error.diagnostics_truncated());
                    assert_eq!(faults.state.persistence_attempts.load(Ordering::SeqCst), 1);
                    assert_eq!(faults.prompt_snapshot(&conversation_id).await.as_ref(), retained.as_ref(),
                        "the losing repair must not change the original generation or revision");
                    tokio::time::timeout(Duration::from_secs(3), async {
                        while stats.active.load(Ordering::SeqCst) != 0
                            || runtime.metrics().num_alive_tasks() > baseline + stats.connections.load(Ordering::SeqCst)
                        {
                            tokio::task::yield_now().await;
                        }
                    }).await.expect("conflicted construction must settle before the caller retries");
                    assert_eq!(stats.messages.load(Ordering::SeqCst), 1);
                    assert_eq!(stats.aborts.load(Ordering::SeqCst), 0);
                    assert_eq!(stats.next_session.load(Ordering::SeqCst), 1);
                    eprintln!("same-identity reconstruction attempt 1: {:?}/{:?}/{:?}, cleanup settled",
                        error.kind(), error.context().phase, error.detail());
                }
                let status_before = stats.status_requests.load(Ordering::SeqCst);
                let storage = faults.storage();
                let first_config = config.clone();
                let first_id = agent_id.clone();
                builds.spawn(async move {
                    AgentBuilder::reopen(first_config, storage, first_id).build().await
                });
                let first = match builds.join_next().await.unwrap().expect("first reopen task must join") {
                    Ok(agent) => {
                        reopened_agent = Some(agent);
                        Ok(())
                    }
                    Err(error) => Err(error),
                };
                let after = faults.prompt_snapshot(&conversation_id).await;
                eprintln!("idle reopen {self:?}: first={:?}; retained={}; rollback_failures={}",
                    first.as_ref().map(|_| ()), after.is_some(),
                    faults.state.unchanged_prompt_rollback_failures.load(Ordering::SeqCst));
                assert!(stats.status_requests.load(Ordering::SeqCst) > status_before,
                    "the first reopen must obtain an actual fresh native status");
                match first {
                    Ok(()) => {
                        assert!(matches!(self, Self::DeliveredAmbiguous),
                            "an unobserved or rejected outcome must preserve its integrity failure");
                    }
                    Err(error) => {
                        assert!(!matches!(self, Self::DeliveredAmbiguous),
                            "delivered response loss plus ambiguous cancellation is reconcilable: {error:?}");
                        assert_eq!(error.context().operation, Operation::Reopen);
                        if !matches!(self, Self::MissingActiveRollback) {
                            assert!(error.supplementary().is_empty(), "failed build cleanup must be complete: {error:?}");
                        }
                        assert!(!error.diagnostics_truncated());
                        tokio::time::timeout(Duration::from_secs(3), async {
                            while stats.active.load(Ordering::SeqCst) != 0
                                || runtime.metrics().num_alive_tasks() > baseline + stats.connections.load(Ordering::SeqCst)
                            {
                                tokio::task::yield_now().await;
                            }
                        }).await.expect("failed Builder must settle its event owner before another Builder starts");
                        if missing {
                            assert_eq!(faults.state.unchanged_prompt_rollback_failures.load(Ordering::SeqCst), 1);
                            assert!(after.is_none(), "failed missing-record cleanup must not create a prompt record");
                            assert_eq!(stats.messages.load(Ordering::SeqCst), 0);
                            assert_eq!(stats.aborts.load(Ordering::SeqCst), 0);
                            if matches!(self, Self::MissingActiveRollback) {
                                assert_eq!(error.kind(), ErrorKind::InvalidState, "{error:?}");
                                assert_eq!(error.context().phase, ErrorPhase::Lifecycle);
                                assert!(error.detail().is_none());
                                let [cleanup] = error.supplementary() else {
                                    panic!("native work without a durable owner must retain its failed cleanup: {error:?}");
                                };
                                assert_eq!(cleanup.kind(), ErrorKind::Integration);
                                assert_eq!(cleanup.context().phase, ErrorPhase::Cleanup);
                                assert_eq!(cleanup.detail(), Some(&ErrorDetail::ServiceUnavailable));
                                assert!(cleanup.supplementary().is_empty());
                                assert!(!cleanup.diagnostics_truncated());
                            } else {
                                assert_eq!(error.kind(), ErrorKind::Integration, "{error:?}");
                                assert_eq!(error.context().phase, ErrorPhase::Cleanup);
                                assert_eq!(error.detail(), Some(&ErrorDetail::ServiceUnavailable));
                            }
                            return;
                        }
                        if matches!(self, Self::FailedRollback) {
                            assert_eq!(faults.state.unchanged_prompt_rollback_failures.load(Ordering::SeqCst), 1);
                            assert_eq!(error.kind(), ErrorKind::Integration, "{error:?}");
                            assert_eq!(error.context().phase, ErrorPhase::Cleanup);
                            assert_eq!(error.detail(), Some(&ErrorDetail::ServiceUnavailable));
                            assert_eq!(after.as_ref(), retained.as_ref(),
                                "failed read-only rollback must veto subsequent repair mutation");
                            assert_eq!(stats.messages.load(Ordering::SeqCst), 1);
                            assert_eq!(stats.aborts.load(Ordering::SeqCst), 0);
                            return;
                        }
                        assert_eq!(error.kind(), ErrorKind::InvalidState, "{error:?}");
                        assert_eq!(error.context().phase, ErrorPhase::Lifecycle);
                        assert!(error.detail().is_none());
                        assert_eq!(error.message(), "runtime work ended without a matching terminal response");
                        assert!(after.is_none(), "the failed first reopen must retain its committed repair");
                        assert_eq!(stats.messages.load(Ordering::SeqCst), 1);
                        assert_eq!(stats.aborts.load(Ordering::SeqCst), expected_aborts);
                        assert_eq!(stats.next_session.load(Ordering::SeqCst), 1);
                        eprintln!("same-identity reconstruction repair: {:?}/{:?}, cleanup settled",
                            error.kind(), error.context().phase);
                        let storage = faults.storage();
                        builds.spawn(async move { AgentBuilder::reopen(config, storage, agent_id).build().await });
                        reopened_agent = Some(builds.join_next().await.unwrap()
                            .expect("fresh reopen task must join")
                            .expect("a distinct caller reopen must observe the first repair without replay"));
                    }
                }
                assert!(faults.prompt_snapshot(&conversation_id).await.is_none());
                assert_eq!(stats.messages.load(Ordering::SeqCst), 1);
                assert_eq!(stats.aborts.load(Ordering::SeqCst), expected_aborts);
                assert_eq!(stats.next_session.load(Ordering::SeqCst), 1);
                reopened_workspace = Some(reopened_agent.as_ref().unwrap().get_workspace(workspace_id).await.unwrap());
                reopened_conversation = Some(reopened_workspace.as_ref().unwrap().get_conversation(conversation_id).await.unwrap());
                let conversation = reopened_conversation.as_ref().unwrap();
                conversation.abort().await.expect("a later idle Abort must be a no-op");
                assert_eq!(stats.aborts.load(Ordering::SeqCst), expected_aborts);
                stats.lose_stream_response.store(false, Ordering::SeqCst);
                stats.lose_abort_response.store(false, Ordering::SeqCst);
                stats.reject_abort_without_effects.store(false, Ordering::SeqCst);
                stats.retained_question_history.store(true, Ordering::SeqCst);
                let response = conversation.prompt(PromptRequest::new("return the reuse marker").unwrap()).await.unwrap();
                assert_eq!(response.output(), "RETAINED_HISTORY_REUSE");
                assert_eq!(stats.messages.load(Ordering::SeqCst), 2);
                assert_eq!(stats.aborts.load(Ordering::SeqCst), expected_aborts);
                assert_eq!(stats.question_replies.load(Ordering::SeqCst), 0);
                assert_eq!(stats.question_rejections.load(Ordering::SeqCst), 0);
            },
        ).await;

        // Release every fixture gate only after capturing the main observation.
        // Build and consuming shutdown tasks remain owned outside these bounds.
        stats.unavailable_status.store(false, Ordering::SeqCst);
        stats.lose_abort_response.store(false, Ordering::SeqCst);
        stats
            .reject_abort_without_effects
            .store(false, Ordering::SeqCst);
        stats.native_active.store(false, Ordering::SeqCst);
        stats.prompt_release.add_permits(2);
        faults
            .state
            .fail_unchanged_prompt_rollback
            .store(false, Ordering::SeqCst);
        faults.release();
        callers.abort_all();
        let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
        cleanup
            .settle(async {
                while let Some(result) = callers.join_next().await {
                    if let Err(error) = result {
                        assert!(error.is_cancelled(), "prompt observer panicked: {error}");
                    }
                }
                while let Some(result) = builds.join_next().await {
                    match result.expect("reopen task must join during cleanup") {
                        Ok(agent) => reopened_agent = Some(agent),
                        Err(error) => return Err(error),
                    }
                }
                Ok::<(), SdkError>(())
            })
            .await;
        builds.abort_all();
        cleanup
            .settle(async {
                while let Some(result) = builds.join_next().await {
                    match result {
                        Ok(Ok(agent)) => reopened_agent = Some(agent),
                        Ok(Err(error)) => return Err(error),
                        Err(error) => {
                            assert!(error.is_cancelled(), "reopen task panicked: {error}")
                        }
                    }
                }
                Ok::<(), SdkError>(())
            })
            .await;
        drop(reopened_conversation.take());
        drop(reopened_workspace.take());
        if let Some(agent) = reopened_agent.take() {
            shutdowns.spawn(async move { agent.shutdown().await });
        }
        if let Some(current) = original.take() {
            let OpenCodeStreamCase {
                probe: native,
                agent,
                workspace,
                conversation,
                ..
            } = current;
            probe = Some(native);
            drop(conversation);
            drop(workspace);
            shutdowns.spawn(async move { agent.shutdown().await });
        }
        cleanup
            .settle(async {
                while let Some(result) = shutdowns.join_next().await {
                    result.expect("Agent shutdown must join")?;
                }
                Ok::<(), SdkError>(())
            })
            .await;
        shutdowns.abort_all();
        cleanup
            .settle(async {
                while let Some(result) = shutdowns.join_next().await {
                    match result {
                        Ok(result) => result?,
                        Err(error) => {
                            assert!(error.is_cancelled(), "Agent shutdown panicked: {error}")
                        }
                    }
                }
                let runtime = tokio::runtime::Handle::current();
                while runtime.metrics().num_alive_tasks()
                    > baseline + stats.connections.load(Ordering::SeqCst)
                {
                    tokio::task::yield_now().await;
                }
                Ok::<(), SdkError>(())
            })
            .await;
        if let Some(mut native) = probe.take() {
            native.release_history();
            let mut task = native
                .task
                .take()
                .expect("the native fixture task remains owned");
            let mut joined = false;
            cleanup
                .settle(async {
                    native.shutdown().await;
                    let result = (&mut task).await;
                    joined = true;
                    result.expect("the native fixture task must join");
                    Ok::<(), SdkError>(())
                })
                .await;
            if !joined {
                task.abort();
                cleanup
                    .settle(async {
                        match task.await {
                            Ok(()) => {}
                            Err(error) => {
                                assert!(error.is_cancelled(), "native fixture panicked: {error}")
                            }
                        }
                        Ok::<(), SdkError>(())
                    })
                    .await;
            }
        }
        cleanup
            .settle(async {
                assert_eq!(stats.active.load(Ordering::SeqCst), 0);
                assert_eq!(stats.connections.load(Ordering::SeqCst), 0);
                Ok::<(), SdkError>(())
            })
            .await;
        cleanup.finish(observed);
    }
}

#[tokio::test]
async fn opencode_reopen_admitted_idle_reports_repair_then_fresh_reopen_succeeds() {
    ReopenIdleBoundary::Admitted.verify().await;
}

#[tokio::test]
async fn opencode_reopen_clean_conflict_then_repair_then_same_identity_reconstruction_succeeds() {
    ReopenIdleBoundary::AdmittedConflict.verify().await;
}

#[tokio::test]
async fn opencode_reopen_delivered_ambiguous_response_loss_settles_once() {
    ReopenIdleBoundary::DeliveredAmbiguous.verify().await;
}

#[tokio::test]
async fn opencode_reopen_lost_ambiguous_response_reports_repair_then_fresh_reopen_succeeds() {
    ReopenIdleBoundary::LostAmbiguous.verify().await;
}

#[tokio::test]
async fn opencode_reopen_delivered_rejected_response_reports_repair_then_fresh_reopen_succeeds() {
    ReopenIdleBoundary::DeliveredRejected.verify().await;
}

#[tokio::test]
async fn opencode_reopen_idle_failed_rollback_preserves_record_and_cleanup_error() {
    ReopenIdleBoundary::FailedRollback.verify().await;
}

#[tokio::test]
async fn opencode_reopen_missing_idle_failed_rollback_prevents_publication() {
    ReopenIdleBoundary::MissingIdleRollback.verify().await;
}

#[tokio::test]
async fn opencode_reopen_missing_active_failed_rollback_retains_integrity_and_cleanup() {
    ReopenIdleBoundary::MissingActiveRollback.verify().await;
}

impl OpenCodeStreamCase {
    async fn verify_abort_outcome_commit(fault: PromptPersistenceFault, recovered: bool) {
        let case = Self::start().await;
        let observed = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(20),
            async {
                let mut stream = if recovered {
                    let runtime = tokio::runtime::Handle::current();
                    let foreground_baseline = runtime.metrics().num_alive_tasks()
                        - case.probe.stats.connections.load(Ordering::SeqCst);
                    let mut stream = case
                        .stream_with_admission_fault(PromptPersistenceFault::Admission)
                        .await;
                    let error = tokio::time::timeout(Duration::from_secs(7), stream.recv())
                        .await
                        .expect("foreground admission must settle within its existing bound")
                        .expect("one foreground terminal outcome")
                        .unwrap_err();
                    assert_eq!(error.kind(), ErrorKind::Timeout);
                    assert!(stream.recv().await.is_none());
                    // Stream timeout ends observation, not admitted foreground work.
                    // Keep Admission armed until that owner releases the generation;
                    // otherwise the next fault can target foreground cancellation.
                    tokio::time::timeout(Duration::from_secs(3), async {
                        while runtime.metrics().num_alive_tasks()
                            > foreground_baseline
                                + case.probe.stats.connections.load(Ordering::SeqCst)
                        {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .expect("the original foreground must release ownership before recovery");
                    stream
                } else {
                    let mut stream = case.stream().await;
                    case.begin_text();
                    case.delta(
                        "evt_abort_outcome_ready",
                        Self::ASSISTANT,
                        "prt_stream_text",
                        "text",
                        "accepted",
                    );
                    assert!(matches!(
                        Self::next(&mut stream).await,
                        Ok(PromptEvent::OutputDelta { .. })
                    ));
                    case.wait_for_admission().await;
                    stream
                };
                case.faults
                    .state
                    .persistence_attempts
                    .store(0, Ordering::SeqCst);
                case.faults.set_persistence_fault(fault);
                let rollback_failure = fault == PromptPersistenceFault::AbortOutcomeOnce;
                case.faults
                    .state
                    .persistence_conflict
                    .store(rollback_failure, Ordering::SeqCst);
                case.faults
                    .state
                    .persistence_rollback_failure
                    .store(rollback_failure, Ordering::SeqCst);
                let abort = tokio::time::timeout(Duration::from_secs(7), case.conversation.abort())
                    .await
                    .expect("native outcome persistence keeps the original abort bound");
                let records = case
                    .faults
                    .state
                    .cancellation_outcome_records
                    .lock()
                    .unwrap()
                    .clone();
                assert_eq!(
                    case.faults
                        .state
                        .persistence_attempts
                        .load(Ordering::SeqCst),
                    1
                );
                assert_eq!(case.probe.stats.messages.load(Ordering::SeqCst), 1);
                assert_eq!(
                    case.probe.stats.aborts.load(Ordering::SeqCst),
                    1,
                    "storage attempts must preserve the one original native abort"
                );
                assert_eq!(
                    records.len(),
                    if fault == PromptPersistenceFault::AbortOutcomeCommitConflictOnce {
                        2
                    } else {
                        1
                    },
                    "only a proven nonpublication authorizes a new mutation"
                );
                for record in &records {
                    assert_eq!(record.value["cancellation"]["state"], "accepted");
                    assert_eq!(
                        record.value["cancellation"], records[0].value["cancellation"],
                        "retries preserve the exact native outcome"
                    );
                    assert_eq!(
                        record.value["message_id"], records[0].value["message_id"],
                        "retries preserve the exact generation"
                    );
                }
                if fault == PromptPersistenceFault::AbortOutcomeCommitUnavailableOnce
                    || rollback_failure
                {
                    let error = if recovered {
                        abort.expect_err(
                            "unknown outcome cannot become successful recovered cancellation",
                        )
                    } else {
                        let _ = abort;
                        Self::next(&mut stream)
                            .await
                            .expect_err("unknown outcome remains the foreground failure")
                    };
                    if rollback_failure {
                        assert_eq!(error.kind(), ErrorKind::Conflict, "{error:?}");
                        assert_eq!(error.detail(), Some(&ErrorDetail::TransactionConflict));
                        assert!(
                            error
                                .supplementary()
                                .iter()
                                .any(|error| error.context().phase == ErrorPhase::Cleanup
                                    && error.detail() == Some(&ErrorDetail::ServiceUnavailable)),
                            "{error:?}"
                        );
                    } else {
                        assert_eq!(error.kind(), ErrorKind::Integration, "{error:?}");
                        assert_eq!(error.context().phase, ErrorPhase::StateCommit);
                        assert_eq!(error.detail(), Some(&ErrorDetail::ServiceUnavailable));
                    }
                    let retained = case
                        .faults
                        .prompt_snapshot(&case.conversation.id())
                        .await
                        .expect("unconfirmed cancellation remains recovery-owned");
                    assert_eq!(
                        retained.record.value["cancellation"]["state"],
                        "dispatching"
                    );
                } else {
                    abort.expect("confirmed cancellation outcome settles without replay");
                    if !recovered {
                        assert_eq!(
                            Self::next(&mut stream).await.unwrap_err().kind(),
                            ErrorKind::Cancelled
                        );
                    }
                    case.settled().await;
                }
                assert!(stream.recv().await.is_none());
            },
        )
        .await;
        case.faults
            .state
            .persistence_rollback_failure
            .store(false, Ordering::SeqCst);
        if matches!(
            fault,
            PromptPersistenceFault::AbortOutcomeOnce
                | PromptPersistenceFault::AbortOutcomeCommitUnavailableOnce
        ) {
            // The purposefully unconfirmed dispatch cannot be relabeled settled.
            // Drop the hierarchy and prove its existing runtime owners drain,
            // keeping the committed recovery record for later reconstruction.
            let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
            cleanup
                .settle(async {
                    case.revoke().await;
                    Ok::<(), SdkError>(())
                })
                .await;
            cleanup.finish(observed);
        } else {
            case.finish_observed(observed).await;
        }
    }
}

#[tokio::test]
async fn opencode_abort_outcome_foreground_real_commit_conflict_recomputes_without_native_replay() {
    OpenCodeStreamCase::verify_abort_outcome_commit(
        PromptPersistenceFault::AbortOutcomeCommitConflictOnce,
        false,
    )
    .await;
}

#[tokio::test]
async fn opencode_abort_outcome_recovered_real_commit_conflict_recomputes_without_native_replay() {
    OpenCodeStreamCase::verify_abort_outcome_commit(
        PromptPersistenceFault::AbortOutcomeCommitConflictOnce,
        true,
    )
    .await;
}

#[tokio::test]
async fn opencode_abort_outcome_foreground_lost_receipt_is_readback_only() {
    OpenCodeStreamCase::verify_abort_outcome_commit(
        PromptPersistenceFault::AbortOutcomeCommitReceiptLost,
        false,
    )
    .await;
}

#[tokio::test]
async fn opencode_abort_outcome_recovered_lost_receipt_is_readback_only() {
    OpenCodeStreamCase::verify_abort_outcome_commit(
        PromptPersistenceFault::AbortOutcomeCommitReceiptLost,
        true,
    )
    .await;
}

#[tokio::test]
async fn opencode_abort_outcome_foreground_uncertain_commit_is_not_replayed() {
    OpenCodeStreamCase::verify_abort_outcome_commit(
        PromptPersistenceFault::AbortOutcomeCommitUnavailableOnce,
        false,
    )
    .await;
}

#[tokio::test]
async fn opencode_abort_outcome_recovered_uncertain_commit_is_not_replayed() {
    OpenCodeStreamCase::verify_abort_outcome_commit(
        PromptPersistenceFault::AbortOutcomeCommitUnavailableOnce,
        true,
    )
    .await;
}

#[tokio::test]
async fn opencode_abort_outcome_reopen_real_commit_conflict_remains_single_attempt() {
    let mut original = Some(OpenCodeStreamCase::start().await);
    let faults = original.as_ref().unwrap().faults.clone();
    let stats = Arc::clone(&original.as_ref().unwrap().probe.stats);
    let identity = original.as_ref().unwrap().agent.id();
    let conversation_id = original.as_ref().unwrap().conversation.id();
    let baseline = original.as_ref().unwrap().runtime_baseline;
    let config = original
        .as_ref()
        .unwrap()
        .probe
        .config(Duration::from_secs(15), Duration::from_secs(5));
    let mut probe = None;
    let mut reopened = None;
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(25),
        async {
            let case = original.as_ref().unwrap();
            let mut stream = case
                .stream_with_admission_fault(PromptPersistenceFault::Admission)
                .await;
            let initial = tokio::time::timeout(Duration::from_secs(7), stream.recv())
                .await
                .expect("foreground admission must settle within its existing bound")
                .expect("one foreground terminal outcome")
                .unwrap_err();
            assert_eq!(initial.kind(), ErrorKind::Timeout);
            assert!(stream.recv().await.is_none());
            let before = faults.prompt_snapshot(&conversation_id).await.unwrap();
            assert!(stats.native_active.load(Ordering::SeqCst));
            assert_eq!(stats.aborts.load(Ordering::SeqCst), 0);
            let OpenCodeStreamCase {
                probe: native,
                agent,
                workspace,
                conversation,
                ..
            } = original.take().unwrap();
            probe = Some(native);
            drop(conversation);
            drop(workspace);
            drop(agent);
            tokio::time::timeout(Duration::from_secs(3), async {
                while tokio::runtime::Handle::current()
                    .metrics()
                    .num_alive_tasks()
                    > baseline + stats.connections.load(Ordering::SeqCst)
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the original hierarchy joins before caller reconstruction");
            assert_eq!(
                faults.prompt_snapshot(&conversation_id).await.unwrap(),
                before
            );
            stats.prompt_release.add_permits(1);
            tokio::time::timeout(Duration::from_secs(3), async {
                while stats.prompt_responses.load(Ordering::SeqCst) != 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the abandoned native observer must finish");
            probe.as_ref().unwrap().mark_native_active();
            faults.state.persistence_attempts.store(0, Ordering::SeqCst);
            faults.set_persistence_fault(PromptPersistenceFault::AbortOutcomeCommitConflictOnce);
            let result = AgentBuilder::reopen(config, faults.storage(), identity)
                .build()
                .await;
            let error = match result {
                Err(error) => error,
                Ok(agent) => {
                    reopened = Some(agent);
                    panic!("Reopen must surface the first definite cancellation-outcome conflict");
                }
            };
            assert_eq!(error.kind(), ErrorKind::Conflict, "{error:?}");
            assert_eq!(error.context().operation, Operation::Reopen);
            assert_eq!(error.context().phase, ErrorPhase::StateCommit);
            assert_eq!(error.detail(), Some(&ErrorDetail::TransactionConflict));
            assert_eq!(faults.state.persistence_attempts.load(Ordering::SeqCst), 1);
            assert_eq!(
                faults
                    .state
                    .cancellation_outcome_records
                    .lock()
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(stats.messages.load(Ordering::SeqCst), 1);
            assert_eq!(stats.aborts.load(Ordering::SeqCst), 1);
            let retained = faults.prompt_snapshot(&conversation_id).await.unwrap();
            assert_eq!(
                retained.record.value["message_id"],
                before.record.value["message_id"]
            );
            assert_eq!(
                retained.record.value["cancellation"]["state"],
                "dispatching"
            );
        },
    )
    .await;
    faults.set_persistence_fault(PromptPersistenceFault::None);
    stats.prompt_release.add_permits(1);
    stats.native_active.store(false, Ordering::SeqCst);
    let mut cleanup = NativeCleanup::new(Duration::from_secs(7));
    if let Some(case) = original.take() {
        let OpenCodeStreamCase {
            probe: native,
            agent,
            workspace,
            conversation,
            ..
        } = case;
        probe = Some(native);
        drop(conversation);
        drop(workspace);
        cleanup.settle(agent.shutdown()).await;
    }
    if let Some(agent) = reopened.take() {
        cleanup.settle(agent.shutdown()).await;
    }
    if let Some(probe) = probe.take() {
        cleanup
            .settle(async {
                probe.shutdown().await;
                Ok::<(), SdkError>(())
            })
            .await;
    }
    cleanup.finish(observed);
}

#[tokio::test]
async fn opencode_abort_outcome_foreground_failed_rollback_vetoes_retry() {
    OpenCodeStreamCase::verify_abort_outcome_commit(
        PromptPersistenceFault::AbortOutcomeOnce,
        false,
    )
    .await;
}

#[tokio::test]
async fn opencode_abort_outcome_recovered_failed_rollback_vetoes_retry() {
    OpenCodeStreamCase::verify_abort_outcome_commit(PromptPersistenceFault::AbortOutcomeOnce, true)
        .await;
}
