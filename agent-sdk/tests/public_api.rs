use provider_sdk::error::{
    ErrorKind as ProviderErrorKind, ErrorPhase as ProviderErrorPhase,
    Operation as ProviderOperation,
};
use std::future::{Future, pending, poll_fn};
use std::net::{Ipv4Addr, SocketAddr};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use agent_sdk::config::{
    AgentConfig, CodexConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval,
};
use agent_sdk::conversation::{ConversationInfo, FileRead, NewConversation};
use agent_sdk::identity::{AgentId, AgentInfo, ConversationId, NotificationId, WorkspaceId};
use agent_sdk::knowledge::KnowledgeSource;
use agent_sdk::model::ModelSelector;
use agent_sdk::notification::{ConversationNotification, ConversationNotificationKind};
use agent_sdk::tool::ToolSelection;
use agent_sdk::workspace::{
    DeleteWorkspaceRequest, NewWorkspace, WorkspaceContext, WorkspaceDefaults,
    WorkspaceExecutionPolicy, WorkspaceInfo, WorkspaceNetworkAccess, WorkspaceRoot,
    WorkspaceRootAccess, WorkspaceSandboxMode,
};
use agent_sdk::{
    Agent, AgentBackend, AgentBuilder, AgentStorage, Conversation, ConversationBackend, SdkError,
    Workspace, WorkspaceBackend,
};
use provider_sdk::config::{OllamaConfig, ProviderKind, TransportConfig};
use provider_sdk::generation::{GenerateEventStream, GenerateRequest, GenerateResponse};
use provider_sdk::models::{ModelBackend, ModelId};
use provider_sdk::session::SessionBackend;
use provider_sdk::{Provider, ProviderBackend, ProviderConfig, SdkError as ProviderError};
use storage_sdk::database::config::{DatabaseConfig, LanceConfig};
use storage_sdk::database::{
    CapabilitySet,
    catalog::{CatalogRequest, CatalogResponse},
    data::{DataRequest, DataResponse},
    index::{IndexRequest, IndexResponse},
    query::{QueryRequest, QueryResponse},
};
use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_sdk::state::record::{
    StateKey, StateListRequest, StateListResponse, StateRecord, StateRecordSnapshot,
};
use storage_sdk::{
    Database, DatabaseBackend, StateStore, StateStoreBackend, StateStoreError, StateTransaction,
    StateTransactionBackend, StorageError,
};
use tokio::sync::{Semaphore, mpsc};
use tool_sdk::declaration::{ToolInfo, ToolName};
use tool_sdk::execution::{ToolHandlerInput, ToolHandlerOutput};
use tool_sdk::{ToolError, ToolHandlerBackend, ToolsetBackend};

const DEADLINE: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StoreOperation {
    Load,
    List,
    BeginTransaction,
    TransactionLoad,
}

struct BlockingStateStore {
    operations: mpsc::Sender<StoreOperation>,
}

struct DirectProvider;

struct DirectTools;

struct DirectHandler;

impl ToolsetBackend for DirectTools {
    type Handler = DirectHandler;

    async fn list_tools(&self) -> Result<Vec<ToolInfo>, ToolError> {
        Ok(Vec::new())
    }

    async fn tool_handler(&self, name: ToolName) -> Result<Self::Handler, ToolError> {
        Err(ToolError::UnknownTool { name })
    }

    async fn shutdown(&self) -> Result<(), ToolError> {
        Ok(())
    }
}

impl ToolHandlerBackend for DirectHandler {
    async fn step(&mut self, _input: ToolHandlerInput) -> Result<ToolHandlerOutput, ToolError> {
        Err(ToolError::Unsupported {
            operation: "empty test inventory".to_owned(),
        })
    }
}

#[derive(Clone)]
struct DirectContinuation;

struct DirectModel {
    id: ModelId,
}

impl ProviderBackend for DirectProvider {
    type Model = DirectModel;

    fn model(&self, id: ModelId) -> Result<Self::Model, ProviderError> {
        Ok(DirectModel { id })
    }

    async fn shutdown(&self) -> Result<(), ProviderError> {
        Ok(())
    }
}

impl ModelBackend for DirectModel {
    type Continuation = DirectContinuation;
    type Session = std::convert::Infallible;

    fn model_id(&self) -> &ModelId {
        &self.id
    }

    async fn generate(
        &self,
        _request: &GenerateRequest<DirectContinuation>,
    ) -> Result<GenerateResponse<DirectContinuation>, ProviderError> {
        Err(ProviderError::new(
            ProviderErrorKind::Unsupported,
            ProviderOperation::Generate,
            ProviderErrorPhase::InputValidation,
        ))
    }

    async fn generate_stream(
        &self,
        _request: &GenerateRequest<DirectContinuation>,
    ) -> Result<GenerateEventStream<DirectContinuation>, ProviderError> {
        Err(ProviderError::new(
            ProviderErrorKind::Unsupported,
            ProviderOperation::GenerateStream,
            ProviderErrorPhase::InputValidation,
        ))
    }
}

struct BlockingStateTransaction {
    operations: mpsc::Sender<StoreOperation>,
}
struct StoreProbe {
    backend: Option<BlockingStateStore>,
    operations: mpsc::Receiver<StoreOperation>,
}

impl StoreProbe {
    fn new() -> Self {
        let (operations, observed) = mpsc::channel(8);
        Self {
            backend: Some(BlockingStateStore { operations }),
            operations: observed,
        }
    }
    fn storage(&mut self) -> AgentStorage<BlockingStateStore> {
        AgentStorage::new(
            self.backend
                .take()
                .expect("probe StateStore must transfer exactly once"),
        )
    }
    fn agent_config() -> AgentConfig {
        let codex = CodexConfig::tcp(
            SocketAddr::from((Ipv4Addr::LOCALHOST, 9)),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .expect("probe Codex config must be valid");
        AgentConfig::new(
            codex.into(),
            Duration::from_secs(1),
            NonZeroUsize::new(1).expect("one is nonzero"),
        )
        .expect("probe Agent config must be valid")
    }
    fn missing_agent_id() -> AgentId {
        "agent_00000000000000000000000000000001"
            .parse()
            .expect("canonical AgentId")
    }
    async fn next(&mut self) -> StoreOperation {
        tokio::time::timeout(DEADLINE, self.operations.recv())
            .await
            .expect("StateStore operation must begin before its deadline")
            .expect("StateStore operation channel must remain open")
    }
    async fn expect_missing_lookup(&mut self, storage: &AgentStorage<BlockingStateStore>) {
        let lookup = tokio::spawn({
            let storage = storage.clone();
            async move { storage.agent_info(Self::missing_agent_id()).await }
        });
        assert_eq!(self.next().await, StoreOperation::Load);
        assert!(matches!(tokio::time::timeout(DEADLINE, lookup)
                .await
                .expect("lookup must remain bounded")
                .expect("lookup task must not panic"), Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::NotFound)));
    }
    async fn abort<T>(task: tokio::task::JoinHandle<T>) {
        task.abort();
        let result = tokio::time::timeout(DEADLINE, task)
            .await
            .expect("aborted task must join");
        assert!(matches!(result, Err(error) if error.is_cancelled()));
    }
}

impl StateStoreBackend for BlockingStateStore {
    type Transaction = BlockingStateTransaction;
    async fn load(&self, _key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.operations
            .send(StoreOperation::Load)
            .await
            .expect("probe must remain available");
        Ok(None)
    }
    async fn list(&self, _request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        self.operations
            .send(StoreOperation::List)
            .await
            .expect("probe must remain available");
        pending().await
    }
    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        self.operations
            .send(StoreOperation::BeginTransaction)
            .await
            .expect("probe must remain available");
        Ok(BlockingStateTransaction {
            operations: self.operations.clone(),
        })
    }
}

impl StateTransactionBackend for BlockingStateTransaction {
    async fn load(
        &mut self,
        _key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.operations
            .send(StoreOperation::TransactionLoad)
            .await
            .expect("probe must remain available");
        pending().await
    }
    async fn put(&mut self, _record: StateRecord) -> Result<(), StateStoreError> {
        Ok(())
    }
    async fn delete(&mut self, _key: StateKey) -> Result<(), StateStoreError> {
        Ok(())
    }
    fn commit(self) -> impl Future<Output = Result<(), StateStoreError>> + Send {
        pending()
    }
    fn rollback(self) -> impl Future<Output = Result<(), StateStoreError>> + Send {
        pending()
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CommitFault {
    None,
    ConflictBeforeCommit,
    UnavailableBeforeCommit,
    UnavailableAfterCommit,
    BlockThenCommit,
    TimedOutBeforeCommit,
    TimedOutAfterCommit,
    BlockThenTimedOutBeforeCommit,
    BlockThenTimedOutAfterCommit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CloseStorageOperation {
    Begin,
    Load,
    Put,
    Delete,
    Commit,
    Rollback,
}

enum OperationFaultTarget {
    Transaction(usize),
    Invocation(usize),
}

enum CommitFaultTarget {
    WriteOrdinal(usize),
    Delete(StateKey),
}

struct ArmedCommitFault {
    fault: CommitFault,
    target: CommitFaultTarget,
}

struct CommitFaultState {
    armed: Mutex<Option<ArmedCommitFault>>,
    write_commits: AtomicUsize,
    blocked: Semaphore,
    release: Semaphore,
    operation_fault: Mutex<Option<(CloseStorageOperation, OperationFaultTarget, StateStoreError)>>,
    held_load: Mutex<Option<(usize, bool)>>,
    settled_loads: AtomicUsize,
    load_settled: Semaphore,
    transactions: AtomicUsize,
    injected_timeouts: AtomicUsize,
    injected_faults: AtomicUsize,
}

#[derive(Clone)]
struct FaultStateStore {
    inner: StateStore,
    state: Arc<CommitFaultState>,
}

impl FaultStateStore {
    fn new(inner: StateStore) -> Self {
        Self {
            inner,
            state: Arc::new(CommitFaultState {
                armed: Mutex::new(None),
                write_commits: AtomicUsize::new(0),
                blocked: Semaphore::new(0),
                release: Semaphore::new(0),
                operation_fault: Mutex::new(None),
                held_load: Mutex::new(None),
                settled_loads: AtomicUsize::new(0),
                load_settled: Semaphore::new(0),
                transactions: AtomicUsize::new(0),
                injected_timeouts: AtomicUsize::new(0),
                injected_faults: AtomicUsize::new(0),
            }),
        }
    }
    fn arm(&self, fault: CommitFault, target: usize) {
        assert!(target > 0);
        self.arm_target(fault, CommitFaultTarget::WriteOrdinal(target));
    }
    fn arm_delete(&self, fault: CommitFault, key: StateKey) {
        self.arm_target(fault, CommitFaultTarget::Delete(key));
    }
    fn arm_target(&self, fault: CommitFault, target: CommitFaultTarget) {
        assert!(fault != CommitFault::None);
        self.state.write_commits.store(0, Ordering::SeqCst);
        self.state.transactions.store(0, Ordering::SeqCst);
        self.state.injected_timeouts.store(0, Ordering::SeqCst);
        self.state.injected_faults.store(0, Ordering::SeqCst);
        *self
            .state
            .armed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            Some(ArmedCommitFault { fault, target });
    }
    fn clear(&self) {
        *self
            .state
            .armed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        *self.state.operation_fault.lock().expect("operation fault") = None;
        *self.state.held_load.lock().expect("held load fault") = None;
    }
    fn arm_operation_timeout(&self, operation: CloseStorageOperation, transaction: usize) {
        self.arm_operation_fault(
            operation,
            OperationFaultTarget::Transaction(transaction),
            StateStoreError::timed_out("private-close-timeout", 41),
        );
    }
    fn arm_operation_fault(
        &self,
        operation: CloseStorageOperation,
        target: OperationFaultTarget,
        error: StateStoreError,
    ) {
        self.clear();
        self.state.write_commits.store(0, Ordering::SeqCst);
        self.state.transactions.store(0, Ordering::SeqCst);
        self.state.injected_timeouts.store(0, Ordering::SeqCst);
        self.state.injected_faults.store(0, Ordering::SeqCst);
        *self.state.operation_fault.lock().expect("operation fault") =
            Some((operation, target, error));
    }
    fn arm_held_load(&self, transaction: usize, fail_after_release: bool) {
        self.clear();
        self.state.write_commits.store(0, Ordering::SeqCst);
        self.state.transactions.store(0, Ordering::SeqCst);
        self.state.settled_loads.store(0, Ordering::SeqCst);
        *self.state.held_load.lock().expect("held load fault") =
            Some((transaction, fail_after_release));
    }
    async fn hold_load(&self, transaction: usize) -> Result<(), StateStoreError> {
        let fault = {
            let mut fault = self.state.held_load.lock().expect("held load fault");
            if fault
                .as_ref()
                .is_some_and(|(target, _)| *target == transaction)
            {
                fault.take()
            } else {
                None
            }
        };
        let Some((_, fail_after_release)) = fault else {
            return Ok(());
        };
        self.state.blocked.add_permits(1);
        self.state
            .release
            .acquire()
            .await
            .expect("load release")
            .forget();
        self.state.settled_loads.fetch_add(1, Ordering::SeqCst);
        self.state.load_settled.add_permits(1);
        if fail_after_release {
            Err(StateStoreError::timed_out("private-close-timeout", 41))
        } else {
            Ok(())
        }
    }
    async fn wait_until_load_settled(&self) {
        self.state
            .load_settled
            .acquire()
            .await
            .expect("load outcome")
            .forget();
    }
    fn check_operation_fault(
        &self,
        operation: CloseStorageOperation,
        transaction: usize,
    ) -> Result<(), StateStoreError> {
        let mut fault = self.state.operation_fault.lock().expect("operation fault");
        let selected = match fault.as_mut() {
            Some((selected, target, _)) if *selected == operation => match target {
                OperationFaultTarget::Transaction(target) => *target == transaction,
                OperationFaultTarget::Invocation(remaining) => {
                    *remaining -= 1;
                    *remaining == 0
                }
            },
            Some(_) | None => false,
        };
        if selected {
            let (_, _, error) = fault.take().expect("selected operation fault");
            if matches!(&error, StateStoreError::TimedOut { .. }) {
                self.state.injected_timeouts.fetch_add(1, Ordering::SeqCst);
            }
            self.state.injected_faults.fetch_add(1, Ordering::SeqCst);
            return Err(error);
        }
        Ok(())
    }
    fn transaction_count(&self) -> usize {
        self.state.transactions.load(Ordering::SeqCst)
    }
    fn write_commit_count(&self) -> usize {
        self.state.write_commits.load(Ordering::SeqCst)
    }
    async fn before_commit(&self, wrote: bool, deleted: &[StateKey]) -> CommitFault {
        if !wrote {
            return CommitFault::None;
        }
        let ordinal = self.state.write_commits.fetch_add(1, Ordering::SeqCst) + 1;
        let fault = {
            let mut armed = self
                .state
                .armed
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let matches = armed.as_ref().is_some_and(|armed| match &armed.target {
                CommitFaultTarget::WriteOrdinal(target) => ordinal == *target,
                CommitFaultTarget::Delete(target) => deleted.contains(target),
            });
            if matches {
                self.state.injected_faults.fetch_add(1, Ordering::SeqCst);
                armed.take().map_or(CommitFault::None, |armed| armed.fault)
            } else {
                CommitFault::None
            }
        };
        if matches!(
            fault,
            CommitFault::BlockThenCommit
                | CommitFault::BlockThenTimedOutBeforeCommit
                | CommitFault::BlockThenTimedOutAfterCommit
        ) {
            self.state.blocked.add_permits(1);
            self.state
                .release
                .acquire()
                .await
                .expect("fault release semaphore must remain open")
                .forget();
        }
        fault
    }
    async fn wait_until_blocked(&self) {
        self.state
            .blocked
            .acquire()
            .await
            .expect("fault block semaphore must remain open")
            .forget();
    }
    fn release(&self) {
        self.state.release.add_permits(1);
    }
}

struct FaultStateTransaction {
    inner: StateTransaction,
    faults: FaultStateStore,
    wrote: bool,
    deleted: Vec<StateKey>,
    ordinal: usize,
}

impl StateStoreBackend for FaultStateStore {
    type Transaction = FaultStateTransaction;
    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.inner.load(key).await
    }
    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        self.inner.list(request).await
    }
    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        let ordinal = self.state.transactions.fetch_add(1, Ordering::SeqCst) + 1;
        self.check_operation_fault(CloseStorageOperation::Begin, ordinal)?;
        Ok(FaultStateTransaction {
            inner: self.inner.begin_transaction().await?,
            faults: self.clone(),
            wrote: false,
            deleted: Vec::new(),
            ordinal,
        })
    }
}

impl StateTransactionBackend for FaultStateTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.faults
            .check_operation_fault(CloseStorageOperation::Load, self.ordinal)?;
        self.faults.hold_load(self.ordinal).await?;
        self.inner.load(key).await
    }
    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        self.faults
            .check_operation_fault(CloseStorageOperation::Put, self.ordinal)?;
        self.wrote = true;
        self.inner.put(record).await
    }
    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        self.faults
            .check_operation_fault(CloseStorageOperation::Delete, self.ordinal)?;
        self.wrote = true;
        self.inner.delete(key.clone()).await?;
        self.deleted.push(key);
        Ok(())
    }
    async fn commit(self) -> Result<(), StateStoreError> {
        match self.faults.before_commit(self.wrote, &self.deleted).await {
            CommitFault::ConflictBeforeCommit => {
                self.inner.rollback().await?;
                Err(StateStoreError::transaction_conflict(
                    "commit",
                    "injected conflict",
                ))
            }
            CommitFault::UnavailableBeforeCommit => {
                self.inner.rollback().await?;
                Err(StateStoreError::unavailable(
                    "commit",
                    "injected unavailability",
                ))
            }
            CommitFault::UnavailableAfterCommit => {
                self.inner.commit().await?;
                Err(StateStoreError::unavailable(
                    "commit",
                    "ambiguous commit response",
                ))
            }
            CommitFault::TimedOutBeforeCommit | CommitFault::BlockThenTimedOutBeforeCommit => {
                self.inner.rollback().await?;
                self.faults
                    .state
                    .injected_timeouts
                    .fetch_add(1, Ordering::SeqCst);
                Err(StateStoreError::timed_out("private-close-timeout", 41))
            }
            CommitFault::TimedOutAfterCommit | CommitFault::BlockThenTimedOutAfterCommit => {
                self.inner.commit().await?;
                self.faults
                    .state
                    .injected_timeouts
                    .fetch_add(1, Ordering::SeqCst);
                Err(StateStoreError::timed_out("private-close-timeout", 41))
            }
            CommitFault::None | CommitFault::BlockThenCommit => self.inner.commit().await,
        }
    }
    async fn rollback(self) -> Result<(), StateStoreError> {
        self.inner.rollback().await?;
        self.faults
            .check_operation_fault(CloseStorageOperation::Rollback, self.ordinal)
    }
}

struct KnowledgeDeleteProbe {
    database: Database,
    gate: Arc<KnowledgeDeleteGate>,
}

struct KnowledgeDeleteControl {
    gate: Arc<KnowledgeDeleteGate>,
}

struct KnowledgeDeleteGate {
    armed: AtomicBool,
    started: AtomicBool,
    observed: Semaphore,
    release: Semaphore,
    fail_after_release: AtomicBool,
    calls: AtomicUsize,
    settled: AtomicUsize,
    interrupted: AtomicUsize,
    outcome: Semaphore,
}

struct KnowledgeDeleteGuard<'a> {
    gate: &'a KnowledgeDeleteGate,
    settled: bool,
}

impl KnowledgeDeleteGuard<'_> {
    fn complete(mut self) {
        self.settled = true;
        self.gate.settled.fetch_add(1, Ordering::SeqCst);
        self.gate.outcome.add_permits(1);
    }
}

impl Drop for KnowledgeDeleteGuard<'_> {
    fn drop(&mut self) {
        if !self.settled {
            self.gate.interrupted.fetch_add(1, Ordering::SeqCst);
            self.gate.outcome.add_permits(1);
        }
    }
}

impl KnowledgeDeleteProbe {
    fn new(database: Database) -> (Self, KnowledgeDeleteControl) {
        let gate = Arc::new(KnowledgeDeleteGate {
            armed: AtomicBool::new(false),
            started: AtomicBool::new(false),
            observed: Semaphore::new(0),
            release: Semaphore::new(0),
            fail_after_release: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
            settled: AtomicUsize::new(0),
            interrupted: AtomicUsize::new(0),
            outcome: Semaphore::new(0),
        });
        (
            Self {
                database,
                gate: Arc::clone(&gate),
            },
            KnowledgeDeleteControl { gate },
        )
    }
}

impl KnowledgeDeleteControl {
    fn fail_after_release(&self) {
        self.gate.fail_after_release.store(true, Ordering::SeqCst);
    }

    fn calls(&self) -> usize {
        self.gate.calls.load(Ordering::SeqCst)
    }

    fn arm(&self) {
        self.gate.calls.store(0, Ordering::SeqCst);
        assert!(
            !self.gate.armed.swap(true, Ordering::SeqCst),
            "knowledge delete probe must not already be armed"
        );
        assert!(
            !self.gate.started.load(Ordering::SeqCst),
            "knowledge delete probe supports one exact deletion"
        );
    }

    async fn wait_for_outcome(&self) {
        self.gate
            .outcome
            .acquire()
            .await
            .expect("delete outcome")
            .forget();
    }

    fn deletion_started(&self) -> bool {
        self.gate.started.load(Ordering::SeqCst)
    }

    async fn wait_until_blocked(&self) {
        self.gate
            .observed
            .acquire()
            .await
            .expect("knowledge delete observation must remain open")
            .forget();
    }

    fn release(&self) {
        self.gate.release.add_permits(1);
    }
}

impl DatabaseBackend for KnowledgeDeleteProbe {
    fn capabilities(&self) -> CapabilitySet {
        self.database.capabilities()
    }

    async fn catalog(&self, request: CatalogRequest) -> Result<CatalogResponse, StorageError> {
        self.database.catalog(request).await
    }

    async fn data(&self, request: DataRequest) -> Result<DataResponse, StorageError> {
        if matches!(&request, DataRequest::Delete(_)) {
            self.gate.calls.fetch_add(1, Ordering::SeqCst);
        }
        if matches!(&request, DataRequest::Delete(_))
            && self.gate.armed.swap(false, Ordering::SeqCst)
        {
            let guard = KnowledgeDeleteGuard {
                gate: &self.gate,
                settled: false,
            };
            self.gate.started.store(true, Ordering::SeqCst);
            self.gate.observed.add_permits(1);
            self.gate
                .release
                .acquire()
                .await
                .expect("knowledge delete release must remain open")
                .forget();
            let result = if self.gate.fail_after_release.load(Ordering::SeqCst) {
                Err(StorageError::backend_op(
                    "delete",
                    "private knowledge failure",
                ))
            } else {
                self.database.data(request).await
            };
            guard.complete();
            return result;
        }
        self.database.data(request).await
    }

    async fn query(&self, request: QueryRequest) -> Result<QueryResponse, StorageError> {
        self.database.query(request).await
    }

    async fn index(&self, request: IndexRequest) -> Result<IndexResponse, StorageError> {
        self.database.index(request).await
    }
}

struct CustomFixture<D: DatabaseBackend = Database> {
    faults: FaultStateStore,
    agent: Agent<FaultStateStore, D>,
    root: PathBuf,
}

impl<D: DatabaseBackend> CustomFixture<D> {
    fn config(lifecycle_timeout: Duration) -> AgentConfig {
        let custom = CustomAgentConfig::new(
            ModelSelector::new("qwen3").expect("fixture model selector must be valid"),
            ToolSelection::None,
            CustomLoopLimits::new(2, 0, 1).expect("fixture loop limits must be valid"),
            CustomToolApproval::Deny,
        )
        .expect("fixture Custom config must be valid");
        AgentConfig::new(
            custom.into(),
            lifecycle_timeout,
            NonZeroUsize::new(1).expect("one is nonzero"),
        )
        .expect("fixture Agent config must be valid")
    }
    fn provider() -> Provider {
        Provider::from_config(ProviderConfig::new(
            ProviderKind::Ollama(OllamaConfig::local()),
            TransportConfig::default(),
        ))
        .expect("fixture Provider must build without network access")
    }
    async fn build_with_knowledge_database(database: D) -> Self {
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("fixture Lance StateStore must build");
        let faults = FaultStateStore::new(store);
        let root = std::env::current_dir().expect("fixture cwd must be available");
        let agent = AgentBuilder::create(
            Self::config(Duration::from_secs(2)),
            AgentStorage::new(faults.clone()),
        )
        .with_provider(Self::provider())
        .with_knowledge_database(database)
        .build()
        .await
        .expect("fixture Custom Agent must build");
        Self {
            faults,
            agent,
            root,
        }
    }

    fn workspace_request(&self, name: &str) -> NewWorkspace {
        let path = self.root.clone();
        let context = WorkspaceContext::new(
            path.clone(),
            vec![
                WorkspaceRoot::new(path, WorkspaceRootAccess::ReadWrite)
                    .expect("fixture workspace root must be valid"),
            ],
            WorkspaceExecutionPolicy::new(
                WorkspaceSandboxMode::FullAccess,
                WorkspaceNetworkAccess::Disabled,
            ),
        )
        .expect("fixture workspace context must be valid");
        NewWorkspace::new(name, context, WorkspaceDefaults::empty())
            .expect("fixture workspace request must be valid")
    }
}

impl CustomFixture<Database> {
    async fn build() -> Self {
        Self::build_with_lifecycle_timeout(Duration::from_secs(2)).await
    }

    async fn build_with_lifecycle_timeout(lifecycle_timeout: Duration) -> Self {
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("fixture Lance StateStore must build");
        let faults = FaultStateStore::new(store);
        let root = std::env::current_dir().expect("fixture cwd must be available");
        let agent = AgentBuilder::create(
            Self::config(lifecycle_timeout),
            AgentStorage::new(faults.clone()),
        )
        .with_provider(Self::provider())
        .build()
        .await
        .expect("fixture Custom Agent must build");
        Self {
            faults,
            agent,
            root,
        }
    }

    async fn reopen(faults: &FaultStateStore, id: AgentId) -> Agent<FaultStateStore> {
        AgentBuilder::reopen(
            Self::config(Duration::from_secs(2)),
            AgentStorage::new(faults.clone()),
            id,
        )
        .with_provider(Self::provider())
        .build()
        .await
        .expect("fixture Custom Agent must reopen")
    }

    fn conversation_request() -> NewConversation {
        NewConversation::new(None, None, Vec::new()).expect("valid conversation request")
    }
}

#[tokio::test]
async fn agent_storage_reads_progress_behind_reads_and_associated_transactions() {
    let mut probe = StoreProbe::new();
    let storage = probe.storage();
    let blocked_list = tokio::spawn({
        let storage = storage.clone();
        async move { storage.list_agents().await }
    });
    assert_eq!(probe.next().await, StoreOperation::List);
    probe.expect_missing_lookup(&storage).await;
    assert!(!blocked_list.is_finished());
    StoreProbe::abort(blocked_list).await;

    let blocked_build = tokio::spawn({
        let storage = storage.clone();
        async move {
            AgentBuilder::create_for_backends::<DirectProvider, DirectTools>(
                StoreProbe::agent_config(),
                storage,
            )
            .build()
            .await
        }
    });
    assert_eq!(probe.next().await, StoreOperation::BeginTransaction);
    assert_eq!(probe.next().await, StoreOperation::TransactionLoad);
    probe.expect_missing_lookup(&storage).await;
    assert!(!blocked_build.is_finished());
    StoreProbe::abort(blocked_build).await;
}

#[test]
fn static_integration_types_preserve_associated_children_without_clone_bounds() {
    fn assert_clone<T: Clone>() {}
    fn assert_transaction<T: StateTransactionBackend>() {}
    fn assert_session<T: SessionBackend>() {}
    fn assert_direct_model<T>()
    where
        T: ModelBackend<Continuation = DirectContinuation, Session = std::convert::Infallible>,
    {
    }
    fn assert_hierarchy<S, D, P, T>()
    where
        S: StateStoreBackend,
        D: DatabaseBackend,
        P: ProviderBackend,
        T: ToolsetBackend,
        Agent<S, D, P, T>: AgentBackend<Workspace = Workspace<S, D, P, T>>,
        Workspace<S, D, P, T>: WorkspaceBackend<Conversation = Conversation<S, D, P, T>>,
        Conversation<S, D, P, T>: ConversationBackend,
    {
        assert_transaction::<S::Transaction>();
        assert_session::<<<P as ProviderBackend>::Model as ModelBackend>::Session>();
    }

    fn replace_database(
        builder: AgentBuilder<StateStore, Database, DirectProvider, DirectTools>,
        database: KnowledgeDeleteProbe,
    ) -> AgentBuilder<StateStore, KnowledgeDeleteProbe, DirectProvider, DirectTools> {
        builder.with_knowledge_database(database)
    }

    assert_clone::<AgentStorage<BlockingStateStore>>();
    assert_direct_model::<DirectModel>();
    assert_hierarchy::<BlockingStateStore, KnowledgeDeleteProbe, DirectProvider, DirectTools>();
    assert_hierarchy::<StateStore, Database, Provider, tool_sdk::Toolset>();

    let mut probe = StoreProbe::new();
    let storage = probe.storage();
    let _: AgentBuilder<BlockingStateStore, Database, DirectProvider, DirectTools> =
        AgentBuilder::<BlockingStateStore>::create_for_backends::<DirectProvider, DirectTools>(
            StoreProbe::agent_config(),
            storage.clone(),
        );
    let _: AgentBuilder<BlockingStateStore, Database, DirectProvider, DirectTools> =
        AgentBuilder::<BlockingStateStore>::reopen_for_backends::<DirectProvider, DirectTools>(
            StoreProbe::agent_config(),
            storage,
            StoreProbe::missing_agent_id(),
        );
    let default_create: fn(
        AgentConfig,
        AgentStorage<BlockingStateStore>,
    ) -> AgentBuilder<BlockingStateStore> = AgentBuilder::create;
    let default_reopen: fn(
        AgentConfig,
        AgentStorage<BlockingStateStore>,
        AgentId,
    ) -> AgentBuilder<BlockingStateStore> = AgentBuilder::reopen;
    let select_direct_provider: fn(
        AgentBuilder,
    ) -> AgentBuilder<StateStore, Database, DirectProvider> =
        |builder| builder.with_provider(DirectProvider);
    let _ = (
        default_create,
        default_reopen,
        replace_database,
        select_direct_provider,
    );
}

#[tokio::test]
async fn custom_close_propagates_settled_storage_timeouts_without_restarting_a_phase() {
    use agent_sdk::error::{DeadlineKind, ErrorDetail, ErrorKind, ErrorPhase};
    // With no active prompt, cancellation preserves the continuation and must
    // finish its read transaction through rollback. Close admission and terminal
    // close still mutate state; pending interactions have a separate read.
    for (target, completion) in [
        (1, CloseStorageOperation::Put),
        (2, CloseStorageOperation::Rollback),
        (4, CloseStorageOperation::Put),
    ] {
        for operation in [
            CloseStorageOperation::Begin,
            CloseStorageOperation::Load,
            completion,
        ] {
            let fixture = CustomFixture::build().await;
            let workspace = fixture
                .agent
                .new_workspace(fixture.workspace_request("close-operation-timeout"))
                .await
                .expect("workspace");
            let conversation = workspace
                .new_conversation(CustomFixture::conversation_request())
                .await
                .expect("conversation");
            let receiver = conversation
                .notification_stream()
                .expect("notification receiver");
            fixture.faults.arm_operation_timeout(operation, target);
            let result = tokio::time::timeout(DEADLINE, conversation.close()).await;
            let attempts = fixture.faults.transaction_count();
            let injected = fixture
                .faults
                .state
                .injected_timeouts
                .load(Ordering::SeqCst);
            fixture.faults.clear();
            drop(receiver);
            drop(workspace);
            let shutdown = tokio::time::timeout(DEADLINE, fixture.agent.shutdown()).await;

            shutdown
                .expect("shutdown remains bounded")
                .expect("shutdown cleans owned work");
            let error = match result.expect("close remains bounded") {
                Err(error) => error,
                Ok(()) => panic!(
                    "settled storage timeout must end close: target={target} operation={operation:?} injected={injected} attempts={attempts}"
                ),
            };
            assert_eq!(
                error.kind(),
                ErrorKind::Timeout,
                "target={target} operation={operation:?}"
            );
            assert_eq!(
                error.context().phase,
                match operation {
                    CloseStorageOperation::Load => ErrorPhase::StateRead,
                    CloseStorageOperation::Begin
                    | CloseStorageOperation::Put
                    | CloseStorageOperation::Delete => ErrorPhase::StateWrite,
                    CloseStorageOperation::Commit => ErrorPhase::StateCommit,
                    CloseStorageOperation::Rollback => ErrorPhase::Cleanup,
                }
            );
            assert_eq!(
                error.detail(),
                Some(&ErrorDetail::DeadlineElapsed {
                    kind: DeadlineKind::Operation,
                    timeout: Some(Duration::from_millis(41)),
                })
            );
            assert_eq!(injected, 1);
            assert_eq!(
                attempts, target,
                "no retry or later close phase may start after failure"
            );
            assert!(!format!("{error:?}").contains("private-close-timeout"));
        }
    }
}

#[tokio::test]
async fn custom_close_commit_timeouts_preserve_exact_commit_authority() {
    use agent_sdk::error::{DeadlineKind, ErrorDetail, ErrorKind, ErrorPhase};
    for target in [1, 2] {
        for commit_first in [false, true] {
            let fixture = CustomFixture::build().await;
            let workspace = fixture
                .agent
                .new_workspace(fixture.workspace_request("close-commit-timeout"))
                .await
                .expect("workspace");
            let conversation = workspace
                .new_conversation(CustomFixture::conversation_request())
                .await
                .expect("conversation");
            let receiver = conversation
                .notification_stream()
                .expect("notification receiver");
            fixture.faults.arm(
                if commit_first {
                    CommitFault::TimedOutAfterCommit
                } else {
                    CommitFault::TimedOutBeforeCommit
                },
                target,
            );
            let result = tokio::time::timeout(DEADLINE, conversation.close()).await;
            let attempts = fixture.faults.transaction_count();
            let writes = fixture.faults.write_commit_count();
            let injected = fixture
                .faults
                .state
                .injected_timeouts
                .load(Ordering::SeqCst);
            fixture.faults.clear();
            drop(receiver);
            drop(workspace);
            let shutdown = tokio::time::timeout(DEADLINE, fixture.agent.shutdown()).await;

            shutdown
                .expect("shutdown remains bounded")
                .expect("shutdown cleans owned work");
            let result = result.expect("close remains bounded");
            if commit_first {
                result.expect("an exactly observed commit is authoritative");
                assert_eq!(
                    attempts, 4,
                    "two close mutations plus cancellation and interaction reads; no replay"
                );
                assert_eq!(writes, 2);
            } else {
                let error = result.expect_err("an uncommitted close mutation must fail");
                assert_eq!(error.kind(), ErrorKind::Timeout);
                assert_eq!(error.context().phase, ErrorPhase::StateCommit);
                assert_eq!(
                    error.detail(),
                    Some(&ErrorDetail::DeadlineElapsed {
                        kind: DeadlineKind::Operation,
                        timeout: Some(Duration::from_millis(41)),
                    })
                );
                assert_eq!(
                    attempts,
                    if target == 1 { 1 } else { 4 },
                    "a failed commit cannot restart its phase"
                );
                assert_eq!(writes, target);
            }
            assert_eq!(injected, 1);
        }
    }
}

#[tokio::test]
async fn custom_close_retains_one_held_mutation_after_waiter_expiry_or_caller_loss() {
    use agent_sdk::error::ErrorKind;
    let lifecycle_timeout = Duration::from_millis(300);
    for target in [1, 2] {
        for commit_first in [false, true] {
            for caller_loss in [false, true] {
                let fixture = CustomFixture::build_with_lifecycle_timeout(lifecycle_timeout).await;
                let workspace = fixture
                    .agent
                    .new_workspace(fixture.workspace_request("held-close-mutation"))
                    .await
                    .expect("workspace");
                let conversation = workspace
                    .new_conversation(CustomFixture::conversation_request())
                    .await
                    .expect("conversation");
                let mut receiver = conversation
                    .notification_stream()
                    .expect("notification receiver");
                fixture.faults.arm(
                    if commit_first {
                        CommitFault::BlockThenTimedOutAfterCommit
                    } else {
                        CommitFault::BlockThenTimedOutBeforeCommit
                    },
                    target,
                );
                let mut close = tokio::spawn(async move { conversation.close().await });
                let blocked =
                    tokio::time::timeout(DEADLINE, fixture.faults.wait_until_blocked()).await;
                if caller_loss {
                    close.abort();
                }
                let observation = tokio::time::timeout(DEADLINE, &mut close).await;
                // The admitted transaction stays held beyond both observation modes' phase budget.
                tokio::time::sleep(lifecycle_timeout + Duration::from_millis(25)).await;
                let held_writes = fixture.faults.write_commit_count();
                fixture.faults.clear();
                fixture.faults.release();
                let joined_after_release = if observation.is_err() {
                    Some(tokio::time::timeout(DEADLINE, &mut close).await)
                } else {
                    None
                };
                let drained = if commit_first {
                    Some(tokio::time::timeout(DEADLINE, receiver.recv()).await)
                } else {
                    None
                };
                drop(workspace);
                let shutdown = tokio::time::timeout(DEADLINE, fixture.agent.shutdown()).await;
                let writes = fixture.faults.write_commit_count();
                let injected = fixture
                    .faults
                    .state
                    .injected_timeouts
                    .load(Ordering::SeqCst);
                drop(receiver);

                blocked.expect("the selected close mutation reached its gate");
                shutdown
                    .expect("shutdown joins released close work within its budget")
                    .expect("shutdown cleans the remaining runtime");
                assert!(
                    joined_after_release.is_none(),
                    "the public close observer must settle before release"
                );
                let observation = observation.expect("public close observation stays bounded");
                if caller_loss {
                    assert!(observation.expect_err("aborted observer").is_cancelled());
                } else {
                    assert_eq!(
                        observation
                            .expect("close observer task")
                            .expect_err("held mutation outlives the observer")
                            .kind(),
                        ErrorKind::Timeout
                    );
                }
                assert_eq!(
                    held_writes, target,
                    "waiting cannot dispatch another mutation"
                );
                assert_eq!(injected, 1);
                if commit_first {
                    assert!(
                        drained
                            .expect("drain observation")
                            .expect("late close seals delivery")
                            .is_none()
                    );
                    assert_eq!(
                        writes, 4,
                        "two close mutations plus notification and conversation reclamation; no replay"
                    );
                } else {
                    assert_eq!(
                        writes, target,
                        "a late failed result must stop its original phase"
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn custom_close_retains_pending_interaction_cancellation_receipt() {
    use agent_sdk::error::ErrorKind;
    for fail_after_release in [false, true] {
        let fixture = CustomFixture::build_with_lifecycle_timeout(Duration::from_millis(300)).await;
        let workspace = fixture
            .agent
            .new_workspace(fixture.workspace_request("held-close-cancellation"))
            .await
            .expect("workspace");
        let conversation = workspace
            .new_conversation(CustomFixture::conversation_request())
            .await
            .expect("conversation");
        let mut receiver = conversation
            .notification_stream()
            .expect("notification receiver");
        // Pending interactions are read after close admission and the unchanged
        // cancellation continuation has settled its read transaction.
        fixture.faults.arm_held_load(3, fail_after_release);
        let mut close = tokio::spawn(async move { conversation.close().await });
        let blocked = tokio::time::timeout(DEADLINE, fixture.faults.wait_until_blocked()).await;
        let observation = tokio::time::timeout(DEADLINE, &mut close).await;
        let settled_before_release = tokio::time::timeout(
            Duration::from_millis(325),
            fixture.faults.wait_until_load_settled(),
        )
        .await;
        let held_transactions = fixture.faults.transaction_count();
        fixture.faults.clear();
        fixture.faults.release();
        let joined_after_release = if observation.is_err() {
            Some(tokio::time::timeout(DEADLINE, &mut close).await)
        } else {
            None
        };
        let drained = if fail_after_release {
            None
        } else {
            Some(tokio::time::timeout(DEADLINE, receiver.recv()).await)
        };
        drop(workspace);
        let shutdown = tokio::time::timeout(DEADLINE, fixture.agent.shutdown()).await;
        let writes = fixture.faults.write_commit_count();
        let settled_loads = fixture.faults.state.settled_loads.load(Ordering::SeqCst);
        drop(receiver);

        blocked.expect("pending interaction cancellation reaches its gate");
        shutdown
            .expect("shutdown remains bounded")
            .expect("shutdown joins cancellation");
        assert!(joined_after_release.is_none());
        assert!(
            settled_before_release.is_err(),
            "the original read remains held until release"
        );
        assert_eq!(
            observation
                .expect("close bounded")
                .expect("close task")
                .expect_err("held cancellation outlives observation")
                .kind(),
            ErrorKind::Timeout
        );
        assert_eq!(
            held_transactions, 3,
            "no later close mutation starts while cancellation is held"
        );
        assert_eq!(settled_loads, 1, "the admitted read settles once");
        if fail_after_release {
            assert_eq!(writes, 1, "late failed cancellation stops the phase");
        } else {
            assert!(
                drained
                    .expect("drain observation")
                    .expect("late successful cancellation must complete the original close")
                    .is_none()
            );
            assert_eq!(
                writes, 4,
                "two close mutations plus two receiver reclamations; no replay"
            );
        }
    }
}

#[tokio::test]
async fn custom_close_final_delete_timeout_remains_recoverable_without_replay() {
    use agent_sdk::error::{DeadlineKind, ErrorDetail, ErrorKind, ErrorPhase};
    let fixture = CustomFixture::build().await;
    let agent_id = fixture.agent.id();
    let workspace = fixture
        .agent
        .new_workspace(fixture.workspace_request("close-delete-timeout"))
        .await
        .expect("workspace");
    let workspace_id = workspace.id();
    let conversation = workspace
        .new_conversation(CustomFixture::conversation_request())
        .await
        .expect("conversation");
    let key = StateKey::new(
        "agent_sdk_conversations_v1",
        format!("{workspace_id}:{}", conversation.id()),
    )
    .expect("conversation key");
    fixture
        .faults
        .arm_delete(CommitFault::TimedOutBeforeCommit, key.clone());
    let result = tokio::time::timeout(DEADLINE, conversation.close()).await;
    let writes = fixture.faults.write_commit_count();
    let stored = fixture.faults.load(key).await;
    fixture.faults.clear();
    drop(workspace);
    let shutdown = tokio::time::timeout(DEADLINE, fixture.agent.shutdown()).await;
    let reopened = CustomFixture::reopen(&fixture.faults, agent_id).await;
    let workspace = reopened
        .get_workspace(workspace_id)
        .await
        .expect("reopened workspace");
    let recovered = workspace.list_conversations().await;
    let workspace_cleanup = workspace.close().await;
    let reopened_cleanup = reopened.shutdown().await;

    shutdown
        .expect("original shutdown bounded")
        .expect("original shutdown");
    workspace_cleanup.expect("reopened workspace cleanup");
    reopened_cleanup.expect("reopened Agent cleanup");
    let error = result
        .expect("close bounded")
        .expect_err("failed final deletion cannot claim success");
    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert_eq!(error.context().phase, ErrorPhase::StateCommit);
    assert_eq!(
        error.detail(),
        Some(&ErrorDetail::DeadlineElapsed {
            kind: DeadlineKind::Operation,
            timeout: Some(Duration::from_millis(41)),
        })
    );
    assert_eq!(writes, 3, "the final removal is attempted once");
    assert!(stored.expect("stored conversation readable").is_some());
    assert!(recovered.expect("recovered inventory").is_empty());
}

#[tokio::test]
async fn custom_close_retains_knowledge_cleanup_observation_without_restarting_deletion() {
    use agent_sdk::error::ErrorKind;
    for fail_after_release in [false, true] {
        let database = Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path(
            "memory://",
        )))
        .await
        .expect("knowledge database");
        let (knowledge, control) = KnowledgeDeleteProbe::new(database);
        let fixture = CustomFixture::build_with_knowledge_database(knowledge).await;
        let workspace = fixture
            .agent
            .new_workspace(fixture.workspace_request("held-close-knowledge"))
            .await
            .expect("workspace");
        let source = KnowledgeSource::text(
            "close-source",
            "Close source",
            "Retain knowledge cleanup ownership.",
        )
        .expect("knowledge source");
        let conversation = workspace
            .new_conversation(
                NewConversation::new(None, None, vec![source]).expect("conversation request"),
            )
            .await
            .expect("conversation");
        let conversation_key = StateKey::new(
            "agent_sdk_conversations_v1",
            format!("{}:{}", workspace.id(), conversation.id()),
        )
        .expect("conversation key");
        let obligation_key = StateKey::new(
            "agent_sdk_knowledge_forget_v1",
            format!(
                "{}:conversation:{}:{}",
                fixture.agent.id(),
                workspace.id(),
                conversation.id()
            ),
        )
        .expect("forget obligation key");
        control.arm();
        if fail_after_release {
            control.fail_after_release();
        }
        let mut close = tokio::spawn(async move { conversation.close().await });
        let blocked = tokio::time::timeout(DEADLINE, control.wait_until_blocked()).await;
        let admitted_transactions = fixture.faults.transaction_count();
        let obligation_before = fixture.faults.load(obligation_key.clone()).await;
        let conversation_before = fixture.faults.load(conversation_key.clone()).await;
        let observation = tokio::time::timeout(DEADLINE, &mut close).await;
        let outcome_before_release =
            tokio::time::timeout(Duration::from_millis(100), control.wait_for_outcome()).await;
        let held_transactions = fixture.faults.transaction_count();
        control.release();
        let joined_after_release = if observation.is_err() {
            Some(tokio::time::timeout(DEADLINE, &mut close).await)
        } else {
            None
        };
        drop(workspace);
        let shutdown = tokio::time::timeout(DEADLINE, fixture.agent.shutdown()).await;
        let obligation_after = fixture.faults.load(obligation_key).await;
        let conversation_after = fixture.faults.load(conversation_key).await;
        let calls = control.calls();
        let settled = control.gate.settled.load(Ordering::SeqCst);
        let interrupted = control.gate.interrupted.load(Ordering::SeqCst);

        blocked.expect("knowledge deletion reached its gate");
        shutdown
            .expect("shutdown remains bounded after knowledge release")
            .expect("shutdown joins the original knowledge operation");
        assert!(joined_after_release.is_none());
        assert_eq!(
            observation
                .expect("close observation bounded")
                .expect("close task")
                .expect_err("held knowledge cleanup outlives observation")
                .kind(),
            ErrorKind::Timeout
        );
        assert_eq!(
            held_transactions, admitted_transactions,
            "expiry must not restart the final state mutation while knowledge deletion is held"
        );
        assert!(
            outcome_before_release.is_err(),
            "accepted deletion must remain held until release"
        );
        assert_eq!(
            interrupted, 0,
            "accepted database deletion must not be dropped"
        );
        assert_eq!(settled, 1, "the original deletion reaches its late outcome");
        let obligation_before = obligation_before
            .expect("held obligation is readable")
            .expect("staged forget obligation exists");
        let conversation_before = conversation_before
            .expect("held conversation is readable")
            .expect("staged conversation exists");
        assert_eq!(
            obligation_after.expect("settled obligation is readable"),
            Some(obligation_before),
            "expiry preserves the durable forget obligation for recovery"
        );
        assert_eq!(
            conversation_after.expect("settled conversation is readable"),
            Some(conversation_before),
            "expiry cannot admit the final conversation removal"
        );
        assert_eq!(
            calls, 1,
            "the accepted knowledge deletion must never replay"
        );
    }
}

#[derive(Clone, Copy, Debug)]
enum KnowledgeCleanupFailure {
    Unavailable,
    TimedOut,
    Conflict,
    CommittedTimeout,
}

impl KnowledgeCleanupFailure {
    fn arm(self, store: &FaultStateStore, operation: CloseStorageOperation, invocation: usize) {
        match operation {
            CloseStorageOperation::Commit => store.arm(
                match self {
                    Self::Unavailable => CommitFault::UnavailableBeforeCommit,
                    Self::TimedOut => CommitFault::TimedOutBeforeCommit,
                    Self::Conflict => CommitFault::ConflictBeforeCommit,
                    Self::CommittedTimeout => CommitFault::TimedOutAfterCommit,
                },
                invocation,
            ),
            CloseStorageOperation::Begin
            | CloseStorageOperation::Load
            | CloseStorageOperation::Put
            | CloseStorageOperation::Delete
            | CloseStorageOperation::Rollback => store.arm_operation_fault(
                operation,
                OperationFaultTarget::Invocation(invocation),
                match self {
                    Self::Unavailable => StateStoreError::unavailable(
                        "private-knowledge-operation",
                        "private knowledge dependency failure",
                    ),
                    Self::TimedOut => StateStoreError::timed_out("private-knowledge-operation", 41),
                    Self::Conflict => StateStoreError::transaction_conflict(
                        "private-knowledge-operation",
                        "private knowledge conflict",
                    ),
                    Self::CommittedTimeout => panic!("committed fault requires commit"),
                },
            ),
        }
    }

    fn assert_error(self, error: &SdkError, operation: CloseStorageOperation) {
        use agent_sdk::error::{DeadlineKind, ErrorDetail, ErrorKind, ErrorPhase};
        assert_eq!(
            error.context().phase,
            match operation {
                CloseStorageOperation::Load => ErrorPhase::StateRead,
                CloseStorageOperation::Commit => ErrorPhase::StateCommit,
                CloseStorageOperation::Rollback => ErrorPhase::Cleanup,
                CloseStorageOperation::Begin
                | CloseStorageOperation::Put
                | CloseStorageOperation::Delete => ErrorPhase::StateWrite,
            },
            "failure={self:?}, method={operation:?}: {error:?}",
        );
        let (kind, detail) = match self {
            Self::Unavailable => (ErrorKind::Integration, ErrorDetail::ServiceUnavailable),
            Self::TimedOut | Self::CommittedTimeout => (
                ErrorKind::Timeout,
                ErrorDetail::DeadlineElapsed {
                    kind: DeadlineKind::Operation,
                    timeout: Some(Duration::from_millis(41)),
                },
            ),
            Self::Conflict => (ErrorKind::Conflict, ErrorDetail::TransactionConflict),
        };
        assert_eq!(error.kind(), kind);
        assert_eq!(error.detail(), Some(&detail));
        let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
        while let Some(error) = source {
            for rendered in [format!("{error}"), format!("{error:?}")] {
                for private in [
                    "private-knowledge-operation",
                    "private knowledge dependency failure",
                    "private knowledge conflict",
                    "private-close-timeout",
                    "injected unavailability",
                    "injected conflict",
                ] {
                    assert!(
                        !rendered.contains(private),
                        "lower error escaped: {rendered}"
                    );
                }
            }
            source = error.source();
        }
    }

    async fn sweep(self, operation: CloseStorageOperation) {
        // Each position is an independent fixture and one public close attempt.
        // Exhaust the selected public storage method without fixing its count,
        // transaction order, private namespace, or record encoding.
        let mut surfaced_after_delete = false;
        for invocation in 1..=64 {
            let database = Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path(
                "memory://",
            )))
            .await
            .expect("knowledge database");
            let (knowledge, control) = KnowledgeDeleteProbe::new(database.clone());
            let fixture = CustomFixture::build_with_knowledge_database(knowledge).await;
            let agent_id = fixture.agent.id();
            let workspace = fixture
                .agent
                .new_workspace(fixture.workspace_request("knowledge-error-phase"))
                .await
                .expect("workspace");
            let workspace_id = workspace.id();
            let conversation = workspace
                .new_conversation(
                    NewConversation::new(
                        None,
                        None,
                        vec![
                            KnowledgeSource::text(
                                "phase-source",
                                "Phase source",
                                "Knowledge deletion preserves its detecting storage phase.",
                            )
                            .expect("source"),
                        ],
                    )
                    .expect("conversation request"),
                )
                .await
                .expect("conversation");
            let conversation_id = conversation.id();
            self.arm(&fixture.faults, operation, invocation);
            control.arm();
            control.release();
            let result = tokio::time::timeout(DEADLINE, conversation.close()).await;
            let injected = fixture.faults.state.injected_faults.load(Ordering::SeqCst);
            let deletion_started = control.deletion_started();
            let deletions = control.calls();
            fixture.faults.clear();
            control.release();
            drop(workspace);
            let shutdown = tokio::time::timeout(DEADLINE, fixture.agent.shutdown()).await;

            // Recovery uses the same caller-owned storage/database domains. It
            // is not another faulted close attempt or an implicit SDK retry.
            let (knowledge, _) = KnowledgeDeleteProbe::new(database);
            let reopened = AgentBuilder::reopen(
                CustomFixture::<KnowledgeDeleteProbe>::config(Duration::from_secs(2)),
                AgentStorage::new(fixture.faults.clone()),
                agent_id,
            )
            .with_provider(CustomFixture::<KnowledgeDeleteProbe>::provider())
            .with_knowledge_database(knowledge)
            .build()
            .await;
            let recovery = match reopened {
                Ok(agent) => {
                    let recovery = async {
                        let workspace = agent.get_workspace(workspace_id).await?;
                        let observed = async {
                            let before = workspace.list_conversations().await?;
                            if before.iter().any(|info| info.id() == &conversation_id) {
                                workspace
                                    .get_conversation(conversation_id.clone())
                                    .await?
                                    .close()
                                    .await?;
                            }
                            let after = workspace.list_conversations().await?;
                            Ok::<_, SdkError>((before, after))
                        }
                        .await;
                        let cleanup = workspace.close().await;
                        match (observed, cleanup) {
                            (result, Ok(())) => result,
                            (Ok(_), Err(error)) => Err(error),
                            (Err(primary), Err(cleanup)) => {
                                Err(primary.with_supplementary(cleanup))
                            }
                        }
                    }
                    .await;
                    let cleanup = agent.shutdown().await;
                    match (recovery, cleanup) {
                        (result, Ok(())) => result,
                        (Ok(_), Err(error)) => Err(error),
                        (Err(primary), Err(cleanup)) => Err(primary.with_supplementary(cleanup)),
                    }
                }
                Err(error) => Err(error),
            };

            // Check results only after all owned fixtures have been cleaned up.
            let shutdown = shutdown.expect("shutdown bounded");
            let (before, after) = recovery.expect("unfaulted reopen reconciles cleanup");
            assert!(before.iter().all(|info| info.id() == &conversation_id));
            assert!(after.is_empty(), "recovery leaves no closed conversation");
            if deletion_started {
                assert!(
                    before.is_empty(),
                    "durable closing state must recover to deletion"
                );
            }
            assert!(
                deletions <= 1,
                "one close must not replay Database deletion"
            );
            let result = result.expect("close bounded");
            if let Err(error) = &result {
                assert_eq!(
                    injected, 1,
                    "a returned failure must match the injected fault"
                );
                assert_eq!(
                    error.context().operation,
                    agent_sdk::error::Operation::CloseConversation,
                );
                self.assert_error(error, operation);
                surfaced_after_delete |= deletion_started;
            }
            if let Err(error) = &shutdown {
                // Notification reclamation can retain the same dependency
                // failure for shutdown. Disarming the injector does not erase
                // that already-observed failure or require replaying cleanup.
                assert_eq!(injected, 1, "unfaulted shutdown must succeed");
                assert_eq!(
                    error.context().operation,
                    agent_sdk::error::Operation::Shutdown,
                );
                self.assert_error(error, operation);
            }
            if result.is_ok() {
                assert!(
                    injected == 0
                        || shutdown.is_err()
                        || matches!(self, Self::Conflict | Self::CommittedTimeout)
                        || operation == CloseStorageOperation::Commit,
                    "an injected non-conflict failure must be reported by its owning operation",
                );
                assert!(before.is_empty(), "confirmed close remains terminal");
            }
            if injected == 0 {
                result.expect("untriggered control closes successfully");
                assert!(
                    surfaced_after_delete,
                    "sweep must cover post-Database storage failures"
                );
                return;
            }
            assert_eq!(injected, 1, "each fixture injects one fault");
        }
        panic!("knowledge cleanup fault sweep exceeded its finite bound");
    }
}

#[tokio::test]
async fn custom_knowledge_cleanup_load_unavailability_keeps_read_phase() {
    KnowledgeCleanupFailure::Unavailable
        .sweep(CloseStorageOperation::Load)
        .await;
}

#[tokio::test]
async fn custom_knowledge_cleanup_commit_unavailability_keeps_commit_phase() {
    KnowledgeCleanupFailure::Unavailable
        .sweep(CloseStorageOperation::Commit)
        .await;
}

#[tokio::test]
async fn custom_knowledge_cleanup_timeouts_keep_phase_and_duration() {
    for operation in [
        CloseStorageOperation::Begin,
        CloseStorageOperation::Load,
        CloseStorageOperation::Delete,
        CloseStorageOperation::Commit,
    ] {
        KnowledgeCleanupFailure::TimedOut.sweep(operation).await;
    }
}

#[tokio::test]
async fn custom_knowledge_cleanup_conflicts_keep_transaction_detail() {
    for operation in [CloseStorageOperation::Load, CloseStorageOperation::Commit] {
        KnowledgeCleanupFailure::Conflict.sweep(operation).await;
    }
}

#[tokio::test]
async fn custom_knowledge_cleanup_ambiguous_commit_keeps_recovery_authority() {
    KnowledgeCleanupFailure::CommittedTimeout
        .sweep(CloseStorageOperation::Commit)
        .await;
}

#[tokio::test]
async fn custom_state_classifies_conflicts_and_reconciles_commit_observations() {
    let fixture = CustomFixture::build().await;
    let workspace = fixture
        .agent
        .new_workspace(fixture.workspace_request("state-contract-workspace"))
        .await
        .expect("fixture workspace must be created");
    let before = workspace.info().await.expect("workspace info must load");

    fixture.faults.arm(CommitFault::ConflictBeforeCommit, 1);
    let request = DeleteWorkspaceRequest::new(before.id().clone(), before.revision())
        .expect("delete request must be valid");
    assert!(
        matches!(fixture.agent.delete_workspace(request).await, Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::Conflict))
    );
    assert_eq!(fixture.faults.write_commit_count(), 1);
    assert_eq!(
        workspace.info().await.expect("workspace must survive"),
        before
    );

    fixture.faults.arm(CommitFault::UnavailableBeforeCommit, 1);
    let request = DeleteWorkspaceRequest::new(before.id().clone(), before.revision())
        .expect("delete request must be valid");
    assert!(
        matches!(fixture.agent.delete_workspace(request).await, Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::Integration))
    );
    assert_eq!(
        fixture.faults.write_commit_count(),
        1,
        "non-conflicts must not enter the conflict retry path"
    );
    assert_eq!(
        workspace.info().await.expect("workspace must survive"),
        before
    );

    fixture.faults.arm(CommitFault::ConflictBeforeCommit, 2);
    let conversation = tokio::time::timeout(
        DEADLINE,
        workspace.new_conversation(CustomFixture::conversation_request()),
    )
    .await
    .expect("conflict retry must remain bounded")
    .expect("a definite promotion conflict must be retried");
    assert_eq!(
        fixture.faults.write_commit_count(),
        3,
        "creation, conflicted promotion, and one retry are the only writes"
    );
    fixture.faults.clear();
    conversation
        .close()
        .await
        .expect("conflict fixture conversation must close");

    fixture.faults.arm(CommitFault::UnavailableAfterCommit, 2);
    let conversation = tokio::time::timeout(
        DEADLINE,
        workspace.new_conversation(CustomFixture::conversation_request()),
    )
    .await
    .expect("ambiguous commit reconciliation must remain bounded")
    .expect("the durably observed promoted value must win");
    assert_eq!(
        fixture.faults.write_commit_count(),
        2,
        "creation and the ambiguously successful promotion are the only writes"
    );
    fixture.faults.clear();
    conversation
        .close()
        .await
        .expect("reconciled fixture conversation must close");

    let info = workspace.info().await.expect("workspace info must reload");
    drop(workspace);
    fixture
        .agent
        .delete_workspace(
            DeleteWorkspaceRequest::new(info.id().clone(), info.revision())
                .expect("cleanup delete request must be valid"),
        )
        .await
        .expect("fixture workspace cleanup must succeed");
    fixture
        .agent
        .shutdown()
        .await
        .expect("fixture Agent must shut down");
}

#[tokio::test]
async fn custom_workspace_close_preserves_reopen_without_reviving_old_descendants() {
    let fixture = CustomFixture::build().await;
    let workspace = fixture
        .agent
        .new_workspace(fixture.workspace_request("workspace-durable-reopen"))
        .await
        .expect("fixture workspace must be created");
    let workspace_id = workspace.id();
    let conversation = workspace
        .new_conversation(CustomFixture::conversation_request())
        .await
        .expect("fixture conversation must be created");
    let conversation_id = conversation.id();

    workspace
        .close()
        .await
        .expect("workspace close must preserve durable descendants");
    assert!(
        matches!(conversation.info().await, Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::Closed))
    );

    let reopened_workspace = fixture
        .agent
        .get_workspace(workspace_id.clone())
        .await
        .expect("durable workspace must reopen in the same Agent runtime");
    reopened_workspace
        .info()
        .await
        .expect("the reopened workspace handle must own the new runtime epoch");
    let reopened_conversation = reopened_workspace
        .get_conversation(conversation_id)
        .await
        .expect("durable conversation must reopen under the reopened workspace");
    assert!(
        matches!(conversation.info().await, Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::Closed)),
        "the pre-close descendant must not revive after durable reopen"
    );

    reopened_conversation
        .close()
        .await
        .expect("reopened conversation must close");
    reopened_workspace
        .close()
        .await
        .expect("reopened workspace must close");
    let durable = fixture
        .agent
        .workspace_info(workspace_id.clone())
        .await
        .expect("closed workspace metadata must remain readable");
    fixture
        .agent
        .delete_workspace(
            DeleteWorkspaceRequest::new(workspace_id, durable.revision())
                .expect("durable workspace deletion must be valid"),
        )
        .await
        .expect("closed durable workspace must remain deletable");
    fixture
        .agent
        .shutdown()
        .await
        .expect("fixture Agent must shut down");
}

#[tokio::test]
async fn custom_receiver_holds_knowledge_until_notification_reclamation() {
    let database = Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path(
        "memory://",
    )))
    .await
    .expect("fixture Lance Database must build");
    let (knowledge, control) = KnowledgeDeleteProbe::new(database);
    let fixture = CustomFixture::build_with_knowledge_database(knowledge).await;
    let workspace = fixture
        .agent
        .new_workspace(fixture.workspace_request("knowledge-terminal-drain-workspace"))
        .await
        .expect("fixture workspace must be created");
    let workspace_id = workspace.id();
    let source = KnowledgeSource::text(
        "terminal-drain-source",
        "Terminal drain source",
        "Knowledge must remain durable until notification delivery is reclaimed.",
    )
    .expect("fixture knowledge source must be valid");
    let conversation = workspace
        .new_conversation(
            NewConversation::new(None, None, vec![source])
                .expect("knowledge fixture conversation request must be valid"),
        )
        .await
        .expect("knowledge fixture conversation must be created");
    let conversation_key = StateKey::new(
        "agent_sdk_conversations_v1",
        format!("{workspace_id}:{}", conversation.id()),
    )
    .expect("terminal conversation key must be valid");
    let receiver = conversation
        .notification_stream()
        .expect("fixture receiver must be acquired");

    control.arm();
    tokio::time::timeout(DEADLINE, conversation.close())
        .await
        .expect("conversation close must not wait for receiver-owned cleanup")
        .expect("fixture conversation must enter terminal drain");
    assert!(
        !control.deletion_started(),
        "knowledge deletion must not start while notification delivery is retained"
    );

    drop(receiver);
    tokio::time::timeout(DEADLINE, control.wait_until_blocked())
        .await
        .expect("receiver drop must hand off knowledge cleanup");
    assert!(
        fixture
            .faults
            .load(conversation_key.clone())
            .await
            .expect("terminal conversation record must remain readable")
            .is_some(),
        "conversation deletion must wait for knowledge settlement"
    );

    control.release();
    tokio::time::timeout(DEADLINE, async {
        loop {
            if fixture
                .faults
                .load(conversation_key.clone())
                .await
                .expect("terminal conversation record must remain readable")
                .is_none()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal conversation deletion must follow knowledge settlement");

    workspace
        .close()
        .await
        .expect("fixture workspace must close");
    let durable = fixture
        .agent
        .workspace_info(workspace_id.clone())
        .await
        .expect("closed workspace metadata must remain readable");
    fixture
        .agent
        .delete_workspace(
            DeleteWorkspaceRequest::new(workspace_id, durable.revision())
                .expect("workspace deletion must be valid"),
        )
        .await
        .expect("fixture workspace must delete");
    fixture
        .agent
        .shutdown()
        .await
        .expect("fixture Agent must shut down");
}

#[tokio::test]
async fn custom_held_receivers_start_terminal_cleanup_time_at_handoff() {
    let lifecycle_timeout = Duration::from_millis(200);
    let fixture = CustomFixture::build_with_lifecycle_timeout(lifecycle_timeout).await;
    let workspace = fixture
        .agent
        .new_workspace(fixture.workspace_request("terminal-drain-workspace"))
        .await
        .expect("fixture workspace must be created");
    let workspace_id = workspace.id();
    let dropped_conversation = workspace
        .new_conversation(CustomFixture::conversation_request())
        .await
        .expect("fixture conversation must be created");
    let dropped_receiver = dropped_conversation
        .notification_stream()
        .expect("fixture receiver must be acquired");
    let received_conversation = workspace
        .new_conversation(CustomFixture::conversation_request())
        .await
        .expect("second fixture conversation must be created");
    let mut received_receiver = received_conversation
        .notification_stream()
        .expect("second fixture receiver must be acquired");

    tokio::time::timeout(
        lifecycle_timeout + Duration::from_millis(100),
        pending::<()>(),
    )
    .await
    .expect_err("receiver hold must outlast the cleanup lifecycle budget");

    tokio::time::timeout(DEADLINE, dropped_conversation.close())
        .await
        .expect("conversation close must not wait for caller receipt")
        .expect("fixture conversation must close");
    drop(dropped_receiver);
    tokio::time::timeout(DEADLINE, received_conversation.close())
        .await
        .expect("second conversation close must not wait for caller receipt")
        .expect("second fixture conversation must close");
    assert!(
        tokio::time::timeout(DEADLINE, received_receiver.recv())
            .await
            .expect("terminal receive must remain bounded")
            .is_none(),
        "receiver lifetime must not consume its later cleanup deadline"
    );

    tokio::time::timeout(DEADLINE, async {
        loop {
            if workspace
                .list_conversations()
                .await
                .expect("conversation inventory must remain readable")
                .is_empty()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("receiver drop must drive terminal durable reclamation");

    workspace
        .close()
        .await
        .expect("fixture workspace must close");
    let durable = fixture
        .agent
        .workspace_info(workspace_id.clone())
        .await
        .expect("closed workspace metadata must remain readable");
    fixture
        .agent
        .delete_workspace(
            DeleteWorkspaceRequest::new(workspace_id, durable.revision())
                .expect("workspace deletion must be valid"),
        )
        .await
        .expect("fixture workspace must delete");
    fixture
        .agent
        .shutdown()
        .await
        .expect("fixture Agent must shut down");
}

#[tokio::test]
async fn custom_reopen_finishes_a_durably_staged_terminal_close() {
    let fixture = CustomFixture::build().await;
    let agent_id = fixture.agent.id();
    let workspace = fixture
        .agent
        .new_workspace(fixture.workspace_request("terminal-recovery-workspace"))
        .await
        .expect("fixture workspace must be created");
    let workspace_id = workspace.id();
    let conversation = workspace
        .new_conversation(CustomFixture::conversation_request())
        .await
        .expect("fixture conversation must be created");
    let conversation_key = StateKey::new(
        "agent_sdk_conversations_v1",
        format!("{workspace_id}:{}", conversation.id()),
    )
    .expect("terminal conversation key must be valid");

    fixture
        .faults
        .arm_delete(CommitFault::UnavailableBeforeCommit, conversation_key);
    let error = conversation
        .close()
        .await
        .expect_err("final terminal deletion must fail before commit");
    assert!(matches!(
        (error).kind(),
        agent_sdk::error::ErrorKind::Integration
    ));
    fixture.faults.clear();
    drop(workspace);

    let CustomFixture {
        faults,
        agent,
        root: _,
    } = fixture;
    agent
        .shutdown()
        .await
        .expect("fixture Agent must shut down with staged terminal state");
    let reopened = CustomFixture::reopen(&faults, agent_id).await;
    let reopened_workspace = reopened
        .get_workspace(workspace_id.clone())
        .await
        .expect("fixture workspace must reopen");
    assert!(
        reopened_workspace
            .list_conversations()
            .await
            .expect("reopened conversation inventory must load")
            .is_empty(),
        "Custom reopen must finish the staged terminal deletion"
    );
    reopened_workspace
        .close()
        .await
        .expect("reopened workspace must close");
    let durable = reopened
        .workspace_info(workspace_id.clone())
        .await
        .expect("closed workspace metadata must remain readable");
    reopened
        .delete_workspace(
            DeleteWorkspaceRequest::new(workspace_id, durable.revision())
                .expect("workspace deletion must be valid"),
        )
        .await
        .expect("fixture workspace must delete");
    reopened
        .shutdown()
        .await
        .expect("reopened Agent must shut down");
}

#[tokio::test]
async fn custom_shutdown_does_not_extend_a_terminal_cleanup_deadline() {
    let lifecycle_timeout = Duration::from_millis(500);
    let fixture = CustomFixture::build_with_lifecycle_timeout(lifecycle_timeout).await;
    let workspace = fixture
        .agent
        .new_workspace(fixture.workspace_request("terminal-shutdown-workspace"))
        .await
        .expect("fixture workspace must be created");
    let conversation = workspace
        .new_conversation(CustomFixture::conversation_request())
        .await
        .expect("fixture conversation must be created");
    let conversation_key = StateKey::new(
        "agent_sdk_conversations_v1",
        format!("{}:{}", workspace.id(), conversation.id()),
    )
    .expect("terminal conversation key must be valid");
    let receiver = conversation
        .notification_stream()
        .expect("fixture receiver must be acquired");
    conversation
        .close()
        .await
        .expect("fixture conversation must enter terminal drain");

    fixture
        .faults
        .arm_delete(CommitFault::BlockThenCommit, conversation_key.clone());
    drop(receiver);
    tokio::time::timeout(DEADLINE, fixture.faults.wait_until_blocked())
        .await
        .expect("terminal conversation deletion must reach its injected block");
    let writes_at_delete_block = fixture.faults.write_commit_count();
    tokio::time::timeout(lifecycle_timeout * 3 / 4, pending::<()>())
        .await
        .expect_err("terminal delete must consume most of its original cleanup budget");
    drop(workspace);

    let CustomFixture {
        faults,
        agent,
        root: _,
    } = fixture;
    let result = tokio::time::timeout(lifecycle_timeout * 3 / 4, agent.shutdown())
        .await
        .expect("shutdown must finish within the remaining original cleanup episode");
    assert!(
        matches!(result, Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::Timeout))
    );
    assert_eq!(
        faults.write_commit_count(),
        writes_at_delete_block,
        "shutdown must join the expired delete instead of redispatching it"
    );
    faults.release();
    assert!(
        faults
            .load(conversation_key)
            .await
            .expect("terminal conversation record must remain readable")
            .is_some(),
        "an aborted terminal delete must not commit after shutdown"
    );
}

#[tokio::test]
async fn custom_shutdown_settles_accepted_work_after_waiter_loss() {
    let fixture = CustomFixture::build().await;
    let workspace = fixture
        .agent
        .new_workspace(fixture.workspace_request("waiter-loss-workspace"))
        .await
        .expect("fixture workspace must be created");
    fixture.faults.arm(CommitFault::BlockThenCommit, 2);

    let mut creation = Box::pin(workspace.new_conversation(CustomFixture::conversation_request()));
    tokio::time::timeout(DEADLINE, async {
        tokio::select! {
            () = fixture.faults.wait_until_blocked() => {}
            _ = &mut creation => {
                panic!("accepted creation completed before its injected block");
            }
        }
    })
    .await
    .expect("promotion must block before caller loss");
    drop(creation);
    drop(workspace);

    let CustomFixture {
        faults,
        agent,
        root: _,
    } = fixture;
    let mut shutdown = Box::pin(agent.shutdown());
    let early = poll_fn(|context| match shutdown.as_mut().poll(context) {
        std::task::Poll::Ready(result) => std::task::Poll::Ready(Some(result)),
        std::task::Poll::Pending => std::task::Poll::Ready(None),
    })
    .await;
    assert!(
        early.is_none(),
        "shutdown must retain the accepted owner after waiter loss: {early:?}"
    );

    faults.release();
    tokio::time::timeout(DEADLINE, shutdown)
        .await
        .expect("shutdown must settle retained cleanup before its deadline")
        .expect("shutdown must reconcile accepted work");
}

#[test]
fn public_result_constructors_enforce_portable_invariants() {
    let agent_id: AgentId = "agent_00000000000000000000000000000011"
        .parse()
        .expect("canonical AgentId");
    let workspace_id: WorkspaceId = "workspace_00000000000000000000000000000012"
        .parse()
        .expect("canonical WorkspaceId");
    let conversation_id: ConversationId = "conversation_00000000000000000000000000000013"
        .parse()
        .expect("canonical ConversationId");
    let notification_id: NotificationId = "notification_00000000000000000000000000000014"
        .parse()
        .expect("canonical NotificationId");
    AgentInfo::new(agent_id.clone(), Some("Example agent".to_owned())).expect("valid AgentInfo");
    assert!(
        matches!(AgentInfo::new(agent_id, Some(String::new())), Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::InvalidInput))
    );

    let root = std::env::current_dir().expect("fixture cwd must be available");
    let context = WorkspaceContext::new(
        root.clone(),
        vec![WorkspaceRoot::new(root, WorkspaceRootAccess::ReadWrite).expect("valid root")],
        WorkspaceExecutionPolicy::new(
            WorkspaceSandboxMode::FullAccess,
            WorkspaceNetworkAccess::Disabled,
        ),
    )
    .expect("valid context");
    WorkspaceInfo::new(
        workspace_id.clone(),
        "Example workspace".to_owned(),
        context.clone(),
        WorkspaceDefaults::empty(),
        1,
    )
    .expect("valid WorkspaceInfo");
    assert!(matches!(WorkspaceInfo::new(
            workspace_id.clone(),
            "Example workspace".to_owned(),
            context,
            WorkspaceDefaults::empty(),
            0,
        ), Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::InvalidState)));
    let conversation = ConversationInfo::new(
        conversation_id,
        workspace_id,
        Some("Example conversation".to_owned()),
    )
    .expect("valid ConversationInfo");
    assert_eq!(conversation.title(), Some("Example conversation"));
    assert!(matches!(ConversationInfo::new(
            conversation.id().clone(),
            conversation.workspace_id().clone(),
            Some(" padded".to_owned()),
        ), Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::InvalidInput)));
    let inserted_at = SystemTime::now();
    ConversationNotification::new(
        notification_id.clone(),
        inserted_at,
        ConversationNotificationKind::Message {
            text: "exact message".to_owned(),
        },
    )
    .expect("valid notification");
    assert!(matches!(ConversationNotification::new(
            notification_id,
            inserted_at,
            ConversationNotificationKind::Message {
                text: " \n".to_owned(),
            },
        ), Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::InvalidInput)));
    let text = FileRead::from_bytes(PathBuf::from("notes.txt"), b"ready\n".to_vec())
        .expect("valid FileRead");
    assert_eq!(text.text(), Some("ready\n"));
    assert!(
        matches!(FileRead::from_bytes(PathBuf::from("../escape"), Vec::new()), Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::InvalidInput))
    );
}

#[test]
fn every_portable_error_category_has_a_public_safe_constructor() {
    let errors = [
        (SdkError::invalid_config("invalid config"), "invalid config"),
        (SdkError::invalid_input("invalid input"), "invalid input"),
        (SdkError::not_found("not found"), "not found"),
        (SdkError::conflict("conflict"), "conflict"),
        (SdkError::closed("closed"), "closed"),
        (SdkError::busy("busy"), "busy"),
        (SdkError::cancelled("cancelled"), "cancelled"),
        (SdkError::timeout("timeout"), "timeout"),
        (SdkError::unsupported("unsupported"), "unsupported"),
        (
            SdkError::not_yet_implemented("not yet implemented"),
            "not yet implemented",
        ),
        (SdkError::protocol("protocol"), "protocol"),
        (SdkError::invalid_state("invalid state"), "invalid state"),
        (SdkError::integration("integration"), "integration"),
    ];

    for (error, message) in errors {
        assert_eq!(error.message(), message);
    }
}
