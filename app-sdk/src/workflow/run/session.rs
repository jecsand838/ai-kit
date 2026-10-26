use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use agent_sdk::AgentBackend;
use futures_util::{FutureExt, Stream};
use serde_json::{Map, Value};
use storage_sdk::StateStoreBackend;
use tokio::sync::{OwnedSemaphorePermit, mpsc, oneshot};

use crate::AppError;
use crate::app::{AppRuntime, DriverCommand, FinalResult, RunTaskKind};
use crate::storage::StorageFailure;
use crate::workflow::action::ActionInputs;
use crate::workflow::definition::{JsonBoundary, Step};
use crate::workflow::execution::{
    BspDecision, ExecutionEngine, StagedActivationRecord, StagedOutcomeRecord,
};
use crate::workflow::run::{
    ActionPosition, CheckpointPhaseRecord, ChildAdmissionReceipt, ControlRecord,
    ControlRecordState, ControlTransition, FailureCategory, FrontierRecord, LoadedRun,
    MAX_DRIVER_TRANSITIONS, RECONCILIATION_INITIAL_DELAY, RECONCILIATION_MAX_DELAY, RunAdmission,
    RunRecord, RuntimeOperationFailure, ScheduleCause, WorkflowRunRuntime,
};
use crate::workflow::{
    ActionBackend, ActionContext, ActionEvent, ActionMode, ActionOutcome, ActionRequest,
    ChildBatch, ChildCompletion, ChildExecutor, ConditionBackend, ExecutionEvent, ExecutionOptions,
    InlineExecutor, JsonState, LifecycleEvent, LifecycleHooks, NoActions, NoConditions, NoHooks,
    RunSnapshot, RunStatus, StateTyping, WorkflowFailure, WorkflowRun, WorkflowState,
};

/// Exclusive lazy advancement whose caller source stays on the polling task.
///
/// Dropping an operation future preserves the source and any admitted write on
/// this owner. Dropping the owner stops source polling and retains tracked write
/// reconciliation before another execution can acquire the run.
///
/// Before native commit admission, owner drop discards uncommitted state and
/// revokes later advancement. After admission, App retains native commit authority
/// and reconciles the original receipt; observation loss cannot promise rollback.
/// This does not undo caller Action I/O. Prefer explicit finish/abort while the
/// runtime is driven; ordinary run and history observers never own cancellation.
///
/// # Example
///
/// ```
/// # use agent_sdk::Agent;
/// # use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
/// # use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
/// #     RunStatus, WorkflowDefinition, WorkflowState};
/// # use storage_sdk::StateStore;
/// # use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
/// #
/// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
/// # runtime.block_on(async {
/// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
/// # let definition = WorkflowDefinition::parse(r#"{
/// #     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
/// #     "state":{"count":0}, "required":["count"],
/// #     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
/// #     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
/// #         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
/// #     ]}}], "routes":{}
/// # }"#)?;
/// # let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
/// # let mut app = App::open(config).await?;
/// # let mut workspace = app.create_workspace(WorkspaceConfig {
/// #     name: Some("local counter".into()), root: std::env::current_dir()?,
/// # }).await?;
/// # let workflow = workspace.workflow("counter".parse()?).await?;
/// # let mut run = workflow.prepare(WorkflowState::default()).await?;
/// let mut execution = run.execution(ExecutionOptions::default()).await?;
/// while let Some(event) = execution.next().await? {
///     if let ExecutionEvent::Committed { outcomes, .. } = event {
///         assert_eq!(outcomes[0].node, "increment");
///     }
/// }
/// let snapshot = execution.finish().await?;
/// assert_eq!(snapshot.state.values()["count"], 1);
/// #
/// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[must_use = "execution owns lazy advancement; finish or abort it explicitly"]
pub struct Execution<
    'a,
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend = NoActions,
    C: ConditionBackend = NoConditions,
    T: StateTyping = JsonState,
    H: LifecycleHooks = NoHooks,
    E: ChildExecutor = InlineExecutor,
> {
    runtime: WorkflowRunRuntime<CS, A, X, C, T, H, E>,
    app: &'a AppRuntime<CS, A, X, C, T, H, E>,
    lane: ExecutionLane,
    admission: Option<OwnedSemaphorePermit>,
    children: Vec<ChildExecution<'a, ChildFuture<Self>, E>>,
    wake: Option<Pin<Box<tokio::sync::futures::OwnedNotified>>>,
    timer: Option<Pin<Box<tokio::time::Sleep>>>,
    generation: Option<u64>,
    parent_deadline: Option<tokio::time::Instant>,
    state: SessionState,
    sources: Vec<CallerSource<X>>,
    next_source: usize,
    native_partials: Option<(mpsc::Sender<ExecutionEvent>, mpsc::Receiver<ExecutionEvent>)>,
    prefetched: Option<(usize, Result<Option<ActionEvent>, FailureCategory>)>,
    transitions: usize,
    observed: Option<RunRecord>,
}

/// Blocking advancement of the same caller-owned execution source.
///
/// Calls require a thread outside any entered Tokio runtime context. The App's
/// captured runtime and caller-provided lower runtimes must remain alive and
/// driven elsewhere. Source polling and destruction remain on the caller thread;
/// dropping this owner delegates nonblocking revocation to [`Execution`].
///
/// # Example
///
/// ```
/// # use agent_sdk::Agent;
/// # use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
/// # use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
/// #     RunStatus, WorkflowDefinition, WorkflowState};
/// # use storage_sdk::StateStore;
/// # use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
/// # use app_sdk::workflow::SyncExecution;
/// let runtime = tokio::runtime::Builder::new_multi_thread()
///     .worker_threads(2).enable_all().build()?;
/// # let (mut app, mut run) = runtime.block_on(async {
/// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
/// # let definition = WorkflowDefinition::parse(r#"{
/// #     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
/// #     "state":{"count":0}, "required":["count"],
/// #     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
/// #     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
/// #         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
/// #     ]}}], "routes":{}
/// # }"#)?;
/// # let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
/// # let mut app = App::open(config).await?;
/// # let mut workspace = app.create_workspace(WorkspaceConfig {
/// #     name: Some("local counter".into()), root: std::env::current_dir()?,
/// # }).await?;
/// # let workflow = workspace.workflow("counter".parse()?).await?;
/// # let mut run = workflow.prepare(WorkflowState::default()).await?;
/// # Ok::<_, Box<dyn std::error::Error>>((app, run))
/// # })?;
/// // Runtime workers stay alive; this thread is outside any entered Tokio context.
/// let mut execution = SyncExecution::open(&mut run, ExecutionOptions::default())?;
/// while execution.try_next()?.is_some() {}
/// assert_eq!(execution.finish()?.state.values()["count"], 1);
/// # runtime.block_on(app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }))?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[must_use = "execution owns lazy advancement; finish or abort it explicitly"]
pub struct SyncExecution<
    'a,
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend = NoActions,
    C: ConditionBackend = NoConditions,
    T: StateTyping = JsonState,
    H: LifecycleHooks = NoHooks,
    E: ChildExecutor = InlineExecutor,
> {
    execution: Execution<'a, CS, A, X, C, T, H, E>,
    runtime: tokio::runtime::Handle,
}

impl<
    'a,
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend,
    C: ConditionBackend,
    T: StateTyping,
    H: LifecycleHooks,
    E: ChildExecutor,
> SyncExecution<'a, CS, A, X, C, T, H, E>
{
    /// Acquires lazy synchronous advancement on a thread outside Tokio.
    ///
    /// The App's captured runtime must remain alive and driven elsewhere.
    /// Acquisition validates options before callbacks or source construction.
    /// The returned owner borrows the run exclusively through its last use.
    ///
    /// A live synchronous owner prevents overlapping access to the run:
    ///
    /// ```compile_fail
    /// use agent_sdk::AgentBackend;
    /// use app_sdk::{AppError, workflow::{ExecutionOptions, RunSnapshot, SyncExecution, WorkflowRun}};
    /// use storage_sdk::StateStoreBackend;
    /// fn overlapping<CS: StateStoreBackend, A: AgentBackend>(
    /// run: &mut WorkflowRun<CS, A>,
    /// ) -> Result<RunSnapshot, AppError> {
    /// let execution = SyncExecution::open(run, ExecutionOptions::default())?;
    /// let _ = run.id();
    /// execution.abort()
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` inside an entered Tokio runtime context before blocking.
    /// Otherwise propagates the corresponding asynchronous execution errors, including
    /// stale/revoked authority, invalid options, durable failures and timeouts.
    /// The captured runtime and lower runtimes must stay alive and driven elsewhere.
    ///
    /// # Example
    ///
    /// ```
    /// # use agent_sdk::Agent;
    /// # use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_sdk::StateStore;
    /// # use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// # use app_sdk::workflow::SyncExecution;
    /// let runtime = tokio::runtime::Builder::new_multi_thread()
    ///     .worker_threads(2).enable_all().build()?;
    /// # let (mut app, mut run) = runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
    /// #     "state":{"count":0}, "required":["count"],
    /// #     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    /// #     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    /// #         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    /// #     ]}}], "routes":{}
    /// # }"#)?;
    /// # let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
    /// # let mut app = App::open(config).await?;
    /// # let mut workspace = app.create_workspace(WorkspaceConfig {
    /// #     name: Some("local counter".into()), root: std::env::current_dir()?,
    /// # }).await?;
    /// # let workflow = workspace.workflow("counter".parse()?).await?;
    /// # let mut run = workflow.prepare(WorkflowState::default()).await?;
    /// # Ok::<_, Box<dyn std::error::Error>>((app, run))
    /// # })?;
    /// // Runtime workers stay alive; this thread is outside any entered Tokio context.
    /// let execution = SyncExecution::open(&mut run, ExecutionOptions::default())?;
    /// assert_eq!(execution.finish()?.state.values()["count"], 1);
    /// # runtime.block_on(app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }))?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn open(
        run: &'a mut WorkflowRun<CS, A, X, C, T, H, E>,
        options: ExecutionOptions,
    ) -> Result<Self, AppError> {
        Self::ensure_blocking()?;
        let runtime = run.runtime.app.execution_handle()?;
        let execution = runtime.block_on(Execution::open(run, options, ActionMode::Synchronous))?;
        Ok(Self { execution, runtime })
    }

    fn ensure_blocking() -> Result<(), AppError> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(AppError::InvalidInput(
                "synchronous execution requires a thread outside an entered Tokio runtime".into(),
            ));
        }
        Ok(())
    }

    /// Blocks until one partial result or committed boundary is available.
    ///
    /// An entered runtime context is rejected before advancing this owner.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` inside an entered Tokio runtime context before blocking.
    /// Otherwise propagates the corresponding asynchronous execution errors, including
    /// stale/revoked authority, invalid options, durable failures and timeouts.
    /// The captured runtime and lower runtimes must stay alive and driven elsewhere.
    ///
    /// # Example
    ///
    /// ```
    /// # use agent_sdk::Agent;
    /// # use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_sdk::StateStore;
    /// # use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// # use app_sdk::workflow::SyncExecution;
    /// let runtime = tokio::runtime::Builder::new_multi_thread()
    ///     .worker_threads(2).enable_all().build()?;
    /// # let (mut app, mut run) = runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
    /// #     "state":{"count":0}, "required":["count"],
    /// #     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    /// #     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    /// #         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    /// #     ]}}], "routes":{}
    /// # }"#)?;
    /// # let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
    /// # let mut app = App::open(config).await?;
    /// # let mut workspace = app.create_workspace(WorkspaceConfig {
    /// #     name: Some("local counter".into()), root: std::env::current_dir()?,
    /// # }).await?;
    /// # let workflow = workspace.workflow("counter".parse()?).await?;
    /// # let mut run = workflow.prepare(WorkflowState::default()).await?;
    /// # Ok::<_, Box<dyn std::error::Error>>((app, run))
    /// # })?;
    /// // Runtime workers stay alive; this thread is outside any entered Tokio context.
    /// let mut execution = SyncExecution::open(&mut run, ExecutionOptions::default())?;
    /// assert!(matches!(execution.try_next()?, Some(ExecutionEvent::Committed { .. })));
    /// execution.finish()?;
    /// # runtime.block_on(app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }))?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn try_next(&mut self) -> Result<Option<ExecutionEvent>, AppError> {
        Self::ensure_blocking()?;
        self.runtime.block_on(self.execution.next())
    }

    /// Drains partial results and blocks until one committed boundary.
    ///
    /// An entered runtime context is rejected before advancing this owner.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` inside an entered Tokio runtime context before blocking.
    /// Otherwise propagates the corresponding asynchronous execution errors, including
    /// stale/revoked authority, invalid options, durable failures and timeouts.
    /// The captured runtime and lower runtimes must stay alive and driven elsewhere.
    ///
    /// # Example
    ///
    /// ```
    /// # use agent_sdk::Agent;
    /// # use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_sdk::StateStore;
    /// # use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// # use app_sdk::workflow::SyncExecution;
    /// let runtime = tokio::runtime::Builder::new_multi_thread()
    ///     .worker_threads(2).enable_all().build()?;
    /// # let (mut app, mut run) = runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
    /// #     "state":{"count":0}, "required":["count"],
    /// #     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    /// #     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    /// #         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    /// #     ]}}], "routes":{}
    /// # }"#)?;
    /// # let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
    /// # let mut app = App::open(config).await?;
    /// # let mut workspace = app.create_workspace(WorkspaceConfig {
    /// #     name: Some("local counter".into()), root: std::env::current_dir()?,
    /// # }).await?;
    /// # let workflow = workspace.workflow("counter".parse()?).await?;
    /// # let mut run = workflow.prepare(WorkflowState::default()).await?;
    /// # Ok::<_, Box<dyn std::error::Error>>((app, run))
    /// # })?;
    /// // Runtime workers stay alive; this thread is outside any entered Tokio context.
    /// let mut execution = SyncExecution::open(&mut run, ExecutionOptions::default())?;
    /// assert!(matches!(execution.step()?, Some(ExecutionEvent::Committed { .. })));
    /// assert_eq!(execution.finish()?.state.values()["count"], 1);
    /// # runtime.block_on(app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }))?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn step(&mut self) -> Result<Option<ExecutionEvent>, AppError> {
        Self::ensure_blocking()?;
        self.runtime.block_on(self.execution.step())
    }

    /// Drains execution to a halt or exhaustion and finalizes its observation.
    ///
    /// Rejection inside an entered runtime drops this consumed owner; existing
    /// execution cleanup retains any previously admitted publication.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` inside an entered Tokio runtime context before blocking.
    /// Otherwise propagates the corresponding asynchronous execution errors, including
    /// stale/revoked authority, invalid options, durable failures and timeouts.
    /// The captured runtime and lower runtimes must stay alive and driven elsewhere.
    ///
    /// # Example
    ///
    /// ```
    /// # use agent_sdk::Agent;
    /// # use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_sdk::StateStore;
    /// # use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// # use app_sdk::workflow::SyncExecution;
    /// let runtime = tokio::runtime::Builder::new_multi_thread()
    ///     .worker_threads(2).enable_all().build()?;
    /// # let (mut app, mut run) = runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
    /// #     "state":{"count":0}, "required":["count"],
    /// #     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    /// #     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    /// #         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    /// #     ]}}], "routes":{}
    /// # }"#)?;
    /// # let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
    /// # let mut app = App::open(config).await?;
    /// # let mut workspace = app.create_workspace(WorkspaceConfig {
    /// #     name: Some("local counter".into()), root: std::env::current_dir()?,
    /// # }).await?;
    /// # let workflow = workspace.workflow("counter".parse()?).await?;
    /// # let mut run = workflow.prepare(WorkflowState::default()).await?;
    /// # Ok::<_, Box<dyn std::error::Error>>((app, run))
    /// # })?;
    /// // Runtime workers stay alive; this thread is outside any entered Tokio context.
    /// let execution = SyncExecution::open(&mut run, ExecutionOptions::default())?;
    /// let snapshot = execution.finish()?;
    /// assert!(matches!(snapshot.status, RunStatus::Succeeded));
    /// # runtime.block_on(app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }))?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn finish(self) -> Result<RunSnapshot, AppError> {
        Self::ensure_blocking()?;
        self.runtime.block_on(self.execution.finish())
    }

    /// Finalizes the current uncommitted boundary without cancelling the run.
    ///
    /// Rejection inside an entered runtime drops this consumed owner; it does
    /// not dispatch a new operation or discard accepted publication ownership.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` inside an entered Tokio runtime context before blocking.
    /// Otherwise propagates the corresponding asynchronous execution errors, including
    /// stale/revoked authority, invalid options, durable failures and timeouts.
    /// The captured runtime and lower runtimes must stay alive and driven elsewhere.
    ///
    /// # Example
    ///
    /// ```
    /// # use agent_sdk::Agent;
    /// # use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_sdk::StateStore;
    /// # use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// # use app_sdk::workflow::SyncExecution;
    /// let runtime = tokio::runtime::Builder::new_multi_thread()
    ///     .worker_threads(2).enable_all().build()?;
    /// # let (mut app, mut run) = runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
    /// #     "state":{"count":0}, "required":["count"],
    /// #     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    /// #     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    /// #         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    /// #     ]}}], "routes":{}
    /// # }"#)?;
    /// # let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
    /// # let mut app = App::open(config).await?;
    /// # let mut workspace = app.create_workspace(WorkspaceConfig {
    /// #     name: Some("local counter".into()), root: std::env::current_dir()?,
    /// # }).await?;
    /// # let workflow = workspace.workflow("counter".parse()?).await?;
    /// # let mut run = workflow.prepare(WorkflowState::default()).await?;
    /// # Ok::<_, Box<dyn std::error::Error>>((app, run))
    /// # })?;
    /// // Runtime workers stay alive; this thread is outside any entered Tokio context.
    /// let execution = SyncExecution::open(&mut run, ExecutionOptions::default())?;
    /// let parked = execution.abort()?;
    /// assert_eq!(parked.state.values()["count"], 0);
    /// assert_eq!(SyncExecution::open(&mut run, ExecutionOptions::default())?.finish()?.state.values()["count"], 1);
    /// # runtime.block_on(app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }))?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn abort(self) -> Result<RunSnapshot, AppError> {
        Self::ensure_blocking()?;
        self.runtime.block_on(self.execution.abort())
    }
}

struct CallerSource<X: ActionBackend> {
    activation: FrontierRecord,
    context: Arc<ActionContext>,
    deadline: tokio::time::Instant,
    state: CallerSourceState<X>,
}

enum CallerSourceState<X: ActionBackend> {
    Polling(Pin<Box<X::Stream>>),
    Failed(FailureCategory),
    Ended,
    Eof,
}

impl<X: ActionBackend> CallerSource<X> {
    fn stop(&mut self) -> Result<(), FailureCategory> {
        let previous = std::mem::replace(&mut self.state, CallerSourceState::Ended);
        match previous {
            CallerSourceState::Polling(source) => catch_unwind(AssertUnwindSafe(|| drop(source)))
                .map_err(|_| FailureCategory::Execution),
            previous => {
                self.state = previous;
                Ok(())
            }
        }
    }

    fn poll(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<Option<ActionEvent>, FailureCategory>> {
        let event = match &mut self.state {
            CallerSourceState::Polling(source) => {
                match catch_unwind(AssertUnwindSafe(|| source.as_mut().poll_next(context))) {
                    Ok(Poll::Pending) => return Poll::Pending,
                    Ok(Poll::Ready(Some(Ok(event)))) => Ok(Some(event)),
                    Ok(Poll::Ready(Some(Err(_)))) => Err(FailureCategory::Action),
                    Ok(Poll::Ready(None)) => Ok(None),
                    Err(_) => Err(FailureCategory::Execution),
                }
            }
            CallerSourceState::Failed(category) => {
                let category = *category;
                self.state = CallerSourceState::Ended;
                return Poll::Ready(Err(category));
            }
            CallerSourceState::Ended | CallerSourceState::Eof => return Poll::Pending,
        };
        Poll::Ready(event)
    }
}

#[derive(Clone, Copy, PartialEq)]
enum ExecutionLane {
    Caller,
    Worker,
}

struct ChildExecution<'a, F, E>
where
    F: Future<Output = Result<ChildCompletion, AppError>> + 'a,
    E: ChildExecutor,
{
    sequence: u64,
    parent: Option<Arc<RunRecord>>,
    context: Arc<ActionContext>,
    lineage: String,
    deadline: tokio::time::Instant,
    state: ChildExecutionState<'a, F, E>,
    inputs: Vec<Map<String, Value>>,
}

enum ChildExecutionState<'a, F, E>
where
    F: Future<Output = Result<ChildCompletion, AppError>> + 'a,
    E: ChildExecutor,
{
    Awaiting,
    Prepared(Vec<RunRecord>),
    Local(Pin<Box<E::Execution<'a, F>>>),
    Pipeline(ChildBatch<F>),
    Worker(oneshot::Receiver<Result<SessionReceipt, RuntimeOperationFailure>>),
    Receipt(Box<Result<SessionReceipt, RuntimeOperationFailure>>),
    Complete(Result<Vec<ChildCompletion>, FailureCategory>),
    Closed,
}

impl<
    'a,
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend,
    C: ConditionBackend,
    T: StateTyping,
    H: LifecycleHooks,
    E: ChildExecutor,
> ChildExecution<'a, ChildFuture<Execution<'a, CS, A, X, C, T, H, E>>, E>
{
    fn start_local(
        &mut self,
        runtime: &WorkflowRunRuntime<CS, A, X, C, T, H, E>,
        app: &'a AppRuntime<CS, A, X, C, T, H, E>,
        lane: ExecutionLane,
        children: Vec<RunRecord>,
    ) -> Result<(), AppError> {
        if tokio::time::Instant::now() >= self.deadline {
            runtime.retain_source_failure(&self.context, &self.lineage, FailureCategory::Timeout);
            self.state = ChildExecutionState::Complete(Err(FailureCategory::Timeout));
            return Ok(());
        }
        let parent = self.parent.as_ref().ok_or_else(|| {
            AppError::Integration("child group parent evidence is unavailable".into())
        })?;
        let definition = app.definition_for(parent)?;
        let control = parent
            .checkpoint
            .control(self.sequence)
            .ok_or_else(|| AppError::Conflict("child control is unavailable".into()))?;
        let step = definition
            .node(&control.node)
            .ok_or_else(|| AppError::Conflict("child definition changed".into()))?
            .step();
        let (parallelism, _) = step
            .child_plan()
            .ok_or_else(|| AppError::Conflict("child definition changed".into()))?;
        let slots = Arc::new(tokio::sync::Semaphore::new(parallelism));
        let notification = runtime.notifier()?;
        let mut tasks = Vec::with_capacity(children.len());
        for record in children {
            record.validate_child(parent, &definition)?;
            let child = WorkflowRunRuntime::attach_record(Arc::clone(&runtime.app), &record)?;
            if !record.checkpoint.is_terminal() {
                child.cell.bind_child(runtime.key.clone())?;
                child
                    .cell
                    .retain_child_observation(Arc::clone(&self.context))?;
            }
            tasks.push(ChildFuture::new(
                Execution::owned(child, app, lane),
                Arc::clone(parent),
                self.sequence,
                self.deadline,
                Arc::clone(&slots),
                Arc::clone(&notification),
            ));
        }
        let batch = ChildBatch::new(tasks, self.context.mode)?;
        self.state = if matches!(step, Step::Pipeline { .. }) {
            ChildExecutionState::Pipeline(batch)
        } else {
            match catch_unwind(AssertUnwindSafe(|| app.child_executor().execute(batch))) {
                Ok(future) => ChildExecutionState::Local(Box::pin(future)),
                Err(_) => {
                    runtime.retain_source_failure(
                        &self.context,
                        &self.lineage,
                        FailureCategory::Execution,
                    );
                    ChildExecutionState::Complete(Err(FailureCategory::Execution))
                }
            }
        };
        Ok(())
    }

    fn stop(&mut self, runtime: &WorkflowRunRuntime<CS, A, X, C, T, H, E>) {
        let previous = std::mem::replace(&mut self.state, ChildExecutionState::Closed);
        if let ChildExecutionState::Complete(Err(category)) = &previous {
            runtime.retain_source_failure(&self.context, &self.lineage, *category);
        }
        if catch_unwind(AssertUnwindSafe(|| drop(previous))).is_err() {
            // An already retained source/E error remains primary in the
            // original observation. This only supplies a destructor fallback.
            runtime.retain_source_failure(&self.context, &self.lineage, FailureCategory::Execution);
        }
    }

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        runtime: &WorkflowRunRuntime<CS, A, X, C, T, H, E>,
    ) -> Poll<()> {
        if matches!(
            self.state,
            ChildExecutionState::Complete(_)
                | ChildExecutionState::Receipt(_)
                | ChildExecutionState::Closed
        ) {
            return Poll::Ready(());
        }
        if matches!(
            self.state,
            ChildExecutionState::Awaiting | ChildExecutionState::Prepared(_)
        ) {
            return Poll::Pending;
        }
        if matches!(self.state, ChildExecutionState::Worker(_)) {
            return self.poll_receipt(cx);
        }
        let result = if tokio::time::Instant::now() >= self.deadline {
            Err(FailureCategory::Timeout)
        } else {
            match catch_unwind(AssertUnwindSafe(|| match &mut self.state {
                ChildExecutionState::Local(future) => future.as_mut().poll(cx),
                ChildExecutionState::Pipeline(batch) => Pin::new(batch).poll(cx),
                _ => unreachable!(),
            })) {
                Ok(Poll::Pending) => return Poll::Pending,
                Ok(Poll::Ready(result)) => result.map_err(|_| FailureCategory::Child),
                Err(_) => Err(FailureCategory::Execution),
            }
        };
        self.complete(runtime, result);
        Poll::Ready(())
    }

    fn poll_receipt(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let ChildExecutionState::Worker(receipt) = &mut self.state else {
            unreachable!()
        };
        match Pin::new(receipt).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                self.state = ChildExecutionState::Receipt(Box::new(
                    result
                        .map_err(|_| {
                            RuntimeOperationFailure::from(AppError::Closed(
                                "child executor receipt closed".into(),
                            ))
                        })
                        .and_then(|result| result),
                ));
                Poll::Ready(())
            }
        }
    }

    fn complete(
        &mut self,
        runtime: &WorkflowRunRuntime<CS, A, X, C, T, H, E>,
        result: Result<Vec<ChildCompletion>, FailureCategory>,
    ) {
        if let Err(category) = result.as_ref() {
            runtime.retain_source_failure(&self.context, &self.lineage, *category);
        }
        let previous = std::mem::replace(&mut self.state, ChildExecutionState::Complete(result));
        if catch_unwind(AssertUnwindSafe(|| drop(previous))).is_err()
            && matches!(&self.state, ChildExecutionState::Complete(Ok(_)))
        {
            runtime.retain_source_failure(&self.context, &self.lineage, FailureCategory::Execution);
            self.state = ChildExecutionState::Complete(Err(FailureCategory::Execution));
        }
    }
}

struct ChildFuture<EX> {
    execution: EX,
    parent: Arc<RunRecord>,
    sequence: u64,
    slots: Arc<tokio::sync::Semaphore>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    notification: Arc<tokio::sync::Notify>,
    wake: Pin<Box<tokio::sync::futures::OwnedNotified>>,
    state: ChildFutureState,
}

enum ChildFutureState {
    Admission,
    Driving,
    Reconcile(Option<oneshot::Receiver<Result<LoadedRun, AppError>>>),
    Retry(tokio::time::Instant),
    Finishing,
    Waiting,
    Finished,
}

impl<
    'a,
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend,
    C: ConditionBackend,
    T: StateTyping,
    H: LifecycleHooks,
    E: ChildExecutor,
> ChildFuture<Execution<'a, CS, A, X, C, T, H, E>>
{
    fn new(
        mut execution: Execution<'a, CS, A, X, C, T, H, E>,
        parent: Arc<RunRecord>,
        sequence: u64,
        deadline: tokio::time::Instant,
        slots: Arc<tokio::sync::Semaphore>,
        notification: Arc<tokio::sync::Notify>,
    ) -> Self {
        let wake = Box::pin(Arc::clone(&notification).notified_owned());
        execution.parent_deadline = Some(deadline);
        Self {
            execution,
            parent,
            sequence,
            slots,
            permit: None,
            notification,
            wake,
            state: ChildFutureState::Admission,
        }
    }

    fn poll_reconciliation(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), AppError>> {
        let ChildFutureState::Reconcile(receipt) = &mut self.state else {
            unreachable!()
        };
        if receipt.is_none() {
            // Finish an already admitted publication before sampling
            // failure authority. Its older result must not restart
            // caller work after the panic is reconciled.
            match &mut self.execution.state {
                SessionState::Publishing { receipt, .. } => {
                    if Pin::new(receipt).poll(cx).is_pending() {
                        return Poll::Pending;
                    }
                }
                SessionState::ObservingItem { receipt, .. } => {
                    if Pin::new(receipt).poll(cx).is_pending() {
                        return Poll::Pending;
                    }
                }
                _ => {}
            }
            self.execution.state = SessionState::Finished;
            *receipt = Some(
                self.execution
                    .runtime
                    .reconcile_child_panic(self.execution.parent_deadline),
            );
        }
        let loaded = match Pin::new(receipt.as_mut().expect("reconciliation was admitted")).poll(cx)
        {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Ok(result)) => result?,
            Poll::Ready(Err(_)) => {
                self.execution
                    .runtime
                    .app
                    .tasks_failed(FinalResult::Integration(
                        "child panic reconciliation receipt closed".into(),
                    ));
                return Poll::Ready(Err(AppError::Integration(
                    "child panic reconciliation receipt closed".into(),
                )));
            }
        };
        self.execution.state = SessionState::Loaded(loaded);
        self.state = ChildFutureState::Driving;
        Poll::Ready(Ok(()))
    }

    fn poll_completion(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<ChildCompletion>, AppError>> {
        match self.execution.poll_publication(cx, false) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(Some(_))) => Poll::Ready(Ok(None)),
            Poll::Ready(Ok(None)) => {
                let SessionState::Loaded(loaded) =
                    std::mem::replace(&mut self.execution.state, SessionState::Finished)
                else {
                    return Poll::Ready(Err(AppError::Integration(
                        "child terminal observation is unavailable".into(),
                    )));
                };
                if !loaded.record.checkpoint.is_terminal() {
                    self.execution.state = SessionState::Loaded(loaded);
                    self.state = ChildFutureState::Waiting;
                    return Poll::Ready(Ok(None));
                }
                let definition = self.execution.runtime.app.definition_for(&self.parent)?;
                let result = if matches!(
                    loaded.record.checkpoint.phase,
                    CheckpointPhaseRecord::Succeeded
                ) {
                    ChildCompletion::admitted(loaded.record, &self.parent, &definition)
                } else {
                    Err(AppError::Integration(
                        "child workflow did not succeed".into(),
                    ))
                };
                Poll::Ready(result.map(Some))
            }
        }
    }
}

impl<
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend,
    C: ConditionBackend,
    T: StateTyping,
    H: LifecycleHooks,
    E: ChildExecutor,
> Future for ChildFuture<Execution<'_, CS, A, X, C, T, H, E>>
{
    type Output = Result<ChildCompletion, AppError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let child = self.get_mut();
        let observed = (|| -> Poll<Result<ChildCompletion, AppError>> {
            loop {
                match child.state {
                    ChildFutureState::Admission => {
                        child.wake.as_mut().enable();
                        match Arc::clone(&child.slots).try_acquire_owned() {
                            Ok(permit) => {
                                child.permit = Some(permit);
                                child.state = ChildFutureState::Driving;
                            }
                            Err(tokio::sync::TryAcquireError::NoPermits) => {
                                if child.wake.as_mut().poll(cx).is_pending() {
                                    return Poll::Pending;
                                }
                                child.wake =
                                    Box::pin(Arc::clone(&child.notification).notified_owned());
                            }
                            Err(tokio::sync::TryAcquireError::Closed) => {
                                return Poll::Ready(Err(AppError::Closed(
                                    "child execution capacity is closed".into(),
                                )));
                            }
                        }
                    }
                    ChildFutureState::Reconcile(_) => match child.poll_reconciliation(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(result) => result?,
                    },
                    ChildFutureState::Retry(deadline) => {
                        if !child.execution.poll_timer(cx, Some(deadline)) {
                            return Poll::Pending;
                        }
                        child.execution.state = SessionState::Ready;
                        child.state = ChildFutureState::Driving;
                    }
                    ChildFutureState::Driving => {
                        let observed = catch_unwind(AssertUnwindSafe(|| {
                            child.execution.poll_advance(cx, true)
                        }));
                        match observed {
                            Err(_) => {
                                // Unrelated wakes cannot re-admit this child before
                                // its original failure publication is authoritative.
                                child.state = ChildFutureState::Reconcile(None);
                            }
                            Ok(Poll::Pending) => return Poll::Pending,
                            Ok(Poll::Ready(Err(error))) => {
                                if let Some(delay) = child.execution.runtime.cell.complete_drive() {
                                    child.state = ChildFutureState::Retry(
                                        tokio::time::Instant::now() + delay,
                                    );
                                } else {
                                    return Poll::Ready(Err(error));
                                }
                            }
                            Ok(Poll::Ready(Ok(Some(_)))) => continue,
                            Ok(Poll::Ready(Ok(None))) => {
                                child.execution.publish(
                                    SessionPublication::FinishChild {
                                        parent: Arc::clone(&child.parent),
                                        sequence: child.sequence,
                                    },
                                    false,
                                )?;
                                child.state = ChildFutureState::Finishing;
                            }
                        }
                    }
                    ChildFutureState::Finishing => match child.poll_completion(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                        Poll::Ready(Ok(Some(completion))) => return Poll::Ready(Ok(completion)),
                        Poll::Ready(Ok(None)) => continue,
                    },
                    ChildFutureState::Waiting => {
                        // A child's public Input can pause its lifecycle, but the
                        // same admitted batch future remains owned by its parent.
                        if !child.execution.poll_notification(cx)? {
                            return Poll::Pending;
                        }
                        child.execution.state = SessionState::Ready;
                        child.state = ChildFutureState::Driving;
                    }
                    ChildFutureState::Finished => {
                        return Poll::Ready(Err(AppError::Conflict(
                            "child future already completed".into(),
                        )));
                    }
                }
            }
        })();
        if observed.is_ready() {
            child.release();
            child.state = ChildFutureState::Finished;
        }
        observed
    }
}

impl<EX> ChildFuture<EX> {
    fn release(&mut self) {
        if self.permit.take().is_some() {
            self.notification.notify_waiters();
        }
    }
}

impl<EX> Drop for ChildFuture<EX> {
    fn drop(&mut self) {
        self.release();
    }
}

enum SessionState {
    Ready,
    Loaded(LoadedRun),
    ActivationReady {
        loaded: LoadedRun,
        activation: FrontierRecord,
        context: Arc<ActionContext>,
        remaining: std::vec::IntoIter<FrontierRecord>,
        staged: Vec<StagedActivationRecord>,
        deadline: tokio::time::Instant,
        deadline_millis: u64,
    },
    Sources {
        loaded: LoadedRun,
    },
    ObservingItem {
        loaded: LoadedRun,
        index: usize,
        receipt: oneshot::Receiver<Result<Option<Map<String, Value>>, AppError>>,
    },
    Publishing {
        receipt: oneshot::Receiver<Result<SessionReceipt, RuntimeOperationFailure>>,
        purpose: PublicationPurpose,
    },
    Eof,
    Finished,
}

enum PublicationPurpose {
    Load,
    Reload,
    ObserveChildren,
    AdmitChildren,
    VerifyChildren,
    Start {
        activation: FrontierRecord,
        context: Arc<ActionContext>,
        remaining: std::vec::IntoIter<FrontierRecord>,
        deadline: tokio::time::Instant,
        deadline_millis: u64,
    },
    Continue {
        remaining: std::vec::IntoIter<FrontierRecord>,
        ordinary: bool,
    },
    Descendants {
        context: Arc<ActionContext>,
        lineage: String,
        inputs: Vec<Map<String, Value>>,
        remaining: std::vec::IntoIter<FrontierRecord>,
        ordinary: bool,
    },
    Commit {
        ordinary: bool,
    },
    Advance,
    EndSource,
}

pub(crate) enum SessionReceipt {
    Checkpoint {
        loaded: LoadedRun,
        staged: Vec<StagedActivationRecord>,
    },
    Children {
        loaded: LoadedRun,
        sequence: u64,
        children: Vec<RunRecord>,
        verified: bool,
    },
}

impl SessionReceipt {
    fn record(&self) -> &RunRecord {
        match self {
            Self::Checkpoint { loaded, .. } | Self::Children { loaded, .. } => &loaded.record,
        }
    }

    fn with_loaded(self, loaded: LoadedRun) -> Self {
        match self {
            Self::Checkpoint { staged, .. } => Self::Checkpoint { loaded, staged },
            Self::Children {
                sequence,
                children,
                verified,
                ..
            } => Self::Children {
                loaded,
                sequence,
                children,
                verified,
            },
        }
    }
}

enum SessionPublication {
    ObserveChildren {
        loaded: LoadedRun,
        sequence: u64,
    },
    VerifyChildren {
        parent: Arc<RunRecord>,
        sequence: u64,
        completions: Vec<ChildCompletion>,
    },
    ChildOutcome {
        control: ControlRecord,
        transition: ControlTransition,
    },
    FinishChild {
        parent: Arc<RunRecord>,
        sequence: u64,
    },
    SettleStopping {
        loaded: LoadedRun,
    },
    Load {
        terminal: Option<u64>,
    },
    Reload {
        receipt: SessionReceipt,
        deadline: Option<tokio::time::Instant>,
    },
    BeginActivation {
        loaded: LoadedRun,
        activation: FrontierRecord,
        context: Arc<ActionContext>,
        remaining: std::vec::IntoIter<FrontierRecord>,
        staged: Vec<StagedActivationRecord>,
        deadline: tokio::time::Instant,
        deadline_millis: u64,
    },
    Decision {
        loaded: LoadedRun,
        decision: BspDecision,
        activation_deadline: Option<u64>,
        context: Option<Arc<ActionContext>>,
        staged: Vec<StagedActivationRecord>,
        remaining: std::vec::IntoIter<FrontierRecord>,
    },
    Transition {
        loaded: LoadedRun,
        transition: ControlTransition,
    },
    Retry {
        loaded: LoadedRun,
    },
    EndSource {
        loaded: LoadedRun,
    },
    Release,
}

impl<
    'a,
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend,
    C: ConditionBackend,
    T: StateTyping,
    H: LifecycleHooks,
    E: ChildExecutor,
> Execution<'a, CS, A, X, C, T, H, E>
{
    pub(crate) async fn open(
        run: &'a mut WorkflowRun<CS, A, X, C, T, H, E>,
        mut options: ExecutionOptions,
        mode: ActionMode,
    ) -> Result<Self, AppError> {
        let admission = run.runtime.app.reserve_caller()?;
        if !options.state.values().is_empty() {
            return Err(AppError::InvalidInput(
                "existing execution cannot replace initial state".into(),
            ));
        }
        JsonBoundary::validate_inputs(&options.inputs)?;
        run.runtime.app.ensure_open().await?;
        let loaded = run.runtime.load().await?;
        let definition = run.runtime.app.definition_for(&loaded.record)?;
        let graph = run.runtime.app.graph(&definition);
        options.resolve_selectors(&graph)?;
        let pending_controls = !loaded.record.checkpoint.controls.is_empty();
        let discard_staged = loaded.record.checkpoint.rollback_stage_keys(&definition);
        let generation = run.runtime.cell.claim_execution(mode)?;
        run.runtime.cell.configure_execution(generation, options)?;
        let mut execution = Self {
            runtime: run.runtime.clone(),
            app: &run.runtime.app,
            lane: ExecutionLane::Caller,
            admission: Some(admission),
            children: Vec::new(),
            wake: None,
            timer: None,
            generation: Some(generation),
            parent_deadline: None,
            state: SessionState::Ready,
            sources: Vec::new(),
            next_source: 0,
            native_partials: None,
            prefetched: None,
            transitions: 0,
            observed: Some(loaded.record.clone()),
        };
        if loaded.record.checkpoint.can_retry_failed() {
            execution.publish(SessionPublication::Retry { loaded }, false)?;
            poll_fn(|cx| execution.poll_publication(cx, false)).await?;
        } else if !loaded.record.checkpoint.is_terminal()
            && (loaded.record.checkpoint.admission != RunAdmission::Prepared
                || !discard_staged.is_empty())
        {
            execution.publish_transition(loaded, ControlTransition::Pause { discard_staged })?;
            poll_fn(|cx| execution.poll_publication(cx, false)).await?;
        }
        // Acquisition is bounded; a returned lazy execution owns no caller slot.
        drop(execution.admission.take());
        execution.runtime.cell.admit_lifecycle(generation)?;
        if pending_controls {
            execution.runtime.schedule(ScheduleCause::Reconcile);
        }
        Ok(execution)
    }

    pub(super) fn background(run: &'a mut WorkflowRun<CS, A, X, C, T, H, E>) -> Self {
        Self::owned(run.runtime.clone(), &run.runtime.app, ExecutionLane::Worker)
    }

    fn owned(
        runtime: WorkflowRunRuntime<CS, A, X, C, T, H, E>,
        app: &'a AppRuntime<CS, A, X, C, T, H, E>,
        lane: ExecutionLane,
    ) -> Self {
        Self {
            runtime,
            app,
            lane,
            admission: None,
            children: Vec::new(),
            wake: None,
            timer: None,
            generation: None,
            parent_deadline: None,
            state: SessionState::Ready,
            sources: Vec::new(),
            next_source: 0,
            native_partials: None,
            prefetched: None,
            transitions: 0,
            observed: None,
        }
    }

    /// Advances lazily to one partial result or committed boundary.
    ///
    /// Partials are transient; only Committed events publish state/outcomes. Streaming
    /// iteration checks halt-after-prior before halt-before-next. A retained halt is
    /// not reclassified by a later step/finish call. EOF without Completed commits
    /// no action and ends this observation without automatically retrying it.
    ///
    /// # Errors
    ///
    /// Returns `Closed` or `Conflict` when execution authority is revoked or stale,
    /// `Integration` for unavailable durable/lower evidence, and `Timeout` for expired
    /// observation. Invalid execution input may return `InvalidInput`; accepted action
    /// failures are retained in the returned run state. Cleanup preserves primary
    /// failures and does not detach an admitted native commit.
    ///
    /// # Example
    ///
    /// ```
    /// # use agent_sdk::Agent;
    /// # use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_sdk::StateStore;
    /// # use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
    /// #     "state":{"count":0}, "required":["count"],
    /// #     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    /// #     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    /// #         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    /// #     ]}}], "routes":{}
    /// # }"#)?;
    /// # let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
    /// # let mut app = App::open(config).await?;
    /// # let mut workspace = app.create_workspace(WorkspaceConfig {
    /// #     name: Some("local counter".into()), root: std::env::current_dir()?,
    /// # }).await?;
    /// # let workflow = workspace.workflow("counter".parse()?).await?;
    /// # let mut run = workflow.prepare(WorkflowState::default()).await?;
    /// let mut execution = run.execution(ExecutionOptions::default()).await?;
    /// let event = execution.next().await?.expect("the state action commits");
    /// assert!(matches!(event, ExecutionEvent::Committed { .. }));
    /// execution.finish().await?;
    /// #
    /// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn next(&mut self) -> Result<Option<ExecutionEvent>, AppError> {
        let event = poll_fn(|cx| self.poll_advance(cx, false)).await;
        if (matches!(&event, Ok(Some(ExecutionEvent::Committed { .. })))
            || matches!(&event, Ok(None)) && matches!(&self.state, SessionState::Eof)
            || event.is_err() && self.runtime.cell.lifecycle_generation().is_none())
            && let Some(generation) = self.generation
        {
            self.runtime.cell.returned_boundary(generation);
        }
        event
    }

    fn poll_advance(
        &mut self,
        cx: &mut Context<'_>,
        ordinary: bool,
    ) -> Poll<Result<Option<ExecutionEvent>, AppError>> {
        loop {
            // Sources polls once after starting newly prepared groups. Keep
            // its pre-start authority check, but do not double-poll pending
            // descendants at every nested level.
            if !matches!(&self.state, SessionState::Sources { .. })
                || self
                    .children
                    .iter()
                    .any(|group| matches!(group.state, ChildExecutionState::Prepared(_)))
            {
                self.poll_child_groups(cx)?;
            }
            match &mut self.state {
                SessionState::ObservingItem { .. } => match self.poll_item(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Ready(Ok(Some(event))) if !ordinary => {
                        return Poll::Ready(Ok(Some(event)));
                    }
                    Poll::Ready(Ok(_)) => continue,
                },
                SessionState::Finished => return Poll::Ready(Ok(None)),
                SessionState::Eof => self.state = SessionState::Ready,
                SessionState::ActivationReady { .. } => self.activate_ready(ordinary)?,
                SessionState::Publishing { .. } => {
                    match self.poll_publication(cx, false) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                        Poll::Ready(Ok(Some(event))) => {
                            if ordinary && matches!(event, ExecutionEvent::Partial { .. }) {
                                continue;
                            }
                            return Poll::Ready(Ok(Some(event)));
                        }
                        Poll::Ready(Ok(None)) => {}
                    }
                    if matches!(&self.state, SessionState::Eof) {
                        return Poll::Ready(Ok(None));
                    }
                }
                SessionState::Ready => {
                    self.ensure_owned()?;
                    self.arm_notification()?;
                    let terminal = self
                        .observed
                        .as_ref()
                        .filter(|record| {
                            self.generation.is_none() && record.checkpoint.is_terminal()
                        })
                        .map(|record| record.checkpoint.sequence);
                    self.publish(SessionPublication::Load { terminal }, ordinary)?;
                }
                SessionState::Loaded(_) => {
                    if self.advance_loaded(ordinary)? {
                        return Poll::Ready(Ok(None));
                    }
                }
                SessionState::Sources { .. } => {
                    self.start_child_groups(cx)?;
                    if !matches!(self.state, SessionState::Sources { .. }) {
                        continue;
                    }
                    self.poll_child_groups(cx)?;
                    if self.publish_child_completion(ordinary, false)? {
                        continue;
                    }
                    if self.prefetched.is_none()
                        && !self.sources.is_empty()
                        && self.sources.iter().all(|source| {
                            matches!(
                                source.state,
                                CallerSourceState::Ended | CallerSourceState::Eof
                            )
                        })
                    {
                        let SessionState::Sources { loaded, .. } = &self.state else {
                            unreachable!()
                        };
                        if !self.retains_external_continuation(&loaded.record) {
                            self.finish_wave(ordinary)?;
                            continue;
                        }
                    }
                    if self.prefetched.is_none() {
                        match self.poll_sources_observation(cx) {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                            Poll::Ready(Ok(Some(event))) if !ordinary => {
                                return Poll::Ready(Ok(Some(event)));
                            }
                            Poll::Ready(Ok(Some(_))) => continue,
                            Poll::Ready(Ok(None)) => {}
                        }
                        if !matches!(&self.state, SessionState::Sources { .. }) {
                            continue;
                        }
                    }
                    if let Some((index, event)) = self.prefetched.take() {
                        self.consume_source_event(index, event, ordinary)?;
                    } else {
                        return Poll::Ready(Ok(None));
                    }
                }
            }
        }
    }

    fn advance_loaded(&mut self, ordinary: bool) -> Result<bool, AppError> {
        let SessionState::Loaded(loaded) =
            std::mem::replace(&mut self.state, SessionState::Finished)
        else {
            unreachable!()
        };
        self.ensure_owned()?;
        self.runtime.supervise_controls(&loaded.record);
        if loaded.record.checkpoint.is_stopping() {
            self.stop_children();
            self.stop_sources();
            self.publish(SessionPublication::SettleStopping { loaded }, ordinary)?;
            return Ok(false);
        }
        if loaded.record.checkpoint.is_terminal() {
            self.stop_children();
            self.stop_sources();
            self.observed = Some(loaded.record);
            return Ok(true);
        }
        self.observed = Some(loaded.record.clone());
        if self.children.iter().any(|group| {
            matches!(
                group.state,
                ChildExecutionState::Worker(_) | ChildExecutionState::Receipt(_)
            ) && group
                .parent
                .as_ref()
                .is_some_and(|parent| !loaded.record.owns_child_activation(parent, group.sequence))
        }) {
            // A worker may have staged its result before its final
            // receipt arrives. Keep that original receipt ahead of
            // parent evaluation, halting and barrier publication.
            self.state = SessionState::Sources { loaded };
            return Ok(false);
        }
        if self.generation.is_none()
            && self.parent_deadline.is_none()
            && loaded.record.checkpoint.is_paused()
        {
            // Background cleanup may finish already admitted children, but
            // their receipt cannot authorize a later prepared parent barrier.
            self.state = SessionState::Sources { loaded };
            return Ok(true);
        }
        if self.runtime.cell.execution_boundary()? == Some(false) && self.halt_after(&loaded)? {
            return Ok(false);
        }
        if self.transitions >= MAX_DRIVER_TRANSITIONS {
            self.publish_decision(
                loaded,
                BspDecision::Failed {
                    category: FailureCategory::Limit,
                    message: "workflow transition bound exhausted".into(),
                    observation: None,
                },
                ordinary,
            )?;
            return Ok(false);
        }
        let mut decision = match self.runtime.decide_checkpoint(&loaded) {
            Ok(decision) => decision,
            Err(failure) => {
                if self.generation.is_none() && self.runtime.schedule_operation_failure(&failure) {
                    self.observed = None;
                }
                return Err(failure.into_error());
            }
        };
        if let BspDecision::Activate(selected) = &mut decision {
            selected.retain(|activation| {
                !self
                    .sources
                    .iter()
                    .any(|source| source.activation == *activation)
            });
            if selected.is_empty() {
                decision = BspDecision::AwaitControls;
            }
        }
        let boundary = self.runtime.cell.execution_boundary()?;
        if boundary.is_some()
            && let Some(node) =
                decision.pending_node(&self.runtime.cell.execution_options()?.halt_before)
        {
            let node = node.to_owned();
            self.publish_transition(loaded, ControlTransition::Halt { node, result: None })?;
            return Ok(false);
        }
        if boundary == Some(true)
            && !matches!(&decision, BspDecision::Routed { .. })
            && self.halt_after(&loaded)?
        {
            return Ok(false);
        }
        if !matches!(&decision, BspDecision::Routed { .. }) {
            self.runtime.cell.clear_execution_boundary()?;
        }
        match decision {
            BspDecision::AwaitControls => {
                let pending = loaded
                    .record
                    .checkpoint
                    .controls
                    .values()
                    .find(|control| {
                        matches!(control.state, ControlRecordState::Child { .. })
                            && !self.children.iter().any(|group| {
                                group.sequence == control.sequence
                                    && !matches!(group.state, ChildExecutionState::Awaiting)
                            })
                    })
                    .map(|control| control.sequence);
                if let Some(sequence) = pending {
                    self.publish(
                        SessionPublication::ObserveChildren { loaded, sequence },
                        ordinary,
                    )?;
                } else {
                    self.state = SessionState::Sources { loaded };
                }
            }
            BspDecision::Activate(selected) => {
                self.transitions += 1;
                let mut remaining = selected.into_iter();
                let activation = remaining.next().ok_or_else(|| {
                    AppError::Integration("native selected prefix is empty".into())
                })?;
                self.begin_activation(loaded, activation, remaining, Vec::new(), ordinary)?;
            }
            decision => {
                self.transitions += 1;
                self.publish_decision(loaded, decision, ordinary)?;
            }
        }
        Ok(false)
    }

    /// Drains transient partials through one committed boundary.
    ///
    /// Consumes partials until a committed boundary or halt. Ordinary iteration
    /// checks halt-before-next before halt-after-prior. Only a committed action
    /// advances its ordinal; control checkpoints and partials do not.
    ///
    /// # Errors
    ///
    /// Returns `Closed` or `Conflict` when execution authority is revoked or stale,
    /// `Integration` for unavailable durable/lower evidence, and `Timeout` for expired
    /// observation. Invalid execution input may return `InvalidInput`; accepted action
    /// failures are retained in the returned run state. Cleanup preserves primary
    /// failures and does not detach an admitted native commit.
    ///
    /// # Example
    ///
    /// ```
    /// # use agent_sdk::Agent;
    /// # use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_sdk::StateStore;
    /// # use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
    /// #     "state":{"count":0}, "required":["count"],
    /// #     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    /// #     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    /// #         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    /// #     ]}}], "routes":{}
    /// # }"#)?;
    /// # let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
    /// # let mut app = App::open(config).await?;
    /// # let mut workspace = app.create_workspace(WorkspaceConfig {
    /// #     name: Some("local counter".into()), root: std::env::current_dir()?,
    /// # }).await?;
    /// # let workflow = workspace.workflow("counter".parse()?).await?;
    /// # let mut run = workflow.prepare(WorkflowState::default()).await?;
    /// let mut execution = run.execution(ExecutionOptions::default()).await?;
    /// let event = execution.step().await?.expect("the state action commits");
    /// assert!(matches!(event, ExecutionEvent::Committed { .. }));
    /// assert_eq!(execution.finish().await?.state.values()["count"], 1);
    /// #
    /// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn step(&mut self) -> Result<Option<ExecutionEvent>, AppError> {
        loop {
            match poll_fn(|cx| self.poll_advance(cx, true)).await {
                Ok(Some(ExecutionEvent::Partial { .. })) => {}
                event => {
                    if (matches!(&event, Ok(Some(ExecutionEvent::Committed { .. })))
                        || matches!(&event, Ok(None)) && matches!(&self.state, SessionState::Eof)
                        || event.is_err() && self.runtime.cell.lifecycle_generation().is_none())
                        && let Some(generation) = self.generation
                    {
                        self.runtime.cell.returned_boundary(generation);
                    }
                    return event;
                }
            }
        }
    }

    /// Drains this execution to exhaustion, a halt or an actionable control.
    ///
    /// Consumes this owner and drains advancement until halt, actionable parking, EOF
    /// or exhaustion, then finalizes observation. A returned snapshot need not be
    /// Succeeded: inspect status and interactions. Accepted child/native cleanup
    /// retains its original authority and deadlines.
    ///
    /// # Errors
    ///
    /// Returns `Closed` or `Conflict` when execution authority is revoked or stale,
    /// `Integration` for unavailable durable/lower evidence, and `Timeout` for expired
    /// observation. Invalid execution input may return `InvalidInput`; accepted action
    /// failures are retained in the returned run state. Cleanup preserves primary
    /// failures and does not detach an admitted native commit.
    ///
    /// # Example
    ///
    /// ```
    /// # use agent_sdk::Agent;
    /// # use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_sdk::StateStore;
    /// # use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
    /// #     "state":{"count":0}, "required":["count"],
    /// #     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    /// #     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    /// #         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    /// #     ]}}], "routes":{}
    /// # }"#)?;
    /// # let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
    /// # let mut app = App::open(config).await?;
    /// # let mut workspace = app.create_workspace(WorkspaceConfig {
    /// #     name: Some("local counter".into()), root: std::env::current_dir()?,
    /// # }).await?;
    /// # let workflow = workspace.workflow("counter".parse()?).await?;
    /// # let mut run = workflow.prepare(WorkflowState::default()).await?;
    /// let terminal = run.execution(ExecutionOptions::default()).await?.finish().await?;
    /// assert!(matches!(terminal.status, RunStatus::Succeeded));
    /// assert_eq!(terminal.state.values()["count"], 1);
    /// #
    /// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn finish(mut self) -> Result<RunSnapshot, AppError> {
        if !matches!(&self.state, SessionState::Eof) {
            while poll_fn(|cx| self.poll_advance(cx, true)).await?.is_some() {}
        }
        self.close().await
    }

    /// Discards uncommitted caller work after joining admitted publication.
    ///
    /// Consumes this owner and finalizes its uncommitted boundary without requesting
    /// durable run cancellation. An already admitted native publication retains its
    /// commit authority; abort is not a guarantee that external work was rolled back.
    ///
    /// # Errors
    ///
    /// Returns `Closed` or `Conflict` when execution authority is revoked or stale,
    /// `Integration` for unavailable durable/lower evidence, and `Timeout` for expired
    /// observation. Invalid execution input may return `InvalidInput`; accepted action
    /// failures are retained in the returned run state. Cleanup preserves primary
    /// failures and does not detach an admitted native commit.
    ///
    /// # Example
    ///
    /// ```
    /// # use agent_sdk::Agent;
    /// # use app_sdk::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_sdk::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_sdk::StateStore;
    /// # use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// #
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-sdk.workflow/v2", "id":"counter", "revision":"r1",
    /// #     "state":{"count":0}, "required":["count"],
    /// #     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    /// #     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    /// #         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    /// #     ]}}], "routes":{}
    /// # }"#)?;
    /// # let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition]);
    /// # let mut app = App::open(config).await?;
    /// # let mut workspace = app.create_workspace(WorkspaceConfig {
    /// #     name: Some("local counter".into()), root: std::env::current_dir()?,
    /// # }).await?;
    /// # let workflow = workspace.workflow("counter".parse()?).await?;
    /// # let mut run = workflow.prepare(WorkflowState::default()).await?;
    /// let execution = run.execution(ExecutionOptions::default()).await?;
    /// let parked = execution.abort().await?;
    /// assert_eq!(parked.state.values()["count"], 0);
    /// assert!(parked.last_action_ordinal.is_none());
    /// let completed = run.execution(ExecutionOptions::default()).await?.finish().await?;
    /// assert_eq!(completed.state.values()["count"], 1);
    /// #
    /// # app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn abort(mut self) -> Result<RunSnapshot, AppError> {
        self.close().await
    }

    async fn close(&mut self) -> Result<RunSnapshot, AppError> {
        self.stop_sources();
        self.children.retain(|group| {
            !matches!(
                group.state,
                ChildExecutionState::Awaiting | ChildExecutionState::Prepared(_)
            )
        });
        poll_fn(|cx| -> Poll<Result<(), AppError>> {
            loop {
                self.poll_child_groups(cx)?;
                let deadline = self.source_deadline(None);
                if self.poll_timer(cx, deadline) {
                    self.poll_child_groups(cx)?;
                }
                if matches!(self.state, SessionState::Publishing { .. }) {
                    match self.poll_publication(cx, true) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(result) => {
                            result?;
                            continue;
                        }
                    }
                }
                if let SessionState::ObservingItem { receipt, .. } = &mut self.state {
                    let observed = match Pin::new(receipt).poll(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(result) => result.map_err(|_| {
                            AppError::Closed("stream observation receipt closed".into())
                        })?,
                    };
                    self.finish_item(observed)?;
                    continue;
                }
                if self.children.is_empty() {
                    return Poll::Ready(Ok(()));
                }
                if let SessionState::Loaded(_) = &self.state {
                    let SessionState::Loaded(loaded) =
                        std::mem::replace(&mut self.state, SessionState::Finished)
                    else {
                        unreachable!()
                    };
                    self.state = SessionState::Sources { loaded };
                }
                if matches!(self.state, SessionState::Sources { .. }) {
                    if self.publish_child_completion(false, true)? {
                        continue;
                    }
                    // Observe cancellation under the original receipt/deadline;
                    // no new group or source can be constructed during close.
                    if self.poll_notification(cx)? {
                        let SessionState::Sources { loaded } =
                            std::mem::replace(&mut self.state, SessionState::Finished)
                        else {
                            unreachable!()
                        };
                        self.publish(
                            SessionPublication::Reload {
                                receipt: SessionReceipt::Checkpoint {
                                    loaded,
                                    staged: Vec::new(),
                                },
                                deadline,
                            },
                            false,
                        )?;
                        continue;
                    }
                }
                return Poll::Pending;
            }
        })
        .await?;
        if !self.sources.is_empty() {
            if !matches!(&self.state, SessionState::Sources { .. }) {
                let deadline = LifecycleEvent::deadline(self.parent_deadline);
                let loaded = if matches!(&self.state, SessionState::Loaded(_)) {
                    // Closing an already-admitted publication kept its exact
                    // receipt. Preserve the generation fence without decoding
                    // that same checkpoint again.
                    self.runtime
                        .observe_generation(deadline)
                        .await
                        .map_err(|failure| {
                            self.runtime.retain_execution_read_failure(&failure);
                            failure.into_error()
                        })?;
                    let SessionState::Loaded(loaded) =
                        std::mem::replace(&mut self.state, SessionState::Finished)
                    else {
                        unreachable!()
                    };
                    loaded
                } else {
                    self.runtime
                        .load_execution_observation(None, Some(deadline))
                        .await
                        .map_err(|failure| {
                            self.runtime.retain_execution_read_failure(&failure);
                            failure.into_error()
                        })?
                        .ok_or_else(|| AppError::NotFound("workflow run does not exist".into()))?
                };
                self.state = SessionState::Sources { loaded };
            }
            let buffered_failure = self
                .prefetched
                .take()
                .and_then(|(index, event)| event.err().map(|category| (index, category)));
            for index in 0..self.sources.len() {
                let source = &mut self.sources[index];
                let failure = buffered_failure
                    .filter(|(buffered, _)| *buffered == index)
                    .map(|(_, category)| category)
                    .or(match source.state {
                        CallerSourceState::Failed(category) => Some(category),
                        _ => None,
                    });
                source.state = CallerSourceState::Ended;
                if let Some(category) = failure {
                    self.stage_source_failure(index, category, false)?;
                    poll_fn(|cx| self.poll_publication(cx, true)).await?;
                    self.runtime
                        .observe_generation(LifecycleEvent::deadline(self.parent_deadline))
                        .await
                        .map_err(|failure| {
                            self.runtime.retain_execution_read_failure(&failure);
                            failure.into_error()
                        })?;
                    let SessionState::Loaded(loaded) =
                        std::mem::replace(&mut self.state, SessionState::Finished)
                    else {
                        unreachable!("closing publication retains its original checkpoint receipt")
                    };
                    self.state = SessionState::Sources { loaded };
                }
            }
            let SessionState::Sources { loaded, .. } = &self.state else {
                unreachable!()
            };
            if loaded
                .record
                .checkpoint
                .staged
                .iter()
                .any(|activation| matches!(activation.outcome, StagedOutcomeRecord::Failed { .. }))
            {
                self.finish_wave(false)?;
                poll_fn(|cx| self.poll_publication(cx, true)).await?;
            }
        }
        if let SessionState::Sources { loaded, .. } =
            std::mem::replace(&mut self.state, SessionState::Finished)
        {
            self.observed = Some(loaded.record);
        }
        self.sources.clear();
        let foreground = self.generation.is_some();
        if foreground {
            self.publish(SessionPublication::Release, false)?;
            poll_fn(|cx| self.poll_publication(cx, true)).await?;
            self.state = SessionState::Finished;
            self.generation = None;
        }
        let snapshot = if foreground {
            self.runtime.snapshot().await?
        } else {
            self.runtime.app.ensure_snapshot_read()?;
            let terminal = self
                .observed
                .as_ref()
                .filter(|record| record.checkpoint.is_terminal())
                .map(|record| record.checkpoint.sequence);
            let loaded = self
                .runtime
                .load_execution_observation(terminal, self.parent_deadline)
                .await
                .map_err(|failure| {
                    self.runtime.retain_execution_read_failure(&failure);
                    failure.into_error()
                })?
                .ok_or_else(|| AppError::NotFound("workflow run does not exist".into()))?;
            let mut deadline = LifecycleEvent::deadline(self.parent_deadline);
            if loaded.record.checkpoint.is_terminal()
                && let Some(generation) = self.runtime.cell.lifecycle_generation()
            {
                deadline = self
                    .runtime
                    .cell
                    .begin_action_cleanup(generation)?
                    .min(deadline);
            }
            if !self.retains_execution_lifecycle(&loaded.record)
                && (loaded.record.checkpoint.controls.is_empty()
                    || loaded.record.checkpoint.is_public_outcome())
            {
                self.runtime
                    .finish_execution_observation(
                        &loaded.record,
                        None,
                        loaded.record.checkpoint.is_terminal().then_some(deadline),
                        false,
                    )
                    .await?;
            }
            if loaded.record.checkpoint.is_terminal() {
                // Recheck the generation after awaited hooks, while retaining
                // this terminal receipt instead of reading its run head again.
                self.runtime
                    .observe_generation(deadline)
                    .await
                    .map_err(|failure| {
                        self.runtime.retain_execution_read_failure(&failure);
                        failure.into_error()
                    })?;
                let snapshot = self.runtime.project(&loaded.record)?;
                self.runtime.app.ensure_snapshot_read()?;
                snapshot
            } else {
                self.runtime.snapshot().await?
            }
        };
        if !foreground
            && matches!(
                snapshot.status,
                RunStatus::Succeeded
                    | RunStatus::Cancelled
                    | RunStatus::Failed(_)
                    | RunStatus::Halted { .. }
            )
        {
            self.runtime.cell.clear_execution_options(None);
        }
        if foreground && matches!(snapshot.status, crate::workflow::RunStatus::Running) {
            self.runtime.wait().await
        } else {
            Ok(snapshot)
        }
    }

    fn stop_sources(&mut self) {
        for source in &mut self.sources {
            let failure = match source.state {
                CallerSourceState::Failed(category) => Some(category),
                _ => None,
            };
            let destroyed = source.stop().err();
            if let Some(category) = failure.or(destroyed) {
                self.runtime.retain_source_failure(
                    &source.context,
                    &source.activation.lineage,
                    category,
                );
                source.state = CallerSourceState::Failed(category);
            }
        }
    }

    fn stop_children(&mut self) {
        for group in &mut self.children {
            group.stop(&self.runtime);
        }
        self.children.clear();
    }

    fn plan_child_group(
        &mut self,
        loaded: LoadedRun,
        sequence: u64,
        children: Vec<RunRecord>,
        verified: bool,
    ) -> Result<(), AppError> {
        if loaded.record.checkpoint.is_stopping() || loaded.record.checkpoint.is_terminal() {
            self.state = SessionState::Loaded(loaded);
            return Ok(());
        }
        if let Some(group) = self
            .children
            .iter_mut()
            .find(|group| group.sequence == sequence)
            && let Some(ControlRecord {
                state: ControlRecordState::Child { admitted, .. },
                ..
            }) = loaded.record.checkpoint.control(sequence)
        {
            // Only confirmed native admission transfers these maps to the
            // child's concrete execution options. Retry retains the original.
            for values in group.inputs.iter_mut().take(admitted.len()) {
                values.clear();
            }
        }
        let decision = catch_unwind(AssertUnwindSafe(|| {
            self.runtime
                .plan_children(&loaded, sequence, children, verified)
        }))
        .map_err(|_| AppError::Integration("child planning callback panicked".into()))??;
        match decision {
            BspDecision::ExecuteChildren { sequence, children } => {
                let control = loaded
                    .record
                    .checkpoint
                    .control(sequence)
                    .ok_or_else(|| AppError::Conflict("child control is unavailable".into()))?;
                let context = self.runtime.child_context(&loaded.record, sequence)?;
                let definition = self.runtime.app.definition_for(&loaded.record)?;
                let step = definition
                    .node(&control.node)
                    .ok_or_else(|| AppError::Integration("child definition is unavailable".into()))?
                    .step();
                step.validate_mode(context.mode)?;
                let deadline = tokio::time::Instant::now()
                    .checked_add(WorkflowRunRuntime::<CS, A, X, C, T, H, E>::remaining(
                        control.deadline_millis,
                    )?)
                    .ok_or_else(|| {
                        AppError::Timeout("child deadline is outside the clock range".into())
                    })?;
                if let Some(group) = self
                    .children
                    .iter_mut()
                    .find(|group| group.sequence == sequence)
                {
                    if !matches!(group.state, ChildExecutionState::Awaiting) {
                        return Err(AppError::Conflict(
                            "child group already has a polling owner".into(),
                        ));
                    }
                    group.state = ChildExecutionState::Prepared(children);
                } else {
                    self.children.push(ChildExecution {
                        sequence,
                        parent: None,
                        context,
                        lineage: control.lineage.clone(),
                        deadline,
                        state: ChildExecutionState::Prepared(children),
                        inputs: Vec::new(),
                    });
                }
                self.state = SessionState::Loaded(loaded);
                Ok(())
            }
            BspDecision::AwaitControls => {
                self.state = SessionState::Loaded(loaded);
                Ok(())
            }
            decision => {
                let transition = match decision {
                    BspDecision::Stage(activations) => ControlTransition::Stage { activations },
                    BspDecision::Failed {
                        category,
                        message,
                        observation,
                    } => ControlTransition::Failed {
                        category,
                        message,
                        observation,
                    },
                    decision => return self.publish_decision(loaded, decision, false),
                };
                let control = loaded
                    .record
                    .checkpoint
                    .control(sequence)
                    .cloned()
                    .ok_or_else(|| {
                        AppError::Conflict("completed child control is unavailable".into())
                    })?;
                self.publish(
                    SessionPublication::ChildOutcome {
                        control,
                        transition,
                    },
                    false,
                )
            }
        }
    }

    fn start_child_groups(&mut self, cx: &mut Context<'_>) -> Result<(), AppError> {
        if !self
            .children
            .iter()
            .any(|group| matches!(group.state, ChildExecutionState::Prepared(_)))
        {
            return Ok(());
        }
        if self.poll_notification(cx)? {
            let deadline = self.source_deadline(None);
            let SessionState::Sources { loaded } =
                std::mem::replace(&mut self.state, SessionState::Finished)
            else {
                unreachable!()
            };
            self.publish(
                SessionPublication::Reload {
                    receipt: SessionReceipt::Checkpoint {
                        loaded,
                        staged: Vec::new(),
                    },
                    deadline,
                },
                false,
            )?;
            return Ok(());
        }
        let SessionState::Sources { loaded, .. } = &self.state else {
            return Ok(());
        };
        let parent = Arc::new(loaded.record.clone());
        for index in 0..self.children.len() {
            if !matches!(self.children[index].state, ChildExecutionState::Prepared(_)) {
                continue;
            }
            let group = &mut self.children[index];
            let ChildExecutionState::Prepared(children) =
                std::mem::replace(&mut group.state, ChildExecutionState::Closed)
            else {
                unreachable!()
            };
            group.parent = Some(Arc::clone(&parent));
            if group.context.mode == ActionMode::Asynchronous && self.lane == ExecutionLane::Caller
            {
                let (sender, receipt) = oneshot::channel();
                if !self.runtime.app.spawn_driver(
                    self.runtime.key.clone(),
                    Duration::ZERO,
                    DriverCommand::Child {
                        sequence: group.sequence,
                        generation: self.generation,
                        parent: Arc::clone(&parent),
                        children,
                        receipt: sender,
                    },
                ) {
                    return Err(AppError::Busy(
                        "child group already has an accepted owner".into(),
                    ));
                }
                group.state = ChildExecutionState::Worker(receipt);
            } else {
                group.start_local(&self.runtime, self.app, self.lane, children)?;
            }
        }
        Ok(())
    }

    fn poll_child_groups(&mut self, cx: &mut Context<'_>) -> Result<(), AppError> {
        let stopped = match &self.state {
            SessionState::Loaded(loaded)
            | SessionState::Sources { loaded }
            | SessionState::ObservingItem { loaded, .. } => {
                loaded.record.checkpoint.is_stopping() || loaded.record.checkpoint.is_terminal()
            }
            _ => false,
        };
        if stopped {
            self.stop_children();
            return Ok(());
        }
        if let SessionState::Loaded(loaded)
        | SessionState::Sources { loaded }
        | SessionState::ObservingItem { loaded, .. } = &self.state
        {
            self.children.retain(|group| {
                !matches!(group.state, ChildExecutionState::Awaiting)
                    || loaded.record.checkpoint.control(group.sequence).is_some()
            });
            if self.children.iter().any(|group| {
                !matches!(
                    group.state,
                    ChildExecutionState::Awaiting
                        | ChildExecutionState::Worker(_)
                        | ChildExecutionState::Receipt(_)
                ) && group.parent.as_ref().is_some_and(|parent| {
                    !loaded.record.owns_child_activation(parent, group.sequence)
                })
            }) {
                return Err(AppError::Conflict(
                    "child parent reservation changed".into(),
                ));
            }
        }
        let observing_parent = matches!(
            self.state,
            SessionState::Publishing {
                purpose: PublicationPurpose::Reload,
                ..
            }
        );
        for group in &mut self.children {
            if observing_parent
                && tokio::time::Instant::now() >= group.deadline
                && matches!(
                    group.state,
                    ChildExecutionState::Local(_) | ChildExecutionState::Pipeline(_)
                )
            {
                // The outstanding authority read owns deadline disposition.
                // Keep the original executor until that receipt or fatal cleanup.
                continue;
            }
            let _ = group.poll_ready(cx, &self.runtime);
        }
        Ok(())
    }

    fn publish_child_completion(
        &mut self,
        ordinary: bool,
        closing: bool,
    ) -> Result<bool, AppError> {
        let Some(index) = self.children.iter().position(|group| {
            matches!(
                group.state,
                ChildExecutionState::Complete(_) | ChildExecutionState::Receipt(_)
            )
        }) else {
            return Ok(false);
        };
        let group = &mut self.children[index];
        let sequence = group.sequence;
        let parent = group.parent.take();
        let context = Arc::clone(&group.context);
        let lineage = group.lineage.clone();
        let completed = std::mem::replace(&mut group.state, ChildExecutionState::Awaiting);
        if closing {
            self.children.remove(index);
        }
        if let ChildExecutionState::Receipt(result) = completed {
            match (*result).map_err(RuntimeOperationFailure::into_error)? {
                SessionReceipt::Children {
                    loaded,
                    sequence,
                    children,
                    verified: true,
                } => {
                    if closing {
                        self.state = SessionState::Loaded(loaded);
                    } else {
                        self.plan_child_group(loaded, sequence, children, true)?;
                    }
                }
                SessionReceipt::Checkpoint { loaded, .. } => {
                    self.state = SessionState::Loaded(loaded)
                }
                SessionReceipt::Children {
                    verified: false, ..
                } => {
                    return Err(AppError::Integration(
                        "child worker returned an unverified execution result".into(),
                    ));
                }
            }
            return Ok(true);
        }
        let ChildExecutionState::Complete(result) = completed else {
            unreachable!()
        };
        let SessionState::Sources { loaded } =
            std::mem::replace(&mut self.state, SessionState::Finished)
        else {
            unreachable!()
        };
        match result {
            Ok(completions) => self.publish(
                SessionPublication::VerifyChildren {
                    parent: parent.ok_or_else(|| {
                        AppError::Integration("child group parent evidence is unavailable".into())
                    })?,
                    sequence,
                    completions,
                },
                ordinary,
            )?,
            Err(category) => {
                let activation = loaded
                    .record
                    .checkpoint
                    .frontier
                    .iter()
                    .find(|activation| {
                        activation.node == context.node && activation.lineage == lineage
                    })
                    .cloned()
                    .ok_or_else(|| AppError::Conflict("child activation changed".into()))?;
                self.publish_failure(loaded, &activation, &context, category)?;
            }
        }
        Ok(true)
    }

    fn retains_external_continuation(&self, record: &RunRecord) -> bool {
        !self.children.is_empty()
            || self.runtime.app.has_child_work(&self.runtime.key)
            || record.checkpoint.controls.values().any(|control| {
                matches!(&control.state, ControlRecordState::Attempt(_))
                    && self
                        .runtime
                        .app
                        .has_control_work(&self.runtime.key, control.sequence)
            })
    }

    fn retains_execution_lifecycle(&self, record: &RunRecord) -> bool {
        self.retains_execution_work(record)
            || record
                .checkpoint
                .controls
                .values()
                .any(|control| matches!(control.state, ControlRecordState::Inputs { .. }))
    }

    fn retains_execution_work(&self, record: &RunRecord) -> bool {
        self.sources.iter().any(|source| {
            matches!(
                source.state,
                CallerSourceState::Polling(_) | CallerSourceState::Failed(_)
            )
        }) || self.prefetched.is_some()
            || self.retains_external_continuation(record)
            || record.checkpoint.controls.values().any(|control| {
                matches!(control.state, ControlRecordState::Sleep)
                    && self
                        .runtime
                        .app
                        .has_control_work(&self.runtime.key, control.sequence)
            })
    }

    fn ensure_owned(&self) -> Result<(), AppError> {
        self.runtime.app.ensure_runtime_owned()?;
        if self
            .generation
            .is_some_and(|generation| !self.runtime.cell.execution_is_live(generation))
        {
            return Err(AppError::Closed(
                "run execution generation is closed".into(),
            ));
        }
        Ok(())
    }

    fn source_deadline(&self, excluded: Option<usize>) -> Option<tokio::time::Instant> {
        self.sources
            .iter()
            .enumerate()
            .filter_map(|(index, source)| {
                (Some(index) != excluded && matches!(source.state, CallerSourceState::Polling(_)))
                    .then_some(source.deadline)
            })
            .chain(self.children.iter().filter_map(|group| {
                (!matches!(
                    group.state,
                    ChildExecutionState::Complete(_) | ChildExecutionState::Receipt(_)
                ))
                .then_some(group.deadline)
            }))
            .min()
    }

    fn expire_sources(&mut self, excluded: Option<usize>) {
        let now = tokio::time::Instant::now();
        for (index, source) in self.sources.iter_mut().enumerate() {
            if Some(index) == excluded
                || now < source.deadline
                || !matches!(source.state, CallerSourceState::Polling(_))
            {
                continue;
            }
            self.runtime.retain_source_failure(
                &source.context,
                &source.activation.lineage,
                FailureCategory::Timeout,
            );
            let _ = source.stop();
            if self
                .prefetched
                .as_ref()
                .is_some_and(|(buffered, _)| *buffered == index)
            {
                self.prefetched = Some((index, Err(FailureCategory::Timeout)));
            } else {
                source.state = CallerSourceState::Failed(FailureCategory::Timeout);
            }
        }
    }

    fn poll_sources(&mut self, context: &mut Context<'_>, excluded: Option<usize>) {
        self.expire_sources(excluded);
        if self.prefetched.is_some() {
            return;
        }
        let count = self.sources.len();
        for offset in 0..count {
            let index = (self.next_source + offset) % count;
            if Some(index) == excluded {
                continue;
            }
            if let Poll::Ready(mut event) = self.sources[index].poll(context) {
                let source = &mut self.sources[index];
                if let Err(category) = &event {
                    // Preserve sampled failure before invoking the destructor:
                    // the original sibling callback may settle during Drop.
                    self.runtime.retain_source_failure(
                        &source.context,
                        &source.activation.lineage,
                        *category,
                    );
                }
                if matches!(
                    &event,
                    Ok(Some(ActionEvent::Completed { .. })) | Ok(None) | Err(_)
                ) {
                    let destroyed = source.stop();
                    if event.is_ok()
                        && let Err(category) = destroyed
                    {
                        self.runtime.retain_source_failure(
                            &source.context,
                            &source.activation.lineage,
                            category,
                        );
                        event = Err(category);
                    }
                    if matches!(&event, Ok(None)) {
                        source.state = CallerSourceState::Eof;
                    }
                }
                self.next_source = (index + 1) % count;
                self.prefetched = Some((index, event));
                return;
            }
        }
    }

    fn arm_notification(&mut self) -> Result<(), AppError> {
        if self.wake.is_none() {
            self.wake = Some(Box::pin(self.runtime.notifier()?.notified_owned()));
        }
        let wake = self.wake.as_mut().unwrap();
        wake.as_mut().enable();
        // Retain registration before admitting reads; polling installs the
        // current task waker without recreating this notification future.
        Ok(())
    }

    fn poll_notification(&mut self, cx: &mut Context<'_>) -> Result<bool, AppError> {
        self.arm_notification()?;
        let ready = self.wake.as_mut().unwrap().as_mut().poll(cx).is_ready();
        if ready {
            self.wake = None;
        }
        Ok(ready)
    }

    fn poll_timer(&mut self, cx: &mut Context<'_>, deadline: Option<tokio::time::Instant>) -> bool {
        let Some(deadline) = deadline else {
            self.timer = None;
            return false;
        };
        let timer = self
            .timer
            .get_or_insert_with(|| Box::pin(tokio::time::sleep_until(deadline)));
        if timer.deadline() != deadline {
            timer.as_mut().reset(deadline);
        }
        timer.as_mut().poll(cx).is_ready()
    }

    fn poll_native_partial(&mut self, cx: &mut Context<'_>) -> Option<ExecutionEvent> {
        let Some((_, receiver)) = &mut self.native_partials else {
            return None;
        };
        match receiver.poll_recv(cx) {
            Poll::Ready(Some(event)) => Some(event),
            Poll::Ready(None) => {
                self.native_partials = None;
                None
            }
            Poll::Pending => None,
        }
    }

    fn poll_sources_observation(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<ExecutionEvent>, AppError>> {
        self.ensure_owned()?;
        self.expire_sources(None);
        let deadline = self.source_deadline(None);
        if self.poll_timer(cx, deadline) {
            self.expire_sources(None);
        }
        let pending_failure = self
            .sources
            .iter()
            .any(|source| matches!(source.state, CallerSourceState::Failed(_)));
        if !pending_failure && self.poll_notification(cx)? {
            let SessionState::Sources { loaded } =
                std::mem::replace(&mut self.state, SessionState::Finished)
            else {
                unreachable!()
            };
            self.publish(
                SessionPublication::Reload {
                    receipt: SessionReceipt::Checkpoint {
                        loaded,
                        staged: Vec::new(),
                    },
                    deadline,
                },
                false,
            )?;
            return Poll::Ready(Ok(None));
        }
        self.poll_sources(cx, None);
        if let Some(event) = self.poll_native_partial(cx) {
            return Poll::Ready(Ok(Some(event)));
        }
        if self.prefetched.is_some() {
            return Poll::Ready(Ok(None));
        }
        let SessionState::Sources { loaded, .. } = &self.state else {
            unreachable!()
        };
        let known_eof = self
            .sources
            .iter()
            .any(|source| matches!(source.state, CallerSourceState::Eof));
        if (self.generation.is_some() || !self.retains_execution_work(&loaded.record))
            && !known_eof
            && !self.children.iter().any(|group| {
                matches!(
                    group.state,
                    ChildExecutionState::Complete(_) | ChildExecutionState::Receipt(_)
                )
            })
            && loaded.record.checkpoint.is_actionable()
        {
            return Poll::Ready(Ok(None));
        }
        Poll::Pending
    }

    fn poll_item(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<ExecutionEvent>, AppError>> {
        let SessionState::ObservingItem { index, .. } = &self.state else {
            unreachable!()
        };
        let index = *index;
        self.expire_sources(Some(index));
        let deadline = self.source_deadline(Some(index));
        if self.poll_timer(cx, deadline) {
            self.expire_sources(Some(index));
        }
        let SessionState::ObservingItem { receipt, .. } = &mut self.state else {
            unreachable!()
        };
        if let Poll::Ready(result) = Pin::new(receipt).poll(cx) {
            let result = result
                .map_err(|_| AppError::Closed("stream observation receipt closed".into()))
                .and_then(|result| result);
            return Poll::Ready(self.finish_item(result).map(|result| {
                result.map(|result| ExecutionEvent::Partial {
                    node: self.sources[index].activation.node.clone(),
                    result,
                })
            }));
        }
        self.poll_sources(cx, Some(index));
        if let Some(event) = self.poll_native_partial(cx) {
            return Poll::Ready(Ok(Some(event)));
        }
        Poll::Pending
    }

    fn finish_item(
        &mut self,
        result: Result<Option<Map<String, Value>>, AppError>,
    ) -> Result<Option<Map<String, Value>>, AppError> {
        let SessionState::ObservingItem {
            loaded, receipt, ..
        } = std::mem::replace(&mut self.state, SessionState::Finished)
        else {
            unreachable!()
        };
        drop(receipt);
        if result.is_err() {
            self.observed = Some(loaded.record);
            for source in &mut self.sources {
                let _ = source.stop();
            }
        } else {
            self.state = SessionState::Sources { loaded };
        }
        result
    }

    fn consume_source_event(
        &mut self,
        index: usize,
        event: Result<Option<ActionEvent>, FailureCategory>,
        ordinary: bool,
    ) -> Result<(), AppError> {
        match event {
            Ok(Some(ActionEvent::Partial { result })) => {
                if JsonBoundary::validate_size(&result).is_err() {
                    let _ = self.sources[index].stop();
                    return self.stage_source_failure(index, FailureCategory::State, ordinary);
                }
                self.publish_item(index, ActionEvent::Partial { result })
            }
            Ok(Some(
                event @ (ActionEvent::SpanStarted { .. }
                | ActionEvent::SpanAttribute { .. }
                | ActionEvent::SpanFinished { .. }),
            )) => self.publish_item(index, event),
            Ok(Some(ActionEvent::Completed { result, mut writes })) => {
                let SessionState::Sources { loaded, .. } = &self.state else {
                    unreachable!()
                };
                let source = &self.sources[index];
                let staged = catch_unwind(AssertUnwindSafe(|| {
                    let definition = self.runtime.app.definition_for(&loaded.record)?;
                    if let Some(Step::Action {
                        action,
                        revision,
                        project_result: true,
                    }) = definition
                        .node(&source.activation.node)
                        .map(|node| node.step())
                    {
                        writes.insert(
                            0,
                            self.runtime
                                .app
                                .action(action, revision)?
                                .declaration()
                                .result_projection(&result),
                        );
                    }
                    StagedActivationRecord::action(
                        &source.activation,
                        &loaded.record.checkpoint.state,
                        &writes,
                        result,
                        self.runtime.app.typing(),
                    )
                }));
                match staged {
                    Ok(Ok(staged)) => self.stage_source(index, staged, ordinary),
                    Ok(Err(_)) => {
                        self.stage_source_failure(index, FailureCategory::State, ordinary)
                    }
                    Err(_) => {
                        self.stage_source_failure(index, FailureCategory::Execution, ordinary)
                    }
                }
            }
            Ok(None) => Ok(()),
            Err(category) => self.stage_source_failure(index, category, ordinary),
        }
    }

    fn stage_source_failure(
        &mut self,
        index: usize,
        category: FailureCategory,
        ordinary: bool,
    ) -> Result<(), AppError> {
        let source = &self.sources[index];
        self.runtime
            .retain_source_failure(&source.context, &source.activation.lineage, category);
        let staged = StagedActivationRecord::failed(
            source.activation.node.clone(),
            source.activation.lineage.clone(),
            category,
            "caller action execution failed".into(),
        );
        self.stage_source(index, staged, ordinary)
    }

    fn stage_source(
        &mut self,
        index: usize,
        staged: StagedActivationRecord,
        ordinary: bool,
    ) -> Result<(), AppError> {
        let SessionState::Sources { loaded } =
            std::mem::replace(&mut self.state, SessionState::Finished)
        else {
            unreachable!()
        };
        self.observed = Some(loaded.record.clone());
        self.publish(
            SessionPublication::Decision {
                loaded,
                decision: BspDecision::Stage(vec![staged]),
                activation_deadline: None,
                context: Some(Arc::clone(&self.sources[index].context)),
                staged: Vec::new(),
                remaining: Vec::new().into_iter(),
            },
            ordinary,
        )
    }

    fn finish_wave(&mut self, ordinary: bool) -> Result<(), AppError> {
        let SessionState::Sources { loaded } =
            std::mem::replace(&mut self.state, SessionState::Finished)
        else {
            unreachable!()
        };
        let definition = self.runtime.app.definition_for(&loaded.record)?;
        let failed = loaded
            .record
            .checkpoint
            .selected_staged(&definition)
            .filter_map(|activation| {
                if let StagedOutcomeRecord::Failed { category, message } = &activation.outcome {
                    Some((activation, *category, message))
                } else {
                    None
                }
            })
            .min_by(|left, right| {
                (&left.0.node, &left.0.lineage).cmp(&(&right.0.node, &right.0.lineage))
            });
        if let Some((activation, category, message)) = failed {
            let message = message.clone();
            let observation = match self.sources.iter().find(|source| {
                source.activation.node == activation.node
                    && source.activation.lineage == activation.lineage
            }) {
                Some(source) => ActionPosition {
                    node: source.context.node.clone(),
                    ordinal: source.context.action_ordinal,
                },
                None => loaded.record.checkpoint.proposed_position(
                    &definition,
                    &activation.node,
                    &activation.lineage,
                    &[],
                )?,
            };
            self.sources.clear();
            self.publish_decision(
                loaded,
                BspDecision::Failed {
                    category,
                    message,
                    observation: Some(observation),
                },
                ordinary,
            )
        } else if self
            .sources
            .iter()
            .any(|source| matches!(source.state, CallerSourceState::Eof))
        {
            self.sources.clear();
            self.publish(SessionPublication::EndSource { loaded }, ordinary)
        } else {
            self.sources.clear();
            self.observed = Some(loaded.record);
            self.state = SessionState::Ready;
            Ok(())
        }
    }

    fn begin_activation(
        &mut self,
        loaded: LoadedRun,
        activation: FrontierRecord,
        remaining: std::vec::IntoIter<FrontierRecord>,
        staged: Vec<StagedActivationRecord>,
        ordinary: bool,
    ) -> Result<(), AppError> {
        self.ensure_owned()?;
        let definition = self.runtime.app.definition_for(&loaded.record)?;
        if let Some(node) = definition.node(&activation.node) {
            node.step()
                .validate_mode(self.runtime.cell.execution_mode()?)?;
        }
        let (started, deadline_millis) = WorkflowRunRuntime::<CS, A, X, C, T, H, E>::node_deadline(
            &definition,
            &activation.node,
        )?;
        let deadline = tokio::time::Instant::now()
            .checked_add(Duration::from_millis(deadline_millis - started))
            .ok_or_else(|| {
                AppError::Timeout("activation deadline is outside the clock range".into())
            })?;
        let context = match self.runtime.cell.action_context(
            loaded.record.checkpoint.base_sequence,
            &activation.node,
            &activation.lineage,
        ) {
            Some(context) => context,
            None => Arc::new(loaded.record.action_context(
                &definition,
                &activation,
                &staged,
                self.runtime.cell.execution_mode()?,
            )?),
        };
        if loaded.record.checkpoint.loses_join_first(
            &definition,
            &activation.node,
            &activation.lineage,
        ) {
            self.state = SessionState::ActivationReady {
                loaded,
                activation,
                context,
                remaining,
                staged,
                deadline,
                deadline_millis,
            };
            return Ok(());
        }
        if let Some(Step::Action {
            action, revision, ..
        }) = definition.node(&activation.node).map(|node| node.step())
        {
            let binding = self.runtime.app.action(action, revision)?;
            let validated = catch_unwind(AssertUnwindSafe(|| {
                binding.declaration().validate_reads(&WorkflowState {
                    values: loaded.record.checkpoint.state.clone(),
                })?;
                binding.declaration().resolve_inputs(
                    &self.runtime.cell.execution_options()?.inputs,
                    self.runtime.cell.execution_mode()?,
                )
            }));
            match validated {
                Ok(Err(AppError::InvalidInput(message))) if self.generation.is_none() => {
                    return self.publish_decision(
                        loaded,
                        BspDecision::Failed {
                            category: FailureCategory::Execution,
                            message,
                            observation: None,
                        },
                        ordinary,
                    );
                }
                Ok(Ok(ActionInputs::Required(required))) => {
                    return self.publish(
                        SessionPublication::Decision {
                            loaded,
                            decision: BspDecision::Inputs {
                                node: activation.node,
                                lineage: activation.lineage,
                                required,
                            },
                            activation_deadline: Some(deadline_millis),
                            context: None,
                            staged,
                            remaining,
                        },
                        ordinary,
                    );
                }
                Ok(result) => {
                    result?;
                }
                Err(_) => {
                    return self.publish_failure(
                        loaded,
                        &activation,
                        &context,
                        FailureCategory::Execution,
                    );
                }
            }
        }
        self.publish(
            SessionPublication::BeginActivation {
                loaded,
                activation,
                context,
                remaining,
                staged,
                deadline,
                deadline_millis,
            },
            ordinary,
        )
    }

    fn activate_ready(&mut self, ordinary: bool) -> Result<(), AppError> {
        self.ensure_owned()?;
        let SessionState::ActivationReady {
            loaded,
            activation,
            context,
            mut remaining,
            mut staged,
            deadline,
            deadline_millis,
        } = std::mem::replace(&mut self.state, SessionState::Finished)
        else {
            unreachable!()
        };
        let definition = self.runtime.app.definition_for(&loaded.record)?;
        let skipped = loaded.record.checkpoint.loses_join_first(
            &definition,
            &activation.node,
            &activation.lineage,
        );
        if !skipped
            && matches!(
                definition.node(&activation.node).map(|node| node.step()),
                Some(Step::Action { .. })
            )
        {
            self.construct_source(&loaded, activation, context, deadline)?;
            return match remaining.next() {
                Some(activation) => {
                    self.begin_activation(loaded, activation, remaining, staged, ordinary)
                }
                None if staged.is_empty() => {
                    self.observed = None;
                    self.state = SessionState::Sources { loaded };
                    Ok(())
                }
                None => self.publish_decision(loaded, BspDecision::Stage(staged), ordinary),
            };
        }
        if !skipped && tokio::time::Instant::now() >= deadline {
            return self.publish_failure(loaded, &activation, &context, FailureCategory::Timeout);
        }
        let engine = ExecutionEngine::new(
            &definition,
            self.runtime.app.typing(),
            self.runtime.app.conditions(),
        );
        let decision = match catch_unwind(AssertUnwindSafe(|| {
            let decision = engine.activate(
                &loaded.record.checkpoint,
                &activation,
                loaded
                    .record
                    .parent
                    .as_ref()
                    .map_or(0, |parent| parent.depth),
                |workflow, revision| {
                    let revision = match revision {
                        Some(revision) => revision,
                        None => loaded.record.child_revision(&workflow)?.to_owned(),
                    };
                    self.runtime.app.definition_revision(&workflow, &revision)?;
                    Ok((workflow, revision))
                },
            )?;
            if let BspDecision::Descendants { inputs, .. } = &decision {
                let step = definition
                    .node(&activation.node)
                    .ok_or_else(|| {
                        AppError::Integration("child declaration is unavailable".into())
                    })?
                    .step();
                for task in inputs {
                    let child = self
                        .runtime
                        .app
                        .definition_revision(&task.workflow, &task.revision)?;
                    step.validate_child_binding(&definition, &child, &task.input)?;
                }
            }
            Ok(decision)
        })) {
            Ok(Ok(decision)) => decision,
            Ok(Err(error)) => BspDecision::Failed {
                category: FailureCategory::Evaluation,
                message: match error {
                    AppError::InvalidInput(message) => message,
                    _ => "workflow evaluation failed".into(),
                },
                observation: Some(loaded.record.checkpoint.proposed_position(
                    &definition,
                    &activation.node,
                    &activation.lineage,
                    &staged,
                )?),
            },
            Err(_) => BspDecision::Failed {
                category: FailureCategory::Execution,
                message: "workflow callback panicked".into(),
                observation: Some(loaded.record.checkpoint.proposed_position(
                    &definition,
                    &activation.node,
                    &activation.lineage,
                    &staged,
                )?),
            },
        };
        match decision {
            BspDecision::Stage(mut activations) => {
                staged.append(&mut activations);
                match remaining.next() {
                    Some(activation) => {
                        self.begin_activation(loaded, activation, remaining, staged, ordinary)
                    }
                    None => self.publish_decision(loaded, BspDecision::Stage(staged), ordinary),
                }
            }
            decision => self.publish(
                SessionPublication::Decision {
                    loaded,
                    decision,
                    activation_deadline: Some(deadline_millis),
                    context: Some(context),
                    staged,
                    remaining,
                },
                ordinary,
            ),
        }
    }

    fn construct_source(
        &mut self,
        loaded: &LoadedRun,
        activation: FrontierRecord,
        context: Arc<ActionContext>,
        deadline: tokio::time::Instant,
    ) -> Result<(), AppError> {
        if tokio::time::Instant::now() >= deadline {
            self.runtime.retain_source_failure(
                &context,
                &activation.lineage,
                FailureCategory::Timeout,
            );
            self.sources.push(CallerSource {
                activation,
                context,
                deadline,
                state: CallerSourceState::Failed(FailureCategory::Timeout),
            });
            return Ok(());
        }
        let definition = self.runtime.app.definition_for(&loaded.record)?;
        let Some(Step::Action {
            action, revision, ..
        }) = definition.node(&activation.node).map(|node| node.step())
        else {
            return Err(AppError::Integration(
                "action decision has no exact declaration".into(),
            ));
        };
        let binding = self.runtime.app.action(action, revision)?;
        let state = WorkflowState {
            values: loaded.record.checkpoint.state.clone(),
        };
        let constructed = catch_unwind(AssertUnwindSafe(|| {
            binding.declaration().validate_reads(&state)?;
            let inputs = binding
                .declaration()
                .resolve_inputs(
                    &self.runtime.cell.execution_options()?.inputs,
                    self.runtime.cell.execution_mode()?,
                )?
                .into_values()?;
            Ok::<_, AppError>(binding.start(ActionRequest {
                state,
                inputs,
                context: context.as_ref().clone(),
            }))
        }));
        let state = match constructed {
            Ok(Ok(source)) => CallerSourceState::Polling(Box::pin(source)),
            Ok(Err(error)) => return Err(error),
            Err(_) => CallerSourceState::Failed(FailureCategory::Execution),
        };
        if let CallerSourceState::Failed(category) = &state {
            self.runtime
                .retain_source_failure(&context, &activation.lineage, *category);
        }
        self.sources.push(CallerSource {
            activation,
            context,
            deadline,
            state,
        });
        self.expire_sources(None);
        Ok(())
    }

    fn halt_after(&mut self, loaded: &LoadedRun) -> Result<bool, AppError> {
        let outcomes = &loaded.record.checkpoint.outcomes;
        let Some(last) = outcomes.last() else {
            return Ok(false);
        };
        let options = self.runtime.cell.execution_options()?;
        let selected = outcomes
            .iter()
            .find(|outcome| options.halt_after.iter().any(|node| node == &outcome.node));
        let outcome = match selected {
            Some(outcome) => outcome,
            None if self.runtime.cell.halt_after(&last.node)? => last,
            None => return Ok(false),
        };
        self.publish_transition(
            LoadedRun {
                record: loaded.record.clone(),
                revision: loaded.revision,
            },
            ControlTransition::Halt {
                node: outcome.node.clone(),
                result: Some(outcome.result.clone()),
            },
        )?;
        Ok(true)
    }

    fn publish_item(&mut self, index: usize, event: ActionEvent) -> Result<(), AppError> {
        let SessionState::Sources { loaded } = &self.state else {
            return Err(AppError::Integration(
                "stream item has no source owner".into(),
            ));
        };
        let source = &self.sources[index];
        let context = Arc::clone(&source.context);
        let deadline = source.deadline;
        let generation = self.generation;
        if let Some(generation) = generation {
            self.runtime.cell.begin_publication(generation)?;
        }
        let runtime = self.runtime.clone();
        let loaded = LoadedRun {
            record: loaded.record.clone(),
            revision: loaded.revision,
        };
        let activation = source.activation.clone();
        let (sender, receipt) = oneshot::channel();
        let kind = generation.map_or(RunTaskKind::Operation, |generation| {
            RunTaskKind::Foreground { generation }
        });
        if !self
            .runtime
            .app
            .spawn_run(self.runtime.key.clone(), kind, None, async move {
                // The admitted observer owns its failure even if the caller
                // drops the source or never polls this receipt again.
                let mut result = match runtime
                    .observe_source_event(&context, event, deadline)
                    .await
                {
                    Ok(result) => Ok(result),
                    Err(failure) => Err(runtime
                        .fail_hook(&loaded, Some((&activation, &context)), failure, Vec::new())
                        .await),
                };
                if let Some(generation) = generation
                    && runtime.cell.end_publication(generation)
                    && let Err(error) = runtime.finish_execution(generation, None).await
                    && result.is_ok()
                {
                    result = Err(error);
                }
                let _ = sender.send(result);
            })
        {
            if let Some(generation) = generation {
                self.runtime.cell.end_publication(generation);
            }
            return Err(AppError::Closed(
                "stream observation admission closed".into(),
            ));
        }
        let SessionState::Sources { loaded } =
            std::mem::replace(&mut self.state, SessionState::Finished)
        else {
            unreachable!()
        };
        self.state = SessionState::ObservingItem {
            loaded,
            index,
            receipt,
        };
        Ok(())
    }

    fn publish_failure(
        &mut self,
        loaded: LoadedRun,
        activation: &FrontierRecord,
        context: &ActionContext,
        category: FailureCategory,
    ) -> Result<(), AppError> {
        let definition = self.runtime.app.definition_for(&loaded.record)?;
        let position = ActionPosition {
            node: context.node.clone(),
            ordinal: context.action_ordinal,
        };
        let (position, category, message) = match loaded.record.checkpoint.preceding_failure(
            &definition,
            &position,
            &activation.lineage,
        )? {
            Some((position, category, message)) => (position, category, message.to_owned()),
            None => (position, category, "caller action execution failed".into()),
        };
        self.publish_decision(
            loaded,
            BspDecision::Failed {
                category,
                message,
                observation: Some(position),
            },
            false,
        )
    }

    fn publish_decision(
        &mut self,
        loaded: LoadedRun,
        decision: BspDecision,
        ordinary: bool,
    ) -> Result<(), AppError> {
        self.publish(
            SessionPublication::Decision {
                loaded,
                decision,
                activation_deadline: None,
                context: None,
                staged: Vec::new(),
                remaining: Vec::new().into_iter(),
            },
            ordinary,
        )
    }

    fn publish_transition(
        &mut self,
        loaded: LoadedRun,
        transition: ControlTransition,
    ) -> Result<(), AppError> {
        self.publish(SessionPublication::Transition { loaded, transition }, false)
    }

    fn publish(
        &mut self,
        mut publication: SessionPublication,
        ordinary: bool,
    ) -> Result<(), AppError> {
        let mut staged_prefix = Vec::new();
        let mut admitted_inputs = Vec::new();
        if let SessionPublication::Decision {
            loaded,
            decision: BspDecision::AdmitChildren { sequence, children },
            ..
        } = &publication
            && let Some(group) = self
                .children
                .iter()
                .find(|group| group.sequence == *sequence)
        {
            let original = loaded
                .record
                .checkpoint
                .control(*sequence)
                .and_then(|control| match &control.state {
                    ControlRecordState::Child { admitted, .. } => Some(admitted.len()),
                    _ => None,
                })
                .ok_or_else(|| {
                    AppError::Integration("child admission evidence is unavailable".into())
                })?;
            if !group.inputs.is_empty() {
                for index in original..children.len() {
                    let values = group.inputs.get(index).ok_or_else(|| {
                        AppError::Integration("child runtime input membership changed".into())
                    })?;
                    // The original group retains this bounded map until its
                    // native admitted prefix is confirmed by the receipt.
                    admitted_inputs.push(values.clone());
                }
            }
        }
        let purpose = match &mut publication {
            SessionPublication::Decision {
                decision:
                    BspDecision::Descendants {
                        lineage,
                        runtime_inputs,
                        ..
                    },
                context,
                remaining,
                ..
            } => PublicationPurpose::Descendants {
                context: Arc::clone(context.as_ref().ok_or_else(|| {
                    AppError::Integration(
                        "child runtime inputs have no original action context".into(),
                    )
                })?),
                lineage: lineage.clone(),
                inputs: std::mem::take(runtime_inputs),
                remaining: std::mem::take(remaining),
                ordinary,
            },
            SessionPublication::Load { .. } => PublicationPurpose::Load,
            SessionPublication::Reload { .. } => PublicationPurpose::Reload,
            SessionPublication::ObserveChildren { .. } => PublicationPurpose::ObserveChildren,
            SessionPublication::VerifyChildren { .. } => PublicationPurpose::VerifyChildren,
            SessionPublication::FinishChild { .. } | SessionPublication::SettleStopping { .. } => {
                PublicationPurpose::Load
            }
            SessionPublication::Decision {
                decision: BspDecision::AdmitChildren { .. },
                ..
            } => PublicationPurpose::AdmitChildren,
            SessionPublication::BeginActivation {
                activation,
                context,
                remaining,
                staged,
                deadline,
                deadline_millis,
                ..
            } => {
                // The same receipt returns this prefix after successful hooks.
                // A rejected hook first preserves its native failure evidence.
                staged_prefix = std::mem::take(staged);
                PublicationPurpose::Start {
                    activation: activation.clone(),
                    context: Arc::clone(context),
                    remaining: std::mem::take(remaining),
                    deadline: *deadline,
                    deadline_millis: *deadline_millis,
                }
            }
            SessionPublication::Decision { remaining, .. } if !remaining.as_slice().is_empty() => {
                PublicationPurpose::Continue {
                    remaining: std::mem::take(remaining),
                    ordinary,
                }
            }
            SessionPublication::Decision {
                decision: BspDecision::Barrier { .. },
                ..
            } => PublicationPurpose::Commit { ordinary },
            SessionPublication::EndSource { .. } => PublicationPurpose::EndSource,
            _ => PublicationPurpose::Advance,
        };
        if let SessionPublication::Decision {
            decision: BspDecision::Agent(request),
            ..
        } = &mut publication
            && request.is_streaming()
        {
            let (sender, _) = self.native_partials.get_or_insert_with(|| mpsc::channel(1));
            request.observe_partials(sender.clone());
        }
        let generation = self.generation;
        if let Some(generation) = generation {
            self.runtime.cell.begin_publication(generation)?;
            if matches!(&publication, SessionPublication::Release) {
                self.runtime.cell.revoke_execution(generation);
            }
        }
        let child_observation = match &publication {
            SessionPublication::Reload { receipt, .. } => receipt
                .record()
                .checkpoint
                .controls
                .values()
                .filter(|control| {
                    matches!(control.state, ControlRecordState::Child { .. })
                        && !self.children.iter().any(|group| {
                            group.sequence == control.sequence
                                && matches!(
                                    group.state,
                                    ChildExecutionState::Worker(_)
                                        | ChildExecutionState::Receipt(_)
                                )
                        })
                })
                .min_by_key(|control| control.deadline_millis)
                .map(|control| control.sequence),
            _ => None,
        };
        let options = self.runtime.cell.execution_options()?;
        let parent_deadline = self.parent_deadline;
        let reconcile_read = generation.is_none()
            && self.runtime.cell.child_parent().is_some()
            && matches!(
                &publication,
                SessionPublication::Load { .. }
                    | SessionPublication::Reload { .. }
                    | SessionPublication::ObserveChildren { .. }
                    | SessionPublication::VerifyChildren { .. }
            );
        let execution_read = matches!(
            &publication,
            SessionPublication::Load { .. }
                | SessionPublication::Reload { .. }
                | SessionPublication::ObserveChildren { .. }
                | SessionPublication::FinishChild { .. }
                | SessionPublication::BeginActivation { .. }
        );
        let runtime = self.runtime.clone();
        let (sender, receipt) = oneshot::channel();
        let kind = generation.map_or(RunTaskKind::Operation, |generation| {
            RunTaskKind::Foreground { generation }
        });
        if !self
            .runtime
            .app
            .spawn_run(self.runtime.key.clone(), kind, self.admission.take(), async move {
                let inputs = &options.inputs;
                let observed = AssertUnwindSafe(async {
                    match publication {
                    SessionPublication::ObserveChildren { loaded, sequence } => async {
                        let (loaded, children) = runtime.observe_children(loaded, sequence).await?;
                        Ok(SessionReceipt::Children { loaded, sequence, children, verified: false })
                    }.await,
                    SessionPublication::VerifyChildren { parent, sequence, completions } => {
                        match runtime.verify_child_completions(&parent, sequence, completions).await {
                            Ok((loaded, children)) => {
                                let control = parent.checkpoint.control(sequence).ok_or_else(|| {
                                    AppError::Conflict("verified child control is unavailable".into())
                                })?;
                                let complete = matches!(&control.state,
                                    ControlRecordState::Child { inputs, .. } if children.len() == inputs.len());
                                if loaded.record.checkpoint.is_stopping() || loaded.record.checkpoint.is_terminal() || !complete {
                                    runtime.schedule_child_retention(control);
                                    Ok(SessionReceipt::Children { loaded, sequence, children, verified: true })
                                } else {
                                    // Keep final collection and Stage in this accepted
                                    // publication even when its local waiter is dropped.
                                    // A partial Pipeline remains caller-owned above.
                                    let transition = runtime.plan_child_completion(&loaded, control, children)?;
                                    runtime.publish_child_outcome(control, transition).await
                                }
                            },
                            Err(RuntimeOperationFailure::Application(error)) => runtime
                                .finish_child_admission(sequence, ChildAdmissionReceipt::Rejected(error))
                                .await
                                .map(|(loaded, children)| SessionReceipt::Children {
                                    loaded, sequence, children, verified: false,
                                }),
                            Err(failure) => Err(failure),
                        }
                    },
                    SessionPublication::Decision { loaded, decision: BspDecision::AdmitChildren { sequence, children }, .. } => {
                        let receipt = runtime.register_children(loaded, sequence, children, admitted_inputs).await;
                        runtime.finish_child_admission(sequence, receipt).await.map(|(loaded, children)| {
                            SessionReceipt::Children { loaded, sequence, children, verified: false }
                        })
                    },
                    SessionPublication::ChildOutcome { control, transition } => {
                        runtime.publish_child_outcome(&control, transition).await
                    },
                    SessionPublication::FinishChild { parent, sequence } => async {
                        let loaded = runtime.load_execution_observation(None, parent_deadline).await?
                            .ok_or_else(|| AppError::NotFound("admitted child is unavailable".into()))?;
                        let definition = runtime.app.definition_for(&parent)?;
                        loaded.record.validate_child(&parent, &definition)?;
                        if loaded.record.parent.as_ref().is_none_or(|relation| relation.control_sequence != sequence) {
                            return Err(AppError::Conflict("child completion group changed".into()).into());
                        }
                        if loaded.record.checkpoint.is_stopping()
                            || loaded.record.checkpoint.is_terminal()
                            || !loaded.record.checkpoint.controls.values().any(|control| {
                                matches!(control.state, ControlRecordState::Inputs { .. })
                            })
                        {
                            runtime.finish_execution_observation(&loaded.record, None, None, false).await?;
                            runtime.cell.lifecycle_observation(loaded.record.checkpoint.sequence, false)?;
                        }
                        Ok(SessionReceipt::Checkpoint { loaded, staged: Vec::new() })
                    }.await,
                    SessionPublication::SettleStopping { loaded } => async {
                        if let Some(loaded) = Box::pin(runtime.settle_stopping(loaded)).await {
                            let observed = runtime.finish_execution_observation(&loaded.record, None, None, false).await;
                            if runtime.cell.child_parent().is_none() {
                                runtime.maintain_retention(None).await;
                            }
                            observed?;
                        }
                        // Retention may have advanced the head revision after
                        // the terminal checkpoint. Observe that current authority
                        // in one finite pass after the native receipt returns.
                        let loaded = runtime.load_execution_observation(None, parent_deadline).await
                            .inspect_err(|failure| runtime.retain_execution_read_failure(failure))?
                            .ok_or_else(|| AppError::NotFound("workflow run does not exist".into()))?;
                        Ok(SessionReceipt::Checkpoint { loaded, staged: Vec::new() })
                    }.await,
                    SessionPublication::Load { terminal } => async {
                        let loaded = runtime.load_execution_observation(terminal, parent_deadline).await?
                            .ok_or_else(|| AppError::NotFound("workflow run does not exist".into()))?;
                        if !loaded.record.checkpoint.is_stopping()
                            && !loaded.record.checkpoint.is_terminal()
                            && loaded.record.checkpoint.controls.values().any(|control| {
                                matches!(control.state, ControlRecordState::Inputs { .. })
                            })
                            && let Err(failure) = runtime.begin_execution_observation().await
                        {
                            return Err(runtime.fail_hook(&loaded, None, failure, Vec::new()).await.into());
                        }
                        Ok(SessionReceipt::Checkpoint { loaded, staged: Vec::new() })
                    }.await,
                    SessionPublication::Reload { receipt, deadline } => {
                        let observed = tokio::select! {
                            biased;
                            _ = async { match deadline {
                                Some(deadline) => tokio::time::sleep_until(deadline).await,
                                None => std::future::pending().await,
                            }}, if child_observation.is_none() && parent_deadline.is_none() => Ok(None),
                            observed = async {
                                // Child authority retains its own sampled deadline;
                                // it cannot fall back to a stale receipt on timeout.
                                // The original worker retains its own exact child
                                // watchdog and final receipt after staging its result.
                                let observed = match child_observation {
                                    Some(sequence) => runtime.observe_child_parent(receipt.record(), sequence, false, None).await?,
                                    None => runtime.load_execution_observation(None, deadline.into_iter().chain(parent_deadline).min()).await?
                                        .ok_or_else(|| AppError::NotFound("workflow run does not exist".into()))?,
                                };
                                Ok::<_, RuntimeOperationFailure>(Some(observed))
                            } => observed,
                        };
                        observed.map(|observed| match observed {
                            Some(loaded) => receipt.with_loaded(loaded),
                            None => receipt,
                        })
                    },
                    SessionPublication::BeginActivation { loaded, activation, context, deadline, .. } => async {
                        if let Err(failure) = runtime.observe_activation_start(&loaded, &activation, &context, inputs, deadline).await {
                            return Err(runtime.fail_hook(&loaded, Some((&activation, &context)), failure, std::mem::take(&mut staged_prefix)).await.into());
                        }
                        let current = runtime.load_execution_observation(
                            None, parent_deadline.into_iter().chain(Some(deadline)).min(),
                        ).await?.ok_or_else(|| AppError::NotFound("workflow run does not exist".into()))?;
                        if !(current.record.checkpoint.is_terminal()
                            || current.record.checkpoint.is_stopping()
                            || current.record.same_run(&loaded.record)
                                && current.record.checkpoint.base_sequence == loaded.record.checkpoint.base_sequence
                                && current.record.checkpoint.frontier.iter().any(|candidate| candidate == &activation)
                                && current.record.checkpoint.control_for(&activation.node, &activation.lineage).is_none()
                                && !current.record.checkpoint.staged.iter().any(|staged| staged.node == activation.node && staged.lineage == activation.lineage))
                        {
                            return Err(AppError::Conflict("action pre-hook authority changed".into()).into());
                        }
                        Ok(SessionReceipt::Checkpoint { loaded: current, staged: std::mem::take(&mut staged_prefix) })
                    }.await,
                    publication => async {
                    match publication {
                        SessionPublication::Load { .. } | SessionPublication::Reload { .. }
                        | SessionPublication::ObserveChildren { .. } | SessionPublication::VerifyChildren { .. }
                        | SessionPublication::ChildOutcome { .. } | SessionPublication::FinishChild { .. } | SessionPublication::SettleStopping { .. }
                        | SessionPublication::BeginActivation { .. } => unreachable!(),
                        SessionPublication::Decision { loaded, decision, activation_deadline, context, staged, .. } => {
                            if matches!(decision, BspDecision::Inputs { .. })
                                && let Err(failure) = runtime.begin_execution_observation().await
                            {
                                return Err(runtime.fail_hook(&loaded, None, failure, staged).await);
                            }
                            runtime.finish_staged_spans(&loaded, &decision, context.as_deref()).await?;
                            runtime.apply_decision(loaded, decision, activation_deadline, staged).await
                        }
                        SessionPublication::Transition { loaded, transition } => {
                            let loaded = runtime.commit_for_recovery(loaded, transition).await
                                .map_err(RuntimeOperationFailure::into_error)?;
                            // The native checkpoint remains this publication's
                            // exact result. Only its App generation needs a
                            // finite post-receipt observation before returning.
                            runtime.observe_generation(LifecycleEvent::deadline(parent_deadline)).await
                                .map_err(|failure| {
                                    runtime.retain_execution_read_failure(&failure);
                                    failure.into_error()
                                })?;
                            Ok(loaded)
                        }
                        SessionPublication::Retry { loaded } => {
                            let loaded = runtime.readmit_failed(loaded).await?;
                            runtime.observe_generation(LifecycleEvent::deadline(parent_deadline)).await
                                .map_err(|failure| {
                                    runtime.retain_execution_read_failure(&failure);
                                    failure.into_error()
                                })?;
                            Ok(loaded)
                        }
                        SessionPublication::EndSource { loaded } => {
                            let loaded = runtime.end_source(loaded, generation).await?;
                            runtime.observe_generation(LifecycleEvent::deadline(parent_deadline)).await
                                .map_err(|failure| {
                                    runtime.retain_execution_read_failure(&failure);
                                    failure.into_error()
                                })?;
                            Ok(loaded)
                        }
                        SessionPublication::Release => {
                            if let Some(generation) = generation { runtime.finish_execution(generation, None).await?; }
                            runtime.load_execution_observation(None, parent_deadline).await
                                .map_err(|failure| {
                                    runtime.retain_execution_read_failure(&failure);
                                    failure.into_error()
                                })?
                                .ok_or_else(|| AppError::NotFound("workflow run does not exist".into()))
                        }
                    }
                    }.await.map(|loaded| SessionReceipt::Checkpoint { loaded, staged: std::mem::take(&mut staged_prefix) })
                        .map_err(RuntimeOperationFailure::from),
                    }
                }).catch_unwind().await;
                let mut result: Result<SessionReceipt, RuntimeOperationFailure> = match observed {
                    Ok(result) => result,
                    Err(payload) if reconcile_read => {
                        drop(payload);
                        match runtime.reconcile_child_panic(parent_deadline).await {
                            Ok(result) => result.map(|loaded| SessionReceipt::Checkpoint {
                                loaded, staged: Vec::new(),
                            }).map_err(RuntimeOperationFailure::from),
                            Err(_) => {
                                runtime.app.tasks_failed(FinalResult::Integration(
                                    "child read panic reconciliation receipt closed".into(),
                                ));
                                Err(AppError::Integration("child read panic reconciliation receipt closed".into()).into())
                            }
                        }
                    }
                    Err(payload) => std::panic::resume_unwind(payload),
                };
                if execution_read
                    && let Err(RuntimeOperationFailure::Storage(failure)) = &result
                {
                    runtime.retain_execution_read_failure(failure);
                }
                if let Some(generation) = generation
                    && runtime.cell.end_publication(generation)
                {
                    let loaded = match &result {
                        Ok(SessionReceipt::Checkpoint { loaded, .. })
                            if (loaded.record.checkpoint.is_paused() || loaded.record.checkpoint.is_stopping())
                                && loaded.record.checkpoint.controls.is_empty()
                                && runtime.cell.lifecycle_generation().is_none()
                                && !runtime.app.has_child_work(&runtime.key) =>
                        {
                            // The publication receipt remains available to a
                            // consuming close waiter while cleanup borrows its
                            // already-settled native authority.
                            Some(LoadedRun { record: loaded.record.clone(), revision: loaded.revision })
                        }
                        _ => None,
                    };
                    if let Err(error) = runtime.finish_execution(generation, loaded).await
                        && result.is_ok()
                    {
                        result = Err(error.into());
                    }
                }
                let _ = sender.send(result);
            })
        {
            if let Some(generation) = generation {
                self.runtime.cell.end_publication(generation);
            }
            return Err(AppError::Closed(
                "run publication admission is closed".into(),
            ));
        }
        self.state = SessionState::Publishing { receipt, purpose };
        Ok(())
    }

    fn poll_publication(
        &mut self,
        cx: &mut Context<'_>,
        closing: bool,
    ) -> Poll<Result<Option<ExecutionEvent>, AppError>> {
        if !matches!(&self.state, SessionState::Publishing { .. }) {
            return Poll::Ready(Ok(None));
        }
        self.expire_sources(None);
        let deadline = self.source_deadline(None);
        if self.poll_timer(cx, deadline) {
            self.expire_sources(None);
        }
        let commit = matches!(
            &self.state,
            SessionState::Publishing {
                purpose: PublicationPurpose::Commit { .. },
                ..
            }
        );
        if commit && let Some(event) = self.poll_native_partial(cx) {
            return Poll::Ready(Ok(Some(event)));
        }
        let SessionState::Publishing { receipt, .. } = &mut self.state else {
            unreachable!()
        };
        let observed = match Pin::new(receipt).poll(cx) {
            Poll::Ready(observed) => observed
                .map_err(|_| {
                    RuntimeOperationFailure::from(AppError::Closed(
                        "run publication observation closed".into(),
                    ))
                })
                .and_then(|observed| observed),
            Poll::Pending => {
                if let Some(event) = self.poll_native_partial(cx) {
                    return Poll::Ready(Ok(Some(event)));
                }
                return Poll::Pending;
            }
        };
        let SessionState::Publishing { purpose, .. } =
            std::mem::replace(&mut self.state, SessionState::Finished)
        else {
            unreachable!()
        };
        let observed = match observed {
            Ok(observed) => observed,
            Err(failure) => {
                if self.generation.is_none() {
                    if matches!(
                        purpose,
                        PublicationPurpose::Load
                            | PublicationPurpose::Reload
                            | PublicationPurpose::ObserveChildren
                            | PublicationPurpose::VerifyChildren
                    ) {
                        if self.runtime.schedule_operation_failure(&failure) {
                            self.observed = None;
                        }
                    } else if matches!(purpose, PublicationPurpose::AdmitChildren) {
                        // The original admission result already ran its exact
                        // retry/failure policy. Do not spend its budget twice.
                        self.observed = None;
                    }
                }
                return Poll::Ready(Err(failure.into_error()));
            }
        };
        let (loaded, staged) = match observed {
            SessionReceipt::Checkpoint { loaded, staged } => (loaded, staged),
            SessionReceipt::Children {
                loaded,
                sequence,
                children,
                verified,
            } => {
                if closing {
                    self.state = SessionState::Loaded(loaded);
                } else if matches!(purpose, PublicationPurpose::AdmitChildren)
                    && self.poll_notification(cx)?
                {
                    let deadline = loaded
                        .record
                        .checkpoint
                        .control(sequence)
                        .map(|control| {
                            WorkflowRunRuntime::<CS, A, X, C, T, H, E>::remaining(
                                control.deadline_millis,
                            )
                        })
                        .transpose()?
                        .and_then(|remaining| tokio::time::Instant::now().checked_add(remaining));
                    self.publish(
                        SessionPublication::Reload {
                            receipt: SessionReceipt::Children {
                                loaded,
                                sequence,
                                children,
                                verified,
                            },
                            deadline,
                        },
                        false,
                    )?;
                } else {
                    self.plan_child_group(loaded, sequence, children, verified)?;
                }
                return Poll::Ready(Ok(None));
            }
        };
        if matches!(purpose, PublicationPurpose::Commit { .. })
            && let Err(error) = self
                .runtime
                .cell
                .lifecycle_observation(loaded.record.checkpoint.sequence, false)
        {
            // A failed post-hook may already have paused this exact native
            // barrier. Keep its receipt for owning cleanup before projecting
            // the original retained lifecycle failure to the caller.
            self.state = SessionState::Loaded(loaded);
            return Poll::Ready(Err(error));
        }
        if closing
            || matches!(
                purpose,
                PublicationPurpose::Load | PublicationPurpose::Reload
            )
        {
            self.state = SessionState::Loaded(loaded);
            return Poll::Ready(Ok(None));
        }
        self.state = SessionState::Ready;
        if let PublicationPurpose::Start {
            activation,
            context,
            remaining,
            deadline,
            deadline_millis,
        } = purpose
        {
            self.state = if loaded.record.checkpoint.is_terminal()
                || loaded.record.checkpoint.is_stopping()
            {
                self.observed = Some(loaded.record);
                SessionState::Finished
            } else {
                SessionState::ActivationReady {
                    loaded,
                    activation,
                    context,
                    remaining,
                    staged,
                    deadline,
                    deadline_millis,
                }
            };
            return Poll::Ready(Ok(None));
        }
        let purpose = match purpose {
            PublicationPurpose::Descendants {
                context,
                lineage,
                inputs,
                remaining,
                ordinary,
            } => {
                if !loaded.record.checkpoint.is_terminal()
                    && !loaded.record.checkpoint.is_stopping()
                {
                    let control = loaded
                        .record
                        .checkpoint
                        .control_for(&context.node, &lineage)
                        .ok_or_else(|| {
                            AppError::Integration(
                                "original child input control is unavailable".into(),
                            )
                        })?;
                    let deadline = tokio::time::Instant::now()
                        .checked_add(WorkflowRunRuntime::<CS, A, X, C, T, H, E>::remaining(
                            control.deadline_millis,
                        )?)
                        .ok_or_else(|| {
                            AppError::Timeout("child deadline is outside the clock range".into())
                        })?;
                    self.children.push(ChildExecution {
                        sequence: control.sequence,
                        parent: None,
                        context,
                        lineage,
                        deadline,
                        state: ChildExecutionState::Awaiting,
                        inputs,
                    });
                }
                if remaining.as_slice().is_empty() {
                    PublicationPurpose::Advance
                } else {
                    PublicationPurpose::Continue {
                        remaining,
                        ordinary,
                    }
                }
            }
            purpose => purpose,
        };
        if let PublicationPurpose::Continue {
            mut remaining,
            ordinary,
        } = purpose
        {
            if loaded.record.checkpoint.is_terminal() || loaded.record.checkpoint.is_stopping() {
                self.observed = Some(loaded.record);
                self.state = SessionState::Finished;
            } else {
                let activation = remaining.next().ok_or_else(|| {
                    AppError::Integration("native activation continuation is empty".into())
                })?;
                self.begin_activation(loaded, activation, remaining, staged, ordinary)?;
            }
            return Poll::Ready(Ok(None));
        }
        if let RunAdmission::Halted { node, result } = &loaded.record.checkpoint.admission {
            let event = ExecutionEvent::Halted {
                node: node.clone(),
                result: result.clone(),
            };
            self.observed = Some(loaded.record);
            self.state = SessionState::Finished;
            return Poll::Ready(Ok(Some(event)));
        }
        if let PublicationPurpose::Commit { ordinary } = purpose {
            self.observed = Some(loaded.record.clone());
            self.runtime
                .cell
                .record_execution_boundary(loaded.record.checkpoint.outcomes.len(), ordinary)?;
            return Poll::Ready(Ok(Some(ExecutionEvent::Committed {
                checkpoint: loaded.record.checkpoint.sequence,
                outcomes: loaded.record.checkpoint.outcomes,
            })));
        }
        if matches!(purpose, PublicationPurpose::EndSource) {
            self.observed = Some(loaded.record);
            self.state = SessionState::Eof;
        } else {
            self.observed = None;
            self.state = SessionState::Loaded(loaded);
        }
        Poll::Ready(Ok(None))
    }
}

impl<
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend,
    C: ConditionBackend,
    T: StateTyping,
    H: LifecycleHooks,
    E: ChildExecutor,
> Drop for Execution<'_, CS, A, X, C, T, H, E>
{
    fn drop(&mut self) {
        if let Some(generation) = self.generation {
            self.runtime.cell.clear_execution_options(Some(generation));
        }
        let pending_observation = matches!(
            &self.state,
            SessionState::Publishing { .. } | SessionState::ObservingItem { .. }
        );
        self.stop_sources();
        self.stop_children();
        if self.generation.is_none()
            && !pending_observation
            && matches!(&self.state, SessionState::Sources { .. })
        {
            let failed = self
                .sources
                .iter()
                .enumerate()
                .filter_map(|(index, source)| {
                    let category = self
                        .prefetched
                        .as_ref()
                        .filter(|(buffered, _)| *buffered == index)
                        .and_then(|(_, event)| event.as_ref().err().copied())
                        .or(match source.state {
                            CallerSourceState::Failed(category) => Some(category),
                            _ => None,
                        });
                    category.map(|category| (index, category))
                })
                .min_by(|left, right| {
                    let left = &self.sources[left.0].activation;
                    let right = &self.sources[right.0].activation;
                    (&left.node, &left.lineage).cmp(&(&right.node, &right.lineage))
                });
            if let Some((index, category)) = failed {
                let _ = self.stage_source_failure(index, category, false);
            }
        }
        let source_failed = self
            .runtime
            .cell
            .lifecycle_generation()
            .is_some_and(|generation| self.runtime.cell.source_failure(generation).is_some());
        let record = match &self.state {
            SessionState::Loaded(loaded)
            | SessionState::Sources { loaded }
            | SessionState::ObservingItem { loaded, .. }
            | SessionState::ActivationReady { loaded, .. } => Some(&loaded.record),
            _ => self.observed.as_ref(),
        };
        let child_pending = self.runtime.app.has_child_work(&self.runtime.key);
        let local_only = !child_pending
            && !pending_observation
            && !matches!(&self.state, SessionState::Publishing { .. })
            && !source_failed
            && !self.sources.is_empty()
            && record.is_some_and(|record| {
                record.checkpoint.staged.is_empty() && record.checkpoint.controls.is_empty()
            });
        if !child_pending
            && !pending_observation
            && !matches!(&self.state, SessionState::Publishing { .. })
            && !source_failed
            && let Some(record) = record
            && !self.retains_execution_lifecycle(record)
            && (self.generation.is_some()
                || record.checkpoint.controls.is_empty()
                || record.checkpoint.is_public_outcome())
            && !record
                .checkpoint
                .staged
                .iter()
                .any(|activation| matches!(activation.outcome, StagedOutcomeRecord::Failed { .. }))
        {
            let _ = self.runtime.finish_execution_observation_sync(record);
        }
        if let Some(generation) = self.generation.take()
            && self.runtime.cell.drop_execution(generation)
        {
            if local_only {
                if self.runtime.cell.release_execution(generation) {
                    self.runtime.schedule(ScheduleCause::Reconcile);
                }
                return;
            }
            let loaded = if !child_pending
                && !pending_observation
                && !source_failed
                && self.runtime.cell.lifecycle_generation().is_none()
                && matches!(&self.state, SessionState::Loaded(loaded)
                    if (loaded.record.checkpoint.is_paused() || loaded.record.checkpoint.is_stopping())
                        && loaded.record.checkpoint.controls.is_empty())
            {
                let SessionState::Loaded(loaded) =
                    std::mem::replace(&mut self.state, SessionState::Finished)
                else {
                    unreachable!()
                };
                Some(loaded)
            } else {
                None
            };
            let runtime = self.runtime.clone();
            self.runtime.app.spawn_run(
                self.runtime.key.clone(),
                RunTaskKind::Foreground { generation },
                None,
                async move {
                    let _ = runtime.finish_execution(generation, loaded).await;
                },
            );
        }
    }
}

impl<
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend,
    C: ConditionBackend,
    T: StateTyping,
    H: LifecycleHooks,
    E: ChildExecutor,
> WorkflowRunRuntime<CS, A, X, C, T, H, E>
{
    pub(crate) async fn execute_child_group(
        &self,
        sequence: u64,
        generation: Option<u64>,
        parent: Arc<RunRecord>,
        children: Vec<RunRecord>,
    ) -> Result<SessionReceipt, RuntimeOperationFailure> {
        if generation.is_some_and(|generation| !self.cell.execution_is_live(generation)) {
            return Err(
                AppError::Closed("child executor admission generation changed".into()).into(),
            );
        }
        let mut outcome_published = false;
        let result = async {
            let control = parent.checkpoint.control(sequence).ok_or_else(|| AppError::Conflict("child control is unavailable".into()))?;
            let remaining = Self::remaining(control.deadline_millis)?;
            if remaining.is_zero() {
                // Queue time belongs to the original activation. Publish its
                // fenced Timeout without constructing E or starting a read
                // against an already exhausted observation budget.
                outcome_published = true;
                let loaded = self.commit_scheduled_transition(control, ControlTransition::Failed {
                    category: FailureCategory::Timeout, message: "child workflow deadline expired".into(), observation: None,
                }).await.ok_or_else(|| AppError::Closed(
                    "child timeout publication requires reconciliation".into(),
                ))?;
                return Ok(SessionReceipt::Checkpoint { loaded, staged: Vec::new() });
            }
            let deadline = tokio::time::Instant::now().checked_add(remaining)
                .ok_or_else(|| AppError::Timeout("child deadline is outside the clock range".into()))?;
            // Retain the original ordered child records. Only parent authority
            // needs a fresh observation after waiting in the worker queue.
            let current = self.load_execution_observation(None, Some(deadline)).await?
                .ok_or_else(|| AppError::NotFound("child parent is unavailable".into()))?;
            if current.record.checkpoint.is_stopping() || current.record.checkpoint.is_terminal() {
                return Ok(SessionReceipt::Checkpoint { loaded: current, staged: Vec::new() });
            }
            if !current.record.owns_child_activation(&parent, sequence) {
                return Err(AppError::Conflict("child parent reservation changed".into()).into());
            }
            drop(current);
            let context = self.child_context(&parent, sequence)?;
            let mut group = ChildExecution {
                sequence, parent: Some(Arc::clone(&parent)), context,
                lineage: control.lineage.clone(), deadline,
                state: ChildExecutionState::Prepared(Vec::new()), inputs: Vec::new(),
            };
            group.start_local(self, &self.app, ExecutionLane::Worker, children)?;
            let context = Arc::clone(&group.context);
            let stopped = {
                let mut executor = Box::pin(poll_fn(|cx| group.poll_ready(cx, self)));
                let mut stopping = Box::pin(self.observe_child_parent(&parent, sequence, true, None));
                tokio::select! {
                    biased;
                    result = &mut stopping => {
                        let loaded = result?;
                        let loaded = if !loaded.record.checkpoint.is_stopping() {
                            self.retain_source_failure(&context, &control.lineage, FailureCategory::Timeout);
                            outcome_published = true;
                            let mut failure = Box::pin(self.commit_scheduled_transition(control, ControlTransition::Failed {
                                category: FailureCategory::Timeout, message: "child workflow deadline expired".into(), observation: None,
                            }));
                            let committed = tokio::select! {
                                _ = &mut executor => failure.await,
                                result = &mut failure => result,
                            };
                            committed.ok_or_else(|| AppError::Closed(
                                "child timeout publication requires reconciliation".into(),
                            ))?
                        } else { loaded };
                        Some(loaded)
                    }
                    _ = &mut executor => None,
                }
            };
            if let Some(loaded) = stopped {
                // The original parent observer or native timeout publication
                // already supplied authority. E destruction does not require
                // another head read before returning that exact receipt.
                group.stop(self);
                return Ok(SessionReceipt::Checkpoint { loaded, staged: Vec::new() });
            }
            let ChildExecutionState::Complete(completed) = group.state else {
                return Err(AppError::Integration("child worker completed without its local executor".into()).into());
            };
            let completions = match completed {
                Ok(completions) => completions,
                Err(category) => {
                    outcome_published = true;
                    let loaded = self.commit_scheduled_transition(control, ControlTransition::Failed {
                        category, message: "child execution failed".into(), observation: None,
                    }).await.ok_or_else(|| AppError::Closed(
                        "child failure publication requires reconciliation".into(),
                    ))?;
                    return Ok(SessionReceipt::Checkpoint { loaded, staged: Vec::new() });
                }
            };
            let (loaded, children) = self.verify_child_completions(&parent, sequence, completions).await?;
            if loaded.record.checkpoint.is_stopping() || loaded.record.checkpoint.is_terminal() {
                self.schedule_child_retention(control);
                return Ok(SessionReceipt::Checkpoint { loaded, staged: Vec::new() });
            }
            let transition = self.plan_child_completion(&loaded, control, children)?;
            // The original worker publishes only its fenced stage before
            // sending. Dropping either a pending or queued caller receipt
            // cannot lose completed children or permit another E invocation.
            outcome_published = true;
            self.publish_child_outcome(control, transition).await
        }.await;
        match result {
            Ok(receipt) => Ok(receipt),
            Err(RuntimeOperationFailure::Storage(failure)) => {
                self.retain_execution_read_failure(&failure);
                self.schedule_after_storage_failure(&failure);
                Err(failure.into())
            }
            Err(RuntimeOperationFailure::Application(error)) => {
                if outcome_published {
                    return Err(error.into());
                }
                let failure = self
                    .finish_child_admission(sequence, ChildAdmissionReceipt::Rejected(error))
                    .await;
                let loaded = self
                    .load_execution_observation(None, None)
                    .await
                    .inspect_err(|failure| self.retain_execution_read_failure(failure))?
                    .ok_or_else(|| AppError::NotFound("child parent is unavailable".into()))?;
                if loaded.record.checkpoint.is_stopping() || loaded.record.checkpoint.is_terminal()
                {
                    Ok(SessionReceipt::Checkpoint {
                        loaded,
                        staged: Vec::new(),
                    })
                } else {
                    match failure {
                        Err(failure) => Err(failure),
                        Ok(_) => Err(AppError::Integration(
                            "child failure policy returned admission success".into(),
                        )
                        .into()),
                    }
                }
            }
        }
    }

    fn schedule_child_retention(&self, control: &ControlRecord) {
        let ControlRecordState::Child { admitted, .. } = &control.state else {
            return;
        };
        for member in admitted {
            match Self::attach(Arc::clone(&self.app), member.key.clone()) {
                Ok(child) => {
                    child.schedule(ScheduleCause::Reconcile);
                }
                Err(error) => self.app.tasks_failed(FinalResult::from(&error)),
            }
        }
    }

    fn plan_child_completion(
        &self,
        loaded: &LoadedRun,
        control: &ControlRecord,
        children: Vec<RunRecord>,
    ) -> Result<ControlTransition, AppError> {
        let planned = catch_unwind(AssertUnwindSafe(|| {
            self.plan_children(loaded, control.sequence, children, true)
        }))
        .map_err(|_| AppError::Integration("child planning callback panicked".into()))
        .and_then(|result| result)
        .and_then(|decision| match decision {
            BspDecision::Stage(activations) => Ok(ControlTransition::Stage { activations }),
            BspDecision::Failed {
                category,
                message,
                observation,
            } => Ok(ControlTransition::Failed {
                category,
                message,
                observation,
            }),
            _ => Err(AppError::Integration(
                "completed child group has no final outcome".into(),
            )),
        });
        if !matches!(&planned, Ok(ControlTransition::Stage { activations })
            if activations.iter().all(|activation| matches!(activation.outcome, StagedOutcomeRecord::Complete { .. })))
        {
            // Failed children, output validation, expired deadlines and planning
            // errors keep their existing independent terminal cleanup.
            self.schedule_child_retention(control);
        }
        planned
    }

    async fn publish_child_outcome(
        &self,
        control: &ControlRecord,
        transition: ControlTransition,
    ) -> Result<SessionReceipt, RuntimeOperationFailure> {
        // Retain the verified result under its original Child control while
        // unrelated sibling resolutions advance the parent revision. Both
        // local and worker execution keep this one publication receipt.
        let completed = matches!(&transition, ControlTransition::Stage { activations }
            if activations.iter().all(|activation| matches!(activation.outcome, StagedOutcomeRecord::Complete { .. })));
        if let Some(loaded) = self.commit_scheduled_transition(control, transition).await {
            // The native commit owner already retained Applied authority or
            // reconciled Unknown. Return that exact receipt and revision.
            return Ok(SessionReceipt::Checkpoint {
                loaded,
                staged: Vec::new(),
            });
        }
        if completed {
            // No applied Stage receipt owns the batch. The original exact
            // terminal children retain independent cleanup, including a stop
            // that won before this publication or unresolved native authority.
            self.schedule_child_retention(control);
        }
        let loaded = match tokio::time::timeout(
            Self::remaining(control.deadline_millis)?,
            self.load_for_recovery(),
        )
        .await
        {
            Ok(result) => result
                .map_err(RuntimeOperationFailure::from)
                .and_then(|loaded| {
                    loaded.ok_or_else(|| {
                        AppError::NotFound("child parent is unavailable".into()).into()
                    })
                }),
            Err(_) => Err(AppError::Timeout("child outcome observation timed out".into()).into()),
        };
        if !loaded.as_ref().is_ok_and(|loaded| {
            loaded.record.checkpoint.is_stopping() || loaded.record.checkpoint.is_terminal()
        }) {
            self.app.tasks_failed(FinalResult::Integration(
                "child outcome publication authority is unresolved".into(),
            ));
            return Err(loaded.err().unwrap_or_else(|| {
                AppError::Closed("child outcome publication requires reconciliation".into()).into()
            }));
        }
        Ok(SessionReceipt::Checkpoint {
            loaded: loaded?,
            staged: Vec::new(),
        })
    }

    fn retain_source_failure(
        &self,
        context: &ActionContext,
        lineage: &str,
        category: FailureCategory,
    ) {
        let Some(generation) = self.cell.lifecycle_generation() else {
            return;
        };
        match self
            .cell
            .observe_source_failure(generation, context, lineage, category)
        {
            Ok(()) | Err(AppError::Closed(_)) => {
                // Completed guards have already projected authoritative truth.
                // A later source destructor cannot reopen that observation.
            }
            Err(_) => self.app.tasks_failed(FinalResult::Integration(
                "source failure observation authority failed".into(),
            )),
        }
    }

    async fn end_source(
        &self,
        loaded: LoadedRun,
        execution: Option<u64>,
    ) -> Result<LoadedRun, AppError> {
        let generation = self
            .cell
            .lifecycle_generation()
            .ok_or_else(|| AppError::Closed("source cleanup observation is unavailable".into()))?;
        let deadline = self.cell.begin_action_cleanup(generation)?;
        // EOF rolls back the complete uncommitted wave. Close its original
        // guards before discarding any evidence a failing hook may need.
        for context in self.cell.action_observations() {
            if context.checkpoint != loaded.record.checkpoint.base_sequence {
                continue;
            }
            let mut observed_activation = None;
            for activation in &loaded.record.checkpoint.frontier {
                if self
                    .cell
                    .action_context(
                        loaded.record.checkpoint.base_sequence,
                        &activation.node,
                        &activation.lineage,
                    )
                    .is_some_and(|observed| Arc::ptr_eq(&observed, &context))
                {
                    observed_activation = Some(activation);
                    break;
                }
            }
            let activation = observed_activation.ok_or_else(|| {
                AppError::Integration("source cleanup activation is unavailable".into())
            })?;
            if let Err(failure) = self
                .observe_step_finish(
                    generation,
                    &context,
                    None,
                    &loaded.record,
                    loaded
                        .record
                        .checkpoint
                        .control_for(&activation.node, &activation.lineage)
                        .is_some(),
                    Some(deadline),
                )
                .await
            {
                return Err(self
                    .fail_hook(&loaded, Some((activation, &context)), failure, Vec::new())
                    .await);
            }
        }
        let loaded = self
            .load_execution_observation(None, Some(deadline))
            .await
            .map_err(|failure| {
                self.retain_execution_read_failure(&failure);
                failure.into_error()
            })?
            .ok_or_else(|| AppError::NotFound("workflow run does not exist".into()))?;
        let loaded = if let Some(generation) = execution {
            // Keep this Execution's lease and options: its caller must first
            // observe the retained None before choosing retry or finalization.
            self.settle_execution(generation, Some(loaded)).await?.0
        } else if loaded.record.checkpoint.is_stopping() || loaded.record.checkpoint.is_terminal() {
            loaded
        } else {
            let definition = self.app.definition_for(&loaded.record)?;
            let discard_staged = loaded.record.checkpoint.rollback_stage_keys(&definition);
            // The original EndSource operation owns the native pause receipt.
            self.commit_for_recovery(loaded, ControlTransition::Pause { discard_staged })
                .await
                .map_err(RuntimeOperationFailure::into_error)?
        };
        Ok(loaded)
    }

    pub(super) async fn finish_execution(
        &self,
        generation: u64,
        loaded: Option<LoadedRun>,
    ) -> Result<RunRecord, AppError> {
        if !self.cell.owns_execution(generation) {
            return Err(AppError::Closed(
                "run execution generation no longer owns cleanup".into(),
            ));
        }
        let mut changes = self.app.runtime_changes();
        while self.app.has_child_work(&self.key) {
            changes
                .changed()
                .await
                .map_err(|_| AppError::Closed("child receipt observation closed".into()))?;
        }
        let settled = self.settle_execution(generation, loaded).await;
        if let Err(error) = &settled
            && self.cell.source_failure(generation).is_some()
        {
            if !matches!(error, AppError::Closed(_)) {
                self.app.tasks_failed(FinalResult::Integration(
                    "source failure cleanup authority failed".into(),
                ));
            }
            return settled.map(|(loaded, _)| loaded.record);
        }
        let retained = settled.as_ref().is_ok_and(|(_, retained)| *retained);
        let mut result = settled.map(|(loaded, _)| loaded.record);
        let cleanup_pending = result.as_ref().is_ok_and(|record| {
            record.checkpoint.is_stopping()
                || !record.checkpoint.controls.is_empty()
                || record.checkpoint.is_terminal() && !retained
        });
        if let Ok(record) = &result {
            let observed = self
                .finish_execution_observation(record, None, None, false)
                .await;
            if record.checkpoint.is_terminal() && self.cell.child_parent().is_none() {
                self.maintain_retention(None).await;
            }
            if let Err(error) = observed {
                result = Err(error);
            }
        }
        self.cell.end_publication(generation);
        self.cell.clear_execution_options(Some(generation));
        let scheduled = self.cell.release_execution(generation);
        if !retained && (cleanup_pending || scheduled) {
            self.schedule(ScheduleCause::Reconcile);
        }
        result
    }

    pub(super) fn settle_execution(
        &self,
        generation: u64,
        loaded: Option<LoadedRun>,
    ) -> Pin<Box<impl Future<Output = Result<(LoadedRun, bool), AppError>> + '_>> {
        Box::pin(async move {
            if !self.cell.owns_execution(generation) {
                return Err(AppError::Closed(
                    "run execution generation no longer owns cleanup".into(),
                ));
            }
            let mut retained = false;
            let result = async {
                let mut loaded = match loaded {
                    Some(loaded) => loaded,
                    None => {
                        let deadline = self.cell.begin_action_cleanup(generation)?;
                        // Pure settlement reads and unfinished guards retain
                        // the same originally sampled cleanup budget.
                        self.load_execution_observation(None, Some(deadline))
                            .await
                            .map_err(|failure| {
                                self.retain_execution_read_failure(&failure);
                                failure.into_error()
                            })?
                            .ok_or_else(|| {
                                AppError::NotFound("workflow run does not exist".into())
                            })?
                    }
                };
                if let Some((context, lineage, category)) = self.cell.source_failure(generation)
                    && context.checkpoint == loaded.record.checkpoint.base_sequence
                    && !loaded.record.checkpoint.is_stopping()
                    && !loaded.record.checkpoint.is_terminal()
                    && loaded
                        .record
                        .checkpoint
                        .position
                        .as_ref()
                        .is_none_or(|committed| committed.ordinal < context.action_ordinal)
                {
                    let position = ActionPosition {
                        node: context.node.clone(),
                        ordinal: context.action_ordinal,
                    };
                    let definition = self.app.definition_for(&loaded.record)?;
                    let (position, category, message) = match loaded
                        .record
                        .checkpoint
                        .preceding_failure(&definition, &position, &lineage)?
                    {
                        Some((position, category, message)) => {
                            (position, category, message.to_owned())
                        }
                        None => (position, category, "caller action execution failed".into()),
                    };
                    loaded = self
                        .commit_for_recovery(
                            loaded,
                            ControlTransition::Failed {
                                category,
                                message,
                                observation: Some(position),
                            },
                        )
                        .await
                        .map_err(RuntimeOperationFailure::into_error)?;
                }
                if !loaded.record.checkpoint.is_terminal()
                    && !loaded.record.checkpoint.is_stopping()
                {
                    let definition = self.app.definition_for(&loaded.record)?;
                    let failed = loaded
                        .record
                        .checkpoint
                        .selected_staged(&definition)
                        .filter_map(|activation| {
                            if let StagedOutcomeRecord::Failed { category, message } =
                                &activation.outcome
                            {
                                Some((activation, *category, message))
                            } else {
                                None
                            }
                        })
                        .min_by(|left, right| {
                            (&left.0.node, &left.0.lineage).cmp(&(&right.0.node, &right.0.lineage))
                        });
                    if let Some((activation, category, message)) = failed {
                        let message = message.clone();
                        let observation = loaded.record.checkpoint.proposed_position(
                            &definition,
                            &activation.node,
                            &activation.lineage,
                            &[],
                        )?;
                        loaded = self
                            .commit_for_recovery(
                                loaded,
                                ControlTransition::Failed {
                                    category,
                                    message,
                                    observation: Some(observation),
                                },
                            )
                            .await
                            .map_err(RuntimeOperationFailure::into_error)?;
                    }
                }
                // No lower owner remains in this case. Complete the accepted
                // rollback on this generation before making a retry admissible.
                if loaded.record.checkpoint.is_stopping()
                    && loaded.record.checkpoint.controls.is_empty()
                {
                    loaded = match Box::pin(self.settle_stopping(loaded)).await {
                        Some(loaded) => loaded,
                        None => {
                            // No terminal receipt owns the result. Observe
                            // current authority under the helper's finite pass;
                            // native mutations keep their separate ownership.
                            self.load_execution_observation(None, None)
                                .await
                                .map_err(|failure| {
                                    self.retain_execution_read_failure(&failure);
                                    failure.into_error()
                                })?
                                .ok_or_else(|| {
                                    AppError::NotFound("workflow run does not exist".into())
                                })?
                        }
                    };
                }
                if !loaded.record.checkpoint.staged.is_empty()
                    && !loaded.record.checkpoint.is_terminal()
                    && !loaded.record.checkpoint.is_stopping()
                {
                    let definition = self.app.definition_for(&loaded.record)?;
                    let discard_staged = loaded.record.checkpoint.rollback_stage_keys(&definition);
                    return self
                        .commit_for_recovery(loaded, ControlTransition::Pause { discard_staged })
                        .await
                        .map_err(RuntimeOperationFailure::into_error);
                }
                if loaded.record.checkpoint.is_terminal() {
                    // Registration has no active execution guards. Completed
                    // guards likewise permit the original capacity cleanup.
                    let deadline = if self.cell.lifecycle_generation().is_none() {
                        Box::pin(self.maintain_retention(Some(loaded))).await;
                        // Retention can advance the head revision. Its native
                        // receipt starts this required fresh capacity-read pass.
                        let deadline = LifecycleEvent::deadline(None);
                        loaded = self
                            .load_execution_observation(None, Some(deadline))
                            .await
                            .map_err(|failure| {
                                self.retain_execution_read_failure(&failure);
                                failure.into_error()
                            })?
                            .ok_or_else(|| {
                                AppError::NotFound("workflow run does not exist".into())
                            })?;
                        deadline
                    } else {
                        LifecycleEvent::deadline(None)
                    };
                    retained = tokio::time::timeout_at(deadline, async {
                        let mut delay = RECONCILIATION_INITIAL_DELAY;
                        loop {
                            match self.app.storage().run_is_active(&loaded.record).await {
                                Ok(active) => {
                                    // Marker authority cannot replace the App
                                    // generation fence of the native result.
                                    self.observe_generation(deadline).await?;
                                    return Ok(!active);
                                }
                                Err(failure)
                                    if failure.is_recoverable()
                                        && self.schedule_after_storage_failure(&failure) =>
                                {
                                    tokio::time::sleep(delay).await;
                                    delay = delay.saturating_mul(2).min(RECONCILIATION_MAX_DELAY);
                                }
                                Err(failure) => return Err(failure),
                            }
                        }
                    })
                    .await
                    .unwrap_or_else(|_| {
                        Err(StorageFailure::permanent(AppError::Timeout(
                            "workflow capacity observation deadline expired".into(),
                        )))
                    })
                    .map_err(|failure| {
                        self.retain_execution_read_failure(&failure);
                        failure.into_error()
                    })?;
                } else if loaded.record.checkpoint.is_stopping() {
                    // Failed may retain admitted controls in Failing. Returning
                    // that native receipt still fences this App generation.
                    self.observe_generation(LifecycleEvent::deadline(None))
                        .await
                        .map_err(|failure| {
                            self.retain_execution_read_failure(&failure);
                            failure.into_error()
                        })?;
                }
                Ok(loaded)
            }
            .await;
            result.map(|loaded| (loaded, retained))
        })
    }
}

impl<
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend,
    C: ConditionBackend,
    T: StateTyping,
    H: LifecycleHooks,
    E: ChildExecutor,
> WorkflowRunRuntime<CS, A, X, C, T, H, E>
{
    pub(super) async fn begin_execution_observation(&self) -> Result<u64, WorkflowFailure> {
        let owner = self
            .cell
            .observation_owner()
            .map_err(|error| WorkflowFailure::Execution(error.to_string()))?;
        let (generation, opening) = self
            .cell
            .begin_lifecycle(owner)
            .map_err(|error| WorkflowFailure::Execution(error.to_string()))?;
        if opening {
            let deadline = self
                .cell
                .begin_action_cleanup(generation)
                .map_err(|error| WorkflowFailure::Execution(error.to_string()))?;
            LifecycleEvent::ExecuteStarted {
                run: self.id(),
                occurrence: self.occurrence(),
            }
            .deliver(
                self.app.hooks(),
                self.cell
                    .execution_mode()
                    .map_err(|error| WorkflowFailure::Execution(error.to_string()))?,
                deadline,
            )
            .await?;
            self.cell
                .opened_lifecycle(generation)
                .map_err(|error| WorkflowFailure::Execution(error.to_string()))?;
        }
        Ok(generation)
    }

    async fn observe_activation_start(
        &self,
        loaded: &LoadedRun,
        activation: &FrontierRecord,
        context: &Arc<ActionContext>,
        inputs: &Map<String, Value>,
        deadline: tokio::time::Instant,
    ) -> Result<(), WorkflowFailure> {
        let generation = self.begin_execution_observation().await?;
        let definition = self
            .app
            .definition_for(&loaded.record)
            .map_err(|error| WorkflowFailure::Execution(error.to_string()))?;
        let Some(Step::Action {
            action, revision, ..
        }) = definition.node(&activation.node).map(|node| node.step())
        else {
            return self
                .observe_step_start(
                    generation,
                    &loaded.record,
                    context,
                    &activation.lineage,
                    inputs,
                    deadline,
                )
                .await
                .map(|_| ());
        };
        let binding = self
            .app
            .action(action, revision)
            .map_err(|error| WorkflowFailure::Execution(error.to_string()))?;
        let inputs = catch_unwind(AssertUnwindSafe(|| {
            binding
                .declaration()
                .resolve_inputs(inputs, self.cell.execution_mode()?)
                .and_then(ActionInputs::into_values)
        }))
        .map_err(|_| WorkflowFailure::Execution("action declaration panicked".into()))?
        .map_err(|error| WorkflowFailure::State(error.to_string()))?;
        let context = self
            .observe_step_start(
                generation,
                &loaded.record,
                context,
                &activation.lineage,
                &inputs,
                deadline,
            )
            .await?;
        self.observe_stream_start(generation, &context, deadline)
            .await
    }

    pub(super) async fn observe_stream_start(
        &self,
        generation: u64,
        context: &ActionContext,
        deadline: tokio::time::Instant,
    ) -> Result<(), WorkflowFailure> {
        if self.cell.lifecycle_generation() != Some(generation)
            || self.cell.execution_was_dropped(generation)
        {
            return Ok(());
        }
        LifecycleEvent::StreamStarted {
            context,
            span: None,
        }
        .deliver(
            self.app.hooks(),
            context.mode,
            LifecycleEvent::deadline(Some(deadline)),
        )
        .await?;
        self.cell
            .open_action_stream(generation, context)
            .map_err(|error| WorkflowFailure::Execution(error.to_string()))?;
        Ok(())
    }

    pub(super) async fn observe_stream_item(
        &self,
        generation: u64,
        context: &ActionContext,
        result: &Map<String, Value>,
        deadline: tokio::time::Instant,
    ) -> Result<(), WorkflowFailure> {
        if self.cell.lifecycle_generation() != Some(generation)
            || self.cell.execution_was_dropped(generation)
        {
            return Ok(());
        }
        let index = self
            .cell
            .action_stream_item(generation, context, false)
            .map_err(|error| WorkflowFailure::Execution(error.to_string()))?;
        LifecycleEvent::StreamItem {
            context,
            span: None,
            index,
            result,
        }
        .deliver(
            self.app.hooks(),
            context.mode,
            LifecycleEvent::deadline(Some(deadline)),
        )
        .await?;
        self.cell
            .action_stream_item(generation, context, true)
            .map_err(|error| WorkflowFailure::Execution(error.to_string()))?;
        Ok(())
    }

    async fn observe_source_event(
        &self,
        context: &ActionContext,
        event: ActionEvent,
        deadline: tokio::time::Instant,
    ) -> Result<Option<Map<String, Value>>, WorkflowFailure> {
        let generation = self.cell.lifecycle_generation().ok_or_else(|| {
            WorkflowFailure::Execution("source observation generation closed".into())
        })?;
        if let ActionEvent::Partial { result } = event {
            self.observe_stream_item(generation, context, &result, deadline)
                .await?;
            return Ok(Some(result));
        }
        if self.cell.execution_was_dropped(generation) {
            return Ok(None);
        }
        event
            .validate_span()
            .map_err(|error| WorkflowFailure::Execution(error.to_string()))?;
        let deadline = LifecycleEvent::deadline(Some(deadline));
        match event {
            ActionEvent::SpanStarted {
                id,
                name,
                parent,
                dependencies,
            } => {
                let parent_index = self
                    .cell
                    .span_parent(generation, context, &id, parent.as_deref())
                    .map_err(|error| WorkflowFailure::Execution(error.to_string()))?;
                LifecycleEvent::SpanStarted {
                    context,
                    id: &id,
                    name: &name,
                    parent: parent.as_deref(),
                    dependencies: &dependencies,
                }
                .deliver(self.app.hooks(), context.mode, deadline)
                .await?;
                self.cell
                    .opened_span(generation, context, id, parent_index)
                    .map_err(|error| WorkflowFailure::Execution(error.to_string()))?;
            }
            ActionEvent::SpanAttribute {
                span,
                key,
                value,
                tags,
            } => {
                self.cell
                    .span_attribute(generation, context, span.as_deref())
                    .map_err(|error| WorkflowFailure::Execution(error.to_string()))?;
                LifecycleEvent::SpanAttribute {
                    context,
                    span: span.as_deref(),
                    key: &key,
                    value: &value,
                    tags: &tags,
                }
                .deliver(self.app.hooks(), context.mode, deadline)
                .await?;
            }
            ActionEvent::SpanFinished { id } => {
                let id = self
                    .cell
                    .take_span(generation, context, Some(&id))
                    .map_err(|error| WorkflowFailure::Execution(error.to_string()))?
                    .ok_or_else(|| {
                        WorkflowFailure::Execution(
                            "caller span finish target is unavailable".into(),
                        )
                    })?;
                LifecycleEvent::SpanFinished { context, id: &id }
                    .deliver(self.app.hooks(), context.mode, deadline)
                    .await?;
            }
            ActionEvent::Partial { .. } | ActionEvent::Completed { .. } => unreachable!(),
        }
        Ok(None)
    }

    async fn finish_spans(
        &self,
        generation: u64,
        context: &ActionContext,
        async_deadline: Option<tokio::time::Instant>,
    ) -> Result<(), WorkflowFailure> {
        let mut failure = None;
        loop {
            let id = match self.cell.take_span(generation, context, None) {
                Ok(Some(id)) => id,
                Ok(None) => break,
                Err(error) => {
                    return Err(
                        failure.unwrap_or_else(|| WorkflowFailure::Execution(error.to_string()))
                    );
                }
            };
            let event = LifecycleEvent::SpanFinished { context, id: &id };
            let observed =
                match async_deadline.filter(|_| !self.cell.execution_was_dropped(generation)) {
                    Some(deadline) => {
                        event
                            .deliver(self.app.hooks(), context.mode, deadline)
                            .await
                    }
                    None => event.deliver_sync(self.app.hooks()),
                };
            if let Err(next) = observed
                && failure.is_none()
            {
                failure = Some(next);
            }
        }
        failure.map_or(Ok(()), Err)
    }

    fn finish_spans_sync(
        &self,
        generation: u64,
        context: &ActionContext,
    ) -> Result<(), WorkflowFailure> {
        let mut failure = None;
        loop {
            let id = match self.cell.take_span(generation, context, None) {
                Ok(Some(id)) => id,
                Ok(None) => break,
                Err(error) => {
                    return Err(
                        failure.unwrap_or_else(|| WorkflowFailure::Execution(error.to_string()))
                    );
                }
            };
            if let Err(next) =
                (LifecycleEvent::SpanFinished { context, id: &id }).deliver_sync(self.app.hooks())
                && failure.is_none()
            {
                failure = Some(next);
            }
        }
        failure.map_or(Ok(()), Err)
    }

    async fn finish_staged_spans(
        &self,
        loaded: &LoadedRun,
        decision: &BspDecision,
        context: Option<&ActionContext>,
    ) -> Result<(), AppError> {
        let (BspDecision::Stage(staged), Some(context)) = (decision, context) else {
            return Ok(());
        };
        let Some(generation) = self.cell.lifecycle_generation() else {
            return Ok(());
        };
        let Some(staged) = staged.iter().find(|staged| {
            self.cell
                .action_context(
                    loaded.record.checkpoint.base_sequence,
                    &staged.node,
                    &staged.lineage,
                )
                .is_some_and(|observed| std::ptr::eq(observed.as_ref(), context))
                && matches!(staged.outcome, StagedOutcomeRecord::Complete { .. })
        }) else {
            return Ok(());
        };
        let activation = loaded
            .record
            .checkpoint
            .frontier
            .iter()
            .find(|activation| {
                activation.node == staged.node && activation.lineage == staged.lineage
            })
            .ok_or_else(|| {
                AppError::Integration("completed caller activation is unavailable".into())
            })?;
        // The one caller Stage carries its original context across sibling
        // completion; no ordinal is resampled after another source settles.
        if let Err(failure) = self
            .finish_spans(generation, context, Some(LifecycleEvent::deadline(None)))
            .await
        {
            return Err(self
                .fail_hook(loaded, Some((activation, context)), failure, Vec::new())
                .await);
        }
        Ok(())
    }

    pub(super) async fn fail_hook(
        &self,
        loaded: &LoadedRun,
        activation: Option<(&FrontierRecord, &ActionContext)>,
        failure: WorkflowFailure,
        staged: Vec<StagedActivationRecord>,
    ) -> AppError {
        let error = failure.application_error();
        let mut prior = None;
        let settled = async {
            let generation = self
                .cell
                .lifecycle_generation()
                .ok_or_else(|| AppError::Closed("lifecycle failure cleanup is closed".into()))?;
            let deadline = self.cell.begin_action_cleanup(generation)?;
            let mut loaded = LoadedRun {
                record: loaded.record.clone(),
                revision: loaded.revision,
            };
            if !staged.is_empty() {
                // Preserve the exact prefix before failure selection: its
                // Complete entries determine the earlier failure's ordinal.
                loaded = self
                    .commit_for_recovery(
                        loaded,
                        ControlTransition::Stage {
                            activations: staged,
                        },
                    )
                    .await
                    .map_err(RuntimeOperationFailure::into_error)?;
            }
            let definition = self.app.definition_for(&loaded.record)?;
            let retained = self
                .cell
                .lifecycle_generation()
                .and_then(|generation| self.cell.source_failure(generation));
            let source = retained.as_ref().filter(|(context, lineage, _)| {
                context.checkpoint == loaded.record.checkpoint.base_sequence
                    && activation.is_some_and(|(activation, _)| {
                        (&context.node, lineage) < (&activation.node, &activation.lineage)
                    })
            });
            // An earlier source can finish while this original callback is
            // held. Its sampled identity wins; a same-action destructor never
            // replaces the callback's own primary Hook failure.
            let (mut observation, lineage, mut category, mut message) = match source {
                Some((context, lineage, category)) => {
                    let message = "caller action execution failed".to_owned();
                    prior = Some(category.project(message.clone()));
                    (
                        Some(ActionPosition {
                            node: context.node.clone(),
                            ordinal: context.action_ordinal,
                        }),
                        Some(lineage.as_str()),
                        *category,
                        message,
                    )
                }
                None => (
                    activation.map(|(_, context)| ActionPosition {
                        node: context.node.clone(),
                        ordinal: context.action_ordinal,
                    }),
                    activation.map(|(activation, _)| activation.lineage.as_str()),
                    FailureCategory::from_failure(&failure),
                    failure.message().to_owned(),
                ),
            };
            if let Some((position, lineage)) = observation.as_ref().zip(lineage)
                && let Some((position, preceding, text)) = loaded
                    .record
                    .checkpoint
                    .preceding_failure(&definition, position, lineage)?
            {
                observation = Some(position);
                category = preceding;
                message = text.to_owned();
                prior = Some(category.project(message.clone()));
            }
            let mut loaded = self
                .commit_for_recovery(
                    loaded,
                    ControlTransition::Failed {
                        category,
                        message,
                        observation,
                    },
                )
                .await
                .map_err(RuntimeOperationFailure::into_error)?;
            if !loaded.record.checkpoint.is_terminal() {
                // Accepted controls can progress independently while their
                // original failure is published. Refresh only that authority;
                // the native Stage and terminal receipts remain exact values.
                loaded = self
                    .load_execution_observation(None, Some(deadline))
                    .await
                    .map_err(|failure| {
                        self.retain_execution_read_failure(&failure);
                        failure.into_error()
                    })?
                    .ok_or_else(|| AppError::NotFound("workflow run does not exist".into()))?;
                if loaded.record.checkpoint.is_stopping()
                    && loaded.record.checkpoint.controls.is_empty()
                {
                    loaded = match Box::pin(self.settle_stopping(loaded)).await {
                        Some(loaded) => loaded,
                        None => self
                            .load_execution_observation(None, Some(deadline))
                            .await
                            .map_err(|failure| {
                                self.retain_execution_read_failure(&failure);
                                failure.into_error()
                            })?
                            .ok_or_else(|| {
                                AppError::NotFound("workflow run does not exist".into())
                            })?,
                    };
                }
            }
            if loaded.record.checkpoint.is_terminal() {
                // The accepted failure receipt is complete authority. Preserve
                // generation fencing without requiring the same head again.
                self.observe_generation(deadline).await.map_err(|failure| {
                    self.retain_execution_read_failure(&failure);
                    failure.into_error()
                })?;
            }
            Ok(loaded)
        }
        .await;
        match settled {
            Ok(loaded) => {
                let record = &loaded.record;
                // A prior canonical action failure remains durable; this later
                // callback error is its execution-completion context. Ordinary
                // precommit Hook failure still projects directly from storage.
                let completion = prior.map(|prior| failure.with_cause(Some(prior)));
                let _ = self
                    .finish_execution_observation(record, completion, None, false)
                    .await;
                if record.checkpoint.is_terminal() && self.cell.child_parent().is_none() {
                    Box::pin(self.maintain_retention(Some(loaded))).await;
                }
            }
            Err(AppError::Closed(_)) => {}
            Err(_) => self.app.tasks_failed(FinalResult::Integration(
                "lifecycle failure cleanup authority failed".into(),
            )),
        }
        error
    }

    async fn observe_step_start(
        &self,
        generation: u64,
        record: &RunRecord,
        context: &Arc<ActionContext>,
        lineage: &str,
        inputs: &Map<String, Value>,
        deadline: tokio::time::Instant,
    ) -> Result<Arc<ActionContext>, WorkflowFailure> {
        if let Some(context) = self
            .cell
            .action_context(context.checkpoint, &context.node, lineage)
        {
            return Ok(context);
        }
        let state = WorkflowState {
            values: record.checkpoint.state.clone(),
        };
        LifecycleEvent::StepStarted {
            context,
            state: &state,
            inputs,
        }
        .deliver(
            self.app.hooks(),
            context.mode,
            LifecycleEvent::deadline(Some(deadline)),
        )
        .await?;
        self.cell
            .observe_action(generation, Arc::clone(context), lineage)
            .map_err(|error| WorkflowFailure::Execution(error.to_string()))?;
        Ok(Arc::clone(context))
    }
}

impl<
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend,
    C: ConditionBackend,
    T: StateTyping,
    H: LifecycleHooks,
    E: ChildExecutor,
> WorkflowRunRuntime<CS, A, X, C, T, H, E>
{
    async fn observe_step_finish(
        &self,
        generation: u64,
        context: &ActionContext,
        outcome: Option<Result<&ActionOutcome, &WorkflowFailure>>,
        record: &RunRecord,
        unresolved: bool,
        async_deadline: Option<tokio::time::Instant>,
    ) -> Result<(), WorkflowFailure> {
        let checkpoint = self
            .project_history(record)
            .map_err(|error| WorkflowFailure::Execution(error.to_string()))?;
        let spans = self.finish_spans(generation, context, async_deadline).await;
        let Some((context, items)) = self.cell.take_action_observation(generation, context) else {
            if !unresolved {
                self.cell.retire_closed_observation(generation, context);
            }
            return spans;
        };
        let event = LifecycleEvent::StepFinished {
            context: &context,
            outcome,
            checkpoint: &checkpoint,
        };
        let step = match async_deadline.filter(|_| !self.cell.execution_was_dropped(generation)) {
            Some(deadline) => {
                event
                    .deliver(self.app.hooks(), context.mode, deadline)
                    .await
            }
            None => event.deliver_sync(self.app.hooks()),
        };
        let result = spans.and(step);
        let result = if let Some(items) = items {
            let failure = result
                .as_ref()
                .err()
                .or_else(|| outcome.and_then(Result::err));
            let event = LifecycleEvent::StreamFinished {
                context: &context,
                span: None,
                items,
                failure,
            };
            // A failed step guard unwinds the still-open stream guard. Its
            // synchronous Drop callback must not start another async lane.
            let stream_result = match async_deadline
                .filter(|_| result.is_ok() && !self.cell.execution_was_dropped(generation))
            {
                Some(deadline) => {
                    event
                        .deliver(self.app.hooks(), context.mode, deadline)
                        .await
                }
                None => event.deliver_sync(self.app.hooks()),
            };
            result.and(stream_result)
        } else {
            result
        };
        self.cell
            .finish_action_observation(generation, &context, unresolved);
        result
    }

    fn observe_step_finish_sync(
        &self,
        generation: u64,
        context: &ActionContext,
        record: &RunRecord,
        unresolved: bool,
    ) -> Result<(), WorkflowFailure> {
        let checkpoint = self
            .project_history(record)
            .map_err(|error| WorkflowFailure::Execution(error.to_string()))?;
        let spans = self.finish_spans_sync(generation, context);
        let Some((context, items)) = self.cell.take_action_observation(generation, context) else {
            if !unresolved {
                self.cell.retire_closed_observation(generation, context);
            }
            return spans;
        };
        let step = LifecycleEvent::StepFinished {
            context: &context,
            outcome: None,
            checkpoint: &checkpoint,
        }
        .deliver_sync(self.app.hooks());
        let result = spans.and(step);
        let result = if let Some(items) = items {
            let next = LifecycleEvent::StreamFinished {
                context: &context,
                span: None,
                items,
                failure: result.as_ref().err(),
            }
            .deliver_sync(self.app.hooks());
            result.and(next)
        } else {
            result
        };
        self.cell
            .finish_action_observation(generation, &context, unresolved);
        result
    }

    fn finish_execution_observation_sync(&self, record: &RunRecord) -> Result<(), AppError> {
        let Some(generation) = self.cell.lifecycle_generation() else {
            return self
                .cell
                .lifecycle_observation(record.checkpoint.sequence, false)
                .map(|_| ());
        };
        let unfinished = record
            .checkpoint
            .controls
            .values()
            .filter_map(|control| {
                self.cell.action_context(
                    record.checkpoint.base_sequence,
                    &control.node,
                    &control.lineage,
                )
            })
            .collect::<Vec<_>>();
        if !self
            .cell
            .begin_lifecycle_finish(generation, record.checkpoint.sequence)
        {
            return self
                .cell
                .lifecycle_observation(record.checkpoint.sequence, false)
                .map(|_| ());
        }
        let mut failure = None;
        for context in self.cell.action_observations() {
            let unresolved = unfinished
                .iter()
                .any(|unfinished| Arc::ptr_eq(unfinished, &context));
            if let Err(next) =
                self.observe_step_finish_sync(generation, &context, record, unresolved)
                && failure.is_none()
            {
                failure = Some(next);
            }
        }
        match self.project(record) {
            Ok(snapshot) => {
                if let Err(next) = (LifecycleEvent::ExecuteFinished {
                    snapshot: &snapshot,
                    failure: failure.as_ref(),
                })
                .deliver_sync(self.app.hooks())
                    && failure.is_none()
                {
                    failure = Some(next);
                }
            }
            Err(error) => {
                if failure.is_none() {
                    failure = Some(WorkflowFailure::Execution(error.to_string()));
                }
            }
        }
        if record.checkpoint.is_terminal() || record.checkpoint.is_paused() {
            self.cell.clear_execution_options(Some(generation));
        }
        let error = failure.as_ref().map(WorkflowFailure::application_error);
        let completed =
            self.cell
                .complete_lifecycle(generation, record.checkpoint.sequence, failure);
        match error {
            Some(error) => Err(error),
            None => completed,
        }
    }

    pub(super) async fn finish_execution_observation(
        &self,
        record: &RunRecord,
        mut failure: Option<WorkflowFailure>,
        deadline: Option<tokio::time::Instant>,
        synchronous: bool,
    ) -> Result<(), AppError> {
        let Some(generation) = self.cell.lifecycle_generation() else {
            return self
                .cell
                .lifecycle_observation(record.checkpoint.sequence, false)
                .map(|_| ());
        };
        let synchronous = synchronous || self.cell.execution_was_dropped(generation);
        let unfinished = record
            .checkpoint
            .controls
            .values()
            .filter_map(|control| {
                self.cell.action_context(
                    record.checkpoint.base_sequence,
                    &control.node,
                    &control.lineage,
                )
            })
            .collect::<Vec<_>>();
        if !self
            .cell
            .begin_lifecycle_finish(generation, record.checkpoint.sequence)
        {
            return self
                .cell
                .lifecycle_observation(record.checkpoint.sequence, false)
                .map(|_| ());
        }
        let deadline = match deadline {
            Some(deadline) => deadline,
            None => self.cell.begin_action_cleanup(generation)?,
        };
        let primary = match &record.checkpoint.phase {
            CheckpointPhaseRecord::Failing {
                category, message, ..
            }
            | CheckpointPhaseRecord::Failed {
                category, message, ..
            } => Some(category.project(message.clone())),
            _ => None,
        };
        for context in self.cell.action_observations() {
            let unresolved = unfinished
                .iter()
                .any(|unfinished| Arc::ptr_eq(unfinished, &context));
            if unresolved && !record.checkpoint.is_stopping() && !synchronous {
                continue;
            }
            let outcome = record.checkpoint.outcomes.iter().find(|outcome| {
                outcome.ordinal == context.action_ordinal && outcome.node == context.node
            });
            let failed = record
                .checkpoint
                .failed_observation()
                .is_some_and(|position| {
                    position.ordinal == context.action_ordinal && position.node == context.node
                });
            let observed = outcome
                .map(Ok)
                .or_else(|| failed.then(|| primary.as_ref().map(Err)).flatten());
            if let Err(next) = self
                .observe_step_finish(
                    generation,
                    &context,
                    observed,
                    record,
                    unresolved,
                    (!synchronous && failure.is_none()).then_some(deadline),
                )
                .await
                && failure.is_none()
            {
                failure = Some(next.with_cause(match &record.checkpoint.phase {
                    CheckpointPhaseRecord::Failing {
                        category, message, ..
                    }
                    | CheckpointPhaseRecord::Failed {
                        category, message, ..
                    } => Some(category.project(message.clone())),
                    _ => None,
                }));
            }
        }
        match self.project(record) {
            Ok(snapshot) => {
                let event = LifecycleEvent::ExecuteFinished {
                    snapshot: &snapshot,
                    failure: failure.as_ref().or(primary.as_ref()),
                };
                let observed = if synchronous || self.cell.execution_was_dropped(generation) {
                    event.deliver_sync(self.app.hooks())
                } else {
                    event
                        .deliver(self.app.hooks(), self.cell.execution_mode()?, deadline)
                        .await
                };
                if let Err(next) = observed
                    && failure.is_none()
                {
                    failure = Some(next.with_cause(primary));
                }
            }
            Err(error) => {
                if failure.is_none() {
                    failure = Some(WorkflowFailure::Execution(error.to_string()));
                }
            }
        }
        if record.parent.is_some()
            && record.checkpoint.is_terminal()
            && let Err(next) = self.observe_child_finished(record, deadline).await
            && failure.is_none()
        {
            failure = Some(next);
        }
        if record.checkpoint.is_terminal() || record.checkpoint.is_paused() {
            self.cell.clear_execution_options(Some(generation));
        }
        let error = failure.as_ref().map(WorkflowFailure::application_error);
        let completed =
            self.cell
                .complete_lifecycle(generation, record.checkpoint.sequence, failure);
        match error {
            Some(error) => Err(error),
            None => completed,
        }
    }

    pub(super) async fn observe_barrier(&self, loaded: LoadedRun) -> Result<LoadedRun, AppError> {
        let Some(generation) = self.cell.lifecycle_generation() else {
            return Ok(loaded);
        };
        let record = &loaded.record;
        let observations = self.cell.action_observations();
        for outcome in &record.checkpoint.outcomes {
            let context = observations.iter().find(|context| {
                context.action_ordinal == outcome.ordinal && context.node == outcome.node
            });
            let Some(context) = context else {
                continue;
            };
            let deadline = self.cell.begin_action_cleanup(generation)?;
            if let Err(failure) = self
                .observe_step_finish(
                    generation,
                    context,
                    Some(Ok(outcome)),
                    record,
                    false,
                    Some(deadline),
                )
                .await
            {
                let error = failure.application_error();
                // The committed barrier owns this Pause intent even when the
                // callback spent its cleanup budget. Native revision-fenced
                // reconciliation preserves independent cancellation.
                let parked = self
                    .commit_for_recovery(
                        loaded,
                        ControlTransition::Pause {
                            discard_staged: Vec::new(),
                        },
                    )
                    .await
                    .map_err(RuntimeOperationFailure::into_error);
                match parked {
                    Ok(parked) => {
                        let _ = self
                            .finish_execution_observation(
                                &parked.record,
                                Some(failure),
                                Some(deadline),
                                false,
                            )
                            .await;
                        return Ok(parked);
                    }
                    Err(AppError::Closed(_)) => {}
                    Err(_) => self.app.tasks_failed(FinalResult::Integration(
                        "post-hook cleanup authority failed".into(),
                    )),
                }
                return Err(error);
            }
        }
        Ok(loaded)
    }
}
