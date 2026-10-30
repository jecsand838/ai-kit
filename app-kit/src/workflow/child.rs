use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::AppError;
use crate::workflow::ActionMode;
use crate::workflow::definition::CompiledDefinition;
use crate::workflow::run::RunRecord;

/// Caller-selected execution of a complete, already-admitted child batch.
///
/// App owns admission; the executor chooses polling order and returns each
/// admitted completion exactly once or reports failure. App validates membership
/// before reducing results in original child order.
///
/// The associated future preserves concrete child and execution types, both of
/// which may be non-Send and non-Unpin. This GAT is the App-specific exception
/// to ordinary RPITIT methods.
///
/// # Example
///
/// ```
/// use agents_kit::Agent;
/// use app_kit::{App, AppConfig, Shutdown, WorkspaceConfig};
/// use app_kit::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
///     RunStatus, WorkflowDefinition, WorkflowState};
/// use storage_kit::StateStore;
/// use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
/// use app_kit::{AppError, workflow::{ActionMode, ChildBatch, ChildCompletion, ChildExecutor}};
/// use futures_util::FutureExt;
/// use std::future::Future;
/// struct InspectChildren;
/// impl InspectChildren {
///     fn inspect(result: Result<Vec<ChildCompletion>, AppError>) -> Result<Vec<ChildCompletion>, AppError> {
///         let completed = result?;
///         assert_eq!(completed.len(), 1);
///         assert_eq!(completed[0].ordinal(), 0);
///         Ok(completed)
///     }
/// }
/// impl ChildExecutor for InspectChildren {
///     type Execution<'a, F> = futures_util::future::Map<ChildBatch<F>,
///         fn(Result<Vec<ChildCompletion>, AppError>) -> Result<Vec<ChildCompletion>, AppError>>
///     where Self: 'a, F: Future<Output = Result<ChildCompletion, AppError>> + 'a;
///     fn execute<'a, F>(&'a self, batch: ChildBatch<F>) -> Self::Execution<'a, F>
///     where F: Future<Output = Result<ChildCompletion, AppError>> + 'a {
///         assert_eq!(batch.mode(), ActionMode::Asynchronous);
///         assert_eq!(batch.len(), 1);
///         assert!(!batch.is_empty());
///         batch.map(Self::inspect as fn(_) -> _)
///     }
/// }
/// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
/// # runtime.block_on(async {
/// let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
/// let definition = WorkflowDefinition::parse(r#"{
///  "schema":"app-kit.workflow/v2","id":"counter","revision":"r1",
///  "state":{"sum":0},"required":["sum"],"reducers":{"sum":"sum_integer"},
///  "entry":"child","nodes":[{"id":"child","step":{"kind":"child","workflow":"child",
///  "input":{"count":{"kind":"literal","value":4}},
///  "reductions":[{"source":"count","target":"sum","reducer":"sum_integer"}]}}],"routes":{}
/// }"#)?;
/// let child = WorkflowDefinition::parse(r#"{
///     "schema":"app-kit.workflow/v2", "id":"child", "revision":"r1",
///     "state":{"count":0}, "required":["count"],
///     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
///     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
///         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
///     ]}}], "routes":{}
/// }"#)?;
/// let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition, child]).with_child_executor(InspectChildren);
/// let mut app = App::open(config).await?;
/// let outcome = async {
///     let mut workspace = app.create_workspace(WorkspaceConfig {
///         name: Some("local counter".into()), root: std::env::current_dir()?,
///     }).await?;
///     let workflow = workspace.workflow("counter".parse()?).await?;
///     let mut run = workflow.prepare(WorkflowState::default()).await?;
///     let terminal = run.execution(ExecutionOptions::default()).await?.finish().await?;
///     assert_eq!(terminal.state.values()["sum"], 5);
///     #
///     Ok::<(), Box<dyn std::error::Error>>(())
/// }.await;
/// let cleanup = app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await;
/// if let Err(error) = &cleanup { eprintln!("App shutdown: {error}"); }
/// outcome?;
/// cleanup?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub trait ChildExecutor: Send + Sync + 'static {
    /// The exact caller future for this batch, without future erasure.
    type Execution<'a, F>: Future<Output = Result<Vec<ChildCompletion>, AppError>> + 'a
    where
        Self: 'a,
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a;

    /// Selects how the admitted child futures are polled and awaited.
    ///
    /// App supplies already admitted work; this method chooses polling and collection,
    /// not admission or new child identity. Pipeline nodes use their own synchronous
    /// state handoff through the same kernel and bypass this seam.
    ///
    /// # Errors
    ///
    /// Any `Err` returned by the execution future becomes
    /// [`WorkflowFailure::Child`](crate::workflow::WorkflowFailure::Child),
    /// regardless of the [`AppError`] variant. Input and admission operations may
    /// classify the same variant differently. A panic while constructing or
    /// polling the future is contained as
    /// [`WorkflowFailure::Execution`](crate::workflow::WorkflowFailure::Execution).
    /// Neither failure reduces the parent from partial child results. Existing
    /// descendant cleanup retains ownership before terminal publication.
    ///
    /// See [the complete child executor workflow](ChildExecutor).
    fn execute<'a, F>(&'a self, batch: ChildBatch<F>) -> Self::Execution<'a, F>
    where
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a;
}

/// A non-Clone owner of concrete, pinned, admitted child futures.
///
/// Async execution polls all admitted children and awaits every result before
/// collecting in original order. Sync execution polls sequentially and stops
/// on the first error. Before its first poll a custom executor can transfer the exact pins with
/// [`Self::into_tasks`]. No child future needs Send or Unpin.
///
/// See [the complete child executor workflow](ChildExecutor).
pub struct ChildBatch<F> {
    mode: ActionMode,
    tasks: Vec<ChildSlot<F>>,
    state: ChildBatchState,
}

enum ChildSlot<F> {
    Pending(Pin<Box<F>>),
    Completed(Result<ChildCompletion, AppError>),
}

enum ChildBatchState {
    Unpolled,
    Polling,
    Finished,
}

/// Exact terminal child evidence produced only by an admitted child future.
///
/// This value cannot be constructed or cloned by a caller. The private record
/// retains the original run and parent membership for App's validation.
///
/// ```compile_fail
/// use app_kit::workflow::ChildCompletion;
///
/// fn duplicate(completion: ChildCompletion) -> (ChildCompletion, ChildCompletion) {
/// (completion.clone(), completion)
/// }
/// ```
///
/// Struct update syntax also cannot construct completion evidence:
///
/// ```compile_fail
/// use app_kit::workflow::ChildCompletion;
/// fn reconstruct(completion: ChildCompletion) -> ChildCompletion {
/// ChildCompletion { ..completion }
/// }
/// ```
///
/// See [the complete child executor workflow](ChildExecutor).
pub struct ChildCompletion {
    ordinal: usize,
    record: Box<RunRecord>,
}

impl<F> ChildBatch<F> {
    pub(super) fn new(tasks: Vec<F>, mode: ActionMode) -> Result<Self, AppError> {
        if mode == ActionMode::Both {
            return Err(AppError::InvalidInput(
                "child execution requires an actual mode".into(),
            ));
        }
        Ok(Self {
            mode,
            tasks: tasks
                .into_iter()
                .map(|task| ChildSlot::Pending(Box::pin(task)))
                .collect(),
            state: ChildBatchState::Unpolled,
        })
    }

    /// Returns the actual synchronous or asynchronous execution mode.
    ///
    /// This never returns the declaration-only [`ActionMode::Both`] value.
    ///
    /// See [the complete child executor workflow](ChildExecutor).
    pub fn mode(&self) -> ActionMode {
        self.mode
    }

    /// Returns the number of child tasks or completed results still owned here.
    ///
    /// After the Future returns its results or an error, the batch owns none.
    ///
    /// See [the complete child executor workflow](ChildExecutor).
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Reports whether this batch still owns any child task or result.
    ///
    /// See [the complete child executor workflow](ChildExecutor).
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Transfers the exact pinned child futures before this batch is polled.
    ///
    /// Once polling begins, returns Conflict and drops the consumed batch,
    /// including its remaining futures and retained completion evidence. This
    /// never moves a child out of its pin or creates a second execution owner.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::Conflict`] once any batch polling has begun, even if no
    /// child has completed. The consumed batch is dropped on error, preserving pin
    /// safety and preventing duplicate completion ownership.
    ///
    /// # Example
    ///
    /// ```
    /// # use agents_kit::Agent;
    /// # use app_kit::{App, AppConfig, Shutdown, WorkspaceConfig};
    /// # use app_kit::workflow::{CheckpointSelector, ExecutionEvent, ExecutionOptions, RunSelector,
    /// #     RunStatus, WorkflowDefinition, WorkflowState};
    /// # use storage_kit::StateStore;
    /// # use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
    /// use app_kit::{AppError, workflow::{ChildBatch, ChildCompletion, ChildExecutor}};
    /// use futures_util::FutureExt;
    /// use std::{future::Future, pin::Pin};
    /// struct InspectChildren;
    /// impl InspectChildren {
    ///     fn collect(results: Vec<Result<ChildCompletion, AppError>>) -> Result<Vec<ChildCompletion>, AppError> {
    ///         results.into_iter().collect()
    ///     }
    /// }
    /// impl ChildExecutor for InspectChildren {
    ///     type Execution<'a, F> = futures_util::future::Map<futures_util::future::JoinAll<Pin<Box<F>>>,
    ///         fn(Vec<Result<ChildCompletion, AppError>>) -> Result<Vec<ChildCompletion>, AppError>>
    ///     where Self: 'a, F: Future<Output = Result<ChildCompletion, AppError>> + 'a;
    ///     fn execute<'a, F>(&'a self, batch: ChildBatch<F>) -> Self::Execution<'a, F>
    ///     where F: Future<Output = Result<ChildCompletion, AppError>> + 'a {
    ///         // App delivers an unpolled batch. Keep each concrete future in its original pin.
    ///         let tasks = batch.into_tasks().expect("batch has not been polled");
    ///         futures_util::future::join_all(tasks).map(Self::collect as fn(_) -> _)
    ///     }
    /// }
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let definition = WorkflowDefinition::parse(r#"{
    /// #  "schema":"app-kit.workflow/v2","id":"counter","revision":"r1",
    /// #  "state":{"sum":0},"required":["sum"],"reducers":{"sum":"sum_integer"},
    /// #  "entry":"child","nodes":[{"id":"child","step":{"kind":"child","workflow":"child",
    /// #  "input":{"count":{"kind":"literal","value":4}},
    /// #  "reductions":[{"source":"count","target":"sum","reducer":"sum_integer"}]}}],"routes":{}
    /// # }"#)?;
    /// # let child = WorkflowDefinition::parse(r#"{
    /// #     "schema":"app-kit.workflow/v2", "id":"child", "revision":"r1",
    /// #     "state":{"count":0}, "required":["count"],
    /// #     "reducers":{"count":"reject_concurrent"}, "entry":"increment",
    /// #     "nodes":[{"id":"increment","step":{"kind":"state","mutations":[
    /// #         {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
    /// #     ]}}], "routes":{}
    /// # }"#)?;
    /// # let config = AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition, child]).with_child_executor(InspectChildren);
    /// # let mut app = App::open(config).await?;
    /// # let outcome = async {
    /// #     let mut workspace = app.create_workspace(WorkspaceConfig {
    /// #         name: Some("local counter".into()), root: std::env::current_dir()?,
    /// #     }).await?;
    /// #     let workflow = workspace.workflow("counter".parse()?).await?;
    /// #     let mut run = workflow.prepare(WorkflowState::default()).await?;
    ///     let terminal = run.execution(ExecutionOptions::default()).await?.finish().await?;
    ///     assert_eq!(terminal.state.values()["sum"], 5);
    ///     #
    /// #     Ok::<(), Box<dyn std::error::Error>>(())
    /// # }.await;
    /// # let cleanup = app.shutdown(Shutdown::CancelAndDrain { timeout: std::time::Duration::from_secs(5) }).await;
    /// # if let Err(error) = &cleanup { eprintln!("App shutdown: {error}"); }
    /// # outcome?;
    /// # cleanup?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn into_tasks(self) -> Result<Vec<Pin<Box<F>>>, AppError> {
        match self.state {
            ChildBatchState::Unpolled => self
                .tasks
                .into_iter()
                .map(|slot| match slot {
                    ChildSlot::Pending(task) => Ok(task),
                    ChildSlot::Completed(_) => Err(AppError::Conflict(
                        "child batch polling already began".into(),
                    )),
                })
                .collect(),
            ChildBatchState::Polling | ChildBatchState::Finished => Err(AppError::Conflict(
                "child batch polling already began".into(),
            )),
        }
    }
}

impl<F: Future<Output = Result<ChildCompletion, AppError>>> Future for ChildBatch<F> {
    type Output = Result<Vec<ChildCompletion>, AppError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let batch = self.get_mut();
        if matches!(batch.state, ChildBatchState::Unpolled) {
            batch.state = ChildBatchState::Polling;
        }
        let ChildBatchState::Polling = batch.state else {
            return Poll::Ready(Err(AppError::Conflict(
                "child batch already finished".into(),
            )));
        };
        let mut pending = false;
        for slot in &mut batch.tasks {
            let ChildSlot::Pending(task) = slot else {
                continue;
            };
            let result = match task.as_mut().poll(cx) {
                Poll::Pending if batch.mode == ActionMode::Synchronous => return Poll::Pending,
                Poll::Pending => {
                    pending = true;
                    continue;
                }
                Poll::Ready(result) => result,
            };
            // Dropping the completed future releases its resources while its
            // exact completion evidence remains owned until the batch returns.
            match (batch.mode, result) {
                (ActionMode::Synchronous, Err(error)) => {
                    batch.tasks.clear();
                    batch.state = ChildBatchState::Finished;
                    return Poll::Ready(Err(error));
                }
                (_, result) => *slot = ChildSlot::Completed(result),
            }
        }
        if pending {
            return Poll::Pending;
        }
        let results = std::mem::take(&mut batch.tasks)
            .into_iter()
            .map(|slot| match slot {
                ChildSlot::Completed(result) => result,
                ChildSlot::Pending(_) => Err(AppError::Integration(
                    "child completion is unavailable".into(),
                )),
            })
            .collect();
        batch.state = ChildBatchState::Finished;
        Poll::Ready(results)
    }
}

impl ChildCompletion {
    pub(super) fn admitted(
        record: RunRecord,
        parent: &RunRecord,
        definition: &CompiledDefinition,
    ) -> Result<Self, AppError> {
        record.validate_child(parent, definition)?;
        if !record.checkpoint.is_terminal() {
            return Err(AppError::Conflict(
                "child has no terminal completion".into(),
            ));
        }
        let ordinal = record
            .parent
            .as_ref()
            .ok_or_else(|| AppError::Integration("child parent evidence unavailable".into()))?
            .index;
        Ok(Self {
            ordinal,
            record: Box::new(record),
        })
    }

    /// Returns this child's ordinal in the original admitted batch.
    ///
    /// See [the complete child executor workflow](ChildExecutor).
    pub fn ordinal(&self) -> usize {
        self.ordinal
    }

    pub(super) fn record(&self) -> &RunRecord {
        &self.record
    }

    pub(super) fn into_record(self) -> RunRecord {
        *self.record
    }
}

/// Default executor that applies the batch's actual execution-mode policy.
///
/// See [the complete child executor workflow](ChildExecutor).
#[derive(Default)]
pub struct InlineExecutor;

impl ChildExecutor for InlineExecutor {
    type Execution<'a, F>
        = ChildBatch<F>
    where
        Self: 'a,
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a;

    fn execute<'a, F>(&'a self, batch: ChildBatch<F>) -> Self::Execution<'a, F>
    where
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a,
    {
        batch
    }
}
