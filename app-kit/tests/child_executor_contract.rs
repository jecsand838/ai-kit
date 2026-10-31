//! App's admitted-child seam preserves concrete local futures and membership.
//! The ordering oracle is the fixed rurr parallelism executor/task-action path;
//! these cases do not claim to execute the vendored package.

use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::future::Future;
use std::marker::PhantomPinned;
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread::ThreadId;
use std::time::Duration;

use agents_kit::Agent;
use app_kit::workflow::{
    ActionBackend, ActionContext, ActionDeclaration, ActionEvent, ActionMode, ActionRequest,
    CheckpointSelector, ChildBatch, ChildCompletion, ChildExecutor, ExecutionEvent,
    ExecutionOptions, InlineExecutor, Interaction, LifecycleEvent, LifecycleHooks, Resolution,
    RunSelector, RunStatus, StateChange, StateTyping, SyncExecution, WorkflowDefinition,
    WorkflowFailure, WorkflowState,
};
use app_kit::{App, AppConfig, AppError, Shutdown, WorkspaceConfig};
use futures_util::{FutureExt, Stream};
use serde_json::{Map, Value, json};
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_kit::state::record::{
    StateKey, StateKeyPrefix, StateListRequest, StateListResponse, StateRecord, StateRecordSnapshot,
};
use storage_kit::{
    StateStore, StateStoreBackend, StateStoreError, StateTransaction, StateTransactionBackend,
};
use tokio::sync::Notify;

// The source type intentionally has neither Send nor Unpin, and borrows data
// rather than requiring 'static. No fixture can manufacture ChildCompletion.
struct LocalChild<'a> {
    polls: &'a Cell<usize>,
    local: Rc<()>,
    pinned: PhantomPinned,
}

impl Future for LocalChild<'_> {
    type Output = Result<ChildCompletion, AppError>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let source = self.as_ref().get_ref();
        source.polls.set(source.polls.get() + 1);
        assert_eq!(Rc::strong_count(&source.local), 1);
        let _ = &source.pinned;
        Poll::Ready(Err(AppError::Conflict("caller child fixture".into())))
    }
}

struct ChildSignatures;

impl ChildSignatures {
    fn inline<'a>(batch: ChildBatch<LocalChild<'a>>) -> ChildBatch<LocalChild<'a>> {
        InlineExecutor.execute(batch)
    }

    fn transfer<'a>(
        batch: ChildBatch<LocalChild<'a>>,
    ) -> Result<Vec<Pin<Box<LocalChild<'a>>>>, AppError> {
        let count = batch.len();
        assert_eq!(count == 0, batch.is_empty());
        batch.into_tasks()
    }

    fn future<'a>(
        batch: ChildBatch<LocalChild<'a>>,
    ) -> impl Future<Output = Result<Vec<ChildCompletion>, AppError>> + 'a {
        batch
    }
}

// The custom executor itself is Send + Sync and non-Clone. Its returned future
// is local and non-Unpin; only the already pinned admitted children are moved.
struct ReverseExecutor {
    observed: Arc<Mutex<Vec<usize>>>,
    effects: Arc<ChildEffects>,
}

struct EmptyExecutor {
    batches: Arc<Mutex<Vec<(ActionMode, usize)>>>,
}

impl ChildExecutor for EmptyExecutor {
    type Execution<'a, F>
        = ChildBatch<F>
    where
        Self: 'a,
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a;

    fn execute<'a, F>(&'a self, batch: ChildBatch<F>) -> Self::Execution<'a, F>
    where
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a,
    {
        self.batches
            .lock()
            .unwrap()
            .push((batch.mode(), batch.len()));
        assert!(batch.is_empty());
        InlineExecutor.execute(batch)
    }
}

struct FaultExecutor {
    effects: Arc<ChildEffects>,
}

impl ChildExecutor for FaultExecutor {
    type Execution<'a, F>
        = ChildBatch<F>
    where
        Self: 'a,
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a;

    fn execute<'a, F>(&'a self, batch: ChildBatch<F>) -> Self::Execution<'a, F>
    where
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a,
    {
        assert_eq!(batch.mode(), ActionMode::Asynchronous);
        assert_eq!(batch.len(), 2);
        self.effects.executions.fetch_add(1, Ordering::SeqCst);
        if self.effects.ordered_admission {
            self.effects.parent_checkpoint_entered.notify_one();
        }
        if matches!(
            self.effects.fault,
            ChildFault::ReadPanic | ChildFault::ReadyReadPanic
        ) {
            // The public executor receives admission before any child is
            // polled. Admission/setup reads have already finished; this arms
            // one lower read owned by the actual first child future.
            self.effects.read_fault_armed.store(true, Ordering::SeqCst);
        }
        InlineExecutor.execute(batch)
    }
}

#[derive(Default)]
enum ChildFault {
    #[default]
    None,
    RejectedMutation,
    ReadPanic,
    ReadyReadPanic,
    PermanentRead,
    StageReceipt,
    StageReceiptLoss,
    HeldVerification,
    CompletionRead {
        boundary: CompletionReadBoundary,
        failure: AuthorityReadFault,
    },
    PermanentAfterHook,
    RecoverableAfterHook,
    HeldExecutionRead,
    HeldAfterHook,
    ParentStopReadFailure,
    ParentStopReadPermanent,
    ParentStopReadCorruption,
    ParentStopReadHeld,
    StaleParentRead,
    StoppingRead {
        boundary: StoppingReadBoundary,
        failure: AuthorityReadFault,
    },
    ChildInventoryRead {
        failure: AuthorityReadFault,
        shared_budget: bool,
    },
    QueuedParentRead {
        failure: AuthorityReadFault,
        shared_budget: bool,
    },
}

#[derive(Clone, Copy)]
enum CompletionReadBoundary {
    Child { shared_budget: bool },
    Parent { shared_budget: bool },
    ParentFailure,
}

#[derive(Clone, Copy)]
enum StoppingReadBoundary {
    Parent { retired: bool },
    Descendants { shared_budget: bool },
}

#[derive(Clone, Copy)]
enum AuthorityReadFault {
    Recoverable,
    Permanent,
    Held,
}

#[derive(Default)]
struct ChildEffects {
    ordered_admission: bool,
    ordered_receipt: Mutex<Option<StateKey>>,
    ordered_readbacks: AtomicUsize,
    step_outcomes: Mutex<Vec<(String, u64, bool)>>,
    hold_source_item: bool,
    source_item_entered: Notify,
    source_item_release: Notify,
    source_item_released: AtomicBool,
    values: Mutex<Vec<i64>>,
    mutation_threads: Mutex<Vec<ThreadId>>,
    hook_events: Mutex<Vec<(&'static str, String, ActionMode)>>,
    hold_child_finished: AtomicBool,
    held_child_finished: Mutex<Option<String>>,
    child_finished_entered: Notify,
    child_finished_release: Notify,
    async_hooks: AtomicUsize,
    gate_first_commit: bool,
    second_polled: Notify,
    fault: ChildFault,
    executions: AtomicUsize,
    execution_returns: AtomicUsize,
    incomplete_execution_owner: Mutex<Option<ThreadId>>,
    verification_read_pending: AtomicBool,
    verification_read_entered: Notify,
    verification_read_release: Notify,
    verification_read_returned: AtomicBool,
    verification_read_retried: AtomicBool,
    faults: AtomicUsize,
    read_fault_armed: AtomicBool,
    panic_recovery_pending: AtomicBool,
    panic_recovery_entered: Notify,
    panic_recovery_release: Notify,
    panic_recovery_returned: AtomicBool,
    panic_recovery_retried: AtomicBool,
    fault_key: Mutex<Option<StateKey>>,
    faulted_record: Mutex<Option<Value>>,
    decision_read_remaining: AtomicUsize,
    decision_read_observed: Notify,
    held_execution_read: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    execution_drops: AtomicUsize,
    executor_drops: AtomicUsize,
    failure_commits: AtomicUsize,
    stop_read_started: AtomicBool,
    stop_read_release: Notify,
    stop_read_released: AtomicBool,
    stop_read_recovered: Notify,
    cancellations: Mutex<Vec<ChildCancellation>>,
    hold_parent_checkpoint: AtomicBool,
    parent_checkpoint_entered: Notify,
    parent_checkpoint_release: Notify,
    child_constructions: AtomicUsize,
    hold_cancellation_receipt: bool,
    cancellation_receipt_entered: Notify,
    cancellation_receipt_release: Notify,
    cancellation_receipt_released: AtomicBool,
    observing_startup: AtomicBool,
    startup_reads: Mutex<BTreeSet<String>>,
}

impl ChildEffects {
    fn observe_startup_read(&self, key: &StateKey) {
        if self.observing_startup.load(Ordering::SeqCst)
            && (key.key().starts_with("run/") || key.key().starts_with("history/"))
        {
            self.startup_reads
                .lock()
                .unwrap()
                .insert(key.key().to_owned());
        }
    }

    fn reorder_admitted_input(&self, key: &StateKey, snapshot: &mut Option<StateRecordSnapshot>) {
        if self.ordered_admission
            && self.read_fault_armed.load(Ordering::SeqCst)
            && self.faults.load(Ordering::SeqCst) == 2
            && self.fault_key.lock().unwrap().as_ref() == Some(key)
            && let Some(snapshot) = snapshot
        {
            let original = Arc::clone(&snapshot.record.value);
            let control = Arc::make_mut(&mut snapshot.record.value)["checkpoint"]["controls"]
                .as_object_mut()
                .unwrap()
                .values_mut()
                .next()
                .unwrap();
            let input = control["state"]["inputs"][0]["input"]
                .as_object_mut()
                .unwrap();
            assert_eq!(input.len(), 2, "the real admitted task has two input keys");
            *input = std::mem::take(input).into_iter().rev().collect();
            assert_eq!(
                snapshot.record.value, original,
                "only insertion order changes"
            );
            assert_ne!(
                serde_json::to_vec(snapshot.record.value.as_ref()).unwrap(),
                serde_json::to_vec(original.as_ref()).unwrap()
            );
            self.ordered_readbacks.fetch_add(1, Ordering::SeqCst);
        }
    }

    async fn queued_parent_read(
        &self,
        key: &StateKey,
        snapshot: &Option<StateRecordSnapshot>,
        current: &Option<StateRecordSnapshot>,
    ) -> Result<(), StateStoreError> {
        let selected = self.fault_key.lock().unwrap().as_ref() == Some(key);
        if let ChildFault::QueuedParentRead {
            failure,
            shared_budget,
        } = self.fault
            && self.read_fault_armed.load(Ordering::SeqCst)
        {
            if shared_budget
                && current.as_ref().is_some_and(|row| {
                    self.fault_key.lock().unwrap().as_ref() == Some(&row.record.key)
                })
                && self.verification_read_pending.swap(false, Ordering::SeqCst)
            {
                // The real head names the selected immutable row. Both reads
                // belong to one queued parent's authority observation pass.
                tokio::time::sleep(Duration::from_secs(20)).await;
            }
            if selected && self.read_fault_armed.swap(false, Ordering::SeqCst) {
                *self.faulted_record.lock().unwrap() = current
                    .as_ref()
                    .or(snapshot.as_ref())
                    .map(|row| row.record.value.as_ref().clone());
                self.faults.fetch_add(1, Ordering::SeqCst);
                if matches!(failure, AuthorityReadFault::Held) {
                    let (held, receipt) = tokio::sync::oneshot::channel();
                    *self.held_execution_read.lock().unwrap() = Some(receipt);
                    self.decision_read_observed.notify_one();
                    self.verification_read_release.notified().await;
                    let _ = held.send(());
                } else {
                    self.decision_read_observed.notify_one();
                }
                return Err(if matches!(failure, AuthorityReadFault::Permanent) {
                    StateStoreError::serialization("queued child parent authority is unreadable")
                } else {
                    StateStoreError::unavailable(
                        "load",
                        "queued child parent authority is unavailable",
                    )
                });
            }
        }
        Ok(())
    }

    fn release_stop_read(&self) {
        self.stop_read_released.store(true, Ordering::SeqCst);
        self.stop_read_release.notify_waiters();
    }

    async fn await_stop_read_release(&self) {
        let released = self.stop_read_release.notified();
        tokio::pin!(released);
        released.as_mut().enable();
        if !self.stop_read_released.load(Ordering::SeqCst) {
            released.await;
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ChildCancellation {
    run: String,
    occurrence: String,
    parent: String,
    ordinal: u64,
    item: i64,
}

// This is an actual callback inside each child's State node, not a record of
// when the executor retrieves a future whose work might already be completed.
struct ChildTyping {
    effects: Arc<ChildEffects>,
}

impl StateTyping for ChildTyping {
    type Data = Rc<WorkflowState>;

    fn construct_data(&self, state: &WorkflowState) -> Result<Self::Data, AppError> {
        if state.values().contains_key("item") {
            self.effects
                .child_constructions
                .fetch_add(1, Ordering::SeqCst);
        }
        Ok(Rc::new(state.clone()))
    }

    fn construct_state(&self, data: &Self::Data) -> Result<WorkflowState, AppError> {
        Ok(data.as_ref().clone())
    }

    fn validate_pre(&self, state: &WorkflowState, _change: &StateChange) -> Result<(), AppError> {
        if let Some(item) = state.values().get("item") {
            let item = item.as_i64().expect("child item");
            self.effects
                .mutation_threads
                .lock()
                .unwrap()
                .push(std::thread::current().id());
            self.effects.values.lock().unwrap().push(item);
            if item == 20 {
                self.effects.second_polled.notify_one();
            }
            if item == 10 && matches!(self.effects.fault, ChildFault::RejectedMutation) {
                self.effects.faults.fetch_add(1, Ordering::SeqCst);
                return Err(AppError::InvalidInput(
                    "caller rejects first child mutation".into(),
                ));
            }
        }
        Ok(())
    }
}

impl LifecycleHooks for ChildTyping {
    fn observe(&self, event: &LifecycleEvent<'_>) -> Result<(), AppError> {
        let observed = match event {
            LifecycleEvent::Registered {
                run,
                parent: Some(parent),
                ..
            } => Some(("registered", run.to_string(), parent.mode)),
            LifecycleEvent::StepStarted { context, .. } => {
                Some(("start", context.node.clone(), context.mode))
            }
            LifecycleEvent::StreamItem { context, .. } => {
                Some(("item", context.node.clone(), context.mode))
            }
            LifecycleEvent::StepFinished {
                context, outcome, ..
            } => {
                let observed = (
                    context.node.clone(),
                    context.action_ordinal,
                    outcome.is_none(),
                );
                self.effects.step_outcomes.lock().unwrap().push(observed);
                Some(("finish", context.node.clone(), context.mode))
            }
            LifecycleEvent::ChildFinished { parent, snapshot } => {
                Some(("child_finished", snapshot.id.to_string(), parent.mode))
            }
            _ => None,
        };
        if let Some(observed) = observed {
            self.effects.hook_events.lock().unwrap().push(observed);
        }
        Ok(())
    }

    fn observe_async<'a>(
        &'a self,
        event: &'a LifecycleEvent<'a>,
    ) -> impl Future<Output = Result<(), AppError>> + Send + 'a {
        self.effects.async_hooks.fetch_add(1, Ordering::SeqCst);
        async move {
            if let LifecycleEvent::ChildFinished { snapshot, .. } = event
                && self
                    .effects
                    .hold_child_finished
                    .swap(false, Ordering::SeqCst)
            {
                *self.effects.held_child_finished.lock().unwrap() = Some(snapshot.id.to_string());
                self.effects.child_finished_entered.notify_one();
                self.effects.child_finished_release.notified().await;
            }
            if self.effects.hold_source_item
                && matches!(event, LifecycleEvent::StreamItem { context, .. } if context.node == "a_source")
            {
                let release = self.effects.source_item_release.notified();
                tokio::pin!(release);
                release.as_mut().enable();
                self.effects.source_item_entered.notify_one();
                if !self.effects.source_item_released.load(Ordering::SeqCst) {
                    release.await;
                }
            }
            if matches!(
                self.effects.fault,
                ChildFault::PermanentAfterHook
                    | ChildFault::RecoverableAfterHook
                    | ChildFault::HeldAfterHook
            ) && matches!(event, LifecycleEvent::StepStarted { state, .. }
                    if state.values().get("item") == Some(&json!(10)))
            {
                // This original public pre-hook returns before the selected
                // child's next authority read can fail. No effect has run.
                self.effects
                    .decision_read_remaining
                    .store(1, Ordering::SeqCst);
            }
            Ok(())
        }
    }
}

#[tokio::test]
async fn terminal_child_observation_survives_dropping_every_public_run_handle() {
    let effects = Arc::new(ChildEffects {
        hold_child_finished: AtomicBool::new(true),
        ..ChildEffects::default()
    });
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let config: AppConfig<StateStore, Agent> =
        AppConfig::new(store, Vec::new(), ChildDefinitions::all(Some(5000), 2));
    let mut app = App::open(
        config
            .with_hooks(ChildTyping {
                effects: Arc::clone(&effects),
            })
            .with_state_typing(ChildTyping {
                effects: Arc::clone(&effects),
            }),
    )
    .await
    .unwrap();
    let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(8), async {
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().unwrap(),
            })
            .await
            .unwrap();
        let workflow = workspace
            .workflow("executor-parent".parse().unwrap())
            .await
            .unwrap();
        let child_workflow = workspace
            .workflow("executor-child".parse().unwrap())
            .await
            .unwrap();
        let parent = workflow.start(ExecutionOptions::default()).await.unwrap();
        let parent_id = parent.id().clone();
        effects.child_finished_entered.notified().await;
        let child_id = effects
            .held_child_finished
            .lock()
            .unwrap()
            .clone()
            .unwrap()
            .parse()
            .unwrap();
        let mut child = child_workflow
            .open_run(RunSelector::Latest(child_id))
            .await
            .unwrap();
        let child_snapshot = child.snapshot().await.unwrap();
        assert!(matches!(child_snapshot.status, RunStatus::Succeeded));
        let child_selector = RunSelector::Exact {
            id: child_snapshot.id.clone(),
            occurrence: child_snapshot.occurrence.clone(),
        };
        let second = child_workflow
            .open_run(child_selector.clone())
            .await
            .unwrap();
        assert!(
            matches!(
                child.delete(child_snapshot.checkpoint).await,
                Err(AppError::Busy(_))
            ),
            "the unreduced parent retains exact child membership"
        );
        let pending = parent.snapshot().await.unwrap();
        assert_eq!(pending.state.values()["collected"], json!([]));
        assert!(pending.outcomes.is_empty());
        drop(child);
        drop(second);
        drop(parent);
        effects.child_finished_release.notify_one();
        let parent = workflow
            .open_run(RunSelector::Latest(parent_id))
            .await
            .unwrap();
        let terminal = parent.wait().await.unwrap();
        assert!(matches!(terminal.status, RunStatus::Succeeded));
        assert_eq!(terminal.state.values()["collected"], json!([11, 21]));
        assert_eq!(terminal.outcomes.len(), 1);
        let child = child_workflow.open_run(child_selector).await.unwrap();
        let retained = child.wait().await.unwrap();
        assert!(matches!(retained.status, RunStatus::Succeeded));
        assert_eq!(retained.checkpoint, child_snapshot.checkpoint);
        assert_eq!(retained.state, child_snapshot.state);
        let mut values = effects.values.lock().unwrap().clone();
        values.sort();
        assert_eq!(
            values,
            [10, 20],
            "dropping observers never replays the child work"
        );
        let events = effects.hook_events.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|(event, _, _)| *event == "child_finished")
                .count(),
            2
        );
        assert_eq!(
            events
                .iter()
                .filter(
                    |(event, run, _)| *event == "child_finished" && *run == retained.id.to_string()
                )
                .count(),
            1
        );
    }))
    .catch_unwind()
    .await;
    effects.child_finished_release.notify_one();
    let closed = tokio::time::timeout(
        Duration::from_secs(5),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        }),
    )
    .await;
    drop(app);
    match checked {
        Ok(result) => result.expect("the held child observation remains bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    closed
        .expect("child and parent observation owners join")
        .unwrap();
}

struct ReverseChildren<F> {
    state: RefCell<ReverseState<F>>,
    local: Rc<Cell<usize>>,
    observed: Arc<Mutex<Vec<usize>>>,
    effects: Arc<ChildEffects>,
    pinned: PhantomPinned,
}

struct ReverseState<F> {
    tasks: Vec<Pin<Box<F>>>,
    completions: Vec<ChildCompletion>,
}

impl ChildExecutor for ReverseExecutor {
    type Execution<'a, F>
        = ReverseChildren<F>
    where
        Self: 'a,
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a;

    fn execute<'a, F>(&'a self, batch: ChildBatch<F>) -> Self::Execution<'a, F>
    where
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a,
    {
        let invocation = self.effects.executions.fetch_add(1, Ordering::SeqCst);
        if invocation == 0 {
            let no_effects = self.effects.values.lock().unwrap().is_empty();
            assert!(
                no_effects,
                "admission must not execute child effects before their executor polls"
            );
        }
        assert_eq!(batch.mode(), ActionMode::Asynchronous);
        assert_eq!(
            batch.len(),
            2,
            "executor receives the complete admitted batch"
        );
        assert!(!batch.is_empty());
        ReverseChildren {
            state: RefCell::new(ReverseState {
                tasks: batch
                    .into_tasks()
                    .expect("unpolled batch transfers exact pins"),
                completions: Vec::new(),
            }),
            local: Rc::new(Cell::new(0)),
            observed: Arc::clone(&self.observed),
            effects: Arc::clone(&self.effects),
            pinned: PhantomPinned,
        }
    }
}

impl<F: Future<Output = Result<ChildCompletion, AppError>>> Future for ReverseChildren<F> {
    type Output = Result<Vec<ChildCompletion>, AppError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let execution = self.as_ref().get_ref();
        execution.local.set(execution.local.get() + 1);
        let _ = &execution.pinned;
        let mut state = execution.state.borrow_mut();
        while state.completions.len() < state.tasks.len() {
            // Retain every original pinned future after Ready. At parallelism
            // one the next child must progress without dropping the first.
            let remaining = state.tasks.len() - state.completions.len();
            let result = state.tasks[remaining - 1].as_mut().poll(cx);
            let completion = match result {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(completion)) => completion,
            };
            execution
                .observed
                .lock()
                .unwrap()
                .push(completion.ordinal());
            state.completions.push(completion);
        }
        execution
            .effects
            .execution_returns
            .fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Ok(std::mem::take(&mut state.completions)))
    }
}

impl<F> Drop for ReverseChildren<F> {
    fn drop(&mut self) {
        self.effects.execution_drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl Drop for ReverseExecutor {
    fn drop(&mut self) {
        self.effects.executor_drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn child_signatures_accept_borrowed_local_non_unpin_futures() {
    // Naming the consumers type-checks the public methods without introducing
    // an unauthorized public ChildBatch or ChildCompletion constructor.
    let _inline = ChildSignatures::inline;
    let _transfer = ChildSignatures::transfer;
    let _future = ChildSignatures::future;
    let polls = Cell::new(0);
    let mut source = Box::pin(LocalChild {
        polls: &polls,
        local: Rc::new(()),
        pinned: PhantomPinned,
    });
    let mut context = Context::from_waker(futures_util::task::noop_waker_ref());
    assert!(matches!(source.as_mut().poll(&mut context),
        Poll::Ready(Err(AppError::Conflict(message))) if message == "caller child fixture"));
    assert_eq!(polls.get(), 1);
}

struct ChildDefinitions;

enum RevisionCase {
    Behavior,
    Schema,
    ReversedDependencies,
}

impl ChildDefinitions {
    fn revision_definitions(include_new: bool, case: &RevisionCase) -> Vec<WorkflowDefinition> {
        let mut definitions = Vec::new();
        for (revision, field, increment) in [("v1", "old", 1), ("v2", "new", 100)] {
            if revision == "v2" && !include_new {
                break;
            }
            let (child_id, parent_id) =
                if revision == "v2" && matches!(case, RevisionCase::ReversedDependencies) {
                    ("executor-parent", "executor-child")
                } else {
                    ("executor-child", "executor-parent")
                };
            let mut child = json!({
                "schema":"app-kit.workflow/v2", "id":child_id, "revision":revision,
                "state":{"item":0}, "required":["item"], "reducers":{"item":"reject_concurrent"},
                "entry":"add", "nodes":[{"id":"add", "step":{"kind":"state", "mutations":[
                    {"kind":"increment", "target":"item", "value":{"kind":"literal","value":increment}}
                ]}}], "routes":{"add":{"mode":"first","branches":[{"target":{"kind":"end"}}]}}
            });
            if matches!(case, RevisionCase::Schema) {
                child["state"][field] = json!(0);
                child["required"].as_array_mut().unwrap().push(json!(field));
                child["reducers"][field] = json!("reject_concurrent");
                child["nodes"][0]["step"]["mutations"][0]["value"] =
                    json!({"kind":"state", "path":field});
            }
            definitions
                .push(WorkflowDefinition::parse(&child.to_string()).expect("child revision"));
            if revision == "v2" && matches!(case, RevisionCase::Behavior) {
                continue;
            }
            let mut parent = json!({
                "schema":"app-kit.workflow/v2", "id":parent_id, "revision":revision,
                "state":{"items":[10,20],"collected":[]}, "required":["items","collected"],
                "reducers":{"items":"reject_concurrent","collected":"collect"}, "entry":"wait",
                "nodes":[
                    {"id":"wait", "step":{"kind":"input", "message":[{"kind":"text","text":"Admit children after reopen"}]}},
                    {"id":"map", "step":{"kind":"map", "workflow":child_id,
                        "items":{"kind":"state","path":"items"}, "input_key":"item", "max_parallel":2,
                        "reductions":[{"source":"item","target":"collected","reducer":"collect"}]}}
                ],
                "routes":{
                    "wait":{"mode":"first","branches":[{"target":{"kind":"node","id":"map"}}]},
                    "map":{"mode":"first","branches":[{"target":{"kind":"end"}}]}
                }
            });
            if matches!(case, RevisionCase::Schema) {
                parent["nodes"][1]["step"]["input"] =
                    json!({field:{"kind":"literal", "value":increment}});
            }
            definitions
                .push(WorkflowDefinition::parse(&parent.to_string()).expect("parent revision"));
        }
        definitions
    }

    const CHILD: &str = r#"{
      "schema":"app-kit.workflow/v2","id":"executor-child","revision":"v1","entry":"add",
      "state":{"item":0},"required":["item"],"reducers":{"item":"reject_concurrent"},
      "nodes":[{"id":"add","step":{"kind":"state","mutations":[
        {"kind":"increment","target":"item","value":{"kind":"literal","value":1}}
      ]}}],"routes":{"add":{"mode":"first","branches":[{"target":{"kind":"end"}}]}}
    }"#;

    fn all(deadline_millis: Option<u64>, max_parallel: usize) -> Vec<WorkflowDefinition> {
        let child = WorkflowDefinition::parse(Self::CHILD).expect("child definition");
        let mut parent: serde_json::Value = serde_json::from_str(
            r#"{
      "schema":"app-kit.workflow/v2","id":"executor-parent","revision":"v1","entry":"map",
      "state":{"items":[10,20],"collected":[]},"required":["items","collected"],
      "reducers":{"items":"reject_concurrent","collected":"collect"},
      "nodes":[{"id":"map","step":{"kind":"map","workflow":"executor-child",
        "items":{"kind":"state","path":"items"},"input_key":"item","max_parallel":2,
        "reductions":[{"source":"item","target":"collected","reducer":"collect"}]}}],
      "routes":{"map":{"mode":"first","branches":[{"target":{"kind":"end"}}]}}
    }"#,
        )
        .expect("parent definition JSON");
        if let Some(deadline_millis) = deadline_millis {
            parent["nodes"][0]["deadline_ms"] = json!(deadline_millis);
        }
        parent["nodes"][0]["step"]["max_parallel"] = json!(max_parallel);
        let parent = WorkflowDefinition::parse(&parent.to_string()).expect("parent definition");
        vec![child, parent]
    }
}

#[tokio::test]
async fn custom_executor_reverses_polling_but_app_reduces_original_child_order() {
    for max_parallel in [2, 1] {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let effects = Arc::new(ChildEffects::default());
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("real transactional state");
        let config: AppConfig<StateStore, Agent> = AppConfig::new(
            store,
            Vec::new(),
            ChildDefinitions::all(Some(5000), max_parallel),
        );
        let mut app = tokio::time::timeout(
            Duration::from_secs(3),
            App::open(
                config
                    .with_state_typing(ChildTyping {
                        effects: Arc::clone(&effects),
                    })
                    .with_child_executor(ReverseExecutor {
                        observed: Arc::clone(&observed),
                        effects: Arc::clone(&effects),
                    }),
            ),
        )
        .await
        .expect("caller executor fixture opens within its bound")
        .expect("caller executor accepted directly");
        let checked =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), async {
                let workspace = app
                    .create_workspace(WorkspaceConfig {
                        name: Some("executor-contract".into()),
                        root: std::env::current_dir().unwrap(),
                    })
                    .await
                    .expect("workspace");
                let workflow = workspace
                    .workflow("executor-parent".parse().unwrap())
                    .await
                    .unwrap();
                let run = workflow
                    .start(ExecutionOptions::default())
                    .await
                    .expect("admit parent and real children");
                let snapshot = run.wait().await.expect("terminal parent");
                assert!(matches!(snapshot.status, RunStatus::Succeeded));
                assert_eq!(
                    snapshot.state.values().get("collected"),
                    Some(&json!([11, 21]))
                );
                let values = effects.values.lock().unwrap().clone();
                assert_eq!(
                    values,
                    [20, 10],
                    "the executor controls real child State effects, not only result retrieval"
                );
                let order = observed.lock().unwrap().clone();
                assert_eq!(
                    order,
                    [1, 0],
                    "retaining the completed first future cannot retain its parallelism slot"
                );
                assert_eq!(effects.executions.load(Ordering::SeqCst), 1);
                assert_eq!(effects.execution_returns.load(Ordering::SeqCst), 1);
            }))
            .catch_unwind()
            .await;
        let shutdown = if matches!(&checked, Ok(Ok(()))) {
            Shutdown::Drain {
                timeout: Duration::from_secs(3),
            }
        } else {
            Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }
        };
        let cleanup = std::panic::AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(shutdown),
        ))
        .catch_unwind()
        .await;
        drop(app);
        match checked {
            Ok(result) => {
                result.expect("both children complete while their Ready futures stay allocated")
            }
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup
            .expect("executor cleanup does not panic")
            .expect("executor cleanup joins")
            .unwrap();
        assert_eq!(effects.execution_drops.load(Ordering::SeqCst), 1);
    }
}

// The gate is before the native commit call. If App cancels this test operation,
// the still-uncommitted real transaction follows its ordinary rollback-on-drop
// contract. Once inner.commit() is called, the real backend retains authority.
struct ChildStore {
    inner: StateStore,
    effects: Arc<ChildEffects>,
}

struct ChildTransaction {
    inner: StateTransaction,
    effects: Arc<ChildEffects>,
    wait_for_second: bool,
    recovering_first: bool,
    cancellation: Option<ChildCancellation>,
    parent_checkpoint: bool,
    ordered_admission: Option<StateRecord>,
    receipt: Option<StateKey>,
}

impl ChildStore {
    fn has_single_control(record: &serde_json::Value, kind: &str) -> bool {
        record["checkpoint"]["controls"]
            .as_object()
            .is_some_and(|controls| {
                controls.len() == 1
                    && controls
                        .values()
                        .any(|control| control["state"]["kind"] == kind)
            })
    }

    fn is_checkpoint(record: &StateRecord) -> bool {
        let Some(workspace) = record
            .value
            .get("workspace_id")
            .and_then(serde_json::Value::as_str)
        else {
            return false;
        };
        let Some(workflow) = record
            .value
            .get("workflow_id")
            .and_then(serde_json::Value::as_str)
        else {
            return false;
        };
        let Some(run) = record.value.get("id").and_then(serde_json::Value::as_str) else {
            return false;
        };
        let Some(occurrence) = record.value.get("occurrence").and_then(Value::as_str) else {
            return false;
        };
        let Some(sequence) = record
            .value
            .pointer("/checkpoint/sequence")
            .and_then(serde_json::Value::as_u64)
        else {
            return false;
        };
        record.key.key()
            == format!(
                "history/{workspace}/{workflow}/{run}/{occurrence}/checkpoint/{sequence:020}"
            )
    }

    async fn current_record(
        store: &StateStore,
        head: &StateRecordSnapshot,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        let Some(scope) = head.record.key.key().strip_prefix("run/") else {
            return Ok(None);
        };
        let Some(sequence) = head
            .record
            .value
            .get("sequence")
            .and_then(serde_json::Value::as_u64)
        else {
            return Ok(None);
        };
        let row = store
            .load(StateKey::new(
                "app-kit",
                format!("history/{scope}/checkpoint/{sequence:020}"),
            )?)
            .await?;
        if let Some(row) = &row {
            assert!(Self::is_checkpoint(&row.record));
            for field in ["id", "workspace_id", "workflow_id"] {
                assert_eq!(row.record.value[field], head.record.value[field]);
            }
            assert_eq!(
                row.record.value["occurrence"],
                head.record.value["creation"]
            );
            assert_eq!(row.record.value["checkpoint"]["sequence"], json!(sequence));
        }
        Ok(row)
    }
}

impl StateStoreBackend for ChildStore {
    type Transaction = ChildTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        let obscure_receipt = if self.effects.ordered_admission {
            let mut receipt = self.effects.ordered_receipt.lock().unwrap();
            if receipt.as_ref() == Some(&key) {
                receipt.take();
                true
            } else {
                false
            }
        } else {
            false
        };
        if obscure_receipt {
            assert_eq!(self.effects.faults.fetch_add(1, Ordering::SeqCst), 1);
            return Err(StateStoreError::unavailable(
                "load",
                "original child admission receipt unavailable",
            ));
        }
        let after_panic_recovery = self.effects.panic_recovery_returned.load(Ordering::SeqCst)
            && self.effects.fault_key.lock().unwrap().as_ref() == Some(&key);
        if matches!(self.effects.fault, ChildFault::ReadPanic)
            && self.effects.faults.load(Ordering::SeqCst) > 0
            && self.effects.fault_key.lock().unwrap().as_ref() == Some(&key)
            && self
                .effects
                .panic_recovery_pending
                .swap(false, Ordering::SeqCst)
        {
            // Only a new head read after the actual panic can consume this
            // held transient error. Earlier admitted reads cannot satisfy it.
            self.effects.panic_recovery_entered.notify_one();
            self.effects.panic_recovery_release.notified().await;
            self.effects
                .panic_recovery_returned
                .store(true, Ordering::SeqCst);
            return Err(StateStoreError::unavailable(
                "load",
                "one panic reconciliation read failed",
            ));
        }
        let after_executor_return = self.effects.execution_returns.load(Ordering::SeqCst) > 0;
        let after_verification_error = self
            .effects
            .verification_read_returned
            .load(Ordering::SeqCst);
        self.effects.observe_startup_read(&key);
        if self.effects.executions.load(Ordering::SeqCst) > 0
            && self.effects.fault_key.lock().unwrap().as_ref() == Some(&key)
            && self
                .effects
                .decision_read_remaining
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        {
            // Real admission selected this exact child head. E must have
            // received the complete batch before its retained read can fail.
            self.effects.faults.fetch_add(1, Ordering::SeqCst);
            if matches!(
                self.effects.fault,
                ChildFault::HeldExecutionRead | ChildFault::HeldAfterHook
            ) {
                let (held, receipt) = tokio::sync::oneshot::channel();
                *self.effects.held_execution_read.lock().unwrap() = Some(receipt);
                self.effects.decision_read_observed.notify_one();
                self.effects.verification_read_release.notified().await;
                let _ = held.send(());
            } else {
                self.effects.decision_read_observed.notify_one();
            }
            return Err(
                if matches!(
                    self.effects.fault,
                    ChildFault::PermanentRead | ChildFault::PermanentAfterHook
                ) {
                    StateStoreError::serialization(
                        "selected child execution authority is unreadable",
                    )
                } else {
                    StateStoreError::unavailable(
                        "load",
                        "selected native child read is temporarily unavailable",
                    )
                },
            );
        }
        let after_stop_failure = self.effects.faults.load(Ordering::SeqCst) == 1;
        let selected = self.effects.fault_key.lock().unwrap().as_ref() == Some(&key);
        let stop_read_ready = if selected
            && matches!(
                self.effects.fault,
                ChildFault::ParentStopReadFailure
                    | ChildFault::ParentStopReadPermanent
                    | ChildFault::ParentStopReadCorruption
                    | ChildFault::ParentStopReadHeld
            ) {
            if self.effects.stop_read_started.load(Ordering::SeqCst) {
                true
            } else {
                let parent = key
                    .key()
                    .rsplit('/')
                    .nth(1)
                    .expect("selected parent identity");
                let children = ChildDefinitions::admitted_records(&self.inner, parent).await;
                children.len() == 2
                    && children[0]["checkpoint"]["phase"]["state"] == "succeeded"
                    && Self::has_single_control(&children[1], "input")
            }
        } else {
            false
        };
        if matches!(
            self.effects.fault,
            ChildFault::ParentStopReadFailure
                | ChildFault::ParentStopReadPermanent
                | ChildFault::ParentStopReadCorruption
                | ChildFault::ParentStopReadHeld
        ) && stop_read_ready
            && if matches!(
                self.effects.fault,
                ChildFault::ParentStopReadPermanent | ChildFault::ParentStopReadHeld
            ) {
                // An authority outage applies to every read of this exact head,
                // including concurrent owners while the first read is held.
                self.effects.read_fault_armed.load(Ordering::SeqCst)
            } else {
                self.effects.read_fault_armed.swap(false, Ordering::SeqCst)
            }
        {
            // The original children already reached durable completion/Input.
            // Hold this exact parent read while their executor remains owned;
            // no caller-poll interval or storage thread is part of the oracle.
            let first = !self.effects.stop_read_started.swap(true, Ordering::SeqCst);
            if first
                || matches!(
                    self.effects.fault,
                    ChildFault::ParentStopReadPermanent | ChildFault::ParentStopReadHeld
                )
            {
                self.effects.await_stop_read_release().await;
            }
            self.effects.faults.fetch_add(1, Ordering::SeqCst);
            if matches!(self.effects.fault, ChildFault::ParentStopReadCorruption) {
                return Err(StateStoreError::serialization(
                    "parent observation cannot be decoded",
                ));
            }
            return Err(StateStoreError::unavailable(
                "load",
                "one parent stop observation failed",
            ));
        }
        if matches!(self.effects.fault, ChildFault::ReadyReadPanic)
            && self.effects.fault_key.lock().unwrap().as_ref() == Some(&key)
            && self.effects.read_fault_armed.swap(false, Ordering::SeqCst)
        {
            // This first selected load panics before constructing a native read.
            // The sibling-gated failure commit and exact effect evidence expose
            // any child re-entry while the original failure is being published.
            self.effects.faults.fetch_add(1, Ordering::SeqCst);
            panic!("ready-only selected child read fault");
        }
        let mut snapshot = self.inner.load(key.clone()).await?;
        if matches!(self.effects.fault, ChildFault::StaleParentRead)
            && snapshot.as_ref().is_some_and(|row| {
                key.key().starts_with("run/")
                    && row.record.value["workflow_id"] == "executor-parent"
            })
            && self.effects.read_fault_armed.swap(false, Ordering::SeqCst)
        {
            // The native read has completed and released its ownership. Hold
            // only this captured pre-cancellation response while another caller
            // commits cancellation, then return the original read unchanged.
            self.effects.faults.fetch_add(1, Ordering::SeqCst);
            self.effects.verification_read_entered.notify_one();
            self.effects.await_stop_read_release().await;
            self.effects
                .verification_read_returned
                .store(true, Ordering::SeqCst);
        }
        self.effects.reorder_admitted_input(&key, &mut snapshot);
        if after_panic_recovery && snapshot.is_some() {
            self.effects
                .panic_recovery_retried
                .store(true, Ordering::SeqCst);
        }
        // Fault the actual head-load owner, inspecting only the immutable row
        // named by that observed head. Historical predecessor reads cannot arm
        // or consume a current-run read fault.
        let current = if let Some(head) = &snapshot {
            Self::current_record(&self.inner, head).await?
        } else {
            None
        };
        self.effects
            .queued_parent_read(&key, &snapshot, &current)
            .await?;
        if matches!(
            self.effects.fault,
            ChildFault::ChildInventoryRead {
                shared_budget: true,
                ..
            }
        ) && self.effects.read_fault_armed.load(Ordering::SeqCst)
            && current
                .as_ref()
                .is_some_and(|row| row.record.value["parent"]["index"] == 0)
            && self
                .effects
                .decision_read_remaining
                .swap(0, Ordering::SeqCst)
                == 1
        {
            // This first valid child authority belongs to the same inventory
            // pass as the later held sibling. Its elapsed read time is retained.
            tokio::time::sleep(Duration::from_secs(20)).await;
        }
        if let ChildFault::ChildInventoryRead { failure, .. } = self.effects.fault
            && selected
            && self.effects.read_fault_armed.swap(false, Ordering::SeqCst)
        {
            *self.effects.faulted_record.lock().unwrap() = current
                .as_ref()
                .map(|row| row.record.value.as_ref().clone());
            self.effects.faults.fetch_add(1, Ordering::SeqCst);
            if matches!(failure, AuthorityReadFault::Held) {
                let (held, receipt) = tokio::sync::oneshot::channel();
                *self.effects.held_execution_read.lock().unwrap() = Some(receipt);
                self.effects.decision_read_observed.notify_one();
                self.effects.verification_read_release.notified().await;
                let _ = held.send(());
            } else {
                self.effects.decision_read_observed.notify_one();
            }
            return Err(if matches!(failure, AuthorityReadFault::Permanent) {
                StateStoreError::serialization("admitted child inventory authority is unreadable")
            } else {
                StateStoreError::unavailable(
                    "load",
                    "admitted child inventory authority is unavailable",
                )
            });
        }
        if let ChildFault::StoppingRead {
            boundary: StoppingReadBoundary::Descendants { shared_budget },
            failure,
        } = self.effects.fault
            && self.effects.stop_read_started.load(Ordering::SeqCst)
            && self.effects.read_fault_armed.load(Ordering::SeqCst)
            && let Some(row) = &current
            && row.record.value["checkpoint"]["phase"]["state"] == "succeeded"
            && let Some(parent) = row.record.value["parent"].as_object()
        {
            let parent_key = StateKey::new(
                "app-kit",
                format!(
                    "run/{}/{}/{}/{}",
                    row.record.value["workspace_id"].as_str().unwrap(),
                    parent["workflow_id"].as_str().unwrap(),
                    parent["run_id"].as_str().unwrap(),
                    parent["occurrence"].as_str().unwrap(),
                ),
            )?;
            let parent_head = self.inner.load(parent_key).await?.unwrap();
            let parent_row = Self::current_record(&self.inner, &parent_head)
                .await?
                .unwrap();
            if parent_row.record.value["checkpoint"]["phase"]["state"] == "cancelling" {
                if shared_budget
                    && parent["index"] == 0
                    && self
                        .effects
                        .verification_read_pending
                        .swap(false, Ordering::SeqCst)
                {
                    // The original child is valid; observing its authority
                    // spends the same scan budget as the later sibling read.
                    tokio::time::sleep(Duration::from_secs(20)).await;
                }
                if selected && self.effects.read_fault_armed.swap(false, Ordering::SeqCst) {
                    *self.effects.faulted_record.lock().unwrap() =
                        Some(row.record.value.as_ref().clone());
                    self.effects.faults.fetch_add(1, Ordering::SeqCst);
                    if matches!(failure, AuthorityReadFault::Held) {
                        let (held, receipt) = tokio::sync::oneshot::channel();
                        *self.effects.held_execution_read.lock().unwrap() = Some(receipt);
                        self.effects.decision_read_observed.notify_one();
                        self.effects.verification_read_release.notified().await;
                        let _ = held.send(());
                    } else {
                        self.effects.decision_read_observed.notify_one();
                    }
                    return Err(if matches!(failure, AuthorityReadFault::Permanent) {
                        StateStoreError::serialization(
                            "stopping descendant authority is unreadable",
                        )
                    } else {
                        StateStoreError::unavailable(
                            "load",
                            "stopping descendant authority is unavailable",
                        )
                    });
                }
            }
        }
        if let ChildFault::StoppingRead {
            boundary: StoppingReadBoundary::Parent { retired },
            failure,
        } = self.effects.fault
            && selected
            && self.effects.stop_read_started.load(Ordering::SeqCst)
            && current.as_ref().is_some_and(|snapshot| {
                snapshot.record.value.pointer("/checkpoint/phase/state")
                    == Some(&json!("cancelling"))
                    && (!retired
                        || snapshot.record.value["checkpoint"]["controls"]
                            .as_object()
                            .is_some_and(serde_json::Map::is_empty))
            })
            && self.effects.read_fault_armed.swap(false, Ordering::SeqCst)
        {
            *self.effects.faulted_record.lock().unwrap() = current
                .as_ref()
                .map(|row| row.record.value.as_ref().clone());
            // Cancellation is already native truth and the original E was
            // dropped. Select only its subsequent pure stopping authority read.
            self.effects.faults.fetch_add(1, Ordering::SeqCst);
            if matches!(failure, AuthorityReadFault::Held) {
                let (held, receipt) = tokio::sync::oneshot::channel();
                *self.effects.held_execution_read.lock().unwrap() = Some(receipt);
                self.effects.decision_read_observed.notify_one();
                self.effects.verification_read_release.notified().await;
                let _ = held.send(());
            } else {
                self.effects.decision_read_observed.notify_one();
            }
            return Err(if matches!(failure, AuthorityReadFault::Permanent) {
                StateStoreError::serialization("stopping parent authority is unreadable")
            } else {
                StateStoreError::unavailable("load", "stopping parent authority is unavailable")
            });
        }
        if matches!(
            self.effects.fault,
            ChildFault::ParentStopReadPermanent | ChildFault::ParentStopReadHeld
        ) && self.effects.read_fault_armed.load(Ordering::SeqCst)
            && let Some(snapshot) = &current
            && snapshot.record.value["workflow_id"] == "executor-child"
            && snapshot.record.value["parent"]["index"] == 0
            && snapshot.record.value["checkpoint"]["sequence"] == 0
        {
            // This pure read finishes before the first child can execute. Let
            // the other child durably park first: the first child's eventual
            // completion then wakes the parent after both observed boundaries,
            // so a previously valid cached read cannot stand in for the outage.
            let parent = snapshot.record.value["parent"]["run_id"].as_str().unwrap();
            while self.effects.read_fault_armed.load(Ordering::SeqCst) {
                let children = ChildDefinitions::admitted_records(&self.inner, parent).await;
                if children.len() == 2 && Self::has_single_control(&children[1], "input") {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
        if let ChildFault::CompletionRead { boundary, failure } = self.effects.fault
            && after_executor_return
            && self
                .effects
                .incomplete_execution_owner
                .lock()
                .unwrap()
                .is_none_or(|owner| owner == std::thread::current().id())
            && self.effects.read_fault_armed.load(Ordering::SeqCst)
            && let Some(row) = &current
        {
            let shared_budget = match boundary {
                CompletionReadBoundary::Child { shared_budget }
                | CompletionReadBoundary::Parent { shared_budget } => shared_budget,
                CompletionReadBoundary::ParentFailure => false,
            };
            let child = row.record.value["workflow_id"] == "executor-child"
                && row.record.value["checkpoint"]["phase"]["state"] == "succeeded";
            if shared_budget
                && child
                && row.record.value["parent"]["index"] == 0
                && self
                    .effects
                    .verification_read_pending
                    .swap(false, Ordering::SeqCst)
            {
                // The first original terminal child consumes the same scan
                // budget as the later child or final parent authority read.
                tokio::time::sleep(Duration::from_secs(20)).await;
                return Ok(snapshot);
            }
            let selected = match boundary {
                CompletionReadBoundary::Child { shared_budget } => {
                    child && row.record.value["parent"]["index"] == usize::from(shared_budget)
                }
                CompletionReadBoundary::Parent { .. } => {
                    row.record.value["workflow_id"] == "executor-parent"
                }
                CompletionReadBoundary::ParentFailure => {
                    let phase = &row.record.value["checkpoint"]["phase"];
                    row.record.value["workflow_id"] == "executor-parent"
                        && matches!(phase["state"].as_str(), Some("failing" | "failed"))
                        && phase["category"] == "child"
                }
            };
            if selected && self.effects.read_fault_armed.swap(false, Ordering::SeqCst) {
                *self.effects.fault_key.lock().unwrap() = Some(key.clone());
                *self.effects.faulted_record.lock().unwrap() =
                    Some(row.record.value.as_ref().clone());
                self.effects.faults.fetch_add(1, Ordering::SeqCst);
                let (held, receipt) = tokio::sync::oneshot::channel();
                *self.effects.held_execution_read.lock().unwrap() = Some(receipt);
                self.effects.verification_read_entered.notify_one();
                self.effects.verification_read_release.notified().await;
                drop(held);
                self.effects
                    .verification_read_returned
                    .store(true, Ordering::SeqCst);
                return Err(if matches!(failure, AuthorityReadFault::Permanent) {
                    StateStoreError::serialization("completed group authority is unreadable")
                } else {
                    StateStoreError::unavailable("load", "completed group authority is unavailable")
                });
            }
        }
        if after_executor_return
            && current.as_ref().is_some_and(|snapshot| {
                snapshot.record.value["workflow_id"] == "executor-child"
                    && snapshot.record.value["parent"]["index"] == json!(0)
                    && snapshot.record.value["checkpoint"]["phase"]["state"] == "succeeded"
            })
            && self
                .effects
                .verification_read_pending
                .swap(false, Ordering::SeqCst)
        {
            // The original executor already returned both real completions.
            // Hold only this selected read response, after the native read ended.
            *self.effects.fault_key.lock().unwrap() = Some(key.clone());
            self.effects.faults.fetch_add(1, Ordering::SeqCst);
            self.effects.verification_read_entered.notify_one();
            self.effects.verification_read_release.notified().await;
            self.effects
                .verification_read_returned
                .store(true, Ordering::SeqCst);
            if matches!(self.effects.fault, ChildFault::HeldVerification) {
                return Ok(snapshot);
            }
            return Err(if matches!(self.effects.fault, ChildFault::PermanentRead) {
                StateStoreError::serialization("selected completed child authority is unreadable")
            } else {
                StateStoreError::unavailable("load", "transient completion verification read")
            });
        }
        if after_verification_error
            && self.effects.fault_key.lock().unwrap().as_ref() == Some(&key)
            && current.is_some()
        {
            self.effects
                .verification_read_retried
                .store(true, Ordering::SeqCst);
        }
        let first_child = current.as_ref().is_some_and(|snapshot| {
            snapshot.record.value["workflow_id"] == "executor-child"
                && snapshot.record.value.pointer("/checkpoint/state/item") == Some(&json!(10))
        });
        if first_child
            && matches!(self.effects.fault, ChildFault::ReadPanic)
            && self.effects.read_fault_armed.swap(false, Ordering::SeqCst)
        {
            self.effects.faults.fetch_add(1, Ordering::SeqCst);
            *self.effects.fault_key.lock().unwrap() = Some(key.clone());
            self.effects
                .panic_recovery_pending
                .store(true, Ordering::SeqCst);
            panic!("private child read fault");
        }
        if matches!(
            self.effects.fault,
            ChildFault::ParentStopReadFailure
                | ChildFault::ParentStopReadPermanent
                | ChildFault::ParentStopReadCorruption
                | ChildFault::ParentStopReadHeld
        ) && current.as_ref().is_some_and(|snapshot| {
            snapshot.record.value["workflow_id"] == "executor-parent"
                && ChildStore::has_single_control(&snapshot.record.value, "child")
        }) {
            *self.effects.fault_key.lock().unwrap() = Some(key);
            // A read admitted after the error proves resumed observation;
            // an earlier child-completion read cannot satisfy this witness.
            if selected && after_stop_failure {
                self.effects.stop_read_recovered.notify_one();
            }
        }
        Ok(snapshot)
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        self.inner.list(request).await
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        Ok(ChildTransaction {
            inner: self.inner.begin_transaction().await?,
            effects: Arc::clone(&self.effects),
            wait_for_second: false,
            recovering_first: false,
            cancellation: None,
            parent_checkpoint: false,
            ordered_admission: None,
            receipt: None,
        })
    }
}

impl StateTransactionBackend for ChildTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.effects.observe_startup_read(&key);
        let mut snapshot = self.inner.load(key.clone()).await?;
        self.effects.reorder_admitted_input(&key, &mut snapshot);
        if matches!(
            self.effects.fault,
            ChildFault::QueuedParentRead {
                shared_budget: true,
                ..
            }
        ) && self.effects.read_fault_armed.load(Ordering::SeqCst)
        {
            // Recovery reads its head and checkpoint in one native snapshot.
            // Inspect the exact row named by this head through that same
            // transaction before charging the first read's existing delay.
            let current = if let Some(head) = &snapshot
                && let Some(scope) = head.record.key.key().strip_prefix("run/")
                && let Some(sequence) = head.record.value.get("sequence").and_then(Value::as_u64)
            {
                let row = self
                    .inner
                    .load(StateKey::new(
                        "app-kit",
                        format!("history/{scope}/checkpoint/{sequence:020}"),
                    )?)
                    .await?;
                if let Some(row) = &row {
                    assert!(ChildStore::is_checkpoint(&row.record));
                    for field in ["id", "workspace_id", "workflow_id"] {
                        assert_eq!(row.record.value[field], head.record.value[field]);
                    }
                    assert_eq!(
                        row.record.value["occurrence"],
                        head.record.value["creation"]
                    );
                    assert_eq!(row.record.value["checkpoint"]["sequence"], json!(sequence));
                }
                row
            } else {
                None
            };
            self.effects
                .queued_parent_read(&key, &snapshot, &current)
                .await?;
        }
        Ok(snapshot)
    }

    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        let checkpoint = ChildStore::is_checkpoint(&record);
        if self.effects.ordered_admission {
            if record.key.key().starts_with("receipt/") {
                self.receipt = Some(record.key.clone());
            }
            if self.effects.faults.load(Ordering::SeqCst) == 0
                && checkpoint
                && record.value["workflow_id"] == "executor-parent"
                && ChildStore::has_single_control(&record.value, "child")
                && record.value["checkpoint"]["controls"]
                    .as_object()
                    .unwrap()
                    .values()
                    .any(|control| {
                        control["state"]["admitted"]
                            .as_array()
                            .is_some_and(|admitted| admitted.len() == 2)
                    })
            {
                self.ordered_admission = Some(record.clone());
            }
        }
        if (matches!(
            self.effects.fault,
            ChildFault::ReadyReadPanic
                | ChildFault::PermanentAfterHook
                | ChildFault::RecoverableAfterHook
                | ChildFault::HeldAfterHook
        ) || self.effects.decision_read_remaining.load(Ordering::SeqCst) > 0)
            && checkpoint
            && record.value["workflow_id"] == "executor-child"
            && record.value["parent"]["index"] == 0
            && record.value["checkpoint"]["sequence"] == 0
            && record.value["checkpoint"]["state"]["item"] == 10
        {
            // Capture the selected head from real admission. The existing E
            // callback arms it only after this complete batch has committed.
            *self.effects.fault_key.lock().unwrap() = Some(StateKey::new(
                record.key.namespace().as_str(),
                format!(
                    "run/{}/{}/{}/{}",
                    record.value["workspace_id"].as_str().unwrap(),
                    record.value["workflow_id"].as_str().unwrap(),
                    record.value["id"].as_str().unwrap(),
                    record.value["occurrence"].as_str().unwrap(),
                ),
            )?);
        }
        self.parent_checkpoint |= self.effects.hold_parent_checkpoint.load(Ordering::SeqCst)
            && checkpoint
            && record.value["workflow_id"] == "executor-parent"
            && record.value.pointer("/checkpoint/phase/state") == Some(&json!("active"))
            && if matches!(
                self.effects.fault,
                ChildFault::StageReceipt | ChildFault::StageReceiptLoss
            ) {
                record.value["checkpoint"]["staged"]
                    .as_array()
                    .is_some_and(|staged| {
                        staged.len() == 1
                            && staged[0]["node"] == "map"
                            && staged[0]["outcome"]["state"] == "complete"
                    })
            } else {
                record.value["checkpoint"]["controls"]
                    .as_object()
                    .is_some_and(|controls| {
                        controls.values().any(|control| {
                            control["state"]["kind"] == "child"
                                && control["state"]["admitted"]
                                    .as_array()
                                    .is_some_and(|admitted| {
                                        if matches!(
                                            self.effects.fault,
                                            ChildFault::ChildInventoryRead { .. }
                                        ) {
                                            admitted.len() == 2
                                        } else {
                                            admitted.is_empty()
                                        }
                                    })
                        })
                    })
            };
        self.wait_for_second |= self.effects.gate_first_commit
            && checkpoint
            && record.value.pointer("/checkpoint/state/item") == Some(&json!(11));
        self.recovering_first |= matches!(self.effects.fault, ChildFault::ReadyReadPanic)
            && checkpoint
            && record.value["workflow_id"] == "executor-child"
            && record.value.pointer("/checkpoint/state/item") == Some(&json!(10))
            && record.value.pointer("/checkpoint/phase/state") == Some(&json!("failed"));
        if checkpoint
            && record.value["workflow_id"] == "executor-child"
            && record.value.pointer("/checkpoint/phase/state") == Some(&json!("cancelled"))
        {
            self.cancellation = Some(ChildCancellation {
                run: record.value["id"].as_str().unwrap().to_owned(),
                occurrence: record.value["occurrence"].as_str().unwrap().to_owned(),
                parent: record.value["parent"]["run_id"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
                ordinal: record.value["parent"]["index"].as_u64().unwrap(),
                item: record.value["checkpoint"]["state"]["item"]
                    .as_i64()
                    .unwrap(),
            });
        }
        self.inner.put(record).await
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        self.inner.delete(key).await
    }

    async fn commit(self) -> Result<(), StateStoreError> {
        if self.wait_for_second || self.recovering_first {
            // notify_one retains its permit if the second effect already ran.
            // The value check also permits repeated writes/cleanup of child 0.
            if !self.effects.values.lock().unwrap().contains(&20) {
                self.effects.second_polled.notified().await;
            }
        }
        let result = self.inner.commit().await;
        if result.is_ok()
            && let Some(record) = self.ordered_admission
        {
            *self.effects.fault_key.lock().unwrap() = Some(record.key);
            *self.effects.faulted_record.lock().unwrap() = Some(record.value.as_ref().clone());
            *self.effects.ordered_receipt.lock().unwrap() =
                Some(self.receipt.expect("same native admission receipt"));
            assert_eq!(self.effects.faults.fetch_add(1, Ordering::SeqCst), 0);
            return Err(StateStoreError::unavailable(
                "commit",
                "applied child admission receipt unavailable",
            ));
        }
        if result.is_ok()
            && self.parent_checkpoint
            && self
                .effects
                .hold_parent_checkpoint
                .swap(false, Ordering::SeqCst)
        {
            self.effects.parent_checkpoint_entered.notify_one();
            self.effects.parent_checkpoint_release.notified().await;
            if matches!(self.effects.fault, ChildFault::StageReceiptLoss) {
                self.effects.faults.fetch_add(1, Ordering::SeqCst);
                return Err(StateStoreError::unavailable(
                    "commit",
                    "native child Stage receipt was lost",
                ));
            }
        }
        if result.is_ok()
            && let Some(cancellation) = self.cancellation
        {
            if self.effects.hold_cancellation_receipt {
                // The native writer has committed and released ownership. Only
                // the original caller transaction's receipt remains pending.
                let mut release = Box::pin(self.effects.cancellation_receipt_release.notified());
                release.as_mut().enable();
                if !self
                    .effects
                    .cancellation_receipt_released
                    .load(Ordering::SeqCst)
                {
                    self.effects.cancellation_receipt_entered.notify_one();
                    release.await;
                }
            }
            self.effects
                .cancellations
                .lock()
                .unwrap()
                .push(cancellation);
        }
        if result.is_ok() && self.recovering_first {
            self.effects.failure_commits.fetch_add(1, Ordering::SeqCst);
        }
        result
    }

    async fn rollback(self) -> Result<(), StateStoreError> {
        self.inner.rollback().await
    }
}

#[tokio::test]
async fn asynchronous_default_polls_other_children_while_the_first_is_pending() {
    let effects = Arc::new(ChildEffects {
        gate_first_commit: true,
        ..ChildEffects::default()
    });
    let inner = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .expect("real transactional state");
    let config: AppConfig<ChildStore, Agent> = AppConfig::new(
        ChildStore {
            inner,
            effects: Arc::clone(&effects),
        },
        Vec::new(),
        ChildDefinitions::all(None, 2),
    );
    let mut app = App::open(config.with_state_typing(ChildTyping {
        effects: Arc::clone(&effects),
    }))
    .await
    .unwrap();
    let workspace = app
        .create_workspace(WorkspaceConfig {
            name: None,
            root: std::env::current_dir().unwrap(),
        })
        .await
        .unwrap();
    let workflow = workspace
        .workflow("executor-parent".parse().unwrap())
        .await
        .unwrap();
    let run = workflow.start(ExecutionOptions::default()).await.unwrap();
    let snapshot = tokio::time::timeout(Duration::from_secs(5), run.wait())
        .await
        .expect("async default must poll the second child before the first finishes")
        .expect("terminal parent");
    assert!(matches!(snapshot.status, RunStatus::Succeeded));
    assert_eq!(
        snapshot.state.values().get("collected"),
        Some(&json!([11, 21]))
    );
    let mut actual = effects.values.lock().unwrap().clone();
    actual.sort();
    assert_eq!(actual, [10, 20], "both real effects happen exactly once");
    app.shutdown(Shutdown::Drain {
        timeout: Duration::from_secs(3),
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn custom_executor_receives_the_complete_empty_map_batch_once() {
    let batches = Arc::new(Mutex::new(Vec::new()));
    let effects = Arc::new(ChildEffects::default());
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .expect("real transactional state");
    let config: AppConfig<StateStore, Agent> =
        AppConfig::new(store, Vec::new(), ChildDefinitions::all(None, 2));
    let mut app = App::open(
        config
            .with_state_typing(ChildTyping {
                effects: Arc::clone(&effects),
            })
            .with_child_executor(EmptyExecutor {
                batches: Arc::clone(&batches),
            }),
    )
    .await
    .unwrap();
    let workspace = app
        .create_workspace(WorkspaceConfig {
            name: None,
            root: std::env::current_dir().unwrap(),
        })
        .await
        .unwrap();
    let workflow = workspace
        .workflow("executor-parent".parse().unwrap())
        .await
        .unwrap();
    let run = workflow
        .start(ExecutionOptions {
            state: WorkflowState::new(Map::from_iter([("items".into(), json!([]))])).unwrap(),
            ..ExecutionOptions::default()
        })
        .await
        .unwrap();
    let snapshot = tokio::time::timeout(Duration::from_secs(3), run.wait())
        .await
        .expect("empty child batch settles")
        .expect("terminal parent");
    assert!(matches!(snapshot.status, RunStatus::Succeeded));
    assert_eq!(snapshot.state.values().get("collected"), Some(&json!([])));
    assert_eq!(
        *batches.lock().unwrap(),
        [(ActionMode::Asynchronous, 0)],
        "rurr invokes the configured executor for an empty complete batch"
    );
    assert!(effects.values.lock().unwrap().is_empty());
    app.shutdown(Shutdown::Drain {
        timeout: Duration::from_secs(3),
    })
    .await
    .unwrap();
}

impl ChildDefinitions {
    async fn assert_child_failure(fault: ChildFault) {
        let expected_category = match fault {
            ChildFault::RejectedMutation => "state",
            ChildFault::ReadPanic | ChildFault::ReadyReadPanic => "execution",
            ChildFault::None
            | ChildFault::PermanentRead
            | ChildFault::StageReceipt
            | ChildFault::StageReceiptLoss
            | ChildFault::HeldVerification
            | ChildFault::CompletionRead { .. }
            | ChildFault::PermanentAfterHook
            | ChildFault::RecoverableAfterHook
            | ChildFault::HeldExecutionRead
            | ChildFault::HeldAfterHook
            | ChildFault::ParentStopReadFailure
            | ChildFault::ParentStopReadPermanent
            | ChildFault::ParentStopReadCorruption
            | ChildFault::ParentStopReadHeld
            | ChildFault::StaleParentRead
            | ChildFault::StoppingRead { .. }
            | ChildFault::ChildInventoryRead { .. }
            | ChildFault::QueuedParentRead { .. } => {
                panic!("a fault case must select its failure")
            }
        };
        let effects = Arc::new(ChildEffects {
            fault,
            ..ChildEffects::default()
        });
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let config: AppConfig<ChildStore, Agent> = AppConfig::new(
            ChildStore {
                inner: store.clone(),
                effects: Arc::clone(&effects),
            },
            Vec::new(),
            Self::all(None, 2),
        );
        let mut app = App::open(
            config
                .with_state_typing(ChildTyping {
                    effects: Arc::clone(&effects),
                })
                .with_child_executor(FaultExecutor {
                    effects: Arc::clone(&effects),
                }),
        )
        .await
        .unwrap();
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().unwrap(),
            })
            .await
            .unwrap();
        let workflow = workspace
            .workflow("executor-parent".parse().unwrap())
            .await
            .unwrap();
        let run = workflow.start(ExecutionOptions::default()).await.unwrap();
        let held_recovery = if matches!(effects.fault, ChildFault::ReadPanic) {
            let observed = std::panic::AssertUnwindSafe(async {
                let entered = tokio::time::timeout(
                    Duration::from_secs(3),
                    effects.panic_recovery_entered.notified(),
                )
                .await;
                let terminal = if entered.is_ok() {
                    Some(tokio::time::timeout(Duration::from_millis(100), run.wait()).await)
                } else {
                    None
                };
                let values = effects.values.lock().unwrap().clone();
                (entered, terminal, values)
            })
            .catch_unwind()
            .await;
            // Always release before observing or propagating a reached failure.
            effects.panic_recovery_release.notify_one();
            Some(observed)
        } else {
            None
        };
        let snapshot =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), run.wait()))
                .catch_unwind()
                .await;
        let recovery_returned = effects.panic_recovery_returned.load(Ordering::SeqCst);
        let recovery_retried = effects.panic_recovery_retried.load(Ordering::SeqCst);
        // Retain evidence before cleanup and assert afterward, so a reached
        // regression still closes the actual accepted work it exercised.
        let mut observed = effects.values.lock().unwrap().clone();
        observed.sort();
        let mut children = Self::current_records(&store)
            .await
            .into_iter()
            .filter(|record| record["workflow_id"] == "executor-child")
            .collect::<Vec<_>>();
        children.sort_by_key(|record| record["parent"]["index"].as_u64().unwrap());
        let closed = std::panic::AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        }))
        .catch_unwind()
        .await;
        let held_recovery = held_recovery.map(|observed| match observed {
            Ok(evidence) => evidence,
            Err(failure) => std::panic::resume_unwind(failure),
        });
        let snapshot = match snapshot {
            Ok(result) => result
                .expect("child failure and sibling cleanup settle")
                .expect("durable parent failure"),
            Err(failure) => std::panic::resume_unwind(failure),
        };
        match closed {
            Ok(result) => result.expect("owned child cleanup"),
            Err(failure) => std::panic::resume_unwind(failure),
        }
        if let Some((entered, terminal, values)) = held_recovery {
            entered.expect("actual child panic is followed by an owned reconciliation read");
            assert!(
                matches!(terminal, Some(Err(_))),
                "parent completion must retain the held panic reconciliation read"
            );
            assert!(
                !values.contains(&10),
                "failed child cannot re-enter State while reconciliation is held"
            );
            assert!(recovery_returned);
            assert!(
                recovery_retried,
                "the transient reconciliation read must be retried before terminal publication"
            );
        }
        assert!(matches!(
            snapshot.status,
            RunStatus::Failed(WorkflowFailure::Child { .. })
        ));
        assert_eq!(snapshot.state.values()["collected"], json!([]));
        assert_eq!(snapshot.last_action_ordinal, None);
        assert!(snapshot.outcomes.is_empty());
        assert_eq!(effects.executions.load(Ordering::SeqCst), 1);
        assert_eq!(effects.faults.load(Ordering::SeqCst), 1);
        if matches!(effects.fault, ChildFault::ReadyReadPanic) {
            assert_eq!(effects.failure_commits.load(Ordering::SeqCst), 1);
        }
        assert_eq!(children.len(), 2);
        assert_eq!(children[0]["checkpoint"]["phase"]["state"], "failed");
        assert_eq!(
            children[0]["checkpoint"]["phase"]["category"],
            expected_category
        );
        assert_eq!(children[0]["checkpoint"]["state"]["item"], 10);
        assert_eq!(children[0]["checkpoint"]["position"], json!(null));
        assert_eq!(children[1]["checkpoint"]["phase"]["state"], "succeeded");
        assert_eq!(children[1]["checkpoint"]["state"]["item"], 21);
        assert_eq!(children[1]["checkpoint"]["position"]["ordinal"], 0);
        let expected = match effects.fault {
            ChildFault::RejectedMutation => vec![10, 20],
            ChildFault::ReadPanic | ChildFault::ReadyReadPanic => vec![20],
            ChildFault::None
            | ChildFault::PermanentRead
            | ChildFault::StageReceipt
            | ChildFault::StageReceiptLoss
            | ChildFault::HeldVerification
            | ChildFault::CompletionRead { .. }
            | ChildFault::PermanentAfterHook
            | ChildFault::RecoverableAfterHook
            | ChildFault::HeldExecutionRead
            | ChildFault::HeldAfterHook
            | ChildFault::ParentStopReadFailure
            | ChildFault::ParentStopReadPermanent
            | ChildFault::ParentStopReadCorruption
            | ChildFault::ParentStopReadHeld
            | ChildFault::StaleParentRead
            | ChildFault::StoppingRead { .. }
            | ChildFault::ChildInventoryRead { .. }
            | ChildFault::QueuedParentRead { .. } => {
                unreachable!("selected fault remains unchanged")
            }
        };
        assert_eq!(
            observed, expected,
            "async default continues later child effects"
        );
    }
}

#[tokio::test]
async fn asynchronous_default_awaits_later_children_after_a_child_state_failure() {
    ChildDefinitions::assert_child_failure(ChildFault::RejectedMutation).await;
}

#[tokio::test]
async fn child_read_panic_keeps_exact_child_failure_ownership_and_polls_later_siblings() {
    ChildDefinitions::assert_child_failure(ChildFault::ReadPanic).await;
}

#[tokio::test]
async fn ready_only_child_read_panics_wait_for_one_reconciliation_and_allow_sibling_progress() {
    ChildDefinitions::assert_child_failure(ChildFault::ReadyReadPanic).await;
}
#[derive(Clone, Copy)]
enum IntegrityPolicy {
    Missing,
    MissingRead {
        boundary: CompletionReadBoundary,
        failure: AuthorityReadFault,
    },
    Foreign,
    RejectBeforePoll,
    PanicConstruction,
    PanicPolling,
    PanicDrop,
    StopReadFailure,
    CancelParked,
    CancelCompleted,
    HoldForeground,
    HoldSynchronous,
    HoldAfterFirst,
}

impl IntegrityPolicy {
    fn fails_before_child_poll(self) -> bool {
        matches!(
            self,
            Self::RejectBeforePoll | Self::PanicConstruction | Self::PanicPolling
        )
    }
}

#[derive(Default)]
struct IntegrityEvidence {
    construction_gate: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    source_eof: bool,
    source_eof_observed: AtomicUsize,
    source_contexts: Mutex<Vec<ActionContext>>,
    source_threads: Mutex<Vec<ThreadId>>,
    source_pending: AtomicBool,
    source_released: AtomicBool,
    source_completed: AtomicUsize,
    source_dropped: AtomicUsize,
    source_wakers: Mutex<Vec<Waker>>,
    calls: AtomicUsize,
    dropped: AtomicUsize,
    executor_dropped: AtomicUsize,
    panics: AtomicUsize,
    retained: Mutex<Option<ChildCompletion>>,
    entered: Notify,
    holding: AtomicBool,
    released: AtomicBool,
    stop_requested: AtomicBool,
    waker: futures_util::task::AtomicWaker,
    threads: Mutex<Vec<ThreadId>>,
}

impl IntegrityEvidence {
    fn release_sources(&self) {
        self.source_released.store(true, Ordering::SeqCst);
        let wakers = std::mem::take(&mut *self.source_wakers.lock().unwrap());
        for waker in wakers {
            waker.wake();
        }
    }

    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.waker.wake();
    }

    fn request_stop(&self) {
        self.stop_requested.store(true, Ordering::SeqCst);
        self.waker.wake();
    }
}

struct IntegrityExecutor {
    declaration: ActionDeclaration,
    policy: IntegrityPolicy,
    evidence: Arc<IntegrityEvidence>,
    effects: Arc<ChildEffects>,
}

struct IntegrityExecution<'a, F> {
    executor: &'a IntegrityExecutor,
    work: IntegrityWork<F>,
    thread: Rc<ThreadId>,
}

enum IntegrityWork<F> {
    Batch(Pin<Box<ChildBatch<F>>>),
    First {
        tasks: Vec<Pin<Box<F>>>,
        completed: Option<ChildCompletion>,
    },
}

impl IntegrityExecutor {
    fn new(
        policy: IntegrityPolicy,
        evidence: &Arc<IntegrityEvidence>,
        effects: &Arc<ChildEffects>,
    ) -> Self {
        Self {
            policy,
            evidence: Arc::clone(evidence),
            effects: Arc::clone(effects),
            declaration: ActionDeclaration {
                key: "mixed-source".into(),
                revision: "v1".into(),
                reads: Vec::new(),
                writes: vec!["source".into()],
                required_inputs: Vec::new(),
                optional_inputs: Vec::new(),
                bound_inputs: Map::new(),
                default_inputs: Map::new(),
                tags: Vec::new(),
                source: None,
                mode: ActionMode::Asynchronous,
            },
        }
    }
}

impl ActionBackend for IntegrityExecutor {
    type Stream = IntegritySource;

    fn declaration(&self) -> &ActionDeclaration {
        &self.declaration
    }

    fn start(&self, request: ActionRequest) -> Self::Stream {
        self.evidence
            .source_contexts
            .lock()
            .unwrap()
            .push(request.context);
        let caller = std::thread::current().id();
        self.evidence.source_threads.lock().unwrap().push(caller);
        IntegritySource {
            evidence: Arc::clone(&self.evidence),
            thread: Rc::new(caller),
            state: IntegritySourceState::Partial,
        }
    }
}

struct IntegritySource {
    evidence: Arc<IntegrityEvidence>,
    thread: Rc<ThreadId>,
    state: IntegritySourceState,
}

enum IntegritySourceState {
    Partial,
    Pending,
    Completed,
}

impl Stream for IntegritySource {
    type Item = Result<ActionEvent, AppError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        assert_eq!(*self.thread, std::thread::current().id());
        self.evidence
            .source_threads
            .lock()
            .unwrap()
            .push(std::thread::current().id());
        match self.state {
            IntegritySourceState::Partial => {
                self.state = IntegritySourceState::Pending;
                Poll::Ready(Some(Ok(ActionEvent::Partial {
                    result: Map::from_iter([("progress".into(), json!("primed"))]),
                })))
            }
            IntegritySourceState::Pending => {
                {
                    let mut wakers = self.evidence.source_wakers.lock().unwrap();
                    if !wakers.iter().any(|waker| waker.will_wake(cx.waker())) {
                        wakers.push(cx.waker().clone());
                    }
                }
                if !self.evidence.source_released.load(Ordering::SeqCst) {
                    self.evidence.source_pending.store(true, Ordering::SeqCst);
                    return Poll::Pending;
                }
                self.state = IntegritySourceState::Completed;
                if self.evidence.source_eof {
                    self.evidence
                        .source_eof_observed
                        .fetch_add(1, Ordering::SeqCst);
                    return Poll::Ready(None);
                }
                self.evidence
                    .source_completed
                    .fetch_add(1, Ordering::SeqCst);
                Poll::Ready(Some(Ok(ActionEvent::Completed {
                    result: Map::from_iter([("done".into(), json!(true))]),
                    writes: vec![StateChange::Assign {
                        values: Map::from_iter([("source".into(), json!(true))]),
                    }],
                })))
            }
            IntegritySourceState::Completed => panic!("completed caller source was polled again"),
        }
    }
}

impl Drop for IntegritySource {
    fn drop(&mut self) {
        self.evidence
            .source_threads
            .lock()
            .unwrap()
            .push(std::thread::current().id());
        self.evidence.source_dropped.fetch_add(1, Ordering::SeqCst);
    }
}

impl ChildExecutor for IntegrityExecutor {
    type Execution<'a, F>
        = IntegrityExecution<'a, F>
    where
        Self: 'a,
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a;

    fn execute<'a, F>(&'a self, batch: ChildBatch<F>) -> Self::Execution<'a, F>
    where
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a,
    {
        assert_eq!(batch.len(), 2);
        assert_eq!(
            batch.mode(),
            if matches!(self.policy, IntegrityPolicy::HoldSynchronous) {
                ActionMode::Synchronous
            } else {
                ActionMode::Asynchronous
            }
        );
        let prior_calls = self.evidence.calls.fetch_add(1, Ordering::SeqCst);
        let construction_gate = self.evidence.construction_gate.lock().unwrap().take();
        if let Some(gate) = construction_gate {
            // This unrelated public E constructor temporarily occupies the
            // local worker. The caller always releases it, including on failure.
            self.evidence.entered.notify_one();
            gate.recv_timeout(Duration::from_secs(8))
                .expect("bounded caller executor gate");
        }
        if matches!(self.policy, IntegrityPolicy::StopReadFailure) && prior_calls == 0 {
            self.effects.read_fault_armed.store(true, Ordering::SeqCst);
        }
        if matches!(self.policy, IntegrityPolicy::PanicConstruction) {
            self.evidence.panics.fetch_add(1, Ordering::SeqCst);
            panic!("private caller executor construction diagnostic");
        }
        let thread = std::thread::current().id();
        if matches!(
            self.policy,
            IntegrityPolicy::HoldForeground | IntegrityPolicy::HoldSynchronous
        ) {
            self.evidence.threads.lock().unwrap().push(thread);
        }
        IntegrityExecution {
            executor: self,
            work: if matches!(self.policy, IntegrityPolicy::HoldAfterFirst) {
                IntegrityWork::First {
                    tasks: batch.into_tasks().expect("original unpolled child pins"),
                    completed: None,
                }
            } else {
                IntegrityWork::Batch(Box::pin(batch))
            },
            thread: Rc::new(thread),
        }
    }
}

impl Drop for IntegrityExecutor {
    fn drop(&mut self) {
        self.evidence
            .executor_dropped
            .fetch_add(1, Ordering::SeqCst);
    }
}

impl<F: Future<Output = Result<ChildCompletion, AppError>>> Future for IntegrityExecution<'_, F> {
    type Output = Result<Vec<ChildCompletion>, AppError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let execution = self.get_mut();
        assert_eq!(*execution.thread, std::thread::current().id());
        if matches!(
            execution.executor.policy,
            IntegrityPolicy::HoldForeground
                | IntegrityPolicy::HoldSynchronous
                | IntegrityPolicy::HoldAfterFirst
        ) {
            let evidence = &execution.executor.evidence;
            evidence
                .threads
                .lock()
                .unwrap()
                .push(std::thread::current().id());
            evidence.waker.register(cx.waker());
            if !evidence.released.load(Ordering::SeqCst) {
                if !evidence.holding.swap(true, Ordering::SeqCst) {
                    evidence.entered.notify_one();
                }
                return Poll::Pending;
            }
        }
        if matches!(execution.executor.policy, IntegrityPolicy::CancelCompleted)
            && execution.executor.evidence.holding.load(Ordering::SeqCst)
        {
            return Poll::Pending;
        }
        if matches!(execution.executor.policy, IntegrityPolicy::PanicPolling) {
            execution
                .executor
                .evidence
                .panics
                .fetch_add(1, Ordering::SeqCst);
            panic!("private caller executor poll diagnostic");
        }
        if matches!(execution.executor.policy, IntegrityPolicy::RejectBeforePoll) {
            return Poll::Ready(Err(AppError::InvalidInput(
                "caller executor rejected its admitted batch".into(),
            )));
        }
        let mut completed = match &mut execution.work {
            IntegrityWork::Batch(batch) => match batch.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(completed)) => completed,
            },
            IntegrityWork::First { tasks, completed } => {
                if execution
                    .executor
                    .evidence
                    .stop_requested
                    .load(Ordering::SeqCst)
                {
                    return Poll::Ready(Err(AppError::Integration(
                        "test-owned executor stopped after a failed cancellation observation"
                            .into(),
                    )));
                }
                if completed.is_none() {
                    match tasks[0].as_mut().poll(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                        Poll::Ready(Ok(first)) => {
                            assert_eq!(first.ordinal(), 0);
                            *completed = Some(first);
                            // Child 0's original final checks and completion
                            // have settled; child 1 stays owned and unpolled.
                            execution
                                .executor
                                .effects
                                .read_fault_armed
                                .store(true, Ordering::SeqCst);
                        }
                    }
                }
                return Poll::Pending;
            }
        };
        if matches!(execution.executor.policy, IntegrityPolicy::CancelCompleted) {
            // Both original child futures have returned their exact terminal
            // receipts. Keep E pending until independent parent cancellation.
            execution
                .executor
                .evidence
                .holding
                .store(true, Ordering::SeqCst);
            execution.executor.evidence.entered.notify_one();
            return Poll::Pending;
        }
        match execution.executor.policy {
            IntegrityPolicy::Missing | IntegrityPolicy::MissingRead { .. } => {
                completed
                    .pop()
                    .expect("one exact completion deliberately omitted");
            }
            IntegrityPolicy::Foreign => {
                // Safe caller code can retain an opaque completion, but cannot
                // fabricate or clone one. Preserve the ordinal and vector size
                // when substituting a different parent's real completion.
                let mut retained = execution.executor.evidence.retained.lock().unwrap();
                let current = completed.pop().expect("original second completion");
                if let Some(prior) = retained.take() {
                    assert_eq!(prior.ordinal(), current.ordinal());
                    completed.push(prior);
                } else {
                    *retained = Some(current);
                }
            }
            IntegrityPolicy::HoldForeground
            | IntegrityPolicy::HoldSynchronous
            | IntegrityPolicy::HoldAfterFirst
            | IntegrityPolicy::CancelParked
            | IntegrityPolicy::CancelCompleted
            | IntegrityPolicy::StopReadFailure
            | IntegrityPolicy::PanicDrop
            | IntegrityPolicy::RejectBeforePoll
            | IntegrityPolicy::PanicConstruction
            | IntegrityPolicy::PanicPolling => {}
        }
        if matches!(
            execution.executor.policy,
            IntegrityPolicy::MissingRead { .. }
        ) {
            // The retained foreground execution gives this original E one
            // worker lane. Its calling thread identifies that lane without
            // assuming the custom local worker is a spawned Tokio task.
            *execution
                .executor
                .effects
                .incomplete_execution_owner
                .lock()
                .unwrap() = Some(std::thread::current().id());
        }
        if matches!(
            execution.executor.effects.fault,
            ChildFault::HeldVerification
        ) || matches!(
            execution.executor.policy,
            IntegrityPolicy::MissingRead { .. }
        ) {
            execution
                .executor
                .effects
                .execution_returns
                .fetch_add(1, Ordering::SeqCst);
        }
        Poll::Ready(Ok(completed))
    }
}

impl<F> Drop for IntegrityExecution<'_, F> {
    fn drop(&mut self) {
        if matches!(
            self.executor.policy,
            IntegrityPolicy::HoldForeground | IntegrityPolicy::HoldSynchronous
        ) {
            self.executor
                .evidence
                .threads
                .lock()
                .unwrap()
                .push(std::thread::current().id());
        }
        self.executor
            .evidence
            .dropped
            .fetch_add(1, Ordering::SeqCst);
        if matches!(self.executor.effects.fault, ChildFault::StoppingRead { .. }) {
            self.executor
                .effects
                .stop_read_started
                .store(true, Ordering::SeqCst);
        }
        if matches!(self.executor.policy, IntegrityPolicy::PanicDrop) {
            self.executor.evidence.panics.fetch_add(1, Ordering::SeqCst);
            panic!("private caller executor destructor diagnostic");
        }
    }
}

impl ChildDefinitions {
    fn integrity_definitions(policy: IntegrityPolicy) -> Vec<WorkflowDefinition> {
        let mut definitions = Self::all(None, 2);
        if matches!(
            policy,
            IntegrityPolicy::CancelParked | IntegrityPolicy::StopReadFailure
        ) {
            definitions[0] = WorkflowDefinition::parse(
                &json!({
                    "schema":"app-kit.workflow/v2", "id":"executor-child", "revision":"v1",
                    "state":{"item":0}, "required":["item"],
                    "reducers":{"item":"reject_concurrent"}, "entry":"add",
                    "nodes":[
                        {"id":"add", "step":{"kind":"state", "mutations":[
                            {"kind":"increment", "target":"item", "value":{"kind":"literal", "value":1}}
                        ]}},
                        {"id":"wait", "deadline_ms":30000, "step":{"kind":"input",
                            "message":[{"kind":"text", "text":"Keep the second child parked"}]}}
                    ],
                    "routes":{"add":{"mode":"first", "branches":[
                        {"when":{"kind":"equal", "left":{"kind":"state", "path":"item"},
                            "right":{"kind":"literal", "value":21}}, "target":{"kind":"node", "id":"wait"}},
                        {"target":{"kind":"end"}}
                    ]}}
                })
                .to_string(),
            )
            .expect("real child input control");
        }
        definitions
    }

    async fn current_records(store: &StateStore) -> Vec<serde_json::Value> {
        let mut records = Vec::new();
        let mut cursor = None;
        loop {
            let mut request = StateListRequest::new("app-kit")
                .unwrap()
                .with_key_prefix(StateKeyPrefix::new("run/").unwrap());
            if let Some(after) = cursor {
                request = request.start_after(after).unwrap();
            }
            let page = store.list(request).await.expect("real durable run heads");
            for head in page.records {
                let current = ChildStore::current_record(store, &head)
                    .await
                    .expect("read exact immutable checkpoint")
                    .expect("each observed run head names its current checkpoint");
                records.push(Arc::unwrap_or_clone(current.record.value));
            }
            cursor = page.next_start_after;
            if cursor.is_none() {
                return records;
            }
        }
    }

    async fn admitted_records(store: &StateStore, parent: &str) -> Vec<serde_json::Value> {
        let mut children = Self::current_records(store)
            .await
            .into_iter()
            .filter(|record| record["parent"]["run_id"] == parent)
            .collect::<Vec<_>>();
        children.sort_by_key(|record| record["parent"]["index"].as_u64().unwrap());
        children
    }

    async fn assert_executor_integrity(policy: IntegrityPolicy) {
        let fatal_read = matches!(
            policy,
            IntegrityPolicy::MissingRead {
                failure: AuthorityReadFault::Permanent | AuthorityReadFault::Held,
                ..
            }
        );
        let effects = Arc::new(ChildEffects {
            fault: match policy {
                IntegrityPolicy::StopReadFailure => ChildFault::ParentStopReadFailure,
                IntegrityPolicy::MissingRead { boundary, failure } => {
                    ChildFault::CompletionRead { boundary, failure }
                }
                _ => ChildFault::None,
            },
            read_fault_armed: AtomicBool::new(matches!(
                policy,
                IntegrityPolicy::MissingRead { .. }
            )),
            ..ChildEffects::default()
        });
        let evidence = Arc::new(IntegrityEvidence::default());
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("real transactional state");
        let config: AppConfig<ChildStore, Agent> = AppConfig::new(
            ChildStore {
                inner: store.clone(),
                effects: Arc::clone(&effects),
            },
            Vec::new(),
            Self::integrity_definitions(policy),
        );
        let mut app = App::open(
            config
                .with_state_typing(ChildTyping {
                    effects: Arc::clone(&effects),
                })
                .with_child_executor(IntegrityExecutor::new(policy, &evidence, &effects)),
        )
        .await
        .unwrap();
        let result = std::panic::AssertUnwindSafe(async {
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().unwrap(),
                })
                .await
                .unwrap();
            let workflow = workspace
                .workflow("executor-parent".parse().unwrap())
                .await
                .unwrap();
            let runs = if matches!(policy, IntegrityPolicy::Foreign) {
                2
            } else {
                1
            };
            for _ in 0..runs {
                let mut run = if matches!(policy, IntegrityPolicy::MissingRead { .. }) {
                    workflow.prepare(WorkflowState::default()).await.expect("prepared parent admission")
                } else {
                    workflow.start(ExecutionOptions::default()).await.expect("parent admission")
                };
                let parent = run.id().to_string();
                if let IntegrityPolicy::MissingRead { boundary, failure } = policy {
                    let occurrence = run.snapshot().await.unwrap().occurrence;
                    let observer = workflow.open_run(RunSelector::Exact {
                        id: run.id().clone(), occurrence,
                    }).await.unwrap();
                    // The retained foreground owner queues the original local E.
                    // Its worker must settle the incomplete result before returning.
                    let execution = run.execution(ExecutionOptions::default()).await.unwrap();
                    let mut finish = Box::pin(execution.finish());
                    tokio::time::timeout(Duration::from_secs(3), async {
                        tokio::select! {
                            _ = effects.verification_read_entered.notified() => {},
                            _ = &mut finish => panic!("original incomplete-result owner returned before its selected authority read"),
                        }
                    }).await.expect("the incomplete original E result reaches its parent authority owner");
                    assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
                    assert_eq!(evidence.dropped.load(Ordering::SeqCst), 1);
                    assert_eq!(effects.execution_returns.load(Ordering::SeqCst), 1);
                    assert_eq!(effects.faults.load(Ordering::SeqCst), 1);
                    let original = effects.faulted_record.lock().unwrap().clone().unwrap();
                    assert_eq!(original["id"], parent);
                    assert_eq!(original["checkpoint"]["state"]["collected"], json!([]));
                    assert!(original["checkpoint"]["position"].is_null());
                    assert_eq!(original["checkpoint"]["outcomes"], json!([]));
                    if matches!(boundary, CompletionReadBoundary::ParentFailure) {
                        assert!(matches!(original["checkpoint"]["phase"]["state"].as_str(), Some("failing" | "failed")));
                        assert_eq!(original["checkpoint"]["phase"]["category"], "child");
                    }
                    let children = Self::admitted_records(&store, &parent).await;
                    assert_eq!(children.len(), 2);
                    for (index, child) in children.iter().enumerate() {
                        assert_eq!(child["checkpoint"]["phase"]["state"], "succeeded");
                        assert_eq!(child["checkpoint"]["state"]["item"], json!(11 + index * 10));
                        assert_eq!(child["checkpoint"]["position"]["ordinal"], 0);
                    }
                    if !matches!(failure, AuthorityReadFault::Held) {
                        effects.verification_read_release.notify_one();
                    }
                    if fatal_read {
                        tokio::time::timeout(Duration::from_secs(if matches!(failure, AuthorityReadFault::Held) { 33 } else { 3 }), async {
                            loop {
                                match app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await {
                                    Err(AppError::Closed(_)) => break,
                                    Ok(_) => tokio::task::yield_now().await,
                                    Err(error) => panic!("unexpected incomplete-result closure observation: {error}"),
                                }
                            }
                        }).await.expect("unreadable incomplete-result authority closes within one finite read pass");
                        let dropped = effects.held_execution_read.lock().unwrap().take().unwrap();
                        assert!(tokio::time::timeout(Duration::from_secs(1), dropped).await
                            .expect("the original authority read is destroyed before fatal closure").is_err());
                        assert!(matches!(observer.snapshot().await, Err(AppError::Closed(_))));
                        let current = Self::current_records(&store).await;
                        let current = current.iter().find(|record| record["id"] == parent).unwrap();
                        if matches!(boundary, CompletionReadBoundary::ParentFailure) {
                            // The exact Child failure was already accepted before
                            // this observation. Cleanup may retire its controls,
                            // but must preserve that primary and immutable row.
                            assert!(matches!(current["checkpoint"]["phase"]["state"].as_str(), Some("failing" | "failed")));
                            assert_eq!(current["checkpoint"]["phase"]["category"], "child");
                            for field in ["state", "position", "outcomes"] {
                                assert_eq!(current["checkpoint"][field], original["checkpoint"][field]);
                            }
                            let head_key = effects.fault_key.lock().unwrap().clone().unwrap();
                            let scope = head_key.key().strip_prefix("run/").unwrap();
                            let failure_key = StateKey::new("app-kit", format!(
                                "history/{scope}/checkpoint/{:020}",
                                original["checkpoint"]["sequence"].as_u64().unwrap(),
                            )).unwrap();
                            assert_eq!(store.load(failure_key).await.unwrap().unwrap().record.value.as_ref(), &original);
                        } else {
                            assert_eq!(current, &original, "fatal authority does not invent a failure checkpoint");
                        }
                        assert_eq!(Self::admitted_records(&store, &parent).await, children);
                        continue;
                    }
                    let complete = tokio::time::timeout(Duration::from_secs(5), finish).await
                        .expect("the original incomplete-result receipt finishes after recovery")
                        .expect("the original child failure remains a workflow result");
                    assert!(matches!(complete.status, RunStatus::Failed(WorkflowFailure::Child { .. })));
                }
                if matches!(
                    policy,
                    IntegrityPolicy::CancelParked | IntegrityPolicy::StopReadFailure
                ) {
                    tokio::time::timeout(Duration::from_secs(3), async {
                        loop {
                            let children = Self::admitted_records(&store, &parent).await;
                            if children.len() == 2
                                && children[0]["checkpoint"]["phase"]["state"] == "succeeded"
                                && ChildStore::has_single_control(&children[1], "input")
                                && (!matches!(policy, IntegrityPolicy::StopReadFailure)
                                    || effects.stop_read_started.load(Ordering::SeqCst))
                            {
                                assert_eq!(children[0]["checkpoint"]["state"]["item"], 11);
                                assert_eq!(children[1]["checkpoint"]["state"]["item"], 21);
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(2)).await;
                        }
                    })
                    .await
                    .expect("one actual child completes while the other owns input");
                    if matches!(policy, IntegrityPolicy::StopReadFailure) {
                        assert!(effects.stop_read_started.load(Ordering::SeqCst));
                        assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
                        assert_eq!(evidence.dropped.load(Ordering::SeqCst), 0);
                        effects.release_stop_read();
                        tokio::time::timeout(
                            Duration::from_secs(3),
                            effects.stop_read_recovered.notified(),
                        )
                        .await
                        .expect("parent observation resumes after its one transient read failure");
                        assert_eq!(effects.faults.load(Ordering::SeqCst), 1);
                        assert_eq!(
                            evidence.dropped.load(Ordering::SeqCst),
                            0,
                            "a stop-observer read failure must retain the accepted executor future"
                        );
                        assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
                    }
                    let before = run.snapshot().await.unwrap();
                    run.cancel(before.checkpoint)
                        .await
                        .expect("fenced parent cancellation");
                }
                let observed = tokio::time::timeout(Duration::from_secs(5), run.wait()).await;
                let snapshot = match observed {
                    Ok(result) => result.unwrap(),
                    Err(_) => {
                        let records = Self::current_records(&store).await;
                        panic!("executor outcome and descendant cleanup timed out; durable records: {records:?}");
                    }
                };
                if matches!(
                    policy,
                    IntegrityPolicy::CancelParked | IntegrityPolicy::StopReadFailure
                ) {
                    assert!(matches!(snapshot.status, RunStatus::Cancelled));
                } else if matches!(
                    policy,
                    IntegrityPolicy::PanicConstruction
                        | IntegrityPolicy::PanicPolling
                        | IntegrityPolicy::PanicDrop
                ) {
                    let RunStatus::Failed(WorkflowFailure::Execution(message)) = &snapshot.status
                    else {
                        panic!("captured caller executor panic must preserve Execution failure");
                    };
                    assert!(!message.contains("private caller executor"));
                } else {
                    if !matches!(&snapshot.status, RunStatus::Failed(WorkflowFailure::Child { .. })) {
                        let records = Self::current_records(&store).await;
                        panic!("executor integrity failure must remain Child; durable records: {records:?}");
                    }
                }
                assert_eq!(snapshot.state.values()["collected"], json!([]));
                assert_eq!(snapshot.last_action_ordinal, None);
                assert!(snapshot.outcomes.is_empty());
                let children = Self::admitted_records(&store, &parent).await;
                assert_eq!(children.len(), 2);
                for (ordinal, child) in children.iter().enumerate() {
                    let cancelled = policy.fails_before_child_poll()
                        || (matches!(
                            policy,
                            IntegrityPolicy::CancelParked | IntegrityPolicy::StopReadFailure
                        ) && ordinal == 1);
                    assert_eq!(
                        child["checkpoint"]["phase"]["state"],
                        if cancelled { "cancelled" } else { "succeeded" }
                    );
                    let expected = if policy.fails_before_child_poll() {
                        10 + ordinal * 10
                    } else {
                        11 + ordinal * 10
                    };
                    assert_eq!(child["checkpoint"]["state"]["item"], json!(expected));
                }
            }
            assert_eq!(evidence.calls.load(Ordering::SeqCst), runs);
            let constructed = if matches!(policy, IntegrityPolicy::PanicConstruction) {
                0
            } else {
                runs
            };
            assert_eq!(evidence.dropped.load(Ordering::SeqCst), constructed);
            let panicked = usize::from(matches!(
                policy,
                IntegrityPolicy::PanicConstruction
                    | IntegrityPolicy::PanicPolling
                    | IntegrityPolicy::PanicDrop
            ));
            assert_eq!(evidence.panics.load(Ordering::SeqCst), panicked);
            assert!(evidence.retained.lock().unwrap().is_none());
            let mut observed = effects.values.lock().unwrap().clone();
            observed.sort();
            let expected = match policy {
                IntegrityPolicy::RejectBeforePoll
                | IntegrityPolicy::PanicConstruction
                | IntegrityPolicy::PanicPolling => vec![],
                IntegrityPolicy::Foreign => vec![10, 10, 20, 20],
                IntegrityPolicy::HoldAfterFirst => vec![10],
                IntegrityPolicy::Missing
                | IntegrityPolicy::MissingRead { .. }
                | IntegrityPolicy::HoldForeground
                | IntegrityPolicy::HoldSynchronous
                    | IntegrityPolicy::CancelParked
                | IntegrityPolicy::CancelCompleted
                | IntegrityPolicy::PanicDrop
                | IntegrityPolicy::StopReadFailure => vec![10, 20],
            };
            assert_eq!(
                observed, expected,
                "no child effect is replayed or fabricated"
            );
        })
        .catch_unwind()
        .await;
        effects.release_stop_read();
        effects.read_fault_armed.store(false, Ordering::SeqCst);
        effects.verification_read_release.notify_one();
        let closed = app
            .shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            })
            .await;
        drop(app);
        if let Err(failure) = result {
            std::panic::resume_unwind(failure);
        }
        if fatal_read {
            assert!(matches!(closed, Err(AppError::Integration(_))));
        } else {
            closed.expect("all accepted child work closes after executor ownership ends");
        }
        tokio::time::timeout(Duration::from_secs(3), async {
            while evidence.executor_dropped.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the sole transferred executor owner is released");
        assert_eq!(evidence.executor_dropped.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn executor_missing_completion_fails_without_reducing_completed_children() {
    ChildDefinitions::assert_executor_integrity(IntegrityPolicy::Missing).await;
}

#[tokio::test]
async fn executor_foreign_parent_completion_is_rejected_even_with_exact_count_and_ordinal() {
    ChildDefinitions::assert_executor_integrity(IntegrityPolicy::Foreign).await;
}

#[tokio::test]
async fn executor_rejection_before_poll_cancels_admitted_children_without_effects() {
    ChildDefinitions::assert_executor_integrity(IntegrityPolicy::RejectBeforePoll).await;
}

#[tokio::test]
async fn executor_construction_panic_preserves_parent_failure_and_revokes_unpolled_children() {
    ChildDefinitions::assert_executor_integrity(IntegrityPolicy::PanicConstruction).await;
}

#[tokio::test]
async fn executor_poll_panic_drops_its_future_and_revokes_unpolled_children() {
    ChildDefinitions::assert_executor_integrity(IntegrityPolicy::PanicPolling).await;
}

#[tokio::test]
async fn executor_destructor_panic_preserves_completed_children_and_parent_rollback() {
    ChildDefinitions::assert_executor_integrity(IntegrityPolicy::PanicDrop).await;
}

#[tokio::test]
async fn child_stop_observer_read_failure_retains_executor_and_parked_child_ownership() {
    ChildDefinitions::assert_executor_integrity(IntegrityPolicy::StopReadFailure).await;
}

#[tokio::test]
async fn parent_cancel_drops_executor_and_settles_its_parked_child_without_replay() {
    ChildDefinitions::assert_executor_integrity(IntegrityPolicy::CancelParked).await;
}

impl ChildDefinitions {
    fn exhaustion_definitions(short_deadline: bool) -> Vec<WorkflowDefinition> {
        let mut definitions = Self::integrity_definitions(IntegrityPolicy::StopReadFailure);
        if short_deadline {
            definitions[1] = WorkflowDefinition::parse(&json!({
                "schema":"app-kit.workflow/v2", "id":"executor-parent", "revision":"v1",
                "state":{"items":[10,20],"collected":[]}, "required":["items","collected"],
                "reducers":{"items":"reject_concurrent","collected":"collect"}, "entry":"map",
                "nodes":[{"id":"map", "deadline_ms":1000, "step":{"kind":"map",
                    "workflow":"executor-child", "items":{"kind":"state","path":"items"},
                    "input_key":"item", "max_parallel":2,
                    "reductions":[{"source":"item","target":"collected","reducer":"collect"}]}}],
                "routes":{"map":{"mode":"first","branches":[{"target":{"kind":"end"}}]}}
            }).to_string()).unwrap();
        }
        definitions.push(
            WorkflowDefinition::parse(
                r#"{
            "schema":"app-kit.workflow/v2","id":"retained-unrelated","revision":"v1",
            "state":{"count":0},"required":["count"],"reducers":{"count":"reject_concurrent"},"entry":"add",
            "nodes":[{"id":"add","step":{"kind":"state","mutations":[
                {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
            ]}}],"routes":{"add":{"mode":"first","branches":[{"target":{"kind":"end"}}]}}
        }"#,
            )
            .unwrap(),
        );
        definitions
    }

    async fn assert_stop_observation_exhaustion(
        short_deadline: bool,
        fault: ChildFault,
        hold_cancellation_receipt: bool,
    ) {
        let effects = Arc::new(ChildEffects {
            fault,
            ..ChildEffects::default()
        });
        let evidence = Arc::new(IntegrityEvidence::default());
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let config: AppConfig<ChildStore, Agent> = AppConfig::new(
            ChildStore {
                inner: store.clone(),
                effects: Arc::clone(&effects),
            },
            Vec::new(),
            Self::exhaustion_definitions(short_deadline),
        );
        let mut app = App::open(
            config
                .with_state_typing(ChildTyping {
                    effects: Arc::clone(&effects),
                })
                .with_child_executor(IntegrityExecutor::new(
                    IntegrityPolicy::StopReadFailure,
                    &evidence,
                    &effects,
                )),
        )
        .await
        .unwrap();
        let observed = std::panic::AssertUnwindSafe(async {
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().unwrap(),
                })
                .await
                .unwrap();
            let unrelated = if matches!(effects.fault, ChildFault::ParentStopReadCorruption) {
                let workflow = workspace
                    .workflow("retained-unrelated".parse().unwrap())
                    .await
                    .unwrap();
                let run = workflow.start(ExecutionOptions::default()).await.unwrap();
                let snapshot = tokio::time::timeout(Duration::from_secs(3), run.wait())
                    .await
                    .expect("unrelated run completes before the child scenario")
                    .unwrap();
                assert!(matches!(snapshot.status, RunStatus::Succeeded));
                assert_eq!(snapshot.state.values()["count"], 1);
                Some(format!(
                    "{}/retained-unrelated/{}/{}",
                    workspace.id(),
                    run.id(),
                    snapshot.occurrence
                ))
            } else {
                None
            };
            let workflow = workspace
                .workflow("executor-parent".parse().unwrap())
                .await
                .unwrap();
            let run = workflow.start(ExecutionOptions::default()).await.unwrap();
            let parent = run.id().to_string();
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let children = Self::admitted_records(&store, &parent).await;
                    if children.len() == 2
                        && children[0]["checkpoint"]["phase"]["state"] == "succeeded"
                        && ChildStore::has_single_control(&children[1], "input")
                        && effects.stop_read_started.load(Ordering::SeqCst)
                    {
                        assert_eq!(children[0]["checkpoint"]["state"]["item"], 11);
                        assert_eq!(children[1]["checkpoint"]["state"]["item"], 21);
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .expect("actual child effects and parked input precede the observer fault");
            assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.dropped.load(Ordering::SeqCst), 0);
            if let Some(unrelated) = &unrelated {
                let children = Self::admitted_records(&store, &parent).await;
                let child = format!(
                    "{}/executor-child/{}/{}",
                    workspace.id(),
                    children[0]["id"].as_str().unwrap(),
                    children[0]["occurrence"].as_str().unwrap()
                );
                tokio::time::timeout(Duration::from_secs(2), async {
                    for scope in [&child, unrelated] {
                        loop {
                            let head = store
                                .load(StateKey::new("app-kit", format!("run/{scope}")).unwrap())
                                .await
                                .unwrap()
                                .expect("completed run retains its head");
                            if head.record.value.pointer("/retention/state")
                                == Some(&json!("retained"))
                                && store
                                    .load(
                                        StateKey::new("app-kit", format!("active/{scope}"))
                                            .unwrap(),
                                    )
                                    .await
                                    .unwrap()
                                    .is_none()
                            {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(2)).await;
                        }
                    }
                })
                .await
                .expect("exact terminal capacity release precedes fatal ownership loss");
            }
            if !matches!(effects.fault, ChildFault::ParentStopReadHeld) {
                effects.release_stop_read();
            }
            let result = tokio::time::timeout(Duration::from_secs(4), async {
                // Observe global closure without consuming the selected parent
                // read fault through this independent public waiter.
                loop {
                    match app
                        .list_workspaces(None, NonZeroUsize::new(1).unwrap())
                        .await
                    {
                        Err(AppError::Closed(_)) => break,
                        Ok(_) => tokio::task::yield_now().await,
                        Err(error) => panic!("global closure observation failed: {error}"),
                    }
                }
                run.wait().await
            })
            .await
            .expect(
                "unreadable parent authority must cause bounded fatal closure without rebuilding E",
            );
            assert!(matches!(result, Err(AppError::Closed(_))));
            assert!(matches!(run.snapshot().await, Err(AppError::Closed(_))));
            let children = Self::admitted_records(&store, &parent).await;
            assert_eq!(children.len(), 2);
            assert_eq!(children[0]["checkpoint"]["phase"]["state"], "succeeded");
            assert!(ChildStore::has_single_control(&children[1], "input"));
            let records = Self::current_records(&store).await;
            let parent_record = records
                .iter()
                .find(|record| record["id"] == parent)
                .expect("last durable parent evidence retained");
            assert!(ChildStore::has_single_control(parent_record, "child"));
            assert_eq!(parent_record["checkpoint"]["state"]["collected"], json!([]));
            assert_eq!(parent_record["checkpoint"]["position"], json!(null));
            let retained = unrelated.map(|unrelated| {
                (
                    format!(
                        "{}/executor-child/{}/{}",
                        workspace.id(),
                        children[0]["id"].as_str().unwrap(),
                        children[0]["occurrence"].as_str().unwrap()
                    ),
                    unrelated,
                )
            });
            (
                parent,
                children[1]["id"].as_str().unwrap().to_owned(),
                children[1]["occurrence"].as_str().unwrap().to_owned(),
                retained,
            )
        })
        .catch_unwind()
        .await;
        // A passing held-read case must join its cancelled read before this
        // test releases the gate. Regression cleanup still restores access.
        if observed.is_err() {
            effects.read_fault_armed.store(false, Ordering::SeqCst);
            effects.release_stop_read();
        }
        let closed = app
            .shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            })
            .await;
        effects.read_fault_armed.store(false, Ordering::SeqCst);
        effects.release_stop_read();
        if matches!(closed, Err(AppError::Timeout(_))) {
            let _ = app
                .shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(3),
                })
                .await;
        }
        drop(app);
        let (parent, child_id, child_occurrence, retained) = match observed {
            Ok(identities) => identities,
            Err(failure) => std::panic::resume_unwind(failure),
        };
        let Err(AppError::Integration(message)) = closed else {
            panic!("fatal stop-observation failure must retain its shutdown error");
        };
        assert!(!message.contains("one parent stop observation failed"));
        match effects.fault {
            ChildFault::ParentStopReadHeld => assert_eq!(effects.faults.load(Ordering::SeqCst), 0),
            ChildFault::ParentStopReadCorruption => {
                assert_eq!(effects.faults.load(Ordering::SeqCst), 1)
            }
            ChildFault::ParentStopReadPermanent => {
                assert!(effects.faults.load(Ordering::SeqCst) > 0)
            }
            _ => unreachable!("only permanent stop-observer scenarios use this helper"),
        }
        tokio::time::timeout(Duration::from_secs(3), async {
            while evidence.executor_dropped.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fatal cleanup joins and releases the original executor owner");
        assert_eq!(evidence.executor_dropped.load(Ordering::SeqCst), 1);
        assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
        assert_eq!(evidence.dropped.load(Ordering::SeqCst), 1);

        let mut unrelated_checkpoints = Vec::new();
        if let Some((child, unrelated)) = &retained {
            for scope in [child, unrelated] {
                let head = store
                    .load(StateKey::new("app-kit", format!("run/{scope}")).unwrap())
                    .await
                    .unwrap()
                    .expect("completed run retains its exact head");
                if scope == unrelated {
                    let sequence = head.record.value["sequence"].as_u64().unwrap();
                    assert!(
                        sequence > 0,
                        "the completed run has a predecessor checkpoint"
                    );
                    unrelated_checkpoints = [sequence - 1, sequence]
                        .map(|sequence| format!("history/{scope}/checkpoint/{sequence:020}"))
                        .into_iter()
                        .collect();
                }
                assert_eq!(
                    head.record.value.pointer("/retention/state"),
                    Some(&json!("retained"))
                );
                assert!(
                    store
                        .load(StateKey::new("app-kit", format!("active/{scope}")).unwrap())
                        .await
                        .unwrap()
                        .is_none(),
                    "retained terminal runs release active capacity"
                );
            }
        }
        let recovered = Arc::new(ChildEffects {
            observing_startup: AtomicBool::new(retained.is_some()),
            hold_cancellation_receipt,
            ..ChildEffects::default()
        });
        let config: AppConfig<ChildStore, Agent> = AppConfig::new(
            ChildStore {
                inner: store.clone(),
                effects: Arc::clone(&recovered),
            },
            Vec::new(),
            Self::exhaustion_definitions(short_deadline),
        );
        let mut fresh = tokio::time::timeout(
            Duration::from_secs(3),
            App::open(config.with_state_typing(ChildTyping {
                effects: Arc::clone(&recovered),
            })),
        )
        .await
        .expect("startup validation of active runs and retained children is bounded")
        .expect("fresh generation reclaims the exact durable children");
        recovered.observing_startup.store(false, Ordering::SeqCst);
        let startup_reads = recovered.startup_reads.lock().unwrap().clone();
        let receipt_observed =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(4), async {
                if !hold_cancellation_receipt {
                    return;
                }
                let children = Self::admitted_records(&store, &parent).await;
                let workspace = fresh
                    .workspace(
                        children[1]["workspace_id"]
                            .as_str()
                            .unwrap()
                            .parse()
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                let workflow = workspace
                    .workflow("executor-parent".parse().unwrap())
                    .await
                    .unwrap();
                let parent_run = workflow
                    .open_run(RunSelector::Latest(parent.parse().unwrap()))
                    .await
                    .unwrap();
                let before = parent_run.snapshot().await.unwrap();
                parent_run.cancel(before.checkpoint).await.unwrap();
                recovered.cancellation_receipt_entered.notified().await;

                let child_workflow = workspace
                    .workflow("executor-child".parse().unwrap())
                    .await
                    .unwrap();
                let child_run = child_workflow
                    .open_run(RunSelector::Exact {
                        id: child_id.parse().unwrap(),
                        occurrence: children[1]["occurrence"].as_str().unwrap().to_owned(),
                    })
                    .await
                    .unwrap();
                let child = child_run.snapshot().await.unwrap();
                assert!(matches!(child.status, RunStatus::Cancelled));
                assert_eq!(child.state.values()["item"], 21);
                assert_eq!(child.last_action_ordinal, Some(0));
                assert_eq!(child.outcomes.len(), 1);
                let current = parent_run.snapshot().await.unwrap();
                assert!(matches!(
                    current.status,
                    RunStatus::Cancelling | RunStatus::Cancelled
                ));
                assert_eq!(current.state.values()["collected"], json!([]));
                assert!(current.outcomes.is_empty());
                let cancellations = recovered.cancellations.lock().unwrap().clone();
                assert!(
                    cancellations.is_empty(),
                    "the exact native receipt remains held"
                );
                // Hold beyond ordinary retry scheduling so a still-cancelling
                // parent cannot trivially mask premature finalization.
                let shutdown = fresh
                    .shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_secs(2),
                    })
                    .await;
                assert!(
                    matches!(shutdown, Err(AppError::Timeout(_))),
                    "graceful shutdown must await the held cancellation receipt: {shutdown:?}"
                );
            }))
            .catch_unwind()
            .await;
        recovered.cancellation_receipt_release.notify_one();
        let shutdown = tokio::time::timeout(
            Duration::from_secs(5),
            fresh.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        )
        .await;
        drop(fresh);
        match receipt_observed {
            Ok(observed) => observed.expect("cancellation receipt observation remains bounded"),
            Err(failure) => std::panic::resume_unwind(failure),
        }
        shutdown
            .expect("fresh owner cleanup remains bounded")
            .expect("fresh owner settles retained child input without replay");
        if let Some((child, unrelated)) = retained {
            assert!(
                startup_reads.contains(&format!("run/{child}")),
                "startup validates the exact retained child referenced by its active parent"
            );
            assert!(
                startup_reads.contains(&format!("run/{unrelated}")),
                "startup validates every readable retained head"
            );
            assert!(
                unrelated_checkpoints
                    .iter()
                    .all(|key| startup_reads.contains(key)),
                "startup validates the retained head's exact latest row and predecessor"
            );
            assert!(
                startup_reads
                    .iter()
                    .filter(|key| key.starts_with(&format!("history/{unrelated}/checkpoint/")))
                    .all(|key| unrelated_checkpoints.contains(key)),
                "startup does not hydrate older unrelated checkpoints"
            );
        }
        assert!(recovered.values.lock().unwrap().is_empty());
        let cancellations = recovered.cancellations.lock().unwrap().clone();
        let expected = [ChildCancellation {
            run: child_id,
            occurrence: child_occurrence,
            parent: parent.clone(),
            ordinal: 1,
            item: 21,
        }];
        assert_eq!(
            cancellations, expected,
            "the fresh owner commits the exact retained child's cancellation"
        );
        let mut original = effects.values.lock().unwrap().clone();
        original.sort();
        assert_eq!(original, [10, 20]);
        for child in Self::admitted_records(&store, &parent).await {
            let ordinal = child["parent"]["index"].as_u64().unwrap();
            assert_eq!(
                child["checkpoint"]["phase"]["state"],
                if ordinal == 0 {
                    "succeeded"
                } else {
                    "cancelled"
                }
            );
            assert_eq!(
                child["checkpoint"]["state"]["item"],
                json!(11 + ordinal * 10)
            );
        }
    }
}

#[tokio::test]
async fn unreadable_child_parent_deadline_closes_runtime_then_recovers_without_executor_replay() {
    ChildDefinitions::assert_stop_observation_exhaustion(
        true,
        ChildFault::ParentStopReadPermanent,
        false,
    )
    .await;
}

#[tokio::test]
async fn unreadable_child_parent_without_explicit_deadline_has_bounded_fatal_cleanup() {
    ChildDefinitions::assert_stop_observation_exhaustion(
        false,
        ChildFault::ParentStopReadPermanent,
        false,
    )
    .await;
}

#[tokio::test]
async fn corrupt_child_parent_observation_closes_without_retrying_permanent_failure() {
    ChildDefinitions::assert_stop_observation_exhaustion(
        false,
        ChildFault::ParentStopReadCorruption,
        false,
    )
    .await;
}

#[tokio::test]
async fn held_child_stop_read_is_revoked_and_joined_by_its_activation_deadline() {
    ChildDefinitions::assert_stop_observation_exhaustion(
        true,
        ChildFault::ParentStopReadHeld,
        false,
    )
    .await;
}

#[tokio::test]
async fn shutdown_waits_for_the_original_cancelled_child_commit_receipt() {
    ChildDefinitions::assert_stop_observation_exhaustion(
        false,
        ChildFault::ParentStopReadCorruption,
        true,
    )
    .await;
}

#[tokio::test]
async fn parent_cancellation_survives_a_stale_read_after_child_completion() {
    let effects = Arc::new(ChildEffects {
        fault: ChildFault::StaleParentRead,
        hold_cancellation_receipt: true,
        ..ChildEffects::default()
    });
    let evidence = Arc::new(IntegrityEvidence::default());
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let config: AppConfig<ChildStore, Agent> = AppConfig::new(
        ChildStore {
            inner: store.clone(),
            effects: Arc::clone(&effects),
        },
        Vec::new(),
        ChildDefinitions::all(None, 2),
    );
    let mut app = App::open(
        config
            .with_state_typing(ChildTyping {
                effects: Arc::clone(&effects),
            })
            .with_child_executor(IntegrityExecutor::new(
                IntegrityPolicy::HoldAfterFirst,
                &evidence,
                &effects,
            )),
    )
    .await
    .unwrap();
    let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(12), async {
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().unwrap(),
            })
            .await
            .unwrap();
        let workflow = workspace
            .workflow("executor-parent".parse().unwrap())
            .await
            .unwrap();
        let parent = workflow.start(ExecutionOptions::default()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), evidence.entered.notified())
            .await
            .expect("the original caller executor owns both unpolled children");
        let before = parent.snapshot().await.unwrap();
        assert!(effects.values.lock().unwrap().is_empty());
        assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
        evidence.release();
        tokio::time::timeout(
            Duration::from_secs(3),
            effects.verification_read_entered.notified(),
        )
        .await
        .expect("child completion precedes a captured native parent read");
        assert_eq!(effects.faults.load(Ordering::SeqCst), 1);
        assert!(!effects.verification_read_returned.load(Ordering::SeqCst));
        assert_eq!(*effects.values.lock().unwrap(), [10]);
        let children = ChildDefinitions::admitted_records(&store, &parent.id().to_string()).await;
        assert_eq!(children.len(), 2);
        assert_eq!(children[0]["checkpoint"]["phase"]["state"], "succeeded");
        assert_eq!(children[0]["checkpoint"]["state"]["item"], 11);
        assert_eq!(children[1]["checkpoint"]["state"]["item"], 20);
        assert_eq!(children[1]["checkpoint"]["outcomes"], json!([]));
        let current = parent.snapshot().await.unwrap();
        assert_eq!(current.state, before.state);
        assert!(current.outcomes.is_empty());
        assert_eq!(current.last_action_ordinal, None);
        // The captured native read already released its ownership, so
        // this independent cancellation must commit before that read returns.
        parent.cancel(current.checkpoint).await.unwrap();
        assert!(!effects.verification_read_returned.load(Ordering::SeqCst));
        effects.release_stop_read();
        tokio::time::timeout(
            Duration::from_secs(4),
            effects.cancellation_receipt_entered.notified(),
        )
        .await
        .expect("cancellation accepted during the stale read reaches the original unpolled child");
        assert!(effects.verification_read_returned.load(Ordering::SeqCst));
        assert!(effects.cancellations.lock().unwrap().is_empty());
        assert_eq!(*effects.values.lock().unwrap(), [10]);
        let current = parent.snapshot().await.unwrap();
        assert!(matches!(current.status, RunStatus::Cancelling));
        assert_eq!(current.state, before.state);
        assert!(current.outcomes.is_empty());
        assert_eq!(current.last_action_ordinal, None);
        let shutdown = app
            .shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(2),
            })
            .await;
        assert!(
            matches!(shutdown, Err(AppError::Timeout(_))),
            "the original child cancellation receipt remains owned: {shutdown:?}"
        );
        (
            parent.id().to_string(),
            children[1]["id"].as_str().unwrap().to_owned(),
            children[1]["occurrence"].as_str().unwrap().to_owned(),
        )
    }))
    .catch_unwind()
    .await;
    // Release every test-owned hold before surfacing a red assertion or timeout.
    effects.read_fault_armed.store(false, Ordering::SeqCst);
    effects.release_stop_read();
    evidence.release();
    if !matches!(&observed, Ok(Ok(_))) {
        // A failed wake-up assertion must not leave this caller's deliberate
        // executor hold alive when the test runtime is destroyed. This signal
        // is sent only after the original bounded observation already failed.
        evidence.request_stop();
    }
    effects
        .cancellation_receipt_released
        .store(true, Ordering::SeqCst);
    effects.cancellation_receipt_release.notify_waiters();
    let cleanup = AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(5),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        }),
    ))
    .catch_unwind()
    .await;
    drop(app);
    let (parent, child, occurrence) = match observed {
        Ok(result) => result.expect("stale parent read cancellation remains bounded"),
        Err(failure) => std::panic::resume_unwind(failure),
    };
    cleanup
        .expect("stale-read cleanup does not panic")
        .expect("stale-read cleanup remains bounded")
        .unwrap();
    assert_eq!(
        *effects.cancellations.lock().unwrap(),
        [ChildCancellation {
            run: child,
            occurrence,
            parent: parent.clone(),
            ordinal: 1,
            item: 20
        }]
    );
    assert_eq!(*effects.values.lock().unwrap(), [10]);
    assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
    assert_eq!(evidence.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(evidence.executor_dropped.load(Ordering::SeqCst), 1);
    let children = ChildDefinitions::admitted_records(&store, &parent).await;
    assert_eq!(children[0]["checkpoint"]["phase"]["state"], "succeeded");
    assert_eq!(children[1]["checkpoint"]["phase"]["state"], "cancelled");
    assert_eq!(children[1]["checkpoint"]["state"]["item"], 20);
    assert_eq!(children[1]["checkpoint"]["outcomes"], json!([]));
}

#[tokio::test]
async fn parent_stop_retains_the_cancelled_child_receipt_owned_inside_its_executor() {
    let effects = Arc::new(ChildEffects {
        hold_cancellation_receipt: true,
        ..ChildEffects::default()
    });
    let evidence = Arc::new(IntegrityEvidence::default());
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let policy = IntegrityPolicy::CancelParked;
    let config: AppConfig<ChildStore, Agent> = AppConfig::new(
        ChildStore {
            inner: store.clone(),
            effects: Arc::clone(&effects),
        },
        Vec::new(),
        ChildDefinitions::integrity_definitions(policy),
    );
    let mut app = App::open(
        config
            .with_state_typing(ChildTyping {
                effects: Arc::clone(&effects),
            })
            .with_child_executor(IntegrityExecutor::new(policy, &evidence, &effects)),
    )
    .await
    .unwrap();
    let observed =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(12), async {
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().unwrap(),
                })
                .await
                .unwrap();
            let workflow = workspace
                .workflow("executor-parent".parse().unwrap())
                .await
                .unwrap();
            let parent = workflow.start(ExecutionOptions::default()).await.unwrap();
            let children = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let children =
                        ChildDefinitions::admitted_records(&store, &parent.id().to_string()).await;
                    if children.len() == 2
                        && children[0]["checkpoint"]["phase"]["state"] == "succeeded"
                        && ChildStore::has_single_control(&children[1], "input")
                    {
                        break children;
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .expect("the original executor owns one completed and one parked child");
            assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.dropped.load(Ordering::SeqCst), 0);
            let child_workflow = workspace
                .workflow("executor-child".parse().unwrap())
                .await
                .unwrap();
            let child = child_workflow
                .open_run(RunSelector::Exact {
                    id: children[1]["id"].as_str().unwrap().parse().unwrap(),
                    occurrence: children[1]["occurrence"].as_str().unwrap().to_owned(),
                })
                .await
                .unwrap();
            let before = child.snapshot().await.unwrap();
            assert_eq!(before.state.values()["item"], 21);
            assert_eq!(before.last_action_ordinal, Some(0));
            assert_eq!(before.outcomes.len(), 1);
            child.cancel(before.checkpoint).await.unwrap();
            effects.cancellation_receipt_entered.notified().await;
            let terminal = child.snapshot().await.unwrap();
            assert!(matches!(terminal.status, RunStatus::Cancelled));
            assert_eq!(terminal.state.values()["item"], 21);
            assert_eq!(terminal.last_action_ordinal, Some(0));
            assert_eq!(terminal.outcomes.len(), 1);
            assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.dropped.load(Ordering::SeqCst), 0);
            let cancellations = effects.cancellations.lock().unwrap().clone();
            assert!(
                cancellations.is_empty(),
                "the original native receipt remains held"
            );
            let before = parent.snapshot().await.unwrap();
            assert!(!matches!(
                before.status,
                RunStatus::Succeeded
                    | RunStatus::Failed(_)
                    | RunStatus::Cancelling
                    | RunStatus::Cancelled
            ));
            assert_eq!(before.state.values()["collected"], json!([]));
            assert!(before.outcomes.is_empty());
            assert_eq!(before.last_action_ordinal, None);
            parent.cancel(before.checkpoint).await.unwrap();
            let shutdown = app
                .shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(2),
                })
                .await;
            assert!(
                matches!(shutdown, Err(AppError::Timeout(_))),
                "parent stop cannot discard its child's accepted native receipt: {shutdown:?}"
            );
            (
                parent.id().to_string(),
                child.id().to_string(),
                terminal.occurrence,
            )
        }))
        .catch_unwind()
        .await;
    effects.cancellation_receipt_release.notify_one();
    let closed = std::panic::AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(5),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        }),
    ))
    .catch_unwind()
    .await;
    drop(app);
    let (parent, child, occurrence) = match observed {
        Ok(result) => result.expect("the original child receipt scenario remains bounded"),
        Err(failure) => std::panic::resume_unwind(failure),
    };
    match closed {
        Ok(result) => result.expect("joined cleanup remains bounded").unwrap(),
        Err(failure) => std::panic::resume_unwind(failure),
    }
    let cancellations = effects.cancellations.lock().unwrap().clone();
    assert_eq!(
        cancellations,
        [ChildCancellation {
            run: child,
            occurrence,
            parent: parent.clone(),
            ordinal: 1,
            item: 21,
        }]
    );
    let mut values = effects.values.lock().unwrap().clone();
    values.sort();
    assert_eq!(values, [10, 20]);
    assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
    assert_eq!(evidence.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(evidence.executor_dropped.load(Ordering::SeqCst), 1);
    let records = tokio::time::timeout(
        Duration::from_secs(2),
        ChildDefinitions::current_records(&store),
    )
    .await
    .expect("joined terminal records remain readable");
    let current = records
        .iter()
        .find(|record| record["id"] == parent)
        .unwrap();
    assert_eq!(current["checkpoint"]["phase"]["state"], "cancelled");
    assert_eq!(current["checkpoint"]["state"]["collected"], json!([]));
    assert_eq!(current["checkpoint"]["outcomes"], json!([]));
    assert_eq!(current["checkpoint"]["position"], json!(null));
    let mut children = records
        .iter()
        .filter(|record| record["parent"]["run_id"] == parent)
        .collect::<Vec<_>>();
    children.sort_by_key(|record| record["parent"]["index"].as_u64().unwrap());
    assert_eq!(children.len(), 2);
    for (index, child) in children.iter().enumerate() {
        assert_eq!(
            child["checkpoint"]["phase"]["state"],
            if index == 0 { "succeeded" } else { "cancelled" }
        );
        assert_eq!(child["checkpoint"]["state"]["item"], json!(11 + index * 10));
        assert_eq!(child["checkpoint"]["position"]["ordinal"], 0);
        assert_eq!(child["checkpoint"]["outcomes"].as_array().unwrap().len(), 1);
    }
}

enum ParentCheckpointCase {
    Receipt(ChildFault),
    StageCapacity(ChildFault),
    InvalidTerminalChild,
}

impl ChildDefinitions {
    async fn assert_parent_checkpoint_receipt(case: ParentCheckpointCase) {
        let invalid_terminal = matches!(&case, ParentCheckpointCase::InvalidTerminalChild);
        let capacity = matches!(&case, ParentCheckpointCase::StageCapacity(_));
        let fault = match case {
            ParentCheckpointCase::Receipt(fault) | ParentCheckpointCase::StageCapacity(fault) => {
                fault
            }
            ParentCheckpointCase::InvalidTerminalChild => ChildFault::StageReceipt,
        };
        let staged = matches!(
            fault,
            ChildFault::StageReceipt | ChildFault::StageReceiptLoss
        );
        let effects = Arc::new(ChildEffects {
            hold_parent_checkpoint: AtomicBool::new(true),
            fault,
            ..ChildEffects::default()
        });
        let evidence = Arc::new(IntegrityEvidence::default());
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let config: AppConfig<ChildStore, Agent> = AppConfig::new(
            ChildStore {
                inner: store.clone(),
                effects: Arc::clone(&effects),
            },
            Vec::new(),
            ChildDefinitions::all(Some(5000), 2),
        );
        let mut app = App::open(
            config
                .with_hooks(ChildTyping {
                    effects: Arc::clone(&effects),
                })
                .with_state_typing(ChildTyping {
                    effects: Arc::clone(&effects),
                })
                .with_child_executor(IntegrityExecutor::new(
                    IntegrityPolicy::CancelParked,
                    &evidence,
                    &effects,
                )),
        )
        .await
        .unwrap();
        let mut reopened = None;
        let observed = std::panic::AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(12), async {
                let workspace = app.create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().unwrap(),
                }).await.unwrap();
                let workflow = workspace.workflow("executor-parent".parse().unwrap()).await.unwrap();
                let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
                let id = run.id().clone();
                let constructions = effects.child_constructions.load(Ordering::SeqCst);
                let mut execution = run.execution(ExecutionOptions::default()).await.unwrap();
                let mut next = Box::pin(execution.next());
                tokio::select! {
                    _ = effects.parent_checkpoint_entered.notified() => {},
                    result = &mut next => panic!("the native parent checkpoint receipt remains held: {}", result.is_ok()),
                }
                drop(next);
                drop(execution);
                let original_children = ChildDefinitions::admitted_records(&store, &id.to_string()).await;
                let mut values = effects.values.lock().unwrap().clone();
                values.sort();
                if staged {
                    assert_eq!(original_children.len(), 2);
                    for (index, child) in original_children.iter().enumerate() {
                        assert_eq!(child["checkpoint"]["phase"]["state"], "succeeded");
                        assert_eq!(child["checkpoint"]["state"]["item"], json!(11 + index * 10));
                    }
                    let records = ChildDefinitions::current_records(&store).await;
                    let parent = records.iter().find(|record| record["id"] == id.to_string()).unwrap();
                    let staged = parent["checkpoint"]["staged"].as_array().unwrap();
                    assert_eq!(staged.len(), 1);
                    assert_eq!(staged[0]["node"], "map");
                    assert_eq!(staged[0]["outcome"]["state"], "complete");
                    assert_eq!(parent["checkpoint"]["state"]["collected"], json!([]));
                    assert_eq!(parent["checkpoint"]["outcomes"], json!([]));
                    assert_eq!(parent["checkpoint"]["position"], json!(null));
                    assert_eq!(values, [10, 20]);
                } else {
                    assert!(original_children.is_empty());
                    assert!(values.is_empty());
                    assert_eq!(effects.child_constructions.load(Ordering::SeqCst), constructions);
                }
                assert_eq!(evidence.calls.load(Ordering::SeqCst), usize::from(staged));
                assert_eq!(effects.faults.load(Ordering::SeqCst), 0);
                if capacity {
                    let application = store.load(StateKey::new("app-kit", "app").unwrap())
                        .await.unwrap().unwrap().record;
                    assert_eq!(application.value["run_count"], 1,
                        "only the unreduced parent still owns active capacity");
                    let child_workflow = workspace.workflow("executor-child".parse().unwrap()).await.unwrap();
                    for child in &original_children {
                        let id_text = child["id"].as_str().unwrap();
                        let id = id_text.parse().unwrap();
                        let occurrence = child["occurrence"].as_str().unwrap().to_owned();
                        let scope = format!("{}/executor-child/{id}/{occurrence}", workspace.id());
                        let head = store.load(StateKey::new("app-kit", format!("run/{scope}")).unwrap())
                            .await.unwrap().unwrap().record;
                        assert_eq!(head.value.pointer("/retention/state"), Some(&json!("retained")));
                        assert!(store.load(StateKey::new("app-kit", format!("active/{scope}")).unwrap())
                            .await.unwrap().is_none());
                        assert_eq!(effects.hook_events.lock().unwrap().iter()
                            .filter(|(kind, run, _)| *kind == "child_finished" && run.as_str() == id_text).count(), 1,
                            "original child lifecycle finishes before its released capacity is observed at Stage");
                        let mut retained = child_workflow.open_run(RunSelector::Exact { id, occurrence }).await.unwrap();
                        let snapshot = retained.snapshot().await.unwrap();
                        assert!(matches!(retained.delete(snapshot.checkpoint).await, Err(AppError::Busy(_))),
                            "capacity release preserves the staged parent history dependency");
                    }
                }
                if invalid_terminal {
                    let records = ChildDefinitions::current_records(&store).await;
                    let parent = records.iter().find(|record| record["id"] == id.to_string()).unwrap();
                    let sequence = parent["checkpoint"]["sequence"].as_u64().unwrap();
                    let original = store.load(StateKey::new("app-kit", format!(
                        "history/{}/executor-parent/{id}/{}/checkpoint/{sequence:020}", workspace.id(), parent["occurrence"].as_str().unwrap(),
                    )).unwrap()).await.unwrap().unwrap().record;
                    let predecessor = store.load(StateKey::new("app-kit", format!(
                        "history/{}/executor-parent/{id}/{}/checkpoint/{:020}", workspace.id(), parent["occurrence"].as_str().unwrap(), sequence - 1,
                    )).unwrap()).await.unwrap().unwrap().record;
                    assert!(predecessor.value["checkpoint"]["controls"].as_object().unwrap().values().any(|control| {
                        control["state"]["kind"] == "child"
                            && control["state"]["admitted"].as_array().is_some_and(|children| children.len() == 2)
                    }), "actual Stage predecessor owns both accepted children");
                    let mut altered = original.clone();
                    Arc::make_mut(&mut altered.value)["checkpoint"]["staged"] = json!([]);
                    Arc::make_mut(&mut altered.value)["checkpoint"]["controls"] = json!({});
                    Arc::make_mut(&mut altered.value)["checkpoint"]["phase"] = json!({
                        "state":"failed", "category":"evaluation", "message":"invalid missing accepted Child",
                        "observation":null, "cancellation_checkpoint":null
                    });
                    let checked = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(4), async {
                        let mut write = store.begin_transaction().await.unwrap();
                        assert_eq!(write.load(original.key.clone()).await.unwrap().unwrap().record, original);
                        write.put(altered).await.unwrap();
                        write.commit().await.unwrap();
                        let projection = matches!(workflow.open_run(RunSelector::Latest(id.clone())).await, Err(AppError::Integration(_)));
                        let config: AppConfig<StateStore, Agent> = AppConfig::new(
                            store.clone(), Vec::new(), ChildDefinitions::all(Some(5000), 2),
                        );
                        let result = App::open(config).await;
                        let recovery = matches!(&result, Err(AppError::Integration(_)));
                        reopened = result.ok();
                        (projection, recovery)
                    })).catch_unwind().await;
                    let mut write = store.begin_transaction().await.unwrap();
                    write.load(original.key.clone()).await.unwrap().unwrap();
                    write.put(original).await.unwrap();
                    write.commit().await.unwrap();
                    let (projection, recovery) = match checked {
                        Ok(results) => results.expect("corrupt Child projection and recovery are bounded"),
                        Err(panic) => std::panic::resume_unwind(panic),
                    };
                    assert!(projection, "projection cannot retire accepted Child directly into terminal failure");
                    assert!(recovery, "startup cannot erase accepted Child predecessor authority");
                }
                let held_constructions = effects.child_constructions.load(Ordering::SeqCst);
                let mut observer = workflow.open_run(RunSelector::Latest(id.clone())).await.unwrap();
                assert!(matches!(observer.execution(ExecutionOptions::default()).await, Err(AppError::Busy(_))),
                    "dropping next and its owner cannot bypass the original native receipt");
                effects.parent_checkpoint_release.notify_one();
                let completed = tokio::time::timeout(Duration::from_secs(4), async {
                    loop {
                        let mut current = workflow.open_run(RunSelector::Latest(id.clone())).await.unwrap();
                        let snapshot = current.snapshot().await.unwrap();
                        assert_eq!(snapshot.state.values()["items"], json!([10,20]));
                        assert_eq!(snapshot.state.values()["collected"], json!([]));
                        assert_eq!(snapshot.last_action_ordinal, None);
                        assert!(snapshot.outcomes.is_empty());
                        assert!(!matches!(snapshot.status,
                            RunStatus::Succeeded | RunStatus::Failed(_) | RunStatus::Cancelling | RunStatus::Cancelled));
                        assert_eq!(ChildDefinitions::admitted_records(&store, &id.to_string()).await, original_children);
                        assert_eq!(evidence.calls.load(Ordering::SeqCst), usize::from(staged));
                        assert_eq!(effects.child_constructions.load(Ordering::SeqCst), held_constructions);
                        let mut current_values = effects.values.lock().unwrap().clone();
                        current_values.sort();
                        assert_eq!(current_values, values);
                        match current.execution(ExecutionOptions::default()).await {
                            Ok(execution) => {
                                assert_eq!(ChildDefinitions::admitted_records(&store, &id.to_string()).await, original_children);
                                assert_eq!(evidence.calls.load(Ordering::SeqCst), usize::from(staged));
                                assert_eq!(effects.child_constructions.load(Ordering::SeqCst), held_constructions);
                                break execution.finish().await.unwrap();
                            }
                            Err(AppError::Busy(_)) => tokio::task::yield_now().await,
                            Err(error) => panic!("explicit resume retains the original checkpoint: {error}"),
                        }
                    }
                }).await.expect("original native checkpoint receipt settles before explicit resumption");
                assert!(matches!(completed.status, RunStatus::Succeeded));
                assert_eq!(completed.state.values()["collected"], json!([11,21]));
                assert_eq!(completed.last_action_ordinal, Some(0));
                assert_eq!(completed.outcomes.len(), 1);
                assert_eq!(completed.outcomes[0].node, "map");
                let children = ChildDefinitions::admitted_records(&store, &id.to_string()).await;
                assert_eq!(children.len(), 2);
                for (index, child) in children.iter().enumerate() {
                    assert_eq!(child["checkpoint"]["phase"]["state"], "succeeded");
                    assert_eq!(child["checkpoint"]["state"]["item"], json!(11 + index * 10));
                }
                assert_eq!(effects.faults.load(Ordering::SeqCst), usize::from(matches!(effects.fault, ChildFault::StageReceiptLoss)));
                if staged {
                    assert_eq!(children, original_children, "receipt reconciliation cannot replay either completed child");
                }
                assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
                assert_eq!(evidence.dropped.load(Ordering::SeqCst), 1);
                let mut values = effects.values.lock().unwrap().clone();
                values.sort();
                assert_eq!(values, [10,20]);
            },
        )).catch_unwind().await;
        effects.parent_checkpoint_release.notify_one();
        effects.cancellation_receipt_release.notify_one();
        if let Some(mut reopened) = reopened {
            tokio::time::timeout(
                Duration::from_secs(5),
                reopened.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(3),
                }),
            )
            .await
            .expect("unexpected reopened owner cleanup is bounded")
            .expect("unexpected reopened owner joins");
        }
        let closed = std::panic::AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        ))
        .catch_unwind()
        .await;
        drop(app);
        match observed {
            Ok(result) => result.expect("checkpoint Drop and explicit resume remain bounded"),
            Err(failure) => std::panic::resume_unwind(failure),
        }
        match closed {
            Ok(result) => result.expect("joined cleanup remains bounded").unwrap(),
            Err(failure) => std::panic::resume_unwind(failure),
        }
        assert_eq!(evidence.executor_dropped.load(Ordering::SeqCst), 1);
        if capacity {
            let application = store
                .load(StateKey::new("app-kit", "app").unwrap())
                .await
                .unwrap()
                .unwrap()
                .record;
            assert_eq!(
                application.value["run_count"], 0,
                "Stage receipt reconciliation cannot release either child twice"
            );
        }
    }
}

#[tokio::test]
async fn owning_drop_preserves_park_without_admitting_the_first_child_group() {
    ChildDefinitions::assert_parent_checkpoint_receipt(ParentCheckpointCase::Receipt(
        ChildFault::None,
    ))
    .await;
}

#[tokio::test]
async fn owning_drop_retains_applied_child_stage_receipt_without_executor_replay() {
    ChildDefinitions::assert_parent_checkpoint_receipt(ParentCheckpointCase::Receipt(
        ChildFault::StageReceipt,
    ))
    .await;
}

#[tokio::test]
async fn owning_drop_reconciles_lost_child_stage_receipt_without_executor_replay() {
    ChildDefinitions::assert_parent_checkpoint_receipt(ParentCheckpointCase::Receipt(
        ChildFault::StageReceiptLoss,
    ))
    .await;
}

#[tokio::test]
async fn referenced_terminal_child_history_survives_until_parent_cleanup() {
    let effects = Arc::new(ChildEffects::default());
    let evidence = Arc::new(IntegrityEvidence::default());
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let config: AppConfig<ChildStore, Agent> = AppConfig::new(
        ChildStore {
            inner: store.clone(),
            effects: Arc::clone(&effects),
        },
        Vec::new(),
        ChildDefinitions::integrity_definitions(IntegrityPolicy::CancelParked),
    );
    let mut app = App::open(
        config
            .with_state_typing(ChildTyping {
                effects: Arc::clone(&effects),
            })
            .with_child_executor(IntegrityExecutor::new(
                IntegrityPolicy::CancelParked,
                &evidence,
                &effects,
            )),
    )
    .await
    .unwrap();
    let observed =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(10), async {
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().unwrap(),
                })
                .await
                .unwrap();
            let workflow = workspace
                .workflow("executor-parent".parse().unwrap())
                .await
                .unwrap();
            let mut parent = workflow.start(ExecutionOptions::default()).await.unwrap();
            let children = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let children =
                        ChildDefinitions::admitted_records(&store, &parent.id().to_string()).await;
                    if children.len() == 2
                        && children[0]["checkpoint"]["phase"]["state"] == "succeeded"
                        && ChildStore::has_single_control(&children[1], "input")
                    {
                        break children;
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .expect("real child success and sibling input precede deletion");
            let leaf = workspace
                .workflow("executor-child".parse().unwrap())
                .await
                .unwrap();
            let mut child = leaf
                .open_run(RunSelector::Exact {
                    id: children[0]["id"].as_str().unwrap().parse().unwrap(),
                    occurrence: children[0]["occurrence"].as_str().unwrap().to_owned(),
                })
                .await
                .unwrap();
            let snapshot = child.snapshot().await.unwrap();
            assert!(matches!(snapshot.status, RunStatus::Succeeded));
            assert_eq!(snapshot.state.values()["item"], 11);
            let scope = format!(
                "{}/executor-child/{}/{}",
                workspace.id(),
                child.id(),
                snapshot.occurrence
            );
            let head_key = StateKey::new("app-kit", format!("run/{scope}")).unwrap();
            let head = tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let head = store.load(head_key.clone()).await.unwrap().unwrap().record;
                    if head.value.pointer("/retention/state") == Some(&json!("retained"))
                        && store
                            .load(StateKey::new("app-kit", format!("active/{scope}")).unwrap())
                            .await
                            .unwrap()
                            .is_none()
                    {
                        break head;
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .expect("completed child releases capacity before the deletion attempt");

            // Preserve test-owned bytes for failure cleanup: an incorrect successful
            // deletion must not strand the parent before the test reports its red.
            let page = store
                .list(
                    StateListRequest::new("app-kit")
                        .unwrap()
                        .with_key_prefix(StateKeyPrefix::new(format!("history/{scope}/")).unwrap()),
                )
                .await
                .unwrap();
            assert!(
                page.next_start_after.is_none(),
                "one native child has bounded fixture history"
            );
            let result = std::panic::AssertUnwindSafe(tokio::time::timeout(
                Duration::from_secs(2),
                child.delete(snapshot.checkpoint),
            ))
            .catch_unwind()
            .await;
            let rejected = matches!(&result, Ok(Ok(Err(AppError::Busy(_)))));
            if !rejected {
                let mut transaction = store.begin_transaction().await.unwrap();
                for row in page.records {
                    transaction.put(row.record).await.unwrap();
                }
                transaction.put(head).await.unwrap();
                transaction.commit().await.unwrap();
            }
            match result {
                Ok(result) => {
                    let _ = result.expect("child deletion observation remains bounded");
                }
                Err(panic) => std::panic::resume_unwind(panic),
            }
            assert!(
                rejected,
                "active parent membership prevents deleting its completed child history"
            );
            assert_eq!(child.snapshot().await.unwrap().state.values()["item"], 11);
            assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.dropped.load(Ordering::SeqCst), 0);
            let before = parent.snapshot().await.unwrap();
            parent.cancel(before.checkpoint).await.unwrap();
            let cancelled = tokio::time::timeout(Duration::from_secs(3), parent.wait())
                .await
                .unwrap()
                .unwrap();
            assert!(matches!(cancelled.status, RunStatus::Cancelled));
            assert_eq!(cancelled.state.values()["collected"], json!([]));
            assert!(cancelled.outcomes.is_empty() && cancelled.last_action_ordinal.is_none());
            let children =
                ChildDefinitions::admitted_records(&store, &parent.id().to_string()).await;
            assert_eq!(children.len(), 2);
            assert_eq!(children[0]["checkpoint"]["phase"]["state"], "succeeded");
            assert_eq!(children[1]["checkpoint"]["phase"]["state"], "cancelled");
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    match parent.delete(cancelled.checkpoint).await {
                        Ok(()) => break,
                        Err(AppError::Busy(_)) => {
                            tokio::time::sleep(Duration::from_millis(2)).await
                        }
                        Err(error) => panic!("completed parent deletion failed: {error}"),
                    }
                }
            })
            .await
            .expect("parent deletion joins its original cleanup");
            assert!(matches!(
                parent.snapshot().await,
                Err(AppError::NotFound(_))
            ));
            let before = child.snapshot().await.unwrap();
            let observed = child
                .execution(ExecutionOptions::default())
                .await
                .expect("terminal child observation has independent retained history")
                .finish()
                .await
                .expect("terminal child observation requires no deleted parent");
            assert!(matches!(observed.status, RunStatus::Succeeded));
            assert_eq!(observed.checkpoint, before.checkpoint);
            assert_eq!(observed.state.values(), before.state.values());
            assert_eq!(observed.last_action_ordinal, before.last_action_ordinal);
            assert!(observed.outcomes == before.outcomes);
            let current = child.snapshot().await.unwrap();
            child
                .delete(current.checkpoint)
                .await
                .expect("terminal parent has released its child dependency");
            assert!(store.load(head_key).await.unwrap().is_none());
            assert!(matches!(child.snapshot().await, Err(AppError::NotFound(_))));
            assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.dropped.load(Ordering::SeqCst), 1);
            let mut values = effects.values.lock().unwrap().clone();
            values.sort();
            assert_eq!(values, [10, 20]);
        }))
        .catch_unwind()
        .await;
    let closed = tokio::time::timeout(
        Duration::from_secs(5),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        }),
    )
    .await;
    drop(app);
    match observed {
        Ok(result) => result.expect("public child deletion and parent cleanup remain bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    closed
        .expect("joined App cleanup remains bounded")
        .expect("App cleanup");
    assert_eq!(evidence.executor_dropped.load(Ordering::SeqCst), 1);
}

impl ChildDefinitions {
    async fn assert_revision_pins(case: RevisionCase) {
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let effects = Arc::new(ChildEffects::default());
        let config: AppConfig<StateStore, Agent> = AppConfig::new(
            store.clone(),
            Vec::new(),
            ChildDefinitions::revision_definitions(false, &case),
        );
        let mut old = Some(
            App::open(config.with_state_typing(ChildTyping {
                effects: Arc::clone(&effects),
            }))
            .await
            .unwrap(),
        );
        let mut fresh = None;
        let observed =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(15), async {
                let workspace = old
                    .as_ref()
                    .unwrap()
                    .create_workspace(WorkspaceConfig {
                        name: None,
                        root: std::env::current_dir().unwrap(),
                    })
                    .await
                    .unwrap();
                let workflow = workspace
                    .workflow("executor-parent".parse().unwrap())
                    .await
                    .unwrap();
                let parked = workflow.start(ExecutionOptions::default()).await.unwrap();
                let waiting = tokio::time::timeout(Duration::from_secs(3), parked.wait())
                    .await
                    .unwrap()
                    .unwrap();
                assert!(matches!(
                    waiting.interactions.as_slice(),
                    [Interaction::Input { .. }]
                ));
                assert!(
                    ChildDefinitions::admitted_records(&store, &parked.id().to_string())
                        .await
                        .is_empty(),
                    "the old parent has not yet created any child"
                );
                assert!(effects.values.lock().unwrap().is_empty());
                let workspace_id = workspace.id().clone();
                let parent_id = parked.id().clone();
                let config: AppConfig<StateStore, Agent> = AppConfig::new(
                    store.clone(),
                    Vec::new(),
                    ChildDefinitions::revision_definitions(true, &case),
                );
                fresh = Some(
                    App::open(config.with_state_typing(ChildTyping {
                        effects: Arc::clone(&effects),
                    }))
                    .await
                    .expect("reopen accepts retained v1 and newly supplied v2"),
                );
                // The new generation owns the parked record before the old handles drop.
                // Explicit cancellation here would destroy the recovery prerequisite.
                drop(parked);
                drop(workflow);
                drop(workspace);
                drop(old.take());
                let workspace = fresh
                    .as_ref()
                    .unwrap()
                    .workspace(workspace_id)
                    .await
                    .unwrap();
                let workflow = workspace
                    .workflow("executor-parent".parse().unwrap())
                    .await
                    .unwrap();
                let current_root = if matches!(case, RevisionCase::ReversedDependencies) {
                    "executor-child"
                } else {
                    "executor-parent"
                };
                let current = workspace
                    .workflow(current_root.parse().unwrap())
                    .await
                    .unwrap();
                let new_child = if matches!(case, RevisionCase::ReversedDependencies) {
                    "executor-parent"
                } else {
                    "executor-child"
                };
                for (run, expected, child_workflow, child_revision) in [
                    (
                        workflow
                            .open_run(RunSelector::Latest(parent_id))
                            .await
                            .unwrap(),
                        json!([11, 21]),
                        "executor-child",
                        "v1",
                    ),
                    (
                        current.start(ExecutionOptions::default()).await.unwrap(),
                        json!([110, 120]),
                        new_child,
                        "v2",
                    ),
                ] {
                    let waiting = tokio::time::timeout(Duration::from_secs(3), run.wait())
                        .await
                        .unwrap()
                        .unwrap();
                    let [
                        Interaction::Input {
                            sequence,
                            generation,
                            ..
                        },
                    ] = waiting.interactions.as_slice()
                    else {
                        panic!(
                            "each parent reaches its actual input control before child admission"
                        );
                    };
                    run.resolve(
                        *sequence,
                        *generation,
                        Resolution::Input(serde_json::Value::Null),
                    )
                    .await
                    .unwrap();
                    let completed = tokio::time::timeout(Duration::from_secs(3), run.wait())
                        .await
                        .unwrap()
                        .unwrap();
                    assert!(matches!(completed.status, RunStatus::Succeeded));
                    assert_eq!(completed.state.values()["collected"], expected);
                    let children =
                        ChildDefinitions::admitted_records(&store, &run.id().to_string()).await;
                    assert_eq!(children.len(), 2);
                    for (child, expected) in children.iter().zip(expected.as_array().unwrap()) {
                        assert_eq!(child["workflow_id"], child_workflow);
                        assert_eq!(child["definition_revision"], child_revision);
                        assert_eq!(child["checkpoint"]["phase"]["state"], "succeeded");
                        assert_eq!(&child["checkpoint"]["state"]["item"], expected);
                    }
                }
                let mut actual = effects.values.lock().unwrap().clone();
                actual.sort();
                assert_eq!(
                    actual,
                    [10, 10, 20, 20],
                    "each child performs one real state mutation"
                );
            }))
            .catch_unwind()
            .await;
        let fresh_closed = match fresh.as_mut() {
            Some(app) => Some(
                tokio::time::timeout(
                    Duration::from_secs(5),
                    app.shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_secs(3),
                    }),
                )
                .await,
            ),
            None => None,
        };
        drop(fresh);
        if let Some(mut app) = old {
            // Setup failure cleanup only; the passing path already dropped the
            // fenced owner after transferring its parked run to the new generation.
            let _ = tokio::time::timeout(
                Duration::from_secs(5),
                app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(3),
                }),
            )
            .await;
        }
        match observed {
            Ok(result) => result.expect("revision selection is bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        fresh_closed
            .expect("fresh generation was admitted")
            .expect("fresh owner joins")
            .expect("fresh owner cleanup");
    }
}

#[tokio::test]
async fn parked_parent_pins_future_children_while_new_parent_selects_new_revision() {
    ChildDefinitions::assert_revision_pins(RevisionCase::Behavior).await;
}

#[tokio::test]
async fn retained_parent_schema_is_validated_against_its_selected_child_revision() {
    ChildDefinitions::assert_revision_pins(RevisionCase::Schema).await;
}

#[tokio::test]
async fn distinct_revision_dependencies_do_not_create_a_cycle_between_selected_closures() {
    // Old A/v1 -> B/v1 and current B/v2 -> A/v2 each terminate. Collapsing
    // versions into a union of workflow IDs fabricates an A -> B -> A cycle.
    ChildDefinitions::assert_revision_pins(RevisionCase::ReversedDependencies).await;
}

impl ChildDefinitions {
    async fn assert_foreground_child_ownership(drop_owner: bool) {
        let caller = std::thread::current().id();
        let effects = Arc::new(ChildEffects::default());
        let evidence = Arc::new(IntegrityEvidence::default());
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let config: AppConfig<ChildStore, Agent> = AppConfig::new(
            ChildStore {
                inner: store.clone(),
                effects: Arc::clone(&effects),
            },
            Vec::new(),
            Self::all(None, 2),
        );
        let mut app = App::open(
            config
                .with_state_typing(ChildTyping {
                    effects: Arc::clone(&effects),
                })
                .with_child_executor(IntegrityExecutor::new(
                    IntegrityPolicy::HoldForeground,
                    &evidence,
                    &effects,
                )),
        )
        .await
        .unwrap();
        let observed = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(10), async {
            let workspace = app.create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().unwrap(),
            }).await.unwrap();
            let workflow = workspace.workflow("executor-parent".parse().unwrap()).await.unwrap();
            let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
            let mut observer = workflow.open_run(RunSelector::Latest(run.id().clone())).await.unwrap();
            let mut execution = run.execution(ExecutionOptions::default()).await.unwrap();
            let mut next = Box::pin(execution.next());
            tokio::time::timeout(Duration::from_secs(2), async {
                tokio::select! {
                    biased;
                    _ = evidence.entered.notified() => {}
                    event = &mut next => panic!("next returned before its admitted child batch completed: {}", match event {
                        Ok(None) => "exhausted",
                        Ok(Some(_)) => "event",
                        Err(_) => "error",
                    }),
                }
            }).await.expect("the real local executor reaches its hold");
            // Cancelling the waiter leaves the original batch on the Execution's
            // retained publication owner, without constructing another future.
            drop(next);
            assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.dropped.load(Ordering::SeqCst), 0);
            let no_effects = effects.values.lock().unwrap().is_empty();
            assert!(no_effects);
            let held = observer.snapshot().await.unwrap();
            assert_eq!(held.state.values()["collected"], json!([]));
            assert!(held.last_action_ordinal.is_none() && held.outcomes.is_empty());
            let children = Self::admitted_records(&store, &observer.id().to_string()).await;
            assert_eq!(children.len(), 2, "the complete real group commits before E");
            for (index, child) in children.iter().enumerate() {
                assert_eq!(child["checkpoint"]["sequence"], 0);
                assert_eq!(child["checkpoint"]["state"]["item"], 10 + index * 10);
                assert_eq!(child["parent"]["index"], index);
            }
            if drop_owner {
                drop(execution);
                assert!(matches!(observer.execution(ExecutionOptions::default()).await, Err(AppError::Busy(_))),
                    "owning Drop cannot release a still-held child publication");
                assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
                assert_eq!(evidence.dropped.load(Ordering::SeqCst), 0);
                let no_effects = effects.values.lock().unwrap().is_empty();
                assert!(no_effects);
                assert_eq!(observer.snapshot().await.unwrap().checkpoint, held.checkpoint);
                evidence.release();
                let paused = observer.wait().await.unwrap();
                assert!(matches!(paused.status, RunStatus::Paused));
                assert_eq!(paused.state.values()["collected"], json!([]));
                assert!(paused.last_action_ordinal.is_none() && paused.outcomes.is_empty());
                let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
                let complete = loop {
                    assert!(tokio::time::Instant::now() < deadline, "child publication must release its generation");
                    let mut candidate = workflow.open_run(RunSelector::Latest(observer.id().clone())).await.unwrap();
                    let acquisition = candidate.execution(ExecutionOptions::default()).await;
                    match acquisition {
                        Ok(execution) => break execution.finish().await.unwrap(),
                        Err(AppError::Busy(_)) => {
                            let current = observer.snapshot().await.unwrap();
                            assert!(matches!(current.status, RunStatus::Paused));
                            assert_eq!(current.checkpoint, paused.checkpoint);
                            assert_eq!(current.state.values(), paused.state.values());
                            assert_eq!(current.last_action_ordinal, paused.last_action_ordinal);
                            assert!(current.outcomes == paused.outcomes);
                            assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
                            assert_eq!(evidence.dropped.load(Ordering::SeqCst), 1);
                            let mut values = effects.values.lock().unwrap().clone();
                            values.sort();
                            assert_eq!(values, [10, 20]);
                            tokio::task::yield_now().await;
                        }
                        Err(error) => panic!("paused child continuation could not acquire: {error}"),
                    }
                };
                assert!(matches!(complete.status, RunStatus::Succeeded));
                assert_eq!(complete.state.values()["collected"], json!([11, 21]));
            } else {
                evidence.release();
                let event = execution.next().await.unwrap();
                assert!(matches!(event, Some(ExecutionEvent::Committed { .. })),
                    "resuming the same waiter must reach the parent barrier");
                let complete = execution.finish().await.unwrap();
                assert!(matches!(complete.status, RunStatus::Succeeded));
                assert_eq!(complete.state.values()["collected"], json!([11, 21]));
                assert_eq!(complete.last_action_ordinal, Some(0));
            }
            assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.dropped.load(Ordering::SeqCst), 1);
            let mut values = effects.values.lock().unwrap().clone();
            values.sort();
            assert_eq!(values, [10, 20], "neither accepted child effect repeats");
            let threads = evidence.threads.lock().unwrap().clone();
            assert!(threads.len() >= 3);
            assert_ne!(threads[0], caller, "asynchronous native child work uses the existing worker");
            assert!(threads.iter().all(|thread| *thread == threads[0]),
                "construction, every poll and Drop retain the same local owner");
        })).catch_unwind().await;
        evidence.release();
        let cleanup = std::panic::AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(6),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(4),
            }),
        ))
        .catch_unwind()
        .await;
        drop(app);
        let released = tokio::time::timeout(Duration::from_secs(3), async {
            while evidence.executor_dropped.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        match observed {
            Ok(result) => result.expect("foreground child proof remains bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup
            .expect("cleanup does not panic")
            .expect("cleanup remains bounded")
            .unwrap();
        released.expect("the transferred executor owner is released");
        assert_eq!(evidence.executor_dropped.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn cancelled_next_retains_the_exact_local_child_batch_until_parent_commit() {
    ChildDefinitions::assert_foreground_child_ownership(false).await;
}

#[tokio::test]
async fn owning_execution_drop_retains_child_publication_before_reacquisition() {
    ChildDefinitions::assert_foreground_child_ownership(true).await;
}

impl ChildDefinitions {
    async fn assert_native_child_read_disposition(fault: ChildFault) {
        let effects = Arc::new(ChildEffects {
            decision_read_remaining: AtomicUsize::new(match fault {
                ChildFault::PermanentRead | ChildFault::HeldExecutionRead => 1,
                ChildFault::PermanentAfterHook
                | ChildFault::RecoverableAfterHook
                | ChildFault::HeldAfterHook => 0,
                _ => 12,
            }),
            fault,
            ..ChildEffects::default()
        });
        let observed_order = Arc::new(Mutex::new(Vec::new()));
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let mut definitions = Self::all(
            match effects.fault {
                ChildFault::HeldExecutionRead => Some(1_000),
                ChildFault::HeldAfterHook => Some(10_000),
                _ => None,
            },
            2,
        );
        if matches!(effects.fault, ChildFault::HeldAfterHook) {
            let mut child: serde_json::Value = serde_json::from_str(Self::CHILD).unwrap();
            child["nodes"][0]["deadline_ms"] = json!(1_000);
            definitions[0] = WorkflowDefinition::parse(&child.to_string()).unwrap();
        }
        let config: AppConfig<ChildStore, Agent> = AppConfig::new(
            ChildStore {
                inner: store.clone(),
                effects: Arc::clone(&effects),
            },
            Vec::new(),
            definitions,
        );
        let mut app = App::open(
            config
                .with_state_typing(ChildTyping {
                    effects: Arc::clone(&effects),
                })
                .with_hooks(ChildTyping {
                    effects: Arc::clone(&effects),
                })
                .with_child_executor(ReverseExecutor {
                    observed: Arc::clone(&observed_order),
                    effects: Arc::clone(&effects),
                }),
        )
        .await
        .unwrap();
        let observed =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(15), async {
                let workspace = app
                    .create_workspace(WorkspaceConfig {
                        name: None,
                        root: std::env::current_dir().unwrap(),
                    })
                    .await
                    .unwrap();
                let workflow = workspace
                    .workflow("executor-parent".parse().unwrap())
                    .await
                    .unwrap();
                let run = workflow.start(ExecutionOptions::default()).await.unwrap();
                let retry = tokio::time::timeout(Duration::from_secs(2), async {
                    while effects.faults.load(Ordering::SeqCst) == 0 {
                        effects.decision_read_observed.notified().await;
                    }
                    let selected = effects.fault_key.lock().unwrap().clone();
                    let children = Self::admitted_records(&store, &run.id().to_string()).await;
                    assert_eq!(children.len(), 2);
                    assert_eq!(children[0]["parent"]["index"], 0);
                    let initial = store
                        .load(
                            StateKey::new(
                                "app-kit",
                                format!(
                                    "history/{}/executor-child/{}/{}/checkpoint/{:020}",
                                    workspace.id(),
                                    children[0]["id"].as_str().unwrap(),
                                    children[0]["occurrence"].as_str().unwrap(),
                                    0,
                                ),
                            )
                            .unwrap(),
                        )
                        .await
                        .unwrap()
                        .expect("the actual admission retains checkpoint zero");
                    assert_eq!(initial.record.value["checkpoint"]["state"]["item"], 10);
                    assert_eq!(
                        initial.record.value["parent"]["run_id"],
                        run.id().to_string()
                    );
                    assert_eq!(
                        selected,
                        Some(
                            StateKey::new(
                                "app-kit",
                                format!(
                                    "run/{}/executor-child/{}/{}",
                                    workspace.id(),
                                    children[0]["id"].as_str().unwrap(),
                                    children[0]["occurrence"].as_str().unwrap(),
                                )
                            )
                            .unwrap()
                        ),
                        "the first fault belongs to the exact initially admitted child"
                    );
                    if matches!(effects.fault, ChildFault::None) {
                        while effects.faults.load(Ordering::SeqCst) < 2 {
                            effects.decision_read_observed.notified().await;
                        }
                    }
                })
                .await;
                retry.expect(
                    "the exact admitted child execution read reaches its selected boundary",
                );
                if matches!(
                    effects.fault,
                    ChildFault::HeldExecutionRead | ChildFault::HeldAfterHook
                ) {
                    assert_eq!(effects.executions.load(Ordering::SeqCst), 1);
                    assert_eq!(effects.execution_returns.load(Ordering::SeqCst), 0);
                    assert_eq!(
                        effects.execution_drops.load(Ordering::SeqCst),
                        0,
                        "the original executor remains owned while the read is held"
                    );
                    assert!(
                        !effects.values.lock().unwrap().contains(&10),
                        "the held authority read precedes this child's first effect"
                    );
                }
                if matches!(
                    effects.fault,
                    ChildFault::PermanentRead
                        | ChildFault::PermanentAfterHook
                        | ChildFault::HeldExecutionRead
                        | ChildFault::HeldAfterHook
                ) {
                    // This checks global fatal closure, not a single run's
                    // operation error. A subsequent valid read must not hide
                    // the permanent disposition and execute this child.
                    tokio::time::timeout(Duration::from_secs(4), async {
                        loop {
                            match app
                                .list_workspaces(None, NonZeroUsize::new(1).unwrap())
                                .await
                            {
                                Err(AppError::Closed(_)) => break,
                                Ok(_) => tokio::task::yield_now().await,
                                Err(error) => {
                                    panic!("unexpected App closure observation: {error:?}")
                                }
                            }
                        }
                        assert!(matches!(run.snapshot().await, Err(AppError::Closed(_))));
                        assert!(matches!(run.wait().await, Err(AppError::Closed(_))));
                    })
                    .await
                    .expect("unreadable child authority closes within its original read budget");
                    if matches!(
                        effects.fault,
                        ChildFault::HeldExecutionRead | ChildFault::HeldAfterHook
                    ) {
                        let mut held = effects
                            .held_execution_read
                            .lock()
                            .unwrap()
                            .take()
                            .expect("the original exact child-head read owns its witness");
                        assert!(
                            matches!(
                                held.try_recv(),
                                Err(tokio::sync::oneshot::error::TryRecvError::Closed)
                            ),
                            "the held pure-read future is dropped before public fatal closure"
                        );
                    }
                    let children = Self::admitted_records(&store, &run.id().to_string()).await;
                    assert_eq!(children.len(), 2);
                    assert_eq!(children[0]["checkpoint"]["sequence"], 0);
                    assert_eq!(children[0]["checkpoint"]["state"]["item"], 10);
                    assert_eq!(children[0]["checkpoint"]["position"], json!(null));
                    assert_eq!(children[0]["checkpoint"]["outcomes"], json!([]));
                    assert_eq!(children[1]["checkpoint"]["phase"]["state"], "succeeded");
                    assert_eq!(children[1]["checkpoint"]["state"]["item"], 21);
                    let records = Self::current_records(&store).await;
                    let parent = records
                        .iter()
                        .find(|record| record["id"] == run.id().to_string())
                        .expect("original parent remains durable recovery evidence");
                    assert_eq!(parent["checkpoint"]["state"]["collected"], json!([]));
                    assert_eq!(parent["checkpoint"]["position"], json!(null));
                    assert_eq!(parent["checkpoint"]["outcomes"], json!([]));
                    let values = effects.values.lock().unwrap().clone();
                    assert_eq!(
                        values,
                        [20],
                        "unreadable child authority admits no State effect"
                    );
                    assert_eq!(effects.faults.load(Ordering::SeqCst), 1);
                    return;
                }
                // Keep public run observations outside the injected outage:
                // only the original child future may spend these failures.
                let expected_faults = if matches!(effects.fault, ChildFault::RecoverableAfterHook) {
                    1
                } else {
                    12
                };
                let complete = tokio::time::timeout(Duration::from_secs(10), async {
                    while effects.execution_returns.load(Ordering::SeqCst) == 0 {
                        tokio::task::yield_now().await;
                    }
                    assert_eq!(effects.execution_returns.load(Ordering::SeqCst), 1);
                    assert_eq!(effects.faults.load(Ordering::SeqCst), expected_faults);
                    run.wait().await
                })
                .await
                .expect("recoverable native reads retain the original hook and child executor")
                .unwrap();
                assert!(matches!(complete.status, RunStatus::Succeeded));
                assert_eq!(complete.state.values()["collected"], json!([11, 21]));
                assert_eq!(effects.faults.load(Ordering::SeqCst), expected_faults);
                let mut values = effects.values.lock().unwrap().clone();
                values.sort();
                assert_eq!(
                    values,
                    [10, 20],
                    "native decision read retries do not replay effects"
                );
                assert_eq!(effects.execution_drops.load(Ordering::SeqCst), 1);
                let order = observed_order.lock().unwrap().clone();
                assert_eq!(
                    order,
                    [1, 0],
                    "both original pins complete in caller-selected order"
                );
                assert_eq!(
                    effects.executions.load(Ordering::SeqCst),
                    1,
                    "retry retains the originally admitted local executor"
                );
            }))
            .catch_unwind()
            .await;
        effects.decision_read_remaining.store(0, Ordering::SeqCst);
        effects.verification_read_release.notify_one();
        let cleanup = std::panic::AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(6),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(4),
            }),
        ))
        .catch_unwind()
        .await;
        drop(app);
        let released = tokio::time::timeout(Duration::from_secs(3), async {
            while effects.executor_drops.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        match observed {
            Ok(result) => result.expect("native child-read fault proof remains bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        let cleanup = cleanup
            .expect("cleanup does not panic")
            .expect("cleanup remains bounded");
        if matches!(
            effects.fault,
            ChildFault::PermanentRead
                | ChildFault::PermanentAfterHook
                | ChildFault::HeldExecutionRead
                | ChildFault::HeldAfterHook
        ) {
            assert!(
                matches!(cleanup, Err(AppError::Integration(_))),
                "fatal cleanup retains the permanent read failure"
            );
            let values = effects.values.lock().unwrap().clone();
            assert_eq!(
                values,
                [20],
                "joined cleanup cannot execute the unreadable child"
            );
            if matches!(
                effects.fault,
                ChildFault::PermanentAfterHook | ChildFault::HeldAfterHook
            ) {
                let hooks = effects.hook_events.lock().unwrap().clone();
                assert_eq!(
                    hooks
                        .iter()
                        .filter(|(kind, node, _)| *kind == "start" && node == "add")
                        .count(),
                    2,
                    "joined cleanup preserves each original child pre-hook without replay"
                );
            }
        } else {
            cleanup.unwrap();
        }
        if matches!(effects.fault, ChildFault::RecoverableAfterHook) {
            let hooks = effects.hook_events.lock().unwrap().clone();
            assert_eq!(
                hooks
                    .iter()
                    .filter(|(kind, node, _)| *kind == "start" && node == "add")
                    .count(),
                2,
                "recovery and joined cleanup preserve each original child pre-hook without replay"
            );
            let values = effects.values.lock().unwrap().clone();
            assert_eq!(
                values,
                [20, 10],
                "joined cleanup cannot repeat either child effect"
            );
        }
        released.expect("the transferred executor is released");
        assert_eq!(effects.executions.load(Ordering::SeqCst), 1);
        assert_eq!(effects.execution_drops.load(Ordering::SeqCst), 1);
        assert_eq!(effects.executor_drops.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn native_child_head_read_recovers_beyond_the_transient_retry_window_without_replay() {
    ChildDefinitions::assert_native_child_read_disposition(ChildFault::None).await;
}

#[tokio::test]
async fn permanent_child_execution_read_closes_without_reclassifying_or_running_the_child() {
    ChildDefinitions::assert_native_child_read_disposition(ChildFault::PermanentRead).await;
}

#[tokio::test]
async fn permanent_child_read_after_pre_hook_closes_without_replaying_hook_or_effect() {
    ChildDefinitions::assert_native_child_read_disposition(ChildFault::PermanentAfterHook).await;
}

#[tokio::test]
async fn recoverable_child_read_after_pre_hook_retains_original_hook_and_executes_once() {
    ChildDefinitions::assert_native_child_read_disposition(ChildFault::RecoverableAfterHook).await;
}

#[tokio::test]
async fn held_child_execution_read_expires_and_joins_under_its_original_parent_deadline() {
    ChildDefinitions::assert_native_child_read_disposition(ChildFault::HeldExecutionRead).await;
}

#[tokio::test]
async fn held_child_read_after_successful_hook_expires_at_the_earlier_node_deadline() {
    ChildDefinitions::assert_native_child_read_disposition(ChildFault::HeldAfterHook).await;
}

impl ChildDefinitions {
    fn assert_synchronous_children(fault: ChildFault) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("the caller keeps its lower runtime driven");
        let caller = std::thread::current().id();
        let rejected = matches!(fault, ChildFault::RejectedMutation);
        let cancelled = matches!(fault, ChildFault::HeldVerification);
        let effects = Arc::new(ChildEffects {
            verification_read_pending: AtomicBool::new(cancelled),
            fault,
            ..ChildEffects::default()
        });
        let evidence = Arc::new(IntegrityEvidence::default());
        let store = runtime
            .block_on(StateStore::connect(StateStoreConfig::Lance(
                LanceStateStoreConfig::memory(),
            )))
            .unwrap();
        let mut app = runtime.block_on(async {
            let config: AppConfig<ChildStore, Agent> = AppConfig::new(
                ChildStore {
                    inner: store.clone(),
                    effects: Arc::clone(&effects),
                },
                Vec::new(),
                Self::all(Some(5000), 2),
            );
            App::open(
                config
                    .with_state_typing(ChildTyping {
                        effects: Arc::clone(&effects),
                    })
                    .with_hooks(ChildTyping {
                        effects: Arc::clone(&effects),
                    })
                    .with_child_executor(IntegrityExecutor::new(
                        IntegrityPolicy::HoldSynchronous,
                        &evidence,
                        &effects,
                    )),
            )
            .await
            .unwrap()
        });
        let mut release = None;
        let observed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let workspace = runtime
                .block_on(app.create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().unwrap(),
                }))
                .unwrap();
            let workflow = runtime
                .block_on(workspace.workflow("executor-parent".parse().unwrap()))
                .unwrap();
            let mut run = runtime
                .block_on(workflow.prepare(WorkflowState::default()))
                .unwrap();
            let prior_async = effects.async_hooks.load(Ordering::SeqCst);
            let held = Arc::clone(&evidence);
            let held_effects = Arc::clone(&effects);
            let observer = runtime
                .block_on(workflow.open_run(RunSelector::Latest(run.id().clone())))
                .unwrap();
            let native = store.clone();
            release = Some(runtime.spawn(async move {
                let entered = tokio::time::timeout(Duration::from_secs(2), held.entered.notified())
                    .await
                    .is_ok();
                let no_effects = held_effects.values.lock().unwrap().is_empty();
                let calls = held.calls.load(Ordering::SeqCst);
                let dropped = held.dropped.load(Ordering::SeqCst);
                // The same stored waker resumes the real Rc future; no polling
                // or effect is performed by this test-side release task.
                held.release();
                if cancelled {
                    let checked = std::panic::AssertUnwindSafe(tokio::time::timeout(
                        Duration::from_secs(3),
                        async {
                            held_effects.verification_read_entered.notified().await;
                            assert_eq!(held_effects.execution_returns.load(Ordering::SeqCst), 1);
                            assert_eq!(held.dropped.load(Ordering::SeqCst), 1);
                            assert_eq!(
                                held_effects
                                    .hook_events
                                    .lock()
                                    .unwrap()
                                    .iter()
                                    .filter(|(event, _, _)| *event == "child_finished")
                                    .count(),
                                2
                            );
                            let children =
                                Self::admitted_records(&native, &observer.id().to_string()).await;
                            assert_eq!(children.len(), 2);
                            for (index, child) in children.iter().enumerate() {
                                assert_eq!(child["checkpoint"]["phase"]["state"], "succeeded");
                                assert_eq!(
                                    child["checkpoint"]["state"]["item"],
                                    json!(11 + index * 10)
                                );
                            }
                            let snapshot = observer.snapshot().await.unwrap();
                            assert_eq!(snapshot.state.values()["collected"], json!([]));
                            assert!(
                                snapshot.outcomes.is_empty()
                                    && snapshot.last_action_ordinal.is_none()
                            );
                            observer.cancel(snapshot.checkpoint).await.unwrap();
                        },
                    ))
                    .catch_unwind()
                    .await;
                    held_effects.verification_read_release.notify_one();
                    match checked {
                        Ok(result) => result.expect(
                            "public cancellation reaches the held local completion verifier",
                        ),
                        Err(panic) => std::panic::resume_unwind(panic),
                    }
                }
                (entered, no_effects, calls, dropped)
            }));
            let mut execution = SyncExecution::open(&mut run, ExecutionOptions::default()).unwrap();
            if cancelled {
                assert!(
                    execution.try_next().unwrap().is_none(),
                    "cancellation cannot publish a parent barrier"
                );
            } else if !rejected {
                assert!(
                    matches!(execution.try_next().unwrap(), Some(ExecutionEvent::Committed { outcomes, .. })
                    if outcomes.len() == 1 && outcomes[0].node == "map" && outcomes[0].ordinal == 0),
                    "Sync next must drive the original child group through its parent barrier"
                );
            }
            let complete = if cancelled {
                // Blocking next has now returned. Owning Drop is reachable here,
                // while the held verification was owned by the accepted publisher.
                drop(execution);
                runtime
                    .block_on(async {
                        tokio::time::timeout(Duration::from_secs(3), run.wait()).await
                    })
                    .expect("original cancellation settles after Sync owner Drop")
                    .unwrap()
            } else {
                execution
                    .finish()
                    .expect("the original Sync child owner finishes")
            };
            if cancelled {
                assert!(matches!(complete.status, RunStatus::Cancelled));
                assert_eq!(complete.state.values()["collected"], json!([]));
                assert!(complete.last_action_ordinal.is_none() && complete.outcomes.is_empty());
                assert!(effects.verification_read_returned.load(Ordering::SeqCst));
            } else if rejected {
                assert!(matches!(
                    complete.status,
                    RunStatus::Failed(WorkflowFailure::Child { .. })
                ));
                assert_eq!(complete.state.values()["collected"], json!([]));
                assert!(complete.last_action_ordinal.is_none() && complete.outcomes.is_empty());
            } else {
                assert!(matches!(complete.status, RunStatus::Succeeded));
                assert_eq!(complete.state.values()["collected"], json!([11, 21]));
                assert_eq!(complete.last_action_ordinal, Some(0));
                assert_eq!(complete.outcomes.len(), 1);
                assert_eq!(complete.outcomes[0].node, "map");
            }
            let children = runtime.block_on(async {
                workspace
                    .workflow("executor-child".parse().unwrap())
                    .await
                    .unwrap()
                    .list_runs(None, NonZeroUsize::new(3).unwrap())
                    .await
                    .unwrap()
            });
            assert_eq!(
                children.len(),
                2,
                "Sync still admits the complete group before polling"
            );
            let mut children = children;
            children.sort_by_key(|child| child.state.values()["item"].as_i64().unwrap());
            if rejected {
                assert!(matches!(
                    children[0].status,
                    RunStatus::Failed(WorkflowFailure::State(_))
                ));
                assert!(matches!(children[1].status, RunStatus::Cancelled));
                assert_eq!(children[0].state.values()["item"], json!(10));
                assert_eq!(children[1].state.values()["item"], json!(20));
                assert!(
                    children
                        .iter()
                        .all(|child| child.last_action_ordinal.is_none()
                            && child.outcomes.is_empty())
                );
            } else {
                for (child, item) in children.iter().zip([11, 21]) {
                    assert!(matches!(child.status, RunStatus::Succeeded));
                    assert_eq!(child.state.values()["item"], json!(item));
                    assert_eq!(child.last_action_ordinal, Some(0));
                    assert_eq!(child.outcomes.len(), 1);
                }
            }
            let values = effects.values.lock().unwrap().clone();
            assert_eq!(
                values,
                if rejected { vec![10] } else { vec![10, 20] },
                "Sync polls in order and never starts a later child after the first error"
            );
            let mutations = effects.mutation_threads.lock().unwrap().clone();
            assert_eq!(mutations.len(), values.len());
            assert!(
                mutations.iter().all(|thread| *thread == caller),
                "real child State callbacks remain on the blocking caller: {mutations:?}"
            );
            let threads = evidence.threads.lock().unwrap().clone();
            assert!(
                threads.len() >= 4,
                "construct, Pending poll, resumed poll and Drop are observed"
            );
            assert!(
                threads.iter().all(|thread| *thread == caller),
                "the same Rc executor is constructed, polled and dropped on the caller: {threads:?}"
            );
            assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.dropped.load(Ordering::SeqCst), 1);
            assert_eq!(
                effects.faults.load(Ordering::SeqCst),
                usize::from(rejected || cancelled)
            );
            let hooks = effects.hook_events.lock().unwrap().clone();
            assert!(
                hooks
                    .iter()
                    .all(|(_, _, mode)| *mode == ActionMode::Synchronous)
            );
            let registrations = hooks
                .iter()
                .filter(|(event, _, _)| *event == "registered")
                .map(|(_, run, _)| run.clone())
                .collect::<BTreeSet<_>>();
            assert_eq!(
                hooks
                    .iter()
                    .filter(|(event, _, _)| *event == "registered")
                    .count(),
                2
            );
            assert_eq!(
                registrations,
                children
                    .iter()
                    .map(|child| child.id.to_string())
                    .collect::<BTreeSet<_>>()
            );
            assert_eq!(
                hooks
                    .iter()
                    .filter(|(event, _, _)| *event == "child_finished")
                    .count(),
                2
            );
            assert_eq!(
                hooks
                    .iter()
                    .filter(|(event, _, _)| *event == "child_finished")
                    .map(|(_, run, _)| run.clone())
                    .collect::<BTreeSet<_>>(),
                registrations
            );
            assert_eq!(
                hooks
                    .iter()
                    .filter(|(event, node, _)| *event == "start" && node == "map")
                    .count(),
                1
            );
            assert_eq!(
                hooks
                    .iter()
                    .filter(|(event, node, _)| *event == "finish" && node == "map")
                    .count(),
                1
            );
            assert_eq!(
                hooks
                    .iter()
                    .filter(|(event, node, _)| *event == "start" && node == "add")
                    .count(),
                values.len()
            );
            assert_eq!(
                hooks
                    .iter()
                    .filter(|(event, node, _)| *event == "finish" && node == "add")
                    .count(),
                values.len()
            );
            assert_eq!(
                effects.async_hooks.load(Ordering::SeqCst),
                prior_async,
                "Sync parent, registered children and finalization do not admit async hook lanes"
            );
        }));
        evidence.release();
        effects
            .verification_read_pending
            .store(false, Ordering::SeqCst);
        effects.verification_read_release.notify_one();
        let released = runtime.block_on(async {
            match release {
                Some(mut task) => Some(
                    match tokio::time::timeout(Duration::from_secs(3), &mut task).await {
                        Ok(result) => Ok(result),
                        Err(timeout) => {
                            task.abort();
                            let _ = task.await;
                            Err(timeout)
                        }
                    },
                ),
                None => None,
            }
        });
        let cleanup = runtime.block_on(async {
            std::panic::AssertUnwindSafe(tokio::time::timeout(
                Duration::from_secs(6),
                app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(4),
                }),
            ))
            .catch_unwind()
            .await
        });
        drop(app);
        let owner_dropped = runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(3), async {
                while evidence.executor_dropped.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
        });
        if let Err(panic) = observed {
            std::panic::resume_unwind(panic);
        }
        assert_eq!(
            released
                .expect("the release task was admitted")
                .expect("release task joins within its bound")
                .expect("release task does not panic"),
            (true, true, 1, 0)
        );
        cleanup
            .expect("cleanup does not panic")
            .expect("cleanup remains bounded")
            .unwrap();
        runtime.block_on(async {
            assert_eq!(
                store
                    .load(StateKey::new("app-kit", "app").unwrap())
                    .await
                    .unwrap()
                    .unwrap()
                    .record
                    .value["run_count"],
                0,
                "joined cleanup releases every original capacity exactly once"
            );
        });
        owner_dropped.expect("App releases its transferred executor");
        assert_eq!(evidence.executor_dropped.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn synchronous_child_execution_keeps_pending_local_executor_and_child_effects_on_the_caller() {
    ChildDefinitions::assert_synchronous_children(ChildFault::None);
}

#[test]
fn synchronous_child_batch_stops_after_the_first_real_child_error() {
    ChildDefinitions::assert_synchronous_children(ChildFault::RejectedMutation);
}

#[test]
fn synchronous_completed_child_verification_retains_cancellation_and_joins_capacity_after_drop() {
    ChildDefinitions::assert_synchronous_children(ChildFault::HeldVerification);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parent_cancellation_reaches_each_child_group_while_original_receipts_are_held() {
    let effects = Arc::new(ChildEffects {
        hold_cancellation_receipt: true,
        ..ChildEffects::default()
    });
    let evidence = Arc::new(IntegrityEvidence::default());
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let policy = IntegrityPolicy::CancelParked;
    let mut definitions = ChildDefinitions::integrity_definitions(policy);
    definitions.pop();
    definitions.push(WorkflowDefinition::parse(&json!({
        "schema":"app-kit.workflow/v2","id":"executor-parent","revision":"v1",
        "state":{"items":[10,20],"collected":[],"seed":0},"required":["items","collected"],
        "reducers":{"items":"reject_concurrent","collected":"collect","seed":"reject_concurrent"},
        "entry":"seed","nodes":[
            {"id":"seed","step":{"kind":"state","mutations":[
                {"kind":"increment","target":"seed","value":{"kind":"literal","value":1}}
            ]}},
            {"id":"z","deadline_ms":10000,"step":{"kind":"map","workflow":"executor-child",
                "items":{"kind":"state","path":"items"},"input_key":"item","max_parallel":2,
                "reductions":[{"source":"item","target":"collected","reducer":"collect"}]}},
            {"id":"a","deadline_ms":10000,"step":{"kind":"map","workflow":"executor-child",
                "items":{"kind":"state","path":"items"},"input_key":"item","max_parallel":2,
                "reductions":[{"source":"item","target":"collected","reducer":"collect"}]}}
        ],"routes":{"seed":{"mode":"broadcast","branches":[
            {"target":{"kind":"node","id":"z"}},
            {"target":{"kind":"node","id":"a"}}
        ]}}
    }).to_string()).expect("two independent child groups"));
    let config: AppConfig<ChildStore, Agent> = AppConfig::new(
        ChildStore {
            inner: store.clone(),
            effects: Arc::clone(&effects),
        },
        Vec::new(),
        definitions,
    );
    let mut app = App::open(
        config
            .with_state_typing(ChildTyping {
                effects: Arc::clone(&effects),
            })
            .with_child_executor(IntegrityExecutor::new(policy, &evidence, &effects)),
    )
    .await
    .unwrap();
    let observed = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(15), async {
        let workspace = app.create_workspace(WorkspaceConfig {
            name: None, root: std::env::current_dir().unwrap(),
        }).await.unwrap();
        let workflow = workspace.workflow("executor-parent".parse().unwrap()).await.unwrap();
        let child_workflow = workspace.workflow("executor-child".parse().unwrap()).await.unwrap();
        let parent = workflow.start(ExecutionOptions::default()).await.unwrap();
        let children = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let children = child_workflow.list_runs(None, NonZeroUsize::new(8).unwrap()).await.unwrap();
                assert!(children.len() <= 4, "each group has its exact two-member admission");
                if children.len() == 4
                    && children.iter().filter(|child| matches!(child.status, RunStatus::Succeeded)).count() == 2
                    && children.iter().filter(|child| child.interactions.len() == 1 && matches!(child.interactions[0], Interaction::Input { .. })).count() == 2 {
                    break children;
                }
                tokio::task::yield_now().await;
            }
        }).await.expect("both original E groups reach their actual child input reservations");
        assert_eq!(evidence.calls.load(Ordering::SeqCst), 2);
        assert_eq!(evidence.dropped.load(Ordering::SeqCst), 0);
        let waiting = children.iter().filter(|child| child.interactions.len() == 1 && matches!(child.interactions[0], Interaction::Input { .. })).map(|child| (child.id.to_string(), child.occurrence.clone())).collect::<BTreeSet<_>>();
        assert_eq!(waiting.len(), 2);
        for child in &children {
            assert_eq!(child.state.values()["item"], if waiting.contains(&(child.id.to_string(), child.occurrence.clone())) { 21 } else { 11 });
            assert_eq!(child.last_action_ordinal, Some(0));
            assert_eq!(child.outcomes.len(), 1);
            assert_eq!(child.outcomes[0].node, "add");
        }
        let before = parent.snapshot().await.unwrap();
        assert_eq!(before.state.values()["collected"], json!([]));
        assert_eq!(before.state.values()["seed"], 1);
        assert_eq!(before.last_action_ordinal, Some(0));
        assert_eq!(before.outcomes.len(), 1);
        assert_eq!(before.outcomes[0].node, "seed");
        parent.cancel(before.checkpoint).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let children = child_workflow.list_runs(None, NonZeroUsize::new(8).unwrap()).await.unwrap();
                let cancelled = children.iter().filter(|child| matches!(child.status, RunStatus::Cancelled))
                    .map(|child| (child.id.to_string(), child.occurrence.clone())).collect::<BTreeSet<_>>();
                if cancelled == waiting {
                    assert_eq!(children.len(), 4);
                    break;
                }
                tokio::task::yield_now().await;
            }
        }).await.expect("parent-only cancellation reaches both groups before either native receipt is released");
        let returned = effects.cancellations.lock().unwrap().clone();
        assert!(returned.is_empty(), "both original native cancellation receipts remain held");
        let cancelling = parent.snapshot().await.unwrap();
        assert!(matches!(cancelling.status, RunStatus::Cancelling));
        assert_eq!(cancelling.state, before.state);
        assert_eq!(cancelling.last_action_ordinal, before.last_action_ordinal);
        assert!(cancelling.outcomes == before.outcomes);
        // All child cancellation requests above came from parent.cancel. Drain
        // now observes the already-accepted receipts without initiating them.
        let drain = app.shutdown(Shutdown::Drain { timeout: Duration::from_secs(2) }).await;
        assert!(matches!(drain, Err(AppError::Timeout(_))),
            "held child receipts remain owned through parent and App drain: {drain:?}");
        (parent.id().to_string(), waiting)
    })).catch_unwind().await;
    // The level-triggered flag also releases any original receipt whose native
    // commit completed before its waiting future could register with Notify.
    effects
        .cancellation_receipt_released
        .store(true, Ordering::SeqCst);
    effects.cancellation_receipt_release.notify_waiters();
    let cleanup = std::panic::AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(5),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        }),
    ))
    .catch_unwind()
    .await;
    drop(app);
    let (parent, waiting) = match observed {
        Ok(result) => result.expect("two-group cancellation is bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    };
    match cleanup {
        Ok(result) => result
            .expect("joined two-group cleanup is bounded")
            .unwrap(),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    let returned = effects.cancellations.lock().unwrap().clone();
    assert_eq!(returned.len(), 2);
    assert_eq!(
        returned
            .iter()
            .map(|child| (child.run.clone(), child.occurrence.clone()))
            .collect::<BTreeSet<_>>(),
        waiting
    );
    for child in returned {
        assert_eq!(child.parent, parent);
        assert_eq!(child.ordinal, 1);
        assert_eq!(child.item, 21);
    }
    let mut values = effects.values.lock().unwrap().clone();
    values.sort_unstable();
    assert_eq!(values, [10, 10, 20, 20]);
    assert_eq!(evidence.calls.load(Ordering::SeqCst), 2);
    assert_eq!(evidence.dropped.load(Ordering::SeqCst), 2);
    assert_eq!(evidence.executor_dropped.load(Ordering::SeqCst), 1);
    let records = tokio::time::timeout(
        Duration::from_secs(2),
        ChildDefinitions::current_records(&store),
    )
    .await
    .unwrap();
    let terminal = records
        .iter()
        .find(|record| record["id"] == parent)
        .unwrap();
    assert_eq!(terminal["checkpoint"]["phase"]["state"], "cancelled");
    assert_eq!(terminal["checkpoint"]["state"]["collected"], json!([]));
    assert_eq!(terminal["checkpoint"]["position"]["ordinal"], 0);
    assert_eq!(
        terminal["checkpoint"]["outcomes"].as_array().unwrap().len(),
        1
    );
    assert_eq!(terminal["checkpoint"]["outcomes"][0]["node"], "seed");
}

impl ChildDefinitions {
    async fn assert_completion_verification_read(fault: ChildFault) {
        let observation = match &fault {
            ChildFault::CompletionRead { boundary, failure } => Some((*boundary, *failure)),
            _ => None,
        };
        let shared_budget = matches!(
            observation,
            Some((
                CompletionReadBoundary::Child {
                    shared_budget: true
                } | CompletionReadBoundary::Parent {
                    shared_budget: true
                },
                _,
            ))
        );
        let held = matches!(observation, Some((_, AuthorityReadFault::Held)));
        let fatal = matches!(fault, ChildFault::PermanentRead)
            || matches!(
                observation,
                Some((_, AuthorityReadFault::Permanent | AuthorityReadFault::Held))
            );
        let effects = Arc::new(ChildEffects {
            verification_read_pending: AtomicBool::new(observation.is_none() || shared_budget),
            read_fault_armed: AtomicBool::new(observation.is_some()),
            fault,
            ..ChildEffects::default()
        });
        let observed_order = Arc::new(Mutex::new(Vec::new()));
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let config: AppConfig<ChildStore, Agent> = AppConfig::new(
            ChildStore {
                inner: store.clone(),
                effects: Arc::clone(&effects),
            },
            Vec::new(),
            ChildDefinitions::all(
                Some(if observation.is_some() {
                    60_000
                } else {
                    12_000
                }),
                2,
            ),
        );
        let mut app = tokio::time::timeout(
            Duration::from_secs(3),
            App::open(
                config
                    .with_state_typing(ChildTyping {
                        effects: Arc::clone(&effects),
                    })
                    .with_child_executor(ReverseExecutor {
                        observed: Arc::clone(&observed_order),
                        effects: Arc::clone(&effects),
                    }),
            ),
        )
        .await
        .expect("verification fixture opens within its bound")
        .unwrap();
        let checked =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(if observation.is_some() { 36 } else { 10 }), async {
                let workspace = app
                    .create_workspace(WorkspaceConfig {
                        name: None,
                        root: std::env::current_dir().unwrap(),
                    })
                    .await
                    .unwrap();
                let workflow = workspace
                    .workflow("executor-parent".parse().unwrap())
                    .await
                    .unwrap();
                let run = workflow.start(ExecutionOptions::default()).await.unwrap();
                // No public read can consume the selected fault before the original
                // executor has returned both genuine completions to App.
                tokio::time::timeout(
                    Duration::from_secs(if shared_budget { 23 } else { 3 }),
                    effects.verification_read_entered.notified(),
                )
                .await
                .expect("verification reads the terminal child after the original E return");
                assert_eq!(effects.executions.load(Ordering::SeqCst), 1);
                assert_eq!(effects.execution_returns.load(Ordering::SeqCst), 1);
                assert_eq!(effects.faults.load(Ordering::SeqCst), 1);
                assert!(!effects.verification_read_returned.load(Ordering::SeqCst));
                let order = observed_order.lock().unwrap().clone();
                assert_eq!(
                    order,
                    [1, 0],
                    "the original executor returned both original completions"
                );
                let records = ChildDefinitions::admitted_records(&store, &run.id().to_string()).await;
                assert_eq!(records.len(), 2);
                for (index, record) in records.iter().enumerate() {
                    assert_eq!(record["parent"]["index"], json!(index));
                    assert_eq!(record["checkpoint"]["phase"]["state"], "succeeded");
                    assert_eq!(
                        record["checkpoint"]["state"]["item"],
                        json!(if index == 0 { 11 } else { 21 })
                    );
                }
                let fault_key = effects.fault_key.lock().unwrap().clone().unwrap();
                if let Some((boundary, _)) = observation {
                    let selected = effects.faulted_record.lock().unwrap().clone().unwrap();
                    match boundary {
                        CompletionReadBoundary::Child { shared_budget } => {
                            assert_eq!(selected, records[usize::from(shared_budget)]);
                        }
                        CompletionReadBoundary::Parent { .. }
                        | CompletionReadBoundary::ParentFailure => {
                            assert_eq!(selected["id"], run.id().to_string());
                            assert_eq!(selected["checkpoint"]["state"]["collected"], json!([]));
                            assert!(selected["checkpoint"]["position"].is_null());
                            assert!(selected["checkpoint"]["outcomes"].as_array().unwrap().is_empty());
                        }
                    }
                } else {
                    assert_eq!(fault_key, StateKey::new("app-kit", format!(
                        "run/{}/executor-child/{}/{}", workspace.id(), records[0]["id"].as_str().unwrap(),
                        records[0]["occurrence"].as_str().unwrap(),
                    )).unwrap());
                }
                let before = run.snapshot().await.unwrap();
                assert_eq!(before.state.values()["collected"], json!([]));
                assert!(
                    before.outcomes.is_empty() && before.last_action_ordinal.is_none(),
                    "the parent cannot publish child results before their verification receipt"
                );
                let values = effects.values.lock().unwrap().clone();
                assert_eq!(values, [20, 10]);
                if !held { effects.verification_read_release.notify_one(); }
                if fatal {
                    tokio::time::timeout(Duration::from_secs(if held {
                        if shared_budget { 13 } else { 33 }
                    } else { 3 }), async {
                        loop {
                            match app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await {
                                Err(AppError::Closed(_)) => break,
                                Ok(_) => tokio::task::yield_now().await,
                                Err(error) => panic!("unexpected verification closure observation: {error:?}"),
                            }
                        }
                        assert!(matches!(run.snapshot().await, Err(AppError::Closed(_))));
                        assert!(matches!(run.wait().await, Err(AppError::Closed(_))));
                    }).await.expect("Permanent verification failure closes without consuming the original results");
                    assert_eq!(effects.verification_read_returned.load(Ordering::SeqCst), !held);
                    if observation.is_some() {
                        let dropped = effects.held_execution_read.lock().unwrap().take().unwrap();
                        assert!(tokio::time::timeout(Duration::from_secs(1), dropped).await
                            .expect("the original verification read is destroyed before fatal closure").is_err());
                    }
                    assert_eq!(effects.faults.load(Ordering::SeqCst), 1);
                    assert_eq!(effects.executions.load(Ordering::SeqCst), 1);
                    assert_eq!(effects.execution_returns.load(Ordering::SeqCst), 1);
                    let current = ChildDefinitions::current_records(&store).await;
                    let parent = current.iter().find(|record| record["id"] == run.id().to_string())
                        .expect("original parent remains durable recovery evidence");
                    assert_eq!(parent["checkpoint"]["state"]["collected"], json!([]));
                    assert_eq!(parent["checkpoint"]["position"], json!(null));
                    assert_eq!(parent["checkpoint"]["outcomes"], json!([]));
                    if matches!(observation, Some((CompletionReadBoundary::Parent { .. }, _))) {
                        let original = effects.faulted_record.lock().unwrap().clone().unwrap();
                        assert_eq!(parent, &original, "fatal parent observation preserves the exact original native authority");
                    }
                    let children = ChildDefinitions::admitted_records(&store, &run.id().to_string()).await;
                    assert_eq!(children, records, "fatal verification retains both original completed child records");
                    return;
                }
                let complete = tokio::time::timeout(Duration::from_secs(3), run.wait())
                    .await
                    .expect("the original completions survive a transient verification read")
                    .unwrap();
                assert!(matches!(complete.status, RunStatus::Succeeded));
                assert_eq!(complete.state.values()["collected"], json!([11, 21]));
                assert_eq!(complete.last_action_ordinal, Some(0));
                assert_eq!(complete.outcomes.len(), 1);
                assert_eq!(complete.outcomes[0].node, "map");
                assert!(
                    effects.verification_read_returned.load(Ordering::SeqCst),
                    "the selected native read returned its real Unavailable failure"
                );
                if observation.is_none() {
                    assert!(effects.verification_read_retried.load(Ordering::SeqCst),
                        "the same selected child key is successfully observed after that failure");
                }
                assert_eq!(
                    effects.executions.load(Ordering::SeqCst),
                    1,
                    "verification retry must retain completions rather than invoke E again"
                );
                assert_eq!(effects.execution_returns.load(Ordering::SeqCst), 1);
                let values = effects.values.lock().unwrap().clone();
                assert_eq!(
                    values,
                    [20, 10],
                    "neither original child mutation is replayed"
                );
                let order = observed_order.lock().unwrap().clone();
                assert_eq!(order, [1, 0]);
            }))
            .catch_unwind()
            .await;
        effects.read_fault_armed.store(false, Ordering::SeqCst);
        effects
            .verification_read_pending
            .store(false, Ordering::SeqCst);
        effects.verification_read_release.notify_one();
        let cleanup = std::panic::AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        ))
        .catch_unwind()
        .await;
        drop(app);
        let released = tokio::time::timeout(Duration::from_secs(3), async {
            while effects.executor_drops.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        match checked {
            Ok(result) => result.expect("verification retry proof remains bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        let cleanup = cleanup
            .expect("verification cleanup does not panic")
            .expect("verification cleanup is bounded");
        if fatal {
            assert!(
                matches!(cleanup, Err(AppError::Integration(_))),
                "fatal verification retains its original typed failure"
            );
            assert_eq!(effects.executions.load(Ordering::SeqCst), 1);
            assert_eq!(effects.execution_returns.load(Ordering::SeqCst), 1);
            let values = effects.values.lock().unwrap().clone();
            assert_eq!(
                values,
                [20, 10],
                "joined cleanup cannot replay completed children"
            );
        } else {
            cleanup.unwrap();
        }
        released.expect("the original executor owner is released");
        assert_eq!(effects.execution_drops.load(Ordering::SeqCst), 1);
        assert_eq!(effects.executor_drops.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn transient_completion_verification_read_retains_original_executor_results() {
    ChildDefinitions::assert_completion_verification_read(ChildFault::None).await;
}

#[tokio::test]
async fn permanent_completion_verification_read_closes_with_original_executor_results() {
    ChildDefinitions::assert_completion_verification_read(ChildFault::PermanentRead).await;
}

#[tokio::test]
async fn actionable_input_and_cancelled_next_retain_the_same_caller_source_and_child_batch() {
    let caller = std::thread::current().id();
    let effects = Arc::new(ChildEffects {
        hold_source_item: true,
        ..ChildEffects::default()
    });
    let evidence = Arc::new(IntegrityEvidence::default());
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let definitions = ChildDefinitions::mixed(true);
    let config: AppConfig<ChildStore, Agent> = AppConfig::new(
        ChildStore {
            inner: store,
            effects: Arc::clone(&effects),
        },
        Vec::new(),
        definitions,
    );
    let mut app = App::open(
        config
            .with_state_typing(ChildTyping {
                effects: Arc::clone(&effects),
            })
            .with_hooks(ChildTyping {
                effects: Arc::clone(&effects),
            })
            .with_actions(vec![IntegrityExecutor::new(
                IntegrityPolicy::HoldForeground,
                &evidence,
                &effects,
            )])
            .with_child_executor(IntegrityExecutor::new(
                IntegrityPolicy::HoldForeground,
                &evidence,
                &effects,
            )),
    )
    .await
    .unwrap();
    let observed = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(10), async {
        let workspace = app.create_workspace(WorkspaceConfig {
            name: None, root: std::env::current_dir().unwrap(),
        }).await.unwrap();
        let workflow = workspace.workflow("executor-parent".parse().unwrap()).await.unwrap();
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        let observer = workflow.open_run(RunSelector::Latest(run.id().clone())).await.unwrap();
        let mut execution = run.execution(ExecutionOptions::default()).await.unwrap();
        assert!(matches!(execution.next().await.unwrap(), Some(ExecutionEvent::Committed { outcomes, .. })
            if outcomes.len() == 1 && outcomes[0].node == "seed"));
        let base = observer.snapshot().await.unwrap();
        let mut next = Box::pin(execution.next());
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                biased;
                _ = async {
                    effects.source_item_entered.notified().await;
                    while !evidence.holding.load(Ordering::SeqCst) {
                        evidence.entered.notified().await;
                    }
                } => {}
                event = &mut next => panic!("next completed before the real item receipt and E hold: {}",
                    match event { Ok(None) => "none", Ok(Some(_)) => "event", Err(_) => "error" }),
            }
        }).await.expect("caller item and child executor both reach their original holds");
        drop(next);
        let held = observer.snapshot().await.unwrap();
        let [Interaction::Input { sequence, generation, message, .. }] = held.interactions.as_slice()
            else { panic!("the ready root Input remains actionable beside the two original local futures") };
        let target = (*sequence, *generation);
        assert_eq!(message, "Release the mixed wave");
        assert_eq!(held.state.values(), base.state.values());
        assert_eq!(held.last_action_ordinal, Some(0));
        assert!(held.outcomes == base.outcomes);
        assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
        assert_eq!(evidence.dropped.load(Ordering::SeqCst), 0);
        assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 0);
        let contexts = evidence.source_contexts.lock().unwrap().clone();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].node, "a_source");
        assert_eq!(contexts[0].checkpoint, base.checkpoint);
        assert_eq!(contexts[0].action_ordinal, 1);
        assert_eq!(contexts[0].mode, ActionMode::Asynchronous);
        let no_effects = effects.values.lock().unwrap().is_empty();
        assert!(no_effects);
        effects.source_item_released.store(true, Ordering::SeqCst);
        effects.source_item_release.notify_waiters();
        assert!(matches!(execution.next().await.unwrap(), Some(ExecutionEvent::Partial { node, result })
            if node == "a_source" && result == Map::from_iter([("progress".into(), json!("primed"))])));
        assert!(execution.next().await.unwrap().is_none(),
            "actionable None follows a fair pass without consuming merely Pending source or E");
        assert!(evidence.source_pending.load(Ordering::SeqCst));
        let constructions = evidence.source_contexts.lock().unwrap().len();
        assert_eq!(constructions, 1);
        assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 0);
        assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
        assert_eq!(evidence.dropped.load(Ordering::SeqCst), 0);
        let paused = observer.snapshot().await.unwrap();
        let [Interaction::Input { sequence, generation, .. }] = paused.interactions.as_slice()
            else { panic!("outward None preserves the original Input target") };
        assert_eq!((*sequence, *generation), target);
        assert_eq!(paused.state.values(), base.state.values());
        assert!(paused.outcomes == base.outcomes);
        observer.resolve(target.0, target.1, Resolution::Input(json!("accepted"))).await.unwrap();
        evidence.release_sources();
        evidence.release();
        assert!(matches!(execution.next().await.unwrap(), Some(ExecutionEvent::Committed { outcomes, .. })
            if outcomes.iter().map(|outcome| (outcome.node.as_str(), outcome.ordinal)).collect::<Vec<_>>()
                == [("a_source",1),("b_children",2),("z_input",3)]));
        let complete = execution.finish().await.unwrap();
        assert!(matches!(complete.status, RunStatus::Succeeded));
        assert!(complete.interactions.is_empty());
        assert_eq!(complete.state.values()["source"], json!(true));
        assert_eq!(complete.state.values()["answer"], json!("accepted"));
        assert_eq!(complete.state.values()["collected"], json!([11,21]));
        assert_eq!(complete.last_action_ordinal, Some(3));
        let constructions = evidence.source_contexts.lock().unwrap().len();
        assert_eq!(constructions, 1);
        assert_eq!(evidence.source_completed.load(Ordering::SeqCst), 1);
        assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 1);
        assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
        assert_eq!(evidence.dropped.load(Ordering::SeqCst), 1);
        let source_threads = evidence.source_threads.lock().unwrap().clone();
        assert!(source_threads.len() >= 4 && source_threads.iter().all(|thread| *thread == caller));
        let child_threads = evidence.threads.lock().unwrap().clone();
        assert!(child_threads.len() >= 3);
        assert_ne!(child_threads[0], caller);
        assert!(child_threads.iter().all(|thread| *thread == child_threads[0]));
        let mut values = effects.values.lock().unwrap().clone();
        values.sort();
        assert_eq!(values, [10,20]);
    })).catch_unwind().await;
    effects.source_item_released.store(true, Ordering::SeqCst);
    effects.source_item_release.notify_waiters();
    evidence.release_sources();
    evidence.release();
    let cleanup = std::panic::AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(6),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(4),
        }),
    ))
    .catch_unwind()
    .await;
    drop(app);
    match observed {
        Ok(result) => result.expect("mixed source/child/Input lifetime proof is bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    cleanup
        .expect("mixed cleanup does not panic")
        .expect("mixed cleanup joins")
        .unwrap();
    assert_eq!(evidence.executor_dropped.load(Ordering::SeqCst), 2);
}

impl ChildDefinitions {
    fn mixed(with_children: bool) -> Vec<WorkflowDefinition> {
        let mut definitions = Self::all(None, 2);
        let mut parent = json!({
        "schema":"app-kit.workflow/v2", "id":"executor-parent", "revision":"v1",
        "state":{"items":[10,20],"collected":[],"source":false,"answer":null},
        "required":["items","collected","source","answer"],
        "reducers":{"items":"reject_concurrent","collected":"collect",
            "source":"reject_concurrent","answer":"reject_concurrent"},
        "entry":"seed", "nodes":[
            {"id":"seed","step":{"kind":"state","mutations":[]}},
            {"id":"a_source","deadline_ms":12000,
                "step":{"kind":"action","action":"mixed-source","revision":"v1"}},
            {"id":"b_children","deadline_ms":12000,
                "step":{"kind":"map","workflow":"executor-child",
                    "items":{"kind":"state","path":"items"},"input_key":"item","max_parallel":2,
                    "reductions":[{"source":"item","target":"collected","reducer":"collect"}]}},
            {"id":"z_input","deadline_ms":12000,"step":{"kind":"input","target":"answer",
                "message":[{"kind":"text","text":"Release the mixed wave"}]}}
        ],
        "routes":{"seed":{"mode":"broadcast","branches":[
            {"target":{"kind":"node","id":"z_input"}},
            {"target":{"kind":"node","id":"b_children"}},
            {"target":{"kind":"node","id":"a_source"}}
        ]}}
        });
        if !with_children {
            parent["nodes"][2] = json!({"id":"b_sleep","deadline_ms":120000,
                "step":{"kind":"sleep","message":"unresolved timer","delay_ms":60000}});
            parent["routes"]["seed"]["branches"][1]["target"]["id"] = json!("b_sleep");
        }
        definitions[1] = WorkflowDefinition::parse(&parent.to_string()).unwrap();
        definitions
    }
}

#[tokio::test]
async fn source_eof_joins_the_original_child_receipt_without_waiting_on_pure_controls() {
    for with_children in [true, false] {
        let effects = Arc::new(ChildEffects::default());
        let evidence = Arc::new(IntegrityEvidence {
            source_eof: true,
            ..IntegrityEvidence::default()
        });
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let config: AppConfig<ChildStore, Agent> = AppConfig::new(
            ChildStore {
                inner: store,
                effects: Arc::clone(&effects),
            },
            Vec::new(),
            ChildDefinitions::mixed(with_children),
        );
        let mut app = App::open(
            config
                .with_state_typing(ChildTyping {
                    effects: Arc::clone(&effects),
                })
                .with_hooks(ChildTyping {
                    effects: Arc::clone(&effects),
                })
                .with_actions(vec![IntegrityExecutor::new(
                    IntegrityPolicy::HoldForeground,
                    &evidence,
                    &effects,
                )])
                .with_child_executor(IntegrityExecutor::new(
                    IntegrityPolicy::HoldForeground,
                    &evidence,
                    &effects,
                )),
        )
        .await
        .unwrap();
        let observed = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(12), async {
            let workspace = app.create_workspace(WorkspaceConfig { name: None, root: std::env::current_dir().unwrap() }).await.unwrap();
            let workflow = workspace.workflow("executor-parent".parse().unwrap()).await.unwrap();
            let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
            let observer = workflow.open_run(RunSelector::Latest(run.id().clone())).await.unwrap();
            let mut execution = run.execution(ExecutionOptions::default()).await.unwrap();
            assert!(matches!(execution.next().await.unwrap(), Some(ExecutionEvent::Committed { outcomes, .. })
                if outcomes.len() == 1 && outcomes[0].node == "seed"));
            let base = observer.snapshot().await.unwrap();
            assert!(matches!(execution.next().await.unwrap(), Some(ExecutionEvent::Partial { node, .. }) if node == "a_source"));
            if with_children {
                tokio::time::timeout(Duration::from_secs(3), async {
                    loop {
                        let entered = evidence.entered.notified();
                        tokio::pin!(entered);
                        entered.as_mut().enable();
                        if evidence.holding.load(Ordering::SeqCst) { break; }
                        tokio::select! {
                            event = execution.next() => {
                                assert!(event.unwrap().is_none(), "only the root Input may be actionable before EOF");
                                tokio::task::yield_now().await;
                            }
                            _ = &mut entered => {}
                        }
                    }
                }).await.expect("the original worker E is actually Pending before EOF");
                assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
                assert_eq!(evidence.dropped.load(Ordering::SeqCst), 0);
            }
            let admitted = observer.snapshot().await.unwrap();
            let [Interaction::Input { sequence, generation, message, schema, .. }] = admitted.interactions.as_slice()
                else { panic!("the original Input is admitted before the source returns EOF") };
            let target = (*sequence, *generation, message.clone(), schema.clone());
            assert_eq!(message, "Release the mixed wave");
            evidence.release_sources();
            let mut next = Box::pin(execution.next());
            if with_children {
                tokio::time::timeout(Duration::from_secs(3), async {
                    loop {
                        if evidence.source_eof_observed.load(Ordering::SeqCst) == 1
                            && evidence.source_dropped.load(Ordering::SeqCst) == 1 { break; }
                        tokio::select! {
                            _ = &mut next => panic!("sampled EOF cannot finish while the original E remains held"),
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                }).await.expect("the caller really returns EOF and is destroyed once");
                assert!(tokio::time::timeout(Duration::from_millis(100), &mut next).await.is_err(),
                    "the original child continuation still owns its unfinished receipt");
                let finishes = effects.step_outcomes.lock().unwrap().clone();
                assert!(!finishes.iter().any(|(node, _, _)| node == "b_children"),
                    "EOF cannot close an accepted child activation guard before its original E settles");
                let held = observer.snapshot().await.unwrap();
                assert_eq!(held.state.values(), base.state.values());
                assert!(held.outcomes == base.outcomes);
                assert_eq!(held.last_action_ordinal, Some(0));
                evidence.release();
            }
            assert!(tokio::time::timeout(Duration::from_secs(3), next).await
                .expect("EOF rollback does not require a public answer or sixty-second timer").unwrap().is_none());
            assert_eq!(evidence.source_eof_observed.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.source_completed.load(Ordering::SeqCst), 0);
            assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 1);
            let after = observer.snapshot().await.unwrap();
            let [Interaction::Input { sequence, generation, message, schema, .. }] = after.interactions.as_slice()
                else { panic!("the unresolved root Input remains actionable after EOF") };
            assert_eq!((*sequence, *generation, message.clone(), schema.clone()), target);
            assert_eq!(after.state.values(), base.state.values());
            assert!(after.outcomes == base.outcomes);
            assert_eq!(after.last_action_ordinal, Some(0));
            let complete = tokio::time::timeout(Duration::from_secs(3), execution.finish()).await
                .expect("finish consumes the already observed EOF without retry or waiting for Input").unwrap();
            assert_eq!(complete.state.values(), base.state.values());
            assert!(complete.outcomes == base.outcomes);
            assert_eq!(complete.last_action_ordinal, Some(0));
            assert!(!matches!(complete.status, RunStatus::Failed(_) | RunStatus::Cancelled | RunStatus::Succeeded));
            let [Interaction::Input { sequence, generation, message, schema, .. }] = complete.interactions.as_slice()
                else { panic!("EOF finish preserves the same public Input") };
            assert_eq!((*sequence, *generation, message.clone(), schema.clone()), target);
            let contexts = evidence.source_contexts.lock().unwrap().clone();
            assert_eq!(contexts.len(), 1);
            assert_eq!(contexts[0].checkpoint, base.checkpoint);
            assert_eq!(contexts[0].action_ordinal, 1);
            let finishes = effects.step_outcomes.lock().unwrap().clone();
            assert_eq!(finishes.iter().filter(|(node, ordinal, none)| node == "a_source" && *ordinal == 1 && *none).count(), 1);
            let hooks = effects.hook_events.lock().unwrap().clone();
            let child_ids = hooks.iter().filter(|(event, _, _)| *event == "registered").map(|(_, id, _)| id.clone()).collect::<Vec<_>>();
            let mut mutations = effects.values.lock().unwrap().clone();
            mutations.sort();
            if with_children {
                assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
                assert_eq!(evidence.dropped.load(Ordering::SeqCst), 1);
                assert_eq!(child_ids.len(), 2);
                assert_eq!(mutations, [10, 20]);
                assert_eq!(finishes.iter().filter(|(node, ordinal, none)| node == "b_children" && *ordinal == 2 && *none).count(), 1);
                let child_workflow = workspace.workflow("executor-child".parse().unwrap()).await.unwrap();
                let mut states = Vec::new();
                for id in child_ids {
                    let child = child_workflow.open_run(RunSelector::Latest(id.parse().unwrap())).await.unwrap().snapshot().await.unwrap();
                    assert!(matches!(child.status, RunStatus::Succeeded));
                    assert_eq!(child.last_action_ordinal, Some(0));
                    assert_eq!(child.outcomes.len(), 1);
                    states.push(child.state.values()["item"].as_i64().unwrap());
                }
                states.sort();
                assert_eq!(states, [11, 21]);
            } else {
                assert_eq!(evidence.calls.load(Ordering::SeqCst), 0);
                assert_eq!(evidence.dropped.load(Ordering::SeqCst), 0);
                assert!(child_ids.is_empty() && mutations.is_empty());
                assert!(hooks.iter().any(|(event, node, _)| *event == "start" && node == "b_sleep"),
                    "a real sixty-second Sleep reservation coexists with EOF and Input");
            }
        })).catch_unwind().await;
        effects.source_item_released.store(true, Ordering::SeqCst);
        effects.source_item_release.notify_waiters();
        evidence.release_sources();
        evidence.release();
        let cleanup = std::panic::AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(6),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(4),
            }),
        ))
        .catch_unwind()
        .await;
        drop(app);
        match observed {
            Ok(result) => result.expect("mixed EOF proof is bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup
            .expect("mixed EOF cleanup does not panic")
            .expect("mixed EOF cleanup joins")
            .unwrap();
        assert_eq!(evidence.executor_dropped.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn nested_mixed_groups_keep_the_original_worker_sources_and_executor_after_cancelled_next() {
    let effects = Arc::new(ChildEffects::default());
    let evidence = Arc::new(IntegrityEvidence {
        released: AtomicBool::new(true),
        ..IntegrityEvidence::default()
    });
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let mut definitions = ChildDefinitions::mixed(true);
    definitions[0] = WorkflowDefinition::parse(
        &json!({
            "schema":"app-kit.workflow/v2", "id":"executor-child", "revision":"v1",
            "state":{"item":0}, "required":["item"], "reducers":{"item":"reject_concurrent"},
            "entry":"add", "nodes":[
                {"id":"add", "step":{"kind":"state", "mutations":[
                    {"kind":"increment", "target":"item", "value":{"kind":"literal", "value":1}}
                ]}},
                {"id":"wait", "deadline_ms":15000, "step":{"kind":"input",
                    "message":[{"kind":"text", "text":"Release nested leaf"}]}}
            ],
            "routes":{"add":{"mode":"first", "branches":[{"target":{"kind":"node", "id":"wait"}}]}}
        })
        .to_string(),
    )
    .unwrap();
    definitions.push(WorkflowDefinition::parse(&json!({
        "schema":"app-kit.workflow/v2", "id":"nested-root", "revision":"v1",
        "state":{"items":[10,20],"collected":[]}, "required":["items","collected"],
        "reducers":{"items":"reject_concurrent","collected":"collect"}, "entry":"seed",
        "nodes":[
            {"id":"seed", "step":{"kind":"state", "mutations":[]}},
            {"id":"outer", "deadline_ms":15000, "step":{"kind":"map", "workflow":"executor-parent",
                "items":{"kind":"state", "path":"items"}, "input_key":"answer", "max_parallel":2,
                "reductions":[{"source":"answer", "target":"collected", "reducer":"collect"}]}}
        ],
        "routes":{"seed":{"mode":"first", "branches":[{"target":{"kind":"node", "id":"outer"}}]}}
    }).to_string()).unwrap());
    let config: AppConfig<ChildStore, Agent> = AppConfig::new(
        ChildStore {
            inner: store,
            effects: Arc::clone(&effects),
        },
        Vec::new(),
        definitions,
    );
    let mut app = App::open(
        config
            .with_state_typing(ChildTyping {
                effects: Arc::clone(&effects),
            })
            .with_hooks(ChildTyping {
                effects: Arc::clone(&effects),
            })
            .with_actions(vec![IntegrityExecutor::new(
                IntegrityPolicy::HoldForeground,
                &evidence,
                &effects,
            )])
            .with_child_executor(IntegrityExecutor::new(
                IntegrityPolicy::HoldForeground,
                &evidence,
                &effects,
            )),
    )
    .await
    .unwrap();
    let observed = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(12), async {
        let caller = std::thread::current().id();
        let workspace = app.create_workspace(WorkspaceConfig { name: None, root: std::env::current_dir().unwrap() }).await.unwrap();
        let workflow = workspace.workflow("nested-root".parse().unwrap()).await.unwrap();
        let middle_workflow = workspace.workflow("executor-parent".parse().unwrap()).await.unwrap();
        let leaf_workflow = workspace.workflow("executor-child".parse().unwrap()).await.unwrap();
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        let run_id = run.id().clone();
        let observer = workflow.open_run(RunSelector::Latest(run_id.clone())).await.unwrap();
        let mut execution = run.execution(ExecutionOptions::default()).await.unwrap();
        assert!(matches!(execution.next().await.unwrap(), Some(ExecutionEvent::Committed { outcomes, .. })
            if outcomes.len() == 1 && outcomes[0].node == "seed"));
        let base = observer.snapshot().await.unwrap();
        let mut next = Box::pin(execution.next());
        let gather = async {
            loop {
                let contexts = evidence.source_contexts.lock().unwrap().clone();
                let hooks = effects.hook_events.lock().unwrap().clone();
                let registered = hooks.iter().filter(|(event, _, _)| *event == "registered")
                    .map(|(_, id, _)| id.clone()).collect::<Vec<_>>();
                if evidence.calls.load(Ordering::SeqCst) == 3 && contexts.len() == 2
                    && registered.len() == 6 && hooks.iter().filter(|(event, node, _)| *event == "item" && node == "a_source").count() == 2
                {
                    assert_eq!(registered.iter().collect::<BTreeSet<_>>().len(), 6);
                    let mut middles = Vec::new();
                    for context in &contexts {
                        assert_eq!(context.workflow.to_string(), "executor-parent");
                        assert_eq!(context.parent.as_ref(), Some(&run_id));
                        assert_eq!(context.node, "a_source");
                        assert_eq!(context.action_ordinal, 1);
                        assert_eq!(context.mode, ActionMode::Asynchronous);
                        let middle = middle_workflow.open_run(RunSelector::Exact { id: context.run.clone(), occurrence: context.occurrence.clone() }).await.unwrap();
                        let saved = middle.snapshot().await.unwrap();
                        if let [Interaction::Input { sequence, generation, message, .. }] = saved.interactions.as_slice() {
                            assert_eq!(message, "Release the mixed wave");
                            assert_eq!(saved.last_action_ordinal, Some(0));
                            assert_eq!(saved.state.values()["source"], false);
                            assert_eq!(saved.state.values()["collected"], json!([]));
                            let seed = middle.checkpoint(CheckpointSelector::Action(0)).await.unwrap();
                            assert_eq!(seed.sequence, context.checkpoint);
                            middles.push((middle, *sequence, *generation, saved.state.values()["answer"].as_i64().unwrap()));
                        }
                    }
                    let middle_ids = contexts.iter().map(|context| context.run.to_string()).collect::<Vec<_>>();
                    assert_ne!(middle_ids[0], middle_ids[1]);
                    let mut leaves = Vec::new();
                    for id in registered.iter().filter(|id| !middle_ids.contains(id)) {
                        let leaf = leaf_workflow.open_run(RunSelector::Latest(id.parse().unwrap())).await.unwrap();
                        let saved = leaf.snapshot().await.unwrap();
                        if let [Interaction::Input { sequence, generation, message, .. }] = saved.interactions.as_slice() {
                            assert_eq!(message, "Release nested leaf");
                            assert_eq!(saved.last_action_ordinal, Some(0));
                            assert_eq!(saved.outcomes.len(), 1);
                            assert_eq!(saved.outcomes[0].node, "add");
                            leaves.push((leaf, *sequence, *generation, saved.state.values()["item"].as_i64().unwrap()));
                        }
                    }
                    if middles.len() == 2 && leaves.len() == 4 { break (middles, leaves); }
                }
                tokio::task::yield_now().await;
            }
        };
        let (middles, leaves) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                result = &mut next => panic!("nested child targets are not a root actionable boundary: {}", result.is_ok()),
                targets = gather => targets,
            }
        }).await.expect("both nested mixed waves and all four real leaf targets progress");
        assert_eq!(evidence.calls.load(Ordering::SeqCst), 3);
        assert_eq!(evidence.dropped.load(Ordering::SeqCst), 0);
        assert_eq!(evidence.source_completed.load(Ordering::SeqCst), 0);
        assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 0);
        assert!(evidence.source_pending.load(Ordering::SeqCst));
        let held = observer.snapshot().await.unwrap();
        assert!(held.interactions.is_empty());
        assert_eq!(held.state.values(), base.state.values());
        assert!(held.outcomes == base.outcomes);
        assert_eq!(held.last_action_ordinal, Some(0));
        drop(next);
        let mut mutations = effects.values.lock().unwrap().clone();
        mutations.sort();
        assert_eq!(mutations, [10,10,20,20]);
        for (leaf, sequence, generation, _) in leaves.iter().rev() {
            leaf.resolve(*sequence, *generation, Resolution::Input(json!(null))).await.unwrap();
        }
        for (middle, sequence, generation, item) in middles.iter().rev() {
            middle.resolve(*sequence, *generation, Resolution::Input(json!(format!("middle-{item}")))).await.unwrap();
        }
        tokio::time::timeout(Duration::from_secs(3), async {
            while evidence.dropped.load(Ordering::SeqCst) != 2 { tokio::task::yield_now().await; }
        }).await.expect("the original two nested executors settle while the outer executor retains its children");
        assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 0);
        assert!(tokio::time::timeout(Duration::from_millis(100), execution.next()).await.is_err(),
            "resumed observation keeps the same outer executor and two pending original sources");
        evidence.release_sources();
        let complete = execution.finish().await.unwrap();
        assert!(matches!(complete.status, RunStatus::Succeeded));
        assert_eq!(complete.state.values()["collected"], json!(["middle-10","middle-20"]));
        assert_eq!(complete.last_action_ordinal, Some(1));
        assert_eq!(complete.outcomes.len(), 1);
        assert_eq!(complete.outcomes[0].node, "outer");
        for (middle, _, _, item) in middles {
            let saved = middle.snapshot().await.unwrap();
            assert!(matches!(saved.status, RunStatus::Succeeded));
            assert_eq!(saved.last_action_ordinal, Some(3));
            assert_eq!(saved.state.values()["source"], true);
            assert_eq!(saved.state.values()["answer"], json!(format!("middle-{item}")));
            assert_eq!(saved.state.values()["collected"], json!([11,21]));
            assert_eq!(saved.outcomes.iter().map(|outcome| (outcome.node.as_str(), outcome.ordinal)).collect::<Vec<_>>(),
                [("a_source",1),("b_children",2),("z_input",3)]);
        }
        let mut leaf_values = Vec::new();
        for (leaf, _, _, item) in leaves {
            let saved = leaf.snapshot().await.unwrap();
            assert!(matches!(saved.status, RunStatus::Succeeded));
            assert_eq!(saved.last_action_ordinal, Some(1));
            assert_eq!(saved.state.values()["item"], item);
            leaf_values.push(item);
        }
        leaf_values.sort();
        assert_eq!(leaf_values, [11,11,21,21]);
        assert_eq!(evidence.calls.load(Ordering::SeqCst), 3);
        assert_eq!(evidence.dropped.load(Ordering::SeqCst), 3);
        let construction_count = evidence.source_contexts.lock().unwrap().len();
        assert_eq!(construction_count, 2);
        assert_eq!(evidence.source_completed.load(Ordering::SeqCst), 2);
        assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 2);
        let hooks = effects.hook_events.lock().unwrap().clone();
        let registered = hooks.iter().filter(|(event, _, _)| *event == "registered")
            .map(|(_, id, _)| id).collect::<Vec<_>>();
        let finished = hooks.iter().filter(|(event, _, _)| *event == "child_finished")
            .map(|(_, id, _)| id).collect::<Vec<_>>();
        assert_eq!(registered.len(), 6);
        assert_eq!(finished.len(), 6);
        assert_eq!(registered.iter().collect::<BTreeSet<_>>().len(), 6);
        assert_eq!(finished.iter().collect::<BTreeSet<_>>(), registered.iter().collect::<BTreeSet<_>>());
        let threads = evidence.threads.lock().unwrap().clone();
        assert!(threads.len() >= 9);
        assert_ne!(threads[0], caller);
        assert!(threads.iter().all(|thread| *thread == threads[0]));
        let source_threads = evidence.source_threads.lock().unwrap().clone();
        assert!(source_threads.len() >= 8 && source_threads.iter().all(|thread| *thread == threads[0]));
        let mut final_mutations = effects.values.lock().unwrap().clone();
        final_mutations.sort();
        assert_eq!(final_mutations, mutations, "nested leaf effects are never replayed");
    })).catch_unwind().await;
    effects.source_item_released.store(true, Ordering::SeqCst);
    effects.source_item_release.notify_waiters();
    evidence.release_sources();
    evidence.release();
    let cleanup = std::panic::AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(6),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(4),
        }),
    ))
    .catch_unwind()
    .await;
    drop(app);
    match observed {
        Ok(result) => result.expect("nested worker mixed lifetime proof is bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    cleanup
        .expect("nested cleanup does not panic")
        .expect("nested cleanup joins")
        .unwrap();
    assert_eq!(evidence.executor_dropped.load(Ordering::SeqCst), 2);
}

impl ChildDefinitions {
    async fn assert_stopping_authority_read(
        failure: AuthorityReadFault,
        boundary: StoppingReadBoundary,
    ) {
        let retired = matches!(boundary, StoppingReadBoundary::Parent { retired: true });
        let descendants = matches!(boundary, StoppingReadBoundary::Descendants { .. });
        let shared_budget = matches!(
            boundary,
            StoppingReadBoundary::Descendants {
                shared_budget: true
            }
        );
        let policy = if descendants {
            IntegrityPolicy::CancelCompleted
        } else {
            IntegrityPolicy::CancelParked
        };
        let effects = Arc::new(ChildEffects {
            fault: ChildFault::StoppingRead { boundary, failure },
            ..ChildEffects::default()
        });
        let evidence = Arc::new(IntegrityEvidence::default());
        let inner = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let config: AppConfig<ChildStore, Agent> = AppConfig::new(
            ChildStore {
                inner: inner.clone(),
                effects: Arc::clone(&effects),
            },
            Vec::new(),
            Self::integrity_definitions(policy),
        );
        let mut app = App::open(
            config
                .with_state_typing(ChildTyping {
                    effects: Arc::clone(&effects),
                })
                .with_child_executor(IntegrityExecutor::new(policy, &evidence, &effects)),
        )
        .await
        .unwrap();
        let checked = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(38), async {
            let workspace = app.create_workspace(WorkspaceConfig {
                name: None, root: std::env::current_dir().unwrap(),
            }).await.unwrap();
            let workflow = workspace.workflow("executor-parent".parse().unwrap()).await.unwrap();
            let run = workflow.start(ExecutionOptions::default()).await.unwrap();
            let parent = run.id().to_string();
            let original = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let children = Self::admitted_records(&inner, &parent).await;
                    if children.len() == 2
                        && children[0]["checkpoint"]["phase"]["state"] == "succeeded"
                        && if descendants {
                            children[1]["checkpoint"]["phase"]["state"] == "succeeded"
                                && evidence.holding.load(Ordering::SeqCst)
                        } else { ChildStore::has_single_control(&children[1], "input") }
                    { break children; }
                    tokio::task::yield_now().await;
                }
            }).await.expect("real sibling receipts precede independent parent cancellation");
            assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.dropped.load(Ordering::SeqCst), 0);
            let before = run.snapshot().await.unwrap();
            let key = if descendants {
                StateKey::new("app-kit", format!("run/{}/executor-child/{}/{}", workspace.id(),
                    original[usize::from(shared_budget)]["id"].as_str().unwrap(),
                    original[usize::from(shared_budget)]["occurrence"].as_str().unwrap())).unwrap()
            } else {
                StateKey::new("app-kit", format!("run/{}/executor-parent/{parent}/{}", workspace.id(), before.occurrence)).unwrap()
            };
            effects.verification_read_pending.store(shared_budget, Ordering::SeqCst);
            *effects.fault_key.lock().unwrap() = Some(key);
            effects.read_fault_armed.store(true, Ordering::SeqCst);
            run.cancel(before.checkpoint).await.expect("original native cancellation intent");
            let read_used = if retired {
                tokio::time::timeout(Duration::from_secs(3), async {
                    loop {
                        if effects.faults.load(Ordering::SeqCst) != 0 { break true; }
                        let records = Self::current_records(&inner).await;
                        let current = records.iter().find(|record| record["id"] == parent).unwrap();
                        if current["checkpoint"]["phase"]["state"] == "cancelled"
                            && effects.read_fault_armed.swap(false, Ordering::SeqCst)
                        { break false; }
                        tokio::task::yield_now().await;
                    }
                }).await.expect("original Stage receipt either completes cleanup or selects its later read")
            } else {
                tokio::time::timeout(Duration::from_secs(if shared_budget { 23 } else { 3 }), effects.decision_read_observed.notified())
                    .await.expect("actual stopping read follows native cancellation and original executor Drop");
                true
            };
            assert_eq!(effects.faults.load(Ordering::SeqCst), usize::from(read_used));
            assert_eq!(evidence.dropped.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
            if descendants {
                assert_eq!(effects.faulted_record.lock().unwrap().as_ref().unwrap(),
                    &original[usize::from(shared_budget)], "exact completed child authority precedes parent reduction");
                if shared_budget {
                    assert!(!effects.verification_read_pending.load(Ordering::SeqCst));
                }
            }
            if retired && read_used {
                let selected = effects.faulted_record.lock().unwrap().clone().unwrap();
                let parent = &selected;
                assert_eq!(parent["checkpoint"]["phase"]["state"], "cancelling");
                assert!(parent["checkpoint"]["controls"].as_object().unwrap().is_empty(),
                    "the selected read follows original descendant reservation retirement");
                assert_eq!(parent["checkpoint"]["staged"].as_array().unwrap().len(), 1);
                let children = Self::admitted_records(&inner, parent["id"].as_str().unwrap()).await;
                assert_eq!(children[0]["checkpoint"]["phase"]["state"], "succeeded");
                assert_eq!(children[1]["checkpoint"]["phase"]["state"], "cancelled");
            }
            if !read_used || matches!(failure, AuthorityReadFault::Recoverable) {
                let stopped = tokio::time::timeout(Duration::from_secs(3), run.wait()).await
                    .expect("recoverable stopping read retains original cancellation cleanup").unwrap();
                assert!(matches!(stopped.status, RunStatus::Cancelled));
                assert_eq!(stopped.state.values()["collected"], json!([]));
                assert!(stopped.outcomes.is_empty() && stopped.last_action_ordinal.is_none());
                let children = Self::admitted_records(&inner, &parent).await;
                assert_eq!(children[0]["checkpoint"]["phase"]["state"], "succeeded");
                assert_eq!(children[1]["checkpoint"]["phase"]["state"], if descendants { "succeeded" } else { "cancelled" });
                app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await.unwrap();
            } else {
                tokio::time::timeout(Duration::from_secs(if shared_budget { 13 } else if matches!(failure, AuthorityReadFault::Held) { 33 } else { 3 }), async {
                    loop {
                        match app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await {
                            Err(AppError::Closed(_)) => break,
                            Ok(_) => tokio::task::yield_now().await,
                            Err(error) => panic!("unexpected stopping authority disposition: {error}"),
                        }
                    }
                }).await.expect("unreadable stopping authority has a bounded fatal disposition");
                if matches!(failure, AuthorityReadFault::Held) {
                    let mut receipt = effects.held_execution_read.lock().unwrap().take().unwrap();
                    assert!(matches!(receipt.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Closed)),
                        "original held read is destroyed before fatal closure is observable");
                }
                assert!(matches!(run.wait().await, Err(AppError::Closed(_))));
                if descendants {
                    assert_eq!(Self::admitted_records(&inner, &parent).await, original,
                        "unreadable cleanup cannot mutate or replay completed children");
                }
                let records = Self::current_records(&inner).await;
                let parent = records.iter().find(|record| record["id"] == parent).unwrap();
                assert_eq!(parent["checkpoint"]["state"]["collected"], json!([]));
                assert_eq!(parent["checkpoint"]["position"], json!(null));
                assert_eq!(parent["checkpoint"]["outcomes"], json!([]));
            }
            let mut values = effects.values.lock().unwrap().clone();
            values.sort();
            assert_eq!(values, [10, 20], "stopping read recovery cannot replay either child effect");
            assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.dropped.load(Ordering::SeqCst), 1);
        })).catch_unwind().await;
        effects.read_fault_armed.store(false, Ordering::SeqCst);
        effects.verification_read_release.notify_one();
        let closed = tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        )
        .await;
        drop(app);
        match checked {
            Ok(result) => result.expect("stopping authority proof is bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        let closed = closed.expect("original stopping owners join");
        if matches!(failure, AuthorityReadFault::Recoverable)
            || retired && effects.faults.load(Ordering::SeqCst) == 0
        {
            closed.unwrap();
        } else {
            assert!(matches!(closed, Err(AppError::Integration(_))));
        }
    }
}

#[tokio::test]
async fn recoverable_stopping_read_retains_cancelled_parent_and_original_child_ownership() {
    ChildDefinitions::assert_stopping_authority_read(
        AuthorityReadFault::Recoverable,
        StoppingReadBoundary::Parent { retired: false },
    )
    .await;
}

#[tokio::test]
async fn permanent_stopping_read_closes_without_replaying_admitted_child_effects() {
    ChildDefinitions::assert_stopping_authority_read(
        AuthorityReadFault::Permanent,
        StoppingReadBoundary::Parent { retired: false },
    )
    .await;
}

#[tokio::test]
async fn held_stopping_read_expires_and_drops_before_fatal_closure() {
    ChildDefinitions::assert_stopping_authority_read(
        AuthorityReadFault::Held,
        StoppingReadBoundary::Parent { retired: false },
    )
    .await;
}

#[tokio::test]
async fn terminal_failure_cannot_erase_accepted_child_predecessor_authority() {
    ChildDefinitions::assert_parent_checkpoint_receipt(ParentCheckpointCase::InvalidTerminalChild)
        .await;
}

#[tokio::test]
async fn recoverable_retired_control_read_preserves_original_parent_cancellation() {
    ChildDefinitions::assert_stopping_authority_read(
        AuthorityReadFault::Recoverable,
        StoppingReadBoundary::Parent { retired: true },
    )
    .await;
}

#[tokio::test]
async fn retired_control_cleanup_keeps_receipts_or_rejects_unreadable_authority() {
    ChildDefinitions::assert_stopping_authority_read(
        AuthorityReadFault::Permanent,
        StoppingReadBoundary::Parent { retired: true },
    )
    .await;
}

#[tokio::test]
async fn retired_control_cleanup_preserves_receipts_when_a_later_read_would_hang() {
    ChildDefinitions::assert_stopping_authority_read(
        AuthorityReadFault::Held,
        StoppingReadBoundary::Parent { retired: true },
    )
    .await;
}

impl ChildDefinitions {
    async fn assert_child_inventory_read(failure: AuthorityReadFault, shared_budget: bool) {
        let effects = Arc::new(ChildEffects {
            fault: ChildFault::ChildInventoryRead {
                failure,
                shared_budget,
            },
            hold_parent_checkpoint: AtomicBool::new(true),
            ..ChildEffects::default()
        });
        let evidence = Arc::new(IntegrityEvidence::default());
        let inner = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let config: AppConfig<ChildStore, Agent> = AppConfig::new(
            ChildStore {
                inner: inner.clone(),
                effects: Arc::clone(&effects),
            },
            Vec::new(),
            Self::all(None, 2),
        );
        let mut app = App::open(
            config
                .with_state_typing(ChildTyping {
                    effects: Arc::clone(&effects),
                })
                .with_child_executor(IntegrityExecutor::new(
                    IntegrityPolicy::CancelParked,
                    &evidence,
                    &effects,
                )),
        )
        .await
        .unwrap();
        let checked = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(44), async {
            let workspace = app.create_workspace(WorkspaceConfig {
                name: None, root: std::env::current_dir().unwrap(),
            }).await.unwrap();
            let workflow = workspace.workflow("executor-parent".parse().unwrap()).await.unwrap();
            let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
            let parent = run.id().clone();
            let mut execution = run.execution(ExecutionOptions::default()).await.unwrap();
            let mut next = Box::pin(execution.next());
            tokio::time::timeout(Duration::from_secs(4), async {
                tokio::select! {
                    _ = effects.parent_checkpoint_entered.notified() => {},
                    result = &mut next => panic!("original child admission receipt remains held: {}", result.is_ok()),
                }
            }).await.expect("real complete child admission commits before observer loss");
            let original = Self::admitted_records(&inner, &parent.to_string()).await;
            assert_eq!(original.len(), 2);
            assert_eq!(evidence.calls.load(Ordering::SeqCst), 0);
            assert!(effects.values.lock().unwrap().is_empty());
            drop(next);
            drop(execution);
            // The accepted admission may drive inventory immediately after
            // its original observer drops. Arm before releasing that receipt.
            *effects.fault_key.lock().unwrap() = Some(StateKey::new("app-kit", format!(
                "run/{}/executor-child/{}/{}", workspace.id(), original[usize::from(shared_budget)]["id"].as_str().unwrap(),
                original[usize::from(shared_budget)]["occurrence"].as_str().unwrap(),
            )).unwrap());
            effects.decision_read_remaining.store(usize::from(shared_budget), Ordering::SeqCst);
            effects.read_fault_armed.store(true, Ordering::SeqCst);
            effects.parent_checkpoint_release.notify_one();
            tokio::time::timeout(Duration::from_secs(if shared_budget { 23 } else { 3 }), effects.decision_read_observed.notified())
                .await.expect("original admitted work observes its actual child inventory");
            assert_eq!(effects.faults.load(Ordering::SeqCst), 1);
            let selected = effects.faulted_record.lock().unwrap().clone().unwrap();
            assert_eq!(selected, original[usize::from(shared_budget)], "fault follows real admission and precedes every child effect");
            if shared_budget {
                assert_eq!(effects.decision_read_remaining.load(Ordering::SeqCst), 0,
                    "the first actual valid child read spends the original inventory budget");
            }
            if matches!(failure, AuthorityReadFault::Recoverable) {
                let admission_deadline = tokio::time::Instant::now() + Duration::from_secs(4);
                let result = loop {
                    let mut observer = workflow.open_run(RunSelector::Latest(parent.clone())).await.unwrap();
                    let execution = match observer.execution(ExecutionOptions::default()).await {
                        Ok(execution) => execution,
                        Err(AppError::Busy(_)) => {
                            assert!(tokio::time::Instant::now() < admission_deadline,
                                "original accepted work retains ownership until explicit handoff");
                            tokio::task::yield_now().await;
                            continue;
                        }
                        Err(error) => panic!("explicit parent recovery was rejected: {error}"),
                    };
                    let records = Self::current_records(&inner).await;
                    let retained = records.iter().find(|record| record["id"] == parent.to_string()).unwrap();
                    assert_eq!(retained["checkpoint"]["state"]["collected"], json!([]));
                    assert_eq!(retained["checkpoint"]["position"], json!(null));
                    assert_eq!(retained["checkpoint"]["outcomes"], json!([]),
                        "owning Drop preserves admitted children without publishing a later parent barrier");
                    break tokio::time::timeout(Duration::from_secs(4), execution.finish()).await
                        .expect("the original admitted children finish within their retained execution")
                        .expect("recoverable child inventory preserves the original admitted work");
                };
                assert!(matches!(result.status, RunStatus::Succeeded));
                assert_eq!(result.state.values()["collected"], json!([11,21]));
                assert_eq!(result.outcomes.len(), 1);
                assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
                assert_eq!(evidence.dropped.load(Ordering::SeqCst), 1);
                let mut values = effects.values.lock().unwrap().clone();
                values.sort();
                assert_eq!(values, [10,20]);
                app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await.unwrap();
            } else {
                tokio::time::timeout(Duration::from_secs(if shared_budget { 13 }
                        else if matches!(failure, AuthorityReadFault::Held) { 33 } else { 3 }), async {
                    loop {
                        match app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await {
                            Err(AppError::Closed(_)) => break,
                            Ok(_) => tokio::task::yield_now().await,
                            Err(error) => panic!("unexpected child inventory read disposition: {error}"),
                        }
                    }
                }).await.expect("unreadable inventory closes before admitting any child effect");
                if matches!(failure, AuthorityReadFault::Held) {
                    let mut receipt = effects.held_execution_read.lock().unwrap().take().unwrap();
                    assert!(matches!(receipt.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Closed)),
                        "original inventory read is destroyed before fatal closure is observable");
                }
                assert_eq!(evidence.calls.load(Ordering::SeqCst), 0);
                assert!(effects.values.lock().unwrap().is_empty());
                assert_eq!(Self::admitted_records(&inner, &parent.to_string()).await, original);
            }
        })).catch_unwind().await;
        effects.read_fault_armed.store(false, Ordering::SeqCst);
        effects
            .hold_parent_checkpoint
            .store(false, Ordering::SeqCst);
        effects.parent_checkpoint_release.notify_one();
        effects.verification_read_release.notify_one();
        let closed = tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        )
        .await;
        drop(app);
        match checked {
            Ok(result) => result.expect("child inventory read proof is bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        let closed = closed.expect("original child admission and inventory owners join");
        if matches!(failure, AuthorityReadFault::Recoverable) {
            closed.unwrap();
        } else {
            assert!(matches!(closed, Err(AppError::Integration(_))));
        }
    }
}

#[tokio::test]
async fn recoverable_child_inventory_read_preserves_original_execution_and_effects_once() {
    ChildDefinitions::assert_child_inventory_read(AuthorityReadFault::Recoverable, false).await;
}

#[tokio::test]
async fn permanent_child_inventory_read_closes_before_constructing_the_executor() {
    ChildDefinitions::assert_child_inventory_read(AuthorityReadFault::Permanent, false).await;
}

#[tokio::test]
async fn held_child_inventory_read_expires_and_drops_before_any_child_effect() {
    ChildDefinitions::assert_child_inventory_read(AuthorityReadFault::Held, false).await;
}

#[tokio::test]
async fn child_inventory_reads_share_the_original_budget_across_siblings() {
    ChildDefinitions::assert_child_inventory_read(AuthorityReadFault::Held, true).await;
}

#[tokio::test]
async fn recoverable_descendant_cleanup_read_keeps_original_terminal_children() {
    ChildDefinitions::assert_stopping_authority_read(
        AuthorityReadFault::Recoverable,
        StoppingReadBoundary::Descendants {
            shared_budget: false,
        },
    )
    .await;
}

#[tokio::test]
async fn permanent_descendant_cleanup_read_closes_without_reducing_the_parent() {
    ChildDefinitions::assert_stopping_authority_read(
        AuthorityReadFault::Permanent,
        StoppingReadBoundary::Descendants {
            shared_budget: false,
        },
    )
    .await;
}

#[tokio::test]
async fn held_descendant_cleanup_read_drops_before_fatal_closure() {
    ChildDefinitions::assert_stopping_authority_read(
        AuthorityReadFault::Held,
        StoppingReadBoundary::Descendants {
            shared_budget: false,
        },
    )
    .await;
}

#[tokio::test]
async fn descendant_cleanup_reads_share_one_original_scan_budget() {
    ChildDefinitions::assert_stopping_authority_read(
        AuthorityReadFault::Held,
        StoppingReadBoundary::Descendants {
            shared_budget: true,
        },
    )
    .await;
}

#[derive(Clone, Copy)]
enum QueuedParentCase {
    ObserverDropped,
    Unreadable {
        failure: AuthorityReadFault,
        shared_budget: bool,
    },
    Expired,
}

impl ChildDefinitions {
    async fn assert_queued_parent_authority(case: QueuedParentCase) {
        let shared_budget = matches!(
            case,
            QueuedParentCase::Unreadable {
                shared_budget: true,
                ..
            }
        );
        let effects = Arc::new(ChildEffects {
            fault: match case {
                QueuedParentCase::Unreadable {
                    failure,
                    shared_budget,
                } => ChildFault::QueuedParentRead {
                    failure,
                    shared_budget,
                },
                QueuedParentCase::Expired | QueuedParentCase::ObserverDropped => ChildFault::None,
            },
            ..ChildEffects::default()
        });
        let (release_worker, hold_worker) = std::sync::mpsc::channel();
        let evidence = Arc::new(IntegrityEvidence {
            construction_gate: Mutex::new(Some(hold_worker)),
            ..IntegrityEvidence::default()
        });
        let mut definitions = Self::all(Some(60000), 2);
        let target = json!({
            "schema":"app-kit.workflow/v2", "id":"executor-parent", "revision":"v1",
            "state":{"items":[10,20],"collected":[],"source":false},
            "required":["items","collected","source"],
            "reducers":{"items":"reject_concurrent","collected":"collect","source":"reject_concurrent"},
            "entry":"seed", "nodes":[
                {"id":"seed","step":{"kind":"state","mutations":[]}},
                {"id":"a_source","deadline_ms":60000,
                    "step":{"kind":"action","action":"mixed-source","revision":"v1"}},
                {"id":"b_children","deadline_ms":if matches!(case, QueuedParentCase::Expired) { 2000 } else { 60000 },"step":{"kind":"map","workflow":"executor-child",
                    "items":{"kind":"state","path":"items"},"input_key":"item","max_parallel":2,
                    "reductions":[{"source":"item","target":"collected","reducer":"collect"}]}}
            ],
            "routes":{"seed":{"mode":"broadcast","branches":[
                {"target":{"kind":"node","id":"a_source"}},
                {"target":{"kind":"node","id":"b_children"}}
            ]}}
        });
        definitions[1] = WorkflowDefinition::parse(&target.to_string()).unwrap();
        definitions.push(WorkflowDefinition::parse(&json!({
            "schema":"app-kit.workflow/v2", "id":"queue-blocker", "revision":"v1",
            "state":{"items":[30,40],"collected":[]}, "required":["items","collected"],
            "reducers":{"items":"reject_concurrent","collected":"collect"},
            "entry":"map", "nodes":[{"id":"map","deadline_ms":60000,"step":{
                "kind":"map","workflow":"executor-child","items":{"kind":"state","path":"items"},
                "input_key":"item","max_parallel":2,
                "reductions":[{"source":"item","target":"collected","reducer":"collect"}]
            }}], "routes":{}
        }).to_string()).unwrap());
        let inner = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let config: AppConfig<ChildStore, Agent> = AppConfig::new(
            ChildStore {
                inner: inner.clone(),
                effects: Arc::clone(&effects),
            },
            Vec::new(),
            definitions,
        );
        let mut app = App::open(
            config
                .with_state_typing(ChildTyping {
                    effects: Arc::clone(&effects),
                })
                .with_hooks(ChildTyping {
                    effects: Arc::clone(&effects),
                })
                .with_actions(vec![IntegrityExecutor::new(
                    IntegrityPolicy::CancelParked,
                    &evidence,
                    &effects,
                )])
                .with_child_executor(IntegrityExecutor::new(
                    IntegrityPolicy::CancelParked,
                    &evidence,
                    &effects,
                )),
        )
        .await
        .unwrap();
        let checked = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(40), async {
            let workspace = app.create_workspace(WorkspaceConfig {
                name: None, root: std::env::current_dir().unwrap(),
            }).await.unwrap();
            if matches!(case, QueuedParentCase::ObserverDropped) {
                let blocker = workspace.workflow("queue-blocker".parse().unwrap()).await.unwrap()
                    .start(ExecutionOptions::default()).await.unwrap();
                tokio::time::timeout(Duration::from_secs(3), evidence.entered.notified()).await
                    .expect("the real child executor occupies the sole worker before root admission");
                let workflow = workspace.workflow("executor-child".parse().unwrap()).await.unwrap();
                let run = workflow.start(ExecutionOptions {
                    state: WorkflowState::new(Map::from_iter([("item".into(), json!(10))])).unwrap(),
                    ..ExecutionOptions::default()
                }).await.unwrap();
                let admitted = run.snapshot().await.unwrap();
                assert_eq!(admitted.checkpoint, 0);
                assert_eq!(admitted.state.values()["item"], 10);
                assert!(!effects.values.lock().unwrap().contains(&10), "the accepted driver is still queued");
                let key = StateKey::new("app-kit", format!("run/{}/executor-child/{}/{}",
                    workspace.id(), admitted.id, admitted.occurrence)).unwrap();
                drop(run);
                drop(workflow);
                // No public run handle or execution owner remains. Observe the
                // real native terminal commit before reopening can attach anything.
                release_worker.send(()).unwrap();
                let committed = tokio::time::timeout(Duration::from_secs(3), async {
                    loop {
                        let head = inner.load(key.clone()).await.unwrap().unwrap();
                        let current = ChildStore::current_record(&inner, &head).await.unwrap().unwrap();
                        if current.record.value.pointer("/checkpoint/phase/state") == Some(&json!("succeeded")) {
                            break current;
                        }
                        tokio::task::yield_now().await;
                    }
                }).await.expect("queued ownership survives loss of every public observer");
                assert_eq!(committed.record.value.pointer("/checkpoint/state/item"), Some(&json!(11)));
                assert_eq!(effects.values.lock().unwrap().iter().filter(|item| **item == 10).count(), 1);
                let workflow = workspace.workflow("executor-child".parse().unwrap()).await.unwrap();
                let observer = workflow.open_run(RunSelector::Exact {
                    id: admitted.id, occurrence: admitted.occurrence,
                }).await.unwrap();
                let retained = observer.wait().await.unwrap();
                assert!(matches!(retained.status, RunStatus::Succeeded));
                assert_eq!(retained.state.values()["item"], 11);
                assert_eq!(retained.checkpoint, committed.record.value["checkpoint"]["sequence"].as_u64().unwrap());
                assert_eq!(effects.values.lock().unwrap().iter().filter(|item| **item == 10).count(), 1,
                    "observing retained completion never replays the queued effect");
                assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
                drop(blocker);
                return;
            }
            let workflow = workspace.workflow("executor-parent".parse().unwrap()).await.unwrap();
            let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
            let parent = run.id().to_string();
            let mut execution = run.execution(ExecutionOptions::default()).await.unwrap();
            let blocker = workspace.workflow("queue-blocker".parse().unwrap()).await.unwrap()
                .start(ExecutionOptions::default()).await.unwrap();
            tokio::time::timeout(Duration::from_secs(3), evidence.entered.notified()).await
                .expect("the unrelated public executor constructor actually occupies the worker");
            assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
            assert!(matches!(execution.next().await.unwrap(), Some(ExecutionEvent::Committed { outcomes, .. })
                if outcomes.len() == 1 && outcomes[0].node == "seed"));
            assert!(matches!(execution.next().await.unwrap(), Some(ExecutionEvent::Partial { node, .. }) if node == "a_source"));
            // The original group is queued before App exposes the sibling
            // source's Partial. Neither E nor any child effect can run yet.
            assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.source_contexts.lock().unwrap().len(), 1);
            let original_children = Self::admitted_records(&inner, &parent).await;
            assert_eq!(original_children.len(), 2);
            assert!(original_children.iter().all(|child| child["checkpoint"]["sequence"] == 0));
            let head_key = StateKey::new("app-kit", format!("run/{}/executor-parent/{parent}/{}", workspace.id(), original_children[0]["parent"]["occurrence"].as_str().unwrap())).unwrap();
            let head = inner.load(head_key.clone()).await.unwrap().unwrap();
            let original = ChildStore::current_record(&inner, &head).await.unwrap().unwrap();
            assert!(ChildStore::has_single_control(&original.record.value, "child"));
            assert_eq!(original.record.value["checkpoint"]["state"]["collected"], json!([]));
            if let QueuedParentCase::Unreadable { failure, .. } = case {
                *effects.fault_key.lock().unwrap() = Some(if shared_budget { original.record.key.clone() } else { head_key });
                effects.verification_read_pending.store(shared_budget, Ordering::SeqCst);
                effects.read_fault_armed.store(true, Ordering::SeqCst);
                release_worker.send(()).unwrap();
                tokio::time::timeout(Duration::from_secs(if shared_budget { 23 } else { 3 }), effects.decision_read_observed.notified())
                    .await.expect("queued child work refreshes the original parent's authority before constructing E");
                assert_eq!(effects.faults.load(Ordering::SeqCst), 1);
                assert_eq!(effects.faulted_record.lock().unwrap().as_ref(), Some(original.record.value.as_ref()));
                assert_eq!(evidence.calls.load(Ordering::SeqCst), 1, "the selected parent's E is still unconstructed");
                if shared_budget { assert!(!effects.verification_read_pending.load(Ordering::SeqCst)); }
                tokio::time::timeout(Duration::from_secs(if shared_budget { 13 } else if matches!(failure, AuthorityReadFault::Held) { 33 } else { 3 }), async {
                    loop {
                        match app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await {
                            Err(AppError::Closed(_)) => break,
                            Ok(_) => tokio::task::yield_now().await,
                            Err(error) => panic!("unexpected queued authority disposition: {error}"),
                        }
                    }
                }).await.expect("unreadable queued authority closes within its original pure-read budget");
                if matches!(failure, AuthorityReadFault::Held) {
                    let mut receipt = effects.held_execution_read.lock().unwrap().take().unwrap();
                    assert!(matches!(receipt.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Closed)),
                        "the original queued read is destroyed before fatal closure is observable");
                }
                assert_eq!(Self::admitted_records(&inner, &parent).await, original_children,
                    "unreadable queued authority cannot execute or rewrite admitted children");
                drop(execution);
            } else {
                let controls = original.record.value["checkpoint"]["controls"].as_object().unwrap();
                let deadline = controls.values().next().unwrap()["deadline_millis"].as_u64().unwrap();
                let now = u64::try_from(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
                    .unwrap().as_millis()).unwrap();
                tokio::time::sleep(Duration::from_millis(deadline.saturating_sub(now) + 50)).await;
                release_worker.send(()).unwrap();
                let terminal = tokio::time::timeout(Duration::from_secs(3), execution.finish()).await
                    .expect("original queued activation expiry retains a bounded native timeout result").unwrap();
                assert!(matches!(terminal.status, RunStatus::Failed(WorkflowFailure::Timeout(_))),
                    "valid queued authority publishes the original activation timeout");
                assert_eq!(terminal.state.values()["collected"], json!([]));
                assert_eq!(terminal.state.values()["source"], false);
                assert_eq!(terminal.last_action_ordinal, Some(0));
                assert_eq!(terminal.outcomes.len(), 1);
                assert_eq!(terminal.outcomes[0].node, "seed");
                assert_eq!(effects.faults.load(Ordering::SeqCst), 0);
                let children = Self::admitted_records(&inner, &parent).await;
                assert_eq!(children.len(), original_children.len());
                for (child, before) in children.iter().zip(&original_children) {
                    assert_eq!(child["checkpoint"]["state"], before["checkpoint"]["state"]);
                    assert_eq!(child["checkpoint"]["position"], json!(null));
                    assert_eq!(child["checkpoint"]["outcomes"], json!([]));
                    assert_eq!(child["checkpoint"]["phase"]["state"], "cancelled");
                }
                app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await
                    .expect("ordinary queue expiry leaves the App usable");
            }
            let current = inner.load(original.record.key.clone()).await.unwrap().unwrap();
            assert_eq!(current, original, "the committed parent boundary remains immutable");
            assert!(!effects.values.lock().unwrap().iter().any(|item| *item == 10 || *item == 20));
            assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.source_completed.load(Ordering::SeqCst), 0);
            assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 1);
            drop(blocker);
        })).catch_unwind().await;
        // Never leave the unrelated synchronous constructor or selected read
        // held when a red assertion unwinds the retained execution owner.
        let _ = release_worker.send(());
        effects.read_fault_armed.store(false, Ordering::SeqCst);
        effects.verification_read_release.notify_one();
        evidence.release_sources();
        let closed = tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        )
        .await;
        drop(app);
        match checked {
            Ok(result) => result.expect("queued authority proof is bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        let closed = closed.expect("queued authority owners join");
        match case {
            QueuedParentCase::Unreadable { .. } => {
                assert!(matches!(closed, Err(AppError::Integration(_))))
            }
            QueuedParentCase::Expired | QueuedParentCase::ObserverDropped => closed.unwrap(),
        }
        assert_eq!(evidence.executor_dropped.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn permanent_queued_parent_read_closes_before_original_executor_construction() {
    ChildDefinitions::assert_queued_parent_authority(QueuedParentCase::Unreadable {
        failure: AuthorityReadFault::Permanent,
        shared_budget: false,
    })
    .await;
}

#[tokio::test]
async fn held_queued_parent_read_is_destroyed_before_fatal_closure() {
    ChildDefinitions::assert_queued_parent_authority(QueuedParentCase::Unreadable {
        failure: AuthorityReadFault::Held,
        shared_budget: false,
    })
    .await;
}

#[tokio::test]
async fn queued_parent_head_and_checkpoint_reads_share_one_original_budget() {
    ChildDefinitions::assert_queued_parent_authority(QueuedParentCase::Unreadable {
        failure: AuthorityReadFault::Held,
        shared_budget: true,
    })
    .await;
}

#[tokio::test]
async fn queued_activation_expiry_preserves_native_timeout_without_constructing_its_executor() {
    ChildDefinitions::assert_queued_parent_authority(QueuedParentCase::Expired).await;
}

#[tokio::test]
async fn successful_child_stage_observes_released_capacity_before_parent_reduction() {
    ChildDefinitions::assert_parent_checkpoint_receipt(ParentCheckpointCase::StageCapacity(
        ChildFault::StageReceipt,
    ))
    .await;
}

#[tokio::test]
async fn lost_child_stage_receipt_preserves_released_capacity_after_owning_drop() {
    ChildDefinitions::assert_parent_checkpoint_receipt(ParentCheckpointCase::StageCapacity(
        ChildFault::StageReceiptLoss,
    ))
    .await;
}

#[tokio::test]
async fn held_completion_child_read_expires_with_original_results() {
    ChildDefinitions::assert_completion_verification_read(ChildFault::CompletionRead {
        boundary: CompletionReadBoundary::Child {
            shared_budget: false,
        },
        failure: AuthorityReadFault::Held,
    })
    .await;
}

#[tokio::test]
async fn completion_child_scan_shares_one_original_read_budget() {
    ChildDefinitions::assert_completion_verification_read(ChildFault::CompletionRead {
        boundary: CompletionReadBoundary::Child {
            shared_budget: true,
        },
        failure: AuthorityReadFault::Held,
    })
    .await;
}

#[tokio::test]
async fn recoverable_completion_parent_read_retains_original_results() {
    ChildDefinitions::assert_completion_verification_read(ChildFault::CompletionRead {
        boundary: CompletionReadBoundary::Parent {
            shared_budget: false,
        },
        failure: AuthorityReadFault::Recoverable,
    })
    .await;
}

#[tokio::test]
async fn permanent_completion_parent_read_closes_with_original_results() {
    ChildDefinitions::assert_completion_verification_read(ChildFault::CompletionRead {
        boundary: CompletionReadBoundary::Parent {
            shared_budget: false,
        },
        failure: AuthorityReadFault::Permanent,
    })
    .await;
}

#[tokio::test]
async fn held_completion_parent_read_expires_with_original_results() {
    ChildDefinitions::assert_completion_verification_read(ChildFault::CompletionRead {
        boundary: CompletionReadBoundary::Parent {
            shared_budget: false,
        },
        failure: AuthorityReadFault::Held,
    })
    .await;
}

#[tokio::test]
async fn completion_child_and_parent_reads_share_one_original_budget() {
    ChildDefinitions::assert_completion_verification_read(ChildFault::CompletionRead {
        boundary: CompletionReadBoundary::Parent {
            shared_budget: true,
        },
        failure: AuthorityReadFault::Held,
    })
    .await;
}

#[tokio::test]
async fn readable_parent_observation_does_not_charge_a_valid_executor_wait_to_its_read_budget() {
    let effects = Arc::new(ChildEffects::default());
    let evidence = Arc::new(IntegrityEvidence::default());
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let config: AppConfig<ChildStore, Agent> = AppConfig::new(
        ChildStore {
            inner: store,
            effects: Arc::clone(&effects),
        },
        Vec::new(),
        ChildDefinitions::all(Some(60_000), 2),
    );
    let mut app = App::open(
        config
            .with_state_typing(ChildTyping {
                effects: Arc::clone(&effects),
            })
            .with_hooks(ChildTyping {
                effects: Arc::clone(&effects),
            })
            .with_child_executor(IntegrityExecutor::new(
                IntegrityPolicy::HoldForeground,
                &evidence,
                &effects,
            )),
    )
    .await
    .unwrap();
    let checked =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(39), async {
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().unwrap(),
                })
                .await
                .unwrap();
            let workflow = workspace
                .workflow("executor-parent".parse().unwrap())
                .await
                .unwrap();
            let run = workflow.start(ExecutionOptions::default()).await.unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let entered = evidence.entered.notified();
                    tokio::pin!(entered);
                    entered.as_mut().enable();
                    if evidence.holding.load(Ordering::SeqCst) {
                        break;
                    }
                    entered.await;
                }
            })
            .await
            .expect("the original accepted executor holds its unpolled native children");
            assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.dropped.load(Ordering::SeqCst), 0);
            assert!(effects.values.lock().unwrap().is_empty());
            let original = run.snapshot().await.unwrap();
            assert_eq!(original.state.values()["collected"], json!([]));
            assert!(original.outcomes.is_empty() && original.last_action_ordinal.is_none());
            // The accepted E is allowed to remain pending past one pure-read
            // ceiling while its original native activation deadline is still live.
            tokio::time::sleep(Duration::from_secs(31)).await;
            app.list_workspaces(None, NonZeroUsize::new(1).unwrap())
                .await
                .unwrap();
            let waiting = run.snapshot().await.unwrap();
            assert_eq!(waiting.checkpoint, original.checkpoint);
            assert_eq!(waiting.state, original.state);
            assert!(waiting.outcomes == original.outcomes);
            assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.dropped.load(Ordering::SeqCst), 0);
            assert!(effects.values.lock().unwrap().is_empty());
            evidence.release();
            let terminal = tokio::time::timeout(Duration::from_secs(3), run.wait())
                .await
                .unwrap()
                .unwrap();
            assert!(matches!(terminal.status, RunStatus::Succeeded));
            assert_eq!(terminal.state.values()["collected"], json!([11, 21]));
            assert_eq!(terminal.last_action_ordinal, Some(0));
            assert_eq!(terminal.outcomes.len(), 1);
            let mut values = effects.values.lock().unwrap().clone();
            values.sort();
            assert_eq!(values, [10, 20]);
            assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.dropped.load(Ordering::SeqCst), 1);
        }))
        .catch_unwind()
        .await;
    evidence.release();
    let cleanup = tokio::time::timeout(
        Duration::from_secs(5),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        }),
    )
    .await;
    drop(app);
    match checked {
        Ok(result) => result.expect("valid parent observation and executor wait remain bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    cleanup
        .expect("original executor and parent observers join")
        .unwrap();
    assert_eq!(evidence.executor_dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn recoverable_incomplete_child_result_authority_retains_original_child_failure() {
    ChildDefinitions::assert_executor_integrity(IntegrityPolicy::MissingRead {
        boundary: CompletionReadBoundary::Parent {
            shared_budget: false,
        },
        failure: AuthorityReadFault::Recoverable,
    })
    .await;
}

#[tokio::test]
async fn permanent_incomplete_child_result_authority_closes_without_reducing_children() {
    ChildDefinitions::assert_executor_integrity(IntegrityPolicy::MissingRead {
        boundary: CompletionReadBoundary::Parent {
            shared_budget: false,
        },
        failure: AuthorityReadFault::Permanent,
    })
    .await;
}

#[tokio::test]
async fn held_incomplete_child_result_authority_expires_without_reducing_children() {
    ChildDefinitions::assert_executor_integrity(IntegrityPolicy::MissingRead {
        boundary: CompletionReadBoundary::Parent {
            shared_budget: false,
        },
        failure: AuthorityReadFault::Held,
    })
    .await;
}

#[tokio::test]
async fn held_post_failure_child_authority_preserves_original_child_failure_and_completions() {
    ChildDefinitions::assert_executor_integrity(IntegrityPolicy::MissingRead {
        boundary: CompletionReadBoundary::ParentFailure,
        failure: AuthorityReadFault::Held,
    })
    .await;
}

#[tokio::test]
async fn unknown_child_admission_rejects_reordered_control_input_before_executor() {
    let effects = Arc::new(ChildEffects {
        ordered_admission: true,
        read_fault_armed: AtomicBool::new(true),
        ..ChildEffects::default()
    });
    let native = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let mut child: Value = serde_json::from_str(ChildDefinitions::CHILD).unwrap();
    child["state"]["marker"] = json!(0);
    child["reducers"]["marker"] = json!("reject_concurrent");
    let parent = json!({
        "schema":"app-kit.workflow/v2","id":"executor-parent","revision":"v1","entry":"map",
        "state":{"items":[10,20],"collected":[]},"required":["items","collected"],
        "reducers":{"items":"reject_concurrent","collected":"collect"},
        "nodes":[{"id":"map","deadline_ms":5000,"step":{"kind":"map","workflow":"executor-child",
            "items":{"kind":"state","path":"items"},"input_key":"item","max_parallel":2,
            "input":{"marker":{"kind":"literal","value":0}},
            "reductions":[{"source":"item","target":"collected","reducer":"collect"}]}}],"routes":{}
    });
    let config: AppConfig<ChildStore, Agent> = AppConfig::new(
        ChildStore {
            inner: native.clone(),
            effects: Arc::clone(&effects),
        },
        Vec::new(),
        vec![
            WorkflowDefinition::parse(&parent.to_string()).unwrap(),
            WorkflowDefinition::parse(&child.to_string()).unwrap(),
        ],
    );
    let mut app = App::open(
        config
            .with_state_typing(ChildTyping {
                effects: Arc::clone(&effects),
            })
            .with_child_executor(FaultExecutor {
                effects: Arc::clone(&effects),
            }),
    )
    .await
    .unwrap();
    let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(12), async {
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().unwrap(),
            })
            .await
            .unwrap();
        let workflow = workspace
            .workflow("executor-parent".parse().unwrap())
            .await
            .unwrap();
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        let execution = run.execution(ExecutionOptions::default()).await.unwrap();
        let observed = {
            let completion = execution.finish();
            tokio::pin!(completion);
            tokio::select! {
                result = &mut completion => Some(result),
                _ = effects.parent_checkpoint_entered.notified() => None,
            }
        };
        assert_eq!(
            effects.faults.load(Ordering::SeqCst),
            2,
            "actual child commit applied and its exact receipt read was obscured"
        );
        assert!(effects.ordered_readbacks.load(Ordering::SeqCst) > 0);
        let receipt_obscured = effects.ordered_receipt.lock().unwrap().is_none();
        assert!(receipt_obscured);
        let key = effects.fault_key.lock().unwrap().clone().unwrap();
        let expected = effects.faulted_record.lock().unwrap().clone().unwrap();
        let original = native.load(key.clone()).await.unwrap().unwrap();
        assert_eq!(original.revision.get(), 1);
        assert_eq!(
            serde_json::to_vec(original.record.value.as_ref()).unwrap(),
            serde_json::to_vec(&expected).unwrap(),
            "native input order remains original"
        );
        let control = expected["checkpoint"]["controls"]
            .as_object()
            .unwrap()
            .values()
            .next()
            .unwrap();
        assert_eq!(control["state"]["admitted"].as_array().unwrap().len(), 2);
        assert_eq!(control["state"]["inputs"][0]["input"]["item"], 10);
        assert_eq!(control["state"]["inputs"][1]["input"]["item"], 20);
        let children = ChildDefinitions::admitted_records(&native, &run.id().to_string()).await;
        assert_eq!(
            children.len(),
            2,
            "both native admission members really exist"
        );
        let repeated = native.load(key).await.unwrap().unwrap();
        assert_eq!(repeated.revision, original.revision);
        assert_eq!(
            serde_json::to_vec(repeated.record.value.as_ref()).unwrap(),
            serde_json::to_vec(original.record.value.as_ref()).unwrap()
        );
        let effects_observed = effects.values.lock().unwrap().clone();
        assert!(
            effects_observed.is_empty(),
            "contradictory admission cannot authorize child effects: {effects_observed:?}"
        );
        assert_eq!(
            effects.executions.load(Ordering::SeqCst),
            0,
            "reordered admitted input cannot authorize the public ChildExecutor"
        );
        assert!(
            matches!(
                observed,
                Some(Err(AppError::Integration(_)
                    | AppError::Conflict(_)
                    | AppError::Closed(_)))
            ),
            "original execution must reject contradictory admission without a timeout"
        );
    }))
    .catch_unwind()
    .await;
    effects.read_fault_armed.store(false, Ordering::SeqCst);
    effects.ordered_receipt.lock().unwrap().take();
    let cleanup = AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(12),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(10),
        }),
    ))
    .catch_unwind()
    .await;
    drop(app);
    match checked {
        Ok(result) => result.expect("ordered child admission observation is bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    assert!(
        matches!(
            cleanup
                .expect("cleanup does not panic")
                .expect("cleanup joins within its bound"),
            Err(AppError::Integration(_))
        ),
        "fatal child admission authority remains the shutdown result"
    );
    assert!(effects.values.lock().unwrap().is_empty());
    assert_eq!(effects.executions.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn queued_driver_survives_dropping_every_public_run_handle_without_replay() {
    ChildDefinitions::assert_queued_parent_authority(QueuedParentCase::ObserverDropped).await;
}
