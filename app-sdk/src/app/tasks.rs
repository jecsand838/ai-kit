use std::collections::HashMap;
use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, Weak};
use std::task::Poll;
use std::time::Duration;

use agent_sdk::AgentBackend;
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};
use storage_sdk::StateStoreBackend;
use tracing::instrument::WithSubscriber;

use crate::workflow::{
    ActionBackend, ChildExecutor, ConditionBackend, LifecycleHooks, StateTyping,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tokio::task::{AbortHandle, Id, JoinError, JoinHandle, JoinSet};

use crate::app::{AppRuntime, AppTrace, FinalResult, MAX_RUNS};
use crate::workflow::run::session::SessionReceipt;
use crate::workflow::run::{
    RunKey, RunRecord, RuntimeOperationFailure, ScheduleCause, WorkflowRunRuntime,
};
use crate::workspace::WorkspaceOperationIdentity;
use crate::{AppError, WorkspaceId};

pub(crate) struct AppTasks<
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend,
    C: ConditionBackend,
    T: StateTyping,
    H: LifecycleHooks,
    E: ChildExecutor,
> {
    state: Mutex<AppTasksState<CS, A, X, C, T, H, E>>,
    runtime: tokio::runtime::Handle,
    callers: Arc<Semaphore>,
    changed: watch::Sender<()>,
    supervisor: Mutex<Option<JoinHandle<()>>>,
}

struct AppTasksState<
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend,
    C: ConditionBackend,
    T: StateTyping,
    H: LifecycleHooks,
    E: ChildExecutor,
> {
    admission: TaskAdmission<TrackedTask<CS, A, X, C, T, H, E>>,
    tasks: JoinSet<FinalResult>,
    worker: Option<DriverWorker>,
}

enum TaskAdmission<Task> {
    Open(HashMap<Id, Task>),
    Draining(HashMap<Id, Task>),
    Closed(HashMap<Id, Task>),
}

impl<Task> TaskAdmission<Task> {
    fn owners(&self) -> &HashMap<Id, Task> {
        match self {
            Self::Open(owners) | Self::Draining(owners) | Self::Closed(owners) => owners,
        }
    }

    fn owners_mut(&mut self) -> &mut HashMap<Id, Task> {
        match self {
            Self::Open(owners) | Self::Draining(owners) | Self::Closed(owners) => owners,
        }
    }

    fn drain(&mut self) {
        if matches!(self, Self::Open(_)) {
            *self = Self::Draining(std::mem::take(self.owners_mut()));
        }
    }

    fn close(&mut self) {
        *self = Self::Closed(std::mem::take(self.owners_mut()));
    }
}

struct TrackedTask<
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend,
    C: ConditionBackend,
    T: StateTyping,
    H: LifecycleHooks,
    E: ChildExecutor,
> {
    owner: TaskOwner<CS, A, X, C, T, H, E>,
    dispatch: tracing::Dispatch,
    cancellation: TaskCancellation,
    admission: Option<OwnedSemaphorePermit>,
}

enum TaskCancellation {
    Tokio(AbortHandle),
    Driver(watch::Sender<bool>),
    WorkerJoin,
}

struct DriverWorker {
    commands: mpsc::Sender<DriverWork>,
    stop: watch::Sender<bool>,
}

struct DriverWork {
    key: RunKey,
    dispatch: tracing::Dispatch,
    start_after: Duration,
    command: DriverCommand,
    stop: watch::Receiver<bool>,
    completion: oneshot::Sender<FinalResult>,
}

pub(crate) enum DriverCommand {
    Scheduled,
    Child {
        sequence: u64,
        generation: Option<u64>,
        parent: Arc<RunRecord>,
        children: Vec<RunRecord>,
        receipt: oneshot::Sender<Result<SessionReceipt, RuntimeOperationFailure>>,
    },
}

enum TaskOwner<
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend,
    C: ConditionBackend,
    T: StateTyping,
    H: LifecycleHooks,
    E: ChildExecutor,
> {
    Workspace {
        workspace: WorkspaceId,
        identity: Arc<WorkspaceOperationIdentity>,
    },
    Run {
        runtime: WorkflowRunRuntime<CS, A, X, C, T, H, E>,
        kind: RunTaskKind,
    },
    Worker,
    Finalizer {
        runtime: Arc<AppRuntime<CS, A, X, C, T, H, E>>,
        completion: watch::Sender<Option<FinalResult>>,
        dropping: bool,
    },
}

#[derive(Clone, Copy)]
pub(crate) enum RunTaskKind {
    Driver,
    Operation,
    Control {
        sequence: u64,
    },
    Child {
        sequence: u64,
    },
    Resolution {
        control_sequence: u64,
        target_sequence: u64,
        generation: u64,
    },
    Foreground {
        generation: u64,
    },
    Reconciliation,
}

impl<
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend,
    C: ConditionBackend,
    T: StateTyping,
    H: LifecycleHooks,
    E: ChildExecutor,
> AppTasks<CS, A, X, C, T, H, E>
{
    pub(crate) fn new(runtime: tokio::runtime::Handle) -> Self {
        Self {
            state: Mutex::new(AppTasksState {
                admission: TaskAdmission::Open(HashMap::new()),
                tasks: JoinSet::new(),
                worker: None,
            }),
            runtime,
            callers: Arc::new(Semaphore::new(1_024)),
            changed: watch::channel(()).0,
            supervisor: Mutex::new(None),
        }
    }

    pub(crate) fn reserve_caller(&self) -> Result<OwnedSemaphorePermit, AppError> {
        Arc::clone(&self.callers).try_acquire_owned().map_err(|_| {
            AppError::Busy("application caller operation capacity is exhausted".into())
        })
    }

    pub(super) fn runtime(&self) -> &tokio::runtime::Handle {
        &self.runtime
    }

    pub(crate) fn start(self: &Arc<Self>, runtime: Weak<AppRuntime<CS, A, X, C, T, H, E>>) {
        let tasks = Arc::clone(self);
        let supervisor = self
            .runtime
            .spawn(async move { tasks.supervise(runtime).await }.with_current_subscriber());
        *self
            .supervisor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(supervisor);
    }

    pub(crate) fn spawn_workspace<F>(
        &self,
        workspace: WorkspaceId,
        identity: Arc<WorkspaceOperationIdentity>,
        admission: Option<OwnedSemaphorePermit>,
        future: F,
    ) -> bool
    where
        F: Future<Output = FinalResult> + Send + 'static,
    {
        self.spawn_owned(
            TaskOwner::Workspace {
                workspace,
                identity,
            },
            admission,
            future,
        )
    }

    pub(crate) fn spawn_run<F>(
        &self,
        runtime: WorkflowRunRuntime<CS, A, X, C, T, H, E>,
        kind: RunTaskKind,
        admission: Option<OwnedSemaphorePermit>,
        future: F,
    ) -> bool
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_owned(TaskOwner::Run { runtime, kind }, admission, async move {
            future.await;
            FinalResult::Succeeded
        })
    }

    pub(crate) fn spawn_driver(
        &self,
        runtime: Weak<AppRuntime<CS, A, X, C, T, H, E>>,
        key: RunKey,
        owner: WorkflowRunRuntime<CS, A, X, C, T, H, E>,
        start_after: Duration,
        command: DriverCommand,
    ) -> Result<bool, FinalResult> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !matches!(state.admission, TaskAdmission::Open(_)) {
            return Ok(false);
        }
        if let DriverCommand::Child { sequence, .. } = &command
            && state.admission.owners().values().any(|task| matches!(&task.owner,
                TaskOwner::Run { runtime: current, kind: RunTaskKind::Child { sequence: current_sequence } }
                    if current.key() == &key && current_sequence == sequence))
        {
            return Ok(false);
        }
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        if state.worker.is_none() {
            self.start_worker(&mut state, runtime)?;
        }
        let commands = state
            .worker
            .as_ref()
            .ok_or_else(|| FinalResult::Integration("workflow worker is unavailable".into()))?
            .commands
            .clone();
        let permit = commands.try_reserve_owned().map_err(|_| {
            FinalResult::Integration("workflow worker admission is unavailable".into())
        })?;
        // RunCell already owns the generation. Register its cancellation and
        // completion observer before making this intent visible to the worker.
        let (stop, stopped) = watch::channel(false);
        let (completion, completed) = oneshot::channel();
        let abort = state.tasks.spawn_on(
            async move {
                completed.await.unwrap_or_else(|_| {
                    FinalResult::Integration("workflow worker lost a driver result".into())
                })
            },
            &self.runtime,
        );
        state.admission.owners_mut().insert(
            abort.id(),
            TrackedTask {
                owner: TaskOwner::Run {
                    runtime: owner,
                    kind: match &command {
                        DriverCommand::Scheduled => RunTaskKind::Driver,
                        DriverCommand::Child { sequence, .. } => RunTaskKind::Child {
                            sequence: *sequence,
                        },
                    },
                },
                dispatch: dispatch.clone(),
                cancellation: TaskCancellation::Driver(stop),
                admission: None,
            },
        );
        permit.send(DriverWork {
            key,
            dispatch,
            start_after,
            command,
            stop: stopped,
            completion,
        });
        self.changed.send_replace(());
        Ok(true)
    }

    fn start_worker(
        &self,
        state: &mut AppTasksState<CS, A, X, C, T, H, E>,
        runtime: Weak<AppRuntime<CS, A, X, C, T, H, E>>,
    ) -> Result<(), FinalResult> {
        let (commands, receiver) = mpsc::channel(MAX_RUNS);
        let (stop, stopped) = watch::channel(false);
        // This control is installed before spawn, including the failed-spawn
        // path. No thread or callback can acquire an unregistered stop owner.
        state.worker = Some(DriverWorker { commands, stop });
        let handle = self.runtime.clone();
        let (finished, completion) = oneshot::channel();
        let spawned = std::thread::Builder::new()
            .name("app-sdk-execution".into())
            .spawn(move || {
                let outcome = catch_unwind(AssertUnwindSafe(|| {
                    handle.block_on(Self::drive_worker(receiver, stopped, runtime));
                }));
                let _ = finished.send(());
                match outcome {
                    Ok(()) => FinalResult::Succeeded,
                    Err(_) => FinalResult::Integration("workflow worker thread panicked".into()),
                }
            });
        let thread = match spawned {
            Ok(thread) => thread,
            Err(_) => {
                state.worker.take();
                return Err(FinalResult::Integration(
                    "workflow worker thread could not start".into(),
                ));
            }
        };
        let handle = self.runtime.clone();
        let abort = state.tasks.spawn_on(
            async move {
                // Waiting for exit uses no caller blocking-pool slot. Only the
                // finished thread join enters that pool, and its handle remains
                // owned by this non-abortable tracked completion observer.
                let _ = completion.await;
                match handle.spawn_blocking(move || thread.join()).await {
                    Ok(Ok(outcome)) => outcome,
                    Ok(Err(_)) | Err(_) => {
                        FinalResult::Integration("workflow worker join failed".into())
                    }
                }
            },
            &self.runtime,
        );
        state.admission.owners_mut().insert(
            abort.id(),
            TrackedTask {
                owner: TaskOwner::Worker,
                dispatch: tracing::dispatcher::get_default(Clone::clone),
                cancellation: TaskCancellation::WorkerJoin,
                admission: None,
            },
        );
        Ok(())
    }

    async fn drive_worker(
        mut commands: mpsc::Receiver<DriverWork>,
        mut stop: watch::Receiver<bool>,
        runtime: Weak<AppRuntime<CS, A, X, C, T, H, E>>,
    ) {
        let mut active = FuturesUnordered::new();
        let mut closing = false;
        loop {
            if closing && active.is_empty() {
                break;
            }
            tokio::select! {
                _ = stop.changed(), if !closing => {
                    closing = true;
                    commands.close();
                    while let Ok(command) = commands.try_recv() {
                        let _ = command.completion.send(FinalResult::Closed("workflow driver ownership was revoked".into()));
                    }
                }
                command = commands.recv(), if !closing && active.len() < MAX_RUNS => {
                    match command {
                        Some(command) => {
                            let dispatch = command.dispatch.clone();
                            active.push(Box::pin(Self::drive_on_worker(command, runtime.clone())
                                .with_subscriber(dispatch)));
                        },
                        None => closing = true,
                    }
                }
                _ = active.next(), if !active.is_empty() => {}
            }
        }
    }

    async fn drive_on_worker(work: DriverWork, runtime: Weak<AppRuntime<CS, A, X, C, T, H, E>>) {
        let DriverWork {
            key,
            dispatch: _,
            start_after,
            command,
            mut stop,
            completion,
        } = work;
        let result = if *stop.borrow() {
            FinalResult::Closed("workflow driver ownership was revoked".into())
        } else if let Some(app) = runtime.upgrade() {
            match WorkflowRunRuntime::attach(app, key.clone()) {
                Ok(runtime) => {
                    // Construct the same driver only here. Future state and
                    // caller-local values never cross the command channel.
                    let operation = match &command {
                        DriverCommand::Scheduled => "driver",
                        DriverCommand::Child { .. } => "child",
                    };
                    let span = AppTrace::span(|| {
                        tracing::info_span!(target: "app_sdk", "app.operation",
                        operation, run_id = %runtime.id(), occurrence = %runtime.occurrence())
                    });
                    let mut drive = span.observe(Box::pin(
                        AssertUnwindSafe(async move {
                            match command {
                                DriverCommand::Scheduled => {
                                    runtime.drive_scheduled(start_after).await
                                }
                                DriverCommand::Child {
                                    sequence,
                                    generation,
                                    parent,
                                    children,
                                    receipt,
                                } => {
                                    let result = runtime
                                        .execute_child_group(sequence, generation, parent, children)
                                        .await;
                                    let _ = receipt.send(result);
                                }
                            }
                        })
                        .catch_unwind(),
                    ));
                    tokio::select! {
                        biased;
                        _ = stop.changed() => FinalResult::Closed("workflow driver ownership was revoked".into()),
                        result = &mut drive => match result {
                            Ok(()) => FinalResult::Succeeded,
                            Err(_) => FinalResult::Integration("workflow driver task panicked".into()),
                        },
                    }
                }
                Err(error) => FinalResult::from(&error),
            }
        } else {
            FinalResult::Closed("workflow runtime is no longer available".into())
        };
        let _ = completion.send(result);
    }

    pub(crate) fn spawn_finalizer<F>(
        &self,
        runtime: Arc<AppRuntime<CS, A, X, C, T, H, E>>,
        completion: watch::Sender<Option<FinalResult>>,
        dropping: bool,
        future: F,
    ) -> bool
    where
        F: Future<Output = FinalResult> + Send + 'static,
    {
        self.spawn_owned(
            TaskOwner::Finalizer {
                runtime,
                completion,
                dropping,
            },
            None,
            future,
        )
    }

    pub(crate) fn has_run_work(&self, selected: Option<&RunKey>) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .admission
            .owners()
            .values()
            .any(|task| {
                matches!(&task.owner, TaskOwner::Run { runtime, .. }
                if selected.is_none_or(|selected| selected == runtime.key()))
            })
    }

    pub(crate) fn has_control_work(&self, selected: &RunKey, sequence: u64) -> bool {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).admission.owners().values()
            .any(|task| matches!(&task.owner, TaskOwner::Run { runtime, kind: RunTaskKind::Control { sequence: owned } }
                if runtime.key() == selected && *owned == sequence))
    }

    pub(crate) fn retarget_control(
        &self,
        id: Id,
        selected: &RunKey,
        prior: u64,
        next: u64,
    ) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.admission.owners().iter().any(|(current_id, task)| *current_id != id
            && matches!(&task.owner, TaskOwner::Run { runtime, kind: RunTaskKind::Control { sequence } }
                if runtime.key() == selected && *sequence == next))
        {
            return false;
        }
        let Some(task) = state.admission.owners_mut().get_mut(&id) else {
            return false;
        };
        match &mut task.owner {
            TaskOwner::Run {
                runtime,
                kind: RunTaskKind::Control { sequence },
            } if runtime.key() == selected && *sequence == prior => {
                *sequence = next;
                self.changed.send_replace(());
                true
            }
            _ => false,
        }
    }

    pub(crate) fn has_child_work(&self, selected: &RunKey) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .admission
            .owners()
            .values()
            .any(|task| {
                matches!(&task.owner, TaskOwner::Run { runtime, kind: RunTaskKind::Child { .. } }
                if runtime.key() == selected)
            })
    }

    pub(crate) fn has_resolution_work(&self, selected: &RunKey, control: Option<u64>) -> bool {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
            .admission.owners().values().any(|task| matches!(&task.owner,
                TaskOwner::Run { runtime, kind: RunTaskKind::Resolution { control_sequence, .. } }
                    if runtime.key() == selected && control.is_none_or(|sequence| sequence == *control_sequence)
            ))
    }

    fn spawn_owned<F>(
        &self,
        owner: TaskOwner<CS, A, X, C, T, H, E>,
        admission: Option<OwnedSemaphorePermit>,
        future: F,
    ) -> bool
    where
        F: Future<Output = FinalResult> + Send + 'static,
    {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if matches!(state.admission, TaskAdmission::Closed(_))
            || (matches!(state.admission, TaskAdmission::Draining(_))
                && !matches!(owner, TaskOwner::Finalizer { .. }))
        {
            return false;
        }
        if let TaskOwner::Run {
            runtime,
            kind: RunTaskKind::Control { sequence },
        } = &owner && state.admission.owners().values().any(|task| {
            matches!(&task.owner,
                TaskOwner::Run { runtime: current, kind: RunTaskKind::Control { sequence: reserved } }
                    if current.key() == runtime.key() && reserved == sequence
            )
        }) {
            return false;
        }
        if let TaskOwner::Run { runtime, kind: RunTaskKind::Resolution { control_sequence, target_sequence, generation } } = &owner
            && state.admission.owners().values().any(|task| matches!(&task.owner,
                TaskOwner::Run { runtime: current, kind: RunTaskKind::Resolution {
                    control_sequence: current_control, target_sequence: current_target, generation: current_generation,
                } } if current.key() == runtime.key() && current_control == control_sequence && current_target == target_sequence && current_generation == generation
            ))
        {
            return false;
        }
        let operation = match &owner {
            TaskOwner::Workspace { .. } => "workspace",
            TaskOwner::Run { kind, .. } => match kind {
                RunTaskKind::Driver => "driver",
                RunTaskKind::Operation => "operation",
                RunTaskKind::Control { .. } => "control",
                RunTaskKind::Child { .. } => "child",
                RunTaskKind::Resolution { .. } => "resolution",
                RunTaskKind::Foreground { .. } => "foreground",
                RunTaskKind::Reconciliation => "reconciliation",
            },
            TaskOwner::Worker => "worker",
            TaskOwner::Finalizer { .. } => "finalizer",
        };
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        // Pin the concrete task once before wrapping it. Native publication
        // futures can be large; nested child polling must not copy that state
        // through the tracing and Tokio admission frames.
        let future = Box::pin(future);
        // Subscriber callbacks begin only on the accepted task's first poll,
        // after this admission lock has been released.
        let future = async move {
            AppTrace::span(|| tracing::info_span!(target: "app_sdk", "app.operation", operation))
                .observe(future)
                .await
        }
        .with_subscriber(dispatch.clone());
        let abort = state.tasks.spawn_on(future, &self.runtime);
        state.admission.owners_mut().insert(
            abort.id(),
            TrackedTask {
                owner,
                dispatch,
                cancellation: TaskCancellation::Tokio(abort),
                admission,
            },
        );
        self.changed.send_replace(());
        true
    }

    async fn supervise(self: Arc<Self>, runtime: Weak<AppRuntime<CS, A, X, C, T, H, E>>) {
        while let Some((tracked, result)) = self.next_exit().await {
            let TrackedTask {
                owner,
                dispatch,
                admission,
                ..
            } = tracked;
            // Completion can admit reconciliation or failure work. Scope this
            // synchronous handoff to the completed task's original caller,
            // rather than the subscriber which opened the App supervisor.
            tracing::dispatcher::with_default(&dispatch, || match owner {
                TaskOwner::Workspace {
                    workspace,
                    identity,
                } => {
                    if let Some(runtime) = runtime.upgrade() {
                        let outcome = result.unwrap_or_else(|error| {
                            if error.is_panic() {
                                FinalResult::Integration(
                                    "workspace operation task panicked".to_owned(),
                                )
                            } else {
                                FinalResult::Closed(
                                    "workspace operation ownership was revoked".to_owned(),
                                )
                            }
                        });
                        runtime
                            .workspace_operations()
                            .finish(&workspace, &identity, outcome);
                        runtime.mark_runtime_changed();
                    }
                }
                TaskOwner::Run {
                    runtime: run, kind, ..
                } => {
                    let failed = match result {
                        Err(error) => error.is_panic(),
                        Ok(FinalResult::Integration(_)) => true,
                        Ok(_) => false,
                    };
                    let reconcile = matches!(
                        kind,
                        RunTaskKind::Control { .. }
                            | RunTaskKind::Child { .. }
                            | RunTaskKind::Resolution { .. }
                    );
                    if failed {
                        run.task_failed(kind);
                    } else if reconcile {
                        run.wake();
                        run.schedule(ScheduleCause::Reconcile);
                    }
                }
                TaskOwner::Worker => {
                    let failed = match result {
                        Ok(FinalResult::Succeeded) => None,
                        Ok(outcome) => Some(outcome),
                        Err(_) => Some(FinalResult::Integration(
                            "workflow worker observer failed".into(),
                        )),
                    };
                    if let Some(failure) = failed
                        && let Some(runtime) = runtime.upgrade()
                    {
                        runtime.tasks_failed(failure);
                    }
                }
                TaskOwner::Finalizer {
                    runtime,
                    completion,
                    dropping,
                } => match result {
                    Ok(result) => {
                        if dropping || runtime.finalization_closed() {
                            self.finish();
                        }
                        completion.send_replace(Some(result));
                    }
                    Err(_) => runtime.resume_finalizer(completion, dropping),
                },
            });

            // Capacity returns only after completion has handed off reconciliation.
            drop(admission);
            self.changed.send_replace(());
        }
    }

    async fn next_exit(
        &self,
    ) -> Option<(
        TrackedTask<CS, A, X, C, T, H, E>,
        Result<FinalResult, JoinError>,
    )> {
        let mut changes = self.changes();
        loop {
            tokio::select! {
                exit = poll_fn(|context| {
                    let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                    match state.tasks.poll_join_next_with_id(context) {
                        Poll::Ready(Some(result)) => {
                            let id = match &result {
                                Ok((id, _)) => *id,
                                Err(error) => error.id(),
                            };
                            let Some(tracked) = state.admission.owners_mut().remove(&id) else {
                                context.waker().wake_by_ref();
                                return Poll::Pending;
                            };
                            Poll::Ready(Some((tracked, result.map(|(_, result)| result))))
                        }
                        Poll::Ready(None) if matches!(state.admission, TaskAdmission::Closed(_)) => Poll::Ready(None),
                        Poll::Ready(None) | Poll::Pending => Poll::Pending,
                    }
                }) => return exit,
                _ = changes.changed() => {}
            }
        }
    }

    pub(crate) fn changes(&self) -> watch::Receiver<()> {
        self.changed.subscribe()
    }

    pub(crate) fn mark_changed(&self) {
        self.changed.send_replace(());
    }

    pub(crate) fn stop_runtime(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.admission.drain();
        if let Some(worker) = &state.worker {
            worker.stop.send_replace(true);
        }
        for tracked in state.admission.owners().values() {
            if !matches!(tracked.owner, TaskOwner::Finalizer { .. }) {
                match &tracked.cancellation {
                    TaskCancellation::Tokio(abort) => abort.abort(),
                    TaskCancellation::Driver(stop) => {
                        stop.send_replace(true);
                    }
                    TaskCancellation::WorkerJoin => {}
                }
            }
        }
        self.changed.send_replace(());
    }

    pub(crate) async fn close(&self) {
        let mut changes = self.changes();
        self.stop_runtime();
        loop {
            let drained = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .admission
                .owners()
                .values()
                .all(|tracked| matches!(tracked.owner, TaskOwner::Finalizer { .. }));
            if drained {
                return;
            }
            let _ = changes.changed().await;
        }
    }

    pub(crate) fn finish(&self) {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .admission
            .close();
        self.changed.send_replace(());
    }

    pub(crate) async fn join(&self) {
        let mut changes = self.changes();
        loop {
            tokio::select! {
                outcome = poll_fn(|context| {
                    let mut supervisor = self.supervisor.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                    let Some(handle) = supervisor.as_mut() else { return Poll::Ready(Ok(())); };
                    match std::pin::Pin::new(handle).poll(context) {
                        Poll::Ready(outcome) => {
                            supervisor.take();
                            self.changed.send_replace(());
                            Poll::Ready(outcome)
                        }
                        Poll::Pending => Poll::Pending,
                    }
                }) => {
                    if outcome.is_err() {
                        AppTrace::event(|| tracing::warn!(target: "app_sdk", event = "app.runtime.supervisor_failed"));
                    }
                    return;
                }
                _ = changes.changed() => {}
            }
        }
    }
}
