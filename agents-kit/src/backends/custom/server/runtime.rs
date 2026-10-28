use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::Poll;
use tools_kit::ToolsetBackend;

use providers_kit::ProviderBackend;
use tokio::sync::{Notify, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout_at};

use crate::backends::custom::adapters::{CustomProviderCall, CustomProviderRuntime};
use crate::{KitError, PromptResponse, WorkspaceId};
use storage_kit::{DatabaseBackend, StateStoreBackend};

pub(crate) struct CustomRoute<
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
> {
    workspace_id: WorkspaceId,
    admission: Mutex<CustomRouteAdmission>,
    runtime: CustomProviderRuntime<S, D, P, T>,
    cancellation: Mutex<Option<(u64, watch::Sender<bool>)>>,
    phase: Mutex<CustomRoutePhase>,
    task: Mutex<CustomRouteTask>,
    changed: Notify,
}

pub(crate) struct CustomProviderRun<
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
> {
    route: Arc<CustomRoute<S, D, P, T>>,
    generation: u64,
    state: CustomProviderRunState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CustomRouteAdmission {
    Open,
    Closing { deadline: Instant },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CustomRoutePhase {
    Idle,
    Waiting {
        generation: u64,
        cancellation: CustomRouteCancellation,
    },
    Running {
        generation: u64,
        registered: bool,
        cancellation: CustomRouteCancellation,
    },
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CustomRouteCancellation {
    None,
    Requested,
    Attempted { run_cancelled: bool },
}

enum CustomRouteTask {
    Idle,
    Running {
        generation: Option<u64>,
        handle: JoinHandle<()>,
        state: CustomRouteTaskState,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CustomRouteTaskState {
    Active,
    Acknowledged,
    Finished,
}

impl CustomRouteTask {
    fn is_abnormally_finished(&self) -> bool {
        match self {
            Self::Running { handle, state, .. } => {
                handle.is_finished() && *state != CustomRouteTaskState::Finished
            }
            Self::Idle => false,
        }
    }
}

enum CustomProviderRunState {
    Proposed(Pin<Box<dyn Future<Output = Result<PromptResponse, KitError>> + Send + 'static>>),
    Accepted(Pin<Box<dyn Future<Output = Result<PromptResponse, KitError>> + Send + 'static>>),
    Buffered(Option<Result<PromptResponse, KitError>>),
    Resolving,
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend>
    CustomRoute<S, D, P, T>
{
    pub(crate) fn new(
        workspace_id: WorkspaceId,
        runtime: CustomProviderRuntime<S, D, P, T>,
    ) -> Self {
        Self {
            workspace_id,
            admission: Mutex::new(CustomRouteAdmission::Open),
            runtime,
            cancellation: Mutex::new(None),
            phase: Mutex::new(CustomRoutePhase::Idle),
            task: Mutex::new(CustomRouteTask::Idle),
            changed: Notify::new(),
        }
    }

    pub(crate) fn spawn<F>(self: &Arc<Self>, future: F) -> Result<oneshot::Receiver<()>, KitError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let _admission = self.open_admission()?;
        let mut task = self.task();
        let previous = std::mem::replace(&mut *task, CustomRouteTask::Idle);
        match previous {
            CustomRouteTask::Idle => {}
            CustomRouteTask::Running {
                handle,
                state: CustomRouteTaskState::Finished,
                ..
            } => drop(handle),
            running @ CustomRouteTask::Running { .. } => {
                let error = if running.is_abnormally_finished() {
                    KitError::invalid_state("prompt task terminated unexpectedly")
                } else {
                    KitError::busy("conversation already has active work")
                };
                *task = running;
                return Err(error);
            }
        }
        let (start, ready) = oneshot::channel();
        let (complete, completion) = oneshot::channel();
        let route = Arc::clone(self);
        let handle = tokio::spawn(async move {
            if ready.await.is_ok() {
                future.await;
            }
            route.complete_task();
            let _ = complete.send(());
        });
        *task = CustomRouteTask::Running {
            generation: None,
            handle,
            state: CustomRouteTaskState::Active,
        };
        drop(task);
        start
            .send(())
            .map_err(|_| KitError::invalid_state("prompt task could not start"))?;
        Ok(completion)
    }

    pub(crate) fn bind_task(&self, generation: u64) -> Result<(), KitError> {
        if generation == 0 {
            return Err(KitError::invalid_state("prompt identity is invalid"));
        }
        let _admission = self.open_admission()?;
        {
            let mut task = self.task();
            match &mut *task {
                CustomRouteTask::Running {
                    generation: active @ None,
                    state: CustomRouteTaskState::Active,
                    ..
                } => *active = Some(generation),
                CustomRouteTask::Running {
                    generation: Some(active),
                    state: CustomRouteTaskState::Active,
                    ..
                } if *active == generation => {}
                CustomRouteTask::Running { .. } => {
                    return Err(KitError::conflict(
                        "prompt task changed before identity binding",
                    ));
                }
                CustomRouteTask::Idle => {
                    return Err(KitError::invalid_state("prompt task is not active"));
                }
            }
        }
        let mut phase = self.phase();
        match *phase {
            CustomRoutePhase::Idle => {
                let (cancellation, _receiver) = watch::channel(false);
                *self
                    .cancellation
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                    Some((generation, cancellation));
                *phase = CustomRoutePhase::Waiting {
                    generation,
                    cancellation: CustomRouteCancellation::None,
                };
                Ok(())
            }
            CustomRoutePhase::Waiting {
                generation: active, ..
            } if active == generation => Ok(()),
            CustomRoutePhase::Waiting { .. } | CustomRoutePhase::Running { .. } => {
                Err(KitError::busy("conversation already has active work"))
            }
            CustomRoutePhase::Closed => Err(KitError::closed("conversation execution is closed")),
        }
    }

    pub(crate) fn acknowledge_prompt(&self, generation: Option<u64>) -> Result<(), KitError> {
        let mut task = self.task();
        let phase = self.phase();
        if !matches!(*phase, CustomRoutePhase::Idle) {
            return Err(KitError::invalid_state(
                "prompt completed while conversation work remained active",
            ));
        }
        match &mut *task {
            CustomRouteTask::Running {
                generation: active,
                state,
                ..
            } if *active == generation && *state == CustomRouteTaskState::Active => {
                *state = CustomRouteTaskState::Acknowledged;
            }
            CustomRouteTask::Running {
                generation: active,
                state: CustomRouteTaskState::Acknowledged | CustomRouteTaskState::Finished,
                ..
            } if *active == generation => {}
            CustomRouteTask::Running { .. } => {
                return Err(KitError::conflict(
                    "prompt completion changed before delivery",
                ));
            }
            CustomRouteTask::Idle => {
                return Err(KitError::invalid_state(
                    "prompt completion has no active delivery owner",
                ));
            }
        }
        drop(phase);
        drop(task);
        self.changed.notify_waiters();
        Ok(())
    }

    pub(crate) async fn await_prompt_settlement(&self, deadline: Instant) -> Result<(), KitError> {
        loop {
            let mut changed = std::pin::pin!(self.changed.notified());
            changed.as_mut().enable();
            let should_wait = {
                let task = self.task();
                let phase = self.phase();
                if task.is_abnormally_finished() {
                    return Err(KitError::invalid_state(
                        "prompt task terminated unexpectedly",
                    ));
                }
                match (&*task, *phase) {
                    (CustomRouteTask::Idle, CustomRoutePhase::Idle | CustomRoutePhase::Closed)
                    | (
                        CustomRouteTask::Running {
                            state: CustomRouteTaskState::Finished,
                            ..
                        },
                        CustomRoutePhase::Idle | CustomRoutePhase::Closed,
                    ) => false,
                    (
                        CustomRouteTask::Running {
                            state: CustomRouteTaskState::Acknowledged,
                            ..
                        },
                        CustomRoutePhase::Idle,
                    ) => true,
                    (_, CustomRoutePhase::Waiting { .. } | CustomRoutePhase::Running { .. })
                    | (
                        CustomRouteTask::Running {
                            state: CustomRouteTaskState::Active,
                            ..
                        },
                        CustomRoutePhase::Idle,
                    ) => {
                        return Err(KitError::busy("conversation already has an active prompt"));
                    }
                    _ => {
                        return Err(KitError::invalid_state(
                            "prompt settlement state is inconsistent",
                        ));
                    }
                }
            };
            if !should_wait {
                return Ok(());
            }
            timeout_at(deadline, changed)
                .await
                .map_err(|_| KitError::timeout("prompt completion settlement timed out"))?;
        }
    }

    pub(crate) fn start(
        self: &Arc<Self>,
        generation: u64,
        call: CustomProviderCall,
    ) -> Result<CustomProviderRun<S, D, P, T>, KitError> {
        if generation == 0 {
            return Err(KitError::invalid_state("prompt identity is invalid"));
        }
        let _admission = self.open_admission()?;
        self.ensure_task(generation)?;
        let cancellation = self.cancellation_receiver(generation)?;
        {
            let mut phase = self.phase();
            match *phase {
                CustomRoutePhase::Waiting {
                    generation: active,
                    cancellation: CustomRouteCancellation::None,
                } if active == generation => {
                    *phase = CustomRoutePhase::Running {
                        generation,
                        registered: false,
                        cancellation: CustomRouteCancellation::None,
                    };
                }
                CustomRoutePhase::Waiting {
                    generation: active,
                    cancellation: CustomRouteCancellation::Requested,
                } if active == generation => {
                    *phase = CustomRoutePhase::Idle;
                    drop(phase);
                    self.changed.notify_waiters();
                    return Err(KitError::cancelled(
                        "prompt was cancelled before model acceptance",
                    ));
                }
                CustomRoutePhase::Waiting { .. } | CustomRoutePhase::Running { .. } => {
                    return Err(KitError::busy("conversation already has active work"));
                }
                CustomRoutePhase::Idle => {
                    return Err(KitError::invalid_state(
                        "prompt execution was not bound before submission",
                    ));
                }
                CustomRoutePhase::Closed => {
                    return Err(KitError::closed("conversation execution is closed"));
                }
            }
        }
        Ok(CustomProviderRun {
            route: Arc::clone(self),
            generation,
            state: CustomProviderRunState::Proposed(Box::pin(
                call.execute(self.runtime.clone(), cancellation),
            )),
        })
    }

    pub(crate) fn decline(&self, generation: u64) -> Result<(), KitError> {
        let mut phase = self.phase();
        match *phase {
            CustomRoutePhase::Waiting {
                generation: active, ..
            } if active == generation => {
                *phase = CustomRoutePhase::Idle;
                drop(phase);
                self.clear_cancellation(generation);
                self.changed.notify_waiters();
                Ok(())
            }
            CustomRoutePhase::Idle => Ok(()),
            CustomRoutePhase::Waiting { .. } => Err(KitError::conflict(
                "prompt identity changed before submission was declined",
            )),
            CustomRoutePhase::Running { .. } => Err(KitError::invalid_state(
                "model work began before submission was declined",
            )),
            CustomRoutePhase::Closed => Err(KitError::closed("conversation execution is closed")),
        }
    }

    pub(crate) fn cancel(&self, generation: u64) -> Result<bool, KitError> {
        let registered = {
            let mut phase = self.phase();
            match *phase {
                CustomRoutePhase::Idle => return Ok(false),
                CustomRoutePhase::Waiting {
                    generation: active,
                    cancellation,
                } if active == generation => {
                    if cancellation == CustomRouteCancellation::None {
                        *phase = CustomRoutePhase::Waiting {
                            generation: active,
                            cancellation: CustomRouteCancellation::Requested,
                        };
                    }
                    self.signal_cancellation(generation)?;
                    return Ok(false);
                }
                CustomRoutePhase::Running {
                    generation: active,
                    registered,
                    cancellation,
                } if active == generation => {
                    if cancellation == CustomRouteCancellation::None {
                        *phase = CustomRoutePhase::Running {
                            generation: active,
                            registered,
                            cancellation: CustomRouteCancellation::Requested,
                        };
                    }
                    registered
                }
                CustomRoutePhase::Waiting { .. } | CustomRoutePhase::Running { .. } => {
                    return Err(KitError::conflict(
                        "prompt identity changed before cancellation",
                    ));
                }
                CustomRoutePhase::Closed => {
                    return Err(KitError::closed("conversation execution is closed"));
                }
            }
        };
        if !registered {
            return Ok(false);
        }
        self.dispatch_cancellation(generation)
    }

    pub(crate) async fn shutdown(&self, deadline: Instant) -> Result<(), KitError> {
        self.seal(deadline);
        self.await_quiescence_before(deadline).await?;
        self.finish_quiescent_task().await?;
        self.close_phase();
        Ok(())
    }

    pub(super) async fn settle_shutdown(&self) -> Result<(), KitError> {
        self.seal(Instant::now());
        self.settle_quiescence().await?;
        self.finish_quiescent_task().await?;
        self.close_phase();
        Ok(())
    }

    async fn await_quiescence_before(&self, deadline: Instant) -> Result<(), KitError> {
        loop {
            let mut changed = std::pin::pin!(self.changed.notified());
            changed.as_mut().enable();
            let idle = {
                let phase = self.phase();
                match *phase {
                    CustomRoutePhase::Idle | CustomRoutePhase::Closed => true,
                    CustomRoutePhase::Waiting { .. } | CustomRoutePhase::Running { .. } => false,
                }
            };
            let quiescent = idle && self.task_quiescent();
            if quiescent {
                break;
            }
            if idle && self.task_finishing() {
                if Instant::now() >= deadline {
                    return Err(KitError::timeout("prompt task did not quiesce"));
                }
                tokio::task::yield_now().await;
                continue;
            }
            if timeout_at(deadline, changed).await.is_err() {
                return Err(KitError::timeout("conversation execution did not quiesce"));
            }
        }
        Ok(())
    }

    async fn settle_quiescence(&self) -> Result<(), KitError> {
        loop {
            let mut changed = std::pin::pin!(self.changed.notified());
            changed.as_mut().enable();
            let idle = {
                let phase = self.phase();
                match *phase {
                    CustomRoutePhase::Idle | CustomRoutePhase::Closed => true,
                    CustomRoutePhase::Waiting { .. } | CustomRoutePhase::Running { .. } => false,
                }
            };
            let quiescent = idle && self.task_quiescent();
            if quiescent {
                return Ok(());
            }
            if idle && self.task_finishing() {
                tokio::task::yield_now().await;
            } else {
                changed.await;
            }
        }
    }

    async fn finish_quiescent_task(&self) -> Result<(), KitError> {
        if let CustomRouteTask::Running { handle, state, .. } = self.take_task() {
            debug_assert!(handle.is_finished());
            match handle.await {
                Ok(()) if state == CustomRouteTaskState::Finished => {}
                Ok(()) | Err(_) => {
                    return Err(KitError::invalid_state(
                        "prompt task terminated unexpectedly",
                    ));
                }
            }
        }
        Ok(())
    }

    fn close_phase(&self) {
        *self.phase() = CustomRoutePhase::Closed;
        self.cancellation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        self.changed.notify_waiters();
    }

    fn finish(&self, generation: u64) {
        let mut phase = self.phase();
        if matches!(
            *phase,
            CustomRoutePhase::Waiting {
                generation: active,
                ..
            } | CustomRoutePhase::Running {
                generation: active,
                ..
            } if active == generation
        ) {
            *phase = CustomRoutePhase::Idle;
        }
        drop(phase);
        self.clear_cancellation(generation);
        self.changed.notify_waiters();
    }

    fn register(&self, generation: u64) -> Result<(), KitError> {
        let cancellation_requested = {
            let mut phase = self.phase();
            match *phase {
                CustomRoutePhase::Running {
                    generation: active,
                    registered: false,
                    cancellation,
                } if active == generation => {
                    *phase = CustomRoutePhase::Running {
                        generation: active,
                        registered: true,
                        cancellation,
                    };
                    cancellation == CustomRouteCancellation::Requested
                }
                CustomRoutePhase::Running {
                    generation: active,
                    registered: true,
                    ..
                } if active == generation => return Ok(()),
                CustomRoutePhase::Running { .. } => {
                    return Err(KitError::conflict(
                        "prompt identity changed during execution registration",
                    ));
                }
                CustomRoutePhase::Waiting { .. } => {
                    return Err(KitError::invalid_state(
                        "prompt execution was not submitted before registration",
                    ));
                }
                CustomRoutePhase::Idle => {
                    return Err(KitError::invalid_state(
                        "prompt execution settled before registration",
                    ));
                }
                CustomRoutePhase::Closed => {
                    return Err(KitError::closed("conversation execution is closed"));
                }
            }
        };
        if cancellation_requested {
            let _ = self.dispatch_cancellation(generation)?;
        }
        Ok(())
    }

    fn dispatch_cancellation(&self, generation: u64) -> Result<bool, KitError> {
        let mut phase = self.phase();
        match *phase {
            CustomRoutePhase::Running {
                generation: active,
                registered: true,
                cancellation: CustomRouteCancellation::Requested,
            } if active == generation => {
                self.signal_cancellation(generation)?;
                *phase = CustomRoutePhase::Running {
                    generation,
                    registered: true,
                    cancellation: CustomRouteCancellation::Attempted {
                        run_cancelled: true,
                    },
                };
                drop(phase);
                self.changed.notify_waiters();
                Ok(true)
            }
            CustomRoutePhase::Running {
                generation: active,
                cancellation: CustomRouteCancellation::Attempted { run_cancelled },
                ..
            } if active == generation => Ok(run_cancelled),
            CustomRoutePhase::Running {
                generation: active,
                registered: false,
                ..
            } if active == generation => Ok(false),
            CustomRoutePhase::Running { .. } => Err(KitError::conflict(
                "prompt identity changed before cancellation",
            )),
            CustomRoutePhase::Idle => Ok(false),
            CustomRoutePhase::Waiting {
                generation: active,
                cancellation: CustomRouteCancellation::Requested,
            } if active == generation => Ok(false),
            CustomRoutePhase::Waiting { .. } => Err(KitError::conflict(
                "prompt identity changed before cancellation",
            )),
            CustomRoutePhase::Closed => Err(KitError::closed("conversation execution is closed")),
        }
    }

    pub(crate) fn seal(&self, deadline: Instant) {
        let mut admission = self
            .admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *admission = match *admission {
            CustomRouteAdmission::Open => CustomRouteAdmission::Closing { deadline },
            CustomRouteAdmission::Closing { deadline: current } => CustomRouteAdmission::Closing {
                deadline: current.min(deadline),
            },
        };
        drop(admission);
        let generation = match *self.phase() {
            CustomRoutePhase::Waiting { generation, .. }
            | CustomRoutePhase::Running { generation, .. } => Some(generation),
            CustomRoutePhase::Idle | CustomRoutePhase::Closed => None,
        };
        if let Some(generation) = generation {
            let _ = self.signal_cancellation(generation);
        }
        self.changed.notify_waiters();
    }

    pub(crate) fn prepare_reopen(
        &self,
        workspace_id: &WorkspaceId,
        deadline: Instant,
    ) -> Result<(), KitError> {
        if &self.workspace_id != workspace_id {
            return Err(KitError::invalid_state(
                "conversation execution contradicts its workspace",
            ));
        }
        let mut admission = self
            .admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let task = self.task();
        let phase = self.phase();
        if !matches!(*phase, CustomRoutePhase::Idle | CustomRoutePhase::Closed)
            || !Self::task_is_quiescent(&task)
        {
            return Err(KitError::busy(
                "conversation execution still has unsettled work",
            ));
        }
        *admission = match *admission {
            CustomRouteAdmission::Open => CustomRouteAdmission::Closing { deadline },
            CustomRouteAdmission::Closing { deadline: current } => CustomRouteAdmission::Closing {
                deadline: current.min(deadline),
            },
        };
        drop(phase);
        drop(task);
        drop(admission);
        self.changed.notify_waiters();
        Ok(())
    }

    pub(crate) fn belongs_to(&self, workspace_id: &WorkspaceId) -> bool {
        &self.workspace_id == workspace_id
    }

    pub(crate) async fn await_result<O, F>(&self, future: F) -> Result<O, KitError>
    where
        F: Future<Output = Result<O, KitError>>,
    {
        let mut future = std::pin::pin!(future);
        loop {
            let mut changed = std::pin::pin!(self.changed.notified());
            changed.as_mut().enable();
            match self.closing_deadline() {
                Some(deadline) => {
                    return tokio::select! {
                        biased;
                        result = &mut future => result,
                        _ = tokio::time::sleep_until(deadline) => {
                            Err(KitError::closed("conversation lifecycle deadline elapsed"))
                        }
                    };
                }
                None => {
                    tokio::select! {
                        biased;
                        result = &mut future => return result,
                        _ = &mut changed => {}
                    }
                }
            }
        }
    }

    fn open_admission(&self) -> Result<MutexGuard<'_, CustomRouteAdmission>, KitError> {
        let admission = self
            .admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if matches!(*admission, CustomRouteAdmission::Closing { .. }) {
            return Err(KitError::closed("conversation execution is closing"));
        }
        Ok(admission)
    }

    fn phase(&self) -> MutexGuard<'_, CustomRoutePhase> {
        self.phase
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn task(&self) -> MutexGuard<'_, CustomRouteTask> {
        self.task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn ensure_task(&self, generation: u64) -> Result<(), KitError> {
        match &*self.task() {
            CustomRouteTask::Running {
                generation: Some(active),
                state: CustomRouteTaskState::Active,
                ..
            } if *active == generation => Ok(()),
            CustomRouteTask::Running { .. } => Err(KitError::conflict(
                "prompt task identity does not match the active request",
            )),
            CustomRouteTask::Idle => Err(KitError::invalid_state("prompt task is not active")),
        }
    }

    fn complete_task(&self) {
        if let CustomRouteTask::Running { state, .. } = &mut *self.task() {
            *state = CustomRouteTaskState::Finished;
        }
        self.changed.notify_waiters();
    }

    fn task_quiescent(&self) -> bool {
        Self::task_is_quiescent(&self.task())
    }

    fn task_is_quiescent(task: &CustomRouteTask) -> bool {
        match task {
            CustomRouteTask::Idle => true,
            CustomRouteTask::Running { handle, .. } => handle.is_finished(),
        }
    }

    fn task_finishing(&self) -> bool {
        matches!(
            &*self.task(),
            CustomRouteTask::Running {
                handle,
                state: CustomRouteTaskState::Finished,
                ..
            } if !handle.is_finished()
        )
    }

    fn closing_deadline(&self) -> Option<Instant> {
        match *self
            .admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
        {
            CustomRouteAdmission::Open => None,
            CustomRouteAdmission::Closing { deadline } => Some(deadline),
        }
    }

    fn take_task(&self) -> CustomRouteTask {
        std::mem::replace(&mut *self.task(), CustomRouteTask::Idle)
    }

    fn cancellation_receiver(&self, generation: u64) -> Result<watch::Receiver<bool>, KitError> {
        self.cancellation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .filter(|(active, _)| *active == generation)
            .map(|(_, cancellation)| cancellation.subscribe())
            .ok_or_else(|| {
                KitError::conflict("prompt cancellation ownership changed before submission")
            })
    }

    fn signal_cancellation(&self, generation: u64) -> Result<(), KitError> {
        let cancellation = self
            .cancellation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some((active, cancellation)) = cancellation.as_ref() else {
            return Ok(());
        };
        if *active != generation {
            return Err(KitError::conflict("prompt cancellation identity changed"));
        }
        cancellation.send_replace(true);
        Ok(())
    }

    fn clear_cancellation(&self, generation: u64) {
        let mut cancellation = self
            .cancellation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if cancellation
            .as_ref()
            .is_some_and(|(active, _)| *active == generation)
        {
            cancellation.take();
        }
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend>
    CustomProviderRun<S, D, P, T>
{
    pub(crate) fn decline(mut self) -> Result<(), KitError> {
        match std::mem::replace(&mut self.state, CustomProviderRunState::Resolving) {
            CustomProviderRunState::Proposed(_execution) => {
                self.route.finish(self.generation);
                Ok(())
            }
            CustomProviderRunState::Accepted(_) | CustomProviderRunState::Buffered(_) => {
                Err(KitError::invalid_state("model run was already accepted"))
            }
            CustomProviderRunState::Resolving => {
                Err(KitError::invalid_state("model run was already declined"))
            }
        }
    }

    pub(crate) async fn accept(&mut self) -> Result<(), KitError> {
        let state = std::mem::replace(&mut self.state, CustomProviderRunState::Resolving);
        let CustomProviderRunState::Proposed(mut execution) = state else {
            return Err(KitError::invalid_state(
                "model run acceptance was observed more than once",
            ));
        };
        let first = poll_fn(|context| match execution.as_mut().poll(context) {
            Poll::Ready(result) => Poll::Ready(Some(result)),
            Poll::Pending => Poll::Ready(None),
        })
        .await;
        match first {
            Some(Err(error)) if Self::is_preacceptance_rejection(&error) => {
                self.route.finish(self.generation);
                Err(error)
            }
            Some(result) => {
                if let Err(error) = self.route.register(self.generation) {
                    self.route.finish(self.generation);
                    return Err(error);
                }
                self.state = CustomProviderRunState::Buffered(Some(result));
                Ok(())
            }
            None => {
                if let Err(error) = self.route.register(self.generation) {
                    self.route.finish(self.generation);
                    return Err(error);
                }
                self.state = CustomProviderRunState::Accepted(execution);
                Ok(())
            }
        }
    }

    pub(crate) async fn finish(mut self) -> Result<PromptResponse, KitError> {
        let result = self.resolve().await;
        self.route.finish(self.generation);
        result
    }

    async fn resolve(&mut self) -> Result<PromptResponse, KitError> {
        match std::mem::replace(&mut self.state, CustomProviderRunState::Resolving) {
            CustomProviderRunState::Accepted(execution) => execution.await,
            CustomProviderRunState::Buffered(mut result) => result
                .take()
                .ok_or_else(|| KitError::invalid_state("model terminal state is missing"))?,
            CustomProviderRunState::Proposed(_) => {
                Err(KitError::invalid_state("model run was not accepted"))
            }
            CustomProviderRunState::Resolving => Err(KitError::invalid_state(
                "model run terminal state was observed more than once",
            )),
        }
    }

    fn is_preacceptance_rejection(error: &KitError) -> bool {
        matches!(
            (error).kind(),
            crate::error::ErrorKind::InvalidInput
                | crate::error::ErrorKind::Unsupported
                | crate::error::ErrorKind::NotYetImplemented
        )
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend> Drop
    for CustomProviderRun<S, D, P, T>
{
    fn drop(&mut self) {
        if matches!(&self.state, CustomProviderRunState::Proposed(_)) {
            self.route.finish(self.generation);
        }
    }
}
