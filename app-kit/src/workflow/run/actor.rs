use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use agents_kit::AgentBackend;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::sync::Notify;

use crate::app::MAX_RUNS;
use crate::workflow::definition::{JsonBoundary, MAX_COLLECTION_ITEMS, MAX_DEPTH};
use crate::workflow::execution::{MAX_FRONTIER, MAX_RUNTIME_BYTES};
use crate::workflow::interaction::Attempt;
use crate::workflow::{
    ActionContext, ActionMode, ExecutionOptions, LifecycleEvent, RunId, WorkflowFailure, WorkflowId,
};
use crate::{AppError, WorkspaceId};

use super::FailureCategory;

const MAX_TRANSIENT_SCHEDULE_ATTEMPTS: u8 = 8;
const TRANSIENT_SCHEDULE_INITIAL_DELAY: Duration = Duration::from_millis(25);
const TRANSIENT_SCHEDULE_MAX_DELAY: Duration = Duration::from_secs(1);

#[derive(Clone, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct RunKey {
    workspace: WorkspaceId,
    workflow: WorkflowId,
    run: RunId,
    occurrence: String,
}

pub(crate) enum RetryDisposition {
    BoundedTransient,
    RetainUntilProgress,
}

pub(crate) enum ScheduleCause {
    CheckpointReady(u64),
    Reconcile,
    CheckpointObserved(u64),
    CurrentState,
    StorageFailure(RetryDisposition),
}

pub(crate) struct RunCell<A: AgentBackend> {
    state: Mutex<RunCellState<A>>,
    notification: Arc<Notify>,
}

pub(crate) struct RunRegistry<A: AgentBackend> {
    state: Mutex<RunRegistryState<A>>,
}

enum RunCellState<A: AgentBackend> {
    Retained {
        schedule: Box<RunSchedule>,
        interactions: BTreeMap<u64, Arc<Attempt<A>>>,
    },
    Released,
}

enum RunRegistryState<A: AgentBackend> {
    Open {
        cells: BTreeMap<RunKey, Arc<RunCell<A>>>,
        preparations: BTreeMap<(WorkspaceId, String, String), Weak<tokio::sync::Mutex<()>>>,
    },
    Closed,
}

struct RunSchedule {
    owners: usize,
    retained: bool,
    driver: RunDriver,
    mode: ActionMode,
    options: Arc<ExecutionOptions>,
    committed: usize,
    boundary: Option<bool>,
    bounded_attempts: u8,
    recoverable_attempts: u8,
    last_checkpoint: Option<u64>,
    generation: u64,
    lifecycle: RunLifecycle,
    observations: Vec<ActionObservation>,
    parent_observation: Option<Arc<ActionContext>>,
}

impl Default for RunSchedule {
    fn default() -> Self {
        Self {
            owners: 0,
            retained: false,
            driver: RunDriver::default(),
            mode: ActionMode::Asynchronous,
            options: Arc::new(ExecutionOptions::default()),
            committed: 0,
            boundary: None,
            bounded_attempts: 0,
            recoverable_attempts: 0,
            last_checkpoint: None,
            generation: 0,
            lifecycle: RunLifecycle::default(),
            observations: Vec::new(),
            parent_observation: None,
        }
    }
}

#[derive(Default)]
enum RunDriver {
    #[default]
    Idle,
    Driving {
        pending: Option<Duration>,
    },
    Exclusive {
        generation: u64,
        disposition: ExecutionDisposition,
        publishing: bool,
        pending: Option<Duration>,
    },
    Child {
        parent: RunKey,
        pending: Option<Duration>,
    },
}

enum ExecutionDisposition {
    Live,
    Finalizing,
    Dropped,
}

#[derive(Default)]
enum RunLifecycle {
    #[default]
    Dormant,
    Opening {
        deadline: tokio::time::Instant,
    },
    Active {
        cleanup: Option<tokio::time::Instant>,
    },
    Finishing {
        checkpoint: u64,
        deadline: tokio::time::Instant,
    },
    Complete {
        checkpoint: u64,
        failure: Option<WorkflowFailure>,
        cleanup: Option<tokio::time::Instant>,
    },
}

struct ActionObservation {
    context: Arc<ActionContext>,
    lineage: String,
    // The exact source fact survives a cancelled waiter or owning Drop until
    // the original publication makes the canonical failure durable.
    failure: Option<FailureCategory>,
    state: ActionObservationState,
}

enum ActionObservationState {
    Open {
        stream_items: Option<usize>,
        spans: Vec<SpanObservation>,
    },
    Completing,
    Closed,
}

enum SpanObservation {
    Open { id: String, parent: Option<usize> },
    Closed { id: String },
}

impl SpanObservation {
    fn id(&self) -> &str {
        match self {
            Self::Open { id, .. } | Self::Closed { id } => id,
        }
    }
}

impl ActionObservation {
    fn matches(&self, context: &ActionContext) -> bool {
        std::ptr::eq(self.context.as_ref(), context)
    }
}

impl RunKey {
    pub(crate) fn new(
        workspace: WorkspaceId,
        workflow: WorkflowId,
        run: RunId,
        occurrence: String,
    ) -> Self {
        Self {
            workspace,
            workflow,
            run,
            occurrence,
        }
    }

    pub(crate) fn workspace(&self) -> &WorkspaceId {
        &self.workspace
    }

    pub(crate) fn workflow(&self) -> &WorkflowId {
        &self.workflow
    }

    pub(crate) fn run(&self) -> &RunId {
        &self.run
    }

    pub(crate) fn occurrence(&self) -> &str {
        &self.occurrence
    }
}

impl<A: AgentBackend> RunCell<A> {
    fn new() -> Self {
        Self {
            state: Mutex::new(RunCellState::Retained {
                schedule: Box::default(),
                interactions: BTreeMap::new(),
            }),
            notification: Arc::new(Notify::new()),
        }
    }

    pub(crate) fn retain_owner(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let RunCellState::Retained { schedule, .. } = &mut *state {
            schedule.owners += 1;
        }
    }

    fn release_owner(&self) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let RunCellState::Retained {
            schedule,
            interactions,
        } = &mut *state
        else {
            return false;
        };
        schedule.owners -= 1;
        schedule.owners == 0
            && schedule.retained
            && matches!(schedule.driver, RunDriver::Idle)
            && matches!(
                schedule.lifecycle,
                RunLifecycle::Dormant | RunLifecycle::Complete { failure: None, .. }
            )
            && schedule.observations.is_empty()
            && schedule.parent_observation.is_none()
            && interactions.is_empty()
    }

    pub(crate) fn retained(&self, checkpoint: u64) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let RunCellState::Retained { schedule, .. } = &mut *state
            && schedule
                .last_checkpoint
                .is_none_or(|latest| checkpoint >= latest)
        {
            schedule.observe_checkpoint(checkpoint);
            schedule.retained = true;
        }
    }

    pub(crate) fn schedule(&self, cause: ScheduleCause) -> (bool, Option<Duration>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let result = match &mut *state {
            RunCellState::Retained { schedule, .. } => schedule.request_cause(cause),
            RunCellState::Released => (false, None),
        };
        if result.0
            && matches!(
                &*state,
                RunCellState::Retained { schedule, .. }
                    if matches!(&schedule.driver, RunDriver::Child { .. })
            )
        {
            self.notification.notify_waiters();
        }
        result
    }

    pub(crate) fn claim_execution(&self, mode: ActionMode) -> Result<u64, AppError> {
        if mode == ActionMode::Both {
            return Err(AppError::Integration(
                "execution requires one actual mode".into(),
            ));
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let RunCellState::Retained { schedule, .. } = &mut *state else {
            return Err(AppError::Closed("run execution owner is closed".into()));
        };
        if !matches!(schedule.driver, RunDriver::Idle) {
            return Err(AppError::Busy("run already has an execution owner".into()));
        }
        schedule.generation = schedule
            .generation
            .checked_add(1)
            .ok_or_else(|| AppError::Conflict("run execution generation is exhausted".into()))?;
        schedule.mode = mode;
        schedule.driver = RunDriver::Exclusive {
            generation: schedule.generation,
            disposition: ExecutionDisposition::Live,
            publishing: false,
            pending: None,
        };
        Ok(schedule.generation)
    }

    pub(crate) fn execution_mode(&self) -> Result<ActionMode, AppError> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        match &*state {
            RunCellState::Retained { schedule, .. } => Ok(schedule.mode),
            RunCellState::Released => Err(AppError::Closed("run execution owner is closed".into())),
        }
    }

    pub(crate) fn configure_execution(
        &self,
        generation: u64,
        options: ExecutionOptions,
    ) -> Result<(), AppError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let RunCellState::Retained { schedule, .. } = &mut *state else {
            return Err(AppError::Closed("run execution owner is closed".into()));
        };
        if !matches!(&schedule.driver, RunDriver::Exclusive {
            generation: current, disposition: ExecutionDisposition::Live, ..
        } if *current == generation)
        {
            return Err(AppError::Closed("run execution generation changed".into()));
        }
        schedule.options = Arc::new(options);
        schedule.committed = 0;
        schedule.boundary = None;
        Ok(())
    }

    pub(crate) fn execution_options(&self) -> Result<Arc<ExecutionOptions>, AppError> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        match &*state {
            RunCellState::Retained { schedule, .. } => Ok(Arc::clone(&schedule.options)),
            RunCellState::Released => Err(AppError::Closed("run execution owner is closed".into())),
        }
    }

    pub(crate) fn bind_inputs(
        &self,
        required: &[String],
        values: &Map<String, Value>,
    ) -> Result<u64, AppError> {
        JsonBoundary::validate_inputs(values)?;
        if required.is_empty()
            || required.len() != values.len()
            || required.windows(2).any(|pair| pair[0] >= pair[1])
            || required.iter().any(|key| !values.contains_key(key))
        {
            return Err(AppError::Conflict("execution input key set changed".into()));
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let RunCellState::Retained { schedule, .. } = &mut *state else {
            return Err(AppError::Closed("run execution owner is closed".into()));
        };
        let generation = schedule
            .active_input_generation()
            .ok_or_else(|| AppError::Conflict("execution input owner is not active".into()))?;
        if required
            .iter()
            .all(|key| schedule.options.inputs.get(key) == values.get(key))
        {
            return Ok(generation);
        }
        if required
            .iter()
            .any(|key| schedule.options.inputs.contains_key(key))
        {
            return Err(AppError::Conflict(
                "execution input is already bound differently".into(),
            ));
        }
        let mut inputs = schedule.options.inputs.clone();
        inputs.extend(values.clone());
        JsonBoundary::validate_inputs(&inputs)?;
        Arc::make_mut(&mut schedule.options).inputs = inputs;
        Ok(generation)
    }

    pub(crate) fn matches_inputs(
        &self,
        generation: u64,
        required: &[String],
        values: &Map<String, Value>,
    ) -> bool {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let RunCellState::Retained { schedule, .. } = &*state else {
            return false;
        };
        schedule.active_input_generation() == Some(generation)
            && !required.is_empty()
            && required.len() == values.len()
            && required.windows(2).all(|pair| pair[0] < pair[1])
            && required.iter().all(|key| {
                values
                    .get(key)
                    .is_some_and(|value| schedule.options.inputs.get(key) == Some(value))
            })
    }

    pub(crate) fn execution_boundary(&self) -> Result<Option<bool>, AppError> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        match &*state {
            RunCellState::Retained { schedule, .. } => Ok(schedule.boundary),
            RunCellState::Released => Err(AppError::Closed("run execution owner is closed".into())),
        }
    }

    pub(crate) fn clear_execution_boundary(&self) -> Result<(), AppError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        match &mut *state {
            RunCellState::Retained { schedule, .. } => {
                schedule.boundary = None;
                Ok(())
            }
            RunCellState::Released => Err(AppError::Closed("run execution owner is closed".into())),
        }
    }

    pub(crate) fn record_execution_boundary(
        &self,
        outcomes: usize,
        ordinary: bool,
    ) -> Result<(), AppError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let RunCellState::Retained { schedule, .. } = &mut *state else {
            return Err(AppError::Closed("run execution owner is closed".into()));
        };
        schedule.committed = schedule
            .committed
            .checked_add(outcomes)
            .ok_or_else(|| AppError::Conflict("execution boundary count is exhausted".into()))?;
        schedule.boundary = Some(ordinary);
        Ok(())
    }

    pub(crate) fn halt_after(&self, node: &str) -> Result<bool, AppError> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let RunCellState::Retained { schedule, .. } = &*state else {
            return Err(AppError::Closed("run execution owner is closed".into()));
        };
        Ok(schedule
            .options
            .halt_after
            .iter()
            .any(|selected| selected == node)
            || schedule
                .options
                .max_steps
                .is_some_and(|limit| schedule.committed >= limit.get()))
    }

    pub(crate) fn clear_execution_options(&self, generation: Option<u64>) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let RunCellState::Retained { schedule, .. } = &mut *state
            && generation.is_none_or(|generation| schedule.generation == generation)
        {
            // Accepted publications retain their own original input snapshot.
            // Clearing a finished/dropped session never changes that receipt.
            schedule.options = Arc::new(ExecutionOptions::default());
            schedule.committed = 0;
            schedule.boundary = None;
        }
    }

    pub(crate) fn owns_execution(&self, generation: u64) -> bool {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let RunCellState::Retained { schedule, .. } = &*state else {
            return false;
        };
        matches!(&schedule.driver, RunDriver::Exclusive {
            generation: current, ..
        } if *current == generation)
    }

    pub(crate) fn execution_is_live(&self, generation: u64) -> bool {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let RunCellState::Retained { schedule, .. } = &*state else {
            return false;
        };
        matches!(&schedule.driver, RunDriver::Exclusive {
            generation: current, disposition: ExecutionDisposition::Live, ..
        } if *current == generation)
    }

    pub(crate) fn begin_publication(&self, generation: u64) -> Result<(), AppError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let RunCellState::Retained { schedule, .. } = &mut *state
            && let RunDriver::Exclusive {
                generation: current,
                disposition: ExecutionDisposition::Live,
                publishing,
                ..
            } = &mut schedule.driver
            && *current == generation
            && !*publishing
        {
            *publishing = true;
            return Ok(());
        }
        Err(AppError::Closed(
            "run execution generation is unavailable".into(),
        ))
    }

    pub(crate) fn end_publication(&self, generation: u64) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let RunCellState::Retained { schedule, .. } = &mut *state
            && let RunDriver::Exclusive {
                generation: current,
                disposition,
                publishing,
                ..
            } = &mut schedule.driver
            && *current == generation
        {
            *publishing = false;
            return !matches!(disposition, ExecutionDisposition::Live);
        }
        false
    }

    pub(crate) fn revoke_execution(&self, generation: u64) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let RunCellState::Retained { schedule, .. } = &mut *state
            && let RunDriver::Exclusive {
                generation: current,
                disposition,
                ..
            } = &mut schedule.driver
            && *current == generation
        {
            if !matches!(disposition, ExecutionDisposition::Dropped) {
                *disposition = ExecutionDisposition::Finalizing;
            }
            self.notification.notify_waiters();
        }
    }

    pub(crate) fn drop_execution(&self, generation: u64) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let RunCellState::Retained { schedule, .. } = &mut *state
            && let RunDriver::Exclusive {
                generation: current,
                disposition,
                publishing,
                ..
            } = &mut schedule.driver
            && *current == generation
        {
            *disposition = ExecutionDisposition::Dropped;
            self.notification.notify_waiters();
            return !*publishing;
        }
        false
    }

    pub(crate) fn execution_was_dropped(&self, generation: u64) -> bool {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let RunCellState::Retained { schedule, .. } = &*state else {
            return false;
        };
        matches!(&schedule.driver, RunDriver::Exclusive {
            generation: current, disposition: ExecutionDisposition::Dropped, ..
        } if *current == generation)
    }

    pub(crate) fn source_failure(
        &self,
        generation: u64,
    ) -> Option<(Arc<ActionContext>, String, FailureCategory)> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let RunCellState::Retained { schedule, .. } = &*state else {
            return None;
        };
        if !schedule.owns_lifecycle(generation) {
            return None;
        }
        schedule
            .observations
            .iter()
            .filter_map(|entry| entry.failure.map(|category| (entry, category)))
            .min_by(|(left, _), (right, _)| {
                (&left.context.node, &left.lineage).cmp(&(&right.context.node, &right.lineage))
            })
            .map(|(entry, category)| (Arc::clone(&entry.context), entry.lineage.clone(), category))
    }

    pub(crate) fn observe_source_failure(
        &self,
        generation: u64,
        context: &ActionContext,
        lineage: &str,
        category: FailureCategory,
    ) -> Result<(), AppError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let entry = state.action_observation(generation, context)?;
        if entry.lineage != lineage {
            return Err(AppError::Integration(
                "source observation lineage changed".into(),
            ));
        }
        if matches!(entry.state, ActionObservationState::Open { .. }) {
            entry.failure.get_or_insert(category);
        }
        Ok(())
    }

    pub(crate) fn release_execution(&self, generation: u64) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let RunCellState::Retained { schedule, .. } = &mut *state
            && let RunDriver::Exclusive {
                generation: current,
                publishing: false,
                pending,
                ..
            } = &schedule.driver
            && *current == generation
        {
            let scheduled = pending.is_some();
            schedule.driver = RunDriver::Idle;
            self.notification.notify_waiters();
            return scheduled;
        }
        false
    }

    pub(crate) fn admit_lifecycle(&self, generation: u64) -> Result<(), AppError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let RunCellState::Retained { schedule, .. } = &mut *state else {
            return Err(AppError::Closed("run lifecycle owner is closed".into()));
        };
        if !matches!(&schedule.driver, RunDriver::Exclusive { generation: current, disposition: ExecutionDisposition::Live, .. } if *current == generation)
        {
            return Err(AppError::Closed(
                "run execution generation is unavailable".into(),
            ));
        }
        // Called only after all input, selector and durable admission checks.
        // A failed acquisition cannot erase a previous completion error.
        schedule.lifecycle = RunLifecycle::Dormant;
        Ok(())
    }

    pub(crate) fn observation_owner(&self) -> Result<Option<u64>, AppError> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let RunCellState::Retained { schedule, .. } = &*state else {
            return Err(AppError::Closed("run lifecycle owner is closed".into()));
        };
        match &schedule.driver {
            RunDriver::Exclusive { generation, .. } => Ok(Some(*generation)),
            RunDriver::Driving { .. } | RunDriver::Child { .. } => Ok(None),
            RunDriver::Idle => Err(AppError::Closed(
                "run lifecycle owner is unavailable".into(),
            )),
        }
    }

    pub(crate) fn begin_lifecycle(&self, owner: Option<u64>) -> Result<(u64, bool), AppError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let RunCellState::Retained { schedule, .. } = &mut *state else {
            return Err(AppError::Closed("run lifecycle owner is closed".into()));
        };
        let owned = match (&schedule.driver, owner) {
            (
                RunDriver::Exclusive {
                    generation,
                    disposition: ExecutionDisposition::Live,
                    ..
                },
                Some(owner),
            ) => *generation == owner,
            (RunDriver::Driving { .. } | RunDriver::Child { .. }, None) => true,
            _ => false,
        };
        if !owned {
            return Err(AppError::Closed(
                "run lifecycle generation is unavailable".into(),
            ));
        }
        match &schedule.lifecycle {
            RunLifecycle::Active { .. } => return Ok((schedule.generation, false)),
            RunLifecycle::Opening { .. } | RunLifecycle::Finishing { .. } => {
                return Err(AppError::Busy(
                    "run lifecycle observation is already owned".into(),
                ));
            }
            RunLifecycle::Dormant | RunLifecycle::Complete { .. } => {}
        }
        if owner.is_none() {
            schedule.generation = schedule.generation.checked_add(1).ok_or_else(|| {
                AppError::Conflict("run execution generation is exhausted".into())
            })?;
        }
        schedule.lifecycle = RunLifecycle::Opening {
            deadline: LifecycleEvent::deadline(None),
        };
        Ok((schedule.generation, true))
    }

    pub(crate) fn opened_lifecycle(&self, generation: u64) -> Result<(), AppError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let RunCellState::Retained { schedule, .. } = &mut *state else {
            return Err(AppError::Closed("run lifecycle owner is closed".into()));
        };
        if !schedule.owns_lifecycle(generation)
            || !matches!(schedule.lifecycle, RunLifecycle::Opening { .. })
        {
            return Err(AppError::Closed("run lifecycle generation changed".into()));
        }
        schedule.lifecycle = RunLifecycle::Active { cleanup: None };
        Ok(())
    }

    pub(crate) fn begin_lifecycle_finish(&self, generation: u64, checkpoint: u64) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let RunCellState::Retained { schedule, .. } = &mut *state else {
            return false;
        };
        if !schedule.owns_lifecycle(generation)
            || !matches!(
                schedule.lifecycle,
                RunLifecycle::Active { .. } | RunLifecycle::Opening { .. }
            )
        {
            return false;
        }
        let deadline = match &schedule.lifecycle {
            RunLifecycle::Opening { deadline }
            | RunLifecycle::Active {
                cleanup: Some(deadline),
            } => *deadline,
            _ => LifecycleEvent::deadline(None),
        };
        schedule.lifecycle = RunLifecycle::Finishing {
            checkpoint,
            deadline,
        };
        true
    }

    pub(crate) fn complete_lifecycle(
        &self,
        generation: u64,
        checkpoint: u64,
        failure: Option<WorkflowFailure>,
    ) -> Result<(), AppError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let RunCellState::Retained { schedule, .. } = &mut *state else {
            return Err(AppError::Closed("run lifecycle owner is closed".into()));
        };
        let deadline = match &schedule.lifecycle {
            RunLifecycle::Finishing {
                checkpoint: start,
                deadline,
            } if schedule.owns_lifecycle(generation) && checkpoint >= *start => *deadline,
            _ => {
                return Err(AppError::Closed(
                    "run lifecycle completion generation changed".into(),
                ));
            }
        };
        schedule.lifecycle = RunLifecycle::Complete {
            checkpoint,
            failure,
            cleanup: Some(deadline),
        };
        // Publish completion before waking observers; the driver still owns release.
        self.notification.notify_waiters();
        Ok(())
    }

    pub(crate) fn lifecycle_observation(
        &self,
        checkpoint: u64,
        actionable: bool,
    ) -> Result<bool, AppError> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let RunCellState::Retained { schedule, .. } = &*state else {
            return Err(AppError::Closed("run lifecycle owner is closed".into()));
        };
        match &schedule.lifecycle {
            RunLifecycle::Opening { .. } | RunLifecycle::Finishing { .. } => Ok(false),
            RunLifecycle::Active { .. } => Ok(actionable),
            RunLifecycle::Complete {
                checkpoint: completed,
                ..
            } if checkpoint < *completed => Ok(false),
            RunLifecycle::Complete {
                failure: Some(failure),
                ..
            } => Err(failure.application_error()),
            RunLifecycle::Dormant | RunLifecycle::Complete { .. } => Ok(true),
        }
    }

    pub(crate) fn terminal_observation_is_settled(&self, checkpoint: u64) -> bool {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        matches!(
            &*state,
            RunCellState::Retained { schedule, .. }
                if matches!(schedule.driver, RunDriver::Driving { pending: None })
                    && matches!(schedule.lifecycle,
                        RunLifecycle::Complete { checkpoint: completed, .. }
                            if completed == checkpoint)
                    && schedule.parent_observation.is_none()
        )
    }

    pub(crate) fn returned_boundary(&self, generation: u64) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let RunCellState::Retained { schedule, .. } = &mut *state
            && schedule.owns_lifecycle(generation)
            && let RunLifecycle::Active { cleanup } | RunLifecycle::Complete { cleanup, .. } =
                &mut schedule.lifecycle
        {
            *cleanup = None;
        }
    }

    pub(crate) fn begin_action_cleanup(
        &self,
        generation: u64,
    ) -> Result<tokio::time::Instant, AppError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let RunCellState::Retained { schedule, .. } = &mut *state else {
            return Err(AppError::Closed("run cleanup observation is closed".into()));
        };
        if !schedule.owns_lifecycle(generation) {
            return Err(AppError::Closed("run cleanup generation changed".into()));
        }
        match &mut schedule.lifecycle {
            RunLifecycle::Active { cleanup } | RunLifecycle::Complete { cleanup, .. } => {
                Ok(*cleanup.get_or_insert_with(|| LifecycleEvent::deadline(None)))
            }
            RunLifecycle::Opening { deadline } | RunLifecycle::Finishing { deadline, .. } => {
                Ok(*deadline)
            }
            // Abort before the first poll still owns a bounded authority read;
            // sampling its budget must not open execution observation hooks.
            RunLifecycle::Dormant => Ok(LifecycleEvent::deadline(None)),
        }
    }

    pub(crate) fn observe_action(
        &self,
        generation: u64,
        context: Arc<ActionContext>,
        lineage: &str,
    ) -> Result<(), AppError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let RunCellState::Retained { schedule, .. } = &mut *state else {
            return Err(AppError::Closed("run action observation is closed".into()));
        };
        if !schedule.owns_lifecycle(generation) {
            return Err(AppError::Closed(
                "run action observation generation changed".into(),
            ));
        }
        if schedule
            .observations
            .iter()
            .any(|entry| entry.matches(&context))
        {
            return Ok(());
        }
        if schedule.observations.len() >= MAX_FRONTIER {
            return Err(AppError::Integration(
                "run action observation bound exhausted".into(),
            ));
        }
        if let RunLifecycle::Active { cleanup } = &mut schedule.lifecycle {
            *cleanup = None;
        }
        schedule.observations.push(ActionObservation {
            context,
            lineage: lineage.to_owned(),
            failure: None,
            state: ActionObservationState::Open {
                stream_items: None,
                spans: Vec::new(),
            },
        });
        Ok(())
    }

    pub(crate) fn lifecycle_generation(&self) -> Option<u64> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        match &*state {
            RunCellState::Retained { schedule, .. }
                if matches!(
                    schedule.lifecycle,
                    RunLifecycle::Opening { .. }
                        | RunLifecycle::Active { .. }
                        | RunLifecycle::Finishing { .. }
                ) =>
            {
                Some(schedule.generation)
            }
            RunCellState::Retained { .. } | RunCellState::Released => None,
        }
    }

    pub(crate) fn retire_closed_observation(&self, generation: u64, context: &ActionContext) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let RunCellState::Retained { schedule, .. } = &mut *state else {
            return;
        };
        if !schedule.owns_lifecycle(generation) {
            return;
        }
        schedule.observations.retain(|entry| {
            !(entry.matches(context) && matches!(entry.state, ActionObservationState::Closed))
        });
    }

    pub(crate) fn action_context(
        &self,
        checkpoint: u64,
        node: &str,
        lineage: &str,
    ) -> Option<Arc<ActionContext>> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let RunCellState::Retained { schedule, .. } = &*state else {
            return None;
        };
        schedule
            .observations
            .iter()
            .find(|entry| {
                entry.context.checkpoint == checkpoint
                    && entry.context.node == node
                    && entry.lineage == lineage
            })
            .map(|entry| Arc::clone(&entry.context))
    }

    pub(crate) fn action_observations(&self) -> Vec<Arc<ActionContext>> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        match &*state {
            RunCellState::Retained { schedule, .. } => schedule
                .observations
                .iter()
                .map(|entry| Arc::clone(&entry.context))
                .collect(),
            RunCellState::Released => Vec::new(),
        }
    }

    pub(crate) fn open_action_stream(
        &self,
        generation: u64,
        context: &ActionContext,
    ) -> Result<(), AppError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let RunCellState::Retained { schedule, .. } = &mut *state else {
            return Err(AppError::Closed("run stream observation is closed".into()));
        };
        if !schedule.owns_lifecycle(generation) {
            return Err(AppError::Closed(
                "run action observation generation changed".into(),
            ));
        }
        let entry = schedule
            .observations
            .iter_mut()
            .find(|entry| entry.matches(context))
            .ok_or_else(|| AppError::Closed("run action observation is unavailable".into()))?;
        match &mut entry.state {
            ActionObservationState::Open { stream_items, .. } => {
                stream_items.get_or_insert(0);
                Ok(())
            }
            ActionObservationState::Completing | ActionObservationState::Closed => Ok(()),
        }
    }

    pub(crate) fn action_stream_item(
        &self,
        generation: u64,
        context: &ActionContext,
        observed: bool,
    ) -> Result<usize, AppError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let RunCellState::Retained { schedule, .. } = &mut *state else {
            return Err(AppError::Closed("run stream observation is closed".into()));
        };
        if !schedule.owns_lifecycle(generation) {
            return Err(AppError::Closed(
                "run action observation generation changed".into(),
            ));
        }
        let entry = schedule
            .observations
            .iter_mut()
            .find(|entry| entry.matches(context))
            .ok_or_else(|| AppError::Closed("run action observation is unavailable".into()))?;
        let ActionObservationState::Open {
            stream_items: Some(items),
            ..
        } = &mut entry.state
        else {
            return Err(AppError::Closed(
                "run stream observation is not open".into(),
            ));
        };
        let index = *items;
        if observed {
            *items = items.checked_add(1).ok_or_else(|| {
                AppError::Integration("stream observation count exhausted".into())
            })?;
        }
        Ok(index)
    }

    pub(crate) fn span_parent(
        &self,
        generation: u64,
        context: &ActionContext,
        id: &str,
        parent: Option<&str>,
    ) -> Result<Option<usize>, AppError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let ActionObservationState::Open { spans, .. } =
            &mut state.action_observation(generation, context)?.state
        else {
            return Err(AppError::Closed("action span observation is closed".into()));
        };
        if spans.len() >= MAX_COLLECTION_ITEMS
            || spans.iter().any(|span| span.id() == id)
            || spans
                .iter()
                .try_fold(id.len(), |bytes, span| bytes.checked_add(span.id().len()))
                .is_none_or(|bytes| bytes > MAX_RUNTIME_BYTES)
        {
            return Err(AppError::InvalidInput(
                "caller span identity exceeds its activation boundary".into(),
            ));
        }
        let parent = parent
            .map(|parent| {
                spans
                    .iter()
                    .position(
                        |span| matches!(span, SpanObservation::Open { id, .. } if id == parent),
                    )
                    .ok_or_else(|| AppError::InvalidInput("caller span parent is not open".into()))
            })
            .transpose()?;
        let mut ancestor = parent;
        let mut depth = 1;
        while let Some(index) = ancestor {
            depth += 1;
            let Some(SpanObservation::Open { parent, .. }) = spans.get(index) else {
                return Err(AppError::Integration(
                    "caller span ancestry is unavailable".into(),
                ));
            };
            ancestor = *parent;
        }
        if depth > MAX_DEPTH {
            return Err(AppError::InvalidInput(
                "caller span nesting exceeds its activation boundary".into(),
            ));
        }
        Ok(parent)
    }

    pub(crate) fn opened_span(
        &self,
        generation: u64,
        context: &ActionContext,
        id: String,
        parent: Option<usize>,
    ) -> Result<(), AppError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let ActionObservationState::Open { spans, .. } =
            &mut state.action_observation(generation, context)?.state
        else {
            return Err(AppError::Closed("action span observation is closed".into()));
        };
        // The original admitted source receipt retains this observation from
        // validation through both start hooks; Drop cannot close it in between.
        spans.push(SpanObservation::Open { id, parent });
        Ok(())
    }

    pub(crate) fn span_attribute(
        &self,
        generation: u64,
        context: &ActionContext,
        target: Option<&str>,
    ) -> Result<(), AppError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let ActionObservationState::Open { spans, .. } =
            &mut state.action_observation(generation, context)?.state
        else {
            return Err(AppError::Closed("action span observation is closed".into()));
        };
        if target.is_some_and(|target| {
            !spans
                .iter()
                .any(|span| matches!(span, SpanObservation::Open { id, .. } if id == target))
        }) {
            return Err(AppError::InvalidInput(
                "caller span attribute target is not open".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn take_span(
        &self,
        generation: u64,
        context: &ActionContext,
        target: Option<&str>,
    ) -> Result<Option<String>, AppError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let ActionObservationState::Open { spans, .. } =
            &mut state.action_observation(generation, context)?.state
        else {
            return if target.is_none() {
                Ok(None)
            } else {
                Err(AppError::Closed("action span observation is closed".into()))
            };
        };
        let index = match target {
            Some(target) => Some(
                spans
                    .iter()
                    .position(|span| span.id() == target)
                    .filter(|index| matches!(spans[*index], SpanObservation::Open { .. }))
                    .ok_or_else(|| {
                        AppError::InvalidInput("caller span finish target is not open".into())
                    })?,
            ),
            None => spans
                .iter()
                .rposition(|span| matches!(span, SpanObservation::Open { .. })),
        };
        let Some(index) = index else { return Ok(None) };
        if spans.iter().any(|span| matches!(span, SpanObservation::Open { parent: Some(parent), .. } if *parent == index)) {
            return Err(AppError::InvalidInput("caller span still has an open child".into()));
        }
        let closed = SpanObservation::Closed {
            id: spans[index].id().to_owned(),
        };
        let SpanObservation::Open { id, .. } = std::mem::replace(&mut spans[index], closed) else {
            unreachable!()
        };
        Ok(Some(id))
    }

    pub(crate) fn take_action_observation(
        &self,
        generation: u64,
        context: &ActionContext,
    ) -> Option<(Arc<ActionContext>, Option<usize>)> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let entry = state.action_observation(generation, context).ok()?;
        let ActionObservationState::Open { stream_items, .. } = &entry.state else {
            return None;
        };
        let taken = (Arc::clone(&entry.context), *stream_items);
        entry.state = ActionObservationState::Completing;
        Some(taken)
    }

    pub(crate) fn finish_action_observation(
        &self,
        generation: u64,
        context: &ActionContext,
        unresolved: bool,
    ) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let RunCellState::Retained { schedule, .. } = &mut *state else {
            return;
        };
        if !schedule.owns_lifecycle(generation) {
            return;
        }
        let Some(index) = schedule
            .observations
            .iter()
            .position(|entry| entry.matches(context))
        else {
            return;
        };
        if !matches!(
            &schedule.observations[index].state,
            ActionObservationState::Completing
        ) {
            return;
        }
        if unresolved {
            schedule.observations[index].state = ActionObservationState::Closed;
        } else {
            schedule.observations.remove(index);
        }
    }

    pub(crate) fn handoff_child(
        &self,
        generation: u64,
        context: Arc<ActionContext>,
    ) -> Result<(), AppError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let RunCellState::Retained { schedule, .. } = &mut *state
            && matches!(&schedule.driver, RunDriver::Exclusive {
                generation: current, disposition: ExecutionDisposition::Live, publishing: false, ..
            } if *current == generation)
        {
            // Registration and child execution share one uninterrupted owner.
            // An Idle interval would let a public execution steal the child.
            schedule.driver = RunDriver::Child {
                parent: RunKey::new(
                    context.workspace.clone(),
                    context.workflow.clone(),
                    context.run.clone(),
                    context.occurrence.clone(),
                ),
                pending: Some(Duration::ZERO),
            };
            schedule.mode = context.mode;
            schedule.parent_observation = Some(context);
            self.notification.notify_waiters();
            return Ok(());
        }
        Err(AppError::Closed(
            "child registration generation changed".into(),
        ))
    }

    pub(crate) fn retain_child_observation(
        &self,
        context: Arc<ActionContext>,
    ) -> Result<(), AppError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let RunCellState::Retained { schedule, .. } = &mut *state
            && let RunDriver::Child { parent, .. } = &schedule.driver
            && parent.workspace == context.workspace
            && parent.workflow == context.workflow
            && parent.run == context.run
        {
            schedule.mode = schedule.parent_observation.get_or_insert(context).mode;
            return Ok(());
        }
        Err(AppError::Closed(
            "child observation ownership changed".into(),
        ))
    }

    pub(crate) fn begin_settled_child_finish(
        &self,
        checkpoint: u64,
        deadline: tokio::time::Instant,
    ) -> Option<(u64, Option<WorkflowFailure>)> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let RunCellState::Retained { schedule, .. } = &mut *state else {
            return None;
        };
        let RunDriver::Child { parent, .. } = &schedule.driver else {
            return None;
        };
        let context = schedule.parent_observation.as_ref()?;
        if parent.workspace != context.workspace
            || parent.workflow != context.workflow
            || parent.run != context.run
        {
            return None;
        }
        let failure = match &mut schedule.lifecycle {
            RunLifecycle::Dormant => None,
            RunLifecycle::Complete {
                checkpoint: completed,
                failure,
                ..
            } if checkpoint >= *completed => failure.take(),
            RunLifecycle::Opening { .. }
            | RunLifecycle::Active { .. }
            | RunLifecycle::Finishing { .. }
            | RunLifecycle::Complete { .. } => return None,
        };
        // A stopped child may have closed Execute before becoming terminal, or
        // never opened it. Only its retained ChildFinished observation remains.
        schedule.lifecycle = RunLifecycle::Finishing {
            checkpoint,
            deadline,
        };
        Some((schedule.generation, failure))
    }

    pub(crate) fn take_child_observation(&self) -> Option<Arc<ActionContext>> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        match &mut *state {
            RunCellState::Retained { schedule, .. }
                if matches!(schedule.lifecycle, RunLifecycle::Finishing { .. }) =>
            {
                schedule.parent_observation.take()
            }
            RunCellState::Retained { .. } | RunCellState::Released => None,
        }
    }

    pub(crate) fn bind_child(&self, parent: RunKey) -> Result<(), AppError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let RunCellState::Retained { schedule, .. } = &mut *state else {
            return Err(AppError::Closed(
                "child execution ownership was released".into(),
            ));
        };
        match &schedule.driver {
            RunDriver::Idle => {
                schedule.driver = RunDriver::Child {
                    parent,
                    pending: Some(Duration::ZERO),
                };
                Ok(())
            }
            RunDriver::Child {
                parent: current, ..
            } if current == &parent => Ok(()),
            RunDriver::Child { .. } | RunDriver::Driving { .. } | RunDriver::Exclusive { .. } => {
                Err(AppError::Conflict(
                    "child already has another execution owner".into(),
                ))
            }
        }
    }

    pub(crate) fn child_parent(&self) -> Option<RunKey> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let RunCellState::Retained { schedule, .. } = &*state
            && let RunDriver::Child { parent, .. } = &schedule.driver
        {
            Some(parent.clone())
        } else {
            None
        }
    }

    pub(crate) fn release_child(&self, parent: &RunKey) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let RunCellState::Retained { schedule, .. } = &mut *state
            && matches!(&schedule.driver, RunDriver::Child { parent: current, .. } if current == parent)
        {
            schedule.driver = RunDriver::Idle;
        }
    }

    pub(crate) fn complete_drive(&self) -> Option<Duration> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match &mut *state {
            RunCellState::Retained { schedule, .. } => schedule.complete(),
            RunCellState::Released => None,
        }
    }

    pub(crate) fn cancel_drive(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let RunCellState::Retained { schedule, .. } = &mut *state {
            match &mut schedule.driver {
                RunDriver::Child { pending, .. } | RunDriver::Exclusive { pending, .. } => {
                    *pending = None
                }
                RunDriver::Idle | RunDriver::Driving { .. } => schedule.driver = RunDriver::Idle,
            }
        }
    }

    pub(crate) fn notifier(&self) -> Result<Arc<Notify>, AppError> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match &*state {
            RunCellState::Retained { .. } => Ok(Arc::clone(&self.notification)),
            RunCellState::Released => Err(AppError::Closed(
                "workflow run runtime ownership is closed".to_owned(),
            )),
        }
    }

    pub(crate) fn wake(&self) {
        self.notification.notify_waiters();
    }

    pub(crate) fn register_interaction(
        &self,
        interaction: Arc<Attempt<A>>,
    ) -> Option<Arc<Attempt<A>>> {
        let sequence = interaction.sequence();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let RunCellState::Retained { interactions, .. } = &mut *state else {
            return None;
        };
        if let Some(current) = interactions.get(&sequence) {
            Some(Arc::clone(current))
        } else {
            interactions.insert(sequence, Arc::clone(&interaction));
            Some(interaction)
        }
    }

    pub(crate) fn interaction(&self, sequence: u64) -> Option<Arc<Attempt<A>>> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match &*state {
            RunCellState::Retained { interactions, .. } => {
                interactions.get(&sequence).map(Arc::clone)
            }
            RunCellState::Released => None,
        }
    }

    pub(crate) fn remove_interaction(&self, sequence: u64) {
        let removed = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match &mut *state {
                RunCellState::Retained { interactions, .. } => interactions.remove(&sequence),
                RunCellState::Released => None,
            }
        };
        drop(removed);
    }

    fn close(&self) {
        let retained = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            std::mem::replace(&mut *state, RunCellState::Released)
        };
        // Release every peer after revoking admission and leaving the cell lock.
        drop(retained);
        self.notification.notify_waiters();
    }
}

impl<A: AgentBackend> RunRegistry<A> {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(RunRegistryState::Open {
                cells: BTreeMap::new(),
                preparations: BTreeMap::new(),
            }),
        }
    }

    pub(crate) fn acquire(&self, key: RunKey) -> Result<Arc<RunCell<A>>, AppError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let RunRegistryState::Open { cells, .. } = &mut *state else {
            return Err(AppError::Closed(
                "application run registry is closed".to_owned(),
            ));
        };
        let cell = cells.entry(key).or_insert_with(|| Arc::new(RunCell::new()));
        cell.retain_owner();
        Ok(Arc::clone(cell))
    }

    pub(crate) fn get(&self, key: &RunKey) -> Option<Arc<RunCell<A>>> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match &*state {
            RunRegistryState::Open { cells, .. } => cells.get(key).map(|cell| {
                cell.retain_owner();
                Arc::clone(cell)
            }),
            RunRegistryState::Closed => None,
        }
    }

    pub(crate) fn release_owner(&self, key: &RunKey, cell: &Arc<RunCell<A>>) {
        let removed = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            let RunRegistryState::Open { cells, .. } = &mut *state else {
                return;
            };
            // Acquiring the next observer and releasing the final original
            // owner share exclusion, including concurrent final handle drops.
            if cells
                .get(key)
                .is_some_and(|registered| Arc::ptr_eq(registered, cell))
                && cell.release_owner()
            {
                cells.remove(key)
            } else {
                None
            }
        };
        if let Some(removed) = removed {
            removed.close();
        }
    }

    pub(crate) fn preparation(
        &self,
        run: &RunKey,
        agent: &str,
        revision: &str,
    ) -> Result<Arc<tokio::sync::Mutex<()>>, AppError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let RunRegistryState::Open {
            cells,
            preparations,
        } = &mut *state
        else {
            return Err(AppError::Closed(
                "application run registry is closed".to_owned(),
            ));
        };
        if !cells.contains_key(run) {
            return Err(AppError::Closed(
                "workflow run preparation ownership was released".to_owned(),
            ));
        }
        let key = (run.workspace.clone(), agent.to_owned(), revision.to_owned());
        // Upgrade while retaining registry exclusion. The returned strong
        // owner prevents a last-permit drop from splitting the same queue.
        if let Some(preparation) = preparations.get(&key).and_then(Weak::upgrade) {
            return Ok(preparation);
        }
        preparations.retain(|_, preparation| preparation.strong_count() != 0);
        if preparations.len() >= MAX_RUNS {
            return Err(AppError::Busy(
                "workflow preparation capacity is exhausted".to_owned(),
            ));
        }
        let preparation = Arc::new(tokio::sync::Mutex::new(()));
        preparations.insert(key, Arc::downgrade(&preparation));
        Ok(preparation)
    }

    pub(crate) fn release(&self, key: &RunKey, cell: &Arc<RunCell<A>>) {
        let removed = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let RunRegistryState::Open { cells, .. } = &mut *state else {
                return;
            };
            if cells
                .get(key)
                .is_some_and(|registered| Arc::ptr_eq(registered, cell))
            {
                cells.remove(key)
            } else {
                None
            }
        };
        if let Some(removed) = removed {
            removed.close();
        }
    }

    pub(crate) fn close(&self) {
        let cells = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match std::mem::replace(&mut *state, RunRegistryState::Closed) {
                RunRegistryState::Open { cells, .. } => cells,
                RunRegistryState::Closed => return,
            }
        };
        for cell in cells.into_values() {
            cell.close();
        }
    }
}

impl<A: AgentBackend> RunCellState<A> {
    fn action_observation(
        &mut self,
        generation: u64,
        context: &ActionContext,
    ) -> Result<&mut ActionObservation, AppError> {
        let Self::Retained { schedule, .. } = self else {
            return Err(AppError::Closed("run action observation is closed".into()));
        };
        if !schedule.owns_lifecycle(generation) {
            return Err(AppError::Closed(
                "run action observation generation changed".into(),
            ));
        }
        schedule
            .observations
            .iter_mut()
            .find(|entry| entry.matches(context))
            .ok_or_else(|| AppError::Closed("run action observation is unavailable".into()))
    }
}

impl RunSchedule {
    fn active_input_generation(&self) -> Option<u64> {
        (self.generation != 0
            && matches!(self.lifecycle, RunLifecycle::Active { .. })
            && !matches!(
                self.driver,
                RunDriver::Exclusive {
                    disposition: ExecutionDisposition::Finalizing | ExecutionDisposition::Dropped,
                    ..
                }
            ))
        .then_some(self.generation)
    }

    fn owns_lifecycle(&self, generation: u64) -> bool {
        self.generation == generation
            && match &self.driver {
                RunDriver::Exclusive {
                    generation: current,
                    ..
                } => *current == generation,
                RunDriver::Driving { .. } | RunDriver::Child { .. } => true,
                RunDriver::Idle => false,
            }
    }

    fn request_cause(&mut self, cause: ScheduleCause) -> (bool, Option<Duration>) {
        match cause {
            ScheduleCause::CheckpointReady(checkpoint) => {
                self.observe_checkpoint(checkpoint);
                (true, self.request(Duration::ZERO))
            }
            ScheduleCause::Reconcile => (true, self.request(Duration::ZERO)),
            ScheduleCause::CheckpointObserved(checkpoint) => {
                self.observe_checkpoint(checkpoint);
                (false, None)
            }
            ScheduleCause::CurrentState => self.request_current(),
            ScheduleCause::StorageFailure(disposition) => self.request_retry(disposition),
        }
    }

    fn observe_checkpoint(&mut self, checkpoint: u64) {
        match self.last_checkpoint {
            Some(last_checkpoint) if checkpoint > last_checkpoint => {
                self.last_checkpoint = Some(checkpoint);
                self.retained = false;
                self.bounded_attempts = 0;
                self.recoverable_attempts = 0;
            }
            None => self.last_checkpoint = Some(checkpoint),
            Some(_) => {}
        }
    }

    fn request(&mut self, delay: Duration) -> Option<Duration> {
        match &mut self.driver {
            RunDriver::Idle => {
                self.mode = self
                    .parent_observation
                    .as_ref()
                    .map_or(ActionMode::Asynchronous, |context| context.mode);
                self.driver = RunDriver::Driving { pending: None };
                Some(delay)
            }
            RunDriver::Driving { pending }
            | RunDriver::Child { pending, .. }
            | RunDriver::Exclusive { pending, .. } => {
                *pending = Some(pending.map_or(delay, |pending| pending.min(delay)));
                None
            }
        }
    }

    fn request_retry(&mut self, disposition: RetryDisposition) -> (bool, Option<Duration>) {
        let attempts = match disposition {
            RetryDisposition::RetainUntilProgress => &mut self.recoverable_attempts,
            RetryDisposition::BoundedTransient => {
                if self.bounded_attempts >= MAX_TRANSIENT_SCHEDULE_ATTEMPTS {
                    return (false, None);
                }
                &mut self.bounded_attempts
            }
        };
        let attempt = (*attempts).min(MAX_TRANSIENT_SCHEDULE_ATTEMPTS - 1);
        let multiplier = 1u32 << u32::from(attempt);
        let delay = TRANSIENT_SCHEDULE_INITIAL_DELAY
            .saturating_mul(multiplier)
            .min(TRANSIENT_SCHEDULE_MAX_DELAY);
        *attempts = attempts.saturating_add(1);
        (true, self.request(delay))
    }

    fn request_current(&mut self) -> (bool, Option<Duration>) {
        if matches!(
            self.driver,
            RunDriver::Driving { pending: Some(_) }
                | RunDriver::Child {
                    pending: Some(_),
                    ..
                }
        ) {
            return (true, None);
        }
        if self.bounded_attempts >= MAX_TRANSIENT_SCHEDULE_ATTEMPTS {
            return (false, None);
        }
        let multiplier = 1u32 << u32::from(self.bounded_attempts);
        let delay = TRANSIENT_SCHEDULE_INITIAL_DELAY
            .saturating_mul(multiplier)
            .min(TRANSIENT_SCHEDULE_MAX_DELAY);
        self.bounded_attempts += 1;
        (true, self.request(delay))
    }

    fn complete(&mut self) -> Option<Duration> {
        match std::mem::take(&mut self.driver) {
            RunDriver::Driving {
                pending: Some(delay),
            } => {
                self.driver = RunDriver::Driving { pending: None };
                Some(delay)
            }
            RunDriver::Driving { pending: None } | RunDriver::Idle => {
                self.driver = RunDriver::Idle;
                None
            }
            exclusive @ RunDriver::Exclusive { .. } => {
                self.driver = exclusive;
                None
            }
            RunDriver::Child { parent, pending } => {
                self.driver = RunDriver::Child {
                    parent,
                    pending: None,
                };
                pending
            }
        }
    }
}
