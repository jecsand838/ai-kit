use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agent_sdk::identity::{ConversationId, NotificationId, WorkspaceId as AgentWorkspaceId};
use agent_sdk::notification::{ConversationNotificationKind, NotificationResolution};
use agent_sdk::prompt::{PromptEvent, PromptRequest, PromptResponse, PromptStream};
use agent_sdk::{AgentBackend, ConversationBackend, SdkError, WorkspaceBackend};
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use storage_sdk::StateStoreBackend;

use crate::workflow::{
    ActionBackend, ActionContext, ActionMode, ChildCompletion, ChildExecutor, ConditionBackend,
    ExecutionOptions, LifecycleEvent, LifecycleHooks, StateTyping,
};
use uuid::Uuid;

use crate::app::{
    AppRecord, AppRuntime, AppTrace, DriverCommand, FinalResult, MAX_AGENTS, MAX_RUNS, RunTaskKind,
};
use crate::storage::{AppCommitOutcome, AppMutation, EPOCH, RecordCodec, RunCopy, StorageFailure};
use crate::workflow::definition::{
    ChildHistory, ChildInitializer, ChildSelection, CompiledDefinition, JsonBoundary,
    MapDeclaration, SchemaText, Step,
};
use crate::workflow::execution::{
    AgentRequest, BspDecision, ExecutionEngine, MAX_FRONTIER, MAX_RUNTIME_BYTES,
    StagedActivationRecord, StagedOutcomeRecord,
};
use crate::workflow::interaction::{Attempt, AttemptObservation};
use crate::workflow::{
    ActionOutcome, CheckpointKind, Interaction, Resolution, RunId, RunSelector, RunSnapshot,
    RunStatus, WorkflowFailure, WorkflowId,
};
use crate::workflow::{
    Checkpoint, CheckpointSelector, HistoryCursor, HistoryPage, RestoreOptions, ResumePosition,
    RunLineage, WorkflowState,
};
use crate::workspace::{WorkspaceRuntime, WorkspaceStateRecord};
use crate::{AppError, WorkspaceId};

mod actor;
mod notification;
pub(crate) mod session;

pub(crate) use notification::{InteractionSettlement, MAX_INTERACTION_SETTLEMENTS};
use notification::{SettlementDecision, SettlementState, SettlementTarget};

pub(crate) use actor::{RetryDisposition, RunCell, RunKey, RunRegistry, ScheduleCause};

pub(crate) const MAX_LIST_LIMIT: usize = 1_000;
pub(crate) const MAX_CHILD_DEPTH: u8 = 64;
const MAX_DRIVER_TRANSITIONS: usize = 1_000_000;
const MAX_RECONCILIATION_ATTEMPTS: usize = 8;
pub(crate) const RECONCILIATION_INITIAL_DELAY: Duration = Duration::from_millis(25);
pub(crate) const RECONCILIATION_MAX_DELAY: Duration = Duration::from_secs(1);
const MAX_ABORT_OBSERVATION: Duration = Duration::from_secs(1);
const MAX_WAIT: Duration = Duration::from_secs(365 * 24 * 60 * 60);

#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub(crate) struct ChildTask {
    pub(crate) workflow: WorkflowId,
    pub(crate) revision: String,
    pub(crate) task_key: String,
    pub(crate) input: Map<String, Value>,
    pub(crate) initializer: Option<ChildInitializer<Value>>,
    pub(crate) history: ChildHistory,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub(crate) struct ChildMembershipRecord {
    pub(crate) key: RunKey,
    pub(crate) revision: String,
    pub(crate) history: ChildHistory,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum RunAdmission {
    Background,
    Prepared,
    Halted {
        node: String,
        result: Option<Map<String, Value>>,
    },
}

enum RunOrigin {
    Fresh {
        options: ExecutionOptions,
        admission: RunAdmission,
    },
    Copied {
        source: Box<RunCopy>,
        checkpoint: CheckpointRecord,
        action_ordinal: Option<u64>,
    },
}

enum AgentPreparation {
    Ready {
        prompt: Box<PromptRequest>,
        workspace_id: AgentWorkspaceId,
    },
    RetrySafe(&'static str),
    Rejected,
    Storage(StorageFailure),
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub(crate) struct RunRecord {
    pub(crate) epoch: String,
    pub(crate) id: RunId,
    pub(crate) occurrence: String,
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) workspace_revision: u64,
    pub(crate) workflow_id: WorkflowId,
    pub(crate) definition_revision: String,
    pub(crate) child_revisions: BTreeMap<WorkflowId, String>,
    pub(crate) agent_revisions: BTreeMap<String, String>,
    pub(crate) checkpoint: CheckpointRecord,
    #[serde(skip)]
    pub(crate) previous: Option<CheckpointRecord>,
    pub(crate) parent: Option<ParentRecord>,
    lineage: Option<LineageRecord>,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct LineageRecord {
    source: RunId,
    source_occurrence: String,
    checkpoint: u64,
    action_ordinal: Option<u64>,
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct CheckpointRecord {
    pub(crate) sequence: u64,
    pub(crate) superstep: u64,
    pub(crate) base_sequence: u64,
    pub(crate) state: Map<String, Value>,
    pub(crate) frontier: Vec<FrontierRecord>,
    pub(crate) settled_joins: BTreeMap<String, usize>,
    pub(crate) staged: Vec<StagedActivationRecord>,
    pub(crate) controls: BTreeMap<u64, ControlRecord>,
    pub(crate) phase: CheckpointPhaseRecord,
    admission: RunAdmission,
    position: Option<ActionPosition>,
    outcomes: Vec<ActionOutcome>,
    // RecordCodec owns the physical page projection and the exact logical
    // inline encoding. Runtime records always contain the hydrated ledger.
    #[serde(skip)]
    settlements: Arc<Vec<InteractionSettlement>>,
    #[serde(skip)]
    settlement_pages: Vec<u64>,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub(crate) struct ActionPosition {
    pub(crate) node: String,
    pub(crate) ordinal: u64,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(crate) enum CheckpointPhaseRecord {
    Routing {
        sources: Vec<FrontierRecord>,
    },
    Active,
    Failing {
        category: FailureCategory,
        message: String,
        observation: Option<ActionPosition>,
        cancellation_checkpoint: Option<u64>,
    },
    Cancelling {
        cancellation_checkpoint: u64,
    },
    Succeeded,
    Failed {
        category: FailureCategory,
        message: String,
        observation: Option<ActionPosition>,
        cancellation_checkpoint: Option<u64>,
    },
    Cancelled {
        cancellation_checkpoint: u64,
    },
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub(crate) struct FrontierRecord {
    pub(crate) node: String,
    pub(crate) arrived: bool,
    pub(crate) lineage: String,
}

#[derive(Clone, Copy, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FailureCategory {
    Evaluation,
    State,
    Agent,
    Action,
    Hook,
    Child,
    Execution,
    Timeout,
    Limit,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub(crate) struct ControlRecord {
    pub(crate) sequence: u64,
    pub(crate) node: String,
    pub(crate) lineage: String,
    pub(crate) deadline_millis: u64,
    pub(crate) state: ControlRecordState,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum ControlRecordState {
    Input {
        message: String,
    },
    Inputs {
        required: Vec<String>,
    },
    Attempt(AttemptRecord),
    Sleep,
    Child {
        inputs: Vec<ChildTask>,
        admitted: Vec<ChildMembershipRecord>,
    },
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub(crate) struct AttemptRecord {
    pub(crate) number: u32,
    pub(crate) identity: Option<AttemptIdentity>,
    pub(crate) phase: AttemptPhaseRecord,
    pub(crate) abort: ReconciliationRecord,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(crate) enum AttemptPhaseRecord {
    Intent,
    Dispatched,
    Waiting {
        notification_id: NotificationId,
    },
    Unknown {
        notification_id: Option<NotificationId>,
    },
    Retry {
        wake_millis: u64,
    },
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub(crate) struct AttemptIdentity {
    pub(crate) workspace_id: AgentWorkspaceId,
    pub(crate) conversation_id: ConversationId,
}

#[derive(Clone, Copy, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReconciliationRecord {
    Idle,
    Intended,
    Applied,
    Unknown,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub(crate) struct ParentRecord {
    pub(crate) depth: u8,
    pub(crate) workflow_id: WorkflowId,
    pub(crate) run_id: RunId,
    pub(crate) occurrence: String,
    pub(crate) control_sequence: u64,
    pub(crate) index: usize,
    pub(crate) base_checkpoint: u64,
}

pub(crate) struct WorkflowRuntime<
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend,
    C: ConditionBackend,
    T: StateTyping,
    H: LifecycleHooks,
    E: ChildExecutor,
> {
    app: Arc<AppRuntime<CS, A, X, C, T, H, E>>,
    workspace: WorkspaceId,
    workspace_revision: Option<u64>,
    id: WorkflowId,
}

pub(crate) struct WorkflowRunRuntime<
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend,
    C: ConditionBackend,
    T: StateTyping,
    H: LifecycleHooks,
    E: ChildExecutor,
> {
    app: Arc<AppRuntime<CS, A, X, C, T, H, E>>,
    key: RunKey,
    cell: Arc<RunCell<A>>,
}

impl<
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend,
    C: ConditionBackend,
    T: StateTyping,
    H: LifecycleHooks,
    E: ChildExecutor,
> Clone for WorkflowRuntime<CS, A, X, C, T, H, E>
{
    fn clone(&self) -> Self {
        Self {
            app: Arc::clone(&self.app),
            workspace: self.workspace.clone(),
            workspace_revision: self.workspace_revision,
            id: self.id.clone(),
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
> Clone for WorkflowRunRuntime<CS, A, X, C, T, H, E>
{
    fn clone(&self) -> Self {
        self.cell.retain_owner();
        Self {
            app: Arc::clone(&self.app),
            key: self.key.clone(),
            cell: Arc::clone(&self.cell),
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
> Drop for WorkflowRunRuntime<CS, A, X, C, T, H, E>
{
    fn drop(&mut self) {
        self.app.run_registry().release_owner(&self.key, &self.cell);
    }
}

#[derive(Clone)]
enum ControlTransition {
    Stage {
        activations: Vec<StagedActivationRecord>,
    },
    Barrier {
        state: Map<String, Value>,
        frontier: Vec<FrontierRecord>,
        settled_joins: BTreeMap<String, usize>,
        superstep: u64,
        outcomes: Vec<ActionOutcome>,
        sources: Vec<FrontierRecord>,
    },
    Routed {
        frontier: Vec<FrontierRecord>,
    },
    Park {
        control: ControlRecord,
    },
    Settlement {
        settlement: InteractionSettlement,
    },
    Resolve {
        settlement: InteractionSettlement,
        activation: StagedActivationRecord,
    },
    Retry {
        settlement: InteractionSettlement,
        control: ControlRecord,
    },
    RetryReady {
        sequence: u64,
    },
    Pause {
        discard_staged: Vec<(String, String)>,
    },
    RetryFailed,
    Halt {
        node: String,
        result: Option<Map<String, Value>>,
    },
    Cancel {
        cancellation_checkpoint: u64,
    },
    Cancelled,
    AttemptSettled {
        sequence: u64,
    },
    Succeeded,
    Failed {
        category: FailureCategory,
        message: String,
        observation: Option<ActionPosition>,
    },
}

pub(crate) struct LoadedRun {
    record: RunRecord,
    revision: u64,
}

pub(crate) enum RuntimeOperationFailure {
    Storage(StorageFailure),
    Application(AppError),
}

impl RuntimeOperationFailure {
    fn is_retryable_conflict(&self) -> bool {
        match self {
            Self::Storage(failure) => failure.is_retryable_conflict(),
            Self::Application(error) => matches!(error, AppError::Conflict(_)),
        }
    }

    pub(crate) fn into_error(self) -> AppError {
        match self {
            Self::Storage(failure) => failure.into_error(),
            Self::Application(error) => error,
        }
    }
}

impl From<AppError> for RuntimeOperationFailure {
    fn from(error: AppError) -> Self {
        Self::Application(error)
    }
}

impl From<StorageFailure> for RuntimeOperationFailure {
    fn from(failure: StorageFailure) -> Self {
        Self::Storage(failure)
    }
}

enum ChildAdmissionReceipt {
    Applied {
        parent: Box<LoadedRun>,
        children: Vec<RunRecord>,
    },
    Rejected(AppError),
    Storage(StorageFailure),
    NotApplied,
    Unknown,
}

impl<
    CS: StateStoreBackend,
    A: AgentBackend,
    X: ActionBackend,
    C: ConditionBackend,
    T: StateTyping,
    H: LifecycleHooks,
    E: ChildExecutor,
> WorkflowRuntime<CS, A, X, C, T, H, E>
{
    pub(crate) fn new(
        app: Arc<AppRuntime<CS, A, X, C, T, H, E>>,
        workspace: WorkspaceId,
        workspace_revision: Option<u64>,
        id: WorkflowId,
    ) -> Self {
        Self {
            app,
            workspace,
            workspace_revision,
            id,
        }
    }

    pub(crate) fn id(&self) -> &WorkflowId {
        &self.id
    }

    pub(crate) async fn start(
        &self,
        mut options: ExecutionOptions,
    ) -> Result<WorkflowRunRuntime<CS, A, X, C, T, H, E>, AppError> {
        let admission = self.app.reserve_caller()?;
        JsonBoundary::validate_inputs(&options.inputs)?;
        self.app.ensure_open().await?;
        let definition = self.app.definition(&self.id)?;
        options.resolve_selectors(&self.app.graph(&definition))?;
        self.admit(
            RunOrigin::Fresh {
                options,
                admission: RunAdmission::Background,
            },
            admission,
        )
        .await
    }

    pub(crate) async fn prepare(
        &self,
        input: Map<String, Value>,
    ) -> Result<WorkflowRunRuntime<CS, A, X, C, T, H, E>, AppError> {
        let admission = self.app.reserve_caller()?;
        self.admit(
            RunOrigin::Fresh {
                options: ExecutionOptions {
                    state: WorkflowState::new(input)?,
                    ..ExecutionOptions::default()
                },
                admission: RunAdmission::Prepared,
            },
            admission,
        )
        .await
    }

    pub(crate) async fn restore(
        &self,
        options: RestoreOptions,
    ) -> Result<WorkflowRunRuntime<CS, A, X, C, T, H, E>, AppError> {
        // Restoration retains several hydrated observations. Keep its concrete
        // future off the caller's stack without changing admission or ownership.
        Box::pin(async move {
            let admission = self.app.reserve_caller()?;
            self.app.admit()?;
            self.workspace_revision
                .ok_or_else(|| AppError::NotFound("workflow workspace does not exist".into()))?;
            let source = self
                .app
                .storage()
                .copy_source(
                    &self.workspace,
                    &self.id,
                    &options.source,
                    (&options.checkpoint, &options.position),
                    |record| self.app.definition_for(record),
                )
                .await
                .map_err(RuntimeOperationFailure::into_error)?;
            let (checkpoint, action_ordinal) = match &source.source {
                Some((selected, _)) => {
                    let definition = self.app.definition_for(selected)?;
                    let action_ordinal = match &options.checkpoint {
                        CheckpointSelector::Action(ordinal) => Some(*ordinal),
                        CheckpointSelector::Latest | CheckpointSelector::Sequence(_) => selected
                            .checkpoint
                            .failed_observation()
                            .or(selected.checkpoint.position.as_ref())
                            .map(|position| position.ordinal),
                    };
                    (
                        selected.restored(
                            &definition,
                            &options.checkpoint,
                            &options.position,
                            options.overrides.values(),
                            source.saved.as_ref().map(|record| &record.checkpoint),
                        )?,
                        action_ordinal,
                    )
                }
                None => {
                    let state = options.fallback.ok_or_else(|| {
                        AppError::NotFound("restore checkpoint does not exist".into())
                    })?;
                    let definition = self.app.definition(&self.id)?;
                    definition.validate_state(state.values())?;
                    let mut checkpoint = CheckpointRecord::admitted(
                        state.values,
                        definition.entry().to_owned(),
                        definition.entry().to_owned(),
                    );
                    checkpoint.admission = RunAdmission::Prepared;
                    (checkpoint, None)
                }
            };
            let origin = RunOrigin::Copied {
                source: Box::new(source),
                checkpoint,
                action_ordinal,
            };
            self.admit(origin, admission).await
        })
        .await
    }

    async fn admit(
        &self,
        origin: RunOrigin,
        admission: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<WorkflowRunRuntime<CS, A, X, C, T, H, E>, AppError> {
        self.workspace_revision
            .ok_or_else(|| AppError::NotFound("workflow workspace does not exist".into()))?;
        let runtime = self.clone();
        let id = RunId::generate();
        let occurrence = Uuid::new_v4().simple().to_string();
        let key = RunKey::new(
            self.workspace.clone(),
            self.id.clone(),
            id.clone(),
            occurrence.clone(),
        );
        let admitted = WorkflowRunRuntime::attach(Arc::clone(&self.app), key.clone())?;
        let generation = admitted.cell.claim_execution(ActionMode::Asynchronous)?;
        let cleanup = admitted.clone();
        let (outcome, observation) = tokio::sync::oneshot::channel();
        if !self
            .app
            .spawn_run(key, RunTaskKind::Foreground { generation }, Some(admission), async move {
                let span = AppTrace::span(|| {
                    tracing::info_span!(target: "app_sdk", "app.workflow.activate",
                    workspace_id = %runtime.workspace, workflow_id = %runtime.id, run_id = %id, occurrence = %occurrence)
                });
                let result = span
                    .observe(std::pin::pin!(
                        runtime.admit_owned(id, occurrence, origin, generation)
                    ))
                    .await;
                if result.is_err() && cleanup.cell.owns_execution(generation) {
                    cleanup.cell.revoke_execution(generation);
                    match cleanup.settle_execution(generation, None).await {
                        Ok((loaded, true)) if loaded.record.checkpoint.is_terminal() => {
                            cleanup.cell.end_publication(generation);
                            cleanup.cell.release_execution(generation);
                        }
                        Err(AppError::NotFound(_)) => {
                            cleanup.cell.end_publication(generation);
                            cleanup.cell.release_execution(generation);
                            cleanup.release();
                        }
                        Err(AppError::Closed(_)) => {
                            cleanup.cell.end_publication(generation);
                            cleanup.cell.release_execution(generation);
                        }
                        _ => {
                            cleanup.app.tasks_failed(FinalResult::Integration(
                                "run admission cleanup authority failed".into(),
                            ));
                        }
                    }
                }
                let _ = outcome.send(result);
            })
        {
            admitted.cell.release_execution(generation);
            admitted.release();
            return Err(AppError::Closed(
                "run admission ownership is closed".to_owned(),
            ));
        }
        observation
            .await
            .map_err(|_| AppError::Closed("run admission observation closed".to_owned()))?
    }

    async fn admit_owned(
        &self,
        id: RunId,
        occurrence: String,
        origin: RunOrigin,
        generation: u64,
    ) -> Result<WorkflowRunRuntime<CS, A, X, C, T, H, E>, AppError> {
        let workspace_revision = self
            .workspace_revision
            .ok_or_else(|| AppError::NotFound("workflow workspace does not exist".into()))?;
        let mut attempts = 0;
        let mut delay = RECONCILIATION_INITIAL_DELAY;
        let record = loop {
            self.app.admit()?;
            let Some((workspace, workspace_storage_revision)) =
                self.app.storage().load_workspace(&self.workspace).await?
            else {
                return Err(AppError::NotFound(
                    "workflow workspace does not exist".to_owned(),
                ));
            };
            workspace.public_config()?;
            if workspace.revision != workspace_revision {
                return Err(AppError::Conflict(
                    "workflow workspace revision is stale".to_owned(),
                ));
            }
            let copied = match &origin {
                RunOrigin::Copied { source, .. } => {
                    source.source.as_ref().map(|(record, _)| record)
                }
                RunOrigin::Fresh { .. } => None,
            };
            let definition = match copied {
                Some(selected) => self.app.definition_for(selected)?,
                None => self.app.definition(&self.id)?,
            };
            let (mut checkpoint, lineage) = match &origin {
                RunOrigin::Fresh { options, admission } => {
                    let mut checkpoint = CheckpointRecord::admitted(
                        definition.validate_input(options.state.values())?,
                        definition.entry().to_owned(),
                        definition.entry().to_owned(),
                    );
                    checkpoint.admission = admission.clone();
                    (checkpoint, None)
                }
                RunOrigin::Copied {
                    checkpoint,
                    action_ordinal,
                    ..
                } => (
                    checkpoint.clone(),
                    copied.map(|selected| LineageRecord {
                        source: selected.id.clone(),
                        source_occurrence: selected.occurrence.clone(),
                        checkpoint: selected.checkpoint.sequence,
                        action_ordinal: *action_ordinal,
                    }),
                ),
            };
            let state = WorkflowState::new(std::mem::take(&mut checkpoint.state))?;
            // No run or App mutation exists yet. Caller validation cannot lose
            // an admitted receipt or invoke work on the copied boundary.
            catch_unwind(AssertUnwindSafe(|| {
                state.validate_typing(self.app.typing())
            }))
            .map_err(|_| {
                AppError::Integration("initial workflow state validation panicked".into())
            })??;
            checkpoint.state = state.values;
            let (mut app_record, app_revision) = self.app.admission_record().await?;
            let child_revisions = match copied {
                Some(selected) => selected.child_revisions.clone(),
                None => self.app.child_revisions(&definition)?,
            };
            let agent_revisions = match copied {
                Some(selected) => selected.agent_revisions.clone(),
                None => {
                    self.app
                        .agent_revisions(&definition, &child_revisions, &app_record.agents)?
                }
            };
            if !workspace.has_agents(&agent_revisions)? {
                return Err(AppError::Conflict(
                    "workflow agent revisions are not materialized in this workspace".into(),
                ));
            }
            let record = RunRecord {
                epoch: EPOCH.to_owned(),
                id: id.clone(),
                occurrence: occurrence.clone(),
                workspace_id: self.workspace.clone(),
                workspace_revision,
                workflow_id: self.id.clone(),
                definition_revision: definition.revision().to_owned(),
                child_revisions,
                agent_revisions,
                checkpoint,
                previous: None,
                parent: None,
                lineage,
            };
            if app_record.run_count >= MAX_RUNS {
                return Err(AppError::Busy(
                    "application run capacity is exhausted".to_owned(),
                ));
            }
            app_record.run_count = app_record
                .run_count
                .checked_add(1)
                .ok_or_else(|| AppError::Conflict("run capacity is exhausted".to_owned()))?;
            let mutation = match &origin {
                RunOrigin::Fresh { .. } => AppMutation::admit_run(
                    app_revision,
                    workspace_storage_revision,
                    &app_record,
                    &workspace,
                    &record,
                )?,
                RunOrigin::Copied { source, .. } => AppMutation::admit_restore(
                    app_revision,
                    workspace_storage_revision,
                    &app_record,
                    &workspace,
                    &record,
                    source,
                )?,
            };
            match self.app.storage().commit(mutation).await {
                AppCommitOutcome::Applied => {}
                AppCommitOutcome::Rejected(failure)
                    if failure.is_retryable_conflict()
                        && attempts + 1 < MAX_RECONCILIATION_ATTEMPTS =>
                {
                    // Copy selection and absence/cleanliness guards remain
                    // fixed while ordinary App/workspace transaction fences retry.
                    attempts += 1;
                    tokio::time::sleep(delay).await;
                    delay = delay.saturating_mul(2).min(RECONCILIATION_MAX_DELAY);
                    continue;
                }
                AppCommitOutcome::Rejected(failure) => return Err(failure.into_error()),
                AppCommitOutcome::Unknown(_) => {
                    if let Ok(runtime) = WorkflowRunRuntime::attach(
                        Arc::clone(&self.app),
                        RunKey::new(
                            self.workspace.clone(),
                            self.id.clone(),
                            id.clone(),
                            occurrence.clone(),
                        ),
                    ) {
                        runtime.schedule(ScheduleCause::Reconcile);
                    }
                    let target = self
                        .app
                        .storage()
                        .load_run(&RunKey::new(
                            self.workspace.clone(),
                            self.id.clone(),
                            id.clone(),
                            occurrence.clone(),
                        ))
                        .await?;
                    let before = self.app.storage().load_app().await?;
                    let inventory = self.app.storage().list_all_runs().await?;
                    let after = self.app.storage().load_app().await?;
                    let applied = match &target {
                        Some((loaded, _)) => loaded.same_persisted_row(&record)?,
                        None => false,
                    } && matches!(
                        (before, after),
                        (Some((ref before, before_revision)), Some((ref after, after_revision)))
                            if before_revision == after_revision
                                && before == after
                                && before.accounts_for_inventory(&app_record, inventory.len())
                    );
                    if !applied {
                        return Err(AppError::Integration(
                            "run admission outcome is unknown".to_owned(),
                        ));
                    }
                }
            }
            break record;
        };
        AppTrace::event(|| {
            tracing::info!(
                target: "app_sdk",
                event = "app.workflow.activate",
                workspace_id = %self.workspace,
                workflow_id = %self.id,
                run_id = %id,
                occurrence = %record.occurrence,
                checkpoint = 0_u64,
            )
        });
        let runtime = WorkflowRunRuntime::attach(Arc::clone(&self.app), record.key())?;
        runtime
            .observe_registered(&record, None, generation)
            .await?;
        if let RunOrigin::Fresh { mut options, .. } = origin {
            options.state = WorkflowState::default();
            runtime.cell.configure_execution(generation, options)?;
        }
        let scheduled = runtime.cell.release_execution(generation);
        if !record.checkpoint.is_paused() {
            runtime.schedule(ScheduleCause::CheckpointReady(record.checkpoint.sequence));
        } else if scheduled {
            runtime.schedule(ScheduleCause::Reconcile);
        }
        Ok(runtime)
    }

    pub(crate) async fn open_run(
        &self,
        selector: RunSelector,
    ) -> Result<WorkflowRunRuntime<CS, A, X, C, T, H, E>, AppError> {
        selector.validate()?;
        self.app.ensure_open().await?;
        let key = self
            .app
            .storage()
            .select_run(&self.workspace, &self.id, &selector)
            .await?
            .ok_or_else(|| AppError::NotFound("workflow run occurrence does not exist".into()))?;
        WorkflowRunRuntime::open(Arc::clone(&self.app), key)
            .await
            .map_err(RuntimeOperationFailure::into_error)
    }

    pub(crate) async fn list_runs(
        &self,
        after: Option<RunSelector>,
        limit: NonZeroUsize,
    ) -> Result<Vec<RunSnapshot>, AppError> {
        self.app.ensure_open().await?;
        if limit.get() > MAX_LIST_LIMIT {
            return Err(AppError::InvalidInput(
                "run list limit exceeds the private bound".to_owned(),
            ));
        }
        let records = self
            .app
            .storage()
            .list_runs(&self.workspace, &self.id, after.as_ref(), limit)
            .await?;
        let mut output = Vec::new();
        for record in records {
            output.push(record.project_snapshot(self.app.definition_for(&record)?.as_ref())?);
        }
        self.app.current_generation_record().await?;
        Ok(output)
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
    pub(crate) fn attach(
        app: Arc<AppRuntime<CS, A, X, C, T, H, E>>,
        key: RunKey,
    ) -> Result<Self, AppError> {
        let cell = app.run_registry().acquire(key.clone())?;
        Ok(Self { app, key, cell })
    }

    pub(crate) fn existing(
        app: Arc<AppRuntime<CS, A, X, C, T, H, E>>,
        key: RunKey,
    ) -> Option<Self> {
        let cell = app.run_registry().get(&key)?;
        Some(Self { app, key, cell })
    }

    pub(crate) fn attach_record(
        app: Arc<AppRuntime<CS, A, X, C, T, H, E>>,
        record: &RunRecord,
    ) -> Result<Self, AppError> {
        let runtime = Self::attach(app, record.key())?;
        if !record.checkpoint.is_terminal()
            && let Some(parent) = &record.parent
        {
            runtime.cell.bind_child(RunKey::new(
                record.workspace_id.clone(),
                parent.workflow_id.clone(),
                parent.run_id.clone(),
                parent.occurrence.clone(),
            ))?;
        }
        Ok(runtime)
    }

    pub(crate) async fn open(
        app: Arc<AppRuntime<CS, A, X, C, T, H, E>>,
        key: RunKey,
    ) -> Result<Self, RuntimeOperationFailure> {
        let loaded = Self::load_scoped_for_recovery(&app, &key)
            .await?
            .ok_or_else(|| AppError::NotFound("workflow run does not exist".to_owned()))?;
        let retained = loaded.record.checkpoint.is_terminal()
            && !tokio::time::timeout_at(LifecycleEvent::deadline(None), async {
                let mut attempts = 0;
                let mut delay = RECONCILIATION_INITIAL_DELAY;
                loop {
                    match app.storage().run_is_active(&loaded.record).await {
                        Err(StorageFailure::Recoverable(AppError::Busy(_)))
                            if attempts + 1 < MAX_RECONCILIATION_ATTEMPTS =>
                        {
                            attempts += 1;
                            tokio::time::sleep(delay).await;
                            delay = delay.saturating_mul(2).min(RECONCILIATION_MAX_DELAY);
                        }
                        observed => break observed,
                    }
                }
            })
            .await
            .map_err(|_| AppError::Timeout("retained run observation deadline expired".into()))??;
        let runtime = Self::attach(app, loaded.record.key())?;
        if retained {
            runtime.cell.retained(loaded.record.checkpoint.sequence);
        }
        Ok(runtime)
    }

    pub(crate) fn id(&self) -> &RunId {
        self.key.run()
    }

    pub(crate) fn key(&self) -> &RunKey {
        &self.key
    }

    pub(crate) fn occurrence(&self) -> &str {
        self.key.occurrence()
    }

    async fn observe_registered(
        &self,
        record: &RunRecord,
        parent: Option<&ActionContext>,
        generation: u64,
    ) -> Result<(), AppError> {
        if !self.cell.execution_is_live(generation) {
            return Err(AppError::Closed(
                "run registration ownership was revoked".into(),
            ));
        }
        let definition = self.app.definition_for(record)?;
        let graph = self.app.graph(&definition);
        let lineage = record.lineage.as_ref().map(|lineage| RunLineage {
            source: lineage.source.clone(),
            source_occurrence: lineage.source_occurrence.clone(),
            checkpoint: lineage.checkpoint,
            action_ordinal: lineage.action_ordinal,
        });
        let event = LifecycleEvent::Registered {
            workspace: &record.workspace_id,
            run: &record.id,
            occurrence: &record.occurrence,
            graph: &graph,
            parent,
            lineage: lineage.as_ref(),
        };
        if let Err(failure) = event
            .deliver(
                self.app.hooks(),
                parent.map_or(ActionMode::Asynchronous, |context| context.mode),
                LifecycleEvent::deadline(None),
            )
            .await
        {
            let error = failure.application_error();
            let settled = async {
                // The rejected callback does not carry a current native
                // revision. Retain its original owner through this bounded
                // post-callback authority read before admitting settlement.
                let loaded = self
                    .load_execution_observation(None, None)
                    .await
                    .map_err(StorageFailure::into_error)?
                    .ok_or_else(|| AppError::NotFound("workflow run does not exist".into()))?;
                if !loaded.record.checkpoint.is_stopping()
                    && !loaded.record.checkpoint.is_terminal()
                {
                    // Admission is already visible. Keep its identity discoverable
                    // and settle it without inventing an attempted action ordinal.
                    self.commit_failure(
                        loaded,
                        FailureCategory::from_failure(&failure),
                        failure.message().to_owned(),
                    )
                    .await?;
                }
                self.cell.revoke_execution(generation);
                let (loaded, retained) = self.settle_execution(generation, None).await?;
                if !loaded.record.checkpoint.is_terminal() || !retained {
                    return Err(AppError::Integration(
                        "registration cleanup remains unresolved".into(),
                    ));
                }
                Ok::<(), AppError>(())
            }
            .await;
            match settled {
                Ok(()) => {
                    self.cell.end_publication(generation);
                    self.cell.release_execution(generation);
                }
                Err(AppError::Closed(_)) => {
                    // These cleanup paths report Closed only for a proven
                    // generation fence or an already revoked runtime owner.
                    self.cell.end_publication(generation);
                    self.cell.release_execution(generation);
                }
                Err(_) => {
                    // Revoke the existing owner before another execution can
                    // observe a runnable admission whose registration failed.
                    // There must be no await between this revocation and sending
                    // the primary error from the accepted admission operation.
                    self.app.tasks_failed(FinalResult::Integration(
                        "run registration cleanup authority failed".into(),
                    ));
                }
            }
            return Err(error);
        }
        Ok(())
    }

    fn workspace(&self) -> &WorkspaceId {
        self.key.workspace()
    }

    pub(crate) fn schedule(&self, cause: ScheduleCause) -> bool {
        if self.app.ensure_runtime_owned().is_err() {
            return false;
        }
        let (accepted, start_after) = self.cell.schedule(cause);
        let Some(delay) = start_after else {
            if accepted {
                self.app.mark_runtime_changed();
            }
            return accepted;
        };
        let started = self
            .app
            .spawn_driver(self.key.clone(), delay, DriverCommand::Scheduled);
        if started {
            self.app.mark_runtime_changed();
            accepted
        } else {
            self.cell.cancel_drive();
            false
        }
    }

    pub(crate) async fn drive_scheduled(self, mut delay: Duration) {
        loop {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            self.clone().drive().await;
            let Some(next_delay) = self.cell.complete_drive() else {
                self.app.mark_runtime_changed();
                break;
            };
            delay = next_delay;
        }
    }

    pub(crate) fn task_failed(&self, kind: RunTaskKind) {
        if matches!(kind, RunTaskKind::Driver) {
            self.cell.cancel_drive();
        }
        if matches!(kind, RunTaskKind::Reconciliation) {
            self.schedule(ScheduleCause::StorageFailure(
                RetryDisposition::BoundedTransient,
            ));
            return;
        }
        let runtime = self.clone();
        self.app.spawn_run(
            self.key.clone(),
            RunTaskKind::Reconciliation,
            None,
            async move {
                runtime.reconcile_task_failure(kind).await;
                if let RunTaskKind::Foreground { generation } = kind {
                    runtime.cell.end_publication(generation);
                    let _ = runtime.finish_execution(generation, None).await;
                }
            },
        );
    }

    fn reconcile_child_panic(
        &self,
        parent_deadline: Option<tokio::time::Instant>,
    ) -> tokio::sync::oneshot::Receiver<Result<LoadedRun, AppError>> {
        self.cell.cancel_drive();
        let runtime = self.clone();
        let (completion, receipt) = tokio::sync::oneshot::channel();
        self.app.spawn_run(
            self.key.clone(),
            RunTaskKind::Reconciliation,
            None,
            async move {
                let result = AssertUnwindSafe(async {
                    let deadline = LifecycleEvent::deadline(parent_deadline);
                    let mut delay = RECONCILIATION_INITIAL_DELAY;
                    for attempt in 0..MAX_RECONCILIATION_ATTEMPTS {
                        runtime.app.ensure_runtime_owned()?;
                        if runtime.cell.child_parent().is_none() {
                            return Err(AppError::Closed(
                                "child panic ownership was revoked".into(),
                            ));
                        }
                        let loaded = tokio::time::timeout_at(deadline, runtime.load_for_recovery())
                            .await
                            .map_err(|_| {
                                AppError::Timeout(
                                    "child panic reconciliation read timed out".into(),
                                )
                            })?;
                        let result = match loaded {
                            Ok(Some(loaded)) if loaded.record.checkpoint.is_terminal() => {
                                return Ok(loaded);
                            }
                            Ok(Some(loaded)) => {
                                runtime
                                    .commit_failure_for_recovery(
                                        loaded,
                                        FailureCategory::Execution,
                                        "workflow owned task panicked".into(),
                                    )
                                    .await
                            }
                            Ok(None) => {
                                return Err(AppError::NotFound(
                                    "panicked child is unavailable".into(),
                                ));
                            }
                            Err(failure) => Err(RuntimeOperationFailure::Storage(failure)),
                        };
                        match result {
                            Ok(loaded) => return Ok(loaded),
                            Err(failure) => {
                                let retry = match &failure {
                                    RuntimeOperationFailure::Storage(failure) => {
                                        failure.is_recoverable() || failure.is_retryable_conflict()
                                    }
                                    RuntimeOperationFailure::Application(error) => matches!(
                                        error,
                                        AppError::Conflict(_)
                                            | AppError::Busy(_)
                                            | AppError::Timeout(_)
                                    ),
                                };
                                if !retry
                                    || attempt + 1 == MAX_RECONCILIATION_ATTEMPTS
                                    || tokio::time::Instant::now() >= deadline
                                {
                                    return Err(failure.into_error());
                                }
                            }
                        }
                        tokio::time::sleep_until(
                            (tokio::time::Instant::now() + delay).min(deadline),
                        )
                        .await;
                        delay = delay.saturating_mul(2).min(RECONCILIATION_MAX_DELAY);
                    }
                    unreachable!("bounded reconciliation returns on its final attempt")
                })
                .catch_unwind()
                .await
                .unwrap_or_else(|_| {
                    Err(AppError::Integration(
                        "child panic reconciliation panicked".into(),
                    ))
                });
                if result
                    .as_ref()
                    .is_err_and(|error| !matches!(error, AppError::Closed(_)))
                {
                    runtime.app.tasks_failed(FinalResult::Integration(
                        "child panic reconciliation lost durable authority".into(),
                    ));
                }
                let _ = completion.send(result);
            },
        );
        receipt
    }

    async fn reconcile_task_failure(&self, kind: RunTaskKind) {
        let loaded = match self.load_execution_observation(None, None).await {
            Ok(Some(loaded)) => loaded,
            Ok(None) => return,
            Err(failure) => {
                self.retain_execution_read_failure(&failure);
                self.schedule_after_storage_failure(&failure);
                return;
            }
        };
        if loaded.record.checkpoint.is_terminal() {
            self.schedule(ScheduleCause::CheckpointReady(
                loaded.record.checkpoint.sequence,
            ));
            return;
        }
        let sequence = match kind {
            RunTaskKind::Control { sequence } => sequence,
            RunTaskKind::Child { sequence } => {
                if let Some(control) = loaded.record.checkpoint.control(sequence) {
                    self.commit_scheduled_transition(
                        control,
                        ControlTransition::Failed {
                            category: FailureCategory::Execution,
                            message: "child execution task panicked".into(),
                            observation: None,
                        },
                    )
                    .await;
                }
                return;
            }
            RunTaskKind::Resolution {
                control_sequence,
                target_sequence,
                generation,
            } => {
                let Some(mut settlement) = loaded
                    .record
                    .checkpoint
                    .settlement(target_sequence, generation)
                    .cloned()
                else {
                    return;
                };
                let Some(control) = loaded.record.checkpoint.control(control_sequence) else {
                    return;
                };
                if matches!(settlement.state, SettlementState::Intended { .. })
                    && settlement.settle(false, &loaded.record.checkpoint).is_ok()
                {
                    self.commit_scheduled_transition(
                        control,
                        ControlTransition::Settlement { settlement },
                    )
                    .await;
                }
                return;
            }
            _ => {
                let _ = self
                    .commit_failure(
                        loaded,
                        FailureCategory::Execution,
                        "workflow owned task panicked".into(),
                    )
                    .await;
                return;
            }
        };
        let Some(mut control) = loaded.record.checkpoint.control(sequence).cloned() else {
            return;
        };
        if matches!(
            &control.state,
            ControlRecordState::Attempt(AttemptRecord {
                phase: AttemptPhaseRecord::Intent,
                ..
            })
        ) && self
            .interaction(sequence)
            .is_some_and(|attempt| !attempt.may_have_created_conversation())
        {
            self.remove_interaction(sequence);
            self.settle_unsubmitted_failure(
                control,
                FailureCategory::Execution,
                "agent preparation task panicked before conversation creation",
            )
            .await;
            return;
        }
        let owner = control.clone();
        if let ControlRecordState::Attempt(attempt) = &mut control.state {
            let notification_id = match &attempt.phase {
                AttemptPhaseRecord::Waiting { notification_id } => Some(notification_id.clone()),
                AttemptPhaseRecord::Unknown { notification_id } => notification_id.clone(),
                _ => None,
            };
            if matches!(attempt.abort, ReconciliationRecord::Intended) {
                attempt.abort = ReconciliationRecord::Unknown;
            }
            attempt.phase = AttemptPhaseRecord::Unknown { notification_id };
            match self
                .commit_control_transition(&owner, ControlTransition::Park { control }, None)
                .await
            {
                Ok(Some(loaded)) => {
                    self.remove_interaction(sequence);
                    self.schedule(ScheduleCause::CheckpointReady(
                        loaded.record.checkpoint.sequence,
                    ));
                }
                Ok(None) => {}
                Err(failure) => {
                    self.schedule_after_storage_failure(&failure);
                }
            }
        } else {
            self.commit_scheduled_transition(
                &owner,
                ControlTransition::Stage {
                    activations: vec![control.failed_activation(
                        FailureCategory::Execution,
                        "workflow control task panicked".into(),
                    )],
                },
            )
            .await;
        }
    }

    pub(crate) fn notifier(&self) -> Result<Arc<tokio::sync::Notify>, AppError> {
        self.cell.notifier()
    }

    pub(crate) fn wake(&self) {
        self.cell.wake();
        self.app.mark_runtime_changed();
    }

    pub(crate) fn register_interaction(
        &self,
        interaction: Arc<Attempt<A>>,
    ) -> Option<Arc<Attempt<A>>> {
        let registered = self.cell.register_interaction(interaction);
        if registered.is_some() {
            self.app.mark_runtime_changed();
        }
        registered
    }

    pub(crate) fn interaction(&self, sequence: u64) -> Option<Arc<Attempt<A>>> {
        self.cell.interaction(sequence)
    }

    pub(crate) fn remove_interaction(&self, sequence: u64) {
        self.cell.remove_interaction(sequence);
        self.app.mark_runtime_changed();
    }

    pub(crate) fn release(&self) {
        self.app.run_registry().release(&self.key, &self.cell);
        self.app.mark_runtime_changed();
    }

    pub(crate) async fn snapshot(&self) -> Result<RunSnapshot, AppError> {
        self.app.ensure_snapshot_read()?;
        let loaded = self.load().await?;
        let snapshot = self.project(&loaded.record)?;
        self.app.ensure_snapshot_read()?;
        Ok(snapshot)
    }

    pub(crate) async fn checkpoint(
        &self,
        selector: CheckpointSelector,
    ) -> Result<Checkpoint, AppError> {
        self.app.ensure_snapshot_read()?;
        let current = self.load().await?;
        let definition = self.app.definition_for(&current.record)?;
        let (record, _) = self
            .app
            .storage()
            .checkpoint(&current.record, &selector, &definition)
            .await
            .map_err(RuntimeOperationFailure::into_error)?
            .ok_or_else(|| AppError::NotFound("workflow checkpoint does not exist".into()))?;
        let checkpoint = self.project_history(&record)?;
        self.app.current_generation_revision().await?;
        self.app.ensure_snapshot_read()?;
        Ok(checkpoint)
    }

    pub(crate) async fn history(
        &self,
        after: Option<HistoryCursor>,
        limit: NonZeroUsize,
    ) -> Result<HistoryPage, AppError> {
        if limit.get() > MAX_LIST_LIMIT {
            return Err(AppError::InvalidInput(
                "history page limit exceeds the private bound".into(),
            ));
        }
        self.app.ensure_snapshot_read()?;
        let current = self.load().await?;
        let (application, _) = self.app.current_generation_record().await?;
        let (records, next) = self
            .app
            .storage()
            .history(
                &current.record,
                application.generation,
                after.as_ref(),
                limit,
            )
            .await?;
        let checkpoints = records
            .iter()
            .map(|record| self.project_history(record))
            .collect::<Result<Vec<_>, _>>()?;
        self.app.current_generation_revision().await?;
        self.app.ensure_snapshot_read()?;
        Ok(HistoryPage { checkpoints, next })
    }

    fn project_history(&self, record: &RunRecord) -> Result<Checkpoint, AppError> {
        let definition = self.app.definition_for(record)?;
        record.validate_recovery(&definition)?;
        record.project_checkpoint(definition.revision(), record.checkpoint.lifecycle_status())
    }

    pub(crate) async fn wait(&self) -> Result<RunSnapshot, AppError> {
        let notifier = self.notifier()?;
        let mut notified = Box::pin(notifier.notified());
        let deadline = tokio::time::Instant::now()
            .checked_add(MAX_WAIT)
            .ok_or_else(|| AppError::Timeout("run wait deadline is invalid".to_owned()))?;
        loop {
            notified.as_mut().enable();
            self.app.ensure_runtime_owned()?;
            let loaded = match self.load().await {
                Ok(loaded) => loaded,
                Err(error @ AppError::NotFound(_)) => {
                    self.release();
                    return Err(error);
                }
                Err(error) => return Err(error),
            };
            if loaded.record.checkpoint.is_public_outcome()
                && self.cell.lifecycle_observation(
                    loaded.record.checkpoint.sequence,
                    !loaded.record.checkpoint.controls.is_empty()
                        && !loaded.record.checkpoint.is_terminal(),
                )?
            {
                return self.project(&loaded.record);
            }
            tokio::select! {
                _ = &mut notified => {
                    notified.set(notifier.notified());
                }
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(AppError::Timeout("run observation timed out".to_owned()));
                }
            }
        }
    }

    pub(crate) async fn delete(&self, expected_checkpoint: u64) -> Result<(), AppError> {
        let admission = self.app.reserve_caller()?;
        self.app.ensure_open().await?;
        let runtime = self.clone();
        let (outcome, observation) = tokio::sync::oneshot::channel();
        if !self.app.spawn_run(
            self.key.clone(),
            RunTaskKind::Operation,
            Some(admission),
            async move {
                let _ = outcome.send(runtime.delete_owned(expected_checkpoint).await);
            },
        ) {
            return Err(AppError::Closed("run deletion ownership is closed".into()));
        }
        observation
            .await
            .map_err(|_| AppError::Closed("run deletion observation closed".into()))?
    }

    async fn delete_owned(&self, expected_checkpoint: u64) -> Result<(), AppError> {
        let mut delay = RECONCILIATION_INITIAL_DELAY;
        for attempt in 0..MAX_RECONCILIATION_ATTEMPTS {
            self.app.current_generation_record().await?;
            if let Some(sequence) = self
                .app
                .storage()
                .deletion_checkpoint(&self.key)
                .await
                .map_err(StorageFailure::into_error)?
            {
                if sequence != expected_checkpoint {
                    return Err(AppError::Conflict(
                        "run deletion checkpoint fence is stale".into(),
                    ));
                }
                return self.resume_deletion().await;
            }
            let loaded = self.load().await?;
            if loaded.record.checkpoint.sequence != expected_checkpoint {
                return Err(AppError::Conflict(
                    "run deletion checkpoint fence is stale".into(),
                ));
            }
            if !loaded.record.checkpoint.is_terminal() {
                return Err(AppError::Busy(
                    "only terminal run history can be deleted".into(),
                ));
            }
            match self.app.storage().run_is_active(&loaded.record).await {
                Ok(true) => Box::pin(self.maintain_retention(Some(loaded))).await,
                Ok(false) => {
                    let parent = match &loaded.record.parent {
                        Some(relation) => {
                            self.app
                                .storage()
                                .load_run(&RunKey::new(
                                    loaded.record.workspace_id.clone(),
                                    relation.workflow_id.clone(),
                                    relation.run_id.clone(),
                                    relation.occurrence.clone(),
                                ))
                                .await?
                        }
                        None => None,
                    };
                    if let Some((parent, _)) = &parent
                        && !parent.checkpoint.is_terminal()
                    {
                        let definition = self.app.definition_for(parent)?;
                        parent.validate_recovery(&definition)?;
                        if parent.admitted_child_keys().any(|key| key == &self.key) {
                            loaded.record.validate_child(parent, &definition)?;
                            return Err(AppError::Busy(
                                "active parent still owns this terminal child".into(),
                            ));
                        }
                    }
                    let (_, revision) = self.app.current_generation_record().await?;
                    let mutation = AppMutation::seal_run(
                        revision,
                        loaded.revision,
                        &loaded.record,
                        parent
                            .as_ref()
                            .map(|(record, revision)| (record, *revision)),
                    )?;
                    match self.app.storage().commit(mutation).await {
                        AppCommitOutcome::Applied => return self.resume_deletion().await,
                        AppCommitOutcome::Rejected(failure)
                            if !failure.is_recoverable() && !failure.is_retryable_conflict() =>
                        {
                            return Err(failure.into_error());
                        }
                        AppCommitOutcome::Rejected(failure)
                        | AppCommitOutcome::Unknown(failure) => {
                            if let Some(sequence) = self
                                .app
                                .storage()
                                .deletion_checkpoint(&self.key)
                                .await
                                .map_err(StorageFailure::into_error)?
                            {
                                if sequence != expected_checkpoint {
                                    return Err(AppError::Conflict(
                                        "run deletion checkpoint fence is stale".into(),
                                    ));
                                }
                                return self.resume_deletion().await;
                            }
                            if attempt + 1 == MAX_RECONCILIATION_ATTEMPTS {
                                return Err(failure.into_error());
                            }
                        }
                    }
                }
                Err(failure)
                    if failure.is_recoverable() && attempt + 1 < MAX_RECONCILIATION_ATTEMPTS => {}
                Err(failure) => return Err(failure.into_error()),
            }
            tokio::time::sleep(delay).await;
            delay = delay.saturating_mul(2).min(RECONCILIATION_MAX_DELAY);
        }
        Err(AppError::Busy(
            "terminal run capacity release has not settled".into(),
        ))
    }

    pub(crate) async fn resume_deletion(&self) -> Result<(), AppError> {
        let mut failures = 0;
        let mut delay = RECONCILIATION_INITIAL_DELAY;
        loop {
            let (_, revision) = self.app.current_generation_record().await?;
            let mutation = self
                .app
                .storage()
                .delete_run_page(revision, &self.key)
                .await;
            let outcome = match mutation {
                Ok(Some(mutation)) => self.app.storage().commit(mutation).await,
                Ok(None) => {
                    // This existing read observes completion of the native
                    // deletion owner; sealing or submitting a page is not proof.
                    AppTrace::event(|| {
                        tracing::info!(
                            target: "app_sdk", event = "app.workflow.delete",
                            run_id = %self.id(), occurrence = %self.occurrence(), transition = "completed",
                        )
                    });
                    self.app.run_registry().release(&self.key, &self.cell);
                    self.app.mark_runtime_changed();
                    return Ok(());
                }
                Err(failure) => AppCommitOutcome::Rejected(failure),
            };
            match outcome {
                AppCommitOutcome::Applied => {
                    failures = 0;
                    delay = RECONCILIATION_INITIAL_DELAY;
                }
                AppCommitOutcome::Rejected(failure) | AppCommitOutcome::Unknown(failure) => {
                    failures += 1;
                    if (!failure.is_recoverable() && !failure.is_retryable_conflict())
                        || failures >= MAX_RECONCILIATION_ATTEMPTS
                    {
                        // The sealed head/progress remains the next generation's
                        // authority when this observer cannot prove completion.
                        return Err(failure.into_error());
                    }
                    tokio::time::sleep(delay).await;
                    delay = delay.saturating_mul(2).min(RECONCILIATION_MAX_DELAY);
                }
            }
        }
    }

    pub(crate) async fn cancel(&self, expected_checkpoint: u64) -> Result<RunSnapshot, AppError> {
        let admission = self.app.reserve_caller()?;
        let runtime = self.clone();
        let (outcome, observation) = tokio::sync::oneshot::channel();
        if !self.app.spawn_run(
            self.key.clone(),
            RunTaskKind::Operation,
            Some(admission),
            async move {
                let _ = outcome.send(runtime.cancel_owned(expected_checkpoint).await);
            },
        ) {
            return Err(AppError::Closed(
                "run cancellation ownership is closed".to_owned(),
            ));
        }
        observation
            .await
            .map_err(|_| AppError::Closed("run cancellation observation closed".to_owned()))?
    }

    async fn cancel_owned(&self, expected_checkpoint: u64) -> Result<RunSnapshot, AppError> {
        let loaded = self.load().await?;
        let cancellation_checkpoint = match loaded.record.checkpoint.cancellation_checkpoint() {
            Some(cancellation_checkpoint) if cancellation_checkpoint == expected_checkpoint => {
                cancellation_checkpoint
            }
            Some(_) => {
                return Err(AppError::Conflict(
                    "run cancellation checkpoint fence does not match".to_owned(),
                ));
            }
            None if loaded.record.checkpoint.sequence == expected_checkpoint => expected_checkpoint,
            None => {
                return Err(AppError::Conflict(
                    "run checkpoint fence is stale".to_owned(),
                ));
            }
        };
        self.request_cancel(loaded, cancellation_checkpoint)
            .await
            .map_err(RuntimeOperationFailure::into_error)
    }

    pub(crate) async fn cancel_unfenced(&self) -> Result<RunSnapshot, RuntimeOperationFailure> {
        let mut loaded = self
            .load_for_recovery()
            .await?
            .ok_or_else(|| AppError::NotFound("workflow run does not exist".to_owned()))?;
        for attempt in 0..MAX_RECONCILIATION_ATTEMPTS {
            if loaded.record.checkpoint.is_terminal() {
                return self
                    .project(&loaded.record)
                    .map_err(RuntimeOperationFailure::from);
            }
            let observed = loaded.record.checkpoint.sequence;
            let cancellation_checkpoint = loaded
                .record
                .checkpoint
                .cancellation_checkpoint()
                .unwrap_or(observed);
            match self.request_cancel(loaded, cancellation_checkpoint).await {
                Err(failure)
                    if failure.is_retryable_conflict()
                        && attempt + 1 < MAX_RECONCILIATION_ATTEMPTS =>
                {
                    // Follow only later validated authority. An accepted
                    // cancellation retains its first fence.
                    loaded = self.load_for_recovery().await?.ok_or_else(|| {
                        AppError::NotFound("workflow run does not exist".to_owned())
                    })?;
                    if loaded.record.checkpoint.is_terminal() {
                        return self
                            .project(&loaded.record)
                            .map_err(RuntimeOperationFailure::from);
                    }
                    if loaded.record.checkpoint.sequence <= observed {
                        return Err(failure);
                    }
                }
                result => return result,
            }
        }
        unreachable!()
    }

    pub(crate) async fn resolve(
        &self,
        sequence: u64,
        generation: u64,
        resolution: Resolution,
    ) -> Result<RunSnapshot, AppError> {
        let admission = self.app.reserve_caller()?;
        let runtime = self.clone();
        let (outcome, observation) = tokio::sync::oneshot::channel();
        if !self
            .app
            .spawn_run(self.key.clone(), RunTaskKind::Operation, Some(admission), async move {
                let _ = outcome.send(
                    AppTrace::span(|| tracing::info_span!(target: "app_sdk", "app.workflow.interaction",
                        run_id = %runtime.id(), occurrence = %runtime.occurrence(), control = sequence, generation, operation = "resolve"))
                        .observe(std::pin::pin!(runtime.resolve_owned(sequence, generation, resolution))).await,
                );
            })
        {
            return Err(AppError::Closed(
                "run resolution ownership is closed".into(),
            ));
        }
        observation
            .await
            .map_err(|_| AppError::Closed("run resolution observation closed".into()))?
    }

    async fn resolve_owned(
        &self,
        sequence: u64,
        generation: u64,
        resolution: Resolution,
    ) -> Result<RunSnapshot, AppError> {
        if let Resolution::Notification(resolution) = resolution {
            return self
                .resolve_notification(sequence, generation, resolution)
                .await;
        }
        let mut binding_generation = None;
        for retry in 0..MAX_RECONCILIATION_ATTEMPTS {
            let loaded = self.load().await?;
            let mut settlement = loaded
                .record
                .checkpoint
                .settlement(sequence, generation)
                .cloned()
                .ok_or_else(|| {
                    AppError::Conflict("interaction target generation is stale".into())
                })?;
            if let SettlementState::Applied { decision, .. } = &settlement.state {
                let identical = match (decision, &resolution) {
                    (
                        SettlementDecision::Inputs {
                            keys,
                            app_generation,
                            execution_generation,
                        },
                        Resolution::Inputs(values),
                    ) => {
                        let (application, _) = self.app.current_generation_record().await?;
                        application.generation == *app_generation
                            && self
                                .cell
                                .matches_inputs(*execution_generation, keys, values)
                    }
                    _ => decision.matches_resolution(&resolution),
                };
                return if identical {
                    self.applied_snapshot(&loaded.record, &settlement)
                        .await
                        .map_err(RuntimeOperationFailure::into_error)
                } else {
                    Err(AppError::Conflict(
                        "interaction target already owns a different or revoked decision".into(),
                    ))
                };
            }
            if !matches!(settlement.state, SettlementState::Pending) {
                return Err(AppError::Conflict(
                    "interaction target has an unresolved prior decision".into(),
                ));
            }
            let control = loaded
                .record
                .checkpoint
                .control(settlement.control_sequence)
                .cloned()
                .ok_or_else(|| {
                    AppError::Conflict("interaction control is no longer current".into())
                })?;
            if !settlement.owns(&control)
                || loaded.record.checkpoint.current_settlement(&control) != Some(&settlement)
                || loaded.record.checkpoint.is_terminal()
            {
                return Err(AppError::Conflict(
                    "interaction target is not currently actionable".into(),
                ));
            }
            if loaded.record.checkpoint.is_stopping()
                && !matches!(
                    (&control.state, &resolution),
                    (
                        ControlRecordState::Attempt(AttemptRecord {
                            phase: AttemptPhaseRecord::Unknown { .. },
                            ..
                        }),
                        Resolution::Accept(_) | Resolution::Fail
                    )
                )
            {
                return Err(AppError::Conflict(
                    "run terminal settlement owns this control".into(),
                ));
            }
            let transition = match (&control.state, &resolution) {
                (ControlRecordState::Input { .. }, Resolution::Input(value)) => {
                    let definition = self.app.definition_for(&loaded.record)?;
                    let Some(Step::Input { schema, target, .. }) =
                        definition.node(&control.node).map(|node| node.step())
                    else {
                        return Err(AppError::Integration(
                            "input control definition is unavailable".into(),
                        ));
                    };
                    if let Some(schema) = schema {
                        crate::workflow::definition::JsonBoundary::validate_instance(
                            schema, value,
                        )?;
                    }
                    let contributions = target
                        .as_ref()
                        .map(|target| {
                            definition
                                .reducer(target)
                                .map(|_| vec![(target.clone(), value.clone())])
                                .ok_or_else(|| {
                                    AppError::Integration("input target reducer is missing".into())
                                })
                        })
                        .transpose()?
                        .unwrap_or_default();
                    let activation =
                        ExecutionEngine::new(&definition, self.app.typing(), self.app.conditions())
                            .stage_after_control(
                                &control.node,
                                &control.lineage,
                                contributions,
                                Map::from_iter([("value".to_owned(), value.clone())]),
                            )?;
                    settlement.apply(
                        SettlementDecision::Input {
                            value: value.clone(),
                        },
                        &loaded.record.checkpoint,
                    )?;
                    ControlTransition::Resolve {
                        settlement,
                        activation,
                    }
                }
                (ControlRecordState::Inputs { required }, Resolution::Inputs(values)) => {
                    let (application, _) = self.app.current_generation_record().await?;
                    let execution_generation = match binding_generation {
                        Some(generation) => {
                            if !self.cell.matches_inputs(generation, required, values) {
                                return Err(AppError::Conflict(
                                    "transient input binding owner changed".into(),
                                ));
                            }
                            generation
                        }
                        None => {
                            let generation = self.cell.bind_inputs(required, values)?;
                            binding_generation = Some(generation);
                            generation
                        }
                    };
                    settlement.apply(
                        SettlementDecision::Inputs {
                            keys: required.clone(),
                            app_generation: application.generation,
                            execution_generation,
                        },
                        &loaded.record.checkpoint,
                    )?;
                    ControlTransition::Settlement { settlement }
                }
                (
                    ControlRecordState::Attempt(AttemptRecord {
                        phase: AttemptPhaseRecord::Unknown { .. },
                        ..
                    }),
                    Resolution::Accept(values),
                ) => {
                    let definition = self.app.definition_for(&loaded.record)?;
                    let mut contributions = Vec::with_capacity(values.len());
                    for (target, value) in values {
                        definition.reducer(target).ok_or_else(|| {
                            AppError::InvalidInput(
                                "unknown-effect settlement targets undeclared state".into(),
                            )
                        })?;
                        contributions.push((target.clone(), value.clone()));
                    }
                    let activation =
                        ExecutionEngine::new(&definition, self.app.typing(), self.app.conditions())
                            .stage_after_control(
                                &control.node,
                                &control.lineage,
                                contributions,
                                Map::new(),
                            )?;
                    settlement.apply(
                        SettlementDecision::Accept {
                            contribution: values.clone(),
                        },
                        &loaded.record.checkpoint,
                    )?;
                    ControlTransition::Resolve {
                        settlement,
                        activation,
                    }
                }
                (
                    ControlRecordState::Attempt(AttemptRecord {
                        phase: AttemptPhaseRecord::Unknown { .. },
                        ..
                    }),
                    Resolution::Fail,
                ) => {
                    let (category, message) = match &loaded.record.checkpoint.phase {
                        CheckpointPhaseRecord::Failing {
                            category, message, ..
                        } => (*category, message.clone()),
                        _ => (
                            FailureCategory::Agent,
                            "caller failed an external effect with unknown outcome".into(),
                        ),
                    };
                    let activation = control.clone().failed_activation(category, message);
                    settlement.apply(SettlementDecision::Fail, &loaded.record.checkpoint)?;
                    ControlTransition::Resolve {
                        settlement,
                        activation,
                    }
                }
                (
                    ControlRecordState::Attempt(AttemptRecord {
                        phase: AttemptPhaseRecord::Unknown { .. },
                        ..
                    }),
                    Resolution::Retry,
                ) => match self.reconcile_retry(loaded, control, settlement).await {
                    Err(failure)
                        if failure.is_retryable_conflict()
                            && retry + 1 < MAX_RECONCILIATION_ATTEMPTS =>
                    {
                        continue;
                    }
                    result => return result.map_err(RuntimeOperationFailure::into_error),
                },
                _ => {
                    return Err(AppError::Conflict(
                        "resolution does not match the current interaction".into(),
                    ));
                }
            };
            let closes_attempt = matches!(control.state, ControlRecordState::Attempt(_));
            match self.commit(loaded, transition).await {
                Ok(record) => {
                    if closes_attempt {
                        self.remove_interaction(control.sequence);
                    }
                    self.schedule(ScheduleCause::CheckpointReady(record.checkpoint.sequence));
                    let applied = record
                        .checkpoint
                        .settlement(sequence, generation)
                        .ok_or_else(|| {
                            AppError::Integration("applied interaction evidence is missing".into())
                        })?;
                    return self
                        .applied_snapshot(&record, applied)
                        .await
                        .map_err(RuntimeOperationFailure::into_error);
                }
                Err(failure)
                    if failure.is_retryable_conflict()
                        && retry + 1 < MAX_RECONCILIATION_ATTEMPTS => {}
                Err(failure) => return Err(failure.into_error()),
            }
        }
        unreachable!()
    }

    async fn resolve_notification(
        &self,
        sequence: u64,
        generation: u64,
        resolution: NotificationResolution,
    ) -> Result<RunSnapshot, AppError> {
        let notifier = self.notifier()?;
        let mut notified = Box::pin(notifier.notified());
        let decision = SettlementDecision::from(&resolution);
        let mut conflicts = 0;
        'targets: loop {
            notified.as_mut().enable();
            let loaded = self.load().await?;
            let settlement = loaded
                .record
                .checkpoint
                .settlement(sequence, generation)
                .cloned()
                .ok_or_else(|| {
                    AppError::Conflict("notification target generation is stale".into())
                })?;
            let SettlementTarget::Notification { notification, .. } = &settlement.target else {
                return Err(AppError::Conflict(
                    "resolution does not match this interaction kind".into(),
                ));
            };
            let notification_id = notification.id();
            match &settlement.state {
                SettlementState::Applied {
                    decision: prior, ..
                } if prior == &decision => {
                    return self
                        .applied_snapshot(&loaded.record, &settlement)
                        .await
                        .map_err(RuntimeOperationFailure::into_error);
                }
                SettlementState::Uncertain { decision: prior } if prior == &decision => {
                    return self.project(&loaded.record);
                }
                SettlementState::Intended { decision: prior } if prior == &decision => {
                    // Reattachment observes original ownership; it never invokes the lower decision again.
                    self.schedule(ScheduleCause::CheckpointReady(
                        loaded.record.checkpoint.sequence,
                    ));
                }
                SettlementState::Pending => {
                    let control = loaded
                        .record
                        .checkpoint
                        .control(settlement.control_sequence)
                        .cloned()
                        .ok_or_else(|| {
                            AppError::Conflict("notification has no active attempt".into())
                        })?;
                    if loaded.record.checkpoint.is_stopping()
                        || loaded.record.checkpoint.current_settlement(&control)
                            != Some(&settlement)
                        || !settlement.owns(&control)
                    {
                        return Err(AppError::Conflict(
                            "notification target is not currently actionable".into(),
                        ));
                    }
                    // Unavailable recovery is an observation, not admitted resolution
                    // work. Completing a Busy task would wake and interrupt that recovery.
                    let interaction = self.interaction(control.sequence).ok_or_else(|| {
                        AppError::Busy(
                            "exact notification is being recovered from its lower owner".into(),
                        )
                    })?;
                    let delivered = interaction.notification(notification_id)?;
                    if SettlementTarget::from_notification(&control, &delivered)?
                        != settlement.target
                    {
                        return Err(AppError::Conflict(
                            "live notification evidence changed".into(),
                        ));
                    }
                    let control_sequence = control.sequence;
                    let observed_checkpoint = loaded.record.checkpoint.sequence;
                    let runtime = self.clone();
                    let (outcome, observation) = tokio::sync::oneshot::channel();
                    let lower_resolution = resolution.clone();
                    let admitted = self.app.spawn_run(
                        self.key.clone(),
                        RunTaskKind::Resolution {
                            control_sequence: control.sequence,
                            target_sequence: sequence,
                            generation,
                        },
                        None,
                        async move {
                            let _ = outcome.send(
                                runtime
                                    .resolve_notification_owned(
                                        loaded,
                                        control,
                                        settlement,
                                        interaction,
                                        lower_resolution,
                                    )
                                    .await,
                            );
                        },
                    );
                    if admitted {
                        tokio::pin!(observation);
                        loop {
                            tokio::select! {
                                biased;
                                result = &mut observation => {
                                    match result.map_err(|_| AppError::Closed("native notification resolution observation closed".into()))? {
                                        Err(failure) if failure.is_retryable_conflict() => {
                                            conflicts += 1;
                                            if conflicts >= MAX_RECONCILIATION_ATTEMPTS
                                                || self.load().await?.record.checkpoint.sequence <= observed_checkpoint
                                            { return Err(failure.into_error()); }
                                            continue 'targets;
                                        },
                                        result => return result.map_err(RuntimeOperationFailure::into_error),
                                    }
                                }
                                current = async {
                                    notified.as_mut().await;
                                    notified.set(notifier.notified());
                                    notified.as_mut().enable();
                                    self.load().await
                                } => {
                                    let current = current?;
                                    if current.record.checkpoint.settlement(sequence, generation).is_some_and(|target| {
                                        matches!(&target.state, SettlementState::Uncertain { decision: prior } if prior == &decision)
                                    }) {
                                        return self.project(&current.record);
                                    }
                                }
                            }
                        }
                    }
                    if !self
                        .app
                        .has_resolution_work(&self.key, Some(control_sequence))
                    {
                        return Err(AppError::Closed(
                            "native notification resolution ownership is closed".into(),
                        ));
                    }
                }
                _ => {
                    return Err(AppError::Conflict(
                        "notification target already owns a different decision".into(),
                    ));
                }
            }
            notified.as_mut().await;
            notified.set(notifier.notified());
        }
    }

    async fn reconcile_retry(
        &self,
        loaded: LoadedRun,
        mut control: ControlRecord,
        mut settlement: InteractionSettlement,
    ) -> Result<RunSnapshot, RuntimeOperationFailure> {
        let now = Self::now_millis()?;
        if loaded.record.checkpoint.is_stopping() || now >= control.deadline_millis {
            return Err(AppError::Conflict(
                "stopping or expired work cannot dispatch another attempt".into(),
            )
            .into());
        }
        if !matches!(control.abort()?, ReconciliationRecord::Applied) {
            return Err(
                AppError::Conflict("external effect is not proven safe to retry".into()).into(),
            );
        }
        let definition = self.app.definition_for(&loaded.record)?;
        let node = definition.node(&control.node).ok_or_else(|| {
            AppError::Integration("retry node is missing from its definition".into())
        })?;
        let number = control.attempt_number()?;
        let started_millis = control
            .deadline_millis
            .saturating_sub(node.deadline_millis());
        let delay = node
            .retry_delay_millis(number, now.saturating_sub(started_millis))
            .ok_or_else(|| AppError::Conflict("node retry policy is exhausted".into()))?;
        let wake_millis = now
            .checked_add(delay)
            .filter(|wake| *wake <= control.deadline_millis)
            .ok_or_else(|| AppError::Conflict("retry exceeds its original deadline".into()))?;
        let next_attempt = number
            .checked_add(1)
            .ok_or_else(|| AppError::Conflict("attempt count is exhausted".into()))?;
        let sequence = control.sequence;
        control.prepare_retry(next_attempt, wake_millis)?;
        let target_sequence = settlement.sequence;
        let generation = settlement.generation;
        settlement.apply(SettlementDecision::Retry, &loaded.record.checkpoint)?;
        let record = self
            .commit(
                loaded,
                ControlTransition::Retry {
                    settlement,
                    control,
                },
            )
            .await?;
        self.remove_interaction(sequence);
        self.schedule(ScheduleCause::CheckpointReady(record.checkpoint.sequence));
        let applied = record
            .checkpoint
            .settlement(target_sequence, generation)
            .ok_or_else(|| AppError::Integration("retry interaction receipt is missing".into()))?;
        self.applied_snapshot(&record, applied).await
    }

    async fn resolve_notification_owned(
        &self,
        loaded: LoadedRun,
        control: ControlRecord,
        mut settlement: InteractionSettlement,
        interaction: Arc<Attempt<A>>,
        resolution: NotificationResolution,
    ) -> Result<RunSnapshot, RuntimeOperationFailure> {
        let SettlementTarget::Notification { notification, .. } = &settlement.target else {
            return Err(
                AppError::Conflict("native notification target kind changed".into()).into(),
            );
        };
        let notification_id = notification.id().clone();
        let sequence = settlement.sequence;
        let generation = settlement.generation;
        if Self::remaining(control.deadline_millis)?.is_zero() {
            return Err(
                AppError::Conflict("expired work cannot admit a lower resolution".into()).into(),
            );
        }
        settlement.intend(SettlementDecision::from(&resolution))?;
        let intended = self
            .commit_without_schedule(
                loaded,
                ControlTransition::Settlement {
                    settlement: settlement.clone(),
                },
            )
            .await;
        if let Err(failure) = &intended
            && !matches!(failure, RuntimeOperationFailure::Storage(failure) if failure.is_cleanup_failed())
        {
            self.schedule_operation_failure(failure);
        }
        let intended = intended?;
        self.schedule(ScheduleCause::CheckpointReady(
            intended.record.checkpoint.sequence,
        ));
        // Once admitted, the original lower future remains owned through its return.
        // The separate accepted control turn retains deadline/abort supervision.
        self.app.current_generation_record().await?;
        let outcome = interaction
            .resolve(notification_id.clone(), resolution)
            .await;
        settlement.settle(outcome.is_ok(), &intended.record.checkpoint)?;
        let applied = matches!(settlement.state, SettlementState::Applied { .. });
        let record = self
            .commit_control_transition(
                &control,
                ControlTransition::Settlement { settlement },
                Some(intended),
            )
            .await?
            .ok_or_else(|| {
                AppError::Conflict("notification resolution reservation is stale".into())
            })?;
        self.schedule(ScheduleCause::CheckpointReady(
            record.record.checkpoint.sequence,
        ));
        let record = record.record;
        if applied {
            interaction.remove_notification(&notification_id);
        }
        if applied {
            let retained = record
                .checkpoint
                .settlement(sequence, generation)
                .ok_or_else(|| {
                    AppError::Integration("applied notification evidence is missing".into())
                })?;
            self.applied_snapshot(&record, retained).await
        } else {
            self.project(&record).map_err(RuntimeOperationFailure::from)
        }
    }

    async fn applied_snapshot(
        &self,
        current: &RunRecord,
        settlement: &InteractionSettlement,
    ) -> Result<RunSnapshot, RuntimeOperationFailure> {
        let SettlementState::Applied { checkpoint, .. } = &settlement.state else {
            return Err(AppError::Conflict("interaction has no applied checkpoint".into()).into());
        };
        if current.checkpoint.sequence == *checkpoint {
            if current
                .checkpoint
                .settlement(settlement.sequence, settlement.generation)
                != Some(settlement)
            {
                return Err(AppError::Integration(
                    "applied interaction checkpoint evidence changed".into(),
                )
                .into());
            }
            self.app.current_generation_revision().await?;
            return self.project(current).map_err(RuntimeOperationFailure::from);
        }
        let definition = self.app.definition_for(current)?;
        let (record, _) = self
            .app
            .storage()
            .checkpoint(
                current,
                &CheckpointSelector::Sequence(*checkpoint),
                &definition,
            )
            .await?
            .ok_or_else(|| {
                AppError::Integration("applied interaction checkpoint is missing".into())
            })?;
        if record
            .checkpoint
            .settlement(settlement.sequence, settlement.generation)
            != Some(settlement)
        {
            return Err(AppError::Integration(
                "applied interaction checkpoint evidence changed".into(),
            )
            .into());
        }
        self.app.current_generation_revision().await?;
        self.project(&record).map_err(RuntimeOperationFailure::from)
    }

    async fn request_cancel(
        &self,
        loaded: LoadedRun,
        cancellation_checkpoint: u64,
    ) -> Result<RunSnapshot, RuntimeOperationFailure> {
        match &loaded.record.checkpoint.phase {
            CheckpointPhaseRecord::Failed {
                cancellation_checkpoint: Some(_),
                ..
            } => {
                return self
                    .project(&loaded.record)
                    .map_err(RuntimeOperationFailure::from);
            }
            CheckpointPhaseRecord::Succeeded | CheckpointPhaseRecord::Failed { .. } => {
                return Err(AppError::Conflict(
                    "terminal run has no accepted cancellation intent".to_owned(),
                )
                .into());
            }
            CheckpointPhaseRecord::Cancelled { .. } => {
                return self
                    .project(&loaded.record)
                    .map_err(RuntimeOperationFailure::from);
            }
            _ => {}
        }
        let record = if loaded.record.checkpoint.cancellation_requested() {
            loaded.record
        } else {
            self.commit(
                loaded,
                ControlTransition::Cancel {
                    cancellation_checkpoint,
                },
            )
            .await?
        };
        if !record.checkpoint.is_terminal() {
            self.schedule(ScheduleCause::CheckpointReady(record.checkpoint.sequence));
        }
        self.project(&record).map_err(RuntimeOperationFailure::from)
    }

    async fn settle_stopping(&self, loaded: LoadedRun) -> Option<LoadedRun> {
        if !loaded.record.checkpoint.is_stopping() {
            return None;
        }
        self.supervise_controls(&loaded.record);
        let mut committed = None;
        if loaded
            .record
            .checkpoint
            .controls
            .values()
            .any(|control| matches!(control.state, ControlRecordState::Child { .. }))
        {
            let Ok(notifier) = self.notifier() else {
                return None;
            };
            let mut notified = Box::pin(notifier.notified());
            notified.as_mut().enable();
            let complete = match self.cancel_descendants(&loaded.record).await {
                Ok(complete) => complete,
                Err(failure) => {
                    // A failed descendant cleanup cannot authorize another cancellation
                    // pass. Pure reads retain the same typed failure boundary.
                    self.retain_execution_read_failure(&failure);
                    self.schedule_after_storage_failure(&failure);
                    return None;
                }
            };
            if !complete {
                tokio::select! {
                    _ = &mut notified => {}
                    _ = tokio::time::sleep(RECONCILIATION_MAX_DELAY) => {}
                }
                self.schedule(ScheduleCause::CurrentState);
                return None;
            }
            // Every original descendant receipt and lifecycle handoff is now
            // settled. Retire each parent reservation without publishing a
            // partial wave; the original failure/cancellation keeps authority.
            for control in loaded
                .record
                .checkpoint
                .controls
                .values()
                .filter(|control| matches!(control.state, ControlRecordState::Child { .. }))
            {
                match self
                    .commit_scheduled_transition(
                        control,
                        ControlTransition::Stage {
                            activations: vec![control.failed_activation(
                                FailureCategory::Child,
                                "child workflow stopped with its parent".into(),
                            )],
                        },
                    )
                    .await
                {
                    Some(loaded) => committed = Some(loaded),
                    None => {
                        self.schedule(ScheduleCause::CurrentState);
                        return None;
                    }
                }
            }
        }
        // The caller's validated row or the last native Stage receipt already
        // owns this decision. Independent accepted work keeps its control here;
        // concurrent progress remains fenced by the terminal commit below.
        let current = committed.unwrap_or(loaded);
        if !current.record.checkpoint.controls.is_empty()
            || self.app.has_resolution_work(&self.key, None)
        {
            return None;
        }
        let transition = match &current.record.checkpoint.phase {
            CheckpointPhaseRecord::Failing {
                category,
                message,
                observation,
                ..
            } => ControlTransition::Failed {
                category: *category,
                message: message.clone(),
                observation: observation.clone(),
            },
            CheckpointPhaseRecord::Cancelling { .. } => ControlTransition::Cancelled,
            _ => return None,
        };
        match self.commit_for_recovery(current, transition).await {
            Ok(loaded) => Some(loaded),
            Err(_) => {
                self.schedule(ScheduleCause::CurrentState);
                None
            }
        }
    }

    async fn cancel_descendants(&self, record: &RunRecord) -> Result<bool, StorageFailure> {
        let deadline = LifecycleEvent::deadline(None);
        let mut cancelled = true;
        for control in record.checkpoint.controls.values() {
            let ControlRecordState::Child { admitted, .. } = &control.state else {
                continue;
            };
            let Ok(parent_definition) = self.app.definition_for(record) else {
                return Ok(false);
            };
            for member in admitted {
                // A completed native cancellation or closing callback may
                // exhaust this pass. Resume through the existing scan owner;
                // never start a new read with its already-spent budget.
                if tokio::time::Instant::now() >= deadline {
                    return Ok(false);
                }
                let key = &member.key;
                let Ok(runtime) = Self::attach(Arc::clone(&self.app), key.clone()) else {
                    return Ok(false);
                };
                let Some(current) = runtime
                    .load_execution_observation(None, Some(deadline))
                    .await?
                else {
                    return Ok(false);
                };
                if current
                    .record
                    .validate_child(record, &parent_definition)
                    .is_err()
                {
                    return Ok(false);
                }
                if !current.record.checkpoint.is_terminal() {
                    let cancellation_checkpoint = current
                        .record
                        .checkpoint
                        .cancellation_checkpoint()
                        .unwrap_or(current.record.checkpoint.sequence);
                    // The same validated child row owns this fenced intent.
                    // A conflict is observed on the next existing scan pass.
                    let snapshot = match runtime
                        .request_cancel(current, cancellation_checkpoint)
                        .await
                    {
                        Ok(snapshot) => snapshot,
                        Err(RuntimeOperationFailure::Storage(failure))
                            if failure.is_cleanup_failed() =>
                        {
                            return Err(failure);
                        }
                        Err(_) => return Ok(false),
                    };
                    // Parent stop is already durable and the executor was dropped.
                    // Only now may the child's ordinary cleanup driver take over.
                    runtime.cell.release_child(&self.key);
                    runtime.schedule(ScheduleCause::CheckpointReady(snapshot.checkpoint));
                    // Re-read this child's terminal authority on the next
                    // pass without withholding cancellation from siblings.
                    cancelled = false;
                    continue;
                }
                if self.app.has_run_work(&runtime.key)
                    || runtime.cell.bind_child(self.key.clone()).is_err()
                {
                    cancelled = false;
                    continue;
                }
                // Keep the parent binding across both paths. No fresh driver may
                // take this child between receipt settlement and ChildFinished.
                let _ = runtime
                    .finish_execution_observation(&current.record, None, Some(deadline), false)
                    .await;
                if let Some((generation, mut failure)) = runtime
                    .cell
                    .begin_settled_child_finish(current.record.checkpoint.sequence, deadline)
                {
                    if let Err(next) = runtime
                        .observe_child_finished(&current.record, deadline)
                        .await
                        && failure.is_none()
                    {
                        failure = Some(next);
                    }
                    if runtime
                        .cell
                        .complete_lifecycle(generation, current.record.checkpoint.sequence, failure)
                        .is_err()
                    {
                        runtime.cell.release_child(&self.key);
                        cancelled = false;
                        continue;
                    }
                }
                runtime.cell.release_child(&self.key);
                runtime.schedule(ScheduleCause::CheckpointReady(
                    current.record.checkpoint.sequence,
                ));
            }
        }
        Ok(cancelled)
    }

    async fn preparation_permit(
        &self,
        control: &ControlRecord,
        agent: &str,
    ) -> Result<(tokio::sync::OwnedMutexGuard<()>, Arc<A>, String), RuntimeOperationFailure> {
        // Callers retain the original activation or abort phase deadline.
        // Each required read has its own finite pass; a valid shared-lease wait
        // is outside those read budgets and remains inside the caller's phase.
        let selected = self
            .load_execution_observation(None, None)
            .await?
            .ok_or_else(|| {
                AppError::Closed("workflow preparation ownership was removed".to_owned())
            })?;
        let revision = selected.record.agent_revision(agent)?;
        let permit = self
            .app
            .run_registry()
            .preparation(&self.key, agent, revision)?
            .lock_owned()
            .await;
        let current = self
            .load_execution_observation(None, None)
            .await?
            .ok_or_else(|| {
                AppError::Closed("workflow preparation ownership was removed".to_owned())
            })?;
        if current.record.checkpoint.control(control.sequence) != Some(control)
            || !control.matches_checkpoint(&current.record.checkpoint)
            || current.record.agent_revision(agent)? != revision
        {
            return Err(
                AppError::Closed("workflow preparation ownership was revoked".to_owned()).into(),
            );
        }
        let agent = self.app.agent(agent, revision).await?;
        Ok((permit, agent, revision.to_owned()))
    }

    async fn abort_control(
        &self,
        definition: &CompiledDefinition,
        control: &ControlRecord,
    ) -> Result<(), RuntimeOperationFailure> {
        let remaining = Self::remaining(control.deadline_millis)?;
        let budget = if remaining.is_zero() {
            MAX_ABORT_OBSERVATION
        } else {
            remaining.min(MAX_ABORT_OBSERVATION)
        };
        let abort = async {
            if let Some(interaction) = self.interaction(control.sequence) {
                self.app.current_generation_record_for_recovery().await?;
                return interaction.abort().await.map_err(Into::into);
            }
            let (workspace_id, conversation_id) = control.attempt_identity()?;
            let agent_key = control.agent_key(definition)?;
            let conversation = {
                let (_preparation, agent, _) = self.preparation_permit(control, agent_key).await?;
                let workspace = agent
                    .get_workspace(workspace_id.clone())
                    .await
                    .map_err(AppError::from)?;
                workspace
                    .get_conversation(conversation_id.clone())
                    .await
                    .map_err(AppError::from)?
            };
            self.app.current_generation_record_for_recovery().await?;
            conversation
                .abort()
                .await
                .map_err(AppError::from)
                .map_err(Into::into)
        };
        tokio::time::timeout(budget, abort).await.map_err(|_| {
            RuntimeOperationFailure::Application(AppError::Timeout(
                "agent abort reconciliation deadline expired".to_owned(),
            ))
        })?
    }

    async fn reconcile_abort(
        &self,
        definition: &CompiledDefinition,
        owner: &ControlRecord,
        mut control: ControlRecord,
    ) {
        let applied = match AssertUnwindSafe(self.abort_control(definition, &control))
            .catch_unwind()
            .await
        {
            Ok(Ok(())) => true,
            Ok(Err(RuntimeOperationFailure::Storage(failure))) => {
                self.schedule_after_storage_failure(&failure);
                return;
            }
            Ok(Err(RuntimeOperationFailure::Application(_))) | Err(_) => false,
        };
        if control.settle_abort(applied).is_err() {
            return;
        }
        if self
            .commit_scheduled_transition(owner, ControlTransition::Park { control })
            .await
            .is_none()
        {
            self.schedule(ScheduleCause::CurrentState);
        }
    }

    async fn load(&self) -> Result<LoadedRun, AppError> {
        self.load_for_recovery()
            .await
            .map_err(StorageFailure::into_error)?
            .ok_or_else(|| AppError::NotFound("workflow run does not exist".to_owned()))
    }

    async fn load_for_recovery(&self) -> Result<Option<LoadedRun>, StorageFailure> {
        Self::load_scoped_for_recovery(&self.app, &self.key).await
    }

    async fn observe_generation(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), StorageFailure> {
        tokio::time::timeout_at(deadline, async {
            let mut delay = RECONCILIATION_INITIAL_DELAY;
            loop {
                match self.app.current_generation_record_for_recovery().await {
                    Ok(_) => return Ok(()),
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
                "workflow execution generation read deadline expired".into(),
            )))
        })
    }

    async fn load_execution_observation(
        &self,
        terminal: Option<u64>,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<Option<LoadedRun>, StorageFailure> {
        // Every execution authority pass has a finite observation ceiling.
        // An earlier original activation/cleanup deadline remains authoritative.
        let deadline = LifecycleEvent::deadline(deadline);
        let mut changes = self.app.runtime_changes();
        let mut draining = false;
        let observed = AssertUnwindSafe(async {
            tokio::select! {
                biased;
                _ = async {
                    loop {
                        if self.app.is_draining_accepted_work()
                            && terminal.is_some_and(|checkpoint| self.cell.terminal_observation_is_settled(checkpoint))
                        {
                            // Set this before select drops the held read. A
                            // destructor panic must reach the fatal App owner.
                            draining = true;
                            break;
                        }
                        if changes.changed().await.is_err() {
                            std::future::pending::<()>().await;
                        }
                    }
                } => Err(StorageFailure::permanent(AppError::Closed(
                    "terminal workflow observation drained".into(),
                ))),
                loaded = async {
                    let read = async {
                        let mut delay = RECONCILIATION_INITIAL_DELAY;
                        loop {
                            let observed = async {
                                let loaded = self.load_for_recovery().await?;
                                if let Some(loaded) = &loaded {
                                    // A terminal row admits no child effect. Its
                                    // parent may already be deleted; parent watchdogs
                                    // and completion verification retain the separate
                                    // relationship authority before any reduction.
                                    if !loaded.record.checkpoint.is_terminal() {
                                        self.validate_parent_for_recovery(loaded).await?;
                                    }
                                } else if self.cell.child_parent().is_some() {
                                    return Err(StorageFailure::permanent(AppError::NotFound(
                                        "admitted child execution authority is unavailable".into(),
                                    )));
                                }
                                Ok::<_, StorageFailure>(loaded)
                            }.await;
                            match observed {
                                Ok(loaded) => return Ok(loaded),
                                Err(failure) if failure.is_recoverable()
                                    && self.schedule_after_storage_failure(&failure) => {
                                    // Retry only this read under the original
                                    // publication and hook owner. Native commits
                                    // retain their separate reconciliation path.
                                    tokio::time::sleep(delay).await;
                                    delay = delay.saturating_mul(2).min(RECONCILIATION_MAX_DELAY);
                                }
                                Err(failure) => return Err(failure),
                            }
                        }
                    };
                    tokio::time::timeout_at(deadline, read).await
                        .unwrap_or_else(|_| Err(StorageFailure::permanent(AppError::Timeout(
                            "workflow execution authority read deadline expired".into(),
                        ))))
                } => loaded,
            }
        })
        .catch_unwind()
        .await;
        match observed {
            Ok(observed) => observed,
            Err(_) if draining => {
                self.app.tasks_failed(FinalResult::Integration(
                    "workflow read destruction failed during shutdown".into(),
                ));
                Err(StorageFailure::permanent(AppError::Closed(
                    "workflow runtime ownership was revoked".into(),
                )))
            }
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }

    async fn load_preserving_observation(
        &self,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<Option<LoadedRun>, StorageFailure> {
        // The caller retains its actual lower observation through this pass;
        // native publication begins only after current authority is readable.
        self.load_execution_observation(None, deadline)
            .await
            .inspect_err(|failure| self.retain_execution_read_failure(failure))
    }

    fn schedule_after_transient(&self, error: &AppError) -> bool {
        self.schedule_after_error(
            error,
            matches!(error, AppError::Busy(_) | AppError::Timeout(_)),
        )
    }

    fn schedule_after_error(&self, error: &AppError, recoverable: bool) -> bool {
        if matches!(
            error,
            AppError::Busy(_) | AppError::Timeout(_) | AppError::Integration(_)
        ) {
            self.schedule(ScheduleCause::StorageFailure(if recoverable {
                RetryDisposition::RetainUntilProgress
            } else {
                RetryDisposition::BoundedTransient
            }))
        } else {
            false
        }
    }

    fn schedule_after_storage_failure(&self, failure: &StorageFailure) -> bool {
        if failure.is_cleanup_failed() {
            self.app.tasks_failed(FinalResult::from(failure.error()));
            return false;
        }
        let recoverable = failure.is_recoverable();
        self.schedule_after_error(failure.error(), recoverable)
    }

    async fn commit(
        &self,
        loaded: LoadedRun,
        transition: ControlTransition,
    ) -> Result<RunRecord, RuntimeOperationFailure> {
        let result = self.commit_without_schedule(loaded, transition).await;
        if let Err(failure) = &result
            && !matches!(failure, RuntimeOperationFailure::Storage(failure) if failure.is_cleanup_failed())
        {
            self.schedule_operation_failure(failure);
        }
        result.map(|loaded| loaded.record)
    }

    async fn readmit_failed(&self, mut loaded: LoadedRun) -> Result<LoadedRun, AppError> {
        let mut delay = RECONCILIATION_INITIAL_DELAY;
        for attempt in 0..MAX_RECONCILIATION_ATTEMPTS {
            self.app.admit()?;
            if !loaded.record.checkpoint.can_retry_failed() {
                return Err(AppError::Conflict("failed run is not safe to retry".into()));
            }
            if self
                .app
                .storage()
                .run_is_active(&loaded.record)
                .await
                .map_err(StorageFailure::into_error)?
            {
                return Err(AppError::Busy(
                    "terminal cleanup still owns run capacity".into(),
                ));
            }
            let (workspace, workspace_revision) = self
                .app
                .storage()
                .load_workspace(self.workspace())
                .await?
                .ok_or_else(|| AppError::NotFound("workflow workspace does not exist".into()))?;
            workspace.public_config()?;
            if workspace.revision != loaded.record.workspace_revision {
                return Err(AppError::Conflict(
                    "workflow workspace revision is stale".into(),
                ));
            }
            let (mut app_record, app_revision) = self.app.admission_record().await?;
            if app_record.run_count >= MAX_RUNS {
                return Err(AppError::Busy(
                    "application run capacity is exhausted".into(),
                ));
            }
            app_record.run_count += 1;
            let mut candidate = loaded.record.clone();
            ControlTransition::RetryFailed.apply(&mut candidate, false)?;
            candidate.validate_recovery(self.app.definition_for(&candidate)?.as_ref())?;
            let mutation = AppMutation::readmit_run(
                app_revision,
                workspace_revision,
                loaded.revision,
                &app_record,
                &workspace,
                &candidate,
            )?;
            let failure = match self.app.storage().commit(mutation).await {
                AppCommitOutcome::Applied => {
                    self.schedule(ScheduleCause::CheckpointObserved(
                        candidate.checkpoint.sequence,
                    ));
                    self.wake();
                    return Ok(LoadedRun {
                        record: candidate,
                        revision: loaded.revision.checked_add(1).ok_or_else(|| {
                            AppError::Integration("storage revision is exhausted".into())
                        })?,
                    });
                }
                AppCommitOutcome::Rejected(failure) if !failure.is_retryable_conflict() => {
                    return Err(failure.into_error());
                }
                AppCommitOutcome::Rejected(failure) | AppCommitOutcome::Unknown(failure) => failure,
            };
            let current = self
                .load_for_recovery()
                .await
                .map_err(StorageFailure::into_error)?
                .ok_or_else(|| AppError::NotFound("workflow run does not exist".into()))?;
            if current.record.contains_checkpoint(&candidate)?
                && self
                    .app
                    .storage()
                    .run_is_active(&current.record)
                    .await
                    .map_err(StorageFailure::into_error)?
            {
                self.schedule(ScheduleCause::CheckpointObserved(
                    current.record.checkpoint.sequence,
                ));
                self.wake();
                return Ok(current);
            }
            if current.record != loaded.record || current.revision != loaded.revision {
                return Err(AppError::Conflict(
                    "failed retry lost its durable revision fence".into(),
                ));
            }
            if attempt + 1 == MAX_RECONCILIATION_ATTEMPTS {
                return Err(failure.into_error());
            }
            loaded = current;
            tokio::time::sleep(delay).await;
            delay = delay.saturating_mul(2).min(RECONCILIATION_MAX_DELAY);
        }
        unreachable!()
    }

    async fn commit_for_recovery(
        &self,
        loaded: LoadedRun,
        transition: ControlTransition,
    ) -> Result<LoadedRun, RuntimeOperationFailure> {
        let result = self.commit_without_schedule(loaded, transition).await;
        if let Err(failure) = &result {
            self.schedule_operation_failure(failure);
        }
        result
    }

    fn retain_execution_read_failure(&self, failure: &StorageFailure) {
        if failure.is_cleanup_failed() {
            self.app.tasks_failed(FinalResult::from(failure.error()));
        } else if !failure.is_recoverable()
            && !matches!(failure.error(), AppError::Closed(_) | AppError::Conflict(_))
        {
            // Unreadable execution authority cannot authorize another effect.
            // This receiver is called only by execution observation owners,
            // never native mutation receipts or Unknown reconciliation.
            self.app.tasks_failed(FinalResult::Integration(
                "workflow execution authority observation failed".into(),
            ));
        }
    }

    fn schedule_operation_failure(&self, failure: &RuntimeOperationFailure) -> bool {
        match failure {
            RuntimeOperationFailure::Storage(failure) => {
                self.schedule_after_storage_failure(failure)
            }
            RuntimeOperationFailure::Application(error) => self.schedule_after_transient(error),
        }
    }

    fn commit_without_schedule(
        &self,
        loaded: LoadedRun,
        transition: ControlTransition,
    ) -> Pin<Box<impl Future<Output = Result<LoadedRun, RuntimeOperationFailure>> + '_>> {
        Box::pin(async move {
            let runtime = self.clone();
            let (outcome, observation) = tokio::sync::oneshot::channel();
            // A local child turn can disappear with its executor. Its admitted
            // checkpoint transaction and exact readback retain their own task.
            if !self
                .app
                .spawn_run(self.key.clone(), RunTaskKind::Operation, None, async move {
                    let result = runtime
                        .commit_checkpoint_owned(loaded, transition, None)
                        .await;
                    let _ = outcome.send(result);
                })
            {
                return Err(
                    AppError::Closed("checkpoint publication ownership is closed".into()).into(),
                );
            }
            observation.await.map_err(|_| {
                RuntimeOperationFailure::Application(AppError::Closed(
                    "checkpoint publication observation closed".into(),
                ))
            })?
        })
    }

    fn commit_checkpoint_owned(
        &self,
        mut loaded: LoadedRun,
        transition: ControlTransition,
        admission_owner: Option<tokio::task::Id>,
    ) -> impl Future<Output = Result<LoadedRun, RuntimeOperationFailure>> + '_ {
        let span = AppTrace::span(|| {
            tracing::info_span!(target: "app_sdk", "app.workflow.checkpoint",
            run_id = %self.id(), occurrence = %self.occurrence(), checkpoint = loaded.record.checkpoint.sequence,
            transition = transition.kind())
        });
        let boundary = AppTrace::span(|| match &transition {
            ControlTransition::Barrier { superstep, .. } => {
                tracing::info_span!(target: "app_sdk", "app.workflow.superstep",
                    run_id = %self.id(), occurrence = %self.occurrence(), superstep = *superstep)
            }
            ControlTransition::Park { .. } => {
                // The provisional sequence may change after a definite native
                // rejection. Accepted identity belongs to the committed event
                // and the original Control task's subsequent attempt span.
                tracing::info_span!(target: "app_sdk", "app.workflow.interaction",
                    run_id = %self.id(), occurrence = %self.occurrence(), operation = "create")
            }
            _ => tracing::Span::none(),
        });
        boundary.observe(span.observe(Box::pin(async move {
            let mut transition_kind = transition.kind();
            let previous = loaded.record.previous.take();
            let mut source = loaded.record.clone();
            source.previous = previous;
            let admission = matches!(&transition, ControlTransition::Park { control }
                if source.checkpoint.control_for(&control.node, &control.lineage).is_none());
            let pause_generation = match &transition {
                ControlTransition::Pause { discard_staged }
                    if discard_staged.is_empty()
                        && source.checkpoint.base_sequence == source.checkpoint.sequence
                        && matches!(source.checkpoint.phase, CheckpointPhaseRecord::Routing { .. }) =>
                {
                    self.cell.lifecycle_generation()
                }
                _ => None,
            };
            let deleting = if matches!(&transition, ControlTransition::Barrier { .. }) {
                source.children_to_delete(self.app.definition_for(&source)?.as_ref())
            } else {
                Vec::new()
            };
            transition.apply(&mut loaded.record, self.app.has_resolution_work(&self.key, None))?;
            loaded
                .record
                .validate_recovery(self.app.definition_for(&loaded.record)?.as_ref())?;
            let mut delay = RECONCILIATION_INITIAL_DELAY;
            let mut attempts = 0;
            let loaded = 'mutation: loop {
                let (app_record, app_revision) = self.app.current_generation_record_for_recovery().await?;
                let mutation = match AppMutation::advance_run(app_revision, loaded.revision, &app_record, &loaded.record, &deleting) {
                    Err(error @ AppError::InvalidInput(_))
                        if matches!(source.checkpoint.admission, RunAdmission::Background) =>
                    {
                        let Some(control) = loaded.record.checkpoint.controls.values().find(|control| {
                            source.checkpoint.control(control.sequence).is_none()
                                && matches!(control.state, ControlRecordState::Attempt(AttemptRecord {
                                    number: 1, identity: None, phase: AttemptPhaseRecord::Intent,
                                    abort: ReconciliationRecord::Idle,
                                }))
                        }) else {
                            return Err(error.into());
                        };
                        // No native mutation or lower effect was admitted. Publish
                        // the running node's limit from its original authority.
                        let definition = self.app.definition_for(&source)?;
                        let mut position = match self.cell.action_context(
                            source.checkpoint.base_sequence, &control.node, &control.lineage,
                        ) {
                            Some(context) => ActionPosition {
                                node: context.node.clone(), ordinal: context.action_ordinal,
                            },
                            None => source.checkpoint.proposed_position(
                                &definition, &control.node, &control.lineage, &[],
                            )?,
                        };
                        let mut lineage = control.lineage.clone();
                        let mut category = FailureCategory::Limit;
                        let mut message = "agent attempt exceeds workflow capacity".to_owned();
                        if let Some((context, earlier_lineage, earlier_category)) = self.cell
                            .lifecycle_generation().and_then(|generation| self.cell.source_failure(generation))
                            && context.checkpoint == source.checkpoint.base_sequence
                            && (&context.node, &earlier_lineage) < (&position.node, &lineage)
                        {
                            position = ActionPosition { node: context.node.clone(), ordinal: context.action_ordinal };
                            lineage = earlier_lineage;
                            category = earlier_category;
                            message = "caller action execution failed".into();
                        }
                        if let Some((earlier, earlier_category, earlier_message)) = source.checkpoint
                            .preceding_failure(&definition, &position, &lineage)?
                        {
                            position = earlier;
                            category = earlier_category;
                            message = earlier_message.to_owned();
                        }
                        loaded.record.checkpoint = loaded.record.previous.take().ok_or_else(|| {
                            AppError::Integration("initial attempt predecessor is unavailable".into())
                        })?;
                        ControlTransition::Failed { category, message, observation: Some(position) }
                            .apply(&mut loaded.record, self.app.has_resolution_work(&self.key, None))?;
                        loaded.record.validate_recovery(&definition)?;
                        transition_kind = "failed";
                        AppMutation::advance_run(app_revision, loaded.revision, &app_record, &loaded.record, &[])?
                    }
                    result => result?,
                };
                let (mut authority_failure, commit_may_have_applied) =
                    match self.app.storage().commit(mutation).await {
                        AppCommitOutcome::Applied => {
                            loaded.revision = loaded.revision.checked_add(1).ok_or_else(|| AppError::Integration("storage revision is exhausted".into()))?;
                            break loaded;
                        },
                        AppCommitOutcome::Rejected(failure)
                            if failure.is_retryable_conflict() =>
                        {
                            (failure, false)
                        }
                        AppCommitOutcome::Rejected(failure) => return Err(failure.into()),
                        AppCommitOutcome::Unknown(failure) => (failure, true),
                    };
                attempts += 1;
                loop {
                    match self
                        .app
                        .storage()
                        .load_run_for_recovery(&self.key)
                        .await
                    {
                        Ok(Some((record, revision)))
                            if commit_may_have_applied
                                && record.contains_checkpoint(&loaded.record)? =>
                        {
                            break 'mutation LoadedRun { record, revision };
                        }
                        Ok(Some((record, revision)))
                            if commit_may_have_applied && admission
                                && record.same_run(&source)
                                && record.checkpoint.base_sequence == source.checkpoint.base_sequence
                                && record.checkpoint.sequence > loaded.record.checkpoint.sequence =>
                        {
                            // Public Input settlement can retire the accepted
                            // control before its original native receipt returns.
                            // Only the exact immutable admission row proves this
                            // candidate applied; later staged values alone do not.
                            let definition = self.app.definition_for(&record)?;
                            record.validate_recovery(&definition)?;
                            let admitted = self.app.storage().checkpoint(
                                &record,
                                &CheckpointSelector::Sequence(loaded.record.checkpoint.sequence),
                                &definition,
                            ).await?.ok_or_else(|| AppError::Integration(
                                "native admission checkpoint is missing".into(),
                            ))?.0;
                            if !admitted.same_persisted_row(&loaded.record)? {
                                return Err(AppError::Conflict(
                                    "native admission checkpoint differs from its original candidate".into(),
                                ).into());
                            }
                            break 'mutation LoadedRun { record, revision };
                        }
                        Ok(Some((record, revision)))
                            if revision == loaded.revision && record == source =>
                        {
                            break;
                        }
                        Ok(Some((record, revision))) if !commit_may_have_applied && admission => {
                            let Some(mut control) = loaded.record.checkpoint.controls
                                .remove(&loaded.record.checkpoint.sequence) else {
                                    return Err(AppError::Conflict("native admission no longer owns its control".into()).into());
                                };
                            let prior = control.sequence;
                            if !control.rebase_admission(&source, &record)? {
                                return Err(AppError::Conflict("native admission lost its activation".into()).into());
                            }
                            if let Some(owner) = admission_owner {
                                self.app.retarget_control(owner, &self.key, prior, control.sequence)?;
                            }
                            loaded = LoadedRun { record, revision };
                            let previous = loaded.record.previous.take();
                            source = loaded.record.clone();
                            source.previous = previous;
                            ControlTransition::Park { control }.apply(&mut loaded.record,
                                self.app.has_resolution_work(&self.key, None))?;
                            loaded.record.validate_recovery(self.app.definition_for(&loaded.record)?.as_ref())?;
                            break;
                        }
                        Ok(Some((record, revision)))
                            if !commit_may_have_applied
                                && pause_generation.is_some_and(|generation| {
                                    self.cell.lifecycle_generation() == Some(generation)
                                })
                                && record.same_run(&source)
                                && record.checkpoint.base_sequence == source.checkpoint.base_sequence
                                && record.checkpoint.superstep == source.checkpoint.superstep
                                && record.checkpoint.frontier == source.checkpoint.frontier
                                && record.checkpoint.settled_joins == source.checkpoint.settled_joins
                                && record.checkpoint.position == source.checkpoint.position =>
                        {
                            // Only a definitively rejected empty Pause can
                            // follow independent changes to this same barrier.
                            // Keep current stopping/control truth intact; an
                            // Unknown native receipt never enters this path.
                            record.validate_recovery(self.app.definition_for(&record)?.as_ref())?;
                            if serde_json::to_vec(&(&record.checkpoint.state, &record.checkpoint.outcomes))
                                .map_err(|_| AppError::Integration("committed barrier evidence encoding failed".into()))?
                                != serde_json::to_vec(&(&source.checkpoint.state, &source.checkpoint.outcomes))
                                    .map_err(|_| AppError::Integration("committed barrier evidence encoding failed".into()))?
                            {
                                return Err(AppError::Conflict("post-hook cleanup lost its committed barrier".into()).into());
                            }
                            loaded = LoadedRun { record, revision };
                            let previous = loaded.record.previous.take();
                            source = loaded.record.clone();
                            source.previous = previous;
                            ControlTransition::Pause { discard_staged: Vec::new() }
                                .apply(&mut loaded.record, self.app.has_resolution_work(&self.key, None))?;
                            loaded.record.validate_recovery(self.app.definition_for(&loaded.record)?.as_ref())?;
                            break;
                        }
                        Ok(Some(_)) => {
                            return Err(AppError::Conflict(
                                "run mutation lost its durable revision fence".to_owned(),
                            )
                            .into());
                        }
                        Ok(None) => {
                            return Err(AppError::NotFound(
                                "workflow run disappeared during reconciliation".to_owned(),
                            )
                            .into());
                        }
                        Err(failure) if failure.is_cleanup_failed() => {
                            return Err(failure.into());
                        }
                        Err(failure) => {
                            authority_failure = failure;
                            attempts += 1;
                            if attempts >= MAX_RECONCILIATION_ATTEMPTS {
                                return Err(authority_failure.into());
                            }
                            self.app.current_generation_record_for_recovery().await?;
                            tokio::time::sleep(delay).await;
                            delay = delay.saturating_mul(2).min(RECONCILIATION_MAX_DELAY);
                        }
                    }
                }
                if attempts >= MAX_RECONCILIATION_ATTEMPTS {
                    return Err(authority_failure.into());
                }
                self.app.current_generation_record_for_recovery().await?;
                tokio::time::sleep(delay).await;
                delay = delay.saturating_mul(2).min(RECONCILIATION_MAX_DELAY);
            };
            let record = &loaded.record;
            self.schedule(ScheduleCause::CheckpointObserved(
                record.checkpoint.sequence,
            ));
            self.wake();
            AppTrace::event(|| tracing::info!(
                target: "app_sdk",
                event = "app.workflow.checkpoint",
                run_id = %self.id(), occurrence = %self.occurrence(),
                checkpoint = record.checkpoint.sequence,
                superstep = record.checkpoint.superstep,
                transition = transition_kind,
                status = record.checkpoint.kind(),
            ));
            if transition_kind == "barrier" {
                AppTrace::event(|| tracing::info!(
                    target: "app_sdk",
                    event = "app.workflow.superstep",
                    run_id = %self.id(), occurrence = %self.occurrence(),
                    checkpoint = record.checkpoint.sequence,
                    superstep = record.checkpoint.superstep,
                    frontier = record.checkpoint.frontier.len(),
                ));
            }
            // Settlement successors preserve their existing prefix. Observe
            // changed targets directly, including an applied target whose
            // runnable control was removed by this same native publication.
            for (index, target) in record.checkpoint.settlements.iter().enumerate() {
                if source.checkpoint.settlements.get(index) == Some(target) {
                    continue;
                }
                let category = match &target.target {
                    SettlementTarget::Input => "input",
                    SettlementTarget::Inputs => "inputs",
                    SettlementTarget::Notification { .. } => "notification",
                    SettlementTarget::Unknown { .. } => "unknown",
                };
                let status = match &target.state {
                    SettlementState::Pending => "pending",
                    SettlementState::Intended { .. } => "intended",
                    SettlementState::Applied { .. } => "applied",
                    SettlementState::Uncertain { .. } => "uncertain",
                };
                AppTrace::event(|| tracing::info!(
                    target: "app_sdk", event = "app.workflow.interaction",
                    run_id = %self.id(), occurrence = %self.occurrence(), checkpoint = record.checkpoint.sequence,
                    control = target.sequence, generation = target.generation,
                    transition = transition_kind, category, status,
                ));
            }
            if transition_kind == "barrier" {
                for (key, _) in deleting {
                    // The parent receipt already proves the native seal. A
                    // closed local deletion owner cannot uncommit that result;
                    // startup resumes the same sealed head.
                    let child = match Self::attach(Arc::clone(&self.app), key.clone()) {
                        Ok(child) => child,
                        Err(error) => { self.app.tasks_failed(FinalResult::from(&error)); continue; }
                    };
                    let owner = Arc::clone(&self.app);
                    if !self.app.spawn_run(key, RunTaskKind::Operation, None, async move {
                        if let Err(error) = child.resume_deletion().await {
                            owner.tasks_failed(FinalResult::from(&error));
                        }
                    }) {
                        self.app.tasks_failed(FinalResult::Integration("copied child deletion ownership is closed".into()));
                    }
                }
            }
            if record.checkpoint.is_terminal()
                && let Some(parent) = &record.parent
            {
                WorkflowRunRuntime::schedule_parent(&self.app, &record.workspace_id, parent);
            }
            Ok(loaded)
        })))
    }

    async fn commit_failure(
        &self,
        loaded: LoadedRun,
        category: FailureCategory,
        message: String,
    ) -> Result<(), AppError> {
        match self
            .commit_failure_for_recovery(loaded, category, message)
            .await
        {
            Ok(_) => Ok(()),
            Err(failure) => {
                self.schedule_operation_failure(&failure);
                Err(failure.into_error())
            }
        }
    }

    async fn commit_failure_for_recovery(
        &self,
        loaded: LoadedRun,
        category: FailureCategory,
        message: String,
    ) -> Result<LoadedRun, RuntimeOperationFailure> {
        let loaded = self
            .commit_without_schedule(
                loaded,
                ControlTransition::Failed {
                    category,
                    message,
                    observation: None,
                },
            )
            .await?;
        let record = &loaded.record;
        match &record.checkpoint.phase {
            CheckpointPhaseRecord::Failing { .. } => {
                self.schedule(ScheduleCause::CheckpointReady(record.checkpoint.sequence));
            }
            CheckpointPhaseRecord::Failed { .. } => {
                if self.cell.child_parent().is_none() {
                    self.maintain_retention(None).await;
                }
            }
            _ => {
                return Err(AppError::Integration(
                    "failure transition did not preserve a failure state".to_owned(),
                )
                .into());
            }
        }
        Ok(loaded)
    }

    fn project(&self, record: &RunRecord) -> Result<RunSnapshot, AppError> {
        record.project_snapshot(self.app.definition_for(record)?.as_ref())
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
    pub(crate) fn drive(self) -> Pin<Box<impl Future<Output = ()>>> {
        Box::pin(async move {
            let runtime = self;
            let loaded = match runtime.load_execution_observation(None, None).await {
                Ok(Some(loaded)) => loaded,
                Err(failure) => {
                    runtime.retain_execution_read_failure(&failure);
                    runtime.schedule_after_storage_failure(&failure);
                    return;
                }
                Ok(None) => {
                    runtime.release();
                    return;
                }
            };
            runtime.supervise_controls(&loaded.record);
            if loaded.record.checkpoint.is_stopping() {
                if let Some(loaded) = Box::pin(runtime.settle_stopping(loaded)).await {
                    let _ = runtime
                        .finish_execution_observation(&loaded.record, None, None, false)
                        .await;
                    if runtime.cell.child_parent().is_none() {
                        runtime.maintain_retention(Some(loaded)).await;
                    }
                }
                return;
            }
            if loaded.record.checkpoint.is_terminal() || loaded.record.checkpoint.is_paused() {
                if !loaded
                    .record
                    .checkpoint
                    .controls
                    .values()
                    .any(|control| matches!(control.state, ControlRecordState::Inputs { .. }))
                {
                    let _ = runtime
                        .finish_execution_observation(&loaded.record, None, None, false)
                        .await;
                }
                if loaded.record.checkpoint.is_terminal() && runtime.cell.child_parent().is_none() {
                    Box::pin(runtime.maintain_retention(Some(loaded))).await;
                }
                return;
            }
            drop(loaded);
            let mut run = crate::workflow::WorkflowRun { runtime };
            let _ = Box::pin(session::Execution::background(&mut run).finish()).await;
        })
    }

    fn supervise_controls(&self, record: &RunRecord) {
        for control in record.checkpoint.controls.values() {
            if !control.needs_supervision(record.checkpoint.is_stopping())
                || record.checkpoint.is_stopping()
                    && matches!(control.abort(), Ok(ReconciliationRecord::Applied))
                    && self
                        .app
                        .has_resolution_work(&self.key, Some(control.sequence))
            {
                continue;
            }
            let sequence = control.sequence;
            let runtime = self.clone();
            self.app.spawn_run(
                self.key.clone(),
                RunTaskKind::Control { sequence },
                None,
                async move {
                    runtime.continue_control(sequence).await;
                },
            );
        }
    }

    async fn continue_control(&self, sequence: u64) {
        loop {
            // The current authority pass shares one pure-read budget. Waiting
            // on a valid Input/Sleep and native publication remain separate.
            let read_deadline = LifecycleEvent::deadline(None);
            let loaded = match self
                .load_execution_observation(None, Some(read_deadline))
                .await
            {
                Ok(Some(loaded)) => loaded,
                Ok(None) => return,
                Err(failure) => {
                    self.retain_execution_read_failure(&failure);
                    self.schedule_after_storage_failure(&failure);
                    return;
                }
            };
            let Some(control) = loaded.record.checkpoint.control(sequence).cloned() else {
                return;
            };
            if !control.needs_supervision(loaded.record.checkpoint.is_stopping()) {
                return;
            }
            if loaded.record.checkpoint.is_stopping() {
                let Ok(definition) = self.app.definition_for(&loaded.record) else {
                    return;
                };
                match control.abort() {
                    Ok(ReconciliationRecord::Idle) => {
                        let mut next = control.clone();
                        if next.begin_abort().is_err() {
                            return;
                        }
                        if self
                            .commit_scheduled_transition(
                                &control,
                                ControlTransition::Park { control: next },
                            )
                            .await
                            .is_none()
                        {
                            return;
                        }
                    }
                    Ok(ReconciliationRecord::Intended) => {
                        self.reconcile_abort(&definition, &control, control.clone())
                            .await;
                    }
                    Ok(ReconciliationRecord::Applied) => {
                        // The exact lower decision owner remains independent of
                        // abort acknowledgement. Its task join wakes this run.
                        if self.app.has_resolution_work(&self.key, Some(sequence)) {
                            return;
                        }
                        if self
                            .commit_scheduled_transition(
                                &control,
                                ControlTransition::AttemptSettled { sequence },
                            )
                            .await
                            .is_some()
                        {
                            self.remove_interaction(sequence);
                        }
                        return;
                    }
                    Ok(ReconciliationRecord::Unknown) | Err(_) => return,
                }
                continue;
            }
            match &control.state {
                ControlRecordState::Input { .. }
                | ControlRecordState::Inputs { .. }
                | ControlRecordState::Sleep => {
                    self.drive_deadline(loaded, control, read_deadline).await;
                }
                ControlRecordState::Attempt(AttemptRecord {
                    phase: AttemptPhaseRecord::Retry { .. },
                    ..
                }) => {
                    self.drive_retry(control, read_deadline).await;
                    return;
                }
                ControlRecordState::Attempt(AttemptRecord {
                    phase: AttemptPhaseRecord::Waiting { .. },
                    ..
                }) => {
                    self.drive_recovered_notification(loaded, control).await;
                }
                ControlRecordState::Attempt(_) => {
                    self.drive_unsettled_attempt(loaded, control).await;
                }
                ControlRecordState::Child { .. } => return,
            }
        }
    }

    async fn drive_unsettled_attempt(&self, loaded: LoadedRun, mut control: ControlRecord) {
        match &control.state {
            ControlRecordState::Attempt(AttemptRecord {
                phase: AttemptPhaseRecord::Unknown { .. },
                ..
            }) => {
                let owner = control.clone();
                match control.abort() {
                    Ok(ReconciliationRecord::Idle) => {
                        if control.begin_abort().is_ok() {
                            self.commit_scheduled_transition(
                                &owner,
                                ControlTransition::Park { control },
                            )
                            .await;
                        }
                    }
                    Ok(ReconciliationRecord::Intended) => {
                        let Ok(definition) = self.app.definition_for(&loaded.record) else {
                            return;
                        };
                        self.reconcile_abort(&definition, &owner, control).await;
                    }
                    _ => {}
                }
            }
            ControlRecordState::Attempt(AttemptRecord {
                phase: phase @ (AttemptPhaseRecord::Intent | AttemptPhaseRecord::Dispatched),
                ..
            }) => {
                if matches!(phase, AttemptPhaseRecord::Intent)
                    && control.retained_attempt_identity().is_some()
                {
                    let Ok(definition) = self.app.definition_for(&loaded.record) else {
                        return;
                    };
                    let Ok(request) = AgentRequest::recover(
                        &definition,
                        &loaded.record.checkpoint.state,
                        &control.node,
                        &control.lineage,
                        control.retained_attempt_identity().cloned(),
                    ) else {
                        return;
                    };
                    self.dispatch_agent(&definition, control, request).await;
                } else {
                    let owner = control.clone();
                    if control
                        .set_attempt_phase(AttemptPhaseRecord::Unknown {
                            notification_id: None,
                        })
                        .is_ok()
                    {
                        self.commit_scheduled_transition(
                            &owner,
                            ControlTransition::Park { control },
                        )
                        .await;
                    }
                }
            }
            _ => {}
        }
    }

    fn decide_checkpoint(
        &self,
        loaded: &LoadedRun,
    ) -> Result<BspDecision, RuntimeOperationFailure> {
        let definition = self.app.definition_for(&loaded.record)?;
        Ok(
            match catch_unwind(AssertUnwindSafe(|| {
                ExecutionEngine::new(&definition, self.app.typing(), self.app.conditions())
                    .advance(&loaded.record.checkpoint)
            })) {
                Ok(Ok(BspDecision::Succeeded)) if self.app.has_resolution_work(&self.key, None) => {
                    BspDecision::AwaitControls
                }
                Ok(Ok(decision)) => decision,
                Ok(Err(AppError::InvalidInput(message))) => BspDecision::Failed {
                    category: FailureCategory::Evaluation,
                    message,
                    observation: None,
                },
                Ok(Err(_)) => BspDecision::Failed {
                    category: FailureCategory::Evaluation,
                    message: "workflow evaluation failed".into(),
                    observation: None,
                },
                Err(_) => BspDecision::Failed {
                    category: FailureCategory::Execution,
                    message: "workflow callback panicked".into(),
                    observation: None,
                },
            },
        )
    }

    fn apply_decision(
        &self,
        mut loaded: LoadedRun,
        decision: BspDecision,
        activation_deadline: Option<u64>,
        prefix: Vec<StagedActivationRecord>,
    ) -> Pin<Box<impl Future<Output = Result<LoadedRun, AppError>> + '_>> {
        Box::pin(async move {
            if self.cell.lifecycle_generation().is_none()
                && let Err(failure) = self.begin_execution_observation().await
            {
                return Err(self.fail_hook(&loaded, None, failure, Vec::new()).await);
            }
            if !prefix.is_empty() {
                loaded = self
                    .commit_without_schedule(
                        loaded,
                        ControlTransition::Stage {
                            activations: prefix,
                        },
                    )
                    .await
                    .map_err(RuntimeOperationFailure::into_error)?;
            }
            let transition = match decision {
                BspDecision::Activate(_)
                | BspDecision::AwaitControls
                | BspDecision::AdmitChildren { .. }
                | BspDecision::ExecuteChildren { .. } => {
                    return Err(AppError::Integration(
                        "native activation did not produce a decision".into(),
                    ));
                }
                BspDecision::Stage(activations) => ControlTransition::Stage { activations },
                BspDecision::Barrier {
                    state,
                    frontier,
                    settled_joins,
                    superstep,
                    outcomes,
                    sources,
                } => ControlTransition::Barrier {
                    state,
                    frontier,
                    settled_joins,
                    superstep,
                    outcomes,
                    sources,
                },
                BspDecision::Routed { frontier } => ControlTransition::Routed { frontier },
                BspDecision::Interaction {
                    node,
                    lineage,
                    message,
                } => {
                    let sequence = loaded.record.checkpoint.next_control()?;
                    let deadline_millis = activation_deadline.ok_or_else(|| {
                        AppError::Integration(
                            "native control has no sampled activation deadline".into(),
                        )
                    })?;
                    ControlTransition::Park {
                        control: ControlRecord {
                            sequence,
                            node,
                            lineage,
                            deadline_millis,
                            state: ControlRecordState::Input { message },
                        },
                    }
                }
                BspDecision::Inputs {
                    node,
                    lineage,
                    required,
                } => {
                    let sequence = loaded.record.checkpoint.next_control()?;
                    let deadline_millis = activation_deadline.ok_or_else(|| {
                        AppError::Integration(
                            "transient input has no sampled activation deadline".into(),
                        )
                    })?;
                    ControlTransition::Park {
                        control: ControlRecord {
                            sequence,
                            node,
                            lineage,
                            deadline_millis,
                            state: ControlRecordState::Inputs { required },
                        },
                    }
                }
                BspDecision::Agent(request) => {
                    return self
                        .admit_agent(
                            loaded,
                            request,
                            activation_deadline.ok_or_else(|| {
                                AppError::Integration(
                                    "native Agent has no sampled activation deadline".into(),
                                )
                            })?,
                        )
                        .await;
                }
                BspDecision::Descendants {
                    node,
                    lineage,
                    inputs,
                    runtime_inputs,
                } => {
                    if !runtime_inputs.is_empty() {
                        return Err(AppError::Integration(
                            "child runtime inputs lost their execution owner".into(),
                        ));
                    }
                    let sequence = loaded.record.checkpoint.next_control()?;
                    let receipt = Box::pin(self.admit_descendants(
                        loaded,
                        node,
                        lineage,
                        inputs,
                        activation_deadline.ok_or_else(|| {
                            AppError::Integration("child activation has no sampled deadline".into())
                        })?,
                        sequence,
                    ))
                    .await;
                    let (parent, _) = Box::pin(self.finish_child_admission(sequence, receipt))
                        .await
                        .map_err(RuntimeOperationFailure::into_error)?;
                    return Ok(parent);
                }
                BspDecision::Sleep {
                    node,
                    lineage,
                    delay_millis,
                } => {
                    let sequence = loaded.record.checkpoint.next_control()?;
                    let started = Self::now_millis()?;
                    let deadline = activation_deadline.ok_or_else(|| {
                        AppError::Integration("sleep activation has no sampled deadline".into())
                    })?;
                    let deadline_millis = started
                        .checked_add(delay_millis)
                        .map(|wake| wake.min(deadline))
                        .ok_or_else(|| {
                            AppError::Integration("workflow sleep deadline is invalid".into())
                        })?;
                    ControlTransition::Park {
                        control: ControlRecord {
                            sequence,
                            node,
                            lineage,
                            deadline_millis,
                            state: ControlRecordState::Sleep,
                        },
                    }
                }
                BspDecision::Succeeded => ControlTransition::Succeeded,
                BspDecision::Failed {
                    category,
                    message,
                    observation,
                } => ControlTransition::Failed {
                    category,
                    message,
                    observation,
                },
            };
            let barrier = matches!(&transition, ControlTransition::Barrier { .. });
            let scheduled = matches!(
                &transition,
                ControlTransition::Park { .. } | ControlTransition::Failed { .. }
            );
            let mut committed = self
                .commit_without_schedule(loaded, transition)
                .await
                .map_err(RuntimeOperationFailure::into_error)?;
            if barrier {
                committed = self.observe_barrier(committed).await?;
            }
            if committed.record.checkpoint.is_terminal() {
                let observed = self
                    .finish_execution_observation(&committed.record, None, None, false)
                    .await;
                self.maintain_retention(None).await;
                observed?;
            } else if scheduled {
                self.schedule(ScheduleCause::CheckpointReady(
                    committed.record.checkpoint.sequence,
                ));
            }
            self.supervise_controls(&committed.record);
            Ok(committed)
        })
    }

    async fn admit_agent(
        &self,
        loaded: LoadedRun,
        request: AgentRequest,
        deadline_millis: u64,
    ) -> Result<LoadedRun, AppError> {
        let sequence = loaded.record.checkpoint.next_control()?;
        let intent = ControlRecord {
            sequence,
            node: request.node().to_owned(),
            lineage: request.lineage().to_owned(),
            deadline_millis,
            state: ControlRecordState::Attempt(AttemptRecord {
                number: 1,
                identity: None,
                phase: AttemptPhaseRecord::Intent,
                abort: ReconciliationRecord::Idle,
            }),
        };
        let transition = ControlTransition::Park {
            control: intent.clone(),
        };
        self.admit_attempt(loaded, transition, intent, request)?
            .await
            .map_err(|_| AppError::Closed("native control admission observation closed".into()))?
    }

    fn admit_attempt(
        &self,
        loaded: LoadedRun,
        transition: ControlTransition,
        intent: ControlRecord,
        request: AgentRequest,
    ) -> Result<tokio::sync::oneshot::Receiver<Result<LoadedRun, AppError>>, AppError> {
        let definition = self.app.definition_for(&loaded.record)?;
        let runtime = self.clone();
        let sequence = intent.sequence;
        let (outcome, observation) = tokio::sync::oneshot::channel();
        // Register the exact original native owner before its intent is visible.
        // The publication observer may finish while this task retains the prompt.
        if !self.app.spawn_run(
            self.key.clone(),
            RunTaskKind::Control { sequence },
            None,
            async move {
                // This original Control task already owns the native publication.
                // Its task ID also fences any pre-dispatch sequence replacement.
                let committed = runtime
                    .commit_checkpoint_owned(loaded, transition, Some(tokio::task::id()))
                    .await;
                let accepted = committed.as_ref().ok().and_then(|loaded| {
                    loaded
                        .record
                        .checkpoint
                        .control_for(&intent.node, &intent.lineage)
                        .filter(|control| {
                            control.deadline_millis == intent.deadline_millis
                                && control.state == intent.state
                        })
                        .cloned()
                });
                if accepted.is_none()
                    && let Ok(loaded) = &committed
                {
                    runtime.schedule(ScheduleCause::CheckpointReady(
                        loaded.record.checkpoint.sequence,
                    ));
                }
                let _ = outcome.send(committed.map_err(RuntimeOperationFailure::into_error));
                if let Some(intent) = accepted {
                    let sequence = intent.sequence;
                    runtime.dispatch_agent(&definition, intent, request).await;
                    runtime.continue_control(sequence).await;
                }
            },
        ) {
            return Err(AppError::Closed(
                "native control admission is unavailable".into(),
            ));
        }
        Ok(observation)
    }

    fn dispatch_agent<'a>(
        &'a self,
        definition: &'a CompiledDefinition,
        intent: ControlRecord,
        request: AgentRequest,
    ) -> impl Future<Output = ()> + 'a {
        let span = AppTrace::span(|| {
            tracing::info_span!(target: "app_sdk", "app.workflow.attempt",
            run_id = %self.id(), occurrence = %self.occurrence(), control = intent.sequence, attempt = intent.attempt_number().ok())
        });
        span.observe(Box::pin(async move {
            let sequence = intent.sequence;
            let Some(intent) = self
                .retain_unexpired(intent, "agent attempt deadline expired")
                .await
            else {
                return;
            };
            let attempt = Arc::new(Attempt::new(intent.sequence));
            let Some(registered) = self.register_interaction(Arc::clone(&attempt)) else {
                return;
            };
            if !Arc::ptr_eq(&registered, &attempt) {
                return;
            }
            let Ok(preparation_notifier) = self.notifier() else {
                return;
            };
            let mut preparation_changed = Box::pin(preparation_notifier.notified());
            preparation_changed.as_mut().enable();
            let remaining = Self::remaining(intent.deadline_millis).unwrap_or(Duration::ZERO);
            let prepared = {
                let preparation = async {
                    let (preparation, agent, revision) =
                        match self.preparation_permit(&intent, request.agent()).await {
                            Ok(preparation) => preparation,
                            Err(RuntimeOperationFailure::Storage(failure)) => {
                                return Ok(AgentPreparation::Storage(failure));
                            }
                            Err(RuntimeOperationFailure::Application(error)) => return Err(error),
                        };
                    let workspace = match WorkspaceRuntime::open(
                        Arc::clone(&self.app),
                        self.workspace().clone(),
                    )
                    .await
                    {
                        Ok(workspace) => workspace,
                        Err(_) => {
                            return Ok(AgentPreparation::RetrySafe(
                                "agent workspace is unavailable",
                            ));
                        }
                    };
                    let lower_workspace =
                        match workspace.lower_workspace(request.agent(), &revision).await {
                            Ok(value) => value,
                            Err(_) => {
                                return Ok(AgentPreparation::RetrySafe(
                                    "configured agent workspace is unavailable",
                                ));
                            }
                        };
                    if let Err(failure) = self.app.current_generation_record_for_recovery().await {
                        return Ok(AgentPreparation::Storage(failure));
                    }
                    let prompt = match Attempt::<A>::prompt(&request) {
                        Ok(prompt) => prompt,
                        Err(_) => return Ok(AgentPreparation::Rejected),
                    };
                    if Self::remaining(intent.deadline_millis)?.is_zero() {
                        return Err(AppError::Timeout(
                            "agent preparation deadline expired".to_owned(),
                        ));
                    }
                    attempt.prepare(agent, &lower_workspace, &request).await?;
                    drop(preparation);
                    Ok(AgentPreparation::Ready {
                        prompt: Box::new(prompt),
                        workspace_id: lower_workspace,
                    })
                };
                tokio::pin!(preparation);
                let deadline = tokio::time::sleep(remaining);
                tokio::pin!(deadline);
                loop {
                    tokio::select! {
                        biased;
                        _ = &mut deadline => break Err(AppError::Timeout("agent preparation deadline expired".to_owned())),
                        result = &mut preparation => break result,
                        _ = &mut preparation_changed => {
                            preparation_changed.set(preparation_notifier.notified());
                            preparation_changed.as_mut().enable();
                            let current = tokio::select! {
                                result = self.load_execution_observation(None, None) => result,
                                _ = &mut deadline => break Err(AppError::Timeout("agent preparation deadline expired".to_owned())),
                            };
                            match current {
                                Ok(Some(current)) if !current.record.checkpoint.is_stopping()
                                    && current.record.checkpoint.control(sequence).is_some_and(|control| control.same_owner(&intent)) => {}
                                Err(failure) => {
                                    // The original preparation may already be
                                    // creating a conversation. This read cannot
                                    // manufacture a known-no-effect settlement.
                                    self.retain_execution_read_failure(&failure);
                                    self.schedule_after_storage_failure(&failure);
                                    return;
                                },
                                Ok(_) => break Err(AppError::Closed("agent preparation ownership was revoked".to_owned())),
                            }
                        }
                    }
                }
            };
            // The timed phase owns cancellable reads and lower preparation only.
            // Once a durable settlement is admitted, retain it through its receipt.
            let (prompt, lower_workspace) = match prepared {
                Ok(AgentPreparation::Ready {
                    prompt,
                    workspace_id,
                }) => (*prompt, workspace_id),
                Ok(AgentPreparation::RetrySafe(message)) => {
                    self.retry_safe_failure(definition, intent.clone(), FailureCategory::Agent, message)
                        .await;
                    self.remove_interaction(intent.sequence);
                    return;
                }
                Ok(AgentPreparation::Rejected) => {
                    self.settle_unsubmitted_failure(
                        intent.clone(),
                        FailureCategory::Agent,
                        "agent prompt preparation was rejected",
                    )
                    .await;
                    self.remove_interaction(intent.sequence);
                    return;
                }
                Ok(AgentPreparation::Storage(failure)) => {
                    if failure.is_recoverable() {
                        // These reads precede conversation creation. Retain the
                        // original no-effect proof through its native settlement.
                        self.retry_safe_failure(definition, intent.clone(), FailureCategory::Agent,
                            "app storage failed before conversation creation").await;
                    } else {
                        self.retain_execution_read_failure(&failure);
                    }
                    self.remove_interaction(intent.sequence);
                    return;
                }
                Err(error) => {
                    if attempt.may_have_created_conversation() {
                        let owner = intent.clone();
                        let mut unknown = intent;
                        if unknown
                            .set_attempt_phase(AttemptPhaseRecord::Unknown {
                                notification_id: None,
                            })
                            .is_ok()
                        {
                            self.commit_scheduled_transition(
                                &owner,
                                ControlTransition::Park { control: unknown },
                            )
                            .await;
                        }
                    } else {
                        let category = if matches!(error, AppError::Timeout(_)) {
                            FailureCategory::Timeout
                        } else {
                            FailureCategory::Agent
                        };
                        self.retry_safe_failure(
                            definition,
                            intent,
                            category,
                            "agent preparation failed before conversation creation",
                        )
                        .await;
                        self.remove_interaction(sequence);
                    }
                    return;
                }
            };
            let Ok(conversation_id) = attempt.conversation_id() else {
                return;
            };
            let owner = intent;
            let mut dispatched = owner.clone();
            if dispatched
                .prepare_dispatch(AttemptIdentity {
                    workspace_id: lower_workspace,
                    conversation_id,
                })
                .is_err()
            {
                return;
            }
            let record = match self
                .commit_control_transition(
                    &owner,
                    ControlTransition::Park {
                        control: dispatched.clone(),
                    },
                    None,
                )
                .await
            {
                Ok(Some(record)) => record,
                Ok(None) => {
                    self.schedule(ScheduleCause::CurrentState);
                    return;
                }
                Err(failure) => {
                    self.schedule_after_storage_failure(&failure);
                    return;
                }
            };
            let record = record.record;
            if record.checkpoint.is_stopping() || record.checkpoint.control(sequence) != Some(&dispatched) {
                self.schedule(ScheduleCause::CheckpointReady(record.checkpoint.sequence));
                return;
            }
            if !Self::now_millis().is_ok_and(|now| now < dispatched.deadline_millis) {
                self.expire_attempt(dispatched).await;
                return;
            }
            self.observe_attempt(definition, dispatched, request, attempt, prompt)
                .await;
        }))
    }

    fn observe_attempt<'a>(
        &'a self,
        definition: &'a CompiledDefinition,
        mut dispatched: ControlRecord,
        mut request: AgentRequest,
        attempt: Arc<Attempt<A>>,
        prompt: PromptRequest,
    ) -> Pin<Box<impl Future<Output = ()> + 'a>> {
        Box::pin(async move {
            let sequence = dispatched.sequence;
            let observed_at = tokio::time::Instant::now();
            let hook_deadline = observed_at
                .checked_add(Self::remaining(dispatched.deadline_millis).unwrap_or(Duration::ZERO))
                .unwrap_or(observed_at);
            let Ok(notifier) = self.notifier() else {
                return;
            };
            let mut notified = Box::pin(notifier.notified());
            notified.as_mut().enable();
            let receiver = match attempt.notification_stream() {
                Ok(receiver) => receiver,
                Err(_) => {
                    self.remove_interaction(dispatched.sequence);
                    let owner = dispatched.clone();
                    let mut unknown = dispatched;
                    if unknown
                        .set_attempt_phase(AttemptPhaseRecord::Unknown {
                            notification_id: None,
                        })
                        .is_err()
                        || self
                            .commit_scheduled_transition(
                                &owner,
                                ControlTransition::Park { control: unknown },
                            )
                            .await
                            .is_none()
                    {
                        self.schedule(ScheduleCause::CurrentState);
                    }
                    return;
                }
            };
            let Ok(conversation) = attempt.conversation() else {
                return;
            };
            let mut stream_observation = None;
            let (ready, settle) = match self
                .load_execution_observation(None, Some(hook_deadline))
                .await
            {
                Ok(Some(current)) => {
                    if request.is_streaming() {
                        stream_observation =
                            self.cell.lifecycle_generation().and_then(|generation| {
                                self.cell
                                    .action_context(
                                        current.record.checkpoint.base_sequence,
                                        request.node(),
                                        request.lineage(),
                                    )
                                    .map(|context| (generation, context))
                            });
                    }
                    let same_reservation = current
                        .record
                        .checkpoint
                        .control(sequence)
                        .is_some_and(|control| control.same_owner(&dispatched));
                    (
                        same_reservation
                            && !current.record.checkpoint.is_stopping()
                            && current.record.checkpoint.control(sequence) == Some(&dispatched),
                        same_reservation && current.record.checkpoint.is_stopping(),
                    )
                }
                Ok(None) => (false, false),
                Err(failure) => {
                    self.remove_interaction(dispatched.sequence);
                    self.retain_execution_read_failure(&failure);
                    self.schedule_after_storage_failure(&failure);
                    return;
                }
            };
            if !ready {
                self.remove_interaction(dispatched.sequence);
                if settle {
                    self.commit_scheduled_transition(
                        &dispatched,
                        ControlTransition::AttemptSettled { sequence },
                    )
                    .await;
                }
                return;
            }
            AppTrace::event(|| {
                tracing::info!(
                    target: "app_sdk",
                    event = "app.workflow.attempt",
                    run_id = %self.id(), occurrence = %self.occurrence(),
                    control = dispatched.sequence,
                    attempt = dispatched.attempt_number().unwrap_or(0),
                    transition = "dispatched",
                )
            });
            let streaming = request.is_streaming();
            let mut partials = request.take_partials();
            let prompt_future = async {
                if streaming {
                    conversation
                        .prompt_stream(prompt)
                        .await
                        .map(AttemptObservation::Stream)
                } else {
                    conversation
                        .prompt(prompt)
                        .await
                        .map(AttemptObservation::Response)
                }
            };
            tokio::pin!(prompt_future);
            let mut stream: Option<PromptStream> = None;
            let mut pending_partial = None;
            let mut pending_terminal = None;
            let mut receiver = Some(receiver);
            let mut receiver_open = true;
            let mut receiver_delay = RECONCILIATION_INITIAL_DELAY;
            let mut deadline_open = true;
            loop {
                notified.as_mut().enable();
                let remaining =
                    Self::remaining(dispatched.deadline_millis).unwrap_or(Duration::ZERO);
                let ready = matches!(
                    &dispatched.state,
                    ControlRecordState::Attempt(AttemptRecord {
                        phase: AttemptPhaseRecord::Dispatched,
                        ..
                    })
                ) && !self.app.has_resolution_work(&self.key, Some(sequence));
                let next_notification = if ready {
                    attempt.next_notification()
                } else {
                    None
                };
                let observation = tokio::select! {
                    biased;
                    _ = tokio::time::sleep(remaining), if deadline_open => {
                        deadline_open = false;
                        self.expire_attempt(dispatched.clone()).await;
                        continue;
                    }
                    _ = &mut notified => {
                        notified.set(notifier.notified());
                        notified.as_mut().enable();
                        // A valid attempt retains its original deadline. After
                        // its timeout publication owner returns, observing its
                        // abort uses a new finite pure-read pass.
                        let loaded = match self.load_execution_observation(
                            None, deadline_open.then_some(hook_deadline),
                        ).await {
                            Ok(Some(loaded)) => loaded,
                            Ok(None) => return,
                            Err(failure) => {
                                self.retain_execution_read_failure(&failure);
                                self.schedule_after_storage_failure(&failure);
                                return;
                            }
                        };
                        if loaded.record.checkpoint.is_stopping() {
                            return;
                        }
                        let Some(current) = loaded.record.checkpoint.control(sequence) else {
                            return;
                        };
                        if !matches!(&current.state, ControlRecordState::Attempt(_))
                            || !current.same_owner(&dispatched)
                        {
                            return;
                        }
                        let unknown = matches!(&current.state, ControlRecordState::Attempt(AttemptRecord {
                            phase: AttemptPhaseRecord::Unknown { .. }, ..
                        }));
                        dispatched = current.clone();
                        if unknown {
                            receiver_open = false;
                            // This control still owns the original prompt. Advance
                            // its abort here; another same-sequence task cannot enter.
                            self.drive_unsettled_attempt(loaded, dispatched.clone()).await;
                        }
                        continue;
                    }
                    notification = async {
                        match receiver.as_mut() {
                            Some(receiver) => receiver.recv().await,
                            None => None,
                        }
                    }, if receiver_open => {
                        match notification {
                            Some(Ok(notification)) => {
                                receiver_delay = RECONCILIATION_INITIAL_DELAY;
                                AttemptObservation::Notification(Arc::new(notification))
                            }
                            Some(Err(_)) => {
                                receiver_open = false;
                                continue;
                            }
                            None => {
                                receiver = None;
                                receiver_open = false;
                                continue;
                            }
                        }
                    }
                    _ = std::future::ready(()), if ready && (next_notification.is_some() || pending_terminal.is_some()) => {
                        match next_notification {
                            Some(notification) => AttemptObservation::Notification(notification),
                            None => pending_terminal.take().expect("ready terminal observation is retained"),
                        }
                    }
                    response = &mut prompt_future, if stream.is_none() && pending_terminal.is_none() => {
                        match response {
                            Ok(AttemptObservation::Response(response)) => {
                                pending_terminal = Some(AttemptObservation::Response(response));
                                continue;
                            }
                            Ok(response) => response,
                            Err(error) => AttemptObservation::Failed(error),
                        }
                    }
                    _ = tokio::time::sleep(receiver_delay), if !receiver_open && !matches!(&dispatched.state, ControlRecordState::Attempt(AttemptRecord { phase: AttemptPhaseRecord::Unknown { .. }, .. })) => {
                        if receiver.is_some() {
                            receiver_open = true;
                            receiver_delay = receiver_delay
                                .saturating_mul(2)
                                .min(RECONCILIATION_MAX_DELAY);
                            continue;
                        }
                        match conversation.notification_stream() {
                            Ok(next) => {
                                receiver = Some(next);
                                receiver_open = true;
                                receiver_delay = RECONCILIATION_INITIAL_DELAY;
                            }
                            Err(_) => {
                                receiver_delay = receiver_delay
                                    .saturating_mul(2)
                                    .min(RECONCILIATION_MAX_DELAY);
                            }
                        }
                        continue;
                    }
                    reservation = async {
                        partials.as_ref().expect("pending native output has a sender")
                            .clone().reserve_owned().await
                    }, if pending_partial.is_some() => {
                        let delivered = match reservation {
                            Ok(permit) => {
                                permit.send(crate::workflow::ExecutionEvent::Partial {
                                    node: request.node().to_owned(),
                                    result: pending_partial.take().expect("reserved native output exists"),
                                });
                                true
                            }
                            Err(_) => false,
                        };
                        if !delivered {
                            // Losing the Execution receiver ends observation,
                            // not the already accepted lower attempt. Its same
                            // owner still observes terminal or stopping truth.
                            partials = None;
                            pending_partial = None;
                        }
                        continue;
                    }
                    event = async {
                        stream.as_mut().expect("native stream has been accepted").recv().await
                    }, if stream.is_some() && pending_partial.is_none() && pending_terminal.is_none() => {
                        match event {
                            Some(Ok(PromptEvent::OutputDelta { text })) => {
                                let result = Map::from_iter([("output_delta".to_owned(), Value::String(text))]);
                                match JsonBoundary::validate_size(&result) {
                                    Ok(()) => AttemptObservation::Partial(result),
                                    Err(_) => AttemptObservation::Failed(SdkError::protocol(
                                        "native prompt partial exceeds the result byte bound",
                                    )),
                                }
                            }
                            Some(Ok(PromptEvent::Completed(response))) => {
                                pending_terminal = Some(AttemptObservation::Response(response));
                                continue;
                            },
                            Some(Err(error)) => AttemptObservation::Failed(error),
                            None => AttemptObservation::Failed(SdkError::protocol(
                                "native prompt stream ended without terminal evidence",
                            )),
                        }
                    }
                };
                let transition = match observation {
                    AttemptObservation::Stream(accepted) => {
                        stream = Some(accepted);
                        let Some((generation, context)) = &stream_observation else {
                            continue;
                        };
                        match self
                            .observe_stream_start(*generation, context, hook_deadline)
                            .await
                        {
                            Ok(()) => continue,
                            Err(failure) => ControlTransition::Failed {
                                category: FailureCategory::from_failure(&failure),
                                message: failure.message().to_owned(),
                                observation: None,
                            },
                        }
                    }
                    AttemptObservation::Partial(result) => {
                        let observed = match &stream_observation {
                            Some((generation, context)) => {
                                self.observe_stream_item(
                                    *generation,
                                    context,
                                    &result,
                                    hook_deadline,
                                )
                                .await
                            }
                            None => Ok(()),
                        };
                        match observed {
                            Err(failure) => ControlTransition::Failed {
                                category: FailureCategory::from_failure(&failure),
                                message: failure.message().to_owned(),
                                observation: None,
                            },
                            Ok(()) => {
                                if partials.is_some() {
                                    pending_partial = Some(result);
                                } else {
                                    // A background recovery or revoked observer still
                                    // uses prompt_stream. Cooperate while draining ready
                                    // output so other owners and cancellation can run.
                                    tokio::task::yield_now().await;
                                }
                                continue;
                            }
                        }
                    }
                    AttemptObservation::Notification(notification) => {
                        if !matches!(
                            notification.kind(),
                            ConversationNotificationKind::Interaction { .. }
                        ) {
                            continue;
                        }
                        let Some(current) = self
                            .observe_notification(
                                &dispatched,
                                notification,
                                deadline_open.then_some(hook_deadline),
                            )
                            .await
                        else {
                            return;
                        };
                        dispatched = current;
                        continue;
                    }
                    AttemptObservation::Response(response) => {
                        let staged = self
                            .stage_agent_response(definition, &request, response)
                            .unwrap_or_else(|category| {
                                StagedActivationRecord::failed(
                                    request.node().to_owned(),
                                    request.lineage().to_owned(),
                                    category,
                                    "agent response could not be staged".to_owned(),
                                )
                            });
                        ControlTransition::Stage {
                            activations: vec![staged],
                        }
                    }
                    AttemptObservation::Failed(error) => {
                        let Ok(Some(loaded)) = self
                            .load_preserving_observation(deadline_open.then_some(hook_deadline))
                            .await
                        else {
                            return;
                        };
                        let Some(mut control) = loaded.record.checkpoint.control(sequence).cloned()
                        else {
                            return;
                        };
                        if !control.same_owner(&dispatched) {
                            return;
                        }
                        let stopping = loaded.record.checkpoint.is_stopping();
                        // A lower terminal error cannot replace a notification
                        // resolution's original receipt, regardless of category
                        // or whether the prompt returned an accepted stream.
                        if !(stopping && error.kind() == agent_sdk::error::ErrorKind::Cancelled)
                            && (attempt.next_notification().is_some()
                                || self.app.has_resolution_work(&self.key, Some(sequence))
                                || !matches!(
                                    &control.state,
                                    ControlRecordState::Attempt(AttemptRecord {
                                        phase: AttemptPhaseRecord::Dispatched,
                                        ..
                                    })
                                ))
                        {
                            pending_terminal = Some(AttemptObservation::Failed(error));
                            continue;
                        }
                        match error.kind() {
                            agent_sdk::error::ErrorKind::Cancelled if stopping => {
                                ControlTransition::AttemptSettled { sequence }
                            }
                            agent_sdk::error::ErrorKind::NotFound
                            | agent_sdk::error::ErrorKind::Closed
                            | agent_sdk::error::ErrorKind::Cancelled
                                if stream.is_none() =>
                            {
                                ControlTransition::Stage {
                                    activations: vec![StagedActivationRecord::failed(
                                        request.node().to_owned(),
                                        request.lineage().to_owned(),
                                        FailureCategory::Agent,
                                        "agent prompt failed".to_owned(),
                                    )],
                                }
                            }
                            agent_sdk::error::ErrorKind::InvalidConfig
                            | agent_sdk::error::ErrorKind::InvalidInput
                            | agent_sdk::error::ErrorKind::NotFound
                            | agent_sdk::error::ErrorKind::Closed
                            | agent_sdk::error::ErrorKind::Cancelled
                            | agent_sdk::error::ErrorKind::Unsupported
                            | agent_sdk::error::ErrorKind::NotYetImplemented
                            | agent_sdk::error::ErrorKind::Conflict
                            | agent_sdk::error::ErrorKind::Busy
                            | agent_sdk::error::ErrorKind::Timeout
                            | agent_sdk::error::ErrorKind::Protocol
                            | agent_sdk::error::ErrorKind::InvalidState
                            | agent_sdk::error::ErrorKind::Integration => {
                                // After dispatch, an error category or the
                                // absence of a stream handle cannot prove that
                                // the lower prompt performed no work.
                                let notification_id = control.notification_id().ok().cloned();
                                if control
                                    .set_attempt_phase(AttemptPhaseRecord::Unknown {
                                        notification_id,
                                    })
                                    .is_err()
                                {
                                    return;
                                }
                                ControlTransition::Park { control }
                            }
                        }
                    }
                };
                if self
                    .commit_scheduled_transition(&dispatched, transition)
                    .await
                    .is_some()
                {
                    self.remove_interaction(dispatched.sequence);
                } else {
                    self.schedule(ScheduleCause::CurrentState);
                }
                return;
            }
        })
    }

    async fn observe_notification(
        &self,
        owner: &ControlRecord,
        notification: Arc<agent_sdk::notification::ConversationNotification>,
        deadline: Option<tokio::time::Instant>,
    ) -> Option<ControlRecord> {
        for _ in 0..MAX_RECONCILIATION_ATTEMPTS {
            let loaded = self.load_preserving_observation(deadline).await.ok()??;
            let control = loaded.record.checkpoint.control(owner.sequence)?.clone();
            if !control.same_attempt(owner) {
                return None;
            }
            if loaded.record.checkpoint.is_stopping() {
                return Some(control);
            }
            let attempt = self.interaction(control.sequence)?;
            let delivered = SettlementTarget::from_notification(&control, &notification).ok()?;
            if let Some(target) = loaded
                .record
                .checkpoint
                .notification_target(&control, notification.id())
            {
                if target.target != delivered {
                    return None;
                }
                if matches!(target.state, SettlementState::Applied { .. }) {
                    attempt.remove_notification(notification.id());
                } else {
                    attempt
                        .install_notification(Arc::clone(&notification))
                        .ok()?;
                    self.wake();
                }
                return Some(control);
            }
            let target = attempt
                .install_notification(Arc::clone(&notification))
                .and_then(|()| {
                    if !matches!(
                        &control.state,
                        ControlRecordState::Attempt(AttemptRecord {
                            phase: AttemptPhaseRecord::Dispatched,
                            ..
                        })
                    ) || self
                        .app
                        .has_resolution_work(&self.key, Some(control.sequence))
                        || attempt
                            .next_notification()
                            .is_none_or(|next| next.id() != notification.id())
                    {
                        return Ok(None);
                    }
                    InteractionSettlement::pending(&loaded.record.checkpoint, &control, delivered)
                        .map(Some)
                });
            let committed = match target {
                Ok(Some(target)) => Some(
                    self.commit_for_recovery(
                        loaded,
                        ControlTransition::Settlement { settlement: target },
                    )
                    .await,
                ),
                Ok(None) => return Some(control),
                Err(_) => None,
            };
            match committed {
                Some(Ok(loaded)) => {
                    return loaded.record.checkpoint.control(owner.sequence).cloned();
                }
                None
                | Some(Err(RuntimeOperationFailure::Application(AppError::InvalidInput(_)))) => {
                    self.commit_scheduled_transition(
                        &control,
                        ControlTransition::Failed {
                            category: FailureCategory::Limit,
                            message: "workflow notification ownership limit exceeded".to_owned(),
                            observation: None,
                        },
                    )
                    .await;
                    return None;
                }
                Some(Err(failure)) if failure.is_retryable_conflict() => {
                    tokio::task::yield_now().await;
                }
                Some(Err(_)) => return None,
            }
        }
        self.schedule(ScheduleCause::CurrentState);
        None
    }

    async fn commit_control_transition(
        &self,
        owner: &ControlRecord,
        mut transition: ControlTransition,
        mut loaded: Option<LoadedRun>,
    ) -> Result<Option<LoadedRun>, StorageFailure> {
        let Ok(notifier) = self.notifier() else {
            return Ok(None);
        };
        let mut notified = Box::pin(notifier.notified());
        let mut delay = RECONCILIATION_INITIAL_DELAY;
        let mut conflict_revision = None;
        let mut transient_attempts = 0;
        loop {
            notified.as_mut().enable();
            // An owned publication can supply the first candidate. Its native
            // head/predecessor fences still arbitrate concurrent progress; every
            // retry obtains fresh authority through the existing read path.
            let current = match loaded.take() {
                Some(loaded) => Ok(Some(loaded)),
                None => self.load_for_recovery().await,
            };
            match current {
                Ok(Some(loaded)) => {
                    let pending = transition.awaits_attempt_settlement(&loaded.record, owner);
                    if !transition.rebase(&loaded.record, owner) && !pending {
                        return Ok(None);
                    }
                    if !pending {
                        let mut candidate = transition.clone();
                        if !loaded.record.checkpoint.is_stopping()
                            && let ControlTransition::Failed {
                                category,
                                message,
                                observation,
                            } = &mut candidate
                            && observation.is_none()
                        {
                            let Ok(definition) = self.app.definition_for(&loaded.record) else {
                                return Ok(None);
                            };
                            let position = match self.cell.action_context(
                                loaded.record.checkpoint.base_sequence,
                                &owner.node,
                                &owner.lineage,
                            ) {
                                Some(context) => ActionPosition {
                                    node: context.node.clone(),
                                    ordinal: context.action_ordinal,
                                },
                                None => match loaded.record.checkpoint.proposed_position(
                                    &definition,
                                    &owner.node,
                                    &owner.lineage,
                                    &[],
                                ) {
                                    Ok(position) => position,
                                    Err(_) => return Ok(None),
                                },
                            };
                            *observation = Some(
                                match loaded.record.checkpoint.preceding_failure(
                                    &definition,
                                    &position,
                                    &owner.lineage,
                                ) {
                                    Ok(Some((position, prior_category, prior_message))) => {
                                        *category = prior_category;
                                        *message = prior_message.to_owned();
                                        position
                                    }
                                    Ok(None) => position,
                                    Err(_) => return Ok(None),
                                },
                            );
                        }
                        let revision = loaded.revision;
                        match self.commit_without_schedule(loaded, candidate).await {
                            Ok(loaded) => return Ok(Some(loaded)),
                            Err(RuntimeOperationFailure::Application(AppError::Conflict(_))) => {
                                if conflict_revision == Some(revision) {
                                    return Ok(None);
                                }
                                conflict_revision = Some(revision);
                            }
                            Err(RuntimeOperationFailure::Storage(failure)) => {
                                if failure.is_cleanup_failed() {
                                    return Err(failure);
                                }
                                transient_attempts += 1;
                                if transient_attempts >= MAX_RECONCILIATION_ATTEMPTS {
                                    self.schedule_after_storage_failure(&failure);
                                    return Ok(None);
                                }
                            }
                            Err(RuntimeOperationFailure::Application(error))
                                if matches!(
                                    error,
                                    AppError::Busy(_)
                                        | AppError::Timeout(_)
                                        | AppError::Integration(_)
                                ) =>
                            {
                                transient_attempts += 1;
                                if transient_attempts >= MAX_RECONCILIATION_ATTEMPTS {
                                    self.schedule_after_transient(&error);
                                    return Ok(None);
                                }
                            }
                            Err(_) => return Ok(None),
                        }
                    }
                }
                Err(failure) => {
                    if failure.is_cleanup_failed() {
                        return Err(failure);
                    }
                    transient_attempts += 1;
                    if transient_attempts >= MAX_RECONCILIATION_ATTEMPTS {
                        self.schedule_after_storage_failure(&failure);
                        return Ok(None);
                    }
                }
                Ok(None) => return Ok(None),
            }
            tokio::select! {
                _ = &mut notified => {
                    notified.set(notifier.notified());
                }
                _ = tokio::time::sleep(delay) => {}
            }
            delay = delay.saturating_mul(2).min(RECONCILIATION_MAX_DELAY);
        }
    }

    fn commit_scheduled_transition<'a>(
        &'a self,
        owner: &'a ControlRecord,
        transition: ControlTransition,
    ) -> Pin<Box<impl Future<Output = Option<LoadedRun>> + 'a>> {
        Box::pin(async move {
            let loaded = match self
                .commit_control_transition(owner, transition, None)
                .await
            {
                Ok(loaded) => loaded?,
                Err(failure) => {
                    self.schedule_after_storage_failure(&failure);
                    return None;
                }
            };
            self.schedule(ScheduleCause::CheckpointReady(
                loaded.record.checkpoint.sequence,
            ));
            Some(loaded)
        })
    }

    async fn retry_safe_failure(
        &self,
        definition: &CompiledDefinition,
        mut control: ControlRecord,
        category: FailureCategory,
        message: &str,
    ) {
        let unsubmitted = control.clone();
        let Ok(now) = Self::now_millis() else {
            return;
        };
        let Some(node) = definition.node(&control.node) else {
            return;
        };
        let number = match control.attempt_number() {
            Ok(number) => number,
            Err(_) => return,
        };
        let started_millis = control
            .deadline_millis
            .saturating_sub(node.deadline_millis());
        let delay = node.retry_delay_millis(number, now.saturating_sub(started_millis));
        let Some(delay) = delay else {
            self.settle_unsubmitted_failure(unsubmitted, category, message)
                .await;
            return;
        };
        let Some(wake_millis) = now.checked_add(delay) else {
            return;
        };
        if wake_millis > control.deadline_millis {
            self.settle_unsubmitted_failure(
                unsubmitted,
                FailureCategory::Timeout,
                "agent retry deadline expired",
            )
            .await;
            return;
        }
        let Some(next_attempt) = number.checked_add(1) else {
            self.settle_unsubmitted_failure(
                unsubmitted,
                FailureCategory::Limit,
                "agent attempt count is exhausted",
            )
            .await;
            return;
        };
        if control.prepare_retry(next_attempt, wake_millis).is_err() {
            return;
        }
        if self
            .commit_scheduled_transition(&unsubmitted, ControlTransition::Park { control })
            .await
            .is_none()
        {
            self.settle_unsubmitted_failure(unsubmitted, category, message)
                .await;
        }
    }

    async fn settle_unsubmitted_failure(
        &self,
        control: ControlRecord,
        category: FailureCategory,
        message: &str,
    ) {
        let loaded = match self.load_execution_observation(None, None).await {
            Ok(Some(loaded)) => loaded,
            Err(failure) => {
                self.retain_execution_read_failure(&failure);
                self.schedule_after_storage_failure(&failure);
                return;
            }
            Ok(None) => return,
        };
        let same_reservation = loaded
            .record
            .checkpoint
            .control(control.sequence)
            .is_some_and(|current| current.same_owner(&control));
        if !same_reservation {
            return;
        }
        if !loaded.record.checkpoint.is_stopping()
            && loaded.record.checkpoint.control(control.sequence) != Some(&control)
        {
            return;
        }
        // This owner proved the prompt was not accepted. Staging that
        // result also retires a cancelled reservation without inventing a
        // lower abort receipt or replacing the accepted cancellation.
        let transition = ControlTransition::Stage {
            activations: vec![control.failed_activation(category, message.to_owned())],
        };
        match self.commit(loaded, transition).await {
            Ok(record) => {
                self.schedule(ScheduleCause::CheckpointReady(record.checkpoint.sequence));
            }
            Err(RuntimeOperationFailure::Storage(failure)) if failure.is_cleanup_failed() => {
                self.schedule_after_storage_failure(&failure);
            }
            Err(_) => {
                self.schedule(ScheduleCause::CurrentState);
            }
        }
    }

    async fn retain_unexpired(
        &self,
        control: ControlRecord,
        message: &str,
    ) -> Option<ControlRecord> {
        let now = Self::now_millis().ok()?;
        if now < control.deadline_millis {
            return Some(control);
        }
        self.settle_unsubmitted_failure(control, FailureCategory::Timeout, message)
            .await;
        None
    }

    async fn expire_attempt(&self, control: ControlRecord) {
        let transition = ControlTransition::timeout("agent attempt deadline expired");
        if self
            .commit_scheduled_transition(&control, transition)
            .await
            .is_none()
        {
            self.schedule(ScheduleCause::CurrentState);
        }
    }

    async fn drive_deadline(
        &self,
        loaded: LoadedRun,
        control: ControlRecord,
        read_deadline: tokio::time::Instant,
    ) {
        let transition = match &control.state {
            ControlRecordState::Input { .. } | ControlRecordState::Inputs { .. } => {
                ControlTransition::timeout("caller interaction deadline expired")
            }
            ControlRecordState::Sleep => {
                let Ok(definition) = self.app.definition_for(&loaded.record) else {
                    return;
                };
                let wake_millis = definition.node(&control.node).and_then(|node| {
                    let Step::Sleep { delay_ms, .. } = node.step() else {
                        return None;
                    };
                    control
                        .deadline_millis
                        .checked_sub((*delay_ms).min(node.deadline_millis()))?
                        .checked_add(*delay_ms)
                });
                let Some(wake_millis) = wake_millis else {
                    let _ = self
                        .commit_failure(
                            loaded,
                            FailureCategory::Execution,
                            "workflow sleep deadline is invalid".to_owned(),
                        )
                        .await;
                    return;
                };
                if wake_millis > control.deadline_millis {
                    ControlTransition::timeout("workflow sleep deadline expired")
                } else {
                    let staged =
                        ExecutionEngine::new(&definition, self.app.typing(), self.app.conditions())
                            .stage_after_control(
                                &control.node,
                                &control.lineage,
                                Vec::new(),
                                Map::new(),
                            )
                            .unwrap_or_else(|_| {
                                control.failed_activation(
                                    FailureCategory::Limit,
                                    "workflow sleep output exceeds the private bound".to_owned(),
                                )
                            });
                    ControlTransition::Stage {
                        activations: vec![staged],
                    }
                }
            }
            _ => return,
        };
        let Ok(notifier) = self.notifier() else {
            return;
        };
        let mut notified = Box::pin(notifier.notified());
        notified.as_mut().enable();
        let current = match self
            .load_execution_observation(None, Some(read_deadline))
            .await
        {
            Ok(Some(current)) => current,
            Err(failure) => {
                self.retain_execution_read_failure(&failure);
                self.schedule_after_storage_failure(&failure);
                return;
            }
            Ok(None) => return,
        };
        if current.record.checkpoint.control(control.sequence) != Some(&control) {
            return;
        }
        let Ok(remaining) = Self::remaining(control.deadline_millis) else {
            return;
        };
        tokio::select! {
            _ = &mut notified => return,
            _ = tokio::time::sleep(remaining) => {}
        }
        self.commit_scheduled_transition(&control, transition).await;
    }

    async fn drive_retry(&self, control: ControlRecord, read_deadline: tokio::time::Instant) {
        let wake_millis = match &control.state {
            ControlRecordState::Attempt(AttemptRecord {
                phase: AttemptPhaseRecord::Retry { wake_millis },
                ..
            }) => *wake_millis,
            _ => return,
        };
        let Ok(notifier) = self.notifier() else {
            return;
        };
        let mut notified = Box::pin(notifier.notified());
        notified.as_mut().enable();
        let current = match self
            .load_execution_observation(None, Some(read_deadline))
            .await
        {
            Ok(Some(current)) => current,
            Err(failure) => {
                self.retain_execution_read_failure(&failure);
                self.schedule_after_storage_failure(&failure);
                return;
            }
            Ok(None) => return,
        };
        if current.record.checkpoint.control(control.sequence) != Some(&control) {
            return;
        }
        let Ok(remaining) = Self::remaining(wake_millis) else {
            return;
        };
        tokio::select! {
            _ = &mut notified => return,
            _ = tokio::time::sleep(remaining) => {}
        }
        // The valid retry wait is complete. Observe current authority under a
        // new pure-read pass before the existing native activation-expiry check.
        let loaded = match self.load_execution_observation(None, None).await {
            Ok(Some(loaded)) => loaded,
            Err(failure) => {
                self.retain_execution_read_failure(&failure);
                self.schedule_after_storage_failure(&failure);
                return;
            }
            Ok(None) => return,
        };
        if loaded.record.checkpoint.control(control.sequence) != Some(&control) {
            return;
        }
        let Some(control) = self
            .retain_unexpired(control, "agent retry deadline expired")
            .await
        else {
            return;
        };
        let Ok(definition) = self.app.definition_for(&loaded.record) else {
            return;
        };
        let Some(node) = definition.node(&control.node) else {
            return;
        };
        let (Ok(number), Ok(now)) = (control.attempt_number(), Self::now_millis()) else {
            return;
        };
        let started_millis = control
            .deadline_millis
            .saturating_sub(node.deadline_millis());
        if !node.retry_permits_attempt(number, now.saturating_sub(started_millis)) {
            self.settle_unsubmitted_failure(
                control,
                FailureCategory::Agent,
                "node retry policy is exhausted",
            )
            .await;
            return;
        }
        let sequence = control.sequence;
        let Ok(next) = loaded.record.checkpoint.next_control() else {
            return;
        };
        let Ok(intent) = control.retry_intent(next) else {
            return;
        };
        let Ok(request) = AgentRequest::recover(
            &definition,
            &loaded.record.checkpoint.state,
            &intent.node,
            &intent.lineage,
            intent.retained_attempt_identity().cloned(),
        ) else {
            return;
        };
        let accepted = match self.admit_attempt(
            loaded,
            ControlTransition::RetryReady { sequence },
            intent,
            request,
        ) {
            Ok(receipt) => receipt.await.is_ok_and(|result| result.is_ok()),
            Err(_) => false,
        };
        if !accepted {
            self.schedule(ScheduleCause::CurrentState);
        }
    }

    async fn drive_recovered_notification(&self, loaded: LoadedRun, control: ControlRecord) {
        let Ok(definition) = self.app.definition_for(&loaded.record) else {
            return;
        };
        let Some(mut target) = loaded
            .record
            .checkpoint
            .current_settlement(&control)
            .cloned()
        else {
            return;
        };
        match &target.state {
            SettlementState::Intended { .. } => {
                if !self
                    .app
                    .has_resolution_work(&self.key, Some(control.sequence))
                {
                    if target.settle(false, &loaded.record.checkpoint).is_ok() {
                        self.commit_scheduled_transition(
                            &control,
                            ControlTransition::Settlement { settlement: target },
                        )
                        .await;
                    }
                    return;
                }
            }
            SettlementState::Applied { .. } | SettlementState::Uncertain { .. } => return,
            SettlementState::Pending => {}
        }
        let Ok(notifier) = self.notifier() else {
            return;
        };
        let mut notified = Box::pin(notifier.notified());
        let remaining = Self::remaining(control.deadline_millis).unwrap_or(Duration::ZERO);
        let deadline = tokio::time::sleep(remaining);
        tokio::pin!(deadline);
        let mut delay = RECONCILIATION_INITIAL_DELAY;
        loop {
            notified.as_mut().enable();
            let outcome = tokio::select! {
                result = self.recover_notification_runtime(&definition, &control) => result,
                _ = &mut notified => return,
                _ = &mut deadline => { self.expire_attempt(control.clone()).await; return; }
            };
            match outcome {
                Ok(()) => {
                    tokio::select! {
                        _ = &mut notified => {}
                        _ = &mut deadline => self.expire_attempt(control.clone()).await,
                    }
                    return;
                }
                Err(RuntimeOperationFailure::Storage(failure)) => {
                    self.retain_execution_read_failure(&failure);
                    self.schedule_after_storage_failure(&failure);
                    return;
                }
                Err(RuntimeOperationFailure::Application(_)) => {
                    tokio::select! {
                        _ = &mut notified => return,
                        _ = &mut deadline => { self.expire_attempt(control.clone()).await; return; }
                        _ = tokio::time::sleep(delay) => {}
                    }
                    let current = tokio::select! {
                        current = self.load_execution_observation(None, None) => current,
                        _ = &mut notified => return,
                        _ = &mut deadline => { self.expire_attempt(control.clone()).await; return; }
                    };
                    let current = match current {
                        Ok(Some(current)) => current,
                        Ok(None) => return,
                        Err(failure) => {
                            self.retain_execution_read_failure(&failure);
                            return;
                        }
                    };
                    if current.record.checkpoint.control(control.sequence) != Some(&control) {
                        return;
                    }
                    delay = delay.saturating_mul(2).min(RECONCILIATION_MAX_DELAY);
                }
            }
        }
    }

    async fn recover_notification_runtime(
        &self,
        definition: &CompiledDefinition,
        control: &ControlRecord,
    ) -> Result<(), RuntimeOperationFailure> {
        let notification_id = control.notification_id()?.clone();
        if let Some(interaction) = self.interaction(control.sequence)
            && interaction.notification(&notification_id).is_ok()
        {
            return Ok(());
        }
        let (workspace_id, conversation_id) = control.attempt_identity()?;
        let agent_key = control.agent_key(definition)?;
        let conversation = {
            let (_preparation, agent, _) = self.preparation_permit(control, agent_key).await?;
            let workspace = agent
                .get_workspace(workspace_id.clone())
                .await
                .map_err(AppError::from)?;
            Arc::new(
                workspace
                    .get_conversation(conversation_id.clone())
                    .await
                    .map_err(AppError::from)?,
            )
        };
        let mut receiver = conversation.notification_stream().map_err(AppError::from)?;
        // Retain the original recovered conversation and its notification
        // receiver while this required pure observation retries. The caller's
        // original attempt deadline still encloses lower setup and valid waits.
        let current = self
            .load_execution_observation(None, None)
            .await?
            .ok_or_else(|| AppError::NotFound("workflow run does not exist".to_owned()))?;
        if current.record.checkpoint.control(control.sequence) != Some(control) {
            return Err(AppError::Conflict(
                "notification recovery reservation is stale".to_owned(),
            )
            .into());
        }
        let interaction = self
            .register_interaction(Arc::new(Attempt::recovered(control.sequence, conversation)))
            .ok_or_else(|| {
                AppError::Conflict("notification recovery reservation is stale".to_owned())
            })?;
        let notification = loop {
            match receiver.recv().await {
                Some(Ok(notification))
                    if notification.id() == &notification_id
                        && matches!(
                            notification.kind(),
                            ConversationNotificationKind::Interaction { .. }
                        ) =>
                {
                    break notification;
                }
                Some(Ok(_)) => {}
                Some(Err(error)) => return Err(AppError::from(error).into()),
                None => {
                    return Err(
                        AppError::Closed("agent notification receiver closed".to_owned()).into(),
                    );
                }
            }
        };
        let current = self
            .load_preserving_observation(None)
            .await?
            .ok_or_else(|| {
                AppError::Conflict("notification recovery truth is unavailable".to_owned())
            })?;
        if current.record.checkpoint.control(control.sequence) != Some(control) {
            self.remove_interaction(control.sequence);
            return Err(AppError::Conflict(
                "notification recovery reservation is stale".to_owned(),
            )
            .into());
        }
        interaction.install_notification(Arc::new(notification))?;
        self.wake();
        Ok(())
    }

    fn stage_agent_response(
        &self,
        definition: &CompiledDefinition,
        request: &AgentRequest,
        response: PromptResponse,
    ) -> Result<StagedActivationRecord, FailureCategory> {
        let mut contributions = Vec::new();
        if let Some(target) = request.text_target() {
            definition.reducer(target).ok_or(FailureCategory::Agent)?;
            contributions.push((
                target.to_owned(),
                Value::String(response.output().to_owned()),
            ));
        }
        if let Some(target) = request.structured_target() {
            let value = response
                .structured_output()
                .cloned()
                .ok_or(FailureCategory::Agent)?;
            definition.reducer(target).ok_or(FailureCategory::Agent)?;
            contributions.push((target.to_owned(), value));
        }
        let mut result = Map::from_iter([(
            "output".to_owned(),
            Value::String(response.output().to_owned()),
        )]);
        if let Some(structured) = response.structured_output() {
            result.insert("structured_output".to_owned(), structured.clone());
        }
        if let Some(usage) = response.usage() {
            result.insert(
                "usage".to_owned(),
                Value::Object(Map::from_iter([
                    ("input_tokens".to_owned(), Value::from(usage.input_tokens())),
                    (
                        "output_tokens".to_owned(),
                        Value::from(usage.output_tokens()),
                    ),
                ])),
            );
        }
        ExecutionEngine::new(definition, self.app.typing(), self.app.conditions())
            .stage_after_control(request.node(), request.lineage(), contributions, result)
            .map_err(|_| FailureCategory::Limit)
    }

    async fn admit_descendants(
        &self,
        loaded: LoadedRun,
        node: String,
        lineage: String,
        inputs: Vec<ChildTask>,
        deadline_millis: u64,
        sequence: u64,
    ) -> ChildAdmissionReceipt {
        let control = ControlRecord {
            sequence,
            node,
            lineage,
            deadline_millis,
            state: ControlRecordState::Child {
                inputs,
                admitted: Vec::new(),
            },
        };
        let parent = match self
            .commit_without_schedule(loaded, ControlTransition::Park { control })
            .await
        {
            Ok(parent) => parent,
            Err(RuntimeOperationFailure::Storage(failure)) => {
                return ChildAdmissionReceipt::Storage(failure);
            }
            Err(RuntimeOperationFailure::Application(error)) => {
                return ChildAdmissionReceipt::Rejected(error);
            }
        };
        // The existing control turn constructs child state and polls its batch
        // on the owning worker or Sync caller after this receipt completes.
        self.schedule(ScheduleCause::CurrentState);
        ChildAdmissionReceipt::Applied {
            parent: Box::new(parent),
            children: Vec::new(),
        }
    }

    async fn observe_children(
        &self,
        loaded: LoadedRun,
        sequence: u64,
    ) -> Result<(LoadedRun, Vec<RunRecord>), RuntimeOperationFailure> {
        let Some(control) = loaded.record.checkpoint.control(sequence) else {
            return Ok((loaded, Vec::new()));
        };
        let ControlRecordState::Child { admitted, .. } = &control.state else {
            return Err(AppError::Integration("child control is unavailable".into()).into());
        };
        let definition = self.app.definition_for(&loaded.record)?;
        if !matches!(
            loaded.record.checkpoint.phase,
            CheckpointPhaseRecord::Failing { .. }
        ) && Self::now_millis()? >= control.deadline_millis
        {
            return Ok((loaded, Vec::new()));
        }
        let activation = tokio::time::Instant::now()
            .checked_add(Self::remaining(control.deadline_millis)?)
            .ok_or_else(|| AppError::Timeout("child deadline is outside the clock range".into()))?;
        let deadline = LifecycleEvent::deadline(Some(activation));
        // Keep the parent's original validated receipt. Only the admitted child
        // inventory needs observation, under one budget for this entire pass.
        let children = tokio::time::timeout_at(deadline, async {
            let mut children = Vec::with_capacity(admitted.len());
            let mut delay = RECONCILIATION_INITIAL_DELAY;
            for member in admitted {
                let key = &member.key;
                loop {
                    match Self::load_scoped_for_recovery(&self.app, key).await {
                        Ok(Some(child)) => {
                            child.record.validate_child(&loaded.record, &definition)?;
                            children.push(child.record);
                            break;
                        }
                        Ok(None) => {
                            return Err(AppError::Integration(
                                "admitted child record is missing".into(),
                            )
                            .into());
                        }
                        Err(failure)
                            if failure.is_recoverable()
                                && self.schedule_after_storage_failure(&failure) =>
                        {
                            tokio::time::sleep(delay).await;
                            delay = delay.saturating_mul(2).min(RECONCILIATION_MAX_DELAY);
                        }
                        Err(failure) => return Err(RuntimeOperationFailure::Storage(failure)),
                    }
                }
            }
            Ok::<_, RuntimeOperationFailure>(children)
        })
        .await
        .unwrap_or_else(|_| {
            Err(StorageFailure::permanent(AppError::Timeout(
                "child inventory authority read deadline expired".into(),
            ))
            .into())
        })?;
        Ok((loaded, children))
    }

    fn plan_children(
        &self,
        loaded: &LoadedRun,
        sequence: u64,
        mut children: Vec<RunRecord>,
        verified: bool,
    ) -> Result<BspDecision, AppError> {
        let Some(control) = loaded.record.checkpoint.control(sequence) else {
            return Ok(BspDecision::AwaitControls);
        };
        let (inputs, admitted_count) = match &control.state {
            ControlRecordState::Child { inputs, admitted } => (inputs, admitted.len()),
            _ => return Ok(BspDecision::AwaitControls),
        };
        let input_count = inputs.len();
        let parent_definition = self.app.definition_for(&loaded.record)?;
        let step = parent_definition
            .node(&control.node)
            .map(|node| node.step())
            .ok_or_else(|| AppError::Integration("child definition is unavailable".into()))?;
        let (_, reductions) = step
            .child_plan()
            .ok_or_else(|| AppError::Integration("child definition is unavailable".to_owned()))?;
        let pipeline = matches!(step, Step::Pipeline { .. });
        let parent_failing = matches!(
            &loaded.record.checkpoint.phase,
            CheckpointPhaseRecord::Failing { .. }
        );
        if !parent_failing && Self::now_millis()? >= control.deadline_millis {
            return Ok(BspDecision::Failed {
                category: FailureCategory::Timeout,
                message: "child workflow deadline expired".to_owned(),
                observation: None,
            });
        }
        if !parent_failing && !verified && (admitted_count > 0 || input_count == 0) {
            return Ok(BspDecision::ExecuteChildren { sequence, children });
        }
        let has_active_children = children.iter().any(|child| !child.checkpoint.is_terminal());
        if parent_failing && !has_active_children && admitted_count < inputs.len() {
            let staged = control.failed_activation(
                FailureCategory::Child,
                "child workflow did not complete".to_owned(),
            );
            return Ok(BspDecision::Stage(vec![staged]));
        }
        if pipeline
            && children.iter().any(|child| {
                child.checkpoint.is_terminal()
                    && !matches!(child.checkpoint.phase, CheckpointPhaseRecord::Succeeded)
            })
        {
            let staged = control.failed_activation(
                FailureCategory::Child,
                "child workflow did not succeed".into(),
            );
            return Ok(BspDecision::Stage(vec![staged]));
        }
        if !parent_failing && admitted_count < inputs.len() && (!pipeline || !has_active_children) {
            if pipeline {
                step.validate_mode(self.child_context(&loaded.record, sequence)?.mode)?;
            }
            // Targets were captured once before Park. The existing admitted
            // prefix advances only after its predecessor succeeds.
            let next = if pipeline {
                admitted_count + 1
            } else {
                inputs.len()
            };
            for (index, input) in inputs.iter().enumerate().take(next).skip(admitted_count) {
                let (workflow, revision) = loaded.record.child_target(sequence, index)?;
                let definition = self.app.definition_revision(&workflow, revision)?;
                let definition_evidence = definition.revision();
                let encoded_index = index.to_string();
                let entry = definition.entry();
                if control
                    .lineage
                    .len()
                    .checked_add(encoded_index.len())
                    .and_then(|bytes| bytes.checked_add(entry.len()))
                    .and_then(|bytes| bytes.checked_add(2))
                    .is_none_or(|bytes| bytes > MAX_RUNTIME_BYTES)
                {
                    return Err(AppError::Busy(
                        "child workflow lineage byte bound exhausted".to_owned(),
                    ));
                }
                let lineage = format!("{}/{encoded_index}/{entry}", control.lineage);
                let id = loaded.record.id.child(&input.task_key);
                let state = if pipeline && index > 0 {
                    let previous = children.last().ok_or_else(|| {
                        AppError::Integration("pipeline predecessor is unavailable".into())
                    })?;
                    // Inputs were captured once from the original parent;
                    // later child defaults must not overwrite this handoff.
                    let mut state = previous.checkpoint.state.clone();
                    state.extend(input.input.clone());
                    if input.initializer.is_none() {
                        definition.validate_state(&state)?;
                    }
                    crate::workflow::WorkflowState::new(state)?
                } else {
                    crate::workflow::WorkflowState::new(definition.validate_input(&input.input)?)?
                };
                if input.initializer.is_none() {
                    state.validate_typing(self.app.typing())?;
                }
                children.push(RunRecord {
                    epoch: EPOCH.to_owned(),
                    id,
                    occurrence: Uuid::new_v4().simple().to_string(),
                    workspace_id: loaded.record.workspace_id.clone(),
                    workspace_revision: loaded.record.workspace_revision,
                    workflow_id: workflow,
                    definition_revision: definition_evidence.to_owned(),
                    child_revisions: loaded.record.child_revisions.clone(),
                    agent_revisions: self.app.agent_revisions(
                        &definition,
                        &loaded.record.child_revisions,
                        &loaded.record.agent_revisions,
                    )?,
                    checkpoint: CheckpointRecord::admitted(state.values, entry.to_owned(), lineage),
                    previous: None,
                    parent: Some(ParentRecord {
                        depth: loaded
                            .record
                            .parent
                            .as_ref()
                            .map_or(0, |parent| parent.depth)
                            .checked_add(1)
                            .filter(|depth| *depth <= MAX_CHILD_DEPTH)
                            .ok_or_else(|| {
                                AppError::Busy("child workflow depth bound exhausted".into())
                            })?,
                        workflow_id: loaded.record.workflow_id.clone(),
                        run_id: loaded.record.id.clone(),
                        occurrence: loaded.record.occurrence.clone(),
                        control_sequence: sequence,
                        index,
                        base_checkpoint: loaded.record.checkpoint.base_sequence,
                    }),
                    lineage: None,
                });
            }
            return Ok(BspDecision::AdmitChildren { sequence, children });
        }

        if has_active_children || children.len() != input_count {
            if !parent_failing {
                return Ok(BspDecision::ExecuteChildren { sequence, children });
            }
            return Ok(BspDecision::AwaitControls);
        }
        let mut by_target = BTreeMap::<String, Vec<Value>>::new();
        let mut child_states = Vec::new();
        for child in children {
            if matches!(
                &child.checkpoint.phase,
                CheckpointPhaseRecord::Failed { .. } | CheckpointPhaseRecord::Cancelled { .. }
            ) {
                let staged = control.failed_activation(
                    FailureCategory::Child,
                    "child workflow did not succeed".to_owned(),
                );
                return Ok(BspDecision::Stage(vec![staged]));
            }
            for reduction in reductions {
                let value = crate::workflow::definition::JsonBoundary::at_path(
                    &child.checkpoint.state,
                    reduction.source(),
                )
                .cloned()
                .ok_or_else(|| {
                    AppError::InvalidInput("child reduction source is missing".to_owned())
                })?;
                by_target
                    .entry(reduction.target().to_owned())
                    .or_default()
                    .push(value);
            }
            if pipeline {
                child_states.push(Value::Object(child.checkpoint.state));
            }
        }
        let mut contributions = Vec::new();
        for reduction in reductions {
            if let Some(values) = by_target.remove(reduction.target()) {
                for value in values {
                    contributions.push((reduction.target().to_owned(), value));
                }
            }
        }
        let mut result = Map::new();
        if pipeline {
            let final_state = child_states
                .last()
                .and_then(Value::as_object)
                .ok_or_else(|| {
                    AppError::Integration("pipeline final state is unavailable".into())
                })?;
            if reductions.is_empty() {
                contributions.extend(
                    final_state
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone())),
                );
            }
            let final_state = final_state.clone();
            result.insert("child_states".into(), Value::Array(child_states));
            result.insert("final_state".into(), Value::Object(final_state));
        }
        let staged =
            ExecutionEngine::new(&parent_definition, self.app.typing(), self.app.conditions())
                .stage_after_control(&control.node, &control.lineage, contributions, result)
                .unwrap_or_else(|_| {
                    control.failed_activation(
                        FailureCategory::Limit,
                        "child workflow output exceeds the private bound".to_owned(),
                    )
                });
        Ok(BspDecision::Stage(vec![staged]))
    }

    fn child_context(
        &self,
        parent: &RunRecord,
        sequence: u64,
    ) -> Result<Arc<ActionContext>, AppError> {
        let control = parent
            .checkpoint
            .control(sequence)
            .ok_or_else(|| AppError::Integration("child action control is unavailable".into()))?;
        // Sibling progress may change a freshly projected ordinal; the exact
        // original observation still owns this group's context and mode.
        if let Some(context) = self.cell.action_context(
            parent.checkpoint.base_sequence,
            &control.node,
            &control.lineage,
        ) {
            return Ok(context);
        }
        let definition = self.app.definition_for(parent)?;
        parent
            .action_context(
                &definition,
                &FrontierRecord {
                    node: control.node.clone(),
                    lineage: control.lineage.clone(),
                    arrived: true,
                },
                &[],
                self.cell.execution_mode()?,
            )
            .map(Arc::new)
    }

    async fn register_children(
        &self,
        mut parent: LoadedRun,
        sequence: u64,
        mut group: Vec<RunRecord>,
        runtime_inputs: Vec<Map<String, Value>>,
    ) -> ChildAdmissionReceipt {
        let prepared = async {
            let mut control = parent
                .record
                .checkpoint
                .control(sequence)
                .cloned()
                .ok_or_else(|| AppError::Integration("child control is unavailable".into()))?;
            let ControlRecordState::Child { inputs, admitted } = &mut control.state else {
                return Err(AppError::Integration("child control is unavailable".into()).into());
            };
            if group.len() <= admitted.len() {
                return Err(AppError::Conflict("child admission has no new members".into()).into());
            }
            let definition = self.app.definition_for(&parent.record)?;
            for (index, record) in group.iter().take(admitted.len()).enumerate() {
                record.validate_child(&parent.record, &definition)?;
                if !record.checkpoint.is_terminal()
                    || record
                        .parent
                        .as_ref()
                        .is_none_or(|relation| relation.index != index)
                {
                    return Err(AppError::Conflict("child admission prefix changed".into()).into());
                }
            }
            let admitted_prefix = admitted.len();
            let read_deadline = LifecycleEvent::deadline(None).min(
                tokio::time::Instant::now()
                    .checked_add(Self::remaining(control.deadline_millis)?)
                    .ok_or_else(|| {
                        AppError::Integration("child initializer deadline is invalid".into())
                    })?,
            );
            let mut sources = Vec::with_capacity(group.len() - admitted_prefix);
            for (index, child) in group.iter_mut().skip(admitted_prefix).enumerate() {
                let task = inputs.get(admitted_prefix + index).ok_or_else(|| {
                    AppError::Integration("child task membership is unavailable".into())
                })?;
                if child.workflow_id != task.workflow || child.definition_revision != task.revision
                {
                    return Err(AppError::Integration(
                        "child target changed before admission".into(),
                    )
                    .into());
                }
                let source = if let Some(initializer) = &task.initializer {
                    let selector = initializer.selector()?;
                    let mut attempts = 0;
                    let mut delay = RECONCILIATION_INITIAL_DELAY;
                    let source = loop {
                        let failure = match tokio::time::timeout_at(
                            read_deadline,
                            self.app.storage().copy_source(
                                &child.workspace_id,
                                &child.workflow_id,
                                &selector,
                                (&initializer.checkpoint, &initializer.position),
                                |record| self.app.definition_for(record),
                            ),
                        )
                        .await
                        {
                            Ok(Ok(source)) => break source,
                            Ok(Err(RuntimeOperationFailure::Application(error))) => {
                                return Err(error.into());
                            }
                            Ok(Err(RuntimeOperationFailure::Storage(failure))) => failure,
                            Err(_) => StorageFailure::permanent(AppError::Timeout(
                                "child initializer authority read expired".into(),
                            )),
                        };
                        attempts += 1;
                        if failure.is_recoverable()
                            && attempts < MAX_RECONCILIATION_ATTEMPTS
                            && tokio::time::Instant::now() < read_deadline
                        {
                            tokio::time::sleep_until(
                                (tokio::time::Instant::now() + delay).min(read_deadline),
                            )
                            .await;
                            delay = delay.saturating_mul(2).min(RECONCILIATION_MAX_DELAY);
                            continue;
                        }
                        self.retain_execution_read_failure(&failure);
                        return Err(failure.into());
                    };
                    let target = self.app.definition_for(child)?;
                    if let Some((selected, _)) = &source.source {
                        let overrides = initializer
                            .overrides
                            .iter()
                            .map(|(key, value)| (key.clone(), value.clone()))
                            .collect();
                        child.checkpoint = selected.restored(
                            &target,
                            &initializer.checkpoint,
                            &initializer.position,
                            &overrides,
                            source.saved.as_ref().map(|record| &record.checkpoint),
                        )?;
                        child.lineage = Some(LineageRecord {
                            source: selected.id.clone(),
                            source_occurrence: selected.occurrence.clone(),
                            checkpoint: selected.checkpoint.sequence,
                            action_ordinal: match &initializer.checkpoint {
                                CheckpointSelector::Action(ordinal) => Some(*ordinal),
                                CheckpointSelector::Latest | CheckpointSelector::Sequence(_) => {
                                    selected
                                        .checkpoint
                                        .failed_observation()
                                        .or(selected.checkpoint.position.as_ref())
                                        .map(|position| position.ordinal)
                                }
                            },
                        });
                    } else if let Some(fallback) = &initializer.fallback {
                        let state = fallback
                            .as_object()
                            .ok_or_else(|| {
                                AppError::InvalidInput(
                                    "initializer fallback must be an object".into(),
                                )
                            })?
                            .clone();
                        target.validate_state(&state)?;
                        child.checkpoint.state = state;
                    }
                    target.validate_state(&child.checkpoint.state)?;
                    crate::workflow::WorkflowState::new(child.checkpoint.state.clone())?
                        .validate_typing(self.app.typing())?;
                    Some(source)
                } else {
                    None
                };
                sources.push(source);
                admitted.push(ChildMembershipRecord {
                    key: child.key(),
                    revision: task.revision.clone(),
                    history: task.history,
                });
            }
            let children = &group[admitted_prefix..];
            ControlTransition::Park { control }.apply(&mut parent.record, false)?;
            let (workspace_record, workspace_storage_revision) = self
                .app
                .storage()
                .load_workspace_for_recovery(&parent.record.workspace_id)
                .await?
                .ok_or_else(|| AppError::NotFound("child workspace does not exist".to_owned()))?;
            if workspace_record.revision != parent.record.workspace_revision {
                return Err(
                    AppError::Conflict("child workspace revision is stale".to_owned()).into(),
                );
            }
            workspace_record.public_config()?;
            if !workspace_record.has_agents(&parent.record.agent_revisions)? {
                return Err(AppError::Conflict(
                    "child workflow is not materialized in this workspace".to_owned(),
                )
                .into());
            }
            let (mut app_record, app_revision) =
                self.app.current_generation_record_for_recovery().await?;
            if app_record
                .run_count
                .checked_add(children.len())
                .is_none_or(|count| count > MAX_RUNS)
            {
                return Err(
                    AppError::Busy("application run capacity is exhausted".to_owned()).into(),
                );
            }
            app_record.run_count += children.len();
            let mutation = AppMutation::admit_children(
                app_revision,
                workspace_storage_revision,
                parent.revision,
                &app_record,
                &workspace_record,
                &parent.record,
                children.iter().zip(sources),
            )?;
            Ok::<_, RuntimeOperationFailure>((app_record, mutation, admitted_prefix))
        }
        .await;
        let (app_record, mutation, admitted_prefix) = match prepared {
            Ok(prepared) => prepared,
            Err(RuntimeOperationFailure::Storage(failure)) => {
                return ChildAdmissionReceipt::Storage(failure);
            }
            Err(RuntimeOperationFailure::Application(error)) => {
                return ChildAdmissionReceipt::Rejected(error);
            }
        };
        let Some(next_revision) = parent.revision.checked_add(1) else {
            return ChildAdmissionReceipt::Rejected(AppError::Conflict(
                "durable record revision is exhausted".into(),
            ));
        };
        let admitted_checkpoint = parent.record.checkpoint.sequence;

        let context = self.child_context(&parent.record, sequence);
        let context = match context {
            Ok(context) => context,
            Err(error) => return ChildAdmissionReceipt::Rejected(error),
        };
        if !runtime_inputs.is_empty() && runtime_inputs.len() != group.len() - admitted_prefix {
            return ChildAdmissionReceipt::Rejected(AppError::Integration(
                "child runtime input membership changed".into(),
            ));
        }
        let mut runtime_inputs = runtime_inputs.into_iter();
        let mut owners: Vec<(Self, u64)> = Vec::with_capacity(group.len() - admitted_prefix);
        for child in &group[admitted_prefix..] {
            let reserved = Self::attach(Arc::clone(&self.app), child.key()).and_then(|runtime| {
                let generation = runtime.cell.claim_execution(context.mode)?;
                if let Err(error) = runtime.cell.configure_execution(
                    generation,
                    crate::workflow::ExecutionOptions {
                        inputs: runtime_inputs.next().unwrap_or_default(),
                        ..crate::workflow::ExecutionOptions::default()
                    },
                ) {
                    runtime.cell.release_execution(generation);
                    runtime.release();
                    return Err(error);
                }
                Ok((runtime, generation))
            });
            match reserved {
                Ok(owner) => owners.push(owner),
                Err(error) => {
                    for (runtime, generation) in owners {
                        runtime.cell.release_execution(generation);
                        runtime.release();
                    }
                    return ChildAdmissionReceipt::Rejected(error);
                }
            }
        }
        // One tracked App operation owns this whole publication. Caller or E
        // observation loss does not drop it. Catching its kernel panic here
        // prevents reconciliation from forgetting reserved child generations.
        let span = AppTrace::span(|| {
            tracing::info_span!(target: "app_sdk", "app.workflow.activate",
            run_id = %self.id(), occurrence = %self.occurrence(), operation = "children", count = group.len() - admitted_prefix)
        });
        let published = span
            .observe(std::pin::pin!(
                AssertUnwindSafe(async {
                    let children = &group[admitted_prefix..];
                    let (parent, readback) = match self.app.storage().commit(mutation).await {
                        AppCommitOutcome::Applied => {
                            parent.revision = next_revision;
                            (Box::new(parent), None)
                        }
                        AppCommitOutcome::Rejected(failure) => {
                            for (runtime, generation) in &owners {
                                runtime.cell.release_execution(*generation);
                                runtime.release();
                            }
                            return ChildAdmissionReceipt::Storage(failure);
                        }
                        AppCommitOutcome::Unknown(failure) => {
                            match self
                                .reconcile_child_admission(
                                    &parent.record,
                                    sequence,
                                    &app_record,
                                    children,
                                )
                                .await
                            {
                                ChildAdmissionReceipt::NotApplied => {
                                    for (runtime, generation) in &owners {
                                        runtime.cell.release_execution(*generation);
                                        runtime.release();
                                    }
                                    return ChildAdmissionReceipt::Storage(failure);
                                }
                                ChildAdmissionReceipt::Applied {
                                    parent,
                                    children: records,
                                } => (parent, Some(records)),
                                receipt => {
                                    self.app.tasks_failed(FinalResult::Integration(
                                        "child registration admission authority is unresolved"
                                            .into(),
                                    ));
                                    return receipt;
                                }
                            }
                        }
                    };
                    self.schedule(ScheduleCause::CheckpointObserved(admitted_checkpoint));
                    self.wake();
                    AppTrace::event(|| {
                        tracing::info!(
                            target: "app_sdk", event = "app.workflow.checkpoint",
                            run_id = %parent.record.id, occurrence = %parent.record.occurrence, checkpoint = admitted_checkpoint,
                            transition = "child_admitted", children = children.len(),
                        )
                    });
                    for ((runtime, generation), record) in owners.iter().zip(children) {
                        if let Err(error) = runtime
                            .observe_registered(record, Some(&context), *generation)
                            .await
                        {
                            // No child has been handed to E. Settle every still-reserved
                            // sibling before returning the original registration error.
                            for (sibling, generation) in &owners {
                                if sibling.cell.owns_execution(*generation)
                                    && sibling
                                        .settle_child_registration(*generation)
                                        .await
                                        .is_err()
                                {
                                    self.app.tasks_failed(FinalResult::Integration(
                                        "child registration cleanup authority failed".into(),
                                    ));
                                }
                            }
                            return ChildAdmissionReceipt::Rejected(error);
                        }
                    }
                    for (runtime, generation) in &owners {
                        if let Err(error) = runtime
                            .cell
                            .handoff_child(*generation, Arc::clone(&context))
                        {
                            self.app.tasks_failed(FinalResult::Integration(
                                "child registration handoff authority failed".into(),
                            ));
                            return ChildAdmissionReceipt::Rejected(error);
                        }
                    }
                    self.schedule(ScheduleCause::CurrentState);
                    self.wake();
                    if let Some(records) = readback {
                        if records.is_empty() {
                            group.clear();
                        } else {
                            group.truncate(admitted_prefix);
                            group.extend(records);
                        }
                    }
                    ChildAdmissionReceipt::Applied {
                        parent,
                        children: group,
                    }
                })
                .catch_unwind()
            ))
            .await;
        match published {
            Ok(receipt) => receipt,
            Err(_) => {
                self.app.tasks_failed(FinalResult::Integration(
                    "child registration publication panicked".into(),
                ));
                ChildAdmissionReceipt::Rejected(AppError::Integration(
                    "child registration publication panicked".into(),
                ))
            }
        }
    }

    async fn settle_child_registration(&self, generation: u64) -> Result<(), AppError> {
        let loaded = self
            .load_execution_observation(None, None)
            .await
            .map_err(StorageFailure::into_error)?
            .ok_or_else(|| AppError::NotFound("workflow run does not exist".into()))?;
        if !loaded.record.checkpoint.is_stopping() && !loaded.record.checkpoint.is_terminal() {
            self.commit_failure(
                loaded,
                FailureCategory::Child,
                "child registration group did not complete".into(),
            )
            .await?;
        }
        self.cell.revoke_execution(generation);
        let (loaded, retained) = self.settle_execution(generation, None).await?;
        if !loaded.record.checkpoint.is_terminal() || !retained {
            return Err(AppError::Integration(
                "child registration cleanup remains unresolved".into(),
            ));
        }
        self.cell.end_publication(generation);
        self.cell.release_execution(generation);
        Ok(())
    }

    async fn finish_child_admission(
        &self,
        sequence: u64,
        receipt: ChildAdmissionReceipt,
    ) -> Result<(LoadedRun, Vec<RunRecord>), RuntimeOperationFailure> {
        match receipt {
            ChildAdmissionReceipt::Applied { parent, children } => {
                if !children.is_empty() {
                    self.schedule(ScheduleCause::CurrentState);
                }
                Ok((*parent, children))
            }
            ChildAdmissionReceipt::Rejected(error) => {
                let loaded = match self.load_execution_observation(None, None).await {
                    Ok(Some(loaded)) => loaded,
                    Err(failure) => {
                        self.retain_execution_read_failure(&failure);
                        self.schedule_after_storage_failure(&failure);
                        return Err(error.into());
                    }
                    Ok(None) => return Err(error.into()),
                };
                if loaded.record.checkpoint.is_stopping() {
                    self.schedule(ScheduleCause::CheckpointReady(
                        loaded.record.checkpoint.sequence,
                    ));
                    return Err(error.into());
                }
                let Some(control) = loaded.record.checkpoint.control(sequence).cloned() else {
                    return Err(error.into());
                };
                if !matches!(&control.state, ControlRecordState::Child { .. }) {
                    return Err(error.into());
                }
                let (category, message) = match &error {
                    AppError::Busy(_) => (
                        FailureCategory::Limit,
                        "child admission capacity is exhausted",
                    ),
                    AppError::InvalidInput(_) => (
                        FailureCategory::Evaluation,
                        "child admission input is invalid",
                    ),
                    AppError::Timeout(_) => (FailureCategory::Timeout, "child admission timed out"),
                    AppError::NotFound(_)
                    | AppError::Conflict(_)
                    | AppError::Closed(_)
                    | AppError::Integration(_) => {
                        (FailureCategory::Child, "child admission failed")
                    }
                };
                if self
                    .commit_scheduled_transition(
                        &control,
                        ControlTransition::Failed {
                            category,
                            message: message.to_owned(),
                            observation: None,
                        },
                    )
                    .await
                    .is_none()
                {
                    self.schedule(ScheduleCause::CurrentState);
                }
                Err(error.into())
            }
            ChildAdmissionReceipt::Storage(failure) => {
                self.schedule_after_storage_failure(&failure);
                Err(failure.into())
            }
            ChildAdmissionReceipt::NotApplied | ChildAdmissionReceipt::Unknown => {
                Err(AppError::Integration("child admission authority is unresolved".into()).into())
            }
        }
    }

    async fn verify_child_completions(
        &self,
        parent: &RunRecord,
        sequence: u64,
        mut completions: Vec<ChildCompletion>,
    ) -> Result<(LoadedRun, Vec<RunRecord>), RuntimeOperationFailure> {
        let control = parent
            .checkpoint
            .control(sequence)
            .ok_or_else(|| AppError::Conflict("child control is unavailable".into()))?;
        let ControlRecordState::Child { admitted, .. } = &control.state else {
            return Err(AppError::Conflict("child execution reservation changed".into()).into());
        };
        let definition = self.app.definition_for(parent)?;
        let mut expected = admitted
            .iter()
            .map(|member| &member.key)
            .collect::<BTreeSet<_>>();
        if completions.len() != expected.len() {
            return Err(
                AppError::Conflict("child executor returned incomplete membership".into()).into(),
            );
        }
        let read_deadline = LifecycleEvent::deadline(Some(
            tokio::time::Instant::now()
                .checked_add(Self::remaining(control.deadline_millis)?)
                .ok_or_else(|| {
                    AppError::Timeout("child deadline is outside the clock range".into())
                })?,
        ));
        completions.sort_unstable_by_key(ChildCompletion::ordinal);
        for completion in &completions {
            let ordinal = completion.ordinal();
            let record = completion.record();
            record.validate_child(parent, &definition)?;
            let key = record.key();
            if !record.checkpoint.is_terminal() || !expected.remove(&key) {
                return Err(AppError::Conflict(
                    "child executor returned duplicate or foreign membership".into(),
                )
                .into());
            }
            let mut attempts = 0;
            let mut delay = RECONCILIATION_INITIAL_DELAY;
            loop {
                let failure = match tokio::time::timeout_at(
                    read_deadline,
                    Self::load_scoped_for_recovery(&self.app, &record.key()),
                )
                .await
                {
                    Ok(Ok(Some(current))) => {
                        if &current.record != record {
                            return Err(AppError::Conflict(
                                "child completion no longer matches its durable record".into(),
                            )
                            .into());
                        }
                        break;
                    }
                    Ok(Ok(None)) => {
                        return Err(AppError::Conflict(
                            "child completion record is unavailable".into(),
                        )
                        .into());
                    }
                    Ok(Err(failure)) => {
                        if matches!(failure.error(), AppError::Closed(_)) {
                            return Err(failure.into());
                        }
                        attempts += 1;
                        let remaining =
                            read_deadline.saturating_duration_since(tokio::time::Instant::now());
                        if failure.is_recoverable()
                            && attempts < MAX_RECONCILIATION_ATTEMPTS
                            && !remaining.is_zero()
                        {
                            // The original completion vector remains owned by
                            // this publisher while only this read is retried.
                            tokio::time::sleep(delay.min(remaining)).await;
                            delay = delay.saturating_mul(2).min(RECONCILIATION_MAX_DELAY);
                            continue;
                        }
                        RuntimeOperationFailure::Storage(failure)
                    }
                    Err(_) => {
                        AppError::Timeout("child completion observation timed out".into()).into()
                    }
                };
                self.app.tasks_failed(FinalResult::Integration(
                    "child completion observation failed".into(),
                ));
                return Err(failure);
            }
            if ordinal >= admitted.len() {
                return Err(AppError::Conflict(
                    "child completion ordinal is outside its admitted batch".into(),
                )
                .into());
            }
        }
        // All original child proofs remain retained while the same bounded
        // parent observer obtains the final authority or stopping evidence.
        let loaded = self
            .observe_child_parent(parent, sequence, false, Some(read_deadline))
            .await?;
        let children = completions
            .into_iter()
            .map(ChildCompletion::into_record)
            .collect::<Vec<_>>();
        for record in &children {
            let child = Self::attach_record(Arc::clone(&self.app), record)?;
            child.cell.release_child(&self.key);
            // The accepted verifier retains the completed group until either
            // its native Stage or the original individual fallback owns capacity.
        }
        Ok((loaded, children))
    }

    async fn observe_child_parent(
        &self,
        parent: &RunRecord,
        sequence: u64,
        wait_for_stop: bool,
        read_deadline: Option<tokio::time::Instant>,
    ) -> Result<LoadedRun, RuntimeOperationFailure> {
        let deadline = parent
            .checkpoint
            .control(sequence)
            .ok_or_else(|| AppError::Conflict("child control is unavailable".into()))?
            .deadline_millis;
        let activation_deadline = tokio::time::Instant::now()
            .checked_add(Self::remaining(deadline)?)
            .ok_or_else(|| AppError::Timeout("child deadline is outside the clock range".into()))?;
        let mut read_deadline = LifecycleEvent::deadline(Some(
            read_deadline.map_or(activation_deadline, |deadline| {
                deadline.min(activation_deadline)
            }),
        ));
        let notifier = self.notifier()?;
        let mut attempts = 0;
        let mut delay = RECONCILIATION_INITIAL_DELAY;
        loop {
            let mut notified = Box::pin(notifier.notified());
            notified.as_mut().enable();
            // The verifier supplies its original scan budget. A watchdog
            // shares each finite read pass across retries, then waits on valid
            // authority outside that budget. E and native publication retain
            // their surrounding execution/finalization owners.
            let failure = match tokio::time::timeout_at(read_deadline, async {
                let loaded = self.load_for_recovery().await?;
                if let Some(loaded) = &loaded {
                    self.validate_parent_for_recovery(loaded).await?;
                }
                Ok::<_, StorageFailure>(loaded)
            })
            .await
            {
                Ok(Ok(Some(current))) => {
                    if current.record.checkpoint.is_stopping() || Self::now_millis()? >= deadline {
                        return Ok(current);
                    }
                    if !current.record.owns_child_activation(parent, sequence) {
                        return Err(
                            AppError::Conflict("child parent reservation changed".into()).into(),
                        );
                    }
                    if !wait_for_stop {
                        return Ok(current);
                    }
                    attempts = 0;
                    delay = RECONCILIATION_INITIAL_DELAY;
                    tokio::select! {
                        _ = &mut notified => {
                            read_deadline = LifecycleEvent::deadline(Some(activation_deadline));
                            continue;
                        },
                        // This is an actual loaded revision. The existing
                        // timeout publication checks its fence; do not start
                        // a new zero-budget read when the timer expires.
                        _ = tokio::time::sleep(Self::remaining(deadline)?) => return Ok(current),
                    }
                }
                Ok(Err(failure)) => {
                    if matches!(failure.error(), AppError::Closed(_)) {
                        return Err(failure.into());
                    }
                    attempts += 1;
                    let remaining =
                        read_deadline.saturating_duration_since(tokio::time::Instant::now());
                    if failure.is_recoverable()
                        && attempts < MAX_RECONCILIATION_ATTEMPTS
                        && !remaining.is_zero()
                    {
                        // Retry observation while the same E future remains
                        // pinned. Re-admitting E would replay caller ownership.
                        tokio::time::sleep(delay.min(remaining)).await;
                        delay = delay.saturating_mul(2).min(RECONCILIATION_MAX_DELAY);
                        continue;
                    }
                    RuntimeOperationFailure::Storage(failure)
                }
                Ok(Ok(None)) => AppError::NotFound("child parent is unavailable".into()).into(),
                Err(_) => AppError::Timeout("child parent observation timed out".into()).into(),
            };
            // There is no current parent authority to commit a run outcome.
            // Preserve its durable evidence and use the existing fatal owner
            // to revoke/join E and all accepted work without another attempt.
            self.app.tasks_failed(FinalResult::Integration(
                "child parent observation failed".into(),
            ));
            return Err(failure);
        }
    }

    pub(super) async fn observe_child_finished(
        &self,
        record: &RunRecord,
        deadline: tokio::time::Instant,
    ) -> Result<(), WorkflowFailure> {
        if record.parent.is_none() {
            return Ok(());
        }
        if !record.checkpoint.is_terminal() {
            return Ok(());
        }
        let Some(parent) = self.cell.take_child_observation() else {
            return Ok(());
        };
        let snapshot = self
            .project(record)
            .map_err(|error| WorkflowFailure::Execution(error.to_string()))?;
        LifecycleEvent::ChildFinished {
            parent: &parent,
            snapshot: &snapshot,
        }
        .deliver(self.app.hooks(), parent.mode, deadline)
        .await
    }

    async fn reconcile_child_admission(
        &self,
        parent: &RunRecord,
        sequence: u64,
        app_record: &AppRecord,
        children: &[RunRecord],
    ) -> ChildAdmissionReceipt {
        let Ok(notifier) = self.notifier() else {
            return ChildAdmissionReceipt::Unknown;
        };
        let mut notified = Box::pin(notifier.notified());
        let mut delay = RECONCILIATION_INITIAL_DELAY;
        let mut attempts = 0;
        loop {
            notified.as_mut().enable();
            let receipt = self
                .reconcile_child_admission_once(parent, sequence, app_record, children)
                .await;
            match receipt {
                ChildAdmissionReceipt::Unknown => {
                    attempts += 1;
                    if attempts >= MAX_RECONCILIATION_ATTEMPTS {
                        return ChildAdmissionReceipt::Unknown;
                    }
                }
                receipt => return receipt,
            }
            if let Err(failure) = self.app.current_generation_record_for_recovery().await {
                return ChildAdmissionReceipt::Storage(failure);
            }
            tokio::select! {
                _ = &mut notified => {
                    notified.set(notifier.notified());
                }
                _ = tokio::time::sleep(delay) => {}
            }
            delay = delay.saturating_mul(2).min(RECONCILIATION_MAX_DELAY);
        }
    }

    async fn reconcile_child_admission_once(
        &self,
        parent: &RunRecord,
        sequence: u64,
        app_record: &AppRecord,
        children: &[RunRecord],
    ) -> ChildAdmissionReceipt {
        let current_parent = match Self::load_scoped_for_recovery(&self.app, &self.key).await {
            Ok(loaded) => loaded,
            Err(failure) => return ChildAdmissionReceipt::Storage(failure),
        };
        let Some(current_parent) = current_parent else {
            return ChildAdmissionReceipt::Rejected(AppError::Integration(
                "child admission parent record is missing".to_owned(),
            ));
        };
        self.schedule(ScheduleCause::CheckpointObserved(
            current_parent.record.checkpoint.sequence,
        ));
        let parent_matches = current_parent.record.same_run(parent);
        let parent_contains_admission = match current_parent.record.contains_checkpoint(parent) {
            Ok(contains) => contains,
            Err(error) => return ChildAdmissionReceipt::Rejected(error),
        };
        let parent_owns_activation =
            match current_parent.record.owns_child_admission(parent, sequence) {
                Ok(owns) => owns,
                Err(error) => return ChildAdmissionReceipt::Rejected(error),
            };
        if parent_contains_admission && !parent_owns_activation {
            if !current_parent.record.checkpoint.is_terminal() {
                self.wake();
                self.schedule(ScheduleCause::CheckpointReady(
                    current_parent.record.checkpoint.sequence,
                ));
            }
            return ChildAdmissionReceipt::Applied {
                parent: Box::new(current_parent),
                children: Vec::new(),
            };
        }
        let (loaded_app, loaded_revision) =
            match self.app.current_generation_record_for_recovery().await {
                Ok(loaded) => loaded,
                Err(failure) => return ChildAdmissionReceipt::Storage(failure),
            };
        let runs = match self.app.storage().list_all_runs_for_recovery().await {
            Ok(runs) => runs,
            Err(failure) => return ChildAdmissionReceipt::Storage(failure),
        };
        let (confirmed_app, confirmed_revision) =
            match self.app.current_generation_record_for_recovery().await {
                Ok(loaded) => loaded,
                Err(failure) => return ChildAdmissionReceipt::Storage(failure),
            };
        if loaded_revision != confirmed_revision || loaded_app != confirmed_app {
            return ChildAdmissionReceipt::Unknown;
        }
        let inventory_consistent = confirmed_app.accounts_for_inventory(app_record, runs.len());
        if children.is_empty() {
            return ChildAdmissionReceipt::Rejected(AppError::Integration(
                "admitted child workflow identity is invalid".to_owned(),
            ));
        }
        let definition = match self.app.definition_for(parent) {
            Ok(definition) => definition,
            Err(error) => return ChildAdmissionReceipt::Rejected(error),
        };
        let mut applied = Vec::new();
        let mut confirmed = 0;
        for child in children {
            match Self::load_scoped_for_recovery(&self.app, &child.key()).await {
                Ok(Some(loaded)) => {
                    let contains = match loaded.record.contains_checkpoint(child) {
                        Ok(contains) => contains,
                        Err(error) => return ChildAdmissionReceipt::Rejected(error),
                    };
                    if !contains || loaded.record.validate_child(parent, &definition).is_err() {
                        return ChildAdmissionReceipt::Rejected(AppError::Integration(
                            "child admission produced contradictory durable evidence".to_owned(),
                        ));
                    }
                    confirmed += 1;
                    applied.push(loaded.record);
                }
                Ok(None) => {}
                Err(failure) => return ChildAdmissionReceipt::Storage(failure),
            }
        }
        if let Err(failure) = self.app.current_generation_record_for_recovery().await {
            return ChildAdmissionReceipt::Storage(failure);
        }
        if !inventory_consistent || !parent_matches {
            ChildAdmissionReceipt::Rejected(AppError::Integration(
                "child admission atomicity evidence is contradictory".to_owned(),
            ))
        } else if confirmed == children.len() {
            if parent_owns_activation {
                let workspace_applied = match self
                    .app
                    .storage()
                    .load_workspace_for_recovery(self.workspace())
                    .await
                {
                    Ok(Some((workspace, _)))
                        if matches!(&workspace.state, WorkspaceStateRecord::Active { .. }) =>
                    {
                        workspace.revision == parent.workspace_revision
                            && workspace
                                .has_agents(&parent.agent_revisions)
                                .unwrap_or(false)
                    }
                    Ok(Some((workspace, _)))
                        if matches!(&workspace.state, WorkspaceStateRecord::Deleting { .. }) =>
                    {
                        self.wake();
                        if !current_parent.record.checkpoint.is_terminal() {
                            self.schedule(ScheduleCause::CheckpointReady(
                                current_parent.record.checkpoint.sequence,
                            ));
                        }
                        return ChildAdmissionReceipt::Applied {
                            parent: Box::new(current_parent),
                            children: Vec::new(),
                        };
                    }
                    Ok(Some(_)) => {
                        return ChildAdmissionReceipt::Rejected(AppError::Integration(
                            "child admission workspace evidence is contradictory".to_owned(),
                        ));
                    }
                    Ok(None) => {
                        return ChildAdmissionReceipt::Rejected(AppError::Integration(
                            "child admission workspace record is missing".to_owned(),
                        ));
                    }
                    Err(failure) => return ChildAdmissionReceipt::Storage(failure),
                };
                if !workspace_applied {
                    return ChildAdmissionReceipt::Rejected(AppError::Integration(
                        "child admission workspace evidence is contradictory".to_owned(),
                    ));
                }
                let confirmed_parent =
                    match Self::load_scoped_for_recovery(&self.app, &self.key).await {
                        Ok(parent) => parent,
                        Err(failure) => return ChildAdmissionReceipt::Storage(failure),
                    };
                match confirmed_parent {
                    Some(current) => {
                        let owns = match current.record.owns_child_admission(parent, sequence) {
                            Ok(owns) => owns,
                            Err(error) => return ChildAdmissionReceipt::Rejected(error),
                        };
                        if owns {
                            ChildAdmissionReceipt::Applied {
                                parent: Box::new(current),
                                children: applied,
                            }
                        } else {
                            match current.record.contains_checkpoint(parent) {
                                Ok(true) => {
                                    if !current.record.checkpoint.is_terminal() {
                                        self.schedule(ScheduleCause::CheckpointReady(
                                            current.record.checkpoint.sequence,
                                        ));
                                    }
                                    ChildAdmissionReceipt::Applied {
                                        parent: Box::new(current),
                                        children: Vec::new(),
                                    }
                                }
                                Ok(false) => {
                                    ChildAdmissionReceipt::Rejected(AppError::Integration(
                                        "child admission parent reservation is stale".to_owned(),
                                    ))
                                }
                                Err(error) => ChildAdmissionReceipt::Rejected(error),
                            }
                        }
                    }
                    None => ChildAdmissionReceipt::Rejected(AppError::Integration(
                        "child admission parent record is missing".to_owned(),
                    )),
                }
            } else {
                ChildAdmissionReceipt::Rejected(AppError::Integration(
                    "child admission parent evidence is contradictory".to_owned(),
                ))
            }
        } else if confirmed == 0 {
            if parent_contains_admission {
                return ChildAdmissionReceipt::Rejected(AppError::Integration(
                    "child admission produced incomplete durable evidence".to_owned(),
                ));
            }
            ChildAdmissionReceipt::NotApplied
        } else {
            ChildAdmissionReceipt::Rejected(AppError::Integration(
                "child admission atomicity evidence is contradictory".to_owned(),
            ))
        }
    }

    async fn load_scoped_for_recovery(
        app: &Arc<AppRuntime<CS, A, X, C, T, H, E>>,
        key: &RunKey,
    ) -> Result<Option<LoadedRun>, StorageFailure> {
        app.current_generation_record_for_recovery().await?;
        let Some((record, revision)) = app.storage().load_run_for_recovery(key).await? else {
            app.current_generation_record_for_recovery().await?;
            return Ok(None);
        };
        app.current_generation_record_for_recovery().await?;
        // The fresh selected and predecessor reads each passed AppStore's
        // immutable-row checks. Recheck their context and relationship here.
        record
            .validate_recovery_context(
                app.definition_for(&record)
                    .map_err(StorageFailure::permanent)?
                    .as_ref(),
            )
            .map_err(StorageFailure::permanent)?;
        if &record.key() != key {
            return Err(StorageFailure::permanent(AppError::Integration(
                "durable run scope does not match its storage key".to_owned(),
            )));
        }
        Ok(Some(LoadedRun { record, revision }))
    }

    async fn validate_parent_for_recovery(&self, loaded: &LoadedRun) -> Result<(), StorageFailure> {
        let Some(relation) = &loaded.record.parent else {
            return Ok(());
        };
        let parent = Self::load_scoped_for_recovery(
            &self.app,
            &RunKey::new(
                loaded.record.workspace_id.clone(),
                relation.workflow_id.clone(),
                relation.run_id.clone(),
                relation.occurrence.clone(),
            ),
        )
        .await?
        .ok_or_else(|| {
            StorageFailure::permanent(AppError::Integration(
                "parent run is unavailable".to_owned(),
            ))
        })?;
        loaded
            .record
            .validate_child(
                &parent.record,
                self.app
                    .definition_for(&parent.record)
                    .map_err(StorageFailure::permanent)?
                    .as_ref(),
            )
            .map(|_| ())
            .map_err(StorageFailure::permanent)
    }

    fn schedule_parent(
        app: &Arc<AppRuntime<CS, A, X, C, T, H, E>>,
        workspace: &WorkspaceId,
        relation: &ParentRecord,
    ) {
        if let Ok(runtime) = Self::attach(
            Arc::clone(app),
            RunKey::new(
                workspace.clone(),
                relation.workflow_id.clone(),
                relation.run_id.clone(),
                relation.occurrence.clone(),
            ),
        ) {
            // The original parent owner reads its current authority. A child
            // terminal receipt need only notify that already known identity.
            runtime.wake();
            runtime.schedule(ScheduleCause::Reconcile);
        }
    }

    async fn maintain_retention(&self, loaded: Option<LoadedRun>) {
        if self.app.ensure_runtime_owned().is_err() {
            return;
        }
        // Required capacity cleanup retains one budget for every pure read.
        // Its native mutation keeps separate commit authority after admission.
        let mut checkpoint = None;
        let prepared = tokio::time::timeout_at(LifecycleEvent::deadline(None), async {
            let loaded = match loaded {
                Some(loaded) => loaded,
                None => match self.load_for_recovery().await? {
                    Some(loaded) => loaded,
                    None => return Ok(None),
                },
            };
            if !loaded.record.checkpoint.is_terminal() {
                return Ok(None);
            }
            checkpoint = Some(loaded.record.checkpoint.sequence);
            if !self.app.storage().run_is_active(&loaded.record).await? {
                return Ok(None);
            }
            let (mut app_record, app_revision) =
                self.app.current_generation_record_for_recovery().await?;
            if app_record.run_count == 0 {
                // The earlier active observation may precede another owner's
                // release. Only stable active ownership proves corruption.
                if !self.app.storage().run_is_active(&loaded.record).await? {
                    return Ok(None);
                }
                if self.app.current_generation_record_for_recovery().await?.1 != app_revision {
                    return Err(StorageFailure::permanent(AppError::Conflict(
                        "run capacity changed during observation".into(),
                    )));
                }
            }
            app_record.run_count = app_record.run_count.checked_sub(1).ok_or_else(|| {
                StorageFailure::permanent(AppError::Integration(
                    "active run capacity is inconsistent".into(),
                ))
            })?;
            AppMutation::retain_run(app_revision, loaded.revision, &app_record, &loaded.record)
                .map(Some)
                .map_err(StorageFailure::permanent)
        })
        .await;
        let retained = match prepared {
            Ok(Ok(Some(mutation))) => match self.app.storage().commit(mutation).await {
                AppCommitOutcome::Applied => Ok(()),
                AppCommitOutcome::Rejected(failure) | AppCommitOutcome::Unknown(failure) => {
                    Err(failure)
                }
            },
            Ok(Ok(None)) => {
                if let Some(checkpoint) = checkpoint {
                    self.cell.retained(checkpoint);
                }
                return;
            }
            Ok(Err(failure)) => {
                // This failure precedes native admission. Preserve the typed
                // pure-read disposition rather than retrying permanent loss.
                self.retain_execution_read_failure(&failure);
                Err(failure)
            }
            Err(_) => {
                self.app.tasks_failed(FinalResult::Integration(
                    "workflow retention authority read deadline expired".into(),
                ));
                return;
            }
        };
        match retained {
            Ok(()) => {
                if let Some(checkpoint) = checkpoint {
                    self.cell.retained(checkpoint);
                }
                self.wake();
            }
            Err(failure) if failure.is_retryable_conflict() => {
                self.schedule(ScheduleCause::StorageFailure(
                    RetryDisposition::RetainUntilProgress,
                ));
            }
            Err(failure) => {
                self.schedule_after_storage_failure(&failure);
            }
        }
    }

    fn node_deadline(
        definition: &crate::workflow::definition::CompiledDefinition,
        node: &str,
    ) -> Result<(u64, u64), AppError> {
        let started = Self::now_millis()?;
        let duration = definition
            .node(node)
            .ok_or_else(|| AppError::Integration("workflow node is missing".to_owned()))?
            .deadline_millis();
        let deadline = started.checked_add(duration).ok_or_else(|| {
            AppError::Integration("workflow deadline exceeds clock range".to_owned())
        })?;
        Ok((started, deadline))
    }

    fn remaining(deadline_millis: u64) -> Result<Duration, AppError> {
        Ok(Duration::from_millis(
            deadline_millis.saturating_sub(Self::now_millis()?),
        ))
    }

    fn now_millis() -> Result<u64, AppError> {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| AppError::Integration("system clock precedes Unix epoch".to_owned()))?;
        u64::try_from(duration.as_millis())
            .map_err(|_| AppError::Integration("system clock exceeds workflow range".to_owned()))
    }
}

impl ControlTransition {
    fn timeout(message: &str) -> Self {
        Self::Failed {
            category: FailureCategory::Timeout,
            message: message.to_owned(),
            observation: None,
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Stage { .. } => "stage",
            Self::Barrier { .. } => "barrier",
            Self::Routed { .. } => "routed",
            Self::Park { .. } => "park",
            Self::Settlement { .. } => "settlement",
            Self::Resolve { .. } => "resolve",
            Self::Retry { .. } => "retry",
            Self::RetryReady { .. } => "retry_ready",
            Self::Pause { .. } => "paused",
            Self::RetryFailed => "failed_retry",
            Self::Halt { .. } => "halted",
            Self::Cancel { .. } => "cancel_intent",
            Self::Cancelled => "cancelled",
            Self::AttemptSettled { .. } => "attempt_settled",
            Self::Succeeded => "succeeded",
            Self::Failed { .. } => "failed",
        }
    }

    fn rebase(&mut self, record: &RunRecord, owner: &ControlRecord) -> bool {
        if let Self::Settlement { settlement } = self {
            return record
                .checkpoint
                .settlement_transition_allowed(owner, settlement);
        }
        let Some(current) = record.checkpoint.control(owner.sequence) else {
            return false;
        };
        match self {
            Self::Stage { .. } => match (&current.state, &owner.state) {
                (
                    ControlRecordState::Attempt(AttemptRecord {
                        phase: AttemptPhaseRecord::Dispatched,
                        ..
                    }),
                    ControlRecordState::Attempt(_),
                ) => current.same_attempt(owner),
                (ControlRecordState::Attempt(_), _) | (_, ControlRecordState::Attempt(_)) => false,
                _ => current == owner,
            },
            Self::Failed { .. } => {
                current.same_owner(owner)
                    && (!matches!(&owner.state, ControlRecordState::Attempt(_))
                        || current.same_attempt(owner))
                    && !record.checkpoint.is_stopping()
                    && matches!(
                        &owner.state,
                        ControlRecordState::Input { .. }
                            | ControlRecordState::Inputs { .. }
                            | ControlRecordState::Sleep
                            | ControlRecordState::Attempt(_)
                            | ControlRecordState::Child { .. }
                    )
                    && std::mem::discriminant(&owner.state)
                        == std::mem::discriminant(&current.state)
            }
            Self::AttemptSettled { sequence } => {
                *sequence == owner.sequence
                    && current.same_attempt(owner)
                    && record.checkpoint.is_stopping()
            }
            Self::Park { .. } if current == owner => true,
            Self::Park { control } => {
                let mut rebased = current.clone();
                if !rebased.merge_attempt_evidence(owner, control) {
                    return false;
                }
                *control = rebased;
                true
            }
            _ => false,
        }
    }

    fn awaits_attempt_settlement(&self, record: &RunRecord, owner: &ControlRecord) -> bool {
        let Some(current) = record.checkpoint.control(owner.sequence) else {
            return false;
        };
        matches!(self, Self::Stage { .. })
            && current.same_attempt(owner)
            && matches!(
                &current.state,
                ControlRecordState::Attempt(AttemptRecord {
                    phase: AttemptPhaseRecord::Waiting { .. },
                    ..
                })
            )
    }

    fn stage(
        checkpoint: &mut CheckpointRecord,
        activations: Vec<StagedActivationRecord>,
    ) -> Result<(), AppError> {
        if checkpoint.is_terminal() {
            return Err(AppError::Conflict(
                "terminal run cannot stage workflow truth".into(),
            ));
        }
        for mut activation in activations {
            if activation.children.is_some() {
                return Err(AppError::Conflict(
                    "staged child membership must come from its admitted control".into(),
                ));
            }
            if !checkpoint.frontier.iter().any(|frontier| {
                frontier.node == activation.node && frontier.lineage == activation.lineage
            }) || checkpoint.staged.iter().any(|staged| {
                staged.node == activation.node && staged.lineage == activation.lineage
            }) {
                return Err(AppError::Conflict(
                    "workflow activation is not an unfinished frontier member".into(),
                ));
            }
            if let Some(sequence) = checkpoint
                .controls
                .values()
                .find(|control| {
                    control.node == activation.node && control.lineage == activation.lineage
                })
                .map(|control| control.sequence)
                && let Some(control) = checkpoint.controls.remove(&sequence)
                && let ControlRecordState::Child { admitted, .. } = control.state
            {
                activation.children = Some((sequence, admitted));
            }
            checkpoint.staged.push(activation);
        }
        checkpoint
            .staged
            .sort_by(|left, right| (&left.node, &left.lineage).cmp(&(&right.node, &right.lineage)));
        Ok(())
    }

    fn park(
        checkpoint: &mut CheckpointRecord,
        control: ControlRecord,
        next: u64,
    ) -> Result<(), AppError> {
        control.validate_transition(checkpoint, next)?;
        let target = match &control.state {
            ControlRecordState::Input { .. } => Some(SettlementTarget::Input),
            ControlRecordState::Inputs { .. } => Some(SettlementTarget::Inputs),
            ControlRecordState::Attempt(AttemptRecord {
                phase: AttemptPhaseRecord::Unknown { .. },
                number,
                ..
            }) => Some(SettlementTarget::Unknown {
                attempt_number: *number,
            }),
            _ => None,
        };
        if let Some(target) = target
            && checkpoint.current_settlement(&control).is_none()
        {
            let settlement = if matches!(target, SettlementTarget::Unknown { .. }) {
                let prior = checkpoint
                    .control(control.sequence)
                    .and_then(|owner| checkpoint.current_settlement(owner));
                match prior {
                    Some(prior) if !matches!(prior.state, SettlementState::Applied { .. }) => {
                        prior.replacement_unknown(checkpoint, &control)?
                    }
                    _ => InteractionSettlement::pending(checkpoint, &control, target)?,
                }
            } else {
                InteractionSettlement::pending(checkpoint, &control, target)?
            };
            checkpoint.apply_settlement(settlement, next)?;
        }
        if matches!(
            &control.state,
            ControlRecordState::Attempt(AttemptRecord {
                phase: AttemptPhaseRecord::Unknown { .. },
                ..
            })
        ) {
            checkpoint.mark_settlement_uncertain(&control);
        }
        checkpoint.controls.insert(control.sequence, control);
        Ok(())
    }

    fn settle(
        checkpoint: &mut CheckpointRecord,
        settlement: InteractionSettlement,
        next: u64,
    ) -> Result<(), AppError> {
        let current = checkpoint.control(settlement.control_sequence).cloned();
        let owns_current = current.as_ref().is_some_and(|control| {
            settlement.owns(control)
                && (checkpoint
                    .current_settlement(control)
                    .is_some_and(|target| target.same_target(&settlement))
                    || matches!(
                        (&settlement.target, &settlement.state, &control.state),
                        (
                            SettlementTarget::Notification { .. },
                            SettlementState::Pending,
                            ControlRecordState::Attempt(AttemptRecord {
                                phase: AttemptPhaseRecord::Dispatched,
                                ..
                            })
                        )
                    ))
        });
        checkpoint.apply_settlement(settlement.clone(), next)?;
        if !owns_current {
            return Ok(());
        }
        let mut control = current
            .ok_or_else(|| AppError::Conflict("settlement has no current control".into()))?;
        match (&settlement.target, &settlement.state) {
            (
                SettlementTarget::Notification { notification, .. },
                SettlementState::Pending | SettlementState::Intended { .. },
            ) => {
                control.set_attempt_phase(AttemptPhaseRecord::Waiting {
                    notification_id: notification.id().clone(),
                })?;
                checkpoint.controls.insert(control.sequence, control);
            }
            (SettlementTarget::Notification { .. }, SettlementState::Applied { .. }) => {
                control.set_attempt_phase(AttemptPhaseRecord::Dispatched)?;
                checkpoint.controls.insert(control.sequence, control);
            }
            (
                SettlementTarget::Notification { notification, .. },
                SettlementState::Uncertain { .. },
            ) => {
                control.set_attempt_phase(AttemptPhaseRecord::Unknown {
                    notification_id: Some(notification.id().clone()),
                })?;
                Self::park(checkpoint, control, next)?;
            }
            (SettlementTarget::Inputs, SettlementState::Applied { .. }) => {
                checkpoint.controls.remove(&control.sequence);
            }
            _ => {}
        }
        Ok(())
    }

    fn apply(self, record: &mut RunRecord, pending_resolutions: bool) -> Result<(), AppError> {
        let previous = record.checkpoint.clone();
        let checkpoint = &mut record.checkpoint;
        let next = previous
            .sequence
            .checked_add(1)
            .ok_or_else(|| AppError::Conflict("run checkpoint sequence is exhausted".to_owned()))?;
        match self {
            Self::Halt { node, result } => {
                checkpoint.admission = RunAdmission::Halted { node, result };
            }
            Self::Pause { discard_staged } => {
                checkpoint.admission = RunAdmission::Prepared;
                if !checkpoint.is_stopping() && !checkpoint.is_terminal() {
                    checkpoint.staged.retain(|activation| {
                        !discard_staged.iter().any(|(node, lineage)| {
                            node == &activation.node && lineage == &activation.lineage
                        })
                    });
                }
            }
            Self::RetryFailed => {
                if !previous.can_retry_failed() {
                    return Err(AppError::Conflict("failed run is not safe to retry".into()));
                }
                checkpoint.admission = RunAdmission::Prepared;
                checkpoint.phase = CheckpointPhaseRecord::Active;
                checkpoint.controls.clear();
                checkpoint.staged.clear();
            }
            Self::Stage { activations } => Self::stage(checkpoint, activations)?,
            Self::Resolve {
                settlement,
                activation,
            } => {
                let control = previous
                    .control(settlement.control_sequence)
                    .ok_or_else(|| {
                        AppError::Conflict("interaction control is no longer current".into())
                    })?;
                if previous
                    .current_settlement(control)
                    .is_none_or(|current| !current.allows(&settlement))
                    || activation.node != control.node
                    || activation.lineage != control.lineage
                    || !matches!(
                        (&settlement.target, &settlement.state),
                        (
                            SettlementTarget::Input | SettlementTarget::Unknown { .. },
                            SettlementState::Applied { .. }
                        )
                    )
                {
                    return Err(AppError::Conflict(
                        "interaction does not own this activation".into(),
                    ));
                }
                checkpoint.apply_settlement(settlement, next)?;
                Self::stage(checkpoint, vec![activation])?;
            }
            Self::Retry {
                settlement,
                control,
            } => {
                let current = previous.control(control.sequence).ok_or_else(|| {
                    AppError::Conflict("retry interaction is no longer current".into())
                })?;
                if !current.same_owner(&control)
                    || previous
                        .current_settlement(current)
                        .is_none_or(|target| !target.allows(&settlement))
                    || !matches!(
                        &settlement.state,
                        SettlementState::Applied {
                            decision: SettlementDecision::Retry,
                            ..
                        }
                    )
                    || !control.is_retry()
                    || !matches!(current.abort()?, ReconciliationRecord::Applied)
                    || previous.is_stopping()
                {
                    return Err(AppError::Conflict(
                        "interaction cannot admit this retry".into(),
                    ));
                }
                checkpoint.apply_settlement(settlement, next)?;
                Self::park(checkpoint, control, next)?;
            }
            Self::Barrier {
                state,
                frontier,
                settled_joins,
                superstep,
                outcomes,
                sources,
            } => {
                if !checkpoint.controls.is_empty() {
                    return Err(AppError::Conflict(
                        "workflow barrier has unsettled controls".into(),
                    ));
                }
                if let Some(last) = outcomes.last() {
                    checkpoint.position = Some(ActionPosition {
                        node: last.node.clone(),
                        ordinal: last.ordinal,
                    });
                    checkpoint.outcomes = outcomes;
                }
                checkpoint.state = state;
                checkpoint.frontier = frontier;
                checkpoint.settled_joins = settled_joins;
                checkpoint.staged.clear();
                checkpoint.superstep = superstep;
                checkpoint.base_sequence = next;
                checkpoint.phase = CheckpointPhaseRecord::Routing { sources };
            }
            Self::Routed { frontier } => {
                if !matches!(&previous.phase, CheckpointPhaseRecord::Routing { .. }) {
                    return Err(AppError::Conflict(
                        "committed routing is no longer current".to_owned(),
                    ));
                }
                checkpoint.frontier = frontier;
                checkpoint.phase = CheckpointPhaseRecord::Active;
            }
            Self::Settlement { settlement } => Self::settle(checkpoint, settlement, next)?,
            Self::Park { control } => Self::park(checkpoint, control, next)?,
            Self::RetryReady { sequence } => {
                let control = previous
                    .control(sequence)
                    .cloned()
                    .ok_or_else(|| AppError::Conflict("retry control is no longer current".into()))?
                    .retry_intent(next)?;
                checkpoint.controls.remove(&sequence);
                checkpoint.controls.insert(control.sequence, control);
            }
            Self::Cancel {
                cancellation_checkpoint,
            } => {
                if previous.cancellation_checkpoint().is_some()
                    || cancellation_checkpoint != previous.sequence
                {
                    return Err(AppError::Conflict(
                        "run cancellation checkpoint fence is stale".to_owned(),
                    ));
                }
                checkpoint.retain_accepted_controls()?;
                checkpoint.phase = match &previous.phase {
                    CheckpointPhaseRecord::Failing {
                        category,
                        message,
                        observation,
                        ..
                    } => CheckpointPhaseRecord::Failing {
                        category: *category,
                        message: message.clone(),
                        observation: observation.clone(),
                        cancellation_checkpoint: Some(cancellation_checkpoint),
                    },
                    _ => CheckpointPhaseRecord::Cancelling {
                        cancellation_checkpoint,
                    },
                };
            }
            Self::Cancelled => {
                if !checkpoint.controls.is_empty() || pending_resolutions {
                    return Err(AppError::Conflict(
                        "run still owns accepted controls or resolutions".into(),
                    ));
                }
                let CheckpointPhaseRecord::Cancelling {
                    cancellation_checkpoint,
                    ..
                } = &previous.phase
                else {
                    return Err(AppError::Conflict(
                        "run has no accepted cancellation intent".to_owned(),
                    ));
                };
                checkpoint.staged.clear();
                checkpoint.phase = CheckpointPhaseRecord::Cancelled {
                    cancellation_checkpoint: *cancellation_checkpoint,
                };
            }
            Self::AttemptSettled { sequence } => {
                if !previous.is_stopping()
                    || !matches!(
                        checkpoint.control(sequence).map(|control| &control.state),
                        Some(ControlRecordState::Attempt(_))
                    )
                {
                    return Err(AppError::Conflict(
                        "attempt settlement does not own a stopping run".to_owned(),
                    ));
                }
                checkpoint.controls.remove(&sequence);
            }
            Self::Succeeded => {
                if !checkpoint.staged.is_empty()
                    || !checkpoint.controls.is_empty()
                    || pending_resolutions
                {
                    return Err(AppError::Conflict(
                        "run cannot succeed with unsettled durable truth".to_owned(),
                    ));
                }
                checkpoint.phase = CheckpointPhaseRecord::Succeeded;
            }
            Self::Failed {
                category,
                message,
                observation,
            } => {
                if previous.is_terminal() {
                    return Err(AppError::Conflict(
                        "terminal run cannot publish another failure".to_owned(),
                    ));
                }
                checkpoint.retain_accepted_controls()?;
                let (category, message) = match &previous.phase {
                    CheckpointPhaseRecord::Failing {
                        category, message, ..
                    } => (*category, message.clone()),
                    _ => (category, message),
                };
                let observation = previous.failed_observation().cloned().or(observation);
                if !checkpoint.controls.is_empty()
                    || pending_resolutions
                    || (!matches!(&previous.phase, CheckpointPhaseRecord::Failing { .. })
                        && !checkpoint.staged.is_empty())
                {
                    checkpoint.phase = CheckpointPhaseRecord::Failing {
                        category,
                        message,
                        observation,
                        cancellation_checkpoint: previous.cancellation_checkpoint(),
                    };
                } else {
                    checkpoint.phase = CheckpointPhaseRecord::Failed {
                        category,
                        message,
                        observation,
                        cancellation_checkpoint: previous.cancellation_checkpoint(),
                    };
                    checkpoint.staged.clear();
                }
            }
        }
        checkpoint.prepare_settlement_pages(Some(&previous), next)?;
        record.previous = Some(previous);
        checkpoint.sequence = next;
        Ok(())
    }
}

impl AppRecord {
    fn accounts_for_inventory(&self, expected: &Self, durable_run_count: usize) -> bool {
        self.epoch == expected.epoch
            && self.generation == expected.generation
            && self.agents == expected.agents
            && self.definitions == expected.definitions
            && self.run_count == durable_run_count
    }
}

impl RunRecord {
    pub(crate) fn saved_action(&self) -> Option<&CheckpointRecord> {
        std::iter::once(&self.checkpoint)
            .chain(self.previous.iter())
            .find(|checkpoint| {
                matches!(checkpoint.phase, CheckpointPhaseRecord::Routing { .. })
                    && checkpoint.position == self.checkpoint.position
                    && checkpoint.outcomes == self.checkpoint.outcomes
            })
    }

    fn restored(
        &self,
        definition: &CompiledDefinition,
        selector: &CheckpointSelector,
        position: &ResumePosition,
        overrides: &Map<String, Value>,
        saved: Option<&CheckpointRecord>,
    ) -> Result<CheckpointRecord, AppError> {
        let source = &self.checkpoint;
        if !source.is_copy_clean() {
            return Err(AppError::Conflict(
                "restore source has unsettled execution work".into(),
            ));
        }
        let mut state = source.state.clone();
        state.extend(overrides.clone());
        definition.validate_state(&state)?;
        let mut checkpoint = if matches!(position, ResumePosition::Entrypoint) {
            CheckpointRecord::admitted(
                state,
                definition.entry().to_owned(),
                definition.entry().to_owned(),
            )
        } else {
            let mut checkpoint = source.clone();
            checkpoint.state = state;
            if source.failed_observation().is_some() {
                if !matches!(position, ResumePosition::AtSaved) {
                    return Err(AppError::InvalidInput(
                        "failed restore requires the saved position".into(),
                    ));
                }
                // A Failed observation represents the whole uncommitted wave.
                checkpoint.phase = CheckpointPhaseRecord::Active;
            } else {
                let action = saved.or_else(|| self.saved_action()).unwrap_or(source);
                if matches!(position, ResumePosition::AtSaved) {
                    // Reuse the actual action's pre-routing remainder, not its
                    // already-routed successors. Selected state stays intact.
                    checkpoint.frontier = action.frontier.clone();
                    checkpoint.settled_joins = action.settled_joins.clone();
                    checkpoint.phase = action.phase.clone();
                }
                let saved = match selector {
                    CheckpointSelector::Action(ordinal) => Some(
                        action
                            .outcomes
                            .iter()
                            .find(|outcome| outcome.ordinal == *ordinal)
                            .map(|outcome| outcome.node.as_str())
                            .ok_or_else(|| {
                                AppError::Integration("selected action outcome is missing".into())
                            })?,
                    ),
                    CheckpointSelector::Latest | CheckpointSelector::Sequence(_) => source
                        .position
                        .as_ref()
                        .map(|position| position.node.as_str()),
                };
                if let Some(node) = saved.filter(|_| action.sequence > 0) {
                    if definition.node(node).is_none() {
                        return Err(AppError::InvalidInput(
                            "saved position is absent from target workflow".into(),
                        ));
                    }
                    let activation = match &action.phase {
                        CheckpointPhaseRecord::Routing { sources } => sources
                            .iter()
                            .zip(&action.outcomes)
                            .find(|(activation, outcome)| {
                                activation.node == node
                                    && match selector {
                                        CheckpointSelector::Action(ordinal) => {
                                            outcome.ordinal == *ordinal
                                        }
                                        _ => source.position.as_ref().is_some_and(|position| {
                                            outcome.ordinal == position.ordinal
                                        }),
                                    }
                            })
                            .map(|(activation, _)| activation.clone()),
                        _ => None,
                    }
                    .or_else(|| {
                        (!matches!(position, ResumePosition::AtSaved)).then(|| FrontierRecord {
                            node: node.to_owned(),
                            arrived: true,
                            lineage: node.to_owned(),
                        })
                    })
                    .ok_or_else(|| {
                        AppError::Integration("restore saved activation evidence is missing".into())
                    })?;
                    if matches!(position, ResumePosition::AtSaved) {
                        // This exact successful join visit is being replayed;
                        // other completed joins continue to suppress their losers.
                        if definition.is_join_first(node)
                            && checkpoint.settled_joins.get(node)
                                == Some(&definition.join_visit(node, &activation.lineage))
                        {
                            checkpoint.settled_joins.remove(node);
                        }
                        if !checkpoint.frontier.iter().any(|entry| {
                            entry.node == activation.node && entry.lineage == activation.lineage
                        }) {
                            checkpoint.frontier.push(activation);
                        }
                        checkpoint.phase = CheckpointPhaseRecord::Active;
                    } else if self.workflow_id.as_str() != definition.id()
                        || self.definition_revision != definition.revision()
                    {
                        // The source proves state and saved position; routing
                        // belongs to the independently selected target graph.
                        checkpoint.frontier.clear();
                        checkpoint.settled_joins.clear();
                        checkpoint.phase = CheckpointPhaseRecord::Routing {
                            sources: vec![activation],
                        };
                    }
                }
                if checkpoint.is_stopping() || checkpoint.is_terminal() {
                    checkpoint.phase = CheckpointPhaseRecord::Active;
                }
            }
            checkpoint
        };
        checkpoint.sequence = 0;
        checkpoint.base_sequence = 0;
        checkpoint.superstep = 0;
        checkpoint.admission = RunAdmission::Prepared;
        checkpoint.staged.clear();
        checkpoint.outcomes.clear();
        checkpoint.settlements = Arc::new(Vec::new());
        checkpoint.settlement_pages.clear();
        checkpoint
            .validate_frontier(definition)
            .and_then(|()| checkpoint.validate_outcomes(definition, true))
            .and_then(|()| checkpoint.validate_routing(definition, None, true))
            .map_err(|_| {
                AppError::InvalidInput("saved position is incompatible with target workflow".into())
            })?;
        Ok(checkpoint)
    }

    pub(crate) fn key(&self) -> RunKey {
        RunKey::new(
            self.workspace_id.clone(),
            self.workflow_id.clone(),
            self.id.clone(),
            self.occurrence.clone(),
        )
    }

    fn action_context(
        &self,
        definition: &CompiledDefinition,
        activation: &FrontierRecord,
        staged_prefix: &[StagedActivationRecord],
        mode: ActionMode,
    ) -> Result<ActionContext, AppError> {
        let position = self.checkpoint.proposed_position(
            definition,
            &activation.node,
            &activation.lineage,
            staged_prefix,
        )?;
        Ok(ActionContext {
            workspace: self.workspace_id.clone(),
            workflow: self.workflow_id.clone(),
            run: self.id.clone(),
            occurrence: self.occurrence.clone(),
            checkpoint: self.checkpoint.base_sequence,
            action_ordinal: position.ordinal,
            node: activation.node.clone(),
            attempt: u64::from(
                self.checkpoint
                    .control_for(&activation.node, &activation.lineage)
                    .filter(|control| matches!(&control.state, ControlRecordState::Attempt(_)))
                    .map(ControlRecord::attempt_number)
                    .transpose()?
                    .unwrap_or(1),
            ),
            mode,
            parent: self.parent.as_ref().map(|parent| parent.run_id.clone()),
            parent_occurrence: self.parent.as_ref().map(|parent| parent.occurrence.clone()),
        })
    }

    pub(crate) fn definition_pins(&self) -> impl Iterator<Item = (&WorkflowId, &str)> {
        std::iter::once((&self.workflow_id, self.definition_revision.as_str()))
            .chain(
                self.child_revisions
                    .iter()
                    .map(|(id, revision)| (id, revision.as_str())),
            )
            .chain(
                self.checkpoint
                    .controls
                    .values()
                    .filter_map(|control| match &control.state {
                        ControlRecordState::Child { inputs, .. } => Some(inputs.as_slice()),
                        _ => None,
                    })
                    .flatten()
                    .map(|task| (&task.workflow, task.revision.as_str())),
            )
            .chain(
                self.checkpoint
                    .staged
                    .iter()
                    .filter_map(|activation| activation.children.as_ref())
                    .flat_map(|(_, members)| members.iter())
                    .map(|member| (member.key.workflow(), member.revision.as_str())),
            )
    }

    pub(crate) fn observed_predecessor(&self) -> Result<Self, AppError> {
        let previous = self.previous.as_ref().ok_or_else(|| {
            AppError::Integration("checkpoint append has no observed predecessor".into())
        })?;
        if previous.sequence.checked_add(1) != Some(self.checkpoint.sequence) {
            return Err(AppError::Integration(
                "checkpoint append predecessor is not adjacent".into(),
            ));
        }
        Ok(Self {
            epoch: self.epoch.clone(),
            id: self.id.clone(),
            occurrence: self.occurrence.clone(),
            workspace_id: self.workspace_id.clone(),
            workspace_revision: self.workspace_revision,
            workflow_id: self.workflow_id.clone(),
            definition_revision: self.definition_revision.clone(),
            child_revisions: self.child_revisions.clone(),
            agent_revisions: self.agent_revisions.clone(),
            checkpoint: previous.clone(),
            previous: None,
            parent: self.parent.clone(),
            lineage: self.lineage.clone(),
        })
    }

    pub(crate) fn same_persisted_row(&self, other: &Self) -> Result<bool, AppError> {
        RecordCodec::same_run(self, other)
    }

    pub(crate) fn checkpoint_kind(&self) -> CheckpointKind {
        if self.checkpoint.sequence == 0 {
            return CheckpointKind::Initial;
        }
        if let Some(observation) = self.checkpoint.failed_observation()
            && self
                .previous
                .as_ref()
                .and_then(CheckpointRecord::failed_observation)
                != Some(observation)
        {
            return CheckpointKind::Failed {
                node: observation.node.clone(),
                ordinal: observation.ordinal,
            };
        }
        if self.checkpoint.is_terminal() {
            return CheckpointKind::Terminal;
        }
        if self.checkpoint.base_sequence == self.checkpoint.sequence
            && !self.checkpoint.outcomes.is_empty()
        {
            return CheckpointKind::Action;
        }
        CheckpointKind::Control
    }

    fn project_snapshot(
        &self,
        definition: &crate::workflow::definition::CompiledDefinition,
    ) -> Result<RunSnapshot, AppError> {
        let mut controls = self.checkpoint.controls.values().collect::<Vec<_>>();
        controls
            .sort_by(|left, right| (&left.node, &left.lineage).cmp(&(&right.node, &right.lineage)));
        let mut interactions = Vec::new();
        if !self.checkpoint.is_terminal() {
            for control in controls {
                let interaction = match &control.state {
                    ControlRecordState::Input { message } if !self.checkpoint.is_stopping() => {
                        let target =
                            self.checkpoint.current_settlement(control).ok_or_else(|| {
                                AppError::Integration("input control has no current target".into())
                            })?;
                        let Some(Step::Input { schema, .. }) =
                            definition.node(&control.node).map(|node| node.step())
                        else {
                            return Err(AppError::Integration(
                                "input control definition is unavailable".into(),
                            ));
                        };
                        Some(Interaction::Input {
                            sequence: target.sequence,
                            generation: target.generation,
                            message: message.clone(),
                            schema: schema.clone(),
                        })
                    }
                    ControlRecordState::Inputs { required } if !self.checkpoint.is_stopping() => {
                        let target =
                            self.checkpoint.current_settlement(control).ok_or_else(|| {
                                AppError::Integration(
                                    "transient inputs have no current target".into(),
                                )
                            })?;
                        Some(Interaction::Inputs {
                            sequence: target.sequence,
                            generation: target.generation,
                            required: required.clone(),
                        })
                    }
                    ControlRecordState::Attempt(AttemptRecord {
                        phase: AttemptPhaseRecord::Waiting { .. },
                        ..
                    }) if !self.checkpoint.is_stopping() => {
                        let target =
                            self.checkpoint.current_settlement(control).ok_or_else(|| {
                                AppError::Integration(
                                    "notification control has no current target".into(),
                                )
                            })?;
                        Some(Interaction::Notification {
                            sequence: target.sequence,
                            generation: target.generation,
                            notification: Arc::new(target.target.notification()?),
                        })
                    }
                    ControlRecordState::Attempt(AttemptRecord {
                        phase: AttemptPhaseRecord::Unknown { .. },
                        abort,
                        ..
                    }) => {
                        let target =
                            self.checkpoint.current_settlement(control).ok_or_else(|| {
                                AppError::Integration(
                                    "unknown control has no current target".into(),
                                )
                            })?;
                        Some(Interaction::Unknown {
                            sequence: target.sequence,
                            generation: target.generation,
                            retry_safe: !self.checkpoint.is_stopping()
                                && matches!(abort, ReconciliationRecord::Applied),
                        })
                    }
                    _ => None,
                };
                if let Some(interaction) = interaction {
                    interactions.push(interaction);
                }
            }
        }
        Ok(RunSnapshot {
            id: self.id.clone(),
            occurrence: self.occurrence.clone(),
            checkpoint: self.checkpoint.sequence,
            status: self.checkpoint.lifecycle_status(),
            state: crate::workflow::WorkflowState {
                values: self.checkpoint.state.clone(),
            },
            interactions,
            last_action_ordinal: self
                .checkpoint
                .position
                .as_ref()
                .map(|position| position.ordinal),
            outcomes: self.checkpoint.outcomes.clone(),
        })
    }

    pub(crate) fn project_checkpoint(
        &self,
        revision: &str,
        status: RunStatus,
    ) -> Result<Checkpoint, AppError> {
        let kind = self.checkpoint_kind();
        let status = match (&kind, &self.checkpoint.phase) {
            (
                CheckpointKind::Failed { .. },
                CheckpointPhaseRecord::Failing {
                    category, message, ..
                }
                | CheckpointPhaseRecord::Failed {
                    category, message, ..
                },
            ) => RunStatus::Failed(category.project(message.clone())),
            (CheckpointKind::Failed { .. }, _) => {
                return Err(AppError::Integration(
                    "failed checkpoint observation has no recorded failure".into(),
                ));
            }
            _ => status,
        };
        Ok(Checkpoint {
            run: self.id.clone(),
            occurrence: self.occurrence.clone(),
            sequence: self.checkpoint.sequence,
            outcomes: if matches!(&kind, CheckpointKind::Action) {
                self.checkpoint.outcomes.clone()
            } else {
                Vec::new()
            },
            kind,
            workflow: self.workflow_id.clone(),
            revision: revision.to_owned(),
            action_ordinal: self
                .checkpoint
                .position
                .as_ref()
                .map(|position| position.ordinal),
            position: self
                .checkpoint
                .position
                .as_ref()
                .map(|position| position.node.clone()),
            state: WorkflowState {
                values: self.checkpoint.state.clone(),
            },
            status,
            lineage: self.lineage.as_ref().map(|lineage| RunLineage {
                source: lineage.source.clone(),
                source_occurrence: lineage.source_occurrence.clone(),
                checkpoint: lineage.checkpoint,
                action_ordinal: lineage.action_ordinal,
            }),
        })
    }

    pub(crate) fn validate_recovery(
        &self,
        definition: &CompiledDefinition,
    ) -> Result<(), AppError> {
        RunId::validate_occurrence(&self.occurrence)
            .map_err(|_| AppError::Integration("durable run occurrence is invalid".into()))?;
        self.checkpoint.validate_settlements()?;
        if let Some(previous) = &self.previous {
            previous.validate_settlements()?;
        }
        self.validate_recovery_context(definition)
    }

    fn validate_recovery_context(&self, definition: &CompiledDefinition) -> Result<(), AppError> {
        self.validate_recovery_structure(definition)?;
        self.validate_recovery_predecessor(definition)
    }

    // Only migration of a sealed deleting occurrence may lack the already
    // deleted predecessor. Its caller proves this exact first surviving
    // sequence from the retained deletion cursor before invoking this method.
    pub(crate) fn validate_retained_fragment(
        &self,
        definition: &CompiledDefinition,
        first_sequence: u64,
    ) -> Result<(), AppError> {
        if first_sequence == 0
            || self.checkpoint.sequence != first_sequence
            || self.previous.is_some()
        {
            return Err(AppError::Integration(
                "retained deletion boundary is inconsistent".into(),
            ));
        }
        RunId::validate_occurrence(&self.occurrence)
            .map_err(|_| AppError::Integration("durable run occurrence is invalid".into()))?;
        self.checkpoint.validate_settlements()?;
        self.validate_recovery_structure(definition)
    }

    fn validate_recovery_structure(&self, definition: &CompiledDefinition) -> Result<(), AppError> {
        if let Some(parent) = &self.parent {
            RunId::validate_occurrence(&parent.occurrence).map_err(|_| {
                AppError::Integration("durable parent occurrence is invalid".into())
            })?;
        }
        if let Some(lineage) = &self.lineage {
            RunId::validate_occurrence(&lineage.source_occurrence).map_err(|_| {
                AppError::Integration("durable source occurrence is invalid".into())
            })?;
        }
        if self.workflow_id.as_str() != definition.id()
            || self.definition_revision != definition.revision()
        {
            return Err(AppError::Integration(
                "durable workflow definition pin is inconsistent".into(),
            ));
        }
        if self
            .parent
            .as_ref()
            .is_some_and(|parent| parent.depth == 0 || parent.depth > MAX_CHILD_DEPTH)
        {
            return Err(AppError::Integration(
                "durable child depth is outside the supported range".into(),
            ));
        }
        if self.agent_revisions.len() > MAX_AGENTS
            || self.agent_revisions.iter().any(|(key, revision)| {
                SchemaText::node_or_state("agent key", key).is_err()
                    || SchemaText::revision(revision).is_err()
            })
            || definition
                .agent_keys()
                .iter()
                .any(|key| !self.agent_revisions.contains_key(*key))
        {
            return Err(AppError::Integration(
                "durable workflow agent revision pins are inconsistent".into(),
            ));
        }
        for checkpoint in std::iter::once(&self.checkpoint).chain(self.previous.as_ref()) {
            definition.validate_state(&checkpoint.state).map_err(|_| {
                AppError::Integration(
                    "durable workflow state violates its definition or JSON bounds".into(),
                )
            })?;
            checkpoint.validate_frontier(definition)?;
            checkpoint.validate_controls(definition)?;
            for control in checkpoint.controls.values() {
                if let ControlRecordState::Child { inputs, .. } = &control.state {
                    for (index, task) in inputs.iter().enumerate() {
                        task.validate(self, definition, &control.node, index)?;
                    }
                }
            }
        }
        if self.admitted_child_keys().any(|key| {
            key.workspace() != &self.workspace_id
                || RunId::validate_occurrence(key.occurrence()).is_err()
        }) {
            return Err(AppError::Integration(
                "admitted child workspace is inconsistent".into(),
            ));
        }
        self.checkpoint
            .validate_outcomes(definition, self.lineage.is_some())?;
        self.checkpoint
            .validate_failure_observation(definition, self.previous.as_ref())?;
        self.checkpoint.validate_routing(
            definition,
            self.previous.as_ref(),
            self.lineage.is_some(),
        )?;
        if self.checkpoint.base_sequence > self.checkpoint.sequence {
            return Err(AppError::Integration(
                "durable run retention bounds are inconsistent".to_owned(),
            ));
        }
        Ok(())
    }

    fn validate_recovery_predecessor(
        &self,
        definition: &CompiledDefinition,
    ) -> Result<(), AppError> {
        if self.previous.as_ref().is_some_and(|checkpoint| {
            checkpoint.sequence.checked_add(1) != Some(self.checkpoint.sequence)
                || checkpoint.base_sequence > checkpoint.sequence
        }) || self.previous.is_some() != (self.checkpoint.sequence > 0)
        {
            return Err(AppError::Integration(
                "durable run predecessor checkpoint is inconsistent".to_owned(),
            ));
        }
        if let Some(previous) = &self.previous {
            previous.validate_outcomes(definition, self.lineage.is_some())?;
            previous.validate_failure_observation(definition, None)?;
            previous.validate_routing(definition, None, self.lineage.is_some())?;
            self.checkpoint.validate_outcome_successor(previous)?;
            self.checkpoint.validate_cancellation_successor(previous)?;
            self.checkpoint.validate_settlement_successor(previous)?;
            self.checkpoint.validate_control_successor(previous)?;
        } else if self.checkpoint.cancellation_checkpoint().is_some() {
            return Err(AppError::Integration(
                "durable cancellation checkpoint fence has no predecessor".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn child_revision(&self, workflow: &WorkflowId) -> Result<&str, AppError> {
        self.child_revisions
            .get(workflow)
            .map(String::as_str)
            .ok_or_else(|| {
                AppError::Integration("durable child definition revision is unavailable".to_owned())
            })
    }

    pub(crate) fn agent_revision(&self, key: &str) -> Result<&str, AppError> {
        self.agent_revisions
            .get(key)
            .map(String::as_str)
            .ok_or_else(|| {
                AppError::Integration("durable agent revision pin is unavailable".into())
            })
    }

    fn child_target(&self, sequence: u64, index: usize) -> Result<(WorkflowId, &str), AppError> {
        if let Some(control) = self.checkpoint.control(sequence)
            && let ControlRecordState::Child { inputs, .. } = &control.state
            && let Some(task) = inputs.get(index)
        {
            return Ok((task.workflow.clone(), task.revision.as_str()));
        }
        self.checkpoint
            .staged
            .iter()
            .find_map(|activation| {
                activation
                    .children
                    .as_ref()
                    .filter(|(owned, _)| *owned == sequence)
                    .and_then(|(_, members)| members.get(index))
            })
            .map(|member| (member.key.workflow().clone(), member.revision.as_str()))
            .ok_or_else(|| AppError::Integration("selected child target is unavailable".into()))
    }

    fn children_to_delete(&self, definition: &CompiledDefinition) -> Vec<(RunKey, String)> {
        self.checkpoint
            .selected_staged(definition)
            .filter(|activation| matches!(activation.outcome, StagedOutcomeRecord::Complete { .. }))
            .filter_map(|activation| activation.children.as_ref())
            .flat_map(|(_, children)| children.iter())
            .filter(|member| member.history == ChildHistory::DeleteAfterReduction)
            .map(|member| (member.key.clone(), member.revision.clone()))
            .collect()
    }

    pub(crate) fn admitted_child_keys(&self) -> impl Iterator<Item = &RunKey> {
        self.checkpoint
            .controls
            .values()
            .filter_map(|control| match &control.state {
                ControlRecordState::Child { admitted, .. } => Some(admitted.as_slice()),
                _ => None,
            })
            .chain(self.checkpoint.staged.iter().filter_map(|activation| {
                activation
                    .children
                    .as_ref()
                    .map(|(_, children)| children.as_slice())
            }))
            .flatten()
            .map(|member| &member.key)
    }

    pub(crate) fn validate_admitted_children(
        &self,
        definition: &CompiledDefinition,
        inventory: &BTreeMap<RunKey, &RunRecord>,
    ) -> Result<(), AppError> {
        for key in self.admitted_child_keys() {
            let child = inventory.get(key).ok_or_else(|| {
                AppError::Integration("admitted child record is missing".to_owned())
            })?;
            child.validate_child(self, definition)?;
        }
        Ok(())
    }

    pub(crate) fn validate_child<'a>(
        &self,
        parent: &'a Self,
        definition: &CompiledDefinition,
    ) -> Result<&'a str, AppError> {
        let relation = self
            .parent
            .as_ref()
            .ok_or_else(|| AppError::Integration("child evidence unavailable".into()))?;
        let (lineage, admitted, node) =
            if let Some(control) = parent.checkpoint.control(relation.control_sequence) {
                let ControlRecordState::Child { inputs, admitted } = &control.state else {
                    return Err(AppError::Integration("child evidence unavailable".into()));
                };
                let task = inputs
                    .get(relation.index)
                    .ok_or_else(|| AppError::Integration("child task index is missing".into()))?;
                let expected = parent.id.child(&task.task_key);
                if expected != self.id {
                    return Err(AppError::Integration(
                        "admitted child identity is inconsistent".into(),
                    ));
                }
                (control.lineage.as_str(), admitted, control.node.as_str())
            } else {
                let activation = parent
                    .checkpoint
                    .staged
                    .iter()
                    .find(|activation| {
                        activation
                            .children
                            .as_ref()
                            .is_some_and(|(sequence, _)| *sequence == relation.control_sequence)
                    })
                    .ok_or_else(|| AppError::Integration("child evidence unavailable".into()))?;
                let (_, admitted) = activation
                    .children
                    .as_ref()
                    .ok_or_else(|| AppError::Integration("child evidence unavailable".into()))?;
                (
                    activation.lineage.as_str(),
                    admitted,
                    activation.node.as_str(),
                )
            };
        let step = definition
            .node(node)
            .ok_or_else(|| AppError::Integration("child declaration is unavailable".into()))?
            .step();
        if !matches!(step, Step::Map(MapDeclaration::Tasks { .. })) {
            let (selection, history) = step.child_selection(relation.index)?;
            if let ChildSelection::Literal { workflow, revision } = selection {
                let workflow = WorkflowId::from_str(workflow)?;
                let revision = revision
                    .map(Ok)
                    .unwrap_or_else(|| parent.child_revision(&workflow))?;
                if self.workflow_id != workflow || self.definition_revision != revision {
                    return Err(AppError::Integration("literal child target changed".into()));
                }
            }
            if admitted
                .get(relation.index)
                .is_none_or(|member| member.history != history)
            {
                return Err(AppError::Integration("child history policy changed".into()));
            }
        }
        let (workflow, revision) =
            parent.child_target(relation.control_sequence, relation.index)?;
        let expected = self.key();
        (self.workspace_id == parent.workspace_id
            && parent.workflow_id == relation.workflow_id
            && parent.id == relation.run_id
            && parent.occurrence == relation.occurrence
            && parent
                .parent
                .as_ref()
                .map_or(0, |parent| parent.depth)
                .checked_add(1)
                == Some(relation.depth)
            && relation.depth <= MAX_CHILD_DEPTH
            && self.workflow_id == workflow
            && self.definition_revision == revision
            && self.child_revisions == parent.child_revisions
            && self
                .agent_revisions
                .iter()
                .all(|(key, revision)| parent.agent_revisions.get(key) == Some(revision))
            && parent.checkpoint.base_sequence == relation.base_checkpoint
            && relation.index < admitted.len()
            && admitted.get(relation.index).is_some_and(|member| {
                member.key == expected && member.revision == self.definition_revision
            }))
        .then_some(lineage)
        .ok_or_else(|| AppError::Integration("active child evidence is inconsistent".into()))
    }

    fn contains_checkpoint(&self, expected: &Self) -> Result<bool, AppError> {
        if !self.same_run(expected) {
            return Ok(false);
        }
        if self.checkpoint.sequence == expected.checkpoint.sequence {
            return self.same_persisted_row(expected);
        }
        if self
            .previous
            .as_ref()
            .is_some_and(|checkpoint| checkpoint.sequence == expected.checkpoint.sequence)
        {
            return self.observed_predecessor()?.same_persisted_row(expected);
        }
        Ok(false)
    }

    pub(crate) fn same_run(&self, expected: &Self) -> bool {
        self.epoch == expected.epoch
            && self.id == expected.id
            && self.occurrence == expected.occurrence
            && self.workspace_id == expected.workspace_id
            && self.workspace_revision == expected.workspace_revision
            && self.workflow_id == expected.workflow_id
            && self.definition_revision == expected.definition_revision
            && self.child_revisions == expected.child_revisions
            && self.agent_revisions == expected.agent_revisions
            && self.parent == expected.parent
            && self.lineage == expected.lineage
    }

    fn owns_child_admission(&self, expected: &Self, sequence: u64) -> Result<bool, AppError> {
        if !self.owns_child_activation(expected, sequence) {
            return Ok(false);
        }
        let encode = |control: Option<&ControlRecord>| {
            serde_json::to_vec(&control).map_err(|_| {
                AppError::Integration("child admission evidence encoding failed".into())
            })
        };
        Ok(encode(self.checkpoint.control(sequence))?
            == encode(expected.checkpoint.control(sequence))?)
    }

    fn owns_child_activation(&self, expected: &Self, sequence: u64) -> bool {
        self.same_run(expected)
            && self.checkpoint.base_sequence == expected.checkpoint.base_sequence
            && !self.checkpoint.is_stopping()
            && self.checkpoint.control(sequence).is_some_and(|control| {
                expected.checkpoint.control(sequence) == Some(control)
                    && matches!(&control.state, ControlRecordState::Child { .. })
            })
    }
}

// Physical page references are observed persistence evidence, not another
// semantic checkpoint field. Exact physical fences are checked by AppStore.
impl PartialEq for CheckpointRecord {
    fn eq(&self, other: &Self) -> bool {
        self.sequence == other.sequence
            && self.superstep == other.superstep
            && self.base_sequence == other.base_sequence
            && self.state == other.state
            && self.frontier == other.frontier
            && self.settled_joins == other.settled_joins
            && self.staged == other.staged
            && self.controls == other.controls
            && self.phase == other.phase
            && self.admission == other.admission
            && self.position == other.position
            && self.outcomes == other.outcomes
            && self.settlements == other.settlements
    }
}

impl CheckpointRecord {
    pub(crate) fn prepare_settlement_pages(
        &mut self,
        previous: Option<&Self>,
        sequence: u64,
    ) -> Result<(), AppError> {
        let mut references = Vec::with_capacity(self.settlements.len().div_ceil(64));
        for index in 0..self.settlements.len().div_ceil(64) {
            let retained = match previous {
                Some(prior) if self.same_settlement_page(prior, index)? => {
                    prior.settlement_pages.get(index).copied()
                }
                None => self.settlement_pages.get(index).copied(),
                Some(_) => None,
            };
            references.push(retained.unwrap_or(sequence));
        }
        self.settlement_pages = references;
        Ok(())
    }

    pub(crate) fn settlement_count(&self) -> usize {
        self.settlements.len()
    }

    pub(crate) fn settlement_references(&self) -> &[u64] {
        &self.settlement_pages
    }

    pub(crate) fn settlement_bytes(&self) -> Result<usize, AppError> {
        RecordCodec::encoded_len(self.settlements.as_ref())
    }

    pub(crate) fn settlement_evidence(&self) -> Result<Vec<u8>, AppError> {
        serde_json::to_vec(self.settlements.as_ref()).map_err(|_| {
            AppError::Integration("workflow checkpoint evidence encoding failed".into())
        })
    }

    pub(crate) fn settlement_page(
        &self,
        index: usize,
    ) -> Result<&[InteractionSettlement], AppError> {
        let start = index
            .checked_mul(64)
            .ok_or_else(|| AppError::Integration("settlement page index is invalid".into()))?;
        self.settlements
            .get(start..self.settlements.len().min(start + 64))
            .ok_or_else(|| AppError::Integration("settlement page index is invalid".into()))
    }

    pub(crate) fn same_settlement_page(
        &self,
        other: &Self,
        index: usize,
    ) -> Result<bool, AppError> {
        let start = index
            .checked_mul(64)
            .ok_or_else(|| AppError::Integration("settlement page index is invalid".into()))?;
        let Some(left) = self
            .settlements
            .get(start..self.settlements.len().min(start + 64))
        else {
            return Ok(false);
        };
        let Some(right) = other
            .settlements
            .get(start..other.settlements.len().min(start + 64))
        else {
            return Ok(false);
        };
        let left = serde_json::to_vec(left).map_err(|_| {
            AppError::Integration("workflow checkpoint evidence encoding failed".into())
        })?;
        let right = serde_json::to_vec(right).map_err(|_| {
            AppError::Integration("workflow checkpoint evidence encoding failed".into())
        })?;
        Ok(left == right)
    }

    pub(crate) fn hydrate_settlements(
        &mut self,
        pages: &[Arc<Value>],
        references: Vec<u64>,
        count: usize,
    ) -> Result<(), AppError> {
        let mut entries = Vec::with_capacity(count.min(MAX_INTERACTION_SETTLEMENTS));
        for page in pages {
            let values = page.get("entries").ok_or_else(|| {
                AppError::Integration("settlement page entries are missing".into())
            })?;
            let decoded: Vec<InteractionSettlement> = Vec::deserialize(values)
                .map_err(|_| AppError::Integration("app durable record is corrupt".into()))?;
            entries.extend(decoded);
        }
        if entries.len() != count || count > MAX_INTERACTION_SETTLEMENTS {
            return Err(AppError::Integration(
                "settlement page count is inconsistent".into(),
            ));
        }
        self.settlements = Arc::new(entries);
        self.settlement_pages = references;
        Ok(())
    }

    pub(crate) fn hydrate_legacy_settlements(&mut self, value: &Value) -> Result<(), AppError> {
        self.settlements = Arc::new(
            Vec::deserialize(value)
                .map_err(|_| AppError::Integration("app durable record is corrupt".into()))?,
        );
        self.settlement_pages.clear();
        Ok(())
    }

    fn validate_controls(&self, definition: &CompiledDefinition) -> Result<(), AppError> {
        let invalid =
            || AppError::Integration("durable run control evidence is inconsistent".into());
        if self.controls.len() > MAX_FRONTIER {
            return Err(invalid());
        }
        let mut activations = BTreeSet::new();
        for (sequence, control) in &self.controls {
            if *sequence == 0
                || *sequence != control.sequence
                || *sequence > self.sequence
                || !control.matches_checkpoint(self)
                || !control.has_consistent_evidence()
                || !activations.insert((&control.node, &control.lineage))
                || !self.frontier.iter().any(|activation| {
                    activation.node == control.node
                        && activation.lineage == control.lineage
                        && self.activation_ready(definition, activation)
                })
                || self.staged.iter().any(|activation| {
                    activation.node == control.node && activation.lineage == control.lineage
                })
            {
                return Err(invalid());
            }
            let Some(node) = definition.node(&control.node) else {
                return Err(invalid());
            };
            if !matches!(
                (&control.state, node.step()),
                (ControlRecordState::Input { .. }, Step::Input { .. })
                    | (ControlRecordState::Inputs { .. }, Step::Action { .. })
                    | (ControlRecordState::Attempt(_), Step::Agent { .. })
                    | (ControlRecordState::Sleep, Step::Sleep { .. })
                    | (
                        ControlRecordState::Child { .. },
                        Step::Child { .. } | Step::Map(_) | Step::Pipeline { .. }
                    )
            ) {
                return Err(invalid());
            }
            if let ControlRecordState::Child { inputs, admitted } = &control.state
                && (inputs.len() > MAX_FRONTIER
                    || admitted.len() > inputs.len()
                    || inputs
                        .iter()
                        .any(|task| SchemaText::revision(&task.revision).is_err())
                    || admitted.iter().zip(inputs).any(|(member, task)| {
                        member.key.workflow() != &task.workflow
                            || member.revision != task.revision
                            || member.history != task.history
                    }))
            {
                return Err(invalid());
            }
        }
        Ok(())
    }

    fn validate_control_successor(&self, previous: &Self) -> Result<(), AppError> {
        let invalid =
            || AppError::Integration("durable control predecessor is inconsistent".into());
        let newly_staged = |control: &ControlRecord| {
            self.staged.iter().any(|activation| {
                activation.node == control.node
                    && activation.lineage == control.lineage
                    && !previous.staged.iter().any(|prior| {
                        prior.node == activation.node && prior.lineage == activation.lineage
                    })
            })
        };
        let mut changed = 0;
        for (sequence, before) in &previous.controls {
            match self.controls.get(sequence) {
                Some(after) if before == after => {}
                Some(after) => {
                    if !after.follows(before) { return Err(invalid()); }
                    if self.is_stopping() && !previous.is_stopping() {
                        let mut stopping = before.clone();
                        stopping.begin_abort().map_err(|_| invalid())?;
                        if &stopping != after { return Err(invalid()); }
                    } else { changed += 1; }
                }
                None if newly_staged(before) => {
                    let activation = self.staged.iter().find(|activation| {
                        activation.node == before.node && activation.lineage == before.lineage
                    }).ok_or_else(invalid)?;
                    match (&before.state, &activation.children) {
                        (ControlRecordState::Child { inputs, admitted }, Some((sequence, children)))
                            if *sequence == before.sequence && children == admitted
                                && (!matches!(activation.outcome, StagedOutcomeRecord::Complete { .. })
                                    || admitted.len() == inputs.len()) => {}
                        (ControlRecordState::Child { .. }, _) | (_, Some(_)) => return Err(invalid()),
                        (_, None) => {}
                    }
                }
                None if (self.is_stopping() || matches!(self.phase, CheckpointPhaseRecord::Failed { .. }))
                    && (matches!(before.state, ControlRecordState::Input { .. } | ControlRecordState::Inputs { .. } | ControlRecordState::Sleep)
                        || before.is_retry()) => {}
                None if previous.is_stopping() && before.abort().is_ok_and(|abort| abort == ReconciliationRecord::Applied) => { changed += 1; }
                None if matches!(before.state, ControlRecordState::Inputs { .. })
                    && self.settlements.iter().any(|target| target.owns(before)
                        && matches!(target.state, SettlementState::Applied { checkpoint, .. } if checkpoint == self.sequence)) => { changed += 1; }
                None if before.clone().retry_intent(self.sequence).is_ok_and(|intent| self.controls.get(&self.sequence) == Some(&intent)) => {}
                None => return Err(invalid()),
            }
        }
        for activation in &self.staged {
            if let Some(prior) = previous
                .staged
                .iter()
                .find(|prior| prior.node == activation.node && prior.lineage == activation.lineage)
            {
                if activation.children != prior.children {
                    return Err(invalid());
                }
            } else if let Some((sequence, _)) = &activation.children
                && !previous.controls.get(sequence).is_some_and(|control| {
                    control.node == activation.node
                        && control.lineage == activation.lineage
                        && matches!(control.state, ControlRecordState::Child { .. })
                })
            {
                return Err(invalid());
            }
        }
        let mut added = 0;
        for (sequence, after) in &self.controls {
            if !previous.controls.contains_key(sequence) {
                if *sequence != self.sequence || self.is_stopping() {
                    return Err(invalid());
                }
                after
                    .validate_transition(previous, self.sequence)
                    .or_else(|error| {
                        if previous
                            .control_for(&after.node, &after.lineage)
                            .is_some_and(|before| {
                                before
                                    .clone()
                                    .retry_intent(self.sequence)
                                    .is_ok_and(|intent| &intent == after)
                            })
                        {
                            Ok(())
                        } else {
                            Err(error)
                        }
                    })
                    .map_err(|_| invalid())?;
                added += 1;
            }
        }
        if added > 1 || changed + added > 1 {
            return Err(invalid());
        }
        Ok(())
    }

    fn validate_frontier(&self, definition: &CompiledDefinition) -> Result<(), AppError> {
        let mut frontier = BTreeSet::new();
        if self.frontier.len() > MAX_FRONTIER
            || self.frontier.iter().any(|activation| {
                definition.node(&activation.node).is_none()
                    || activation.lineage.len() > MAX_RUNTIME_BYTES
                    || !frontier.insert((activation.node.as_str(), activation.lineage.as_str()))
            })
        {
            return Err(AppError::Integration(
                "durable activation frontier is inconsistent".into(),
            ));
        }
        let mut staged = BTreeSet::new();
        let mut child_sequences = self.controls.keys().copied().collect::<BTreeSet<_>>();
        let mut child_keys = BTreeSet::new();
        for control in self.controls.values() {
            if let ControlRecordState::Child { admitted, .. } = &control.state
                && admitted
                    .iter()
                    .any(|member| !child_keys.insert(&member.key))
            {
                return Err(AppError::Integration(
                    "durable child membership is duplicated".into(),
                ));
            }
        }
        if self.staged.iter().any(|activation| {
            let identity = (activation.node.as_str(), activation.lineage.as_str());
            !frontier.contains(&identity)
                || !staged.insert(identity)
                || activation
                    .children
                    .as_ref()
                    .is_some_and(|(sequence, children)| {
                        *sequence == 0
                            || *sequence > self.sequence
                            || !child_sequences.insert(*sequence)
                            || children.len() > MAX_FRONTIER
                            || children.iter().any(|member| {
                                !child_keys.insert(&member.key)
                                    || SchemaText::revision(&member.revision).is_err()
                            })
                            || matches!(activation.outcome, StagedOutcomeRecord::Deferred)
                            || !definition.node(&activation.node).is_some_and(|node| {
                                matches!(
                                    node.step(),
                                    Step::Child { .. } | Step::Map(_) | Step::Pipeline { .. }
                                )
                            })
                    })
        }) {
            return Err(AppError::Integration(
                "durable staged activation is inconsistent".into(),
            ));
        }
        Ok(())
    }

    fn lifecycle_status(&self) -> RunStatus {
        if self.is_paused() {
            return match &self.admission {
                RunAdmission::Halted { node, result } => RunStatus::Halted {
                    node: node.clone(),
                    result: result.clone(),
                },
                RunAdmission::Prepared | RunAdmission::Background => RunStatus::Paused,
            };
        }
        match &self.phase {
            CheckpointPhaseRecord::Active
            | CheckpointPhaseRecord::Routing { .. }
            | CheckpointPhaseRecord::Failing { .. } => RunStatus::Running,
            CheckpointPhaseRecord::Cancelling { .. } => RunStatus::Cancelling,
            CheckpointPhaseRecord::Succeeded => RunStatus::Succeeded,
            CheckpointPhaseRecord::Failed {
                category, message, ..
            } => RunStatus::Failed(category.project(message.clone())),
            CheckpointPhaseRecord::Cancelled { .. } => RunStatus::Cancelled,
        }
    }

    pub(crate) fn proposed_position(
        &self,
        definition: &CompiledDefinition,
        node: &str,
        lineage: &str,
        staged_prefix: &[StagedActivationRecord],
    ) -> Result<ActionPosition, AppError> {
        let preceding = self
            .frontier
            .iter()
            .filter(|activation| {
                (activation.node.as_str(), activation.lineage.as_str()) < (node, lineage)
                    && self.activation_ready(definition, activation)
                    && !self.loses_join_first(definition, &activation.node, &activation.lineage)
                    && !self.staged.iter().chain(staged_prefix).any(|staged| {
                        staged.node == activation.node
                            && staged.lineage == activation.lineage
                            && (matches!(staged.outcome, StagedOutcomeRecord::Deferred)
                                || matches!(staged.outcome, StagedOutcomeRecord::Failed { .. })
                                    && definition.node(&staged.node).is_some_and(|node| {
                                        matches!(
                                            node.step(),
                                            Step::State { .. }
                                                | Step::JoinAll { .. }
                                                | Step::JoinFirst { .. }
                                        )
                                    }))
                    })
            })
            .count() as u64;
        Ok(ActionPosition {
            node: node.to_owned(),
            ordinal: self
                .next_action_ordinal()?
                .checked_add(preceding)
                .ok_or_else(|| AppError::Conflict("run action ordinal is exhausted".into()))?,
        })
    }

    pub(crate) fn activation_ready(
        &self,
        definition: &CompiledDefinition,
        activation: &FrontierRecord,
    ) -> bool {
        match definition.node(&activation.node).map(|node| node.step()) {
            Some(Step::JoinFirst {}) => activation.arrived,
            Some(Step::JoinAll {}) => {
                let visit = definition.join_visit(&activation.node, &activation.lineage);
                !self.frontier.iter().any(|candidate| {
                    definition.join_visit(&activation.node, &candidate.lineage) + 1 == visit
                        && definition.can_reach(&candidate.node, &activation.node)
                })
            }
            Some(_) => true,
            None => false,
        }
    }

    pub(crate) fn failed_observation(&self) -> Option<&ActionPosition> {
        match &self.phase {
            CheckpointPhaseRecord::Failing { observation, .. }
            | CheckpointPhaseRecord::Failed { observation, .. } => observation.as_ref(),
            _ => None,
        }
    }

    fn can_retry_failed(&self) -> bool {
        matches!(
            &self.phase,
            CheckpointPhaseRecord::Failed {
                observation: Some(_),
                cancellation_checkpoint: None,
                ..
            }
        ) && self.staged.is_empty()
            && !self.frontier.is_empty()
    }

    fn validate_failure_observation(
        &self,
        definition: &CompiledDefinition,
        previous: Option<&Self>,
    ) -> Result<(), AppError> {
        let invalid =
            || AppError::Integration("durable failed action observation is inconsistent".into());
        if let Some(observation) = self.failed_observation() {
            if self.sequence == 0 {
                return Err(invalid());
            }
            let activation = self.failure_activation(definition, observation)?;
            if let Some(previous) = previous
                && (previous
                    .failed_observation()
                    .is_some_and(|prior| prior != observation)
                    || previous.failure_activation(definition, observation)? != activation)
            {
                return Err(invalid());
            }
        } else if let Some(previous) = previous
            && previous.failed_observation().is_some()
            && (!previous.can_retry_failed()
                || !matches!(&self.phase, CheckpointPhaseRecord::Active)
                || self.admission != RunAdmission::Prepared
                || !self.staged.is_empty()
                || self.frontier != previous.frontier
                || self.settled_joins != previous.settled_joins)
        {
            return Err(invalid());
        }
        Ok(())
    }

    fn failure_activation(
        &self,
        definition: &CompiledDefinition,
        observation: &ActionPosition,
    ) -> Result<&FrontierRecord, AppError> {
        let invalid =
            || AppError::Integration("durable failed action observation is inconsistent".into());
        let mut selected = None;
        for activation in &self.frontier {
            if activation.node == observation.node
                && self.activation_ready(definition, activation)
                && !self.loses_join_first(definition, &activation.node, &activation.lineage)
                && !self.staged.iter().any(|staged| {
                    staged.node == activation.node
                        && staged.lineage == activation.lineage
                        && matches!(&staged.outcome, StagedOutcomeRecord::Deferred)
                })
                && self.proposed_position(definition, &activation.node, &activation.lineage, &[])?
                    == *observation
                && selected.replace(activation).is_some()
            {
                return Err(invalid());
            }
        }
        let selected = selected.ok_or_else(invalid)?;
        if self.selected_staged(definition).any(|staged| {
            matches!(&staged.outcome, StagedOutcomeRecord::Failed { .. })
                && (staged.node.as_str(), staged.lineage.as_str())
                    < (selected.node.as_str(), selected.lineage.as_str())
        }) {
            return Err(invalid());
        }
        Ok(selected)
    }

    pub(crate) fn outcomes(&self) -> &[ActionOutcome] {
        &self.outcomes
    }

    pub(crate) fn loses_join_first(
        &self,
        definition: &CompiledDefinition,
        node: &str,
        lineage: &str,
    ) -> bool {
        let settled = |node: &str, lineage: &str| {
            self.settled_joins.iter().any(|(join, ordinal)| {
                definition.is_join_first(join)
                    && definition.can_reach(node, join)
                    && definition
                        .join_visit(join, lineage)
                        .checked_add(usize::from(node != join))
                        == Some(*ordinal)
            })
        };
        let winner = |join: &str, winner_lineage: &str| {
            let visit = definition.join_visit(join, winner_lineage);
            !settled(join, winner_lineage)
                && definition
                    .join_visit(join, lineage)
                    .checked_add(1)
                    .is_some_and(|candidate| candidate == visit)
                && definition.can_reach(node, join)
        };
        settled(node, lineage)
            || self.frontier.iter().any(|activation| {
                activation.arrived
                    && definition.is_join_first(&activation.node)
                    && !self.staged.iter().any(|staged| {
                        staged.node == activation.node
                            && staged.lineage == activation.lineage
                            && matches!(&staged.outcome, StagedOutcomeRecord::Deferred)
                    })
                    && winner(&activation.node, &activation.lineage)
            })
    }

    pub(crate) fn selected_staged<'a>(
        &'a self,
        definition: &'a CompiledDefinition,
    ) -> impl Iterator<Item = &'a StagedActivationRecord> {
        self.staged.iter().filter(move |activation| {
            !matches!(&activation.outcome, StagedOutcomeRecord::Deferred)
                && !self.loses_join_first(definition, &activation.node, &activation.lineage)
        })
    }

    fn preceding_failure<'a>(
        &'a self,
        definition: &'a CompiledDefinition,
        position: &ActionPosition,
        lineage: &str,
    ) -> Result<Option<(ActionPosition, FailureCategory, &'a str)>, AppError> {
        for staged in self.selected_staged(definition) {
            if let StagedOutcomeRecord::Failed { category, message } = &staged.outcome
                && (staged.node.as_str(), staged.lineage.as_str())
                    < (position.node.as_str(), lineage)
            {
                return Ok(Some((
                    self.proposed_position(definition, &staged.node, &staged.lineage, &[])?,
                    *category,
                    message,
                )));
            }
        }
        Ok(None)
    }

    fn validate_routing(
        &self,
        definition: &CompiledDefinition,
        previous: Option<&Self>,
        restored: bool,
    ) -> Result<(), AppError> {
        let CheckpointPhaseRecord::Routing { sources } = &self.phase else {
            return Ok(());
        };
        let invalid =
            || AppError::Integration("durable committed routing is inconsistent".to_owned());
        if sources.len() > MAX_FRONTIER
            || !self.staged.is_empty()
            || self.base_sequence > self.sequence
            || sources.iter().any(|source| {
                definition.node(&source.node).is_none() || source.lineage.len() > MAX_RUNTIME_BYTES
            })
            || sources
                .windows(2)
                .any(|pair| (&pair[0].node, &pair[0].lineage) >= (&pair[1].node, &pair[1].lineage))
            || !(sources.is_empty() || restored && self.sequence == 0 && self.outcomes.is_empty())
                && (sources.len() != self.outcomes.len()
                    || sources
                        .iter()
                        .zip(&self.outcomes)
                        .any(|(source, outcome)| source.node != outcome.node))
        {
            return Err(invalid());
        }
        let Some(previous) = previous else {
            return Ok(());
        };
        if matches!(&previous.phase, CheckpointPhaseRecord::Routing { .. }) {
            // Parking advances only control sequence/disposition. The committed
            // wave and its ordered data remain the exact prior routing proof.
            if self.base_sequence != previous.base_sequence
                || self.superstep != previous.superstep
                || self.phase != previous.phase
                || self.frontier != previous.frontier
                || self.settled_joins != previous.settled_joins
                || self.position != previous.position
                || serde_json::to_vec(&(&self.state, &self.outcomes, self.settlements.as_ref()))
                    .map_err(|_| invalid())?
                    != serde_json::to_vec(&(
                        &previous.state,
                        &previous.outcomes,
                        previous.settlements.as_ref(),
                    ))
                    .map_err(|_| invalid())?
            {
                return Err(invalid());
            }
            return Ok(());
        }
        if self.base_sequence != self.sequence {
            return Err(invalid());
        }
        let mut selected = previous.selected_staged(definition);
        if sources.is_empty() {
            if self.position != previous.position || self.outcomes != previous.outcomes {
                return Err(invalid());
            }
        } else {
            if self.outcomes.first().map(|outcome| outcome.ordinal)
                != previous.next_action_ordinal().ok()
            {
                return Err(invalid());
            }
            for (source, outcome) in sources.iter().zip(&self.outcomes) {
                let Some(staged) = selected.next() else {
                    return Err(invalid());
                };
                if !previous.frontier.contains(source)
                    || staged.node != source.node
                    || staged.lineage != source.lineage
                    || !matches!(&staged.outcome,
                        StagedOutcomeRecord::Complete { result, .. } if result == &outcome.result)
                {
                    return Err(invalid());
                }
            }
        }
        if selected.next().is_some() {
            return Err(invalid());
        }
        Ok(())
    }

    fn validate_outcomes(
        &self,
        definition: &CompiledDefinition,
        restored: bool,
    ) -> Result<(), AppError> {
        let invalid =
            || AppError::Integration("durable action outcomes are inconsistent".to_owned());
        match (&self.position, self.outcomes.last()) {
            (None, None) => {}
            (Some(position), None) if restored && definition.node(&position.node).is_some() => {}
            (Some(position), Some(last))
                if position.node == last.node && position.ordinal == last.ordinal => {}
            _ => return Err(invalid()),
        }
        if self.outcomes.len() > MAX_FRONTIER
            || self.sequence == 0
                && (!self.outcomes.is_empty() || !restored && self.position.is_some())
        {
            return Err(invalid());
        }
        for outcome in &self.outcomes {
            if definition.node(&outcome.node).is_none()
                || JsonBoundary::validate_size(&outcome.result).is_err()
            {
                return Err(invalid());
            }
        }
        if self.outcomes.windows(2).any(|pair| {
            pair[0].node > pair[1].node || pair[0].ordinal.checked_add(1) != Some(pair[1].ordinal)
        }) {
            return Err(invalid());
        }
        Ok(())
    }

    fn validate_outcome_successor(&self, previous: &Self) -> Result<(), AppError> {
        let unchanged = self.position == previous.position && self.outcomes == previous.outcomes;
        let valid = if self.base_sequence == previous.base_sequence {
            unchanged
        } else if self.base_sequence == self.sequence
            && previous.superstep.checked_add(1) == Some(self.superstep)
        {
            unchanged
                || self.outcomes.first().is_some_and(|outcome| {
                    previous.next_action_ordinal().ok() == Some(outcome.ordinal)
                })
        } else {
            false
        };
        if valid {
            Ok(())
        } else {
            Err(AppError::Integration(
                "durable action outcome predecessor is inconsistent".to_owned(),
            ))
        }
    }

    fn rollback_stage_keys(&self, definition: &CompiledDefinition) -> Vec<(String, String)> {
        self.staged
            .iter()
            .filter(|activation| {
                matches!(
                    definition.node(&activation.node).map(|node| node.step()),
                    Some(
                        Step::Action { .. }
                            | Step::State { .. }
                            | Step::JoinAll { .. }
                            | Step::JoinFirst { .. }
                    )
                )
            })
            .map(|activation| (activation.node.clone(), activation.lineage.clone()))
            .collect()
    }

    fn is_paused(&self) -> bool {
        matches!(
            &self.admission,
            RunAdmission::Prepared | RunAdmission::Halted { .. }
        ) && !self.is_stopping()
            && !self.is_terminal()
            && self.controls.values().all(|control| {
                matches!(&control.state, ControlRecordState::Child { admitted, .. } if admitted.is_empty())
                    || matches!(self.admission, RunAdmission::Prepared)
                        && matches!(control.state,
                            ControlRecordState::Input { .. }
                                | ControlRecordState::Inputs { .. }
                                | ControlRecordState::Sleep)
            })
    }

    pub(crate) fn control(&self, sequence: u64) -> Option<&ControlRecord> {
        self.controls.get(&sequence)
    }

    pub(crate) fn control_for(&self, node: &str, lineage: &str) -> Option<&ControlRecord> {
        self.controls
            .values()
            .find(|control| control.node == node && control.lineage == lineage)
    }

    fn retain_accepted_controls(&mut self) -> Result<(), AppError> {
        self.controls.retain(|_, control| {
            matches!(control.state, ControlRecordState::Child { .. })
                || matches!(control.state, ControlRecordState::Attempt(_)) && !control.is_retry()
        });
        for control in self.controls.values_mut() {
            if matches!(control.state, ControlRecordState::Attempt(_)) {
                control.begin_abort()?;
            }
        }
        Ok(())
    }

    pub(crate) fn is_stopping(&self) -> bool {
        matches!(
            &self.phase,
            CheckpointPhaseRecord::Cancelling { .. } | CheckpointPhaseRecord::Failing { .. }
        )
    }

    pub(crate) fn has_unknown_effect(&self) -> bool {
        self.controls.values().any(|control| {
            matches!(
                &control.state,
                ControlRecordState::Attempt(AttemptRecord {
                    phase: AttemptPhaseRecord::Unknown { .. },
                    ..
                })
            )
        })
    }

    pub(crate) fn is_terminal(&self) -> bool {
        matches!(
            &self.phase,
            CheckpointPhaseRecord::Succeeded
                | CheckpointPhaseRecord::Failed { .. }
                | CheckpointPhaseRecord::Cancelled { .. }
        )
    }

    fn cancellation_requested(&self) -> bool {
        self.cancellation_checkpoint().is_some()
    }

    fn cancellation_checkpoint(&self) -> Option<u64> {
        match &self.phase {
            CheckpointPhaseRecord::Cancelling {
                cancellation_checkpoint,
                ..
            }
            | CheckpointPhaseRecord::Cancelled {
                cancellation_checkpoint,
                ..
            } => Some(*cancellation_checkpoint),
            CheckpointPhaseRecord::Failing {
                cancellation_checkpoint,
                ..
            }
            | CheckpointPhaseRecord::Failed {
                cancellation_checkpoint,
                ..
            } => *cancellation_checkpoint,
            CheckpointPhaseRecord::Routing { .. }
            | CheckpointPhaseRecord::Active
            | CheckpointPhaseRecord::Succeeded => None,
        }
    }

    fn validate_cancellation_successor(&self, previous: &Self) -> Result<(), AppError> {
        let current = self.cancellation_checkpoint();
        let predecessor = previous.cancellation_checkpoint();
        let monotonic = match (predecessor, current) {
            (None, None) => true,
            (None, Some(cancellation_checkpoint)) => cancellation_checkpoint == previous.sequence,
            (Some(predecessor), Some(current)) => predecessor == current,
            (Some(_), None) => false,
        };
        if !monotonic || current.is_some_and(|checkpoint| checkpoint >= self.sequence) {
            return Err(AppError::Integration(
                "durable cancellation checkpoint fence is inconsistent".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn recoverable(&self) -> bool {
        self.controls.is_empty()
            || self.frontier.iter().any(|activation| {
                self.control_for(&activation.node, &activation.lineage)
                    .is_none()
                    && !self.staged.iter().any(|staged| {
                        staged.node == activation.node && staged.lineage == activation.lineage
                    })
            })
            || self.controls.values().any(|control| match &control.state {
                ControlRecordState::Attempt(attempt) => {
                    !matches!(
                        (&self.phase, &attempt.phase),
                        (
                            CheckpointPhaseRecord::Active,
                            AttemptPhaseRecord::Unknown {
                                notification_id: None
                            }
                        )
                    ) || attempt.identity.is_some()
                        && matches!(
                            attempt.abort,
                            ReconciliationRecord::Idle | ReconciliationRecord::Intended
                        )
                }
                _ => true,
            })
    }

    fn is_public_outcome(&self) -> bool {
        self.is_terminal() || self.is_paused() || self.is_actionable()
    }

    fn is_actionable(&self) -> bool {
        self.controls.values().any(|control| match &control.state {
            ControlRecordState::Input { .. }
            | ControlRecordState::Inputs { .. }
            | ControlRecordState::Attempt(AttemptRecord {
                phase: AttemptPhaseRecord::Waiting { .. },
                ..
            }) => !self.is_stopping(),
            ControlRecordState::Attempt(AttemptRecord {
                phase: AttemptPhaseRecord::Unknown { .. },
                abort,
                ..
            }) => !self.is_stopping() || matches!(abort, ReconciliationRecord::Unknown),
            _ => false,
        })
    }

    fn kind(&self) -> &'static str {
        match &self.phase {
            CheckpointPhaseRecord::Routing { .. } => "routing",
            CheckpointPhaseRecord::Active => "running",
            CheckpointPhaseRecord::Failing { .. } => "failing",
            CheckpointPhaseRecord::Cancelling { .. } => "cancelling",
            CheckpointPhaseRecord::Succeeded => "succeeded",
            CheckpointPhaseRecord::Failed { .. } => "failed",
            CheckpointPhaseRecord::Cancelled { .. } => "cancelled",
        }
    }

    pub(crate) fn is_copy_clean(&self) -> bool {
        self.controls.is_empty()
            && (self.staged.is_empty() || self.failed_observation().is_some())
            && !matches!(&self.phase, CheckpointPhaseRecord::Cancelling { .. })
    }

    fn admitted(state: Map<String, Value>, node: String, lineage: String) -> Self {
        Self {
            sequence: 0,
            superstep: 0,
            base_sequence: 0,
            state,
            frontier: vec![FrontierRecord {
                node,
                arrived: false,
                lineage,
            }],
            settled_joins: BTreeMap::new(),
            staged: Vec::new(),
            controls: BTreeMap::new(),
            phase: CheckpointPhaseRecord::Active,
            admission: RunAdmission::Background,
            position: None,
            outcomes: Vec::new(),
            settlements: Arc::new(Vec::new()),
            settlement_pages: Vec::new(),
        }
    }

    fn next_control(&self) -> Result<u64, AppError> {
        self.sequence
            .checked_add(1)
            .ok_or_else(|| AppError::Conflict("run checkpoint sequence is exhausted".to_owned()))
    }

    pub(crate) fn committed_ordinal(&self) -> Option<u64> {
        self.position.as_ref().map(|position| position.ordinal)
    }

    pub(crate) fn next_action_ordinal(&self) -> Result<u64, AppError> {
        self.position.as_ref().map_or(Ok(0), |position| {
            position
                .ordinal
                .checked_add(1)
                .ok_or_else(|| AppError::Conflict("run action ordinal is exhausted".to_owned()))
        })
    }
}

impl ControlRecord {
    fn needs_supervision(&self, stopping: bool) -> bool {
        match &self.state {
            ControlRecordState::Child { .. } => false,
            ControlRecordState::Input { .. }
            | ControlRecordState::Inputs { .. }
            | ControlRecordState::Sleep => !stopping,
            ControlRecordState::Attempt(attempt) => {
                if stopping {
                    !matches!(attempt.abort, ReconciliationRecord::Unknown)
                } else if matches!(attempt.phase, AttemptPhaseRecord::Unknown { .. }) {
                    attempt.identity.is_some()
                        && matches!(
                            attempt.abort,
                            ReconciliationRecord::Idle | ReconciliationRecord::Intended
                        )
                } else {
                    true
                }
            }
        }
    }

    fn failed_activation(
        &self,
        category: FailureCategory,
        message: String,
    ) -> StagedActivationRecord {
        StagedActivationRecord::failed(self.node.clone(), self.lineage.clone(), category, message)
    }

    fn has_consistent_evidence(&self) -> bool {
        match &self.state {
            ControlRecordState::Attempt(AttemptRecord {
                number,
                identity,
                phase,
                abort,
                ..
            }) => {
                *number > 0
                    && match phase {
                        AttemptPhaseRecord::Intent => true,
                        AttemptPhaseRecord::Waiting { .. } | AttemptPhaseRecord::Dispatched => {
                            identity.is_some()
                        }
                        AttemptPhaseRecord::Unknown { notification_id } => {
                            notification_id.is_none() || identity.is_some()
                        }
                        AttemptPhaseRecord::Retry { wake_millis } => {
                            *wake_millis <= self.deadline_millis
                        }
                    }
                    && (!matches!(
                        abort,
                        ReconciliationRecord::Applied | ReconciliationRecord::Unknown
                    ) || matches!(
                        phase,
                        AttemptPhaseRecord::Unknown { .. } | AttemptPhaseRecord::Retry { .. }
                    ))
            }
            ControlRecordState::Inputs { required } => {
                !required.is_empty()
                    && required.iter().all(|key| !key.is_empty())
                    && required.windows(2).all(|pair| pair[0] < pair[1])
            }
            ControlRecordState::Input { .. }
            | ControlRecordState::Sleep
            | ControlRecordState::Child { .. } => true,
        }
    }

    fn follows(&self, before: &Self) -> bool {
        if !self.same_owner(before)
            || self.deadline_millis != before.deadline_millis
            || !self.has_consistent_evidence()
        {
            return false;
        }
        match (&before.state, &self.state) {
            (
                ControlRecordState::Child {
                    inputs: prior,
                    admitted: old,
                },
                ControlRecordState::Child { inputs, admitted },
            ) => inputs == prior && admitted.starts_with(old) && admitted.len() <= inputs.len(),
            (ControlRecordState::Attempt(prior), ControlRecordState::Attempt(after)) => {
                let identity = after.identity == prior.identity
                    || prior.identity.is_none()
                        && after.identity.is_some()
                        && matches!(
                            prior.phase,
                            AttemptPhaseRecord::Intent | AttemptPhaseRecord::Unknown { .. }
                        );
                let abort = after.abort == prior.abort
                    || matches!(
                        (prior.abort, after.abort),
                        (ReconciliationRecord::Idle, ReconciliationRecord::Intended)
                            | (
                                ReconciliationRecord::Intended,
                                ReconciliationRecord::Applied | ReconciliationRecord::Unknown
                            )
                    );
                let phase = if matches!(after.phase, AttemptPhaseRecord::Retry { .. })
                    && !matches!(prior.phase, AttemptPhaseRecord::Retry { .. })
                {
                    prior.number.checked_add(1) == Some(after.number)
                        && (matches!(prior.phase, AttemptPhaseRecord::Intent)
                            || matches!(prior.phase, AttemptPhaseRecord::Dispatched)
                                && prior.abort == ReconciliationRecord::Idle
                            || matches!(prior.phase, AttemptPhaseRecord::Unknown { .. })
                                && prior.abort == ReconciliationRecord::Applied)
                } else {
                    after.number == prior.number
                        && (after.phase == prior.phase
                            || matches!(
                                (&prior.phase, &after.phase),
                                (
                                    AttemptPhaseRecord::Intent,
                                    AttemptPhaseRecord::Dispatched
                                        | AttemptPhaseRecord::Unknown { .. }
                                ) | (
                                    AttemptPhaseRecord::Dispatched,
                                    AttemptPhaseRecord::Waiting { .. }
                                        | AttemptPhaseRecord::Unknown { .. }
                                ) | (
                                    AttemptPhaseRecord::Waiting { .. },
                                    AttemptPhaseRecord::Dispatched
                                        | AttemptPhaseRecord::Unknown { .. }
                                )
                            ))
                };
                identity && abort && phase
            }
            _ => self.state == before.state,
        }
    }

    fn rebase_admission(
        &mut self,
        source: &RunRecord,
        current: &RunRecord,
    ) -> Result<bool, AppError> {
        let before = &source.checkpoint;
        let after = &current.checkpoint;
        if !current.same_run(source)
            || before.base_sequence != after.base_sequence
            || before.state != after.state
            || before.frontier != after.frontier
            || before.admission != after.admission
            || after.sequence <= before.sequence
            || !matches!(after.phase, CheckpointPhaseRecord::Active)
            || before.control_for(&self.node, &self.lineage).is_some()
            || after.control_for(&self.node, &self.lineage).is_some()
            || after.staged.iter().any(|activation| {
                activation.node == self.node && activation.lineage == self.lineage
            })
            || !after.frontier.iter().any(|activation| {
                activation.node == self.node && activation.lineage == self.lineage
            })
        {
            return Ok(false);
        }
        self.sequence = after.next_control()?;
        Ok(true)
    }

    fn validate_transition(
        &self,
        checkpoint: &CheckpointRecord,
        next_sequence: u64,
    ) -> Result<(), AppError> {
        if checkpoint.is_terminal()
            || !checkpoint.frontier.iter().any(|activation| {
                activation.node == self.node && activation.lineage == self.lineage
            })
        {
            return Err(AppError::Conflict(
                "control activation is no longer current".to_owned(),
            ));
        }
        match checkpoint.control_for(&self.node, &self.lineage) {
            Some(current) if !self.same_owner(current) => {
                return Err(AppError::Busy(
                    "another control reservation owns the activation".to_owned(),
                ));
            }
            Some(current) if !self.follows(current) => {
                return Err(AppError::Conflict(
                    "control evidence cannot move backwards".into(),
                ));
            }
            Some(_) => {}
            None => {
                if self.sequence != next_sequence || checkpoint.is_stopping() {
                    return Err(AppError::Conflict(
                        "new control reservation is stale".to_owned(),
                    ));
                }
            }
        }
        if !self.matches_checkpoint(checkpoint) {
            return Err(AppError::Conflict(
                "control status does not match durable evidence".to_owned(),
            ));
        }
        Ok(())
    }

    fn matches_checkpoint(&self, checkpoint: &CheckpointRecord) -> bool {
        match &checkpoint.phase {
            CheckpointPhaseRecord::Active => true,
            CheckpointPhaseRecord::Failing { .. } | CheckpointPhaseRecord::Cancelling { .. } => {
                !matches!(
                    &self.state,
                    ControlRecordState::Attempt(AttemptRecord {
                        abort: ReconciliationRecord::Idle,
                        ..
                    })
                )
            }
            CheckpointPhaseRecord::Routing { .. }
            | CheckpointPhaseRecord::Succeeded
            | CheckpointPhaseRecord::Failed { .. }
            | CheckpointPhaseRecord::Cancelled { .. } => false,
        }
    }

    fn same_owner(&self, other: &Self) -> bool {
        self.sequence == other.sequence && self.node == other.node && self.lineage == other.lineage
    }

    fn same_attempt(&self, other: &Self) -> bool {
        if !self.same_owner(other) {
            return false;
        }
        match (&self.state, &other.state) {
            (
                ControlRecordState::Attempt(AttemptRecord {
                    number, identity, ..
                }),
                ControlRecordState::Attempt(AttemptRecord {
                    number: other_number,
                    identity: other_identity,
                    ..
                }),
            ) => number == other_number && identity == other_identity,
            _ => false,
        }
    }

    fn merge_attempt_evidence(&mut self, owner: &Self, target: &Self) -> bool {
        if self.same_owner(owner)
            && target.same_owner(owner)
            && let (
                ControlRecordState::Attempt(current),
                ControlRecordState::Attempt(owner),
                ControlRecordState::Attempt(target),
            ) = (&mut self.state, &owner.state, &target.state)
            && current.number == owner.number
            && target.number == owner.number
            && current.identity.is_none()
            && owner.identity.is_none()
            && target.identity.is_some()
            && matches!(&owner.phase, AttemptPhaseRecord::Intent)
            && matches!(&target.phase, AttemptPhaseRecord::Dispatched)
            && matches!(&owner.abort, ReconciliationRecord::Idle)
            && matches!(&target.abort, ReconciliationRecord::Idle)
        {
            current.identity.clone_from(&target.identity);
            return self.has_consistent_evidence();
        }
        if !self.same_attempt(owner) || !target.same_attempt(owner) {
            return false;
        }
        let ControlRecordState::Attempt(AttemptRecord {
            abort: owner_abort, ..
        }) = &owner.state
        else {
            return false;
        };
        let ControlRecordState::Attempt(AttemptRecord {
            phase: target_phase,
            abort: target_abort,
            ..
        }) = &target.state
        else {
            return false;
        };
        if target_abort != owner_abort {
            if !matches!(
                *target_abort,
                ReconciliationRecord::Applied | ReconciliationRecord::Unknown
            ) {
                return false;
            }
            let settled = match self.abort() {
                Ok(ReconciliationRecord::Intended) => self
                    .settle_abort(matches!(*target_abort, ReconciliationRecord::Applied))
                    .is_ok(),
                Ok(observed) => observed == *target_abort,
                Err(_) => false,
            };
            if settled {
                let ControlRecordState::Attempt(AttemptRecord { phase, .. }) = &mut self.state
                else {
                    return false;
                };
                *phase = target_phase.clone();
            }
            return settled && self.has_consistent_evidence();
        }
        let ControlRecordState::Attempt(AttemptRecord { phase, abort, .. }) = &mut self.state
        else {
            return false;
        };
        if matches!(target_phase, AttemptPhaseRecord::Dispatched) && *abort != *owner_abort {
            return false;
        }
        if matches!(
            abort,
            ReconciliationRecord::Idle | ReconciliationRecord::Intended
        ) {
            *phase = target_phase.clone();
        }
        self.has_consistent_evidence()
    }

    fn attempt(&self) -> Result<&AttemptRecord, AppError> {
        let ControlRecordState::Attempt(attempt) = &self.state else {
            return Err(AppError::Conflict(
                "control does not own an agent attempt".to_owned(),
            ));
        };
        Ok(attempt)
    }

    fn agent_key<'a>(&self, definition: &'a CompiledDefinition) -> Result<&'a str, AppError> {
        match definition.node(&self.node).map(|node| node.step()) {
            Some(Step::Agent { agent, .. }) => Ok(agent),
            _ => Err(AppError::Integration(
                "agent control definition is unavailable".to_owned(),
            )),
        }
    }

    fn attempt_mut(&mut self) -> Result<&mut AttemptRecord, AppError> {
        let ControlRecordState::Attempt(attempt) = &mut self.state else {
            return Err(AppError::Conflict(
                "control does not own an agent attempt".to_owned(),
            ));
        };
        Ok(attempt)
    }

    fn attempt_identity(&self) -> Result<(&AgentWorkspaceId, &ConversationId), AppError> {
        let identity = self.attempt()?.identity.as_ref().ok_or_else(|| {
            AppError::Conflict("control has no durable attempt identity".to_owned())
        })?;
        Ok((&identity.workspace_id, &identity.conversation_id))
    }

    fn retained_attempt_identity(&self) -> Option<&AttemptIdentity> {
        self.attempt().ok()?.identity.as_ref()
    }

    fn attempt_number(&self) -> Result<u32, AppError> {
        Ok(self.attempt()?.number)
    }

    fn is_retry(&self) -> bool {
        self.attempt()
            .is_ok_and(|attempt| matches!(&attempt.phase, AttemptPhaseRecord::Retry { .. }))
    }

    fn notification_id(&self) -> Result<&NotificationId, AppError> {
        match &self.attempt()?.phase {
            AttemptPhaseRecord::Waiting { notification_id }
            | AttemptPhaseRecord::Unknown {
                notification_id: Some(notification_id),
            } => Ok(notification_id),
            _ => Err(AppError::Conflict(
                "control has no exact notification identity".to_owned(),
            )),
        }
    }

    fn abort(&self) -> Result<ReconciliationRecord, AppError> {
        Ok(self.attempt()?.abort)
    }

    fn prepare_dispatch(&mut self, identity: AttemptIdentity) -> Result<(), AppError> {
        let attempt = self.attempt_mut()?;
        attempt.identity = Some(identity);
        attempt.phase = AttemptPhaseRecord::Dispatched;
        Ok(())
    }

    fn set_attempt_phase(&mut self, phase: AttemptPhaseRecord) -> Result<(), AppError> {
        self.attempt_mut()?.phase = phase;
        Ok(())
    }

    fn begin_abort(&mut self) -> Result<(), AppError> {
        let abort = &mut self.attempt_mut()?.abort;
        if matches!(abort, ReconciliationRecord::Idle) {
            *abort = ReconciliationRecord::Intended;
        }
        Ok(())
    }

    fn settle_abort(&mut self, applied: bool) -> Result<(), AppError> {
        let attempt = self.attempt_mut()?;
        let next = if applied {
            ReconciliationRecord::Applied
        } else {
            ReconciliationRecord::Unknown
        };
        if !matches!(attempt.abort, ReconciliationRecord::Intended) {
            return Err(AppError::Conflict(
                "abort evidence transition is not monotonic".to_owned(),
            ));
        }
        let notification_id = match &attempt.phase {
            AttemptPhaseRecord::Waiting { notification_id } => Some(notification_id.clone()),
            AttemptPhaseRecord::Unknown {
                notification_id, ..
            } => notification_id.clone(),
            AttemptPhaseRecord::Intent
            | AttemptPhaseRecord::Dispatched
            | AttemptPhaseRecord::Retry { .. } => None,
        };
        attempt.abort = next;
        attempt.phase = AttemptPhaseRecord::Unknown { notification_id };
        Ok(())
    }

    fn retry_intent(mut self, sequence: u64) -> Result<Self, AppError> {
        if !self.is_retry() || sequence <= self.sequence {
            return Err(AppError::Conflict(
                "retry activation is no longer current".into(),
            ));
        }
        self.sequence = sequence;
        let attempt = self.attempt_mut()?;
        attempt.phase = AttemptPhaseRecord::Intent;
        attempt.abort = ReconciliationRecord::Idle;
        Ok(self)
    }

    fn prepare_retry(&mut self, number: u32, wake_millis: u64) -> Result<(), AppError> {
        let attempt = self.attempt_mut()?;
        attempt.number = number;
        attempt.phase = AttemptPhaseRecord::Retry { wake_millis };
        Ok(())
    }
}

impl FailureCategory {
    fn from_failure(failure: &WorkflowFailure) -> Self {
        match failure {
            WorkflowFailure::Evaluation(_) => Self::Evaluation,
            WorkflowFailure::State(_) => Self::State,
            WorkflowFailure::Agent(_) => Self::Agent,
            WorkflowFailure::Action(_) => Self::Action,
            WorkflowFailure::Hook { .. } => Self::Hook,
            WorkflowFailure::Child(_) => Self::Child,
            WorkflowFailure::Execution(_) => Self::Execution,
            WorkflowFailure::Timeout(_) => Self::Timeout,
            WorkflowFailure::Limit(_) => Self::Limit,
        }
    }

    fn project(&self, message: String) -> WorkflowFailure {
        match self {
            Self::Evaluation => WorkflowFailure::Evaluation(message),
            Self::State => WorkflowFailure::State(message),
            Self::Agent => WorkflowFailure::Agent(message),
            Self::Action => WorkflowFailure::Action(message),
            Self::Hook => WorkflowFailure::Hook {
                message,
                cause: None,
            },
            Self::Child => WorkflowFailure::Child(message),
            Self::Execution => WorkflowFailure::Execution(message),
            Self::Timeout => WorkflowFailure::Timeout(message),
            Self::Limit => WorkflowFailure::Limit(message),
        }
    }
}

impl ChildTask {
    fn validate(
        &self,
        parent: &RunRecord,
        definition: &CompiledDefinition,
        node: &str,
        index: usize,
    ) -> Result<(), AppError> {
        if let Some(initializer) = &self.initializer {
            initializer.validate().map_err(|_| {
                AppError::Integration("durable child initializer is invalid".into())
            })?;
        }
        let step = definition
            .node(node)
            .ok_or_else(|| AppError::Integration("child declaration is unavailable".into()))?
            .step();
        if matches!(step, Step::Map(MapDeclaration::Tasks { .. })) {
            return Ok(());
        }
        let (selection, history) = step.child_selection(index)?;
        if let ChildSelection::Literal { workflow, revision } = selection {
            let workflow = WorkflowId::from_str(workflow)?;
            let revision = revision
                .map(Ok)
                .unwrap_or_else(|| parent.child_revision(&workflow))?;
            if self.workflow != workflow || self.revision != revision {
                return Err(AppError::Integration(
                    "durable child target differs from its literal declaration".into(),
                ));
            }
        }
        if self.history != history {
            return Err(AppError::Integration(
                "durable child history policy differs from its declaration".into(),
            ));
        }
        Ok(())
    }
}
