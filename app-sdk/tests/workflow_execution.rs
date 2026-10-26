use std::collections::{BTreeMap, VecDeque};
use std::panic::AssertUnwindSafe;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::{error::Error, num::NonZeroUsize, path::PathBuf, time::Duration};

use agent_sdk::conversation::{ConversationInfo, FileRead, NewConversation};
use agent_sdk::identity::{AgentId, AgentInfo, ConversationId, NotificationId, WorkspaceId};
use agent_sdk::knowledge::IngestKnowledgeRequest;
use agent_sdk::model::ModelInfo;
use agent_sdk::notification::{
    ConversationNotification, ConversationNotificationKind, ConversationNotificationReceiver,
    NotificationResolution,
};
use agent_sdk::prompt::{PromptEvent, PromptRequest, PromptResponse, PromptStream};
use agent_sdk::tool::ToolInfo;
use agent_sdk::workspace::{
    DeleteWorkspaceRequest, NewWorkspace, UpdateWorkspaceRequest, WorkspaceInfo,
    WorkspaceUpdateReadiness,
};
use agent_sdk::{Agent, AgentBackend, ConversationBackend, SdkError, WorkspaceBackend};
use app_sdk::workflow::{
    ActionContext, ActionMode, CheckpointKind, CheckpointSelector, ExecutionOptions, Interaction,
    LifecycleEvent, LifecycleHooks, Resolution, RestoreOptions, ResumePosition, RunSelector,
    RunSnapshot, RunStatus, WorkflowDefinition, WorkflowFailure, WorkflowState,
};
use app_sdk::{AgentSpec, App, AppConfig, AppError, Shutdown, Workspace, WorkspaceConfig};
use futures_util::FutureExt;
use tracing::instrument::WithSubscriber;
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

mod app_v6_fixture;

use app_v6_fixture::AppV6Fixture;

pub mod observability_support;
use observability_support::InteractionCapture;
use serde_json::{Map, Value};
use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_sdk::state::record::{
    StateKey, StateKeyPrefix, StateListRequest, StateListResponse, StateRecord,
    StateRecordSnapshot, StateRevision,
};
use storage_sdk::{
    StateStore, StateStoreBackend, StateStoreError, StateTransaction, StateTransactionBackend,
};
use tokio::sync::{Barrier, Notify};

#[derive(Clone)]
struct ClaimRaceStore {
    state: Arc<ClaimRaceState>,
}

struct ClaimRaceTransaction {
    state: Arc<ClaimRaceState>,
    snapshot: BTreeMap<String, StateRecordSnapshot>,
    changes: BTreeMap<String, Option<StateRecord>>,
    claims_missing_app: bool,
}

struct ClaimRaceState {
    records: Mutex<BTreeMap<String, StateRecordSnapshot>>,
    claims: Barrier,
    claims_applied: Barrier,
}

struct FaultStateStore {
    inner: StateStore,
    faults: Arc<StorageFaults>,
}

struct FaultStateTransaction {
    inner: StateTransaction,
    faults: Arc<StorageFaults>,
    admits_run: bool,
    admission_record: Option<StateRecord>,
    admits_workspace: bool,
    parks_sleep: bool,
    parks_agent: bool,
    publishes_cancelling: bool,
    advances_run: bool,
    monitored_mutation: bool,
    mutation_receipt_seen: bool,
    sibling_admission: Option<SiblingAdmissionCase>,
    ordered_receipt: Option<StateRecord>,
    receipt_writes: usize,
    receipt_deletes: usize,
    staged_mutation: bool,
    observation: Option<ObservedStorageTransaction>,
    migration_progress: Option<StateRecord>,
    settlement_rollback_failure: bool,
}

struct SettlementRollbackFault {
    key: StateKey,
    primary: StateStoreError,
    remaining: usize,
    attempts: usize,
    loads: usize,
    rollbacks: usize,
    commits: usize,
}

enum MigrationFault {
    None,
    ConcurrentClaims {
        entered: usize,
        succeeded: usize,
        conflicted: usize,
        released: bool,
    },
    StopAfter {
        boundary: MigrationBoundary,
    },
    LoseCommitReply {
        boundary: MigrationBoundary,
        after: Option<String>,
    },
    CommitReplyLost {
        progress: StateRecord,
    },
    LoseReplyBeforeReadback {
        after: String,
        mismatched_revision: bool,
    },
    ReadbackRejected {
        progress: StateRecord,
        mismatched_revision: bool,
        reads: usize,
        rollbacks: usize,
        commits: usize,
    },
    FailNextBegin {
        progress: StateRecord,
    },
    Stopped {
        progress: StateRecord,
    },
    RevisionOverflow {
        key: StateKey,
        revision: StateRevision,
    },
}

#[derive(Clone, Copy, Debug)]
enum MigrationBoundary {
    Claim,
    Checkpoint,
    Records,
    Audit,
    Ready,
}

impl MigrationBoundary {
    fn observes(self, root: &Value) -> bool {
        let state = root.pointer("/migration/state").and_then(Value::as_str);
        if matches!(self, Self::Ready) {
            return state == Some("ready");
        }
        if state != Some("migrating") {
            return false;
        }
        let phase = root
            .pointer("/migration/phase/phase")
            .and_then(Value::as_str);
        let advanced = root
            .pointer("/migration/phase/after")
            .is_some_and(Value::is_string);
        match self {
            Self::Claim => phase == Some("deletions") && !advanced,
            Self::Checkpoint => phase == Some("checkpoints") && advanced,
            Self::Records => phase == Some("records") && advanced,
            Self::Audit => phase == Some("audit"),
            Self::Ready => unreachable!(),
        }
    }
}

#[derive(Clone, Copy)]
enum NotificationCorruption {
    None,
    LeafKey,
    LeafNamespace,
    DuplicateTarget,
    RemoveApplied,
    ChangeAppliedDecision,
    TupleObject {
        path: &'static str,
    },
    TupleMissing {
        path: &'static str,
    },
    TupleExtra {
        path: &'static str,
    },
    TupleField {
        path: &'static str,
    },
    TupleText {
        path: &'static str,
        text: &'static str,
    },
    TupleTag {
        path: &'static str,
    },
    TupleTimestampOverflow,
    OrphanTarget {
        observer: Option<tokio::task::Id>,
    },
    ProspectiveBytes {
        observer: Option<tokio::task::Id>,
    },
}

#[derive(Clone, Copy)]
enum StatePreparationFault {
    None,
    WholeActivationDeadline,
    StallAfterIntent,
    StallNextLoad,
    PanicAfterIntent,
    PanicNextLoad,
    FailAfterIntent,
    FailNextLoad,
    Observed,
}

#[derive(Clone, Copy, Debug)]
enum InventoryPageFault {
    None,
    ForeignNamespace,
    WorkspacePrefix,
    CursorMismatch,
    Short {
        prefix: Option<&'static str>,
        extra: usize,
        pages: usize,
        corruption: Option<(usize, InventoryCorruption)>,
    },
    Observed,
}

#[derive(Clone, Copy, Debug)]
enum InventoryCorruption {
    Epoch,
    Namespace,
    Prefix,
    Oversized,
    EmptyWithCursor,
    Nonprogressing,
}

#[derive(Clone, Copy, Debug)]
enum CheckpointEvidence {
    StateOrder,
    NestedOrder,
    DtoOrder,
    IntegerFloat,
    ZeroSign,
    Unchanged,
}

impl CheckpointEvidence {
    fn contradicts(self) -> bool {
        !matches!(self, Self::DtoOrder | Self::Unchanged)
    }

    fn apply(self, value: &mut Value) {
        let path = match self {
            Self::StateOrder => "/checkpoint/state",
            Self::NestedOrder => "/checkpoint/state/evidence",
            Self::DtoOrder => "/checkpoint",
            Self::IntegerFloat => {
                *value
                    .pointer_mut("/checkpoint/state/evidence/numbers/2")
                    .unwrap() = serde_json::json!(1.0);
                return;
            }
            Self::ZeroSign => {
                *value
                    .pointer_mut("/checkpoint/state/evidence/numbers/4")
                    .unwrap() = serde_json::json!(0.0);
                return;
            }
            Self::Unchanged => return,
        };
        let object = value.pointer_mut(path).unwrap().as_object_mut().unwrap();
        assert!(object.len() > 1);
        *object = std::mem::take(object).into_iter().rev().collect();
        if matches!(self, Self::DtoOrder) {
            let object = value.as_object_mut().unwrap();
            *object = std::mem::take(object).into_iter().rev().collect();
        }
    }
}

#[derive(Clone, Copy)]
enum AttemptReadCase {
    BeforeWake { held: bool },
    AfterWake { held: bool },
    Unsubmitted { fault: AttemptReadFault },
    Preparation { fault: AttemptReadFault },
    PreparationWake { fault: AttemptReadFault },
    PreparationQueued { fault: AttemptReadFault },
    BeforePrompt { fault: AttemptReadFault },
    PromptWake { fault: AttemptReadFault },
    NotificationDelivery { fault: AttemptReadFault },
    PromptFailure { fault: AttemptReadFault },
    ResolutionObservation { owner: Option<tokio::task::Id> },
}

impl AttemptReadCase {
    fn fault(self) -> AttemptReadFault {
        match self {
            Self::ResolutionObservation { .. } => AttemptReadFault::Held,
            Self::BeforeWake { held } | Self::AfterWake { held } => {
                if held {
                    AttemptReadFault::Held
                } else {
                    AttemptReadFault::Permanent
                }
            }
            Self::Unsubmitted { fault }
            | Self::Preparation { fault }
            | Self::PreparationWake { fault }
            | Self::PreparationQueued { fault }
            | Self::BeforePrompt { fault }
            | Self::PromptWake { fault }
            | Self::NotificationDelivery { fault }
            | Self::PromptFailure { fault } => fault,
        }
    }

    fn native_phase(self) -> &'static str {
        match self {
            Self::BeforeWake { .. } | Self::AfterWake { .. } => "retry",
            Self::Unsubmitted { .. }
            | Self::Preparation { .. }
            | Self::PreparationWake { .. }
            | Self::PreparationQueued { .. } => "intent",
            Self::BeforePrompt { .. }
            | Self::NotificationDelivery { .. }
            | Self::PromptFailure { .. } => "dispatched",
            Self::PromptWake { .. } | Self::ResolutionObservation { .. } => "waiting",
        }
    }
}

#[derive(Clone, Copy)]
enum AttemptReadFault {
    Recoverable,
    Permanent,
    Held,
}

#[derive(Clone, Copy)]
enum SiblingAdmissionCase {
    RejectedInput,
    RejectedAgent,
    LostAgent,
    LostInput,
    CorruptAgent,
}

enum AppStoreAdmissionScope {
    Any,
    Capture,
    Owned(tokio::task::Id),
}

struct AdmissionAttempt {
    record: StateRecord,
    outcome: Result<(), StateStoreError>,
}

#[derive(Clone, Debug, Default)]
struct StorageObservation {
    started: usize,
    completed: usize,
    active: usize,
    maximum_active: usize,
    failed: usize,
    dropped_observers: usize,
    completed_elapsed: Duration,
    maximum_completed: Duration,
    dropped_elapsed: Duration,
    maximum_dropped: Duration,
}

impl StorageObservation {
    fn start(&mut self) {
        self.started += 1;
        self.active += 1;
        self.maximum_active = self.maximum_active.max(self.active);
    }

    fn end(&mut self, elapsed: Duration, failed: Option<bool>) {
        self.active -= 1;
        match failed {
            Some(failed) => {
                self.completed += 1;
                self.failed += usize::from(failed);
                self.completed_elapsed += elapsed;
                self.maximum_completed = self.maximum_completed.max(elapsed);
            }
            None => {
                self.dropped_observers += 1;
                self.dropped_elapsed += elapsed;
                self.maximum_dropped = self.maximum_dropped.max(elapsed);
            }
        }
    }

    fn merge(&mut self, other: &Self) {
        self.started += other.started;
        self.completed += other.completed;
        self.active += other.active;
        self.maximum_active = self.maximum_active.max(other.maximum_active);
        self.failed += other.failed;
        self.dropped_observers += other.dropped_observers;
        self.completed_elapsed += other.completed_elapsed;
        self.maximum_completed = self.maximum_completed.max(other.maximum_completed);
        self.dropped_elapsed += other.dropped_elapsed;
        self.maximum_dropped = self.maximum_dropped.max(other.maximum_dropped);
    }
}

// One observed future may end before its admitted native work. This guard owns
// diagnostic accounting only; it never cancels, joins or classifies native work.
// Dropped observers include cancelled futures and unwinding before a result.
struct ObservedStorageCall<'a> {
    faults: &'a StorageFaults,
    operation: &'static str,
    started: Option<std::time::Instant>,
    transaction: Option<(&'a mut ObservedStorageTransaction, usize)>,
}

impl<'a> ObservedStorageCall<'a> {
    fn within(mut self, transaction: Option<&'a mut ObservedStorageTransaction>) -> Self {
        if let Some(transaction) = transaction {
            let index = match self.operation {
                "transaction_load" => 0,
                "put" => 1,
                "delete" => 2,
                _ => unreachable!("only transaction preparation calls are accumulated"),
            };
            transaction.native[index].start();
            self.transaction = Some((transaction, index));
        }
        self
    }

    fn finish(mut self, failed: bool) {
        self.end(Some(failed));
    }

    fn end(&mut self, failed: Option<bool>) {
        if let Some(started) = self.started.take() {
            let elapsed = started.elapsed();
            self.faults.observe_end(self.operation, elapsed, failed);
            if let Some((transaction, index)) = &mut self.transaction {
                transaction.native[*index].end(elapsed, failed);
            }
        }
    }
}

impl Drop for ObservedStorageCall<'_> {
    fn drop(&mut self) {
        self.end(None);
    }
}

// A bounded observation of one actual transaction, from begin's successful
// return through finalization's result or observer loss. It owns no native work.
// The gap includes caller/fixture scheduling and validation, not just App CPU.
struct ObservedStorageTransaction {
    faults: Arc<StorageFaults>,
    started: std::time::Instant,
    native: [StorageObservation; 3],
    mutating: bool,
    finalization: Option<std::time::Instant>,
    settled: bool,
}

impl ObservedStorageTransaction {
    fn new(faults: &Arc<StorageFaults>) -> Option<Self> {
        let mut observations = faults.observations.lock().unwrap();
        let rows = observations.as_mut()?;
        rows.entry("transaction_age").or_default().start();
        Some(Self {
            faults: Arc::clone(faults),
            started: std::time::Instant::now(),
            native: std::array::from_fn(|_| StorageObservation::default()),
            mutating: false,
            finalization: None,
            settled: false,
        })
    }

    fn finalize(&mut self) {
        self.finalization = Some(std::time::Instant::now());
    }

    fn finish(mut self, failed: bool) {
        self.end(Some(failed));
    }

    fn end(&mut self, failed: Option<bool>) {
        if self.settled {
            return;
        }
        self.settled = true;
        let ended = std::time::Instant::now();
        let age = ended.duration_since(self.started);
        let preparation = self
            .finalization
            .unwrap_or(ended)
            .duration_since(self.started);
        let finalization = self.finalization.map(|start| ended.duration_since(start));
        let native = self
            .native
            .iter()
            .map(|row| row.completed_elapsed + row.dropped_elapsed)
            .sum::<Duration>();
        let names = if self.mutating {
            [
                "transaction_mutating_age",
                "transaction_mutating_pre_finalization",
                "transaction_mutating_gap",
                "transaction_mutating_finalization",
                "transaction_mutating_load",
                "transaction_mutating_put",
                "transaction_mutating_delete",
            ]
        } else {
            [
                "transaction_read_only_age",
                "transaction_read_only_pre_finalization",
                "transaction_read_only_gap",
                "transaction_read_only_finalization",
                "transaction_read_only_load",
                "transaction_read_only_put",
                "transaction_read_only_delete",
            ]
        };
        let mut observations = self.faults.observations.lock().unwrap();
        let rows = observations.as_mut().unwrap();
        rows.get_mut("transaction_age").unwrap().end(age, failed);
        // Only transaction_age tracks concurrently live transaction observers.
        // Classified rows are merged at settlement: their maximum_active is a
        // per-transaction peak, not global concurrency within that category.
        for (name, elapsed) in
            names[..3]
                .iter()
                .zip([age, preparation, preparation.saturating_sub(native)])
        {
            let row = rows.entry(name).or_default();
            row.start();
            row.end(elapsed, failed);
        }
        if let Some(elapsed) = finalization {
            let row = rows.entry(names[3]).or_default();
            row.start();
            row.end(elapsed, failed);
        }
        for (name, native) in names[4..].iter().zip(&self.native) {
            rows.entry(name).or_default().merge(native);
        }
    }
}

impl Drop for ObservedStorageTransaction {
    fn drop(&mut self) {
        self.end(None);
    }
}

#[derive(Clone, Default)]
struct CheckpointPollCapture {
    state: Arc<Mutex<CheckpointPollState>>,
}

#[derive(Default)]
struct CheckpointPollState {
    next: u64,
    live: BTreeMap<u64, CheckpointPollSpan>,
    totals: CheckpointPollTotals,
}

struct CheckpointPollSpan {
    references: usize,
    started: std::time::Instant,
    entered: Option<(std::thread::ThreadId, std::time::Instant)>,
}

#[derive(Clone, Copy, Debug, Default)]
struct CheckpointPollTotals {
    opened: usize,
    closed: usize,
    polls: usize,
    active_poll: Duration,
    lifetime: Duration,
    violations: usize,
}

impl CheckpointPollCapture {
    fn snapshot(&self) -> CheckpointPollTotals {
        self.state.lock().unwrap().totals
    }

    fn report_block(&self, first: usize, completed: usize, previous: &mut CheckpointPollTotals) {
        let current = self.snapshot();
        eprintln!(
            "delivery-prefix phase=checkpoint-polls first={first} completed={completed} opened={} closed={} polls={} active_poll_ns={} lifetime_ns={} live={} violations={} scope=inclusive-poll-wall-not-cpu closed_is_not_success=true",
            current.opened - previous.opened,
            current.closed - previous.closed,
            current.polls - previous.polls,
            (current.active_poll - previous.active_poll).as_nanos(),
            (current.lifetime - previous.lifetime).as_nanos(),
            current.opened - current.closed,
            current.violations
        );
        *previous = current;
    }
}

impl Subscriber for CheckpointPollCapture {
    fn register_callsite(&self, _: &'static Metadata<'static>) -> tracing::subscriber::Interest {
        tracing::subscriber::Interest::sometimes()
    }

    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.is_span()
            && metadata.target() == "app_sdk"
            && metadata.name() == "app.workflow.checkpoint"
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
        let mut state = self.state.lock().unwrap();
        state.next += 1;
        let id = state.next;
        state.live.insert(
            id,
            CheckpointPollSpan {
                references: 1,
                started: std::time::Instant::now(),
                entered: None,
            },
        );
        state.totals.opened += 1;
        Id::from_u64(id)
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event_enabled(&self, _: &Event<'_>) -> bool {
        false
    }
    fn event(&self, _: &Event<'_>) {}

    fn enter(&self, id: &Id) {
        let mut state = self.state.lock().unwrap();
        let span = state.live.get_mut(&id.into_u64()).unwrap();
        if span
            .entered
            .replace((std::thread::current().id(), std::time::Instant::now()))
            .is_some()
        {
            state.totals.violations += 1;
        }
    }

    fn exit(&self, id: &Id) {
        let mut state = self.state.lock().unwrap();
        let entered = state.live.get_mut(&id.into_u64()).unwrap().entered.take();
        match entered {
            Some((thread, started)) if thread == std::thread::current().id() => {
                state.totals.polls += 1;
                state.totals.active_poll += started.elapsed();
            }
            _ => state.totals.violations += 1,
        }
    }

    fn clone_span(&self, id: &Id) -> Id {
        self.state
            .lock()
            .unwrap()
            .live
            .get_mut(&id.into_u64())
            .unwrap()
            .references += 1;
        id.clone()
    }

    fn try_close(&self, id: Id) -> bool {
        let mut state = self.state.lock().unwrap();
        let span = state.live.get_mut(&id.into_u64()).unwrap();
        span.references -= 1;
        if span.references != 0 {
            return false;
        }
        let span = state.live.remove(&id.into_u64()).unwrap();
        state.totals.closed += 1;
        state.totals.lifetime += span.started.elapsed();
        state.totals.violations += usize::from(span.entered.is_some());
        true
    }
}

struct StorageFaults {
    observations: Mutex<Option<BTreeMap<&'static str, StorageObservation>>>,
    attempt_read: Mutex<Option<AttemptReadCase>>,
    attempt_owner: Mutex<Option<tokio::task::Id>>,
    attempt_read_records: Mutex<Option<(StateRecord, StateRecord)>>,
    attempt_read_entered: Notify,
    attempt_read_release: Notify,
    attempt_read_dropped: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    sibling_admission: Mutex<Option<SiblingAdmissionCase>>,
    sibling_sequence: AtomicUsize,
    inventory_page: Mutex<InventoryPageFault>,
    inventory_page_reads: AtomicUsize,
    preparation: Mutex<StatePreparationFault>,
    preparation_entered: Notify,
    agent_intents: AtomicUsize,
    agent_intents_observed: Notify,
    notification_corruption: Mutex<NotificationCorruption>,
    migration_fault: Mutex<MigrationFault>,
    settlement_rollback_fault: Mutex<Option<SettlementRollbackFault>>,
    app_load_panics: AtomicUsize,
    app_load_panic_count: AtomicUsize,
    admission_panics: AtomicBool,
    admission_armed: AtomicBool,
    admission_committed: Notify,
    admission_release: Notify,
    admission_attempts: Mutex<Option<Vec<AdmissionAttempt>>>,
    admission_commit_holds: AtomicUsize,
    admission_commit_entered: Notify,
    admission_commit_release: Notify,
    admission_inventory_requested: AtomicBool,
    admission_inventory_armed: AtomicBool,
    admission_inventory_observed: Notify,
    admission_inventory_release: Notify,
    run_transition_monitor: AtomicBool,
    run_transition_gate_armed: AtomicBool,
    run_transition_observed: Notify,
    run_transition_release: Notify,
    active_run_transitions: AtomicUsize,
    maximum_active_run_transitions: AtomicUsize,
    app_store_admission_monitor: AtomicBool,
    app_store_mutation_boundary: AtomicBool,
    monitored_mutations: AtomicUsize,
    monitored_mutation_conflicts: AtomicUsize,
    app_store_admission_scope: Mutex<AppStoreAdmissionScope>,
    transaction_begin_failures: AtomicUsize,
    mutation_admission_failures: AtomicUsize,
    workspace_admission_armed: AtomicBool,
    workspace_admission_committed: Notify,
    workspace_admission_release: Notify,
    workspace_observation_load_armed: AtomicBool,
    workspace_observation_load_faulted: Notify,
    receipt_obscured: AtomicBool,
    ordered_receipt: Mutex<Option<(u64, Option<StateRecord>)>>,
    ordered_receipt_reads: AtomicUsize,
    checkpoint_evidence: Mutex<CheckpointEvidence>,
    predecessor_evidence: Mutex<Option<StateKey>>,
    predecessor_reads: AtomicUsize,
    receipt_page_values: Mutex<Option<Vec<std::sync::Weak<Value>>>>,
    receipt_page_peak: AtomicUsize,
    receipt_writes: AtomicUsize,
    receipt_cleanup_batches: Mutex<Vec<usize>>,
    reject_receipt_cleanup: AtomicBool,
    ambiguity_observed: Notify,
    sleep_fault_requested: AtomicBool,
    sleep_load_armed: AtomicBool,
    sleep_load_faulted: AtomicBool,
    sleep_load_observed: Notify,
    sleep_integration_requested: AtomicUsize,
    sleep_integration_armed: AtomicUsize,
    sleep_integration_attempts: AtomicUsize,
    sleep_integration_notify_at: AtomicUsize,
    excessive_integration_attempts: Notify,
    scripted_sleep_failures: Mutex<VecDeque<usize>>,
    active_sleep_failure: Mutex<Option<ActiveSleepFailure>>,
    scripted_sleep_attempts: Mutex<Vec<usize>>,
    second_sleep_failure_observed: Notify,
    second_sleep_recovery_observed: Notify,
    shutdown_scan_armed: AtomicBool,
    shutdown_scan_observed: Notify,
    shutdown_scan_release: Notify,
    preclosed_load_armed: AtomicBool,
    preclosed_load_observed: Notify,
    preclosed_load_release: Notify,
    cancelling_publications: AtomicUsize,
}

struct StreamFailureAgent {
    state: Arc<StreamFailureState>,
}

struct StreamFailureWorkspace {
    state: Arc<StreamFailureState>,
    claimed: bool,
}

struct StreamFailureConversation {
    state: Arc<StreamFailureState>,
    id: ConversationId,
}

#[derive(Clone, Copy)]
enum StreamFailureDeletion {
    Retry,
    BlockThenComplete,
    Complete,
    TransientThenComplete { remaining: usize },
}

#[derive(Clone, Copy)]
enum PreparationFault {
    Pass,
    Stall,
    Panic,
    WorkspaceStall,
    WorkspacePanic,
    RecoveryStall,
    RecoveryPanic,
    AbortStall,
    AbortGate,
    AbortPanic,
    ExclusiveWorkspace { gate_first_creation: bool },
}

#[derive(Clone, Copy, Debug)]
enum RetryFailure {
    AfterEffect(agent_sdk::error::ErrorKind),
    StreamOpeningAfterEffect(agent_sdk::error::ErrorKind),
    Workspace,
    Busy,
    InvalidConfig,
    InvalidInput,
    Unsupported,
    NotYetImplemented,
    Cancelled,
    Closed,
    NotFound,
    Conflict,
    Timeout,
    Protocol,
    InvalidState,
    Uncertain,
}

impl RetryFailure {
    fn error(self) -> SdkError {
        match self {
            Self::AfterEffect(kind) | Self::StreamOpeningAfterEffect(kind) => {
                SdkError::new(kind, "accepted work failed after an observable effect").with_context(
                    agent_sdk::error::Operation::Prompt,
                    match kind {
                        agent_sdk::error::ErrorKind::InvalidInput
                        | agent_sdk::error::ErrorKind::Unsupported => {
                            agent_sdk::error::ErrorPhase::ToolExecution
                        }
                        _ => agent_sdk::error::ErrorPhase::CompletionValidation,
                    },
                )
            }
            Self::Workspace => SdkError::integration("workspace lookup failed before preparation"),
            Self::Busy => SdkError::busy("prompt was rejected before admission"),
            Self::InvalidInput => SdkError::invalid_input("prompt was rejected before admission"),
            Self::InvalidConfig => SdkError::invalid_config("prompt was rejected before admission"),
            Self::Unsupported => SdkError::unsupported("prompt was rejected before admission"),
            Self::NotYetImplemented => {
                SdkError::not_yet_implemented("prompt was rejected before admission")
            }
            Self::Closed => SdkError::closed("the original owner is closed"),
            Self::NotFound => SdkError::not_found("the addressed resource is unavailable"),
            Self::Conflict => SdkError::conflict("accepted prompt authority conflicts"),
            Self::Timeout => SdkError::timeout("the accepted prompt timed out"),
            Self::Protocol => SdkError::protocol("accepted prompt transport failed"),
            Self::InvalidState => SdkError::invalid_state("accepted prompt authority is invalid"),
            Self::Cancelled => SdkError::cancelled("the admitted prompt was cancelled"),
            Self::Uncertain => {
                SdkError::integration("the accepted prompt has an uncertain outcome")
            }
        }
    }
}

#[derive(Clone, Copy)]
enum QueuedPreparation {
    Complete,
    Cancel,
    Expire,
}

#[derive(Clone, Copy)]
enum NotificationResolutionFault {
    Pass,
    Stall,
    Panic,
    LostReceipt,
    GateLostReceipt,
    GateReceipt,
}

#[derive(Clone, Copy)]
enum NotificationEmission {
    Sequential,
    Burst,
    GateAfterFirst,
    GateBeforeLast,
}

struct NotificationSequence {
    count: usize,
    redeliver: bool,
    emission: Mutex<NotificationEmission>,
    encoded_body_bytes: Mutex<Vec<usize>>,
    emitted: AtomicUsize,
    received: AtomicUsize,
    prompt_returns: AtomicUsize,
    prompt_error: Mutex<Option<agent_sdk::error::ErrorKind>>,
    emission_completed: Notify,
    next_target_entered: Notify,
    next_target_release: Notify,
    resolution_fault: Mutex<NotificationResolutionFault>,
    resolution_entered: Notify,
    resolution_release: Notify,
    resolution_calls: AtomicUsize,
    resolution_returns: AtomicUsize,
    stall_returned: AtomicBool,
    sender: tokio::sync::mpsc::Sender<Result<ConversationNotification, SdkError>>,
    receiver:
        Mutex<Option<tokio::sync::mpsc::Receiver<Result<ConversationNotification, SdkError>>>>,
    resolved: Mutex<Vec<NotificationId>>,
    changed: Notify,
    cancelled: AtomicBool,
}

struct StreamFailureState {
    notifications: Mutex<Option<Arc<NotificationSequence>>>,
    read_authority: Mutex<Option<(Arc<StorageFaults>, AttemptReadCase)>>,
    retry_failures: Mutex<Option<VecDeque<RetryFailure>>>,
    retry_observed: Mutex<Vec<std::time::Instant>>,
    step_contexts: Mutex<Vec<ActionContext>>,
    preparation_fault: Mutex<PreparationFault>,
    preparation_entered: Notify,
    preparation_release: Notify,
    workspace_claimed: AtomicBool,
    agent_id: AgentId,
    workspace_id: WorkspaceId,
    workspace: Mutex<Option<WorkspaceInfo>>,
    conversation_inventory: Mutex<BTreeMap<ConversationId, ConversationInfo>>,
    workspace_lookups: AtomicUsize,
    conversations: AtomicUsize,
    reopens: AtomicUsize,
    streams: AtomicUsize,
    prompts: AtomicUsize,
    side_effects: Mutex<Option<usize>>,
    aborts: AtomicUsize,
    abort_returns: AtomicUsize,
    workspace_check_armed: AtomicBool,
    workspace_check_observed: Notify,
    workspace_check_release: Notify,
    workspace_update_armed: AtomicBool,
    workspace_update_observed: Notify,
    workspace_update_release: Notify,
    workspace_deletion: Mutex<StreamFailureDeletion>,
    workspace_delete_attempts: AtomicUsize,
    workspace_delete_observed: Notify,
    workspace_delete_release: Notify,
    shutdown_panics: AtomicBool,
    shutdowns: AtomicUsize,
    shutdown_armed: AtomicBool,
    shutdown_observed: Notify,
    shutdown_release: Notify,
}

struct ActiveSleepFailure {
    remaining: usize,
    park: usize,
}

impl ClaimRaceState {
    fn store() -> ClaimRaceStore {
        ClaimRaceStore {
            state: Arc::new(Self {
                records: Mutex::new(BTreeMap::new()),
                claims: Barrier::new(2),
                claims_applied: Barrier::new(2),
            }),
        }
    }

    fn apply(&self, changes: BTreeMap<String, Option<StateRecord>>) -> Result<(), StateStoreError> {
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for (key, change) in changes {
            match change {
                Some(record) => {
                    let revision = records
                        .get(&key)
                        .map_or(Ok(StateRevision::first()), |snapshot| {
                            StateRevision::new(snapshot.revision.get() + 1)
                        })?;
                    records.insert(key, StateRecordSnapshot::new(record, revision));
                }
                None => {
                    records.remove(&key);
                }
            }
        }
        Ok(())
    }
}

impl StateStoreBackend for ClaimRaceStore {
    type Transaction = ClaimRaceTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        Ok(self
            .state
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(key.key())
            .cloned())
    }

    async fn list(&self, _request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        Ok(StateListResponse::default())
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        let snapshot = self
            .state
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        Ok(ClaimRaceTransaction {
            state: Arc::clone(&self.state),
            snapshot,
            changes: BTreeMap::new(),
            claims_missing_app: false,
        })
    }
}

impl StateTransactionBackend for ClaimRaceTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        if let Some(change) = self.changes.get(key.key()) {
            return Ok(change.clone().map(|record| {
                let revision = self
                    .snapshot
                    .get(key.key())
                    .map_or(StateRevision::first(), |snapshot| snapshot.revision);
                StateRecordSnapshot::new(record, revision)
            }));
        }
        Ok(self.snapshot.get(key.key()).cloned())
    }

    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        self.claims_missing_app |= record.key.key() == "app" && !self.snapshot.contains_key("app");
        self.changes
            .insert(record.key.key().to_owned(), Some(record));
        Ok(())
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        self.changes.insert(key.key().to_owned(), None);
        Ok(())
    }

    async fn commit(self) -> Result<(), StateStoreError> {
        if !self.claims_missing_app {
            return self.state.apply(self.changes);
        }
        let applies = self.state.claims.wait().await.is_leader();
        if applies {
            self.state.apply(self.changes)?;
        }
        self.state.claims_applied.wait().await;
        if applies {
            Ok(())
        } else {
            Err(StateStoreError::unavailable(
                "commit",
                "test obscured the losing application claim",
            ))
        }
    }

    async fn rollback(self) -> Result<(), StateStoreError> {
        Ok(())
    }
}

impl StorageFaults {
    fn observe_start(&self, operation: &'static str) -> ObservedStorageCall<'_> {
        let started = {
            let mut observations = self.observations.lock().unwrap();
            observations.as_mut().map(|rows| {
                let row = rows.entry(operation).or_default();
                row.start();
                std::time::Instant::now()
            })
        };
        ObservedStorageCall {
            faults: self,
            operation,
            started,
            transaction: None,
        }
    }

    fn observe_end(&self, operation: &'static str, elapsed: Duration, failed: Option<bool>) {
        let mut observations = self.observations.lock().unwrap();
        observations
            .as_mut()
            .unwrap()
            .get_mut(operation)
            .unwrap()
            .end(elapsed, failed);
    }

    async fn assert_checkpoint_evidence(initial: bool, evidence: CheckpointEvidence) {
        let (faults, store) = Self::open().await.unwrap();
        let native = store.inner.clone();
        *faults.checkpoint_evidence.lock().unwrap() = evidence;
        let mut definition: Value = serde_json::from_str(LINEAR).unwrap();
        definition["state"]["evidence"] = serde_json::json!({
            "numbers": [i64::MIN, u64::MAX, 1, 1.0, -0.0, 0.0, 1.25],
            "label": "ordered evidence"
        });
        definition["reducers"]["evidence"] = Value::from("reject_concurrent");
        let mut app = App::open(AppConfig::<FaultStateStore, Agent>::new(
            store,
            Vec::new(),
            vec![WorkflowDefinition::parse(&definition.to_string()).unwrap()],
        ))
        .await
        .unwrap();
        let body = AssertUnwindSafe(async {
            tokio::time::timeout(Duration::from_secs(12), async {
                let workspace = app
                    .create_workspace(WorkspaceConfig {
                        name: Some("ordered-native-receipt".into()),
                        root: std::env::current_dir().unwrap(),
                    })
                    .await
                    .unwrap();
                let workflow = workspace.workflow("linear".parse().unwrap()).await.unwrap();
                let run = if initial {
                    None
                } else {
                    Some(workflow.prepare(WorkflowState::default()).await.unwrap())
                };
                let sequence = if initial { 0 } else { 1 };
                *faults.ordered_receipt.lock().unwrap() = Some((sequence, None));
                let observed = match run {
                    None => workflow.prepare(WorkflowState::default()).await.map(|_| ()),
                    Some(run) => run.cancel(0).await.map(|_| ()),
                };
                let expected = faults
                    .ordered_receipt
                    .lock()
                    .unwrap()
                    .as_ref()
                    .and_then(|(_, record)| record.clone())
                    .expect("original native commit applied");
                assert!(
                    !faults.receipt_obscured.load(Ordering::SeqCst),
                    "original receipt read was obscured"
                );
                assert!(
                    faults.ordered_receipt_reads.load(Ordering::SeqCst) > 0,
                    "the original applied checkpoint was read with only its state order changed"
                );
                let retained = native.load(expected.key.clone()).await.unwrap().unwrap();
                assert_eq!(
                    retained.revision,
                    StateRevision::first(),
                    "immutable row is never rewritten"
                );
                assert_eq!(
                    serde_json::to_vec(retained.record.value.as_ref()).unwrap(),
                    serde_json::to_vec(expected.value.as_ref()).unwrap(),
                    "the actual native row remains original"
                );
                let repeated = native.load(expected.key).await.unwrap().unwrap();
                assert_eq!(repeated.revision, retained.revision);
                assert_eq!(
                    serde_json::to_vec(repeated.record.value.as_ref()).unwrap(),
                    serde_json::to_vec(retained.record.value.as_ref()).unwrap()
                );
                if evidence.contradicts() {
                    assert!(
                        matches!(
                            observed,
                            Err(AppError::Integration(_) | AppError::Conflict(_))
                        ),
                        "{evidence:?}: changed immutable evidence must reject success: {observed:?}"
                    );
                } else {
                    observed.expect("unchanged normalized evidence must settle successfully");
                }
            })
            .await
            .expect("ordered receipt observation is bounded");
        })
        .catch_unwind()
        .await;
        faults.ordered_receipt.lock().unwrap().take();
        faults.receipt_obscured.store(false, Ordering::SeqCst);
        let cleanup = tokio::time::timeout(
            Duration::from_secs(12),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(10),
            }),
        )
        .await;
        drop(app);
        if let Err(panic) = body {
            std::panic::resume_unwind(panic);
        }
        // Shutdown publishes live Closed and joins its supervisor before
        // returning the retained admission failure or successful checkpoint drain.
        match cleanup.expect("ordered receipt cleanup is bounded and joined") {
            Err(AppError::Integration(message)) if initial && evidence.contradicts() => assert_eq!(
                message, "run admission cleanup authority failed",
                "unproven initial admission retains its original fatal authority failure"
            ),
            Ok(()) if !initial || !evidence.contradicts() => {}
            outcome => {
                panic!("unexpected ordered receipt cleanup for initial={initial}: {outcome:?}")
            }
        }
    }

    async fn wait_for_agent_intents(&self, expected: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let observed = self.agent_intents_observed.notified();
                tokio::pin!(observed);
                observed.as_mut().enable();
                if self.agent_intents.load(Ordering::SeqCst) >= expected {
                    return;
                }
                observed.await;
            }
        })
        .await
        .expect("both run-specific Agent intents must commit before the preparation assertion");
    }

    async fn open() -> Result<(Arc<Self>, FaultStateStore), StateStoreError> {
        let inner =
            StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
        let faults = Arc::new(Self {
            observations: Mutex::new(None),
            attempt_read: Mutex::new(None),
            attempt_owner: Mutex::new(None),
            attempt_read_records: Mutex::new(None),
            attempt_read_entered: Notify::new(),
            attempt_read_release: Notify::new(),
            attempt_read_dropped: Mutex::new(None),
            sibling_admission: Mutex::new(None),
            sibling_sequence: AtomicUsize::new(0),
            inventory_page: Mutex::new(InventoryPageFault::None),
            inventory_page_reads: AtomicUsize::new(0),
            preparation: Mutex::new(StatePreparationFault::None),
            preparation_entered: Notify::new(),
            agent_intents: AtomicUsize::new(0),
            agent_intents_observed: Notify::new(),
            app_load_panics: AtomicUsize::new(0),
            app_load_panic_count: AtomicUsize::new(0),
            notification_corruption: Mutex::new(NotificationCorruption::None),
            migration_fault: Mutex::new(MigrationFault::None),
            settlement_rollback_fault: Mutex::new(None),
            admission_panics: AtomicBool::new(false),
            admission_armed: AtomicBool::new(false),
            admission_committed: Notify::new(),
            admission_release: Notify::new(),
            admission_attempts: Mutex::new(None),
            admission_commit_holds: AtomicUsize::new(0),
            admission_commit_entered: Notify::new(),
            admission_commit_release: Notify::new(),
            admission_inventory_requested: AtomicBool::new(false),
            admission_inventory_armed: AtomicBool::new(false),
            admission_inventory_observed: Notify::new(),
            admission_inventory_release: Notify::new(),
            run_transition_monitor: AtomicBool::new(false),
            run_transition_gate_armed: AtomicBool::new(false),
            run_transition_observed: Notify::new(),
            run_transition_release: Notify::new(),
            active_run_transitions: AtomicUsize::new(0),
            maximum_active_run_transitions: AtomicUsize::new(0),
            app_store_admission_monitor: AtomicBool::new(false),
            app_store_mutation_boundary: AtomicBool::new(false),
            monitored_mutations: AtomicUsize::new(0),
            monitored_mutation_conflicts: AtomicUsize::new(0),
            app_store_admission_scope: Mutex::new(AppStoreAdmissionScope::Any),
            transaction_begin_failures: AtomicUsize::new(0),
            mutation_admission_failures: AtomicUsize::new(0),
            workspace_admission_armed: AtomicBool::new(false),
            workspace_admission_committed: Notify::new(),
            workspace_admission_release: Notify::new(),
            workspace_observation_load_armed: AtomicBool::new(false),
            workspace_observation_load_faulted: Notify::new(),
            receipt_obscured: AtomicBool::new(false),
            ordered_receipt: Mutex::new(None),
            ordered_receipt_reads: AtomicUsize::new(0),
            checkpoint_evidence: Mutex::new(CheckpointEvidence::StateOrder),
            predecessor_evidence: Mutex::new(None),
            predecessor_reads: AtomicUsize::new(0),
            receipt_page_values: Mutex::new(None),
            receipt_page_peak: AtomicUsize::new(0),
            receipt_writes: AtomicUsize::new(0),
            receipt_cleanup_batches: Mutex::new(Vec::new()),
            reject_receipt_cleanup: AtomicBool::new(false),
            ambiguity_observed: Notify::new(),
            sleep_fault_requested: AtomicBool::new(false),
            sleep_load_armed: AtomicBool::new(false),
            sleep_load_faulted: AtomicBool::new(false),
            sleep_load_observed: Notify::new(),
            sleep_integration_requested: AtomicUsize::new(0),
            sleep_integration_armed: AtomicUsize::new(0),
            sleep_integration_attempts: AtomicUsize::new(0),
            sleep_integration_notify_at: AtomicUsize::new(0),
            excessive_integration_attempts: Notify::new(),
            scripted_sleep_failures: Mutex::new(VecDeque::new()),
            active_sleep_failure: Mutex::new(None),
            scripted_sleep_attempts: Mutex::new(Vec::new()),
            second_sleep_failure_observed: Notify::new(),
            second_sleep_recovery_observed: Notify::new(),
            shutdown_scan_armed: AtomicBool::new(false),
            shutdown_scan_observed: Notify::new(),
            shutdown_scan_release: Notify::new(),
            preclosed_load_armed: AtomicBool::new(false),
            preclosed_load_observed: Notify::new(),
            preclosed_load_release: Notify::new(),
            cancelling_publications: AtomicUsize::new(0),
        });
        let store = FaultStateStore {
            inner,
            faults: Arc::clone(&faults),
        };
        Ok((faults, store))
    }

    fn arm_admission(&self) {
        self.admission_armed.store(true, Ordering::SeqCst);
    }

    fn observe_admission_commits(&self, held: usize) {
        *self.admission_attempts.lock().unwrap() = Some(Vec::new());
        self.admission_commit_holds.store(held, Ordering::SeqCst);
    }

    fn release_admission_commit(&self) {
        self.admission_commit_release.notify_one();
    }

    fn release_admission(&self) {
        self.admission_release.notify_one();
    }

    fn arm_admission_inventory(&self) {
        self.admission_inventory_requested
            .store(true, Ordering::SeqCst);
    }

    fn release_admission_inventory(&self) {
        self.admission_inventory_release.notify_one();
    }

    fn arm_run_transition_monitor(&self) {
        self.run_transition_gate_armed.store(true, Ordering::SeqCst);
        self.run_transition_monitor.store(true, Ordering::SeqCst);
    }

    fn release_run_transition(&self) {
        self.run_transition_release.notify_one();
    }

    fn arm_app_store_admission_monitor(&self) {
        self.app_store_mutation_boundary
            .store(false, Ordering::SeqCst);
        self.run_transition_gate_armed.store(true, Ordering::SeqCst);
        self.app_store_admission_monitor
            .store(true, Ordering::SeqCst);
    }

    fn arm_app_store_mutation_monitor(&self) {
        self.app_store_mutation_boundary
            .store(true, Ordering::SeqCst);
        self.run_transition_gate_armed.store(true, Ordering::SeqCst);
        self.app_store_admission_monitor
            .store(true, Ordering::SeqCst);
    }

    async fn observe_app_store_admission(&self) -> bool {
        let active = self.active_run_transitions.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum_active_run_transitions
            .fetch_max(active, Ordering::SeqCst);
        let selected = {
            let mut scope = self.app_store_admission_scope.lock().unwrap();
            match &*scope {
                AppStoreAdmissionScope::Any => true,
                AppStoreAdmissionScope::Capture => {
                    *scope = AppStoreAdmissionScope::Owned(
                        tokio::task::try_id().expect("the native publication has a Tokio owner"),
                    );
                    true
                }
                AppStoreAdmissionScope::Owned(owner) => tokio::task::try_id() == Some(*owner),
            }
        };
        if selected && self.run_transition_gate_armed.swap(false, Ordering::SeqCst) {
            self.run_transition_observed.notify_one();
            self.run_transition_release.notified().await;
        }
        selected
    }

    fn release_app_store_admission(&self) {
        self.run_transition_release.notify_one();
    }

    fn fail_transaction_begins(&self, attempts: usize) {
        self.transaction_begin_failures
            .store(attempts, Ordering::SeqCst);
    }

    fn fail_mutation_admissions(&self, attempts: usize) {
        self.mutation_admission_failures
            .store(attempts, Ordering::SeqCst);
    }

    fn arm_workspace_admission(&self) {
        self.workspace_admission_armed.store(true, Ordering::SeqCst);
    }

    fn release_workspace_admission(&self) {
        self.workspace_admission_release.notify_one();
    }

    fn arm_sleep_load(&self) {
        self.sleep_fault_requested.store(true, Ordering::SeqCst);
    }

    fn arm_sleep_integrations(&self, attempts: usize) {
        self.sleep_integration_notify_at.store(
            if attempts == usize::MAX {
                10
            } else if attempts > 1 {
                attempts
            } else {
                0
            },
            Ordering::SeqCst,
        );
        self.sleep_integration_requested
            .store(attempts, Ordering::SeqCst);
    }

    fn disarm_sleep_integration(&self) {
        self.sleep_integration_armed.store(0, Ordering::SeqCst);
    }

    fn script_sleep_failures(&self, failures: impl IntoIterator<Item = usize>) {
        *self
            .scripted_sleep_failures
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = failures.into_iter().collect();
        *self
            .active_sleep_failure
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        self.scripted_sleep_attempts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
    }

    fn scripted_sleep_attempts(&self) -> Vec<usize> {
        self.scripted_sleep_attempts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn arm_shutdown_scan(&self) {
        self.shutdown_scan_armed.store(true, Ordering::SeqCst);
    }

    fn release_shutdown_scan(&self) {
        self.shutdown_scan_release.notify_one();
    }

    fn arm_preclosed_load(&self) {
        self.preclosed_load_armed.store(true, Ordering::SeqCst);
    }

    fn release_preclosed_load(&self) {
        self.preclosed_load_release.notify_one();
    }
}

impl NotificationSequence {
    fn new(count: usize, redeliver: bool) -> Arc<Self> {
        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        Arc::new(Self {
            count,
            redeliver,
            emission: Mutex::new(NotificationEmission::Sequential),
            encoded_body_bytes: Mutex::new(Vec::new()),
            emitted: AtomicUsize::new(0),
            received: AtomicUsize::new(0),
            prompt_returns: AtomicUsize::new(0),
            prompt_error: Mutex::new(None),
            emission_completed: Notify::new(),
            next_target_entered: Notify::new(),
            next_target_release: Notify::new(),
            resolution_fault: Mutex::new(NotificationResolutionFault::Pass),
            resolution_entered: Notify::new(),
            resolution_release: Notify::new(),
            resolution_calls: AtomicUsize::new(0),
            resolution_returns: AtomicUsize::new(0),
            stall_returned: AtomicBool::new(false),
            sender,
            receiver: Mutex::new(Some(receiver)),
            resolved: Mutex::new(Vec::new()),
            changed: Notify::new(),
            cancelled: AtomicBool::new(false),
        })
    }

    fn id(index: usize) -> NotificationId {
        format!("notification_{:032x}", index + 81)
            .parse()
            .expect("test notification identity")
    }

    fn body(bytes: usize) -> String {
        assert!((1..=393_181).contains(&bytes));
        // U+0001 is valid nonblank portable text, encoded as six JSON bytes.
        // ASCII fills the remainder for one-byte adjacency.
        let body = format!(
            "x{}{}",
            "\u{1}".repeat((bytes - 1) / 6),
            "a".repeat((bytes - 1) % 6)
        );
        assert!(
            body.len() <= 65_536,
            "every decoded body obeys the portable text bound"
        );
        assert_eq!(
            serde_json::to_vec(&body).expect("test body encoding").len(),
            bytes + 2
        );
        body
    }

    fn receiver(
        self: &Arc<Self>,
        read_authority: Option<(Arc<StorageFaults>, AttemptReadCase)>,
    ) -> Result<ConversationNotificationReceiver, SdkError> {
        let receiver = self
            .receiver
            .lock()
            .expect("test receiver lock")
            .take()
            .ok_or_else(|| SdkError::busy("test notification receiver is exclusive"))?;
        Ok(ConversationNotificationReceiver::from_stream(
            futures_util::stream::unfold(
                (receiver, Arc::clone(self), read_authority),
                |(mut receiver, sequence, mut read_authority)| async move {
                    receiver.recv().await.map(|item| {
                        sequence.received.fetch_add(1, Ordering::SeqCst);
                        if item.as_ref().is_ok_and(|notification| {
                            matches!(
                                notification.kind(),
                                ConversationNotificationKind::Interaction { .. }
                            )
                        }) && let Some((faults, case)) = read_authority.take()
                        {
                            // Arm only as this original lower receiver yields
                            // its real notification to the retained App owner.
                            *faults.attempt_owner.lock().unwrap() = Some(tokio::task::id());
                            if !matches!(case, AttemptReadCase::ResolutionObservation { .. }) {
                                *faults.attempt_read.lock().unwrap() = Some(case);
                            }
                        }
                        (item, (receiver, sequence, read_authority))
                    })
                },
            ),
        ))
    }

    async fn prompt(&self) -> Result<PromptResponse, SdkError> {
        let emission = *self.emission.lock().expect("test emission lock");
        for index in 0..self.count {
            if (index == 1 && matches!(emission, NotificationEmission::GateAfterFirst))
                || (index + 1 == self.count
                    && matches!(emission, NotificationEmission::GateBeforeLast))
            {
                self.next_target_entered.notify_one();
                self.next_target_release.notified().await;
                if self.cancelled.load(Ordering::SeqCst) {
                    return Err(SdkError::cancelled("test prompt aborted"));
                }
            }
            let id = Self::id(index);
            let encoded = self
                .encoded_body_bytes
                .lock()
                .expect("test body sizes")
                .get(index)
                .copied();
            let body = match encoded {
                None => "Allow the next test action".to_owned(),
                Some(bytes) => Self::body(bytes),
            };
            let notification = ConversationNotification::new(
                id.clone(),
                std::time::SystemTime::UNIX_EPOCH,
                ConversationNotificationKind::Interaction {
                    title: format!("Approval {}", index + 1),
                    body,
                    response_schema: None,
                    dismissible: false,
                },
            )?;
            if self.redeliver {
                self.sender
                    .send(Ok(notification.clone()))
                    .await
                    .map_err(|_| SdkError::closed("test receiver closed"))?;
            }
            self.sender
                .send(Ok(notification.clone()))
                .await
                .map_err(|_| SdkError::closed("test receiver closed"))?;
            self.emitted.fetch_add(1, Ordering::SeqCst);
            if !matches!(emission, NotificationEmission::Burst) {
                self.wait_for_resolution(&id).await?;
            }
            if self.redeliver {
                self.sender
                    .send(Ok(notification))
                    .await
                    .map_err(|_| SdkError::closed("test receiver closed"))?;
            }
        }
        self.emission_completed.notify_one();
        if self.count > 0 && matches!(emission, NotificationEmission::Burst) {
            self.wait_for_resolution(&Self::id(self.count - 1)).await?;
        }
        self.prompt_returns.fetch_add(1, Ordering::SeqCst);
        if let Some(kind) = *self.prompt_error.lock().unwrap() {
            return Err(RetryFailure::AfterEffect(kind).error());
        }
        Ok(PromptResponse::new(
            "both approvals accepted".to_owned(),
            None,
        ))
    }

    async fn wait_for_resolution(&self, id: &NotificationId) -> Result<(), SdkError> {
        loop {
            if self.cancelled.load(Ordering::SeqCst) {
                return Err(SdkError::cancelled("test prompt aborted"));
            }
            if self
                .resolved
                .lock()
                .expect("test resolution lock")
                // resolve accepts only the next fixed-width, ordered identity.
                // Keep the complete vector for exact receipt assertions without
                // rescanning every prior delivery on each prompt wake.
                .binary_search(id)
                .is_ok()
            {
                return Ok(());
            }
            self.changed.notified().await;
        }
    }

    async fn resolve(
        &self,
        id: NotificationId,
        resolution: NotificationResolution,
    ) -> Result<(), SdkError> {
        self.resolution_calls.fetch_add(1, Ordering::SeqCst);
        let fault = std::mem::replace(
            &mut *self.resolution_fault.lock().expect("test resolution fault"),
            NotificationResolutionFault::Pass,
        );
        match fault {
            NotificationResolutionFault::Stall => {
                self.resolution_entered.notify_one();
                self.resolution_release.notified().await;
                self.stall_returned.store(true, Ordering::SeqCst);
                return Err(SdkError::integration(
                    "test unresolved native call released externally",
                ));
            }
            NotificationResolutionFault::Panic => {
                self.resolution_entered.notify_one();
                panic!("injected notification resolution panic");
            }
            _ => {}
        }
        if resolution != NotificationResolution::Allow {
            return Err(SdkError::invalid_input("test requires Allow"));
        }
        {
            let mut resolved = self.resolved.lock().expect("test resolution lock");
            if id != Self::id(resolved.len()) {
                return Err(SdkError::conflict(
                    "test notification target is not current",
                ));
            }
            resolved.push(id);
        }
        self.changed.notify_one();
        let result = match fault {
            NotificationResolutionFault::LostReceipt
            | NotificationResolutionFault::GateLostReceipt => {
                self.resolution_entered.notify_one();
                if matches!(fault, NotificationResolutionFault::GateLostReceipt) {
                    self.resolution_release.notified().await;
                }
                Err(SdkError::integration(
                    "test lost accepted notification receipt",
                ))
            }
            NotificationResolutionFault::GateReceipt => {
                self.resolution_entered.notify_one();
                self.resolution_release.notified().await;
                Ok(())
            }
            _ => Ok(()),
        };
        if result.is_ok() {
            self.resolution_returns.fetch_add(1, Ordering::SeqCst);
        }
        result
    }
}

impl StreamFailureState {
    fn notification_peer_definition(include_input: bool) -> WorkflowDefinition {
        let mut source = serde_json::json!({
            "schema":"app-sdk.workflow/v2", "id":"notification-peers", "revision":"v1",
            "state":{"left":"before", "right":"before"}, "required":["left", "right"],
            "reducers":{"left":"reject_concurrent", "right":"reject_concurrent"},
            "entry":"seed", "nodes":[
                {"id":"seed", "step":{"kind":"state", "mutations":[]}},
                {"id":"a-agent", "deadline_ms":12000, "step":{"kind":"agent", "agent":"left",
                    "prompt":[{"kind":"text", "text":"left prompt"}], "text_target":"left"}},
                {"id":"z-agent", "deadline_ms":12000, "step":{"kind":"agent", "agent":"right",
                    "prompt":[{"kind":"text", "text":"right prompt"}], "text_target":"right"}}
            ], "routes":{"seed":{"mode":"broadcast", "branches":[
                {"target":{"kind":"node", "id":"z-agent"}},
                {"target":{"kind":"node", "id":"a-agent"}}
            ]}}
        });
        if include_input {
            source["state"]["inputs"] = serde_json::json!([]);
            source["required"]
                .as_array_mut()
                .unwrap()
                .push(Value::from("inputs"));
            source["reducers"]["inputs"] = Value::from("collect");
            source["nodes"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({
                    "id":"b-input", "step":{"kind":"input", "target":"inputs",
                        "message":[{"kind":"text", "text":"remaining input"}]}
                }));
            source["routes"]["seed"]["branches"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({"target":{"kind":"node", "id":"b-input"}}));
        }
        WorkflowDefinition::parse(&source.to_string()).unwrap()
    }

    async fn assert_mixed_controls(foreground: bool) {
        let (left, left_agent) = Self::open().unwrap();
        let (right, right_agent) = Self::open().unwrap();
        let left_notifications = NotificationSequence::new(1, false);
        let right_notifications = NotificationSequence::new(1, false);
        *left.notifications.lock().unwrap() = Some(Arc::clone(&left_notifications));
        *right.notifications.lock().unwrap() = Some(Arc::clone(&right_notifications));
        let definition = WorkflowDefinition::parse(&serde_json::json!({
            "schema":"app-sdk.workflow/v2", "id":"mixed-controls", "revision":"v1",
            "state":{"left":"before", "right":"before", "inputs":[]},
            "required":["left", "right", "inputs"],
            "reducers":{"left":"reject_concurrent", "right":"reject_concurrent", "inputs":"collect"},
            "entry":"seed", "nodes":[
                {"id":"seed", "step":{"kind":"state", "mutations":[]}},
                {"id":"a-agent", "deadline_ms":12000, "step":{"kind":"agent", "agent":"left",
                    "prompt":[{"kind":"text", "text":"left prompt"}], "text_target":"left"}},
                {"id":"b-input", "step":{"kind":"input", "target":"inputs",
                    "message":[{"kind":"text", "text":"left input"}]}},
                {"id":"c-sleep", "deadline_ms":2000,
                    "step":{"kind":"sleep", "message":"first timer", "delay_ms":20}},
                {"id":"d-sleep", "deadline_ms":2500,
                    "step":{"kind":"sleep", "message":"second timer", "delay_ms":40}},
                {"id":"e-input", "step":{"kind":"input", "target":"inputs",
                    "message":[{"kind":"text", "text":"right input"}]}},
                {"id":"z-agent", "deadline_ms":12000, "step":{"kind":"agent", "agent":"right",
                    "prompt":[{"kind":"text", "text":"right prompt"}], "text_target":"right"}}
            ], "routes":{"seed":{"mode":"broadcast", "branches":[
                {"target":{"kind":"node", "id":"z-agent"}},
                {"target":{"kind":"node", "id":"e-input"}},
                {"target":{"kind":"node", "id":"d-sleep"}},
                {"target":{"kind":"node", "id":"c-sleep"}},
                {"target":{"kind":"node", "id":"b-input"}},
                {"target":{"kind":"node", "id":"a-agent"}}
            ]}}
        }).to_string()).unwrap();
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let mut app = tokio::time::timeout(
            Duration::from_secs(3),
            App::open(
                AppConfig::new(
                    store,
                    vec![
                        AgentSpec::new("left".into(), "v1".into(), left_agent),
                        AgentSpec::new("right".into(), "v1".into(), right_agent),
                    ],
                    vec![definition],
                )
                .with_hooks(left.agent()),
            ),
        )
        .await
        .expect("mixed App admission is bounded")
        .unwrap();
        let checked = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(18), async {
            let workspace = app.create_workspace(WorkspaceConfig {
                name: None, root: std::env::current_dir().unwrap(),
            }).await?;
            let workflow = workspace.workflow("mixed-controls".parse().unwrap()).await?;
            let mut run = if foreground {
                workflow.prepare(WorkflowState::default()).await?
            } else {
                workflow.start(ExecutionOptions::default()).await?
            };
            // Public resolution remains available while the original caller
            // execution holds its one exclusive mutable run handle.
            let observer = workflow.open_run(RunSelector::Latest(run.id().clone())).await?;
            let mut execution = if foreground {
                Some(run.execution(ExecutionOptions::default()).await?)
            } else { None };
            let ready = tokio::time::timeout(Duration::from_secs(4), async {
                let driving = async {
                    if let Some(execution) = execution.as_mut() {
                        while execution.next().await?.is_some() {}
                    }
                    std::future::pending::<Result<(), AppError>>().await
                };
                tokio::pin!(driving);
                loop {
                    let snapshot = tokio::select! {
                        result = &mut driving => {
                            result?;
                            unreachable!("the observation driver never completes independently")
                        }
                        snapshot = observer.snapshot() => snapshot?,
                    };
                    assert!(!matches!(snapshot.status, RunStatus::Failed(_) | RunStatus::Succeeded),
                        "one native sibling cannot decide the incomplete ready wave");
                    let contexts = left.step_contexts.lock().unwrap().clone();
                    if snapshot.interactions.len() == 4 && contexts.len() == 7
                        && left.prompts.load(Ordering::SeqCst) == 1
                        && right.prompts.load(Ordering::SeqCst) == 1
                    {
                        break Ok::<_, AppError>(snapshot);
                    }
                    tokio::task::yield_now().await;
                }
            }).await.expect("both Agents, Inputs and Sleeps enter before any answer")?;
            let contexts = left.step_contexts.lock().unwrap().clone();
            assert_eq!(contexts.iter().map(|context|
                (context.node.as_str(), context.action_ordinal)).collect::<Vec<_>>(),
                [("seed", 0), ("a-agent", 1), ("b-input", 2), ("c-sleep", 3),
                 ("d-sleep", 4), ("e-input", 5), ("z-agent", 6)]);
            assert!(contexts.iter().all(|context| matches!(context.mode, ActionMode::Asynchronous)));
            assert!(contexts[1..].iter().all(|context| context.checkpoint == contexts[1].checkpoint));
            assert_eq!(left_notifications.emitted.load(Ordering::SeqCst), 1);
            assert_eq!(right_notifications.emitted.load(Ordering::SeqCst), 1);
            assert!(matches!(&ready.interactions[0], Interaction::Notification { .. }));
            assert!(matches!(&ready.interactions[1], Interaction::Input { message, .. } if message == "left input"));
            assert!(matches!(&ready.interactions[2], Interaction::Input { message, .. } if message == "right input"));
            assert!(matches!(&ready.interactions[3], Interaction::Notification { .. }));
            assert_eq!(ready.last_action_ordinal, Some(0));
            assert_eq!(ready.outcomes.len(), 1);
            assert_eq!(ready.outcomes[0].node, "seed");
            assert_eq!(ready.state.values(), &Map::from_iter([
                ("left".into(), Value::from("before")),
                ("right".into(), Value::from("before")),
                ("inputs".into(), serde_json::json!([])),
            ]));

            // Both timers started with their original deadlines. Accepted
            // native work stays supervised while the caller stops observing
            // this execution and the real prompts and Inputs remain held.
            // No private intermediate Stage representation is asserted.
            tokio::time::sleep(Duration::from_secs(3)).await;
            let held = observer.snapshot().await?;
            assert!(!matches!(held.status, RunStatus::Failed(_) | RunStatus::Succeeded));
            Self::assert_same_interactions(&held.interactions, &ready.interactions);
            assert_eq!(held.state, ready.state);
            assert_eq!(held.last_action_ordinal, ready.last_action_ordinal);
            assert!(held.outcomes == ready.outcomes);
            let mut remaining = ready.interactions.clone();
            let mut first_applied = None;
            while let Some(target) = remaining.pop() {
                let (sequence, generation, resolution) = match target {
                    Interaction::Input { sequence, generation, message, .. } =>
                        (sequence, generation, Resolution::Input(Value::from(message))),
                    Interaction::Notification { sequence, generation, .. } =>
                        (sequence, generation, Resolution::Notification(NotificationResolution::Allow)),
                    _ => panic!("the admitted targets remain Inputs and Notifications"),
                };
                let applied = observer.resolve(sequence, generation, resolution).await?;
                if first_applied.is_none() {
                    first_applied = Some((sequence, generation, applied));
                }
                if !remaining.is_empty() {
                    let snapshot = observer.snapshot().await?;
                    Self::assert_same_interactions(&snapshot.interactions, &remaining);
                    assert_eq!(snapshot.state, ready.state);
                    assert_eq!(snapshot.last_action_ordinal, ready.last_action_ordinal);
                    assert!(snapshot.outcomes == ready.outcomes);
                    let left_resolved = left_notifications.resolved.lock().unwrap().clone();
                    let right_resolved = right_notifications.resolved.lock().unwrap().clone();
                    assert!(left_resolved.is_empty(), "z-side settlement cannot resolve a-side approval");
                    assert_eq!(right_resolved, [NotificationSequence::id(0)]);
                }
            }
            let terminal = match execution {
                Some(execution) => execution.finish().await?,
                None => observer.wait().await?,
            };
            assert!(matches!(terminal.status, RunStatus::Succeeded));
            assert!(terminal.interactions.is_empty());
            assert_eq!(terminal.state.values()["inputs"], serde_json::json!(["left input", "right input"]));
            assert_eq!(terminal.state.values()["left"], "both approvals accepted");
            assert_eq!(terminal.state.values()["right"], "both approvals accepted");
            assert_eq!(terminal.last_action_ordinal, Some(6));
            assert_eq!(terminal.outcomes.iter().map(|outcome|
                (outcome.node.as_str(), outcome.ordinal)).collect::<Vec<_>>(),
                [("a-agent", 1), ("b-input", 2), ("c-sleep", 3),
                 ("d-sleep", 4), ("e-input", 5), ("z-agent", 6)]);
            let (sequence, generation, applied) = first_applied.unwrap();
            let replay = observer.resolve(sequence, generation,
                Resolution::Notification(NotificationResolution::Allow)).await?;
            Self::assert_applied_snapshot(&replay, &applied);
            for (state, notifications) in [(&left, &left_notifications), (&right, &right_notifications)] {
                assert_eq!(state.conversations.load(Ordering::SeqCst), 1);
                assert_eq!(state.prompts.load(Ordering::SeqCst), 1);
                assert_eq!(state.streams.load(Ordering::SeqCst), 1);
                let resolved = notifications.resolved.lock().unwrap().clone();
                assert_eq!(resolved, [NotificationSequence::id(0)]);
            }
            Ok::<_, AppError>(())
        })).catch_unwind().await;
        for notifications in [&left_notifications, &right_notifications] {
            notifications.cancelled.store(true, Ordering::SeqCst);
            notifications.changed.notify_waiters();
            notifications.resolution_release.notify_one();
            notifications.next_target_release.notify_one();
        }
        let cleanup = std::panic::AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        ))
        .catch_unwind()
        .await;
        drop(app);
        match checked {
            Ok(result) => result
                .expect("mixed sibling execution stays bounded")
                .unwrap(),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup
            .expect("mixed sibling cleanup does not panic")
            .expect("mixed sibling cleanup is bounded")
            .unwrap();
        assert_eq!(left.shutdowns.load(Ordering::SeqCst), 1);
        assert_eq!(right.shutdowns.load(Ordering::SeqCst), 1);
    }

    fn assert_same_interactions(actual: &[Interaction], expected: &[Interaction]) {
        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.iter().zip(expected) {
            match (actual, expected) {
                (
                    Interaction::Input {
                        sequence,
                        generation,
                        message,
                        schema,
                    },
                    Interaction::Input {
                        sequence: old_sequence,
                        generation: old_generation,
                        message: old_message,
                        schema: old_schema,
                    },
                ) => {
                    assert_eq!(
                        (sequence, generation, message, schema),
                        (old_sequence, old_generation, old_message, old_schema)
                    );
                }
                (
                    Interaction::Notification {
                        sequence,
                        generation,
                        notification,
                    },
                    Interaction::Notification {
                        sequence: old_sequence,
                        generation: old_generation,
                        notification: old_notification,
                    },
                ) => {
                    assert_eq!((sequence, generation), (old_sequence, old_generation));
                    assert_eq!(notification.id(), old_notification.id());
                    assert_eq!(notification.inserted_at(), old_notification.inserted_at());
                    assert_eq!(notification.kind(), old_notification.kind());
                }
                (
                    Interaction::Unknown {
                        sequence,
                        generation,
                        retry_safe,
                    },
                    Interaction::Unknown {
                        sequence: old_sequence,
                        generation: old_generation,
                        retry_safe: old_retry_safe,
                    },
                ) => assert_eq!(
                    (sequence, generation, retry_safe),
                    (old_sequence, old_generation, old_retry_safe)
                ),
                _ => panic!(
                    "this fixture retains only its exact Input, Notification and Unknown targets"
                ),
            }
        }
    }

    fn assert_applied_snapshot(actual: &RunSnapshot, expected: &RunSnapshot) {
        assert_eq!(actual.id, expected.id);
        assert_eq!(actual.checkpoint, expected.checkpoint);
        assert_eq!(actual.state, expected.state);
        assert_eq!(actual.last_action_ordinal, expected.last_action_ordinal);
        assert!(actual.outcomes == expected.outcomes);
        assert!(
            matches!(
                (&actual.status, &expected.status),
                (RunStatus::Running, RunStatus::Running)
                    | (RunStatus::Paused, RunStatus::Paused)
                    | (RunStatus::Succeeded, RunStatus::Succeeded)
            ),
            "an applied receipt retains its original lifecycle, not a later run result"
        );
        Self::assert_same_interactions(&actual.interactions, &expected.interactions);
    }

    fn exclusive_workspace(
        gate_first_creation: bool,
    ) -> Result<(Arc<Self>, StreamFailureAgent), SdkError> {
        let (state, agent) = Self::open()?;
        *state
            .preparation_fault
            .lock()
            .expect("preparation fault lock") = PreparationFault::ExclusiveWorkspace {
            gate_first_creation,
        };
        Ok((state, agent))
    }

    fn preparation_definition(
        agent: &str,
        id: &str,
        deadline_millis: u64,
    ) -> Result<WorkflowDefinition, AppError> {
        let mut source: Value = serde_json::from_str(STREAM_FAILURE)
            .expect("the existing fixture workflow is valid JSON");
        source["id"] = Value::from(id);
        source["nodes"][0]["deadline_ms"] = Value::from(deadline_millis);
        source["nodes"][0]["step"]["agent"] = Value::from(agent);
        WorkflowDefinition::parse(&source.to_string())
    }

    async fn assert_queued_preparation(
        outcome: QueuedPreparation,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let (state, agent) = Self::exclusive_workspace(true)?;
        let (faults, store) = StorageFaults::open().await?;
        let mut app = App::open(AppConfig::new(
            store,
            vec![AgentSpec::new("faulty".to_owned(), "v1".to_owned(), agent)],
            vec![
                Self::preparation_definition("faulty", "lease-long", 5000)?,
                Self::preparation_definition("faulty", "lease-expiry", 500)?,
            ],
        ))
        .await?;
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: Some("concurrent-preparation".to_owned()),
                root: std::env::current_dir()?,
            })
            .await?;
        assert!(!state.workspace_claimed.load(Ordering::SeqCst));
        let workflow = workspace.workflow("lease-long".parse()?).await?;
        let first = workflow.start(ExecutionOptions::default()).await?;
        tokio::time::timeout(Duration::from_secs(2), state.preparation_entered.notified())
            .await
            .expect("the first run must hold the exact lower Workspace lease during creation");
        let second = match outcome {
            QueuedPreparation::Expire => {
                workspace
                    .workflow("lease-expiry".parse()?)
                    .await?
                    .start(ExecutionOptions::default())
                    .await?
            }
            QueuedPreparation::Complete | QueuedPreparation::Cancel => {
                workflow.start(ExecutionOptions::default()).await?
            }
        };
        assert_ne!(first.id(), second.id());
        faults.wait_for_agent_intents(2).await;
        let pending = second.snapshot().await?;
        assert!(
            matches!(pending.status, RunStatus::Running),
            "contention on the same Agent workspace must remain queued before effects"
        );
        assert_eq!(state.conversations.load(Ordering::SeqCst), 0);
        assert_eq!(state.prompts.load(Ordering::SeqCst), 0);
        match outcome {
            QueuedPreparation::Complete => {
                assert!(
                    tokio::time::timeout(Duration::from_millis(100), second.wait())
                        .await
                        .is_err(),
                    "a second run must not fail while another App-owned preparation holds the lease"
                );
            }
            QueuedPreparation::Cancel => {
                second.cancel(pending.checkpoint).await?;
                let cancelled =
                    tokio::time::timeout(Duration::from_secs(2), second.wait()).await??;
                assert!(
                    matches!(cancelled.status, RunStatus::Cancelled),
                    "queued pre-effect cancellation must settle while the first lease is held"
                );
            }
            QueuedPreparation::Expire => {
                let expired = tokio::time::timeout(Duration::from_secs(2), second.wait()).await??;
                assert!(
                    matches!(
                        expired.status,
                        RunStatus::Failed(WorkflowFailure::Timeout(_))
                    ),
                    "the sampled activation deadline must cover waiting for the same lower lease"
                );
            }
        }
        assert_eq!(state.conversations.load(Ordering::SeqCst), 0);
        assert_eq!(state.prompts.load(Ordering::SeqCst), 0);
        state.preparation_release.notify_one();
        let first_done = tokio::time::timeout(Duration::from_secs(2), first.wait()).await??;
        assert!(matches!(first_done.status, RunStatus::Succeeded));
        assert_eq!(first_done.state.values()["answer"], Value::from("respond"));
        if matches!(outcome, QueuedPreparation::Complete) {
            let second_done = tokio::time::timeout(Duration::from_secs(2), second.wait()).await??;
            assert!(matches!(second_done.status, RunStatus::Succeeded));
            assert_eq!(second_done.state.values()["answer"], Value::from("respond"));
        }
        app.shutdown(Shutdown::Drain {
            timeout: Duration::from_secs(5),
        })
        .await?;
        let expected = if matches!(outcome, QueuedPreparation::Complete) {
            2
        } else {
            1
        };
        assert_eq!(
            state.conversations.load(Ordering::SeqCst),
            expected,
            "canceled or expired queue entries must not create late conversations"
        );
        assert_eq!(
            state.prompts.load(Ordering::SeqCst),
            expected,
            "only admitted live preparations may submit their prompts"
        );
        assert_eq!(
            state
                .conversation_inventory
                .lock()
                .expect("test conversation inventory lock")
                .len(),
            expected
        );
        assert!(!state.workspace_claimed.load(Ordering::SeqCst));
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
        Ok(())
    }

    async fn assert_timeout_survives_cancellation(grace_expired: bool) {
        let (state, agent) = Self::open().unwrap();
        let notifications = NotificationSequence::new(1, false);
        *state.notifications.lock().unwrap() = Some(Arc::clone(&notifications));
        *state.preparation_fault.lock().unwrap() = PreparationFault::AbortGate;
        let mut source: Value = serde_json::from_str(STREAM_FAILURE).unwrap();
        source["nodes"][0]["deadline_ms"] = Value::from(1_000);
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let mut app = App::open(AppConfig::new(
            store,
            vec![AgentSpec::new("faulty".into(), "v1".into(), agent)],
            vec![WorkflowDefinition::parse(&source.to_string()).unwrap()],
        ))
        .await
        .unwrap();
        let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(15), async {
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().unwrap(),
                })
                .await
                .unwrap();
            let workflow = workspace
                .workflow("stream-failure".parse().unwrap())
                .await
                .unwrap();
            let run = workflow.start(ExecutionOptions::default()).await.unwrap();
            let pending = run.wait().await.unwrap();
            assert!(matches!(
                pending.interactions.as_slice(),
                [Interaction::Notification { .. }]
            ));
            state.preparation_entered.notified().await;
            let failing = run.snapshot().await.unwrap();
            let cancellation = run.cancel(failing.checkpoint).await.unwrap();
            assert!(cancellation.checkpoint > failing.checkpoint);
            if grace_expired {
                let unknown = loop {
                    let current = run.snapshot().await.unwrap();
                    if matches!(
                        current.interactions.as_slice(),
                        [Interaction::Unknown { .. }]
                    ) {
                        break current;
                    }
                    tokio::task::yield_now().await;
                };
                let [
                    Interaction::Unknown {
                        sequence,
                        generation,
                        ..
                    },
                ] = unknown.interactions.as_slice()
                else {
                    unreachable!("the observed exact unknown target")
                };
                run.resolve(*sequence, *generation, Resolution::Fail)
                    .await
                    .unwrap();
            } else {
                state.preparation_release.notify_one();
            }
            // Snapshot may expose the definite abort receipt while its control
            // is being retired. The public wait observes that reconciliation;
            // an actionable Unknown instead remains a failing acknowledgement.
            let terminal = run.wait().await.unwrap();
            assert!(
                terminal.interactions.is_empty(),
                "acknowledged abort or explicit failure settles without another actionable target"
            );
            let RunStatus::Failed(WorkflowFailure::Timeout(message)) = terminal.status else {
                panic!("later cancellation preserves the original timeout category")
            };
            assert_eq!(message, "agent attempt deadline expired");
            assert_eq!(state.prompts.load(Ordering::SeqCst), 1);
            assert_eq!(state.aborts.load(Ordering::SeqCst), 1);
            assert_eq!(
                state.abort_returns.load(Ordering::SeqCst),
                usize::from(!grace_expired),
                "the acknowledged case requires the exact lower abort to return successfully"
            );
            assert!(terminal.outcomes.is_empty());
            assert_eq!(terminal.state, pending.state);
            if grace_expired {
                state.shutdown_armed.store(true, Ordering::SeqCst);
                assert!(
                    matches!(
                        app.shutdown(Shutdown::Drain {
                            timeout: Duration::from_millis(50)
                        })
                        .await,
                        Err(AppError::Timeout(_))
                    ),
                    "shutdown timeout ends observation while lower cleanup stays owned"
                );
                state.shutdown_observed.notified().await;
                state.shutdown_release.notify_one();
            }
            app.shutdown(Shutdown::Drain {
                timeout: Duration::from_secs(5),
            })
            .await
            .unwrap();
            assert_eq!(
                state.abort_returns.load(Ordering::SeqCst),
                usize::from(!grace_expired),
                "joining cleanup must not manufacture acknowledgement after abort observation expired"
            );
            assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
        }))
        .catch_unwind()
        .await;
        state.preparation_release.notify_one();
        state.shutdown_release.notify_one();
        let cleanup = tokio::time::timeout(
            Duration::from_secs(10),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(5),
            }),
        )
        .await;
        match observed {
            Ok(result) => result.expect("the original timeout and exact abort remain bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup
            .expect("retained runtime joins original abort cleanup")
            .unwrap();
    }

    fn open() -> Result<(Arc<Self>, StreamFailureAgent), SdkError> {
        let state = Arc::new(Self {
            notifications: Mutex::new(None),
            read_authority: Mutex::new(None),
            retry_failures: Mutex::new(None),
            retry_observed: Mutex::new(Vec::new()),
            step_contexts: Mutex::new(Vec::new()),
            preparation_fault: Mutex::new(PreparationFault::Pass),
            preparation_entered: Notify::new(),
            preparation_release: Notify::new(),
            workspace_claimed: AtomicBool::new(false),
            agent_id: "agent_00000000000000000000000000000071".parse()?,
            workspace_id: "workspace_00000000000000000000000000000072".parse()?,
            workspace: Mutex::new(None),
            conversation_inventory: Mutex::new(BTreeMap::new()),
            workspace_lookups: AtomicUsize::new(0),
            conversations: AtomicUsize::new(0),
            reopens: AtomicUsize::new(0),
            streams: AtomicUsize::new(0),
            prompts: AtomicUsize::new(0),
            side_effects: Mutex::new(None),
            aborts: AtomicUsize::new(0),
            abort_returns: AtomicUsize::new(0),
            workspace_check_armed: AtomicBool::new(false),
            workspace_check_observed: Notify::new(),
            workspace_check_release: Notify::new(),
            workspace_update_armed: AtomicBool::new(false),
            workspace_update_observed: Notify::new(),
            workspace_update_release: Notify::new(),
            workspace_deletion: Mutex::new(StreamFailureDeletion::Retry),
            workspace_delete_attempts: AtomicUsize::new(0),
            workspace_delete_observed: Notify::new(),
            workspace_delete_release: Notify::new(),
            shutdown_panics: AtomicBool::new(false),
            shutdowns: AtomicUsize::new(0),
            shutdown_armed: AtomicBool::new(false),
            shutdown_observed: Notify::new(),
            shutdown_release: Notify::new(),
        });
        let agent = state.agent();
        Ok((state, agent))
    }

    async fn assert_notification_corruption(
        corruption: NotificationCorruption,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let (faults, store) = StorageFaults::open().await?;
        let native = store.inner.clone();
        let (state, agent) = Self::open()?;
        let orphan = matches!(corruption, NotificationCorruption::OrphanTarget { .. });
        let prospective = matches!(corruption, NotificationCorruption::ProspectiveBytes { .. });
        let positional = matches!(
            corruption,
            NotificationCorruption::TupleObject { .. }
                | NotificationCorruption::TupleMissing { .. }
                | NotificationCorruption::TupleExtra { .. }
                | NotificationCorruption::TupleField { .. }
                | NotificationCorruption::TupleText { .. }
                | NotificationCorruption::TupleTag { .. }
                | NotificationCorruption::TupleTimestampOverflow
        );
        let applied_count = if prospective { 10 } else { 1 };
        let notifications =
            NotificationSequence::new(applied_count + if orphan { 2 } else { 1 }, false);
        if positional {
            *notifications.encoded_body_bytes.lock().unwrap() = vec![64; notifications.count];
        }
        *state.notifications.lock().expect("test notifications lock") =
            Some(Arc::clone(&notifications));
        let mut definition: Value = serde_json::from_str(STREAM_FAILURE)?;
        definition["nodes"][0]["deadline_ms"] =
            Value::from(if prospective { 30_000 } else { 12_000 });
        let mut app = tokio::time::timeout(
            Duration::from_secs(3),
            App::open(AppConfig::new(
                store,
                vec![AgentSpec::new("faulty".to_owned(), "v1".to_owned(), agent)],
                vec![WorkflowDefinition::parse(&definition.to_string())?],
            )),
        )
        .await??;
        let checked =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(if prospective { 20 } else { 10 }), async {
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().unwrap(),
                })
                .await?;
            let workflow = workspace
                .workflow("stream-failure".parse().unwrap())
                .await?;
            let run = workflow.start(ExecutionOptions::default()).await?;
            let mut applied_checkpoint = 0;
            for index in 0..applied_count {
                let first = tokio::time::timeout(Duration::from_secs(2), run.wait())
                    .await.expect("each original prefix notification becomes actionable")?;
                let [Interaction::Notification { sequence, generation, notification }] = first.interactions.as_slice()
                    else { panic!("each prefix target must be durably waiting") };
                assert_eq!(notification.id(), &NotificationSequence::id(index));
                applied_checkpoint = run.resolve(*sequence, *generation,
                    Resolution::Notification(NotificationResolution::Allow)).await?.checkpoint;
                assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), index + 1);
                assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), index + 1);
            }
            let waiting = tokio::time::timeout(Duration::from_secs(2), run.wait())
                .await
                .expect("the original target after the applied prefix is actionable")?;
            let [
                Interaction::Notification {
                    sequence,
                    generation,
                    notification,
                },
            ] = waiting.interactions.as_slice()
            else {
                panic!("the next target must retain its own original authority")
            };
            assert_eq!(notification.id(), &NotificationSequence::id(applied_count));
            let (sequence, generation) = (*sequence, *generation);
            let mut held = if orphan {
                *notifications.resolution_fault.lock().unwrap() =
                    NotificationResolutionFault::GateReceipt;
                Some(Box::pin(run.resolve(
                    sequence,
                    generation,
                    Resolution::Notification(NotificationResolution::Allow),
                )))
            } else {
                None
            };
            if let Some(receipt) = &mut held {
                tokio::time::timeout(Duration::from_secs(2), async {
                    tokio::select! {
                        _ = notifications.resolution_entered.notified() => {},
                        _ = receipt.as_mut() => panic!("N2's original receipt must remain held"),
                    }
                    while notifications.received.load(Ordering::SeqCst) < 3 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("third real notification is delivered behind the second receipt");
                assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), 2);
                assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), 1);
                let held_snapshot = run.snapshot().await?;
                Self::assert_same_interactions(&held_snapshot.interactions, &waiting.interactions);
            }
            let original = if positional {
                let row = native.load(StateKey::new("app-sdk", format!(
                    "history/{}/{}/{}/{}/checkpoint/{:020}",
                    workspace.id(), workflow.id(), run.id(), waiting.occurrence, waiting.checkpoint,
                ))?).await?.expect("actual waiting checkpoint");
                let logical = StorageFaults::logical_checkpoint(&native, &row.record).await;
                let checkpoint = &logical["checkpoint"];
                let controls = checkpoint["controls"].as_object().unwrap();
                assert_eq!(controls.len(), 1);
                let control = controls.values().next().unwrap();
                let ConversationNotificationKind::Interaction {
                    title, body, response_schema, dismissible,
                } = notification.kind() else { panic!("original public interaction") };
                let inserted_at = notification.inserted_at().duration_since(std::time::UNIX_EPOCH).unwrap();
                assert_eq!(checkpoint["settlements"].as_array().unwrap().len(), 2);
                assert_eq!(checkpoint["settlements"][0][4], serde_json::json!([2, [1], applied_checkpoint]),
                    "the first original Allow receipt retains its actual publication checkpoint");
                assert_eq!(checkpoint["settlements"][1], serde_json::json!([
                    sequence, generation, control["sequence"],
                    [1, 1, notification.id(), [inserted_at.as_secs(), inserted_at.subsec_nanos()],
                     title, body, response_schema, dismissible],
                    [0]
                ]), "ordered arrays preserve the exact public target, timestamp and original control");
                assert_eq!(row.record.value["epoch"], "app/v7");
                Some(row)
            } else { None };
            let original_predecessor = if matches!(corruption, NotificationCorruption::TupleText { .. }) {
                Some(native.load(StateKey::new("app-sdk", format!(
                    "history/{}/{}/{}/{}/checkpoint/{:020}",
                    workspace.id(), workflow.id(), run.id(), waiting.occurrence,
                    waiting.checkpoint.checked_sub(1).expect("the original Applied predecessor"),
                ))?).await?.expect("actual waiting predecessor"))
            } else { None };
            *faults.notification_corruption.lock().unwrap() = corruption;
            let outcome = run.snapshot().await;
            *faults.notification_corruption.lock().unwrap() = NotificationCorruption::None;
            for original in original.into_iter().chain(original_predecessor) {
                let unchanged = native.load(original.record.key.clone()).await?
                    .expect("original native row remains retained");
                assert_eq!(unchanged, original,
                    "rejected tuple observation leaves original native value and revision intact");
                assert_eq!(serde_json::to_vec(unchanged.record.value.as_ref()).expect("returned native row encoding"),
                    serde_json::to_vec(original.record.value.as_ref()).expect("original native row encoding"),
                    "rejected tuple observation preserves exact ordered native bytes");
            }
            assert!(
                matches!(outcome, Err(AppError::Integration(_))),
                "durable targets must preserve decisions, current ownership, and prospective capacity"
            );
            let resolved = notifications.resolved.lock().unwrap().clone();
            assert_eq!(resolved.len(), applied_count + usize::from(orphan));
            assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), applied_count + usize::from(orphan));
            assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), applied_count);
            assert_eq!(state.prompts.load(Ordering::SeqCst), 1);
            if let Some(receipt) = held {
                notifications.resolution_release.notify_one();
                tokio::time::timeout(Duration::from_secs(2), receipt)
                    .await
                    .expect("original N2 receipt is released after corruption is disarmed")?;
                let third = tokio::time::timeout(Duration::from_secs(2), run.wait())
                    .await
                    .expect("the original delivered N3 is then genuinely reserved")?;
                let [
                    Interaction::Notification {
                        sequence,
                        generation,
                        notification,
                    },
                ] = third.interactions.as_slice()
                else {
                    panic!("third delivery remains an independently actionable target")
                };
                assert_eq!(notification.id(), &NotificationSequence::id(2));
                run.resolve(
                    *sequence,
                    *generation,
                    Resolution::Notification(NotificationResolution::Allow),
                )
                .await?;
            } else {
                run.resolve(
                    sequence,
                    generation,
                    Resolution::Notification(NotificationResolution::Allow),
                )
                .await?;
            }
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(2), run.wait())
                    .await
                    .expect("uncorrupted exact receipts finish the original prompt")?
                    .status,
                RunStatus::Succeeded
            ));
            assert_eq!(
                notifications.resolution_calls.load(Ordering::SeqCst),
                notifications.count
            );
            assert_eq!(
                notifications.resolution_returns.load(Ordering::SeqCst),
                notifications.count
            );
            Ok::<_, AppError>(())
        }))
        .catch_unwind()
        .await;
        *faults.notification_corruption.lock().unwrap() = NotificationCorruption::None;
        *notifications.resolution_fault.lock().unwrap() = NotificationResolutionFault::Pass;
        notifications.resolution_release.notify_one();
        notifications.cancelled.store(true, Ordering::SeqCst);
        notifications.changed.notify_waiters();
        let cleanup = std::panic::AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        ))
        .catch_unwind()
        .await;
        drop(app);
        match checked {
            Ok(result) => result.expect("corruption observation is bounded")?,
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup
            .expect("corruption cleanup does not panic")
            .expect("corruption cleanup is bounded")?;
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
        Ok(())
    }

    async fn assert_notifications(
        count: usize,
        redeliver: bool,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let (state, agent) = StreamFailureState::open()?;
        let notifications = NotificationSequence::new(count, redeliver);
        *state.notifications.lock().expect("test notifications lock") =
            Some(Arc::clone(&notifications));
        let store =
            StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
        let mut app = App::open(AppConfig::new(
            store,
            vec![AgentSpec::new("faulty".to_owned(), "v1".to_owned(), agent)],
            vec![WorkflowDefinition::parse(STREAM_FAILURE)?],
        ))
        .await?;
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir()?,
            })
            .await?;
        let workflow = workspace.workflow("stream-failure".parse()?).await?;
        let run = workflow.start(ExecutionOptions::default()).await?;
        let mut receipts = Vec::new();
        for index in 0..count {
            let snapshot = tokio::time::timeout(Duration::from_millis(500), run.wait())
                .await
                .expect("each sequential notification must become actionable")?;
            let Some(Interaction::Notification {
                sequence,
                generation,
                notification,
            }) = snapshot.interactions.first()
            else {
                panic!("each distinct prompt notification must retain caller resolution ownership")
            };
            let (sequence, generation) = (*sequence, *generation);
            assert_eq!(notification.id(), &NotificationSequence::id(index));
            assert!(
                receipts
                    .last()
                    .is_none_or(|(prior, _, _)| *prior < sequence),
                "each notification must get a fresh target sequence"
            );
            let applied = run
                .resolve(
                    sequence,
                    generation,
                    Resolution::Notification(NotificationResolution::Allow),
                )
                .await?;
            receipts.push((sequence, generation, applied));
            if redeliver {
                run.resolve(
                    sequence,
                    generation,
                    Resolution::Notification(NotificationResolution::Allow),
                )
                .await?;
                assert!(matches!(
                    run.resolve(
                        sequence,
                        generation,
                        Resolution::Notification(NotificationResolution::Deny {
                            reason: "changed decision".to_owned()
                        })
                    )
                    .await,
                    Err(AppError::Conflict(_))
                ));
            }
        }
        let terminal = tokio::time::timeout(Duration::from_secs(2), run.wait())
            .await
            .expect("one prompt must complete after both exact settlements")?;
        assert!(matches!(terminal.status, RunStatus::Succeeded));
        assert_eq!(terminal.state.values()["answer"], "both approvals accepted");
        assert_eq!(state.prompts.load(Ordering::SeqCst), 1);
        let resolved = notifications
            .resolved
            .lock()
            .expect("test resolution lock")
            .clone();
        assert_eq!(
            resolved,
            (0..count).map(NotificationSequence::id).collect::<Vec<_>>()
        );
        if redeliver {
            for (sequence, generation, applied) in receipts {
                let repeated = run
                    .resolve(
                        sequence,
                        generation,
                        Resolution::Notification(NotificationResolution::Allow),
                    )
                    .await?;
                Self::assert_applied_snapshot(&repeated, &applied);
            }
        }
        app.shutdown(Shutdown::Drain {
            timeout: Duration::from_secs(2),
        })
        .await?;
        Ok(())
    }

    async fn assert_initial_attempt_capacity() -> Result<(), Box<dyn Error + Send + Sync>> {
        let bound = 4 * 1_048_576;
        let second_node = "z".repeat(128);
        let mut padding_bytes = 1;
        let mut basis = None;
        for reject in [false, true] {
            let (first, first_agent) = Self::open()?;
            let (second, second_agent) = Self::open()?;
            let notifications = NotificationSequence::new(11, false);
            let mut bodies = vec![393_181; 10];
            bodies.push(1);
            *notifications
                .encoded_body_bytes
                .lock()
                .expect("test body sizes") = bodies;
            *first.notifications.lock().expect("first notifications") =
                Some(Arc::clone(&notifications));
            *second
                .preparation_fault
                .lock()
                .expect("second preparation fault") = PreparationFault::ExclusiveWorkspace {
                gate_first_creation: true,
            };
            let definition = WorkflowDefinition::parse(&serde_json::json!({
                "schema":"app-sdk.workflow/v2", "id":"initial-capacity", "revision":"v1",
                "entry":"first", "state":{"answer":"", "padding":"", "second":""},
                "required":["answer", "padding", "second"],
                "reducers":{"answer":"reject_concurrent", "padding":"reject_concurrent", "second":"reject_concurrent"},
                "nodes":[
                    {"id":"first", "deadline_ms":120000, "step":{"kind":"agent", "agent":"first",
                        "prompt":[{"kind":"text", "text":"first"}], "text_target":"answer"}},
                    {"id":"fill", "step":{"kind":"input", "target":"padding",
                        "message":[{"kind":"text", "text":"fill native capacity"}]}},
                    {"id":second_node, "deadline_ms":30000, "step":{"kind":"agent", "agent":"second",
                        "prompt":[{"kind":"text", "text":"second"}], "text_target":"second"}}
                ],
                "routes":{
                    "first":{"mode":"first", "branches":[{"target":{"kind":"node", "id":"fill"}}]},
                    "fill":{"mode":"first", "branches":[{"target":{"kind":"node", "id":second_node}}]}
                }
            }).to_string())?;
            let store =
                StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
                    .await?;
            let mut app = App::open(AppConfig::new(
                store.clone(),
                vec![
                    AgentSpec::new("first".into(), "v1".into(), first_agent),
                    AgentSpec::new("second".into(), "v1".into(), second_agent),
                ],
                vec![definition],
            ))
            .await?;
            let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(90), async {
                let workspace = app.create_workspace(WorkspaceConfig {
                    name: None, root: std::env::current_dir()?,
                }).await?;
                let workflow = workspace.workflow("initial-capacity".parse()?).await?;
                let run = workflow.start(ExecutionOptions::default()).await?;
                for index in 0..11 {
                    let snapshot = run.wait().await?;
                    let [Interaction::Notification { sequence, generation, notification }] = snapshot.interactions.as_slice()
                        else { panic!("the real first Attempt must admit each legal notification") };
                    assert_eq!(notification.id(), &NotificationSequence::id(index));
                    run.resolve(*sequence, *generation,
                        Resolution::Notification(NotificationResolution::Allow)).await?;
                    assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), index + 1);
                    assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), index + 1);
                }
                let input = run.wait().await?;
                let [Interaction::Input { sequence, generation, .. }] = input.interactions.as_slice()
                    else { panic!("the first prompt completes before the native-only capacity filler") };
                assert_eq!(input.state.values()["answer"], "both approvals accepted");
                assert_eq!(input.last_action_ordinal, Some(0));
                assert_eq!(first.prompts.load(Ordering::SeqCst), 1);
                assert_eq!(notifications.prompt_returns.load(Ordering::SeqCst), 1);
                assert_eq!(second.conversations.load(Ordering::SeqCst), 0);
                let lower_workspace = second.workspace_info()?;
                let lower_lookups = second.workspace_lookups.load(Ordering::SeqCst);
                let padding = "p".repeat(padding_bytes);
                let applied = run.resolve(*sequence, *generation, Resolution::Input(Value::from(padding.clone()))).await?;
                let applied_row = store.load(StateKey::new("app-sdk", format!(
                    "history/{}/{}/{}/{}/checkpoint/{:020}", workspace.id(), workflow.id(), run.id(), applied.occurrence, applied.checkpoint,
                ))?).await?.expect("original Input receipt retains its immutable row");
                let applied_logical = StorageFaults::logical_checkpoint(&store, &applied_row.record).await;
                let applied_bytes = serde_json::to_vec(&applied_logical)?.len();
                assert!(applied_bytes <= bound);
                let (prior_bytes, intent_bytes, copies) = if reject {
                    let (prior_basis, intent_basis, copies, applied_basis) = basis.expect("real calibration precedes rejection");
                    assert_eq!(applied_bytes, applied_basis + copies * (padding_bytes - 1),
                        "the same original Input receipt retains its calibrated value occurrences");
                    let terminal = tokio::select! {
                        biased;
                        _ = second.preparation_entered.notified() => panic!("prospective capacity must reject the second Attempt before lower preparation"),
                        result = run.wait() => result?,
                    };
                    assert!(matches!(terminal.status, RunStatus::Failed(WorkflowFailure::Limit(_))),
                        "a running background node that cannot reserve Unknown capacity publishes Limit");
                    assert_eq!(terminal.state.values()["padding"], padding);
                    assert_eq!(terminal.state.values()["answer"], "both approvals accepted");
                    assert_eq!(terminal.state.values()["second"], "");
                    assert_eq!(terminal.last_action_ordinal, Some(1));
                    let failed = store.load(StateKey::new("app-sdk", format!(
                        "history/{}/{}/{}/{}/checkpoint/{:020}", workspace.id(), workflow.id(), run.id(), terminal.occurrence, terminal.checkpoint,
                    ))?).await?.expect("the actual Limit failure remains durably publishable");
                    let failed_logical = StorageFaults::logical_checkpoint(&store, &failed.record).await;
                    assert!(serde_json::to_vec(&failed_logical)?.len() <= bound,
                        "the original pre-admission record has actual capacity for its terminal failure");
                    assert_eq!(second.conversations.load(Ordering::SeqCst), 0);
                    assert_eq!(second.prompts.load(Ordering::SeqCst), 0);
                    assert_eq!(second.streams.load(Ordering::SeqCst), 0);
                    let retained_workspace = second.workspace_info()?;
                    assert_eq!(retained_workspace.id(), lower_workspace.id());
                    assert_eq!(retained_workspace.revision(), lower_workspace.revision());
                    assert_eq!(second.workspace_lookups.load(Ordering::SeqCst), lower_lookups,
                        "capacity rejection preserves the existing materialization without starting lower preparation");
                    let mut prior = None;
                    for sequence in (0..terminal.checkpoint).rev() {
                        let row = store.load(StateKey::new("app-sdk", format!(
                            "history/{}/{}/{}/{}/checkpoint/{sequence:020}", workspace.id(), workflow.id(), run.id(), terminal.occurrence,
                        ))?).await?.expect("terminal history remains contiguous");
                        let checkpoint = &row.record.value["checkpoint"];
                        if checkpoint["phase"]["state"] == "active"
                            && checkpoint["controls"].as_object().expect("canonical controls").is_empty()
                            && checkpoint["staged"].as_array().expect("retained staging").is_empty()
                            && checkpoint["frontier"].as_array().expect("retained frontier").iter()
                                .any(|activation| activation["node"] == second_node)
                        {
                            prior = Some(row);
                            break;
                        }
                    }
                    let prior = prior.expect("the rejected run retains its own actual pre-admission frontier");
                    let prior_logical = StorageFaults::logical_checkpoint(&store, &prior.record).await;
                    let encoded = serde_json::to_vec(&prior_logical)?;
                    let prior_bytes = encoded.len();
                    assert_eq!(prior_bytes, prior_basis + copies * (padding_bytes - 1));
                    let quoted = serde_json::to_string(&padding)?;
                    assert_eq!(std::str::from_utf8(&encoded)?.matches(&quoted).count(), copies);
                    assert_eq!(prior.record.value["checkpoint"]["state"]["padding"], padding);
                    assert!(prior.record.value["checkpoint"]["controls"].as_object().expect("canonical controls").is_empty());
                    let ordinary_intent = intent_basis + copies * (padding_bytes - 1);
                    assert!(ordinary_intent <= bound && bound - ordinary_intent < 64 + copies,
                        "the rejected initial Intent would fit the ordinary record bound with its measured unchanged metadata");
                    (prior_bytes, ordinary_intent, copies)
                } else {
                    second.preparation_entered.notified().await;
                    let held = run.snapshot().await?;
                    let intent = store.load(StateKey::new("app-sdk", format!(
                        "history/{}/{}/{}/{}/checkpoint/{:020}", workspace.id(), workflow.id(), run.id(), held.occurrence, held.checkpoint,
                    ))?).await?.expect("the original second Intent precedes lower preparation return");
                    let controls = intent.record.value["checkpoint"]["controls"].as_object().expect("canonical controls");
                    assert_eq!(controls.len(), 1);
                    let control = controls.values().next().unwrap();
                    assert_eq!(control["node"], second_node);
                    assert_eq!(control["state"]["phase"]["state"], "intent");
                    assert!(control["state"]["identity"].is_null());
                    let prior = store.load(StateKey::new("app-sdk", format!(
                        "history/{}/{}/{}/{}/checkpoint/{:020}", workspace.id(), workflow.id(), run.id(), held.occurrence, held.checkpoint - 1,
                    ))?).await?.expect("original Intent has its actual predecessor");
                    let prior_logical = StorageFaults::logical_checkpoint(&store, &prior.record).await;
                    let encoded = serde_json::to_vec(&prior_logical)?;
                    let quoted = serde_json::to_string(&padding)?;
                    let copies = std::str::from_utf8(&encoded)?.matches(&quoted).count();
                    assert!(copies > 0);
                    assert_eq!(serde_json::to_string(&applied_logical)?.matches(&quoted).count(), copies,
                        "calibrate the real retained Input occurrences before sizing the second journey");
                    let intent_logical = StorageFaults::logical_checkpoint(&store, &intent.record).await;
                    assert_eq!(serde_json::to_string(&intent_logical)?.matches(&quoted).count(), copies);
                    assert_eq!(prior.record.value["checkpoint"]["state"]["padding"], padding);
                    assert!(prior.record.value["checkpoint"]["controls"].as_object().expect("canonical controls").is_empty());
                    let intent_bytes = serde_json::to_vec(&intent_logical)?.len();
                    assert!(applied_bytes <= intent_bytes && encoded.len() < intent_bytes);
                    second.preparation_release.notify_one();
                    let terminal = run.wait().await?;
                    assert!(matches!(terminal.status, RunStatus::Succeeded));
                    assert_eq!(second.conversations.load(Ordering::SeqCst), 1);
                    assert_eq!(second.prompts.load(Ordering::SeqCst), 1);
                    (encoded.len(), intent_bytes, copies)
                };
                assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), 11);
                assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), 11);
                assert_eq!(first.prompts.load(Ordering::SeqCst), 1);
                Ok::<_, Box<dyn Error + Send + Sync>>((prior_bytes, intent_bytes, copies, applied_bytes))
            })).catch_unwind().await;
            second.preparation_release.notify_one();
            *first
                .preparation_fault
                .lock()
                .expect("first preparation fault") = PreparationFault::Pass;
            *notifications
                .resolution_fault
                .lock()
                .expect("test resolution fault") = NotificationResolutionFault::Pass;
            notifications.resolution_release.notify_one();
            notifications.cancelled.store(true, Ordering::SeqCst);
            notifications.changed.notify_waiters();
            let cleanup = AssertUnwindSafe(tokio::time::timeout(
                Duration::from_secs(10),
                app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(5),
                }),
            ))
            .catch_unwind()
            .await;
            drop(app);
            let measured = match observed {
                Ok(result) => result.expect("each actual admission-capacity journey is bounded")?,
                Err(panic) => std::panic::resume_unwind(panic),
            };
            cleanup
                .expect("initial-capacity cleanup does not panic")
                .expect("initial-capacity cleanup joins")?;
            assert_eq!(first.shutdowns.load(Ordering::SeqCst), 1);
            assert_eq!(second.shutdowns.load(Ordering::SeqCst), 1);
            if !reject {
                // The spare ordinary bytes are not a promised failure budget:
                // the rejected journey still requires the actual native Limit row.
                padding_bytes += (bound - measured.1 - 64) / measured.2;
                assert!(padding_bytes > 1);
                basis = Some(measured);
            }
        }
        Ok(())
    }

    fn golden_lower_inventory(&self) -> Result<Value, SdkError> {
        // Only public caller-Agent metadata is recorded. Reconstruction installs
        // this existing deterministic lower inventory, never private App rows.
        let workspace = self.workspace_info()?;
        assert_eq!(
            workspace.defaults(),
            &agent_sdk::workspace::WorkspaceDefaults::empty()
        );
        let context = workspace.context();
        let conversations = self
            .conversation_inventory
            .lock()
            .expect("test conversation inventory");
        Ok(serde_json::json!({
            "agent_id": self.agent_id.to_string(),
            "workspace": {
                "id": workspace.id().to_string(), "name": workspace.name(),
                "revision": workspace.revision(), "cwd": context.cwd(),
                "roots": context.roots().iter().map(|root| serde_json::json!({
                    "path": root.path(), "access": format!("{:?}", root.access()),
                })).collect::<Vec<_>>(),
                "sandbox": format!("{:?}", context.execution_policy().sandbox_mode()),
                "network": format!("{:?}", context.execution_policy().network_access()),
                "defaults": "empty",
            },
            "conversations": conversations.values().map(|conversation| serde_json::json!({
                "id": conversation.id().to_string(),
                "workspace_id": conversation.workspace_id().to_string(),
                "title": conversation.title(),
            })).collect::<Vec<_>>(),
        }))
    }

    fn install_golden_lower_inventory(&self, inventory: &Value) -> Result<(), SdkError> {
        use agent_sdk::workspace::{
            WorkspaceContext, WorkspaceDefaults, WorkspaceExecutionPolicy, WorkspaceNetworkAccess,
            WorkspaceRoot, WorkspaceRootAccess, WorkspaceSandboxMode,
        };
        assert_eq!(inventory["agent_id"], self.agent_id.to_string());
        let original = &inventory["workspace"];
        assert_eq!(original["id"], self.workspace_id.to_string());
        assert_eq!(original["defaults"], "empty");
        assert_eq!(original["sandbox"], "FullAccess");
        assert_eq!(original["network"], "Enabled");
        let roots = original["roots"]
            .as_array()
            .unwrap()
            .iter()
            .map(|root| {
                assert_eq!(root["access"], "ReadWrite");
                WorkspaceRoot::new(
                    PathBuf::from(root["path"].as_str().unwrap()),
                    WorkspaceRootAccess::ReadWrite,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        *self.workspace.lock().unwrap() = Some(WorkspaceInfo::new(
            self.workspace_id.clone(),
            original["name"].as_str().unwrap().to_owned(),
            WorkspaceContext::new(
                PathBuf::from(original["cwd"].as_str().unwrap()),
                roots,
                WorkspaceExecutionPolicy::new(
                    WorkspaceSandboxMode::FullAccess,
                    WorkspaceNetworkAccess::Enabled,
                ),
            )?,
            WorkspaceDefaults::empty(),
            original["revision"].as_u64().unwrap(),
        )?);
        for original in inventory["conversations"].as_array().unwrap() {
            let conversation = ConversationInfo::new(
                original["id"].as_str().unwrap().parse()?,
                original["workspace_id"].as_str().unwrap().parse()?,
                original["title"].as_str().map(str::to_owned),
            )?;
            self.conversation_inventory
                .lock()
                .unwrap()
                .insert(conversation.id().clone(), conversation);
        }
        assert_eq!(self.golden_lower_inventory()?, *inventory);
        Ok(())
    }

    async fn assert_v6_migration(name: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        let fixture = AppV6Fixture::copy(name);
        let expected = fixture.expected();
        let native = StateStore::connect(fixture.config()).await?;
        let original = fixture.records(&native).await;
        let original_root = original
            .iter()
            .find(|row| row.record.key.key() == "app")
            .unwrap();
        assert_eq!(original_root.record.value["epoch"], "app/v6");
        // These records belong to another caller. They are authored through the
        // real Store after copying; they are not synthetic legacy App records.
        let outside = StateKey::new("migration-neighbor", "present")?;
        let tombstone = StateKey::new("migration-neighbor", "deleted")?;
        let mut transaction = native.begin_transaction().await?;
        transaction
            .put(StateRecord::new(
                outside.clone(),
                serde_json::json!({"untouched": true}),
            ))
            .await?;
        transaction
            .put(StateRecord::new(tombstone.clone(), Value::from("old")))
            .await?;
        transaction.commit().await?;
        let mut transaction = native.begin_transaction().await?;
        transaction.delete(tombstone.clone()).await?;
        transaction.commit().await?;
        let outside_before = native.load(outside.clone()).await?.unwrap();
        let (state, agent) = Self::open()?;
        state.install_golden_lower_inventory(&expected["lower_inventory"])?;
        let definition = WorkflowDefinition::parse(&expected["definition"].to_string())?;
        let mut app = App::open(AppConfig::new(
            native.clone(),
            vec![AgentSpec::new("faulty".to_owned(), "v1".to_owned(), agent)],
            vec![definition],
        ))
        .await?;
        let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(120), async {
            let root = native.load(StateKey::new("app-sdk", "app")?).await?.unwrap();
            assert_eq!(root.record.value["epoch"], "app/v7", "open migrates before claiming a new generation");
            assert_eq!(root.record.value["generation"].as_u64().unwrap(), original_root.record.value["generation"].as_u64().unwrap() + 1);
            assert_eq!(root.record.value["run_count"], original_root.record.value["run_count"]);
            let checkpoints = original.iter().filter(|row| row.record.key.key().split('/').nth(5) == Some("checkpoint")).count();
            let other_rows = original.len() - checkpoints - 1;
            // Each checkpoint and each bounded 256-row batch advances the root.
            // Claim, three phase transitions, Ready and the ordinary generation
            // claim add six publications. Receipt retirement does not write it.
            let migration_publications = checkpoints + other_rows.div_ceil(256) + 6;
            assert_eq!(root.revision.get(), original_root.revision.get() + migration_publications as u64,
                "exact physical root publications before later work or shutdown");
            // Rehydrate each actual v7 checkpoint using fresh public leaf loads,
            // then compare the entire logical row with the original v6 payload.
            // Only the epoch/physical settlement encoding is normalized for this
            // assertion; the persisted rows themselves are never rewritten here.
            for before in &original {
                if before.record.key.key() == "app" { continue; }
                if before.record.key.key().starts_with("receipt/") {
                    assert!(native.load(before.record.key.clone()).await?.is_none(), "ordinary cold-open cleanup retires old internal publication receipts");
                    continue;
                }
                let after = native.load(before.record.key.clone()).await?.expect("every original surviving record");
                assert_eq!(after.revision.get(), before.revision.get() + 1, "one physical migration of {}", before.record.key.key());
                let mut logical = (*after.record.value).clone();
                assert_eq!(logical["epoch"], "app/v7");
                logical["epoch"] = Value::from("app/v6");
                if let Some(settlements) = logical.pointer("/checkpoint/settlements") {
                    let descriptor = settlements.as_object().expect("closed v7 descriptor");
                    assert_eq!(descriptor.len(), 2);
                    let mut entries = Vec::new();
                    let scope = before.record.key.key().split_once("/checkpoint/").unwrap().0;
                    for (index, publication) in descriptor["pages"].as_array().unwrap().iter().enumerate() {
                        let publication = publication.as_u64().unwrap();
                        let leaf = native.load(StateKey::new("app-sdk", format!("{scope}/ledger/{index:02}/{publication:020}"))?).await?.expect("all referenced leaves exist");
                        assert_eq!(leaf.revision.get(), 1);
                        let value = leaf.record.value.as_ref();
                        assert_eq!(value["epoch"], "app/v7");
                        assert_eq!(value["workspace_id"], logical["workspace_id"]);
                        assert_eq!(value["workflow_id"], logical["workflow_id"]);
                        assert_eq!(value["id"], logical["id"]);
                        assert_eq!(value["occurrence"], logical["occurrence"]);
                        assert_eq!(value["index"], index);
                        assert_eq!(value["sequence"], publication);
                        let rows = value["entries"].as_array().unwrap();
                        assert!(!rows.is_empty() && rows.len() <= 64);
                        entries.extend(rows.iter().cloned());
                    }
                    assert_eq!(entries.len() as u64, descriptor["count"].as_u64().unwrap());
                    logical["checkpoint"]["settlements"] = Value::Array(entries);
                }
                assert_eq!(logical, *before.record.value, "exact legacy logical record {}", before.record.key.key());
            }
            let scope = expected["scope"].as_str().map(str::to_owned).unwrap_or_else(|| format!("{}/stream-failure/{}/{}", expected["workspace"].as_str().unwrap(), expected["run"].as_str().unwrap(), expected["occurrence"].as_str().unwrap()));
            let scope_parts = scope.split('/').collect::<Vec<_>>();
            let workspace = app.workspace(scope_parts[0].parse()?).await?;
            let workflow = workspace.workflow(scope_parts[1].parse()?).await?;
            let run = workflow.open_run(RunSelector::Latest(scope_parts[2].parse()?)).await?;
            let snapshot = run.snapshot().await?;
            assert_eq!(snapshot.occurrence, scope_parts[3]);
            if name == "notification-history" {
                assert_eq!(workspace.snapshot().await?.revision, expected["workspace_revision"].as_u64().unwrap());
                assert!(matches!(snapshot.status, RunStatus::Succeeded));
                assert_eq!(snapshot.checkpoint, expected["checkpoint"].as_u64().unwrap());
                assert_eq!(snapshot.state.values(), expected["state"].as_object().unwrap());
                let mut history = Vec::new();
                let mut cursor = None;
                loop {
                    let page = run.history(cursor, NonZeroUsize::new(31).unwrap()).await?;
                    for checkpoint in page.checkpoints {
                        history.push(serde_json::json!({"sequence": checkpoint.sequence,
                            "state": checkpoint.state.values(), "action_ordinal": checkpoint.action_ordinal,
                            "position": checkpoint.position}));
                    }
                    match page.next { Some(next) => cursor = Some(next), None => break }
                }
                assert_eq!(Value::Array(history), expected["history"]);
                for receipt in expected["receipts"].as_array().unwrap() {
                    let replay = run.resolve(receipt["sequence"].as_u64().unwrap(), receipt["generation"].as_u64().unwrap(), Resolution::Notification(NotificationResolution::Allow)).await?;
                    assert_eq!(replay.id, snapshot.id);
                    assert_eq!(replay.occurrence, snapshot.occurrence);
                    assert_eq!(replay.checkpoint, receipt["checkpoint"].as_u64().unwrap());
                    assert_eq!(replay.state.values(), expected["history"][receipt["checkpoint"].as_u64().unwrap() as usize]["state"].as_object().unwrap());
                }
                assert!(matches!(workflow.open_run(RunSelector::Latest(expected["deleted_restored_run"].as_str().unwrap().parse()?)).await, Err(AppError::NotFound(_))));
            } else {
                let unknown = run.checkpoint(CheckpointSelector::Sequence(expected["unknown_checkpoint"].as_u64().unwrap())).await?;
                assert_eq!(unknown.sequence, expected["unknown_checkpoint"].as_u64().unwrap());
                assert!(matches!(snapshot.status, RunStatus::Failed(_)));
            }
            let outside_after = native.load(outside).await?.unwrap();
            assert_eq!(outside_after.record.value, outside_before.record.value);
            assert_eq!(outside_after.revision, outside_before.revision);
            assert!(native.load(tombstone.clone()).await?.is_none());
            let mut transaction = native.begin_transaction().await?;
            transaction.put(StateRecord::new(tombstone.clone(), Value::from("new"))).await?;
            transaction.commit().await?;
            assert_eq!(native.load(tombstone).await?.unwrap().revision.get(), 3, "foreign tombstone generation survives migration");
            assert_eq!(state.prompts.load(Ordering::SeqCst), 0, "recovery and replay make no new native effect");
            assert_eq!(state.aborts.load(Ordering::SeqCst), 0);
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        })).catch_unwind().await;
        let cleanup = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(20),
        }))
        .catch_unwind()
        .await;
        if !matches!(&cleanup, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
            loop {
                let joined = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(60),
                }))
                .catch_unwind()
                .await;
                eprintln!(
                    "v6-migration phase=settlement original={cleanup:?} subsequent={joined:?}"
                );
                if matches!(&joined, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
                    break;
                }
            }
        }
        drop(app);
        drop(native);
        match observed {
            Ok(result) => result??,
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
        Ok(())
    }

    async fn assert_v6_migration_resume(
        boundary: MigrationBoundary,
        corrupt_remaining: bool,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Phase ownership does not depend on a large notification history.
        // Keep the full >256-row fixture where bounded checkpoint/record
        // progress is the assertion; small genuine history covers root phases.
        let fixture = AppV6Fixture::copy(match boundary {
            MigrationBoundary::Claim | MigrationBoundary::Audit | MigrationBoundary::Ready => {
                "unknown-history"
            }
            MigrationBoundary::Checkpoint | MigrationBoundary::Records => "notification-history",
        });
        let expected = fixture.expected();
        let native = StateStore::connect(fixture.config()).await?;
        let before = fixture.records(&native).await;
        let (faults, mut store) = StorageFaults::open().await?;
        store.inner = native.clone();
        *faults.migration_fault.lock().unwrap() = MigrationFault::StopAfter { boundary };
        let (state, agent) = Self::open()?;
        state.install_golden_lower_inventory(&expected["lower_inventory"])?;
        let opened = App::open(AppConfig::new(
            store,
            vec![AgentSpec::new("faulty".into(), "v1".into(), agent)],
            vec![WorkflowDefinition::parse(
                &expected["definition"].to_string(),
            )?],
        ))
        .await;
        if let Ok(mut app) = opened {
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(20),
            })
            .await?;
            panic!(
                "the one permanent admission failure must be reported after its committed progress"
            );
        }
        assert!(matches!(opened, Err(AppError::Integration(_))));
        assert_eq!(
            state.shutdowns.load(Ordering::SeqCst),
            1,
            "failed construction joins its supplied lower owner"
        );
        let progress = match &*faults.migration_fault.lock().unwrap() {
            MigrationFault::Stopped { progress } => progress.clone(),
            _ => panic!("the next native begin must be rejected after the selected real commit"),
        };
        let stopped_root = native.load(progress.key.clone()).await?.unwrap();
        assert_eq!(stopped_root.record.value, progress.value);
        let old_root = before
            .iter()
            .find(|row| row.record.key.key() == "app")
            .unwrap();
        assert_eq!(
            stopped_root.record.value["generation"], old_root.record.value["generation"],
            "partial conversion never claims a logical generation"
        );
        assert!(boundary.observes(&stopped_root.record.value));
        if !matches!(boundary, MigrationBoundary::Ready) {
            assert!(
                stopped_root
                    .record
                    .value
                    .pointer("/migration/id")
                    .and_then(Value::as_str)
                    .is_some_and(|id| !id.is_empty())
            );
        }
        let converted = fixture
            .records(&native)
            .await
            .into_iter()
            .filter(|row| {
                row.record.key.key().split('/').nth(5) == Some("checkpoint")
                    && row.record.value["epoch"] == "app/v7"
            })
            .collect::<Vec<_>>();
        let expected_converted = match boundary {
            MigrationBoundary::Claim => 0,
            MigrationBoundary::Checkpoint => 1,
            MigrationBoundary::Records | MigrationBoundary::Audit | MigrationBoundary::Ready => {
                before
                    .iter()
                    .filter(|row| row.record.key.key().split('/').nth(5) == Some("checkpoint"))
                    .count()
            }
        };
        assert_eq!(
            converted.len(),
            expected_converted,
            "exact accepted checkpoint progress at {boundary:?}"
        );
        if corrupt_remaining {
            assert!(matches!(boundary, MigrationBoundary::Checkpoint));
            let selected = before
                .iter()
                .filter(|row| row.record.key.key().split('/').nth(5) == Some("checkpoint"))
                .nth(1)
                .unwrap();
            let current = native.load(selected.record.key.clone()).await?.unwrap();
            assert_eq!(current.record.value["epoch"], "app/v6");
            let mut malformed = (*current.record.value).clone();
            // This field is explicitly forbidden in persisted checkpoints. It
            // is added to an actual remaining legacy row after accepted progress.
            malformed["previous"] = Value::Null;
            let mut transaction = native.begin_transaction().await?;
            transaction
                .put(StateRecord::new(current.record.key, malformed))
                .await?;
            transaction.commit().await?;
            let retained = fixture.records(&native).await;
            let result = App::open(AppConfig::new(
                native.clone(),
                vec![AgentSpec::new("faulty".into(), "v1".into(), state.agent())],
                vec![WorkflowDefinition::parse(
                    &expected["definition"].to_string(),
                )?],
            ))
            .await;
            if let Ok(mut app) = result {
                app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(20),
                })
                .await?;
                panic!("resumed conversion must reject the newly malformed remaining legacy row");
            }
            assert!(matches!(result, Err(AppError::Integration(_))));
            let after = fixture.records(&native).await;
            assert_eq!(after.len(), retained.len());
            for (actual, expected) in after.iter().zip(&retained) {
                assert_eq!(actual.record.key, expected.record.key);
                assert_eq!(actual.record.value, expected.record.value);
                assert_eq!(
                    actual.revision, expected.revision,
                    "rejected resumed state is never normalized away"
                );
            }
            assert_eq!(state.prompts.load(Ordering::SeqCst), 0);
            assert_eq!(state.shutdowns.load(Ordering::SeqCst), 2);
            return Ok(());
        }
        let mut app = App::open(AppConfig::new(
            native.clone(),
            vec![AgentSpec::new("faulty".into(), "v1".into(), state.agent())],
            vec![WorkflowDefinition::parse(
                &expected["definition"].to_string(),
            )?],
        ))
        .await?;
        let checked = AssertUnwindSafe(async {
            let root = native
                .load(StateKey::new("app-sdk", "app")?)
                .await?
                .unwrap();
            assert_eq!(root.record.value["epoch"], "app/v7");
            assert_eq!(root.record.value["migration"]["state"], "ready");
            assert_eq!(
                root.record.value["generation"].as_u64().unwrap(),
                old_root.record.value["generation"].as_u64().unwrap() + 1
            );
            for prior in converted {
                let current = native.load(prior.record.key).await?.unwrap();
                assert_eq!(current.record.value, prior.record.value);
                assert_eq!(
                    current.revision, prior.revision,
                    "resumption does not convert an acknowledged checkpoint twice"
                );
            }
            let workspace = app
                .workspace(
                    expected["workspace"]
                        .as_str()
                        .or_else(|| expected["head"]["workspace_id"].as_str())
                        .unwrap()
                        .parse()?,
                )
                .await?;
            let workflow = workspace.workflow("stream-failure".parse()?).await?;
            let run = workflow
                .open_run(RunSelector::Latest(
                    expected["run"]
                        .as_str()
                        .or_else(|| expected["head"]["id"].as_str())
                        .unwrap()
                        .parse()?,
                ))
                .await?;
            assert_eq!(
                run.snapshot().await?.checkpoint,
                expected["checkpoint"]
                    .as_u64()
                    .or_else(|| expected["head"]["sequence"].as_u64())
                    .unwrap()
            );
            assert_eq!(state.prompts.load(Ordering::SeqCst), 0);
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        })
        .catch_unwind()
        .await;
        let cleanup = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(20),
        }))
        .catch_unwind()
        .await;
        if !matches!(&cleanup, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
            loop {
                let joined = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(60),
                }))
                .catch_unwind()
                .await;
                eprintln!("v6-resume phase=settlement original={cleanup:?} subsequent={joined:?}");
                if matches!(&joined, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
                    break;
                }
            }
        }
        drop(app);
        drop(native);
        match checked {
            Ok(result) => result?,
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 2);
        Ok(())
    }

    async fn assert_v6_migration_commit_readback(
        boundary: MigrationBoundary,
        with_leaves: bool,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Phase ownership does not depend on a large notification history.
        // Keep the full >256-row fixture where bounded checkpoint/record
        // progress is the assertion; small genuine history covers root phases.
        let fixture = AppV6Fixture::copy(match boundary {
            MigrationBoundary::Claim | MigrationBoundary::Audit | MigrationBoundary::Ready => {
                "unknown-history"
            }
            MigrationBoundary::Checkpoint | MigrationBoundary::Records => "notification-history",
        });
        let expected = fixture.expected();
        let native = StateStore::connect(fixture.config()).await?;
        let original = fixture.records(&native).await;
        let old_root = original
            .iter()
            .find(|row| row.record.key.key() == "app")
            .unwrap();
        let (faults, mut store) = StorageFaults::open().await?;
        store.inner = native.clone();
        let leaf_checkpoint = with_leaves.then(|| {
            assert!(matches!(boundary, MigrationBoundary::Checkpoint));
            original
                .iter()
                .find(|row| {
                    row.record.key.key().split('/').nth(5) == Some("checkpoint")
                        && row.record.value["checkpoint"]["settlements"]
                            .as_array()
                            .is_some_and(|entries| entries.len() == 65)
                })
                .expect("the authentic96-delivery history includes its first65-entry checkpoint")
                .record
                .key
                .key()
                .to_owned()
        });
        *faults.migration_fault.lock().unwrap() = MigrationFault::LoseCommitReply {
            boundary,
            after: leaf_checkpoint.clone(),
        };
        let (state, agent) = Self::open()?;
        state.install_golden_lower_inventory(&expected["lower_inventory"])?;
        let mut app = App::open(AppConfig::new(
            store,
            vec![AgentSpec::new("faulty".into(), "v1".into(), agent)],
            vec![WorkflowDefinition::parse(
                &expected["definition"].to_string(),
            )?],
        ))
        .await?;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(120), async {
            let progress = match &*faults.migration_fault.lock().unwrap() {
                MigrationFault::CommitReplyLost { progress } => progress.clone(),
                _ => panic!(
                    "the selected real publication must complete before its one failed reply"
                ),
            };
            assert!(boundary.observes(&progress.value));
            if let Some(key) = &leaf_checkpoint {
                assert_eq!(progress.value["migration"]["phase"]["after"], *key);
                let row = native
                    .load(StateKey::new("app-sdk", key.clone())?)
                    .await?
                    .unwrap();
                let descriptor = &row.record.value["checkpoint"]["settlements"];
                assert_eq!(descriptor["count"], 65);
                let pages = descriptor["pages"].as_array().unwrap();
                assert_eq!(pages.len(), 2);
                let sequence = row.record.value["checkpoint"]["sequence"].as_u64().unwrap();
                assert!(
                    pages[0].as_u64().unwrap() < sequence,
                    "unchanged full predecessor page remains referenced"
                );
                assert_eq!(
                    pages[1].as_u64(),
                    Some(sequence),
                    "the selected publication introduces its real second page"
                );
                let prefix = key.split("/checkpoint/").next().unwrap();
                for (index, publication) in pages.iter().enumerate() {
                    let publication = publication.as_u64().unwrap();
                    let leaf = native
                        .load(StateKey::new(
                            "app-sdk",
                            format!("{prefix}/ledger/{index:02}/{publication:020}"),
                        )?)
                        .await?
                        .unwrap();
                    assert_eq!(
                        leaf.revision.get(),
                        1,
                        "exact consuming-commit readback never rewrites either leaf"
                    );
                }
            }
            let root = native.load(old_root.record.key.clone()).await?.unwrap();
            let checkpoints = original
                .iter()
                .filter(|row| row.record.key.key().split('/').nth(5) == Some("checkpoint"))
                .count();
            let other = original.len() - checkpoints - 1;
            assert_eq!(
                root.revision.get(),
                old_root.revision.get() + (checkpoints + other.div_ceil(256) + 6) as u64,
                "uncertain publication is observed exactly, never replayed"
            );
            assert_eq!(root.record.value["migration"]["state"], "ready");
            assert_eq!(
                root.record.value["generation"].as_u64().unwrap(),
                old_root.record.value["generation"].as_u64().unwrap() + 1
            );
            for before in &original {
                if before.record.key.key() == "app" {
                    continue;
                }
                let after = native.load(before.record.key.clone()).await?;
                if before.record.key.key().starts_with("receipt/") {
                    assert!(after.is_none());
                    continue;
                }
                let after = after.expect("all surviving original rows");
                assert_eq!(after.revision.get(), before.revision.get() + 1);
                let mut logical = if after.record.key.key().split('/').nth(5) == Some("checkpoint")
                {
                    StorageFaults::logical_checkpoint(&native, &after.record).await
                } else {
                    (*after.record.value).clone()
                };
                logical["epoch"] = Value::from("app/v6");
                assert_eq!(
                    logical,
                    *before.record.value,
                    "exact reconciled row {}",
                    before.record.key.key()
                );
            }
            let workspace = app
                .workspace(
                    expected["workspace"]
                        .as_str()
                        .or_else(|| expected["head"]["workspace_id"].as_str())
                        .unwrap()
                        .parse()?,
                )
                .await?;
            let workflow = workspace.workflow("stream-failure".parse()?).await?;
            let run = workflow
                .open_run(RunSelector::Latest(
                    expected["run"]
                        .as_str()
                        .or_else(|| expected["head"]["id"].as_str())
                        .unwrap()
                        .parse()?,
                ))
                .await?;
            assert_eq!(
                run.snapshot().await?.checkpoint,
                expected["checkpoint"]
                    .as_u64()
                    .or_else(|| expected["head"]["sequence"].as_u64())
                    .unwrap()
            );
            assert_eq!(state.prompts.load(Ordering::SeqCst), 0);
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        }))
        .catch_unwind()
        .await;
        let cleanup = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(20),
        }))
        .catch_unwind()
        .await;
        if !matches!(&cleanup, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
            loop {
                let joined = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(60),
                }))
                .catch_unwind()
                .await;
                eprintln!(
                    "v6-readback phase=settlement original={cleanup:?} subsequent={joined:?}"
                );
                if matches!(&joined, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
                    break;
                }
            }
        }
        drop(app);
        drop(native);
        match checked {
            Ok(result) => result??,
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
        Ok(())
    }

    async fn assert_v6_migration_failed_readback_preserves_publication(
        mismatched_revision: bool,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let fixture = AppV6Fixture::copy("unknown-history");
        let expected = fixture.expected();
        let native = StateStore::connect(fixture.config()).await?;
        let original = fixture.records(&native).await;
        let old_root = original
            .iter()
            .find(|row| row.record.key.key() == "app")
            .unwrap();
        let selected = original
            .iter()
            .find(|row| {
                row.record.key.key().split('/').nth(5) == Some("checkpoint")
                    && row.record.value["checkpoint"]["settlements"]
                        .as_array()
                        .is_some_and(|rows| !rows.is_empty())
            })
            .expect("the genuine Unknown history includes a leaf-bearing checkpoint");
        let (faults, mut store) = StorageFaults::open().await?;
        store.inner = native.clone();
        *faults.migration_fault.lock().unwrap() = MigrationFault::LoseReplyBeforeReadback {
            after: selected.record.key.key().to_owned(),
            mismatched_revision,
        };
        let (state, agent) = Self::open()?;
        state.install_golden_lower_inventory(&expected["lower_inventory"])?;
        let opened = App::open(AppConfig::new(
            store,
            vec![AgentSpec::new("faulty".into(), "v1".into(), agent)],
            vec![WorkflowDefinition::parse(
                &expected["definition"].to_string(),
            )?],
        ))
        .await;
        if let Ok(mut app) = opened {
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(20),
            })
            .await?;
            panic!("failed exact readback cannot turn an uncertain consuming commit into success");
        }
        assert!(
            matches!(opened, Err(AppError::Integration(message)) if message == "app state-store operation failed"),
            "the original Backend commit error wins over secondary Timeout or Conflict"
        );
        assert_eq!(
            state.shutdowns.load(Ordering::SeqCst),
            1,
            "failed construction joins its lower owner"
        );
        let progress = match &*faults.migration_fault.lock().unwrap() {
            MigrationFault::ReadbackRejected {
                progress,
                reads,
                rollbacks,
                commits,
                ..
            } => {
                assert_eq!(
                    (*reads, *rollbacks, *commits),
                    (1, 1, 0),
                    "one real observation and completed rollback, no retry or consuming readback commit"
                );
                progress.clone()
            }
            _ => panic!("the real publication must precede its controlled failed readback"),
        };
        let stopped = native.load(progress.key.clone()).await?.unwrap();
        assert_eq!(stopped.record.value, progress.value);
        assert_eq!(stopped.record.value["migration"]["state"], "migrating");
        assert_eq!(
            stopped.record.value["migration"]["phase"]["after"],
            selected.record.key.key()
        );
        assert_eq!(
            stopped.record.value["generation"],
            old_root.record.value["generation"]
        );
        let partial = fixture.records(&native).await;
        let converted = partial
            .iter()
            .filter(|row| {
                row.record.key.key().split('/').nth(5) == Some("checkpoint")
                    && row.record.value["epoch"] == "app/v7"
            })
            .count();
        let expected_converted = original
            .iter()
            .filter(|row| {
                row.record.key.key().split('/').nth(5) == Some("checkpoint")
                    && row.record.key.key() <= selected.record.key.key()
            })
            .count();
        assert_eq!(converted, expected_converted);
        assert_eq!(
            stopped.revision.get(),
            old_root.revision.get() + converted as u64 + 2,
            "only claim, phase entry and acknowledged checkpoint batches were published"
        );
        let accepted = native.load(selected.record.key.clone()).await?.unwrap();
        assert_eq!(accepted.revision.get(), selected.revision.get() + 1);
        let pages = accepted.record.value["checkpoint"]["settlements"]["pages"]
            .as_array()
            .unwrap();
        assert_eq!(pages.len(), 1);
        let publication = pages[0].as_u64().unwrap();
        assert_eq!(
            publication,
            accepted.record.value["checkpoint"]["sequence"]
                .as_u64()
                .unwrap()
        );
        let prefix = selected
            .record
            .key
            .key()
            .split("/checkpoint/")
            .next()
            .unwrap();
        let leaf = native
            .load(StateKey::new(
                "app-sdk",
                format!("{prefix}/ledger/00/{publication:020}"),
            )?)
            .await?
            .unwrap();
        assert_eq!(leaf.revision.get(), 1);
        assert_eq!(
            leaf.record.value["entries"],
            selected.record.value["checkpoint"]["settlements"]
        );
        assert_eq!(state.prompts.load(Ordering::SeqCst), 0);
        assert_eq!(state.aborts.load(Ordering::SeqCst), 0);
        // This is a new public construction after the failed constructor has
        // joined. No lower effect or uncertain transaction is replayed by us.
        let mut app = App::open(AppConfig::new(
            native.clone(),
            vec![AgentSpec::new("faulty".into(), "v1".into(), state.agent())],
            vec![WorkflowDefinition::parse(
                &expected["definition"].to_string(),
            )?],
        ))
        .await?;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(120), async {
            let root = native.load(old_root.record.key.clone()).await?.unwrap();
            let checkpoints = original
                .iter()
                .filter(|row| row.record.key.key().split('/').nth(5) == Some("checkpoint"))
                .count();
            let others = original.len() - checkpoints - 1;
            assert_eq!(
                root.revision.get(),
                old_root.revision.get() + (checkpoints + others.div_ceil(256) + 6) as u64
            );
            assert_eq!(root.record.value["migration"]["state"], "ready");
            assert_eq!(
                root.record.value["generation"].as_u64().unwrap(),
                old_root.record.value["generation"].as_u64().unwrap() + 1
            );
            for before in &original {
                if before.record.key.key() == "app" {
                    continue;
                }
                let after = native.load(before.record.key.clone()).await?;
                if before.record.key.key().starts_with("receipt/") {
                    assert!(after.is_none());
                    continue;
                }
                let after = after.expect("every surviving legacy row");
                assert_eq!(after.revision.get(), before.revision.get() + 1);
                let mut logical = if after.record.key.key().split('/').nth(5) == Some("checkpoint")
                {
                    StorageFaults::logical_checkpoint(&native, &after.record).await
                } else {
                    (*after.record.value).clone()
                };
                logical["epoch"] = Value::from("app/v6");
                assert_eq!(logical, *before.record.value);
            }
            for before in [&accepted, &leaf] {
                let after = native.load(before.record.key.clone()).await?.unwrap();
                assert_eq!(after.record.value, before.record.value);
                assert_eq!(
                    after.revision, before.revision,
                    "acknowledged batch is never rewritten on explicit resume"
                );
            }
            let workspace = app
                .workspace(expected["head"]["workspace_id"].as_str().unwrap().parse()?)
                .await?;
            let workflow = workspace.workflow("stream-failure".parse()?).await?;
            let run = workflow
                .open_run(RunSelector::Latest(
                    expected["head"]["id"].as_str().unwrap().parse()?,
                ))
                .await?;
            assert_eq!(
                run.snapshot().await?.checkpoint,
                expected["head"]["sequence"].as_u64().unwrap()
            );
            assert_eq!(state.prompts.load(Ordering::SeqCst), 0);
            assert_eq!(state.aborts.load(Ordering::SeqCst), 0);
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        }))
        .catch_unwind()
        .await;
        let cleanup = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(20),
        }))
        .catch_unwind()
        .await;
        if !matches!(&cleanup, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
            loop {
                let joined = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(60),
                }))
                .catch_unwind()
                .await;
                eprintln!(
                    "v6-failed-readback phase=settlement original={cleanup:?} subsequent={joined:?}"
                );
                if matches!(&joined, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
                    break;
                }
            }
        }
        drop(app);
        drop(native);
        match checked {
            Ok(result) => result??,
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 2);
        Ok(())
    }

    async fn assert_concurrent_v6_migration_claims() -> Result<(), Box<dyn Error + Send + Sync>> {
        let fixture = AppV6Fixture::copy("notification-history");
        let expected = fixture.expected();
        let native = StateStore::connect(fixture.config()).await?;
        let independent = StateStore::connect(fixture.config()).await?;
        let original = fixture.records(&native).await;
        let old_root = original
            .iter()
            .find(|row| row.record.key.key() == "app")
            .unwrap();
        let (faults, _) = StorageFaults::open().await?;
        *faults.migration_fault.lock().unwrap() = MigrationFault::ConcurrentClaims {
            entered: 0,
            succeeded: 0,
            conflicted: 0,
            released: false,
        };
        let (left_state, left_agent) = Self::open()?;
        let (right_state, right_agent) = Self::open()?;
        left_state.install_golden_lower_inventory(&expected["lower_inventory"])?;
        right_state.install_golden_lower_inventory(&expected["lower_inventory"])?;
        let left = AppConfig::new(
            FaultStateStore {
                inner: native.clone(),
                faults: Arc::clone(&faults),
            },
            vec![AgentSpec::new("faulty".into(), "v1".into(), left_agent)],
            vec![WorkflowDefinition::parse(
                &expected["definition"].to_string(),
            )?],
        );
        let right = AppConfig::new(
            FaultStateStore {
                inner: independent.clone(),
                faults: Arc::clone(&faults),
            },
            vec![AgentSpec::new("faulty".into(), "v1".into(), right_agent)],
            vec![WorkflowDefinition::parse(
                &expected["definition"].to_string(),
            )?],
        );
        let mut opening = Box::pin(async { tokio::join!(App::open(left), App::open(right)) });
        let mut early = None;
        let admitted = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(120), async {
            loop {
                if matches!(&*faults.migration_fault.lock().unwrap(), MigrationFault::ConcurrentClaims { entered: 2, .. }) { break; }
                tokio::select! {
                    _ = faults.admission_commit_entered.notified() => {}
                    result = &mut opening => {
                        early = Some(result);
                        panic!("both real migration claims must reach native commit before either reply");
                    }
                }
            }
        })).catch_unwind().await;
        // Both exact native snapshots are retained by their consuming commits.
        // Releasing the same owners makes Lance choose the real conflict loser.
        if let MigrationFault::ConcurrentClaims { released, .. } =
            &mut *faults.migration_fault.lock().unwrap()
        {
            *released = true;
        }
        faults.admission_commit_release.notify_waiters();
        let outcomes = match early {
            Some(outcomes) => outcomes,
            None => opening.as_mut().await,
        };
        drop(opening);
        let mut outcomes = [outcomes.0, outcomes.1];
        let checked = AssertUnwindSafe(async {
            match admitted {
                Ok(result) => result?,
                Err(panic) => std::panic::resume_unwind(panic),
            }
            assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
            assert_eq!(
                outcomes
                    .iter()
                    .filter(|outcome| matches!(outcome, Err(AppError::Conflict(_))))
                    .count(),
                1
            );
            assert!(matches!(
                &*faults.migration_fault.lock().unwrap(),
                MigrationFault::ConcurrentClaims {
                    entered: 2,
                    succeeded: 1,
                    conflicted: 1,
                    released: true,
                }
            ));
            let current = fixture.records(&native).await;
            let root = current
                .iter()
                .find(|row| row.record.key.key() == "app")
                .unwrap();
            let checkpoints = original
                .iter()
                .filter(|row| row.record.key.key().split('/').nth(5) == Some("checkpoint"))
                .count();
            let other = original.len() - checkpoints - 1;
            assert_eq!(
                root.revision.get(),
                old_root.revision.get() + (checkpoints + other.div_ceil(256) + 6) as u64
            );
            assert_eq!(
                root.record.value["generation"].as_u64().unwrap(),
                old_root.record.value["generation"].as_u64().unwrap() + 1
            );
            assert!(
                current
                    .iter()
                    .all(|row| row.record.value["epoch"] == "app/v7")
            );
            for old in &original {
                if old.record.key.key() == "app" || old.record.key.key().starts_with("receipt/") {
                    continue;
                }
                let row = current
                    .iter()
                    .find(|row| row.record.key == old.record.key)
                    .unwrap();
                assert_eq!(
                    row.revision.get(),
                    old.revision.get() + 1,
                    "one physical conversion despite two original claims"
                );
            }
            let winner = outcomes
                .iter()
                .find_map(|outcome| outcome.as_ref().ok())
                .unwrap();
            let workspace = winner
                .workspace(expected["workspace"].as_str().unwrap().parse()?)
                .await?;
            let workflow = workspace.workflow("stream-failure".parse()?).await?;
            let run = workflow
                .open_run(RunSelector::Latest(
                    expected["run"].as_str().unwrap().parse()?,
                ))
                .await?;
            assert_eq!(
                run.snapshot().await?.checkpoint,
                expected["checkpoint"].as_u64().unwrap()
            );
            assert_eq!(
                left_state.prompts.load(Ordering::SeqCst)
                    + right_state.prompts.load(Ordering::SeqCst),
                0
            );
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        })
        .catch_unwind()
        .await;
        let mut cleanup_failure = None;
        for app in outcomes.iter_mut().flatten() {
            let cleanup = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(20),
            }))
            .catch_unwind()
            .await;
            if !matches!(&cleanup, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
                loop {
                    let joined = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_secs(60),
                    }))
                    .catch_unwind()
                    .await;
                    eprintln!(
                        "v6-concurrent phase=settlement original={cleanup:?} subsequent={joined:?}"
                    );
                    if matches!(&joined, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
                        break;
                    }
                }
            }
            if !matches!(&cleanup, Ok(Ok(()))) {
                cleanup_failure = Some(cleanup);
            }
        }
        drop(outcomes);
        drop(independent);
        drop(native);
        match checked {
            Ok(result) => result?,
            Err(panic) => std::panic::resume_unwind(panic),
        }
        if let Some(failure) = cleanup_failure {
            failure.unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
        }
        assert_eq!(left_state.shutdowns.load(Ordering::SeqCst), 1);
        assert_eq!(right_state.shutdowns.load(Ordering::SeqCst), 1);
        Ok(())
    }

    async fn assert_settlement_read_rollback_vetoes_retry(
        primary: StateStoreError,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let fixture = AppV6Fixture::copy("notification-history");
        let expected = fixture.expected();
        let native = StateStore::connect(fixture.config()).await?;
        let (faults, mut store) = StorageFaults::open().await?;
        store.inner = native.clone();
        let (state, agent) = Self::open()?;
        state.install_golden_lower_inventory(&expected["lower_inventory"])?;
        let mut app = App::open(AppConfig::new(
            store,
            vec![AgentSpec::new("faulty".into(), "v1".into(), agent)],
            vec![WorkflowDefinition::parse(
                &expected["definition"].to_string(),
            )?],
        ))
        .await?;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(120), async {
            let workspace = app.workspace(expected["workspace"].as_str().unwrap().parse()?).await?;
            let workflow = workspace.workflow("stream-failure".parse()?).await?;
            let mut run = workflow.open_run(RunSelector::Latest(expected["run"].as_str().unwrap().parse()?)).await?;
            let snapshot = run.snapshot().await?;
            let scope = format!("{}/{}/{}/{}", workspace.id(), workflow.id(), run.id(), snapshot.occurrence);
            let checkpoint = native.load(StateKey::new("app-sdk", format!("history/{scope}/checkpoint/{:020}", snapshot.checkpoint))?).await?.unwrap();
            let publication = checkpoint.record.value["checkpoint"]["settlements"]["pages"][0].as_u64().unwrap();
            let original = fixture.records(&native).await;
            *faults.settlement_rollback_fault.lock().unwrap() = Some(SettlementRollbackFault {
                key: StateKey::new("app-sdk", format!("history/{scope}/ledger/00/{publication:020}"))?,
                primary: primary.clone(),
                remaining: usize::MAX,
                attempts: 0, loads: 0, rollbacks: 0, commits: 0,
            });
            let result = run.delete(snapshot.checkpoint).await;
            let fault = faults.settlement_rollback_fault.lock().unwrap().take().unwrap();
            match &primary {
                StateStoreError::TransactionConflict { .. } => assert!(matches!(&result,
                    Err(AppError::Conflict(message)) if message == "app state transaction conflicted")),
                StateStoreError::Unavailable { .. } => assert!(matches!(&result,
                    Err(AppError::Integration(message)) if message == "app state-store operation failed")),
                _ => unreachable!("only the two explicit portable retry paths are selected"),
            }
            assert_eq!((fault.attempts, fault.loads, fault.rollbacks, fault.commits), (1, 1, 1, 0),
                "completed rollback failure vetoes automatic mutation retry");
            let after = fixture.records(&native).await;
            assert_eq!(after.len(), original.len());
            for (actual, before) in after.iter().zip(&original) {
                assert_eq!(actual.record.key, before.record.key);
                assert_eq!(actual.record.value, before.record.value);
                assert_eq!(actual.revision, before.revision, "no deletion seal, checkpoint, page or receipt publishes");
            }
            assert_eq!(state.prompts.load(Ordering::SeqCst), 0);
            run.delete(snapshot.checkpoint).await?;
            assert!(matches!(workflow.open_run(RunSelector::Latest(run.id().clone())).await, Err(AppError::NotFound(_))));
            let mut next = workflow.prepare(WorkflowState::default()).await?;
            let prepared = next.snapshot().await?;
            assert_eq!(prepared.checkpoint, 0);
            assert!(matches!(prepared.status, RunStatus::Paused));
            next.cancel(prepared.checkpoint).await?;
            let terminal = next.wait().await?;
            assert!(matches!(terminal.status, RunStatus::Cancelled));
            next.delete(terminal.checkpoint).await?;
            assert_eq!(state.prompts.load(Ordering::SeqCst), 0);
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        })).catch_unwind().await;
        faults.settlement_rollback_fault.lock().unwrap().take();
        let cleanup = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(20),
        }))
        .catch_unwind()
        .await;
        if !matches!(&cleanup, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
            loop {
                let joined = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(60),
                }))
                .catch_unwind()
                .await;
                eprintln!(
                    "settlement-rollback phase=settlement original={cleanup:?} subsequent={joined:?}"
                );
                if matches!(&joined, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
                    break;
                }
            }
        }
        drop(app);
        drop(native);
        match checked {
            Ok(result) => result??,
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
        Ok(())
    }

    async fn assert_v6_migration_preflight() -> Result<(), Box<dyn Error + Send + Sync>> {
        for case in [
            "missing_checkpoint",
            "corrupt_checkpoint",
            "native_revision",
            "root_revision",
            "projection_revision",
        ] {
            let fixture = AppV6Fixture::copy(if case == "projection_revision" {
                "sealed-deletion"
            } else {
                "notification-history"
            });
            let expected = fixture.expected();
            let native = StateStore::connect(fixture.config()).await?;
            let original = fixture.records(&native).await;
            let selected = if case == "root_revision" {
                original
                    .iter()
                    .find(|row| row.record.key.key() == "app")
                    .unwrap()
            } else if case == "projection_revision" {
                let selected = original
                    .iter()
                    .find(|row| {
                        row.record
                            .key
                            .key()
                            .ends_with("/projection/checkpoint/00000000000000000391")
                    })
                    .unwrap();
                assert_eq!(selected.record.value["previous"]["sequence"], 390);
                assert_eq!(
                    selected.record.value["previous"]["occurrence"],
                    expected["occurrence"]
                );
                assert!(selected.record.value["next"].is_null());
                let previous = original
                    .iter()
                    .find(|row| {
                        row.record
                            .key
                            .key()
                            .ends_with("/projection/checkpoint/00000000000000000390")
                    })
                    .unwrap();
                assert_eq!(previous.record.value["next"]["sequence"], 391);
                selected
            } else {
                original
                    .iter()
                    .filter(|row| row.record.key.key().split('/').nth(5) == Some("checkpoint"))
                    .nth(270)
                    .unwrap()
            };
            if matches!(case, "missing_checkpoint" | "corrupt_checkpoint") {
                let mut transaction = native.begin_transaction().await?;
                if case == "missing_checkpoint" {
                    transaction.delete(selected.record.key.clone()).await?;
                } else {
                    let mut corrupt = (*selected.record.value).clone();
                    corrupt["checkpoint"]["sequence"] = Value::from(u64::MAX);
                    transaction
                        .put(StateRecord::new(selected.record.key.clone(), corrupt))
                        .await?;
                }
                transaction.commit().await?;
            }
            let before = fixture.records(&native).await;
            let (faults, mut store) = StorageFaults::open().await?;
            store.inner = native.clone();
            if matches!(
                case,
                "native_revision" | "root_revision" | "projection_revision"
            ) {
                // Deliberate lower-boundary revision response, not a claim that
                // billions of native mutations were executed. Values remain real.
                *faults.migration_fault.lock().unwrap() = MigrationFault::RevisionOverflow {
                    key: selected.record.key.clone(),
                    revision: StateRevision::new(if case == "native_revision" {
                        u64::MAX
                    } else {
                        u64::MAX - 1
                    })?,
                };
            }
            let (state, agent) = Self::open()?;
            let (agents, definitions) = if case == "projection_revision" {
                (Vec::new(), Vec::new())
            } else {
                state.install_golden_lower_inventory(&expected["lower_inventory"])?;
                (
                    vec![AgentSpec::new("faulty".into(), "v1".into(), agent)],
                    vec![WorkflowDefinition::parse(
                        &expected["definition"].to_string(),
                    )?],
                )
            };
            let result = App::open(AppConfig::new(store, agents, definitions)).await;
            if let Ok(mut app) = result {
                app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(20),
                })
                .await?;
                panic!(
                    "{case}: malformed or exhausted legacy state must be rejected before migration"
                );
            }
            if matches!(
                case,
                "native_revision" | "root_revision" | "projection_revision"
            ) {
                assert!(matches!(result, Err(AppError::Conflict(_))));
            } else {
                assert!(matches!(result, Err(AppError::Integration(_))));
            }
            let after = fixture.records(&native).await;
            assert_eq!(
                after.len(),
                before.len(),
                "{case}: no initial root or data mutation"
            );
            for (after, before) in after.iter().zip(&before) {
                assert_eq!(after.record.key, before.record.key, "{case}");
                assert_eq!(after.record.value, before.record.value, "{case}");
                assert_eq!(after.revision, before.revision, "{case}");
            }
            assert_eq!(state.prompts.load(Ordering::SeqCst), 0);
            assert_eq!(
                state.shutdowns.load(Ordering::SeqCst),
                usize::from(case != "projection_revision")
            );
            drop(native);
        }
        Ok(())
    }

    async fn assert_settlement_leaf_freshness() -> Result<(), Box<dyn Error + Send + Sync>> {
        let fixture = AppV6Fixture::copy("notification-history");
        let expected = fixture.expected();
        let native = StateStore::connect(fixture.config()).await?;
        let peer = StateStore::connect(fixture.config()).await?;
        let (faults, mut store) = StorageFaults::open().await?;
        store.inner = native.clone();
        let (state, agent) = Self::open()?;
        state.install_golden_lower_inventory(&expected["lower_inventory"])?;
        let mut app = App::open(AppConfig::new(
            store,
            vec![AgentSpec::new("faulty".into(), "v1".into(), agent)],
            vec![WorkflowDefinition::parse(
                &expected["definition"].to_string(),
            )?],
        ))
        .await?;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(120), async {
            let workspace = app.workspace(expected["workspace"].as_str().unwrap().parse()?).await?;
            let workflow = workspace.workflow("stream-failure".parse()?).await?;
            let run = workflow.open_run(RunSelector::Latest(expected["run"].as_str().unwrap().parse()?)).await?;
            let warm = run.snapshot().await?;
            let scope = format!("{}/{}/{}/{}", workspace.id(), workflow.id(), run.id(), warm.occurrence);
            let head_key = StateKey::new("app-sdk", format!("run/{scope}"))?;
            let head = native.load(head_key.clone()).await?.unwrap();
            let checkpoint = native.load(StateKey::new("app-sdk", format!("history/{scope}/checkpoint/{:020}", warm.checkpoint))?).await?.unwrap();
            let publication = checkpoint.record.value["checkpoint"]["settlements"]["pages"][0].as_u64().unwrap();
            let leaf = native.load(StateKey::new("app-sdk", format!("history/{scope}/ledger/00/{publication:020}"))?).await?.unwrap();
            assert_eq!(leaf.record.value["entries"].as_array().unwrap().len(), 64);
            assert_eq!(checkpoint.record.value["checkpoint"]["settlements"]["count"], 96);
            for case in ["changed_entry", "missing", "foreign_scope", "foreign_index", "extra_field", "short_nonfinal", "wrong_count", "future_reference", "returned_key", "returned_namespace"] {
                Self::assert_applied_snapshot(&run.snapshot().await?, &warm);
                let changes_checkpoint = matches!(case, "wrong_count" | "future_reference");
                let original = if changes_checkpoint { &checkpoint.record } else { &leaf.record };
                let controlled_response = matches!(case, "returned_key" | "returned_namespace");
                if controlled_response {
                    *faults.notification_corruption.lock().unwrap() = if case == "returned_key" { NotificationCorruption::LeafKey } else { NotificationCorruption::LeafNamespace };
                } else {
                    let mut transaction = peer.begin_transaction().await?;
                    if case == "missing" { transaction.delete(original.key.clone()).await?; }
                    else {
                        let mut corrupt = (*original.value).clone();
                        match case {
                            "changed_entry" => corrupt["entries"][0][0] = Value::from(u64::MAX),
                            "foreign_scope" => corrupt["id"] = Value::from(format!("run_{}", uuid::Uuid::new_v4())),
                            "foreign_index" => corrupt["index"] = Value::from(1),
                            "extra_field" => { corrupt.as_object_mut().unwrap().insert("unexpected".into(), Value::Null); },
                            "short_nonfinal" => { corrupt["entries"].as_array_mut().unwrap().pop(); },
                            "wrong_count" => corrupt["checkpoint"]["settlements"]["count"] = Value::from(95),
                            "future_reference" => corrupt["checkpoint"]["settlements"]["pages"][0] = Value::from(u64::MAX),
                            _ => unreachable!(),
                        }
                        transaction.put(StateRecord::new(original.key.clone(), corrupt)).await?;
                    }
                    transaction.commit().await?;
                }
                let observed = run.snapshot().await;
                let receipt = &expected["receipts"][0];
                let mutation = run.resolve(receipt["sequence"].as_u64().unwrap(), receipt["generation"].as_u64().unwrap(), Resolution::Notification(NotificationResolution::Allow)).await;
                *faults.notification_corruption.lock().unwrap() = NotificationCorruption::None;
                if !controlled_response {
                    let mut transaction = peer.begin_transaction().await?;
                    transaction.put(original.clone()).await?;
                    transaction.commit().await?;
                }
                assert!(matches!(observed, Err(AppError::Integration(_))), "{case}: fresh leaf evidence must invalidate a warm logical cache");
                assert!(matches!(mutation, Err(AppError::Integration(_))), "{case}: invalid leaf evidence must reject before receipt mutation");
                let unchanged = native.load(head_key.clone()).await?.unwrap();
                assert_eq!(unchanged.record.value, head.record.value, "{case}");
                assert_eq!(unchanged.revision, head.revision, "{case}");
                Self::assert_applied_snapshot(&run.snapshot().await?, &warm);
                eprintln!("settlement-leaf phase=rejected case={case} native_head_unchanged=true later_read=success");
            }
            let receipt = &expected["receipts"][0];
            let replay = run.resolve(receipt["sequence"].as_u64().unwrap(), receipt["generation"].as_u64().unwrap(), Resolution::Notification(NotificationResolution::Allow)).await?;
            assert_eq!(replay.checkpoint, receipt["checkpoint"].as_u64().unwrap());
            assert_eq!(state.prompts.load(Ordering::SeqCst), 0);
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        })).catch_unwind().await;
        *faults.notification_corruption.lock().unwrap() = NotificationCorruption::None;
        let cleanup = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(20),
        }))
        .catch_unwind()
        .await;
        if !matches!(&cleanup, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
            loop {
                let joined = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(60),
                }))
                .catch_unwind()
                .await;
                eprintln!(
                    "settlement-leaf phase=settlement original={cleanup:?} subsequent={joined:?}"
                );
                if matches!(&joined, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
                    break;
                }
            }
        }
        drop(app);
        drop(peer);
        drop(native);
        match checked {
            Ok(result) => result??,
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
        Ok(())
    }

    async fn assert_input_rollback_failure_stops_its_public_owner(
        delete_workspace: bool,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let (faults, store) = StorageFaults::open().await?;
        let native = store.inner.clone();
        let (state, agent) = Self::open()?;
        let definition = WorkflowDefinition::parse(&serde_json::json!({
            "schema":"app-sdk.workflow/v2", "id":"rollback-input", "revision":"v1",
            "entry":"input", "state":{"value":"original"}, "required":["value"],
            "reducers":{"value":"reject_concurrent"},
            "nodes":[{"id":"input", "deadline_ms":60000, "step":{"kind":"input",
                "target":"value", "message":[{"kind":"text", "text":"retain original input"}]}}],
            "routes":{"input":{"mode":"first","branches":[{"target":{"kind":"end"}}]}}
        }).to_string())?;
        let mut app = App::open(AppConfig::new(
            store,
            vec![AgentSpec::new("faulty".into(), "v1".into(), agent)],
            vec![definition],
        ))
        .await?;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(20), async {
            let mut workspace = app.create_workspace(WorkspaceConfig {
                name: None, root: PathBuf::from("/tmp"),
            }).await?;
            let revision = workspace.snapshot().await?.revision;
            let workflow = workspace.workflow("rollback-input".parse()?).await?;
            let run = workflow.start(ExecutionOptions::default()).await?;
            let waiting = run.wait().await?;
            let [Interaction::Input { sequence, generation, .. }] = waiting.interactions.as_slice()
                else { panic!("the original public Input is pending before the fault"); };
            let scope = format!("{}/{}/{}/{}", workspace.id(), workflow.id(), run.id(), waiting.occurrence);
            let head_key = StateKey::new("app-sdk", format!("run/{scope}"))?;
            let head = native.load(head_key.clone()).await?.unwrap();
            let checkpoint = native.load(StateKey::new("app-sdk", format!("history/{scope}/checkpoint/{:020}", waiting.checkpoint))?).await?.unwrap();
            let publication = checkpoint.record.value["checkpoint"]["settlements"]["pages"][0].as_u64().unwrap();
            *faults.settlement_rollback_fault.lock().unwrap() = Some(SettlementRollbackFault {
                key: StateKey::new("app-sdk", format!("history/{scope}/ledger/00/{publication:020}"))?,
                primary: if delete_workspace { StateStoreError::timed_out("load", 1) }
                    else { StateStoreError::transaction_conflict("load", "selected Input leaf observation conflict") },
                remaining: 2,
                attempts: 0, loads: 0, rollbacks: 0, commits: 0,
            });
            if delete_workspace {
                let result = workspace.delete(revision).await;
                if !matches!(&result, Err(AppError::Timeout(message)) if message == "app state-store operation timed out") {
                    let counts = faults.settlement_rollback_fault.lock().unwrap().as_ref()
                        .map(|fault| (fault.attempts, fault.loads, fault.rollbacks, fault.commits, fault.remaining));
                    eprintln!("rollback-owner phase=workspace-result error={:?} fault_attempt_load_rollback_commit_remaining={counts:?} prompts={} conversations={} shutdowns={}",
                        result.as_ref().err(), state.prompts.load(Ordering::SeqCst),
                        state.conversations.load(Ordering::SeqCst), state.shutdowns.load(Ordering::SeqCst));
                }
                assert!(matches!(&result, Err(AppError::Timeout(message)) if message == "app state-store operation timed out"),
                    "workspace reconciliation preserves the original timeout instead of replaying cancellation");
            } else {
                let result = run.resolve(*sequence, *generation, Resolution::Input(Value::from("accepted"))).await;
                assert!(matches!(&result, Err(AppError::Conflict(message)) if message == "app state transaction conflicted"),
                    "public resolution preserves the original conflict instead of replaying publication");
            }
            {
                let fault = faults.settlement_rollback_fault.lock().unwrap();
                let fault = fault.as_ref().unwrap();
                assert_eq!((fault.loads, fault.rollbacks, fault.commits), (1, 1, 0));
                assert_eq!(fault.remaining, 1, "a second failure allowance is never consumed");
            }
            let after = native.load(head_key).await?.unwrap();
            assert_eq!(after.revision, head.revision);
            assert_eq!(after.record.value, head.record.value, "failed rollback grants no checkpoint publication");
            assert_eq!(state.prompts.load(Ordering::SeqCst), 0);
            assert_eq!(state.conversations.load(Ordering::SeqCst), 0);
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        })).catch_unwind().await;
        // This owner uses CancelAndDrain only. A returned Timeout is a
        // retained outcome only while measured physical elapsed time is below
        // the inner 60s observer deadline; a non-yielding poll may delay Tokio's
        // outer timeout, so the outer 20s bound alone is not joining evidence.
        let cleanup_started = std::time::Instant::now();
        let cleanup = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(20),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(60),
            }),
        ))
        .catch_unwind()
        .await;
        let cleanup_joined = matches!(
            &cleanup,
            Ok(Ok(Ok(()))) | Ok(Ok(Err(AppError::Integration(_) | AppError::Conflict(_))))
        ) || matches!(&cleanup, Ok(Ok(Err(AppError::Timeout(_))))
                if cleanup_started.elapsed() < Duration::from_secs(60));
        if !cleanup_joined {
            loop {
                let join_started = std::time::Instant::now();
                let joined = AssertUnwindSafe(tokio::time::timeout(
                    Duration::from_secs(20),
                    app.shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_secs(60),
                    }),
                ))
                .catch_unwind()
                .await;
                let join_elapsed = join_started.elapsed();
                eprintln!(
                    "rollback-owner phase=settlement original={cleanup:?} subsequent={joined:?} observation_elapsed={join_elapsed:?}"
                );
                if matches!(
                    &joined,
                    Ok(Ok(Ok(()))) | Ok(Ok(Err(AppError::Integration(_) | AppError::Conflict(_))))
                ) || matches!(&joined, Ok(Ok(Err(AppError::Timeout(_))))
                        if join_elapsed < Duration::from_secs(60))
                {
                    break;
                }
            }
        }
        drop(app);
        faults.settlement_rollback_fault.lock().unwrap().take();
        match checked {
            Ok(result) => result??,
            Err(panic) => std::panic::resume_unwind(panic),
        }
        let outcome = cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
        if delete_workspace {
            assert!(
                matches!(&outcome, Ok(()) | Err(AppError::Timeout(_))),
                "cleanup either succeeds or retains the original timeout after joining"
            );
            if let Err(AppError::Timeout(message)) = outcome {
                assert_eq!(message, "app state-store operation timed out");
            }
        } else {
            assert!(matches!(&outcome, Ok(()) | Err(AppError::Conflict(_))));
            if let Err(AppError::Conflict(message)) = outcome {
                assert_eq!(message, "app state transaction conflicted");
            }
        }
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
        Ok(())
    }

    async fn assert_lower_resolution_rollback_failure_stops_publication()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let (faults, store) = StorageFaults::open().await?;
        let native = store.inner.clone();
        let (state, agent) = Self::open()?;
        let notifications = NotificationSequence::new(1, false);
        *notifications.resolution_fault.lock().unwrap() = NotificationResolutionFault::GateReceipt;
        *state.notifications.lock().unwrap() = Some(Arc::clone(&notifications));
        let mut definition: Value = serde_json::from_str(STREAM_FAILURE)?;
        definition["nodes"][0]["deadline_ms"] = Value::from(60000);
        let mut app = App::open(AppConfig::new(
            store,
            vec![AgentSpec::new("faulty".into(), "v1".into(), agent)],
            vec![WorkflowDefinition::parse(&definition.to_string())?],
        ))
        .await?;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(20), async {
            let workspace = app.create_workspace(WorkspaceConfig { name: None, root: PathBuf::from("/tmp") }).await?;
            let workflow = workspace.workflow("stream-failure".parse()?).await?;
            let run = workflow.start(ExecutionOptions::default()).await?;
            let waiting = run.wait().await?;
            let [Interaction::Notification { sequence, generation, notification }] = waiting.interactions.as_slice()
                else { panic!("one actual notification precedes the controlled storage observation failure"); };
            assert_eq!(notification.id(), &NotificationSequence::id(0));
            let mut resolving = Box::pin(run.resolve(*sequence, *generation,
                Resolution::Notification(NotificationResolution::Allow)));
            tokio::select! {
                _ = notifications.resolution_entered.notified() => {}
                result = resolving.as_mut() => panic!("the actual lower result remains held: {:?}", result.err()),
            }
            assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), 1);
            assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), 0);
            let scope = format!("{}/{}/{}/{}", workspace.id(), workflow.id(), run.id(), waiting.occurrence);
            let head_key = StateKey::new("app-sdk", format!("run/{scope}"))?;
            let head = native.load(head_key.clone()).await?.unwrap();
            let sequence = head.record.value["sequence"].as_u64().unwrap();
            let checkpoint = native.load(StateKey::new("app-sdk", format!("history/{scope}/checkpoint/{sequence:020}"))?).await?.unwrap();
            let logical = StorageFaults::logical_checkpoint(&native, &checkpoint.record).await;
            assert_eq!(logical["checkpoint"]["settlements"][0][4][0], 1, "actual Intended publication precedes lower acknowledgement");
            let publication = checkpoint.record.value["checkpoint"]["settlements"]["pages"][0].as_u64().unwrap();
            *faults.settlement_rollback_fault.lock().unwrap() = Some(SettlementRollbackFault {
                key: StateKey::new("app-sdk", format!("history/{scope}/ledger/00/{publication:020}"))?,
                primary: StateStoreError::transaction_conflict("load", "selected lower-outcome leaf observation conflict"),
                remaining: 2,
                attempts: 0, loads: 0, rollbacks: 0, commits: 0,
            });
            notifications.resolution_release.notify_one();
            let result = resolving.await;
            if !matches!(&result, Err(AppError::Conflict(message)) if message == "app state transaction conflicted") {
                let counts = faults.settlement_rollback_fault.lock().unwrap().as_ref()
                    .map(|fault| (fault.attempts, fault.loads, fault.rollbacks, fault.commits, fault.remaining));
                let snapshot = result.as_ref().ok().map(|snapshot|
                    (snapshot.checkpoint, snapshot.interactions.len()));
                eprintln!("rollback-owner phase=lower-publication-result error={:?} snapshot={snapshot:?} fault_attempt_load_rollback_commit_remaining={counts:?} prompts={} prompt_returns={} emitted={} received={} resolution_calls={} resolution_returns={}",
                    result.as_ref().err(), state.prompts.load(Ordering::SeqCst), notifications.prompt_returns.load(Ordering::SeqCst),
                    notifications.emitted.load(Ordering::SeqCst), notifications.received.load(Ordering::SeqCst),
                    notifications.resolution_calls.load(Ordering::SeqCst), notifications.resolution_returns.load(Ordering::SeqCst));
            }
            assert!(matches!(&result, Err(AppError::Conflict(message)) if message == "app state transaction conflicted"));
            {
                let fault = faults.settlement_rollback_fault.lock().unwrap();
                let fault = fault.as_ref().unwrap();
                assert_eq!((fault.loads, fault.rollbacks, fault.commits), (1, 1, 0));
                assert_eq!(fault.remaining, 1);
            }
            let after = native.load(head_key).await?.unwrap();
            assert_eq!(after.revision, head.revision);
            assert_eq!(after.record.value, head.record.value);
            assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), 1);
            assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), 1);
            assert_eq!(*notifications.resolved.lock().unwrap(), vec![NotificationSequence::id(0)]);
            assert_eq!(state.prompts.load(Ordering::SeqCst), 1);
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        })).catch_unwind().await;
        notifications.resolution_release.notify_one();
        notifications.cancelled.store(true, Ordering::SeqCst);
        notifications.changed.notify_waiters();
        let cleanup = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(20),
        }))
        .catch_unwind()
        .await;
        if !matches!(
            &cleanup,
            Ok(Ok(())) | Ok(Err(AppError::Integration(_) | AppError::Conflict(_)))
        ) {
            loop {
                let joined = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(60),
                }))
                .catch_unwind()
                .await;
                eprintln!(
                    "rollback-lower phase=settlement original={cleanup:?} subsequent={joined:?}"
                );
                if matches!(
                    &joined,
                    Ok(Ok(())) | Ok(Err(AppError::Integration(_) | AppError::Conflict(_)))
                ) {
                    break;
                }
            }
        }
        drop(app);
        faults.settlement_rollback_fault.lock().unwrap().take();
        match checked {
            Ok(result) => result??,
            Err(panic) => std::panic::resume_unwind(panic),
        }
        let outcome = cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
        assert!(matches!(&outcome, Ok(()) | Err(AppError::Conflict(_))));
        if let Err(AppError::Conflict(message)) = outcome {
            assert_eq!(message, "app state transaction conflicted");
        }
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
        assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), 1);
        assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), 1);
        Ok(())
    }

    async fn assert_uncertain_resolution_changes_two_pages_atomically()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let (state, agent) = Self::open()?;
        // The next prompt target stays held after64 actual deliveries so the
        // prompt cannot complete while the last accepted reply becomes Unknown.
        let notifications = NotificationSequence::new(65, false);
        *notifications.emission.lock().unwrap() = NotificationEmission::GateBeforeLast;
        *state.notifications.lock().unwrap() = Some(Arc::clone(&notifications));
        let mut definition: Value = serde_json::from_str(STREAM_FAILURE)?;
        definition["nodes"][0]["deadline_ms"] = Value::from(180_000);
        let native =
            StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
        let mut app = App::open(AppConfig::new(
            native.clone(),
            vec![AgentSpec::new("faulty".into(), "v1".into(), agent)],
            vec![WorkflowDefinition::parse(&definition.to_string())?],
        ))
        .await?;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(120), async {
            let workspace = app.create_workspace(WorkspaceConfig { name: None, root: PathBuf::from("/tmp") }).await?;
            let workflow = workspace.workflow("stream-failure".parse()?).await?;
            let run = workflow.start(ExecutionOptions::default()).await?;
            let mut receipts = Vec::new();
            for index in 0..63 {
                let waiting = run.wait().await?;
                let [Interaction::Notification { sequence, generation, notification }] = waiting.interactions.as_slice()
                    else { panic!("each original prefix notification remains actionable"); };
                assert_eq!(notification.id(), &NotificationSequence::id(index));
                let applied = run.resolve(*sequence, *generation,
                    Resolution::Notification(NotificationResolution::Allow)).await?;
                receipts.push((*sequence, *generation, applied));
            }
            let waiting = run.wait().await?;
            let [Interaction::Notification { sequence, generation, notification }] = waiting.interactions.as_slice()
                else { panic!("the64th original notification reaches its genuine decision boundary"); };
            assert_eq!(notification.id(), &NotificationSequence::id(63));
            let target = (*sequence, *generation);
            let scope = format!("{}/{}/{}/{}", workspace.id(), workflow.id(), run.id(), waiting.occurrence);
            *notifications.resolution_fault.lock().unwrap() = NotificationResolutionFault::GateLostReceipt;
            let mut resolving = Box::pin(run.resolve(target.0, target.1,
                Resolution::Notification(NotificationResolution::Allow)));
            tokio::time::timeout(Duration::from_secs(20), async {
                tokio::select! {
                    _ = notifications.resolution_entered.notified() => {}
                    result = resolving.as_mut() => panic!("the original accepted lower reply must remain held: {:?}", result.err()),
                }
                notifications.next_target_entered.notified().await;
            }).await?;
            assert_eq!(notifications.emitted.load(Ordering::SeqCst), 64);
            assert_eq!(notifications.received.load(Ordering::SeqCst), 64);
            assert_eq!(notifications.prompt_returns.load(Ordering::SeqCst), 0);
            assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), 64);
            assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), 63);
            let head = native.load(StateKey::new("app-sdk", format!("run/{scope}"))?).await?.unwrap();
            let sequence = head.record.value["sequence"].as_u64().unwrap();
            let before = native.load(StateKey::new("app-sdk", format!("history/{scope}/checkpoint/{sequence:020}"))?).await?.unwrap();
            let descriptor = &before.record.value["checkpoint"]["settlements"];
            assert_eq!(descriptor["count"], 64);
            let pages = descriptor["pages"].as_array().unwrap();
            assert_eq!(pages.len(), 1);
            let old_publication = pages[0].as_u64().unwrap();
            let old_page = native.load(StateKey::new("app-sdk", format!("history/{scope}/ledger/00/{old_publication:020}"))?).await?.unwrap();
            let logical = StorageFaults::logical_checkpoint(&native, &before.record).await;
            assert_eq!(logical["checkpoint"]["settlements"][63][4][0], 1, "the original Intended receipt is genuinely durable");
            notifications.resolution_release.notify_one();
            let unknown = resolving.await?;
            let [Interaction::Unknown { sequence: unknown_sequence, generation: unknown_generation, .. }] = unknown.interactions.as_slice()
                else { panic!("the original failed lower reply has one truthful Unknown successor"); };
            // The public observation may include later abort reconciliation. Its
            // original target generation names the exact immutable publication
            // that appended Unknown and changed the existing settlement page.
            let publication = *unknown_generation;
            assert!(publication > sequence && publication <= unknown.checkpoint);
            let after = native.load(StateKey::new("app-sdk", format!("history/{scope}/checkpoint/{publication:020}"))?).await?.unwrap();
            let descriptor = &after.record.value["checkpoint"]["settlements"];
            assert_eq!(descriptor["count"], 65);
            let pages = descriptor["pages"].as_array().unwrap();
            assert_eq!(pages.len(), 2);
            assert_eq!(pages, &vec![Value::from(publication), Value::from(publication)],
                "the changed original page and appended Unknown page publish in the same checkpoint");
            for index in 0..2 {
                let leaf = native.load(StateKey::new("app-sdk", format!("history/{scope}/ledger/{index:02}/{publication:020}"))?).await?.unwrap();
                assert_eq!(leaf.revision.get(), 1);
                assert_eq!(leaf.record.value["entries"].as_array().unwrap().len(), if index == 0 { 64 } else { 1 });
            }
            let unchanged = native.load(old_page.record.key.clone()).await?.unwrap();
            assert_eq!(unchanged.revision, old_page.revision);
            assert_eq!(unchanged.record.value, old_page.record.value);
            let logical = StorageFaults::logical_checkpoint(&native, &after.record).await;
            assert_eq!(logical["checkpoint"]["settlements"][63][4][0], 3, "original decision remains Uncertain");
            assert_eq!(logical["checkpoint"]["settlements"][64][0], *unknown_sequence);
            assert_eq!(logical["checkpoint"]["settlements"][64][1], *unknown_generation);
            assert_eq!(logical["checkpoint"]["settlements"][64][3][0], 2, "the appended target is Unknown");
            let replay = run.resolve(target.0, target.1, Resolution::Notification(NotificationResolution::Allow)).await?;
            assert!(matches!(replay.interactions.as_slice(), [Interaction::Unknown { sequence, generation, .. }]
                if sequence == unknown_sequence && generation == unknown_generation));
            for (sequence, generation, applied) in &receipts {
                let replay = run.resolve(*sequence, *generation, Resolution::Notification(NotificationResolution::Allow)).await?;
                Self::assert_applied_snapshot(&replay, applied);
            }
            assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), 64);
            assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), 63);
            assert_eq!(*notifications.resolved.lock().unwrap(), (0..64).map(NotificationSequence::id).collect::<Vec<_>>());
            assert_eq!(notifications.emitted.load(Ordering::SeqCst), 64);
            assert_eq!(notifications.received.load(Ordering::SeqCst), 64);
            run.resolve(*unknown_sequence, *unknown_generation, Resolution::Fail).await?;
            notifications.cancelled.store(true, Ordering::SeqCst);
            notifications.next_target_release.notify_one();
            notifications.changed.notify_waiters();
            assert!(matches!(run.wait().await?.status, RunStatus::Failed(WorkflowFailure::Agent(_))));
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        })).catch_unwind().await;
        notifications.cancelled.store(true, Ordering::SeqCst);
        notifications.resolution_release.notify_one();
        notifications.next_target_release.notify_one();
        notifications.changed.notify_waiters();
        let cleanup = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(20),
        }))
        .catch_unwind()
        .await;
        if !matches!(&cleanup, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
            loop {
                let joined = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(60),
                }))
                .catch_unwind()
                .await;
                eprintln!(
                    "cross-page-unknown phase=settlement original={cleanup:?} subsequent={joined:?}"
                );
                if matches!(&joined, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
                    break;
                }
            }
        }
        drop(app);
        match checked {
            Ok(result) => result??,
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
        assert_eq!(notifications.emitted.load(Ordering::SeqCst), 64);
        assert_eq!(notifications.received.load(Ordering::SeqCst), 64);
        assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), 64);
        assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), 63);
        Ok(())
    }

    async fn assert_older_settlement_page_changes_with_later_pages_retained()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let (state, agent) = Self::exclusive_workspace(true)?;
        let notifications = NotificationSequence::new(128, false);
        *state.notifications.lock().unwrap() = Some(Arc::clone(&notifications));
        let definition = WorkflowDefinition::parse(&serde_json::json!({
            "schema":"app-sdk.workflow/v2", "id":"older-settlement-page", "revision":"v1",
            "state":{"manual":"", "answer":""}, "required":["manual", "answer"],
            "reducers":{"manual":"reject_concurrent", "answer":"reject_concurrent"},
            "entry":"seed", "nodes":[
                {"id":"seed", "step":{"kind":"state", "mutations":[]}},
                {"id":"a-input", "step":{"kind":"input", "target":"manual",
                    "message":[{"kind":"text", "text":"original input"}]}},
                {"id":"z-agent", "deadline_ms":1260000, "step":{"kind":"agent", "agent":"faulty",
                    "prompt":[{"kind":"text", "text":"128 original notifications"}], "text_target":"answer"}}
            ], "routes":{"seed":{"mode":"broadcast", "branches":[
                {"target":{"kind":"node", "id":"z-agent"}},
                {"target":{"kind":"node", "id":"a-input"}}
            ]}}
        }).to_string())?;
        let native =
            StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
        let mut app = App::open(AppConfig::new(
            native.clone(),
            vec![AgentSpec::new("faulty".into(), "v1".into(), agent)],
            vec![definition],
        ))
        .await?;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(1200), async {
            let workspace = app.create_workspace(WorkspaceConfig { name: None, root: PathBuf::from("/tmp") }).await?;
            let workflow = workspace.workflow("older-settlement-page".parse()?).await?;
            let run = workflow.start(ExecutionOptions::default()).await?;
            tokio::time::timeout(Duration::from_secs(20), state.preparation_entered.notified()).await?;
            let initial = tokio::time::timeout(Duration::from_secs(20), run.wait()).await??;
            let [Interaction::Input { sequence: input_sequence, generation: input_generation, .. }] = initial.interactions.as_slice()
                else { panic!("original Input exists before gated lower conversation construction"); };
            let (input_sequence, input_generation) = (*input_sequence, *input_generation);
            assert_eq!(notifications.emitted.load(Ordering::SeqCst), 0);
            *state.preparation_fault.lock().unwrap() = PreparationFault::Pass;
            state.preparation_release.notify_one();
            let scope = format!("{}/{}/{}/{}", workspace.id(), workflow.id(), run.id(), initial.occurrence);
            let mut receipts = Vec::new();
            let mut boundaries = Vec::new();
            for index in 0..128 {
                let waiting = tokio::time::timeout(Duration::from_secs(20), async {
                    loop {
                        let snapshot = run.snapshot().await?;
                        assert!(!matches!(snapshot.status, RunStatus::Failed(_) | RunStatus::Succeeded));
                        if snapshot.interactions.iter().any(|item| matches!(item,
                            Interaction::Notification { notification, .. } if notification.id() == &NotificationSequence::id(index))) {
                            break Ok::<_, AppError>(snapshot);
                        }
                        tokio::task::yield_now().await;
                    }
                }).await??;
                let [Interaction::Input { sequence, generation, .. },
                     Interaction::Notification { sequence: target, generation: target_generation, notification }] = waiting.interactions.as_slice()
                     else { panic!("original Input and current ordered notification remain independently actionable"); };
                assert_eq!((*sequence, *generation), (input_sequence, input_generation));
                assert_eq!(notification.id(), &NotificationSequence::id(index));
                let applied = run.resolve(*target, *target_generation,
                    Resolution::Notification(NotificationResolution::Allow)).await?;
                assert_eq!(applied.id, initial.id);
                assert_eq!(applied.occurrence, initial.occurrence);
                let count = index + 2; // one original pending Input plus notifications
                if matches!(count, 127..=129) {
                    let row = native.load(StateKey::new("app-sdk", format!("history/{scope}/checkpoint/{:020}", applied.checkpoint))?).await?.unwrap();
                    let descriptor = &row.record.value["checkpoint"]["settlements"];
                    assert_eq!(descriptor["count"], count);
                    let references = descriptor["pages"].as_array().unwrap().clone();
                    assert_eq!(references.len(), count.div_ceil(64));
                    let logical = StorageFaults::logical_checkpoint(&native, &row.record).await;
                    let entries = logical["checkpoint"]["settlements"].as_array().unwrap();
                    assert_eq!(entries.len(), count);
                    assert_eq!(entries[0][0], input_sequence);
                    assert_eq!(entries[0][1], input_generation);
                    assert_eq!(entries[0][3], serde_json::json!([0]));
                    assert_eq!(entries[0][4], serde_json::json!([0]));
                    for (entry, expected) in entries[1..].iter().zip(0..=index) {
                        assert_eq!(entry[3][2], NotificationSequence::id(expected).to_string());
                        assert_eq!(entry[4][0], 2);
                    }
                    let mut leaves = Vec::new();
                    for (page, publication) in references.iter().enumerate() {
                        let publication = publication.as_u64().unwrap();
                        let leaf = native.load(StateKey::new("app-sdk", format!("history/{scope}/ledger/{page:02}/{publication:020}"))?).await?.unwrap();
                        assert_eq!(leaf.revision.get(), 1);
                        assert_eq!(leaf.record.value["entries"].as_array().unwrap().len(), (count - page * 64).min(64));
                        leaves.push(leaf);
                    }
                    boundaries.push((count, references, leaves));
                }
                receipts.push((*target, *target_generation, applied));
            }
            assert_eq!(boundaries.iter().map(|row| row.0).collect::<Vec<_>>(), [127,128,129]);
            assert_eq!(boundaries[1].1[0], boundaries[2].1[0]);
            assert_eq!(boundaries[1].1[1], boundaries[2].1[1], "closing page1 keeps its exact reference when page2 begins");
            let before = boundaries.last().unwrap();
            let original_input = run.resolve(input_sequence, input_generation,
                Resolution::Input(Value::from("accepted original input"))).await?;
            let row = native.load(StateKey::new("app-sdk", format!("history/{scope}/checkpoint/{:020}", original_input.checkpoint))?).await?.unwrap();
            let descriptor = &row.record.value["checkpoint"]["settlements"];
            assert_eq!(descriptor["count"], 129, "resolving the older target changes no ledger cardinality");
            let references = descriptor["pages"].as_array().unwrap();
            assert_ne!(references[0], before.1[0]);
            assert_eq!(&references[1..], &before.1[1..], "later unchanged pages retain their original references");
            let logical = StorageFaults::logical_checkpoint(&native, &row.record).await;
            assert_eq!(logical["checkpoint"]["settlements"][0][4][0], 2);
            for (_, _, leaves) in &boundaries {
                for leaf in leaves {
                    let observed = native.load(leaf.record.key.clone()).await?.unwrap();
                    assert_eq!(observed.revision, leaf.revision);
                    assert_eq!(observed.record.value, leaf.record.value, "every historical leaf stays immutable");
                }
            }
            let terminal = tokio::time::timeout(Duration::from_secs(20), run.wait()).await??;
            assert!(matches!(terminal.status, RunStatus::Succeeded));
            assert_eq!(terminal.state.values()["manual"], "accepted original input");
            assert_eq!(terminal.state.values()["answer"], "both approvals accepted");
            assert!(terminal.interactions.is_empty());
            for (sequence, generation, applied) in &receipts {
                let replay = run.resolve(*sequence, *generation,
                    Resolution::Notification(NotificationResolution::Allow)).await?;
                Self::assert_applied_snapshot(&replay, applied);
            }
            let replay = run.resolve(input_sequence, input_generation,
                Resolution::Input(Value::from("accepted original input"))).await?;
            Self::assert_applied_snapshot(&replay, &original_input);
            assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), 128);
            assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), 128);
            assert_eq!(*notifications.resolved.lock().unwrap(), (0..128).map(NotificationSequence::id).collect::<Vec<_>>());
            assert_eq!(state.prompts.load(Ordering::SeqCst), 1);
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        })).catch_unwind().await;
        *state.preparation_fault.lock().unwrap() = PreparationFault::Pass;
        state.preparation_release.notify_one();
        notifications.cancelled.store(true, Ordering::SeqCst);
        notifications.changed.notify_waiters();
        let cleanup = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(20),
        }))
        .catch_unwind()
        .await;
        if !matches!(&cleanup, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
            loop {
                let joined = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(60),
                }))
                .catch_unwind()
                .await;
                eprintln!("older-page phase=settlement original={cleanup:?} subsequent={joined:?}");
                if matches!(&joined, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
                    break;
                }
            }
        }
        drop(app);
        match checked {
            Ok(result) => result??,
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
        Ok(())
    }

    async fn assert_cold_recovery_golden() -> Result<(), Box<dyn Error + Send + Sync>> {
        let retained = std::env::var_os("AI_KIT_APP_GOLDEN_OUTPUT").map(PathBuf::from);
        let directory = retained.clone().unwrap_or_else(|| {
            std::env::temp_dir().join(format!("app-recovery-golden-{}", uuid::Uuid::new_v4()))
        });
        assert!(directory.is_absolute());
        std::fs::create_dir(&directory)?;
        let checked = AssertUnwindSafe(async {
        let historical = serde_json::json!({
            "schema":"app-sdk.workflow/v2", "id":"recovery-history", "revision":"v1",
            "entry":"old", "state":{"value":"v1"}, "required":["value"],
            "reducers":{"value":"reject_concurrent"}, "nodes":[
                {"id":"old", "step":{"kind":"state", "mutations":[
                    {"kind":"assign", "target":"value", "value":{"kind":"literal", "value":"historical v1"}}
                ]}}
            ], "routes":{"old":{"mode":"first","branches":[{"target":{"kind":"end"}}]}}
        });
        let current = serde_json::json!({
            "schema":"app-sdk.workflow/v2", "id":"recovery-history", "revision":"v2",
            "entry":"pending", "state":{"value":"v2"}, "required":["value"],
            "reducers":{"value":"reject_concurrent"}, "nodes":[
                {"id":"pending", "deadline_ms":5000,
                 "step":{"kind":"input", "target":"value", "message":[{"kind":"text", "text":"original pending input"}]}}
            ], "routes":{"pending":{"mode":"first","branches":[{"target":{"kind":"end"}}]}}
        });
        let unknown_definition: Value = serde_json::from_str(STREAM_FAILURE)?;
        let native = StateStore::connect(StateStoreConfig::Lance(
            LanceStateStoreConfig::directory(directory.join("state")))).await?;
        let (first_state, first_agent) = Self::open()?;
        let mut first = App::open(AppConfig::new(native.clone(),
            vec![AgentSpec::new("faulty".into(), "v1".into(), first_agent)],
            vec![WorkflowDefinition::parse(&historical.to_string())?])).await?;
        let original = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(20), async {
            let workspace = first.create_workspace(WorkspaceConfig {
                name: Some("cold recovery golden".into()), root: PathBuf::from("/tmp"),
            }).await?;
            let workflow = workspace.workflow("recovery-history".parse()?).await?;
            let run = workflow.start(ExecutionOptions::default()).await?;
            let terminal = run.wait().await?;
            assert!(matches!(terminal.status, RunStatus::Succeeded));
            assert_eq!(terminal.state.values()["value"], "historical v1");
            Ok::<_, Box<dyn Error + Send + Sync>>((workspace.id().clone(), run.id().clone(), terminal, first_state.golden_lower_inventory()?))
        })).catch_unwind().await;
        let first_cleanup = AssertUnwindSafe(first.shutdown(Shutdown::CancelAndDrain { timeout: Duration::from_secs(20) })).catch_unwind().await;
        if !matches!(&first_cleanup, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
            loop {
                let joined = AssertUnwindSafe(first.shutdown(Shutdown::CancelAndDrain { timeout: Duration::from_secs(60) })).catch_unwind().await;
                eprintln!("recovery-golden phase=original-settlement original={first_cleanup:?} subsequent={joined:?}");
                if matches!(&joined, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) { break; }
            }
        }
        drop(first);
        let (workspace_id, historical_id, historical_snapshot, lower) = match original {
            Ok(result) => result??, Err(panic) => std::panic::resume_unwind(panic),
        };
        first_cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
        assert_eq!(first_state.shutdowns.load(Ordering::SeqCst), 1);
        let (state, agent) = Self::open()?;
        state.install_golden_lower_inventory(&lower)?;
        let mut app = App::open(AppConfig::new(native.clone(),
            vec![AgentSpec::new("faulty".into(), "v1".into(), agent)],
            vec![WorkflowDefinition::parse(&historical.to_string())?,
                WorkflowDefinition::parse(&current.to_string())?,
                WorkflowDefinition::parse(&unknown_definition.to_string())?])).await?;
        let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(120), async {
            let workspace = app.workspace(workspace_id.clone()).await?;
            let old_workflow = workspace.workflow("recovery-history".parse()?).await?;
            let old_run = old_workflow.open_run(RunSelector::Latest(historical_id.clone())).await?;
            Self::assert_applied_snapshot(&old_run.snapshot().await?, &historical_snapshot);
            let pending_run = old_workflow.start(ExecutionOptions::default()).await?;
            let pending = tokio::time::timeout(Duration::from_secs(2), pending_run.wait()).await??;
            let [Interaction::Input { sequence: pending_sequence, generation: pending_generation, .. }] = pending.interactions.as_slice()
                else { panic!("the current v2 revision must author an actual Pending Input"); };
            let unknown_workflow = workspace.workflow("stream-failure".parse()?).await?;
            let unknown_run = unknown_workflow.start(ExecutionOptions::default()).await?;
            let unknown = tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let observed = unknown_run.snapshot().await?;
                    if matches!(observed.interactions.as_slice(), [Interaction::Unknown { retry_safe: true, .. }]) {
                        break Ok::<_, AppError>(observed);
                    }
                    tokio::task::yield_now().await;
                }
            }).await??;
            let [Interaction::Unknown { sequence: unknown_sequence, generation: unknown_generation, retry_safe: true, .. }] = unknown.interactions.as_slice()
                else { unreachable!("the actual lower acquisition failure is reconciled"); };
            assert_eq!(state.conversations.load(Ordering::SeqCst), 1);
            assert_eq!(state.streams.load(Ordering::SeqCst), 1);
            assert_eq!(state.prompts.load(Ordering::SeqCst), 0);
            assert_eq!(state.aborts.load(Ordering::SeqCst), 1);
            assert_eq!(state.abort_returns.load(Ordering::SeqCst), 1);
            let pending_scope = format!("{}/{}/{}/{}", workspace.id(), old_workflow.id(), pending_run.id(), pending.occurrence);
            let unknown_scope = format!("{}/{}/{}/{}", workspace.id(), unknown_workflow.id(), unknown_run.id(), unknown.occurrence);
            let pending_row = native.load(StateKey::new("app-sdk", format!("history/{pending_scope}/checkpoint/{:020}", pending.checkpoint))?).await?.unwrap();
            let unknown_row = native.load(StateKey::new("app-sdk", format!("history/{unknown_scope}/checkpoint/{:020}", unknown.checkpoint))?).await?.unwrap();
            let pending_again = pending_run.snapshot().await?;
            Self::assert_applied_snapshot(&pending_again, &pending);
            let root = native.load(StateKey::new("app-sdk", "app")?).await?.unwrap();
            let expected = serde_json::json!({
                "epoch":root.record.value["epoch"],
                "definitions":[historical, current, unknown_definition],
                "workspace":workspace.id().to_string(),
                "historical_run":historical_id.to_string(),
                "historical_occurrence":historical_snapshot.occurrence,
                "historical_checkpoint":historical_snapshot.checkpoint,
                "historical_state":historical_snapshot.state.values(),
                "pending_run":pending_run.id().to_string(), "pending_scope":pending_scope,
                "pending_sequence":pending_sequence, "pending_generation":pending_generation,
                "pending_checkpoint":pending.checkpoint, "pending_row":pending_row.record.value,
                "unknown_run":unknown_run.id().to_string(), "unknown_scope":unknown_scope,
                "unknown_sequence":unknown_sequence, "unknown_generation":unknown_generation,
                "unknown_checkpoint":unknown.checkpoint, "unknown_row":unknown_row.record.value,
                "lower_inventory":state.golden_lower_inventory()?,
                "original_generation":root.record.value["generation"],
                "lower_prompts":0, "lower_streams":1, "lower_aborts":1,
                "evidence":"actual v1 terminal history, v2 current Pending and current retry-safe Unknown; controlled caller Agent; forced process loss after durable witness, not graceful SDK joining"
            });
            if retained.is_some() {
                std::fs::write(directory.join("expected.json"), serde_json::to_vec_pretty(&expected)?)?;
                std::fs::write(directory.join("ready.json"), b"{\"phase\":\"durable-current-recovery\",\"pending\":1,\"unknown\":1,\"historical_definitions\":2}\n")?;
                eprintln!("recovery-golden phase=durable-current-recovery pending=1 unknown=1 historical_definitions=2 awaiting_owned_process_loss=true");
                std::future::pending::<()>().await;
            }
            pending_run.resolve(*pending_sequence, *pending_generation, Resolution::Input(Value::from("completed live fixture"))).await?;
            assert!(matches!(pending_run.wait().await?.status, RunStatus::Succeeded));
            unknown_run.resolve(*unknown_sequence, *unknown_generation, Resolution::Fail).await?;
            assert!(matches!(unknown_run.wait().await?.status, RunStatus::Failed(WorkflowFailure::Agent(_))));
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        })).catch_unwind().await;
        let cleanup = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain { timeout: Duration::from_secs(20) })).catch_unwind().await;
        if !matches!(&cleanup, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
            loop {
                let joined = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain { timeout: Duration::from_secs(60) })).catch_unwind().await;
                eprintln!("recovery-golden phase=settlement original={cleanup:?} subsequent={joined:?}");
                if matches!(&joined, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) { break; }
            }
        }
        drop(app); drop(native);
        match observed { Ok(result) => result??, Err(panic) => std::panic::resume_unwind(panic) }
        cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
        Ok::<_, Box<dyn Error + Send + Sync>>(())
        }).catch_unwind().await;
        let removed = if retained.is_none() {
            std::fs::remove_dir_all(&directory)
        } else {
            Ok(())
        };
        match checked {
            Ok(result) => result?,
            Err(panic) => std::panic::resume_unwind(panic),
        }
        removed?;
        Ok(())
    }

    async fn assert_current_recovery_migrates_original_deadline_and_definitions()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let fixture = AppV6Fixture::copy("current-recovery");
        let expected = fixture.expected();
        let native = StateStore::connect(fixture.config()).await?;
        let original = fixture.records(&native).await;
        let original_root = original
            .iter()
            .find(|row| row.record.key.key() == "app")
            .unwrap();
        assert_eq!(
            original_root.record.value["generation"],
            expected["original_generation"]
        );
        for kind in ["pending", "unknown"] {
            let scope = expected[format!("{kind}_scope")].as_str().unwrap();
            let sequence = expected[format!("{kind}_checkpoint")].as_u64().unwrap();
            let head = native
                .load(StateKey::new("app-sdk", format!("run/{scope}"))?)
                .await?
                .expect("the reaped child's current run head remains durable");
            assert_eq!(
                head.record.value["sequence"].as_u64().unwrap(),
                sequence,
                "the cold fixture must retain its witnessed current {kind} state"
            );
            let checkpoint = native
                .load(StateKey::new(
                    "app-sdk",
                    format!("history/{scope}/checkpoint/{sequence:020}"),
                )?)
                .await?
                .expect("the current checkpoint named by the cold head exists");
            assert_eq!(
                *checkpoint.record.value,
                expected[format!("{kind}_row")],
                "the cold current {kind} checkpoint matches the pre-loss witness"
            );
        }
        let (faults, mut store) = StorageFaults::open().await?;
        store.inner = native.clone();
        *faults.migration_fault.lock().unwrap() = MigrationFault::StopAfter {
            boundary: MigrationBoundary::Ready,
        };
        let (state, agent) = Self::open()?;
        state.install_golden_lower_inventory(&expected["lower_inventory"])?;
        let definitions = expected["definitions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|definition| WorkflowDefinition::parse(&definition.to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        let opened = App::open(AppConfig::new(
            store,
            vec![AgentSpec::new("faulty".into(), "v1".into(), agent)],
            definitions,
        ))
        .await;
        if let Ok(mut app) = opened {
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(20),
            })
            .await?;
            panic!("conversion must stop after Ready before ordinary recovery claims a generation");
        }
        assert!(matches!(opened, Err(AppError::Integration(_))));
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
        let ready = native
            .load(original_root.record.key.clone())
            .await?
            .unwrap();
        assert!(MigrationBoundary::Ready.observes(&ready.record.value));
        assert_eq!(
            ready.record.value["generation"],
            expected["original_generation"]
        );
        let checkpoints = original
            .iter()
            .filter(|row| row.record.key.key().split('/').nth(5) == Some("checkpoint"))
            .count();
        let other = original.len() - checkpoints - 1;
        assert_eq!(
            ready.revision.get(),
            original_root.revision.get() + (checkpoints + other.div_ceil(256) + 5) as u64
        );
        for before in &original {
            if before.record.key.key() == "app" {
                continue;
            }
            let after = native
                .load(before.record.key.clone())
                .await?
                .expect("every physical migration row survives before normal claim");
            assert_eq!(after.revision.get(), before.revision.get() + 1);
            let mut logical = if before.record.key.key().split('/').nth(5) == Some("checkpoint") {
                StorageFaults::logical_checkpoint(&native, &after.record).await
            } else {
                (*after.record.value).clone()
            };
            logical["epoch"] = Value::from("app/v6");
            assert_eq!(
                logical, *before.record.value,
                "pure conversion preserves pending controls, Unknown and both exact definition revisions"
            );
        }
        let pending_row = &expected["pending_row"];
        let controls = pending_row["checkpoint"]["controls"].as_object().unwrap();
        assert_eq!(controls.len(), 1);
        let pending_control = controls.values().next().unwrap();
        assert_eq!(pending_control["state"]["kind"], "input");
        let deadline = pending_control["deadline_millis"].as_u64().unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as u64;
        // A cold asset never renews its original finite deadline. The source
        // authored 5,000ms; a freshly generated asset may need the remainder.
        if deadline > now {
            assert!(
                deadline - now <= 5_000,
                "fixture retains its original authored deadline"
            );
            tokio::time::sleep(Duration::from_millis(deadline - now + 1)).await;
        }
        let definitions = expected["definitions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|definition| WorkflowDefinition::parse(&definition.to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        let mut app = App::open(AppConfig::new(
            native.clone(),
            vec![AgentSpec::new("faulty".into(), "v1".into(), state.agent())],
            definitions,
        ))
        .await?;
        let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(20), async {
            let workspace = app
                .workspace(expected["workspace"].as_str().unwrap().parse()?)
                .await?;
            let workflow = workspace.workflow("recovery-history".parse()?).await?;
            let historical = workflow
                .open_run(RunSelector::Latest(
                    expected["historical_run"].as_str().unwrap().parse()?,
                ))
                .await?;
            let historical_snapshot = historical.snapshot().await?;
            assert!(matches!(historical_snapshot.status, RunStatus::Succeeded));
            assert_eq!(
                historical_snapshot.checkpoint,
                expected["historical_checkpoint"].as_u64().unwrap()
            );
            assert_eq!(
                serde_json::to_value(historical_snapshot.state.values())?,
                expected["historical_state"]
            );
            assert_eq!(
                historical_snapshot.occurrence,
                expected["historical_occurrence"].as_str().unwrap()
            );
            let pending = workflow
                .open_run(RunSelector::Latest(
                    expected["pending_run"].as_str().unwrap().parse()?,
                ))
                .await?;
            let expired = loop {
                let snapshot = pending.snapshot().await?;
                if matches!(snapshot.status, RunStatus::Failed(_)) {
                    break snapshot;
                }
                assert!(matches!(
                    snapshot.interactions.as_slice(),
                    [Interaction::Input { .. }]
                ));
                tokio::task::yield_now().await;
            };
            assert!(
                matches!(expired.status, RunStatus::Failed(WorkflowFailure::Timeout(ref message))
                if message == "caller interaction deadline expired"),
                "original deadline controls ordinary post-migration recovery"
            );
            assert_eq!(expired.state.values()["value"], "v2");
            assert!(expired.interactions.is_empty());
            let unknown_workflow = workspace.workflow("stream-failure".parse()?).await?;
            let unknown = unknown_workflow
                .open_run(RunSelector::Latest(
                    expected["unknown_run"].as_str().unwrap().parse()?,
                ))
                .await?;
            let snapshot = unknown.wait().await?;
            let [
                Interaction::Unknown {
                    sequence,
                    generation,
                    retry_safe: true,
                    ..
                },
            ] = snapshot.interactions.as_slice()
            else {
                panic!("current Unknown remains caller-settled after migration");
            };
            assert_eq!(*sequence, expected["unknown_sequence"].as_u64().unwrap());
            assert_eq!(
                *generation,
                expected["unknown_generation"].as_u64().unwrap()
            );
            assert_eq!(snapshot.state.values()["answer"], "");
            assert_eq!(
                state.prompts.load(Ordering::SeqCst),
                0,
                "migration and recovery never replay the original lower attempt"
            );
            unknown
                .resolve(*sequence, *generation, Resolution::Fail)
                .await?;
            let terminal = unknown.wait().await?;
            assert!(matches!(
                terminal.status,
                RunStatus::Failed(WorkflowFailure::Agent(_))
            ));
            assert_eq!(state.prompts.load(Ordering::SeqCst), 0);
            let root = native
                .load(StateKey::new("app-sdk", "app")?)
                .await?
                .unwrap();
            assert_eq!(
                root.record.value["generation"].as_u64().unwrap(),
                expected["original_generation"].as_u64().unwrap() + 1
            );
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        }))
        .catch_unwind()
        .await;
        let cleanup = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(20),
        }))
        .catch_unwind()
        .await;
        if !matches!(&cleanup, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
            loop {
                let joined = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(60),
                }))
                .catch_unwind()
                .await;
                eprintln!(
                    "current-recovery phase=settlement original={cleanup:?} subsequent={joined:?}"
                );
                if matches!(&joined, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
                    break;
                }
            }
        }
        drop(app);
        drop(native);
        match observed {
            Ok(result) => result??,
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 2);
        Ok(())
    }

    async fn assert_durable_notification_history() -> Result<(), Box<dyn Error + Send + Sync>> {
        // A real public App writes every row. The controlled lower Agent supplies
        // notifications only; this is persistence/migration evidence, not native
        // Agent strict-live evidence. An explicit, absent output directory allows
        // the source-bound v6 executable to retain a cold physical golden store.
        let retained = std::env::var_os("AI_KIT_APP_GOLDEN_OUTPUT").map(PathBuf::from);
        let directory = retained.clone().unwrap_or_else(|| {
            std::env::temp_dir().join(format!("app-notification-history-{}", uuid::Uuid::new_v4()))
        });
        assert!(directory.is_absolute(), "golden output must be absolute");
        std::fs::create_dir(&directory)?;
        let checked = AssertUnwindSafe(async {
            let (state, agent) = Self::open()?;
            let notifications = NotificationSequence::new(96, false);
            *state.notifications.lock().unwrap() = Some(Arc::clone(&notifications));
            let native = StateStore::connect(StateStoreConfig::Lance(
                LanceStateStoreConfig::directory(directory.join("state")),
            )).await?;
            let mut definition: Value = serde_json::from_str(STREAM_FAILURE)?;
            definition["nodes"][0]["deadline_ms"] = Value::from(1_260_000);
            let mut app = App::open(AppConfig::new(
                native.clone(),
                vec![AgentSpec::new("faulty".to_owned(), "v1".to_owned(), agent)],
                vec![WorkflowDefinition::parse(&definition.to_string())?],
            )).await?;
            let started = std::time::Instant::now();
            let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(1_200), async {
                let workspace = app.create_workspace(WorkspaceConfig {
                    name: Some("notification migration golden".to_owned()),
                    root: PathBuf::from("/tmp"),
                }).await?;
                let workspace_before = workspace.snapshot().await?;
                let workflow = workspace.workflow("stream-failure".parse()?).await?;
                let run = workflow.start(ExecutionOptions::default()).await?;
                let initial = run.snapshot().await?;
                let mut receipts = Vec::new();
                let mut first_full_page = None;
                for index in 0..96 {
                    let waiting = tokio::time::timeout(Duration::from_secs(20), run.wait()).await??;
                    let [Interaction::Notification { sequence, generation, notification }] = waiting.interactions.as_slice()
                        else { panic!("each generated delivery exposes its original target") };
                    assert_eq!(notification.id(), &NotificationSequence::id(index));
                    let applied = run.resolve(*sequence, *generation,
                        Resolution::Notification(NotificationResolution::Allow)).await?;
                    assert_eq!(applied.id, initial.id);
                    assert_eq!(applied.occurrence, initial.occurrence);
                    assert_eq!(applied.state, initial.state);
                    assert!(applied.checkpoint > waiting.checkpoint);
                    assert!(receipts.last().is_none_or(|(prior, _, _)| prior < sequence));
                    if index == 0 || matches!(index + 1, 63..=65) {
                        let scope = format!("{}/{}/{}/{}", workspace.id(), workflow.id(), run.id(), initial.occurrence);
                        let row = native.load(StateKey::new("app-sdk", format!(
                            "history/{scope}/checkpoint/{:020}", applied.checkpoint,
                        ))?).await?.expect("actual immutable Applied checkpoint");
                        assert_eq!(row.record.value["epoch"], "app/v7");
                        let descriptor = row.record.value["checkpoint"]["settlements"].as_object()
                            .expect("v7 stores a bounded settlement descriptor");
                        assert_eq!(descriptor.len(), 2);
                        assert_eq!(descriptor["count"], index + 1);
                        let pages = descriptor["pages"].as_array().expect("dense publication sequence references");
                        assert_eq!(pages.len(), (index + 1).div_ceil(64));
                        if index + 1 == 64 { first_full_page = Some(pages[0].clone()); }
                        if index + 1 == 65 { assert_eq!(first_full_page.as_ref(), Some(&pages[0]), "an unchanged full leaf is reused"); }
                        let mut entries = Vec::new();
                        for (page, publication) in pages.iter().enumerate() {
                            let publication = publication.as_u64().expect("leaf publication sequence");
                            assert!(publication <= applied.checkpoint);
                            let leaf = native.load(StateKey::new("app-sdk", format!(
                                "history/{scope}/ledger/{page:02}/{publication:020}",
                            ))?).await?.expect("exact referenced immutable settlement leaf");
                            assert_eq!(leaf.revision.get(), 1, "each leaf version is immutable");
                            let value = leaf.record.value.as_ref();
                            assert_eq!(value.as_object().unwrap().len(), 8);
                            assert_eq!(value["epoch"], "app/v7");
                            assert_eq!(value["workspace_id"], workspace.id().to_string());
                            assert_eq!(value["workflow_id"], workflow.id().to_string());
                            assert_eq!(value["id"], run.id().to_string());
                            assert_eq!(value["occurrence"], initial.occurrence);
                            assert_eq!(value["index"], page);
                            assert_eq!(value["sequence"], publication);
                            let leaf_entries = value["entries"].as_array().expect("original full settlement tuples");
                            assert_eq!(leaf_entries.len(), (index + 1 - page * 64).min(64));
                            entries.extend(leaf_entries.iter().cloned());
                        }
                        assert_eq!(entries.len(), index + 1);
                        for (entry_index, entry) in entries.iter().enumerate() {
                            assert_eq!(entry[3][2], NotificationSequence::id(entry_index).to_string());
                            assert_eq!(entry[4][0], 2, "each prior original receipt remains Applied");
                            if entry_index < receipts.len() {
                                let (sequence, generation, snapshot): &(u64, u64, RunSnapshot) = &receipts[entry_index];
                                assert_eq!(entry[0], *sequence);
                                assert_eq!(entry[1], *generation);
                                assert_eq!(entry[4], serde_json::json!([2, [1], snapshot.checkpoint]));
                            } else {
                                assert_eq!(entry[0], *sequence);
                                assert_eq!(entry[1], *generation);
                                assert_eq!(entry[4], serde_json::json!([2, [1], applied.checkpoint]));
                            }
                        }
                    }
                    receipts.push((*sequence, *generation, applied));
                    if matches!(index + 1, 32 | 64 | 96) {
                        eprintln!("notification-history phase=generation completed={} elapsed_ms={}", index + 1, started.elapsed().as_millis());
                    }
                }
                let terminal = tokio::time::timeout(Duration::from_secs(20), run.wait()).await??;
                assert!(matches!(terminal.status, RunStatus::Succeeded));
                assert_eq!(terminal.state.values()["answer"], "both approvals accepted");
                assert_eq!(terminal.last_action_ordinal, Some(0));
                assert_eq!(terminal.outcomes.len(), 1);
                assert!(terminal.interactions.is_empty());
                let mut history = Vec::new();
                let mut cursor = None;
                loop {
                    let page = run.history(cursor, NonZeroUsize::new(31).unwrap()).await?;
                    for checkpoint in page.checkpoints {
                        assert_eq!(checkpoint.sequence, history.len() as u64);
                        assert_eq!(checkpoint.run, terminal.id);
                        assert_eq!(checkpoint.occurrence, terminal.occurrence);
                        history.push(serde_json::json!({
                            "sequence": checkpoint.sequence,
                            "state": checkpoint.state.values(),
                            "action_ordinal": checkpoint.action_ordinal,
                            "position": checkpoint.position,
                        }));
                    }
                    match page.next { Some(next) => cursor = Some(next), None => break }
                }
                assert_eq!(history.len() as u64, terminal.checkpoint + 1);
                assert!(history.len() > 256, "golden crosses a real migration/deletion page boundary");
                for (sequence, generation, applied) in &receipts {
                    let replay = run.resolve(*sequence, *generation,
                        Resolution::Notification(NotificationResolution::Allow)).await?;
                    Self::assert_applied_snapshot(&replay, applied);
                }
                let mut restored = workflow.restore(RestoreOptions {
                    source: RunSelector::Latest(run.id().clone()),
                    checkpoint: CheckpointSelector::Latest,
                    overrides: WorkflowState::default(),
                    position: ResumePosition::AfterCompleted,
                    fallback: None,
                }).await?;
                assert_ne!(restored.id(), run.id());
                let copy = restored.snapshot().await?;
                assert_eq!(copy.state, terminal.state);
                assert!(copy.interactions.is_empty());
                assert!(matches!(copy.status, RunStatus::Paused));
                let copied_checkpoint = restored.checkpoint(CheckpointSelector::Sequence(0)).await?;
                assert_eq!(&copied_checkpoint.lineage.as_ref().unwrap().source, run.id());
                let copy_terminal = restored.execution(ExecutionOptions::default()).await?.finish().await?;
                assert!(matches!(copy_terminal.status, RunStatus::Succeeded));
                assert_eq!(copy_terminal.state, terminal.state);
                let deleted = restored.id().to_string();
                restored.delete(copy_terminal.checkpoint).await?;
                assert!(matches!(workflow.open_run(RunSelector::Latest(restored.id().clone())).await, Err(AppError::NotFound(_))));
                let after_deletion = run.snapshot().await?;
                Self::assert_applied_snapshot(&after_deletion, &terminal);
                assert_eq!(workspace.snapshot().await?.revision, workspace_before.revision);
                assert_eq!(*notifications.resolved.lock().unwrap(), (0..96).map(NotificationSequence::id).collect::<Vec<_>>());
                assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), 96);
                assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), 96);
                assert_eq!(state.prompts.load(Ordering::SeqCst), 1);
                assert_eq!(notifications.prompt_returns.load(Ordering::SeqCst), 1);
                assert_eq!(state.aborts.load(Ordering::SeqCst), 0);
                let root = native.load(StateKey::new("app-sdk", "app")?).await?.expect("durable App root");
                Ok::<_, Box<dyn Error + Send + Sync>>(serde_json::json!({
                    "epoch": root.record.value["epoch"],
                    "definition": definition,
                    "workspace": workspace.id().to_string(),
                    "workspace_revision": workspace_before.revision,
                    "workspace_root": workspace_before.config.root,
                    "run": terminal.id.to_string(),
                    "occurrence": terminal.occurrence,
                    "checkpoint": terminal.checkpoint,
                    "state": terminal.state.values(),
                    "history": history,
                    "receipts": receipts.iter().map(|(sequence, generation, snapshot)| serde_json::json!({
                        "sequence": sequence, "generation": generation, "checkpoint": snapshot.checkpoint,
                    })).collect::<Vec<_>>(),
                    "deleted_restored_run": deleted,
                    "deliveries": 96,
                    "lower_prompts": 1,
                    "lower_inventory": state.golden_lower_inventory()?,
                    "evidence": "real App and Lance; controlled caller Agent; cold after joined shutdown",
                }))
            })).catch_unwind().await;
            notifications.cancelled.store(true, Ordering::SeqCst);
            notifications.changed.notify_waiters();
            notifications.resolution_release.notify_one();
            let cleanup = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain { timeout: Duration::from_secs(20) })).catch_unwind().await;
            if !matches!(&cleanup, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
                loop {
                    let joined = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain { timeout: Duration::from_secs(600) })).catch_unwind().await;
                    eprintln!("notification-history phase=settlement original={cleanup:?} subsequent={joined:?}");
                    // This owner requests only valid CancelAndDrain; retained
                    // Integration follows joining, not successful publication.
                    if matches!(&joined, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) { break; }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
            drop(app);
            drop(native);
            let manifest = match observed {
                Ok(result) => result??,
                Err(panic) => std::panic::resume_unwind(panic),
            };
            cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
            assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
            eprintln!("notification-history phase=settled deliveries=96 lower_shutdowns=1 elapsed_ms={}", started.elapsed().as_millis());
            Ok::<_, Box<dyn Error + Send + Sync>>(manifest)
        }).catch_unwind().await;
        match checked {
            Ok(Ok(manifest)) if retained.is_some() => {
                std::fs::write(
                    directory.join("expected.json"),
                    serde_json::to_vec_pretty(&manifest)?,
                )?;
                eprintln!("notification-history golden=retained owner_settlement=joined");
            }
            outcome => {
                let removed = std::fs::remove_dir_all(&directory);
                if let Err(error) = &removed {
                    eprintln!("notification-history secondary cleanup: {error}");
                }
                match outcome {
                    Ok(result) => {
                        result?;
                    }
                    Err(panic) => std::panic::resume_unwind(panic),
                }
                removed?;
            }
        }
        Ok(())
    }

    async fn assert_routine_delivery() -> Result<(), Box<dyn Error + Send + Sync>> {
        let directory =
            std::env::temp_dir().join(format!("app-delivery-routine-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory)?;
        let checked = AssertUnwindSafe(async {
            let (state, agent) = Self::open()?;
            let notifications = NotificationSequence::new(64, false);
            *state.notifications.lock().unwrap() = Some(Arc::clone(&notifications));
            let store = StateStore::connect(StateStoreConfig::Lance(
                LanceStateStoreConfig::directory(directory.join("state")),
            )).await?;
            let mut definition: Value = serde_json::from_str(STREAM_FAILURE)?;
            definition["nodes"][0]["deadline_ms"] = Value::from(1_260_000);
            let mut app = App::open(AppConfig::new(
                store,
                vec![AgentSpec::new("faulty".to_owned(), "v1".to_owned(), agent)],
                vec![WorkflowDefinition::parse(&definition.to_string())?],
            )).await?;
            let started = std::time::Instant::now();
            let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(1_200), async {
                let workspace = app.create_workspace(WorkspaceConfig {
                    name: None, root: std::env::current_dir()?,
                }).await?;
                let workflow = workspace.workflow("stream-failure".parse()?).await?;
                let run = workflow.start(ExecutionOptions::default()).await?;
                let initial = run.snapshot().await?;
                let mut receipts = Vec::new();
                let mut checkpoint = initial.checkpoint;
                for index in 0..63 {
                    let waiting = tokio::time::timeout(Duration::from_secs(20), run.wait()).await??;
                    let [Interaction::Notification { sequence, generation, notification }] = waiting.interactions.as_slice()
                        else { panic!("each routine delivery exposes exactly its original target") };
                    assert_eq!(waiting.id, initial.id);
                    assert_eq!(waiting.occurrence, initial.occurrence);
                    assert_eq!(waiting.state, initial.state);
                    assert_eq!(waiting.last_action_ordinal, initial.last_action_ordinal);
                    assert!(waiting.outcomes.is_empty());
                    assert!(waiting.checkpoint >= checkpoint);
                    assert_eq!(notification.id(), &NotificationSequence::id(index));
                    assert!(receipts.last().is_none_or(|(prior, _, _)| prior < sequence));
                    let applied = run.resolve(*sequence, *generation,
                        Resolution::Notification(NotificationResolution::Allow)).await?;
                    assert_eq!(applied.id, initial.id);
                    assert_eq!(applied.occurrence, initial.occurrence);
                    assert!(applied.checkpoint > waiting.checkpoint);
                    assert_eq!(applied.state, initial.state);
                    assert_eq!(applied.last_action_ordinal, initial.last_action_ordinal);
                    assert!(applied.outcomes.is_empty());
                    checkpoint = applied.checkpoint;
                    receipts.push((*sequence, *generation, applied));
                    assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), index + 1);
                    assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), index + 1);
                }
                let last = tokio::time::timeout(Duration::from_secs(20), run.wait()).await??;
                let [Interaction::Notification { sequence, generation, notification }] = last.interactions.as_slice()
                    else { panic!("the 64th original target remains owned until public cancellation") };
                let cancelled_target = (*sequence, *generation);
                assert_eq!(notification.id(), &NotificationSequence::id(63));
                assert_eq!(last.id, initial.id);
                assert_eq!(last.occurrence, initial.occurrence);
                assert!(last.checkpoint > checkpoint);
                assert!(receipts.last().is_some_and(|(prior, _, _)| prior < sequence));
                assert_eq!(notifications.emitted.load(Ordering::SeqCst), 64);
                assert_eq!(notifications.received.load(Ordering::SeqCst), 64);
                run.cancel(last.checkpoint).await?;
                let terminal = tokio::time::timeout(Duration::from_secs(20), run.wait()).await??;
                assert!(matches!(terminal.status, RunStatus::Cancelled));
                assert_eq!(terminal.id, initial.id);
                assert_eq!(terminal.occurrence, initial.occurrence);
                assert!(terminal.checkpoint > last.checkpoint);
                assert!(terminal.interactions.is_empty());
                assert_eq!(terminal.state, initial.state);
                assert_eq!(terminal.last_action_ordinal, initial.last_action_ordinal);
                assert!(terminal.outcomes.is_empty());
                for (sequence, generation, applied) in &receipts {
                    let replay = run.resolve(*sequence, *generation,
                        Resolution::Notification(NotificationResolution::Allow)).await?;
                    Self::assert_applied_snapshot(&replay, applied);
                }
                assert!(matches!(run.resolve(cancelled_target.0, cancelled_target.1,
                    Resolution::Notification(NotificationResolution::Allow)).await,
                    Err(AppError::Conflict(_))));
                let (sequence, generation, _) = receipts.last().expect("63 original receipts");
                assert!(matches!(run.resolve(*sequence, *generation,
                    Resolution::Notification(NotificationResolution::Deny {
                        reason: "changed decision".to_owned(),
                    })).await, Err(AppError::Conflict(_))));
                assert_eq!(*notifications.resolved.lock().unwrap(),
                    (0..63).map(NotificationSequence::id).collect::<Vec<_>>());
                assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), 63);
                assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), 63);
                assert_eq!(notifications.prompt_returns.load(Ordering::SeqCst), 0);
                assert_eq!(state.prompts.load(Ordering::SeqCst), 1);
                assert_eq!(state.aborts.load(Ordering::SeqCst), 1);
                assert_eq!(state.abort_returns.load(Ordering::SeqCst), 1);
                eprintln!("delivery-routine deliveries=64 original_receipts=63 receipt_replays=63 cancelled_targets=1 terminal_checkpoint={} elapsed_ms={}",
                    terminal.checkpoint, started.elapsed().as_millis());
                Ok::<_, Box<dyn Error + Send + Sync>>(())
            })).catch_unwind().await;
            notifications.cancelled.store(true, Ordering::SeqCst);
            notifications.changed.notify_waiters();
            notifications.resolution_release.notify_one();
            let cleanup = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(20),
            })).catch_unwind().await;
            if !matches!(&cleanup, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
                loop {
                    let joined = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_secs(600),
                    })).catch_unwind().await;
                    eprintln!("delivery-routine settlement original={cleanup:?} subsequent={joined:?}");
                    // This owner uses valid CancelAndDrain exclusively. These
                    // outcomes follow joining; preserve the first failed receipt.
                    if matches!(&joined, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
            drop(app);
            match observed {
                Ok(result) => result??,
                Err(panic) => std::panic::resume_unwind(panic),
            }
            cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
            assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
            assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), 63);
            assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), 63);
            eprintln!("delivery-routine cleanup=joined lower_shutdowns=1 elapsed_ms={}", started.elapsed().as_millis());
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        }).catch_unwind().await;
        let removed = std::fs::remove_dir_all(&directory);
        if let Err(error) = &removed {
            eprintln!("delivery-routine secondary directory cleanup failure: {error}");
        }
        match checked {
            Ok(result) => result?,
            Err(panic) => std::panic::resume_unwind(panic),
        }
        removed?;
        Ok(())
    }

    async fn measure_delivery_prefix(
        deliveries: usize,
        checkpoints: &CheckpointPollCapture,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        assert!(matches!(deliveries, 256 | 1_024));
        let setup_started = std::time::Instant::now();
        let directory =
            std::env::temp_dir().join(format!("app-delivery-prefix-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory)?;
        let (state, agent) = Self::open()?;
        let notifications = NotificationSequence::new(4_096, false);
        *state.notifications.lock().unwrap() = Some(Arc::clone(&notifications));
        let (faults, mut store) = StorageFaults::open().await?;
        store.inner = StateStore::connect(StateStoreConfig::Lance(
            LanceStateStoreConfig::directory(directory.join("state")),
        ))
        .await?;
        *faults.observations.lock().unwrap() = Some(BTreeMap::new());
        let mut definition: Value = serde_json::from_str(STREAM_FAILURE)?;
        definition["nodes"][0]["deadline_ms"] = Value::from(1_260_000);
        let mut app = App::open(AppConfig::new(
            store,
            vec![AgentSpec::new("faulty".to_owned(), "v1".to_owned(), agent)],
            vec![WorkflowDefinition::parse(&definition.to_string())?],
        ))
        .await?;
        eprintln!(
            "delivery-prefix phase=setup deliveries={deliveries} elapsed_ms={} dataset={directory:?} purpose=diagnostic-only extra_json_encoding=false active_counts_observed_futures_only native_work_may_outlive_observation",
            setup_started.elapsed().as_millis()
        );
        let started = std::time::Instant::now();
        let mut completed = 0;
        let mut phase = "initial-workflow";
        let mut target_index = None;
        let body = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(1_200), async {
            let workspace = app.create_workspace(WorkspaceConfig {
                name: None, root: std::env::current_dir()?,
            }).await?;
            let workflow = workspace.workflow("stream-failure".parse()?).await?;
            let run = workflow.start(ExecutionOptions::default()).await?;
            let initial = run.snapshot().await?;
            eprintln!("delivery-prefix phase=initial-workflow elapsed_ms={} checkpoint={}",
                started.elapsed().as_millis(), initial.checkpoint);
            let mut block_started = std::time::Instant::now();
            let mut block_wait = Duration::ZERO;
            let mut block_resolve = Duration::ZERO;
            let mut wait_elapsed = Duration::ZERO;
            let mut resolve_elapsed = Duration::ZERO;
            let mut prior = None;
            let mut checkpoint_block = checkpoints.snapshot();
            for index in 0..deliveries {
                phase = "wait";
                target_index = Some(index);
                let operation = std::time::Instant::now();
                let snapshot = run.wait().await?;
                let elapsed = operation.elapsed();
                wait_elapsed += elapsed;
                block_wait += elapsed;
                let [Interaction::Notification { sequence, generation, notification }] = snapshot.interactions.as_slice()
                    else { panic!("prefix requires one actual notification") };
                assert_eq!(notification.id(), &NotificationSequence::id(index));
                assert!(prior.is_none_or(|prior| prior < *sequence));
                prior = Some(*sequence);
                phase = "resolve";
                let operation = std::time::Instant::now();
                let applied = run.resolve(*sequence, *generation,
                    Resolution::Notification(NotificationResolution::Allow),
                ).await?;
                let elapsed = operation.elapsed();
                resolve_elapsed += elapsed;
                block_resolve += elapsed;
                completed = index + 1;
                assert_eq!(applied.checkpoint, 3 * completed as u64 + 2,
                    "each delivered target retains reservation, intent and application checkpoints");
                assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), index + 1);
                assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), index + 1);
                if completed % 64 == 0 {
                    eprintln!("delivery-prefix milestone={} elapsed_ms={} checkpoint={} wait_ms={} resolve_ms={} storage={:?}",
                        index + 1, started.elapsed().as_millis(), applied.checkpoint,
                        wait_elapsed.as_millis(), resolve_elapsed.as_millis(), faults.observations.lock().unwrap());
                }
                if completed % 64 == 0 {
                    checkpoints.report_block(completed - 64, completed, &mut checkpoint_block);
                }
                if completed % 256 == 0 {
                    eprintln!("delivery-prefix phase=block first={} completed={} block_elapsed_ms={} block_wait_ms={} block_resolve_ms={} checkpoint={} received={} lower_calls={} lower_returns={}",
                        completed - 256, completed, block_started.elapsed().as_millis(),
                        block_wait.as_millis(), block_resolve.as_millis(), applied.checkpoint,
                        notifications.received.load(Ordering::SeqCst),
                        notifications.resolution_calls.load(Ordering::SeqCst),
                        notifications.resolution_returns.load(Ordering::SeqCst));
                    block_started = std::time::Instant::now();
                    block_wait = Duration::ZERO;
                    block_resolve = Duration::ZERO;
                }
            }
            assert_eq!(*notifications.resolved.lock().unwrap(),
                (0..deliveries).map(NotificationSequence::id).collect::<Vec<_>>());
            assert_eq!(state.prompts.load(Ordering::SeqCst), 1);
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        })).catch_unwind().await;
        eprintln!(
            "delivery-prefix phase=body deliveries={deliveries} completed_public_resolutions={completed} active_phase={phase} target_index={target_index:?} elapsed_ms={} success={}",
            started.elapsed().as_millis(),
            matches!(&body, Ok(Ok(Ok(()))))
        );
        notifications.cancelled.store(true, Ordering::SeqCst);
        notifications.changed.notify_waiters();
        notifications.resolution_release.notify_one();
        notifications.next_target_release.notify_one();
        let joining = std::time::Instant::now();
        let cleanup = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(20),
        }))
        .catch_unwind()
        .await;
        if !matches!(&cleanup, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
            loop {
                let joined = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(600),
                }))
                .catch_unwind()
                .await;
                eprintln!(
                    "delivery-prefix settlement elapsed_ms={} result={joined:?}",
                    joining.elapsed().as_millis()
                );
                // This owner uses valid CancelAndDrain exclusively. These terminal
                // outcomes follow joining; the first failed receipt remains primary.
                if matches!(&joined, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        eprintln!(
            "delivery-prefix phase=shutdown settlement=joined elapsed_ms={} original={cleanup:?} storage={:?}",
            joining.elapsed().as_millis(),
            faults.observations.lock().unwrap()
        );
        drop(app);
        let checkpoint_totals = checkpoints.snapshot();
        eprintln!(
            "delivery-prefix checkpoint-polls settlement=joined totals={checkpoint_totals:?}"
        );
        // Physical metadata is diagnostic, never a portable-format assertion.
        let physical = (|| -> Result<(), Box<dyn Error + Send + Sync>> {
            let mut pending = vec![directory.clone()];
            let (mut files, mut bytes, mut manifests) = (0_u64, 0_u64, 0_u64);
            while let Some(path) = pending.pop() {
                for entry in std::fs::read_dir(path)? {
                    let entry = entry?;
                    let metadata = entry.metadata()?;
                    if metadata.is_dir() {
                        pending.push(entry.path());
                    } else {
                        files += 1;
                        bytes += metadata.len();
                        manifests += u64::from(
                            entry
                                .path()
                                .extension()
                                .is_some_and(|extension| extension == "manifest"),
                        );
                    }
                }
            }
            let space = std::process::Command::new("df")
                .arg("-k")
                .arg(&directory)
                .output()?;
            eprintln!(
                "delivery-prefix physical files={files} bytes={bytes} retained_manifests={manifests} df={}",
                String::from_utf8_lossy(&space.stdout)
            );
            Ok(())
        })();
        if let Err(error) = &physical {
            eprintln!("delivery-prefix secondary physical observation failed: {error}");
        }
        let removed = std::fs::remove_dir_all(&directory);
        match body {
            Ok(result) => result??,
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
        assert!(
            checkpoint_totals.opened > 0,
            "checkpoint polling was actually observed"
        );
        assert_eq!(checkpoint_totals.opened, checkpoint_totals.closed);
        assert_eq!(checkpoint_totals.violations, 0);
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
        assert_eq!(
            notifications.resolution_calls.load(Ordering::SeqCst),
            deliveries
        );
        assert_eq!(
            notifications.resolution_returns.load(Ordering::SeqCst),
            deliveries
        );
        for observed in faults
            .observations
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .values()
        {
            assert_eq!(
                observed.active, 0,
                "all measured observers settled after App shutdown"
            );
            assert_eq!(
                observed.started,
                observed.completed + observed.dropped_observers
            );
        }
        physical?;
        removed?;
        Ok(())
    }

    async fn probe_prospective_settlement_bytes(
        final_body_bytes: usize,
    ) -> Result<(Option<usize>, (usize, u64)), Box<dyn Error + Send + Sync>> {
        let (state, agent) = Self::open()?;
        let notifications = NotificationSequence::new(11, false);
        // Ten legal decoded bodies occupy nearly four MiB after JSON escaping.
        // The final body's encoded size is the only input varied across runs.
        let mut bodies = vec![393_181; 10];
        bodies.push(final_body_bytes);
        *notifications
            .encoded_body_bytes
            .lock()
            .expect("test body sizes") = bodies;
        *state.notifications.lock().expect("test notifications lock") =
            Some(Arc::clone(&notifications));
        let mut definition: Value = serde_json::from_str(STREAM_FAILURE)?;
        definition["nodes"][0]["deadline_ms"] = Value::from(120_000);
        let store =
            StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
        let mut app = App::open(AppConfig::new(
            store.clone(),
            vec![AgentSpec::new("faulty".to_owned(), "v1".to_owned(), agent)],
            vec![WorkflowDefinition::parse(&definition.to_string())?],
        ))
        .await?;
        let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(90), async {
            let workspace = app.create_workspace(WorkspaceConfig {
                name: None, root: std::env::current_dir()?,
            }).await?;
            let workflow = workspace.workflow("stream-failure".parse()?).await?;
            let run = workflow.start(ExecutionOptions::default()).await?;
            let initial = run.snapshot().await?;
            let mut prefix = None;
            for index in 0..10 {
                let waiting = run.wait().await?;
                let [Interaction::Notification { sequence, generation, notification }] = waiting.interactions.as_slice()
                    else { panic!("ten genuinely delivered legal bodies fit before the byte boundary") };
                assert_eq!(notification.id(), &NotificationSequence::id(index));
                if index == 9 {
                    *state.preparation_fault.lock().expect("test preparation fault") = PreparationFault::AbortPanic;
                }
                let applied = run.resolve(*sequence, *generation,
                    Resolution::Notification(NotificationResolution::Allow)).await?;
                if index == 9 {
                    let stored = store.load(StateKey::new("app-sdk", format!(
                        "history/{}/{}/{}/{}/checkpoint/{:020}",
                        workspace.id(), workflow.id(), run.id(), applied.occurrence, applied.checkpoint,
                    ))?).await?.expect("the tenth original Applied receipt retains its exact checkpoint");
                    assert_eq!(stored.record.value.pointer("/checkpoint/sequence").and_then(Value::as_u64),
                        Some(applied.checkpoint));
                    let logical = StorageFaults::logical_checkpoint(&store, &stored.record).await;
                    prefix = Some((serde_json::to_vec(&logical)?.len(), applied.checkpoint));
                }
                assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), index + 1);
                assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), index + 1);
            }
            let offered = run.wait().await?;
            assert_eq!(notifications.received.load(Ordering::SeqCst), 11);
            let admitted = match offered.interactions.as_slice() {
                [Interaction::Notification { sequence, generation, notification }] => {
                    assert_eq!(notification.id(), &NotificationSequence::id(10));
                    let target = (*sequence, *generation);
                    // Sizing only: read the actual immutable checkpoint produced
                    // by public admission. No hypothetical record is written.
                    let stored = store.load(StateKey::new("app-sdk", format!(
                        "history/{}/{}/{}/{}/checkpoint/{:020}",
                        workspace.id(), workflow.id(), run.id(), offered.occurrence, offered.checkpoint,
                    ))?).await?.expect("the accepted public target has its original immutable checkpoint");
                    assert_eq!(stored.record.value.pointer("/checkpoint/sequence").and_then(Value::as_u64),
                        Some(offered.checkpoint));
                    let logical = StorageFaults::logical_checkpoint(&store, &stored.record).await;
                    let bytes = serde_json::to_vec(&logical)?.len();
                    *notifications.resolution_fault.lock().expect("test resolution fault") = NotificationResolutionFault::GateReceipt;
                    let mut resolving = Box::pin(run.resolve(target.0, target.1,
                        Resolution::Notification(NotificationResolution::Allow)));
                    tokio::time::timeout(Duration::from_secs(10), async {
                        tokio::select! {
                            _ = notifications.resolution_entered.notified() => {}
                            _ = resolving.as_mut() => panic!("the original final notification receipt must remain held"),
                        }
                    }).await.expect("a publicly admitted target can retain its original lower decision");
                    drop(resolving);
                    assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), 11);
                    assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), 10);
                    let current = run.snapshot().await?;
                    run.cancel(current.checkpoint).await?;
                    Some(bytes)
                }
                [Interaction::Unknown { .. }] => None,
                _ => panic!("a first-overflow notification must preserve earlier accepted work as Unknown"),
            };
            let unknown = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let snapshot = run.wait().await?;
                    if matches!(snapshot.interactions.as_slice(), [Interaction::Unknown { .. }])
                        && state.aborts.load(Ordering::SeqCst) == 1
                    {
                        break Ok::<_, AppError>(snapshot);
                    }
                    assert!(matches!(snapshot.status, RunStatus::Running | RunStatus::Cancelling));
                    tokio::task::yield_now().await;
                }
            }).await.expect("reserved Unknown bytes remain available despite the nearly full immutable record")?;
            assert_eq!(unknown.state, initial.state);
            assert_eq!(unknown.last_action_ordinal, initial.last_action_ordinal);
            assert!(unknown.outcomes.is_empty());
            assert_eq!(state.prompts.load(Ordering::SeqCst), 1);
            assert_eq!(state.aborts.load(Ordering::SeqCst), 1);
            let accepted = 10 + usize::from(admitted.is_some());
            assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), accepted);
            assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), 10);
            let resolved = notifications.resolved.lock().expect("test resolution lock").clone();
            assert_eq!(resolved, (0..accepted).map(NotificationSequence::id).collect::<Vec<_>>());
            let [Interaction::Unknown { sequence, generation, .. }] = unknown.interactions.as_slice()
                else { unreachable!("observed reserved Unknown") };
            run.resolve(*sequence, *generation, Resolution::Fail).await?;
            if admitted.is_some() {
                assert!(matches!(app.shutdown(Shutdown::Drain { timeout: Duration::from_millis(100) }).await,
                    Err(AppError::Timeout(_))), "caller Fail and a full record cannot discard the original native receipt");
                assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), 10);
                notifications.resolution_release.notify_one();
            }
            app.shutdown(Shutdown::Drain { timeout: Duration::from_secs(5) }).await?;
            assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), accepted);
            assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), accepted);
            // Drain may close public run observation immediately after the held
            // receipt returns. Verify the exact head-selected immutable terminal
            // through the same retained StateStore, after joining that Drain.
            let head = store.load(StateKey::new("app-sdk", format!(
                "run/{}/{}/{}/{}", workspace.id(), workflow.id(), run.id(), initial.occurrence,
            ))?).await?.expect("joined cleanup retains the real run head");
            let terminal_sequence = head.record.value["sequence"].as_u64().expect("native head sequence");
            let terminal = store.load(StateKey::new("app-sdk", format!(
                "history/{}/{}/{}/{}/checkpoint/{terminal_sequence:020}",
                workspace.id(), workflow.id(), run.id(), initial.occurrence,
            ))?).await?.expect("native head retains its exact immutable terminal checkpoint");
            let logical = StorageFaults::logical_checkpoint(&store, &terminal.record).await;
            let checkpoint = &logical["checkpoint"];
            assert_eq!(checkpoint["sequence"].as_u64(), Some(terminal_sequence));
            assert_eq!(checkpoint["phase"]["state"], if admitted.is_some() { "cancelled" } else { "failed" });
            if admitted.is_none() { assert_eq!(checkpoint["phase"]["category"], "limit"); }
            assert_eq!(checkpoint["state"], serde_json::to_value(initial.state.values())?);
            assert_eq!(checkpoint.pointer("/position/ordinal").and_then(Value::as_u64), initial.last_action_ordinal);
            assert_eq!(checkpoint["outcomes"], serde_json::json!([]));
            assert!(serde_json::to_vec(&logical)?.len() <= 4 * 1_048_576);
            let settlements = checkpoint["settlements"].as_array().expect("retained actual settlement evidence");
            for index in 0..accepted {
                let id = NotificationSequence::id(index).to_string();
                let rows = settlements.iter().filter(|row| row[3][2] == id).collect::<Vec<_>>();
                assert_eq!(rows.len(), 1, "each original accepted notification retains one exact row");
                assert_eq!(rows[0][4][0], 2, "joined original native returns remain durably Applied");
            }
            if admitted.is_none() {
                assert!(!settlements.iter().any(|row| row[3][2] == NotificationSequence::id(10).to_string()));
            }
            Ok::<_, Box<dyn Error + Send + Sync>>((admitted, prefix.expect("ten actual Applied rows were observed")))
        })).catch_unwind().await;
        *state
            .preparation_fault
            .lock()
            .expect("test preparation fault") = PreparationFault::Pass;
        *notifications
            .resolution_fault
            .lock()
            .expect("test resolution fault") = NotificationResolutionFault::Pass;
        notifications.resolution_release.notify_one();
        notifications.next_target_release.notify_one();
        notifications.cancelled.store(true, Ordering::SeqCst);
        notifications.changed.notify_waiters();
        let cleanup = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(10),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(5),
            }),
        ))
        .catch_unwind()
        .await;
        drop(app);
        let admitted = match observed {
            Ok(result) => {
                result.expect("one genuine eleven-notification byte probe remains bounded")?
            }
            Err(panic) => std::panic::resume_unwind(panic),
        };
        cleanup
            .expect("byte-capacity cleanup does not panic")
            .expect("byte-capacity cleanup joins")?;
        Ok(admitted)
    }

    async fn assert_prospective_settlement_bytes() -> Result<(), Box<dyn Error + Send + Sync>> {
        let bound = 4 * 1_048_576;
        let mut accepted = 1;
        let (initial, prefix) = Self::probe_prospective_settlement_bytes(accepted).await?;
        let mut accepted_record_bytes = initial
            .expect("ten full legal notification bodies leave room for a minimal eleventh target and Unknown");
        let pending_overhead = accepted_record_bytes - accepted;
        // Actual tenth Applied rows must have the same encoded size and sequence
        // in every probe. This fences the pre-admission metadata and next integer
        // widths; fixed-width IDs and epoch notification times add no variation.
        let mut rejected = (accepted + bound - accepted_record_bytes + 1).min(393_181);
        assert!(rejected > accepted);
        let (overflow, overflow_prefix) =
            Self::probe_prospective_settlement_bytes(rejected).await?;
        assert_eq!(
            overflow_prefix, prefix,
            "the rejected probe starts at the same measured byte/sequence basis"
        );
        assert!(overflow.is_none());
        let mut probes = 2;
        while accepted + 1 < rejected {
            assert!(
                probes < 21,
                "legal final-body range needs at most twenty-one real probes"
            );
            let candidate = accepted + (rejected - accepted) / 2;
            let (admitted, candidate_prefix) =
                Self::probe_prospective_settlement_bytes(candidate).await?;
            assert_eq!(
                candidate_prefix, prefix,
                "fresh probes vary only the final encoded body size"
            );
            match admitted {
                Some(bytes) => {
                    assert_eq!(
                        bytes - candidate,
                        pending_overhead,
                        "every actual accepted Pending row confirms the same non-body encoded size"
                    );
                    accepted = candidate;
                    accepted_record_bytes = bytes;
                }
                None => rejected = candidate,
            }
            probes += 1;
        }
        assert_eq!(
            rejected,
            accepted + 1,
            "adjacent actual payload byte sizes establish last fit and first overflow"
        );
        assert!(
            accepted_record_bytes < bound,
            "the first rejected byte still fits the ordinary encoded record; Unknown reserve is the limiting constraint"
        );
        Ok(())
    }

    async fn assert_notification_capacity(
        count: usize,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let (state, agent) = Self::open()?;
        let notifications = NotificationSequence::new(count, false);
        *notifications.emission.lock().expect("test emission lock") = NotificationEmission::Burst;
        *state.notifications.lock().expect("test notifications lock") =
            Some(Arc::clone(&notifications));
        let mut definition: Value = serde_json::from_str(STREAM_FAILURE)?;
        definition["nodes"][0]["deadline_ms"] = Value::from(30_000);
        let store =
            StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
        let mut app = App::open(AppConfig::new(
            store,
            vec![AgentSpec::new("faulty".to_owned(), "v1".to_owned(), agent)],
            vec![WorkflowDefinition::parse(&definition.to_string())?],
        ))
        .await?;
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir()?,
            })
            .await?;
        let workflow = workspace.workflow("stream-failure".parse()?).await?;
        let run = workflow.start(ExecutionOptions::default()).await?;
        let first = tokio::time::timeout(Duration::from_secs(1), run.wait()).await??;
        let [
            Interaction::Notification {
                sequence: first_sequence,
                generation: first_generation,
                notification: first_notification,
            },
        ] = first.interactions.as_slice()
        else {
            panic!("first retained notification target")
        };
        let snapshot = tokio::time::timeout(Duration::from_secs(5), async {
            notifications.emission_completed.notified().await;
            loop {
                let snapshot = run.snapshot().await?;
                let observed = if count <= 256 {
                    notifications.received.load(Ordering::SeqCst) == count
                } else {
                    matches!(snapshot.status, RunStatus::Failed(_))
                        || matches!(
                            snapshot.interactions.as_slice(),
                            [Interaction::Unknown { .. }]
                        )
                };
                if observed {
                    return Ok::<_, AppError>(snapshot);
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "bounded notification delivery stalled: emitted {} of {count}",
                notifications.emitted.load(Ordering::SeqCst)
            )
        })?;
        let resolved = notifications
            .resolved
            .lock()
            .expect("test resolution lock")
            .len();
        assert_eq!(resolved, 0);
        if count <= 256 {
            let [
                Interaction::Notification {
                    sequence,
                    generation,
                    notification,
                },
            ] = snapshot.interactions.as_slice()
            else {
                panic!("queued delivery preserves the first unresolved notification")
            };
            assert_eq!(
                (*sequence, *generation),
                (*first_sequence, *first_generation)
            );
            assert_eq!(notification.id(), first_notification.id());
            run.cancel(snapshot.checkpoint).await?;
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    match run.snapshot().await?.status {
                        RunStatus::Cancelled => break,
                        RunStatus::Cancelling => tokio::task::yield_now().await,
                        _ => panic!("pending-target cancellation must retain its owned outcome"),
                    }
                }
                Ok::<_, AppError>(())
            })
            .await
            .expect("bounded pending ownership must finish its exact lower abort")?;
        } else {
            let mut snapshot = snapshot;
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if matches!(
                        snapshot.status,
                        RunStatus::Failed(WorkflowFailure::Limit(_))
                    ) {
                        break;
                    }
                    if let [
                        Interaction::Unknown {
                            sequence,
                            generation,
                            ..
                        },
                    ] = snapshot.interactions.as_slice()
                    {
                        let observed = snapshot.checkpoint;
                        snapshot = match run.resolve(*sequence, *generation, Resolution::Fail).await
                        {
                            Ok(snapshot) => snapshot,
                            Err(AppError::Conflict(_)) => {
                                // The original abort can retire this exact Unknown
                                // while the caller's stale resolution is in flight.
                                // Only its newer terminal Limit result proves that race.
                                let terminal = run.wait().await?;
                                assert!(terminal.checkpoint > observed);
                                assert!(matches!(
                                    terminal.status,
                                    RunStatus::Failed(WorkflowFailure::Limit(_))
                                ));
                                assert!(terminal.interactions.is_empty());
                                terminal
                            }
                            Err(error) => return Err(error),
                        };
                    } else {
                        snapshot = run.snapshot().await?;
                    }
                    tokio::task::yield_now().await;
                }
                Ok::<_, AppError>(())
            })
            .await
            .expect("overflow must retain and then settle the primary Limit outcome")?;
        }
        assert_eq!(state.prompts.load(Ordering::SeqCst), 1);
        assert_eq!(state.aborts.load(Ordering::SeqCst), 1);
        app.shutdown(Shutdown::Drain {
            timeout: Duration::from_secs(2),
        })
        .await?;
        Ok(())
    }

    async fn assert_notification_resolution_fault(
        fault: NotificationResolutionFault,
        golden: bool,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let capture = InteractionCapture::default();
        let mut trace_targets = None;
        let mut golden_scope = None;
        let retained = golden
            .then(|| std::env::var_os("AI_KIT_APP_GOLDEN_OUTPUT").map(PathBuf::from))
            .flatten();
        let directory = golden.then(|| {
            retained.clone().unwrap_or_else(|| {
                std::env::temp_dir().join(format!("app-unknown-golden-{}", uuid::Uuid::new_v4(),))
            })
        });
        if let Some(directory) = &directory {
            assert!(directory.is_absolute(), "golden output must be absolute");
            std::fs::create_dir(directory)?;
        }
        let (state, agent) = Self::open()?;
        let notifications = NotificationSequence::new(2, false);
        *notifications
            .resolution_fault
            .lock()
            .expect("test fault lock") = fault;
        *state.notifications.lock().expect("test notifications lock") =
            Some(Arc::clone(&notifications));
        let mut source: Value = serde_json::from_str(STREAM_FAILURE)?;
        source["nodes"][0]["deadline_ms"] = Value::from(1_000);
        let store = StateStore::connect(StateStoreConfig::Lance(match &directory {
            Some(directory) => LanceStateStoreConfig::directory(directory.join("state")),
            None => LanceStateStoreConfig::memory(),
        }))
        .await?;
        let native = store.clone();
        let mut app = App::open(AppConfig::new(
            store,
            vec![AgentSpec::new("faulty".to_owned(), "v1".to_owned(), agent)],
            vec![WorkflowDefinition::parse(&source.to_string())?],
        ))
        .await?;
        let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(7), async {
            let workspace = app.create_workspace(WorkspaceConfig {
                name: None, root: std::env::current_dir().unwrap(),
            }).await.unwrap();
            let workflow = workspace.workflow("stream-failure".parse().unwrap()).await.unwrap();
            let run = workflow.start(ExecutionOptions::default()).await.unwrap();
            let waiting = tokio::time::timeout(Duration::from_secs(1), run.wait()).await.unwrap().unwrap();
            let [Interaction::Notification { sequence, generation, .. }] = waiting.interactions.as_slice()
                else { panic!("first notification must be actionable") };
            let target = (*sequence, *generation);
            let mut resolution = Box::pin(run.resolve(target.0, target.1,
                Resolution::Notification(NotificationResolution::Allow)));
            tokio::time::timeout(Duration::from_secs(1), async {
                tokio::select! {
                    _ = notifications.resolution_entered.notified() => {}
                    result = resolution.as_mut() => {
                        tokio::time::timeout(Duration::from_millis(100), notifications.resolution_entered.notified())
                            .await.expect("an App result is not proof that the exact lower fault was entered");
                        drop(result);
                    }
                }
            }).await.expect("the exact lower resolution call must be entered");
            drop(resolution);
            let unknown = tokio::time::timeout(Duration::from_millis(1_500), async {
                loop {
                    let snapshot = run.snapshot().await.unwrap();
                    if matches!(snapshot.interactions.as_slice(), [Interaction::Unknown { .. }]) {
                        break snapshot;
                    }
                    tokio::task::yield_now().await;
                }
            }).await.expect("lost, stalled or panicked resolution remains truthfully Unknown");
            let accepted = notifications.resolved.lock().expect("test resolutions").len();
            assert_eq!(accepted, usize::from(matches!(fault, NotificationResolutionFault::LostReceipt)));
            assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), 1);
            assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), 0);
            let replay = run.resolve(target.0, target.1,
                Resolution::Notification(NotificationResolution::Allow)).await.unwrap();
            assert!(matches!(replay.interactions.as_slice(), [Interaction::Unknown { .. }]),
                "lost lower receipt never permits automatic lower replay");
            let [Interaction::Unknown { sequence, generation, .. }] = unknown.interactions.as_slice()
                else { unreachable!("observed exact Unknown target") };
            run.resolve(*sequence, *generation, Resolution::Fail).await.unwrap();
            if matches!(fault, NotificationResolutionFault::Stall) {
                assert!(matches!(app.shutdown(Shutdown::Drain {
                    timeout: Duration::from_millis(100),
                }).await, Err(AppError::Timeout(_))),
                    "deadline and explicit Fail do not produce the original pending lower return");
                assert!(!notifications.stall_returned.load(Ordering::SeqCst));
                assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), 1);
                assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), 0);
                // Only this external fixture release ends the original lower
                // future. Timeout, Unknown, caller Fail and App Drop do not.
                notifications.resolution_release.notify_one();
            }
            app.shutdown(Shutdown::Drain { timeout: Duration::from_secs(1) }).await.unwrap();
            if matches!(fault, NotificationResolutionFault::Stall) {
                assert!(notifications.stall_returned.load(Ordering::SeqCst));
            }
            assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), 1);
            assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), 0);
            golden_scope = Some(format!("{}/{}/{}/{}", workspace.id(), workflow.id(), run.id(), unknown.occurrence));
            trace_targets = Some((waiting, unknown));
        })).catch_unwind().with_subscriber(capture.clone()).await;
        notifications.resolution_release.notify_one();
        notifications.cancelled.store(true, Ordering::SeqCst);
        notifications.changed.notify_waiters();
        let cleanup = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(4),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(2),
            }),
        ))
        .catch_unwind()
        .await;
        drop(app);
        match observed {
            Ok(result) => {
                result.expect("resolution failure and native receipt ownership stay bounded")
            }
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup
            .expect("resolution cleanup does not panic")
            .expect("resolution cleanup joins")?;
        let (waiting, unknown) =
            trace_targets.expect("both actual public targets were retained for metadata proof");
        capture.assert_target(&waiting, &["pending", "intended", "uncertain"]);
        capture.assert_target(&unknown, &["pending", "applied"]);
        capture.assert_redacted(&[
            &NotificationSequence::id(0).to_string(),
            "both approvals accepted",
        ]);
        if let Some(directory) = directory {
            let scope = golden_scope.expect("actual public Unknown scope");
            let unknown_row = native
                .load(StateKey::new(
                    "app-sdk",
                    format!("history/{scope}/checkpoint/{:020}", unknown.checkpoint,),
                )?)
                .await?
                .expect("immutable real Unknown checkpoint survives joining");
            let head = native
                .load(StateKey::new("app-sdk", format!("run/{scope}"))?)
                .await?
                .expect("joined current head");
            let manifest = serde_json::json!({
                "epoch": unknown_row.record.value["epoch"], "definition": source,
                "scope": scope, "waiting_checkpoint": waiting.checkpoint,
                "unknown_checkpoint": unknown.checkpoint,
                "unknown_row": unknown_row.record.value,
                "unknown_revision": unknown_row.revision.get(),
                "head": head.record.value, "head_revision": head.revision.get(),
                "lower_inventory": state.golden_lower_inventory()?,
                "lower_resolution_calls": notifications.resolution_calls.load(Ordering::SeqCst),
                "lower_resolution_returns": notifications.resolution_returns.load(Ordering::SeqCst),
                "evidence": "real public Unknown then explicit Fail and joined shutdown; historical Unknown, not an unsettled native call",
            });
            drop(native);
            assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
            if retained.is_some() {
                std::fs::write(
                    directory.join("expected.json"),
                    serde_json::to_vec_pretty(&manifest)?,
                )?;
            } else {
                std::fs::remove_dir_all(directory)?;
            }
        }
        Ok(())
    }

    async fn assert_app_preparation_failure(
        fault: StatePreparationFault,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let (faults, store) = StorageFaults::open().await?;
        let (state, agent) = Self::open()?;
        let mut source: Value = serde_json::from_str(STREAM_FAILURE)?;
        let whole_activation = matches!(fault, StatePreparationFault::WholeActivationDeadline);
        source["nodes"][0]["deadline_ms"] = Value::from(if whole_activation { 25 } else { 1_000 });
        let mut app = App::open(AppConfig::new(
            store,
            vec![AgentSpec::new("faulty".to_owned(), "v1".to_owned(), agent)],
            vec![WorkflowDefinition::parse(&source.to_string())?],
        ))
        .await?;
        let observed = AssertUnwindSafe(async {
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir()?,
                })
                .await?;
            let workflow = workspace.workflow("stream-failure".parse()?).await?;
            let prior_lookups = state.workspace_lookups.load(Ordering::SeqCst);
            *faults.preparation.lock().expect("test preparation lock") = if whole_activation {
                StatePreparationFault::StallAfterIntent
            } else {
                fault
            };
            let run = workflow.start(ExecutionOptions::default()).await?;
            if !whole_activation {
                tokio::time::timeout(
                    Duration::from_secs(1),
                    faults.preparation_entered.notified(),
                )
                .await
                .expect("durable intent must enter the selected App storage preparation fault");
            }
            if matches!(fault, StatePreparationFault::FailAfterIntent) {
                tokio::time::timeout(
                    Duration::from_secs(1),
                    faults.run_transition_observed.notified(),
                )
                .await
                .expect("the preparation failure must admit its settlement commit");
                tokio::time::sleep(Duration::from_millis(1_100)).await;
                assert_eq!(
                    faults.maximum_active_run_transitions.load(Ordering::SeqCst),
                    1,
                    "deadline must not replace an admitted settlement commit"
                );
                assert_eq!(
                    faults.active_run_transitions.load(Ordering::SeqCst),
                    1,
                    "the admitted commit stays owned past the activation deadline"
                );
                faults.release_run_transition();
            }
            let failure = tokio::time::timeout(Duration::from_millis(1_500), async {
                loop {
                    match run.snapshot().await?.status {
                        RunStatus::Failed(failure) => return Ok::<_, AppError>(failure),
                        RunStatus::Running => tokio::task::yield_now().await,
                        _ => panic!("App preparation before any Agent call cannot become an unknown lower effect"),
                    }
                }
            }).await.expect("App control-plane preparation must share the activation deadline")?;
            assert!(match fault {
                StatePreparationFault::WholeActivationDeadline
                | StatePreparationFault::StallAfterIntent =>
                    matches!(failure, WorkflowFailure::Timeout(_)),
                StatePreparationFault::PanicAfterIntent =>
                    matches!(failure, WorkflowFailure::Execution(_)),
                StatePreparationFault::FailAfterIntent => matches!(failure, WorkflowFailure::Agent(_)),
                _ => panic!("fixture requires one armed preparation fault"),
            });
            assert_eq!(
                state.workspace_lookups.load(Ordering::SeqCst),
                prior_lookups
            );
            assert_eq!(state.conversations.load(Ordering::SeqCst), 0);
            assert_eq!(state.prompts.load(Ordering::SeqCst), 0);
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        })
        .catch_unwind()
        .await;
        // An assertion may fail while the original failure receipt is held.
        // Release that existing gate before joining the same App owner.
        faults.release_run_transition();
        let cleanup = AssertUnwindSafe(app.shutdown(Shutdown::Drain {
            timeout: Duration::from_secs(1),
        }))
        .catch_unwind()
        .await;
        if !matches!(&cleanup, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
            loop {
                let joined = AssertUnwindSafe(app.shutdown(Shutdown::Drain {
                    timeout: Duration::from_secs(1),
                }))
                .catch_unwind()
                .await;
                // This owner requests valid Drain exclusively; a retained
                // Integration follows finalizer joining. Other outcomes retain
                // the driven owner under the existing process supervisor.
                let settled = matches!(&joined, Ok(Ok(())) | Ok(Err(AppError::Integration(_))));
                eprintln!(
                    "app-preparation-cleanup settlement_joined={settled} outcome_success={}",
                    matches!(&joined, Ok(Ok(())))
                );
                if settled {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
        drop(app);
        match observed {
            Ok(result) => result?,
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
        assert_eq!(faults.active_run_transitions.load(Ordering::SeqCst), 0);
        Ok(())
    }

    async fn assert_preparation_settles(
        fault: PreparationFault,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let (state, agent) = Self::open()?;
        *state
            .preparation_fault
            .lock()
            .expect("preparation fault lock") = fault;
        let mut source: Value = serde_json::from_str(STREAM_FAILURE)?;
        // These cases must reach their exact lower-method fault. The separate
        // short-activation case covers expiration during whole-App preparation.
        source["nodes"][0]["deadline_ms"] = Value::from(200);
        let store =
            StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
        let mut app = App::open(AppConfig::new(
            store,
            vec![AgentSpec::new("faulty".to_owned(), "v1".to_owned(), agent)],
            vec![WorkflowDefinition::parse(&source.to_string())?],
        ))
        .await?;
        let observed = AssertUnwindSafe(async {
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: Some("preparation-fault".to_owned()),
                    root: std::env::current_dir()?,
                })
                .await?;
            let workflow = workspace.workflow("stream-failure".parse()?).await?;
            let started = std::time::Instant::now();
            let run = workflow.start(ExecutionOptions::default()).await?;
            let entered =
                tokio::time::timeout(Duration::from_secs(2), state.preparation_entered.notified())
                    .await;
            if entered.is_err() {
                eprintln!(
                    "preparation-entry outcome=timeout elapsed_ms={} workspace_lookups={} conversations={} prompts={}",
                    started.elapsed().as_millis(),
                    state.workspace_lookups.load(Ordering::SeqCst),
                    state.conversations.load(Ordering::SeqCst),
                    state.prompts.load(Ordering::SeqCst),
                );
            }
            entered.expect("the exact lower conversation call must be entered");
            let observation = tokio::time::timeout(Duration::from_millis(500), async {
                loop {
                    let snapshot = run.wait().await?;
                    if !matches!(snapshot.status, RunStatus::Running)
                        || !snapshot.interactions.is_empty()
                    {
                        return Ok::<_, AppError>(snapshot);
                    }
                }
            })
            .await;
            let snapshot =
                observation.expect("a bounded preparation failure must not orphan Running")?;
            assert!(
                matches!(
                    snapshot.status,
                    RunStatus::Failed(WorkflowFailure::Timeout(_) | WorkflowFailure::Execution(_))
                ) || matches!(
                    snapshot.interactions.as_slice(),
                    [Interaction::Unknown { .. }]
                ),
                "preparation must publish timeout, execution failure or truthful uncertainty"
            );
            assert_eq!(
                state.prompts.load(Ordering::SeqCst),
                0,
                "preparation must not dispatch a prompt"
            );
            if let [
                Interaction::Unknown {
                    sequence,
                    generation,
                    ..
                },
            ] = snapshot.interactions.as_slice()
            {
                tokio::time::timeout(
                    Duration::from_millis(500),
                    run.resolve(*sequence, *generation, Resolution::Fail),
                )
                .await
                .expect(
                    "caller unknown settlement remains bounded under its original target generation",
                )?;
            }
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        })
        .catch_unwind()
        .await;
        let cleanup = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(1),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_millis(750),
            }),
        ))
        .catch_unwind()
        .await;
        let settled = matches!(
            &cleanup,
            Ok(Ok(Ok(()))) | Ok(Ok(Err(AppError::Integration(_))))
        );
        eprintln!(
            "preparation-cleanup observation=initial settlement_joined={settled} outcome_success={}",
            matches!(&cleanup, Ok(Ok(Ok(()))))
        );
        if !settled {
            loop {
                let joined = AssertUnwindSafe(tokio::time::timeout(
                    Duration::from_secs(1),
                    app.shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_millis(750),
                    }),
                ))
                .catch_unwind()
                .await;
                // This owner uses valid CancelAndDrain exclusively. Success or
                // retained Integration follows joining; neither erases the first
                // body failure or original shutdown receipt. Other outcomes retain
                // the same driven owner under external process supervision.
                let settled = matches!(
                    &joined,
                    Ok(Ok(Ok(()))) | Ok(Ok(Err(AppError::Integration(_))))
                );
                eprintln!(
                    "preparation-cleanup observation=subsequent settlement_joined={settled} outcome_success={}",
                    matches!(&joined, Ok(Ok(Ok(()))))
                );
                if settled {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
        drop(app);
        match observed {
            Ok(result) => result?,
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
            .expect("owned preparation cleanup must be bounded")?;
        Ok(())
    }

    fn agent(self: &Arc<Self>) -> StreamFailureAgent {
        StreamFailureAgent {
            state: Arc::clone(self),
        }
    }

    fn workspace_info(&self) -> Result<WorkspaceInfo, SdkError> {
        self.workspace
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
            .ok_or_else(|| SdkError::not_found("test workspace does not exist"))
    }

    fn conversation_info(&self, id: &ConversationId) -> Result<ConversationInfo, SdkError> {
        self.conversation_inventory
            .lock()
            .expect("test conversation inventory lock")
            .get(id)
            .cloned()
            .ok_or_else(|| SdkError::not_found("test conversation does not exist"))
    }

    fn arm_shutdown(&self) {
        self.shutdown_armed.store(true, Ordering::SeqCst);
    }

    fn arm_workspace_update(&self) {
        self.workspace_update_armed.store(true, Ordering::SeqCst);
    }

    fn arm_workspace_check(&self) {
        self.workspace_check_armed.store(true, Ordering::SeqCst);
    }

    fn arm_workspace_deletion(&self) {
        *self
            .workspace_deletion
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            StreamFailureDeletion::BlockThenComplete;
    }

    fn retry_workspace_deletion_then_complete(&self, attempts: usize) {
        *self
            .workspace_deletion
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            StreamFailureDeletion::TransientThenComplete {
                remaining: attempts,
            };
    }

    fn release_workspace_check(&self) {
        self.workspace_check_release.notify_one();
    }

    fn release_workspace_update(&self) {
        self.workspace_update_release.notify_one();
    }

    fn release_workspace_deletion(&self) {
        *self
            .workspace_deletion
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = StreamFailureDeletion::Complete;
        self.workspace_delete_release.notify_waiters();
    }

    fn release_shutdown(&self) {
        self.shutdown_release.notify_one();
    }
}

impl StorageFaults {
    fn apply_checkpoint_read(&self, key: &StateKey, loaded: &mut Option<StateRecordSnapshot>) {
        // Only a successful native commit arms this read-only contradiction,
        // and its missing receipt must be observed before any altered readback.
        if !self.receipt_obscured.load(Ordering::SeqCst)
            && self
                .ordered_receipt
                .lock()
                .unwrap()
                .as_ref()
                .and_then(|(_, record)| record.as_ref())
                .is_some_and(|record| record.key == *key)
            && let Some(snapshot) = loaded
        {
            let original = Arc::clone(&snapshot.record.value);
            let evidence = *self.checkpoint_evidence.lock().unwrap();
            evidence.apply(Arc::make_mut(&mut snapshot.record.value));
            assert_eq!(
                serde_json::to_vec(snapshot.record.value.as_ref()).unwrap()
                    == serde_json::to_vec(original.as_ref()).unwrap(),
                matches!(evidence, CheckpointEvidence::Unchanged),
                "the selected observation-only evidence change must be exercised"
            );
            self.ordered_receipt_reads.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn attempt_read_key(
        &self,
        key: &StateKey,
        loaded: &Option<StateRecordSnapshot>,
    ) -> Result<Option<(AttemptReadCase, StateKey)>, StateStoreError> {
        let attempt_case = *self.attempt_read.lock().unwrap();
        if let Some(case) = attempt_case
            && match case {
                AttemptReadCase::ResolutionObservation { owner: Some(owner) } => {
                    Some(owner) == tokio::task::try_id()
                }
                AttemptReadCase::ResolutionObservation { owner: None } => {
                    tracing::dispatcher::get_default(|dispatch| dispatch.is::<InteractionCapture>())
                        && *self.attempt_owner.lock().unwrap() != tokio::task::try_id()
                }
                _ => true,
            }
            && (!matches!(
                case,
                AttemptReadCase::BeforeWake { .. }
                    | AttemptReadCase::AfterWake { .. }
                    | AttemptReadCase::BeforePrompt { .. }
                    | AttemptReadCase::PromptWake { .. }
                    | AttemptReadCase::PreparationWake { .. }
                    | AttemptReadCase::NotificationDelivery { .. }
                    | AttemptReadCase::PromptFailure { .. }
            ) || self
                .attempt_owner
                .lock()
                .unwrap()
                .is_some_and(|owner| Some(owner) == tokio::task::try_id()))
            && let Some(scope) = key.key().strip_prefix("run/")
            && let Some(head) = loaded
        {
            let sequence = head.record.value["sequence"]
                .as_u64()
                .expect("native run head");
            return Ok(Some((
                case,
                StateKey::new(
                    "app-sdk",
                    format!("history/{scope}/checkpoint/{sequence:020}"),
                )?,
            )));
        }
        Ok(None)
    }

    async fn observe_attempt_read(
        &self,
        case: AttemptReadCase,
        head: &StateRecordSnapshot,
        record: StateRecord,
        settlements: &[Value],
    ) -> Result<(), StateStoreError> {
        let control = record
            .value
            .pointer("/checkpoint/controls")
            .and_then(Value::as_object)
            .and_then(|controls| {
                controls.values().find(|control| {
                    control
                        .pointer("/state/phase/state")
                        .and_then(Value::as_str)
                        == Some(case.native_phase())
                        && match case {
                            AttemptReadCase::ResolutionObservation { owner } => {
                                settlements.iter().any(|target| {
                                    target[2] == control["sequence"]
                                        && target[3][0] == 1
                                        && target[4][0] == if owner.is_some() { 1 } else { 0 }
                                })
                            }
                            AttemptReadCase::PreparationQueued { .. } => {
                                record.value["workflow_id"] == "lease-queued"
                            }
                            AttemptReadCase::PreparationWake { .. } => {
                                record
                                    .value
                                    .pointer("/checkpoint/phase/state")
                                    .and_then(Value::as_str)
                                    == Some("cancelling")
                            }
                            AttemptReadCase::Unsubmitted { .. } => matches!(
                                *self.preparation.lock().unwrap(),
                                StatePreparationFault::Observed
                            ),
                            AttemptReadCase::BeforeWake { .. }
                            | AttemptReadCase::AfterWake { .. }
                            | AttemptReadCase::Preparation { .. }
                            | AttemptReadCase::BeforePrompt { .. }
                            | AttemptReadCase::PromptWake { .. }
                            | AttemptReadCase::NotificationDelivery { .. }
                            | AttemptReadCase::PromptFailure { .. } => true,
                        }
                })
            });
        if let Some(control) = control {
            let selected = match case {
                AttemptReadCase::ResolutionObservation { owner: None } => {
                    // Foreground execution stays parked and the original lower
                    // attempt is excluded. Capture this public resolution's
                    // real Pending observation before holding its Intended read.
                    *self.attempt_read.lock().unwrap() =
                        Some(AttemptReadCase::ResolutionObservation {
                            owner: Some(tokio::task::id()),
                        });
                    false
                }
                AttemptReadCase::ResolutionObservation { owner: Some(_) }
                | AttemptReadCase::Preparation { .. }
                | AttemptReadCase::PreparationQueued { .. }
                | AttemptReadCase::PreparationWake { .. }
                | AttemptReadCase::Unsubmitted { .. }
                | AttemptReadCase::BeforePrompt { .. }
                | AttemptReadCase::PromptWake { .. }
                | AttemptReadCase::NotificationDelivery { .. }
                | AttemptReadCase::PromptFailure { .. } => true,
                AttemptReadCase::BeforeWake { .. } | AttemptReadCase::AfterWake { .. } => {
                    let wake = control
                        .pointer("/state/phase/wake_millis")
                        .and_then(Value::as_u64)
                        .expect("the real retry owns its wake time");
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("test wall clock")
                        .as_millis();
                    match case {
                        AttemptReadCase::BeforeWake { .. } => now < u128::from(wake),
                        AttemptReadCase::AfterWake { .. } => now >= u128::from(wake),
                        AttemptReadCase::ResolutionObservation { .. }
                        | AttemptReadCase::Preparation { .. }
                        | AttemptReadCase::PreparationQueued { .. }
                        | AttemptReadCase::PreparationWake { .. }
                        | AttemptReadCase::Unsubmitted { .. }
                        | AttemptReadCase::BeforePrompt { .. }
                        | AttemptReadCase::PromptWake { .. }
                        | AttemptReadCase::NotificationDelivery { .. }
                        | AttemptReadCase::PromptFailure { .. } => unreachable!(),
                    }
                }
            };
            if selected && self.attempt_read.lock().unwrap().take().is_some() {
                *self.attempt_read_records.lock().unwrap() = Some((head.record.clone(), record));
                let read_drop = self.attempt_read_dropped.lock().unwrap().take();
                self.attempt_read_entered.notify_one();
                let fault = case.fault();
                if matches!(fault, AttemptReadFault::Held)
                    || matches!(fault, AttemptReadFault::Recoverable)
                        && matches!(
                            case,
                            AttemptReadCase::Preparation { .. }
                                | AttemptReadCase::PreparationQueued { .. }
                                | AttemptReadCase::PreparationWake { .. }
                                | AttemptReadCase::BeforePrompt { .. }
                                | AttemptReadCase::PromptWake { .. }
                                | AttemptReadCase::NotificationDelivery { .. }
                                | AttemptReadCase::PromptFailure { .. }
                        )
                {
                    self.attempt_read_release.notified().await;
                }
                drop(read_drop);
                return Err(match fault {
                    AttemptReadFault::Recoverable => StateStoreError::unavailable(
                        "load",
                        "attempt authority is temporarily unavailable",
                    ),
                    AttemptReadFault::Permanent | AttemptReadFault::Held => {
                        StateStoreError::serialization("injected unreadable attempt authority")
                    }
                });
            }
        }
        Ok(())
    }

    async fn logical_checkpoint(native: &StateStore, record: &StateRecord) -> Value {
        let mut logical = (*record.value).clone();
        let descriptor = logical["checkpoint"]["settlements"]
            .as_object()
            .expect("v7 checkpoint descriptor");
        let scope = record.key.key().split_once("/checkpoint/").unwrap().0;
        let mut entries = Vec::new();
        for (index, sequence) in descriptor["pages"].as_array().unwrap().iter().enumerate() {
            let page = native
                .load(
                    StateKey::new(
                        "app-sdk",
                        format!(
                            "{scope}/ledger/{index:02}/{:020}",
                            sequence.as_u64().unwrap()
                        ),
                    )
                    .unwrap(),
                )
                .await
                .unwrap()
                .expect("fresh actual immutable leaf");
            entries.extend(
                page.record.value["entries"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .cloned(),
            );
        }
        assert_eq!(entries.len() as u64, descriptor["count"].as_u64().unwrap());
        logical["checkpoint"]["settlements"] = Value::Array(entries);
        logical
    }

    fn notification_checkpoint_keys(
        &self,
        corruption: NotificationCorruption,
        key: &StateKey,
        head: &Option<StateRecordSnapshot>,
    ) -> Vec<StateKey> {
        if !key.key().contains("/ledger/") {
            return Vec::new();
        }
        let head = head.as_ref().expect("selected real run head");
        let sequence = head.record.value["sequence"].as_u64().unwrap();
        let scope = head.record.key.key().strip_prefix("run/").unwrap();
        let mut selected = vec![
            StateKey::new(
                "app-sdk",
                format!("history/{scope}/checkpoint/{sequence:020}"),
            )
            .unwrap(),
        ];
        if matches!(
            corruption,
            NotificationCorruption::TupleText { .. }
                | NotificationCorruption::ProspectiveBytes { .. }
        ) && let Some(sequence) = sequence.checked_sub(1)
        {
            selected.push(
                StateKey::new(
                    "app-sdk",
                    format!("history/{scope}/checkpoint/{sequence:020}"),
                )
                .unwrap(),
            );
        }
        selected
    }

    fn apply_notification_leaf(
        &self,
        corruption: NotificationCorruption,
        key: &StateKey,
        head: &Option<StateRecordSnapshot>,
        checkpoint: StateRecordSnapshot,
        loaded: &mut Option<StateRecordSnapshot>,
    ) -> bool {
        let references = &checkpoint.record.value["checkpoint"]["settlements"];
        let pages = references["pages"]
            .as_array()
            .expect("actual page descriptor");
        if !matches!(
            corruption,
            NotificationCorruption::LeafKey | NotificationCorruption::LeafNamespace
        ) {
            assert_eq!(
                pages.len(),
                1,
                "these original controlled scenarios contain at most eleven targets"
            );
        }
        let scope = checkpoint
            .record
            .key
            .key()
            .split_once("/checkpoint/")
            .unwrap()
            .0;
        let expected = format!("{scope}/ledger/00/{:020}", pages[0].as_u64().unwrap());
        if key.key() != expected {
            return false;
        }
        let Some(leaf) = loaded else {
            return false;
        };
        if matches!(
            corruption,
            NotificationCorruption::LeafKey | NotificationCorruption::LeafNamespace
        ) {
            leaf.record.key = StateKey::new(
                if matches!(corruption, NotificationCorruption::LeafNamespace) {
                    "foreign-leaf"
                } else {
                    "app-sdk"
                },
                if matches!(corruption, NotificationCorruption::LeafKey) {
                    key.key().replace("/ledger/00/", "/ledger/01/")
                } else {
                    key.key().to_owned()
                },
            )
            .unwrap();
            return true;
        }

        let mut logical = checkpoint.clone();
        Arc::make_mut(&mut logical.record.value)["checkpoint"]["settlements"] =
            leaf.record.value["entries"].clone();
        let mut logical = Some(logical);
        // The wrapper changes a negative response, never native persistence.
        // Reuse the exact original tuple and logical-byte fault assertions.
        self.apply_notification_read(
            corruption,
            &checkpoint.record.key,
            head.clone(),
            &mut logical,
        );
        Arc::make_mut(&mut leaf.record.value)["entries"] =
            logical.unwrap().record.value["checkpoint"]["settlements"].clone();
        true
    }

    fn migration_revision(&self, snapshot: &mut StateRecordSnapshot) {
        if let MigrationFault::RevisionOverflow { key, revision } =
            &*self.migration_fault.lock().unwrap()
            && snapshot.record.key == *key
        {
            snapshot.revision = *revision;
        }
    }

    fn notification_read_key(
        &self,
        key: &StateKey,
    ) -> Result<Option<(NotificationCorruption, StateKey)>, StateStoreError> {
        let corruption = *self
            .notification_corruption
            .lock()
            .expect("test notification corruption lock");
        if !matches!(corruption, NotificationCorruption::None)
            && !matches!(corruption,
                NotificationCorruption::OrphanTarget { observer }
                | NotificationCorruption::ProspectiveBytes { observer }
                if tokio::task::try_id() != observer)
            && key.key().starts_with("history/")
            && let Some((scope, _)) = key.key().strip_prefix("history/").and_then(|key| {
                key.split_once("/checkpoint/")
                    .or_else(|| key.split_once("/ledger/"))
            })
        {
            return Ok(Some((
                corruption,
                StateKey::new("app-sdk", format!("run/{scope}"))?,
            )));
        }
        Ok(None)
    }

    fn apply_notification_read(
        &self,
        corruption: NotificationCorruption,
        key: &StateKey,
        head: Option<StateRecordSnapshot>,
        loaded: &mut Option<StateRecordSnapshot>,
    ) {
        let requested = key
            .key()
            .split_once("/checkpoint/")
            .and_then(|(_, sequence)| sequence.parse::<u64>().ok());
        let current_checkpoint = head.is_some_and(|head| {
            let current = head.record.value.get("sequence").and_then(Value::as_u64);
            current == requested
                || matches!(
                    corruption,
                    NotificationCorruption::ProspectiveBytes { .. }
                        | NotificationCorruption::TupleText { .. }
                ) && current.and_then(|value| value.checked_sub(1)) == requested
        });
        if current_checkpoint && let Some(snapshot) = loaded {
            let checkpoint = Arc::make_mut(&mut snapshot.record.value)
                .get_mut("checkpoint")
                .expect("run checkpoint");
            match corruption {
                NotificationCorruption::None => {}
                NotificationCorruption::LeafKey | NotificationCorruption::LeafNamespace => {
                    unreachable!("leaf identity is altered at the native response boundary")
                }
                NotificationCorruption::ProspectiveBytes { .. } => {
                    let settlements = checkpoint["settlements"]
                        .as_array_mut()
                        .expect("real original target ledger");
                    assert!((10..=11).contains(&settlements.len()));
                    for (index, row) in settlements.iter_mut().take(10).enumerate() {
                        assert_eq!(row[3][2], NotificationSequence::id(index).as_str());
                        assert_eq!(row[4][0], 2);
                        // Change the same retained payload in both actual predecessor
                        // and current reads, preserving successor equality and all IDs.
                        row[3][5] = Value::from(NotificationSequence::body(393_181));
                    }
                    let current = settlements.len() == 11;
                    if current {
                        let row = &mut settlements[10];
                        assert_eq!(row[3][2], NotificationSequence::id(10).as_str());
                        assert_eq!(row[4][0], 0);
                        row[3][5] = Value::from("x");
                    }
                    let bound = 4 * 1_048_576;
                    let bytes = serde_json::to_vec(snapshot.record.value.as_ref())
                        .expect("actual checkpoint encoding")
                        .len();
                    assert!(
                        bytes < bound,
                        "real row metadata and ten legal bodies fit the native bound"
                    );
                    if current {
                        let remaining = bound - bytes + 1;
                        Arc::make_mut(&mut snapshot.record.value)["checkpoint"]["settlements"]
                            [10][3][5] = Value::from(NotificationSequence::body(remaining));
                        assert_eq!(
                            serde_json::to_vec(snapshot.record.value.as_ref())
                                .expect("corrupt checkpoint encoding")
                                .len(),
                            bound,
                            "ordinary record size stays legal while prospective capacity is exhausted"
                        );
                    }
                }
                NotificationCorruption::TupleObject { .. }
                | NotificationCorruption::TupleMissing { .. }
                | NotificationCorruption::TupleExtra { .. }
                | NotificationCorruption::TupleField { .. }
                | NotificationCorruption::TupleTag { .. }
                | NotificationCorruption::TupleTimestampOverflow => {
                    let rows = checkpoint["settlements"].as_array_mut().unwrap();
                    assert_eq!(rows.len(), 2);
                    let row = &mut rows[0];
                    assert_eq!(row.as_array().unwrap().len(), 5);
                    match corruption {
                        NotificationCorruption::TupleObject { path } => {
                            *row.pointer_mut(path).unwrap() = serde_json::json!({});
                        }
                        NotificationCorruption::TupleMissing { path } => {
                            row.pointer_mut(path).unwrap().as_array_mut().unwrap().pop();
                        }
                        NotificationCorruption::TupleExtra { path } => {
                            row.pointer_mut(path)
                                .unwrap()
                                .as_array_mut()
                                .unwrap()
                                .push(Value::Null);
                        }
                        NotificationCorruption::TupleField { path } => {
                            *row.pointer_mut(path).unwrap() = Value::Null;
                        }
                        NotificationCorruption::TupleTag { path } => {
                            *row.pointer_mut(path).unwrap() = Value::from(255);
                        }
                        NotificationCorruption::TupleTimestampOverflow => {
                            row[3][3] = serde_json::json!([u64::MAX, u32::MAX]);
                        }
                        _ => unreachable!("selected tuple corruption"),
                    }
                }
                NotificationCorruption::TupleText { path, text } => {
                    let rows = checkpoint["settlements"].as_array_mut().unwrap();
                    assert!((1..=2).contains(&rows.len()));
                    let row = &mut rows[0];
                    assert_eq!(row[3][2], NotificationSequence::id(0).as_str());
                    assert_eq!(row[4][0], 2, "the same original Applied row is altered");
                    *row.pointer_mut(path).unwrap() = Value::from(text);
                }
                NotificationCorruption::DuplicateTarget => {
                    let notifications = checkpoint["settlements"]
                        .as_array_mut()
                        .expect("two distinct durable target sequences");
                    assert_eq!(notifications.len(), 2);
                    assert_ne!(notifications[0][0], notifications[1][0]);
                    notifications[1][3][2] = notifications[0][3][2].clone();
                }
                NotificationCorruption::OrphanTarget { .. } => {
                    let generation = checkpoint["sequence"]
                        .as_u64()
                        .expect("actual intended checkpoint");
                    let settlements = checkpoint["settlements"]
                        .as_array_mut()
                        .expect("original target ledger");
                    assert_eq!(settlements.len(), 2);
                    let current = settlements.last().unwrap();
                    assert_eq!(current[4][0], 1);
                    assert!(current[1].as_u64().unwrap() < generation);
                    assert_eq!(current[3][2], NotificationSequence::id(1).as_str());
                    let mut orphan = current.clone();
                    orphan[0] = Value::from(generation);
                    orphan[1] = Value::from(generation);
                    orphan[3][2] = Value::from(NotificationSequence::id(2).as_str());
                    orphan[3][4] = Value::from("Approval 3");
                    orphan[4] = serde_json::json!([0]);
                    settlements.push(orphan);
                }
                NotificationCorruption::RemoveApplied
                | NotificationCorruption::ChangeAppliedDecision => {
                    let notifications = checkpoint["settlements"]
                        .as_array_mut()
                        .expect("notification target records");
                    let index = notifications
                        .iter()
                        .position(|target| target[4][0] == 2)
                        .expect("prior exact applied receipt");
                    if matches!(corruption, NotificationCorruption::RemoveApplied) {
                        notifications.remove(index);
                    } else {
                        notifications[index][4][1] =
                            serde_json::json!([2, "corrupted accepted decision"]);
                    }
                }
            }
        }
    }
}

impl StateStoreBackend for FaultStateStore {
    type Transaction = FaultStateTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        // The real Intent already armed this fault. Its next workspace-row
        // observation belongs to preparation before any lower conversation;
        // unrelated run/generation readers cannot consume that original error.
        if key.key().starts_with("workspace/") {
            let preparation = {
                let mut current = self
                    .faults
                    .preparation
                    .lock()
                    .expect("test preparation lock");
                let fault = *current;
                if matches!(
                    fault,
                    StatePreparationFault::StallNextLoad
                        | StatePreparationFault::PanicNextLoad
                        | StatePreparationFault::FailNextLoad
                ) {
                    *current = StatePreparationFault::Observed;
                }
                fault
            };
            match preparation {
                StatePreparationFault::StallNextLoad => {
                    self.faults.preparation_entered.notify_one();
                    std::future::pending::<()>().await;
                }
                StatePreparationFault::PanicNextLoad => {
                    self.faults.preparation_entered.notify_one();
                    panic!("injected App preparation storage panic");
                }
                StatePreparationFault::FailNextLoad => {
                    self.faults.arm_run_transition_monitor();
                    self.faults.preparation_entered.notify_one();
                    return Err(StateStoreError::backend_operation(
                        "load",
                        "injected App preparation read failure",
                    ));
                }
                _ => {}
            }
        }
        if key.key() == "app"
            && self
                .faults
                .app_load_panics
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        {
            self.faults
                .app_load_panic_count
                .fetch_add(1, Ordering::SeqCst);
            panic!("injected repeated app publication panic");
        }
        if key.key().starts_with("receipt/")
            && self.faults.receipt_obscured.swap(false, Ordering::SeqCst)
        {
            self.faults.ambiguity_observed.notify_one();
            return Err(StateStoreError::timed_out("load", 1));
        }
        if key.key().starts_with("run/")
            && self.faults.sleep_load_armed.swap(false, Ordering::SeqCst)
        {
            self.faults.sleep_load_faulted.store(true, Ordering::SeqCst);
            self.faults.sleep_load_observed.notify_one();
            return Err(StateStoreError::timed_out("load", 1));
        }
        if key.key().starts_with("run/") {
            let (scripted, recovered_second) = {
                let mut active = self
                    .faults
                    .active_sleep_failure
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                match active.as_mut() {
                    Some(failure) if failure.remaining > 0 => {
                        failure.remaining -= 1;
                        let mut attempts = self
                            .faults
                            .scripted_sleep_attempts
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        attempts[failure.park] += 1;
                        (Some((failure.park, attempts[failure.park])), false)
                    }
                    Some(failure) => {
                        let recovered_second = failure.park == 1;
                        active.take();
                        (None, recovered_second)
                    }
                    None => (None, false),
                }
            };
            if recovered_second {
                self.faults.second_sleep_recovery_observed.notify_waiters();
            }
            if let Some((park, attempt)) = scripted {
                if park == 1 && attempt == 1 {
                    self.faults.second_sleep_failure_observed.notify_waiters();
                }
                return Err(StateStoreError::backend_operation(
                    "load",
                    "test scripted recoverable state-store failure",
                ));
            }
            let remaining = self.faults.sleep_integration_armed.load(Ordering::SeqCst);
            if remaining == usize::MAX
                || (remaining > 0
                    && self
                        .faults
                        .sleep_integration_armed
                        .compare_exchange(
                            remaining,
                            remaining - 1,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_ok())
            {
                let attempts = self
                    .faults
                    .sleep_integration_attempts
                    .fetch_add(1, Ordering::SeqCst)
                    + 1;
                let notify_at = self
                    .faults
                    .sleep_integration_notify_at
                    .load(Ordering::SeqCst);
                if notify_at > 0 && attempts == notify_at {
                    self.faults.excessive_integration_attempts.notify_one();
                }
                self.faults.sleep_load_observed.notify_one();
                return if remaining == usize::MAX {
                    Err(StateStoreError::serialization(
                        "test persistent state-store integration failure",
                    ))
                } else {
                    Err(StateStoreError::backend_operation(
                        "load",
                        "test one-shot state-store integration failure",
                    ))
                };
            }
        }
        if key.key() == "app"
            && self
                .faults
                .workspace_observation_load_armed
                .swap(false, Ordering::SeqCst)
        {
            self.faults.workspace_observation_load_faulted.notify_one();
            return Err(StateStoreError::timed_out("load", 1));
        }
        let measured = self.faults.observe_start("load");
        let result = self.inner.load(key.clone()).await;
        measured.finish(result.is_err());
        let mut loaded = result?;
        if let Some(snapshot) = &mut loaded {
            self.faults.migration_revision(snapshot);
        }
        self.faults.apply_checkpoint_read(&key, &mut loaded);
        if let Some((case, checkpoint_key)) = self.faults.attempt_read_key(&key, &loaded)? {
            let checkpoint = self
                .inner
                .load(checkpoint_key)
                .await?
                .expect("native checkpoint named by the observed head")
                .record;
            let mut settlements = Vec::new();
            if matches!(case, AttemptReadCase::ResolutionObservation { .. }) {
                let descriptor = &checkpoint.value["checkpoint"]["settlements"];
                let scope = checkpoint.key.key().split_once("/checkpoint/").unwrap().0;
                for (index, sequence) in descriptor["pages"].as_array().unwrap().iter().enumerate()
                {
                    let page_key = StateKey::new(
                        "app-sdk",
                        format!(
                            "{scope}/ledger/{index:02}/{:020}",
                            sequence.as_u64().unwrap()
                        ),
                    )?;
                    // Use the same native owner as the observed checkpoint;
                    // a transaction must retain its original snapshot here.
                    let page = self
                        .inner
                        .load(page_key.clone())
                        .await?
                        .expect("actual settlement page named by the observed checkpoint");
                    assert_eq!(page.record.key, page_key);
                    settlements.extend(
                        page.record.value["entries"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .cloned(),
                    );
                }
                assert_eq!(
                    settlements.len() as u64,
                    descriptor["count"].as_u64().unwrap()
                );
            }
            self.faults
                .observe_attempt_read(
                    case,
                    loaded.as_ref().expect("selected native run head"),
                    checkpoint,
                    &settlements,
                )
                .await?;
        }
        if let Some((corruption, head_key)) = self.faults.notification_read_key(&key)? {
            let head = self.inner.load(head_key).await?;
            if key.key().contains("/ledger/") {
                for checkpoint_key in self
                    .faults
                    .notification_checkpoint_keys(corruption, &key, &head)
                {
                    let checkpoint = self
                        .inner
                        .load(checkpoint_key)
                        .await?
                        .expect("actual selected checkpoint");
                    if self.faults.apply_notification_leaf(
                        corruption,
                        &key,
                        &head,
                        checkpoint,
                        &mut loaded,
                    ) {
                        break;
                    }
                }
            } else if let Some(snapshot) = &mut loaded {
                let selected = head.as_ref().is_some_and(|head| {
                    head.record.value["sequence"] == snapshot.record.value["checkpoint"]["sequence"]
                });
                if selected
                    && matches!(
                        corruption,
                        NotificationCorruption::RemoveApplied
                            | NotificationCorruption::OrphanTarget { .. }
                    )
                {
                    let count = snapshot.record.value["checkpoint"]["settlements"]["count"]
                        .as_u64()
                        .unwrap();
                    Arc::make_mut(&mut snapshot.record.value)["checkpoint"]["settlements"]["count"] =
                        Value::from(
                            if matches!(corruption, NotificationCorruption::RemoveApplied) {
                                count - 1
                            } else {
                                count + 1
                            },
                        );
                }
            }
        }
        if key.key() == "app"
            && loaded.as_ref().is_some_and(|snapshot| {
                snapshot
                    .record
                    .value
                    .pointer("/shutdown/state")
                    .and_then(Value::as_str)
                    == Some("draining")
            })
            && self
                .faults
                .preclosed_load_armed
                .swap(false, Ordering::SeqCst)
        {
            self.faults.preclosed_load_observed.notify_one();
            self.faults.preclosed_load_release.notified().await;
        }
        Ok(loaded)
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        if request
            .key_prefix()
            .is_some_and(|prefix| prefix.as_str() == "active/")
            && self
                .faults
                .admission_inventory_armed
                .swap(false, Ordering::SeqCst)
        {
            self.faults.admission_inventory_observed.notify_one();
            self.faults.admission_inventory_release.notified().await;
        }
        if request
            .key_prefix()
            .is_some_and(|prefix| prefix.as_str() == "active/")
            && self
                .faults
                .shutdown_scan_armed
                .swap(false, Ordering::SeqCst)
        {
            self.faults.shutdown_scan_observed.notify_one();
            self.faults.shutdown_scan_release.notified().await;
        }
        let short = {
            let mut fault = self.faults.inventory_page.lock().unwrap();
            if let InventoryPageFault::Short {
                prefix,
                extra,
                pages,
                corruption,
            } = &mut *fault
            {
                if request.prefix() == *prefix {
                    if request.start_after_key().is_none() {
                        *pages = 0;
                    }
                    *pages += 1;
                    self.faults
                        .inventory_page_reads
                        .fetch_add(1, Ordering::SeqCst);
                    Some((
                        1 + extra
                            .saturating_sub((*pages - 1) * (request.limit() - 1))
                            .min(request.limit() - 1),
                        corruption
                            .filter(|(at, _)| *at == *pages)
                            .map(|(_, kind)| kind),
                    ))
                } else {
                    None
                }
            } else {
                None
            }
        };
        if let Some((limit, corruption)) = short {
            let previous = request.start_after_key().map(str::to_owned);
            let requested = request.limit();
            let limit = if matches!(corruption, Some(InventoryCorruption::Oversized)) {
                requested + 1
            } else {
                limit.min(requested)
            };
            let mut page = self.inner.list(request.with_limit(limit)?).await?;
            if let Some(corruption) = corruption {
                match corruption {
                    InventoryCorruption::Epoch => {
                        Arc::make_mut(&mut page.records.first_mut().unwrap().record.value)["epoch"] =
                            Value::from("app/v5");
                    }
                    InventoryCorruption::Namespace => {
                        let row = &mut page.records[0];
                        row.record.key = StateKey::new("foreign-app-page", row.record.key.key())?;
                    }
                    InventoryCorruption::Prefix => {
                        let row = &mut page.records[0];
                        row.record.key = StateKey::new("app-sdk", "outside-requested-prefix")?;
                    }
                    InventoryCorruption::Oversized => assert!(page.records.len() > requested),
                    InventoryCorruption::EmptyWithCursor => {
                        assert!(page.next_start_after.is_some());
                        page.records.clear();
                    }
                    InventoryCorruption::Nonprogressing => {
                        page.next_start_after =
                            Some(previous.expect("a later real page has a cursor"));
                    }
                }
                *self.faults.inventory_page.lock().unwrap() = InventoryPageFault::Observed;
            }
            if page
                .records
                .first()
                .is_some_and(|row| row.record.key.key().starts_with("receipt/"))
                && let Some(values) = self.faults.receipt_page_values.lock().unwrap().as_mut()
            {
                // Observe the returned page independently of native cache or
                // retained raw snapshots sharing the same JSON allocation.
                for row in &mut page.records {
                    row.record.value = Arc::new(row.record.value.as_ref().clone());
                }
                values.retain(|value| value.strong_count() > 0);
                values.extend(
                    page.records
                        .iter()
                        .map(|row| Arc::downgrade(&row.record.value)),
                );
                self.faults
                    .receipt_page_peak
                    .fetch_max(values.len(), Ordering::SeqCst);
            }
            return Ok(page);
        }
        let fault = *self.faults.inventory_page.lock().unwrap();
        let selected = match fault {
            InventoryPageFault::ForeignNamespace | InventoryPageFault::CursorMismatch => {
                request.key_prefix().is_none()
            }
            InventoryPageFault::WorkspacePrefix => request
                .key_prefix()
                .is_some_and(|prefix| prefix.as_str() == "workspace/"),
            InventoryPageFault::None
            | InventoryPageFault::Observed
            | InventoryPageFault::Short { .. } => false,
        };
        let measured = self.faults.observe_start("list");
        let result = self.inner.list(request).await;
        measured.finish(result.is_err());
        let mut page = result?;
        for row in &mut page.records {
            self.faults.migration_revision(row);
        }
        if selected {
            match fault {
                InventoryPageFault::ForeignNamespace => {
                    let row = page.records.first_mut().expect("real App preflight row");
                    row.record.key = StateKey::new("foreign-app-page", row.record.key.key())?;
                }
                InventoryPageFault::WorkspacePrefix => {
                    let row = page
                        .records
                        .first_mut()
                        .expect("real workspace inventory row");
                    let id = row.record.value["id"]
                        .as_str()
                        .expect("real workspace identity");
                    row.record.key = StateKey::new("app-sdk", format!("receipt/{id}"))?;
                }
                InventoryPageFault::CursorMismatch => {
                    assert!(
                        page.records.len() >= 2,
                        "real App and workspace preflight rows"
                    );
                    page.next_start_after = Some(page.records[0].record.key.key().to_owned());
                }
                InventoryPageFault::None
                | InventoryPageFault::Observed
                | InventoryPageFault::Short { .. } => unreachable!(),
            }
            *self.faults.inventory_page.lock().unwrap() = InventoryPageFault::Observed;
        }
        Ok(page)
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        {
            let mut fault = self.faults.migration_fault.lock().unwrap();
            if let MigrationFault::FailNextBegin { progress } = &*fault {
                *fault = MigrationFault::Stopped {
                    progress: progress.clone(),
                };
                return Err(StateStoreError::serialization(
                    "one migration admission fails after its selected actual publication",
                ));
            }
        }

        let remaining = self
            .faults
            .transaction_begin_failures
            .load(Ordering::SeqCst);
        if remaining > 0
            && self
                .faults
                .transaction_begin_failures
                .compare_exchange(remaining, remaining - 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            return Err(StateStoreError::backend_operation(
                "begin_transaction",
                "test scripted mutation admission failure",
            ));
        }
        // Fresh-run fixtures intentionally gate before native snapshot capture.
        // Existing-run fixtures select the first mutation receipt load instead.
        let monitored = self
            .faults
            .app_store_admission_monitor
            .load(Ordering::SeqCst)
            && !self
                .faults
                .app_store_mutation_boundary
                .load(Ordering::SeqCst);
        if monitored {
            self.faults.observe_app_store_admission().await;
        }
        let measured = self.faults.observe_start("begin");
        let transaction = self.inner.begin_transaction().await;
        measured.finish(transaction.is_err());
        if monitored {
            self.faults
                .active_run_transitions
                .fetch_sub(1, Ordering::SeqCst);
        }
        Ok(FaultStateTransaction {
            inner: transaction?,
            faults: Arc::clone(&self.faults),
            admits_run: false,
            admission_record: None,
            admits_workspace: false,
            parks_sleep: false,
            parks_agent: false,
            publishes_cancelling: false,
            advances_run: false,
            monitored_mutation: false,
            mutation_receipt_seen: false,
            sibling_admission: None,
            ordered_receipt: None,
            receipt_writes: 0,
            receipt_deletes: 0,
            staged_mutation: false,
            observation: ObservedStorageTransaction::new(&self.faults),
            migration_progress: None,
            settlement_rollback_failure: false,
        })
    }
}

impl StateTransactionBackend for FaultStateTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        // App's mutation owner checks its new receipt before staging writes.
        // A read-only authority transaction never reads a receipt key, so it
        // must not consume this deliberately mutation-scoped failure.
        if key.key().starts_with("receipt/")
            && self
                .faults
                .mutation_admission_failures
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        {
            return Err(StateStoreError::backend_operation(
                "load",
                "test scripted mutation admission failure",
            ));
        }
        let monitored = !self.monitored_mutation
            && key.key().starts_with("receipt/")
            && self
                .faults
                .app_store_admission_monitor
                .load(Ordering::SeqCst)
            && self
                .faults
                .app_store_mutation_boundary
                .load(Ordering::SeqCst);
        if monitored {
            self.monitored_mutation = self.faults.observe_app_store_admission().await;
            if self.monitored_mutation {
                self.faults
                    .monitored_mutations
                    .fetch_add(1, Ordering::SeqCst);
            }
        }
        let measured = self
            .faults
            .observe_start("transaction_load")
            .within(self.observation.as_mut());
        let result = self.inner.load(key.clone()).await;
        measured.finish(result.is_err());
        if monitored {
            self.faults
                .active_run_transitions
                .fetch_sub(1, Ordering::SeqCst);
        }
        let mut loaded = result?;
        {
            let mut fault = self.faults.migration_fault.lock().unwrap();
            if let MigrationFault::ReadbackRejected {
                progress,
                mismatched_revision,
                reads,
                ..
            } = &mut *fault
                && key == progress.key
                && *reads == 0
            {
                let actual = loaded
                    .as_mut()
                    .expect("the consuming native migration commit published its root");
                assert_eq!(actual.record.value, progress.value);
                *reads += 1;
                // One deliberate caller-boundary response after the real read:
                // never change persisted bytes or claim a native read failure.
                if *mismatched_revision {
                    actual.revision = StateRevision::new(actual.revision.get() + 1)?;
                } else {
                    return Err(StateStoreError::timed_out("load", 1));
                }
            }
        }
        if let Some(snapshot) = &mut loaded {
            self.faults.migration_revision(snapshot);
        }
        {
            let mut fault = self.faults.settlement_rollback_fault.lock().unwrap();
            if let Some(fault) = fault.as_mut() {
                if key.key().starts_with("receipt/") && !self.mutation_receipt_seen {
                    fault.attempts += 1;
                }
                if self.mutation_receipt_seen && fault.key == key && fault.remaining > 0 {
                    assert!(
                        loaded.is_some(),
                        "the selected native leaf lookup succeeds before its failed observation"
                    );
                    fault.loads += 1;
                    fault.remaining -= 1;
                    self.settlement_rollback_failure = true;
                    // Explicit caller-boundary observation fault after a real
                    // native lookup; this is not a native Lance conflict witness.
                    return Err(fault.primary.clone());
                }
            }
        }
        // Only mutation preparation checks its receipt before the checkpoint.
        // Read-only recovery must first observe the unmodified authored row.
        self.mutation_receipt_seen |= key.key().starts_with("receipt/");
        if self.mutation_receipt_seen
            && self.faults.predecessor_evidence.lock().unwrap().as_ref() == Some(&key)
            && let Some(snapshot) = &mut loaded
        {
            let evidence = *self.faults.checkpoint_evidence.lock().unwrap();
            evidence.apply(Arc::make_mut(&mut snapshot.record.value));
            self.faults.predecessor_reads.fetch_add(1, Ordering::SeqCst);
        }
        self.faults.apply_checkpoint_read(&key, &mut loaded);
        if let Some((case, checkpoint_key)) = self.faults.attempt_read_key(&key, &loaded)? {
            let checkpoint = self
                .inner
                .load(checkpoint_key)
                .await?
                .expect("native checkpoint named by the observed head")
                .record;
            let mut settlements = Vec::new();
            if matches!(case, AttemptReadCase::ResolutionObservation { .. }) {
                let descriptor = &checkpoint.value["checkpoint"]["settlements"];
                let scope = checkpoint.key.key().split_once("/checkpoint/").unwrap().0;
                for (index, sequence) in descriptor["pages"].as_array().unwrap().iter().enumerate()
                {
                    let page_key = StateKey::new(
                        "app-sdk",
                        format!(
                            "{scope}/ledger/{index:02}/{:020}",
                            sequence.as_u64().unwrap()
                        ),
                    )?;
                    // Use the same native owner as the observed checkpoint;
                    // a transaction must retain its original snapshot here.
                    let page = self
                        .inner
                        .load(page_key.clone())
                        .await?
                        .expect("actual settlement page named by the observed checkpoint");
                    assert_eq!(page.record.key, page_key);
                    settlements.extend(
                        page.record.value["entries"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .cloned(),
                    );
                }
                assert_eq!(
                    settlements.len() as u64,
                    descriptor["count"].as_u64().unwrap()
                );
            }
            self.faults
                .observe_attempt_read(
                    case,
                    loaded.as_ref().expect("selected native run head"),
                    checkpoint,
                    &settlements,
                )
                .await?;
        }
        if let Some((corruption, head_key)) = self.faults.notification_read_key(&key)? {
            let head = self.inner.load(head_key).await?;
            if key.key().contains("/ledger/") {
                for checkpoint_key in self
                    .faults
                    .notification_checkpoint_keys(corruption, &key, &head)
                {
                    let checkpoint = self
                        .inner
                        .load(checkpoint_key)
                        .await?
                        .expect("actual selected checkpoint");
                    if self.faults.apply_notification_leaf(
                        corruption,
                        &key,
                        &head,
                        checkpoint,
                        &mut loaded,
                    ) {
                        break;
                    }
                }
            } else if let Some(snapshot) = &mut loaded {
                let selected = head.as_ref().is_some_and(|head| {
                    head.record.value["sequence"] == snapshot.record.value["checkpoint"]["sequence"]
                });
                if selected
                    && matches!(
                        corruption,
                        NotificationCorruption::RemoveApplied
                            | NotificationCorruption::OrphanTarget { .. }
                    )
                {
                    let count = snapshot.record.value["checkpoint"]["settlements"]["count"]
                        .as_u64()
                        .unwrap();
                    Arc::make_mut(&mut snapshot.record.value)["checkpoint"]["settlements"]["count"] =
                        Value::from(
                            if matches!(corruption, NotificationCorruption::RemoveApplied) {
                                count - 1
                            } else {
                                count + 1
                            },
                        );
                }
            }
        }
        Ok(loaded)
    }

    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        if record.key.key() == "app" {
            let fault = self.faults.migration_fault.lock().unwrap();
            if matches!(&*fault, MigrationFault::StopAfter { boundary } if boundary.observes(&record.value))
                || matches!(&*fault, MigrationFault::LoseCommitReply { boundary, after }
                    if boundary.observes(&record.value) && after.as_ref().is_none_or(|key|
                        record.value.pointer("/migration/phase/after").and_then(Value::as_str) == Some(key.as_str())))
                || matches!(&*fault, MigrationFault::LoseReplyBeforeReadback { after, .. }
                    if MigrationBoundary::Checkpoint.observes(&record.value)
                        && record.value.pointer("/migration/phase/after").and_then(Value::as_str) == Some(after.as_str()))
                || matches!(&*fault, MigrationFault::ConcurrentClaims { .. } if MigrationBoundary::Claim.observes(&record.value))
            {
                self.migration_progress = Some(record.clone());
            }
        }
        self.receipt_writes += usize::from(record.key.key().starts_with("receipt/"));
        if record.key.key().starts_with("history/") && record.key.key().contains("/checkpoint/") {
            if self
                .faults
                .ordered_receipt
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|(sequence, original)| {
                    original.is_none()
                        && record
                            .value
                            .pointer("/checkpoint/sequence")
                            .and_then(Value::as_u64)
                            == Some(*sequence)
                })
            {
                self.ordered_receipt = Some(record.clone());
            }
            self.admits_run |= record
                .value
                .pointer("/checkpoint/sequence")
                .and_then(Value::as_u64)
                == Some(0);
            if record
                .value
                .pointer("/checkpoint/sequence")
                .and_then(Value::as_u64)
                == Some(0)
                && self.faults.admission_attempts.lock().unwrap().is_some()
            {
                assert!(self.admission_record.replace(record.clone()).is_none());
            }
            self.advances_run |= record
                .value
                .pointer("/checkpoint/sequence")
                .and_then(Value::as_u64)
                .is_some_and(|sequence| sequence > 0);
            // An immutable checkpoint can retain several old controls. Arm
            // only the control admitted at this checkpoint, never a sibling's
            // later publication that happens to retain the same intent/sleep.
            let sequence = record
                .value
                .pointer("/checkpoint/sequence")
                .and_then(Value::as_u64);
            if let Some(controls) = record
                .value
                .pointer("/checkpoint/controls")
                .and_then(Value::as_object)
            {
                for control in controls
                    .values()
                    .filter(|control| control.get("sequence").and_then(Value::as_u64) == sequence)
                {
                    if control.get("node").and_then(Value::as_str) == Some("b-control") {
                        self.sibling_admission =
                            self.faults.sibling_admission.lock().unwrap().take();
                        if self.sibling_admission.is_some() {
                            self.faults
                                .sibling_sequence
                                .store(sequence.unwrap() as usize, Ordering::SeqCst);
                        }
                    }
                    self.parks_agent |= control.pointer("/state/kind").and_then(Value::as_str)
                        == Some("attempt")
                        && control
                            .pointer("/state/phase/state")
                            .and_then(Value::as_str)
                            == Some("intent");
                    self.parks_sleep |=
                        control.pointer("/state/kind").and_then(Value::as_str) == Some("sleep");
                }
            }
        }
        if record.key.key().starts_with("workspace/") {
            self.admits_workspace |=
                record.value.pointer("/state/state").and_then(Value::as_str) == Some("creating");
        }
        self.publishes_cancelling |= record.key.key() == "app"
            && record
                .value
                .pointer("/shutdown/state")
                .and_then(Value::as_str)
                == Some("cancelling");
        let measured = self
            .faults
            .observe_start("put")
            .within(self.observation.as_mut());
        let result = self.inner.put(record).await;
        measured.finish(result.is_err());
        self.staged_mutation |= result.is_ok();
        if let Some(observation) = &mut self.observation {
            observation.mutating = self.staged_mutation;
        }
        result
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        if key.key().starts_with("receipt/") {
            if self
                .faults
                .reject_receipt_cleanup
                .swap(false, Ordering::SeqCst)
            {
                return Err(StateStoreError::unavailable(
                    "delete",
                    "one retired receipt cleanup fails",
                ));
            }
            self.receipt_deletes += 1;
        }
        let measured = self
            .faults
            .observe_start("delete")
            .within(self.observation.as_mut());
        let result = self.inner.delete(key).await;
        measured.finish(result.is_err());
        self.staged_mutation |= result.is_ok();
        if let Some(observation) = &mut self.observation {
            observation.mutating = self.staged_mutation;
        }
        result
    }

    async fn commit(self) -> Result<(), StateStoreError> {
        let Self {
            inner,
            faults,
            admits_run,
            admission_record,
            admits_workspace,
            parks_sleep,
            parks_agent,
            publishes_cancelling,
            advances_run,
            monitored_mutation,
            mutation_receipt_seen: _,
            sibling_admission,
            ordered_receipt,
            receipt_writes,
            receipt_deletes,
            staged_mutation,
            mut observation,
            migration_progress,
            settlement_rollback_failure,
        } = self;
        if let Some(case) = sibling_admission {
            // Complete or roll back the actual native transaction before holding
            // its receipt, so earlier sibling resolutions retain native authority.
            if let Some(observation) = &mut observation {
                observation.finalize();
            }
            let result = match case {
                SiblingAdmissionCase::RejectedInput | SiblingAdmissionCase::RejectedAgent => {
                    inner.rollback().await
                }
                SiblingAdmissionCase::LostAgent
                | SiblingAdmissionCase::LostInput
                | SiblingAdmissionCase::CorruptAgent => inner.commit().await,
            };
            if let Some(observation) = observation {
                observation.finish(result.is_err());
            }
            result?;
            faults.run_transition_observed.notify_one();
            faults.run_transition_release.notified().await;
            return match case {
                SiblingAdmissionCase::RejectedInput | SiblingAdmissionCase::RejectedAgent => {
                    Err(StateStoreError::transaction_conflict(
                        "commit",
                        "injected rejected sibling admission",
                    ))
                }
                SiblingAdmissionCase::LostAgent
                | SiblingAdmissionCase::LostInput
                | SiblingAdmissionCase::CorruptAgent => {
                    faults.receipt_obscured.store(true, Ordering::SeqCst);
                    Err(StateStoreError::timed_out("commit", 1))
                }
            };
        }
        if settlement_rollback_failure {
            faults
                .settlement_rollback_fault
                .lock()
                .unwrap()
                .as_mut()
                .expect("selected leaf failure remains owned")
                .commits += 1;
        }
        let monitored = advances_run && faults.run_transition_monitor.load(Ordering::SeqCst);
        if monitored {
            let active = faults.active_run_transitions.fetch_add(1, Ordering::SeqCst) + 1;
            faults
                .maximum_active_run_transitions
                .fetch_max(active, Ordering::SeqCst);
            if faults
                .run_transition_gate_armed
                .swap(false, Ordering::SeqCst)
            {
                faults.run_transition_observed.notify_one();
                faults.run_transition_release.notified().await;
            }
        }
        if admission_record.is_some()
            && faults
                .admission_commit_holds
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        {
            faults.admission_commit_entered.notify_one();
            faults.admission_commit_release.notified().await;
        }
        let held_claim = migration_progress.is_some()
            && matches!(
                &*faults.migration_fault.lock().unwrap(),
                MigrationFault::ConcurrentClaims { .. }
            );
        if held_claim {
            let released = faults.admission_commit_release.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            let wait = if let MigrationFault::ConcurrentClaims {
                entered,
                released: was_released,
                ..
            } = &mut *faults.migration_fault.lock().unwrap()
            {
                *entered += 1;
                !*was_released
            } else {
                false
            };
            faults.admission_commit_entered.notify_one();
            if wait {
                released.await;
            }
        }
        let measured = faults.observe_start(if staged_mutation {
            "commit_mutating"
        } else {
            "commit_read_only"
        });
        if let Some(observation) = &mut observation {
            observation.finalize();
        }
        let committed = inner.commit().await;
        measured.finish(committed.is_err());
        if let MigrationFault::ReadbackRejected { reads, commits, .. } =
            &mut *faults.migration_fault.lock().unwrap()
            && *reads > 0
        {
            *commits += 1;
        }
        if held_claim {
            let mut fault = faults.migration_fault.lock().unwrap();
            if let MigrationFault::ConcurrentClaims {
                succeeded,
                conflicted,
                ..
            } = &mut *fault
            {
                *succeeded += usize::from(committed.is_ok());
                *conflicted += usize::from(matches!(
                    &committed,
                    Err(StateStoreError::TransactionConflict { .. })
                ));
            }
        }
        if let Some(observation) = observation {
            observation.finish(committed.is_err());
        }
        if monitored_mutation
            && matches!(&committed, Err(StateStoreError::TransactionConflict { .. }))
        {
            faults
                .monitored_mutation_conflicts
                .fetch_add(1, Ordering::SeqCst);
        }
        if let Some(record) = admission_record {
            let mut attempts = faults.admission_attempts.lock().unwrap();
            let attempts = attempts.as_mut().expect("armed admission observation");
            assert!(
                attempts.len() < 144,
                "initial calls and fresh caller progress are bounded"
            );
            attempts.push(AdmissionAttempt {
                record,
                outcome: committed.clone(),
            });
        }
        if monitored {
            faults.active_run_transitions.fetch_sub(1, Ordering::SeqCst);
        }
        committed?;
        if let Some(progress) = migration_progress {
            let mut fault = faults.migration_fault.lock().unwrap();
            match &*fault {
                MigrationFault::ConcurrentClaims { .. } => {}
                MigrationFault::StopAfter { .. } => {
                    *fault = MigrationFault::FailNextBegin { progress };
                }
                MigrationFault::LoseReplyBeforeReadback {
                    mismatched_revision,
                    ..
                } => {
                    let mismatched_revision = *mismatched_revision;
                    *fault = MigrationFault::ReadbackRejected {
                        progress,
                        mismatched_revision,
                        reads: 0,
                        rollbacks: 0,
                        commits: 0,
                    };
                    return Err(StateStoreError::backend_operation(
                        "commit",
                        "selected migration publication completed before its reply failed",
                    ));
                }
                MigrationFault::LoseCommitReply { .. } => {
                    *fault = MigrationFault::CommitReplyLost { progress };
                    return Err(StateStoreError::backend_operation(
                        "commit",
                        "selected migration publication completed before its reply failed",
                    ));
                }
                _ => {
                    unreachable!("selected publication retains its armed fault until native commit")
                }
            }
        }
        faults
            .receipt_writes
            .fetch_add(receipt_writes, Ordering::SeqCst);
        if receipt_deletes > 0 {
            faults
                .receipt_cleanup_batches
                .lock()
                .unwrap()
                .push(receipt_deletes);
        }
        if let Some(record) = ordered_receipt {
            let mut selected = faults.ordered_receipt.lock().unwrap();
            let (_, original) = selected
                .as_mut()
                .expect("selected actual checkpoint commit");
            assert!(
                original.replace(record).is_none(),
                "only one native commit is faulted"
            );
            faults.receipt_obscured.store(true, Ordering::SeqCst);
            return Err(StateStoreError::unavailable(
                "commit",
                "test obscured an applied ordered checkpoint receipt",
            ));
        }
        if parks_agent {
            if matches!(
                *faults.attempt_read.lock().unwrap(),
                Some(
                    AttemptReadCase::BeforeWake { .. }
                        | AttemptReadCase::AfterWake { .. }
                        | AttemptReadCase::BeforePrompt { .. }
                        | AttemptReadCase::PromptWake { .. }
                )
            ) {
                // The real native Intent's original publisher retains the
                // attempt. Other observers must not steal its read fault.
                faults
                    .attempt_owner
                    .lock()
                    .unwrap()
                    .get_or_insert(tokio::task::id());
            }
            faults.agent_intents.fetch_add(1, Ordering::SeqCst);
            faults.agent_intents_observed.notify_waiters();
            let mut preparation = faults.preparation.lock().expect("test preparation lock");
            *preparation = match *preparation {
                StatePreparationFault::StallAfterIntent => StatePreparationFault::StallNextLoad,
                StatePreparationFault::PanicAfterIntent => StatePreparationFault::PanicNextLoad,
                StatePreparationFault::FailAfterIntent => StatePreparationFault::FailNextLoad,
                current => current,
            };
        }
        if admits_run && faults.admission_panics.swap(false, Ordering::SeqCst) {
            faults.admission_committed.notify_one();
            panic!("injected panic after durable run admission");
        }
        if publishes_cancelling {
            faults
                .cancelling_publications
                .fetch_add(1, Ordering::SeqCst);
        }
        if admits_run && faults.admission_armed.swap(false, Ordering::SeqCst) {
            faults.admission_committed.notify_one();
            faults.admission_release.notified().await;
            faults.receipt_obscured.store(true, Ordering::SeqCst);
            if faults
                .admission_inventory_requested
                .swap(false, Ordering::SeqCst)
            {
                faults
                    .admission_inventory_armed
                    .store(true, Ordering::SeqCst);
            }
            return Err(StateStoreError::unavailable(
                "commit",
                "test obscured an applied admission receipt",
            ));
        }
        if admits_workspace
            && faults
                .workspace_admission_armed
                .swap(false, Ordering::SeqCst)
        {
            faults.receipt_obscured.store(true, Ordering::SeqCst);
            faults.workspace_admission_committed.notify_one();
            faults.workspace_admission_release.notified().await;
            faults
                .workspace_observation_load_armed
                .store(true, Ordering::SeqCst);
            return Err(StateStoreError::unavailable(
                "commit",
                "test obscured an applied workspace admission receipt",
            ));
        }
        if parks_sleep && faults.sleep_fault_requested.swap(false, Ordering::SeqCst) {
            faults.sleep_load_armed.store(true, Ordering::SeqCst);
            faults.sleep_load_faulted.store(false, Ordering::SeqCst);
        }
        if parks_sleep {
            let scripted = faults
                .scripted_sleep_failures
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .pop_front();
            if let Some(remaining) = scripted {
                let park = {
                    let mut attempts = faults
                        .scripted_sleep_attempts
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    let park = attempts.len();
                    attempts.push(0);
                    park
                };
                *faults
                    .active_sleep_failure
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                    Some(ActiveSleepFailure { remaining, park });
            }
            let requested = faults.sleep_integration_requested.swap(0, Ordering::SeqCst);
            if requested > 0 {
                faults
                    .sleep_integration_armed
                    .store(requested, Ordering::SeqCst);
                faults.sleep_integration_attempts.store(0, Ordering::SeqCst);
            }
        }
        Ok(())
    }

    async fn rollback(mut self) -> Result<(), StateStoreError> {
        let measured = self.faults.observe_start("rollback");
        if let Some(observation) = &mut self.observation {
            observation.finalize();
        }
        let result = self.inner.rollback().await;
        measured.finish(result.is_err());
        if result.is_ok()
            && let MigrationFault::ReadbackRejected {
                reads, rollbacks, ..
            } = &mut *self.faults.migration_fault.lock().unwrap()
            && *reads > 0
        {
            *rollbacks += 1;
        }
        if self.settlement_rollback_failure && result.is_ok() {
            self.faults
                .settlement_rollback_fault
                .lock()
                .unwrap()
                .as_mut()
                .expect("selected leaf failure remains owned")
                .rollbacks += 1;
            if let Some(observation) = self.observation {
                observation.finish(true);
            }
            return Err(StateStoreError::backend_operation(
                "rollback",
                "selected settlement rollback reply failed after joining",
            ));
        }
        if let Some(observation) = self.observation {
            observation.finish(result.is_err());
        }
        result
    }
}

impl AgentBackend for StreamFailureAgent {
    type Workspace = StreamFailureWorkspace;

    fn id(&self) -> AgentId {
        self.state.agent_id.clone()
    }

    async fn info(&self) -> Result<AgentInfo, SdkError> {
        AgentInfo::new(
            self.state.agent_id.clone(),
            Some("Stream failure agent".to_owned()),
        )
    }

    async fn new_workspace(&self, request: NewWorkspace) -> Result<Self::Workspace, SdkError> {
        let info = WorkspaceInfo::new(
            self.state.workspace_id.clone(),
            request.name().to_owned(),
            request.context().clone(),
            request.defaults().clone(),
            1,
        )?;
        *self
            .state
            .workspace
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(info);
        StreamFailureWorkspace::claim(&self.state)
    }

    async fn list_workspaces(&self) -> Result<Vec<WorkspaceInfo>, SdkError> {
        Ok(self
            .state
            .workspace
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .cloned()
            .collect())
    }

    async fn workspace_info(&self, id: WorkspaceId) -> Result<WorkspaceInfo, SdkError> {
        if id != self.state.workspace_id {
            return Err(SdkError::not_found("test workspace id is unknown"));
        }
        self.state.workspace_info()
    }

    async fn get_workspace(&self, id: WorkspaceId) -> Result<Self::Workspace, SdkError> {
        self.state.workspace_lookups.fetch_add(1, Ordering::SeqCst);
        {
            let mut script = self.state.retry_failures.lock().unwrap();
            if let Some(failures) = script.as_mut()
                && matches!(failures.front(), Some(RetryFailure::Workspace))
            {
                self.state
                    .retry_observed
                    .lock()
                    .unwrap()
                    .push(std::time::Instant::now());
                return Err(failures.pop_front().unwrap().error());
            }
        }
        let fault = *self
            .state
            .preparation_fault
            .lock()
            .expect("preparation fault lock");
        match fault {
            PreparationFault::WorkspaceStall => {
                if let Some((faults, case)) = self.state.read_authority.lock().unwrap().take() {
                    assert!(matches!(case, AttemptReadCase::PreparationWake { .. }));
                    *faults.attempt_owner.lock().unwrap() = Some(tokio::task::id());
                }
                self.state.preparation_entered.notify_one();
                std::future::pending::<()>().await;
            }
            PreparationFault::WorkspacePanic => {
                self.state.preparation_entered.notify_one();
                panic!("fixture lower workspace lookup panicked");
            }
            PreparationFault::Pass
            | PreparationFault::Stall
            | PreparationFault::Panic
            | PreparationFault::RecoveryStall
            | PreparationFault::RecoveryPanic
            | PreparationFault::AbortStall
            | PreparationFault::AbortGate
            | PreparationFault::AbortPanic
            | PreparationFault::ExclusiveWorkspace { .. } => {}
        }
        self.workspace_info(id).await?;
        StreamFailureWorkspace::claim(&self.state)
    }

    fn check_workspace_update(
        &self,
        request: &UpdateWorkspaceRequest,
    ) -> impl std::future::Future<Output = Result<WorkspaceUpdateReadiness, SdkError>> + Send + '_
    {
        let id = request.id().clone();
        let expected_revision = request.expected_revision();
        async move {
            if self
                .state
                .workspace_check_armed
                .swap(false, Ordering::SeqCst)
            {
                self.state.workspace_check_observed.notify_one();
                self.state.workspace_check_release.notified().await;
            }
            let info = self.workspace_info(id).await?;
            if info.revision() != expected_revision {
                return Err(SdkError::conflict("test workspace revision is stale"));
            }
            Ok(WorkspaceUpdateReadiness::Ready)
        }
    }

    async fn update_workspace(
        &self,
        request: UpdateWorkspaceRequest,
    ) -> Result<WorkspaceInfo, SdkError> {
        if self
            .state
            .workspace_update_armed
            .swap(false, Ordering::SeqCst)
        {
            self.state.workspace_update_observed.notify_one();
            self.state.workspace_update_release.notified().await;
        }
        let info = WorkspaceInfo::new(
            request.id().clone(),
            request.name().to_owned(),
            request.context().clone(),
            request.defaults().clone(),
            request.expected_revision() + 1,
        )?;
        *self
            .state
            .workspace
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(info.clone());
        Ok(info)
    }

    async fn delete_workspace(&self, request: DeleteWorkspaceRequest) -> Result<(), SdkError> {
        self.state
            .workspace_delete_attempts
            .fetch_add(1, Ordering::SeqCst);
        self.state.workspace_delete_observed.notify_waiters();
        let blocked = {
            let mut deletion = self
                .state
                .workspace_deletion
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match &mut *deletion {
                StreamFailureDeletion::Retry => {
                    return Err(SdkError::not_yet_implemented(
                        "test agent does not delete workspaces",
                    ));
                }
                StreamFailureDeletion::BlockThenComplete => true,
                StreamFailureDeletion::Complete => false,
                StreamFailureDeletion::TransientThenComplete { remaining } if *remaining > 0 => {
                    *remaining -= 1;
                    return Err(SdkError::timeout("test lower workspace deletion timed out"));
                }
                StreamFailureDeletion::TransientThenComplete { .. } => false,
            }
        };
        if blocked {
            self.state.workspace_delete_release.notified().await;
        }
        let mut workspace = self
            .state
            .workspace
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(info) = workspace.as_ref() else {
            return Ok(());
        };
        if info.id() != request.id() {
            return Err(SdkError::not_found("test workspace id is unknown"));
        }
        if info.revision() != request.expected_revision() {
            return Err(SdkError::conflict("test workspace revision is stale"));
        }
        workspace.take();
        Ok(())
    }

    async fn list_models(&self) -> Result<Vec<ModelInfo>, SdkError> {
        Ok(Vec::new())
    }

    async fn ingest_knowledge(&self, _request: IngestKnowledgeRequest) -> Result<(), SdkError> {
        Ok(())
    }

    async fn shutdown(self) -> Result<(), SdkError> {
        self.state.shutdowns.fetch_add(1, Ordering::SeqCst);
        assert!(
            !self.state.shutdown_panics.load(Ordering::SeqCst),
            "injected lower shutdown panic"
        );
        if self.state.shutdown_armed.swap(false, Ordering::SeqCst) {
            self.state.shutdown_observed.notify_one();
            self.state.shutdown_release.notified().await;
        }
        Ok(())
    }
}

impl StreamFailureWorkspace {
    fn claim(state: &Arc<StreamFailureState>) -> Result<Self, SdkError> {
        let claimed = matches!(
            *state
                .preparation_fault
                .lock()
                .expect("preparation fault lock"),
            PreparationFault::ExclusiveWorkspace { .. }
        );
        if claimed && state.workspace_claimed.swap(true, Ordering::SeqCst) {
            return Err(SdkError::busy(
                "current durable workspace handle is already live",
            ));
        }
        Ok(Self {
            state: Arc::clone(state),
            claimed,
        })
    }
}

impl Drop for StreamFailureWorkspace {
    fn drop(&mut self) {
        if self.claimed {
            assert!(self.state.workspace_claimed.swap(false, Ordering::SeqCst));
        }
    }
}

impl WorkspaceBackend for StreamFailureWorkspace {
    type Conversation = StreamFailureConversation;

    fn id(&self) -> WorkspaceId {
        self.state.workspace_id.clone()
    }

    async fn info(&self) -> Result<WorkspaceInfo, SdkError> {
        self.state.workspace_info()
    }

    async fn new_conversation(
        &self,
        _request: NewConversation,
    ) -> Result<Self::Conversation, SdkError> {
        let fault = *self
            .state
            .preparation_fault
            .lock()
            .expect("preparation fault lock");
        match fault {
            PreparationFault::Stall => {
                self.state.preparation_entered.notify_one();
                std::future::pending::<()>().await;
            }
            PreparationFault::Panic => {
                self.state.preparation_entered.notify_one();
                panic!("fixture lower conversation construction panicked");
            }
            PreparationFault::ExclusiveWorkspace {
                gate_first_creation,
            } => {
                let first_creation = self
                    .state
                    .conversation_inventory
                    .lock()
                    .expect("test conversation inventory lock")
                    .is_empty();
                if gate_first_creation && first_creation {
                    self.state.preparation_entered.notify_one();
                    self.state.preparation_release.notified().await;
                }
            }
            PreparationFault::Pass
            | PreparationFault::WorkspaceStall
            | PreparationFault::WorkspacePanic
            | PreparationFault::RecoveryStall
            | PreparationFault::RecoveryPanic
            | PreparationFault::AbortStall
            | PreparationFault::AbortGate
            | PreparationFault::AbortPanic => {}
        }
        let mut inventory = self
            .state
            .conversation_inventory
            .lock()
            .expect("test conversation inventory lock");
        if !inventory.is_empty() && !matches!(fault, PreparationFault::ExclusiveWorkspace { .. }) {
            return Err(SdkError::conflict("test conversation already exists"));
        }
        let id = format!("conversation_{:032x}", 0x73 + inventory.len()).parse()?;
        let info = ConversationInfo::new(id, self.state.workspace_id.clone(), None)?;
        let id = info.id().clone();
        assert!(inventory.insert(id.clone(), info).is_none());
        self.state.conversations.fetch_add(1, Ordering::SeqCst);
        Ok(StreamFailureConversation {
            state: Arc::clone(&self.state),
            id,
        })
    }

    async fn list_conversations(&self) -> Result<Vec<ConversationInfo>, SdkError> {
        Ok(self
            .state
            .conversation_inventory
            .lock()
            .expect("test conversation inventory lock")
            .values()
            .cloned()
            .collect())
    }

    async fn conversation_info(&self, id: ConversationId) -> Result<ConversationInfo, SdkError> {
        self.state.conversation_info(&id)
    }

    async fn get_conversation(&self, id: ConversationId) -> Result<Self::Conversation, SdkError> {
        let fault = *self
            .state
            .preparation_fault
            .lock()
            .expect("preparation fault lock");
        match fault {
            PreparationFault::RecoveryStall => {
                self.state.preparation_entered.notify_one();
                std::future::pending::<()>().await;
            }
            PreparationFault::RecoveryPanic => {
                self.state.preparation_entered.notify_one();
                panic!("fixture lower conversation recovery panicked");
            }
            PreparationFault::Pass
            | PreparationFault::Stall
            | PreparationFault::Panic
            | PreparationFault::WorkspaceStall
            | PreparationFault::WorkspacePanic
            | PreparationFault::AbortStall
            | PreparationFault::AbortGate
            | PreparationFault::AbortPanic
            | PreparationFault::ExclusiveWorkspace { .. } => {}
        }
        self.conversation_info(id.clone()).await?;
        self.state.reopens.fetch_add(1, Ordering::SeqCst);
        Ok(StreamFailureConversation {
            state: Arc::clone(&self.state),
            id,
        })
    }

    async fn list_tools(&self) -> Result<Vec<ToolInfo>, SdkError> {
        Ok(Vec::new())
    }

    async fn ingest_knowledge(&self, _request: IngestKnowledgeRequest) -> Result<(), SdkError> {
        Ok(())
    }

    async fn close(self) -> Result<(), SdkError> {
        Ok(())
    }
}

impl LifecycleHooks for StreamFailureAgent {
    fn observe(&self, event: &LifecycleEvent<'_>) -> Result<(), AppError> {
        if let LifecycleEvent::StepStarted { context, .. } = event {
            self.state
                .step_contexts
                .lock()
                .unwrap()
                .push((*context).clone());
        }
        Ok(())
    }
}

impl ConversationBackend for StreamFailureConversation {
    fn id(&self) -> ConversationId {
        self.id.clone()
    }

    async fn info(&self) -> Result<ConversationInfo, SdkError> {
        self.state.conversation_info(&self.id)
    }

    async fn prompt(&self, request: PromptRequest) -> Result<PromptResponse, SdkError> {
        self.state.prompts.fetch_add(1, Ordering::SeqCst);
        if let Some(effects) = self.state.side_effects.lock().unwrap().as_mut() {
            *effects += 1;
        }
        {
            let mut script = self.state.retry_failures.lock().unwrap();
            if let Some(failures) = script.as_mut() {
                self.state
                    .retry_observed
                    .lock()
                    .unwrap()
                    .push(std::time::Instant::now());
                if let Some(failure) = failures.pop_front() {
                    assert!(!matches!(failure, RetryFailure::Workspace));
                    return Err(failure.error());
                }
            }
        }
        if let Some((faults, case)) = self.state.read_authority.lock().unwrap().take() {
            assert!(matches!(case, AttemptReadCase::PromptFailure { .. }));
            *faults.attempt_owner.lock().unwrap() = Some(tokio::task::id());
            *faults.attempt_read.lock().unwrap() = Some(case);
            return Err(SdkError::integration(
                "original lower prompt returned uncertain failure",
            ));
        }
        if matches!(
            *self
                .state
                .preparation_fault
                .lock()
                .expect("preparation fault lock"),
            PreparationFault::ExclusiveWorkspace { .. }
        ) {
            return Ok(PromptResponse::new(request.text().to_owned(), None));
        }
        let notifications = self
            .state
            .notifications
            .lock()
            .expect("test notifications lock")
            .clone();
        match notifications {
            Some(notifications) => notifications.prompt().await,
            None => Ok(PromptResponse::new("unexpected prompt".to_owned(), None)),
        }
    }

    async fn prompt_stream(&self, _request: PromptRequest) -> Result<PromptStream, SdkError> {
        let notifications = self.state.notifications.lock().unwrap().clone();
        if let Some(notifications) = notifications {
            self.state.prompts.fetch_add(1, Ordering::SeqCst);
            return Ok(PromptStream::from_stream(futures_util::stream::once(
                async move { notifications.prompt().await.map(PromptEvent::Completed) },
            )));
        }
        let failure = self
            .state
            .retry_failures
            .lock()
            .unwrap()
            .as_mut()
            .and_then(VecDeque::pop_front);
        let Some(failure) = failure else {
            return Err(SdkError::unsupported(
                "incremental prompt output is unavailable",
            ));
        };
        assert!(!matches!(failure, RetryFailure::Workspace));
        self.state.prompts.fetch_add(1, Ordering::SeqCst);
        if let Some(effects) = self.state.side_effects.lock().unwrap().as_mut() {
            *effects += 1;
        }
        self.state
            .retry_observed
            .lock()
            .unwrap()
            .push(std::time::Instant::now());
        if matches!(failure, RetryFailure::StreamOpeningAfterEffect(_)) {
            return Err(failure.error());
        }
        Ok(PromptStream::from_stream(futures_util::stream::iter([
            Ok(PromptEvent::OutputDelta {
                text: "accepted partial".to_owned(),
            }),
            Err(failure.error()),
        ])))
    }

    async fn abort(&self) -> Result<(), SdkError> {
        self.state.aborts.fetch_add(1, Ordering::SeqCst);
        let fault = *self
            .state
            .preparation_fault
            .lock()
            .expect("preparation fault lock");
        if matches!(fault, PreparationFault::AbortGate) {
            self.state.preparation_entered.notify_one();
            self.state.preparation_release.notified().await;
        }
        if let Some(notifications) = self
            .state
            .notifications
            .lock()
            .expect("test notifications lock")
            .as_ref()
        {
            notifications.cancelled.store(true, Ordering::SeqCst);
            notifications.changed.notify_one();
        }
        let fault = *self
            .state
            .preparation_fault
            .lock()
            .expect("preparation fault lock");
        match fault {
            PreparationFault::AbortStall => {
                self.state.preparation_entered.notify_one();
                std::future::pending::<()>().await;
            }
            PreparationFault::AbortPanic => {
                self.state.preparation_entered.notify_one();
                panic!("fixture lower abort panicked");
            }
            PreparationFault::Pass
            | PreparationFault::Stall
            | PreparationFault::Panic
            | PreparationFault::WorkspaceStall
            | PreparationFault::WorkspacePanic
            | PreparationFault::RecoveryStall
            | PreparationFault::RecoveryPanic
            | PreparationFault::AbortGate
            | PreparationFault::ExclusiveWorkspace { .. } => {}
        }
        self.state.abort_returns.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn read_file(&self, _path: PathBuf) -> Result<FileRead, SdkError> {
        Err(SdkError::not_found("test file does not exist"))
    }

    fn notification_stream(&self) -> Result<ConversationNotificationReceiver, SdkError> {
        self.state.streams.fetch_add(1, Ordering::SeqCst);
        if matches!(
            *self
                .state
                .preparation_fault
                .lock()
                .expect("preparation fault lock"),
            PreparationFault::ExclusiveWorkspace { .. }
        ) {
            return Ok(ConversationNotificationReceiver::from_stream(
                futures_util::stream::pending(),
            ));
        }
        if let Some(notifications) = self
            .state
            .notifications
            .lock()
            .expect("test notifications lock")
            .as_ref()
        {
            let read_authority = {
                let mut pending = self.state.read_authority.lock().unwrap();
                if pending.as_ref().is_some_and(|(_, case)| {
                    matches!(
                        case,
                        AttemptReadCase::NotificationDelivery { .. }
                            | AttemptReadCase::ResolutionObservation { .. }
                    )
                }) {
                    pending.take()
                } else {
                    None
                }
            };
            return notifications.receiver(read_authority);
        }
        Err(SdkError::busy(
            "test notification stream acquisition failed",
        ))
    }

    async fn resolve_notification(
        &self,
        id: NotificationId,
        resolution: NotificationResolution,
    ) -> Result<(), SdkError> {
        let notifications = self
            .state
            .notifications
            .lock()
            .expect("test notifications lock")
            .clone();
        if let Some(notifications) = notifications {
            return notifications.resolve(id, resolution).await;
        }
        Err(SdkError::unsupported(
            "test notification resolution is unavailable",
        ))
    }

    async fn close(self) -> Result<(), SdkError> {
        Ok(())
    }
}

struct ExecutionJourney {
    app: App<StateStore, Agent>,
    workspace: Workspace<StateStore, Agent>,
}

impl ExecutionJourney {
    async fn open(definitions: &[&str]) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let workflows = definitions
            .iter()
            .map(|source| WorkflowDefinition::parse(source))
            .collect::<Result<Vec<_>, _>>()?;
        let config: AppConfig<StateStore, Agent> = AppConfig::new(
            StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
                .await
                .expect("caller memory store"),
            Vec::new(),
            workflows,
        );
        let app = App::open(config).await?;
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: Some("execution-journey".to_owned()),
                root: std::env::current_dir()?,
            })
            .await?;
        Ok(Self { app, workspace })
    }

    async fn run(
        &self,
        workflow: &str,
        input: Map<String, Value>,
    ) -> Result<RunSnapshot, AppError> {
        let workflow = self.workspace.workflow(workflow.parse()?).await?;
        let run = workflow
            .start(ExecutionOptions {
                state: WorkflowState::new(input)?,
                ..ExecutionOptions::default()
            })
            .await?;
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut snapshot = run.wait().await?;
            while snapshot.interactions.iter().any(|interaction| {
                matches!(
                    interaction,
                    Interaction::Input { .. }
                        | Interaction::Notification { .. }
                        | Interaction::Inputs { .. }
                )
            }) {
                snapshot = run.wait().await?;
            }
            Ok(snapshot)
        })
        .await
        .map_err(|_| AppError::Timeout("workflow observation timed out".to_owned()))?
    }

    async fn wait<CS: StateStoreBackend, A: AgentBackend>(
        run: &app_sdk::workflow::WorkflowRun<CS, A>,
    ) -> Result<RunSnapshot, AppError> {
        let observed = tokio::time::timeout(Duration::from_secs(10), run.wait()).await;
        observed.map_err(|_| AppError::Timeout("workflow observation timed out".to_owned()))?
    }

    async fn close(mut self) -> Result<(), AppError> {
        self.app
            .shutdown(Shutdown::Drain {
                timeout: Duration::from_secs(10),
            })
            .await
    }
}

const LINEAR: &str = r#"{
  "schema": "app-sdk.workflow/v2", "id": "linear", "revision": "v1", "entry": "choose",
  "state": {"case": "linear", "count": 0, "threshold": 0, "path": "unset", "value": 0},
  "required": ["case", "count", "threshold", "path", "value"],
  "reducers": {"case": "reject_concurrent", "count": "sum_integer",
    "threshold": "reject_concurrent", "path": "reject_concurrent", "value": "reject_concurrent"},
  "nodes": [
    {"id": "choose", "step": {"kind": "state", "mutations": []}},
    {"id": "fork", "step": {"kind": "state", "mutations": []}},
    {"id": "increment", "step": {"kind": "state", "mutations": [
      {"kind": "assign", "target": "count", "value": {"kind": "literal", "value": 1}}]}},
    {"id": "selected", "step": {"kind": "state", "mutations": [
      {"kind": "assign", "target": "path", "value": {"kind": "literal", "value": "selected"}}]}},
    {"id": "short", "deadline_ms": 1000,
      "step": {"kind": "sleep", "message": "short", "delay_ms": 1}},
    {"id": "expired", "deadline_ms": 10,
      "step": {"kind": "sleep", "message": "expired", "delay_ms": 60000}},
    {"id": "input-expired", "deadline_ms": 10,
      "step": {"kind": "input", "message": [{"kind": "text", "text": "expire"}]}},
    {"id": "slept", "step": {"kind": "state", "mutations": [
      {"kind": "assign", "target": "path", "value": {"kind": "literal", "value": "slept"}}]}},
    {"id": "a", "step": {"kind": "state", "mutations": [
      {"kind": "assign", "target": "value", "value": {"kind": "literal", "value": 1}}]}},
    {"id": "b", "step": {"kind": "state", "mutations": [
      {"kind": "assign", "target": "value", "value": {"kind": "literal", "value": 2}}]}},
    {"id": "wait", "step": {"kind": "input", "message": [{"kind": "text", "text": "wait"}]}}
  ],
  "routes": {
    "choose": {"mode": "first", "branches": [
      {"when": {"kind": "equal", "left": {"kind": "state", "path": "case"},
        "right": {"kind": "literal", "value": "order"}}, "target": {"kind": "node", "id": "selected"}},
      {"when": {"kind": "equal", "left": {"kind": "state", "path": "case"},
        "right": {"kind": "literal", "value": "order"}}, "target": {"kind": "node", "id": "a"}},
      {"when": {"kind": "equal", "left": {"kind": "state", "path": "case"},
        "right": {"kind": "literal", "value": "linear"}}, "target": {"kind": "node", "id": "increment"}},
      {"when": {"kind": "equal", "left": {"kind": "state", "path": "case"},
        "right": {"kind": "literal", "value": "sleep"}}, "target": {"kind": "node", "id": "short"}},
      {"when": {"kind": "equal", "left": {"kind": "state", "path": "case"},
        "right": {"kind": "literal", "value": "input"}}, "target": {"kind": "node", "id": "input-expired"}},
      {"target": {"kind": "node", "id": "fork"}}]},
    "increment": {"mode": "first", "branches": [
      {"when": {"kind": "greater_than", "left": {"kind": "state", "path": "count"},
        "right": {"kind": "state", "path": "threshold"}}, "target": {"kind": "node", "id": "selected"}},
      {"target": {"kind": "end"}}]},
    "selected": {"mode": "first", "branches": [{"target": {"kind": "end"}}]},
    "short": {"mode": "first", "branches": [{"target": {"kind": "node", "id": "slept"}}]},
    "slept": {"mode": "first", "branches": [{"target": {"kind": "node", "id": "expired"}}]},
    "expired": {"mode": "first", "branches": [{"target": {"kind": "end"}}]},
    "input-expired": {"mode": "first", "branches": [{"target": {"kind": "end"}}]},
    "fork": {"mode": "broadcast", "branches": [
      {"target": {"kind": "node", "id": "a"}}, {"target": {"kind": "node", "id": "b"}},
      {"target": {"kind": "node", "id": "wait"}}]},
    "a": {"mode": "first", "branches": [{"target": {"kind": "end"}}]},
    "b": {"mode": "first", "branches": [{"target": {"kind": "end"}}]},
    "wait": {"mode": "first", "branches": [{"target": {"kind": "end"}}]}
  }
}"#;

const STREAM_FAILURE: &str = r#"{
  "schema": "app-sdk.workflow/v2", "id": "stream-failure", "revision": "v1",
  "entry": "agent", "state": {"answer": ""}, "required": ["answer"],
  "reducers": {"answer": "reject_concurrent"},
  "nodes": [{"id": "agent", "deadline_ms": 5000, "step": {
    "kind": "agent", "agent": "faulty",
    "prompt": [{"kind": "text", "text": "respond"}], "text_target": "answer"
  }}],
  "routes": {"agent": {"mode": "first", "branches": [{"target": {"kind": "end"}}]}}
}"#;

const DROP_WAIT: &str = r#"{
  "schema": "app-sdk.workflow/v2", "id": "drop-wait", "revision": "v1", "entry": "sleep",
  "state": {}, "required": [], "reducers": {},
  "nodes": [{"id": "sleep", "deadline_ms": 60000,
    "step": {"kind": "sleep", "message": "hold", "delay_ms": 60000}}],
  "routes": {"sleep": {"mode": "first", "branches": [{"target": {"kind": "end"}}]}}
}"#;

const CHILD: &str = r#"{
  "schema": "app-sdk.workflow/v2", "id": "map-child", "revision": "v1", "entry": "copy",
  "state": {"item": 0, "result": 0}, "required": ["item", "result"],
  "reducers": {"item": "reject_concurrent", "result": "reject_concurrent"},
  "nodes": [{"id": "copy", "step": {"kind": "state", "mutations": []}}],
  "routes": {"copy": {"mode": "first", "branches": [{"target": {"kind": "end"}}]}}
}"#;

const PARALLEL_PARENT: &str = r#"{
  "schema": "app-sdk.workflow/v2", "id": "parallel-parent", "revision": "v1", "entry": "map",
  "state": {"items": [1, 2, 3, 4], "payload": 0, "total": 0, "collected": []},
  "required": ["items", "payload", "total", "collected"],
  "reducers": {"items": "reject_concurrent", "payload": "reject_concurrent",
    "total": "sum_integer", "collected": "collect"},
  "nodes": [
    {"id": "map", "step": {"kind": "map", "workflow": "map-child",
      "items": {"kind": "state", "path": "items"}, "input_key": "item",
      "input": {"result": {"kind": "state", "path": "payload"}}, "max_parallel": 2,
      "reductions": [
        {"source": "item", "target": "total", "reducer": "sum_integer"},
        {"source": "item", "target": "collected", "reducer": "collect"}]}},
    {"id": "left", "step": {"kind": "state", "mutations": []}},
    {"id": "right", "step": {"kind": "state", "mutations": []}},
    {"id": "join", "step": {"kind": "join_all"}},
    {"id": "child", "step": {"kind": "child", "workflow": "map-child", "input": {
      "item": {"kind": "literal", "value": 7}},
      "reductions": [{"source": "item", "target": "total", "reducer": "sum_integer"}]}}
  ],
  "routes": {
    "map": {"mode": "broadcast", "branches": [
      {"target": {"kind": "node", "id": "left"}}, {"target": {"kind": "node", "id": "right"}}]},
    "left": {"mode": "first", "branches": [{"target": {"kind": "node", "id": "join"}}]},
    "right": {"mode": "first", "branches": [{"target": {"kind": "node", "id": "join"}}]},
    "join": {"mode": "first", "branches": [{"target": {"kind": "node", "id": "child"}}]},
    "child": {"mode": "first", "branches": [{"target": {"kind": "end"}}]}
  }
}"#;

const CONDITIONAL_CYCLIC_JOINS: &str = r#"{
  "schema": "app-sdk.workflow/v2", "id": "conditional-cyclic-joins", "revision": "v1", "entry": "select",
  "state": {"loser_visits": 0, "visits": 0}, "required": ["loser_visits", "visits"],
  "reducers": {"loser_visits": "sum_integer", "visits": "sum_integer"},
  "nodes": [
    {"id": "select", "step": {"kind": "state", "mutations": []}},
    {"id": "selected", "step": {"kind": "state", "mutations": []}},
    {"id": "omitted", "step": {"kind": "state", "mutations": []}},
    {"id": "conditional", "step": {"kind": "join_all"}},
    {"id": "short", "step": {"kind": "state", "mutations": []}},
    {"id": "long-one", "step": {"kind": "state", "mutations": []}},
    {"id": "long-two", "step": {"kind": "state", "mutations": [
      {"kind": "assign", "target": "loser_visits", "value": {"kind": "literal", "value": 1}}]}},
    {"id": "race", "step": {"kind": "join_first"}},
    {"id": "visit", "step": {"kind": "state", "mutations": [
      {"kind": "assign", "target": "visits", "value": {"kind": "literal", "value": 1}}]}}
  ],
  "routes": {
    "select": {"mode": "first", "branches": [
      {"when": {"kind": "constant", "value": true}, "target": {"kind": "node", "id": "selected"}},
      {"target": {"kind": "node", "id": "omitted"}}]},
    "selected": {"mode": "first", "branches": [{"target": {"kind": "node", "id": "conditional"}}]},
    "omitted": {"mode": "first", "branches": [{"target": {"kind": "node", "id": "conditional"}}]},
    "conditional": {"mode": "broadcast", "branches": [
      {"target": {"kind": "node", "id": "short"}}, {"target": {"kind": "node", "id": "long-one"}}]},
    "short": {"mode": "first", "branches": [{"target": {"kind": "node", "id": "race"}}]},
    "long-one": {"mode": "first", "branches": [{"target": {"kind": "node", "id": "long-two"}}]},
    "long-two": {"mode": "first", "branches": [{"target": {"kind": "node", "id": "race"}}]},
    "race": {"mode": "first", "branches": [{"target": {"kind": "node", "id": "visit"}}]},
    "visit": {"mode": "first", "branches": [
      {"when": {"kind": "less_than", "left": {"kind": "state", "path": "visits"},
        "right": {"kind": "literal", "value": 2}}, "target": {"kind": "node", "id": "race"}},
      {"target": {"kind": "end"}}]}
  }
}"#;

#[tokio::test]
async fn burr_characterization_first_action_input_ordered_transition_and_checkpoint_snapshot()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let started = tokio::time::Instant::now();
    eprintln!("linear characterization phase=open");
    // These cases test admitted control expiry, after real authority reads.
    let mut definition: Value = serde_json::from_str(LINEAR)?;
    for node in definition["nodes"].as_array_mut().expect("LINEAR nodes") {
        if matches!(node["id"].as_str(), Some("expired" | "input-expired")) {
            node["deadline_ms"] = Value::from(1_000);
        }
    }
    let definition = definition.to_string();
    let journey = ExecutionJourney::open(&[&definition]).await?;
    for count in [0_u64, 9_007_199_254_740_992] {
        let input = Map::from_iter([
            ("count".to_owned(), Value::from(count)),
            ("threshold".to_owned(), Value::from(count)),
        ]);
        eprintln!(
            "linear characterization phase=count:{count} elapsed_ms={}",
            started.elapsed().as_millis()
        );
        let snapshot = journey.run("linear", input).await?;
        assert!(matches!(snapshot.status, RunStatus::Succeeded));
        assert_eq!(
            snapshot.state.values().get("count"),
            Some(&Value::from(count + 1))
        );
        assert_eq!(
            snapshot.state.values().get("path"),
            Some(&Value::from("selected"))
        );
    }
    eprintln!(
        "linear characterization phase=ordered elapsed_ms={}",
        started.elapsed().as_millis()
    );
    let ordered = journey
        .run(
            "linear",
            Map::from_iter([("case".to_owned(), Value::from("order"))]),
        )
        .await?;
    assert!(matches!(ordered.status, RunStatus::Succeeded));
    assert!(ordered.checkpoint > 0);
    assert_eq!(
        ordered.state.values().get("path"),
        Some(&Value::from("selected"))
    );
    assert_eq!(ordered.state.values().get("value"), Some(&Value::from(0)));
    for case in ["sleep", "input"] {
        eprintln!(
            "linear characterization phase=deadline:{case} elapsed_ms={}",
            started.elapsed().as_millis()
        );
        let observed = tokio::time::timeout(
            Duration::from_secs(5),
            journey.run(
                "linear",
                Map::from_iter([("case".to_owned(), Value::from(case))]),
            ),
        )
        .await;
        let snapshot = match observed {
            Ok(Ok(snapshot)) => snapshot,
            Ok(Err(error)) => {
                eprintln!("linear deadline primary: case={case} error={error:?}");
                let cleanup = AssertUnwindSafe(tokio::time::timeout(
                    Duration::from_secs(15),
                    journey.close(),
                ))
                .catch_unwind()
                .await
                .map_err(|_| "shutdown panicked");
                eprintln!("linear deadline retained shutdown: case={case} result={cleanup:?}");
                return Err(error.into());
            }
            Err(_) => {
                let cleanup = AssertUnwindSafe(tokio::time::timeout(
                    Duration::from_secs(15),
                    journey.close(),
                ))
                .catch_unwind()
                .await
                .map_err(|_| "shutdown panicked");
                eprintln!(
                    "linear deadline observer expired: case={case} retained_shutdown={cleanup:?}"
                );
                panic!("{case} must honor its node deadline");
            }
        };
        assert!(matches!(
            snapshot.status,
            RunStatus::Failed(WorkflowFailure::Timeout(_))
        ));
        if case == "sleep" {
            assert_eq!(
                snapshot.state.values().get("path"),
                Some(&Value::from("slept"))
            );
        }
    }
    eprintln!(
        "linear characterization phase=workflow elapsed_ms={}",
        started.elapsed().as_millis()
    );
    let linear = journey.workspace.workflow("linear".parse()?).await?;
    let conflicting = Map::from_iter([("case".to_owned(), Value::from("cancellation"))]);
    eprintln!(
        "linear characterization phase=start-ordinary elapsed_ms={}",
        started.elapsed().as_millis()
    );
    let ordinary = linear
        .start(ExecutionOptions {
            state: WorkflowState::new(conflicting.clone())?,
            ..ExecutionOptions::default()
        })
        .await?;
    eprintln!(
        "linear characterization phase=wait-ordinary elapsed_ms={}",
        started.elapsed().as_millis()
    );
    let ordinary_waiting = ExecutionJourney::wait(&ordinary).await?;
    let [
        Interaction::Input {
            sequence,
            generation,
            ..
        },
    ] = ordinary_waiting.interactions.as_slice()
    else {
        panic!("the conflicting broadcast must wait for input")
    };
    eprintln!(
        "linear characterization phase=resolve-ordinary elapsed_ms={}",
        started.elapsed().as_millis()
    );
    ordinary
        .resolve(*sequence, *generation, Resolution::Input(Value::Null))
        .await?;
    eprintln!(
        "linear characterization phase=wait-ordinary-terminal elapsed_ms={}",
        started.elapsed().as_millis()
    );
    let ordinary_terminal = ExecutionJourney::wait(&ordinary).await?;
    eprintln!(
        "linear characterization phase=start-cancellation elapsed_ms={}",
        started.elapsed().as_millis()
    );
    let cancellation = linear
        .start(ExecutionOptions {
            state: WorkflowState::new(conflicting)?,
            ..ExecutionOptions::default()
        })
        .await?;
    eprintln!(
        "linear characterization phase=wait-cancellation elapsed_ms={}",
        started.elapsed().as_millis()
    );
    let cancellation_waiting = ExecutionJourney::wait(&cancellation).await?;
    let cancellation_checkpoint = cancellation_waiting.checkpoint;
    eprintln!(
        "linear characterization phase=cancel elapsed_ms={}",
        started.elapsed().as_millis()
    );
    cancellation.cancel(cancellation_checkpoint).await?;
    eprintln!(
        "linear characterization phase=wait-cancelled elapsed_ms={}",
        started.elapsed().as_millis()
    );
    let cancelled = ExecutionJourney::wait(&cancellation).await?;
    eprintln!(
        "linear characterization phase=repeat-cancel elapsed_ms={}",
        started.elapsed().as_millis()
    );
    let repeated = cancellation.cancel(cancellation_checkpoint).await?;
    assert_eq!(repeated.checkpoint, cancelled.checkpoint);
    eprintln!(
        "linear characterization phase=reject-stale-cancel elapsed_ms={}",
        started.elapsed().as_millis()
    );
    assert!(matches!(
        cancellation.cancel(cancelled.checkpoint).await,
        Err(AppError::Conflict(_))
    ));
    assert!(matches!(
        ordinary_terminal.status,
        RunStatus::Failed(WorkflowFailure::State(_))
    ));
    for snapshot in [&cancelled, &repeated] {
        assert!(matches!(snapshot.status, RunStatus::Cancelled));
        assert_eq!(snapshot.state, cancellation_waiting.state);
        assert_eq!(
            snapshot.last_action_ordinal,
            cancellation_waiting.last_action_ordinal
        );
        assert!(snapshot.outcomes == cancellation_waiting.outcomes);
    }
    eprintln!(
        "linear characterization phase=close elapsed_ms={}",
        started.elapsed().as_millis()
    );
    journey.close().await?;
    Ok(())
}

#[tokio::test]
async fn bounded_map_payloads_and_parallel_join_publish_one_truth()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let journey = ExecutionJourney::open(&[CHILD, PARALLEL_PARENT]).await?;
    let first = journey.run("parallel-parent", Map::new()).await?;
    let second = journey.run("parallel-parent", Map::new()).await?;
    for snapshot in [&first, &second] {
        assert!(matches!(snapshot.status, RunStatus::Succeeded));
        assert_eq!(
            Value::Object(snapshot.state.values().clone()),
            serde_json::json!({
                "items": [1, 2, 3, 4], "payload": 0, "total": 17,
                "collected": [1, 2, 3, 4]
            })
        );
    }
    assert_eq!(first.state, second.state);
    let child = journey.workspace.workflow("map-child".parse()?).await?;
    let limit = NonZeroUsize::new(1_000).expect("run list limit is nonzero");
    let before = child.list_runs(None, limit).await?.len();
    let items = Value::Array(vec![Value::from(0); 4_096]);
    let oversized = Map::from_iter([
        ("items".to_owned(), items),
        ("payload".to_owned(), Value::String("x".repeat(2_048))),
    ]);
    let oversized_snapshot = tokio::time::timeout(
        Duration::from_secs(1),
        journey.run("parallel-parent", oversized),
    )
    .await
    .expect("oversized map inputs must settle")?;
    assert!(matches!(
        oversized_snapshot.status,
        RunStatus::Failed(WorkflowFailure::Limit(_))
    ));
    assert_eq!(before, child.list_runs(None, limit).await?.len());
    journey.close().await?;
    Ok(())
}

#[tokio::test]
async fn conditional_join_all_and_cyclic_join_first_preserve_structural_truth()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let journey = ExecutionJourney::open(&[CONDITIONAL_CYCLIC_JOINS]).await?;
    let first = journey.run("conditional-cyclic-joins", Map::new()).await?;
    let second = journey.run("conditional-cyclic-joins", Map::new()).await?;
    for snapshot in [&first, &second] {
        assert!(matches!(snapshot.status, RunStatus::Succeeded));
        assert_eq!(
            snapshot.state.values().get("loser_visits"),
            Some(&Value::from(0))
        );
        assert_eq!(snapshot.state.values().get("visits"), Some(&Value::from(2)));
    }
    assert_eq!(first.state, second.state);
    journey.close().await?;
    Ok(())
}

#[test]
fn parser_rejects_unknown_fields_and_unbound_structured_targets() {
    let agent = LINEAR.replacen(
        r#""step": {"kind": "state", "mutations": []}"#,
        r#""step": {"kind": "agent", "agent": "primary", "prompt": [{"kind": "text", "text": "respond"}], "text_target": "value"}"#,
        1,
    );
    let invalid = [
        LINEAR.replace("app-sdk.workflow/v2", "app-sdk.workflow/v1"),
        LINEAR.replacen(r#""revision": "v1""#, r#""revision": "v1", "legacy": 1"#, 1),
        agent.replace(
            r#""text_target": "value""#,
            r#""structured_target": "value""#,
        ),
    ];
    assert!(
        invalid
            .iter()
            .all(|source| WorkflowDefinition::parse(source).is_err())
    );
    for (document, pointer) in [
        (agent.as_str(), "/nodes/0/step/prompt/0"),
        (LINEAR, "/nodes/2/step/mutations/0/value"),
        (LINEAR, "/routes/choose/branches/0/when/left"),
        (PARALLEL_PARENT, "/nodes/0/step/items"),
        (PARALLEL_PARENT, "/nodes/0/step/input/result"),
        (PARALLEL_PARENT, "/nodes/4/step/input/item"),
    ] {
        let mut document: Value = serde_json::from_str(document).expect("fixture must be JSON");
        *document
            .pointer_mut(pointer)
            .expect("read position must exist") =
            serde_json::json!({"kind": "state", "path": "undeclared"});
        WorkflowDefinition::parse(&document.to_string())
            .expect("state reads resolve at activation and may follow earlier key creation");
    }
    let mut nested: Value = serde_json::from_str(LINEAR).expect("fixture must be JSON");
    nested["routes"]["choose"]["branches"][0]["when"]["left"] =
        serde_json::json!({"kind": "state", "path": "case/nested"});
    WorkflowDefinition::parse(&nested.to_string()).expect("declared nested read must parse");
}

#[test]
fn parser_rejects_ambiguous_map_bindings_and_duplicate_defaults() {
    let mut generated_key_collision: Value =
        serde_json::from_str(PARALLEL_PARENT).expect("fixture must be JSON");
    generated_key_collision["nodes"][0]["step"]["index_key"] = Value::from("item");

    let mut item_input_collision: Value =
        serde_json::from_str(PARALLEL_PARENT).expect("fixture must be JSON");
    item_input_collision["nodes"][0]["step"]["input"]["item"] =
        serde_json::json!({"kind": "literal", "value": 0});

    let mut index_input_collision: Value =
        serde_json::from_str(PARALLEL_PARENT).expect("fixture must be JSON");
    index_input_collision["nodes"][0]["step"]["index_key"] = Value::from("position");
    index_input_collision["nodes"][0]["step"]["input"]["position"] =
        serde_json::json!({"kind": "literal", "value": 0});

    let mut duplicate_fallback: Value = serde_json::from_str(LINEAR).expect("fixture must be JSON");
    let branches = duplicate_fallback["routes"]["choose"]["branches"]
        .as_array_mut()
        .expect("branches must be an array");
    let fallback = branches.last().expect("fallback must exist").clone();
    branches.push(fallback);

    for invalid in [
        generated_key_collision,
        item_input_collision,
        index_input_collision,
        duplicate_fallback,
    ] {
        assert!(WorkflowDefinition::parse(&invalid.to_string()).is_err());
    }
}

#[tokio::test]
async fn app_open_rejects_incomplete_child_and_map_relationships_before_agent_use()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let parent = || {
        let mut parent =
            serde_json::from_str::<Value>(PARALLEL_PARENT).expect("fixture must be JSON");
        parent["nodes"][1]["step"] = serde_json::json!({
            "kind": "agent",
            "agent": "unconfigured",
            "prompt": [{"kind": "text", "text": "must not run"}],
            "text_target": "payload"
        });
        parent
    };
    let mut cases = Vec::new();

    let mut child_input = parent();
    child_input["nodes"][4]["step"]["input"]["undeclared"] =
        serde_json::json!({"kind": "literal", "value": 0});
    cases.push(("child input", child_input));

    let mut child_source = parent();
    child_source["nodes"][4]["step"]["reductions"][0]["source"] = Value::from("undeclared");
    cases.push(("child reduction source", child_source));

    let mut child_reducer = parent();
    child_reducer["nodes"][4]["step"]["reductions"][0]["reducer"] = Value::from("collect");
    cases.push(("child reduction reducer", child_reducer));

    let mut map_item = parent();
    map_item["nodes"][0]["step"]["input_key"] = Value::from("undeclared");
    cases.push(("map item key", map_item));

    let mut map_index = parent();
    map_index["nodes"][0]["step"]["index_key"] = Value::from("undeclared");
    cases.push(("map index key", map_index));

    let mut map_input = parent();
    map_input["nodes"][0]["step"]["input"]["undeclared"] =
        serde_json::json!({"kind": "literal", "value": 0});
    cases.push(("map input", map_input));

    let mut map_source = parent();
    map_source["nodes"][0]["step"]["reductions"][0]["source"] = Value::from("undeclared");
    cases.push(("map reduction source", map_source));

    let mut map_reducer = parent();
    map_reducer["nodes"][0]["step"]["reductions"][0]["reducer"] = Value::from("collect");
    cases.push(("map reduction reducer", map_reducer));

    for (case, parent) in cases {
        let workflows = vec![
            WorkflowDefinition::parse(CHILD)?,
            WorkflowDefinition::parse(&parent.to_string())?,
        ];
        let config: AppConfig<StateStore, Agent> = AppConfig::new(
            StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
                .await
                .expect("caller memory store"),
            Vec::new(),
            workflows,
        );
        match App::open(config).await {
            Err(AppError::InvalidInput(message)) => {
                assert_ne!(
                    message, "workflow references an unconfigured agent",
                    "{case} must fail before agent inventory validation"
                );
            }
            Err(error) => panic!("{case} returned the wrong error: {error}"),
            Ok(mut app) => {
                app.shutdown(Shutdown::Drain {
                    timeout: Duration::from_secs(10),
                })
                .await?;
                panic!("{case} was accepted")
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn retired_receipt_cleanup_is_bounded_amortized_and_failure_isolated() {
    for reject_cleanup in [false, true] {
        let (faults, store) = StorageFaults::open().await.unwrap();
        let native = store.inner.clone();
        let mut app: App<_, Agent> = App::open(AppConfig::new(store, Vec::new(), Vec::new()))
            .await
            .unwrap();
        let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(30), async {
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().unwrap(),
                })
                .await
                .unwrap();
            let mut revision = workspace.snapshot().await.unwrap().revision;
            faults
                .reject_receipt_cleanup
                .store(reject_cleanup, Ordering::SeqCst);
            for index in 0..300 {
                let next = tokio::time::timeout(
                    Duration::from_secs(5),
                    workspace.replace(
                        revision,
                        WorkspaceConfig {
                            name: Some(format!("receipt-batch-{index}")),
                            root: std::env::current_dir().unwrap(),
                        },
                    ),
                )
                .await
                .expect("cleanup never exceeds the existing mutation bound")
                .unwrap();
                assert!(next.revision > revision);
                assert_eq!(
                    next.config.name.as_deref(),
                    Some(format!("receipt-batch-{index}").as_str())
                );
                revision = next.revision;
            }
            assert!(
                !faults.reject_receipt_cleanup.load(Ordering::SeqCst),
                "the injected cleanup failure was exercised"
            );
            let writes = faults.receipt_writes.load(Ordering::SeqCst);
            let batches = faults.receipt_cleanup_batches.lock().unwrap().clone();
            assert!(
                writes >= 600,
                "public replacements exercise multiple cleanup pages"
            );
            assert!(!batches.is_empty(), "cleanup actually committed");
            assert!(
                batches.iter().all(|size| *size == 256),
                "retired receipts share one bounded native cleanup transaction: {batches:?}"
            );
            assert_eq!(batches.len(), writes / 256 - usize::from(reject_cleanup));
            let request = StateListRequest::new("app-sdk")
                .unwrap()
                .with_key_prefix(StateKeyPrefix::new("receipt/").unwrap())
                .with_limit(1000)
                .unwrap();
            let pending = native.list(request).await.unwrap();
            assert!(pending.next_start_after.is_none());
            assert_eq!(
                pending.records.len(),
                writes % 256 + usize::from(reject_cleanup) * 256,
                "only bounded remainder and the failed best-effort batch survive"
            );
            pending
                .records
                .into_iter()
                .map(|row| row.record.key)
                .collect::<Vec<_>>()
        }))
        .catch_unwind()
        .await;
        let cleanup = app
            .shutdown(Shutdown::Drain {
                timeout: Duration::from_secs(10),
            })
            .await;
        let old_receipts = match observed {
            Ok(result) => result.expect("receipt batching test stays bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        };
        cleanup.unwrap();
        drop(app);
        // Reopen removes both the bounded idle remainder and best-effort failures.
        let mut reopened: App<_, Agent> =
            App::open(AppConfig::new(native.clone(), Vec::new(), Vec::new()))
                .await
                .unwrap();
        for key in old_receipts {
            assert!(native.load(key).await.unwrap().is_none());
        }
        reopened
            .shutdown(Shutdown::Drain {
                timeout: Duration::from_secs(10),
            })
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn retired_receipt_cleanup_preserves_an_unresolved_native_commit() {
    let (faults, store) = StorageFaults::open().await.unwrap();
    let native = store.inner.clone();
    let mut app: App<_, Agent> = App::open(AppConfig::new(
        store,
        Vec::new(),
        vec![WorkflowDefinition::parse(LINEAR).unwrap()],
    ))
    .await
    .unwrap();
    let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(30), async {
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().unwrap(),
            })
            .await
            .unwrap();
        let second = app
            .create_workspace(WorkspaceConfig {
                name: Some("unresolved".into()),
                root: std::env::current_dir().unwrap(),
            })
            .await
            .unwrap();
        let workflow = second.workflow("linear".parse().unwrap()).await.unwrap();
        let request = StateListRequest::new("app-sdk")
            .unwrap()
            .with_key_prefix(StateKeyPrefix::new("receipt/").unwrap())
            .with_limit(1000)
            .unwrap();
        let before = native.list(request.clone()).await.unwrap();
        assert!(before.next_start_after.is_none());
        faults.arm_admission();
        let mut admission = Box::pin(workflow.prepare(WorkflowState::default()));
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = faults.admission_committed.notified() => {},
                _ = &mut admission => panic!("original native commit return stays held"),
            }
        })
        .await
        .expect("native commit is held after actual publication");
        let during = native.list(request).await.unwrap();
        assert!(during.next_start_after.is_none());
        let newly_committed: Vec<_> = during
            .records
            .into_iter()
            .filter(|row| {
                !before
                    .records
                    .iter()
                    .any(|old| old.record.key == row.record.key)
            })
            .collect();
        assert_eq!(newly_committed.len(), 1);
        let original = &newly_committed[0];
        let mut revision = workspace.snapshot().await.unwrap().revision;
        let cleanup_before = faults.receipt_cleanup_batches.lock().unwrap().len();
        for index in 0..300 {
            revision = tokio::time::timeout(
                Duration::from_secs(5),
                workspace.replace(
                    revision,
                    WorkspaceConfig {
                        name: Some(format!("parallel-retirement-{index}")),
                        root: std::env::current_dir().unwrap(),
                    },
                ),
            )
            .await
            .expect("other mutations retain their original bound")
            .unwrap()
            .revision;
        }
        assert!(
            faults.receipt_cleanup_batches.lock().unwrap().len() > cleanup_before,
            "the original receipt survives actual concurrent cleanup commits"
        );
        assert_eq!(
            native
                .load(original.record.key.clone())
                .await
                .unwrap()
                .as_ref(),
            Some(original),
            "other owners cannot delete or rewrite an unresolved commit's original receipt"
        );
        faults.release_admission();
        let run = tokio::time::timeout(Duration::from_secs(5), admission)
            .await
            .expect("the original uncertain return reconciles within its bound")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), faults.ambiguity_observed.notified())
            .await
            .expect("the original receipt read was actually unavailable");
        assert!(!faults.receipt_obscured.load(Ordering::SeqCst));
        assert!(matches!(
            run.snapshot().await.unwrap().status,
            RunStatus::Paused
        ));
        assert_eq!(
            workflow
                .list_runs(None, NonZeroUsize::new(10).unwrap())
                .await
                .unwrap()
                .len(),
            1,
            "uncertain return cannot replay the accepted admission"
        );
    }))
    .catch_unwind()
    .await;
    faults.release_admission();
    let cleanup = app
        .shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(10),
        })
        .await;
    match observed {
        Ok(result) => {
            result.expect("unresolved receipt test stays bounded");
            cleanup.unwrap();
        }
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

#[tokio::test]
async fn unknown_admission_readback_rejects_reordered_state() {
    StorageFaults::assert_checkpoint_evidence(true, CheckpointEvidence::StateOrder).await;
}

#[tokio::test]
async fn unknown_checkpoint_readback_rejects_reordered_state() {
    StorageFaults::assert_checkpoint_evidence(false, CheckpointEvidence::StateOrder).await;
}

#[tokio::test]
async fn dropped_admission_observer_cannot_orphan_an_applied_run()
-> Result<(), Box<dyn Error + Send + Sync>> {
    for prepared in [false, true] {
        let (faults, store) = StorageFaults::open().await?;
        let mut app = App::open(AppConfig::<FaultStateStore, Agent>::new(
            store,
            Vec::new(),
            vec![WorkflowDefinition::parse(LINEAR)?],
        ))
        .await?;
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: Some("drop-safe-admission".to_owned()),
                root: std::env::current_dir()?,
            })
            .await?;
        let workflow = workspace.workflow("linear".parse()?).await?;

        faults.arm_admission();
        let committed = faults.admission_committed.notified();
        tokio::pin!(committed);
        committed.as_mut().enable();
        let mut admission = Box::pin(async {
            if prepared {
                workflow
                    .prepare(app_sdk::workflow::WorkflowState::default())
                    .await
            } else {
                workflow.start(ExecutionOptions::default()).await
            }
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = &mut committed => {}
                _ = &mut admission => panic!("admission returned before the applied commit gate"),
            }
        })
        .await
        .expect("the applied admission commit must reach its test-owned gate");
        drop(admission);

        let ambiguity = faults.ambiguity_observed.notified();
        tokio::pin!(ambiguity);
        ambiguity.as_mut().enable();
        faults.release_admission();
        tokio::time::timeout(Duration::from_secs(5), &mut ambiguity)
            .await
            .expect("the applied receipt must be obscured exactly once");

        let runs = workflow
            .list_runs(
                None,
                NonZeroUsize::new(10).expect("test list bound is nonzero"),
            )
            .await?;
        assert_eq!(runs.len(), 1, "admission must create exactly one run");
        let run = workflow
            .open_run(RunSelector::Latest(runs[0].id.clone()))
            .await?;
        let terminal = tokio::time::timeout(Duration::from_secs(5), run.wait())
            .await
            .expect("the accepted run must remain runtime-owned after observer drop")?;
        if prepared {
            assert_eq!(terminal.checkpoint, 0);
            assert!(matches!(terminal.status, RunStatus::Paused));
            assert_eq!(terminal.state.values()["count"], 0);
            assert_eq!(terminal.state.values()["path"], "unset");
            assert!(
                tokio::time::timeout(Duration::from_millis(150), async {
                    loop {
                        let snapshot = run
                            .snapshot()
                            .await
                            .expect("prepared readback after lost receipt");
                        assert_eq!(snapshot.checkpoint, 0);
                        assert!(matches!(snapshot.status, RunStatus::Paused));
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .is_err()
            );
            run.cancel(0).await?;
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(5), run.wait())
                    .await
                    .expect("prepared cancellation must settle")?
                    .status,
                RunStatus::Cancelled
            ));
        } else {
            assert!(terminal.checkpoint > 0);
            assert!(matches!(terminal.status, RunStatus::Succeeded));
        }

        app.shutdown(Shutdown::Drain {
            timeout: Duration::from_secs(10),
        })
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_reconcile_and_checkpoint_causes_share_one_run_driver()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let (faults, store) = StorageFaults::open().await?;
    let mut app = App::open(AppConfig::<FaultStateStore, Agent>::new(
        store,
        Vec::new(),
        vec![WorkflowDefinition::parse(DROP_WAIT)?],
    ))
    .await?;
    let workspace = app
        .create_workspace(WorkspaceConfig {
            name: Some("single-run-driver".to_owned()),
            root: std::env::current_dir()?,
        })
        .await?;
    let workflow = workspace.workflow("drop-wait".parse()?).await?;

    faults.arm_admission();
    faults.arm_admission_inventory();
    faults.arm_run_transition_monitor();
    let committed = faults.admission_committed.notified();
    let inventory = faults.admission_inventory_observed.notified();
    let transition = faults.run_transition_observed.notified();
    tokio::pin!(committed, inventory, transition);
    committed.as_mut().enable();
    inventory.as_mut().enable();
    transition.as_mut().enable();
    let mut admission = Box::pin(workflow.start(ExecutionOptions::default()));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = &mut committed => {}
            _ = &mut admission => panic!("admission returned before its ambiguous commit gate"),
        }
    })
    .await
    .expect("run admission must reach its test-owned commit gate");
    faults.release_admission();

    tokio::time::timeout(Duration::from_secs(5), &mut inventory)
        .await
        .expect("unknown admission must retain its original inventory proof");
    assert_eq!(
        faults.active_run_transitions.load(Ordering::SeqCst),
        0,
        "no driver may advance before admission and registration are confirmed"
    );
    faults.release_admission_inventory();
    let run = tokio::time::timeout(Duration::from_secs(5), &mut admission)
        .await
        .expect("checkpoint-ready admission must rejoin its retained reconcile cause")?;
    tokio::time::timeout(Duration::from_secs(5), &mut transition)
        .await
        .expect("one driver advances after the original admission proof");

    // The only run driver is held before its native commit. No Agents or other
    // operations are active; these eight sequential cancellations therefore own
    // every rollback observed before that driver is released.
    *faults.observations.lock().unwrap() = Some(BTreeMap::new());
    faults.fail_mutation_admissions(8);
    for _ in 0..8 {
        assert!(matches!(run.cancel(0).await, Err(AppError::Integration(_))));
    }
    assert_eq!(faults.mutation_admission_failures.load(Ordering::SeqCst), 0);

    let rejected_rollbacks = faults
        .observations
        .lock()
        .unwrap()
        .take()
        .unwrap()
        .remove("rollback")
        .unwrap_or_default();

    let sleep_load = faults.sleep_load_observed.notified();
    tokio::pin!(sleep_load);
    sleep_load.as_mut().enable();
    faults.arm_sleep_load();
    faults.release_run_transition();
    tokio::time::timeout(Duration::from_millis(500), &mut sleep_load)
        .await
        .expect("the earliest zero-delay cause must survive later retained retry requests");
    assert_eq!(
        faults.maximum_active_run_transitions.load(Ordering::SeqCst),
        1,
    );
    faults.run_transition_monitor.store(false, Ordering::SeqCst);
    let current = run.snapshot().await?;
    run.cancel(current.checkpoint).await?;
    let cancelled = tokio::time::timeout(Duration::from_secs(5), run.wait())
        .await
        .expect("the coalesced driver must settle explicit cancellation")?;
    assert!(matches!(cancelled.status, RunStatus::Cancelled));

    app.shutdown(Shutdown::Drain {
        timeout: Duration::from_secs(10),
    })
    .await?;
    // Check the deliberately rejected mutation receipts after normal joining,
    // so the old fixture's missing coverage cannot strand the held driver.
    assert_eq!(
        (
            rejected_rollbacks.started,
            rejected_rollbacks.completed,
            rejected_rollbacks.failed,
            rejected_rollbacks.active,
            rejected_rollbacks.dropped_observers,
        ),
        (8, 8, 0, 0, 0),
        "each deliberately rejected cancellation mutation consumes its native rollback",
    );
    Ok(())
}

#[tokio::test]
async fn independent_app_store_admissions_reach_the_state_store_boundary_concurrently()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let (faults, store) = StorageFaults::open().await?;
    let mut app = App::open(AppConfig::<FaultStateStore, Agent>::new(
        store,
        Vec::new(),
        vec![WorkflowDefinition::parse(DROP_WAIT)?],
    ))
    .await?;
    let workspace = app
        .create_workspace(WorkspaceConfig {
            name: Some("independent-store-admission".to_owned()),
            root: std::env::current_dir()?,
        })
        .await?;
    let workflow = workspace.workflow("drop-wait".parse()?).await?;

    let first_parked = faults.sleep_load_observed.notified();
    tokio::pin!(first_parked);
    first_parked.as_mut().enable();
    faults.arm_sleep_load();
    let first = workflow.start(ExecutionOptions::default()).await?;
    tokio::time::timeout(Duration::from_secs(5), &mut first_parked)
        .await
        .expect("the first run must durably park its sleep control");

    let second_parked = faults.sleep_load_observed.notified();
    tokio::pin!(second_parked);
    second_parked.as_mut().enable();
    faults.arm_sleep_load();
    let second = workflow.start(ExecutionOptions::default()).await?;
    tokio::time::timeout(Duration::from_secs(5), &mut second_parked)
        .await
        .expect("the second run must durably park its sleep control");

    let (first_ready, second_ready) = tokio::try_join!(first.snapshot(), second.snapshot())?;
    assert!(first_ready.checkpoint > 0);
    assert!(second_ready.checkpoint > 0);
    assert!(matches!(first_ready.status, RunStatus::Running));
    assert!(matches!(second_ready.status, RunStatus::Running));

    let (first_cancelled, second_cancelled) = {
        faults.arm_app_store_mutation_monitor();
        let observed = faults.run_transition_observed.notified();
        tokio::pin!(observed);
        observed.as_mut().enable();
        let cancellations = async {
            tokio::join!(
                first.cancel(first_ready.checkpoint),
                second.cancel(second_ready.checkpoint),
            )
        };
        tokio::pin!(cancellations);
        let observed = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = &mut observed => true,
                _ = &mut cancellations => false,
            }
        })
        .await;
        if !matches!(observed, Ok(true)) {
            faults
                .app_store_admission_monitor
                .store(false, Ordering::SeqCst);
            faults.release_app_store_admission();
        }
        assert!(
            observed.expect("one app mutation must reach the test-owned StateStore admission gate"),
            "both cancellations completed before the first StateStore admission was released",
        );
        let overlap = tokio::time::timeout(Duration::from_secs(5), async {
            while faults.maximum_active_run_transitions.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        let maximum = faults.maximum_active_run_transitions.load(Ordering::SeqCst);
        faults
            .app_store_admission_monitor
            .store(false, Ordering::SeqCst);
        faults.release_app_store_admission();
        overlap.expect("the independent mutation must reach StateStore while its peer is gated");
        assert_eq!(
            maximum, 2,
            "app-sdk must not serialize independent mutations before the StateStore boundary",
        );
        tokio::time::timeout(Duration::from_secs(5), &mut cancellations)
            .await
            .expect("both cancellation mutations must settle after admission is released")
    };
    first_cancelled?;
    second_cancelled?;
    let (first_terminal, second_terminal) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(first.wait(), second.wait())
    })
    .await
    .expect("both independently cancelled runs must settle");
    assert!(matches!(first_terminal?.status, RunStatus::Cancelled));
    assert!(matches!(second_terminal?.status, RunStatus::Cancelled));
    assert_eq!(faults.active_run_transitions.load(Ordering::SeqCst), 0);

    app.shutdown(Shutdown::Drain {
        timeout: Duration::from_secs(10),
    })
    .await?;
    assert!(
        faults.monitored_mutations.load(Ordering::SeqCst) >= 2,
        "the monitor must reach actual mutation receipt loads, not read-only authority"
    );
    Ok(())
}

#[tokio::test]
async fn run_admission_reconciles_storage_conflicts_without_bypassing_workspace_fences()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let (faults, store) = StorageFaults::open().await?;
    let mut app = App::open(AppConfig::<FaultStateStore, Agent>::new(
        store,
        Vec::new(),
        vec![WorkflowDefinition::parse(LINEAR)?],
    ))
    .await?;
    let workspace = app
        .create_workspace(WorkspaceConfig {
            name: Some("run-admission-race".to_owned()),
            root: std::env::current_dir()?,
        })
        .await?;
    let revision = workspace.snapshot().await?.revision;
    let workflow = workspace.workflow("linear".parse()?).await?;

    let limit = NonZeroUsize::new(10).expect("test list bound is nonzero");
    faults.arm_app_store_admission_monitor();
    let observed = faults.run_transition_observed.notified();
    tokio::pin!(observed);
    observed.as_mut().enable();
    let mut stale_admission = Box::pin(workflow.start(ExecutionOptions::default()));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = &mut observed => {}
            _ = &mut stale_admission => {
                panic!("old-handle admission returned before its StateStore transaction was gated")
            }
        }
    })
    .await
    .expect("old-handle admission must reach the existing StateStore gate");
    let replaced = tokio::time::timeout(
        Duration::from_secs(5),
        workspace.replace(
            revision,
            WorkspaceConfig {
                name: Some("replaced-run-admission-race".to_owned()),
                root: std::env::current_dir()?,
            },
        ),
    )
    .await;
    faults
        .app_store_admission_monitor
        .store(false, Ordering::SeqCst);
    faults.release_app_store_admission();
    let replaced = replaced.expect("workspace replacement must complete while admission waits")?;
    assert!(replaced.revision > revision);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), &mut stale_admission)
            .await
            .expect("admission must revalidate the logical workspace fence"),
        Err(AppError::Conflict(_))
    ));
    assert!(workflow.list_runs(None, limit).await?.is_empty());

    let revision = workspace.snapshot().await?.revision;
    let workflow = workspace.workflow("linear".parse()?).await?;

    faults.arm_app_store_admission_monitor();
    let observed = faults.run_transition_observed.notified();
    tokio::pin!(observed);
    observed.as_mut().enable();
    let mut admission = Box::pin(workflow.start(ExecutionOptions {
        state: WorkflowState::new(Map::from_iter([
            ("case".to_owned(), Value::from("cancellation")),
            ("threshold".to_owned(), Value::from(7)),
        ]))?,
        ..ExecutionOptions::default()
    }));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = &mut observed => {}
            _ = &mut admission => {
                panic!("run admission returned before its StateStore transaction was gated")
            }
        }
    })
    .await
    .expect("run admission must reach the existing StateStore gate");

    let competing = tokio::time::timeout(
        Duration::from_secs(5),
        workflow.start(ExecutionOptions {
            state: WorkflowState::new(Map::from_iter([
                ("case".to_owned(), Value::from("cancellation")),
                ("threshold".to_owned(), Value::from(11)),
            ]))?,
            ..ExecutionOptions::default()
        }),
    )
    .await;
    faults
        .app_store_admission_monitor
        .store(false, Ordering::SeqCst);
    faults.release_app_store_admission();
    let competing =
        competing.expect("independent run admission must complete while its peer waits")?;
    let admitted = tokio::time::timeout(Duration::from_secs(5), &mut admission)
        .await
        .expect("run admission must reconcile the definite storage conflict")?;
    assert_ne!(admitted.id(), competing.id());

    for (run, threshold) in [(&admitted, 7), (&competing, 11)] {
        let waiting = ExecutionJourney::wait(run).await?;
        assert!(matches!(
            waiting.interactions.as_slice(),
            [Interaction::Input { .. }]
        ));
        assert_eq!(
            waiting.state.values().get("threshold"),
            Some(&Value::from(threshold))
        );
    }
    assert_eq!(workspace.snapshot().await?.revision, revision);
    let runs = workflow.list_runs(None, limit).await?;
    assert_eq!(runs.len(), 2, "the two callers must admit exactly two runs");
    assert!(runs.iter().any(|run| &run.id == admitted.id()));
    assert!(runs.iter().any(|run| &run.id == competing.id()));

    for run in [&admitted, &competing] {
        let before = run.snapshot().await?;
        run.cancel(before.checkpoint).await?;
        let cancelled = ExecutionJourney::wait(run).await?;
        assert!(matches!(cancelled.status, RunStatus::Cancelled));
        assert_eq!(
            cancelled.state, before.state,
            "an incomplete wave cannot publish staged writes"
        );
        assert_eq!(cancelled.last_action_ordinal, before.last_action_ordinal);
        assert!(cancelled.outcomes == before.outcomes);
    }

    app.shutdown(Shutdown::Drain {
        timeout: Duration::from_secs(10),
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn run_advance_retries_after_an_independent_app_revision_change()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let (faults, store) = StorageFaults::open().await?;
    let mut app = App::open(AppConfig::<FaultStateStore, Agent>::new(
        store,
        Vec::new(),
        vec![WorkflowDefinition::parse(LINEAR)?],
    ))
    .await?;
    let workspace = app
        .create_workspace(WorkspaceConfig {
            name: Some("run-app-fence-race".to_owned()),
            root: std::env::current_dir()?,
        })
        .await?;
    let workflow = workspace.workflow("linear".parse()?).await?;
    let run = workflow
        .start(ExecutionOptions {
            state: WorkflowState::new(Map::from_iter([(
                "case".to_owned(),
                Value::from("cancellation"),
            )]))?,
            ..ExecutionOptions::default()
        })
        .await?;
    let waiting = ExecutionJourney::wait(&run).await?;
    let [
        Interaction::Input {
            sequence,
            generation,
            ..
        },
    ] = waiting.interactions.as_slice()
    else {
        panic!("the app-fence race run must park at its input")
    };

    faults.arm_app_store_mutation_monitor();
    let observed = faults.run_transition_observed.notified();
    tokio::pin!(observed);
    observed.as_mut().enable();
    let mut resolution =
        Box::pin(run.resolve(*sequence, *generation, Resolution::Input(Value::Null)));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = &mut observed => {}
            _ = &mut resolution => {
                panic!("run advance returned before its StateStore admission was gated")
            }
        }
    })
    .await
    .expect("the run advance must reach the test-owned StateStore admission gate");

    let competing = workflow.start(ExecutionOptions::default()).await;
    let maximum = faults.maximum_active_run_transitions.load(Ordering::SeqCst);
    faults
        .app_store_admission_monitor
        .store(false, Ordering::SeqCst);
    faults.release_app_store_admission();
    assert!(
        maximum >= 2,
        "the competing admission must advance app authority while the run mutation is gated",
    );
    let competing = competing?;
    let resumed = tokio::time::timeout(Duration::from_secs(5), &mut resolution)
        .await
        .expect("the run mutation must reconcile its stale app fence")?;
    assert!(resumed.checkpoint > waiting.checkpoint);

    let terminal = tokio::time::timeout(Duration::from_secs(5), run.wait())
        .await
        .expect("the reconciled run must retain its driver")?;
    assert!(matches!(
        terminal.status,
        RunStatus::Failed(WorkflowFailure::State(_))
    ));
    let competing_terminal = tokio::time::timeout(Duration::from_secs(5), competing.wait())
        .await
        .expect("the independent app-revision mutation must also converge")?;
    assert!(matches!(competing_terminal.status, RunStatus::Succeeded));

    app.shutdown(Shutdown::Drain {
        timeout: Duration::from_secs(10),
    })
    .await?;
    assert!(
        faults.monitored_mutations.load(Ordering::SeqCst) >= 1,
        "the monitor must reach actual mutation receipt loads, not read-only authority"
    );
    assert!(
        faults.monitored_mutation_conflicts.load(Ordering::SeqCst) >= 1,
        "the held native snapshot must report a real conflict before App retries"
    );
    Ok(())
}

#[tokio::test]
async fn competing_unknown_app_claim_never_publishes_a_second_owner()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let store = ClaimRaceState::store();
    let left = App::open(AppConfig::<ClaimRaceStore, Agent>::new(
        store.clone(),
        Vec::new(),
        Vec::new(),
    ));
    let right = App::open(AppConfig::<ClaimRaceStore, Agent>::new(
        store,
        Vec::new(),
        Vec::new(),
    ));
    let (left, right) =
        tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(left, right) })
            .await
            .expect("the concurrent application claims must settle");
    let (mut owner, rejected) = match (left, right) {
        (Ok(owner), Err(error)) | (Err(error), Ok(owner)) => (owner, error),
        (Ok(_), Ok(_)) => panic!("only the exact-receipt claimant may publish an App"),
        (Err(left), Err(right)) => {
            panic!("one exact-receipt claimant must publish: {left:?}; {right:?}")
        }
    };
    assert!(matches!(
        rejected,
        AppError::Integration(message) if message == "application generation claim is unknown"
    ));
    owner
        .shutdown(Shutdown::Drain {
            timeout: Duration::from_secs(10),
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn dropped_workspace_observer_cannot_orphan_an_ambiguously_applied_admission()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let (faults, store) = StorageFaults::open().await?;
    let mut app = App::open(AppConfig::<FaultStateStore, Agent>::new(
        store,
        Vec::new(),
        Vec::new(),
    ))
    .await?;

    faults.arm_workspace_admission();
    let committed = faults.workspace_admission_committed.notified();
    tokio::pin!(committed);
    committed.as_mut().enable();
    let mut admission = Box::pin(app.create_workspace(WorkspaceConfig {
        name: Some("ambiguous-workspace-admission".to_owned()),
        root: std::env::current_dir()?,
    }));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = &mut committed => {}
            _ = &mut admission => panic!("workspace admission returned before its applied commit gate"),
        }
    })
    .await
    .expect("workspace admission must reach its test-owned commit gate");
    drop(admission);

    let ambiguity = faults.ambiguity_observed.notified();
    tokio::pin!(ambiguity);
    ambiguity.as_mut().enable();
    let observation_fault = faults.workspace_observation_load_faulted.notified();
    tokio::pin!(observation_fault);
    observation_fault.as_mut().enable();
    faults.release_workspace_admission();
    tokio::time::timeout(Duration::from_secs(5), &mut ambiguity)
        .await
        .expect("the applied workspace receipt must be obscured exactly once");
    tokio::time::timeout(Duration::from_secs(5), &mut observation_fault)
        .await
        .expect("the first exact workspace-admission observation must fail transiently");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let workspaces = app.list_workspaces(None, NonZeroUsize::MIN).await?;
            if workspaces.len() == 1 {
                return Ok::<(), AppError>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the owned workspace admission must reconcile after observer drop")?;

    app.shutdown(Shutdown::Drain {
        timeout: Duration::from_secs(10),
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn same_fence_workspace_operations_require_same_intent_and_target()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let (state, agent) = StreamFailureState::open()?;
    let config: AppConfig<StateStore, StreamFailureAgent> = AppConfig::new(
        StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("caller memory store"),
        vec![AgentSpec::new(
            "workspace-owner".to_owned(),
            "v1".to_owned(),
            agent,
        )],
        Vec::new(),
    );
    let mut app = App::open(config).await?;
    let workspace = app
        .create_workspace(WorkspaceConfig {
            name: Some("before".to_owned()),
            root: std::env::current_dir()?,
        })
        .await?;
    let mut competing = app.workspace(workspace.id().clone()).await?;
    let revision = workspace.snapshot().await?.revision;

    state.arm_workspace_update();
    let update_observed = state.workspace_update_observed.notified();
    tokio::pin!(update_observed);
    update_observed.as_mut().enable();
    let mut replacing = Box::pin(workspace.replace(
        revision,
        WorkspaceConfig {
            name: Some("after".to_owned()),
            root: std::env::current_dir()?,
        },
    ));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = &mut update_observed => {}
            _ = &mut replacing => {
                panic!("replacement returned before its lower update was gated")
            }
        }
    })
    .await
    .expect("replacement must reach its lower update gate");

    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), competing.delete(revision)).await,
        Ok(Err(AppError::Conflict(_)))
    ));
    state.release_workspace_update();
    replacing.await?;

    let revision = workspace.snapshot().await?.revision;
    state.arm_workspace_check();
    let check_observed = state.workspace_check_observed.notified();
    tokio::pin!(check_observed);
    check_observed.as_mut().enable();
    let mut first_target = Box::pin(workspace.replace(
        revision,
        WorkspaceConfig {
            name: Some("first-target".to_owned()),
            root: std::env::current_dir()?,
        },
    ));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = &mut check_observed => {}
            _ = &mut first_target => {
                panic!("first replacement returned before its preflight was gated")
            }
        }
    })
    .await
    .expect("first replacement must reach its preflight gate");

    state.arm_workspace_update();
    let second_update_observed = state.workspace_update_observed.notified();
    tokio::pin!(second_update_observed);
    second_update_observed.as_mut().enable();
    let mut second_target = Box::pin(competing.replace(
        revision,
        WorkspaceConfig {
            name: Some("second-target".to_owned()),
            root: std::env::current_dir()?,
        },
    ));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = &mut second_update_observed => {}
            _ = &mut second_target => {
                panic!("second replacement returned before its lower update was gated")
            }
        }
    })
    .await
    .expect("second replacement must own its intended durable target");

    state.release_workspace_check();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), first_target.as_mut()).await,
        Ok(Err(AppError::Conflict(_)))
    ));
    state.release_workspace_update();
    second_target.await?;
    app.shutdown(Shutdown::Drain {
        timeout: Duration::from_secs(10),
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn recovery_publishes_after_deletion_reattachment_before_convergence()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let store =
        StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    let (state, agent) = StreamFailureState::open()?;
    let app = App::open(AppConfig::new(
        store.clone(),
        vec![AgentSpec::new(
            "deletion-owner".to_owned(),
            "v1".to_owned(),
            agent,
        )],
        Vec::new(),
    ))
    .await?;
    let mut workspace = app
        .create_workspace(WorkspaceConfig {
            name: Some("recovering-deletion".to_owned()),
            root: std::env::current_dir()?,
        })
        .await?;
    let workspace_id = workspace.id().clone();
    let revision = workspace.snapshot().await?.revision;

    state.arm_workspace_deletion();
    let first_delete_observed = state.workspace_delete_observed.notified();
    tokio::pin!(first_delete_observed);
    first_delete_observed.as_mut().enable();
    let mut deletion = Box::pin(workspace.delete(revision));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = &mut first_delete_observed => {}
            result = &mut deletion => {
                panic!("deletion returned before its lower operation was gated: {result:?}")
            }
        }
    })
    .await
    .expect("the initial deletion must durably enter its lower operation");
    drop(deletion);
    drop(workspace);
    drop(app);
    tokio::task::yield_now().await;

    let recovered_delete_observed = state.workspace_delete_observed.notified();
    tokio::pin!(recovered_delete_observed);
    recovered_delete_observed.as_mut().enable();
    let mut app = tokio::time::timeout(
        Duration::from_secs(5),
        App::open(AppConfig::new(
            store,
            vec![AgentSpec::new(
                "deletion-owner".to_owned(),
                "v1".to_owned(),
                state.agent(),
            )],
            Vec::new(),
        )),
    )
    .await
    .expect("App open must publish while the reattached lower deletion remains gated")?;
    tokio::time::timeout(Duration::from_secs(5), &mut recovered_delete_observed)
        .await
        .expect("the recovered deletion owner must resume its exact lower operation");
    assert_eq!(state.workspace_delete_attempts.load(Ordering::SeqCst), 2);

    let mut reattacher = app.workspace(workspace_id.clone()).await?;
    assert!(matches!(
        reattacher.snapshot().await,
        Err(AppError::Closed(_))
    ));
    let mut reattached = Box::pin(reattacher.delete(revision));
    assert!(
        tokio::time::timeout(Duration::from_millis(25), reattached.as_mut())
            .await
            .is_err(),
        "the same-fence observer must share the gated recovered deletion",
    );
    assert_eq!(
        state.workspace_delete_attempts.load(Ordering::SeqCst),
        2,
        "same-fence reattachment must not dispatch another lower deletion",
    );

    state.release_workspace_deletion();
    tokio::time::timeout(Duration::from_secs(5), reattached)
        .await
        .expect("the released recovered deletion must converge")?;
    assert_eq!(state.workspace_delete_attempts.load(Ordering::SeqCst), 2);
    assert!(matches!(
        app.workspace(workspace_id).await,
        Err(AppError::NotFound(_))
    ));
    app.shutdown(Shutdown::Drain {
        timeout: Duration::from_secs(10),
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn recovered_workspace_deletion_retains_owner_beyond_transient_retry_window()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let store =
        StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    let (state, agent) = StreamFailureState::open()?;
    let mut app = App::open(AppConfig::new(
        store,
        vec![AgentSpec::new(
            "recovered-persistent-deletion-owner".to_owned(),
            "v1".to_owned(),
            agent,
        )],
        Vec::new(),
    ))
    .await?;
    let mut workspace = app
        .create_workspace(WorkspaceConfig {
            name: Some("recovered-persistent-deletion".to_owned()),
            root: std::env::current_dir()?,
        })
        .await?;
    let workspace_id = workspace.id().clone();
    let revision = workspace.snapshot().await?.revision;

    state.retry_workspace_deletion_then_complete(12);
    let first_attempt = state.workspace_delete_observed.notified();
    tokio::pin!(first_attempt);
    first_attempt.as_mut().enable();
    let mut deletion = Box::pin(workspace.delete(revision));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = &mut first_attempt => {}
            result = &mut deletion => {
                panic!("deletion returned before its initial gated lower attempt: {result:?}")
            }
        }
    })
    .await
    .expect("the initial deletion must durably enter its lower operation");
    assert_eq!(state.workspace_delete_attempts.load(Ordering::SeqCst), 1);
    drop(deletion);
    drop(workspace);

    tokio::time::timeout(Duration::from_secs(15), async {
        while state.workspace_delete_attempts.load(Ordering::SeqCst) < 13 {
            tokio::task::yield_now().await;
        }
        loop {
            match app.workspace(workspace_id.clone()).await {
                Ok(handle) => drop(handle),
                Err(AppError::NotFound(_)) => return Ok::<(), AppError>(()),
                Err(error) => return Err(error),
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the recovered deletion owner must outlive eight transient attempts")?;
    assert_eq!(
        state.workspace_delete_attempts.load(Ordering::SeqCst),
        13,
        "twelve transient calls plus one success from the retained owner must settle deletion",
    );

    app.shutdown(Shutdown::Drain {
        timeout: Duration::from_secs(10),
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn reject_active_seals_admission_before_its_quiescence_scan_and_reverts_exactly()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let (faults, store) = StorageFaults::open().await?;
    let mut app = App::open(AppConfig::<FaultStateStore, Agent>::new(
        store,
        Vec::new(),
        vec![
            WorkflowDefinition::parse(LINEAR)?,
            WorkflowDefinition::parse(DROP_WAIT)?,
        ],
    ))
    .await?;
    let workspace = app
        .create_workspace(WorkspaceConfig {
            name: Some("reject-seal".to_owned()),
            root: std::env::current_dir()?,
        })
        .await?;
    let waiting_workflow = workspace.workflow("drop-wait".parse()?).await?;
    let waiting = waiting_workflow.start(ExecutionOptions::default()).await?;
    let linear = workspace.workflow("linear".parse()?).await?;

    faults.arm_shutdown_scan();
    let observed = faults.shutdown_scan_observed.notified();
    tokio::pin!(observed);
    observed.as_mut().enable();
    let mut rejection = Box::pin(app.shutdown(Shutdown::RejectActive));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = &mut observed => {}
            result = &mut rejection => panic!("RejectActive returned before its gated scan: {result:?}"),
        }
    })
    .await
    .expect("RejectActive must reach its test-owned quiescence gate");

    assert!(matches!(
        linear.start(ExecutionOptions::default()).await,
        Err(AppError::Closed(_))
    ));
    faults.release_shutdown_scan();
    assert!(matches!(rejection.await, Err(AppError::Busy(_))));

    let admitted = linear.start(ExecutionOptions::default()).await?;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), admitted.wait())
            .await??
            .status,
        RunStatus::Succeeded
    ));
    let snapshot = waiting.snapshot().await?;
    waiting.cancel(snapshot.checkpoint).await?;
    app.shutdown(Shutdown::CancelAndDrain {
        timeout: Duration::from_secs(10),
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn shutdown_timeout_only_ends_observation_and_later_observer_reattaches()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let (state, agent) = StreamFailureState::open()?;
    state.arm_shutdown();
    let config: AppConfig<StateStore, StreamFailureAgent> = AppConfig::new(
        StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("caller memory store"),
        vec![AgentSpec::new(
            "timed-finalizer".to_owned(),
            "v1".to_owned(),
            agent,
        )],
        Vec::new(),
    );
    let mut app = App::open(config).await?;

    let lower_shutdown = state.shutdown_observed.notified();
    tokio::pin!(lower_shutdown);
    lower_shutdown.as_mut().enable();
    assert!(matches!(
        app.shutdown(Shutdown::Drain {
            timeout: Duration::from_millis(25),
        })
        .await,
        Err(AppError::Timeout(_))
    ));
    tokio::time::timeout(Duration::from_secs(5), &mut lower_shutdown)
        .await
        .expect("the accepted finalizer must continue into lower shutdown after timeout");

    let mut reattached = Box::pin(app.shutdown(Shutdown::Drain {
        timeout: Duration::from_secs(10),
    }));
    assert!(
        tokio::time::timeout(Duration::from_millis(25), reattached.as_mut())
            .await
            .is_err(),
        "a later observer must share the still-gated finalizer completion",
    );
    drop(reattached);
    state.release_shutdown();
    app.shutdown(Shutdown::Drain {
        timeout: Duration::from_secs(10),
    })
    .await?;
    assert_eq!(
        state.shutdowns.load(Ordering::SeqCst),
        1,
        "dropping successive observers cannot repeat or lose the owned lower cleanup",
    );
    Ok(())
}

#[tokio::test]
async fn shutdown_failure_closes_live_ownership_and_replays_to_late_observers()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let (faults, store) = StorageFaults::open().await?;
    let mut app = App::open(AppConfig::<FaultStateStore, Agent>::new(
        store,
        Vec::new(),
        Vec::new(),
    ))
    .await?;

    faults.fail_transaction_begins(1);
    let retained = match app
        .shutdown(Shutdown::Drain {
            timeout: Duration::from_secs(10),
        })
        .await
    {
        Err(AppError::Integration(message)) => message,
        result => panic!("shutdown must retain its durable publication failure: {result:?}"),
    };
    let replayed = match app
        .shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(10),
        })
        .await
    {
        Err(AppError::Integration(message)) => message,
        result => panic!("a late observer must replay the closed outcome: {result:?}"),
    };
    assert_eq!(replayed, retained);
    assert!(matches!(
        app.create_workspace(WorkspaceConfig {
            name: Some("closed-after-failure".to_owned()),
            root: std::env::current_dir()?,
        })
        .await,
        Err(AppError::Closed(_))
    ));
    Ok(())
}

#[tokio::test]
async fn cancel_cleanup_reobservation_after_timeout_preserves_the_first_receipt()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let (_, store) = StorageFaults::open().await?;
    let (state, agent) = StreamFailureState::open()?;
    let mut app = App::open(AppConfig::new(
        store,
        vec![AgentSpec::new(
            "cleanup-success".to_owned(),
            "v1".to_owned(),
            agent,
        )],
        Vec::new(),
    ))
    .await?;
    state.arm_shutdown();
    let lower_shutdown = state.shutdown_observed.notified();
    tokio::pin!(lower_shutdown);
    lower_shutdown.as_mut().enable();
    let first = app
        .shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_millis(25),
        })
        .await;
    let entered = tokio::time::timeout(Duration::from_secs(5), &mut lower_shutdown).await;
    state.release_shutdown();
    let mut observations = 0;
    let joined = loop {
        let joined = app
            .shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(10),
            })
            .await;
        observations += 1;
        assert_eq!(observations, 1, "the released finalizer must join once");
        // This fresh owner requests only valid CancelAndDrain, so Integration
        // is a retained failure delivered after owned joining.
        if joined.is_ok() || matches!(&joined, Err(AppError::Integration(_))) {
            assert!(
                joined.is_ok(),
                "the released successful cleanup must succeed"
            );
            break joined;
        }
    };

    entered.expect("the timed-out observer leaves its gated lower cleanup owned");
    assert!(matches!(&first, Err(AppError::Timeout(message))
        if message == "application shutdown observation timed out"));
    assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
    assert_eq!(
        Arc::strong_count(&state),
        1,
        "joined cleanup releases its lower owner"
    );
    assert!(matches!(first.and(joined), Err(AppError::Timeout(message))
        if message == "application shutdown observation timed out"));
    app.shutdown(Shutdown::CancelAndDrain {
        timeout: Duration::from_secs(10),
    })
    .await?;
    assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn cancel_cleanup_reobservation_stops_on_retained_joined_failure()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let (faults, store) = StorageFaults::open().await?;
    let (state, agent) = StreamFailureState::open()?;
    let mut app = App::open(AppConfig::new(
        store,
        vec![AgentSpec::new(
            "cleanup-failure".to_owned(),
            "v1".to_owned(),
            agent,
        )],
        Vec::new(),
    ))
    .await?;
    let body = app
        .workspace(format!("appws_{}", uuid::Uuid::new_v4()).parse()?)
        .await
        .map(|_| ());
    state.arm_shutdown();
    let lower_shutdown = state.shutdown_observed.notified();
    tokio::pin!(lower_shutdown);
    lower_shutdown.as_mut().enable();
    let first = app
        .shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_millis(25),
        })
        .await;
    let entered = tokio::time::timeout(Duration::from_secs(5), &mut lower_shutdown).await;
    // Cancellation publication has finished before this lower cleanup gate.
    // Fail only the subsequent durable Closed mutation, through the existing
    // caller Storage boundary; all actual persistence remains Lance-backed.
    faults.fail_transaction_begins(1);
    state.release_shutdown();
    let mut observations = 0;
    let joined = loop {
        let joined = app
            .shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(10),
            })
            .await;
        observations += 1;
        assert!(matches!(&joined, Err(AppError::Integration(message))
            if message == "app state-store operation failed"));
        assert_eq!(
            Arc::strong_count(&state),
            1,
            "failure is delivered after lower joining"
        );
        assert_eq!(
            observations, 1,
            "a joined retained failure must terminate cleanup reobservation",
        );
        // This fresh owner requests only valid CancelAndDrain, so Integration
        // is a retained failure delivered after owned joining.
        if joined.is_ok() || matches!(&joined, Err(AppError::Integration(_))) {
            break joined;
        }
    };

    entered.expect("the original observer times out with lower cleanup still gated");
    assert!(matches!(&first, Err(AppError::Timeout(message))
        if message == "application shutdown observation timed out"));
    assert!(matches!(&body, Err(AppError::NotFound(message))
        if message == "workspace history does not exist"));
    assert_eq!(faults.transaction_begin_failures.load(Ordering::SeqCst), 0);
    assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
    let retained = match &joined {
        Err(AppError::Integration(message)) => message.clone(),
        result => panic!("the joined failure must remain an error: {result:?}"),
    };
    assert!(matches!(
        app.shutdown(Shutdown::CancelAndDrain { timeout: Duration::from_secs(10) }).await,
        Err(AppError::Integration(message)) if message == retained
    ));
    assert!(matches!(
        app.create_workspace(WorkspaceConfig {
            name: Some("closed-after-joined-failure".to_owned()),
            root: std::env::current_dir()?,
        })
        .await,
        Err(AppError::Closed(_))
    ));
    assert!(
        matches!(body.and(first).and(joined), Err(AppError::NotFound(message))
        if message == "workspace history does not exist")
    );
    assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn cancel_escalation_during_finalizing_is_reobserved_before_closed()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let (faults, store) = StorageFaults::open().await?;
    let (state, agent) = StreamFailureState::open()?;
    state.arm_shutdown();
    let mut app = App::open(AppConfig::new(
        store,
        vec![AgentSpec::new(
            "finalizer".to_owned(),
            "v1".to_owned(),
            agent,
        )],
        Vec::new(),
    ))
    .await?;

    let observed = state.shutdown_observed.notified();
    tokio::pin!(observed);
    observed.as_mut().enable();
    let mut draining = Box::pin(app.shutdown(Shutdown::Drain {
        timeout: Duration::from_secs(10),
    }));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = &mut observed => {}
            result = &mut draining => panic!("drain returned before lower shutdown was gated: {result:?}"),
        }
    })
    .await
    .expect("drain must enter Finalizing and reach lower shutdown");
    drop(draining);

    // Escalation publishes before observing the already-owned finalizer. A
    // storage failure here is Integration even though lower cleanup is live.
    faults.fail_transaction_begins(1);
    let rejected_escalation = app
        .shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(10),
        })
        .await;
    let lower_owners_before_release = Arc::strong_count(&state);
    let lower_shutdowns_before_release = state.shutdowns.load(Ordering::SeqCst);
    let admission_failures_remaining = faults.transaction_begin_failures.load(Ordering::SeqCst);

    let mut cancelling = Box::pin(app.shutdown(Shutdown::CancelAndDrain {
        timeout: Duration::from_secs(10),
    }));
    assert!(
        tokio::time::timeout(Duration::from_millis(25), cancelling.as_mut())
            .await
            .is_err(),
        "the escalated observer must reattach while lower shutdown is gated",
    );
    state.release_shutdown();
    cancelling.await?;
    assert!(
        matches!(rejected_escalation, Err(AppError::Integration(message))
        if message == "app state-store operation failed")
    );
    assert!(
        lower_owners_before_release > 1,
        "the failed escalation returns before lower ownership joins",
    );
    assert_eq!(lower_shutdowns_before_release, 1);
    assert_eq!(admission_failures_remaining, 0);
    assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
    assert_eq!(Arc::strong_count(&state), 1);
    assert!(
        faults.cancelling_publications.load(Ordering::SeqCst) > 0,
        "the finalizer must publish the late cancel mode before Closed",
    );
    Ok(())
}

#[tokio::test]
async fn closed_publication_linearized_first_does_not_accept_a_late_cancel()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let (faults, store) = StorageFaults::open().await?;
    let (state, agent) = StreamFailureState::open()?;
    state.arm_shutdown();
    let mut app = App::open(AppConfig::new(
        store,
        vec![AgentSpec::new(
            "preclosed-finalizer".to_owned(),
            "v1".to_owned(),
            agent,
        )],
        Vec::new(),
    ))
    .await?;

    let lower_shutdown = state.shutdown_observed.notified();
    tokio::pin!(lower_shutdown);
    lower_shutdown.as_mut().enable();
    let mut draining = Box::pin(app.shutdown(Shutdown::Drain {
        timeout: Duration::from_secs(10),
    }));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = &mut lower_shutdown => {}
            result = &mut draining => panic!("drain returned before lower shutdown was gated: {result:?}"),
        }
    })
    .await
    .expect("drain must enter Finalizing before the pre-Closed gate");
    drop(draining);

    let preclosed = faults.preclosed_load_observed.notified();
    tokio::pin!(preclosed);
    preclosed.as_mut().enable();
    faults.arm_preclosed_load();
    state.release_shutdown();
    tokio::time::timeout(Duration::from_secs(5), &mut preclosed)
        .await
        .expect("the finalizer must reach the exact durable Closed read fence");

    let mut cancelling = Box::pin(app.shutdown(Shutdown::CancelAndDrain {
        timeout: Duration::from_secs(10),
    }));
    assert!(
        tokio::time::timeout(Duration::from_millis(25), cancelling.as_mut())
            .await
            .is_err(),
        "the late caller must wait behind the already-linearized Closed publication",
    );
    assert_eq!(faults.cancelling_publications.load(Ordering::SeqCst), 0);

    faults.release_preclosed_load();
    cancelling.await?;
    assert_eq!(
        faults.cancelling_publications.load(Ordering::SeqCst),
        0,
        "a close-first ordering must observe Closed without publishing a rejected cancel",
    );
    Ok(())
}

#[tokio::test]
async fn dropping_the_app_wakes_an_existing_run_waiter() -> Result<(), Box<dyn Error + Send + Sync>>
{
    let ExecutionJourney { app, workspace } = ExecutionJourney::open(&[DROP_WAIT]).await?;
    let workflow = workspace.workflow("drop-wait".parse()?).await?;
    let run = workflow.start(ExecutionOptions::default()).await?;
    let mut waiting = Box::pin(run.wait());
    assert!(
        tokio::time::timeout(Duration::from_millis(100), waiting.as_mut())
            .await
            .is_err(),
        "the long sleep must keep the waiter pending before App drop"
    );

    drop(app);
    let outcome = tokio::time::timeout(Duration::from_secs(1), waiting.as_mut())
        .await
        .expect("App drop must wake an already-registered waiter");
    assert!(matches!(outcome, Err(AppError::Closed(_))));
    drop(waiting);
    assert!(matches!(run.snapshot().await?.status, RunStatus::Running));
    Ok(())
}

#[tokio::test]
async fn sleep_truth_load_retries_recoverable_errors_and_closes_on_permanent_failure()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let (faults, store) = StorageFaults::open().await?;
    // Read-fault recovery must reach the later Sleep's admitted timeout path.
    let mut definition: Value = serde_json::from_str(LINEAR)?;
    for node in definition["nodes"].as_array_mut().expect("LINEAR nodes") {
        if node["id"] == "expired" {
            node["deadline_ms"] = Value::from(1_000);
        }
    }
    let mut app = App::open(AppConfig::<FaultStateStore, Agent>::new(
        store,
        Vec::new(),
        vec![WorkflowDefinition::parse(&definition.to_string())?],
    ))
    .await?;
    let mut phase = "workspace-construction";
    let checked = AssertUnwindSafe(async {
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: Some("transient-sleep-load".to_owned()),
                root: std::env::current_dir()?,
            })
            .await?;
        let workflow = workspace.workflow("linear".parse()?).await?;

        let observed = faults.sleep_load_observed.notified();
        tokio::pin!(observed);
        observed.as_mut().enable();
        faults.arm_sleep_load();
        phase = "one-shot-timeout:start";
        let run = workflow
            .start(ExecutionOptions {
                state: WorkflowState::new(Map::from_iter([(
                    "case".to_owned(),
                    Value::from("sleep"),
                )]))?,
                ..ExecutionOptions::default()
            })
            .await?;
        tokio::time::timeout(Duration::from_secs(5), &mut observed)
            .await
            .expect("the driver must consume the armed post-Sleep timeout");
        phase = "one-shot-timeout:wait";
        let terminal = tokio::time::timeout(Duration::from_secs(5), run.wait())
            .await
            .expect("one transient post-Sleep load must not discard the deadline owner")?;
        assert!(faults.sleep_load_faulted.load(Ordering::SeqCst));
        assert!(matches!(
            terminal.status,
            RunStatus::Failed(WorkflowFailure::Timeout(_))
        ));

        let observed = faults.sleep_load_observed.notified();
        tokio::pin!(observed);
        observed.as_mut().enable();
        faults.arm_sleep_integrations(1);
        phase = "one-shot-integration:start";
        let transient = workflow
            .start(ExecutionOptions {
                state: WorkflowState::new(Map::from_iter([(
                    "case".to_owned(),
                    Value::from("sleep"),
                )]))?,
                ..ExecutionOptions::default()
            })
            .await?;
        tokio::time::timeout(Duration::from_secs(5), &mut observed)
            .await
            .expect("the driver must consume the armed post-Sleep Integration");
        phase = "one-shot-integration:wait";
        let terminal = tokio::time::timeout(Duration::from_secs(5), transient.wait())
            .await
            .expect("one transient Integration must not discard the deadline owner")?;
        assert_eq!(faults.sleep_integration_attempts.load(Ordering::SeqCst), 1);
        assert!(matches!(
            terminal.status,
            RunStatus::Failed(WorkflowFailure::Timeout(_))
        ));

        faults.arm_sleep_integrations(12);
        let recovered_attempts = faults.excessive_integration_attempts.notified();
        tokio::pin!(recovered_attempts);
        recovered_attempts.as_mut().enable();
        phase = "twelve-recoverable:start";
        let recovered = workflow
            .start(ExecutionOptions {
                state: WorkflowState::new(Map::from_iter([(
                    "case".to_owned(),
                    Value::from("sleep"),
                )]))?,
                ..ExecutionOptions::default()
            })
            .await
            .expect("the recoverable outage starts only after admission");
        phase = "twelve-recoverable:observed";
        tokio::time::timeout(Duration::from_secs(10), &mut recovered_attempts)
            .await
            .expect(
                "a recoverable state-store outage must remain runtime-owned past the retry budget",
            );
        phase = "twelve-recoverable:wait";
        let terminal = tokio::time::timeout(Duration::from_secs(5), recovered.wait())
            .await
            .expect("the sleeping run must finish after the recoverable outage clears")
            .expect("the cleared outage must not leak through public observation");
        assert_eq!(faults.sleep_integration_attempts.load(Ordering::SeqCst), 12);
        assert!(matches!(
            terminal.status,
            RunStatus::Failed(WorkflowFailure::Timeout(_))
        ));

        faults.arm_sleep_integrations(usize::MAX);
        phase = "permanent:start";
        let persistent = workflow
            .start(ExecutionOptions {
                state: WorkflowState::new(Map::from_iter([(
                    "case".to_owned(),
                    Value::from("sleep"),
                )]))?,
                ..ExecutionOptions::default()
            })
            .await?;
        phase = "permanent:closed";
        // Observe App authority without consuming the selected run-read fault.
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match app
                    .list_workspaces(None, NonZeroUsize::new(1).unwrap())
                    .await
                {
                    Err(AppError::Closed(_)) => break,
                    Ok(_) => tokio::task::yield_now().await,
                    Err(error) => panic!("unexpected permanent sleep-read disposition: {error}"),
                }
            }
        })
        .await
        .expect("a permanently unreadable Sleep checkpoint reaches fatal cleanup");
        let failed_reads = faults.sleep_integration_attempts.load(Ordering::SeqCst);
        assert!(
            failed_reads > 0,
            "the actual admitted Sleep read consumed the fault"
        );
        faults.disarm_sleep_integration();
        assert!(matches!(
            persistent.snapshot().await,
            Err(AppError::Closed(_))
        ));
        phase = "permanent:shutdown";
        let closed = tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        )
        .await
        .expect("fatal Sleep-read cleanup remains owned and joins");
        assert!(matches!(closed, Err(AppError::Integration(_))));
        assert_eq!(
            faults.sleep_integration_attempts.load(Ordering::SeqCst),
            failed_reads,
            "fatal cleanup leaves no autonomous retry owner"
        );
        Ok::<(), Box<dyn Error + Send + Sync>>(())
    })
    .catch_unwind()
    .await;
    if !matches!(&checked, Ok(Ok(()))) {
        eprintln!(
            "Sleep truth diagnostic: phase={phase}; timeout_faulted={}; integration_attempts={}; integration_armed={}; integration_requested={}",
            faults.sleep_load_faulted.load(Ordering::SeqCst),
            faults.sleep_integration_attempts.load(Ordering::SeqCst),
            faults.sleep_integration_armed.load(Ordering::SeqCst),
            faults.sleep_integration_requested.load(Ordering::SeqCst),
        );
        match &checked {
            Ok(Err(error)) => eprintln!("Sleep truth diagnostic: original public error={error:?}"),
            Err(_) => eprintln!("Sleep truth diagnostic: original assertion or observer panic"),
            Ok(Ok(())) => unreachable!(),
        }
        faults.disarm_sleep_integration();
        let cleanup = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        ))
        .catch_unwind()
        .await
        .map_err(|_| "shutdown panicked");
        eprintln!("Sleep truth diagnostic: retained shutdown outcome={cleanup:?}");
    }
    match checked {
        Ok(result) => result,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

#[tokio::test]
async fn storage_retry_counters_reset_only_after_checkpoint_sequence_advances()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let (faults, store) = StorageFaults::open().await?;
    let mut app = App::open(AppConfig::<FaultStateStore, Agent>::new(
        store,
        Vec::new(),
        vec![WorkflowDefinition::parse(LINEAR)?],
    ))
    .await?;
    let workspace = app
        .create_workspace(WorkspaceConfig {
            name: Some("retry-counter-progress".to_owned()),
            root: std::env::current_dir()?,
        })
        .await?;
    let workflow = workspace.workflow("linear".parse()?).await?;

    faults.script_sleep_failures([8, 1]);
    let second_failure = faults.second_sleep_failure_observed.notified();
    let second_recovery = faults.second_sleep_recovery_observed.notified();
    tokio::pin!(second_failure, second_recovery);
    second_failure.as_mut().enable();
    second_recovery.as_mut().enable();
    let retained = workflow
        .start(ExecutionOptions {
            state: WorkflowState::new(Map::from_iter([("case".to_owned(), Value::from("sleep"))]))?,
            ..ExecutionOptions::default()
        })
        .await?;
    tokio::time::timeout(Duration::from_secs(10), &mut second_failure)
        .await
        .expect("the retained retry journey must reach its second checkpoint");
    tokio::time::timeout(Duration::from_millis(750), &mut second_recovery)
        .await
        .expect("progress must reset retained backoff before the second checkpoint retry");
    let terminal = tokio::time::timeout(Duration::from_secs(5), retained.wait())
        .await
        .expect("the retained retry journey must settle after its reset")?;
    assert!(matches!(
        terminal.status,
        RunStatus::Failed(WorkflowFailure::Timeout(_))
    ));
    assert_eq!(faults.scripted_sleep_attempts(), vec![8, 1]);

    app.shutdown(Shutdown::Drain {
        timeout: Duration::from_secs(10),
    })
    .await?;
    Ok(())
}

impl StreamFailureState {
    fn retry_definition(policy: Value) -> WorkflowDefinition {
        let mut source: Value = serde_json::from_str(STREAM_FAILURE).unwrap();
        source["nodes"][0]["retry"] = policy;
        WorkflowDefinition::parse(&source.to_string()).unwrap()
    }

    async fn automatic_retry(
        policy: Value,
        failure: RetryFailure,
        failures: usize,
        expected_attempts: usize,
        minimum_delays: &[u64],
        succeeds: bool,
    ) {
        let (state, agent) = Self::exclusive_workspace(false).unwrap();
        let mut app = App::open(AppConfig::new(
            StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
                .await
                .unwrap(),
            vec![AgentSpec::new("faulty".to_owned(), "v1".to_owned(), agent)],
            vec![Self::retry_definition(policy)],
        ))
        .await
        .unwrap();
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(12), async {
            let workspace = app.create_workspace(WorkspaceConfig {
                name: None, root: std::env::current_dir().unwrap(),
            }).await.unwrap();
            let workflow = workspace.workflow("stream-failure".parse().unwrap()).await.unwrap();
            *state.retry_failures.lock().unwrap() = Some(std::iter::repeat_n(failure, failures).collect());
            let run = workflow.start(ExecutionOptions::default()).await.unwrap();
            let terminal = run.wait().await.unwrap();
            assert!(terminal.interactions.is_empty(), "known pre-admission failure cannot require uncertain-effect resolution: {failure:?}");
            if succeeds {
                assert!(matches!(terminal.status, RunStatus::Succeeded), "finite policy must retry the safe failure: {failure:?}");
                assert_eq!(terminal.state.values()["answer"], "respond");
                assert_eq!(terminal.outcomes.len(), 1);
                assert_eq!(terminal.last_action_ordinal, Some(0));
            } else {
                assert!(matches!(terminal.status, RunStatus::Failed(WorkflowFailure::Agent(_))), "exhausted retry policy has one final failure: {failure:?}");
                assert_eq!(terminal.state.values()["answer"], "");
                assert!(terminal.outcomes.is_empty() && terminal.last_action_ordinal.is_none());
            }
            let observed = state.retry_observed.lock().unwrap().clone();
            assert_eq!(observed.len(), expected_attempts, "the first attempt consumes one slot: {failure:?}");
            assert_eq!(minimum_delays.len(), expected_attempts - 1);
            for (pair, delay) in observed.windows(2).zip(minimum_delays) {
                // Native retry wake times use integral wall-clock milliseconds.
                // Permit only that sub-millisecond quantization, not early backoff.
                assert!(pair[1].duration_since(pair[0]) + Duration::from_millis(1) >= Duration::from_millis(*delay),
                    "the original fixed/capped exponential delay must precede the next lower call");
            }
            assert_eq!(state.prompts.load(Ordering::SeqCst), if matches!(failure, RetryFailure::Workspace) { usize::from(succeeds) } else { expected_attempts });
            assert_eq!(state.conversations.load(Ordering::SeqCst), usize::from(succeeds || !matches!(failure, RetryFailure::Workspace)),
                "retries preserve the original conversation identity once one exists");
        })).catch_unwind().await;
        state.retry_failures.lock().unwrap().take();
        let cleanup = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        ))
        .catch_unwind()
        .await;
        drop(app);
        match checked {
            Ok(result) => {
                result.expect("automatic retry case stays within the original node deadline")
            }
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup
            .expect("automatic retry cleanup does not panic")
            .expect("automatic retry cleanup joins")
            .unwrap();
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
        assert!(!state.workspace_claimed.load(Ordering::SeqCst));
    }

    async fn manual_retry_budget(
        policy: Value,
        permitted: usize,
        elapsed_before_retry: Duration,
        minimum_delay: u64,
    ) {
        let (state, agent) = Self::exclusive_workspace(false).unwrap();
        let mut app = App::open(AppConfig::new(
            StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
                .await
                .unwrap(),
            vec![AgentSpec::new("faulty".to_owned(), "v1".to_owned(), agent)],
            vec![Self::retry_definition(policy)],
        ))
        .await
        .unwrap();
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(12), async {
            let workspace = app.create_workspace(WorkspaceConfig { name: None, root: std::env::current_dir().unwrap() }).await.unwrap();
            let workflow = workspace.workflow("stream-failure".parse().unwrap()).await.unwrap();
            *state.retry_failures.lock().unwrap() = Some(std::iter::repeat_n(RetryFailure::Uncertain, permitted + 2).collect());
            let run = workflow.start(ExecutionOptions::default()).await.unwrap();
            let mut previous_target = None;
            for index in 0..=permitted {
                let unknown = loop {
                    let snapshot = run.snapshot().await.unwrap();
                    if matches!(snapshot.interactions.as_slice(), [Interaction::Unknown { sequence, generation, retry_safe: true, .. }]
                        if previous_target != Some((*sequence, *generation))) { break snapshot; }
                    assert!(matches!(snapshot.status, RunStatus::Running), "uncertain accepted work remains caller-owned");
                    tokio::task::yield_now().await;
                };
                let [Interaction::Unknown { sequence, generation, retry_safe: true, .. }] = unknown.interactions.as_slice() else { unreachable!() };
                let target = (*sequence, *generation);
                assert_eq!(state.prompts.load(Ordering::SeqCst), index + 1);
                assert_eq!(state.aborts.load(Ordering::SeqCst), index + 1,
                    "manual retry needs completed native abort evidence for every attempt");
                assert_eq!(state.conversations.load(Ordering::SeqCst), 1);
                assert_eq!(unknown.state.values()["answer"], "");
                assert!(unknown.outcomes.is_empty() && unknown.last_action_ordinal.is_none());
                if index < permitted {
                    let resolved_at = std::time::Instant::now();
                    run.resolve(target.0, target.1, Resolution::Retry).await.unwrap();
                    loop {
                        if state.prompts.load(Ordering::SeqCst) == index + 2 { break; }
                        tokio::task::yield_now().await;
                    }
                    let observed = state.retry_observed.lock().unwrap()[index + 1];
                    assert!(observed.duration_since(resolved_at) + Duration::from_millis(1) >= Duration::from_millis(minimum_delay),
                        "manual Retry uses the declared backoff before the next prompt");
                    previous_target = Some(target);
                } else {
                    tokio::time::sleep(elapsed_before_retry).await;
                    assert!(matches!(run.resolve(target.0, target.1, Resolution::Retry).await, Err(AppError::Conflict(_))),
                        "safe abort evidence cannot bypass Never, max_attempts or max_elapsed");
                    let retained = run.snapshot().await.unwrap();
                    assert_eq!(retained.checkpoint, unknown.checkpoint, "rejected Retry publishes no new checkpoint");
                    Self::assert_same_interactions(&retained.interactions, &unknown.interactions);
                    assert_eq!(retained.state.values(), unknown.state.values());
                    assert_eq!(state.prompts.load(Ordering::SeqCst), index + 1);
                    assert_eq!(state.aborts.load(Ordering::SeqCst), index + 1);
                    run.resolve(target.0, target.1, Resolution::Fail).await.unwrap();
                    let terminal = run.wait().await.unwrap();
                    assert!(matches!(terminal.status, RunStatus::Failed(WorkflowFailure::Agent(_))));
                    assert_eq!(terminal.state.values()["answer"], "");
                    assert!(terminal.outcomes.is_empty() && terminal.last_action_ordinal.is_none());
                }
            }
        })).catch_unwind().await;
        state.retry_failures.lock().unwrap().take();
        let cleanup = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        ))
        .catch_unwind()
        .await;
        drop(app);
        match checked {
            Ok(result) => {
                result.expect("manual retry case stays within the original node deadline")
            }
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup
            .expect("manual retry cleanup does not panic")
            .expect("manual retry cleanup joins")
            .unwrap();
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
        assert!(!state.workspace_claimed.load(Ordering::SeqCst));
    }
}

#[tokio::test]
async fn retry_never_rejects_manual_retry_after_proven_abort() {
    StreamFailureState::manual_retry_budget(
        serde_json::json!({"kind":"never"}),
        0,
        Duration::ZERO,
        0,
    )
    .await;
}

#[tokio::test]
async fn retry_immediate_counts_manual_attempts_and_preserves_target_generation() {
    StreamFailureState::manual_retry_budget(
        serde_json::json!({"kind":"immediate","max_attempts":2}),
        1,
        Duration::ZERO,
        0,
    )
    .await;
}

#[tokio::test]
async fn retry_fixed_manual_retry_respects_backoff_and_attempt_limit() {
    StreamFailureState::manual_retry_budget(
        serde_json::json!({"kind":"fixed","max_attempts":2,"max_elapsed_ms":4000,"delay_ms":100}),
        1,
        Duration::ZERO,
        100,
    )
    .await;
}

#[tokio::test]
async fn retry_exponential_manual_retry_cannot_exceed_elapsed_budget() {
    StreamFailureState::manual_retry_budget(serde_json::json!({"kind":"exponential","max_attempts":3,"max_elapsed_ms":100,"initial_ms":10,"maximum_ms":40,"multiplier":2}), 0, Duration::from_millis(150), 0).await;
}

#[tokio::test]
async fn retry_never_settles_safe_pre_admission_failures_once() {
    let failure = RetryFailure::Workspace;
    StreamFailureState::automatic_retry(
        serde_json::json!({"kind":"never"}),
        failure,
        2,
        1,
        &[],
        false,
    )
    .await;
}

#[tokio::test]
async fn retry_immediate_counts_first_attempt_and_stops_at_maximum() {
    let failure = RetryFailure::Workspace;
    StreamFailureState::automatic_retry(
        serde_json::json!({"kind":"immediate","max_attempts":1}),
        failure,
        1,
        1,
        &[],
        false,
    )
    .await;
    StreamFailureState::automatic_retry(
        serde_json::json!({"kind":"immediate","max_attempts":2}),
        failure,
        1,
        2,
        &[0],
        true,
    )
    .await;
    StreamFailureState::automatic_retry(
        serde_json::json!({"kind":"immediate","max_attempts":2}),
        failure,
        2,
        2,
        &[0],
        false,
    )
    .await;
}

#[tokio::test]
async fn retry_fixed_observes_delay_and_includes_it_in_elapsed_budget() {
    let failure = RetryFailure::Workspace;
    StreamFailureState::automatic_retry(
        serde_json::json!({"kind":"fixed","max_attempts":3,"max_elapsed_ms":4000,"delay_ms":30}),
        failure,
        2,
        3,
        &[30, 30],
        true,
    )
    .await;
    StreamFailureState::automatic_retry(
        serde_json::json!({"kind":"fixed","max_attempts":3,"max_elapsed_ms":20,"delay_ms":30}),
        failure,
        1,
        1,
        &[],
        false,
    )
    .await;
}

#[tokio::test]
async fn retry_exponential_caps_saturating_delay_without_jitter() {
    let failure = RetryFailure::Workspace;
    StreamFailureState::automatic_retry(serde_json::json!({"kind":"exponential","max_attempts":5,"max_elapsed_ms":4000,"initial_ms":10,"maximum_ms":20,"multiplier":u32::MAX}), failure, 4, 5, &[10,20,20,20], true).await;
}

impl StreamFailureState {
    async fn accepted_failure_is_not_retried(
        failure: RetryFailure,
        streaming: bool,
        manual_retry: bool,
    ) {
        let (state, agent) = Self::exclusive_workspace(false).unwrap();
        if matches!(
            failure,
            RetryFailure::AfterEffect(_) | RetryFailure::StreamOpeningAfterEffect(_)
        ) {
            *state.side_effects.lock().unwrap() = Some(0);
        }
        let mut definition: Value = serde_json::from_str(STREAM_FAILURE).unwrap();
        definition["nodes"][0]["retry"] = serde_json::json!({"kind":"immediate","max_attempts":3});
        definition["nodes"][0]["step"]["stream"] = Value::from(streaming);
        let mut app = App::open(AppConfig::new(
            StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
                .await
                .unwrap(),
            vec![AgentSpec::new("faulty".to_owned(), "v1".to_owned(), agent)],
            vec![WorkflowDefinition::parse(&definition.to_string()).unwrap()],
        ))
        .await
        .unwrap();
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(12), async {
            let workspace = app.create_workspace(WorkspaceConfig { name: None, root: std::env::current_dir().unwrap() }).await.unwrap();
            let workflow = workspace.workflow("stream-failure".parse().unwrap()).await.unwrap();
            *state.retry_failures.lock().unwrap() = Some(if matches!(failure, RetryFailure::StreamOpeningAfterEffect(_)) {
                VecDeque::from([failure, failure])
            } else {
                VecDeque::from([failure])
            });
            let run = workflow.start(ExecutionOptions::default()).await.unwrap();
            if !streaming && matches!(failure, RetryFailure::Cancelled | RetryFailure::Closed | RetryFailure::NotFound) {
                let terminal = run.wait().await.unwrap();
                assert!(matches!(terminal.status, RunStatus::Failed(WorkflowFailure::Agent(_))),
                    "existing ordinary terminal errors remain terminal, never policy-safe: {failure:?}");
                assert!(terminal.interactions.is_empty());
                assert_eq!(terminal.state.values()["answer"], "");
                assert!(terminal.outcomes.is_empty() && terminal.last_action_ordinal.is_none());
                assert_eq!(state.prompts.load(Ordering::SeqCst), 1);
                assert_eq!(state.conversations.load(Ordering::SeqCst), 1);
                return;
            }
            let unknown = loop {
                let snapshot = run.snapshot().await.unwrap();
                if matches!(failure, RetryFailure::AfterEffect(_) | RetryFailure::StreamOpeningAfterEffect(_)) {
                    let effects = state.side_effects.lock().unwrap().unwrap();
                    assert!(effects <= 1,
                        "automatic retry replayed an accepted side effect {effects} times: {failure:?}");
                }
                if matches!(snapshot.interactions.as_slice(), [Interaction::Unknown { retry_safe: true, .. }]) { break snapshot; }
                assert!(matches!(snapshot.status, RunStatus::Running), "accepted error must not establish unsubmitted terminal evidence: {failure:?}");
                tokio::task::yield_now().await;
            };
            assert_eq!(state.prompts.load(Ordering::SeqCst), 1, "accepted or uncertain effects never retry automatically: {failure:?}");
            assert_eq!(state.conversations.load(Ordering::SeqCst), 1);
            if matches!(failure, RetryFailure::AfterEffect(_) | RetryFailure::StreamOpeningAfterEffect(_)) {
                assert_eq!(*state.side_effects.lock().unwrap(), Some(1), "accepted side effect cannot be replayed from an error category");
            }
            assert_eq!(state.aborts.load(Ordering::SeqCst), 1);
            assert_eq!(unknown.state.values()["answer"], "");
            assert!(unknown.outcomes.is_empty() && unknown.last_action_ordinal.is_none());
            let [Interaction::Unknown { sequence, generation, .. }] = unknown.interactions.as_slice() else { unreachable!() };
            if manual_retry {
                run.resolve(*sequence, *generation, Resolution::Retry).await.unwrap();
                assert!(matches!(run.wait().await.unwrap().status, RunStatus::Succeeded));
                assert_eq!(state.prompts.load(Ordering::SeqCst), 2);
                assert_eq!(*state.side_effects.lock().unwrap(), Some(2), "only the explicit caller resolution admits another effect");
            } else {
                run.resolve(*sequence, *generation, Resolution::Fail).await.unwrap();
                assert!(matches!(run.wait().await.unwrap().status, RunStatus::Failed(WorkflowFailure::Agent(_))));
                assert_eq!(state.prompts.load(Ordering::SeqCst), 1);
            }
        })).catch_unwind().await;
        state.retry_failures.lock().unwrap().take();
        let cleanup = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        ))
        .catch_unwind()
        .await;
        drop(app);
        match checked {
            Ok(result) => result.expect("accepted failure reaches finite caller-owned uncertainty"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup
            .expect("accepted failure cleanup does not panic")
            .expect("accepted failure cleanup joins")
            .unwrap();
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
    }

    async fn reopened_retry(expired: bool, cancelled: bool) {
        let (faults, store) = StorageFaults::open().await.unwrap();
        let native = store.inner.clone();
        let (state, agent) = Self::exclusive_workspace(false).unwrap();
        let maximum_elapsed = if expired { 700 } else { 4000 };
        let delay = if expired { 500 } else { 1500 };
        let policy = serde_json::json!({"kind":"fixed","max_attempts":2,"max_elapsed_ms":maximum_elapsed,"delay_ms":delay});
        let mut app = Some(
            App::open(AppConfig::new(
                store,
                vec![AgentSpec::new("faulty".to_owned(), "v1".to_owned(), agent)],
                vec![Self::retry_definition(policy.clone())],
            ))
            .await
            .unwrap(),
        );
        let (read_drop, dropped) = tokio::sync::oneshot::channel();
        *faults.attempt_read_dropped.lock().unwrap() = Some(read_drop);
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(12), async {
            let workspace = app.as_ref().unwrap().create_workspace(WorkspaceConfig { name: None, root: std::env::current_dir().unwrap() }).await.unwrap();
            let workspace_id = workspace.id().clone();
            let workflow = workspace.workflow("stream-failure".parse().unwrap()).await.unwrap();
            *state.retry_failures.lock().unwrap() = Some(VecDeque::new());
            *faults.preparation.lock().unwrap() = StatePreparationFault::FailAfterIntent;
            *faults.attempt_read.lock().unwrap() = Some(AttemptReadCase::BeforeWake { held: true });
            let run = workflow.start(ExecutionOptions::default()).await.unwrap();
            let run_id = run.id().clone();
            faults.preparation_entered.notified().await;
            faults.run_transition_observed.notified().await;
            faults.release_run_transition();
            faults.attempt_read_entered.notified().await;
            let (head, checkpoint) = faults.attempt_read_records.lock().unwrap().clone().unwrap();
            let controls = checkpoint.value["checkpoint"]["controls"].as_object().unwrap();
            assert_eq!(controls.len(), 1);
            let control = controls.values().next().unwrap();
            assert_eq!(control["state"]["phase"]["state"], "retry");
            assert_eq!(control["state"]["number"], 2);
            let wake = control["state"]["phase"]["wake_millis"].as_u64().unwrap();
            let deadline = control["deadline_millis"].as_u64().unwrap();
            let elapsed_limit = deadline.checked_sub(5000).unwrap() + maximum_elapsed;
            assert!(wake <= elapsed_limit && elapsed_limit <= deadline);
            let captured_at = std::time::Instant::now();
            let captured_wall = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();
            let remaining_backoff = u128::from(wake).saturating_sub(captured_wall);
            assert!(remaining_backoff > 0, "the real retry is held before its original wake");
            assert_eq!(faults.agent_intents.load(Ordering::SeqCst), 1);
            assert!(state.retry_observed.lock().unwrap().is_empty());
            assert_eq!(state.conversations.load(Ordering::SeqCst), 0);
            assert_eq!(state.prompts.load(Ordering::SeqCst), 0);
            drop(run);
            drop(workflow);
            drop(workspace);
            drop(app.take());
            assert!(dropped.await.is_err(), "the original parked read is dropped before reopen");
            while Arc::strong_count(&state) > 1 { tokio::task::yield_now().await; }
            assert_eq!(native.load(checkpoint.key.clone()).await.unwrap().unwrap().record.value, checkpoint.value);
            // Spend a meaningful part of the real persisted backoff with no old
            // runtime alive. The success case distinguishes remaining delay from
            // restarting a whole delay without requiring sub-millisecond timing.
            let reopen_at = if expired { elapsed_limit + 25 } else { wake - 500 };
            let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();
            let remaining = u128::from(reopen_at).saturating_sub(now);
            tokio::time::sleep(Duration::from_millis(u64::try_from(remaining).unwrap())).await;
            let before_open = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();
            assert!(before_open < u128::from(deadline), "the original whole-node deadline remains open");
            if !expired { assert!(before_open < u128::from(wake), "reopen begins while original backoff remains"); }
            app = Some(App::open(AppConfig::new(
                FaultStateStore { inner: native.clone(), faults: Arc::clone(&faults) },
                vec![AgentSpec::new("faulty".to_owned(), "v1".to_owned(), state.agent())],
                vec![Self::retry_definition(policy)],
            )).await.unwrap());
            let workspace = app.as_ref().unwrap().workspace(workspace_id).await.unwrap();
            let workflow = workspace.workflow("stream-failure".parse().unwrap()).await.unwrap();
            let run = workflow.open_run(RunSelector::Latest(run_id)).await.unwrap();
            if !expired {
                let current = native.load(head.key.clone()).await.unwrap().unwrap().record;
                assert_eq!(current.value["sequence"], checkpoint.value["checkpoint"]["sequence"],
                    "the original persisted retry remains selected during its remaining backoff");
                assert_eq!(native.load(checkpoint.key.clone()).await.unwrap().unwrap().record.value, checkpoint.value);
            }
            if cancelled {
                let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();
                assert!(now < u128::from(wake), "cancellation is admitted during the original backoff");
                let snapshot = run.snapshot().await.unwrap();
                run.cancel(snapshot.checkpoint).await.unwrap();
            }
            let terminal = if !expired && !cancelled {
                let before_wait = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();
                let budget = u64::try_from((u128::from(wake) + 500).saturating_sub(before_wait)).unwrap();
                assert!(budget < delay, "the witness distinguishes remaining from restarted full backoff");
                tokio::time::timeout(Duration::from_millis(budget), run.wait()).await
                    .expect("reopen waits only the original remaining delay, with 500ms observation allowance").unwrap()
            } else { run.wait().await.unwrap() };
            assert!(terminal.interactions.is_empty());
            if cancelled {
                assert!(matches!(terminal.status, RunStatus::Cancelled));
            } else if expired {
                assert!(matches!(terminal.status, RunStatus::Failed(WorkflowFailure::Agent(_))),
                    "elapsed retry budget survives reopen even while the whole-node deadline remains open");
            } else {
                assert!(matches!(terminal.status, RunStatus::Succeeded));
                assert_eq!(terminal.state.values()["answer"], "respond");
                assert_eq!(terminal.outcomes.len(), 1);
                assert_eq!(terminal.last_action_ordinal, Some(0));
                let observed = state.retry_observed.lock().unwrap().clone();
                assert_eq!(observed.len(), 1);
                assert!(observed[0].duration_since(captured_at) + Duration::from_millis(1) >= Duration::from_millis(u64::try_from(remaining_backoff).unwrap()));
            }
            assert_eq!(state.prompts.load(Ordering::SeqCst), usize::from(!expired && !cancelled));
            assert_eq!(state.conversations.load(Ordering::SeqCst), usize::from(!expired && !cancelled));
            if expired || cancelled {
                assert!(state.retry_observed.lock().unwrap().is_empty());
                assert_eq!(faults.agent_intents.load(Ordering::SeqCst), 1);
                assert_eq!(terminal.state.values()["answer"], "");
                assert!(terminal.outcomes.is_empty() && terminal.last_action_ordinal.is_none());
            }
        })).catch_unwind().await;
        state.retry_failures.lock().unwrap().take();
        *faults.attempt_read.lock().unwrap() = None;
        faults.attempt_read_dropped.lock().unwrap().take();
        faults.attempt_read_release.notify_one();
        faults.release_run_transition();
        let cleanup = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), async {
            if let Some(app) = app.as_mut() {
                app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(3),
                })
                .await
            } else {
                Ok(())
            }
        }))
        .catch_unwind()
        .await;
        drop(app);
        match checked {
            Ok(result) => {
                result.expect("reopened retry remains within the original finite node deadline")
            }
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup
            .expect("reopened retry cleanup does not panic")
            .expect("reopened retry cleanup joins")
            .unwrap();
        assert!(!state.workspace_claimed.load(Ordering::SeqCst));
    }
}

#[tokio::test]
async fn retry_policy_never_repeats_uncertain_prompt_errors() {
    for failure in [
        RetryFailure::Cancelled,
        RetryFailure::Closed,
        RetryFailure::NotFound,
        RetryFailure::Uncertain,
        RetryFailure::Conflict,
        RetryFailure::Timeout,
        RetryFailure::Protocol,
        RetryFailure::InvalidState,
    ] {
        StreamFailureState::accepted_failure_is_not_retried(failure, false, false).await;
    }
}

#[tokio::test]
async fn retry_policy_never_repeats_errors_from_an_accepted_stream() {
    for failure in [
        RetryFailure::Busy,
        RetryFailure::InvalidConfig,
        RetryFailure::InvalidInput,
        RetryFailure::Unsupported,
        RetryFailure::NotYetImplemented,
        RetryFailure::Cancelled,
        RetryFailure::Closed,
        RetryFailure::NotFound,
        RetryFailure::Uncertain,
        RetryFailure::Conflict,
        RetryFailure::Timeout,
        RetryFailure::Protocol,
        RetryFailure::InvalidState,
    ] {
        StreamFailureState::accepted_failure_is_not_retried(failure, true, false).await;
    }
}

#[tokio::test]
async fn retry_fixed_reopen_preserves_original_remaining_backoff() {
    StreamFailureState::reopened_retry(false, false).await;
}

#[tokio::test]
async fn retry_fixed_reopen_cannot_dispatch_after_elapsed_budget() {
    StreamFailureState::reopened_retry(true, false).await;
}

#[tokio::test]
async fn retry_fixed_reopen_cancellation_during_backoff_prevents_dispatch() {
    StreamFailureState::reopened_retry(false, true).await;
}

#[tokio::test]
async fn failed_stream_acquisition_recovers_the_persisted_conversation_identity()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let (state, agent) = StreamFailureState::open()?;
    let config: AppConfig<StateStore, StreamFailureAgent> = AppConfig::new(
        StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("caller memory store"),
        vec![AgentSpec::new("faulty".to_owned(), "v1".to_owned(), agent)],
        vec![WorkflowDefinition::parse(STREAM_FAILURE)?],
    );
    let mut app = App::open(config).await?;
    let workspace = app
        .create_workspace(WorkspaceConfig {
            name: Some("stream-failure".to_owned()),
            root: std::env::current_dir()?,
        })
        .await?;
    let workflow = workspace.workflow("stream-failure".parse()?).await?;
    let run = workflow.start(ExecutionOptions::default()).await?;

    let recovered = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = run.snapshot().await?;
            if matches!(
                snapshot.interactions.as_slice(),
                [Interaction::Unknown {
                    retry_safe: true,
                    ..
                }]
            ) {
                return Ok::<_, AppError>(snapshot);
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("stream acquisition failure must converge through exact abort recovery")?;
    assert_eq!(state.conversations.load(Ordering::SeqCst), 1);
    assert_eq!(state.streams.load(Ordering::SeqCst), 1);
    assert_eq!(state.prompts.load(Ordering::SeqCst), 0);
    assert_eq!(state.reopens.load(Ordering::SeqCst), 1);
    assert_eq!(state.aborts.load(Ordering::SeqCst), 1);

    let [
        Interaction::Unknown {
            sequence,
            generation,
            ..
        },
    ] = recovered.interactions.as_slice()
    else {
        panic!("recovered attempt must remain caller-settled Unknown")
    };
    run.resolve(*sequence, *generation, Resolution::Fail)
        .await?;
    let terminal = ExecutionJourney::wait(&run).await?;
    assert!(matches!(
        terminal.status,
        RunStatus::Failed(WorkflowFailure::Agent(_))
    ));
    app.shutdown(Shutdown::Drain {
        timeout: Duration::from_secs(10),
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn concurrent_preparation_same_agent_runs_both_succeed()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_queued_preparation(QueuedPreparation::Complete).await
}

#[tokio::test]
async fn concurrent_preparation_queued_cancellation_prevents_late_effects()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_queued_preparation(QueuedPreparation::Cancel).await
}

#[tokio::test]
async fn concurrent_preparation_queued_deadline_prevents_late_effects()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_queued_preparation(QueuedPreparation::Expire).await
}

#[tokio::test]
async fn concurrent_preparation_different_agent_keys_progress_independently()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let (first_state, first_agent) = StreamFailureState::exclusive_workspace(true)?;
    let (second_state, second_agent) = StreamFailureState::exclusive_workspace(false)?;
    let store =
        StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    let mut app = App::open(AppConfig::new(
        store,
        vec![
            AgentSpec::new("first".to_owned(), "v1".to_owned(), first_agent),
            AgentSpec::new("second".to_owned(), "v1".to_owned(), second_agent),
        ],
        vec![
            StreamFailureState::preparation_definition("first", "lease-first", 5000)?,
            StreamFailureState::preparation_definition("second", "lease-second", 5000)?,
        ],
    ))
    .await?;
    let workspace = app
        .create_workspace(WorkspaceConfig {
            name: Some("independent-preparation".to_owned()),
            root: std::env::current_dir()?,
        })
        .await?;
    let first = workspace
        .workflow("lease-first".parse()?)
        .await?
        .start(ExecutionOptions::default())
        .await?;
    tokio::time::timeout(
        Duration::from_secs(2),
        first_state.preparation_entered.notified(),
    )
    .await
    .expect("first Agent setup must hold its own Workspace lease");
    let second = workspace
        .workflow("lease-second".parse()?)
        .await?
        .start(ExecutionOptions::default())
        .await?;
    let second_done = tokio::time::timeout(Duration::from_secs(2), second.wait())
        .await
        .expect("a different Agent key must complete before the first setup lease is released")?;
    assert!(matches!(second_done.status, RunStatus::Succeeded));
    assert_eq!(second_done.state.values()["answer"], Value::from("respond"));
    assert_eq!(first_state.conversations.load(Ordering::SeqCst), 0);
    assert_eq!(first_state.prompts.load(Ordering::SeqCst), 0);
    assert_eq!(second_state.conversations.load(Ordering::SeqCst), 1);
    assert_eq!(second_state.prompts.load(Ordering::SeqCst), 1);
    first_state.preparation_release.notify_one();
    let first_done = tokio::time::timeout(Duration::from_secs(2), first.wait()).await??;
    assert!(matches!(first_done.status, RunStatus::Succeeded));
    assert_eq!(first_done.state.values()["answer"], Value::from("respond"));
    app.shutdown(Shutdown::Drain {
        timeout: Duration::from_secs(5),
    })
    .await?;
    for state in [first_state, second_state] {
        assert_eq!(state.conversations.load(Ordering::SeqCst), 1);
        assert_eq!(state.prompts.load(Ordering::SeqCst), 1);
        assert_eq!(
            state
                .conversation_inventory
                .lock()
                .expect("test conversation inventory lock")
                .len(),
            1
        );
        assert!(!state.workspace_claimed.load(Ordering::SeqCst));
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[tokio::test]
async fn activation_deadline_covers_new_conversation() -> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_preparation_settles(PreparationFault::Stall).await
}

#[tokio::test]
async fn panic_during_conversation_creation_does_not_orphan_run()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_preparation_settles(PreparationFault::Panic).await
}

#[tokio::test]
async fn activation_deadline_covers_get_workspace() -> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_preparation_settles(PreparationFault::WorkspaceStall).await
}

#[tokio::test]
async fn panic_during_workspace_lookup_does_not_orphan_run()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_preparation_settles(PreparationFault::WorkspacePanic).await
}

#[tokio::test]
async fn activation_deadline_covers_get_conversation_recovery()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_preparation_settles(PreparationFault::RecoveryStall).await
}

#[tokio::test]
async fn panic_during_conversation_recovery_does_not_orphan_run()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_preparation_settles(PreparationFault::RecoveryPanic).await
}

#[tokio::test]
async fn activation_deadline_covers_abort_cleanup() -> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_preparation_settles(PreparationFault::AbortStall).await
}

#[tokio::test]
async fn panic_during_abort_cleanup_does_not_orphan_run() -> Result<(), Box<dyn Error + Send + Sync>>
{
    StreamFailureState::assert_preparation_settles(PreparationFault::AbortPanic).await
}

#[tokio::test]
async fn two_notifications_same_prompt_settle_distinct_targets()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_notifications(2, false).await
}

#[tokio::test]
async fn notification_redelivery_and_resolution_replay_preserve_exact_targets()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_notifications(3, true).await
}

#[tokio::test]
async fn panicking_agent_shutdown_preserves_remaining_cleanup_and_result()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let (first, first_agent) = StreamFailureState::open()?;
    let (second, second_agent) = StreamFailureState::open()?;
    first.shutdown_panics.store(true, Ordering::SeqCst);
    let store =
        StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    let mut app = App::open(AppConfig::new(
        store,
        vec![
            AgentSpec::new("a-panics".to_owned(), "v1".to_owned(), first_agent),
            AgentSpec::new("b-cleans".to_owned(), "v1".to_owned(), second_agent),
        ],
        Vec::new(),
    ))
    .await?;
    let outcome = app
        .shutdown(Shutdown::Drain {
            timeout: Duration::from_secs(1),
        })
        .await;
    assert!(
        matches!(outcome, Err(AppError::Integration(_))),
        "lower cleanup panic must become the retained primary shutdown failure: {outcome:?}"
    );
    assert_eq!(first.shutdowns.load(Ordering::SeqCst), 1);
    assert_eq!(
        second.shutdowns.load(Ordering::SeqCst),
        1,
        "remaining lower owners must still be shut down before terminal publication"
    );
    let repeated = app
        .shutdown(Shutdown::Drain {
            timeout: Duration::from_secs(1),
        })
        .await;
    assert!(matches!(repeated, Err(AppError::Integration(_))));
    assert_eq!(second.shutdowns.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn dropping_idle_app_completes_owned_lower_cleanup()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let (state, agent) = StreamFailureState::open()?;
    state.arm_shutdown();
    let store =
        StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    let app = App::open(AppConfig::new(
        store,
        vec![AgentSpec::new(
            "drop-owner".to_owned(),
            "v1".to_owned(),
            agent,
        )],
        Vec::new(),
    ))
    .await?;
    drop(app);
    tokio::time::timeout(Duration::from_secs(1), state.shutdown_observed.notified())
        .await
        .expect("root drop must retain cleanup ownership until lower shutdown is observed");
    state.release_shutdown();
    tokio::time::timeout(Duration::from_secs(1), async {
        while Arc::strong_count(&state) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the tracked drop finalizer must release its final lower owner");
    assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn panic_after_durable_run_admission_does_not_orphan_accepted_work()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let (faults, store) = StorageFaults::open().await?;
    let mut app = App::open(AppConfig::<FaultStateStore, Agent>::new(
        store,
        Vec::new(),
        vec![WorkflowDefinition::parse(DROP_WAIT)?],
    ))
    .await?;
    let workspace = app
        .create_workspace(WorkspaceConfig {
            name: None,
            root: std::env::current_dir()?,
        })
        .await?;
    let workflow = workspace.workflow("drop-wait".parse()?).await?;
    faults.admission_panics.store(true, Ordering::SeqCst);
    let result = workflow.start(ExecutionOptions::default()).await;
    assert!(matches!(
        result,
        Err(AppError::Closed(_) | AppError::Integration(_))
    ));
    tokio::time::timeout(
        Duration::from_secs(1),
        faults.admission_committed.notified(),
    )
    .await
    .expect("the panic must occur after the real durable admission commit");
    let settled = tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            let runs = workflow
                .list_runs(None, NonZeroUsize::new(10).expect("nonzero limit"))
                .await?;
            assert_eq!(
                runs.len(),
                1,
                "the single accepted run must remain discoverable"
            );
            if !matches!(runs[0].status, RunStatus::Running) {
                return Ok::<_, AppError>(runs.into_iter().next().expect("one run"));
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the admitted task's panic must be supervised without another public drive request")?;
    assert!(matches!(
        settled.status,
        RunStatus::Failed(WorkflowFailure::Execution(_))
    ));
    app.shutdown(Shutdown::Drain {
        timeout: Duration::from_secs(1),
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn cleanup_panic_preserves_prior_publication_failure()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let (faults, store) = StorageFaults::open().await?;
    let (state, agent) = StreamFailureState::open()?;
    state.shutdown_panics.store(true, Ordering::SeqCst);
    let mut app = App::open(AppConfig::new(
        store,
        vec![AgentSpec::new("cleanup".to_owned(), "v1".to_owned(), agent)],
        Vec::new(),
    ))
    .await?;
    faults.fail_transaction_begins(1);
    let outcome = app
        .shutdown(Shutdown::Drain {
            timeout: Duration::from_secs(1),
        })
        .await;
    assert!(
        matches!(outcome, Err(AppError::Integration(ref message)) if message == "app state-store operation failed"),
        "the earlier publication error must remain primary when lower cleanup panics: {outcome:?}"
    );
    assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn repeated_publication_panics_bound_recovery_and_complete_lower_cleanup()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let (faults, store) = StorageFaults::open().await?;
    let (state, agent) = StreamFailureState::open()?;
    let mut app = App::open(AppConfig::new(
        store,
        vec![AgentSpec::new("cleanup".to_owned(), "v1".to_owned(), agent)],
        Vec::new(),
    ))
    .await?;
    faults.app_load_panics.store(16, Ordering::SeqCst);
    let outcome = app
        .shutdown(Shutdown::Drain {
            timeout: Duration::from_secs(1),
        })
        .await;
    assert!(
        matches!(outcome, Err(AppError::Integration(_))),
        "panic recovery must retain a terminal failure: {outcome:?}"
    );
    assert_eq!(
        state.shutdowns.load(Ordering::SeqCst),
        1,
        "an unusable durable store cannot prevent owned lower cleanup"
    );
    assert!(
        faults.app_load_panic_count.load(Ordering::SeqCst) <= 2,
        "each durable finalization publication phase may fail once; recovery must not re-enter a panicking phase"
    );
    Ok(())
}

#[tokio::test]
async fn cancelling_open_during_inventory_does_not_leak_idle_supervisor()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let (faults, store) = StorageFaults::open().await?;
    let handle = tokio::runtime::Handle::current();
    let baseline = handle.metrics().num_alive_tasks();
    faults
        .admission_inventory_armed
        .store(true, Ordering::SeqCst);
    let mut opening = Box::pin(App::open(AppConfig::<FaultStateStore, Agent>::new(
        store,
        Vec::new(),
        Vec::new(),
    )));
    tokio::time::timeout(Duration::from_secs(1), async {
        tokio::select! {
            _ = faults.admission_inventory_observed.notified() => {}
            result = opening.as_mut() => panic!("opening must reach the post-construction inventory gate before returning: {}", result.is_ok()),
        }
    }).await.expect("opening reaches its exact inventory await");
    assert!(
        handle.metrics().num_alive_tasks() > baseline,
        "the accepted runtime must already own a supervisor at this gate"
    );
    drop(opening);
    tokio::time::timeout(Duration::from_secs(1), async {
        while handle.metrics().num_alive_tasks() > baseline {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelling open must terminate its newly owned idle task on the still-driven runtime");
    Ok(())
}

#[tokio::test]
async fn root_drop_during_rejected_shutdown_cannot_reopen_admission()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let (faults, store) = StorageFaults::open().await?;
    let (state, agent) = StreamFailureState::open()?;
    let mut app = App::open(AppConfig::new(
        store,
        vec![AgentSpec::new("cleanup".to_owned(), "v1".to_owned(), agent)],
        vec![WorkflowDefinition::parse(DROP_WAIT)?],
    ))
    .await?;
    let workspace = app
        .create_workspace(WorkspaceConfig {
            name: None,
            root: std::env::current_dir()?,
        })
        .await?;
    let workflow = workspace.workflow("drop-wait".parse()?).await?;
    let run = workflow.start(ExecutionOptions::default()).await?;
    assert!(matches!(run.snapshot().await?.status, RunStatus::Running));
    faults.arm_shutdown_scan();
    let mut rejecting = Box::pin(app.shutdown(Shutdown::RejectActive));
    tokio::time::timeout(Duration::from_secs(1), async {
        tokio::select! {
            _ = faults.shutdown_scan_observed.notified() => {}
            result = rejecting.as_mut() => panic!("RejectActive must reach the active-inventory gate: {result:?}"),
        }
    }).await.expect("exact shutdown inventory rendezvous");
    drop(rejecting);
    drop(app);
    faults.release_shutdown_scan();
    tokio::time::timeout(Duration::from_secs(1), async {
        while state.shutdowns.load(Ordering::SeqCst) == 0 {
            assert!(
                matches!(
                    workflow.start(ExecutionOptions::default()).await,
                    Err(AppError::Closed(_))
                ),
                "a retained workflow cannot admit after the root was dropped during RejectActive"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("root drop must retain cleanup when RejectActive discovers active work");
    assert!(matches!(
        workflow.start(ExecutionOptions::default()).await,
        Err(AppError::Closed(_))
    ));
    assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn cold_unknown_history_retains_one_real_lower_outcome_for_migration()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_notification_resolution_fault(
        NotificationResolutionFault::LostReceipt,
        true,
    )
    .await
}

#[tokio::test]
async fn activation_deadline_covers_notification_resolution()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_notification_resolution_fault(
        NotificationResolutionFault::Stall,
        false,
    )
    .await
}

#[tokio::test]
async fn panic_during_notification_resolution_retains_unknown_target()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_notification_resolution_fault(
        NotificationResolutionFault::Panic,
        false,
    )
    .await
}

#[tokio::test]
async fn lost_notification_resolution_receipt_does_not_replay_lower_decision()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_notification_resolution_fault(
        NotificationResolutionFault::LostReceipt,
        false,
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn notification_resolution_receipt_outlives_a_stalled_observation() {
    let (state, agent) = StreamFailureState::open().unwrap();
    let notifications = NotificationSequence::new(1, false);
    *notifications.resolution_fault.lock().unwrap() = NotificationResolutionFault::GateReceipt;
    *state.notifications.lock().unwrap() = Some(Arc::clone(&notifications));
    let (faults, store) = StorageFaults::open().await.unwrap();
    let native = store.inner.clone();
    *state.read_authority.lock().unwrap() = Some((
        Arc::clone(&faults),
        AttemptReadCase::ResolutionObservation { owner: None },
    ));
    let mut app = App::open(AppConfig::new(
        store,
        vec![AgentSpec::new("faulty".into(), "v1".into(), agent)],
        vec![WorkflowDefinition::parse(STREAM_FAILURE).unwrap()],
    ))
    .await
    .unwrap();
    let (read_drop, dropped) = tokio::sync::oneshot::channel();
    let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(12), async {
        let workspace = app.create_workspace(WorkspaceConfig {
            name: None, root: std::env::current_dir().unwrap(),
        }).await.unwrap();
        let workflow = workspace.workflow("stream-failure".parse().unwrap()).await.unwrap();
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        let observer = workflow.open_run(RunSelector::Latest(run.id().clone())).await.unwrap();
        let mut finish = Box::pin(run.execution(ExecutionOptions::default()).await.unwrap().finish());
        let waiting = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                waiting = async {
                    loop {
                        let current = observer.wait().await.unwrap();
                        if matches!(current.interactions.as_slice(), [Interaction::Notification { .. }]) {
                            break current;
                        }
                        assert!(!matches!(current.status, RunStatus::Succeeded | RunStatus::Failed(_) | RunStatus::Cancelled),
                            "the original prompt cannot finish before its notification");
                        tokio::task::yield_now().await;
                    }
                } => waiting,
                result = &mut finish => panic!("original prompt ended before its notification: {}", result.is_ok()),
            }
        }).await.expect("the original foreground execution publishes its actual notification");
        let [Interaction::Notification { sequence, generation, notification }] = waiting.interactions.as_slice()
            else { panic!("one real notification is actionable") };
        assert_eq!(notification.id(), &NotificationSequence::id(0));
        let (sequence, generation) = (*sequence, *generation);
        assert!(faults.attempt_owner.lock().unwrap().is_some());
        *faults.attempt_read_dropped.lock().unwrap() = Some(read_drop);
        *faults.attempt_read.lock().unwrap() = Some(AttemptReadCase::ResolutionObservation { owner: None });
        // App preserves each admitted operation's caller dispatcher. Mark only
        // this resolution so its Pending read selects its own Intended observer.
        let mut original = Box::pin(observer.resolve(sequence, generation,
            Resolution::Notification(NotificationResolution::Allow))
            .with_subscriber(InteractionCapture::default()));
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                _ = async { tokio::join!(faults.attempt_read_entered.notified(), notifications.resolution_entered.notified()); } => {},
                result = &mut original => panic!("the original lower receipt and observation are still held: {}", result.is_ok()),
            }
        }).await.expect("the admitted resolution reaches both real held boundaries");
        let (head, intended) = faults.attempt_read_records.lock().unwrap().clone().unwrap();
        let intended_logical = StorageFaults::logical_checkpoint(&native, &intended).await;
        let [target] = intended_logical["checkpoint"]["settlements"].as_array().unwrap().as_slice()
            else { panic!("the original Intended row retains exactly one target") };
        assert_eq!(target[0], sequence);
        assert_eq!(target[1], generation);
        assert_eq!(target[3][2], NotificationSequence::id(0).as_str());
        assert_eq!(target[4][0], 1);
        assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), 1);
        assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), 0);
        notifications.resolution_release.notify_one();
        let scope = head.key.key().strip_prefix("run/").unwrap();
        let applied_sequence = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let head = native.load(head.key.clone()).await.unwrap().unwrap();
                let current = head.record.value["sequence"].as_u64().unwrap();
                let record = native.load(StateKey::new("app-sdk",
                    format!("history/{scope}/checkpoint/{current:020}")).unwrap()).await.unwrap().unwrap();
                let logical = StorageFaults::logical_checkpoint(&native, &record.record).await;
                let targets = logical["checkpoint"]["settlements"].as_array().unwrap();
                if let Some(target) = targets.iter().find(|target| target[0] == sequence
                    && target[1] == generation && target[4][0] == 2) {
                    break target[4][2].as_u64().unwrap();
                }
                tokio::task::yield_now().await;
            }
        }).await.expect("the original lower return publishes its real Applied receipt independently of observation");
        assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), 1);
        let applied = tokio::time::timeout(Duration::from_secs(2), original).await
            .expect("the original durable receipt must return while the unrelated pure observation remains stalled").unwrap();
        assert_eq!(applied.checkpoint, applied_sequence);
        assert_eq!(applied.state, waiting.state);
        assert!(applied.outcomes == waiting.outcomes);
        assert_eq!(applied.last_action_ordinal, waiting.last_action_ordinal);
        assert!(tokio::time::timeout(Duration::from_secs(1), dropped).await
            .expect("returning the original receipt destroys the stalled pure read").is_err());
        let replay = observer.resolve(sequence, generation,
            Resolution::Notification(NotificationResolution::Allow)).await.unwrap();
        assert_eq!(replay.checkpoint, applied.checkpoint);
        assert_eq!(replay.state, applied.state);
        assert_eq!(native.load(intended.key.clone()).await.unwrap().unwrap().record.value, intended.value);
        let boundary = finish.await.unwrap();
        let terminal = if matches!(&boundary.status, RunStatus::Succeeded) {
            boundary
        } else {
            assert!(matches!(&boundary.status, RunStatus::Paused | RunStatus::Running),
                "original actionable-boundary finish returned {}", match &boundary.status {
                    RunStatus::Halted { .. } => "Halted", RunStatus::Cancelling => "Cancelling",
                    RunStatus::Failed(_) => "Failed", RunStatus::Cancelled => "Cancelled", _ => "unexpected status",
                });
            assert_eq!(boundary.state, waiting.state);
            assert!(boundary.outcomes == waiting.outcomes);
            assert_eq!(boundary.last_action_ordinal, waiting.last_action_ordinal);
            // Consuming finish closes at the earlier actionable boundary. Its
            // accepted prompt settles once; explicit execution advances its barrier.
            let acquisition_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
            loop {
                match run.execution(ExecutionOptions::default()).await {
                    Ok(execution) => break execution.finish().await.unwrap(),
                    Err(AppError::Busy(_)) => {
                        assert!(tokio::time::Instant::now() < acquisition_deadline,
                            "original cleanup releases explicit execution ownership");
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("same-run continuation failed: {error}"),
                }
            }
        };
        assert!(matches!(&terminal.status, RunStatus::Succeeded),
            "explicit continuation returned {}", match &terminal.status {
                RunStatus::Running => "Running", RunStatus::Paused => "Paused", RunStatus::Halted { .. } => "Halted",
                RunStatus::Cancelling => "Cancelling", RunStatus::Failed(_) => "Failed", RunStatus::Cancelled => "Cancelled",
                _ => "unexpected status",
            });
        assert_eq!(terminal.state.values()["answer"], "both approvals accepted");
        assert_eq!(terminal.last_action_ordinal, Some(0));
        assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), 1);
        assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), 1);
        assert_eq!(state.prompts.load(Ordering::SeqCst), 1);
    })).catch_unwind().await;
    state.read_authority.lock().unwrap().take();
    faults.attempt_read.lock().unwrap().take();
    faults.attempt_owner.lock().unwrap().take();
    faults.attempt_read_dropped.lock().unwrap().take();
    faults.attempt_read_release.notify_one();
    notifications.resolution_release.notify_one();
    notifications.cancelled.store(true, Ordering::SeqCst);
    notifications.changed.notify_waiters();
    let cleanup = AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(5),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        }),
    ))
    .catch_unwind()
    .await;
    drop(app);
    match checked {
        Ok(result) => result.expect("original receipt observation stays bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    cleanup
        .expect("cleanup does not panic")
        .expect("cleanup joins")
        .unwrap();
    assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
}

impl StreamFailureState {
    async fn notification_receipts_precede_prompt_outcome(
        failure: Option<agent_sdk::error::ErrorKind>,
        streaming: bool,
    ) {
        let failed = failure.is_some();
        let capture = InteractionCapture::default();
        let mut trace_targets = None;
        let (state, agent) = StreamFailureState::open().unwrap();
        let notifications = NotificationSequence::new(2, false);
        *notifications.prompt_error.lock().unwrap() = failure;
        *notifications.resolution_fault.lock().unwrap() = NotificationResolutionFault::GateReceipt;
        *state.notifications.lock().unwrap() = Some(Arc::clone(&notifications));
        let mut definition: Value = serde_json::from_str(STREAM_FAILURE).unwrap();
        definition["nodes"][0]["deadline_ms"] = Value::from(12_000);
        definition["nodes"][0]["retry"] = serde_json::json!({"kind":"immediate","max_attempts":3});
        definition["nodes"][0]["step"]["stream"] = Value::from(streaming);
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let mut app = tokio::time::timeout(
            Duration::from_secs(3),
            App::open(AppConfig::new(
                store,
                vec![AgentSpec::new("faulty".into(), "v1".into(), agent)],
                vec![WorkflowDefinition::parse(&definition.to_string()).unwrap()],
            )),
        )
        .await
        .expect("notification ownership admission is bounded")
        .unwrap();
        let checked = std::panic::AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(10),
        async {
            let workspace = app.create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().unwrap(),
            }).await?;
            let workflow = workspace.workflow("stream-failure".parse().unwrap()).await?;
            let run = workflow.start(ExecutionOptions::default()).await?;
            let first = tokio::time::timeout(Duration::from_secs(2), run.wait())
                .await.expect("first notification is observed")?;
            let [Interaction::Notification { sequence, generation, notification }] = first.interactions.as_slice() else {
                panic!("first exact notification is the sole actionable target")
            };
            assert_eq!(notification.id(), &NotificationSequence::id(0));
            let (first_sequence, first_generation) = (*sequence, *generation);
            let mut original = Box::pin(run.resolve(first_sequence, first_generation,
                Resolution::Notification(NotificationResolution::Allow)));
            tokio::time::timeout(Duration::from_secs(2), async {
                tokio::select! {
                    _ = notifications.resolution_entered.notified() => {},
                    _ = original.as_mut() => panic!("first original receipt is held"),
                }
                while notifications.received.load(Ordering::SeqCst) < 2 {
                    tokio::task::yield_now().await;
                }
            }).await.expect("N2 is actually delivered while N1's original receipt is held");
            assert_eq!(notifications.emitted.load(Ordering::SeqCst), 2);
            assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), 1);
            assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), 0);
            drop(original);
            let mut repeated = Box::pin(run.resolve(first_sequence, first_generation,
                Resolution::Notification(NotificationResolution::Allow)));
            assert!(tokio::time::timeout(Duration::from_millis(100), repeated.as_mut()).await.is_err(),
                "delivery of N2 cannot substitute for N1's original native receipt");
            let held = run.snapshot().await?;
            StreamFailureState::assert_same_interactions(&held.interactions, &first.interactions);
            assert_eq!(held.state, first.state);
            assert!(held.outcomes == first.outcomes);
            assert_eq!(held.last_action_ordinal, first.last_action_ordinal);
            assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), 1);
            capture.assert_not_applied(&first);
            notifications.resolution_release.notify_one();
            let applied = tokio::time::timeout(Duration::from_secs(2), repeated)
                .await.expect("N1 resolves through its original receipt")?;
            let second = tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let current = run.snapshot().await?;
                    if matches!(current.interactions.as_slice(),
                        [Interaction::Notification { notification, .. }] if notification.id() == &NotificationSequence::id(1)) {
                        break Ok::<_, AppError>(current);
                    }
                    assert!(!matches!(current.status, RunStatus::Failed(_) | RunStatus::Succeeded));
                    tokio::task::yield_now().await;
                }
            }).await.expect("buffered N2 receives its own target after N1 settles")?;
            let [Interaction::Notification { sequence, generation, .. }] = second.interactions.as_slice() else {
                unreachable!()
            };
            let (second_sequence, second_generation) = (*sequence, *generation);
            assert_ne!(second_sequence, first_sequence);
            assert_ne!(second_generation, first_generation);
            let replay = run.resolve(first_sequence, first_generation,
                Resolution::Notification(NotificationResolution::Allow)).await?;
            StreamFailureState::assert_applied_snapshot(&replay, &applied);
            *notifications.resolution_fault.lock().unwrap() = NotificationResolutionFault::GateReceipt;
            let mut last = Box::pin(run.resolve(second_sequence, second_generation,
                Resolution::Notification(NotificationResolution::Allow)));
            tokio::time::timeout(Duration::from_secs(2), async {
                tokio::select! {
                    _ = notifications.resolution_entered.notified() => {},
                    _ = last.as_mut() => panic!("second original receipt is held"),
                }
                while notifications.prompt_returns.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
            }).await.expect("actual prompt return arrives before N2's receipt is released");
            assert!(tokio::time::timeout(Duration::from_millis(100), last.as_mut()).await.is_err(),
                "terminal prompt evidence cannot replace the original resolution receipt");
            let terminal_held = run.snapshot().await?;
            StreamFailureState::assert_same_interactions(&terminal_held.interactions, &second.interactions);
            assert_eq!(terminal_held.state, first.state);
            assert!(terminal_held.outcomes == first.outcomes);
            assert_eq!(terminal_held.last_action_ordinal, first.last_action_ordinal);
            assert!(!matches!(terminal_held.status, RunStatus::Succeeded | RunStatus::Failed(_)));
            assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), 2);
            assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), 1);
            notifications.resolution_release.notify_one();
            tokio::time::timeout(Duration::from_secs(2), last)
                .await.expect("N2 retains its own released receipt")?;
            let terminal = tokio::time::timeout(Duration::from_secs(2), run.wait())
                .await.expect("buffered terminal response commits after both receipts settle")?;
            if failed {
                let terminal = tokio::time::timeout(Duration::from_secs(2), async {
                    loop {
                        let current = run.snapshot().await?;
                        if matches!(current.interactions.as_slice(), [Interaction::Unknown { retry_safe: true, .. }]) {
                            break Ok::<_, AppError>(current);
                        }
                        assert!(matches!(current.status, RunStatus::Running));
                        assert_eq!(state.prompts.load(Ordering::SeqCst), 1);
                        tokio::task::yield_now().await;
                    }
                }).await.expect("original abort authority settles before caller failure resolution")?;
                let [Interaction::Unknown { sequence, generation, .. }] = terminal.interactions.as_slice()
                    else { panic!("accepted prompt error remains Unknown after both receipts settle") };
                assert_eq!(terminal.state.values()["answer"], "");
                assert!(terminal.outcomes.is_empty());
                run.resolve(*sequence, *generation, Resolution::Fail).await?;
                assert!(matches!(run.wait().await?.status, RunStatus::Failed(_)));
            } else {
                assert!(matches!(terminal.status, RunStatus::Succeeded));
                assert!(terminal.interactions.is_empty());
                assert_eq!(terminal.state.values()["answer"], "both approvals accepted");
                assert_eq!(terminal.last_action_ordinal, Some(0));
                assert_eq!(terminal.outcomes.len(), 1);
                assert_eq!(terminal.outcomes[0].node, "agent");
            }
            assert_eq!(notifications.prompt_returns.load(Ordering::SeqCst), 1);
            assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), 2);
            assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), 2);
            assert_eq!(state.prompts.load(Ordering::SeqCst), 1);
            assert_eq!(state.aborts.load(Ordering::SeqCst), usize::from(failed));
            trace_targets = Some((first, second));
            Ok::<_, AppError>(())
        },
    )).catch_unwind().with_subscriber(capture.clone()).await;
        *notifications.resolution_fault.lock().unwrap() = NotificationResolutionFault::Pass;
        notifications.resolution_release.notify_one();
        notifications.cancelled.store(true, Ordering::SeqCst);
        notifications.changed.notify_waiters();
        let cleanup = std::panic::AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        ))
        .catch_unwind()
        .await;
        drop(app);
        match checked {
            Ok(result) => result
                .expect("sequential receipt test stays bounded")
                .unwrap(),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup
            .expect("receipt cleanup does not panic")
            .expect("receipt cleanup is bounded")
            .unwrap();
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
        let (first, second) =
            trace_targets.expect("both actual notification targets were retained");
        for target in [&first, &second] {
            capture.assert_target(target, &["pending", "intended", "applied"]);
        }
        capture.assert_redacted(&[
            &NotificationSequence::id(0).to_string(),
            &NotificationSequence::id(1).to_string(),
            "both approvals accepted",
        ]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn later_notification_and_prompt_completion_wait_for_original_resolution_receipts() {
    for streaming in [false, true] {
        StreamFailureState::notification_receipts_precede_prompt_outcome(None, streaming).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispatched_prompt_error_preserves_pending_notification_resolution_receipts() {
    StreamFailureState::notification_receipts_precede_prompt_outcome(
        Some(agent_sdk::error::ErrorKind::InvalidInput),
        false,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unary_protocol_error_preserves_pending_notification_resolution_receipts() {
    StreamFailureState::notification_receipts_precede_prompt_outcome(
        Some(agent_sdk::error::ErrorKind::Protocol),
        false,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unary_integration_error_preserves_pending_notification_resolution_receipts() {
    StreamFailureState::notification_receipts_precede_prompt_outcome(
        Some(agent_sdk::error::ErrorKind::Integration),
        false,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accepted_stream_error_preserves_pending_notification_resolution_receipts() {
    for failure in [
        agent_sdk::error::ErrorKind::InvalidInput,
        agent_sdk::error::ErrorKind::Protocol,
        agent_sdk::error::ErrorKind::Integration,
    ] {
        StreamFailureState::notification_receipts_precede_prompt_outcome(Some(failure), true).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_caller_notification_receipt_reattaches_without_lower_replay() {
    let (left, left_agent) = StreamFailureState::open().unwrap();
    let (right, right_agent) = StreamFailureState::open().unwrap();
    let left_notifications = NotificationSequence::new(2, false);
    let right_notifications = NotificationSequence::new(2, false);
    *left_notifications.resolution_fault.lock().unwrap() = NotificationResolutionFault::GateReceipt;
    *left_notifications.emission.lock().unwrap() = NotificationEmission::GateAfterFirst;
    *left.notifications.lock().unwrap() = Some(Arc::clone(&left_notifications));
    *right.notifications.lock().unwrap() = Some(Arc::clone(&right_notifications));
    let definition = StreamFailureState::notification_peer_definition(false);
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let mut app = tokio::time::timeout(
        Duration::from_secs(3),
        App::open(AppConfig::new(
            store,
            vec![
                AgentSpec::new("left".into(), "v1".into(), left_agent),
                AgentSpec::new("right".into(), "v1".into(), right_agent),
            ],
            vec![definition],
        )),
    )
    .await
    .expect("two-peer admission is bounded")
    .unwrap();
    let checked =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(15), async {
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().unwrap(),
                })
                .await?;
            let workflow = workspace
                .workflow("notification-peers".parse().unwrap())
                .await?;
            let run = workflow.start(ExecutionOptions::default()).await?;
            let first = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let snapshot = run.snapshot().await?;
                    assert!(!matches!(
                        snapshot.status,
                        RunStatus::Failed(_) | RunStatus::Succeeded
                    ));
                    if snapshot.interactions.len() == 2 {
                        break Ok::<_, AppError>(snapshot);
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("both independent first notifications are admitted")?;
            let [
                Interaction::Notification {
                    sequence: a_sequence,
                    generation: a_generation,
                    notification: a_notification,
                },
                Interaction::Notification {
                    sequence: b_sequence,
                    generation: b_generation,
                    notification: b_notification,
                },
            ] = first.interactions.as_slice()
            else {
                panic!("both canonical peers expose their own first notification")
            };
            assert_eq!(a_notification.id(), &NotificationSequence::id(0));
            assert_eq!(b_notification.id(), &NotificationSequence::id(0));
            let (a_sequence, a_generation, b_sequence, b_generation) =
                (*a_sequence, *a_generation, *b_sequence, *b_generation);
            assert_ne!(a_sequence, b_sequence);
            assert_eq!(first.last_action_ordinal, Some(0));
            assert_eq!(first.outcomes.len(), 1);
            assert_eq!(first.outcomes[0].node, "seed");
            assert_eq!(
                first.state.values(),
                &Map::from_iter([
                    ("left".into(), Value::from("before")),
                    ("right".into(), Value::from("before")),
                ])
            );
            let mut lost = Box::pin(run.resolve(
                a_sequence,
                a_generation,
                Resolution::Notification(NotificationResolution::Allow),
            ));
            tokio::time::timeout(Duration::from_secs(2), async {
                tokio::select! {
                    _ = left_notifications.resolution_entered.notified() => {},
                    _ = lost.as_mut() => panic!("the accepted original lower receipt stays gated"),
                }
            })
            .await
            .expect("a's actual lower decision is accepted before its caller is lost");
            drop(lost);
            tokio::time::timeout(
                Duration::from_secs(2),
                left_notifications.next_target_entered.notified(),
            )
            .await
            .expect("a cannot emit N2 before its existing next-target gate is released");
            assert_eq!(left_notifications.emitted.load(Ordering::SeqCst), 1);
            let resolved = left_notifications.resolved.lock().unwrap().clone();
            assert_eq!(resolved, [NotificationSequence::id(0)]);
            assert_eq!(
                left_notifications.resolution_calls.load(Ordering::SeqCst),
                1
            );

            // The held original a receipt cannot block or invalidate b's target.
            let b_applied = run
                .resolve(
                    b_sequence,
                    b_generation,
                    Resolution::Notification(NotificationResolution::Allow),
                )
                .await?;
            let b_second = tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let snapshot = run.snapshot().await?;
                    if let Some(Interaction::Notification {
                        sequence,
                        generation,
                        notification,
                    }) = snapshot.interactions.last()
                        && notification.id() == &NotificationSequence::id(1)
                        && *sequence != b_sequence
                    {
                        assert_eq!(snapshot.state, first.state);
                        assert_eq!(snapshot.last_action_ordinal, first.last_action_ordinal);
                        assert!(snapshot.outcomes == first.outcomes);
                        break Ok::<_, AppError>((*sequence, *generation));
                    }
                    assert!(!matches!(
                        snapshot.status,
                        RunStatus::Failed(_) | RunStatus::Succeeded
                    ));
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("b advances to its second target while a's original receipt is held")?;
            assert_eq!(
                right_notifications.resolution_calls.load(Ordering::SeqCst),
                1
            );
            let mut reattached = Box::pin(run.resolve(
                a_sequence,
                a_generation,
                Resolution::Notification(NotificationResolution::Allow),
            ));
            assert!(
                tokio::time::timeout(Duration::from_millis(100), reattached.as_mut())
                    .await
                    .is_err(),
                "reattachment observes the original accepted lower receipt"
            );
            assert_eq!(
                left_notifications.resolution_calls.load(Ordering::SeqCst),
                1
            );
            left_notifications.resolution_release.notify_one();
            let a_applied = tokio::time::timeout(Duration::from_secs(2), reattached)
                .await
                .expect("the released original receipt reaches its reattached observer")?;
            left_notifications.next_target_release.notify_one();
            let second = tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let snapshot = run.snapshot().await?;
                    if snapshot.interactions.len() == 2
                        && snapshot.interactions.iter().all(|target| {
                            matches!(target, Interaction::Notification { notification, .. }
                        if notification.id() == &NotificationSequence::id(1))
                        })
                    {
                        break Ok::<_, AppError>(snapshot);
                    }
                    assert!(!matches!(
                        snapshot.status,
                        RunStatus::Failed(_) | RunStatus::Succeeded
                    ));
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("both original attempts expose their distinct second targets")?;
            let [
                Interaction::Notification {
                    sequence: a_next,
                    generation: a_next_generation,
                    ..
                },
                Interaction::Notification {
                    sequence: b_next,
                    generation: b_next_generation,
                    ..
                },
            ] = second.interactions.as_slice()
            else {
                panic!("both second notification targets remain canonical")
            };
            let (a_next, a_next_generation, b_next, b_next_generation) =
                (*a_next, *a_next_generation, *b_next, *b_next_generation);
            assert_eq!((b_next, b_next_generation), b_second);
            assert_ne!(a_next, a_sequence);
            assert_ne!(a_next_generation, a_generation);
            assert_eq!(second.state, first.state);
            assert_eq!(second.last_action_ordinal, first.last_action_ordinal);
            assert!(second.outcomes == first.outcomes);
            let replay = run
                .resolve(
                    a_sequence,
                    a_generation,
                    Resolution::Notification(NotificationResolution::Allow),
                )
                .await?;
            StreamFailureState::assert_applied_snapshot(&replay, &a_applied);
            let b_replay = run
                .resolve(
                    b_sequence,
                    b_generation,
                    Resolution::Notification(NotificationResolution::Allow),
                )
                .await?;
            StreamFailureState::assert_applied_snapshot(&b_replay, &b_applied);
            for (sequence, generation, resolution) in [
                (
                    a_sequence,
                    a_generation,
                    NotificationResolution::Deny {
                        reason: "changed".into(),
                    },
                ),
                (a_next, a_generation, NotificationResolution::Allow),
            ] {
                assert!(matches!(
                    run.resolve(sequence, generation, Resolution::Notification(resolution))
                        .await,
                    Err(AppError::Conflict(_))
                ));
                let unchanged = run.snapshot().await?;
                StreamFailureState::assert_applied_snapshot(&unchanged, &second);
                assert_eq!(
                    left_notifications.resolution_calls.load(Ordering::SeqCst),
                    1
                );
                assert_eq!(
                    right_notifications.resolution_calls.load(Ordering::SeqCst),
                    1
                );
            }
            run.resolve(
                b_next,
                b_next_generation,
                Resolution::Notification(NotificationResolution::Allow),
            )
            .await?;
            let held = run.snapshot().await?;
            StreamFailureState::assert_same_interactions(
                &held.interactions,
                &second.interactions[..1],
            );
            assert_eq!(held.state, first.state);
            assert_eq!(held.last_action_ordinal, first.last_action_ordinal);
            assert!(held.outcomes == first.outcomes);
            run.resolve(
                a_next,
                a_next_generation,
                Resolution::Notification(NotificationResolution::Allow),
            )
            .await?;
            let terminal = tokio::time::timeout(Duration::from_secs(3), run.wait())
                .await
                .expect("both original prompts finish one complete wave")?;
            assert!(matches!(terminal.status, RunStatus::Succeeded));
            assert!(terminal.interactions.is_empty());
            assert_eq!(
                terminal.state.values(),
                &Map::from_iter([
                    ("left".into(), Value::from("both approvals accepted")),
                    ("right".into(), Value::from("both approvals accepted")),
                ])
            );
            assert_eq!(terminal.last_action_ordinal, Some(2));
            assert_eq!(
                terminal
                    .outcomes
                    .iter()
                    .map(|outcome| (outcome.node.as_str(), outcome.ordinal))
                    .collect::<Vec<_>>(),
                [("a-agent", 1), ("z-agent", 2)]
            );
            let replay = run
                .resolve(
                    a_sequence,
                    a_generation,
                    Resolution::Notification(NotificationResolution::Allow),
                )
                .await?;
            StreamFailureState::assert_applied_snapshot(&replay, &a_applied);
            for (state, notifications) in
                [(&left, &left_notifications), (&right, &right_notifications)]
            {
                let resolved = notifications.resolved.lock().unwrap().clone();
                assert_eq!(
                    resolved,
                    [NotificationSequence::id(0), NotificationSequence::id(1)]
                );
                assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), 2);
                assert_eq!(state.conversations.load(Ordering::SeqCst), 1);
                assert_eq!(state.prompts.load(Ordering::SeqCst), 1);
                assert_eq!(state.streams.load(Ordering::SeqCst), 1);
            }
            Ok::<_, AppError>(())
        }))
        .catch_unwind()
        .await;
    for notifications in [&left_notifications, &right_notifications] {
        notifications.cancelled.store(true, Ordering::SeqCst);
        notifications.changed.notify_waiters();
        notifications.resolution_release.notify_one();
        notifications.next_target_release.notify_one();
    }
    let cleanup = std::panic::AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(5),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        }),
    ))
    .catch_unwind()
    .await;
    drop(app);
    match checked {
        Ok(result) => result
            .expect("two-peer receipt assertions remain bounded")
            .unwrap(),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    cleanup
        .expect("two-peer cleanup does not panic")
        .expect("two-peer cleanup is bounded")
        .unwrap();
    assert_eq!(left.shutdowns.load(Ordering::SeqCst), 1);
    assert_eq!(right.shutdowns.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_retains_both_accepted_notification_receipts() {
    let (left, left_agent) = StreamFailureState::open().unwrap();
    let (right, right_agent) = StreamFailureState::open().unwrap();
    let left_notifications = NotificationSequence::new(2, false);
    let right_notifications = NotificationSequence::new(2, false);
    for (state, notifications) in [(&left, &left_notifications), (&right, &right_notifications)] {
        *state.notifications.lock().unwrap() = Some(Arc::clone(notifications));
        *notifications.resolution_fault.lock().unwrap() = NotificationResolutionFault::GateReceipt;
        *notifications.emission.lock().unwrap() = NotificationEmission::GateAfterFirst;
    }
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let mut app = tokio::time::timeout(
        Duration::from_secs(3),
        App::open(AppConfig::new(
            store,
            vec![
                AgentSpec::new("left".into(), "v1".into(), left_agent),
                AgentSpec::new("right".into(), "v1".into(), right_agent),
            ],
            vec![StreamFailureState::notification_peer_definition(false)],
        )),
    )
    .await
    .expect("two-peer admission is bounded")
    .unwrap();
    let checked =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(18), async {
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().unwrap(),
                })
                .await?;
            let workflow = workspace
                .workflow("notification-peers".parse().unwrap())
                .await?;
            let run = workflow.start(ExecutionOptions::default()).await?;
            let first = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let snapshot = run.snapshot().await?;
                    assert!(!matches!(
                        snapshot.status,
                        RunStatus::Failed(_) | RunStatus::Succeeded
                    ));
                    if snapshot.interactions.len() == 2 {
                        break Ok::<_, AppError>(snapshot);
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("both real notification targets enter before cancellation")?;
            let [
                Interaction::Notification {
                    sequence: a_sequence,
                    generation: a_generation,
                    notification: a_notification,
                },
                Interaction::Notification {
                    sequence: b_sequence,
                    generation: b_generation,
                    notification: b_notification,
                },
            ] = first.interactions.as_slice()
            else {
                panic!("both peers expose their exact notification target")
            };
            assert_eq!(a_notification.id(), &NotificationSequence::id(0));
            assert_eq!(b_notification.id(), &NotificationSequence::id(0));
            assert_ne!(a_sequence, b_sequence);
            let mut a_resolution = Box::pin(run.resolve(
                *a_sequence,
                *a_generation,
                Resolution::Notification(NotificationResolution::Allow),
            ));
            let mut b_resolution = Box::pin(run.resolve(
                *b_sequence,
                *b_generation,
                Resolution::Notification(NotificationResolution::Allow),
            ));
            let mut a_entered = Box::pin(left_notifications.resolution_entered.notified());
            let mut b_entered = Box::pin(right_notifications.resolution_entered.notified());
            let mut accepted = [false; 2];
            tokio::time::timeout(Duration::from_secs(2), async {
                while !accepted.iter().all(|entered| *entered) {
                    tokio::select! {
                        _ = a_entered.as_mut(), if !accepted[0] => accepted[0] = true,
                        _ = b_entered.as_mut(), if !accepted[1] => accepted[1] = true,
                        _ = a_resolution.as_mut() => panic!("a's original receipt remains held"),
                        _ = b_resolution.as_mut() => panic!("b's original receipt remains held"),
                    }
                }
            })
            .await
            .expect("both independent lower calls accept before either receipt returns");
            drop(a_resolution);
            drop(b_resolution);
            for notifications in [&left_notifications, &right_notifications] {
                tokio::time::timeout(
                    Duration::from_secs(2),
                    notifications.next_target_entered.notified(),
                )
                .await
                .expect("both prompts are held before any next notification");
                let resolved = notifications.resolved.lock().unwrap().clone();
                assert_eq!(resolved, [NotificationSequence::id(0)]);
                assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), 1);
                assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), 0);
                assert_eq!(notifications.emitted.load(Ordering::SeqCst), 1);
            }
            let before_cancel = run.snapshot().await?;
            assert_eq!(before_cancel.state, first.state);
            assert_eq!(before_cancel.last_action_ordinal, Some(0));
            assert!(before_cancel.outcomes == first.outcomes);
            run.cancel(before_cancel.checkpoint).await?;
            // The only remaining external gates are the original accepted receipts.
            // Both prompt emission gates are released before testing finalization.
            left_notifications.next_target_release.notify_one();
            right_notifications.next_target_release.notify_one();
            let cancelling = run.snapshot().await?;
            assert!(matches!(cancelling.status, RunStatus::Cancelling));
            assert_eq!(cancelling.state, first.state);
            assert_eq!(cancelling.last_action_ordinal, first.last_action_ordinal);
            assert!(cancelling.outcomes == first.outcomes);
            assert!(
                matches!(
                    app.shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_secs(2),
                    })
                    .await,
                    Err(AppError::Timeout(_))
                ),
                "shutdown cannot discard either accepted lower receipt"
            );
            assert_eq!(
                left_notifications.resolution_returns.load(Ordering::SeqCst),
                0
            );
            assert_eq!(
                right_notifications
                    .resolution_returns
                    .load(Ordering::SeqCst),
                0
            );
            left_notifications.resolution_release.notify_one();
            tokio::time::timeout(Duration::from_secs(2), async {
                while left_notifications.resolution_returns.load(Ordering::SeqCst) != 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("a's original accepted call returns after its gate release");
            assert_eq!(
                right_notifications
                    .resolution_returns
                    .load(Ordering::SeqCst),
                0
            );
            assert!(
                matches!(
                    app.shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_secs(2),
                    })
                    .await,
                    Err(AppError::Timeout(_))
                ),
                "b alone still owns its admitted lower receipt after a has returned"
            );
            let one_left = run.snapshot().await?;
            assert!(matches!(one_left.status, RunStatus::Cancelling));
            assert_eq!(one_left.state, first.state);
            assert_eq!(one_left.last_action_ordinal, first.last_action_ordinal);
            assert!(one_left.outcomes == first.outcomes);
            assert_eq!(
                right_notifications
                    .resolution_returns
                    .load(Ordering::SeqCst),
                0
            );
            right_notifications.resolution_release.notify_one();
            let terminal = tokio::time::timeout(Duration::from_secs(3), run.wait())
                .await
                .expect("cancellation finishes after both original receipts return")?;
            assert!(matches!(terminal.status, RunStatus::Cancelled));
            assert!(terminal.interactions.is_empty());
            assert_eq!(terminal.state, first.state);
            assert_eq!(terminal.last_action_ordinal, first.last_action_ordinal);
            assert!(terminal.outcomes == first.outcomes);
            for (state, notifications) in
                [(&left, &left_notifications), (&right, &right_notifications)]
            {
                let resolved = notifications.resolved.lock().unwrap().clone();
                assert_eq!(resolved, [NotificationSequence::id(0)]);
                assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), 1);
                assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), 1);
                assert_eq!(state.conversations.load(Ordering::SeqCst), 1);
                assert_eq!(state.prompts.load(Ordering::SeqCst), 1);
                assert_eq!(state.streams.load(Ordering::SeqCst), 1);
                assert_eq!(state.aborts.load(Ordering::SeqCst), 1);
            }
            Ok::<_, AppError>(())
        }))
        .catch_unwind()
        .await;
    for notifications in [&left_notifications, &right_notifications] {
        notifications.cancelled.store(true, Ordering::SeqCst);
        notifications.changed.notify_waiters();
        notifications.resolution_release.notify_one();
        notifications.next_target_release.notify_one();
    }
    let cleanup = std::panic::AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(5),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        }),
    ))
    .catch_unwind()
    .await;
    drop(app);
    match checked {
        Ok(result) => result
            .expect("two-receipt cancellation remains bounded")
            .unwrap(),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    cleanup
        .expect("two-receipt cleanup does not panic")
        .expect("two-receipt cleanup is bounded")
        .unwrap();
    assert_eq!(left.shutdowns.load(Ordering::SeqCst), 1);
    assert_eq!(right.shutdowns.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_notification_accept_preserves_the_other_agent_and_input() {
    let (left, left_agent) = StreamFailureState::open().unwrap();
    let (right, right_agent) = StreamFailureState::open().unwrap();
    let left_notifications = NotificationSequence::new(2, false);
    let right_notifications = NotificationSequence::new(2, false);
    *left_notifications.resolution_fault.lock().unwrap() =
        NotificationResolutionFault::GateLostReceipt;
    *left_notifications.emission.lock().unwrap() = NotificationEmission::GateAfterFirst;
    *left.notifications.lock().unwrap() = Some(Arc::clone(&left_notifications));
    *right.notifications.lock().unwrap() = Some(Arc::clone(&right_notifications));
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let mut app = tokio::time::timeout(
        Duration::from_secs(3),
        App::open(AppConfig::new(
            store,
            vec![
                AgentSpec::new("left".into(), "v1".into(), left_agent),
                AgentSpec::new("right".into(), "v1".into(), right_agent),
            ],
            vec![StreamFailureState::notification_peer_definition(true)],
        )),
    )
    .await
    .expect("Unknown peer admission stays bounded")
    .unwrap();
    let checked =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(15), async {
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().unwrap(),
                })
                .await?;
            let workflow = workspace
                .workflow("notification-peers".parse().unwrap())
                .await?;
            let run = workflow.start(ExecutionOptions::default()).await?;
            let ready = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let snapshot = run.snapshot().await?;
                    assert!(!matches!(
                        snapshot.status,
                        RunStatus::Failed(_) | RunStatus::Succeeded
                    ));
                    if snapshot.interactions.len() == 3 {
                        break Ok::<_, AppError>(snapshot);
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("both Agents and the Input enter before receipt loss")?;
            let [
                Interaction::Notification {
                    sequence: a_sequence,
                    generation: a_generation,
                    notification: a_notification,
                },
                Interaction::Input {
                    sequence: input_sequence,
                    generation: input_generation,
                    ..
                },
                Interaction::Notification {
                    sequence: b_sequence,
                    generation: b_generation,
                    notification: b_notification,
                },
            ] = ready.interactions.as_slice()
            else {
                panic!("the canonical initial targets are a, Input, and b")
            };
            assert_eq!(a_notification.id(), &NotificationSequence::id(0));
            assert_eq!(b_notification.id(), &NotificationSequence::id(0));
            let (
                a_sequence,
                a_generation,
                input_sequence,
                input_generation,
                b_sequence,
                b_generation,
            ) = (
                *a_sequence,
                *a_generation,
                *input_sequence,
                *input_generation,
                *b_sequence,
                *b_generation,
            );
            assert_ne!(a_sequence, b_sequence);
            assert_eq!(ready.last_action_ordinal, Some(0));
            assert_eq!(ready.outcomes.len(), 1);
            assert_eq!(ready.outcomes[0].node, "seed");
            let mut resolution = Box::pin(run.resolve(
                a_sequence,
                a_generation,
                Resolution::Notification(NotificationResolution::Allow),
            ));
            tokio::time::timeout(Duration::from_secs(2), async {
                tokio::select! {
                    _ = left_notifications.resolution_entered.notified() => {},
                    _ = resolution.as_mut() => {
                        left_notifications.resolution_entered.notified().await;
                    },
                }
            })
            .await
            .expect("the lower really accepts a's decision and loses its return");
            drop(resolution);
            let next_target = tokio::time::timeout(
                Duration::from_secs(2),
                left_notifications.next_target_entered.notified(),
            )
            .await;
            if next_target.is_err() {
                eprintln!(
                    "Unknown peer next-target observation: aborts={} cancelled={} emitted={} received={} prompt_returns={} resolution_calls={} resolution_returns={} accepted_ids={}",
                    left.aborts.load(Ordering::SeqCst),
                    left_notifications.cancelled.load(Ordering::SeqCst),
                    left_notifications.emitted.load(Ordering::SeqCst),
                    left_notifications.received.load(Ordering::SeqCst),
                    left_notifications.prompt_returns.load(Ordering::SeqCst),
                    left_notifications.resolution_calls.load(Ordering::SeqCst),
                    left_notifications.resolution_returns.load(Ordering::SeqCst),
                    left_notifications.resolved.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).len(),
                );
            }
            next_target.expect(
                "a's unfinished prompt cannot supply a terminal response as substitute evidence",
            );
            // Publish receipt loss only after the original prompt is really held
            // before N2; otherwise a correct abort can win that fixture race.
            left_notifications.resolution_release.notify_one();
            let unknown = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let snapshot = run.snapshot().await?;
                    if matches!(snapshot.interactions.first(), Some(Interaction::Unknown {
                    sequence, retry_safe: true, ..
                }) if *sequence == a_sequence)
                        && left.aborts.load(Ordering::SeqCst) == 1
                    {
                        break Ok::<_, AppError>(snapshot);
                    }
                    assert!(!matches!(
                        snapshot.status,
                        RunStatus::Failed(_) | RunStatus::Succeeded
                    ));
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("a retains its target identity after exact abort settlement");
            let unknown = unknown?;
            let Some(Interaction::Unknown {
                sequence,
                generation,
                retry_safe: true,
            }) = unknown.interactions.first()
            else {
                panic!("the replacement advertises its own settled Unknown authority")
            };
            let (unknown_sequence, unknown_generation) = (*sequence, *generation);
            assert_eq!(unknown_sequence, a_sequence);
            StreamFailureState::assert_same_interactions(
                &unknown.interactions[1..],
                &ready.interactions[1..],
            );
            assert_eq!(unknown.state, ready.state);
            assert_eq!(unknown.last_action_ordinal, ready.last_action_ordinal);
            assert!(unknown.outcomes == ready.outcomes);
            let resolved = left_notifications.resolved.lock().unwrap().clone();
            assert_eq!(resolved, [NotificationSequence::id(0)]);
            assert_eq!(
                left_notifications.resolution_calls.load(Ordering::SeqCst),
                1
            );
            assert_eq!(
                left_notifications.resolution_returns.load(Ordering::SeqCst),
                0
            );
            assert_eq!(left_notifications.emitted.load(Ordering::SeqCst), 1);
            assert_eq!(left.prompts.load(Ordering::SeqCst), 1);
            assert!(matches!(
                run.resolve(
                    unknown_sequence,
                    unknown_generation,
                    Resolution::Notification(NotificationResolution::Allow)
                )
                .await,
                Err(AppError::Conflict(_))
            ));
            let unchanged = run.snapshot().await?;
            StreamFailureState::assert_applied_snapshot(&unchanged, &unknown);
            run.resolve(
                b_sequence,
                b_generation,
                Resolution::Notification(NotificationResolution::Allow),
            )
            .await?;
            let b_second = tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let snapshot = run.snapshot().await?;
                    if snapshot.interactions.len() == 3
                        && matches!(snapshot.interactions.last(), Some(Interaction::Notification {
                            notification, ..
                        }) if notification.id() == &NotificationSequence::id(1))
                    {
                        break Ok::<_, AppError>(snapshot);
                    }
                    assert!(!matches!(
                        snapshot.status,
                        RunStatus::Failed(_) | RunStatus::Succeeded
                    ));
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("b reaches N2 independently while a and Input remain caller-owned")?;
            StreamFailureState::assert_same_interactions(
                &b_second.interactions[..2],
                &unknown.interactions[..2],
            );
            assert_eq!(b_second.state, ready.state);
            assert_eq!(b_second.last_action_ordinal, ready.last_action_ordinal);
            assert!(b_second.outcomes == ready.outcomes);
            let Some(Interaction::Notification {
                sequence: b_next,
                generation: b_next_generation,
                ..
            }) = b_second.interactions.last()
            else {
                panic!("b's second exact target remains actionable")
            };
            let (b_next, b_next_generation) = (*b_next, *b_next_generation);
            assert_ne!(b_next, b_sequence);
            assert_eq!(
                left_notifications.resolution_calls.load(Ordering::SeqCst),
                1
            );
            assert_eq!(left.prompts.load(Ordering::SeqCst), 1);
            let accepted = Map::from_iter([("left".into(), Value::from("caller accepted"))]);
            let applied = run
                .resolve(
                    unknown_sequence,
                    unknown_generation,
                    Resolution::Accept(accepted.clone()),
                )
                .await?;
            let peers_only = run.snapshot().await?;
            StreamFailureState::assert_same_interactions(
                &peers_only.interactions,
                &b_second.interactions[1..],
            );
            assert_eq!(peers_only.state, ready.state);
            assert_eq!(peers_only.last_action_ordinal, ready.last_action_ordinal);
            assert!(peers_only.outcomes == ready.outcomes);
            let replay = run
                .resolve(
                    unknown_sequence,
                    unknown_generation,
                    Resolution::Accept(accepted),
                )
                .await?;
            StreamFailureState::assert_applied_snapshot(&replay, &applied);
            assert!(matches!(
                run.resolve(
                    unknown_sequence,
                    unknown_generation,
                    Resolution::Accept(Map::from_iter([("left".into(), Value::from("changed"))]))
                )
                .await,
                Err(AppError::Conflict(_))
            ));
            let unchanged = run.snapshot().await?;
            StreamFailureState::assert_applied_snapshot(&unchanged, &peers_only);
            left_notifications.next_target_release.notify_one();
            run.resolve(
                b_next,
                b_next_generation,
                Resolution::Notification(NotificationResolution::Allow),
            )
            .await?;
            run.resolve(
                input_sequence,
                input_generation,
                Resolution::Input(Value::from("manual input")),
            )
            .await?;
            let terminal = tokio::time::timeout(Duration::from_secs(3), run.wait())
                .await
                .expect("caller settlement and final Input publish one complete wave")?;
            assert!(matches!(terminal.status, RunStatus::Succeeded));
            assert!(terminal.interactions.is_empty());
            assert_eq!(
                terminal.state.values(),
                &Map::from_iter([
                    ("left".into(), Value::from("caller accepted")),
                    ("right".into(), Value::from("both approvals accepted")),
                    ("inputs".into(), serde_json::json!(["manual input"])),
                ])
            );
            assert_eq!(terminal.last_action_ordinal, Some(3));
            assert_eq!(
                terminal
                    .outcomes
                    .iter()
                    .map(|outcome| (outcome.node.as_str(), outcome.ordinal))
                    .collect::<Vec<_>>(),
                [("a-agent", 1), ("b-input", 2), ("z-agent", 3)]
            );
            for state in [&left, &right] {
                assert_eq!(state.prompts.load(Ordering::SeqCst), 1);
                assert_eq!(state.conversations.load(Ordering::SeqCst), 1);
            }
            assert_eq!(
                left_notifications.resolution_calls.load(Ordering::SeqCst),
                1
            );
            assert_eq!(
                left_notifications.resolution_returns.load(Ordering::SeqCst),
                0
            );
            assert_eq!(left.aborts.load(Ordering::SeqCst), 1);
            assert_eq!(
                right_notifications.resolution_calls.load(Ordering::SeqCst),
                2
            );
            assert_eq!(
                right_notifications
                    .resolution_returns
                    .load(Ordering::SeqCst),
                2
            );
            Ok::<_, AppError>(())
        }))
        .catch_unwind()
        .await;
    for notifications in [&left_notifications, &right_notifications] {
        notifications.cancelled.store(true, Ordering::SeqCst);
        notifications.changed.notify_waiters();
        notifications.resolution_release.notify_one();
        notifications.next_target_release.notify_one();
    }
    let cleanup = std::panic::AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(5),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        }),
    ))
    .catch_unwind()
    .await;
    drop(app);
    match checked {
        Ok(result) => result
            .expect("Unknown peer settlement stays bounded")
            .unwrap(),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    cleanup
        .expect("Unknown peer cleanup does not panic")
        .expect("Unknown peer cleanup is bounded")
        .unwrap();
    assert_eq!(left.shutdowns.load(Ordering::SeqCst), 1);
    assert_eq!(right.shutdowns.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn notification_settlement_arrays_preserve_evidence_and_reject_malformed_rows()
-> Result<(), Box<dyn Error + Send + Sync>> {
    for path in ["", "/3", "/3/3", "/4", "/4/1"] {
        for corruption in [
            NotificationCorruption::TupleObject { path },
            NotificationCorruption::TupleMissing { path },
            NotificationCorruption::TupleExtra { path },
        ] {
            StreamFailureState::assert_notification_corruption(corruption).await?;
        }
    }
    for path in [
        "/0", "/1", "/2", "/3/0", "/3/1", "/3/2", "/3/3/0", "/3/3/1", "/3/4", "/3/5", "/3/7",
        "/4/0", "/4/1/0", "/4/2",
    ] {
        StreamFailureState::assert_notification_corruption(NotificationCorruption::TupleField {
            path,
        })
        .await?;
    }
    for path in ["/3/0", "/4/0", "/4/1/0"] {
        StreamFailureState::assert_notification_corruption(NotificationCorruption::TupleTag {
            path,
        })
        .await?;
    }
    StreamFailureState::assert_notification_corruption(
        NotificationCorruption::TupleTimestampOverflow,
    )
    .await?;
    for (path, text) in [
        ("/3/4", ""),
        ("/3/4", " padded "),
        ("/3/5", " \t\n"),
        ("/3/6", "scalar schema"),
    ] {
        StreamFailureState::assert_notification_corruption(NotificationCorruption::TupleText {
            path,
            text,
        })
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn duplicate_durable_notification_targets_are_rejected_before_projection()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_notification_corruption(NotificationCorruption::DuplicateTarget)
        .await
}

#[tokio::test]
async fn removed_durable_notification_receipt_is_rejected_before_projection()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_notification_corruption(NotificationCorruption::RemoveApplied).await
}

#[tokio::test]
async fn changed_durable_notification_receipt_is_rejected_before_projection()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_notification_corruption(
        NotificationCorruption::ChangeAppliedDecision,
    )
    .await
}

#[tokio::test]
async fn notification_pending_capacity_below_limit() -> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_notification_capacity(255).await
}

#[tokio::test]
async fn notification_pending_capacity_at_limit() -> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_notification_capacity(256).await
}

#[tokio::test]
async fn newly_reserved_notification_requires_its_matching_current_control()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_notification_corruption(NotificationCorruption::OrphanTarget {
        observer: tokio::task::try_id(),
    })
    .await
}

#[tokio::test]
async fn recovery_rejects_a_legal_sized_checkpoint_without_unknown_capacity()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_notification_corruption(NotificationCorruption::ProspectiveBytes {
        observer: tokio::task::try_id(),
    })
    .await
}

#[tokio::test]
async fn notification_pending_capacity_overflow_preserves_limit_failure()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_notification_capacity(257).await
}

#[tokio::test]
async fn cancelling_second_notification_preserves_first_settlement_receipt()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let (state, agent) = StreamFailureState::open()?;
    let notifications = NotificationSequence::new(2, false);
    *state.notifications.lock().expect("test notifications lock") =
        Some(Arc::clone(&notifications));
    let store =
        StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    let mut app = App::open(AppConfig::new(
        store,
        vec![AgentSpec::new("faulty".to_owned(), "v1".to_owned(), agent)],
        vec![WorkflowDefinition::parse(STREAM_FAILURE)?],
    ))
    .await?;
    let workspace = app
        .create_workspace(WorkspaceConfig {
            name: None,
            root: std::env::current_dir()?,
        })
        .await?;
    let workflow = workspace.workflow("stream-failure".parse()?).await?;
    let run = workflow.start(ExecutionOptions::default()).await?;
    let first = tokio::time::timeout(Duration::from_secs(1), run.wait()).await??;
    let Some(Interaction::Notification {
        sequence: first_sequence,
        generation: first_generation,
        ..
    }) = first.interactions.first()
    else {
        panic!("first target")
    };
    let (first_sequence, first_generation) = (*first_sequence, *first_generation);
    let applied = run
        .resolve(
            first_sequence,
            first_generation,
            Resolution::Notification(NotificationResolution::Allow),
        )
        .await?;
    let second = tokio::time::timeout(Duration::from_secs(1), run.wait()).await??;
    let Some(Interaction::Notification {
        sequence: second_sequence,
        generation: second_generation,
        notification,
    }) = second.interactions.first()
    else {
        panic!("second target")
    };
    let (second_sequence, second_generation) = (*second_sequence, *second_generation);
    assert_eq!(notification.id(), &NotificationSequence::id(1));
    run.cancel(second.checkpoint).await?;
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let snapshot = run.snapshot().await?;
            match snapshot.status {
                RunStatus::Cancelled => break,
                RunStatus::Cancelling => tokio::task::yield_now().await,
                _ => panic!("cancellation must retain its owned outcome while abort completes"),
            }
        }
        Ok::<_, AppError>(())
    })
    .await
    .expect("accepted cancellation must finish its exact lower abort")?;
    let first_replay = run
        .resolve(
            first_sequence,
            first_generation,
            Resolution::Notification(NotificationResolution::Allow),
        )
        .await?;
    StreamFailureState::assert_applied_snapshot(&first_replay, &applied);
    assert!(matches!(
        run.resolve(
            second_sequence,
            second_generation,
            Resolution::Notification(NotificationResolution::Allow)
        )
        .await,
        Err(AppError::Conflict(_))
    ));
    let resolved = notifications
        .resolved
        .lock()
        .expect("test resolution lock")
        .clone();
    assert_eq!(resolved, [NotificationSequence::id(0)]);
    assert_eq!(state.aborts.load(Ordering::SeqCst), 1);
    app.shutdown(Shutdown::Drain {
        timeout: Duration::from_secs(1),
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn notification_settlement_receipt_survives_reopen_before_next_target()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let (state, agent) = StreamFailureState::open()?;
    let notifications = NotificationSequence::new(2, false);
    *notifications.emission.lock().expect("test emission lock") =
        NotificationEmission::GateAfterFirst;
    *state.notifications.lock().expect("test notifications lock") =
        Some(Arc::clone(&notifications));
    let store =
        StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    let app = App::open(AppConfig::new(
        store.clone(),
        vec![AgentSpec::new("faulty".to_owned(), "v1".to_owned(), agent)],
        vec![WorkflowDefinition::parse(STREAM_FAILURE)?],
    ))
    .await?;
    let workspace = app
        .create_workspace(WorkspaceConfig {
            name: None,
            root: std::env::current_dir()?,
        })
        .await?;
    let workspace_id = workspace.id().clone();
    let workflow = workspace.workflow("stream-failure".parse()?).await?;
    let run = workflow.start(ExecutionOptions::default()).await?;
    let run_id = run.id().clone();
    let first = tokio::time::timeout(Duration::from_secs(1), run.wait()).await??;
    let [
        Interaction::Notification {
            sequence: first_sequence,
            generation: first_generation,
            ..
        },
    ] = first.interactions.as_slice()
    else {
        panic!("first target")
    };
    let (first_sequence, first_generation) = (*first_sequence, *first_generation);
    let applied = run
        .resolve(
            first_sequence,
            first_generation,
            Resolution::Notification(NotificationResolution::Allow),
        )
        .await?;
    tokio::time::timeout(
        Duration::from_secs(1),
        notifications.next_target_entered.notified(),
    )
    .await
    .expect("prompt waits after N1 acceptance before N2 exists");
    assert_eq!(notifications.emitted.load(Ordering::SeqCst), 1);
    drop(run);
    drop(workflow);
    drop(workspace);
    drop(app);
    tokio::time::timeout(Duration::from_secs(1), async {
        while Arc::strong_count(&state) > 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the old runtime must release its exact lower attempt before reopen");
    let mut app = App::open(AppConfig::new(
        store,
        vec![AgentSpec::new(
            "faulty".to_owned(),
            "v1".to_owned(),
            StreamFailureAgent {
                state: Arc::clone(&state),
            },
        )],
        vec![WorkflowDefinition::parse(STREAM_FAILURE)?],
    ))
    .await?;
    let workspace = app.workspace(workspace_id).await?;
    let workflow = workspace.workflow("stream-failure".parse()?).await?;
    let run = workflow.open_run(RunSelector::Latest(run_id)).await?;
    let unknown = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let snapshot = run.snapshot().await?;
            if matches!(
                snapshot.interactions.as_slice(),
                [Interaction::Unknown {
                    retry_safe: true,
                    ..
                }]
            ) {
                return Ok::<_, AppError>(snapshot);
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reopen reconciles the old prompt without resubmission")?;
    let replay = run
        .resolve(
            first_sequence,
            first_generation,
            Resolution::Notification(NotificationResolution::Allow),
        )
        .await?;
    StreamFailureState::assert_applied_snapshot(&replay, &applied);
    let resolved = notifications
        .resolved
        .lock()
        .expect("test resolution lock")
        .clone();
    assert_eq!(resolved, [NotificationSequence::id(0)]);
    assert_eq!(state.prompts.load(Ordering::SeqCst), 1);
    assert_eq!(notifications.emitted.load(Ordering::SeqCst), 1);
    let [
        Interaction::Unknown {
            sequence,
            generation,
            ..
        },
    ] = unknown.interactions.as_slice()
    else {
        panic!("recovered unknown attempt")
    };
    run.resolve(*sequence, *generation, Resolution::Fail)
        .await?;
    app.shutdown(Shutdown::Drain {
        timeout: Duration::from_secs(1),
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn short_activation_deadline_expires_before_lower_effects()
-> Result<(), Box<dyn Error + Send + Sync>> {
    // The unchanged 25ms deadline may expire before or during the real intent
    // and preparation reads. Neither outcome permits a lower effect.
    StreamFailureState::assert_app_preparation_failure(
        StatePreparationFault::WholeActivationDeadline,
    )
    .await
}

#[tokio::test]
async fn activation_deadline_covers_app_storage_preparation()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_app_preparation_failure(StatePreparationFault::StallAfterIntent)
        .await
}

#[tokio::test]
async fn panic_during_app_storage_preparation_preserves_pre_effect_failure()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_app_preparation_failure(StatePreparationFault::PanicAfterIntent)
        .await
}

#[tokio::test]
async fn activation_deadline_retains_admitted_preparation_failure_commit()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_app_preparation_failure(StatePreparationFault::FailAfterIntent).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn background_native_siblings_keep_two_agents_inputs_and_sleeps_independent() {
    StreamFailureState::assert_mixed_controls(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exclusive_native_siblings_keep_two_agents_inputs_and_sleeps_independent() {
    StreamFailureState::assert_mixed_controls(true).await;
}

#[tokio::test]
async fn durable_notification_history_restores_deletes_and_exports_a_cold_golden()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_durable_notification_history().await
}

#[tokio::test]
async fn notification_delivery_routine_preserves_receipts_replay_and_cleanup()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_routine_delivery().await
}

#[tokio::test]
async fn initial_attempt_reserves_unknown_capacity_before_lower_preparation()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_initial_attempt_capacity().await
}

#[tokio::test]
async fn settlement_byte_capacity_reserves_unknown_before_first_overflow()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_prospective_settlement_bytes().await
}

impl StorageFaults {
    async fn reject_inventory_page(fault: InventoryPageFault) {
        let (faults, store) = Self::open().await.unwrap();
        let native = store.inner.clone();
        let mut seed = App::open(AppConfig::<StateStore, Agent>::new(
            native.clone(),
            Vec::new(),
            Vec::new(),
        ))
        .await
        .unwrap();
        let seeded = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(3),
            seed.create_workspace(WorkspaceConfig {
                name: Some("inventory-page-scope".into()),
                root: std::env::current_dir().unwrap(),
            }),
        ))
        .catch_unwind()
        .await;
        let seed_closed = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(5),
            seed.shutdown(Shutdown::Drain {
                timeout: Duration::from_secs(3),
            }),
        ))
        .catch_unwind()
        .await;
        drop(seed);
        match seeded {
            Ok(result) => {
                drop(
                    result
                        .expect("seed workspace admission is bounded")
                        .unwrap(),
                );
            }
            Err(panic) => std::panic::resume_unwind(panic),
        }
        match seed_closed {
            Ok(result) => result
                .expect("seed cleanup is bounded")
                .expect("seed App joins"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        let before = native
            .list(StateListRequest::new("app-sdk").unwrap())
            .await
            .unwrap();
        assert!(before.next_start_after.is_none());
        assert!(
            before
                .records
                .iter()
                .any(|row| row.record.key.key() == "app")
        );
        assert!(
            before
                .records
                .iter()
                .any(|row| row.record.key.key().starts_with("workspace/"))
        );
        *faults.inventory_page.lock().unwrap() = fault;
        let mut accepted = None;
        let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), async {
            let reopened = App::open(AppConfig::<FaultStateStore, Agent>::new(
                store,
                Vec::new(),
                Vec::new(),
            ))
            .await;
            let rejected = match reopened {
                Err(AppError::Integration(_)) => true,
                Err(error) => panic!("unexpected malformed inventory rejection: {error}"),
                Ok(app) => {
                    accepted = Some(app);
                    false
                }
            };
            let after = native
                .list(StateListRequest::new("app-sdk").unwrap())
                .await
                .unwrap();
            assert!(after.next_start_after.is_none());
            (rejected, before.records == after.records)
        }))
        .catch_unwind()
        .await;
        let closed = match accepted.as_mut() {
            Some(app) => Some(
                AssertUnwindSafe(tokio::time::timeout(
                    Duration::from_secs(5),
                    app.shutdown(Shutdown::Drain {
                        timeout: Duration::from_secs(3),
                    }),
                ))
                .catch_unwind()
                .await,
            ),
            None => None,
        };
        drop(accepted);
        let (rejected, unchanged) = match observed {
            Ok(result) => result.expect("inventory validation remains bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        };
        if let Some(closed) = closed {
            match closed {
                Ok(result) => result
                    .expect("unexpectedly accepted App cleanup is bounded")
                    .expect("unexpectedly accepted App joins before reporting red"),
                Err(panic) => std::panic::resume_unwind(panic),
            }
        }
        assert!(
            matches!(
                *faults.inventory_page.lock().unwrap(),
                InventoryPageFault::Observed
            ),
            "{fault:?}: exact real inventory page was intercepted"
        );
        assert!(
            rejected,
            "{fault:?}: App::open accepted a page outside the storage list contract"
        );
        assert!(
            unchanged,
            "{fault:?}: inventory rejection must precede generation claim and preserve every native record"
        );
    }
}

#[tokio::test]
async fn app_open_rejects_foreign_namespace_in_preflight_page_before_claim() {
    StorageFaults::reject_inventory_page(InventoryPageFault::ForeignNamespace).await;
}

#[tokio::test]
async fn app_open_rejects_foreign_key_in_workspace_page_before_claim() {
    StorageFaults::reject_inventory_page(InventoryPageFault::WorkspacePrefix).await;
}

#[tokio::test]
async fn app_open_rejects_inconsistent_preflight_cursor_before_claim() {
    StorageFaults::reject_inventory_page(InventoryPageFault::CursorMismatch).await;
}

impl StreamFailureState {
    async fn sibling_admission(case: SiblingAdmissionCase) {
        let (state, agent) = Self::open().unwrap();
        let notifications = NotificationSequence::new(1, false);
        *state.notifications.lock().unwrap() = Some(Arc::clone(&notifications));
        let (faults, store) = StorageFaults::open().await.unwrap();
        let native = store.inner.clone();
        let control = match case {
            SiblingAdmissionCase::RejectedInput | SiblingAdmissionCase::LostInput => {
                serde_json::json!({"kind":"input","target":"inputs","message":[{"kind":"text","text":"last input"}]})
            }
            SiblingAdmissionCase::RejectedAgent
            | SiblingAdmissionCase::LostAgent
            | SiblingAdmissionCase::CorruptAgent => {
                serde_json::json!({"kind":"agent","agent":"agent","prompt":[{"kind":"text","text":"original prompt"}],"text_target":"answer"})
            }
        };
        let definition = WorkflowDefinition::parse(&serde_json::json!({
            "schema":"app-sdk.workflow/v2","id":"sibling-admission","revision":"v1",
            "state":{"inputs":[],"answer":"before"},"required":["inputs","answer"],
            "reducers":{"inputs":"collect","answer":"reject_concurrent"},"entry":"seed",
            "nodes":[
                {"id":"seed","step":{"kind":"state","mutations":[]}},
                {"id":"a0-input","step":{"kind":"input","target":"inputs","message":[{"kind":"text","text":"first input"}]}},
                {"id":"a1-input","step":{"kind":"input","target":"inputs","message":[{"kind":"text","text":"second input"}]}},
                {"id":"b-control","deadline_ms":10000,"step":control}
            ],"routes":{"seed":{"mode":"broadcast","branches":[
                {"target":{"kind":"node","id":"b-control"}},
                {"target":{"kind":"node","id":"a1-input"}},
                {"target":{"kind":"node","id":"a0-input"}}
            ]}}
        }).to_string()).unwrap();
        let mut app = App::open(
            AppConfig::new(
                store,
                vec![AgentSpec::new("agent".into(), "v1".into(), agent)],
                vec![definition],
            )
            .with_hooks(state.agent()),
        )
        .await
        .unwrap();
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(15), async {
            let workspace = app.create_workspace(WorkspaceConfig { name:None,root:std::env::current_dir().unwrap() }).await?;
            let workflow = workspace.workflow("sibling-admission".parse().unwrap()).await?;
            let mut run = workflow.prepare(WorkflowState::default()).await?;
            let observer = workflow.open_run(RunSelector::Latest(run.id().clone())).await?;
            *faults.sibling_admission.lock().unwrap() = Some(case);
            let execution = run.execution(ExecutionOptions::default()).await?;
            let finish = execution.finish();
            tokio::pin!(finish);
            tokio::select! {
                result = &mut finish => panic!("original execution ended before its actual native admission was held: {}", result.is_ok()),
                _ = faults.run_transition_observed.notified() => {}
            }
            let staged_sequence = faults.sibling_sequence.load(Ordering::SeqCst) as u64;
            let held = observer.snapshot().await?;
            assert_eq!(held.last_action_ordinal, Some(0));
            assert_eq!(held.state.values()["inputs"], serde_json::json!([]));
            assert_eq!(state.prompts.load(Ordering::SeqCst), 0, "no lower effect before an accepted intent receipt");
            assert_eq!(state.conversations.load(Ordering::SeqCst), 0);
            let inputs = held.interactions.iter().map(|target| match target {
                Interaction::Input { sequence,generation,message,.. } => (*sequence,*generation,message.clone()),
                _ => panic!("held native admission exposes only its actual Input controls"),
            }).collect::<Vec<_>>();
            let expected_messages: &[&str] = if matches!(case,SiblingAdmissionCase::LostInput) {
                &["first input","second input","last input"]
            } else { &["first input","second input"] };
            assert_eq!(inputs.iter().map(|(_,_,message)|message.as_str()).collect::<Vec<_>>().as_slice(), expected_messages);
            let mut resolved_before_receipt = None;
            for (sequence,generation,message) in inputs {
                let own_input = message == "last input";
                let applied = observer.resolve(sequence,generation,Resolution::Input(Value::from(message))).await?;
                if own_input {
                    assert!(matches!(case,SiblingAdmissionCase::LostInput));
                    assert_eq!(sequence,staged_sequence,"the still-held native receipt owns this exact accepted Input");
                    resolved_before_receipt = Some((sequence,generation,applied));
                }
            }
            let advanced = observer.snapshot().await?;
            assert!(advanced.checkpoint > staged_sequence, "real sibling publications advance beyond the held candidate");
            assert_eq!(advanced.last_action_ordinal, Some(0));
            assert_eq!(advanced.state, held.state);
            assert!(advanced.outcomes == held.outcomes);
            if matches!(case,SiblingAdmissionCase::CorruptAgent) {
                let history = format!("history/{}/sibling-admission/{}/{}/checkpoint/",workspace.id(),observer.id(),advanced.occurrence);
                let original = native.load(StateKey::new("app-sdk",format!("{history}{:020}",advanced.checkpoint)).unwrap())
                    .await.unwrap().unwrap().record;
                let previous = native.load(StateKey::new("app-sdk",format!("{history}{:020}",advanced.checkpoint-1)).unwrap())
                    .await.unwrap().unwrap().record;
                let accepted = original.value["checkpoint"]["controls"].as_object().unwrap()
                    .values().find(|control|control["node"] == "b-control").unwrap();
                assert_eq!(accepted["sequence"],serde_json::json!(staged_sequence));
                assert_eq!(accepted["state"]["kind"],"attempt");
                assert_eq!(accepted["state"]["phase"]["state"],"intent");
                assert!(previous.value["checkpoint"]["controls"].as_object().unwrap().values()
                    .any(|control|control == accepted),"the actual predecessor still owns the original accepted intent");
                let mut altered = original.clone();
                Arc::make_mut(&mut altered.value)["checkpoint"]["controls"] = serde_json::json!({});
                Arc::make_mut(&mut altered.value)["checkpoint"]["phase"] = serde_json::json!({
                    "state":"failed","category":"execution","message":"removed accepted intent",
                    "observation":null,"cancellation_checkpoint":null,
                });
                let rejected = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(3),async {
                    let mut write = native.begin_transaction().await.unwrap();
                    assert_eq!(write.load(original.key.clone()).await.unwrap().unwrap().record,original);
                    write.put(altered).await.unwrap();
                    write.commit().await.unwrap();
                    matches!(observer.snapshot().await,Err(AppError::Integration(_)))
                })).catch_unwind().await;
                // Restore before returning the held native receipt, even if a
                // public corruption assertion panicked or timed out.
                let mut write = native.begin_transaction().await.unwrap();
                write.load(original.key.clone()).await.unwrap().unwrap();
                write.put(original).await.unwrap();
                write.commit().await.unwrap();
                match rejected {
                    Ok(result) => assert!(result.expect("accepted-control corruption read is bounded"),
                        "terminal failure cannot silently retire the original accepted Agent intent"),
                    Err(panic) => std::panic::resume_unwind(panic),
                }
            }
            faults.release_run_transition();
            let (terminal, sequence, generation, applied) = if let Some((sequence,generation,applied)) = resolved_before_receipt {
                let early = tokio::time::timeout(Duration::from_secs(1),async {
                    tokio::select! {
                        _ = faults.ambiguity_observed.notified() => None,
                        result = &mut finish => Some(result),
                    }
                }).await.expect("the original accepted Input receipt read reaches its actual ambiguity");
                assert!(!faults.receipt_obscured.load(Ordering::SeqCst),
                    "the exact committed receipt read is obscured before recovery");
                let terminal = match early { Some(result) => result?, None => finish.await? };
                (terminal,sequence,generation,applied)
            } else {
            let mut original_boundary = None;
            let target = loop {
                tokio::select! {
                    result = &mut finish, if original_boundary.is_none() => {
                        original_boundary = Some(result?);
                    }
                    snapshot = observer.snapshot() => {
                        let snapshot = snapshot?;
                        if let Some(target) = snapshot.interactions.into_iter().next() { break target; }
                    }
                }
                tokio::task::yield_now().await;
            };
            if matches!(case,SiblingAdmissionCase::LostAgent | SiblingAdmissionCase::CorruptAgent) {
                tokio::time::timeout(Duration::from_secs(1),faults.ambiguity_observed.notified())
                    .await.expect("the actual mutation receipt read was obscured");
                assert!(!faults.receipt_obscured.load(Ordering::SeqCst));
            }
            let (sequence,generation,resolution) = match target {
                Interaction::Input { sequence,generation,message,.. } => {
                    assert!(matches!(case,SiblingAdmissionCase::RejectedInput));
                    assert_eq!(message,"last input");
                    (sequence,generation,Resolution::Input(Value::from(message)))
                }
                Interaction::Notification { sequence,generation,.. } => {
                    assert!(!matches!(case,SiblingAdmissionCase::RejectedInput));
                    (sequence,generation,Resolution::Notification(NotificationResolution::Allow))
                }
                _ => panic!("the original admitted control remains answerable"),
            };
            let boundary = match original_boundary {
                Some(boundary) => boundary,
                None => finish.await?,
            };
            assert!(if matches!(case,SiblingAdmissionCase::RejectedInput) {
                matches!(boundary.status,RunStatus::Paused)
            } else { matches!(boundary.status,RunStatus::Running) },
                "finish consumes its original owner at the unresolved target; returned {}",
                match &boundary.status {
                    RunStatus::Running => "running", RunStatus::Paused => "paused",
                    RunStatus::Halted { .. } => "halted", RunStatus::Cancelling => "cancelling",
                    RunStatus::Succeeded => "succeeded", RunStatus::Failed(_) => "failed",
                    RunStatus::Cancelled => "cancelled", _ => "unknown",
                });
            assert!(boundary.interactions.iter().any(|target| match target {
                Interaction::Input { sequence: current, generation: fence, .. }
                | Interaction::Notification { sequence: current, generation: fence, .. } =>
                    *current == sequence && *fence == generation,
                _ => false,
            }), "the original finish retains the same unresolved native target");
            assert_eq!(boundary.last_action_ordinal,Some(0));
            assert_eq!(boundary.state,held.state);
            assert!(boundary.outcomes == held.outcomes);
            let applied = observer.resolve(sequence,generation,resolution).await?;
            let acquisition_deadline = tokio::time::Instant::now()+Duration::from_secs(3);
            let terminal = loop {
                let mut resumed = workflow.open_run(RunSelector::Latest(observer.id().clone())).await?;
                let execution = match resumed.execution(ExecutionOptions::default()).await {
                    Ok(execution) => execution,
                    Err(AppError::Busy(_)) => {
                        assert!(tokio::time::Instant::now()<acquisition_deadline,
                            "the original cleanup releases explicit execution ownership");
                        tokio::task::yield_now().await;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                break execution.finish().await?;
            };
            (terminal,sequence,generation,applied)
            };
            assert!(matches!(terminal.status,RunStatus::Succeeded));
            assert_eq!(terminal.last_action_ordinal,Some(3));
            assert_eq!(terminal.outcomes.iter().map(|outcome|(outcome.node.as_str(),outcome.ordinal)).collect::<Vec<_>>(),
                [("a0-input",1),("a1-input",2),("b-control",3)]);
            let expected = if matches!(case,SiblingAdmissionCase::RejectedInput | SiblingAdmissionCase::LostInput) {
                serde_json::json!(["first input","second input","last input"])
            } else { serde_json::json!(["first input","second input"]) };
            assert_eq!(terminal.state.values()["inputs"],expected);
            let agent_count = usize::from(!matches!(case,SiblingAdmissionCase::RejectedInput | SiblingAdmissionCase::LostInput));
            assert_eq!(terminal.state.values()["answer"], if agent_count == 0 { "before" } else { "both approvals accepted" });
            assert_eq!(state.prompts.load(Ordering::SeqCst),agent_count);
            assert_eq!(state.conversations.load(Ordering::SeqCst),agent_count);
            assert_eq!(state.streams.load(Ordering::SeqCst),agent_count);
            let contexts = state.step_contexts.lock().unwrap().clone();
            assert_eq!(contexts.iter().map(|context|(context.node.as_str(),context.action_ordinal)).collect::<Vec<_>>(),
                [("seed",0),("a0-input",1),("a1-input",2),("b-control",3)]);
            assert!(contexts[1..].iter().all(|context|context.checkpoint == contexts[1].checkpoint));
            let resolution = if matches!(case,SiblingAdmissionCase::RejectedInput | SiblingAdmissionCase::LostInput) {
                Resolution::Input(Value::from("last input"))
            } else { Resolution::Notification(NotificationResolution::Allow) };
            let replay = observer.resolve(sequence,generation,resolution).await?;
            Self::assert_applied_snapshot(&replay,&applied);
            assert!(faults.sibling_admission.lock().unwrap().is_none());
            Ok::<_,AppError>(())
        })).catch_unwind().await;
        faults.release_run_transition();
        notifications.cancelled.store(true, Ordering::SeqCst);
        notifications.changed.notify_waiters();
        notifications.resolution_release.notify_one();
        let cleanup = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        ))
        .catch_unwind()
        .await;
        drop(app);
        match checked {
            Ok(result) => result.expect("sibling admission remains bounded").unwrap(),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup
            .expect("sibling cleanup does not panic")
            .expect("sibling cleanup is bounded")
            .unwrap();
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn rejected_input_admission_preserves_original_decision_after_sibling_commits() {
    StreamFailureState::sibling_admission(SiblingAdmissionCase::RejectedInput).await;
}

#[tokio::test]
async fn rejected_agent_admission_retargets_its_original_owner_after_sibling_commits() {
    StreamFailureState::sibling_admission(SiblingAdmissionCase::RejectedAgent).await;
}

#[tokio::test]
async fn lost_agent_admission_receipt_preserves_original_intent_after_sibling_commits() {
    StreamFailureState::sibling_admission(SiblingAdmissionCase::LostAgent).await;
}

#[tokio::test]
async fn terminal_failure_cannot_discard_an_accepted_agent_control() {
    StreamFailureState::sibling_admission(SiblingAdmissionCase::CorruptAgent).await;
}

#[tokio::test]
async fn accepted_input_settlement_survives_its_original_lost_admission_receipt() {
    StreamFailureState::sibling_admission(SiblingAdmissionCase::LostInput).await;
}

impl StreamFailureState {
    async fn attempt_authority_read(case: AttemptReadCase) {
        let (faults, store) = StorageFaults::open().await.unwrap();
        let native = store.inner.clone();
        let (state, agent) = Self::open().unwrap();
        let unsubmitted = matches!(case, AttemptReadCase::Unsubmitted { .. });
        let preparation = matches!(case, AttemptReadCase::Preparation { .. });
        let preparation_cancel = matches!(case, AttemptReadCase::PreparationWake { .. });
        let prompt_case = matches!(
            case,
            AttemptReadCase::BeforePrompt { .. }
                | AttemptReadCase::PromptWake { .. }
                | AttemptReadCase::NotificationDelivery { .. }
                | AttemptReadCase::PromptFailure { .. }
        );
        let live_attempt = preparation || preparation_cancel || prompt_case;
        let prompt_entered = matches!(
            case,
            AttemptReadCase::PromptWake { .. }
                | AttemptReadCase::NotificationDelivery { .. }
                | AttemptReadCase::PromptFailure { .. }
        );
        let fault = case.fault();
        let recoverable = matches!(fault, AttemptReadFault::Recoverable);
        let held = matches!(fault, AttemptReadFault::Held);
        let notifications =
            (live_attempt && !preparation_cancel).then(|| NotificationSequence::new(1, false));
        *state.notifications.lock().unwrap() = notifications.clone();
        let mut source: Value = serde_json::from_str(STREAM_FAILURE).unwrap();
        source["nodes"][0]["deadline_ms"] = Value::from(60_000);
        if !unsubmitted && !live_attempt {
            source["nodes"][0]["retry"] = serde_json::json!({
                "kind":"fixed", "max_attempts":2, "max_elapsed_ms":60_000, "delay_ms":2_000,
            });
        }
        let mut app = App::open(AppConfig::new(
            store,
            vec![AgentSpec::new("faulty".to_owned(), "v1".to_owned(), agent)],
            vec![WorkflowDefinition::parse(&source.to_string()).unwrap()],
        ))
        .await
        .unwrap();
        let (read_drop, dropped) = tokio::sync::oneshot::channel();
        *faults.attempt_read_dropped.lock().unwrap() = Some(read_drop);
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(39), async {
            let workspace = app.create_workspace(WorkspaceConfig {
                name: None, root: std::env::current_dir().unwrap(),
            }).await.unwrap();
            let workflow = workspace.workflow("stream-failure".parse().unwrap()).await.unwrap();
            let prior_lookups = state.workspace_lookups.load(Ordering::SeqCst);
            if !live_attempt { *faults.preparation.lock().unwrap() = StatePreparationFault::FailAfterIntent; }
            if preparation_cancel {
                // This real lower read precedes conversation creation. Its
                // original preparation owner must observe admitted cancellation.
                *state.preparation_fault.lock().unwrap() = PreparationFault::WorkspaceStall;
                *state.read_authority.lock().unwrap() = Some((Arc::clone(&faults), case));
            } else if matches!(case, AttemptReadCase::NotificationDelivery { .. } | AttemptReadCase::PromptFailure { .. }) {
                *state.read_authority.lock().unwrap() = Some((Arc::clone(&faults), case));
            } else {
                *faults.attempt_read.lock().unwrap() = Some(case);
            }
            let run = workflow.start(ExecutionOptions::default()).await.unwrap();
            if preparation_cancel {
                tokio::time::timeout(Duration::from_secs(2), state.preparation_entered.notified())
                    .await.expect("original preparation enters the lower workspace lookup");
                assert!(faults.attempt_owner.lock().unwrap().is_some(), "the held authority read must belong to the original lower preparation task");
                let current = run.snapshot().await.unwrap();
                assert!(matches!(current.status, RunStatus::Running));
                assert!(current.interactions.is_empty());
                *faults.attempt_read.lock().unwrap() = Some(case);
                let cancelled = run.cancel(current.checkpoint).await.unwrap();
                assert!(matches!(cancelled.status, RunStatus::Cancelling));
            }
            if !live_attempt {
                tokio::time::timeout(Duration::from_secs(2), faults.preparation_entered.notified())
                    .await.expect("original preparation fails before creating a lower conversation");
                if !unsubmitted {
                    tokio::time::timeout(Duration::from_secs(2), faults.run_transition_observed.notified())
                        .await.expect("the original known-no-effect failure admits its native retry transition");
                }
                // Settlement authority precedes Stage; the Retry cases have
                // already reached that mutation. Release the original gate.
                faults.release_run_transition();
            }
            tokio::time::timeout(Duration::from_secs(4), faults.attempt_read_entered.notified())
                .await.expect("the original attempt reaches its selected native authority phase");
            let (head, checkpoint) = faults.attempt_read_records.lock().unwrap().clone()
                .expect("selected native attempt authority");
            assert_eq!(checkpoint.value["id"], run.id().to_string());
            let logical = StorageFaults::logical_checkpoint(&native, &checkpoint).await;
            let controls = logical.pointer("/checkpoint/controls").unwrap().as_object().unwrap();
            assert_eq!(controls.len(), 1);
            assert_eq!(controls.values().next().unwrap().pointer("/state/phase/state").unwrap(),
                case.native_phase());
            if matches!(case, AttemptReadCase::PromptWake { .. }) {
                let control = controls.values().next().unwrap();
                let targets = logical["checkpoint"]["settlements"].as_array().unwrap();
                assert_eq!(targets.len(), 1);
                let target = &targets[0];
                assert_eq!(target[2], control["sequence"]);
                assert_eq!(target[3][0], 1);
                assert_eq!(target[4][0], 0);
                assert_eq!(target[3][2], NotificationSequence::id(0).as_str());
                assert_eq!(control["state"]["phase"]["notification_id"], target[3][2]);
            }
            assert_eq!(checkpoint.value.pointer("/checkpoint/state/answer").unwrap(), "");
            assert!(checkpoint.value.pointer("/checkpoint/outcomes").unwrap().as_array().unwrap().is_empty());
            assert!(checkpoint.value.pointer("/checkpoint/position").unwrap().is_null());
            assert_eq!(faults.agent_intents.load(Ordering::SeqCst), 1);
            if !prompt_case {
                assert_eq!(state.workspace_lookups.load(Ordering::SeqCst), prior_lookups + usize::from(preparation_cancel));
            }
            if preparation_cancel {
                assert_eq!(checkpoint.value["checkpoint"]["phase"]["state"], "cancelling");
            }
            assert_eq!(state.conversations.load(Ordering::SeqCst), usize::from(prompt_case));
            assert_eq!(state.prompts.load(Ordering::SeqCst), usize::from(prompt_entered));
            if let Some(notifications) = &notifications {
                assert_eq!(notifications.prompt_returns.load(Ordering::SeqCst), 0);
                assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), 0);
            }
            if matches!(case, AttemptReadCase::NotificationDelivery { .. } | AttemptReadCase::PromptFailure { .. }) {
                assert_eq!(logical["checkpoint"]["settlements"], serde_json::json!([]));
                let notifications = notifications.as_ref().unwrap();
                assert_eq!(notifications.received.load(Ordering::SeqCst),
                    usize::from(matches!(case, AttemptReadCase::NotificationDelivery { .. })));
            }
            if recoverable && matches!(case, AttemptReadCase::PromptFailure { .. }) {
                faults.attempt_read_release.notify_one();
                let unknown = tokio::time::timeout(Duration::from_secs(3), async {
                    loop {
                        let snapshot = run.snapshot().await.unwrap();
                        if matches!(snapshot.interactions.as_slice(), [Interaction::Unknown { retry_safe: true, .. }]) {
                            break snapshot;
                        }
                        tokio::task::yield_now().await;
                    }
                }).await.expect("the original lower failure retains exact Unknown until actual abort joins");
                assert_eq!(unknown.state.values()["answer"], "");
                assert!(unknown.outcomes.is_empty() && unknown.last_action_ordinal.is_none());
                let [Interaction::Unknown { sequence, generation, .. }] = unknown.interactions.as_slice() else { unreachable!() };
                run.resolve(*sequence, *generation, Resolution::Fail).await.unwrap();
                let failed = tokio::time::timeout(Duration::from_secs(3), run.wait()).await.unwrap().unwrap();
                assert!(matches!(failed.status, RunStatus::Failed(WorkflowFailure::Agent(_))));
                assert_eq!(failed.state.values()["answer"], "");
                assert!(failed.outcomes.is_empty() && failed.last_action_ordinal.is_none());
                assert_eq!(state.aborts.load(Ordering::SeqCst), 1);
                app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await.unwrap();
            } else if recoverable && preparation_cancel {
                faults.attempt_read_release.notify_one();
                let cancelled = tokio::time::timeout(Duration::from_secs(3), run.wait())
                    .await.expect("original unsubmitted preparation observes real cancellation").unwrap();
                assert!(matches!(cancelled.status, RunStatus::Cancelled));
                assert_eq!(cancelled.state.values()["answer"], "");
                assert!(cancelled.outcomes.is_empty() && cancelled.last_action_ordinal.is_none());
                assert!(cancelled.interactions.is_empty());
                app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await.unwrap();
            } else if recoverable && live_attempt {
                // The same selected read held its recoverable return long enough
                // to prove the exact lower-call boundary, then retries in place.
                faults.attempt_read_release.notify_one();
                let waiting = tokio::time::timeout(Duration::from_secs(3), async {
                    loop {
                        let snapshot = run.snapshot().await.unwrap();
                        match snapshot.interactions.as_slice() {
                            [Interaction::Notification { .. }] => break snapshot,
                            [] => tokio::task::yield_now().await,
                            _ => panic!("recoverable prompt authority cannot discard its original attempt"),
                        }
                    }
                }).await.expect("the original prompt still owns its real notification");
                let [Interaction::Notification { sequence, generation, notification }] = waiting.interactions.as_slice()
                    else { unreachable!() };
                assert_eq!(notification.id(), &NotificationSequence::id(0));
                run.resolve(*sequence, *generation, Resolution::Notification(NotificationResolution::Allow)).await.unwrap();
                let finished = tokio::time::timeout(Duration::from_secs(3), run.wait()).await.unwrap().unwrap();
                assert!(matches!(finished.status, RunStatus::Succeeded));
                assert_eq!(finished.state.values()["answer"], "both approvals accepted");
                assert_eq!(finished.last_action_ordinal, Some(0));
                assert_eq!(finished.outcomes.len(), 1);
                let notifications = notifications.as_ref().unwrap();
                assert_eq!(notifications.resolution_calls.load(Ordering::SeqCst), 1);
                assert_eq!(notifications.resolution_returns.load(Ordering::SeqCst), 1);
                assert_eq!(notifications.prompt_returns.load(Ordering::SeqCst), 1);
                app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await.unwrap();
            } else if recoverable {
                let failed = tokio::time::timeout(Duration::from_secs(3), async {
                    loop {
                        let snapshot = run.snapshot().await.unwrap();
                        if matches!(snapshot.status, RunStatus::Failed(_)) { break snapshot; }
                        assert!(snapshot.interactions.is_empty(),
                            "a proved unsubmitted attempt cannot become lower uncertainty after a read retry");
                        tokio::task::yield_now().await;
                    }
                }).await.expect("recoverable authority preserves the original known-no-effect failure owner");
                assert!(matches!(failed.status, RunStatus::Failed(WorkflowFailure::Agent(_))));
                assert_eq!(failed.state.values()["answer"], "");
                assert!(failed.outcomes.is_empty() && failed.last_action_ordinal.is_none());
                app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await.unwrap();
            } else {
            tokio::time::timeout(Duration::from_secs(if held { 33 } else { 3 }), async {
                loop {
                    match app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await {
                        Err(AppError::Closed(_)) => break,
                        Ok(_) => tokio::task::yield_now().await,
                        Err(error) => panic!("unreadable attempt authority must close the App: {error:?}"),
                    }
                }
            }).await.expect("the original attempt read has a finite fatal disposition");
            let current_head = native.load(head.key.clone()).await.unwrap().unwrap().record;
            if current_head.value != head.value {
                let scope = head.key.key().strip_prefix("run/").unwrap();
                let sequence = current_head.value["sequence"].as_u64().unwrap();
                let current_checkpoint = native.load(StateKey::new("app-sdk",
                    format!("history/{scope}/checkpoint/{sequence:020}")).unwrap()).await.unwrap().unwrap().record;
                eprintln!("authority head advanced: checkpoint={}, conversations={}, prompts={}, aborts={}",
                    current_checkpoint.value, state.conversations.load(Ordering::SeqCst),
                    state.prompts.load(Ordering::SeqCst), state.aborts.load(Ordering::SeqCst));
            }
            assert_eq!(current_head.value, head.value);
            }
            assert!(tokio::time::timeout(Duration::from_secs(1), dropped).await
                .expect("the original read is destroyed before authority disposition").is_err());
            assert_eq!(native.load(checkpoint.key.clone()).await.unwrap().unwrap().record.value, checkpoint.value);
            assert_eq!(faults.agent_intents.load(Ordering::SeqCst), 1);
            assert_eq!(state.conversations.load(Ordering::SeqCst), usize::from(prompt_case || preparation && recoverable));
            assert_eq!(state.prompts.load(Ordering::SeqCst), usize::from(prompt_entered || recoverable && live_attempt && !preparation_cancel));
        })).catch_unwind().await;
        state.read_authority.lock().unwrap().take();
        *faults.attempt_read.lock().unwrap() = None;
        faults.attempt_owner.lock().unwrap().take();
        faults.attempt_read_dropped.lock().unwrap().take();
        faults.attempt_read_release.notify_one();
        *state.preparation_fault.lock().unwrap() = PreparationFault::Pass;
        state.preparation_release.notify_one();
        faults.release_run_transition();
        if let Some(notifications) = &notifications {
            notifications.cancelled.store(true, Ordering::SeqCst);
            notifications.changed.notify_waiters();
            notifications.resolution_release.notify_one();
            notifications.next_target_release.notify_one();
        }
        let cleanup = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        ))
        .catch_unwind()
        .await;
        drop(app);
        match checked {
            Ok(result) => result.expect("attempt authority case stays bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        let cleanup = cleanup
            .expect("attempt cleanup does not panic")
            .expect("attempt cleanup joins");
        if recoverable {
            cleanup.unwrap();
        } else {
            assert!(matches!(cleanup, Err(AppError::Integration(_))));
        }
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn permanent_retry_authority_before_wake_closes_without_another_attempt() {
    StreamFailureState::attempt_authority_read(AttemptReadCase::BeforeWake { held: false }).await;
}

#[tokio::test]
async fn held_retry_authority_before_wake_is_destroyed_at_its_read_bound() {
    StreamFailureState::attempt_authority_read(AttemptReadCase::BeforeWake { held: true }).await;
}

#[tokio::test]
async fn permanent_retry_authority_after_wake_closes_without_another_attempt() {
    StreamFailureState::attempt_authority_read(AttemptReadCase::AfterWake { held: false }).await;
}

#[tokio::test]
async fn held_retry_authority_after_wake_is_destroyed_at_its_read_bound() {
    StreamFailureState::attempt_authority_read(AttemptReadCase::AfterWake { held: true }).await;
}

#[tokio::test]
async fn unsubmitted_failure_retries_authority_without_becoming_unknown() {
    StreamFailureState::attempt_authority_read(AttemptReadCase::Unsubmitted {
        fault: AttemptReadFault::Recoverable,
    })
    .await;
}

#[tokio::test]
async fn unsubmitted_failure_permanent_authority_closes_without_another_attempt() {
    StreamFailureState::attempt_authority_read(AttemptReadCase::Unsubmitted {
        fault: AttemptReadFault::Permanent,
    })
    .await;
}

#[tokio::test]
async fn unsubmitted_failure_held_authority_is_destroyed_at_its_read_bound() {
    StreamFailureState::attempt_authority_read(AttemptReadCase::Unsubmitted {
        fault: AttemptReadFault::Held,
    })
    .await;
}

#[tokio::test]
async fn prepared_admission_retries_one_real_lance_conflict_with_the_original_intent() {
    StorageFaults::prepared_admission_conflicts(1).await;
}

#[tokio::test]
async fn prepared_admission_exhausts_real_lance_conflicts_without_publishing_and_then_progresses() {
    StorageFaults::prepared_admission_conflicts(8).await;
}

impl StorageFaults {
    async fn prepared_admission_conflicts(conflicts: usize) {
        let (faults, store) = Self::open().await.unwrap();
        let native = store.inner.clone();
        let mut app = App::open(AppConfig::<FaultStateStore, Agent>::new(
            store,
            Vec::new(),
            vec![WorkflowDefinition::parse(LINEAR).unwrap()],
        ))
        .await
        .unwrap();
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(30), async {
            let workspace = app.create_workspace(WorkspaceConfig {
                name: Some("prepared-admission-contention".into()),
                root: std::env::current_dir().unwrap(),
            }).await.unwrap();
            let revision = workspace.snapshot().await.unwrap().revision;
            let workflow = workspace.workflow("linear".parse().unwrap()).await.unwrap();
            let state = WorkflowState::new(Map::from_iter([
                ("case".into(), Value::from("cancellation")),
                ("threshold".into(), Value::from(17)),
            ])).unwrap();
            let before = native.list(StateListRequest::new("app-sdk").unwrap()).await.unwrap();
            assert!(before.next_start_after.is_none());
            faults.observe_admission_commits(conflicts);
            let mut admission = Box::pin(workflow.prepare(state.clone()));
            for attempt in 0..conflicts {
                tokio::time::timeout(Duration::from_secs(5), async {
                    tokio::select! {
                        _ = faults.admission_commit_entered.notified() => {},
                        result = &mut admission => panic!("admission returned before held attempt {attempt}: {}", result.is_ok()),
                    }
                }).await.expect("the original admission reaches each consuming commit");
                // The admitted transaction already owns an immutable Lance
                // snapshot. An unrelated real publication makes it stale.
                let mut competing = native.begin_transaction().await.unwrap();
                competing.put(StateRecord::new(
                    StateKey::new("admission-test", "competitor").unwrap(),
                    Value::from(attempt),
                )).await.unwrap();
                competing.commit().await.unwrap();
                faults.release_admission_commit();
            }
            let result = admission.await;
            assert_eq!(faults.admission_commit_holds.load(Ordering::SeqCst), 0);
            let (original_id, original_occurrence) = {
                let attempts = faults.admission_attempts.lock().unwrap();
                let attempts = attempts.as_ref().unwrap();
                assert_eq!(attempts.len(), if conflicts == 1 { 2 } else { 8 });
                let first = &attempts[0].record;
                for (index, attempt) in attempts.iter().enumerate() {
                    assert_eq!(attempt.record.key, first.key);
                    assert_eq!(attempt.record.value["id"], first.value["id"]);
                    assert_eq!(attempt.record.value["occurrence"], first.value["occurrence"]);
                    assert_eq!(attempt.record.value["checkpoint"]["state"], first.value["checkpoint"]["state"]);
                    assert_eq!(attempt.record.value["checkpoint"]["sequence"], 0);
                    if index < conflicts {
                        assert!(matches!(attempt.outcome, Err(StateStoreError::TransactionConflict { .. })),
                            "the supplied Lance transaction itself rejects each stale admission");
                    } else {
                        assert!(attempt.outcome.is_ok());
                    }
                }
                (first.value["id"].as_str().unwrap().to_owned(),
                 first.value["occurrence"].as_str().unwrap().to_owned())
            };
            let run = if conflicts == 1 {
                let run = result.expect("one proven nonpublication permits the existing internal retry");
                assert_eq!(run.id().to_string(), original_id);
                assert_eq!(run.snapshot().await.unwrap().occurrence, original_occurrence);
                run
            } else {
                assert!(matches!(result, Err(AppError::Conflict(_))), "the existing bounded retry budget is exhausted");
                assert!(workflow.list_runs(None, NonZeroUsize::new(16).unwrap()).await.unwrap().is_empty());
                let after = native.list(StateListRequest::new("app-sdk").unwrap()).await.unwrap();
                assert!(after.next_start_after.is_none());
                assert_eq!(after.records, before.records,
                    "rejected admission leaves no root, checkpoint, receipt or capacity increment");
                let run = workflow.prepare(state).await.expect("a new caller operation progresses after contention ends");
                assert_ne!(run.id().to_string(), original_id);
                run
            };
            let snapshot = run.snapshot().await.unwrap();
            assert_eq!(snapshot.checkpoint, 0);
            assert!(matches!(snapshot.status, RunStatus::Paused));
            assert!(snapshot.interactions.is_empty() && snapshot.outcomes.is_empty());
            assert!(snapshot.last_action_ordinal.is_none());
            assert_eq!(snapshot.state.values()["case"], "cancellation");
            assert_eq!(snapshot.state.values()["threshold"], 17);
            let inventory = workflow.list_runs(None, NonZeroUsize::new(16).unwrap()).await.unwrap();
            assert_eq!(inventory.len(), 1);
            assert_eq!(&inventory[0].id, run.id());
            assert_eq!(workspace.snapshot().await.unwrap().revision, revision);
            assert_eq!(native.load(StateKey::new("app-sdk", "app").unwrap()).await.unwrap().unwrap().record.value["run_count"], 1);
            assert_eq!(faults.agent_intents.load(Ordering::SeqCst), 0);
        })).catch_unwind().await;
        faults.admission_commit_holds.store(0, Ordering::SeqCst);
        faults.release_admission_commit();
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
            Ok(result) => result.expect("bounded real-conflict admission scenario"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup
            .expect("admission cleanup does not panic")
            .expect("admission cleanup remains bounded")
            .expect("admission owners join after the test");
        assert_eq!(
            native
                .load(StateKey::new("app-sdk", "app").unwrap())
                .await
                .unwrap()
                .unwrap()
                .record
                .value["run_count"],
            0
        );
    }
}

#[tokio::test]
async fn sixteen_concurrent_prepares_admit_distinct_paused_roots_without_effects() {
    let (faults, store) = StorageFaults::open().await.unwrap();
    let native = store.inner.clone();
    let mut app = App::open(AppConfig::<FaultStateStore, Agent>::new(
        store,
        Vec::new(),
        vec![WorkflowDefinition::parse(LINEAR).unwrap()],
    ))
    .await
    .unwrap();
    let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(15), async {
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: Some("concurrent-prepare-capacity".into()),
                root: std::env::current_dir().unwrap(),
            })
            .await
            .unwrap();
        let revision = workspace.snapshot().await.unwrap().revision;
        let workflow = workspace.workflow("linear".parse().unwrap()).await.unwrap();
        faults.observe_admission_commits(0);
        let prepared = futures_util::future::join_all((0..16).map(|value| {
            let workflow = &workflow;
            async move {
                let run = workflow
                    .prepare(
                        WorkflowState::new(Map::from_iter([
                            ("case".into(), Value::from("cancellation")),
                            ("threshold".into(), Value::from(value)),
                        ]))
                        .unwrap(),
                    )
                    .await;
                (value, run)
            }
        }))
        .await;
        let mut ids = std::collections::BTreeSet::new();
        let mut runs = Vec::new();
        let mut rejected = Vec::new();
        let mut unexpected = Vec::new();
        for (value, run) in prepared {
            match run {
                Ok(run) => runs.push((value, run)),
                Err(AppError::Conflict(_)) => rejected.push(value),
                Err(error) => unexpected.push((value, error)),
            }
        }
        assert_eq!(runs.len() + rejected.len() + unexpected.len(), 16);
        assert!(unexpected.is_empty(), "unexpected concurrent prepare failures: {unexpected:?}");
        for (value, run) in &runs {
            assert!(ids.insert(run.id().clone()));
            let snapshot = run.snapshot().await.unwrap();
            assert_eq!(snapshot.checkpoint, 0);
            assert!(matches!(snapshot.status, RunStatus::Paused));
            assert!(snapshot.interactions.is_empty() && snapshot.outcomes.is_empty());
            assert!(snapshot.last_action_ordinal.is_none());
            assert_eq!(snapshot.state.values()["threshold"], *value);
            assert_eq!(snapshot.state.values()["case"], "cancellation");
        }
        // Account for every first caller operation before authoring any new
        // operation. Only genuine consuming Lance outcomes enter this recorder.
        let (published, rejected_records, native_conflicts) = {
            let attempts = faults.admission_attempts.lock().unwrap();
            let attempts = attempts.as_ref().unwrap();
            let mut published = std::collections::BTreeSet::new();
            let mut rejected_records = Vec::new();
            let mut native_conflicts = 0;
            for value in &rejected {
                let selected = attempts.iter().filter(|attempt| {
                    attempt.record.value["checkpoint"]["state"]["threshold"].as_i64()
                        == Some(*value)
                }).collect::<Vec<_>>();
                assert_eq!(selected.len(), 8,
                    "resubmission requires measured exhaustion of eight consuming native attempts");
                let first = &selected[0].record;
                for attempt in &selected {
                    assert!(matches!(attempt.outcome, Err(StateStoreError::TransactionConflict { .. })),
                        "only proven native nonpublication permits the fresh caller operation");
                    assert_eq!(attempt.record.key, first.key);
                    assert_eq!(attempt.record.value["id"], first.value["id"]);
                    assert_eq!(attempt.record.value["occurrence"], first.value["occurrence"]);
                    assert_eq!(attempt.record.value["checkpoint"]["state"], first.value["checkpoint"]["state"]);
                }
            }
            for attempt in attempts {
                let value = attempt.record.value["checkpoint"]["state"]["threshold"]
                    .as_i64()
                    .expect("each admission retains its original test input");
                match &attempt.outcome {
                    Ok(()) => {
                        assert!(published.insert(attempt.record.value["id"].as_str().unwrap().to_owned()),
                            "one logical admission cannot publish twice");
                        assert!(!rejected.contains(&value),
                            "a caller conflict must not conceal its published admission");
                    }
                    Err(StateStoreError::TransactionConflict { .. }) => {
                        native_conflicts += 1;
                        if rejected.contains(&value) {
                            rejected_records.push(attempt.record.key.clone());
                        }
                    }
                    Err(error) => panic!("uncertain native admission cannot authorize resubmission: {error:?}"),
                }
            }
            (published, rejected_records, native_conflicts)
        };
        assert_eq!(published, ids.iter().map(ToString::to_string).collect());
        for key in rejected_records {
            assert!(native.load(key).await.unwrap().is_none(),
                "a rejected native admission leaves its checkpoint absent");
        }
        let inventory = workflow
            .list_runs(None, NonZeroUsize::new(32).unwrap())
            .await
            .unwrap();
        assert_eq!(inventory.len(), runs.len());
        assert_eq!(
            inventory
                .iter()
                .map(|row| row.id.clone())
                .collect::<std::collections::BTreeSet<_>>(),
            ids
        );
        assert_eq!(
            inventory.iter().map(|row| row.state.values()["threshold"].as_i64().unwrap())
                .collect::<std::collections::BTreeSet<_>>(),
            runs.iter().map(|(value, _)| *value).collect(),
            "only successful logical inputs exist before any caller resubmission",
        );
        assert!(
            inventory
                .iter()
                .all(|row| row.checkpoint == 0 && matches!(row.status, RunStatus::Paused))
        );
        assert_eq!(workspace.snapshot().await.unwrap().revision, revision);
        let application = native
            .load(StateKey::new("app-sdk", "app").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            application.record.value["run_count"], runs.len(),
            "each successful original prepared root owns exactly one capacity slot"
        );
        assert_eq!(faults.agent_intents.load(Ordering::SeqCst), 0);
        eprintln!("concurrent prepare initial outcomes: successful={} rejected={} native_conflicts={native_conflicts}",
            runs.len(), rejected.len());
        // The public inventory and native outcomes establish nonpublication.
        // These are fresh caller operations after contention has settled, never
        // replay of an acknowledged or uncertain operation.
        for value in rejected {
            let run = workflow.prepare(WorkflowState::new(Map::from_iter([
                ("case".into(), Value::from("cancellation")),
                ("threshold".into(), Value::from(value)),
            ])).unwrap()).await.expect("one fresh caller operation progresses without contention");
            assert!(ids.insert(run.id().clone()));
            runs.push((value, run));
        }
        assert_eq!(runs.len(), 16);
        for (value, run) in &runs {
            let snapshot = run.snapshot().await.unwrap();
            assert_eq!(snapshot.checkpoint, 0);
            assert!(matches!(snapshot.status, RunStatus::Paused));
            assert!(snapshot.interactions.is_empty() && snapshot.outcomes.is_empty());
            assert!(snapshot.last_action_ordinal.is_none());
            assert_eq!(snapshot.state.values()["threshold"], *value);
            assert_eq!(snapshot.state.values()["case"], "cancellation");
        }
        let inventory = workflow.list_runs(None, NonZeroUsize::new(32).unwrap()).await.unwrap();
        assert_eq!(inventory.len(), 16);
        assert_eq!(inventory.iter().map(|row| row.id.clone()).collect::<std::collections::BTreeSet<_>>(), ids);
        assert_eq!(inventory.iter().map(|row| row.state.values()["threshold"].as_i64().unwrap())
            .collect::<std::collections::BTreeSet<_>>(), (0..16).collect());
        assert_eq!(workspace.snapshot().await.unwrap().revision, revision);
        assert_eq!(native.load(StateKey::new("app-sdk", "app").unwrap()).await.unwrap().unwrap().record.value["run_count"], 16);
        assert_eq!(faults.agent_intents.load(Ordering::SeqCst), 0);
    }))
    .catch_unwind()
    .await;
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
        Ok(result) => result.expect("bounded concurrent prepare workload"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    cleanup
        .expect("concurrent prepare cleanup does not panic")
        .expect("concurrent prepare cleanup stays bounded")
        .expect("all originally admitted roots join cleanup");
    let application = native
        .load(StateKey::new("app-sdk", "app").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(application.record.value["run_count"], 0);
}

#[tokio::test]
async fn recoverable_before_prompt_authority_preserves_the_original_attempt_owner() {
    StreamFailureState::attempt_authority_read(AttemptReadCase::BeforePrompt {
        fault: AttemptReadFault::Recoverable,
    })
    .await;
}

#[tokio::test]
async fn permanent_before_prompt_authority_preserves_the_original_attempt_owner() {
    StreamFailureState::attempt_authority_read(AttemptReadCase::BeforePrompt {
        fault: AttemptReadFault::Permanent,
    })
    .await;
}

#[tokio::test]
async fn held_before_prompt_authority_preserves_the_original_attempt_owner() {
    StreamFailureState::attempt_authority_read(AttemptReadCase::BeforePrompt {
        fault: AttemptReadFault::Held,
    })
    .await;
}

#[tokio::test]
async fn recoverable_prompt_wake_authority_preserves_the_original_attempt_owner() {
    StreamFailureState::attempt_authority_read(AttemptReadCase::PromptWake {
        fault: AttemptReadFault::Recoverable,
    })
    .await;
}

#[tokio::test]
async fn permanent_prompt_wake_authority_preserves_the_original_attempt_owner() {
    StreamFailureState::attempt_authority_read(AttemptReadCase::PromptWake {
        fault: AttemptReadFault::Permanent,
    })
    .await;
}

#[tokio::test]
async fn held_prompt_wake_authority_preserves_the_original_attempt_owner() {
    StreamFailureState::attempt_authority_read(AttemptReadCase::PromptWake {
        fault: AttemptReadFault::Held,
    })
    .await;
}

#[derive(Clone, Copy)]
enum NativeAdmissionFenceCase {
    Generation,
    Shutdown,
}

impl StorageFaults {
    async fn native_admission_fence(case: NativeAdmissionFenceCase) {
        let (faults, store) = Self::open().await.unwrap();
        let native = store.inner.clone();
        let mut app = App::open(AppConfig::<FaultStateStore, Agent>::new(
            store,
            Vec::new(),
            vec![WorkflowDefinition::parse(LINEAR).unwrap()],
        ))
        .await
        .unwrap();
        let mut fresh = None;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(12), async {
            let workspace = app.create_workspace(WorkspaceConfig {
                name:None, root:std::env::current_dir().unwrap(),
            }).await.unwrap();
            let workflow = workspace.workflow("linear".parse().unwrap()).await.unwrap();
            let app_key = StateKey::new("app-sdk", "app").unwrap();
            let before = native.load(app_key.clone()).await.unwrap().unwrap().record;
            assert_eq!(before.value["run_count"], 0);
            faults.arm_app_store_admission_monitor();
            let mut admission = Box::pin(workflow.prepare(WorkflowState::default()));
            tokio::time::timeout(Duration::from_secs(3), async {
                tokio::select! {
                    _ = faults.run_transition_observed.notified() => {},
                    result = &mut admission => panic!("original prepared admission did not reach its native transaction boundary: {}", result.is_ok()),
                }
            }).await.expect("the old admission builds its original candidate before the native transaction");
            let fenced = match case {
                NativeAdmissionFenceCase::Generation => {
                    fresh = Some(App::open(AppConfig::<FaultStateStore, Agent>::new(
                        FaultStateStore { inner:native.clone(), faults:Arc::clone(&faults) },
                        Vec::new(), vec![WorkflowDefinition::parse(LINEAR).unwrap()],
                    )).await.unwrap());
                    let current = native.load(app_key.clone()).await.unwrap().unwrap().record;
                    assert!(current.value["generation"].as_u64().unwrap() > before.value["generation"].as_u64().unwrap());
                    assert_eq!(current.value.pointer("/shutdown/state").unwrap(), "active");
                    current
                }
                NativeAdmissionFenceCase::Shutdown => {
                    assert!(matches!(app.shutdown(Shutdown::CancelAndDrain {
                        timeout:Duration::from_millis(50),
                    }).await, Err(AppError::Timeout(_))),
                        "the finalizer retains the already accepted admission while its native transaction is held");
                    tokio::time::timeout(Duration::from_secs(3), async {
                        loop {
                            let current = native.load(app_key.clone()).await.unwrap().unwrap().record;
                            if current.value.pointer("/shutdown/state").is_some_and(|state| state == "cancelling") { break current; }
                            tokio::task::yield_now().await;
                        }
                    }).await.expect("public shutdown seals native admission before releasing the old candidate")
                }
            };
            assert_eq!(fenced.value["run_count"], 0);
            let heads = native.list(StateListRequest::new("app-sdk").unwrap()
                .with_key_prefix(StateKeyPrefix::new("run/").unwrap()).with_limit(1).unwrap()).await.unwrap();
            assert!(heads.records.is_empty());
            faults.app_store_admission_monitor.store(false, Ordering::SeqCst);
            faults.release_app_store_admission();
            assert!(matches!(tokio::time::timeout(Duration::from_secs(3), &mut admission).await
                .expect("the original native candidate observes the authoritative ownership fence"), Err(AppError::Closed(_))));
            let current = native.load(app_key).await.unwrap().unwrap().record;
            assert_eq!(current.value["generation"], fenced.value["generation"]);
            assert_eq!(current.value["run_count"], 0);
            match case {
                NativeAdmissionFenceCase::Generation => {
                    assert_eq!(current.value.pointer("/shutdown/state").unwrap(), "active");
                    fresh.as_ref().unwrap().list_workspaces(None, NonZeroUsize::new(1).unwrap()).await.unwrap();
                }
                NativeAdmissionFenceCase::Shutdown => assert!(matches!(current.value.pointer("/shutdown/state").and_then(Value::as_str), Some("cancelling" | "closed"))),
            }
            for prefix in ["run/", "active/"] {
                assert!(native.list(StateListRequest::new("app-sdk").unwrap()
                    .with_key_prefix(StateKeyPrefix::new(prefix).unwrap()).with_limit(1).unwrap())
                    .await.unwrap().records.is_empty(), "fenced admission cannot leave a native run or capacity marker");
            }
            assert_eq!(faults.agent_intents.load(Ordering::SeqCst), 0);
        })).catch_unwind().await;
        faults
            .app_store_admission_monitor
            .store(false, Ordering::SeqCst);
        faults.release_app_store_admission();
        let superseded = fresh.is_some();
        let fresh_cleanup = match fresh.as_mut() {
            Some(fresh) => Some(
                AssertUnwindSafe(tokio::time::timeout(
                    Duration::from_secs(5),
                    fresh.shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_secs(3),
                    }),
                ))
                .catch_unwind()
                .await,
            ),
            None => None,
        };
        drop(fresh);
        let cleanup = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        ))
        .catch_unwind()
        .await;
        drop(app);
        match checked {
            Ok(result) => result.expect("native admission ownership fence stays bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        if let Some(cleanup) = fresh_cleanup {
            cleanup
                .expect("new App cleanup does not panic")
                .expect("new App joins")
                .unwrap();
        }
        match cleanup
            .expect("original admission cleanup does not panic")
            .expect("original admission joins")
        {
            Ok(()) => {}
            Err(AppError::Closed(_)) if superseded => {}
            Err(error) => panic!("original admission cleanup failed: {error:?}"),
        }
        assert_eq!(
            native
                .load(StateKey::new("app-sdk", "app").unwrap())
                .await
                .unwrap()
                .unwrap()
                .record
                .value["run_count"],
            0
        );
    }
}

#[tokio::test]
async fn prepared_native_admission_cannot_cross_a_new_app_generation() {
    StorageFaults::native_admission_fence(NativeAdmissionFenceCase::Generation).await;
}

#[tokio::test]
async fn prepared_native_admission_cannot_cross_an_accepted_shutdown_seal() {
    StorageFaults::native_admission_fence(NativeAdmissionFenceCase::Shutdown).await;
}

#[tokio::test]
async fn recoverable_preparation_authority_retains_its_original_unsubmitted_attempt() {
    StreamFailureState::attempt_authority_read(AttemptReadCase::Preparation {
        fault: AttemptReadFault::Recoverable,
    })
    .await;
}

#[tokio::test]
async fn permanent_preparation_authority_closes_before_lower_conversation_creation() {
    StreamFailureState::attempt_authority_read(AttemptReadCase::Preparation {
        fault: AttemptReadFault::Permanent,
    })
    .await;
}

#[tokio::test]
async fn held_preparation_authority_expires_before_lower_conversation_creation() {
    StreamFailureState::attempt_authority_read(AttemptReadCase::Preparation {
        fault: AttemptReadFault::Held,
    })
    .await;
}

#[tokio::test]
async fn recoverable_preparation_wake_authority_retains_admitted_cancellation() {
    StreamFailureState::attempt_authority_read(AttemptReadCase::PreparationWake {
        fault: AttemptReadFault::Recoverable,
    })
    .await;
}

#[tokio::test]
async fn permanent_preparation_wake_authority_closes_without_lower_effects() {
    StreamFailureState::attempt_authority_read(AttemptReadCase::PreparationWake {
        fault: AttemptReadFault::Permanent,
    })
    .await;
}

#[tokio::test]
async fn held_preparation_wake_authority_expires_without_lower_effects() {
    StreamFailureState::attempt_authority_read(AttemptReadCase::PreparationWake {
        fault: AttemptReadFault::Held,
    })
    .await;
}

impl StreamFailureState {
    async fn queued_preparation_authority(fault: Option<AttemptReadFault>) {
        let (state, agent) = Self::exclusive_workspace(true).unwrap();
        let (faults, store) = StorageFaults::open().await.unwrap();
        let native = store.inner.clone();
        let mut app = App::open(AppConfig::new(
            store,
            vec![AgentSpec::new("faulty".into(), "v1".into(), agent)],
            vec![
                Self::preparation_definition("faulty", "lease-first", 60_000).unwrap(),
                Self::preparation_definition("faulty", "lease-queued", 60_000).unwrap(),
            ],
        ))
        .await
        .unwrap();
        let fatal = matches!(
            fault,
            Some(AttemptReadFault::Permanent | AttemptReadFault::Held)
        );
        let monotonic_started = std::time::Instant::now();
        let wall_started = std::time::SystemTime::now();
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(40), async {
            let workspace = app.create_workspace(WorkspaceConfig {
                name:None, root:std::env::current_dir().unwrap(),
            }).await.unwrap();
            let first = workspace.workflow("lease-first".parse().unwrap()).await.unwrap()
                .start(ExecutionOptions::default()).await.unwrap();
            tokio::time::timeout(Duration::from_secs(2), state.preparation_entered.notified())
                .await.expect("first original lower creation holds the shared preparation lease");
            let queued = workspace.workflow("lease-queued".parse().unwrap()).await.unwrap()
                .start(ExecutionOptions::default()).await.unwrap();
            faults.wait_for_agent_intents(2).await;
            let pending = queued.snapshot().await.unwrap();
            assert!(matches!(pending.status, RunStatus::Running));
            assert!(pending.interactions.is_empty());
            assert!(tokio::time::timeout(Duration::from_millis(100), queued.wait()).await.is_err(),
                "original second admission remains queued while the first lower creation holds its lease");
            assert_eq!(state.conversations.load(Ordering::SeqCst), 0);
            assert_eq!(state.prompts.load(Ordering::SeqCst), 0);
            let (read_drop, dropped) = tokio::sync::oneshot::channel();
            if let Some(fault) = fault {
                *faults.attempt_read_dropped.lock().unwrap() = Some(read_drop);
                *faults.attempt_read.lock().unwrap() = Some(AttemptReadCase::PreparationQueued { fault });
            } else {
                drop(read_drop);
                tokio::time::sleep(Duration::from_secs(31)).await;
                let still_queued = queued.snapshot().await.unwrap();
                assert_eq!(still_queued.checkpoint, pending.checkpoint);
                assert_eq!(still_queued.state, pending.state);
                assert!(still_queued.outcomes == pending.outcomes);
                assert_eq!(still_queued.last_action_ordinal, pending.last_action_ordinal);
                assert!(matches!(still_queued.status, RunStatus::Running));
                assert_eq!(state.conversations.load(Ordering::SeqCst), 0);
                assert_eq!(state.prompts.load(Ordering::SeqCst), 0);
                assert!(state.workspace_claimed.load(Ordering::SeqCst));
                app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await.unwrap();
            }
            state.preparation_release.notify_one();
            if let Some(fault) = fault {
                tokio::time::timeout(Duration::from_secs(3), faults.attempt_read_entered.notified())
                    .await.expect("queued original attempt reaches authority after the held lease is released");
                let (head, checkpoint) = faults.attempt_read_records.lock().unwrap().clone().unwrap();
                assert_eq!(checkpoint.value["workflow_id"], "lease-queued");
                assert_eq!(checkpoint.value["id"], queued.id().to_string());
                assert_eq!(checkpoint.value["checkpoint"]["state"]["answer"], "");
                assert!(checkpoint.value["checkpoint"]["position"].is_null());
                assert_eq!(checkpoint.value["checkpoint"]["outcomes"], serde_json::json!([]));
                let controls = checkpoint.value["checkpoint"]["controls"].as_object().unwrap();
                assert_eq!(controls.len(), 1);
                assert_eq!(controls.values().next().unwrap()["state"]["phase"]["state"], "intent");
                // The first caller already created its own conversation; the
                // queued caller must retain its original pre-creation authority.
                assert_eq!(state.conversations.load(Ordering::SeqCst), 1);
                if matches!(fault, AttemptReadFault::Recoverable) {
                    faults.attempt_read_release.notify_one();
                } else {
                    tokio::time::timeout(Duration::from_secs(if matches!(fault, AttemptReadFault::Held) {33} else {3}), async {
                        loop {
                            match app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await {
                                Err(AppError::Closed(_)) => break,
                                Ok(_) => tokio::task::yield_now().await,
                                Err(error) => panic!("queued unreadable authority must revoke App ownership: {error:?}"),
                            }
                        }
                    }).await.expect("queued preparation authority has finite fatal disposition");
                    assert_eq!(native.load(head.key.clone()).await.unwrap().unwrap().record.value, head.value);
                }
                assert!(tokio::time::timeout(Duration::from_secs(1), dropped).await
                    .expect("the selected queued read is destroyed").is_err());
                assert_eq!(native.load(checkpoint.key.clone()).await.unwrap().unwrap().record.value, checkpoint.value);
            }
            if !fatal {
                let (first, queued) = tokio::time::timeout(Duration::from_secs(4), async {
                    (first.wait().await.unwrap(), queued.wait().await.unwrap())
                }).await.expect("the same two original preparations complete after lease release");
                for (label, completed) in [("first", first), ("queued", queued)] {
                    assert!(matches!(completed.status, RunStatus::Succeeded), "{label} preparation must succeed");
                    assert_eq!(completed.state.values()["answer"], "respond");
                    assert_eq!(completed.last_action_ordinal, Some(0));
                    assert_eq!(completed.outcomes.len(), 1);
                }
                assert_eq!(state.conversations.load(Ordering::SeqCst), 2);
                assert_eq!(state.prompts.load(Ordering::SeqCst), 2);
            } else {
                assert_eq!(state.conversations.load(Ordering::SeqCst), 1);
                assert!(state.prompts.load(Ordering::SeqCst) <= 1,
                    "only the first already-created lower conversation can have prompted");
            }
            assert_eq!(faults.agent_intents.load(Ordering::SeqCst), 2);
        })).catch_unwind().await;
        faults.attempt_read.lock().unwrap().take();
        faults.attempt_read_dropped.lock().unwrap().take();
        faults.attempt_read_release.notify_one();
        state.preparation_release.notify_one();
        faults.release_run_transition();
        let cleanup = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        ))
        .catch_unwind()
        .await;
        drop(app);
        if !matches!(&checked, Ok(Ok(()))) {
            eprintln!(
                "queued preparation failed: monotonic_elapsed={:?}, wall_elapsed={:?}, shutdowns={}, workspace_claimed={}",
                monotonic_started.elapsed(),
                wall_started.elapsed(),
                state.shutdowns.load(Ordering::SeqCst),
                state.workspace_claimed.load(Ordering::SeqCst)
            );
            match &cleanup {
                Ok(Ok(result)) => eprintln!("queued preparation cleanup: {result:?}"),
                Ok(Err(error)) => eprintln!("queued preparation cleanup wait: {error:?}"),
                Err(_) => eprintln!("queued preparation cleanup panicked"),
            }
        }
        match checked {
            Ok(result) => result.expect("queued preparation remains bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        let cleanup = cleanup
            .expect("queued preparation cleanup does not panic")
            .expect("queued preparation cleanup joins");
        if fatal {
            assert!(matches!(cleanup, Err(AppError::Integration(_))));
        } else {
            cleanup.unwrap();
        }
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
        assert!(!state.workspace_claimed.load(Ordering::SeqCst));
    }
}

#[tokio::test]
async fn recoverable_queued_preparation_authority_retains_the_original_waiter() {
    StreamFailureState::queued_preparation_authority(Some(AttemptReadFault::Recoverable)).await;
}

#[tokio::test]
async fn permanent_queued_preparation_authority_closes_before_second_lower_creation() {
    StreamFailureState::queued_preparation_authority(Some(AttemptReadFault::Permanent)).await;
}

#[tokio::test]
async fn held_queued_preparation_authority_expires_before_second_lower_creation() {
    StreamFailureState::queued_preparation_authority(Some(AttemptReadFault::Held)).await;
}

#[tokio::test]
async fn readable_preparation_does_not_charge_a_valid_lease_wait_to_its_read_budget() {
    StreamFailureState::queued_preparation_authority(None).await;
}

#[tokio::test]
async fn recoverable_delivered_notification_authority_preserves_its_original_owner() {
    StreamFailureState::attempt_authority_read(AttemptReadCase::NotificationDelivery {
        fault: AttemptReadFault::Recoverable,
    })
    .await;
}

#[tokio::test]
async fn permanent_delivered_notification_authority_preserves_its_original_owner() {
    StreamFailureState::attempt_authority_read(AttemptReadCase::NotificationDelivery {
        fault: AttemptReadFault::Permanent,
    })
    .await;
}

#[tokio::test]
async fn held_delivered_notification_authority_preserves_its_original_owner() {
    StreamFailureState::attempt_authority_read(AttemptReadCase::NotificationDelivery {
        fault: AttemptReadFault::Held,
    })
    .await;
}

#[tokio::test]
async fn recoverable_returned_lower_failure_authority_preserves_its_original_owner() {
    StreamFailureState::attempt_authority_read(AttemptReadCase::PromptFailure {
        fault: AttemptReadFault::Recoverable,
    })
    .await;
}

#[tokio::test]
async fn permanent_returned_lower_failure_authority_preserves_its_original_owner() {
    StreamFailureState::attempt_authority_read(AttemptReadCase::PromptFailure {
        fault: AttemptReadFault::Permanent,
    })
    .await;
}

#[tokio::test]
async fn held_returned_lower_failure_authority_preserves_its_original_owner() {
    StreamFailureState::attempt_authority_read(AttemptReadCase::PromptFailure {
        fault: AttemptReadFault::Held,
    })
    .await;
}

#[derive(Clone, Copy)]
enum NativeAdvanceFenceCase {
    Counts,
    Generation,
    Cancellation,
}

impl StorageFaults {
    async fn native_advance_fence(case: NativeAdvanceFenceCase) {
        let (faults, store) = Self::open().await.unwrap();
        let native = store.inner.clone();
        let mut app = App::open(AppConfig::<FaultStateStore, Agent>::new(
            store,
            Vec::new(),
            vec![WorkflowDefinition::parse(LINEAR).unwrap()],
        ))
        .await
        .unwrap();
        let mut fresh = None;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(15), async {
            let workspace = app.create_workspace(WorkspaceConfig {
                name: None, root: std::env::current_dir().unwrap(),
            }).await.unwrap();
            let workflow = workspace.workflow("linear".parse().unwrap()).await.unwrap();
            let mut run = workflow.prepare(WorkflowState::new(Map::from_iter([
                ("case".into(), Value::from("order")),
            ])).unwrap()).await.unwrap();
            let before = run.snapshot().await.unwrap();
            let observer = workflow.open_run(RunSelector::Exact {
                id: run.id().clone(), occurrence: before.occurrence.clone(),
            }).await.unwrap();
            assert_eq!(before.checkpoint, 0);
            assert!(before.outcomes.is_empty() && before.last_action_ordinal.is_none());
            let scope = format!("{}/{}/{}/{}", workspace.id(), workflow.id(), run.id(), before.occurrence);
            let key = StateKey::new("app-sdk", format!("run/{scope}")).unwrap();
            let checkpoint = native.load(StateKey::new("app-sdk", format!("history/{scope}/checkpoint/{:020}", 0)).unwrap())
                .await.unwrap().unwrap();
            let head = native.load(key.clone()).await.unwrap().unwrap();
            let workspace_key = StateKey::new("app-sdk", format!("workspace/{}", workspace.id())).unwrap();
            let original_workspace = native.load(workspace_key.clone()).await.unwrap().unwrap();
            let app_key = StateKey::new("app-sdk", "app").unwrap();
            let execution = run.execution(ExecutionOptions::default()).await.unwrap();
            *faults.app_store_admission_scope.lock().unwrap() = AppStoreAdmissionScope::Capture;
            faults.arm_app_store_mutation_monitor();
            let mut finishing = Box::pin(execution.finish());
            tokio::select! {
                _ = faults.run_transition_observed.notified() => {},
                result = &mut finishing => panic!("the original native Stage did not reach its transaction: {}", result.is_ok()),
            }
            assert_eq!(native.load(key.clone()).await.unwrap().unwrap().record.value, head.record.value);
            match case {
                NativeAdvanceFenceCase::Counts => {
                    // Hold one genuine native snapshot while all sixteen independent
                    // callers publish. Releasing it creates one real stale attempt;
                    // repeatedly forcing sixteen stale attempts would exceed App's
                    // existing bounded retry contract rather than test count fencing.
                    let mut ids = std::collections::BTreeSet::new();
                    for value in 0..16 {
                        let original = native.load(app_key.clone()).await.unwrap().unwrap();
                        let competing = workflow.prepare(WorkflowState::new(Map::from_iter([
                            ("threshold".into(), Value::from(value)),
                        ])).unwrap()).await.unwrap();
                        assert!(ids.insert(competing.id().clone()));
                        let paused = competing.snapshot().await.unwrap();
                        assert!(matches!(paused.status, RunStatus::Paused));
                        assert_eq!(paused.checkpoint, 0);
                        assert_eq!(paused.state.values()["threshold"], value);
                        assert!(paused.outcomes.is_empty() && paused.last_action_ordinal.is_none());
                        let current = native.load(key.clone()).await.unwrap().unwrap();
                        assert_eq!(current.revision, head.revision);
                        assert_eq!(current.record.value, head.record.value);
                        let current = native.load(app_key.clone()).await.unwrap().unwrap();
                        assert!(current.revision > original.revision);
                        assert_eq!(current.record.value["run_count"].as_u64().unwrap(), original.record.value["run_count"].as_u64().unwrap() + 1);
                        let mut original_authority = original.record.value;
                        let mut current_authority = current.record.value;
                        Arc::make_mut(&mut original_authority).as_object_mut().unwrap().remove("run_count");
                        Arc::make_mut(&mut current_authority).as_object_mut().unwrap().remove("run_count");
                        assert_eq!(current_authority, original_authority);
                        let unchanged = native.load(workspace_key.clone()).await.unwrap().unwrap();
                        assert_eq!(unchanged.revision, original_workspace.revision);
                        assert_eq!(unchanged.record.value, original_workspace.record.value);
                    }
                    faults.app_store_admission_monitor.store(false, Ordering::SeqCst);
                    faults.release_app_store_admission();
                    let finished = finishing.as_mut().await
                        .expect("sixteen independent capacity admissions cannot lose the original Stage");
                    assert!(faults.monitored_mutation_conflicts.load(Ordering::SeqCst) >= 1,
                        "the original native snapshot must reject stale publication before fresh retry");
                    assert!(matches!(finished.status, RunStatus::Succeeded));
                    assert_eq!(finished.state.values()["path"], "selected");
                    assert_eq!(finished.last_action_ordinal, Some(1));
                    assert_eq!(finished.outcomes.len(), 1);
                    assert_eq!(finished.outcomes[0].node, "selected");
                    assert_eq!(finished.outcomes[0].ordinal, 1);
                    let choose = observer.checkpoint(CheckpointSelector::Action(0)).await.unwrap();
                    let selected = observer.checkpoint(CheckpointSelector::Action(1)).await.unwrap();
                    for (saved, ordinal, node) in [(&choose, 0, "choose"), (&selected, 1, "selected")] {
                        assert!(matches!(saved.kind, CheckpointKind::Action));
                        assert_eq!(saved.action_ordinal, Some(ordinal));
                        assert_eq!(saved.position.as_deref(), Some(node));
                        assert_eq!(saved.outcomes.len(), 1);
                        assert_eq!(saved.outcomes[0].node, node);
                        assert_eq!(saved.outcomes[0].ordinal, ordinal);
                    }
                    assert!(choose.sequence < selected.sequence && selected.sequence <= finished.checkpoint);
                    assert_eq!(choose.state, before.state);
                    assert_eq!(selected.state, finished.state);
                    assert!(selected.outcomes == finished.outcomes);
                    assert_eq!(ids.len(), 16);
                    assert_eq!(workflow.list_runs(None, NonZeroUsize::new(32).unwrap()).await.unwrap().len(), 17);
                }
                NativeAdvanceFenceCase::Generation => {
                    let original = native.load(app_key.clone()).await.unwrap().unwrap().record;
                    fresh = Some(App::open(AppConfig::<FaultStateStore, Agent>::new(
                        FaultStateStore { inner: native.clone(), faults: Arc::clone(&faults) },
                        Vec::new(), vec![WorkflowDefinition::parse(LINEAR).unwrap()],
                    )).await.unwrap());
                    let current = native.load(app_key.clone()).await.unwrap().unwrap().record;
                    assert!(current.value["generation"].as_u64().unwrap() > original.value["generation"].as_u64().unwrap());
                    faults.app_store_admission_monitor.store(false, Ordering::SeqCst);
                    faults.release_app_store_admission();
                    assert!(matches!(finishing.as_mut().await, Err(AppError::Closed(_))));
                    let current = native.load(key.clone()).await.unwrap().unwrap();
                    assert_eq!(current.revision, head.revision);
                    assert_eq!(current.record.value, head.record.value);
                }
                NativeAdvanceFenceCase::Cancellation => {
                    observer.cancel(before.checkpoint).await.unwrap();
                    let cancelled = native.load(key.clone()).await.unwrap().unwrap();
                    assert!(cancelled.revision > head.revision);
                    faults.app_store_admission_monitor.store(false, Ordering::SeqCst);
                    faults.release_app_store_admission();
                    assert!(matches!(finishing.as_mut().await, Err(AppError::Conflict(_))));
                    let cancelled = observer.wait().await.unwrap();
                    assert!(matches!(cancelled.status, RunStatus::Cancelled));
                    assert_eq!(cancelled.state, before.state);
                    assert_eq!(cancelled.last_action_ordinal, before.last_action_ordinal);
                    assert!(cancelled.outcomes == before.outcomes);
                }
            }
            assert_eq!(native.load(checkpoint.record.key.clone()).await.unwrap().unwrap().record.value, checkpoint.record.value);
            assert_eq!(faults.agent_intents.load(Ordering::SeqCst), 0);
        })).catch_unwind().await;
        faults
            .app_store_admission_monitor
            .store(false, Ordering::SeqCst);
        faults.release_app_store_admission();
        *faults.app_store_admission_scope.lock().unwrap() = AppStoreAdmissionScope::Any;
        let superseded = fresh.is_some();
        let fresh_cleanup = match fresh.as_mut() {
            Some(fresh) => Some(
                AssertUnwindSafe(tokio::time::timeout(
                    Duration::from_secs(12),
                    fresh.shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_secs(10),
                    }),
                ))
                .catch_unwind()
                .await,
            ),
            None => None,
        };
        drop(fresh);
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
            Ok(result) => result.expect("native Stage authority remains bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        if let Some(cleanup) = fresh_cleanup {
            cleanup
                .expect("replacement cleanup does not panic")
                .expect("replacement cleanup joins")
                .unwrap();
        }
        match cleanup
            .expect("original cleanup does not panic")
            .expect("original cleanup joins")
        {
            Ok(()) => {}
            Err(AppError::Closed(_)) if superseded => {}
            Err(error) => panic!("native Stage cleanup failed: {error:?}"),
        }
        assert!(
            faults.monitored_mutations.load(Ordering::SeqCst) >= 1,
            "native Stage fencing must observe its actual mutation receipt load"
        );
        assert_eq!(
            native
                .load(StateKey::new("app-sdk", "app").unwrap())
                .await
                .unwrap()
                .unwrap()
                .record
                .value["run_count"],
            0
        );
    }
}

#[tokio::test]
async fn native_stage_survives_sixteen_independent_count_only_admissions() {
    StorageFaults::native_advance_fence(NativeAdvanceFenceCase::Counts).await;
}

#[tokio::test]
async fn native_stage_cannot_cross_a_new_app_generation() {
    StorageFaults::native_advance_fence(NativeAdvanceFenceCase::Generation).await;
}

#[tokio::test]
async fn native_stage_cannot_overwrite_public_cancellation() {
    StorageFaults::native_advance_fence(NativeAdvanceFenceCase::Cancellation).await;
}

impl StorageFaults {
    async fn inventory_records(
        store: &StateStore,
        prefix: Option<&str>,
    ) -> Vec<StateRecordSnapshot> {
        let started = std::time::Instant::now();
        let mut pages = 0;
        let mut rows = Vec::new();
        let mut after = None;
        eprintln!(
            "inventory phase=enumeration event=enter elapsed_ms=0 prefix={prefix:?} pages=0 rows=0"
        );
        loop {
            let mut request = StateListRequest::new("app-sdk")
                .unwrap()
                .with_limit(1000)
                .unwrap();
            if let Some(prefix) = prefix {
                request = request.with_key_prefix(StateKeyPrefix::new(prefix).unwrap());
            }
            if let Some(cursor) = after.take() {
                request = request.start_after(cursor).unwrap();
            }
            let page = store.list(request).await;
            if let Err(error) = &page {
                eprintln!(
                    "inventory phase=enumeration event=failure elapsed_ms={} prefix={prefix:?} pages={pages} rows={} error_kind={:?}",
                    started.elapsed().as_millis(),
                    rows.len(),
                    std::mem::discriminant(error),
                );
            }
            let page = page.unwrap();
            pages += 1;
            rows.extend(page.records);
            match page.next_start_after {
                Some(cursor) => after = Some(cursor),
                None => {
                    eprintln!(
                        "inventory phase=enumeration event=complete elapsed_ms={} prefix={prefix:?} pages={pages} rows={}",
                        started.elapsed().as_millis(),
                        rows.len(),
                    );
                    return rows;
                }
            }
        }
    }

    fn inventory_definition(steps: usize) -> WorkflowDefinition {
        WorkflowDefinition::parse(&serde_json::json!({
            "schema": "app-sdk.workflow/v2", "id": "inventory", "revision": "v1",
            "state": {"count": 0}, "required": ["count"],
            "reducers": {"count": "sum_integer"}, "entry": "count",
            "nodes": [{"id":"count", "step":{"kind":"state", "mutations":[
                {"kind":"assign", "target":"count", "value":{"kind":"literal", "value":1}}
            ]}}],
            "routes": {"count": {"mode":"first", "branches":[
                {"when":{"kind":"less_than", "left":{"kind":"state","path":"count"},
                    "right":{"kind":"literal","value":steps}}, "target":{"kind":"node","id":"count"}},
                {"target":{"kind":"end"}}
            ]}}
        }).to_string()).unwrap()
    }

    async fn assert_paged_inventory(heads: bool, corruptions: &[(usize, InventoryCorruption)]) {
        let setup_started = std::time::Instant::now();
        eprintln!(
            "inventory phase=initial-setup event=enter elapsed_ms=0 heads={heads} fault_cases={}",
            corruptions.len()
        );
        let (faults, forwarding) = Self::open().await.unwrap();
        let native = forwarding.inner.clone();
        let steps = if heads { 1 } else { 1450 };
        let mut seed: App<StateStore, Agent> = App::open(AppConfig::new(
            native.clone(),
            Vec::new(),
            vec![Self::inventory_definition(steps)],
        ))
        .await
        .unwrap();
        let workspace = seed
            .create_workspace(WorkspaceConfig {
                name: Some("real-paged-inventory".into()),
                root: std::env::current_dir().unwrap(),
            })
            .await
            .unwrap();
        let workspace_id = workspace.id().clone();
        let workflow = workspace
            .workflow("inventory".parse().unwrap())
            .await
            .unwrap();
        eprintln!(
            "inventory phase=initial-setup event=complete elapsed_ms={} heads={heads} steps={steps} fault_cases={}",
            setup_started.elapsed().as_millis(),
            corruptions.len()
        );
        let mut selected = None;
        let started = std::time::Instant::now();
        let mut completed_heads = 0;
        let mut phase = "admission";
        let mut phase_started = std::time::Instant::now();
        // These real persisted inventories are setup for the page-contract checks.
        // Their aggregate build cost grows with retained Lance state and history.
        let seed_budget = Duration::from_secs(if heads { 7_200 } else { 1_800 });
        eprintln!(
            "inventory phase=initial-generation event=enter elapsed_ms=0 heads={heads} steps={steps} expected_heads={} budget_seconds={}",
            if heads { 4097 } else { 1 },
            seed_budget.as_secs(),
        );
        let seeded = AssertUnwindSafe(tokio::time::timeout(seed_budget, async {
            for index in 0..if heads { 4097 } else { 1 } {
                phase = "admission";
                phase_started = std::time::Instant::now();
                let run = workflow.start(ExecutionOptions::default()).await.unwrap();
                phase = "terminal observation";
                phase_started = std::time::Instant::now();
                let terminal = run.wait().await.unwrap();
                assert!(matches!(terminal.status, RunStatus::Succeeded));
                assert_eq!(terminal.state.values()["count"], serde_json::json!(steps));
                selected = Some((
                    RunSelector::Exact {
                        id: run.id().clone(),
                        occurrence: terminal.occurrence.clone(),
                    },
                    terminal,
                ));
                completed_heads = index + 1;
                if heads && (completed_heads == 1 || completed_heads % 64 == 0) {
                    eprintln!(
                        "seeded {completed_heads} genuine retained heads elapsed_ms={} last_completed_checkpoint={:?}",
                        started.elapsed().as_millis(),
                        selected.as_ref().map(|(_, snapshot)| snapshot.checkpoint),
                    );
                }
            }
        }))
        .catch_unwind()
        .await;
        eprintln!(
            "inventory phase=initial-generation event={} elapsed_ms={} heads={heads} steps={steps} active_operation={phase:?} operation_elapsed_ms={} completed_heads={completed_heads} last_completed_checkpoint={:?} timed_out={} panicked={}",
            if matches!(&seeded, Ok(Ok(()))) {
                "complete"
            } else {
                "failure"
            },
            started.elapsed().as_millis(),
            phase_started.elapsed().as_millis(),
            selected.as_ref().map(|(_, snapshot)| snapshot.checkpoint),
            matches!(&seeded, Ok(Err(_))),
            seeded.is_err(),
        );
        drop(workflow);
        drop(workspace);
        let shutdown_started = std::time::Instant::now();
        eprintln!(
            "inventory phase=initial-shutdown event=enter elapsed_ms=0 heads={heads} completed_heads={completed_heads}"
        );
        let closed = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(15),
            seed.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(10),
            }),
        ))
        .catch_unwind()
        .await;
        eprintln!(
            "inventory phase=initial-shutdown event={} elapsed_ms={} heads={heads} completed_heads={completed_heads} outcome={} settlement={} result={closed:?}",
            if matches!(&closed, Ok(Ok(Ok(())))) {
                "complete"
            } else {
                "failure"
            },
            shutdown_started.elapsed().as_millis(),
            match &closed {
                Ok(Ok(Ok(()))) => "ok",
                Ok(Err(_)) | Ok(Ok(Err(AppError::Timeout(_)))) => "timeout",
                Err(_) => "panic",
                Ok(Ok(Err(_))) => "error",
            },
            if matches!(
                &closed,
                Ok(Ok(Ok(()))) | Ok(Ok(Err(AppError::Integration(_))))
            ) {
                "joined"
            } else {
                "unconfirmed"
            },
        );
        // This owner has requested only valid CancelAndDrain. Its Integration
        // result is retained finalizer failure after joining, never escalation.
        if !matches!(
            &closed,
            Ok(Ok(Ok(()))) | Ok(Ok(Err(AppError::Integration(_))))
        ) {
            let mut observer = 1;
            loop {
                eprintln!(
                    "inventory phase=initial-shutdown event=enter elapsed_ms={} heads={heads} observer={observer}",
                    shutdown_started.elapsed().as_millis()
                );
                let joined = AssertUnwindSafe(seed.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(600),
                }))
                .catch_unwind()
                .await;
                eprintln!(
                    "inventory phase=initial-shutdown event={} elapsed_ms={} heads={heads} observer={observer} outcome={} settlement={} result={joined:?}",
                    if matches!(&joined, Ok(Ok(()))) {
                        "complete"
                    } else {
                        "failure"
                    },
                    shutdown_started.elapsed().as_millis(),
                    match &joined {
                        Ok(Ok(())) => "ok",
                        Ok(Err(AppError::Timeout(_))) => "timeout",
                        Err(_) => "panic",
                        Ok(Err(_)) => "error",
                    },
                    if matches!(&joined, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
                        "joined"
                    } else {
                        "unconfirmed"
                    },
                );
                // This fixture only requests valid CancelAndDrain; Integration is
                // a retained finalizer failure delivered after owned joining.
                if matches!(&joined, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
                    break;
                }
                if !matches!(&joined, Ok(Err(AppError::Timeout(message)))
                    if message == "application shutdown observation timed out")
                {
                    // Keep observing accepted cancellation until joining is
                    // confirmed or the unchanged outer supervisor expires.
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                observer += 1;
            }
        }
        eprintln!("inventory phase=initial-settlement event=enter elapsed_ms=0 heads={heads}");
        let settlement_started = std::time::Instant::now();
        drop(seed);
        eprintln!(
            "inventory phase=initial-settlement event=complete elapsed_ms={} heads={heads} completed_heads={completed_heads}",
            settlement_started.elapsed().as_millis(),
        );
        match seeded {
            Ok(result) => result.expect("the complete real inventory fits its setup guard"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        closed
            .expect("inventory seed cleanup does not panic")
            .expect("inventory seed cleanup is bounded")
            .expect("inventory seed joins");
        let (selector, terminal) = selected.unwrap();
        let prefix = heads.then_some("run/");
        let count = Self::inventory_records(&native, prefix).await.len();
        assert!(
            count >= 4097,
            "the genuine inventory crosses the old page ceiling: {count}"
        );
        // Pack the surplus into full native pages first; subsequent requests stay one-row.
        // The boundary test below uses an exact 4,097-head inventory, while history has
        // enough surplus for late malformed-page observations, including oversized pages.
        let boundaries: &[usize] = if !corruptions.is_empty() {
            &[count]
        } else {
            &[4095, 4096, 4097]
        };
        let mut results = Vec::new();
        let cases: Vec<_> = if corruptions.is_empty() {
            boundaries.iter().map(|pages| (*pages, None)).collect()
        } else {
            corruptions
                .iter()
                .map(|fault| (count, Some(*fault)))
                .collect()
        };
        for (pages, corrupt) in cases {
            eprintln!(
                "inventory phase=pre-reopen-enumeration event=enter elapsed_ms=0 heads={heads} pages={pages} fault={corrupt:?}"
            );
            let enumeration_started = std::time::Instant::now();
            let before = Self::inventory_records(&native, None).await;
            eprintln!(
                "inventory phase=pre-reopen-enumeration event=complete elapsed_ms={} heads={heads} pages={pages} rows={} fault={corrupt:?}",
                enumeration_started.elapsed().as_millis(),
                before.len()
            );
            *faults.inventory_page.lock().unwrap() = InventoryPageFault::Short {
                prefix,
                extra: if heads { count } else { before.len() } - pages,
                pages: 0,
                corruption: corrupt,
            };
            faults.inventory_page_reads.store(0, Ordering::SeqCst);
            let mut phase = "reopen";
            let mut phase_started = std::time::Instant::now();
            eprintln!(
                "inventory phase={phase} event=enter elapsed_ms=0 heads={heads} pages={pages} steps={steps} fault={corrupt:?}"
            );
            let opened = AssertUnwindSafe(tokio::time::timeout(
                Duration::from_secs(90),
                App::open(AppConfig::<FaultStateStore, Agent>::new(
                    FaultStateStore {
                        inner: native.clone(),
                        faults: Arc::clone(&faults),
                    },
                    Vec::new(),
                    vec![Self::inventory_definition(steps)],
                )),
            ))
            .catch_unwind()
            .await;
            let reached = matches!(
                *faults.inventory_page.lock().unwrap(),
                InventoryPageFault::Observed
            );
            let reads = faults.inventory_page_reads.load(Ordering::SeqCst);
            *faults.inventory_page.lock().unwrap() = InventoryPageFault::None;
            eprintln!(
                "inventory phase={phase} event={} elapsed_ms={} heads={heads} pages={pages} reads={reads} fault={corrupt:?} reached={reached} timed_out={} panicked={}",
                if matches!(&opened, Ok(Ok(Ok(_)))) {
                    "complete"
                } else {
                    "failure"
                },
                phase_started.elapsed().as_millis(),
                matches!(&opened, Ok(Err(_))),
                opened.is_err(),
            );
            let opened = match opened {
                Ok(opened) => opened,
                Err(panic) => std::panic::resume_unwind(panic),
            };
            match opened {
                Ok(Ok(mut app)) => {
                    let mut history_rows = 0;
                    let mut history_pages = 0;
                    let observed = AssertUnwindSafe(async {
                        phase = "snapshot";
                        phase_started = std::time::Instant::now();
                        eprintln!("inventory phase={phase} event=enter elapsed_ms=0 heads={heads} pages={pages}");
                        let workspace = app.workspace(workspace_id.clone()).await.unwrap();
                        let workflow = workspace
                            .workflow("inventory".parse().unwrap())
                            .await
                            .unwrap();
                        let run = workflow.open_run(selector.clone()).await.unwrap();
                        let snapshot = run.snapshot().await.unwrap();
                        assert_eq!(snapshot.checkpoint, terminal.checkpoint);
                        assert_eq!(snapshot.state, terminal.state);
                        eprintln!("inventory phase={phase} event=complete elapsed_ms={} heads={heads} pages={pages} checkpoint={}", phase_started.elapsed().as_millis(), snapshot.checkpoint);
                        phase = "history";
                        phase_started = std::time::Instant::now();
                        eprintln!("inventory phase={phase} event=enter elapsed_ms=0 heads={heads} pages={pages} expected_rows={}", terminal.checkpoint + 1);
                        let mut cursor = None;
                        loop {
                            let page = run
                                .history(cursor, NonZeroUsize::new(128).unwrap())
                                .await
                                .unwrap();
                            history_rows += page.checkpoints.len();
                            history_pages += 1;
                            match page.next {
                                Some(next) => cursor = Some(next),
                                None => break,
                            }
                        }
                        assert_eq!(history_rows as u64, terminal.checkpoint + 1);
                        eprintln!("inventory phase={phase} event=complete elapsed_ms={} heads={heads} pages={pages} history_pages={history_pages} history_rows={history_rows}", phase_started.elapsed().as_millis());
                        if corrupt.is_none() && pages == *boundaries.last().unwrap() {
                            phase = "deletion";
                            phase_started = std::time::Instant::now();
                            eprintln!("inventory phase={phase} event=enter elapsed_ms=0 heads={heads} pages={pages} checkpoint={}", terminal.checkpoint);
                            let mut run = run;
                            run.delete(terminal.checkpoint).await.unwrap();
                            assert!(matches!(
                                workflow.open_run(selector.clone()).await,
                                Err(AppError::NotFound(_))
                            ));
                            eprintln!("inventory phase={phase} event=complete elapsed_ms={} heads={heads} pages={pages}", phase_started.elapsed().as_millis());
                            phase = "subsequent-workflow-admission";
                            phase_started = std::time::Instant::now();
                            eprintln!("inventory phase={phase} event=enter elapsed_ms=0 heads={heads} pages={pages} steps={steps}");
                            let next = workflow.start(ExecutionOptions::default()).await.unwrap();
                            eprintln!("inventory phase={phase} event=complete elapsed_ms={} heads={heads} pages={pages} steps={steps}", phase_started.elapsed().as_millis());
                            phase = "subsequent-workflow-completion";
                            phase_started = std::time::Instant::now();
                            eprintln!("inventory phase={phase} event=enter elapsed_ms=0 heads={heads} pages={pages} steps={steps}");
                            let next = next.wait().await.unwrap();
                            assert!(matches!(next.status, RunStatus::Succeeded));
                            assert_eq!(next.state.values()["count"], serde_json::json!(steps));
                            eprintln!("inventory phase={phase} event=complete elapsed_ms={} heads={heads} pages={pages} steps={steps} checkpoint={}", phase_started.elapsed().as_millis(), next.checkpoint);
                        }
                    })
                    .catch_unwind()
                    .await;
                    if observed.is_err() {
                        eprintln!(
                            "inventory phase={phase} event=failure elapsed_ms={} heads={heads} pages={pages} history_pages={history_pages} history_rows={history_rows} panicked=true",
                            phase_started.elapsed().as_millis()
                        );
                    }
                    phase = "final-shutdown";
                    phase_started = std::time::Instant::now();
                    eprintln!(
                        "inventory phase={phase} event=enter elapsed_ms=0 heads={heads} pages={pages}"
                    );
                    let closed = AssertUnwindSafe(tokio::time::timeout(
                        Duration::from_secs(15),
                        app.shutdown(Shutdown::CancelAndDrain {
                            timeout: Duration::from_secs(10),
                        }),
                    ))
                    .catch_unwind()
                    .await;
                    eprintln!(
                        "inventory phase={phase} event={} elapsed_ms={} heads={heads} pages={pages} outcome={} settlement={} result={closed:?}",
                        if matches!(&closed, Ok(Ok(Ok(())))) {
                            "complete"
                        } else {
                            "failure"
                        },
                        phase_started.elapsed().as_millis(),
                        match &closed {
                            Ok(Ok(Ok(()))) => "ok",
                            Ok(Err(_)) | Ok(Ok(Err(AppError::Timeout(_)))) => "timeout",
                            Err(_) => "panic",
                            Ok(Ok(Err(_))) => "error",
                        },
                        if matches!(
                            &closed,
                            Ok(Ok(Ok(()))) | Ok(Ok(Err(AppError::Integration(_))))
                        ) {
                            "joined"
                        } else {
                            "unconfirmed"
                        },
                    );
                    // Only valid CancelAndDrain is requested for this owner;
                    // Integration therefore follows finalization and joining.
                    if !matches!(
                        &closed,
                        Ok(Ok(Ok(()))) | Ok(Ok(Err(AppError::Integration(_))))
                    ) {
                        let mut observer = 1;
                        loop {
                            eprintln!(
                                "inventory phase={phase} event=enter elapsed_ms={} heads={heads} pages={pages} observer={observer}",
                                phase_started.elapsed().as_millis()
                            );
                            let joined = AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
                                timeout: Duration::from_secs(600),
                            }))
                            .catch_unwind()
                            .await;
                            eprintln!(
                                "inventory phase={phase} event={} elapsed_ms={} heads={heads} pages={pages} observer={observer} outcome={} settlement={} result={joined:?}",
                                if matches!(&joined, Ok(Ok(()))) {
                                    "complete"
                                } else {
                                    "failure"
                                },
                                phase_started.elapsed().as_millis(),
                                match &joined {
                                    Ok(Ok(())) => "ok",
                                    Ok(Err(AppError::Timeout(_))) => "timeout",
                                    Err(_) => "panic",
                                    Ok(Err(_)) => "error",
                                },
                                if matches!(&joined, Ok(Ok(())) | Ok(Err(AppError::Integration(_))))
                                {
                                    "joined"
                                } else {
                                    "unconfirmed"
                                },
                            );
                            if matches!(&joined, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
                                break;
                            }
                            if !matches!(&joined, Ok(Err(AppError::Timeout(message)))
                                if message == "application shutdown observation timed out")
                            {
                                // A failed observer is never evidence of a
                                // completed join; keep the owned runtime driven.
                                tokio::time::sleep(Duration::from_secs(1)).await;
                            }
                            observer += 1;
                        }
                    }
                    phase = "final-settlement";
                    phase_started = std::time::Instant::now();
                    eprintln!(
                        "inventory phase={phase} event=enter elapsed_ms=0 heads={heads} pages={pages}"
                    );
                    drop(app);
                    eprintln!(
                        "inventory phase={phase} event=complete elapsed_ms={} heads={heads} pages={pages}",
                        phase_started.elapsed().as_millis()
                    );
                    if let Err(panic) = observed {
                        std::panic::resume_unwind(panic);
                    }
                    closed
                        .expect("reopened inventory cleanup does not panic")
                        .expect("reopened inventory cleanup bounded")
                        .expect("reopened inventory joins");
                    results.push((pages, reads, reached, corrupt, None));
                }
                Ok(Err(error)) => {
                    eprintln!(
                        "inventory phase=reopen event=failure elapsed_ms={} heads={heads} pages={pages} reads={reads} fault={corrupt:?} error_kind={:?}",
                        phase_started.elapsed().as_millis(),
                        std::mem::discriminant(&error)
                    );
                    assert_eq!(
                        Self::inventory_records(&native, None).await,
                        before,
                        "failed preflight must not mutate any actual record"
                    );
                    results.push((pages, reads, reached, corrupt, Some(error)));
                }
                Err(error) => panic!(
                    "inventory phase={phase} pages={pages} reads={reads}: bounded valid-page traversal timed out: {error}"
                ),
            }
        }
        // Evaluate every fault after all Apps and native operations have joined.
        // A late-cutoff failure must not prevent the other injected cases running.
        for (pages, reads, reached, corrupt, error) in &results {
            eprintln!(
                "inventory pages={pages} reads={reads} fault={corrupt:?} reached={reached} outcome={error:?}"
            );
        }
        let settlement_started = std::time::Instant::now();
        eprintln!(
            "inventory phase=store-settlement event=enter elapsed_ms=0 heads={heads} completed_cases={}",
            results.len()
        );
        drop(faults);
        drop(native);
        drop(forwarding);
        eprintln!(
            "inventory phase=store-settlement event=complete elapsed_ms={} heads={heads} completed_cases={}",
            settlement_started.elapsed().as_millis(),
            results.len()
        );
        let assertions_started = std::time::Instant::now();
        eprintln!(
            "inventory phase=final-assertions event=enter elapsed_ms=0 heads={heads} cases={}",
            results.len()
        );
        for (pages, reads, reached, corrupt, error) in results {
            if let Some((at, kind)) = corrupt {
                assert!(
                    reached && reads >= at,
                    "{kind:?}: late fault must be reached; reads={reads}, result={error:?}"
                );
                assert!(
                    matches!(&error, Some(AppError::Integration(_))),
                    "{kind:?}: malformed observation must reject: {error:?}"
                );
                if matches!(kind, InventoryCorruption::Epoch) {
                    assert!(
                        matches!(error,Some(AppError::Integration(message)) if message == "unsupported app record schema version")
                    );
                }
            } else {
                assert!(
                    error.is_none(),
                    "valid {pages}-page inventory must reopen through EOF; reads={reads}, error={error:?}"
                );
                assert!(reads >= pages, "all genuine pages must be observed");
            }
        }
        eprintln!(
            "inventory phase=final-assertions event=complete elapsed_ms={} heads={heads}",
            assertions_started.elapsed().as_millis()
        );
    }
}

#[tokio::test]
async fn reopen_accepts_valid_short_pages_beyond_the_old_scan_window() {
    StorageFaults::assert_paged_inventory(false, &[]).await;
}

#[tokio::test]
async fn retained_revision_scan_accepts_short_pages_at_4095_4096_4097() {
    StorageFaults::assert_paged_inventory(true, &[]).await;
}

#[tokio::test]
async fn late_invalid_inventory_is_observed_before_any_generation_claim() {
    let faults: Vec<_> = [
        InventoryCorruption::Epoch,
        InventoryCorruption::Namespace,
        InventoryCorruption::Oversized,
        InventoryCorruption::EmptyWithCursor,
        InventoryCorruption::Nonprogressing,
    ]
    .into_iter()
    .flat_map(|kind| [(2, kind), (4097, kind)])
    .collect();
    StorageFaults::assert_paged_inventory(false, &faults).await;
}

#[tokio::test]
async fn late_foreign_prefix_is_observed_before_any_generation_claim() {
    StorageFaults::assert_paged_inventory(
        true,
        &[
            (2, InventoryCorruption::Prefix),
            (4097, InventoryCorruption::Prefix),
        ],
    )
    .await;
}

#[tokio::test]
async fn immutable_checkpoint_evidence_accepts_dto_order_and_unchanged_numbers() {
    for change in [CheckpointEvidence::DtoOrder, CheckpointEvidence::Unchanged] {
        for initial in [true, false] {
            StorageFaults::assert_checkpoint_evidence(initial, change).await;
        }
    }
}

#[tokio::test]
async fn immutable_checkpoint_evidence_rejects_nested_order_and_numeric_representation_changes() {
    for change in [
        CheckpointEvidence::NestedOrder,
        CheckpointEvidence::IntegerFloat,
        CheckpointEvidence::ZeroSign,
    ] {
        for initial in [true, false] {
            StorageFaults::assert_checkpoint_evidence(initial, change).await;
        }
    }
}

impl StorageFaults {
    async fn assert_predecessor_evidence(evidence: CheckpointEvidence) {
        let (faults, store) = Self::open().await.unwrap();
        let native = store.inner.clone();
        let mut definition: Value = serde_json::from_str(LINEAR).unwrap();
        definition["state"]["evidence"] = serde_json::json!({
            "numbers": [i64::MIN, u64::MAX, 1, 1.0, -0.0, 0.0, 1.25],
            "label": "ordered evidence"
        });
        definition["reducers"]["evidence"] = Value::from("reject_concurrent");
        let mut app = App::open(AppConfig::<FaultStateStore, Agent>::new(
            store,
            Vec::new(),
            vec![WorkflowDefinition::parse(&definition.to_string()).unwrap()],
        ))
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
            let workflow = workspace.workflow("linear".parse().unwrap()).await.unwrap();
            let run = workflow.prepare(WorkflowState::default()).await.unwrap();
            let before = run.snapshot().await.unwrap();
            let key = StateKey::new(
                "app-sdk",
                format!(
                    "history/{}/{}/{}/{}/checkpoint/{:020}",
                    workspace.id(),
                    workflow.id(),
                    run.id(),
                    before.occurrence,
                    before.checkpoint,
                ),
            )
            .unwrap();
            let original = native.load(key.clone()).await.unwrap().unwrap();
            *faults.checkpoint_evidence.lock().unwrap() = evidence;
            *faults.predecessor_evidence.lock().unwrap() = Some(key.clone());
            let outcome = run.cancel(before.checkpoint).await;
            faults.predecessor_evidence.lock().unwrap().take();
            let reached = faults.predecessor_reads.load(Ordering::SeqCst);
            assert!(
                reached > 0,
                "{evidence:?}: actual predecessor must be read inside the native transaction"
            );
            let unchanged = native.load(key).await.unwrap().unwrap();
            assert_eq!(unchanged.revision, original.revision);
            assert_eq!(
                serde_json::to_vec(unchanged.record.value.as_ref()).unwrap(),
                serde_json::to_vec(original.record.value.as_ref()).unwrap()
            );
            if evidence.contradicts() {
                assert!(
                    matches!(
                        outcome,
                        Err(AppError::Conflict(_) | AppError::Integration(_))
                    ),
                    "{evidence:?}: contradictory predecessor must reject publication: {:?}",
                    outcome.as_ref().err()
                );
                let after = run.snapshot().await.unwrap();
                assert_eq!(after.checkpoint, before.checkpoint);
                assert_eq!(after.state, before.state);
            } else {
                let cancelled = outcome.expect("fixed DTO field order does not change its meaning");
                assert!(matches!(cancelled.status, RunStatus::Cancelling));
                assert_eq!(cancelled.checkpoint, before.checkpoint + 1);
                assert!(matches!(
                    run.wait().await.unwrap().status,
                    RunStatus::Cancelled
                ));
            }
        }))
        .catch_unwind()
        .await;
        faults.predecessor_evidence.lock().unwrap().take();
        let cleanup = tokio::time::timeout(
            Duration::from_secs(12),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(10),
            }),
        )
        .await;
        drop(app);
        cleanup
            .expect("predecessor cleanup bounded")
            .expect("predecessor cleanup joins");
        match checked {
            Ok(result) => result.expect("predecessor observation bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
}

#[tokio::test]
async fn immutable_checkpoint_predecessor_preserves_order_and_number_representations() {
    for evidence in [
        CheckpointEvidence::DtoOrder,
        CheckpointEvidence::Unchanged,
        CheckpointEvidence::StateOrder,
        CheckpointEvidence::NestedOrder,
        CheckpointEvidence::IntegerFloat,
        CheckpointEvidence::ZeroSign,
    ] {
        StorageFaults::assert_predecessor_evidence(evidence).await;
    }
}

#[tokio::test]
async fn receipt_page_ownership_probe_excludes_storage_retained_values() {
    let (faults, store) = StorageFaults::open().await.unwrap();
    let native = store.inner.clone();
    let mut transaction = native.begin_transaction().await.unwrap();
    for index in 0..2 {
        transaction
            .put(StateRecord::new(
                StateKey::new("app-sdk", format!("receipt/probe-{index}")).unwrap(),
                serde_json::json!({
                    "index": index,
                    "evidence": ["literal %_ é", {"integer": 1, "float": 1.0}]
                }),
            ))
            .await
            .unwrap();
    }
    transaction.commit().await.unwrap();
    let request = StateListRequest::new("app-sdk")
        .unwrap()
        .with_key_prefix(StateKeyPrefix::new("receipt/").unwrap())
        .with_limit(2)
        .unwrap();
    let original = native.list(request.clone()).await.unwrap();
    assert_eq!(original.records.len(), 2);
    *faults.receipt_page_values.lock().unwrap() = Some(Vec::new());
    *faults.inventory_page.lock().unwrap() = InventoryPageFault::Short {
        prefix: Some("receipt/"),
        extra: 0,
        pages: 0,
        corruption: None,
    };
    let first = store.list(request.clone()).await.unwrap();
    assert_eq!(first.records.len(), 1);
    assert_eq!(
        first.next_start_after.as_deref(),
        Some(original.records[0].record.key.key())
    );
    let second = store
        .list(
            request
                .start_after(first.next_start_after.clone().unwrap())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second.records.len(), 1);
    assert!(second.next_start_after.is_none());
    for (page, raw) in [
        (&first, &original.records[0]),
        (&second, &original.records[1]),
    ] {
        let observed = &page.records[0];
        assert_eq!(observed.record.key, raw.record.key);
        assert_eq!(observed.revision, raw.revision);
        assert_eq!(
            serde_json::to_vec(observed.record.value.as_ref()).unwrap(),
            serde_json::to_vec(raw.record.value.as_ref()).unwrap()
        );
        assert!(
            !Arc::ptr_eq(&observed.record.value, &raw.record.value),
            "the test probe must measure returned page ownership independently of storage caching"
        );
    }
    let probes = faults.receipt_page_values.lock().unwrap().clone().unwrap();
    assert_eq!(probes.len(), 2);
    assert!(probes.iter().all(|probe| probe.strong_count() > 0));
    assert_eq!(faults.receipt_page_peak.load(Ordering::SeqCst), 2);
    drop(first);
    assert!(probes[0].upgrade().is_none());
    assert!(probes[1].upgrade().is_some());
    drop(second);
    assert!(probes.iter().all(|probe| probe.upgrade().is_none()));
    for raw in &original.records {
        let retained = native.load(raw.record.key.clone()).await.unwrap().unwrap();
        assert_eq!(retained.revision, raw.revision);
        assert_eq!(retained.record.value, raw.record.value);
    }
}

#[tokio::test]
async fn reopening_cleans_receipts_page_by_page_without_retaining_the_inventory() {
    let (faults, store) = StorageFaults::open().await.unwrap();
    let native = store.inner.clone();
    let mut app = App::open(AppConfig::<FaultStateStore, Agent>::new(
        store,
        Vec::new(),
        vec![WorkflowDefinition::parse(LINEAR).unwrap()],
    ))
    .await
    .unwrap();
    let seeded = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(30), async {
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().unwrap(),
            })
            .await
            .unwrap();
        let mut revision = workspace.snapshot().await.unwrap().revision;
        for index in 0..300 {
            faults.reject_receipt_cleanup.store(true, Ordering::SeqCst);
            revision = workspace
                .replace(
                    revision,
                    WorkspaceConfig {
                        name: Some(format!("retained-receipt-{index}")),
                        root: std::env::current_dir().unwrap(),
                    },
                )
                .await
                .unwrap()
                .revision;
        }
        let workflow = workspace.workflow("linear".parse().unwrap()).await.unwrap();
        let run = workflow.start(ExecutionOptions::default()).await.unwrap();
        let terminal = run.wait().await.unwrap();
        let selector = RunSelector::Exact {
            id: run.id().clone(),
            occurrence: terminal.occurrence.clone(),
        };
        (workspace.id().clone(), selector, terminal)
    }))
    .catch_unwind()
    .await;
    faults.reject_receipt_cleanup.store(false, Ordering::SeqCst);
    app.shutdown(Shutdown::Drain {
        timeout: Duration::from_secs(10),
    })
    .await
    .unwrap();
    drop(app);
    let (workspace_id, selector, terminal) = match seeded {
        Ok(result) => result.expect("genuine receipt seeding bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    };
    let originals = StorageFaults::inventory_records(&native, Some("receipt/")).await;
    assert!(
        originals.len() > 512,
        "multiple original cleanup batches remain"
    );
    faults.receipt_cleanup_batches.lock().unwrap().clear();
    *faults.receipt_page_values.lock().unwrap() = Some(Vec::new());
    *faults.inventory_page.lock().unwrap() = InventoryPageFault::Short {
        prefix: Some("receipt/"),
        extra: 0,
        pages: 0,
        corruption: None,
    };
    let opened = tokio::time::timeout(
        Duration::from_secs(30),
        App::open(AppConfig::<FaultStateStore, Agent>::new(
            FaultStateStore {
                inner: native.clone(),
                faults: Arc::clone(&faults),
            },
            Vec::new(),
            vec![WorkflowDefinition::parse(LINEAR).unwrap()],
        )),
    )
    .await;
    *faults.inventory_page.lock().unwrap() = InventoryPageFault::None;
    let peak = faults.receipt_page_peak.load(Ordering::SeqCst);
    faults.receipt_page_values.lock().unwrap().take();
    let mut app = opened
        .expect("short-page receipt cleanup bounded")
        .expect("valid receipts reopen");
    let checked = AssertUnwindSafe(async {
        for original in &originals {
            assert!(native.load(original.record.key.clone()).await.unwrap().is_none());
        }
        let workspace = app.workspace(workspace_id).await.unwrap();
        let workflow = workspace.workflow("linear".parse().unwrap()).await.unwrap();
        let retained = workflow.open_run(selector).await.unwrap().snapshot().await.unwrap();
        assert_eq!(retained.checkpoint, terminal.checkpoint);
        assert_eq!(retained.state, terminal.state);
        let batches = faults.receipt_cleanup_batches.lock().unwrap().clone();
        assert!(!batches.is_empty());
        assert!(batches.iter().all(|batch| *batch <= 256));
        eprintln!("receipt inventory={} peak_live_returned_values={peak} transaction_batches={batches:?}", originals.len());
        // Weak references observe ownership of the public returned Value, without
        // retaining it or asserting names/layout of private runtime helpers.
        assert!(peak <= 2, "one-row cleanup must release consumed page values before collecting its whole inventory; peak={peak}");
    }).catch_unwind().await;
    app.shutdown(Shutdown::Drain {
        timeout: Duration::from_secs(10),
    })
    .await
    .unwrap();
    drop(app);
    if let Err(panic) = checked {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn unary_busy_after_side_effect_is_not_automatically_retried() {
    StreamFailureState::accepted_failure_is_not_retried(
        RetryFailure::AfterEffect(agent_sdk::error::ErrorKind::Busy),
        false,
        false,
    )
    .await;
}

#[tokio::test]
async fn unary_invalid_config_after_side_effect_is_not_automatically_retried() {
    StreamFailureState::accepted_failure_is_not_retried(
        RetryFailure::AfterEffect(agent_sdk::error::ErrorKind::InvalidConfig),
        false,
        false,
    )
    .await;
}

#[tokio::test]
async fn unary_invalid_input_after_side_effect_is_not_automatically_retried() {
    StreamFailureState::accepted_failure_is_not_retried(
        RetryFailure::AfterEffect(agent_sdk::error::ErrorKind::InvalidInput),
        false,
        false,
    )
    .await;
}

#[tokio::test]
async fn unary_unsupported_after_side_effect_is_not_automatically_retried() {
    StreamFailureState::accepted_failure_is_not_retried(
        RetryFailure::AfterEffect(agent_sdk::error::ErrorKind::Unsupported),
        false,
        false,
    )
    .await;
}

#[tokio::test]
async fn unary_not_yet_implemented_after_side_effect_is_not_automatically_retried() {
    StreamFailureState::accepted_failure_is_not_retried(
        RetryFailure::AfterEffect(agent_sdk::error::ErrorKind::NotYetImplemented),
        false,
        false,
    )
    .await;
}

#[tokio::test]
async fn accepted_stream_errors_after_side_effect_are_not_automatically_retried() {
    for kind in [
        agent_sdk::error::ErrorKind::Busy,
        agent_sdk::error::ErrorKind::InvalidConfig,
        agent_sdk::error::ErrorKind::InvalidInput,
        agent_sdk::error::ErrorKind::Unsupported,
        agent_sdk::error::ErrorKind::NotYetImplemented,
    ] {
        StreamFailureState::accepted_failure_is_not_retried(
            RetryFailure::AfterEffect(kind),
            true,
            false,
        )
        .await;
    }
}

#[tokio::test]
async fn caller_resolution_can_retry_an_effectful_unary_failure_after_abort() {
    StreamFailureState::accepted_failure_is_not_retried(
        RetryFailure::AfterEffect(agent_sdk::error::ErrorKind::Integration),
        false,
        true,
    )
    .await;
}

#[tokio::test]
async fn stream_opening_errors_after_side_effect_are_not_automatically_retried() {
    for kind in [
        agent_sdk::error::ErrorKind::Busy,
        agent_sdk::error::ErrorKind::InvalidConfig,
        agent_sdk::error::ErrorKind::InvalidInput,
        agent_sdk::error::ErrorKind::Unsupported,
        agent_sdk::error::ErrorKind::NotYetImplemented,
    ] {
        StreamFailureState::accepted_failure_is_not_retried(
            RetryFailure::StreamOpeningAfterEffect(kind),
            true,
            false,
        )
        .await;
    }
}

#[tokio::test]
async fn cancellation_after_acknowledged_abort_preserves_original_timeout() {
    StreamFailureState::assert_timeout_survives_cancellation(false).await;
}

#[tokio::test]
async fn cancellation_after_abort_grace_preserves_original_timeout_through_unknown() {
    StreamFailureState::assert_timeout_survives_cancellation(true).await;
}

#[tokio::test]
async fn delivery_capacity_prefix_measurement() -> Result<(), Box<dyn Error + Send + Sync>> {
    let checkpoints = CheckpointPollCapture::default();
    StreamFailureState::measure_delivery_prefix(256, &checkpoints)
        .with_subscriber(checkpoints.clone())
        .await
}

#[tokio::test]
async fn storage_observation_cancelled_wait_does_not_remain_active() {
    // This exercises observation accounting through the existing fixture owner,
    // not cancellation or completion of an admitted native Storage operation.
    let (faults, _store) = StorageFaults::open().await.unwrap();
    *faults.observations.lock().unwrap() = Some(BTreeMap::new());
    let mut observation = Box::pin(async {
        let measured = faults.observe_start("fixture_wait");
        faults.preparation_entered.notify_one();
        faults.admission_release.notified().await;
        measured.finish(false);
    });
    tokio::select! {
        _ = faults.preparation_entered.notified() => {}
        _ = &mut observation => panic!("the controlled observation completed before release"),
    }
    drop(observation);
    let rows = faults.observations.lock().unwrap();
    let observed = &rows.as_ref().unwrap()["fixture_wait"];
    assert_eq!(observed.started, 1);
    assert_eq!(observed.completed, 0);
    assert_eq!(
        observed.active, 0,
        "dropped observations are no longer active"
    );
    assert_eq!(observed.dropped_observers, 1);
    assert_eq!(observed.failed, 0);
}

#[tokio::test]
async fn delivery_capacity_growth_measurement() -> Result<(), Box<dyn Error + Send + Sync>> {
    let checkpoints = CheckpointPollCapture::default();
    StreamFailureState::measure_delivery_prefix(1_024, &checkpoints)
        .with_subscriber(checkpoints.clone())
        .await
}

#[tokio::test]
async fn storage_observation_distinguishes_read_only_and_mutating_commits() {
    let (faults, store) = StorageFaults::open().await.unwrap();
    *faults.observations.lock().unwrap() = Some(BTreeMap::new());
    let key = StateKey::new("observation", "value").unwrap();
    let mut read_only = store.begin_transaction().await.unwrap();
    assert!(read_only.load(key.clone()).await.unwrap().is_none());
    read_only.commit().await.unwrap();
    let mut first = store.begin_transaction().await.unwrap();
    first
        .put(StateRecord::new(key.clone(), Value::from(1)))
        .await
        .unwrap();
    first.commit().await.unwrap();
    let mut winner = store.begin_transaction().await.unwrap();
    let mut stale = store.begin_transaction().await.unwrap();
    winner
        .put(StateRecord::new(key.clone(), Value::from(2)))
        .await
        .unwrap();
    stale
        .put(StateRecord::new(key.clone(), Value::from(3)))
        .await
        .unwrap();
    winner.commit().await.unwrap();
    assert!(matches!(
        stale.commit().await,
        Err(StateStoreError::TransactionConflict { .. })
    ));
    let mut deleting = store.begin_transaction().await.unwrap();
    deleting.delete(key.clone()).await.unwrap();
    deleting.commit().await.unwrap();
    assert!(store.load(key).await.unwrap().is_none());
    store
        .begin_transaction()
        .await
        .unwrap()
        .rollback()
        .await
        .unwrap();
    let rows = faults.observations.lock().unwrap();
    let rows = rows.as_ref().unwrap();
    let read_only = &rows["commit_read_only"];
    assert_eq!(
        (read_only.started, read_only.completed, read_only.failed),
        (1, 1, 0)
    );
    let mutating = &rows["commit_mutating"];
    assert_eq!(
        (mutating.started, mutating.completed, mutating.failed),
        (4, 4, 1)
    );
    assert_eq!(rows["rollback"].completed, 1);
    assert_eq!(rows["transaction_age"].completed, 6);
    assert_eq!(rows["transaction_read_only_age"].completed, 2);
    assert_eq!(rows["transaction_mutating_age"].completed, 4);
    assert_eq!(rows["transaction_mutating_age"].failed, 1);
    assert_eq!(rows["transaction_read_only_load"].completed, 1);
    assert_eq!(rows["transaction_mutating_put"].completed, 3);
    assert_eq!(rows["transaction_mutating_delete"].completed, 1);
    for observation in rows.values() {
        assert_eq!(observation.active, 0);
        assert_eq!(observation.dropped_observers, 0);
        assert_eq!(observation.started, observation.completed);
    }
}

#[tokio::test]
async fn storage_transaction_observation_separates_native_calls_and_owner_drop() {
    let (faults, store) = StorageFaults::open().await.unwrap();
    *faults.observations.lock().unwrap() = Some(BTreeMap::new());
    let key = StateKey::new("observation", "transaction-phases").unwrap();
    let mut transaction = store.begin_transaction().await.unwrap();
    assert!(transaction.load(key.clone()).await.unwrap().is_none());
    transaction
        .put(StateRecord::new(key.clone(), Value::from(1)))
        .await
        .unwrap();
    assert!(transaction.load(key.clone()).await.unwrap().is_some());
    transaction.commit().await.unwrap();
    let mut abandoned = store.begin_transaction().await.unwrap();
    assert!(abandoned.load(key).await.unwrap().is_some());
    drop(abandoned);
    let rows = faults.observations.lock().unwrap();
    let rows = rows.as_ref().unwrap();
    let age = &rows["transaction_mutating_age"];
    let preparation = rows["transaction_mutating_pre_finalization"].completed_elapsed;
    let finalization = rows["transaction_mutating_finalization"].completed_elapsed;
    let gap = rows["transaction_mutating_gap"].completed_elapsed;
    let calls = rows["transaction_mutating_load"].completed_elapsed
        + rows["transaction_mutating_put"].completed_elapsed
        + rows["transaction_mutating_delete"].completed_elapsed;
    assert_eq!(age.completed, 1);
    assert_eq!(age.completed_elapsed, preparation + finalization);
    assert_eq!(preparation, gap + calls);
    assert_eq!(rows["transaction_mutating_load"].completed, 2);
    assert_eq!(rows["transaction_mutating_put"].completed, 1);
    assert_eq!(rows["transaction_read_only_age"].dropped_observers, 1);
    assert_eq!(rows["transaction_read_only_load"].completed, 1);
    assert!(
        !rows.contains_key("transaction_read_only_finalization"),
        "owner drop before finalization must not invent a finalization attempt"
    );
    assert_eq!(rows["transaction_age"].active, 0);
    assert_eq!(rows["transaction_age"].completed, 1);
    assert_eq!(rows["transaction_age"].dropped_observers, 1);
    // This proves observer accounting, not a native rollback/join receipt.
}

#[test]
fn checkpoint_poll_capture_excludes_suspended_time_and_retains_open_spans() {
    let capture = CheckpointPollCapture::default();
    let dispatch = tracing::Dispatch::new(capture.clone());
    let span = tracing::dispatcher::with_default(&dispatch, || {
        let ignored = tracing::info_span!(target: "app_sdk", "unrelated");
        assert!(ignored.is_disabled());
        tracing::info_span!(target: "app_sdk", "app.workflow.checkpoint")
    });
    let retained = span.clone();
    {
        let _entered = span.enter();
        std::hint::black_box(1);
    }
    let first = capture.snapshot();
    assert_eq!((first.opened, first.closed, first.polls), (1, 0, 1));
    // Reading the observer while no span is entered must not count suspended
    // lifetime as active polling, regardless of clock resolution or scheduling.
    assert_eq!(capture.snapshot().active_poll, first.active_poll);
    let unwound = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let _entered = span.enter();
        panic!("controlled checkpoint poll unwind");
    }));
    assert!(unwound.is_err());
    drop(span);
    let incomplete = capture.snapshot();
    assert_eq!((incomplete.closed, incomplete.polls), (0, 2));
    drop(retained);
    let complete = capture.snapshot();
    assert_eq!(
        (complete.opened, complete.closed, complete.polls),
        (1, 1, 2)
    );
    assert_eq!(complete.violations, 0);
    assert!(complete.lifetime >= complete.active_poll);
    assert!(capture.state.lock().unwrap().live.is_empty());
}

#[tokio::test]
async fn cold_v6_history_migrates_exact_receipts_and_unrelated_tombstones()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_v6_migration("notification-history").await
}

#[tokio::test]
async fn cold_v6_unknown_history_migrates_without_repeating_the_lower_effect()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_v6_migration("unknown-history").await
}

#[tokio::test]
async fn cold_v6_migration_resumes_after_a_joined_failure_without_rewriting_accepted_progress()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_v6_migration_resume(MigrationBoundary::Checkpoint, false).await
}

#[tokio::test]
async fn cold_v6_migration_resumes_each_root_phase_without_duplicate_conversion()
-> Result<(), Box<dyn Error + Send + Sync>> {
    for boundary in [
        MigrationBoundary::Claim,
        MigrationBoundary::Records,
        MigrationBoundary::Audit,
        MigrationBoundary::Ready,
    ] {
        StreamFailureState::assert_v6_migration_resume(boundary, false).await?;
    }
    Ok(())
}

#[tokio::test]
async fn cold_v6_migration_observes_committed_root_and_checkpoint_after_lost_replies()
-> Result<(), Box<dyn Error + Send + Sync>> {
    for boundary in [MigrationBoundary::Claim, MigrationBoundary::Checkpoint] {
        StreamFailureState::assert_v6_migration_commit_readback(boundary, false).await?;
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_cold_v6_claims_publish_one_conversion_and_forward_the_real_conflict()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_concurrent_v6_migration_claims().await
}

#[tokio::test]
async fn resumed_v6_migration_rejects_a_new_forbidden_field_without_advancing_progress()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_v6_migration_resume(MigrationBoundary::Checkpoint, true).await
}

#[tokio::test]
async fn cold_v6_migration_preflight_rejects_late_corruption_and_exhaustion_without_writes()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_v6_migration_preflight().await
}

#[tokio::test]
async fn fresh_leaf_changes_and_invalid_references_reject_warm_checkpoint_observation_and_replay()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_settlement_leaf_freshness().await
}

#[tokio::test]
async fn settlement_leaf_read_failure_with_failed_rollback_never_retries_the_mutation()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_settlement_read_rollback_vetoes_retry(StateStoreError::unavailable(
        "load",
        "selected settlement leaf observation failed",
    ))
    .await
}

#[tokio::test]
async fn settlement_leaf_conflict_with_failed_rollback_never_retries_the_mutation()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_settlement_read_rollback_vetoes_retry(
        StateStoreError::transaction_conflict(
            "load",
            "selected settlement leaf observation conflicted",
        ),
    )
    .await
}

#[tokio::test]
async fn same_count_older_settlement_update_preserves_later_pages_and_127_128_129_boundaries()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_older_settlement_page_changes_with_later_pages_retained().await
}

#[tokio::test]
async fn cold_current_pending_unknown_and_historical_definitions_export_a_process_loss_golden()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_cold_recovery_golden().await
}

#[tokio::test]
async fn cold_v6_current_pending_unknown_and_historical_definitions_migrate_without_replay()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_current_recovery_migrates_original_deadline_and_definitions().await
}

#[tokio::test]
async fn input_resolution_does_not_retry_a_leaf_conflict_after_failed_rollback()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_input_rollback_failure_stops_its_public_owner(false).await
}

#[tokio::test]
async fn workspace_deletion_does_not_retry_a_leaf_timeout_after_failed_rollback()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_input_rollback_failure_stops_its_public_owner(true).await
}

#[tokio::test]
async fn acknowledged_lower_resolution_does_not_replay_publication_after_failed_rollback()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_lower_resolution_rollback_failure_stops_publication().await
}

#[tokio::test]
async fn lost_lower_reply_atomically_changes_a_full_page_and_introduces_the_unknown_page()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_uncertain_resolution_changes_two_pages_atomically().await
}

#[tokio::test]
async fn cold_v6_migration_observes_committed_checkpoint_and_new_leaf_after_lost_reply()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_v6_migration_commit_readback(MigrationBoundary::Checkpoint, true)
        .await
}

#[tokio::test]
async fn cold_v6_migration_preserves_commit_error_when_readback_observation_fails()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_v6_migration_failed_readback_preserves_publication(false).await
}

#[tokio::test]
async fn cold_v6_migration_preserves_commit_error_when_readback_revision_mismatches()
-> Result<(), Box<dyn Error + Send + Sync>> {
    StreamFailureState::assert_v6_migration_failed_readback_preserves_publication(true).await
}
