use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::SystemTime;

use storage_sdk::{StateStoreBackend, StateTransactionBackend};
use tokio::task::JoinHandle;

use crate::{
    AgentStorage, ConversationId, ConversationNotification, ConversationNotificationKind,
    NotificationId, NotificationResolution, ProcessState, SdkError, WorkspaceContext, WorkspaceId,
};

const NOTIFICATION_NAMESPACE: &str = "agent_sdk_conversation_notifications_v1";
const NOTIFICATION_RECORD_VERSION: u32 = 2;
const MAX_PENDING_NOTIFICATIONS: usize = 4_096;
const MAX_NOTIFICATION_RECORD_BYTES: usize = 8 << 20;
const MAX_INTERACTION_SCHEMA_BYTES: usize = 1 << 20;
const MAX_INTERACTION_SUBMISSION_BYTES: usize = 1 << 20;
const MAX_ACTIVE_RECEIVERS: usize = 4_096;

pub(crate) struct NotificationStore<S: StateStoreBackend> {
    journal: NotificationJournal<S>,
    delivery: Arc<NotificationDeliveryRuntime>,
}

struct NotificationJournal<S: StateStoreBackend> {
    storage: AgentStorage<S>,
}

struct NotificationDeliveryRuntime {
    executor: tokio::runtime::Handle,
    state: Mutex<NotificationDeliveryState>,
    changed: tokio::sync::Notify,
}

struct NotificationDeliveryState {
    next_lease: u64,
    next_reclamation: u64,
    leases: BTreeMap<ConversationId, ReceiverLease>,
    abandoned_drains: BTreeMap<ConversationId, AbandonedDrainState>,
    blocked_receivers: BTreeMap<ConversationId, WorkspaceId>,
    reclamation_admission: ReclamationAdmission,
}

enum ReclamationAdmission {
    Open,
    Sealed,
}

impl NotificationDeliveryState {
    fn queue_abandoned_drain(&mut self, conversation_id: ConversationId) {
        let sealed = matches!(self.reclamation_admission, ReclamationAdmission::Sealed);
        self.abandoned_drains
            .entry(conversation_id)
            .or_insert_with(|| {
                if sealed {
                    AbandonedDrainState::Blocked(Arc::new(SdkError::invalid_state(
                        "terminal notification reclamation began after shutdown sealing",
                    )))
                } else {
                    AbandonedDrainState::Pending
                }
            });
    }
    fn seal_reclamations(&mut self) {
        self.reclamation_admission = ReclamationAdmission::Sealed;
        for drain in self.abandoned_drains.values_mut() {
            drain.seal();
        }
    }
}

enum AbandonedDrainState {
    Pending,
    Task {
        attempt_id: u64,
        handle: JoinHandle<()>,
        phase: AbandonedDrainPhase,
    },
    Deferred,
    Blocked(Arc<SdkError>),
}

enum AbandonedDrainPhase {
    Running { wake_requested: bool },
    Sealed,
    Reported(Arc<SdkError>),
    Exited(AbandonedDrainExit),
}

enum AbandonedDrainExit {
    Reclaimed,
    Deferred { wake_requested: bool },
    Blocked(Arc<SdkError>),
    Interrupted,
}

impl AbandonedDrainState {
    fn seal(&mut self) {
        let Self::Task { phase, .. } = self else {
            return;
        };
        match phase {
            AbandonedDrainPhase::Running { .. } => *phase = AbandonedDrainPhase::Sealed,
            AbandonedDrainPhase::Exited(exit @ AbandonedDrainExit::Deferred { .. }) => {
                *exit = AbandonedDrainExit::Blocked(Arc::new(SdkError::busy(
                    "terminal notification reclamation requires recovery",
                )));
            }
            AbandonedDrainPhase::Sealed
            | AbandonedDrainPhase::Reported(_)
            | AbandonedDrainPhase::Exited(_) => {}
        }
    }

    fn request_wake(&mut self) -> bool {
        let Self::Task {
            phase: AbandonedDrainPhase::Running { wake_requested },
            ..
        } = self
        else {
            return false;
        };
        *wake_requested = true;
        true
    }

    fn take_wake_request(&mut self, attempt_id: u64) -> bool {
        let Self::Task {
            attempt_id: current,
            phase: AbandonedDrainPhase::Running { wake_requested },
            ..
        } = self
        else {
            return false;
        };
        *current == attempt_id && std::mem::take(wake_requested)
    }

    fn report(&mut self, error: Arc<SdkError>) {
        match self {
            Self::Task {
                phase: AbandonedDrainPhase::Reported(_),
                ..
            }
            | Self::Blocked(_) => {}
            Self::Task { phase, .. } => *phase = AbandonedDrainPhase::Reported(error),
            Self::Pending | Self::Deferred => *self = Self::Blocked(error),
        }
    }

    fn abort(&self) {
        if let Self::Task { handle, .. } = self {
            handle.abort();
        }
    }

    fn finish(&mut self, attempt_id: u64, disposition: Option<AbandonedDrainDisposition>) {
        let Self::Task {
            attempt_id: current,
            phase,
            ..
        } = self
        else {
            return;
        };
        if *current != attempt_id {
            return;
        }
        let exit = match phase {
            AbandonedDrainPhase::Running { wake_requested } => match disposition {
                Some(AbandonedDrainDisposition::Reclaimed) => AbandonedDrainExit::Reclaimed,
                Some(AbandonedDrainDisposition::Deferred) => AbandonedDrainExit::Deferred {
                    wake_requested: *wake_requested,
                },
                Some(AbandonedDrainDisposition::Blocked(error)) => {
                    AbandonedDrainExit::Blocked(Arc::new(error))
                }
                None => AbandonedDrainExit::Interrupted,
            },
            AbandonedDrainPhase::Sealed => match disposition {
                Some(AbandonedDrainDisposition::Reclaimed) => AbandonedDrainExit::Reclaimed,
                Some(AbandonedDrainDisposition::Deferred) => AbandonedDrainExit::Blocked(Arc::new(
                    SdkError::busy("terminal notification reclamation requires recovery"),
                )),
                Some(AbandonedDrainDisposition::Blocked(error)) => {
                    AbandonedDrainExit::Blocked(Arc::new(error))
                }
                None => AbandonedDrainExit::Interrupted,
            },
            AbandonedDrainPhase::Reported(_) | AbandonedDrainPhase::Exited(_) => return,
        };
        *phase = AbandonedDrainPhase::Exited(exit);
    }

    fn handle(&mut self) -> Option<&mut JoinHandle<()>> {
        match self {
            Self::Task { handle, .. } => Some(handle),
            Self::Pending | Self::Deferred | Self::Blocked(_) => None,
        }
    }

    fn waiting_observation(&self) -> AbandonedDrainObservation {
        match self {
            Self::Task {
                phase: AbandonedDrainPhase::Running { .. } | AbandonedDrainPhase::Sealed,
                ..
            } => AbandonedDrainObservation::Active,
            Self::Task {
                phase: AbandonedDrainPhase::Reported(error),
                ..
            } => AbandonedDrainObservation::Reported(Arc::clone(error)),
            Self::Task {
                phase: AbandonedDrainPhase::Exited(_),
                ..
            } => AbandonedDrainObservation::Finishing,
            Self::Pending | Self::Deferred => AbandonedDrainObservation::Deferred,
            Self::Blocked(error) => AbandonedDrainObservation::Blocked(Arc::clone(error)),
        }
    }
}

struct ReceiverLease {
    id: u64,
    workspace_id: WorkspaceId,
    state: ReceiverLeaseState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReceiverLeaseState {
    Open,
    Draining,
    Failed,
}

pub(crate) struct NotificationReceiver<S: StateStoreBackend> {
    store: NotificationStore<S>,
    conversation_id: ConversationId,
    lease_id: u64,
    state: NotificationReceiverState,
}

enum NotificationReceiverState {
    Open(NotificationReceiverProgress),
    Draining(NotificationReceiverProgress),
    Ended(NotificationReceiverRelease),
}

enum NotificationReceiverProgress {
    Ready {
        delivery: NotificationDeliveryCursor,
    },
    Reconciling {
        delivery: NotificationDeliveryCursor,
        receipt: Box<NotificationReceipt>,
    },
}

#[derive(Clone)]
enum NotificationDeliveryCursor {
    Empty,
    Delivered(ReceiverDelivery),
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum NotificationReceiverRelease {
    Lease,
    AbandonedDrain,
    Reconciliation,
}

enum AbandonedDrainDisposition {
    Reclaimed,
    Deferred,
    Blocked(SdkError),
}

enum AbandonedDrainObservation {
    Missing,
    Active,
    Finishing,
    Retry,
    Deferred,
    Reported(Arc<SdkError>),
    Blocked(Arc<SdkError>),
}

struct AbandonedDrainGuard<S: StateStoreBackend> {
    journal: NotificationJournal<S>,
    delivery: Arc<NotificationDeliveryRuntime>,
    conversation_id: ConversationId,
    attempt_id: u64,
    disposition: Option<AbandonedDrainDisposition>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ResolutionAttempt([u8; 16]);

pub(crate) struct ResolutionIntent {
    conversation_id: ConversationId,
    notification_id: NotificationId,
    attempt: ResolutionAttempt,
    settlement_commit: Option<Result<(), SdkError>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResolutionSettlement {
    Accepted,
    Rejected,
    Ambiguous,
    Cancelled,
}

pub(crate) enum ResolutionDisposition {
    Applied(ResolutionSettlement),
    AlreadyAccepted,
}

pub(crate) struct NotificationClosure {
    cancelled: Vec<NotificationId>,
    retains_delivery: bool,
    mutation: NotificationMutation,
}

pub(crate) struct NotificationReconciliation {
    unresolved_acceptance: bool,
    mutation: NotificationMutation,
}

struct NotificationMutation {
    conversation_id: ConversationId,
    before: Option<NotificationRecord>,
    expected: Option<NotificationRecord>,
}

struct NotificationReceipt {
    mutation: NotificationMutation,
    step: NotificationReceiptStep,
}

enum NotificationReceiptError {
    TransactionConflict,
    Failure(SdkError),
}

enum NotificationMutationError {
    TransactionConflict,
    Failure(SdkError),
}

enum NotificationReceiptStep {
    Committed(ReceiveStep),
    Conflicted(ReceiveStep),
    Unconfirmed { step: ReceiveStep, error: SdkError },
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum NotificationMutationObservation {
    Expected,
    Unchanged,
    Contradictory,
}

pub(crate) enum NotificationInitializationError {
    TransactionConflict,
    Failure(SdkError),
}

pub(crate) enum NotificationInteractionCheckError {
    TransactionConflict,
    Failure(SdkError),
}

pub(crate) enum NotificationSettlementError {
    TransactionConflict,
    Failure(SdkError),
}

pub(crate) enum NotificationCloseError {
    TransactionConflict(crate::error::ErrorPhase),
    Failure(SdkError),
}

pub(crate) enum NotificationDeleteError {
    TransactionConflict,
    Failure(SdkError),
}

enum ReceiveStep {
    Item {
        notification: ConversationNotification,
        delivered: ReceiverDelivery,
        draining: bool,
    },
    Wait {
        draining: bool,
    },
    Failed {
        error: SdkError,
        draining: bool,
    },
    End,
}

#[derive(Clone)]
enum ReceiverDelivery {
    Passive(NotificationId),
    Interaction(NotificationId),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum DeliveryLifecycle {
    Open,
    Draining,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct NotificationRecord {
    version: u32,
    workspace_id: WorkspaceId,
    conversation_id: ConversationId,
    next_sequence: u64,
    lifecycle: DeliveryLifecycle,
    pending: VecDeque<StoredNotification>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct StoredNotification {
    sequence: u64,
    id: NotificationId,
    inserted_at: SystemTime,
    kind: StoredNotificationKind,
    state: StoredNotificationState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(
    rename_all = "snake_case",
    tag = "state",
    content = "detail",
    deny_unknown_fields
)]
enum StoredNotificationState {
    Passive,
    Pending,
    Resolving {
        attempt: ResolutionAttempt,
        acceptance: StoredAcceptance,
    },
    Terminal {
        outcome: StoredTerminalOutcome,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum StoredAcceptance {
    Unconfirmed,
    Ambiguous,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum StoredTerminalOutcome {
    Accepted { attempt: ResolutionAttempt },
    ReconciledAccepted,
    Rejected { attempt: ResolutionAttempt },
    Cancelled { attempt: ResolutionAttempt },
    ReconciledCancelled,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "state", deny_unknown_fields)]
enum StoredProcessState {
    Running,
    Exited { exit_code: Option<i32> },
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
enum StoredNotificationKind {
    Message {
        text: String,
    },
    FileChanged {
        path: PathBuf,
        summary: Option<String>,
    },
    Process {
        label: String,
        state: StoredProcessState,
    },
    Progress {
        label: String,
        detail: Option<String>,
    },
    Warning {
        message: String,
    },
    Error {
        message: String,
    },
    Interaction {
        title: String,
        body: String,
        response_schema: Option<serde_json::Value>,
        dismissible: bool,
    },
}

impl<S: StateStoreBackend> Clone for NotificationStore<S> {
    fn clone(&self) -> Self {
        Self {
            journal: self.journal.clone(),
            delivery: self.delivery.clone(),
        }
    }
}

impl<S: StateStoreBackend> Clone for NotificationJournal<S> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
        }
    }
}

impl<S: StateStoreBackend> NotificationStore<S> {
    fn with_commit_evidence(error: SdkError, commit_error: Option<&SdkError>) -> SdkError {
        match commit_error {
            Some(commit) => error.with_supplementary(commit.clone()),
            None => error,
        }
    }

    pub(crate) fn new(storage: AgentStorage<S>) -> Result<Self, SdkError> {
        let executor = tokio::runtime::Handle::try_current().map_err(|_| {
            SdkError::invalid_state("notification runtime requires an active Tokio executor")
        })?;
        Ok(Self {
            journal: NotificationJournal { storage },
            delivery: Arc::new(NotificationDeliveryRuntime {
                executor,
                state: Mutex::new(NotificationDeliveryState {
                    next_lease: 1,
                    next_reclamation: 1,
                    leases: BTreeMap::new(),
                    abandoned_drains: BTreeMap::new(),
                    blocked_receivers: BTreeMap::new(),
                    reclamation_admission: ReclamationAdmission::Open,
                }),
                changed: tokio::sync::Notify::new(),
            }),
        })
    }

    pub(crate) fn executor(&self) -> tokio::runtime::Handle {
        self.delivery.executor.clone()
    }

    pub(crate) async fn initialize_in_transaction(
        transaction: &mut impl StateTransactionBackend,
        workspace_id: &WorkspaceId,
        conversation_id: &ConversationId,
    ) -> Result<(), NotificationInitializationError> {
        let key = NotificationRecord::key(conversation_id)
            .map_err(NotificationInitializationError::Failure)?;
        if transaction
            .load(key.clone())
            .await
            .map_err(|error| match error {
                storage_sdk::StateStoreError::TransactionConflict { .. } => {
                    NotificationInitializationError::TransactionConflict
                }
                error => NotificationInitializationError::Failure(
                    SdkError::from_state_store(error, "notification state could not be checked")
                        .in_phase(crate::error::ErrorPhase::StateRead),
                ),
            })?
            .is_some()
        {
            return Err(NotificationInitializationError::Failure(
                SdkError::conflict("notification state already exists for the conversation"),
            ));
        }
        let value = NotificationRecord::new(workspace_id.clone(), conversation_id.clone())
            .encode()
            .map_err(NotificationInitializationError::Failure)?;
        transaction
            .put(storage_sdk::state::record::StateRecord::new(key, value))
            .await
            .map_err(|error| match error {
                storage_sdk::StateStoreError::TransactionConflict { .. } => {
                    NotificationInitializationError::TransactionConflict
                }
                error => NotificationInitializationError::Failure(
                    SdkError::from_state_store(
                        error,
                        "notification state could not be initialized",
                    )
                    .in_phase(crate::error::ErrorPhase::StateWrite),
                ),
            })
    }

    pub(crate) async fn initialization_matches(
        store: &impl StateStoreBackend,
        workspace_id: &WorkspaceId,
        conversation_id: &ConversationId,
    ) -> Result<bool, SdkError> {
        let snapshot = store
            .load(NotificationRecord::key(conversation_id)?)
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "notification state could not be observed")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })?;
        let Some(snapshot) = snapshot else {
            return Ok(false);
        };
        let observed = NotificationRecord::decode(snapshot, conversation_id)?;
        if observed == NotificationRecord::new(workspace_id.clone(), conversation_id.clone()) {
            Ok(true)
        } else {
            Err(SdkError::conflict(
                "notification initialization changed during observation",
            ))
        }
    }

    pub(crate) async fn initialization_is_absent(
        store: &impl StateStoreBackend,
        conversation_id: &ConversationId,
    ) -> Result<bool, SdkError> {
        store
            .load(NotificationRecord::key(conversation_id)?)
            .await
            .map(|snapshot| snapshot.is_none())
            .map_err(|error| {
                SdkError::from_state_store(error, "notification state could not be observed")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })
    }

    pub(crate) async fn delete_initialization_in_transaction(
        transaction: &mut impl StateTransactionBackend,
        workspace_id: &WorkspaceId,
        conversation_id: &ConversationId,
    ) -> Result<(), SdkError> {
        let key = NotificationRecord::key(conversation_id)?;
        let snapshot = transaction
            .load(key.clone())
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "notification state could not be loaded")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })?
            .ok_or_else(|| SdkError::invalid_state("notification state is missing"))?;
        let observed = NotificationRecord::decode(snapshot, conversation_id)?;
        if observed != NotificationRecord::new(workspace_id.clone(), conversation_id.clone()) {
            return Err(SdkError::conflict(
                "notification state changed during conversation provisioning",
            ));
        }
        transaction.delete(key).await.map_err(|error| match error {
            storage_sdk::StateStoreError::TransactionConflict { .. } => {
                SdkError::conflict("notification state transaction conflicted")
                    .with_detail(crate::error::ErrorDetail::TransactionConflict)
            }
            error => SdkError::from_state_store(error, "notification state could not be deleted")
                .in_phase(crate::error::ErrorPhase::StateWrite),
        })
    }

    pub(crate) async fn delete_in_transaction(
        transaction: &mut impl StateTransactionBackend,
        conversation_id: &ConversationId,
    ) -> Result<(), NotificationDeleteError> {
        let key =
            NotificationRecord::key(conversation_id).map_err(NotificationDeleteError::Failure)?;
        let snapshot = transaction
            .load(key.clone())
            .await
            .map_err(|error| match error {
                storage_sdk::StateStoreError::TransactionConflict { .. } => {
                    NotificationDeleteError::TransactionConflict
                }
                error => NotificationDeleteError::Failure(
                    SdkError::from_state_store(error, "notification state could not be loaded")
                        .in_phase(crate::error::ErrorPhase::StateRead),
                ),
            })?
            .ok_or_else(|| {
                NotificationDeleteError::Failure(SdkError::invalid_state(
                    "notification state is missing",
                ))
            })?;
        let record = NotificationRecord::decode(snapshot, conversation_id)
            .map_err(NotificationDeleteError::Failure)?;
        if record.has_unresolved_interaction() {
            return Err(NotificationDeleteError::Failure(SdkError::busy(
                "conversation has an unresolved interaction",
            )));
        }
        transaction.delete(key).await.map_err(|error| match error {
            storage_sdk::StateStoreError::TransactionConflict { .. } => {
                NotificationDeleteError::TransactionConflict
            }
            error => NotificationDeleteError::Failure(
                SdkError::from_state_store(error, "notification state could not be deleted")
                    .in_phase(crate::error::ErrorPhase::StateWrite),
            ),
        })
    }

    pub(crate) async fn close_in_transaction(
        &self,
        transaction: &mut impl StateTransactionBackend,
        conversation_id: &ConversationId,
    ) -> Result<NotificationClosure, NotificationCloseError> {
        let key =
            NotificationRecord::key(conversation_id).map_err(NotificationCloseError::Failure)?;
        let snapshot = transaction
            .load(key.clone())
            .await
            .map_err(|error| match error {
                storage_sdk::StateStoreError::TransactionConflict { .. } => {
                    NotificationCloseError::TransactionConflict(crate::error::ErrorPhase::StateRead)
                }
                error => NotificationCloseError::Failure(
                    SdkError::from_state_store(error, "notification state could not be loaded")
                        .in_phase(crate::error::ErrorPhase::StateRead),
                ),
            })?;
        let Some(snapshot) = snapshot else {
            return Ok(NotificationClosure::reclaimed(conversation_id.clone()));
        };
        let before = NotificationRecord::decode(snapshot, conversation_id)
            .map_err(NotificationCloseError::Failure)?;
        let mut record = before.clone();
        let cancelled = record.close().map_err(NotificationCloseError::Failure)?;
        let retains_delivery =
            self.has_receiver(conversation_id) || record.has_resolving_interaction();
        let expected = if retains_delivery {
            Some(record.clone())
        } else {
            None
        };
        if retains_delivery {
            transaction
                .put(storage_sdk::state::record::StateRecord::new(
                    key,
                    record.encode().map_err(NotificationCloseError::Failure)?,
                ))
                .await
                .map_err(|error| match error {
                    storage_sdk::StateStoreError::TransactionConflict { .. } => {
                        NotificationCloseError::TransactionConflict(
                            crate::error::ErrorPhase::StateWrite,
                        )
                    }
                    error => NotificationCloseError::Failure(
                        SdkError::from_state_store(error, "notification close could not be stored")
                            .in_phase(crate::error::ErrorPhase::StateWrite),
                    ),
                })?;
        } else {
            transaction.delete(key).await.map_err(|error| match error {
                storage_sdk::StateStoreError::TransactionConflict { .. } => {
                    NotificationCloseError::TransactionConflict(
                        crate::error::ErrorPhase::StateWrite,
                    )
                }
                error => NotificationCloseError::Failure(
                    SdkError::from_state_store(error, "notification state could not be reclaimed")
                        .in_phase(crate::error::ErrorPhase::StateWrite),
                ),
            })?;
        }
        Ok(NotificationClosure::new(
            conversation_id.clone(),
            before,
            expected,
            cancelled,
            retains_delivery,
        ))
    }

    pub(crate) async fn publish(
        &self,
        conversation_id: &ConversationId,
        kind: ConversationNotificationKind,
    ) -> Result<NotificationId, SdkError> {
        let id = NotificationId::new();
        self.publish_with_id(conversation_id, id.clone(), kind)
            .await?;
        Ok(id)
    }

    pub(crate) async fn publish_with_id(
        &self,
        conversation_id: &ConversationId,
        id: NotificationId,
        kind: ConversationNotificationKind,
    ) -> Result<(), SdkError> {
        let stored_kind = StoredNotificationKind::from_public(kind)?;
        let inserted_at = SystemTime::now();
        let store = self.journal.store();
        let mut transaction = store
            .begin_transaction()
            .await
            .map_err(|error| match error {
                storage_sdk::StateStoreError::TransactionConflict { .. } => {
                    SdkError::conflict("notification transaction conflicted")
                        .with_detail(crate::error::ErrorDetail::TransactionConflict)
                }
                error => {
                    SdkError::from_state_store(error, "notification transaction could not begin")
                        .in_phase(crate::error::ErrorPhase::StateWrite)
                }
            })?;
        Self::publish_stored_in_transaction(
            &mut transaction,
            conversation_id,
            id.clone(),
            inserted_at,
            stored_kind.clone(),
        )
        .await
        .map_err(|error| match error {
            NotificationMutationError::TransactionConflict => {
                SdkError::conflict("notification state transaction conflicted")
                    .with_detail(crate::error::ErrorDetail::TransactionConflict)
            }
            NotificationMutationError::Failure(error) => error,
        })?;
        let commit_result = transaction.commit().await;
        Self::observe_publication(
            store,
            conversation_id,
            &id,
            inserted_at,
            &stored_kind,
            commit_result,
        )
        .await?;
        self.delivery.changed.notify_waiters();
        Ok(())
    }

    pub(crate) async fn publish_with_id_before(
        &self,
        conversation_id: &ConversationId,
        id: NotificationId,
        kind: ConversationNotificationKind,
        deadline: tokio::time::Instant,
    ) -> Result<(), SdkError> {
        let stored_kind = StoredNotificationKind::from_public(kind)?;
        let inserted_at = SystemTime::now();
        let store = self.journal.store();
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(SdkError::timeout(
                    "notification publication conflict did not settle before its deadline",
                ));
            }
            let mut transaction = match store.begin_transaction().await {
                Ok(transaction) => transaction,
                Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(error) => {
                    return Err(SdkError::from_state_store(
                        error,
                        "notification transaction could not begin",
                    )
                    .in_phase(crate::error::ErrorPhase::StateWrite));
                }
            };
            if tokio::time::Instant::now() >= deadline {
                let error = SdkError::timeout(
                    "notification publication conflict did not settle before its deadline",
                );
                return Err(match transaction.rollback().await {
                    Ok(()) => error,
                    Err(cleanup) => error.with_supplementary(
                        SdkError::from_state_store(cleanup, "state transaction could not close")
                            .at_phase(crate::error::ErrorPhase::Cleanup),
                    ),
                });
            }
            let reconciliation = match Self::publish_stored_in_transaction(
                &mut transaction,
                conversation_id,
                id.clone(),
                inserted_at,
                stored_kind.clone(),
            )
            .await
            {
                Ok(reconciliation) => reconciliation,
                Err(NotificationMutationError::TransactionConflict) => {
                    drop(transaction);
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(NotificationMutationError::Failure(error)) => return Err(error),
            };
            let commit_error = match transaction.commit().await {
                Ok(()) => {
                    self.delivery.changed.notify_waiters();
                    return Ok(());
                }
                Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(error) => {
                    SdkError::from_state_store(error, "notification publication could not commit")
                        .in_phase(crate::error::ErrorPhase::StateCommit)
                }
            };
            loop {
                match reconciliation
                    .observe_publication(store, &id, inserted_at, &stored_kind)
                    .await
                {
                    Ok(NotificationMutationObservation::Expected) => {
                        self.delivery.changed.notify_waiters();
                        return Ok(());
                    }
                    Ok(NotificationMutationObservation::Unchanged) => {
                        return Err(commit_error);
                    }
                    Ok(NotificationMutationObservation::Contradictory) => {
                        return Err(SdkError::invalid_state(
                            "notification publication commit outcome is contradictory",
                        )
                        .with_supplementary(commit_error));
                    }
                    Err(error) => {
                        let evidence = match error {
                            NotificationMutationError::TransactionConflict => {
                                SdkError::conflict("state transaction conflicted")
                                    .with_detail(crate::error::ErrorDetail::TransactionConflict)
                            }
                            NotificationMutationError::Failure(error)
                                if matches!(
                                    error.kind(),
                                    crate::error::ErrorKind::Integration
                                        | crate::error::ErrorKind::Timeout
                                ) =>
                            {
                                error
                            }
                            NotificationMutationError::Failure(error) => {
                                return Err(error.with_supplementary(commit_error));
                            }
                        };

                        if tokio::time::Instant::now() >= deadline {
                            return Err(SdkError::timeout(
                                "notification publication could not be reconciled before its deadline",
                            ).with_supplementary(commit_error).with_supplementary(evidence.in_phase(crate::error::ErrorPhase::Reconciliation)));
                        }
                        tokio::task::yield_now().await;
                    }
                }
            }
        }
    }

    pub(crate) async fn publish_in_transaction(
        transaction: &mut impl StateTransactionBackend,
        conversation_id: &ConversationId,
        kind: ConversationNotificationKind,
    ) -> Result<(NotificationId, NotificationReconciliation), SdkError> {
        let id = NotificationId::new();
        let reconciliation = Self::publish_stored_in_transaction(
            transaction,
            conversation_id,
            id.clone(),
            SystemTime::now(),
            StoredNotificationKind::from_public(kind)?,
        )
        .await
        .map_err(NotificationMutationError::into_sdk)?;
        Ok((id, reconciliation))
    }

    pub(crate) fn wake_receivers(&self) {
        let abandoned = self
            .delivery
            .state()
            .abandoned_drains
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for conversation_id in abandoned {
            self.wake_abandoned_drain(&conversation_id);
        }
        self.delivery.changed.notify_waiters();
    }

    pub(crate) fn finish_reconciliation(&self, conversation_id: &ConversationId) {
        self.delivery
            .state()
            .blocked_receivers
            .remove(conversation_id);
        self.wake_receivers();
    }

    pub(crate) fn finish_close(&self, conversation_id: &ConversationId) {
        let mut state = self.delivery.state();
        if let Some(lease) = state.leases.get_mut(conversation_id) {
            if lease.state == ReceiverLeaseState::Open {
                lease.state = ReceiverLeaseState::Draining;
            }
        } else {
            state.queue_abandoned_drain(conversation_id.clone());
        }
        drop(state);
        self.wake_receivers();
    }

    pub(crate) fn receiver(
        &self,
        workspace_id: &WorkspaceId,
        conversation_id: &ConversationId,
    ) -> Result<NotificationReceiver<S>, SdkError> {
        let mut state = self.delivery.state();
        if state.abandoned_drains.contains_key(conversation_id) {
            return Err(SdkError::closed("notification delivery is closed"));
        }
        if state.blocked_receivers.contains_key(conversation_id) {
            return Err(SdkError::conflict(
                "notification delivery is awaiting interaction settlement",
            ));
        }
        if state.leases.contains_key(conversation_id) {
            return Err(SdkError::busy(
                "conversation already has an active notification receiver",
            ));
        }
        if state.leases.len() >= MAX_ACTIVE_RECEIVERS {
            return Err(SdkError::busy(
                "notification receiver capacity is saturated",
            ));
        }
        let lease_id = state.next_lease;
        state.next_lease = state
            .next_lease
            .checked_add(1)
            .ok_or_else(|| SdkError::invalid_state("notification lease identity exhausted"))?;
        state.leases.insert(
            conversation_id.clone(),
            ReceiverLease {
                id: lease_id,
                workspace_id: workspace_id.clone(),
                state: ReceiverLeaseState::Open,
            },
        );
        Ok(NotificationReceiver {
            store: self.clone(),
            conversation_id: conversation_id.clone(),
            lease_id,
            state: NotificationReceiverState::Open(NotificationReceiverProgress::Ready {
                delivery: NotificationDeliveryCursor::Empty,
            }),
        })
    }

    pub(crate) async fn wait_for_terminal_reclamation(
        &self,
        conversation_id: &ConversationId,
    ) -> Result<(), SdkError> {
        loop {
            let mut changed = std::pin::pin!(self.delivery.changed.notified());
            changed.as_mut().enable();
            self.schedule_abandoned_drain(conversation_id);
            match self.observe_abandoned_drain(conversation_id) {
                AbandonedDrainObservation::Reported(error)
                | AbandonedDrainObservation::Blocked(error) => {
                    return Err(Self::replay_abandoned_drain_error(error.as_ref()));
                }
                AbandonedDrainObservation::Finishing => {
                    tokio::task::yield_now().await;
                    continue;
                }
                AbandonedDrainObservation::Retry => {
                    self.schedule_abandoned_drain(conversation_id);
                    continue;
                }
                AbandonedDrainObservation::Missing
                | AbandonedDrainObservation::Active
                | AbandonedDrainObservation::Deferred => {}
            }
            let store = self.journal.store();
            if Self::initialization_is_absent(store, conversation_id).await? {
                return Ok(());
            }
            changed.await;
        }
    }

    pub(crate) async fn seal_reclamations_before(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), SdkError> {
        self.prepare_terminal_reclamations();
        self.wake_receivers();
        self.delivery.state().seal_reclamations();
        let mut first_error = None;
        let mut deadline_reported = false;
        loop {
            let mut changed = std::pin::pin!(self.delivery.changed.notified());
            changed.as_mut().enable();
            let conversations = self
                .delivery
                .state()
                .abandoned_drains
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            let mut active = false;
            let mut finishing = false;
            for conversation_id in conversations {
                match self.observe_abandoned_drain(&conversation_id) {
                    AbandonedDrainObservation::Missing => {}
                    AbandonedDrainObservation::Active => active = true,
                    AbandonedDrainObservation::Finishing => finishing = true,
                    AbandonedDrainObservation::Retry => {
                        self.schedule_abandoned_drain(&conversation_id);
                        active = true;
                    }
                    AbandonedDrainObservation::Deferred => {
                        let error = self.block_deferred_reclamation(&conversation_id);
                        if first_error.is_none() {
                            first_error = Some(Self::replay_abandoned_drain_error(error.as_ref()));
                        }
                    }
                    AbandonedDrainObservation::Reported(error) => {
                        active = true;
                        if first_error.is_none() {
                            first_error = Some(Self::replay_abandoned_drain_error(error.as_ref()));
                        }
                    }
                    AbandonedDrainObservation::Blocked(error) => {
                        if first_error.is_none() {
                            first_error = Some(Self::replay_abandoned_drain_error(error.as_ref()));
                        }
                    }
                }
            }
            if !active && !finishing {
                return first_error.map_or(Ok(()), Err);
            }
            if deadline_reported {
                if finishing {
                    tokio::task::yield_now().await;
                } else {
                    changed.await;
                }
                continue;
            }
            if finishing {
                if tokio::time::Instant::now() >= deadline {
                    let error = self.block_active_reclamations();
                    if first_error.is_none() {
                        first_error = Some(Self::replay_abandoned_drain_error(error.as_ref()));
                    }
                    deadline_reported = true;
                    continue;
                }
                tokio::task::yield_now().await;
                continue;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                let error = self.block_active_reclamations();
                if first_error.is_none() {
                    first_error = Some(Self::replay_abandoned_drain_error(error.as_ref()));
                }
                deadline_reported = true;
            }
        }
    }

    fn prepare_terminal_reclamations(&self) {
        let mut state = self.delivery.state();
        let draining = state
            .leases
            .iter()
            .filter(|(_, lease)| lease.state == ReceiverLeaseState::Draining)
            .map(|(conversation_id, _)| conversation_id.clone())
            .collect::<Vec<_>>();
        for conversation_id in draining {
            state.leases.remove(&conversation_id);
            state.queue_abandoned_drain(conversation_id);
        }
        drop(state);
        self.delivery.changed.notify_waiters();
    }

    fn block_deferred_reclamation(&self, conversation_id: &ConversationId) -> Arc<SdkError> {
        let mut state = self.delivery.state();
        if let Some(AbandonedDrainState::Blocked(error)) =
            state.abandoned_drains.get(conversation_id)
        {
            return Arc::clone(error);
        }
        let error = Arc::new(SdkError::busy(
            "terminal notification reclamation requires recovery",
        ));
        if matches!(
            state.abandoned_drains.get(conversation_id),
            Some(AbandonedDrainState::Pending | AbandonedDrainState::Deferred)
        ) {
            state.abandoned_drains.insert(
                conversation_id.clone(),
                AbandonedDrainState::Blocked(Arc::clone(&error)),
            );
        }
        drop(state);
        self.delivery.changed.notify_waiters();
        error
    }

    fn block_active_reclamations(&self) -> Arc<SdkError> {
        let error = Arc::new(SdkError::busy(
            "terminal notification reclamation is still active",
        ));
        let mut state = self.delivery.state();
        for drain in state.abandoned_drains.values_mut() {
            drain.report(Arc::clone(&error));
            drain.abort();
        }
        drop(state);
        self.delivery.changed.notify_waiters();
        error
    }

    pub(crate) async fn ensure_no_unresolved_interaction(
        &self,
        conversation_id: &ConversationId,
    ) -> Result<(), SdkError> {
        let store = self.journal.store();
        let snapshot = store
            .load(NotificationRecord::key(conversation_id)?)
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "interaction state could not be loaded")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })?
            .ok_or_else(|| SdkError::invalid_state("notification state is missing"))?;
        let record = NotificationRecord::decode(snapshot, conversation_id)?;
        record.ensure_no_unresolved_interaction()
    }

    pub(crate) async fn ensure_no_unresolved_interaction_in_transaction(
        transaction: &mut impl StateTransactionBackend,
        conversation_id: &ConversationId,
    ) -> Result<(), SdkError> {
        Self::check_no_unresolved_interaction_in_transaction(transaction, conversation_id)
            .await
            .map_err(NotificationInteractionCheckError::into_sdk)
    }

    pub(crate) async fn check_no_unresolved_interaction_in_transaction(
        transaction: &mut impl StateTransactionBackend,
        conversation_id: &ConversationId,
    ) -> Result<(), NotificationInteractionCheckError> {
        let key = NotificationRecord::key(conversation_id)
            .map_err(NotificationInteractionCheckError::Failure)?;
        let snapshot = transaction
            .load(key)
            .await
            .map_err(|error| match error {
                storage_sdk::StateStoreError::TransactionConflict { .. } => {
                    NotificationInteractionCheckError::TransactionConflict
                }
                error => NotificationInteractionCheckError::Failure(
                    SdkError::from_state_store(error, "interaction state could not be loaded")
                        .in_phase(crate::error::ErrorPhase::StateRead),
                ),
            })?
            .ok_or_else(|| {
                NotificationInteractionCheckError::Failure(SdkError::invalid_state(
                    "notification state is missing",
                ))
            })?;
        NotificationRecord::decode(snapshot, conversation_id)
            .map_err(NotificationInteractionCheckError::Failure)?
            .ensure_no_unresolved_interaction()
            .map_err(NotificationInteractionCheckError::Failure)
    }

    pub(crate) async fn ensure_no_resolving_interaction_in_transaction(
        transaction: &mut impl StateTransactionBackend,
        conversation_id: &ConversationId,
    ) -> Result<(), SdkError> {
        Self::check_no_resolving_interaction_in_transaction(transaction, conversation_id)
            .await
            .map_err(NotificationInteractionCheckError::into_sdk)
    }

    pub(crate) async fn check_no_resolving_interaction_in_transaction(
        transaction: &mut impl StateTransactionBackend,
        conversation_id: &ConversationId,
    ) -> Result<(), NotificationInteractionCheckError> {
        let snapshot = transaction
            .load(
                NotificationRecord::key(conversation_id)
                    .map_err(NotificationInteractionCheckError::Failure)?,
            )
            .await
            .map_err(|error| match error {
                storage_sdk::StateStoreError::TransactionConflict { .. } => {
                    NotificationInteractionCheckError::TransactionConflict
                }
                error => NotificationInteractionCheckError::Failure(
                    SdkError::from_state_store(error, "interaction state could not be loaded")
                        .in_phase(crate::error::ErrorPhase::StateRead),
                ),
            })?
            .ok_or_else(|| {
                NotificationInteractionCheckError::Failure(SdkError::invalid_state(
                    "notification state is missing",
                ))
            })?;
        NotificationRecord::decode(snapshot, conversation_id)
            .map_err(NotificationInteractionCheckError::Failure)?
            .ensure_no_resolving_interaction()
            .map_err(NotificationInteractionCheckError::Failure)
    }

    pub(crate) async fn ensure_interaction_pending_in_transaction(
        transaction: &mut impl StateTransactionBackend,
        conversation_id: &ConversationId,
        notification_id: &NotificationId,
    ) -> Result<(), SdkError> {
        let snapshot = transaction
            .load(NotificationRecord::key(conversation_id)?)
            .await
            .map_err(|error| {
                SdkError::from_state_store(
                    error,
                    "interaction cancellation authority could not be loaded",
                )
                .at_phase(crate::error::ErrorPhase::StateRead)
            })?
            .ok_or_else(|| {
                SdkError::invalid_state("interaction notification journal is missing")
            })?;
        let record = NotificationRecord::decode(snapshot, conversation_id)?;
        let notification = record
            .pending
            .iter()
            .find(|item| &item.id == notification_id)
            .ok_or_else(|| SdkError::conflict("interaction cancellation notification is absent"))?;
        if notification.state != StoredNotificationState::Pending {
            return Err(SdkError::conflict(
                "interaction cancellation does not own a pending notification",
            ));
        }
        Ok(())
    }

    pub(crate) async fn ensure_interaction_matches_in_transaction(
        transaction: &mut impl StateTransactionBackend,
        conversation_id: &ConversationId,
        notification_id: &NotificationId,
        kind: &ConversationNotificationKind,
    ) -> Result<(), NotificationInteractionCheckError> {
        let expected = StoredNotificationKind::from_public(kind.clone())
            .map_err(NotificationInteractionCheckError::Failure)?;
        if !expected.is_interaction() {
            return Err(NotificationInteractionCheckError::Failure(
                SdkError::protocol("notification does not describe an interaction"),
            ));
        }
        let snapshot = transaction
            .load(
                NotificationRecord::key(conversation_id)
                    .map_err(NotificationInteractionCheckError::Failure)?,
            )
            .await
            .map_err(|error| match error {
                storage_sdk::StateStoreError::TransactionConflict { .. } => {
                    NotificationInteractionCheckError::TransactionConflict
                }
                error => NotificationInteractionCheckError::Failure(
                    SdkError::from_state_store(error, "interaction state could not be loaded")
                        .in_phase(crate::error::ErrorPhase::StateRead),
                ),
            })?
            .ok_or_else(|| {
                NotificationInteractionCheckError::Failure(SdkError::invalid_state(
                    "notification state is missing",
                ))
            })?;
        let record = NotificationRecord::decode(snapshot, conversation_id)
            .map_err(NotificationInteractionCheckError::Failure)?;
        let actual = record
            .pending
            .iter()
            .find(|notification| &notification.id == notification_id)
            .ok_or_else(|| {
                NotificationInteractionCheckError::Failure(SdkError::invalid_state(
                    "interaction notification is missing",
                ))
            })?;
        if actual.kind != expected || !actual.state.is_unresolved() {
            return Err(NotificationInteractionCheckError::Failure(
                SdkError::protocol("interaction identity was reused with different content"),
            ));
        }
        Ok(())
    }

    pub(crate) async fn begin_resolution(
        &self,
        conversation_id: &ConversationId,
        notification_id: NotificationId,
        resolution: NotificationResolution,
        deadline: tokio::time::Instant,
    ) -> Result<ResolutionIntent, SdkError> {
        resolution.validate()?;
        let attempt = ResolutionAttempt::new();
        let intent = ResolutionIntent::new(conversation_id.clone(), notification_id, attempt);
        let store = self.journal.store();
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(SdkError::timeout(
                    "interaction intent conflict did not settle before its deadline",
                ));
            }
            let mut transaction = match store.begin_transaction().await {
                Ok(transaction) => transaction,
                Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(error) => {
                    return Err(SdkError::from_state_store(
                        error,
                        "interaction transaction could not begin",
                    )
                    .in_phase(crate::error::ErrorPhase::StateWrite));
                }
            };
            if tokio::time::Instant::now() >= deadline {
                let error = SdkError::timeout(
                    "interaction intent conflict did not settle before its deadline",
                );
                return Err(match transaction.rollback().await {
                    Ok(()) => error,
                    Err(cleanup) => error.with_supplementary(
                        SdkError::from_state_store(cleanup, "state transaction could not close")
                            .at_phase(crate::error::ErrorPhase::Cleanup),
                    ),
                });
            }
            let mutation = async {
                let key = NotificationRecord::key(conversation_id)
                    .map_err(NotificationMutationError::Failure)?;
                let snapshot = transaction
                    .load(key.clone())
                    .await
                    .map_err(|error| match error {
                        storage_sdk::StateStoreError::TransactionConflict { .. } => {
                            NotificationMutationError::TransactionConflict
                        }
                        error => NotificationMutationError::Failure(
                            SdkError::from_state_store(
                                error,
                                "interaction state could not be loaded",
                            )
                            .in_phase(crate::error::ErrorPhase::StateRead),
                        ),
                    })?
                    .ok_or_else(|| {
                        NotificationMutationError::Failure(SdkError::not_found(
                            "notification does not exist",
                        ))
                    })?;
                let mut record = NotificationRecord::decode(snapshot, conversation_id)
                    .map_err(NotificationMutationError::Failure)?;
                record
                    .ensure_open()
                    .map_err(NotificationMutationError::Failure)?;
                let notification = record
                    .pending
                    .front_mut()
                    .filter(|notification| &notification.id == intent.notification_id())
                    .ok_or_else(|| {
                        NotificationMutationError::Failure(SdkError::not_found(
                            "notification is not pending",
                        ))
                    })?;
                notification
                    .begin_resolution(&resolution, attempt)
                    .map_err(NotificationMutationError::Failure)?;
                transaction
                    .put(storage_sdk::state::record::StateRecord::new(
                        key,
                        record
                            .encode()
                            .map_err(NotificationMutationError::Failure)?,
                    ))
                    .await
                    .map_err(|error| match error {
                        storage_sdk::StateStoreError::TransactionConflict { .. } => {
                            NotificationMutationError::TransactionConflict
                        }
                        error => NotificationMutationError::Failure(
                            SdkError::from_state_store(
                                error,
                                "interaction intent could not be stored",
                            )
                            .in_phase(crate::error::ErrorPhase::StateWrite),
                        ),
                    })?;
                Ok(())
            }
            .await;
            match mutation {
                Ok(()) => {}
                Err(NotificationMutationError::TransactionConflict) => {
                    drop(transaction);
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(NotificationMutationError::Failure(error)) => return Err(error),
            };
            let commit_error = match transaction.commit().await {
                Ok(()) => None,
                Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(error) => Some(
                    SdkError::from_state_store(error, "interaction intent could not commit")
                        .in_phase(crate::error::ErrorPhase::StateCommit),
                ),
            };
            loop {
                match intent.observe(store).await {
                    Ok(NotificationMutationObservation::Expected) => return Ok(intent),
                    Ok(NotificationMutationObservation::Unchanged) => {
                        return Err(commit_error.unwrap_or_else(|| {
                            SdkError::invalid_state("interaction intent was not durably observed")
                        }));
                    }
                    Ok(NotificationMutationObservation::Contradictory) => {
                        return Err(Self::with_commit_evidence(
                            SdkError::conflict(
                                "interaction intent commit outcome requires recovery",
                            )
                            .with_detail(crate::error::ErrorDetail::AcceptanceUnknown)
                            .in_phase(crate::error::ErrorPhase::Reconciliation),
                            commit_error.as_ref(),
                        ));
                    }
                    Err(error) => {
                        let evidence = match error {
                            NotificationMutationError::TransactionConflict => {
                                SdkError::conflict("state transaction conflicted")
                                    .with_detail(crate::error::ErrorDetail::TransactionConflict)
                            }
                            NotificationMutationError::Failure(error)
                                if matches!(
                                    error.kind(),
                                    crate::error::ErrorKind::Integration
                                        | crate::error::ErrorKind::Timeout
                                ) =>
                            {
                                error
                            }
                            NotificationMutationError::Failure(error) => {
                                return Err(Self::with_commit_evidence(
                                    error,
                                    commit_error.as_ref(),
                                ));
                            }
                        };

                        if tokio::time::Instant::now() >= deadline {
                            return Err(Self::with_commit_evidence(
                                SdkError::conflict(
                                    "interaction intent confirmation requires recovery",
                                )
                                .with_detail(crate::error::ErrorDetail::AcceptanceUnknown)
                                .in_phase(crate::error::ErrorPhase::Reconciliation),
                                commit_error.as_ref(),
                            )
                            .with_supplementary(
                                evidence.in_phase(crate::error::ErrorPhase::Reconciliation),
                            ));
                        }
                        tokio::task::yield_now().await;
                    }
                }
            }
        }
    }

    pub(crate) async fn settle_resolution_in_transaction(
        transaction: &mut impl StateTransactionBackend,
        intent: &ResolutionIntent,
        settlement: ResolutionSettlement,
    ) -> Result<ResolutionDisposition, NotificationSettlementError> {
        let key = NotificationRecord::key(&intent.conversation_id)
            .map_err(NotificationSettlementError::Failure)?;
        let snapshot = transaction
            .load(key.clone())
            .await
            .map_err(|error| {
                NotificationSettlementError::storage(error, "interaction state could not be loaded")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })?
            .ok_or_else(|| {
                NotificationSettlementError::Failure(SdkError::not_found(
                    "notification does not exist",
                ))
            })?;
        let mut record = NotificationRecord::decode(snapshot, &intent.conversation_id)
            .map_err(NotificationSettlementError::Failure)?;
        let disposition = record
            .settle_resolution(intent, settlement)
            .map_err(NotificationSettlementError::Failure)?;
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                key,
                record
                    .encode()
                    .map_err(NotificationSettlementError::Failure)?,
            ))
            .await
            .map_err(|error| {
                NotificationSettlementError::storage(
                    error,
                    "interaction settlement could not be stored",
                )
                .in_phase(crate::error::ErrorPhase::StateWrite)
            })?;
        Ok(disposition)
    }

    pub(crate) async fn reconcile_native_resolution_in_transaction(
        transaction: &mut impl StateTransactionBackend,
        conversation_id: &ConversationId,
        notification_id: &NotificationId,
        settlement: ResolutionSettlement,
    ) -> Result<NotificationReconciliation, NotificationSettlementError> {
        if !matches!(
            settlement,
            ResolutionSettlement::Accepted | ResolutionSettlement::Cancelled
        ) {
            return Err(NotificationSettlementError::Failure(
                SdkError::invalid_state("interaction resolution has no terminal outcome"),
            ));
        }
        let key = NotificationRecord::key(conversation_id)
            .map_err(NotificationSettlementError::Failure)?;
        let snapshot = transaction
            .load(key.clone())
            .await
            .map_err(|error| match error {
                storage_sdk::StateStoreError::TransactionConflict { .. } => {
                    NotificationSettlementError::TransactionConflict
                }
                error => NotificationSettlementError::Failure(
                    SdkError::from_state_store(error, "interaction state could not be loaded")
                        .in_phase(crate::error::ErrorPhase::StateRead),
                ),
            })?
            .ok_or_else(|| {
                NotificationSettlementError::Failure(SdkError::invalid_state(
                    "notification state is missing",
                ))
            })?;
        let before = NotificationRecord::decode(snapshot, conversation_id)
            .map_err(NotificationSettlementError::Failure)?;
        let mut record = before.clone();
        record
            .reconcile_native_resolution(notification_id, settlement)
            .map_err(NotificationSettlementError::Failure)?;
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                key,
                record
                    .encode()
                    .map_err(NotificationSettlementError::Failure)?,
            ))
            .await
            .map_err(|error| match error {
                storage_sdk::StateStoreError::TransactionConflict { .. } => {
                    NotificationSettlementError::TransactionConflict
                }
                error => NotificationSettlementError::Failure(
                    SdkError::from_state_store(error, "interaction resolution could not be stored")
                        .in_phase(crate::error::ErrorPhase::StateWrite),
                ),
            })?;
        let unresolved_acceptance = record.has_resolving_interaction();
        Ok(NotificationReconciliation::new(
            conversation_id.clone(),
            Some(before),
            Some(record),
            unresolved_acceptance,
        ))
    }

    pub(crate) async fn cancel_pending_interactions(
        &self,
        conversation_id: &ConversationId,
    ) -> Result<bool, SdkError> {
        let key = NotificationRecord::key(conversation_id)?;
        let store = self.journal.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            SdkError::from_state_store(error, "interaction state update could not begin")
                .in_phase(crate::error::ErrorPhase::StateWrite)
        })?;
        let snapshot = transaction
            .load(key.clone())
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "interaction state could not be loaded")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })?
            .ok_or_else(|| SdkError::invalid_state("notification state is missing"))?;
        let before = NotificationRecord::decode(snapshot, conversation_id)?;
        let mut expected = before.clone();
        let changed = expected.cancel_pending_interactions();
        let unresolved_acceptance = expected.has_resolving_interaction();
        if !changed {
            return Ok(unresolved_acceptance);
        }
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                key.clone(),
                expected.encode()?,
            ))
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "interaction cancellation could not be stored")
                    .in_phase(crate::error::ErrorPhase::StateWrite)
            })?;
        let commit_error = transaction.commit().await.err().map(|error| {
            SdkError::from_state_store(error, "interaction cancellation could not commit")
                .in_phase(crate::error::ErrorPhase::StateCommit)
        });
        let observed = store
            .load(key)
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "interaction state could not be loaded")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })
            .map_err(|error| Self::with_commit_evidence(error, commit_error.as_ref()))?
            .ok_or_else(|| {
                Self::with_commit_evidence(
                    SdkError::invalid_state("notification state is missing"),
                    commit_error.as_ref(),
                )
            })?;
        let observed = NotificationRecord::decode(observed, conversation_id)
            .map_err(|error| Self::with_commit_evidence(error, commit_error.as_ref()))?;
        if observed == expected {
            return Ok(unresolved_acceptance);
        }
        if observed == before {
            return Err(commit_error.unwrap_or_else(|| {
                SdkError::invalid_state("interaction cancellation was not durably observed")
            }));
        }
        Err(Self::with_commit_evidence(
            SdkError::conflict("interaction state changed while cancellation was reconciling"),
            commit_error.as_ref(),
        ))
    }

    pub(crate) async fn reconcile_lost_interactions_in_transaction(
        transaction: &mut impl StateTransactionBackend,
        conversation_id: &ConversationId,
    ) -> Result<NotificationReconciliation, SdkError> {
        let key =
            NotificationRecord::key(conversation_id).map_err(NotificationMutationError::Failure)?;
        let snapshot = transaction
            .load(key.clone())
            .await
            .map_err(|error| match error {
                storage_sdk::StateStoreError::TransactionConflict { .. } => {
                    NotificationMutationError::TransactionConflict
                }
                error => NotificationMutationError::Failure(
                    SdkError::from_state_store(error, "interaction state could not be loaded")
                        .in_phase(crate::error::ErrorPhase::StateRead),
                ),
            })?
            .ok_or_else(|| {
                NotificationMutationError::Failure(SdkError::invalid_state(
                    "notification state is missing",
                ))
            })?;
        let before = NotificationRecord::decode(snapshot, conversation_id)
            .map_err(NotificationMutationError::Failure)?;
        let mut record = before.clone();
        if record.reconcile_lost_interactions() {
            transaction
                .put(storage_sdk::state::record::StateRecord::new(
                    key,
                    record
                        .encode()
                        .map_err(NotificationMutationError::Failure)?,
                ))
                .await
                .map_err(|error| match error {
                    storage_sdk::StateStoreError::TransactionConflict { .. } => {
                        NotificationMutationError::TransactionConflict
                    }
                    error => NotificationMutationError::Failure(
                        SdkError::from_state_store(
                            error,
                            "lost interaction cancellation could not be stored",
                        )
                        .in_phase(crate::error::ErrorPhase::StateWrite),
                    ),
                })?;
        }
        let unresolved_acceptance = record.has_resolving_interaction();
        Ok(NotificationReconciliation::new(
            conversation_id.clone(),
            Some(before),
            Some(record),
            unresolved_acceptance,
        ))
    }

    pub(crate) async fn reconcile_closing_interactions_in_transaction(
        transaction: &mut impl StateTransactionBackend,
        conversation_id: &ConversationId,
    ) -> Result<NotificationReconciliation, SdkError> {
        let key = NotificationRecord::key(conversation_id)?;
        let Some(snapshot) = transaction.load(key.clone()).await.map_err(|error| {
            SdkError::from_state_store(error, "interaction state could not be loaded")
                .in_phase(crate::error::ErrorPhase::StateRead)
        })?
        else {
            return Ok(NotificationReconciliation::new(
                conversation_id.clone(),
                None,
                None,
                false,
            ));
        };
        let before = NotificationRecord::decode(snapshot, conversation_id)?;
        let mut record = before.clone();
        if record.reconcile_lost_interactions() {
            transaction
                .put(storage_sdk::state::record::StateRecord::new(
                    key,
                    record.encode()?,
                ))
                .await
                .map_err(|error| {
                    SdkError::from_state_store(
                        error,
                        "lost interaction cancellation could not be stored",
                    )
                    .in_phase(crate::error::ErrorPhase::StateWrite)
                })?;
        }
        let unresolved_acceptance = record.has_resolving_interaction();
        Ok(NotificationReconciliation::new(
            conversation_id.clone(),
            Some(before),
            Some(record),
            unresolved_acceptance,
        ))
    }

    pub(crate) async fn reclaim_terminal_in_transaction(
        &self,
        transaction: &mut impl StateTransactionBackend,
        conversation_id: &ConversationId,
    ) -> Result<(), SdkError> {
        let key = NotificationRecord::key(conversation_id)?;
        let snapshot = transaction
            .load(key.clone())
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "notification state could not be loaded")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })?
            .ok_or_else(|| SdkError::invalid_state("notification state is missing"))?;
        let record = NotificationRecord::decode(snapshot, conversation_id)?;
        if record.lifecycle == DeliveryLifecycle::Draining
            && !record.has_resolving_interaction()
            && !self.has_receiver(conversation_id)
        {
            transaction.delete(key).await.map_err(|error| {
                SdkError::from_state_store(
                    error,
                    "terminal notification state could not be reclaimed",
                )
                .in_phase(crate::error::ErrorPhase::StateWrite)
            })?;
        }
        Ok(())
    }

    pub(crate) fn invalidate_all(&self) {
        self.prepare_terminal_reclamations();
        self.wake_receivers();
        let mut state = self.delivery.state();
        state.seal_reclamations();
        state.leases.clear();
        state.blocked_receivers.clear();
        drop(state);
        self.delivery.changed.notify_waiters();
    }

    pub(crate) fn fail_all(&self) {
        let mut state = self.delivery.state();
        for lease in state.leases.values_mut() {
            if lease.state == ReceiverLeaseState::Open {
                lease.state = ReceiverLeaseState::Failed;
            }
        }
        drop(state);
        self.delivery.changed.notify_waiters();
    }

    pub(crate) fn receiver_lease(&self, conversation_id: &ConversationId) -> Option<u64> {
        self.delivery
            .state()
            .leases
            .get(conversation_id)
            .filter(|lease| lease.state == ReceiverLeaseState::Open)
            .map(|lease| lease.id)
    }

    pub(crate) fn fail_receiver(&self, conversation_id: &ConversationId, lease_id: u64) {
        let mut state = self.delivery.state();
        if let Some(lease) = state.leases.get_mut(conversation_id)
            && lease.id == lease_id
            && lease.state == ReceiverLeaseState::Open
        {
            lease.state = ReceiverLeaseState::Failed;
        }
        drop(state);
        self.delivery.changed.notify_waiters();
    }

    pub(crate) fn invalidate_workspace(&self, workspace_id: &WorkspaceId) {
        let mut state = self.delivery.state();
        let draining = state
            .leases
            .iter()
            .filter(|(_, lease)| {
                &lease.workspace_id == workspace_id && lease.state == ReceiverLeaseState::Draining
            })
            .map(|(conversation_id, _)| conversation_id.clone())
            .collect::<Vec<_>>();
        state
            .leases
            .retain(|_, lease| &lease.workspace_id != workspace_id);
        state
            .blocked_receivers
            .retain(|_, blocked_workspace| blocked_workspace != workspace_id);
        for conversation_id in &draining {
            state.queue_abandoned_drain(conversation_id.clone());
        }
        drop(state);
        for conversation_id in draining {
            self.wake_abandoned_drain(&conversation_id);
        }
        self.delivery.changed.notify_waiters();
    }

    pub(crate) fn invalidate_conversation(&self, conversation_id: &ConversationId) {
        let mut state = self.delivery.state();
        let draining = state
            .leases
            .remove(conversation_id)
            .is_some_and(|lease| lease.state == ReceiverLeaseState::Draining);
        state.blocked_receivers.remove(conversation_id);
        if draining {
            state.queue_abandoned_drain(conversation_id.clone());
        }
        drop(state);
        if draining {
            self.wake_abandoned_drain(conversation_id);
        }
        self.delivery.changed.notify_waiters();
    }

    async fn publish_stored_in_transaction(
        transaction: &mut impl StateTransactionBackend,
        conversation_id: &ConversationId,
        id: NotificationId,
        inserted_at: SystemTime,
        kind: StoredNotificationKind,
    ) -> Result<NotificationReconciliation, NotificationMutationError> {
        let key =
            NotificationRecord::key(conversation_id).map_err(NotificationMutationError::Failure)?;
        let snapshot = transaction
            .load(key.clone())
            .await
            .map_err(|error| match error {
                storage_sdk::StateStoreError::TransactionConflict { .. } => {
                    NotificationMutationError::TransactionConflict
                }
                error => NotificationMutationError::Failure(
                    SdkError::from_state_store(error, "notification state could not be loaded")
                        .in_phase(crate::error::ErrorPhase::StateRead),
                ),
            })?
            .ok_or_else(|| {
                NotificationMutationError::Failure(SdkError::invalid_state(
                    "notification state is missing",
                ))
            })?;
        let before = NotificationRecord::decode(snapshot, conversation_id)
            .map_err(NotificationMutationError::Failure)?;
        let mut record = before.clone();
        record
            .publish(id, inserted_at, kind)
            .map_err(NotificationMutationError::Failure)?;
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                key,
                record
                    .encode()
                    .map_err(NotificationMutationError::Failure)?,
            ))
            .await
            .map_err(|error| match error {
                storage_sdk::StateStoreError::TransactionConflict { .. } => {
                    NotificationMutationError::TransactionConflict
                }
                error => NotificationMutationError::Failure(
                    SdkError::from_state_store(error, "notification could not be stored")
                        .in_phase(crate::error::ErrorPhase::StateWrite),
                ),
            })?;
        Ok(NotificationReconciliation::new(
            conversation_id.clone(),
            Some(before),
            Some(record),
            false,
        ))
    }

    async fn observe_publication(
        store: &impl StateStoreBackend,
        conversation_id: &ConversationId,
        id: &NotificationId,
        inserted_at: SystemTime,
        kind: &StoredNotificationKind,
        commit_result: Result<(), storage_sdk::StateStoreError>,
    ) -> Result<(), SdkError> {
        let commit_error = commit_result.err().map(|error| {
            SdkError::from_state_store(error, "notification publication could not commit")
                .in_phase(crate::error::ErrorPhase::StateCommit)
        });
        let snapshot = store
            .load(NotificationRecord::key(conversation_id)?)
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "notification publication could not be observed")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })
            .map_err(|error| Self::with_commit_evidence(error, commit_error.as_ref()))?
            .ok_or_else(|| {
                commit_error.clone().unwrap_or_else(|| {
                    SdkError::integration("notification publication could not be confirmed")
                        .in_phase(crate::error::ErrorPhase::Reconciliation)
                })
            })?;
        let record = NotificationRecord::decode(snapshot, conversation_id)
            .map_err(|error| Self::with_commit_evidence(error, commit_error.as_ref()))?;
        let Some(notification) = record
            .pending
            .iter()
            .find(|notification| &notification.id == id)
        else {
            return Err(commit_error.unwrap_or_else(|| {
                SdkError::integration("notification publication could not be confirmed")
                    .in_phase(crate::error::ErrorPhase::Reconciliation)
            }));
        };
        if notification.confirms_publication(inserted_at, kind) {
            Ok(())
        } else {
            Err(Self::with_commit_evidence(
                SdkError::invalid_state("notification publication is contradictory"),
                commit_error.as_ref(),
            ))
        }
    }

    async fn receive_step(
        &self,
        conversation_id: &ConversationId,
        progress: &mut NotificationReceiverProgress,
    ) -> Result<ReceiveStep, SdkError> {
        let store = self.journal.store();
        loop {
            if matches!(progress, NotificationReceiverProgress::Reconciling { .. }) {
                match Self::observe_receipt(store, progress).await {
                    Ok(Some(step)) => return Ok(step),
                    Ok(None) => continue,
                    Err(NotificationReceiptError::TransactionConflict) => {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    Err(NotificationReceiptError::Failure(error)) => return Err(error),
                }
            }

            let mut transaction = match store.begin_transaction().await {
                Ok(transaction) => transaction,
                Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(error) => {
                    return Err(SdkError::from_state_store(
                        error,
                        "notification receipt could not begin",
                    )
                    .in_phase(crate::error::ErrorPhase::StateWrite));
                }
            };
            let mutation = async {
                let key = NotificationRecord::key(conversation_id)
                    .map_err(NotificationReceiptError::Failure)?;
                let snapshot = transaction
                    .load(key.clone())
                    .await
                    .map_err(|error| match error {
                        storage_sdk::StateStoreError::TransactionConflict { .. } => {
                            NotificationReceiptError::TransactionConflict
                        }
                        error => NotificationReceiptError::Failure(
                            SdkError::from_state_store(
                                error,
                                "notification state could not be loaded",
                            )
                            .in_phase(crate::error::ErrorPhase::StateRead),
                        ),
                    })?
                    .ok_or_else(|| {
                        NotificationReceiptError::Failure(SdkError::invalid_state(
                            "notification state is missing",
                        ))
                    })?;
                let mut record = NotificationRecord::decode(snapshot, conversation_id)
                    .map_err(NotificationReceiptError::Failure)?;
                let before = record.clone();
                let acknowledged = if let Some(ReceiverDelivery::Passive(id)) = progress.delivery()
                {
                    record
                        .acknowledge_passive(id)
                        .map_err(NotificationReceiptError::Failure)?;
                    true
                } else {
                    false
                };
                let compacted = record.compact_terminal();
                let delivered_interaction = match progress.delivery() {
                    Some(ReceiverDelivery::Interaction(id)) => Some(id),
                    Some(ReceiverDelivery::Passive(_)) | None => None,
                };
                let step = record
                    .next_delivery(delivered_interaction)
                    .map_err(NotificationReceiptError::Failure)?;
                match &step {
                    ReceiveStep::End => {
                        transaction.delete(key).await.map_err(|error| match error {
                            storage_sdk::StateStoreError::TransactionConflict { .. } => {
                                NotificationReceiptError::TransactionConflict
                            }
                            error => NotificationReceiptError::Failure(
                                SdkError::from_state_store(
                                    error,
                                    "terminal notification state could not be reclaimed",
                                )
                                .in_phase(crate::error::ErrorPhase::StateWrite),
                            ),
                        })?;
                    }
                    ReceiveStep::Item { .. }
                    | ReceiveStep::Wait { .. }
                    | ReceiveStep::Failed { .. } => {
                        if acknowledged || compacted {
                            transaction
                                .put(storage_sdk::state::record::StateRecord::new(
                                    key,
                                    record.encode().map_err(NotificationReceiptError::Failure)?,
                                ))
                                .await
                                .map_err(|error| match error {
                                    storage_sdk::StateStoreError::TransactionConflict {
                                        ..
                                    } => NotificationReceiptError::TransactionConflict,
                                    error => NotificationReceiptError::Failure(
                                        SdkError::from_state_store(
                                            error,
                                            "notification receipt could not be stored",
                                        )
                                        .in_phase(crate::error::ErrorPhase::StateWrite),
                                    ),
                                })?;
                        }
                    }
                }
                let expected = if matches!(&step, ReceiveStep::End) {
                    None
                } else {
                    Some(record)
                };
                let mutation =
                    NotificationMutation::new(conversation_id.clone(), Some(before), expected);
                Ok((mutation, step))
            }
            .await;
            let (mutation, step) = match mutation {
                Ok(mutation) => mutation,
                Err(NotificationReceiptError::TransactionConflict) => {
                    if let Err(cleanup) = transaction.rollback().await {
                        return Err(SdkError::conflict(
                            "notification receipt transaction conflicted",
                        )
                        .with_detail(crate::error::ErrorDetail::TransactionConflict)
                        .with_supplementary(
                            SdkError::from_state_store(
                                cleanup,
                                "state transaction could not close",
                            )
                            .at_phase(crate::error::ErrorPhase::Cleanup),
                        ));
                    }
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(NotificationReceiptError::Failure(error)) => {
                    return Err(match transaction.rollback().await {
                        Ok(()) => error,
                        Err(cleanup) => error.with_supplementary(
                            SdkError::from_state_store(
                                cleanup,
                                "state transaction could not close",
                            )
                            .at_phase(crate::error::ErrorPhase::Cleanup),
                        ),
                    });
                }
            };
            if !mutation.changes_state() {
                // A read-only view has no durable mutation receipt. Retaining
                // whole-record equality here would reject a later append/close.
                transaction.rollback().await.map_err(|error| {
                    SdkError::from_state_store(
                        error,
                        "notification read transaction could not close",
                    )
                    .at_phase(crate::error::ErrorPhase::Cleanup)
                })?;
                return Ok(step);
            }
            let commit_result = transaction.commit().await;
            let step = NotificationReceiptStep::new(step, commit_result);
            progress.begin_reconciliation(NotificationReceipt { mutation, step })?;
            match Self::observe_receipt(store, progress).await {
                Ok(Some(step)) => return Ok(step),
                Ok(None) => continue,
                Err(NotificationReceiptError::TransactionConflict) => {
                    tokio::task::yield_now().await;
                }
                Err(NotificationReceiptError::Failure(error)) => return Err(error),
            }
        }
    }

    async fn observe_receipt(
        store: &impl StateStoreBackend,
        progress: &mut NotificationReceiverProgress,
    ) -> Result<Option<ReceiveStep>, NotificationReceiptError> {
        let commit_error = progress
            .receipt()
            .map_err(NotificationReceiptError::Failure)?
            .step
            .commit_error()
            .cloned();
        let observation = progress
            .receipt()
            .map_err(NotificationReceiptError::Failure)?
            .observe(store)
            .await
            .map_err(|error| match (error, commit_error.as_ref()) {
                (NotificationMutationError::Failure(error), Some(commit)) => {
                    NotificationReceiptError::Failure(error.with_supplementary(commit.clone()))
                }
                (error, _) => NotificationReceiptError::from(error),
            })?;
        match observation {
            NotificationMutationObservation::Expected => {
                let receipt = progress
                    .finish_reconciliation()
                    .map_err(NotificationReceiptError::Failure)?;
                if matches!(receipt.step.into_receive_step(), ReceiveStep::End) {
                    Ok(Some(ReceiveStep::End))
                } else {
                    // The acknowledged prefix is authoritative, but the saved
                    // next-delivery view may predate new items or terminalization.
                    progress
                        .clear_passive_delivery()
                        .map_err(NotificationReceiptError::Failure)?;
                    Ok(None)
                }
            }
            NotificationMutationObservation::Unchanged => Err(progress
                .finish_reconciliation()
                .map_err(NotificationReceiptError::Failure)?
                .step
                .into_error()),
            NotificationMutationObservation::Contradictory => {
                progress
                    .finish_reconciliation()
                    .map_err(NotificationReceiptError::Failure)?;
                Err(NotificationReceiptError::Failure(
                    Self::with_commit_evidence(
                        SdkError::invalid_state(
                            "notification receipt commit outcome is contradictory",
                        ),
                        commit_error.as_ref(),
                    ),
                ))
            }
        }
    }

    fn has_receiver(&self, conversation_id: &ConversationId) -> bool {
        self.delivery.state().leases.contains_key(conversation_id)
    }

    fn lease_state(
        &self,
        conversation_id: &ConversationId,
        lease_id: u64,
    ) -> Option<ReceiverLeaseState> {
        self.delivery
            .state()
            .leases
            .get(conversation_id)
            .filter(|lease| lease.id == lease_id)
            .map(|lease| lease.state)
    }

    fn lease_is_draining(&self, conversation_id: &ConversationId, lease_id: u64) -> bool {
        self.delivery
            .state()
            .leases
            .get(conversation_id)
            .is_some_and(|lease| {
                lease.id == lease_id && lease.state == ReceiverLeaseState::Draining
            })
    }

    fn release(&self, conversation_id: &ConversationId, lease_id: u64) {
        let mut state = self.delivery.state();
        if state
            .leases
            .get(conversation_id)
            .is_some_and(|lease| lease.id == lease_id)
        {
            state.leases.remove(conversation_id);
        }
        drop(state);
        self.delivery.changed.notify_waiters();
    }

    fn block_receiver(&self, conversation_id: &ConversationId, lease_id: u64) {
        let mut state = self.delivery.state();
        let is_owner = state
            .leases
            .get(conversation_id)
            .is_some_and(|lease| lease.id == lease_id);
        if !is_owner {
            return;
        }
        let Some(lease) = state.leases.remove(conversation_id) else {
            return;
        };
        state
            .blocked_receivers
            .insert(conversation_id.clone(), lease.workspace_id);
        drop(state);
        self.delivery.changed.notify_waiters();
    }

    fn abandon_drain(&self, conversation_id: &ConversationId, lease_id: u64) {
        let mut state = self.delivery.state();
        if state
            .leases
            .get(conversation_id)
            .is_none_or(|lease| lease.id != lease_id)
        {
            return;
        }
        state.leases.remove(conversation_id);
        state.queue_abandoned_drain(conversation_id.clone());
        drop(state);
        self.delivery.changed.notify_waiters();

        self.wake_abandoned_drain(conversation_id);
    }

    fn schedule_abandoned_drain(&self, conversation_id: &ConversationId) {
        let executor = self.delivery.executor.clone();
        let mut state = self.delivery.state();
        if matches!(state.reclamation_admission, ReclamationAdmission::Sealed)
            || !matches!(
                state.abandoned_drains.get(conversation_id),
                Some(AbandonedDrainState::Pending)
            )
        {
            return;
        }
        let attempt_id = state.next_reclamation;
        let Some(next_reclamation) = state.next_reclamation.checked_add(1) else {
            state.abandoned_drains.insert(
                conversation_id.clone(),
                AbandonedDrainState::Blocked(Arc::new(SdkError::invalid_state(
                    "notification reclamation identity exhausted",
                ))),
            );
            drop(state);
            self.delivery.changed.notify_waiters();
            return;
        };
        state.next_reclamation = next_reclamation;
        let journal = self.journal.clone();
        let conversation_id = conversation_id.clone();
        let guard = AbandonedDrainGuard::new(
            journal,
            Arc::clone(&self.delivery),
            conversation_id.clone(),
            attempt_id,
        );
        let handle = executor.spawn(async move {
            let disposition = loop {
                match guard.reclaim().await {
                    Ok(()) => break AbandonedDrainDisposition::Reclaimed,
                    Err(error)
                        if error.kind() == crate::error::ErrorKind::Conflict
                            && (guard.take_wake_request()) => {}
                    Err(error) if error.kind() == crate::error::ErrorKind::Conflict => {
                        break AbandonedDrainDisposition::Deferred;
                    }
                    Err(error) => break AbandonedDrainDisposition::Blocked(error),
                }
            };
            guard.complete(disposition);
        });
        state.abandoned_drains.insert(
            conversation_id,
            AbandonedDrainState::Task {
                attempt_id,
                handle,
                phase: AbandonedDrainPhase::Running {
                    wake_requested: false,
                },
            },
        );
        drop(state);
        self.delivery.changed.notify_waiters();
    }

    fn wake_abandoned_drain(&self, conversation_id: &ConversationId) {
        let _ = self.observe_abandoned_drain(conversation_id);
        let schedulable = {
            let mut state = self.delivery.state();
            if matches!(state.reclamation_admission, ReclamationAdmission::Sealed) {
                false
            } else {
                match state.abandoned_drains.get_mut(conversation_id) {
                    Some(drain @ AbandonedDrainState::Deferred) => {
                        *drain = AbandonedDrainState::Pending;
                        true
                    }
                    Some(AbandonedDrainState::Pending) => true,
                    Some(drain) => {
                        let _ = drain.request_wake();
                        false
                    }
                    None => false,
                }
            }
        };
        if schedulable {
            self.schedule_abandoned_drain(conversation_id);
        }
    }

    fn observe_abandoned_drain(
        &self,
        conversation_id: &ConversationId,
    ) -> AbandonedDrainObservation {
        let mut state = self.delivery.state();
        let drain = match state.abandoned_drains.get_mut(conversation_id) {
            None => return AbandonedDrainObservation::Missing,
            Some(drain) => drain,
        };
        let waiting = drain.waiting_observation();
        let Some(handle) = drain.handle() else {
            return waiting;
        };
        if !handle.is_finished() {
            return waiting;
        }
        let waker = std::task::Waker::noop();
        let mut context = Context::from_waker(waker);
        let joined = match Pin::new(handle).poll(&mut context) {
            Poll::Ready(joined) => joined.is_ok(),
            Poll::Pending => return AbandonedDrainObservation::Finishing,
        };
        let Some(drain) = state.abandoned_drains.remove(conversation_id) else {
            return AbandonedDrainObservation::Missing;
        };
        let observation = match drain {
            AbandonedDrainState::Task {
                phase: AbandonedDrainPhase::Exited(AbandonedDrainExit::Reclaimed),
                ..
            } if joined => AbandonedDrainObservation::Missing,
            AbandonedDrainState::Task {
                phase:
                    AbandonedDrainPhase::Exited(AbandonedDrainExit::Deferred {
                        wake_requested: true,
                    }),
                ..
            } if joined => {
                state
                    .abandoned_drains
                    .insert(conversation_id.clone(), AbandonedDrainState::Pending);
                AbandonedDrainObservation::Retry
            }
            AbandonedDrainState::Task {
                phase:
                    AbandonedDrainPhase::Exited(AbandonedDrainExit::Deferred {
                        wake_requested: false,
                    }),
                ..
            } if joined => {
                state
                    .abandoned_drains
                    .insert(conversation_id.clone(), AbandonedDrainState::Deferred);
                AbandonedDrainObservation::Deferred
            }
            AbandonedDrainState::Task {
                phase: AbandonedDrainPhase::Exited(AbandonedDrainExit::Blocked(error)),
                ..
            } if joined => {
                state.abandoned_drains.insert(
                    conversation_id.clone(),
                    AbandonedDrainState::Blocked(Arc::clone(&error)),
                );
                AbandonedDrainObservation::Blocked(error)
            }
            AbandonedDrainState::Task {
                phase: AbandonedDrainPhase::Reported(error),
                ..
            } if joined => {
                let error = Arc::new(Self::replay_abandoned_drain_error(error.as_ref()));
                state.abandoned_drains.insert(
                    conversation_id.clone(),
                    AbandonedDrainState::Blocked(Arc::clone(&error)),
                );
                AbandonedDrainObservation::Blocked(error)
            }
            AbandonedDrainState::Pending
            | AbandonedDrainState::Task { .. }
            | AbandonedDrainState::Deferred
            | AbandonedDrainState::Blocked(_) => {
                let error = Arc::new(SdkError::invalid_state(
                    "notification reclamation task terminated unexpectedly",
                ));
                state.abandoned_drains.insert(
                    conversation_id.clone(),
                    AbandonedDrainState::Blocked(Arc::clone(&error)),
                );
                AbandonedDrainObservation::Blocked(error)
            }
        };
        drop(state);
        self.delivery.changed.notify_waiters();
        observation
    }

    fn replay_abandoned_drain_error(error: &SdkError) -> SdkError {
        error.clone()
    }
}

impl<S: StateStoreBackend> NotificationJournal<S> {
    fn store(&self) -> &S {
        self.storage.store()
    }
}

impl NotificationDeliveryRuntime {
    fn state(&self) -> std::sync::MutexGuard<'_, NotificationDeliveryState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn take_reclamation_wake(&self, conversation_id: &ConversationId, attempt_id: u64) -> bool {
        let requested = self
            .state()
            .abandoned_drains
            .get_mut(conversation_id)
            .is_some_and(|drain| drain.take_wake_request(attempt_id));
        if requested {
            self.changed.notify_waiters();
        }
        requested
    }

    fn finish_reclamation(
        &self,
        conversation_id: &ConversationId,
        attempt_id: u64,
        disposition: Option<AbandonedDrainDisposition>,
    ) {
        if let Some(drain) = self.state().abandoned_drains.get_mut(conversation_id) {
            drain.finish(attempt_id, disposition);
        }
        self.changed.notify_waiters();
    }
}

impl<S: StateStoreBackend> AbandonedDrainGuard<S> {
    fn new(
        journal: NotificationJournal<S>,
        delivery: Arc<NotificationDeliveryRuntime>,
        conversation_id: ConversationId,
        attempt_id: u64,
    ) -> Self {
        Self {
            journal,
            delivery,
            conversation_id,
            attempt_id,
            disposition: None,
        }
    }

    async fn reclaim(&self) -> Result<(), SdkError> {
        let store = self.journal.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            SdkError::from_state_store(error, "notification reclamation could not begin")
                .in_phase(crate::error::ErrorPhase::StateWrite)
        })?;
        let key = NotificationRecord::key(&self.conversation_id)?;
        let Some(snapshot) = transaction.load(key.clone()).await.map_err(|error| {
            SdkError::from_state_store(error, "notification state could not be loaded")
                .in_phase(crate::error::ErrorPhase::StateRead)
        })?
        else {
            return Ok(());
        };
        let before = NotificationRecord::decode(snapshot, &self.conversation_id)?;
        if before.lifecycle != DeliveryLifecycle::Draining {
            return Err(SdkError::invalid_state(
                "abandoned notification delivery is not terminal",
            ));
        }
        if before.has_resolving_interaction() {
            return Err(SdkError::conflict(
                "interaction settlement is still in progress",
            ));
        }
        let mutation = NotificationMutation::new(self.conversation_id.clone(), Some(before), None);
        transaction.delete(key).await.map_err(|error| {
            SdkError::from_state_store(error, "terminal notification state could not be reclaimed")
                .in_phase(crate::error::ErrorPhase::StateWrite)
        })?;
        let commit_error = transaction.commit().await.err().map(|error| {
            SdkError::from_state_store(error, "notification reclamation could not commit")
                .in_phase(crate::error::ErrorPhase::StateCommit)
        });
        match mutation.observe(store).await.map_err(|error| {
            NotificationStore::<S>::with_commit_evidence(error.into_sdk(), commit_error.as_ref())
        })? {
            NotificationMutationObservation::Expected => Ok(()),
            NotificationMutationObservation::Unchanged => Err(commit_error.unwrap_or_else(|| {
                SdkError::integration("notification reclamation could not commit")
                    .in_phase(crate::error::ErrorPhase::Reconciliation)
            })),
            NotificationMutationObservation::Contradictory => {
                Err(NotificationStore::<S>::with_commit_evidence(
                    SdkError::invalid_state(
                        "notification reclamation commit outcome is contradictory",
                    ),
                    commit_error.as_ref(),
                ))
            }
        }
    }

    fn complete(mut self, disposition: AbandonedDrainDisposition) {
        self.disposition = Some(disposition);
    }

    fn take_wake_request(&self) -> bool {
        self.delivery
            .take_reclamation_wake(&self.conversation_id, self.attempt_id)
    }
}

impl<S: StateStoreBackend> Drop for AbandonedDrainGuard<S> {
    fn drop(&mut self) {
        self.delivery.finish_reclamation(
            &self.conversation_id,
            self.attempt_id,
            self.disposition.take(),
        );
    }
}

impl NotificationInitializationError {
    pub(crate) fn into_sdk(self) -> SdkError {
        match self {
            Self::TransactionConflict => {
                SdkError::conflict("notification initialization transaction conflicted")
                    .with_detail(crate::error::ErrorDetail::TransactionConflict)
            }
            Self::Failure(error) => error,
        }
    }
}

impl NotificationInteractionCheckError {
    fn into_sdk(self) -> SdkError {
        match self {
            Self::TransactionConflict => {
                SdkError::conflict("interaction state transaction conflicted")
                    .with_detail(crate::error::ErrorDetail::TransactionConflict)
            }
            Self::Failure(error) => error,
        }
    }

    pub(crate) fn into_match_sdk(self) -> SdkError {
        match self {
            Self::TransactionConflict => {
                SdkError::integration("interaction state could not be loaded")
                    .with_detail(crate::error::ErrorDetail::TransactionConflict)
                    .in_phase(crate::error::ErrorPhase::StateRead)
            }
            Self::Failure(error) => error,
        }
    }
}

impl From<NotificationSettlementError> for SdkError {
    fn from(error: NotificationSettlementError) -> Self {
        match error {
            NotificationSettlementError::TransactionConflict => {
                Self::conflict("interaction settlement transaction conflicted")
                    .with_detail(crate::error::ErrorDetail::TransactionConflict)
            }
            NotificationSettlementError::Failure(error) => error,
        }
    }
}

impl NotificationSettlementError {
    pub(crate) fn storage(error: storage_sdk::StateStoreError, message: &'static str) -> Self {
        match error {
            storage_sdk::StateStoreError::TransactionConflict { .. } => Self::TransactionConflict,
            error => Self::Failure(SdkError::from_state_store(error, message)),
        }
    }

    pub(crate) fn in_phase(self, phase: crate::error::ErrorPhase) -> Self {
        match self {
            Self::TransactionConflict => Self::TransactionConflict,
            Self::Failure(error) => Self::Failure(error.in_phase(phase)),
        }
    }

    pub(crate) fn into_reconciliation_sdk(self) -> SdkError {
        match self {
            Self::TransactionConflict => {
                SdkError::integration("interaction resolution transaction conflicted")
                    .with_detail(crate::error::ErrorDetail::TransactionConflict)
            }
            Self::Failure(error) => error,
        }
    }
}

impl From<SdkError> for NotificationSettlementError {
    fn from(error: SdkError) -> Self {
        Self::Failure(error)
    }
}

impl NotificationCloseError {
    pub(crate) fn into_sdk(self) -> SdkError {
        match self {
            Self::TransactionConflict(phase) => {
                SdkError::conflict("notification close transaction conflicted")
                    .with_detail(crate::error::ErrorDetail::TransactionConflict)
                    .in_phase(phase)
            }
            Self::Failure(error) => error,
        }
    }
}

impl NotificationDeleteError {
    pub(crate) fn into_sdk(self) -> SdkError {
        match self {
            Self::TransactionConflict => {
                SdkError::conflict("notification state transaction conflicted")
                    .with_detail(crate::error::ErrorDetail::TransactionConflict)
            }
            Self::Failure(error) => error,
        }
    }
}

impl NotificationReceipt {
    async fn observe(
        &self,
        store: &impl StateStoreBackend,
    ) -> Result<NotificationMutationObservation, NotificationMutationError> {
        let observed = self.mutation.load(store).await?;
        if observed == self.mutation.expected {
            Ok(NotificationMutationObservation::Expected)
        } else if observed == self.mutation.before {
            Ok(NotificationMutationObservation::Unchanged)
        } else if let (Some(observed), Some(before), Some(expected)) = (
            observed.as_ref(),
            self.mutation.before.as_ref(),
            self.mutation.expected.as_ref(),
        ) && observed.confirms_receipt(before, expected)
        {
            Ok(NotificationMutationObservation::Expected)
        } else {
            Ok(NotificationMutationObservation::Contradictory)
        }
    }
}

impl NotificationReceiptStep {
    fn new(step: ReceiveStep, commit: Result<(), storage_sdk::StateStoreError>) -> Self {
        match commit {
            Ok(()) => Self::Committed(step),
            Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => Self::Conflicted(step),
            Err(error) => Self::Unconfirmed {
                step,
                error: SdkError::from_state_store(
                    error,
                    "notification receipt could not be committed",
                )
                .in_phase(crate::error::ErrorPhase::StateCommit),
            },
        }
    }

    fn commit_error(&self) -> Option<&SdkError> {
        match self {
            Self::Unconfirmed { error, .. } => Some(error),
            Self::Committed(_) | Self::Conflicted(_) => None,
        }
    }

    fn into_error(self) -> NotificationReceiptError {
        match self {
            Self::Conflicted(_) => NotificationReceiptError::TransactionConflict,
            Self::Unconfirmed { error, .. } => NotificationReceiptError::Failure(error),
            Self::Committed(_) => NotificationReceiptError::Failure(
                SdkError::integration("notification receipt could not be committed")
                    .in_phase(crate::error::ErrorPhase::Reconciliation),
            ),
        }
    }

    fn into_receive_step(self) -> ReceiveStep {
        match self {
            Self::Committed(step) | Self::Conflicted(step) | Self::Unconfirmed { step, .. } => step,
        }
    }
}

impl NotificationMutationError {
    fn into_sdk(self) -> SdkError {
        match self {
            Self::TransactionConflict => {
                SdkError::conflict("notification mutation transaction conflicted")
                    .with_detail(crate::error::ErrorDetail::TransactionConflict)
            }
            Self::Failure(error) => error,
        }
    }
}

impl From<NotificationMutationError> for NotificationReceiptError {
    fn from(error: NotificationMutationError) -> Self {
        match error {
            NotificationMutationError::TransactionConflict => Self::TransactionConflict,
            NotificationMutationError::Failure(error) => Self::Failure(error),
        }
    }
}

impl From<NotificationMutationError> for SdkError {
    fn from(error: NotificationMutationError) -> Self {
        error.into_sdk()
    }
}

impl NotificationMutation {
    fn new(
        conversation_id: ConversationId,
        before: Option<NotificationRecord>,
        expected: Option<NotificationRecord>,
    ) -> Self {
        Self {
            conversation_id,
            before,
            expected,
        }
    }

    async fn load(
        &self,
        store: &impl StateStoreBackend,
    ) -> Result<Option<NotificationRecord>, NotificationMutationError> {
        let key = NotificationRecord::key(&self.conversation_id)
            .map_err(NotificationMutationError::Failure)?;
        store
            .load(key)
            .await
            .map_err(|error| match error {
                storage_sdk::StateStoreError::TransactionConflict { .. } => {
                    NotificationMutationError::TransactionConflict
                }
                error => NotificationMutationError::Failure(
                    SdkError::from_state_store(
                        error,
                        "notification mutation could not be observed",
                    )
                    .in_phase(crate::error::ErrorPhase::StateRead),
                ),
            })?
            .map(|snapshot| NotificationRecord::decode(snapshot, &self.conversation_id))
            .transpose()
            .map_err(NotificationMutationError::Failure)
    }

    async fn observe(
        &self,
        store: &impl StateStoreBackend,
    ) -> Result<NotificationMutationObservation, NotificationMutationError> {
        let observed = self.load(store).await?;
        if observed == self.expected {
            Ok(NotificationMutationObservation::Expected)
        } else if observed == self.before {
            Ok(NotificationMutationObservation::Unchanged)
        } else {
            Ok(NotificationMutationObservation::Contradictory)
        }
    }

    fn changes_state(&self) -> bool {
        self.before != self.expected
    }
}

impl NotificationReconciliation {
    fn new(
        conversation_id: ConversationId,
        before: Option<NotificationRecord>,
        expected: Option<NotificationRecord>,
        unresolved_acceptance: bool,
    ) -> Self {
        Self {
            unresolved_acceptance,
            mutation: NotificationMutation::new(conversation_id, before, expected),
        }
    }

    pub(crate) fn unresolved_acceptance(&self) -> bool {
        self.unresolved_acceptance
    }

    pub(crate) fn changes_state(&self) -> bool {
        self.mutation.changes_state()
    }

    pub(crate) async fn observe(
        &self,
        store: &impl StateStoreBackend,
    ) -> Result<NotificationMutationObservation, SdkError> {
        self.mutation
            .observe(store)
            .await
            .map_err(NotificationMutationError::into_sdk)
    }

    async fn observe_publication(
        &self,
        store: &impl StateStoreBackend,
        id: &NotificationId,
        inserted_at: SystemTime,
        kind: &StoredNotificationKind,
    ) -> Result<NotificationMutationObservation, NotificationMutationError> {
        let observed = store
            .load(
                NotificationRecord::key(&self.mutation.conversation_id)
                    .map_err(NotificationMutationError::Failure)?,
            )
            .await
            .map_err(|error| match error {
                storage_sdk::StateStoreError::TransactionConflict { .. } => {
                    NotificationMutationError::TransactionConflict
                }
                error => NotificationMutationError::Failure(
                    SdkError::from_state_store(
                        error,
                        "notification publication could not be observed",
                    )
                    .in_phase(crate::error::ErrorPhase::StateRead),
                ),
            })?
            .map(|snapshot| NotificationRecord::decode(snapshot, &self.mutation.conversation_id))
            .transpose()
            .map_err(NotificationMutationError::Failure)?;
        if observed.as_ref().is_some_and(|record| {
            record.pending.iter().any(|notification| {
                &notification.id == id
                    && notification.inserted_at == inserted_at
                    && &notification.kind == kind
            })
        }) {
            Ok(NotificationMutationObservation::Expected)
        } else if observed == self.mutation.before {
            Ok(NotificationMutationObservation::Unchanged)
        } else {
            Ok(NotificationMutationObservation::Contradictory)
        }
    }
}

impl NotificationClosure {
    fn new(
        conversation_id: ConversationId,
        before: NotificationRecord,
        expected: Option<NotificationRecord>,
        cancelled: Vec<NotificationId>,
        retains_delivery: bool,
    ) -> Self {
        Self {
            cancelled,
            retains_delivery,
            mutation: NotificationMutation::new(conversation_id, Some(before), expected),
        }
    }

    fn reclaimed(conversation_id: ConversationId) -> Self {
        Self {
            cancelled: Vec::new(),
            retains_delivery: false,
            mutation: NotificationMutation::new(conversation_id, None, None),
        }
    }

    pub(crate) fn contains(&self, notification_id: &NotificationId) -> bool {
        self.cancelled.contains(notification_id)
    }

    pub(crate) fn iter(&self) -> std::slice::Iter<'_, NotificationId> {
        self.cancelled.iter()
    }

    pub(crate) fn retains_delivery(&self) -> bool {
        self.retains_delivery
    }

    pub(crate) fn changes_state(&self) -> bool {
        self.mutation.changes_state()
    }

    pub(crate) async fn observe(
        &self,
        store: &impl StateStoreBackend,
    ) -> Result<NotificationMutationObservation, SdkError> {
        self.mutation
            .observe(store)
            .await
            .map_err(NotificationMutationError::into_sdk)
    }
}

impl NotificationReceiverProgress {
    fn delivery(&self) -> Option<&ReceiverDelivery> {
        match self {
            Self::Ready { delivery } | Self::Reconciling { delivery, .. } => delivery.current(),
        }
    }

    fn begin_reconciliation(&mut self, receipt: NotificationReceipt) -> Result<(), SdkError> {
        let Self::Ready { delivery } = self else {
            return Err(SdkError::invalid_state(
                "notification receipt reconciliation is already active",
            ));
        };
        *self = Self::Reconciling {
            delivery: delivery.clone(),
            receipt: Box::new(receipt),
        };
        Ok(())
    }

    fn receipt(&self) -> Result<&NotificationReceipt, SdkError> {
        match self {
            Self::Reconciling { receipt, .. } => Ok(receipt),
            Self::Ready { .. } => Err(SdkError::invalid_state(
                "notification receipt owner is missing",
            )),
        }
    }

    fn finish_reconciliation(&mut self) -> Result<NotificationReceipt, SdkError> {
        let current = std::mem::replace(
            self,
            Self::Ready {
                delivery: NotificationDeliveryCursor::Empty,
            },
        );
        match current {
            Self::Reconciling { delivery, receipt } => {
                *self = Self::Ready { delivery };
                Ok(*receipt)
            }
            current => {
                *self = current;
                Err(SdkError::invalid_state(
                    "notification receipt owner is missing",
                ))
            }
        }
    }

    fn deliver(&mut self, delivered: ReceiverDelivery) -> Result<(), SdkError> {
        let Self::Ready { delivery } = self else {
            return Err(SdkError::invalid_state(
                "notification receipt reconciliation is still active",
            ));
        };
        *delivery = NotificationDeliveryCursor::Delivered(delivered);
        Ok(())
    }

    fn clear_passive_delivery(&mut self) -> Result<(), SdkError> {
        let Self::Ready { delivery } = self else {
            return Err(SdkError::invalid_state(
                "notification receipt reconciliation is still active",
            ));
        };
        if matches!(
            delivery,
            NotificationDeliveryCursor::Delivered(ReceiverDelivery::Passive(_))
        ) {
            *delivery = NotificationDeliveryCursor::Empty;
        }
        Ok(())
    }
}

impl NotificationDeliveryCursor {
    fn current(&self) -> Option<&ReceiverDelivery> {
        match self {
            Self::Empty => None,
            Self::Delivered(delivery) => Some(delivery),
        }
    }
}

impl NotificationReceiverState {
    fn progress(&mut self) -> Option<&mut NotificationReceiverProgress> {
        match self {
            Self::Open(progress) | Self::Draining(progress) => Some(progress),
            Self::Ended(_) => None,
        }
    }

    fn update_delivery(
        &mut self,
        draining: bool,
        update: impl FnOnce(&mut NotificationReceiverProgress) -> Result<(), SdkError>,
    ) -> Result<(), SdkError> {
        let current = std::mem::replace(
            self,
            Self::Ended(NotificationReceiverRelease::Reconciliation),
        );
        let mut progress = match current {
            Self::Open(progress) | Self::Draining(progress) => progress,
            Self::Ended(release) => {
                *self = Self::Ended(release);
                return Err(SdkError::invalid_state(
                    "notification receiver has already ended",
                ));
            }
        };
        let result = update(&mut progress);
        *self = if draining {
            Self::Draining(progress)
        } else {
            Self::Open(progress)
        };
        result
    }

    fn release(&self) -> Option<NotificationReceiverRelease> {
        match self {
            Self::Open(_) => Some(NotificationReceiverRelease::Lease),
            Self::Draining(_) => Some(NotificationReceiverRelease::AbandonedDrain),
            Self::Ended(_) => None,
        }
    }
}

impl<S: StateStoreBackend> NotificationReceiver<S> {
    pub(crate) fn terminal_reclamation_required(&self) -> bool {
        match &self.state {
            NotificationReceiverState::Draining(_)
            | NotificationReceiverState::Ended(NotificationReceiverRelease::AbandonedDrain) => true,
            NotificationReceiverState::Open(_) => self
                .store
                .lease_is_draining(&self.conversation_id, self.lease_id),
            NotificationReceiverState::Ended(
                NotificationReceiverRelease::Lease | NotificationReceiverRelease::Reconciliation,
            ) => false,
        }
    }

    pub(crate) fn release(&mut self) {
        self.end(None);
    }

    pub(crate) async fn recv(&mut self) -> Option<Result<ConversationNotification, SdkError>> {
        if matches!(self.state, NotificationReceiverState::Ended(_)) {
            return None;
        }
        let delivery = Arc::clone(&self.store.delivery);
        loop {
            let mut changed = std::pin::pin!(delivery.changed.notified());
            changed.as_mut().enable();
            match self.store.lease_state(&self.conversation_id, self.lease_id) {
                None => {
                    self.end(None);
                    return None;
                }
                Some(ReceiverLeaseState::Failed) => {
                    self.end(Some(NotificationReceiverRelease::Reconciliation));
                    return Some(Err(SdkError::closed(
                        "notification delivery runtime failed",
                    )));
                }
                Some(ReceiverLeaseState::Open | ReceiverLeaseState::Draining) => {}
            }
            let store = self.store.clone();
            let conversation_id = self.conversation_id.clone();
            let step = match self.state.progress() {
                Some(progress) => store.receive_step(&conversation_id, progress).await,
                None => return None,
            };
            match step {
                Ok(ReceiveStep::Item {
                    notification,
                    delivered,
                    draining,
                }) => {
                    let updated = self
                        .state
                        .update_delivery(draining, |progress| progress.deliver(delivered));
                    if let Err(error) = updated {
                        self.end(Some(NotificationReceiverRelease::Reconciliation));
                        return Some(Err(error));
                    }
                    return Some(Ok(notification));
                }
                Ok(ReceiveStep::Wait { draining }) => {
                    let updated = self
                        .state
                        .update_delivery(draining, |progress| progress.clear_passive_delivery());
                    if let Err(error) = updated {
                        self.end(Some(NotificationReceiverRelease::Reconciliation));
                        return Some(Err(error));
                    }
                    match self.store.lease_state(&self.conversation_id, self.lease_id) {
                        None => {
                            self.end(None);
                            return None;
                        }
                        Some(ReceiverLeaseState::Failed) => {
                            self.end(Some(NotificationReceiverRelease::Reconciliation));
                            return Some(Err(SdkError::closed(
                                "notification delivery runtime failed",
                            )));
                        }
                        Some(ReceiverLeaseState::Open | ReceiverLeaseState::Draining) => {}
                    }
                    changed.await;
                }
                Ok(ReceiveStep::End) => {
                    self.end(Some(NotificationReceiverRelease::AbandonedDrain));
                    return None;
                }
                Ok(ReceiveStep::Failed { error, draining }) => {
                    self.end(Some(if draining {
                        NotificationReceiverRelease::AbandonedDrain
                    } else {
                        NotificationReceiverRelease::Reconciliation
                    }));
                    return Some(Err(error));
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        crate::error::ErrorKind::Timeout | crate::error::ErrorKind::Integration
                    ) =>
                {
                    return Some(Err(error));
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        crate::error::ErrorKind::Conflict
                            | crate::error::ErrorKind::Closed
                            | crate::error::ErrorKind::Unsupported
                            | crate::error::ErrorKind::NotYetImplemented
                            | crate::error::ErrorKind::Protocol
                            | crate::error::ErrorKind::InvalidState
                    ) =>
                {
                    self.end(Some(NotificationReceiverRelease::Reconciliation));
                    return Some(Err(error));
                }
                Err(error) => return Some(Err(error)),
            }
        }
    }

    fn end(&mut self, release: Option<NotificationReceiverRelease>) {
        let current = match self.state.release() {
            Some(release) => release,
            None => return,
        };
        let release = release.unwrap_or(current);
        self.state = NotificationReceiverState::Ended(release);
        match release {
            NotificationReceiverRelease::Reconciliation => self
                .store
                .block_receiver(&self.conversation_id, self.lease_id),
            NotificationReceiverRelease::AbandonedDrain => self
                .store
                .abandon_drain(&self.conversation_id, self.lease_id),
            NotificationReceiverRelease::Lease
                if self
                    .store
                    .lease_is_draining(&self.conversation_id, self.lease_id) =>
            {
                self.store
                    .abandon_drain(&self.conversation_id, self.lease_id);
            }
            NotificationReceiverRelease::Lease => {
                self.store.release(&self.conversation_id, self.lease_id);
            }
        }
    }
}

impl<S: StateStoreBackend> Drop for NotificationReceiver<S> {
    fn drop(&mut self) {
        self.end(None);
    }
}

impl ResolutionAttempt {
    fn new() -> Self {
        Self(uuid::Uuid::new_v4().into_bytes())
    }
}

impl ResolutionIntent {
    fn new(
        conversation_id: ConversationId,
        notification_id: NotificationId,
        attempt: ResolutionAttempt,
    ) -> Self {
        Self {
            conversation_id,
            notification_id,
            attempt,
            settlement_commit: None,
        }
    }

    pub(crate) fn conversation_id(&self) -> &ConversationId {
        &self.conversation_id
    }

    pub(crate) fn notification_id(&self) -> &NotificationId {
        &self.notification_id
    }

    fn attempt(&self) -> ResolutionAttempt {
        self.attempt
    }

    pub(crate) fn settlement_commit(&self) -> Option<Result<(), SdkError>> {
        self.settlement_commit.clone()
    }

    pub(crate) fn record_settlement_commit(&mut self, commit: Option<Result<(), SdkError>>) {
        self.settlement_commit = commit;
    }

    async fn observe(
        &self,
        store: &impl StateStoreBackend,
    ) -> Result<NotificationMutationObservation, NotificationMutationError> {
        let key = NotificationRecord::key(&self.conversation_id)
            .map_err(NotificationMutationError::Failure)?;
        let snapshot = store
            .load(key)
            .await
            .map_err(|error| match error {
                storage_sdk::StateStoreError::TransactionConflict { .. } => {
                    NotificationMutationError::TransactionConflict
                }
                error => NotificationMutationError::Failure(
                    SdkError::from_state_store(error, "interaction intent could not be observed")
                        .in_phase(crate::error::ErrorPhase::StateRead),
                ),
            })?
            .ok_or_else(|| {
                NotificationMutationError::Failure(SdkError::conflict(
                    "interaction intent could not be confirmed",
                ))
            })?;
        let record = NotificationRecord::decode(snapshot, &self.conversation_id)
            .map_err(NotificationMutationError::Failure)?;
        let Some(notification) = record
            .pending
            .front()
            .filter(|notification| notification.id == self.notification_id)
        else {
            return Ok(NotificationMutationObservation::Contradictory);
        };
        match notification.state {
            StoredNotificationState::Resolving {
                attempt,
                acceptance: StoredAcceptance::Unconfirmed,
            } if attempt == self.attempt => Ok(NotificationMutationObservation::Expected),
            StoredNotificationState::Pending => Ok(NotificationMutationObservation::Unchanged),
            _ => Ok(NotificationMutationObservation::Contradictory),
        }
    }

    pub(crate) async fn observe_settlement(
        &self,
        store: &impl StateStoreBackend,
        settlement: ResolutionSettlement,
    ) -> Result<NotificationMutationObservation, NotificationSettlementError> {
        let key = NotificationRecord::key(&self.conversation_id)
            .map_err(NotificationSettlementError::Failure)?;
        let snapshot = store
            .load(key)
            .await
            .map_err(|error| match error {
                storage_sdk::StateStoreError::TransactionConflict { .. } => {
                    NotificationSettlementError::TransactionConflict
                }
                error => NotificationSettlementError::Failure(
                    SdkError::from_state_store(
                        error,
                        "interaction settlement could not be observed",
                    )
                    .in_phase(crate::error::ErrorPhase::StateRead),
                ),
            })?
            .ok_or_else(|| {
                NotificationSettlementError::Failure(SdkError::conflict(
                    "interaction settlement could not be confirmed",
                ))
            })?;
        NotificationRecord::decode(snapshot, &self.conversation_id)
            .map_err(NotificationSettlementError::Failure)?
            .observe_resolution_settlement(self, settlement)
    }
}

impl NotificationRecord {
    fn new(workspace_id: WorkspaceId, conversation_id: ConversationId) -> Self {
        Self {
            version: NOTIFICATION_RECORD_VERSION,
            workspace_id,
            conversation_id,
            next_sequence: 0,
            lifecycle: DeliveryLifecycle::Open,
            pending: VecDeque::new(),
        }
    }

    fn key(
        conversation_id: &ConversationId,
    ) -> Result<storage_sdk::state::record::StateKey, SdkError> {
        storage_sdk::state::record::StateKey::new(NOTIFICATION_NAMESPACE, conversation_id.as_str())
            .map_err(|_| SdkError::invalid_state("notification key is invalid"))
    }

    fn decode(
        snapshot: storage_sdk::state::record::StateRecordSnapshot,
        conversation_id: &ConversationId,
    ) -> Result<Self, SdkError> {
        let encoded_size = serde_json::to_vec(snapshot.record.value.as_ref())
            .map_err(|_| SdkError::invalid_state("stored notification state is malformed"))?
            .len();
        if encoded_size > MAX_NOTIFICATION_RECORD_BYTES {
            return Err(SdkError::invalid_state(
                "stored notification state exceeds the supported size limit",
            ));
        }
        let record: Self = serde::Deserialize::deserialize(snapshot.record.value.as_ref())
            .map_err(|_| SdkError::invalid_state("stored notification state is malformed"))?;
        record.validate(conversation_id)?;
        Ok(record)
    }

    fn encode(&self) -> Result<serde_json::Value, SdkError> {
        self.validate(&self.conversation_id)?;
        let value = serde_json::to_value(self)
            .map_err(|_| SdkError::invalid_state("notification state could not be encoded"))?;
        if serde_json::to_vec(&value)
            .map_err(|_| SdkError::invalid_state("notification state could not be encoded"))?
            .len()
            > MAX_NOTIFICATION_RECORD_BYTES
        {
            return Err(SdkError::integration(
                "notification state reached its storage capacity",
            ));
        }
        Ok(value)
    }

    fn validate(&self, conversation_id: &ConversationId) -> Result<(), SdkError> {
        if self.version != NOTIFICATION_RECORD_VERSION
            || &self.conversation_id != conversation_id
            || self.workspace_id.as_str().is_empty()
            || self.pending.len() > MAX_PENDING_NOTIFICATIONS
            || self
                .pending
                .iter()
                .zip(self.pending.iter().skip(1))
                .any(|(left, right)| left.sequence >= right.sequence)
            || self
                .pending
                .back()
                .is_some_and(|item| item.sequence >= self.next_sequence)
            || (self.lifecycle == DeliveryLifecycle::Draining
                && self
                    .pending
                    .iter()
                    .any(|notification| notification.state.is_pending()))
        {
            return Err(SdkError::invalid_state(
                "stored notification ownership is contradictory",
            ));
        }
        let mut identities = BTreeSet::new();
        for notification in &self.pending {
            if !identities.insert(notification.id.clone()) {
                return Err(SdkError::invalid_state(
                    "stored notification identity is duplicated",
                ));
            }
            notification.validate_stored()?;
        }
        Ok(())
    }

    fn publish(
        &mut self,
        id: NotificationId,
        inserted_at: SystemTime,
        kind: StoredNotificationKind,
    ) -> Result<(), SdkError> {
        if self.lifecycle != DeliveryLifecycle::Open {
            return Err(SdkError::closed("notification delivery is closed"));
        }
        if self.pending.len() == MAX_PENDING_NOTIFICATIONS {
            return Err(SdkError::integration(
                "notification backlog reached its storage capacity",
            ));
        }
        if self.pending.iter().any(|pending| pending.id == id) {
            return Err(SdkError::invalid_state("notification identity was reused"));
        }
        let state = if kind.is_interaction() {
            StoredNotificationState::Pending
        } else {
            StoredNotificationState::Passive
        };
        let sequence = self.next_sequence;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| SdkError::invalid_state("notification order exhausted"))?;
        self.pending.push_back(StoredNotification {
            sequence,
            id,
            inserted_at,
            kind,
            state,
        });
        if serde_json::to_vec(self)
            .map_err(|_| SdkError::invalid_state("notification state could not be encoded"))?
            .len()
            > MAX_NOTIFICATION_RECORD_BYTES
        {
            return Err(SdkError::integration(
                "notification backlog reached its storage capacity",
            ));
        }
        Ok(())
    }

    fn confirms_receipt(&self, before: &Self, expected: &Self) -> bool {
        let Some(removed) = before.pending.len().checked_sub(expected.pending.len()) else {
            return false;
        };
        let Some(appended) = self
            .pending
            .len()
            .checked_sub(expected.pending.len())
            .and_then(|count| u64::try_from(count).ok())
        else {
            return false;
        };
        let Some(next_sequence) = expected.next_sequence.checked_add(appended) else {
            return false;
        };
        self.version == expected.version
            && self.workspace_id == expected.workspace_id
            && self.conversation_id == expected.conversation_id
            && self.next_sequence == next_sequence
            && (self.lifecycle == expected.lifecycle
                || (expected.lifecycle == DeliveryLifecycle::Open
                    && self.lifecycle == DeliveryLifecycle::Draining))
            // A receive mutation only removes an exact prefix. Every removed
            // identity stays absent, even if a later append claims a new order.
            && before.next_sequence == expected.next_sequence
            && before.lifecycle == expected.lifecycle
            && before.pending.iter().skip(removed).eq(expected.pending.iter())
            && before.pending.iter().take(removed)
                .all(|removed| self.pending.iter().all(|item| item.id != removed.id))
            && self.pending.iter().zip(expected.pending.iter())
                .all(|(observed, expected)| observed.preserves_receipt_identity(expected))
            && self.pending.iter().skip(expected.pending.len())
                .zip(expected.next_sequence..next_sequence)
                .all(|(appended, sequence)| appended.sequence == sequence)
    }

    fn acknowledge_passive(&mut self, id: &NotificationId) -> Result<(), SdkError> {
        let first = self
            .pending
            .front()
            .ok_or_else(|| SdkError::invalid_state("delivered notification is missing"))?;
        if &first.id != id || !first.state.is_passive() {
            return Err(SdkError::invalid_state(
                "passive notification acknowledgment is contradictory",
            ));
        }
        self.pending.pop_front();
        Ok(())
    }

    fn has_unresolved_interaction(&self) -> bool {
        self.pending
            .iter()
            .any(|notification| notification.state.is_unresolved())
    }

    fn has_resolving_interaction(&self) -> bool {
        self.pending
            .iter()
            .any(|notification| notification.state.is_resolving())
    }

    fn cancel_pending_interactions(&mut self) -> bool {
        let mut changed = false;
        for notification in &mut self.pending {
            if notification.state.is_pending() {
                notification.state = StoredNotificationState::Terminal {
                    outcome: StoredTerminalOutcome::ReconciledCancelled,
                };
                changed = true;
            }
        }
        changed
    }

    fn reconcile_lost_interactions(&mut self) -> bool {
        let mut changed = false;
        for notification in &mut self.pending {
            if matches!(
                notification.state,
                StoredNotificationState::Pending
                    | StoredNotificationState::Resolving {
                        acceptance: StoredAcceptance::Unconfirmed,
                        ..
                    }
            ) {
                notification.state = StoredNotificationState::Terminal {
                    outcome: StoredTerminalOutcome::ReconciledCancelled,
                };
                changed = true;
            }
        }
        changed
    }

    fn ensure_open(&self) -> Result<(), SdkError> {
        if self.lifecycle == DeliveryLifecycle::Open {
            Ok(())
        } else {
            Err(SdkError::closed("notification delivery is closed"))
        }
    }

    fn ensure_no_unresolved_interaction(&self) -> Result<(), SdkError> {
        self.ensure_open()?;
        if self.has_unresolved_interaction() {
            Err(SdkError::busy("conversation has an unresolved interaction"))
        } else {
            Ok(())
        }
    }

    fn ensure_no_resolving_interaction(&self) -> Result<(), SdkError> {
        self.ensure_open()?;
        if self.has_resolving_interaction() {
            Err(SdkError::busy(
                "conversation has an interaction awaiting settlement",
            ))
        } else {
            Ok(())
        }
    }

    fn compact_terminal(&mut self) -> bool {
        let original_len = self.pending.len();
        while self
            .pending
            .front()
            .is_some_and(|notification| notification.state.is_terminal())
        {
            self.pending.pop_front();
        }
        self.pending.len() != original_len
    }

    fn next_delivery(
        &self,
        delivered_interaction: Option<&NotificationId>,
    ) -> Result<ReceiveStep, SdkError> {
        let Some(notification) = self.pending.front() else {
            return Ok(match self.lifecycle {
                DeliveryLifecycle::Open => ReceiveStep::Wait { draining: false },
                DeliveryLifecycle::Draining => ReceiveStep::End,
            });
        };
        let draining = self.lifecycle == DeliveryLifecycle::Draining;
        Ok(match notification.state {
            StoredNotificationState::Passive => ReceiveStep::Item {
                notification: notification.to_public()?,
                delivered: ReceiverDelivery::Passive(notification.id.clone()),
                draining,
            },
            StoredNotificationState::Pending => {
                if delivered_interaction == Some(&notification.id) {
                    ReceiveStep::Wait { draining }
                } else {
                    ReceiveStep::Item {
                        notification: notification.to_public()?,
                        delivered: ReceiverDelivery::Interaction(notification.id.clone()),
                        draining,
                    }
                }
            }
            StoredNotificationState::Resolving {
                acceptance: StoredAcceptance::Unconfirmed,
                ..
            } if self.lifecycle == DeliveryLifecycle::Open => ReceiveStep::Wait { draining },
            StoredNotificationState::Resolving {
                acceptance: StoredAcceptance::Unconfirmed,
                ..
            } => ReceiveStep::Failed {
                error: SdkError::conflict("interaction settlement is still in progress"),
                draining,
            },
            StoredNotificationState::Resolving {
                acceptance: StoredAcceptance::Ambiguous,
                ..
            } => ReceiveStep::Failed {
                error: SdkError::conflict("interaction settlement is still in progress"),
                draining,
            },
            StoredNotificationState::Terminal { .. } => ReceiveStep::Failed {
                error: SdkError::invalid_state("terminal notification was not compacted"),
                draining,
            },
        })
    }

    fn observe_resolution_settlement(
        &self,
        intent: &ResolutionIntent,
        settlement: ResolutionSettlement,
    ) -> Result<NotificationMutationObservation, NotificationSettlementError> {
        let Some(notification) = self
            .pending
            .front()
            .filter(|notification| &notification.id == intent.notification_id())
        else {
            return Ok(NotificationMutationObservation::Contradictory);
        };
        match notification.state {
            StoredNotificationState::Terminal {
                outcome: StoredTerminalOutcome::Accepted { attempt },
            } if attempt == intent.attempt() && settlement == ResolutionSettlement::Accepted => {
                Ok(NotificationMutationObservation::Expected)
            }
            StoredNotificationState::Terminal {
                outcome: StoredTerminalOutcome::Rejected { attempt },
            } if attempt == intent.attempt() && settlement == ResolutionSettlement::Rejected => {
                Ok(NotificationMutationObservation::Expected)
            }
            StoredNotificationState::Terminal {
                outcome: StoredTerminalOutcome::Cancelled { attempt },
            } if attempt == intent.attempt() && settlement == ResolutionSettlement::Cancelled => {
                Ok(NotificationMutationObservation::Expected)
            }
            StoredNotificationState::Resolving {
                attempt,
                acceptance: StoredAcceptance::Ambiguous,
            } if attempt == intent.attempt() && settlement == ResolutionSettlement::Ambiguous => {
                Ok(NotificationMutationObservation::Expected)
            }
            StoredNotificationState::Pending
                if settlement == ResolutionSettlement::Rejected
                    && self.lifecycle == DeliveryLifecycle::Open =>
            {
                Ok(NotificationMutationObservation::Expected)
            }
            StoredNotificationState::Resolving {
                attempt,
                acceptance: StoredAcceptance::Unconfirmed,
            } if attempt == intent.attempt() => Ok(NotificationMutationObservation::Unchanged),
            StoredNotificationState::Passive
            | StoredNotificationState::Pending
            | StoredNotificationState::Resolving { .. }
            | StoredNotificationState::Terminal { .. } => {
                Ok(NotificationMutationObservation::Contradictory)
            }
        }
    }

    fn settle_resolution(
        &mut self,
        intent: &ResolutionIntent,
        settlement: ResolutionSettlement,
    ) -> Result<ResolutionDisposition, SdkError> {
        let lifecycle = self.lifecycle;
        let notification = self
            .pending
            .front_mut()
            .filter(|notification| &notification.id == intent.notification_id())
            .ok_or_else(|| SdkError::not_found("notification is not pending"))?;
        let attempt = match notification.state {
            StoredNotificationState::Resolving {
                attempt,
                acceptance: StoredAcceptance::Unconfirmed,
            } if attempt == intent.attempt() => attempt,
            StoredNotificationState::Terminal {
                outcome: StoredTerminalOutcome::Accepted { attempt },
            } if attempt == intent.attempt() => {
                return Ok(ResolutionDisposition::AlreadyAccepted);
            }
            StoredNotificationState::Terminal {
                outcome: StoredTerminalOutcome::Rejected { attempt },
            } if attempt == intent.attempt() && settlement == ResolutionSettlement::Rejected => {
                return Ok(ResolutionDisposition::Applied(settlement));
            }
            StoredNotificationState::Terminal {
                outcome: StoredTerminalOutcome::Cancelled { attempt },
            } if attempt == intent.attempt() && settlement == ResolutionSettlement::Cancelled => {
                return Ok(ResolutionDisposition::Applied(settlement));
            }
            StoredNotificationState::Resolving {
                attempt,
                acceptance: StoredAcceptance::Ambiguous,
            } if attempt == intent.attempt() && settlement == ResolutionSettlement::Ambiguous => {
                return Ok(ResolutionDisposition::Applied(settlement));
            }
            StoredNotificationState::Passive
            | StoredNotificationState::Pending
            | StoredNotificationState::Resolving { .. }
            | StoredNotificationState::Terminal { .. } => {
                return Err(SdkError::conflict(
                    "notification is not awaiting this interaction settlement",
                ));
            }
        };
        notification.state = match settlement {
            ResolutionSettlement::Accepted => StoredNotificationState::Terminal {
                outcome: StoredTerminalOutcome::Accepted { attempt },
            },
            ResolutionSettlement::Rejected if lifecycle == DeliveryLifecycle::Open => {
                StoredNotificationState::Pending
            }
            ResolutionSettlement::Rejected => StoredNotificationState::Terminal {
                outcome: StoredTerminalOutcome::Rejected { attempt },
            },
            ResolutionSettlement::Ambiguous => StoredNotificationState::Resolving {
                attempt,
                acceptance: StoredAcceptance::Ambiguous,
            },
            ResolutionSettlement::Cancelled => StoredNotificationState::Terminal {
                outcome: StoredTerminalOutcome::Cancelled { attempt },
            },
        };
        Ok(ResolutionDisposition::Applied(settlement))
    }

    fn reconcile_native_resolution(
        &mut self,
        id: &NotificationId,
        settlement: ResolutionSettlement,
    ) -> Result<(), SdkError> {
        let notification = self
            .pending
            .iter_mut()
            .find(|notification| &notification.id == id)
            .ok_or_else(|| {
                SdkError::invalid_state("resolved interaction notification is missing")
            })?;
        match notification.state {
            StoredNotificationState::Pending | StoredNotificationState::Resolving { .. } => {
                notification.state = match settlement {
                    ResolutionSettlement::Accepted => StoredNotificationState::Terminal {
                        outcome: StoredTerminalOutcome::ReconciledAccepted,
                    },
                    ResolutionSettlement::Cancelled => StoredNotificationState::Terminal {
                        outcome: StoredTerminalOutcome::ReconciledCancelled,
                    },
                    ResolutionSettlement::Rejected | ResolutionSettlement::Ambiguous => {
                        return Err(SdkError::invalid_state(
                            "interaction resolution is not terminal",
                        ));
                    }
                };
                Ok(())
            }
            StoredNotificationState::Passive | StoredNotificationState::Terminal { .. } => Err(
                SdkError::invalid_state("resolved interaction lifecycle is contradictory"),
            ),
        }
    }

    fn close(&mut self) -> Result<Vec<NotificationId>, SdkError> {
        if self.lifecycle == DeliveryLifecycle::Draining {
            return Ok(Vec::new());
        }
        let mut cancelled = Vec::new();
        for notification in &mut self.pending {
            if notification.state.is_pending() {
                cancelled.push(notification.id.clone());
                notification.state = StoredNotificationState::Terminal {
                    outcome: StoredTerminalOutcome::ReconciledCancelled,
                };
            }
        }
        self.lifecycle = DeliveryLifecycle::Draining;
        Ok(cancelled)
    }
}

impl StoredNotification {
    fn preserves_receipt_identity(&self, expected: &Self) -> bool {
        self.sequence == expected.sequence
            && self.id == expected.id
            && self.inserted_at == expected.inserted_at
            && self.kind == expected.kind
            && match expected.state {
                StoredNotificationState::Passive | StoredNotificationState::Terminal { .. } => {
                    self.state == expected.state
                }
                // Resolution has its own exact attempt authority. A rejected
                // preacceptance attempt can return to Pending and a later attempt
                // can complete while this independent acknowledgment is observed.
                StoredNotificationState::Pending | StoredNotificationState::Resolving { .. } => {
                    !self.state.is_passive()
                }
            }
    }

    fn confirms_publication(&self, inserted_at: SystemTime, kind: &StoredNotificationKind) -> bool {
        self.inserted_at == inserted_at
            && &self.kind == kind
            && if kind.is_interaction() {
                self.state.is_pending()
            } else {
                self.state.is_passive()
            }
    }

    fn validate_stored(&self) -> Result<(), SdkError> {
        self.kind
            .validate()
            .map_err(|_| SdkError::invalid_state("stored notification payload is malformed"))?;
        match (&self.kind, self.state) {
            (kind, StoredNotificationState::Passive) if !kind.is_interaction() => Ok(()),
            (
                kind,
                StoredNotificationState::Pending
                | StoredNotificationState::Resolving { .. }
                | StoredNotificationState::Terminal { .. },
            ) if kind.is_interaction() => Ok(()),
            _ => Err(SdkError::invalid_state(
                "stored notification kind and lifecycle disagree",
            )),
        }
    }

    fn begin_resolution(
        &mut self,
        resolution: &NotificationResolution,
        attempt: ResolutionAttempt,
    ) -> Result<(), SdkError> {
        if !self.state.is_pending() {
            return Err(SdkError::conflict("interaction is not pending resolution"));
        }
        self.kind.validate_resolution(resolution)?;
        self.state = StoredNotificationState::Resolving {
            attempt,
            acceptance: StoredAcceptance::Unconfirmed,
        };
        Ok(())
    }

    fn to_public(&self) -> Result<ConversationNotification, SdkError> {
        ConversationNotification::new(self.id.clone(), self.inserted_at, self.kind.to_public())
            .map_err(|_| SdkError::invalid_state("stored notification payload is malformed"))
    }
}

impl StoredNotificationState {
    fn is_passive(self) -> bool {
        matches!(self, Self::Passive)
    }

    fn is_pending(self) -> bool {
        matches!(self, Self::Pending)
    }

    fn is_resolving(self) -> bool {
        matches!(self, Self::Resolving { .. })
    }

    fn is_unresolved(self) -> bool {
        matches!(self, Self::Pending | Self::Resolving { .. })
    }

    fn is_terminal(self) -> bool {
        matches!(self, Self::Terminal { .. })
    }
}

impl StoredNotificationKind {
    fn from_public(kind: ConversationNotificationKind) -> Result<Self, SdkError> {
        let stored = match kind {
            ConversationNotificationKind::Message { text } => Self::Message { text },
            ConversationNotificationKind::FileChanged { path, summary } => {
                Self::FileChanged { path, summary }
            }
            ConversationNotificationKind::Process { label, state } => Self::Process {
                label,
                state: StoredProcessState::from_public(state),
            },
            ConversationNotificationKind::Progress { label, detail } => {
                Self::Progress { label, detail }
            }
            ConversationNotificationKind::Warning { message } => Self::Warning { message },
            ConversationNotificationKind::Error { message } => Self::Error { message },
            ConversationNotificationKind::Interaction {
                title,
                body,
                response_schema,
                dismissible,
            } => Self::Interaction {
                title,
                body,
                response_schema,
                dismissible,
            },
        };
        stored.validate().map_err(SdkError::protocol)?;
        Ok(stored)
    }

    fn to_public(&self) -> ConversationNotificationKind {
        match self {
            Self::Message { text } => ConversationNotificationKind::Message { text: text.clone() },
            Self::FileChanged { path, summary } => ConversationNotificationKind::FileChanged {
                path: path.clone(),
                summary: summary.clone(),
            },
            Self::Process { label, state } => ConversationNotificationKind::Process {
                label: label.clone(),
                state: state.to_public(),
            },
            Self::Progress { label, detail } => ConversationNotificationKind::Progress {
                label: label.clone(),
                detail: detail.clone(),
            },
            Self::Warning { message } => ConversationNotificationKind::Warning {
                message: message.clone(),
            },
            Self::Error { message } => ConversationNotificationKind::Error {
                message: message.clone(),
            },
            Self::Interaction {
                title,
                body,
                response_schema,
                dismissible,
            } => ConversationNotificationKind::Interaction {
                title: title.clone(),
                body: body.clone(),
                response_schema: response_schema.clone(),
                dismissible: *dismissible,
            },
        }
    }

    fn is_interaction(&self) -> bool {
        matches!(self, Self::Interaction { .. })
    }

    fn validate(&self) -> Result<(), &'static str> {
        match self {
            Self::Message { text }
            | Self::Warning { message: text }
            | Self::Error { message: text }
                if text.trim().is_empty() =>
            {
                return Err("notification message is blank");
            }
            Self::FileChanged { path, summary } => {
                WorkspaceContext::normalized_relative(path)
                    .map_err(|_| "file notification path is malformed")?;
                if summary
                    .as_ref()
                    .is_some_and(|summary| summary.trim().is_empty())
                {
                    return Err("file notification summary is blank");
                }
            }
            Self::Process { label, .. }
                if label.is_empty()
                    || label.trim() != label
                    || label.chars().any(char::is_control) =>
            {
                return Err("process notification label is unsafe");
            }
            Self::Progress { label, detail } => {
                if label.is_empty() || label.trim() != label {
                    return Err("progress notification label is malformed");
                }
                if detail
                    .as_ref()
                    .is_some_and(|detail| detail.trim().is_empty())
                {
                    return Err("progress notification detail is blank");
                }
            }
            Self::Interaction {
                title,
                body,
                response_schema,
                ..
            } => {
                if title.is_empty() || title.trim() != title {
                    return Err("interaction notification title is malformed");
                }
                if body.trim().is_empty() {
                    return Err("interaction notification body is blank");
                }
                if response_schema
                    .as_ref()
                    .is_some_and(|schema| !schema.is_object())
                {
                    return Err("interaction schema is not an object");
                }
            }
            Self::Message { .. }
            | Self::Process { .. }
            | Self::Warning { .. }
            | Self::Error { .. } => {}
        }
        let Self::Interaction {
            response_schema: Some(schema),
            ..
        } = self
        else {
            return Ok(());
        };
        if serde_json::to_vec(schema)
            .map_err(|_| "interaction schema is malformed")?
            .len()
            > MAX_INTERACTION_SCHEMA_BYTES
        {
            return Err("interaction schema exceeds the supported size limit");
        }
        jsonschema::draft202012::meta::validate(schema)
            .map_err(|_| "interaction schema is malformed")?;
        jsonschema::draft202012::options()
            .build(schema)
            .map_err(|_| "interaction schema is not self-contained")?;
        Ok(())
    }

    fn validate_resolution(&self, resolution: &NotificationResolution) -> Result<(), SdkError> {
        let Self::Interaction {
            response_schema,
            dismissible,
            ..
        } = self
        else {
            return Err(SdkError::invalid_input(
                "passive notification cannot be resolved",
            ));
        };
        match (response_schema, resolution) {
            (None, NotificationResolution::Allow | NotificationResolution::Deny { .. }) => Ok(()),
            (Some(schema), NotificationResolution::Submit { content }) => {
                if serde_json::to_vec(content)
                    .map_err(|_| {
                        SdkError::invalid_input("submitted interaction content is invalid")
                    })?
                    .len()
                    > MAX_INTERACTION_SUBMISSION_BYTES
                {
                    return Err(SdkError::invalid_input(
                        "submitted interaction content exceeds the supported size limit",
                    ));
                }
                let validator = jsonschema::draft202012::options()
                    .build(schema)
                    .map_err(|_| SdkError::invalid_state("stored interaction schema is invalid"))?;
                validator.validate(content).map_err(|_| {
                    SdkError::invalid_input("submitted interaction content is invalid")
                })
            }
            (_, NotificationResolution::Dismiss { .. }) if *dismissible => Ok(()),
            _ => Err(SdkError::invalid_input(
                "notification resolution does not match the interaction",
            )),
        }
    }
}

impl StoredProcessState {
    fn from_public(state: ProcessState) -> Self {
        match state {
            ProcessState::Running => StoredProcessState::Running,
            ProcessState::Exited { exit_code } => StoredProcessState::Exited { exit_code },
        }
    }

    fn to_public(&self) -> ProcessState {
        match self {
            Self::Running => ProcessState::Running,
            Self::Exited { exit_code } => ProcessState::Exited {
                exit_code: *exit_code,
            },
        }
    }
}
