use crate::error::{DeadlineKind, ErrorDetail, ErrorKind, ErrorPhase};
mod continuations;
mod prompt;
pub(crate) mod records;

use std::collections::BTreeSet;
use std::time::Duration;

use storage_kit::{StateStoreBackend, StateTransactionBackend};

use crate::agent_storage::AgentStorageKnowledgeError;
use crate::backends::opencode::notifications::OpenCodePromptStatus;
use crate::backends::opencode::protocol::inbound::OpenCodeKnownEvent;
use crate::backends::opencode::protocol::schemas::OpenCodeProcessEffect;
use crate::backends::opencode::tools::{OpenCodeCatalogGeneration, OpenCodeToolCatalog};
use crate::core::hierarchy::HierarchyAdmission;
use crate::core::notification_store::{
    NotificationCloseError, NotificationClosure, NotificationDeleteError,
    NotificationInitializationError, NotificationInteractionCheckError,
    NotificationMutationObservation, NotificationReconciliation, NotificationSettlementError,
    NotificationStore, ResolutionDisposition, ResolutionIntent, ResolutionSettlement,
};
use crate::core::state::{ConversationRecord, WorkspaceRecord};
use crate::{
    AgentId, AgentInfo, AgentStorage, ConversationId, ConversationNotificationKind, KitError,
    KnowledgeOwnerId, NewWorkspace, NotificationId, UpdateWorkspaceRequest, WorkspaceId,
};

use prompt::OpenCodePromptState;
pub(crate) use prompt::{
    OpenCodeAbort, OpenCodeAbortOutcome, OpenCodeAbortProgress, OpenCodeNativePromptState,
    OpenCodePromptGeneration, OpenCodePromptRegistration, OpenCodePromptStartError,
    OpenCodePromptStatusSignal,
};
use records::{
    OpenCodeAttachment, OpenCodeCatalogBinding, OpenCodeCatalogState, OpenCodeCleanup,
    OpenCodeContinuation, OpenCodeContinuationKind, OpenCodeContinuationRequest,
    OpenCodeContinuationState, OpenCodeEventDisposition, OpenCodeEventIdentity,
    OpenCodeEventProgress, OpenCodeNativeResolution, OpenCodePromptRecord, OpenCodeSessionBinding,
    OpenCodeWorkspaceRouting,
};

const OPENCODE_ATTACHMENT_NAMESPACE: &str = "agents_kit_opencode_attachments_v1";
const OPENCODE_CATALOG_NAMESPACE: &str = "agents_kit_opencode_catalogs_v1";
const OPENCODE_SESSION_NAMESPACE: &str = "agents_kit_opencode_sessions_v1";
const OPENCODE_CLEANUP_NAMESPACE: &str = "agents_kit_opencode_cleanup_v1";
const OPENCODE_EVENT_PROGRESS_NAMESPACE: &str = "agents_kit_opencode_event_progress_v1";
const MAX_STATE_PAGES: usize = 1_024;

pub(crate) struct OpenCodeStateStore<S: StateStoreBackend> {
    agent_id: AgentId,
    storage: AgentStorage<S>,
    knowledge_enabled: bool,
    notifications: NotificationStore<S>,
    prompts: OpenCodePromptState<S>,
}

enum OpenCodeConversationCloseError {
    TransactionConflict,
    Failure(KitError),
}

#[derive(Clone)]
pub(super) enum OpenCodeTransactionError {
    TransactionConflict(KitError),
    Failure(KitError),
    TerminalCleanup(KitError),
}

#[derive(Clone, Copy)]
enum OpenCodeMissingBinding {
    InventoryChanged,
    InvalidState,
    NotFound,
}

impl OpenCodeConversationCloseError {
    fn storage(
        error: storage_kit::StateStoreError,
        integration_message: &'static str,
        phase: ErrorPhase,
    ) -> Self {
        match error {
            storage_kit::StateStoreError::TransactionConflict { .. } => Self::TransactionConflict,
            error => Self::Failure(
                KitError::from_state_store(error, integration_message).in_phase(phase),
            ),
        }
    }
}

impl OpenCodeTransactionError {
    fn storage(
        error: storage_kit::StateStoreError,
        integration_message: &'static str,
        phase: ErrorPhase,
    ) -> Self {
        match error {
            error @ storage_kit::StateStoreError::TransactionConflict { .. } => {
                Self::TransactionConflict(
                    KitError::from_state_store(error, integration_message).in_phase(phase),
                )
            }
            error => Self::Failure(
                KitError::from_state_store(error, integration_message).in_phase(phase),
            ),
        }
    }

    fn nested(error: KitError) -> Self {
        match error {
            error if error.kind() == ErrorKind::Conflict => Self::TransactionConflict(error),
            error => Self::Failure(error),
        }
    }
}

impl OpenCodeMissingBinding {
    fn error(self) -> KitError {
        match self {
            Self::InventoryChanged => {
                KitError::busy("conversation inventory changed during binding reconciliation")
            }
            Self::InvalidState => KitError::invalid_state("session binding is missing"),
            Self::NotFound => KitError::not_found("conversation does not exist"),
        }
    }
}

impl From<KitError> for OpenCodeTransactionError {
    fn from(error: KitError) -> Self {
        Self::Failure(error)
    }
}

impl From<NotificationInteractionCheckError> for OpenCodeTransactionError {
    fn from(error: NotificationInteractionCheckError) -> Self {
        match error {
            NotificationInteractionCheckError::TransactionConflict => Self::TransactionConflict(
                KitError::integration("interaction state could not be loaded"),
            ),
            NotificationInteractionCheckError::Failure(error) => Self::Failure(error),
        }
    }
}

impl From<NotificationSettlementError> for OpenCodeTransactionError {
    fn from(error: NotificationSettlementError) -> Self {
        match error {
            NotificationSettlementError::TransactionConflict => Self::TransactionConflict(
                KitError::integration("interaction resolution transaction conflicted"),
            ),
            NotificationSettlementError::Failure(error) => Self::Failure(error),
        }
    }
}

pub(super) enum OpenCodeConversationMutationDisposition {
    Settled,
    RecoveryRequired(KitError),
}

pub(super) enum OpenCodeWorkspaceDeleteDisposition {
    Settled,
    RecoveryRequired(KitError),
}

#[derive(Clone)]
pub(crate) enum OpenCodeEventEffect {
    None,
    CatalogInvalidated,
    Passive {
        conversation_id: ConversationId,
        kinds: Vec<ConversationNotificationKind>,
    },
    Process {
        conversation_id: ConversationId,
        process: OpenCodeProcessEffect,
        kind: ConversationNotificationKind,
    },
    PromptStatus {
        conversation_id: ConversationId,
        status: OpenCodePromptStatus,
    },
    PromptMessage {
        conversation_id: ConversationId,
        session_id: String,
        message_id: String,
    },
    PromptPart {
        conversation_id: ConversationId,
        session_id: String,
        message_id: String,
        part_id: String,
    },
    ConversationDeleted {
        conversation_id: ConversationId,
        workspace_id: WorkspaceId,
        session_id: String,
    },
    InteractionResolved {
        conversation_id: ConversationId,
        session_id: String,
        request_id: String,
        kind: OpenCodeContinuationKind,
        answer_count: Option<usize>,
        outcome: OpenCodeNativeResolution,
    },
    Interaction {
        conversation_id: ConversationId,
        kind: ConversationNotificationKind,
        request: OpenCodeContinuationRequest,
    },
}

impl<S: StateStoreBackend> Clone for OpenCodeStateStore<S> {
    fn clone(&self) -> Self {
        Self {
            agent_id: self.agent_id.clone(),
            storage: self.storage.clone(),
            knowledge_enabled: self.knowledge_enabled,
            notifications: self.notifications.clone(),
            prompts: self.prompts.clone(),
        }
    }
}

impl<S: StateStoreBackend> OpenCodeStateStore<S> {
    // Storage timeouts used to reach these state-owned retry loops as Integration.
    // Keep the same retry policy while retaining the normalized timeout category.
    pub(crate) fn retryable_state_error(error: &KitError) -> bool {
        error.kind() == ErrorKind::Integration
            || (error.kind() == ErrorKind::Timeout
                && matches!(
                    error.detail(),
                    Some(ErrorDetail::DeadlineElapsed {
                        kind: DeadlineKind::Operation,
                        timeout: Some(_)
                    })
                ))
    }

    fn workspace_update_error(
        error: storage_kit::StateStoreError,
        integration_message: &'static str,
    ) -> KitError {
        match error {
            storage_kit::StateStoreError::TransactionConflict { .. } => {
                KitError::conflict("workspace update conflicted")
            }
            error => KitError::from_state_store(error, integration_message)
                .in_phase(ErrorPhase::StateWrite),
        }
    }

    fn workspace_delete_error(
        error: storage_kit::StateStoreError,
        integration_message: &'static str,
    ) -> KitError {
        match error {
            storage_kit::StateStoreError::TransactionConflict { .. } => {
                KitError::conflict("workspace deletion conflicted")
            }
            error => KitError::from_state_store(error, integration_message)
                .in_phase(ErrorPhase::StateWrite),
        }
    }

    pub(crate) fn new(
        agent_id: AgentId,
        storage: AgentStorage<S>,
        knowledge_enabled: bool,
        prompt_limit: usize,
    ) -> Result<Self, KitError> {
        let notifications = NotificationStore::<S>::new(storage.clone())?;
        let prompts =
            OpenCodePromptState::new(storage.clone(), notifications.clone(), prompt_limit);
        Ok(Self {
            agent_id,
            notifications,
            storage,
            knowledge_enabled,
            prompts,
        })
    }

    pub(crate) fn notifications(&self) -> &NotificationStore<S> {
        &self.notifications
    }

    async fn stage_knowledge_forget(
        &self,
        transaction: &mut S::Transaction,
        owner: &KnowledgeOwnerId,
    ) -> Result<(), AgentStorageKnowledgeError> {
        if !self.knowledge_enabled {
            return Ok(());
        }
        self.storage
            .stage_knowledge_forget(transaction, owner)
            .await
    }

    pub(crate) fn prompt_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, KitError> {
        self.prompts.permit()
    }

    pub(crate) fn prompt_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.prompts.changes()
    }

    pub(crate) fn prompt_statuses(
        &self,
    ) -> tokio::sync::broadcast::Receiver<(ConversationId, OpenCodePromptStatusSignal)> {
        self.prompts.statuses()
    }

    pub(crate) fn refresh_prompt_status(&self, conversation_id: &ConversationId) {
        self.prompts
            .publish_status((conversation_id.clone(), OpenCodePromptStatusSignal::Refresh));
    }

    pub(crate) fn abort_prompt_tasks(&self) {
        self.prompts.abort_tasks();
    }

    pub(crate) async fn shutdown_prompt_tasks(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), KitError> {
        self.prompts.shutdown_tasks(deadline).await
    }

    pub(crate) fn observe_prompt_output(
        &self,
        conversation_id: &ConversationId,
        event: &OpenCodeKnownEvent,
        live: bool,
        fresh: bool,
    ) {
        self.prompts
            .observe_output(conversation_id, event, live, fresh);
    }

    pub(crate) fn interrupt_prompt_outputs(&self, error: &KitError) {
        self.prompts.interrupt_outputs(error);
    }

    pub(crate) fn register_prompt(
        &self,
        conversation_id: ConversationId,
        generation_id: String,
        admission: HierarchyAdmission,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<OpenCodePromptRegistration<S>, KitError> {
        self.prompts
            .register(conversation_id, generation_id, admission, permit)
    }

    fn release_prompt_runtime(&self, conversation_id: &ConversationId) {
        self.prompts.release_runtime(conversation_id);
    }

    fn validate_attachment_owner(
        &self,
        info: &AgentInfo,
        attachment: &OpenCodeAttachment,
    ) -> Result<(), KitError> {
        if info.id() != attachment.agent_id() {
            return Err(KitError::invalid_state(
                "attachment owner does not match agent identity",
            ));
        }
        Ok(())
    }

    async fn finish_continuation_reconciliation(
        &self,
        continuation: &OpenCodeContinuation,
    ) -> Result<bool, KitError> {
        loop {
            match self
                .prompts
                .continuation_reconciliation_matches(continuation)
                .await
            {
                Ok(false) => return Ok(false),
                Ok(true) => break,
                Err(error) if Self::retryable_state_error(&error) => {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                Err(error) => return Err(error),
            }
        }
        if continuation.state() == OpenCodeContinuationState::Cancelled {
            self.prompts
                .observe_native_cleanup(continuation, prompt::OpenCodePromptDurability::Reserved)?;
        }
        self.notifications
            .finish_reconciliation(continuation.conversation_id());
        self.refresh_prompt_status(continuation.conversation_id());
        Ok(true)
    }

    async fn reconcile_terminal_prompt(
        &self,
        continuation: &OpenCodeContinuation,
        deadline: tokio::time::Instant,
    ) -> Result<bool, KitError> {
        let conversation_id = continuation.conversation_id();
        let store = self.storage.store();
        let key = OpenCodePromptRecord::key(conversation_id)?;
        let mut last_conflict = None;
        loop {
            if tokio::time::Instant::now() >= deadline {
                let error = KitError::timeout("terminal prompt cleanup deadline elapsed")
                    .at_phase(ErrorPhase::EventSynchronization);
                return Err(match last_conflict {
                    Some(conflict) => error.with_supplementary(conflict),
                    None => error,
                });
            }
            let continuations =
                tokio::time::timeout_at(deadline, self.prompts.continuations(conversation_id))
                    .await
                    .map_err(|_| {
                        let error = KitError::timeout("terminal prompt cleanup deadline elapsed")
                            .at_phase(ErrorPhase::EventSynchronization);
                        match &last_conflict {
                            Some(conflict) => error.with_supplementary(conflict.clone()),
                            None => error,
                        }
                    })??;
            if continuations
                .iter()
                .any(OpenCodeContinuation::blocks_prompt_release)
            {
                return Ok(false);
            }
            if tokio::time::Instant::now() >= deadline {
                let error = KitError::timeout("terminal prompt cleanup deadline elapsed")
                    .at_phase(ErrorPhase::EventSynchronization);
                return Err(match last_conflict {
                    Some(conflict) => error.with_supplementary(conflict),
                    None => error,
                });
            }
            let mut transaction = match store.begin_transaction().await {
                Ok(transaction) => transaction,
                Err(source) => {
                    let error = KitError::from_state_store(
                        source,
                        "terminal prompt cleanup could not begin",
                    )
                    .in_phase(ErrorPhase::StateWrite);
                    if error.detail() == Some(&ErrorDetail::TransactionConflict) {
                        last_conflict = Some(error);
                        tokio::time::sleep_until(
                            deadline.min(tokio::time::Instant::now() + Duration::from_millis(25)),
                        )
                        .await;
                        continue;
                    }
                    return Err(error);
                }
            };
            // Once admitted, the transaction is settled even if its deadline
            // expires. Only a confirmed rollback permits another attempt.
            let staged = async {
                let Some(snapshot) = transaction.load(key.clone()).await.map_err(|error| {
                    KitError::from_state_store(error, "prompt state could not be loaded")
                        .in_phase(ErrorPhase::StateRead)
                })?
                else {
                    return Ok(None);
                };
                let record = OpenCodePromptRecord::decode(snapshot.clone(), conversation_id)?;
                if record.clearable() {
                    transaction.delete(key.clone()).await.map_err(|error| {
                        KitError::from_state_store(error, "terminal prompt could not be cleared")
                            .in_phase(ErrorPhase::StateWrite)
                    })?;
                }
                Ok::<_, KitError>(Some((snapshot, record)))
            }
            .await;
            let (before, generation_id) = match staged {
                Ok(Some((snapshot, record))) if record.clearable() => {
                    (snapshot, record.generation_id().to_owned())
                }
                Ok(observed) => {
                    transaction.rollback().await.map_err(|error| {
                        KitError::from_state_store(error, "terminal prompt rollback failed")
                            .at_phase(ErrorPhase::Cleanup)
                    })?;
                    if tokio::time::Instant::now() >= deadline {
                        let error = KitError::timeout("terminal prompt cleanup deadline elapsed")
                            .at_phase(ErrorPhase::EventSynchronization);
                        return Err(match last_conflict {
                            Some(conflict) => error.with_supplementary(conflict),
                            None => error,
                        });
                    }
                    if observed.is_none()
                        && let Some(generation_id) = continuations
                            .iter()
                            .find(|current| {
                                current.has_same_request(continuation)
                                    && matches!(
                                        current.state(),
                                        OpenCodeContinuationState::Accepted
                                            | OpenCodeContinuationState::Cancelled
                                    )
                            })
                            .and_then(OpenCodeContinuation::generation_id)
                    {
                        // A prior deletion may have committed after its caller
                        // disappeared. Only this settled request can release
                        // its own generation after confirmed absence.
                        self.prompts
                            .mark_prompt_releasable(conversation_id, generation_id);
                    }
                    return Ok(false);
                }
                Err(error) => {
                    if let Err(secondary) = transaction.rollback().await {
                        return Err(error.with_supplementary(
                            KitError::from_state_store(
                                secondary,
                                "terminal prompt rollback failed",
                            )
                            .at_phase(ErrorPhase::Cleanup),
                        ));
                    }
                    if error.detail() == Some(&ErrorDetail::TransactionConflict) {
                        last_conflict = Some(error);
                        tokio::time::sleep_until(
                            deadline.min(tokio::time::Instant::now() + Duration::from_millis(25)),
                        )
                        .await;
                        continue;
                    }
                    return Err(error);
                }
            };
            if let Err(source) = transaction.commit().await {
                let error =
                    KitError::from_state_store(source, "terminal prompt cleanup could not commit")
                        .in_phase(ErrorPhase::StateCommit);
                if error.detail() == Some(&ErrorDetail::TransactionConflict) {
                    last_conflict = Some(error);
                    tokio::time::sleep_until(
                        deadline.min(tokio::time::Instant::now() + Duration::from_millis(25)),
                    )
                    .await;
                    continue;
                }
                self.observe_terminal_prompt_deletion(&before, error, deadline)
                    .await?;
            }
            self.prompts
                .mark_prompt_releasable(conversation_id, &generation_id);
            self.prompts.mark_changed();
            if tokio::time::Instant::now() >= deadline {
                let error = KitError::timeout("terminal prompt cleanup deadline elapsed")
                    .at_phase(ErrorPhase::EventSynchronization);
                return Err(match last_conflict {
                    Some(conflict) => error.with_supplementary(conflict),
                    None => error,
                });
            }
            return Ok(true);
        }
    }

    async fn observe_terminal_prompt_deletion(
        &self,
        before: &storage_kit::state::record::StateRecordSnapshot,
        primary: KitError,
        deadline: tokio::time::Instant,
    ) -> Result<(), KitError> {
        let mut last_read_error = None;
        loop {
            let observation = if tokio::time::Instant::now() >= deadline {
                None
            } else {
                tokio::time::timeout_at(
                    deadline,
                    self.storage.store().load(before.record.key.clone()),
                )
                .await
                .ok()
            };
            let Some(observation) = observation else {
                let mut error = primary.with_supplementary(
                    KitError::timeout("terminal prompt deletion observation deadline elapsed")
                        .at_phase(ErrorPhase::EventSynchronization),
                );
                if let Some(read) = last_read_error {
                    error = error.with_supplementary(read);
                }
                return Err(error);
            };
            match observation {
                Ok(None) => return Ok(()),
                Ok(Some(actual)) => {
                    let receipt = if actual == *before {
                        KitError::integration("terminal prompt deletion remains unconfirmed")
                    } else {
                        KitError::invalid_state("terminal prompt deletion receipt changed")
                    };
                    return Err(
                        primary.with_supplementary(receipt.at_phase(ErrorPhase::Reconciliation))
                    );
                }
                Err(source) => {
                    let error = KitError::from_state_store(
                        source,
                        "terminal prompt cleanup outcome could not be checked",
                    )
                    .in_phase(ErrorPhase::StateRead);
                    if !Self::retryable_state_error(&error)
                        || tokio::time::Instant::now() >= deadline
                    {
                        return Err(primary.with_supplementary(error));
                    }
                    last_read_error = Some(error);
                    tokio::time::sleep_until(
                        deadline.min(tokio::time::Instant::now() + Duration::from_millis(25)),
                    )
                    .await;
                }
            }
        }
    }

    async fn reclaim_terminal_notification(
        &self,
        conversation_id: &ConversationId,
    ) -> Result<(), KitError> {
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            KitError::from_state_store(error, "notification reclamation could not begin")
                .in_phase(ErrorPhase::StateWrite)
        })?;
        self.notifications
            .reclaim_terminal_in_transaction(&mut transaction, conversation_id)
            .await?;
        transaction.commit().await.map(|_| ()).map_err(|error| {
            KitError::from_state_store(error, "notification reclamation could not commit")
                .in_phase(ErrorPhase::StateCommit)
        })
    }

    pub(crate) async fn reconcile_reopen_prompt(
        &self,
        conversation_id: &ConversationId,
        native_active: bool,
        deadline: tokio::time::Instant,
    ) -> Result<Option<String>, KitError> {
        if tokio::time::Instant::now() >= deadline {
            return Err(KitError::timeout("prompt reconciliation deadline elapsed"));
        }
        let store = self.storage.store();
        let key = OpenCodePromptRecord::key(conversation_id)?;
        let mut transaction = match store.begin_transaction().await {
            Ok(transaction) => transaction,
            Err(error) => {
                let error =
                    KitError::from_state_store(error, "prompt reconciliation could not begin")
                        .in_phase(ErrorPhase::StateWrite);
                return Err(if tokio::time::Instant::now() >= deadline {
                    KitError::timeout("prompt reconciliation deadline elapsed")
                        .at_phase(ErrorPhase::Reconciliation)
                        .with_supplementary(error)
                } else {
                    error
                });
            }
        };
        if tokio::time::Instant::now() >= deadline {
            return Err(KitError::timeout("prompt reconciliation deadline elapsed")
                .with_cleanup_result(transaction.rollback().await.map_err(|error| {
                    KitError::from_state_store(error, "prompt reconciliation could not roll back")
                        .at_phase(ErrorPhase::Cleanup)
                })));
        }
        let snapshot = match transaction.load(key.clone()).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                let mut error =
                    KitError::from_state_store(error, "prompt state could not be loaded")
                        .in_phase(ErrorPhase::StateRead);
                if let Err(secondary) = transaction.rollback().await {
                    error = error.with_supplementary(
                        KitError::from_state_store(secondary, "transaction rollback failed")
                            .at_phase(ErrorPhase::Cleanup),
                    );
                }
                return if tokio::time::Instant::now() >= deadline {
                    Err(KitError::timeout("prompt reconciliation deadline elapsed")
                        .at_phase(ErrorPhase::Reconciliation)
                        .with_supplementary(error))
                } else {
                    Err(error)
                };
            }
        };
        let Some(snapshot) = snapshot else {
            let cleanup = transaction.rollback().await.map_err(|error| {
                KitError::from_state_store(error, "prompt reconciliation could not roll back")
                    .at_phase(ErrorPhase::Cleanup)
            });
            if tokio::time::Instant::now() >= deadline {
                return Err(KitError::timeout("prompt reconciliation deadline elapsed")
                    .with_cleanup_result(cleanup));
            }
            if native_active {
                return Err(KitError::invalid_state(
                    "active runtime work lacks a durable prompt owner",
                )
                .with_cleanup_result(cleanup));
            }
            cleanup?;
            let detached_generation = self.prompts.detached_generation(conversation_id)?;
            if let Some(generation_id) = detached_generation {
                self.prompts
                    .mark_prompt_releasable(conversation_id, &generation_id);
            }
            return Ok(None);
        };
        let mut record = match OpenCodePromptRecord::decode(snapshot, conversation_id) {
            Ok(record) => record,
            Err(error) => {
                let rollback = transaction.rollback().await;
                return Err(match rollback {
                    Ok(()) => error,
                    Err(secondary) => error.with_supplementary(
                        KitError::from_state_store(secondary, "transaction rollback failed")
                            .at_phase(ErrorPhase::Cleanup),
                    ),
                });
            }
        };
        let waiter_changed = match record.mark_waiter_lost() {
            Ok(changed) => changed,
            Err(error) => {
                let rollback = transaction.rollback().await;
                return Err(match rollback {
                    Ok(()) => error,
                    Err(secondary) => error.with_supplementary(
                        KitError::from_state_store(secondary, "transaction rollback failed")
                            .at_phase(ErrorPhase::Cleanup),
                    ),
                });
            }
        };
        if record.is_prepared() {
            let live_generation = match self
                .prompts
                .generation(conversation_id, record.generation_id())
                .is_live()
            {
                Ok(live) => live,
                Err(error) => {
                    let rollback = transaction.rollback().await;
                    return Err(match rollback {
                        Ok(()) => error,
                        Err(secondary) => error.with_supplementary(
                            KitError::from_state_store(secondary, "transaction rollback failed")
                                .at_phase(ErrorPhase::Cleanup),
                        ),
                    });
                }
            };
            if live_generation {
                let cleanup = transaction.rollback().await.map_err(|error| {
                    KitError::from_state_store(error, "prompt reconciliation could not roll back")
                        .at_phase(ErrorPhase::Cleanup)
                });
                let primary = if tokio::time::Instant::now() >= deadline {
                    KitError::timeout("prompt reconciliation deadline elapsed")
                } else {
                    KitError::busy("prepared prompt remains owned by its live runtime")
                };
                return Err(primary.with_cleanup_result(cleanup));
            }
            if native_active {
                let outcome = self
                    .prompts
                    .generation(conversation_id, record.generation_id())
                    .commit_recovered_prompt(transaction, &record, deadline)
                    .await;
                outcome?;
                if tokio::time::Instant::now() >= deadline {
                    return Err(KitError::timeout("prompt reconciliation deadline elapsed"));
                }
                return Err(KitError::invalid_state(
                    "runtime work exists before the durable dispatch boundary",
                ));
            }
            if let Err(error) = transaction.delete(key.clone()).await {
                let failure = match error {
                    storage_kit::StateStoreError::TransactionConflict { .. } => {
                        KitError::conflict("prompt recovery transaction conflicted")
                            .with_detail(ErrorDetail::TransactionConflict)
                    }
                    error => {
                        KitError::from_state_store(error, "prepared prompt could not be removed")
                            .in_phase(ErrorPhase::StateWrite)
                    }
                };
                let primary = failure;
                let rollback = transaction.rollback().await;
                return Err(match rollback {
                    Ok(()) => primary,
                    Err(secondary) => primary.with_supplementary(
                        KitError::from_state_store(secondary, "transaction rollback failed")
                            .at_phase(ErrorPhase::Cleanup),
                    ),
                });
            }
            let outcome = match transaction.commit().await {
                Ok(()) => Ok(()),
                Err(storage_kit::StateStoreError::TransactionConflict { .. }) => {
                    Err(KitError::conflict("prompt recovery transaction conflicted")
                        .with_detail(ErrorDetail::TransactionConflict))
                }
                Err(source) => {
                    let primary = KitError::from_state_store(
                        source,
                        "prepared prompt cleanup could not commit",
                    )
                    .in_phase(ErrorPhase::StateCommit);
                    match store.load(key).await {
                        Ok(None) => Ok(()),
                        Ok(Some(_)) => Err(primary),
                        Err(secondary) => Err(primary.with_supplementary(
                            KitError::from_state_store(
                                secondary,
                                "prepared prompt cleanup outcome could not be checked",
                            )
                            .in_phase(ErrorPhase::Reconciliation),
                        )),
                    }
                }
            };
            if outcome.is_ok() {
                let generation_id = record.generation_id().to_owned();
                self.prompts
                    .mark_prompt_releasable(conversation_id, &generation_id);
                self.prompts.mark_changed();
            }
            outcome?;
            if tokio::time::Instant::now() >= deadline {
                return Err(KitError::timeout("prompt reconciliation deadline elapsed"));
            }
            return Ok(None);
        }
        if !native_active {
            let generation_id = record.generation_id().to_owned();
            let cleanup = transaction.rollback().await.map_err(|error| {
                KitError::from_state_store(error, "prompt reconciliation could not roll back")
                    .at_phase(ErrorPhase::Cleanup)
            });
            if tokio::time::Instant::now() >= deadline {
                return Err(KitError::timeout("prompt reconciliation deadline elapsed")
                    .with_cleanup_result(cleanup));
            }
            cleanup?;
            let progress = self
                .prompts
                .generation(conversation_id, &generation_id)
                .reconcile_abort(Some(OpenCodeNativePromptState::Idle), deadline)
                .await?;
            let result = match progress {
                OpenCodeAbortProgress::Settled(OpenCodeAbortOutcome::Complete) => Ok(None),
                OpenCodeAbortProgress::Settled(OpenCodeAbortOutcome::RepairedInvalidState) => {
                    Err(KitError::invalid_state(
                        "runtime work ended without a matching terminal response",
                    ))
                }
                OpenCodeAbortProgress::Pending
                | OpenCodeAbortProgress::Deletion { .. }
                | OpenCodeAbortProgress::Settling(_) => Err(KitError::busy(
                    "prompt remains recovery-owned during reopen",
                )),
            };
            return if tokio::time::Instant::now() >= deadline {
                Err(KitError::timeout("prompt reconciliation deadline elapsed"))
            } else {
                result
            };
        }
        if record.is_terminal() {
            let outcome = if waiter_changed {
                self.prompts
                    .generation(conversation_id, record.generation_id())
                    .commit_recovered_prompt(transaction, &record, deadline)
                    .await
            } else {
                let cleanup = transaction.rollback().await.map_err(|error| {
                    KitError::from_state_store(error, "prompt reconciliation could not roll back")
                        .at_phase(ErrorPhase::Cleanup)
                });
                if tokio::time::Instant::now() >= deadline {
                    return Err(KitError::timeout("prompt reconciliation deadline elapsed")
                        .with_cleanup_result(cleanup));
                }
                cleanup
            };
            outcome?;
            if tokio::time::Instant::now() >= deadline {
                return Err(KitError::timeout("prompt reconciliation deadline elapsed"));
            }
            return Err(KitError::busy(
                "prompt remains recovery-owned during reopen",
            ));
        }
        if let Err(error) = record.mark_admitted() {
            let primary = error;
            let rollback = transaction.rollback().await;
            return Err(match rollback {
                Ok(()) => primary,
                Err(secondary) => primary.with_supplementary(
                    KitError::from_state_store(secondary, "transaction rollback failed")
                        .at_phase(ErrorPhase::Cleanup),
                ),
            });
        }
        if record.cancellation_recovery_owned() {
            let outcome = self
                .prompts
                .generation(conversation_id, record.generation_id())
                .commit_recovered_prompt(transaction, &record, deadline)
                .await;
            outcome?;
            if tokio::time::Instant::now() >= deadline {
                return Err(KitError::timeout("prompt reconciliation deadline elapsed"));
            }
            return Err(KitError::busy("prompt cancellation remains recovery-owned"));
        }
        let generation_id = record.generation_id().to_owned();
        if let Err(error) = record.begin_cancellation() {
            let primary = error;
            let rollback = transaction.rollback().await;
            return Err(match rollback {
                Ok(()) => primary,
                Err(secondary) => primary.with_supplementary(
                    KitError::from_state_store(secondary, "transaction rollback failed")
                        .at_phase(ErrorPhase::Cleanup),
                ),
            });
        }
        let outcome = self
            .prompts
            .generation(conversation_id, record.generation_id())
            .commit_recovered_prompt(transaction, &record, deadline)
            .await;
        outcome?;
        if tokio::time::Instant::now() >= deadline {
            return Err(KitError::timeout("prompt reconciliation deadline elapsed"));
        }
        Ok(Some(generation_id))
    }

    fn registered_abort_target(
        &self,
        conversation_id: &ConversationId,
    ) -> Result<Option<OpenCodeAbort<S>>, KitError> {
        self.prompts.registered_abort(conversation_id)
    }

    async fn unregistered_abort_target(
        &self,
        conversation_id: &ConversationId,
    ) -> Result<OpenCodeAbort<S>, KitError> {
        let snapshot = self
            .storage
            .store()
            .load(OpenCodePromptRecord::key(conversation_id)?)
            .await
            .map_err(|error| {
                KitError::from_state_store(error, "prompt state could not be loaded")
                    .in_phase(ErrorPhase::StateRead)
            })?;
        let durable_generation_id = snapshot
            .map(|snapshot| OpenCodePromptRecord::decode(snapshot, conversation_id))
            .transpose()?
            .map(|record| record.generation_id().to_owned());
        self.prompts
            .register_recovered_abort(conversation_id, durable_generation_id)
    }

    pub(crate) async fn request_abort_before(
        &self,
        conversation_id: &ConversationId,
        deadline: tokio::time::Instant,
    ) -> Result<OpenCodeAbort<S>, KitError> {
        let selected = match self.registered_abort_target(conversation_id)? {
            Some(target) => target,
            None => loop {
                match self.unregistered_abort_target(conversation_id).await {
                    Ok(target) => break target,
                    Err(error)
                        if Self::retryable_state_error(&error)
                            && tokio::time::Instant::now() < deadline =>
                    {
                        if let Some(target) = self.registered_abort_target(conversation_id)? {
                            break target;
                        }
                        tokio::time::sleep_until(
                            deadline.min(tokio::time::Instant::now() + Duration::from_millis(25)),
                        )
                        .await;
                    }
                    Err(error) if Self::retryable_state_error(&error) => {
                        return Err(KitError::timeout("prompt cancellation deadline elapsed")
                            .at_phase(ErrorPhase::Reconciliation)
                            .with_supplementary(error));
                    }
                    Err(error) => return Err(error),
                }
            },
        };
        let OpenCodeAbort::Join(join) = selected else {
            return Ok(OpenCodeAbort::Idle);
        };
        join.generation().request_abort(deadline).await?;
        Ok(OpenCodeAbort::Join(join))
    }

    pub(crate) async fn continuation(
        &self,
        conversation_id: &ConversationId,
        id: &NotificationId,
    ) -> Result<OpenCodeContinuation, KitError> {
        let mut matches = self
            .prompts
            .continuations(conversation_id)
            .await?
            .into_iter()
            .filter(|continuation| continuation.notification_id() == id);
        let continuation = matches
            .next()
            .ok_or_else(|| KitError::not_found("notification continuation does not exist"))?;
        if matches.next().is_some() {
            return Err(KitError::invalid_state(
                "notification continuation identity is duplicated",
            ));
        }
        if continuation.state() != OpenCodeContinuationState::Pending {
            return Err(KitError::conflict(
                "notification interaction is already settled or recovery-owned",
            ));
        }
        Ok(continuation)
    }

    pub(super) async fn reconcile_continuations(
        &self,
        agent_id: &AgentId,
        native_pending: &BTreeSet<(String, String)>,
        closing: Option<&ConversationId>,
        deadline: tokio::time::Instant,
    ) -> Result<(), OpenCodeTransactionError> {
        let durable = tokio::time::timeout_at(deadline, async {
            let bindings = self.bindings_for_agent(agent_id).await?;
            let mut durable = Vec::new();
            let mut durable_native = BTreeSet::new();
            for binding in bindings {
                if closing.is_some_and(|conversation| conversation != binding.conversation_id()) {
                    continue;
                }
                for continuation in self
                    .prompts
                    .continuations(binding.conversation_id())
                    .await?
                {
                    let native_identity = (
                        continuation.session_id().to_owned(),
                        continuation.request_id().to_owned(),
                    );
                    if !durable_native.insert(native_identity) {
                        return Err(KitError::invalid_state(
                            "native continuation ownership is duplicated",
                        ));
                    }
                    durable.push(continuation);
                }
            }
            Ok::<_, KitError>(durable)
        })
        .await
        .map_err(|_| KitError::timeout("event reconciliation deadline elapsed"))??;

        for continuation in durable {
            if tokio::time::Instant::now() >= deadline {
                return Err(KitError::timeout("event reconciliation deadline elapsed").into());
            }
            let key = OpenCodeContinuation::key(
                continuation.conversation_id(),
                continuation.request_id(),
            )?;
            let native_identity = (
                continuation.session_id().to_owned(),
                continuation.request_id().to_owned(),
            );
            if native_pending.contains(&native_identity) {
                if !continuation.is_unresolved() {
                    return Err(KitError::invalid_state(
                        "settled interaction returned to the native pending inventory",
                    )
                    .into());
                }
                continue;
            }
            if continuation.state() == OpenCodeContinuationState::CancellationRequired {
                let binding = self
                    .binding_for_session(agent_id, continuation.session_id())
                    .await?
                    .ok_or_else(|| KitError::conflict("interaction cleanup binding is absent"))?;
                self.settle_native_cancellation(
                    &binding,
                    std::slice::from_ref(&continuation),
                    deadline,
                )
                .await?;
                let _ = self
                    .reconcile_terminal_prompt(&continuation, deadline)
                    .await
                    .map_err(OpenCodeTransactionError::TerminalCleanup)?;
                continue;
            }
            if continuation.cancellation_owner().is_some() {
                return Err(KitError::integration(
                    "native interaction cancellation requires acknowledged outcome",
                )
                .at_phase(ErrorPhase::Reconciliation)
                .into());
            }
            if !continuation.is_unresolved() {
                let _ = self
                    .finish_continuation_reconciliation(&continuation)
                    .await?;
                if tokio::time::Instant::now() >= deadline {
                    return Err(KitError::timeout("event reconciliation deadline elapsed").into());
                }
                let _ = self
                    .reconcile_terminal_prompt(&continuation, deadline)
                    .await
                    .map_err(OpenCodeTransactionError::TerminalCleanup)?;
                if tokio::time::Instant::now() >= deadline {
                    return Err(KitError::timeout("event reconciliation deadline elapsed").into());
                }
                continue;
            }
            if continuation.state() == OpenCodeContinuationState::Ambiguous {
                continue;
            }

            let store = self.storage.store();
            let mut transaction = tokio::time::timeout_at(deadline, store.begin_transaction())
                .await
                .map_err(|_| KitError::timeout("event reconciliation deadline elapsed"))?
                .map_err(|error| {
                    KitError::from_state_store(error, "continuation reconciliation could not begin")
                        .in_phase(ErrorPhase::StateWrite)
                })?;
            let cancelled = continuation.with_state(OpenCodeContinuationState::Cancelled)?;
            let mutation = async {
                let snapshot = transaction
                    .load(key.clone())
                    .await
                    .map_err(|error| {
                        KitError::from_state_store(error, "continuation could not be loaded")
                            .in_phase(ErrorPhase::StateRead)
                    })?
                    .ok_or_else(|| KitError::invalid_state("continuation is missing"))?;
                if OpenCodeContinuation::decode(snapshot, continuation.conversation_id())?
                    != continuation
                {
                    return Err(KitError::invalid_state(
                        "continuation changed during reconciliation",
                    ));
                }
                let notification_reconciliation =
                    NotificationStore::<S>::reconcile_native_resolution_in_transaction(
                        &mut transaction,
                        continuation.conversation_id(),
                        continuation.notification_id(),
                        ResolutionSettlement::Cancelled,
                    )
                    .await
                    .map_err(KitError::from)?;
                transaction
                    .put(storage_kit::state::record::StateRecord::new(
                        key.clone(),
                        serde_json::to_value(&cancelled).map_err(|_| {
                            KitError::invalid_state("continuation could not be encoded")
                        })?,
                    ))
                    .await
                    .map_err(|error| {
                        KitError::from_state_store(error, "lost continuation could not be stored")
                            .in_phase(ErrorPhase::StateWrite)
                    })?;
                Ok::<_, KitError>(notification_reconciliation)
            }
            .await;
            let notification_reconciliation = match mutation {
                Ok(reconciliation) => reconciliation,
                Err(error) => {
                    let rollback = transaction.rollback().await;
                    return Err((match rollback {
                        Ok(()) => error,
                        Err(secondary) => error.with_supplementary(
                            KitError::from_state_store(secondary, "transaction rollback failed")
                                .at_phase(ErrorPhase::Cleanup),
                        ),
                    })
                    .into());
                }
            };
            let commit = transaction.commit().await;
            if matches!(
                &commit,
                Err(storage_kit::StateStoreError::TransactionConflict { .. })
            ) {
                return Err(KitError::conflict("continuation reconciliation conflicted").into());
            }
            if commit.is_err() {
                let (continuation_expected, continuation_unchanged, notification) = loop {
                    let observation = async {
                        Ok::<_, KitError>((
                            self.prompts.continuation_matches(&cancelled).await?,
                            self.prompts.continuation_matches(&continuation).await?,
                            notification_reconciliation.observe(store).await?,
                        ))
                    }
                    .await;
                    match observation {
                        Ok(observation) => break observation,
                        Err(error) if Self::retryable_state_error(&error) => {
                            tokio::time::sleep(Duration::from_millis(25)).await;
                        }
                        Err(error) => return Err(error.into()),
                    }
                };
                let notification_expected =
                    notification == NotificationMutationObservation::Expected;
                let notification_unchanged = notification
                    == NotificationMutationObservation::Unchanged
                    || (!notification_reconciliation.changes_state() && notification_expected);
                if !(continuation_expected && notification_expected) {
                    if continuation_unchanged && notification_unchanged {
                        return Err(KitError::integration(
                            "continuation reconciliation could not commit",
                        )
                        .into());
                    }
                    return Err(KitError::invalid_state(
                        "continuation reconciliation outcome is contradictory",
                    )
                    .into());
                }
            }
            let _ = self.finish_continuation_reconciliation(&cancelled).await?;
            if tokio::time::Instant::now() >= deadline {
                return Err(KitError::timeout("event reconciliation deadline elapsed").into());
            }
            let _ = self
                .reconcile_terminal_prompt(&continuation, deadline)
                .await
                .map_err(OpenCodeTransactionError::TerminalCleanup)?;
            if tokio::time::Instant::now() >= deadline {
                return Err(KitError::timeout("event reconciliation deadline elapsed").into());
            }
        }
        Ok(())
    }

    pub(crate) async fn settle_interaction(
        &self,
        intent: &ResolutionIntent,
        continuation: &OpenCodeContinuation,
        settlement: ResolutionSettlement,
        deadline: tokio::time::Instant,
    ) -> Result<ResolutionSettlement, KitError> {
        if continuation.conversation_id() != intent.conversation_id()
            || continuation.notification_id() != intent.notification_id()
        {
            return Err(KitError::invalid_state(
                "resolution intent contradicts its continuation",
            ));
        }
        let store = self.storage.store();
        let actual = loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(KitError::timeout("interaction settlement deadline elapsed"));
            }
            let result = async {
                let mut transaction = store.begin_transaction().await.map_err(|error| {
                    NotificationSettlementError::storage(
                        error,
                        "interaction settlement could not begin",
                    )
                    .in_phase(crate::error::ErrorPhase::StateWrite)
                })?;
                let continuation_key =
                    OpenCodeContinuation::key(intent.conversation_id(), continuation.request_id())?;
                let snapshot = transaction
                    .load(continuation_key.clone())
                    .await
                    .map_err(|error| {
                        NotificationSettlementError::storage(
                            error,
                            "continuation could not be loaded",
                        )
                        .in_phase(crate::error::ErrorPhase::StateRead)
                    })?
                    .ok_or_else(|| KitError::invalid_state("continuation is missing"))?;
                let actual = OpenCodeContinuation::decode(snapshot, intent.conversation_id())?;
                if !actual.has_same_request(continuation) {
                    return Err(
                        KitError::invalid_state("continuation changed during resolution").into(),
                    );
                }
                if let Some(settlement) = Self::terminal_settlement(actual.state()) {
                    return Ok(settlement);
                }
                if actual.state() == OpenCodeContinuationState::Ambiguous {
                    return Ok(ResolutionSettlement::Ambiguous);
                }
                let next = actual.with_state(Self::continuation_state(settlement))?;
                transaction
                    .put(storage_kit::state::record::StateRecord::new(
                        continuation_key,
                        serde_json::to_value(&next).map_err(|_| {
                            KitError::invalid_state("continuation could not be encoded")
                        })?,
                    ))
                    .await
                    .map_err(|error| {
                        NotificationSettlementError::storage(
                            error,
                            "continuation could not be stored",
                        )
                        .in_phase(crate::error::ErrorPhase::StateWrite)
                    })?;
                let disposition = NotificationStore::<S>::settle_resolution_in_transaction(
                    &mut transaction,
                    intent,
                    settlement,
                )
                .await?;
                let actual = match disposition {
                    ResolutionDisposition::Applied(actual) => actual,
                    ResolutionDisposition::AlreadyAccepted => {
                        return Err(KitError::invalid_state(
                            "continuation and interaction settlement disagree",
                        )
                        .into());
                    }
                };
                match transaction.commit().await {
                    Ok(()) => {}
                    Err(storage_kit::StateStoreError::TransactionConflict { .. }) => {
                        return Err(NotificationSettlementError::TransactionConflict);
                    }
                    Err(error) => {
                        let commit_error = KitError::from_state_store(
                            error,
                            "interaction settlement could not commit",
                        )
                        .in_phase(ErrorPhase::StateCommit);
                        let exact_commit = if actual == ResolutionSettlement::Ambiguous {
                            self.prompts.continuation_matches(&next).await
                        } else {
                            self.prompts
                                .continuation_reconciliation_matches(&next)
                                .await
                        }
                        .map_err(|error| error.with_supplementary(commit_error.clone()))?;
                        if !exact_commit {
                            return Err(commit_error.into());
                        }
                    }
                }
                Ok(actual)
            }
            .await;
            match result {
                Ok(actual) => break actual,
                Err(NotificationSettlementError::TransactionConflict) => {
                    tokio::task::yield_now().await;
                }
                Err(NotificationSettlementError::Failure(error)) => return Err(error),
            }
        };
        if actual == ResolutionSettlement::Ambiguous {
            self.notifications.wake_receivers();
        } else {
            self.notifications
                .finish_reconciliation(intent.conversation_id());
            let _ = self
                .reclaim_terminal_notification(intent.conversation_id())
                .await;
            let _ = self.reconcile_terminal_prompt(continuation, deadline).await;
        }
        Ok(actual)
    }

    pub(crate) async fn event_high_water(
        &self,
        agent_id: &AgentId,
    ) -> Result<std::collections::BTreeMap<String, u64>, KitError> {
        let cleanup_aggregates = self
            .cleanup_obligations(agent_id)
            .await?
            .into_iter()
            .flat_map(|cleanup| cleanup.session_ids().to_vec())
            .collect::<BTreeSet<_>>();
        let store = self.storage.store();
        let snapshot = store
            .load(Self::event_progress_key(agent_id)?)
            .await
            .map_err(|error| {
                KitError::from_state_store(error, "event progress could not be loaded")
                    .in_phase(ErrorPhase::StateRead)
            })?
            .ok_or_else(|| KitError::invalid_state("event progress is missing"))?;
        let progress: OpenCodeEventProgress =
            serde::Deserialize::deserialize(snapshot.record.value.as_ref())
                .map_err(|_| KitError::invalid_state("stored event progress is malformed"))?;
        progress.validate(agent_id)?;
        Ok(progress
            .high_water()
            .iter()
            .filter(|(aggregate, _)| !cleanup_aggregates.contains(*aggregate))
            .map(|(aggregate, sequence)| (aggregate.clone(), *sequence))
            .collect())
    }

    pub(crate) async fn retired_sessions(
        &self,
        agent_id: &AgentId,
    ) -> Result<BTreeSet<String>, KitError> {
        let store = self.storage.store();
        let snapshot = store
            .load(Self::event_progress_key(agent_id)?)
            .await
            .map_err(|error| {
                KitError::from_state_store(error, "event progress could not be loaded")
                    .in_phase(ErrorPhase::StateRead)
            })?
            .ok_or_else(|| KitError::invalid_state("event progress is missing"))?;
        let progress: OpenCodeEventProgress =
            serde::Deserialize::deserialize(snapshot.record.value.as_ref())
                .map_err(|_| KitError::invalid_state("stored event progress is malformed"))?;
        progress.validate(agent_id)?;
        Ok(progress.retired_sessions().clone())
    }

    pub(crate) async fn reconcile_pending_interaction(
        &self,
        effect: OpenCodeEventEffect,
        deadline: tokio::time::Instant,
    ) -> Result<(), KitError> {
        let (conversation_id, kind) = match &effect {
            OpenCodeEventEffect::Interaction {
                conversation_id,
                kind,
                ..
            } => (conversation_id.clone(), kind.clone()),
            _ => {
                return Err(KitError::invalid_state(
                    "pending reconciliation received a non-interaction fact",
                ));
            }
        };
        let store = self.storage.store();
        let mut transaction = tokio::time::timeout_at(deadline, store.begin_transaction())
            .await
            .map_err(|_| KitError::timeout("event reconciliation deadline elapsed"))?
            .map_err(|error| {
                KitError::from_state_store(error, "interaction transaction could not begin")
                    .in_phase(ErrorPhase::StateWrite)
            })?;
        let mutation = self
            .apply_interaction_in_transaction(&mut transaction, effect)
            .await;
        let (wake, prompt_reconciliation, continuation, notification_reconciliation) =
            match mutation {
                Ok(mutation) => mutation,
                Err(OpenCodeTransactionError::TransactionConflict(error)) => {
                    let _ = transaction.rollback().await;
                    return Err(KitError::conflict("interaction reconciliation conflicted")
                        .with_detail(crate::error::ErrorDetail::TransactionConflict)
                        .with_cause(error));
                }
                Err(
                    OpenCodeTransactionError::Failure(error)
                    | OpenCodeTransactionError::TerminalCleanup(error),
                ) => {
                    let rollback = transaction.rollback().await;
                    return Err(match rollback {
                        Ok(()) => error,
                        Err(secondary) => error.with_supplementary(
                            KitError::from_state_store(secondary, "transaction rollback failed")
                                .at_phase(ErrorPhase::Cleanup),
                        ),
                    });
                }
            };
        let commit = transaction.commit().await;
        if matches!(
            &commit,
            Err(storage_kit::StateStoreError::TransactionConflict { .. })
        ) {
            return Err(KitError::conflict("interaction reconciliation conflicted"));
        }
        if commit.is_err() {
            let (
                continuation_expected,
                continuation_unchanged,
                notification_expected,
                notification_unchanged,
                prompt_expected,
                prompt_unchanged,
            ) = loop {
                let observation = async {
                    let continuation_expected =
                        self.prompts.continuation_matches(&continuation).await?;
                    let continuation_unchanged = store
                        .load(continuation.state_key()?)
                        .await
                        .map_err(|error| {
                            KitError::from_state_store(
                                error,
                                "interaction commit outcome could not be checked",
                            )
                            .in_phase(ErrorPhase::StateRead)
                        })?
                        .is_none();
                    let (notification_expected, notification_unchanged) =
                        if let Some(reconciliation) = &notification_reconciliation {
                            let observation = reconciliation.observe(store).await?;
                            (
                                observation == NotificationMutationObservation::Expected,
                                observation == NotificationMutationObservation::Unchanged,
                            )
                        } else {
                            let mut observation =
                                store.begin_transaction().await.map_err(|error| {
                                    KitError::from_state_store(
                                        error,
                                        "interaction commit outcome could not be checked",
                                    )
                                    .in_phase(ErrorPhase::StateRead)
                                })?;
                            let matches =
                                NotificationStore::<S>::ensure_interaction_matches_in_transaction(
                                    &mut observation,
                                    &conversation_id,
                                    continuation.notification_id(),
                                    &kind,
                                )
                                .await;
                            let _ = observation.rollback().await;
                            matches.map_err(NotificationInteractionCheckError::into_match_sdk)?;
                            (true, true)
                        };
                    let (prompt_expected, prompt_unchanged) =
                        if let Some((before, expected)) = &prompt_reconciliation {
                            let snapshot = store
                                .load(OpenCodePromptRecord::key(&conversation_id)?)
                                .await
                                .map_err(|error| {
                                    KitError::from_state_store(
                                        error,
                                        "interaction commit outcome could not be checked",
                                    )
                                    .in_phase(ErrorPhase::StateRead)
                                })?
                                .ok_or_else(|| {
                                    KitError::invalid_state(
                                        "interaction reconciliation outcome is contradictory",
                                    )
                                })?;
                            let prompt = OpenCodePromptRecord::decode(snapshot, &conversation_id)?;
                            (prompt == *expected, prompt == *before)
                        } else {
                            (true, true)
                        };
                    Ok::<_, KitError>((
                        continuation_expected,
                        continuation_unchanged,
                        notification_expected,
                        notification_unchanged,
                        prompt_expected,
                        prompt_unchanged,
                    ))
                }
                .await;
                match observation {
                    Ok(observation) => break observation,
                    Err(error) if Self::retryable_state_error(&error) => {
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                    Err(error) => return Err(error),
                }
            };
            if !(continuation_expected && notification_expected && prompt_expected) {
                if continuation_unchanged && notification_unchanged && prompt_unchanged {
                    return Err(KitError::integration(
                        "interaction transaction could not commit",
                    ));
                }
                return Err(KitError::invalid_state(
                    "interaction reconciliation outcome is contradictory",
                ));
            }
        }
        if wake {
            self.notifications.wake_receivers();
        }
        if prompt_reconciliation.is_some() {
            self.prompts.mark_changed();
        }
        if tokio::time::Instant::now() >= deadline {
            Err(KitError::timeout("event reconciliation deadline elapsed"))
        } else {
            Ok(())
        }
    }

    pub(super) async fn apply_event(
        &self,
        agent_id: &AgentId,
        event: OpenCodeEventIdentity,
        effect: OpenCodeEventEffect,
        deadline: tokio::time::Instant,
    ) -> Result<OpenCodeEventDisposition, OpenCodeTransactionError> {
        let key = Self::event_progress_key(agent_id)?;
        let store = self.storage.store();
        let mut deletion_continuations = match &effect {
            OpenCodeEventEffect::ConversationDeleted {
                conversation_id, ..
            } => self.prompts.continuations(conversation_id).await?,
            OpenCodeEventEffect::None
            | OpenCodeEventEffect::CatalogInvalidated
            | OpenCodeEventEffect::Passive { .. }
            | OpenCodeEventEffect::Process { .. }
            | OpenCodeEventEffect::PromptStatus { .. }
            | OpenCodeEventEffect::PromptMessage { .. }
            | OpenCodeEventEffect::PromptPart { .. }
            | OpenCodeEventEffect::InteractionResolved { .. }
            | OpenCodeEventEffect::Interaction { .. } => Vec::new(),
        };
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            OpenCodeTransactionError::storage(
                error,
                "event transaction could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        let snapshot = transaction
            .load(key.clone())
            .await
            .map_err(|error| {
                OpenCodeTransactionError::storage(
                    error,
                    "event progress could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| KitError::invalid_state("event progress is missing"))?;
        let mut progress: OpenCodeEventProgress =
            serde::Deserialize::deserialize(snapshot.record.value.as_ref())
                .map_err(|_| KitError::invalid_state("stored event progress is malformed"))?;
        progress.validate(agent_id)?;
        let mut committed_event = event;
        let disposition = progress.observe(&mut committed_event)?;
        if disposition == OpenCodeEventDisposition::Gap {
            return Ok(disposition);
        }

        let mut wake = false;
        let mut prompt_changed = false;
        let conversation_deleted = match &effect {
            OpenCodeEventEffect::ConversationDeleted {
                conversation_id, ..
            } if matches!(
                disposition,
                OpenCodeEventDisposition::Applied | OpenCodeEventDisposition::RememberedDuplicate
            ) =>
            {
                Some(conversation_id.clone())
            }
            _ => None,
        };
        let resolved_event = match &effect {
            OpenCodeEventEffect::InteractionResolved {
                conversation_id,
                session_id,
                request_id,
                kind,
                answer_count,
                outcome,
            } if matches!(
                disposition,
                OpenCodeEventDisposition::Applied | OpenCodeEventDisposition::RememberedDuplicate
            ) =>
            {
                Some((
                    conversation_id.clone(),
                    session_id.clone(),
                    request_id.clone(),
                    *kind,
                    *answer_count,
                    *outcome,
                ))
            }
            _ => None,
        };
        let mut resolved_continuation = None;
        let prompt_status = match &effect {
            OpenCodeEventEffect::PromptStatus {
                conversation_id,
                status,
            } if disposition == OpenCodeEventDisposition::Applied => Some((
                conversation_id.clone(),
                OpenCodePromptStatusSignal::Observed(*status),
            )),
            OpenCodeEventEffect::PromptStatus {
                conversation_id, ..
            } if disposition == OpenCodeEventDisposition::RememberedDuplicate => {
                Some((conversation_id.clone(), OpenCodePromptStatusSignal::Refresh))
            }
            _ => None,
        };
        if disposition == OpenCodeEventDisposition::Applied {
            match effect {
                OpenCodeEventEffect::None => {}
                OpenCodeEventEffect::CatalogInvalidated => {
                    let mut catalog = Self::catalog_in_transaction(&mut transaction, agent_id)
                        .await
                        .map_err(OpenCodeTransactionError::nested)?;
                    catalog.invalidate_all()?;
                    Self::store_catalog_in_transaction(&mut transaction, agent_id, catalog)
                        .await
                        .map_err(OpenCodeTransactionError::nested)?;
                }
                OpenCodeEventEffect::Passive {
                    conversation_id,
                    kinds,
                } => {
                    for kind in kinds {
                        NotificationStore::<S>::publish_in_transaction(
                            &mut transaction,
                            &conversation_id,
                            kind,
                        )
                        .await
                        .map_err(OpenCodeTransactionError::nested)?;
                        wake = true;
                    }
                }
                OpenCodeEventEffect::Process {
                    conversation_id,
                    process,
                    kind,
                } => {
                    if progress.observe_process(&process)? {
                        NotificationStore::<S>::publish_in_transaction(
                            &mut transaction,
                            &conversation_id,
                            kind,
                        )
                        .await
                        .map_err(OpenCodeTransactionError::nested)?;
                        wake = true;
                    }
                }
                OpenCodeEventEffect::PromptStatus {
                    conversation_id,
                    status,
                } => {
                    if status == OpenCodePromptStatus::Active {
                        let prompt_key = OpenCodePromptRecord::key(&conversation_id)?;
                        if let Some(snapshot) =
                            transaction
                                .load(prompt_key.clone())
                                .await
                                .map_err(|error| {
                                    OpenCodeTransactionError::storage(
                                        error,
                                        "prompt admission state could not be loaded",
                                        ErrorPhase::StateRead,
                                    )
                                })?
                        {
                            let mut record =
                                OpenCodePromptRecord::decode(snapshot, &conversation_id)?;
                            if record.observe_active_status()? {
                                transaction
                                    .put(storage_kit::state::record::StateRecord::new(
                                        prompt_key,
                                        serde_json::to_value(record).map_err(|_| {
                                            KitError::invalid_state(
                                                "prompt admission could not be encoded",
                                            )
                                        })?,
                                    ))
                                    .await
                                    .map_err(|error| {
                                        OpenCodeTransactionError::storage(
                                            error,
                                            "prompt admission could not be stored",
                                            ErrorPhase::StateWrite,
                                        )
                                    })?;
                                prompt_changed = true;
                            }
                        }
                    }
                }
                OpenCodeEventEffect::PromptMessage {
                    conversation_id,
                    session_id,
                    message_id,
                } => {
                    let prompt_key = OpenCodePromptRecord::key(&conversation_id)?;
                    if let Some(snapshot) =
                        transaction
                            .load(prompt_key.clone())
                            .await
                            .map_err(|error| {
                                OpenCodeTransactionError::storage(
                                    error,
                                    "prompt message state could not be loaded",
                                    ErrorPhase::StateRead,
                                )
                            })?
                    {
                        let mut record = OpenCodePromptRecord::decode(snapshot, &conversation_id)?;
                        if record.observe_prompt_message(&session_id, &message_id)? {
                            transaction
                                .put(storage_kit::state::record::StateRecord::new(
                                    prompt_key,
                                    serde_json::to_value(record).map_err(|_| {
                                        KitError::invalid_state(
                                            "prompt message could not be encoded",
                                        )
                                    })?,
                                ))
                                .await
                                .map_err(|error| {
                                    OpenCodeTransactionError::storage(
                                        error,
                                        "prompt message could not be stored",
                                        ErrorPhase::StateWrite,
                                    )
                                })?;
                            prompt_changed = true;
                        }
                    }
                }
                OpenCodeEventEffect::PromptPart {
                    conversation_id,
                    session_id,
                    message_id,
                    part_id,
                } => {
                    let prompt_key = OpenCodePromptRecord::key(&conversation_id)?;
                    if let Some(snapshot) =
                        transaction
                            .load(prompt_key.clone())
                            .await
                            .map_err(|error| {
                                OpenCodeTransactionError::storage(
                                    error,
                                    "prompt input state could not be loaded",
                                    ErrorPhase::StateRead,
                                )
                            })?
                    {
                        let mut record = OpenCodePromptRecord::decode(snapshot, &conversation_id)?;
                        if record.observe_input_part(&session_id, &message_id, &part_id)? {
                            transaction
                                .put(storage_kit::state::record::StateRecord::new(
                                    prompt_key,
                                    serde_json::to_value(record).map_err(|_| {
                                        KitError::invalid_state("prompt input could not be encoded")
                                    })?,
                                ))
                                .await
                                .map_err(|error| {
                                    OpenCodeTransactionError::storage(
                                        error,
                                        "prompt input could not be stored",
                                        ErrorPhase::StateWrite,
                                    )
                                })?;
                            prompt_changed = true;
                        }
                    }
                }
                OpenCodeEventEffect::ConversationDeleted {
                    conversation_id,
                    workspace_id,
                    session_id,
                } => {
                    let workspace = transaction
                        .load(WorkspaceRecord::key(agent_id, &workspace_id)?)
                        .await
                        .map_err(|error| {
                            OpenCodeTransactionError::storage(
                                error,
                                "deleted session workspace could not be loaded",
                                ErrorPhase::StateRead,
                            )
                        })?;
                    if let Some(workspace) = workspace {
                        let workspace =
                            WorkspaceRecord::decode(workspace, agent_id, Some(&workspace_id))?;
                        let conversation_key =
                            ConversationRecord::key(&workspace_id, &conversation_id)?;
                        let conversation = transaction
                            .load(conversation_key.clone())
                            .await
                            .map_err(|error| {
                                OpenCodeTransactionError::storage(
                                    error,
                                    "deleted conversation could not be loaded",
                                    ErrorPhase::StateRead,
                                )
                            })?;
                        let binding_key = Self::binding_key(&workspace_id, &conversation_id)?;
                        let binding =
                            transaction
                                .load(binding_key.clone())
                                .await
                                .map_err(|error| {
                                    OpenCodeTransactionError::storage(
                                        error,
                                        "deleted session binding could not be loaded",
                                        ErrorPhase::StateRead,
                                    )
                                })?;
                        match (conversation, binding) {
                            (None, None) => {}
                            (Some(conversation), Some(binding)) => {
                                let conversation = ConversationRecord::decode(
                                    conversation,
                                    &workspace_id,
                                    Some(&conversation_id),
                                )?;
                                let binding =
                                    Self::decode_binding(binding, &workspace_id, &conversation_id)?;
                                if binding.session_id() != Some(session_id.as_str()) {
                                    return Err(KitError::protocol(
                                        "deleted session contradicts its durable binding",
                                    )
                                    .into());
                                }
                                let routing = Self::workspace_routing_in_transaction(
                                    &mut transaction,
                                    &workspace,
                                )
                                .await?;
                                binding.verify_owner(&conversation, &workspace, &routing)?;

                                for continuation in std::mem::take(&mut deletion_continuations) {
                                    let key = OpenCodeContinuation::key(
                                        continuation.conversation_id(),
                                        continuation.request_id(),
                                    )?;
                                    if Self::terminal_settlement(continuation.state()).is_none() {
                                        let settlement = if continuation.state()
                                            == OpenCodeContinuationState::Ambiguous
                                        {
                                            ResolutionSettlement::Accepted
                                        } else {
                                            ResolutionSettlement::Cancelled
                                        };
                                        NotificationStore::<S>::reconcile_native_resolution_in_transaction(
                                        &mut transaction,
                                        &conversation_id,
                                        continuation.notification_id(),
                                        settlement,
                                    )
                                    .await?;
                                    }
                                    transaction.delete(key).await.map_err(|error| {
                                        OpenCodeTransactionError::storage(
                                            error,
                                            "deleted continuation could not be removed",
                                            ErrorPhase::StateWrite,
                                        )
                                    })?;
                                }
                                let prompt_key = OpenCodePromptRecord::key(&conversation_id)?;
                                if transaction
                                    .load(prompt_key.clone())
                                    .await
                                    .map_err(|error| {
                                        OpenCodeTransactionError::storage(
                                            error,
                                            "deleted prompt could not be checked",
                                            ErrorPhase::StateRead,
                                        )
                                    })?
                                    .is_some()
                                {
                                    transaction.delete(prompt_key).await.map_err(|error| {
                                        OpenCodeTransactionError::storage(
                                            error,
                                            "deleted prompt could not be removed",
                                            ErrorPhase::StateWrite,
                                        )
                                    })?;
                                }
                                transaction
                                    .delete(conversation_key)
                                    .await
                                    .map_err(|error| {
                                        OpenCodeTransactionError::storage(
                                            error,
                                            "deleted conversation could not be removed",
                                            ErrorPhase::StateWrite,
                                        )
                                    })?;
                                transaction.delete(binding_key).await.map_err(|error| {
                                    OpenCodeTransactionError::storage(
                                        error,
                                        "deleted session binding could not be removed",
                                        ErrorPhase::StateWrite,
                                    )
                                })?;
                                match NotificationStore::<S>::delete_in_transaction(
                                    &mut transaction,
                                    &conversation_id,
                                )
                                .await
                                {
                                    Ok(()) => {}
                                    Err(NotificationDeleteError::TransactionConflict) => {
                                        return Err(OpenCodeTransactionError::TransactionConflict(
                                            KitError::conflict(
                                                "notification state transaction conflicted",
                                            ),
                                        ));
                                    }
                                    Err(NotificationDeleteError::Failure(error)) => {
                                        return Err(OpenCodeTransactionError::Failure(error));
                                    }
                                }
                            }
                            (Some(_), None) | (None, Some(_)) => {
                                return Err(KitError::invalid_state(
                                    "deleted conversation state is incomplete",
                                )
                                .into());
                            }
                        }
                    }
                }
                OpenCodeEventEffect::InteractionResolved {
                    conversation_id,
                    session_id,
                    request_id,
                    kind,
                    answer_count,
                    outcome,
                } => {
                    let continuation_key =
                        OpenCodeContinuation::key(&conversation_id, &request_id)?;
                    if let Some(snapshot) = transaction
                        .load(continuation_key.clone())
                        .await
                        .map_err(|error| {
                            OpenCodeTransactionError::storage(
                                error,
                                "continuation could not be checked",
                                ErrorPhase::StateRead,
                            )
                        })?
                    {
                        let continuation =
                            OpenCodeContinuation::decode(snapshot, &conversation_id)?;
                        if !continuation.matches_native_resolution(
                            &session_id,
                            &request_id,
                            kind,
                            answer_count,
                        ) {
                            return Err(KitError::protocol(
                                "interaction resolution contradicts its continuation",
                            )
                            .into());
                        }
                        if let Some(next) = continuation.native_resolution(outcome)? {
                            if !matches!(
                                continuation.state(),
                                OpenCodeContinuationState::CancellationRequired
                                    | OpenCodeContinuationState::CancellationDispatched { .. }
                            ) {
                                NotificationStore::<S>::reconcile_native_resolution_in_transaction(
                                    &mut transaction,
                                    &conversation_id,
                                    continuation.notification_id(),
                                    if next == OpenCodeContinuationState::Cancelled {
                                        ResolutionSettlement::Cancelled
                                    } else {
                                        ResolutionSettlement::Accepted
                                    },
                                )
                                .await?;
                            }
                            let accepted = continuation.with_state(next)?;
                            transaction
                                .put(storage_kit::state::record::StateRecord::new(
                                    continuation_key,
                                    serde_json::to_value(&accepted).map_err(|_| {
                                        KitError::invalid_state("continuation could not be encoded")
                                    })?,
                                ))
                                .await
                                .map_err(|error| {
                                    OpenCodeTransactionError::storage(
                                        error,
                                        "resolved continuation could not be stored",
                                        ErrorPhase::StateWrite,
                                    )
                                })?;
                            wake = true;
                            resolved_continuation = Some(accepted);
                        } else if matches!(
                            continuation.state(),
                            OpenCodeContinuationState::Accepted
                                | OpenCodeContinuationState::Cancelled
                        ) {
                            resolved_continuation = Some(continuation);
                        }
                    }
                }
                interaction @ OpenCodeEventEffect::Interaction { .. } => {
                    let (interaction_wake, interaction_prompt_reconciliation, _, _) = self
                        .apply_interaction_in_transaction(&mut transaction, interaction)
                        .await?;
                    wake = interaction_wake;
                    prompt_changed |= interaction_prompt_reconciliation.is_some();
                }
            }
        }
        if disposition == OpenCodeEventDisposition::RememberedDuplicate
            && let Some((conversation_id, session_id, request_id, kind, answer_count, outcome)) =
                &resolved_event
        {
            let continuation_key = OpenCodeContinuation::key(conversation_id, request_id)?;
            if let Some(snapshot) = transaction.load(continuation_key).await.map_err(|error| {
                OpenCodeTransactionError::storage(
                    error,
                    "continuation could not be checked",
                    ErrorPhase::StateRead,
                )
            })? {
                let continuation = OpenCodeContinuation::decode(snapshot, conversation_id)?;
                if !continuation.matches_native_resolution(
                    session_id,
                    request_id,
                    *kind,
                    *answer_count,
                ) {
                    return Err(KitError::protocol(
                        "interaction resolution contradicts its continuation",
                    )
                    .into());
                }
                continuation.native_resolution(*outcome)?;
                if matches!(
                    continuation.state(),
                    OpenCodeContinuationState::Accepted | OpenCodeContinuationState::Cancelled
                ) {
                    resolved_continuation = Some(continuation);
                }
            }
        }
        transaction
            .put(storage_kit::state::record::StateRecord::new(
                key.clone(),
                serde_json::to_value(&progress)
                    .map_err(|_| KitError::invalid_state("event progress could not be encoded"))?,
            ))
            .await
            .map_err(|error| {
                OpenCodeTransactionError::storage(
                    error,
                    "event progress could not be stored",
                    ErrorPhase::StateWrite,
                )
            })?;
        let commit = transaction.commit().await;
        if matches!(
            &commit,
            Err(storage_kit::StateStoreError::TransactionConflict { .. })
        ) {
            return Err(OpenCodeTransactionError::TransactionConflict(
                KitError::integration("event transaction could not commit"),
            ));
        }
        if commit.is_err() && disposition != OpenCodeEventDisposition::Duplicate {
            let snapshot = loop {
                match store.load(key.clone()).await {
                    Ok(Some(snapshot)) => break snapshot,
                    Ok(None) => {
                        return Err(
                            KitError::integration("event transaction could not commit").into()
                        );
                    }
                    Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
                }
            };
            let committed_progress: OpenCodeEventProgress =
                serde::Deserialize::deserialize(snapshot.record.value.as_ref())
                    .map_err(|_| KitError::invalid_state("stored event progress is malformed"))?;
            committed_progress.validate(agent_id)?;
            if !committed_progress.remembers_exact(&committed_event)? {
                return Err(KitError::integration("event transaction could not commit").into());
            }
        }
        if let Some(conversation_id) = &conversation_deleted {
            self.release_prompt_runtime(conversation_id);
        }
        if let Some(status) = prompt_status {
            self.prompts.publish_status(status);
        }
        if prompt_changed {
            self.prompts.mark_changed();
        }
        if resolved_continuation.is_none() && wake {
            self.notifications.wake_receivers();
        }
        if let Some(continuation) = resolved_continuation {
            let _ = self
                .finish_continuation_reconciliation(&continuation)
                .await?;
            let _ = self
                .reconcile_terminal_prompt(&continuation, deadline)
                .await
                .map_err(OpenCodeTransactionError::TerminalCleanup)?;
        }
        Ok(disposition)
    }

    async fn apply_interaction_in_transaction(
        &self,
        transaction: &mut S::Transaction,
        effect: OpenCodeEventEffect,
    ) -> Result<
        (
            bool,
            Option<(OpenCodePromptRecord, OpenCodePromptRecord)>,
            OpenCodeContinuation,
            Option<NotificationReconciliation>,
        ),
        OpenCodeTransactionError,
    > {
        let OpenCodeEventEffect::Interaction {
            conversation_id,
            kind,
            request,
        } = effect
        else {
            return Err(
                KitError::invalid_state("interaction mutation received another effect").into(),
            );
        };
        let prompt_key = OpenCodePromptRecord::key(&conversation_id)?;
        let mut generation_id = None;
        let prompt_reconciliation = if let Some(snapshot) = transaction
            .load(prompt_key.clone())
            .await
            .map_err(|error| {
                OpenCodeTransactionError::storage(
                    error,
                    "interaction prompt could not be checked",
                    ErrorPhase::StateRead,
                )
            })? {
            let before = OpenCodePromptRecord::decode(snapshot, &conversation_id)?;
            generation_id = Some(before.generation_id().to_owned());
            let mut record = before.clone();
            if record.observe_interaction_admission()? {
                let expected = record.clone();
                transaction
                    .put(storage_kit::state::record::StateRecord::new(
                        prompt_key,
                        serde_json::to_value(record).map_err(|_| {
                            KitError::invalid_state("interaction prompt could not be encoded")
                        })?,
                    ))
                    .await
                    .map_err(|error| {
                        OpenCodeTransactionError::storage(
                            error,
                            "interaction prompt could not be stored",
                            ErrorPhase::StateWrite,
                        )
                    })?;
                Some((before, expected))
            } else {
                None
            }
        } else {
            None
        };
        let continuation_key = OpenCodeContinuation::key(&conversation_id, request.id())?;
        if let Some(snapshot) =
            transaction
                .load(continuation_key.clone())
                .await
                .map_err(|error| {
                    OpenCodeTransactionError::storage(
                        error,
                        "continuation could not be checked",
                        ErrorPhase::StateRead,
                    )
                })?
        {
            let continuation = OpenCodeContinuation::decode(snapshot, &conversation_id)?;
            if continuation.request() != &request {
                return Err(KitError::protocol("native interaction identity was reused").into());
            }
            if continuation.state() != OpenCodeContinuationState::Pending {
                return Err(
                    KitError::protocol("interaction identity was reused after settlement").into(),
                );
            }
            NotificationStore::<S>::ensure_interaction_matches_in_transaction(
                transaction,
                &conversation_id,
                continuation.notification_id(),
                &kind,
            )
            .await?;
            return Ok((false, prompt_reconciliation, continuation, None));
        }

        let (id, notification_reconciliation) =
            NotificationStore::<S>::publish_in_transaction(transaction, &conversation_id, kind)
                .await
                .map_err(OpenCodeTransactionError::nested)?;
        let continuation = OpenCodeContinuation::new(conversation_id, id, request, generation_id)?;
        transaction
            .put(storage_kit::state::record::StateRecord::new(
                continuation_key,
                serde_json::to_value(&continuation)
                    .map_err(|_| KitError::invalid_state("continuation could not be encoded"))?,
            ))
            .await
            .map_err(|error| {
                OpenCodeTransactionError::storage(
                    error,
                    "continuation could not be stored",
                    ErrorPhase::StateWrite,
                )
            })?;
        Ok((
            true,
            prompt_reconciliation,
            continuation,
            Some(notification_reconciliation),
        ))
    }

    pub(crate) async fn create(
        &self,
        info: &AgentInfo,
        attachment: &OpenCodeAttachment,
        deadline: tokio::time::Instant,
    ) -> Result<(), KitError> {
        self.validate_attachment_owner(info, attachment)?;
        let key = Self::key(attachment.agent_id())?;
        let catalog_key = Self::catalog_key(attachment.agent_id())?;
        let attachment_value = serde_json::to_value(attachment)
            .map_err(|_| KitError::invalid_state("attachment could not be encoded"))?;
        let catalog_value = serde_json::to_value(OpenCodeCatalogState::new(info.id().clone()))
            .map_err(|_| KitError::invalid_state("catalog state could not be encoded"))?;
        let progress_key = Self::event_progress_key(info.id())?;
        let progress_value = serde_json::to_value(OpenCodeEventProgress::new(info.id().clone()))
            .map_err(|_| KitError::invalid_state("event progress could not be encoded"))?;
        let store = self.storage.store();
        if tokio::time::Instant::now() >= deadline {
            return Err(KitError::timeout("agent construction deadline elapsed"));
        }
        let mut transaction = match store.begin_transaction().await {
            Ok(transaction) => transaction,
            Err(error) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(KitError::timeout("agent construction deadline elapsed"));
                }
                return Err(match error {
                    storage_kit::StateStoreError::TransactionConflict { .. } => {
                        KitError::conflict("readiness transaction conflicted")
                            .with_detail(ErrorDetail::TransactionConflict)
                    }
                    error => {
                        KitError::from_state_store(error, "readiness transaction could not begin")
                            .in_phase(ErrorPhase::StateRead)
                    }
                });
            }
        };
        let mutation = async {
            if transaction
                .load(key.clone())
                .await
                .map_err(|error| match error {
                    storage_kit::StateStoreError::TransactionConflict { .. } => {
                        KitError::conflict("attachment transaction conflicted")
                            .with_detail(ErrorDetail::TransactionConflict)
                    }
                    error => KitError::from_state_store(error, "attachment could not be checked")
                        .in_phase(ErrorPhase::StateRead),
                })?
                .is_some()
                || transaction
                    .load(catalog_key.clone())
                    .await
                    .map_err(|error| match error {
                        storage_kit::StateStoreError::TransactionConflict { .. } => {
                            KitError::conflict("catalog state transaction conflicted")
                                .with_detail(ErrorDetail::TransactionConflict)
                        }
                        error => {
                            KitError::from_state_store(error, "catalog state could not be checked")
                                .in_phase(ErrorPhase::StateRead)
                        }
                    })?
                    .is_some()
            {
                return Err(KitError::conflict("durable attachment already exists"));
            }
            self.storage
                .create_agent_in_transaction(&mut transaction, info)
                .await
                .map_err(crate::agent_storage::AgentStorageCreationError::into_sdk)?;
            transaction
                .put(storage_kit::state::record::StateRecord::new(
                    key.clone(),
                    attachment_value.clone(),
                ))
                .await
                .map_err(|error| match error {
                    storage_kit::StateStoreError::TransactionConflict { .. } => {
                        KitError::conflict("attachment transaction conflicted")
                            .with_detail(ErrorDetail::TransactionConflict)
                    }
                    error => KitError::from_state_store(error, "attachment could not be stored")
                        .in_phase(ErrorPhase::StateWrite),
                })?;
            transaction
                .put(storage_kit::state::record::StateRecord::new(
                    catalog_key.clone(),
                    catalog_value.clone(),
                ))
                .await
                .map_err(|error| match error {
                    storage_kit::StateStoreError::TransactionConflict { .. } => {
                        KitError::conflict("catalog state transaction conflicted")
                            .with_detail(ErrorDetail::TransactionConflict)
                    }
                    error => KitError::from_state_store(error, "catalog state could not be stored")
                        .in_phase(ErrorPhase::StateWrite),
                })?;
            transaction
                .put(storage_kit::state::record::StateRecord::new(
                    progress_key.clone(),
                    progress_value.clone(),
                ))
                .await
                .map_err(|error| match error {
                    storage_kit::StateStoreError::TransactionConflict { .. } => {
                        KitError::conflict("event progress transaction conflicted")
                            .with_detail(ErrorDetail::TransactionConflict)
                    }
                    error => {
                        KitError::from_state_store(error, "event progress could not be stored")
                            .in_phase(ErrorPhase::StateWrite)
                    }
                })?;
            Ok::<(), KitError>(())
        }
        .await;
        if let Err(error) = mutation {
            let _ = transaction.rollback().await;
            return if tokio::time::Instant::now() >= deadline {
                Err(KitError::timeout("agent construction deadline elapsed"))
            } else {
                Err(error)
            };
        }

        let outcome = match transaction.commit().await {
            Ok(()) => Ok(()),
            Err(storage_kit::StateStoreError::TransactionConflict { .. }) => {
                Err(KitError::conflict("readiness transaction conflicted")
                    .with_detail(ErrorDetail::TransactionConflict))
            }
            Err(_) => {
                async {
                    let stored_info = match self.storage.agent_info(info.id().clone()).await {
                        Ok(stored) => Some(stored),
                        Err(error) if error.kind() == ErrorKind::NotFound => None,
                        Err(error) => return Err(error),
                    };
                    let stored_attachment = store
                        .load(key)
                        .await
                        .map_err(|error| {
                            KitError::from_state_store(
                                error,
                                "attachment commit could not be checked",
                            )
                            .in_phase(ErrorPhase::StateCommit)
                        })?
                        .map(|snapshot| snapshot.record.value);
                    let stored_catalog = store
                        .load(catalog_key)
                        .await
                        .map_err(|error| {
                            KitError::from_state_store(error, "catalog commit could not be checked")
                                .in_phase(ErrorPhase::StateCommit)
                        })?
                        .map(|snapshot| snapshot.record.value);
                    let stored_progress = store
                        .load(progress_key)
                        .await
                        .map_err(|error| {
                            KitError::from_state_store(
                                error,
                                "event progress commit could not be checked",
                            )
                            .in_phase(ErrorPhase::StateRead)
                        })?
                        .map(|snapshot| snapshot.record.value);
                    if stored_info.as_ref() == Some(info)
                        && stored_attachment.as_deref() == Some(&attachment_value)
                        && stored_catalog.as_deref() == Some(&catalog_value)
                        && stored_progress.as_deref() == Some(&progress_value)
                    {
                        Ok(())
                    } else if stored_info.is_none()
                        && stored_attachment.is_none()
                        && stored_catalog.is_none()
                        && stored_progress.is_none()
                    {
                        Err(KitError::integration(
                            "readiness transaction could not commit",
                        ))
                    } else {
                        Err(KitError::invalid_state(
                            "readiness commit outcome is contradictory",
                        ))
                    }
                }
                .await
            }
        };
        if tokio::time::Instant::now() >= deadline {
            return Err(KitError::timeout("agent construction deadline elapsed"));
        }
        outcome
    }

    pub(crate) async fn verify(&self, expected: &OpenCodeAttachment) -> Result<(), KitError> {
        let key = Self::key(expected.agent_id())?;
        let store = self.storage.store();
        let snapshot = store
            .load(key)
            .await
            .map_err(|error| {
                KitError::from_state_store(error, "attachment could not be loaded")
                    .in_phase(ErrorPhase::StateRead)
            })?
            .ok_or_else(|| KitError::conflict("durable agent is not attached to OpenCode"))?;
        let actual: OpenCodeAttachment =
            serde::Deserialize::deserialize(snapshot.record.value.as_ref())
                .map_err(|_| KitError::invalid_state("stored attachment is malformed"))?;
        actual.validate()?;
        if actual.agent_id() != expected.agent_id() {
            return Err(KitError::invalid_state(
                "stored attachment contradicts its durable owner",
            ));
        }
        if actual != *expected {
            return Err(KitError::conflict("attachment changed"));
        }
        let progress = store
            .load(Self::event_progress_key(expected.agent_id())?)
            .await
            .map_err(|error| {
                KitError::from_state_store(error, "event progress could not be loaded")
                    .in_phase(ErrorPhase::StateRead)
            })?
            .ok_or_else(|| KitError::invalid_state("event progress is missing"))?;
        let progress: OpenCodeEventProgress =
            serde::Deserialize::deserialize(progress.record.value.as_ref())
                .map_err(|_| KitError::invalid_state("stored event progress is malformed"))?;
        progress.validate(expected.agent_id())?;
        let catalog = store
            .load(Self::catalog_key(expected.agent_id())?)
            .await
            .map_err(|error| {
                KitError::from_state_store(error, "catalog state could not be loaded")
                    .in_phase(ErrorPhase::StateRead)
            })?
            .ok_or_else(|| KitError::invalid_state("catalog state is missing"))?;
        Self::decode_catalog(catalog, expected.agent_id())?;
        Ok(())
    }

    pub(crate) async fn create_workspace(
        &self,
        agent_id: &AgentId,
        request: &NewWorkspace,
        directory: std::path::PathBuf,
        deadline: tokio::time::Instant,
    ) -> Result<WorkspaceRecord, KitError> {
        let record = WorkspaceRecord::new(agent_id.clone(), WorkspaceId::new(), request);
        let key = WorkspaceRecord::key(agent_id, record.id())?;
        let routing = OpenCodeWorkspaceRouting::new(&record, directory)?;
        let routing_record = routing.record()?;
        let store = self.storage.store();
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(KitError::timeout("workspace creation deadline elapsed"));
            }
            let result = async {
                let mut transaction = store.begin_transaction().await.map_err(|error| {
                    OpenCodeTransactionError::storage(
                        error,
                        "workspace transaction could not begin",
                        ErrorPhase::StateWrite,
                    )
                })?;
                if transaction
                    .load(key.clone())
                    .await
                    .map_err(|error| {
                        OpenCodeTransactionError::storage(
                            error,
                            "workspace identity could not be checked",
                            ErrorPhase::StateRead,
                        )
                    })?
                    .is_some()
                {
                    return Err(KitError::conflict("workspace identity already exists").into());
                }
                transaction
                    .put(storage_kit::state::record::StateRecord::new(
                        key.clone(),
                        record.encode()?,
                    ))
                    .await
                    .map_err(|error| {
                        OpenCodeTransactionError::storage(
                            error,
                            "workspace could not be stored",
                            ErrorPhase::StateWrite,
                        )
                    })?;
                if transaction
                    .load(routing_record.key.clone())
                    .await
                    .map_err(|error| {
                        OpenCodeTransactionError::storage(
                            error,
                            "workspace routing identity could not be checked",
                            ErrorPhase::StateRead,
                        )
                    })?
                    .is_some()
                {
                    return Err(KitError::invalid_state(
                        "workspace routing identity already exists",
                    )
                    .into());
                }
                transaction
                    .put(routing_record.clone())
                    .await
                    .map_err(|error| {
                        OpenCodeTransactionError::storage(
                            error,
                            "workspace routing could not be stored",
                            ErrorPhase::StateWrite,
                        )
                    })?;
                transaction.commit().await.map_err(|error| {
                    OpenCodeTransactionError::storage(
                        error,
                        "workspace transaction could not commit",
                        ErrorPhase::StateCommit,
                    )
                })
            }
            .await;
            match result {
                Ok(()) => return Ok(record),
                Err(OpenCodeTransactionError::TransactionConflict(_)) => {
                    tokio::task::yield_now().await;
                }
                Err(
                    OpenCodeTransactionError::Failure(error)
                    | OpenCodeTransactionError::TerminalCleanup(error),
                ) => return Err(error),
            }
        }
    }

    pub(crate) async fn workspaces(
        &self,
        agent_id: &AgentId,
    ) -> Result<Vec<WorkspaceRecord>, KitError> {
        let store = self.storage.store();
        WorkspaceRecord::list(store, agent_id).await
    }

    pub(crate) async fn workspace(
        &self,
        agent_id: &AgentId,
        id: &WorkspaceId,
    ) -> Result<WorkspaceRecord, KitError> {
        let store = self.storage.store();
        let snapshot = store
            .load(WorkspaceRecord::key(agent_id, id)?)
            .await
            .map_err(|error| {
                KitError::from_state_store(error, "workspace could not be loaded")
                    .in_phase(ErrorPhase::StateRead)
            })?
            .ok_or_else(|| KitError::not_found("workspace does not exist"))?;
        WorkspaceRecord::decode(snapshot, agent_id, Some(id))
    }

    pub(crate) async fn workspace_routing(
        &self,
        workspace: &WorkspaceRecord,
    ) -> Result<OpenCodeWorkspaceRouting, KitError> {
        let snapshot = self
            .storage
            .store()
            .load(OpenCodeWorkspaceRouting::key(
                workspace.agent_id(),
                workspace.id(),
            )?)
            .await
            .map_err(|error| {
                KitError::from_state_store(error, "workspace routing could not be loaded")
                    .in_phase(ErrorPhase::StateRead)
            })?
            .ok_or_else(|| KitError::invalid_state("workspace routing is missing"))?;
        OpenCodeWorkspaceRouting::decode(snapshot, workspace)
    }

    async fn workspace_routing_in_transaction(
        transaction: &mut S::Transaction,
        workspace: &WorkspaceRecord,
    ) -> Result<OpenCodeWorkspaceRouting, KitError> {
        let snapshot = transaction
            .load(OpenCodeWorkspaceRouting::key(
                workspace.agent_id(),
                workspace.id(),
            )?)
            .await
            .map_err(|error| {
                KitError::from_state_store(error, "workspace routing could not be checked")
                    .in_phase(ErrorPhase::StateRead)
            })?
            .ok_or_else(|| KitError::invalid_state("workspace routing is missing"))?;
        OpenCodeWorkspaceRouting::decode(snapshot, workspace)
    }

    pub(crate) async fn bind_complete_catalog(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        workspace_revision: u64,
        catalog: &OpenCodeToolCatalog,
    ) -> Result<(), KitError> {
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            KitError::from_state_store(error, "catalog binding transaction could not begin")
                .in_phase(ErrorPhase::StateWrite)
        })?;
        let workspace = transaction
            .load(WorkspaceRecord::key(agent_id, workspace_id)?)
            .await
            .map_err(|error| {
                KitError::from_state_store(error, "catalog workspace could not be loaded")
                    .in_phase(ErrorPhase::StateRead)
            })?
            .ok_or_else(|| KitError::not_found("workspace does not exist"))?;
        let workspace = WorkspaceRecord::decode(workspace, agent_id, Some(workspace_id))?;
        if workspace.revision() != workspace_revision {
            return Err(KitError::conflict("workspace handle is stale"));
        }
        let routing = Self::workspace_routing_in_transaction(&mut transaction, &workspace).await?;
        let binding = OpenCodeCatalogBinding::complete(
            agent_id.clone(),
            workspace_id.clone(),
            workspace_revision,
            routing.directory().to_path_buf(),
            catalog.generation().clone(),
        )?;
        let mut state = Self::catalog_in_transaction(&mut transaction, agent_id).await?;
        state.bind(binding)?;
        Self::store_catalog_in_transaction(&mut transaction, agent_id, state).await?;
        transaction.commit().await.map_err(|error| {
            KitError::from_state_store(error, "catalog binding transaction could not commit")
                .in_phase(ErrorPhase::StateCommit)
        })?;
        Ok(())
    }

    pub(crate) async fn reconcile_catalog_binding(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        workspace_revision: u64,
        generation: &OpenCodeCatalogGeneration,
        deadline: tokio::time::Instant,
    ) -> Result<bool, KitError> {
        let store = self.storage.store();
        if tokio::time::Instant::now() >= deadline {
            return Err(KitError::timeout("catalog reconciliation deadline elapsed"));
        }
        let mut transaction = match store.begin_transaction().await {
            Ok(transaction) => transaction,
            Err(error) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(KitError::timeout("catalog reconciliation deadline elapsed"));
                }
                return Err(match error {
                    storage_kit::StateStoreError::TransactionConflict { .. } => {
                        KitError::conflict("catalog reopen transaction conflicted")
                            .with_detail(ErrorDetail::TransactionConflict)
                    }
                    error => KitError::from_state_store(
                        error,
                        "catalog reopen transaction could not begin",
                    )
                    .in_phase(ErrorPhase::StateWrite),
                });
            }
        };
        let mutation = async {
            let workspace = transaction
                .load(WorkspaceRecord::key(agent_id, workspace_id)?)
                .await
                .map_err(|error| match error {
                    storage_kit::StateStoreError::TransactionConflict { .. } => {
                        KitError::conflict("catalog workspace transaction conflicted")
                            .with_detail(ErrorDetail::TransactionConflict)
                    }
                    error => {
                        KitError::from_state_store(error, "catalog workspace could not be loaded")
                            .in_phase(ErrorPhase::StateRead)
                    }
                })?
                .ok_or_else(|| KitError::not_found("workspace does not exist"))?;
            let workspace = WorkspaceRecord::decode(workspace, agent_id, Some(workspace_id))?;
            if workspace.revision() != workspace_revision {
                return Err(KitError::conflict("workspace handle is stale"));
            }
            let mut state = Self::catalog_in_transaction(&mut transaction, agent_id).await?;
            let original = state.clone();
            let was_bound = state.binding(workspace_id).is_some();
            let current = match state.binding(workspace_id) {
                Some(binding) => {
                    let routing =
                        Self::workspace_routing_in_transaction(&mut transaction, &workspace)
                            .await?;
                    binding.verify_owner(agent_id, &workspace, &routing)?;
                    binding.matches_generation(generation)
                }
                None => false,
            };
            if was_bound && !current {
                state.remove(workspace_id)?;
                Self::store_catalog_in_transaction(&mut transaction, agent_id, state.clone())
                    .await?;
            }
            Ok::<_, KitError>((current, original, state))
        }
        .await;
        let (current, original, intended) = match mutation {
            Ok(outcome) => outcome,
            Err(error) => {
                let _ = transaction.rollback().await;
                return if tokio::time::Instant::now() >= deadline {
                    Err(KitError::timeout("catalog reconciliation deadline elapsed"))
                } else {
                    Err(error)
                };
            }
        };
        let outcome = match transaction.commit().await {
            Ok(()) => Ok(current),
            Err(storage_kit::StateStoreError::TransactionConflict { .. }) => {
                Err(KitError::conflict("catalog reopen transaction conflicted")
                    .with_detail(ErrorDetail::TransactionConflict))
            }
            Err(_) => {
                async {
                    let snapshot = store
                        .load(Self::catalog_key(agent_id)?)
                        .await
                        .map_err(|error| {
                            KitError::from_state_store(error, "catalog commit could not be checked")
                                .in_phase(ErrorPhase::StateCommit)
                        })?
                        .ok_or_else(|| KitError::invalid_state("catalog state is missing"))?;
                    let stored = Self::decode_catalog(snapshot, agent_id)?;
                    if stored == intended {
                        Ok(current)
                    } else if stored == original {
                        Err(KitError::integration(
                            "catalog reopen transaction could not commit",
                        ))
                    } else {
                        Err(KitError::invalid_state(
                            "catalog commit outcome is contradictory",
                        ))
                    }
                }
                .await
            }
        };
        if tokio::time::Instant::now() >= deadline {
            return Err(KitError::timeout("catalog reconciliation deadline elapsed"));
        }
        outcome
    }

    pub(crate) async fn begin_conversation_creation(
        &self,
        agent_id: &AgentId,
        record: &ConversationRecord,
        binding: &OpenCodeSessionBinding,
        deadline: tokio::time::Instant,
    ) -> Result<(), KitError> {
        binding.validate()?;
        if binding.is_bound() {
            return Err(KitError::invalid_state(
                "conversation creation intent is already bound",
            ));
        }
        if binding.conversation_id() != record.id()
            || binding.workspace_id() != record.workspace_id()
            || binding.workspace_revision() != record.workspace_revision()
        {
            return Err(KitError::invalid_state(
                "session binding contradicts portable conversation state",
            ));
        }
        let conversation_key = ConversationRecord::key(record.workspace_id(), record.id())?;
        let binding_key = Self::binding_key(record.workspace_id(), record.id())?;
        let value = serde_json::to_value(binding)
            .map_err(|_| KitError::invalid_state("session binding could not be encoded"))?;
        let conversation = record.encode()?;
        let store = self.storage.store();
        let mut last_conflict = None;
        loop {
            if tokio::time::Instant::now() >= deadline {
                let error = KitError::timeout("conversation creation deadline elapsed")
                    .at_phase(ErrorPhase::StateWrite);
                return Err(match last_conflict {
                    Some(conflict) => error.with_supplementary(conflict),
                    None => error,
                });
            }
            let mut transaction = match store.begin_transaction().await {
                Ok(transaction) => transaction,
                Err(source) => {
                    let error = KitError::from_state_store(
                        source,
                        "conversation transaction could not begin",
                    )
                    .in_phase(ErrorPhase::StateWrite);
                    if error.detail() == Some(&ErrorDetail::TransactionConflict) {
                        last_conflict = Some(error);
                        tokio::task::yield_now().await;
                        continue;
                    }
                    return Err(error);
                }
            };
            let mutation = async {
                let workspace = transaction
                    .load(WorkspaceRecord::key(agent_id, record.workspace_id())?)
                    .await
                    .map_err(|error| {
                        KitError::from_state_store(
                            error,
                            "workspace could not be checked for conversation",
                        )
                        .in_phase(ErrorPhase::StateRead)
                    })?
                    .ok_or_else(|| KitError::not_found("workspace does not exist"))?;
                let workspace =
                    WorkspaceRecord::decode(workspace, agent_id, Some(record.workspace_id()))?;
                if workspace.revision() != record.workspace_revision() {
                    return Err(KitError::conflict("workspace handle is stale"));
                }
                let routing =
                    Self::workspace_routing_in_transaction(&mut transaction, &workspace).await?;
                binding.verify_owner(record, &workspace, &routing)?;
                if transaction
                    .load(conversation_key.clone())
                    .await
                    .map_err(|error| {
                        KitError::from_state_store(error, "conversation could not be checked")
                            .in_phase(ErrorPhase::StateRead)
                    })?
                    .is_some()
                    || transaction
                        .load(binding_key.clone())
                        .await
                        .map_err(|error| {
                            KitError::from_state_store(
                                error,
                                "session binding could not be checked",
                            )
                            .in_phase(ErrorPhase::StateRead)
                        })?
                        .is_some()
                {
                    return Err(KitError::conflict("conversation identity already exists"));
                }
                transaction
                    .put(storage_kit::state::record::StateRecord::new(
                        conversation_key.clone(),
                        conversation.clone(),
                    ))
                    .await
                    .map_err(|error| {
                        KitError::from_state_store(error, "conversation could not be stored")
                            .in_phase(ErrorPhase::StateWrite)
                    })?;
                transaction
                    .put(storage_kit::state::record::StateRecord::new(
                        binding_key.clone(),
                        value.clone(),
                    ))
                    .await
                    .map_err(|error| {
                        KitError::from_state_store(error, "session binding could not be stored")
                            .in_phase(ErrorPhase::StateWrite)
                    })
            }
            .await;
            if let Err(error) = mutation {
                if let Err(secondary) = transaction.rollback().await {
                    return Err(error.with_supplementary(
                        KitError::from_state_store(secondary, "transaction rollback failed")
                            .at_phase(ErrorPhase::Cleanup),
                    ));
                }
                if error.detail() == Some(&ErrorDetail::TransactionConflict) {
                    last_conflict = Some(error);
                    tokio::task::yield_now().await;
                    continue;
                }
                return Err(error);
            }
            match transaction.commit().await {
                Ok(()) => return Ok(()),
                Err(source) => {
                    let error = KitError::from_state_store(
                        source,
                        "conversation intent transaction could not commit",
                    )
                    .in_phase(ErrorPhase::StateCommit);
                    if error.detail() != Some(&ErrorDetail::TransactionConflict) {
                        return Err(error);
                    }
                    last_conflict = Some(error);
                    tokio::task::yield_now().await;
                }
            }
        }
    }

    pub(super) async fn complete_conversation_creation(
        &self,
        agent_id: &AgentId,
        record: &ConversationRecord,
        creating: &OpenCodeSessionBinding,
        bound: &OpenCodeSessionBinding,
        deadline: tokio::time::Instant,
    ) -> Result<OpenCodeConversationMutationDisposition, KitError> {
        creating.validate()?;
        bound.validate()?;
        if creating.is_bound()
            || !bound.is_bound()
            || creating.conversation_id() != record.id()
            || creating.workspace_id() != record.workspace_id()
            || creating.workspace_revision() != record.workspace_revision()
            || creating.conversation_id() != bound.conversation_id()
            || creating.workspace_id() != bound.workspace_id()
            || creating.workspace_revision() != bound.workspace_revision()
            || creating.directory() != bound.directory()
            || creating.correlation() != bound.correlation()
        {
            return Err(KitError::invalid_state(
                "completed session binding contradicts its creation intent",
            ));
        }
        let conversation_key = ConversationRecord::key(record.workspace_id(), record.id())?;
        let binding_key = Self::binding_key(record.workspace_id(), record.id())?;
        let store = self.storage.store();
        if tokio::time::Instant::now() >= deadline {
            return Err(KitError::timeout("agent construction deadline elapsed"));
        }
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            KitError::from_state_store(error, "conversation completion transaction could not begin")
                .in_phase(ErrorPhase::StateWrite)
        })?;
        let value = serde_json::to_value(bound)
            .map_err(|_| KitError::invalid_state("session binding could not be encoded"))?;
        let creating_value = serde_json::to_value(creating)
            .map_err(|_| KitError::invalid_state("session intent could not be encoded"))?;
        let mutation = async {
            let workspace = transaction
                .load(WorkspaceRecord::key(agent_id, record.workspace_id())?)
                .await
                .map_err(|error| {
                    KitError::from_state_store(
                        error,
                        "workspace could not be checked for conversation",
                    )
                    .in_phase(ErrorPhase::StateRead)
                })?
                .ok_or_else(|| KitError::not_found("workspace does not exist"))?;
            let workspace =
                WorkspaceRecord::decode(workspace, agent_id, Some(record.workspace_id()))?;
            if workspace.revision() != record.workspace_revision() {
                return Err(KitError::conflict("workspace handle is stale"));
            }
            let stored_record = transaction
                .load(conversation_key)
                .await
                .map_err(|error| {
                    KitError::from_state_store(error, "conversation intent could not be loaded")
                        .in_phase(ErrorPhase::StateRead)
                })?
                .ok_or_else(|| KitError::invalid_state("conversation intent is missing"))?;
            if ConversationRecord::decode(stored_record, record.workspace_id(), Some(record.id()))?
                != *record
            {
                return Err(KitError::invalid_state(
                    "conversation intent changed before completion",
                ));
            }
            let stored_binding = transaction
                .load(binding_key.clone())
                .await
                .map_err(|error| {
                    KitError::from_state_store(error, "session intent could not be loaded")
                        .in_phase(ErrorPhase::StateRead)
                })?
                .ok_or_else(|| KitError::invalid_state("session intent is missing"))?;
            if Self::decode_binding(stored_binding, record.workspace_id(), record.id())?
                != *creating
            {
                return Err(KitError::invalid_state(
                    "session intent changed before completion",
                ));
            }
            transaction
                .put(storage_kit::state::record::StateRecord::new(
                    binding_key.clone(),
                    value.clone(),
                ))
                .await
                .map_err(|error| {
                    KitError::from_state_store(error, "session binding could not be stored")
                        .in_phase(ErrorPhase::StateWrite)
                })?;
            NotificationStore::<S>::initialize_in_transaction(
                &mut transaction,
                record.workspace_id(),
                record.id(),
            )
            .await
            .map_err(NotificationInitializationError::into_sdk)
        }
        .await;
        if let Err(error) = mutation {
            let primary = error;
            let rollback = transaction.rollback().await;
            return Err(match rollback {
                Ok(()) => primary,
                Err(secondary) => primary.with_supplementary(
                    KitError::from_state_store(secondary, "transaction rollback failed")
                        .at_phase(ErrorPhase::Cleanup),
                ),
            });
        }
        match transaction.commit().await {
            Ok(()) => Ok(OpenCodeConversationMutationDisposition::Settled),
            Err(storage_kit::StateStoreError::TransactionConflict { .. }) => Err(
                KitError::conflict("conversation completion transaction conflicted")
                    .with_detail(ErrorDetail::TransactionConflict)
                    .at_phase(ErrorPhase::StateCommit),
            ),
            Err(_) => loop {
                let observation = async {
                    let binding = store.load(binding_key.clone()).await.map_err(|error| {
                        KitError::from_state_store(
                            error,
                            "conversation completion outcome could not be checked",
                        )
                        .in_phase(ErrorPhase::StateRead)
                    })?;
                    let notification = NotificationStore::<S>::initialization_matches(
                        store,
                        record.workspace_id(),
                        record.id(),
                    )
                    .await;
                    let notification = match notification {
                        Ok(matches) => matches,
                        Err(error) if Self::retryable_state_error(&error) => {
                            return Err(KitError::integration(
                                "conversation completion outcome could not be checked",
                            ));
                        }
                        Err(_) => {
                            return Err(KitError::invalid_state(
                                "conversation completion outcome is contradictory",
                            ));
                        }
                    };
                    Ok::<_, KitError>((binding.map(|snapshot| snapshot.record.value), notification))
                }
                .await;
                match observation {
                    Ok((binding, true)) if binding.as_deref() == Some(&value) => {
                        break Ok(OpenCodeConversationMutationDisposition::Settled);
                    }
                    Ok((binding, false)) if binding.as_deref() == Some(&creating_value) => {
                        break Err(KitError::integration(
                            "conversation transaction could not commit",
                        ));
                    }
                    Ok(_) => {
                        break Ok(OpenCodeConversationMutationDisposition::RecoveryRequired(
                            KitError::invalid_state(
                                "conversation completion outcome is contradictory",
                            ),
                        ));
                    }
                    Err(error)
                        if Self::retryable_state_error(&error)
                            && tokio::time::Instant::now() < deadline =>
                    {
                        tokio::time::sleep_until(
                            (tokio::time::Instant::now() + Duration::from_millis(25)).min(deadline),
                        )
                        .await;
                    }
                    Err(error) if Self::retryable_state_error(&error) => {
                        break Ok(OpenCodeConversationMutationDisposition::RecoveryRequired(
                            KitError::busy("conversation completion requires recovery"),
                        ));
                    }
                    Err(error) => break Err(error),
                }
            },
        }
    }

    pub(crate) async fn discard_conversation_creation(
        &self,
        record: &ConversationRecord,
        creating: &OpenCodeSessionBinding,
    ) -> Result<(), KitError> {
        if creating.is_bound()
            || creating.conversation_id() != record.id()
            || creating.workspace_id() != record.workspace_id()
        {
            return Err(KitError::invalid_state(
                "discarded creation contradicts its intent",
            ));
        }
        let conversation_key = ConversationRecord::key(record.workspace_id(), record.id())?;
        let binding_key = Self::binding_key(record.workspace_id(), record.id())?;
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            KitError::from_state_store(error, "conversation discard transaction could not begin")
                .in_phase(ErrorPhase::StateWrite)
        })?;
        let stored_binding = transaction
            .load(binding_key.clone())
            .await
            .map_err(|error| {
                KitError::from_state_store(error, "session intent could not be loaded")
                    .in_phase(ErrorPhase::StateRead)
            })?;
        let stored_record = transaction
            .load(conversation_key.clone())
            .await
            .map_err(|error| {
                KitError::from_state_store(error, "conversation intent could not be loaded")
                    .in_phase(ErrorPhase::StateRead)
            })?;
        match (stored_binding, stored_record) {
            (None, None) => return Ok(()),
            (Some(binding), Some(conversation)) => {
                let binding = Self::decode_binding(binding, record.workspace_id(), record.id())?;
                let conversation = ConversationRecord::decode(
                    conversation,
                    record.workspace_id(),
                    Some(record.id()),
                )?;
                if binding != *creating || conversation != *record {
                    return Err(KitError::invalid_state(
                        "conversation creation intent changed before discard",
                    ));
                }
            }
            _ => {
                return Err(KitError::invalid_state(
                    "conversation creation intent changed before discard",
                ));
            }
        }
        self.stage_knowledge_forget(
            &mut transaction,
            &KnowledgeOwnerId::Conversation {
                agent_id: self.agent_id.clone(),
                workspace_id: record.workspace_id().clone(),
                conversation_id: record.id().clone(),
            },
        )
        .await
        .map_err(AgentStorageKnowledgeError::into_sdk)?;
        transaction.delete(binding_key).await.map_err(|error| {
            KitError::from_state_store(error, "session intent could not be removed")
                .in_phase(ErrorPhase::StateWrite)
        })?;
        transaction
            .delete(conversation_key)
            .await
            .map_err(|error| {
                KitError::from_state_store(error, "conversation intent could not be removed")
                    .in_phase(ErrorPhase::StateWrite)
            })?;
        transaction.commit().await.map_err(|error| {
            KitError::from_state_store(error, "conversation discard transaction could not commit")
                .in_phase(ErrorPhase::StateCommit)
        })?;
        Ok(())
    }

    pub(super) async fn replace_conversation_creation_with_cleanup(
        &self,
        agent_id: &AgentId,
        record: &ConversationRecord,
        creating: &OpenCodeSessionBinding,
        cleanup: &OpenCodeCleanup,
        root_session_id: &str,
        deadline: tokio::time::Instant,
    ) -> Result<OpenCodeConversationMutationDisposition, KitError> {
        creating.validate()?;
        cleanup.validate()?;
        if creating.is_bound()
            || creating.conversation_id() != record.id()
            || creating.workspace_id() != record.workspace_id()
            || creating.workspace_revision() != record.workspace_revision()
            || !cleanup.matches_creation(agent_id, record.id(), creating, root_session_id)
        {
            return Err(KitError::invalid_state(
                "creation cleanup contradicts its durable intent",
            ));
        }
        let conversation_key = ConversationRecord::key(record.workspace_id(), record.id())?;
        let binding_key = Self::binding_key(record.workspace_id(), record.id())?;
        let cleanup_key = Self::cleanup_key(cleanup)?;
        let cleanup_value = serde_json::to_value(cleanup)
            .map_err(|_| KitError::invalid_state("cleanup obligation could not be encoded"))?;
        let store = self.storage.store();
        if tokio::time::Instant::now() >= deadline {
            return Err(KitError::timeout(
                "creation cleanup handoff deadline elapsed",
            ));
        }
        let mut transaction = store
            .begin_transaction()
            .await
            .map_err(|error| match error {
                storage_kit::StateStoreError::TransactionConflict { .. } => {
                    KitError::conflict("creation cleanup transaction conflicted")
                        .with_detail(ErrorDetail::TransactionConflict)
                }
                error => KitError::from_state_store(
                    error,
                    "creation cleanup transaction could not begin",
                )
                .in_phase(ErrorPhase::StateWrite),
            })?;
        let mutation = async {
            let stored_record = transaction
                .load(conversation_key.clone())
                .await
                .map_err(|error| match error {
                    storage_kit::StateStoreError::TransactionConflict { .. } => {
                        KitError::conflict("creation cleanup transaction conflicted")
                            .with_detail(ErrorDetail::TransactionConflict)
                    }
                    error => KitError::from_state_store(
                        error,
                        "conversation creation intent could not be loaded",
                    )
                    .in_phase(ErrorPhase::StateRead),
                })?
                .ok_or_else(|| KitError::invalid_state("conversation intent is missing"))?;
            let stored_record = ConversationRecord::decode(
                stored_record,
                record.workspace_id(),
                Some(record.id()),
            )?;
            let stored_binding = transaction
                .load(binding_key.clone())
                .await
                .map_err(|error| match error {
                    storage_kit::StateStoreError::TransactionConflict { .. } => {
                        KitError::conflict("creation cleanup transaction conflicted")
                            .with_detail(ErrorDetail::TransactionConflict)
                    }
                    error => {
                        KitError::from_state_store(error, "session intent could not be loaded")
                            .in_phase(ErrorPhase::StateRead)
                    }
                })?
                .ok_or_else(|| KitError::invalid_state("session intent is missing"))?;
            let stored_binding =
                Self::decode_binding(stored_binding, record.workspace_id(), record.id())?;
            if stored_record != *record || stored_binding != *creating {
                return Err(KitError::invalid_state(
                    "creation cleanup intent changed before handoff",
                ));
            }
            if transaction
                .load(cleanup_key.clone())
                .await
                .map_err(|error| match error {
                    storage_kit::StateStoreError::TransactionConflict { .. } => {
                        KitError::conflict("creation cleanup transaction conflicted")
                            .with_detail(ErrorDetail::TransactionConflict)
                    }
                    error => {
                        KitError::from_state_store(error, "cleanup obligation could not be checked")
                            .in_phase(ErrorPhase::StateRead)
                    }
                })?
                .is_some()
            {
                return Err(KitError::invalid_state(
                    "creation cleanup obligation already exists",
                ));
            }
            self.stage_knowledge_forget(
                &mut transaction,
                &KnowledgeOwnerId::Conversation {
                    agent_id: agent_id.clone(),
                    workspace_id: record.workspace_id().clone(),
                    conversation_id: record.id().clone(),
                },
            )
            .await
            .map_err(AgentStorageKnowledgeError::into_sdk)?;
            transaction
                .delete(conversation_key.clone())
                .await
                .map_err(|error| match error {
                    storage_kit::StateStoreError::TransactionConflict { .. } => {
                        KitError::conflict("creation cleanup transaction conflicted")
                            .with_detail(ErrorDetail::TransactionConflict)
                    }
                    error => KitError::from_state_store(
                        error,
                        "conversation creation intent could not be removed",
                    )
                    .in_phase(ErrorPhase::StateWrite),
                })?;
            transaction
                .delete(binding_key.clone())
                .await
                .map_err(|error| match error {
                    storage_kit::StateStoreError::TransactionConflict { .. } => {
                        KitError::conflict("creation cleanup transaction conflicted")
                            .with_detail(ErrorDetail::TransactionConflict)
                    }
                    error => KitError::from_state_store(
                        error,
                        "session creation intent could not be removed",
                    )
                    .in_phase(ErrorPhase::StateWrite),
                })?;
            transaction
                .put(storage_kit::state::record::StateRecord::new(
                    cleanup_key.clone(),
                    cleanup_value.clone(),
                ))
                .await
                .map_err(|error| match error {
                    storage_kit::StateStoreError::TransactionConflict { .. } => {
                        KitError::conflict("creation cleanup transaction conflicted")
                            .with_detail(ErrorDetail::TransactionConflict)
                    }
                    error => {
                        KitError::from_state_store(error, "cleanup obligation could not be stored")
                            .in_phase(ErrorPhase::StateWrite)
                    }
                })?;
            Ok::<(), KitError>(())
        }
        .await;
        if let Err(error) = mutation {
            let primary = error;
            let rollback = transaction.rollback().await;
            return Err(match rollback {
                Ok(()) => primary,
                Err(secondary) => primary.with_supplementary(
                    KitError::from_state_store(secondary, "transaction rollback failed")
                        .at_phase(ErrorPhase::Cleanup),
                ),
            });
        }
        match transaction.commit().await {
            Ok(()) => Ok(OpenCodeConversationMutationDisposition::Settled),
            Err(storage_kit::StateStoreError::TransactionConflict { .. }) => Err(
                KitError::conflict("creation cleanup transaction conflicted")
                    .with_detail(ErrorDetail::TransactionConflict),
            ),
            Err(_) => loop {
                let observation = async {
                    let conversation =
                        store
                            .load(conversation_key.clone())
                            .await
                            .map_err(|error| {
                                KitError::from_state_store(
                                    error,
                                    "creation cleanup outcome could not be checked",
                                )
                                .in_phase(ErrorPhase::StateRead)
                            })?;
                    let binding = store.load(binding_key.clone()).await.map_err(|error| {
                        KitError::from_state_store(
                            error,
                            "creation cleanup outcome could not be checked",
                        )
                        .in_phase(ErrorPhase::StateRead)
                    })?;
                    let observed_cleanup =
                        store.load(cleanup_key.clone()).await.map_err(|error| {
                            KitError::from_state_store(
                                error,
                                "creation cleanup outcome could not be checked",
                            )
                            .in_phase(ErrorPhase::StateRead)
                        })?;
                    Ok::<_, KitError>((conversation, binding, observed_cleanup))
                }
                .await;
                match observation {
                    Ok((None, None, Some(cleanup)))
                        if cleanup.record.value.as_ref() == &cleanup_value =>
                    {
                        break Ok(OpenCodeConversationMutationDisposition::Settled);
                    }
                    Ok((Some(conversation), Some(binding), None)) => {
                        let conversation = ConversationRecord::decode(
                            conversation,
                            record.workspace_id(),
                            Some(record.id()),
                        )?;
                        let binding =
                            Self::decode_binding(binding, record.workspace_id(), record.id())?;
                        if conversation == *record && binding == *creating {
                            break Err(KitError::integration(
                                "creation cleanup transaction could not commit",
                            ));
                        }
                        break Ok(OpenCodeConversationMutationDisposition::RecoveryRequired(
                            KitError::invalid_state("creation cleanup outcome is contradictory"),
                        ));
                    }
                    Ok(_) => {
                        break Ok(OpenCodeConversationMutationDisposition::RecoveryRequired(
                            KitError::invalid_state("creation cleanup outcome is contradictory"),
                        ));
                    }
                    Err(_) if tokio::time::Instant::now() < deadline => {
                        tokio::task::yield_now().await;
                    }
                    Err(_) => {
                        break Ok(OpenCodeConversationMutationDisposition::RecoveryRequired(
                            KitError::busy("creation cleanup handoff requires recovery"),
                        ));
                    }
                }
            },
        }
    }

    pub(crate) async fn conversations(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<ConversationRecord>, KitError> {
        let workspace = self.workspace(&self.agent_id, workspace_id).await?;
        let routing = self.workspace_routing(&workspace).await?;
        let store = self.storage.store();
        let mut published = Vec::new();
        for record in ConversationRecord::list(store, workspace_id).await? {
            let binding = Self::stored_binding(
                store,
                workspace_id,
                record.id(),
                OpenCodeMissingBinding::InventoryChanged,
            )
            .await?;
            binding.verify_owner(&record, &workspace, &routing)?;
            if binding.is_bound() {
                published.push(record);
            }
        }
        Ok(published)
    }

    pub(crate) async fn conversation_bindings(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<(ConversationRecord, OpenCodeSessionBinding)>, KitError> {
        let workspace = self.workspace(&self.agent_id, workspace_id).await?;
        let routing = self.workspace_routing(&workspace).await?;
        let store = self.storage.store();
        let mut published = Vec::new();
        for record in ConversationRecord::list(store, workspace_id).await? {
            let binding = Self::stored_binding(
                store,
                workspace_id,
                record.id(),
                OpenCodeMissingBinding::InvalidState,
            )
            .await?;
            binding.verify_owner(&record, &workspace, &routing)?;
            if binding.is_bound() {
                published.push((record, binding));
            }
        }
        Ok(published)
    }

    pub(crate) async fn workspace_has_unsettled_work(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<bool, KitError> {
        let conversations = self.conversations(workspace_id).await?;

        for conversation in conversations {
            let prompt_exists = {
                let store = self.storage.store();
                store
                    .load(OpenCodePromptRecord::key(conversation.id())?)
                    .await
                    .map_err(|error| {
                        KitError::from_state_store(error, "prompt state could not be checked")
                            .in_phase(ErrorPhase::StateRead)
                    })?
                    .is_some()
            };
            if prompt_exists {
                return Ok(true);
            }

            match self
                .notifications
                .ensure_no_unresolved_interaction(conversation.id())
                .await
            {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::Busy => return Ok(true),
                Err(error) => return Err(error),
            }
        }

        Ok(false)
    }

    pub(crate) async fn conversation(
        &self,
        workspace_id: &WorkspaceId,
        id: &ConversationId,
    ) -> Result<ConversationRecord, KitError> {
        let store = self.storage.store();
        let snapshot = store
            .load(ConversationRecord::key(workspace_id, id)?)
            .await
            .map_err(|error| {
                KitError::from_state_store(error, "conversation could not be loaded")
                    .in_phase(ErrorPhase::StateRead)
            })?
            .ok_or_else(|| KitError::not_found("conversation does not exist"))?;
        let record = ConversationRecord::decode(snapshot, workspace_id, Some(id))?;
        if !Self::stored_binding(store, workspace_id, id, OpenCodeMissingBinding::NotFound)
            .await?
            .is_bound()
        {
            return Err(KitError::not_found("conversation does not exist"));
        }
        Ok(record)
    }

    pub(crate) async fn binding(
        &self,
        workspace_id: &WorkspaceId,
        id: &ConversationId,
    ) -> Result<OpenCodeSessionBinding, KitError> {
        let store = self.storage.store();
        let binding = Self::stored_binding(
            store,
            workspace_id,
            id,
            OpenCodeMissingBinding::InvalidState,
        )
        .await?;
        if !binding.is_bound() {
            return Err(KitError::invalid_state("conversation session is not bound"));
        }
        let workspace = self.workspace(&self.agent_id, workspace_id).await?;
        let routing = self.workspace_routing(&workspace).await?;
        let conversation = self.conversation(workspace_id, id).await?;
        binding.verify_owner(&conversation, &workspace, &routing)?;
        Ok(binding)
    }

    pub(crate) async fn unresolved_creations(
        &self,
        agent_id: &AgentId,
    ) -> Result<Vec<(ConversationRecord, OpenCodeSessionBinding)>, KitError> {
        let store = self.storage.store();
        let mut unresolved = Vec::new();
        for workspace in WorkspaceRecord::list(store, agent_id).await? {
            let routing = self.workspace_routing(&workspace).await?;
            for record in ConversationRecord::list(store, workspace.id()).await? {
                let binding = Self::stored_binding(
                    store,
                    workspace.id(),
                    record.id(),
                    OpenCodeMissingBinding::InvalidState,
                )
                .await?;
                binding.verify_owner(&record, &workspace, &routing)?;
                if !binding.is_bound() {
                    unresolved.push((record, binding));
                }
            }
        }
        unresolved.sort_by(|left, right| left.0.id().cmp(right.0.id()));
        Ok(unresolved)
    }

    pub(crate) async fn update_workspace(
        &self,
        agent_id: &AgentId,
        request: &UpdateWorkspaceRequest,
        directory: std::path::PathBuf,
    ) -> Result<WorkspaceRecord, KitError> {
        let store = self.storage.store();
        let conversations = ConversationRecord::list(store, request.id()).await?;
        let workspace_key = WorkspaceRecord::key(agent_id, request.id())?;
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::workspace_update_error(error, "workspace update transaction could not begin")
        })?;
        let snapshot = transaction
            .load(workspace_key.clone())
            .await
            .map_err(|error| {
                Self::workspace_update_error(error, "workspace could not be loaded for update")
            })?
            .ok_or_else(|| KitError::not_found("workspace does not exist"))?;
        let current = WorkspaceRecord::decode(snapshot, agent_id, Some(request.id()))?;
        if !conversations.is_empty() && current.cwd() != request.context().cwd() {
            return Err(KitError::unsupported(
                "cannot change cwd while conversations remain",
            ));
        }
        for conversation in &conversations {
            NotificationStore::<S>::ensure_no_unresolved_interaction_in_transaction(
                &mut transaction,
                conversation.id(),
            )
            .await?;
            if transaction
                .load(OpenCodePromptRecord::key(conversation.id())?)
                .await
                .map_err(|error| {
                    Self::workspace_update_error(
                        error,
                        "prompt state could not be checked for update",
                    )
                })?
                .is_some()
            {
                return Err(KitError::busy(
                    "workspace contains active or unreconciled prompt work",
                ));
            }
        }
        let original_routing =
            Self::workspace_routing_in_transaction(&mut transaction, &current).await?;
        if current.cwd() == request.context().cwd() && original_routing.directory() != directory {
            return Err(KitError::invalid_state(
                "unchanged workspace cwd cannot replace native routing",
            ));
        }
        let replacement = current.replace(request)?;
        let routing = OpenCodeWorkspaceRouting::new(&replacement, directory)?;
        transaction.put(routing.record()?).await.map_err(|error| {
            Self::workspace_update_error(error, "workspace routing update could not be stored")
        })?;
        transaction
            .put(storage_kit::state::record::StateRecord::new(
                workspace_key,
                replacement.encode()?,
            ))
            .await
            .map_err(|error| {
                Self::workspace_update_error(error, "workspace update could not be stored")
            })?;
        for listed in conversations {
            let key = ConversationRecord::key(request.id(), listed.id())?;
            let snapshot = transaction
                .load(key.clone())
                .await
                .map_err(|error| {
                    Self::workspace_update_error(
                        error,
                        "conversation could not be loaded for update",
                    )
                })?
                .ok_or_else(|| KitError::invalid_state("conversation disappeared during update"))?;
            let conversation =
                ConversationRecord::decode(snapshot, request.id(), Some(listed.id()))?
                    .with_workspace_revision(replacement.revision())?;
            transaction
                .put(storage_kit::state::record::StateRecord::new(
                    key,
                    conversation.encode()?,
                ))
                .await
                .map_err(|error| {
                    Self::workspace_update_error(error, "conversation revision could not be stored")
                })?;

            let key = Self::binding_key(request.id(), listed.id())?;
            let snapshot = transaction
                .load(key.clone())
                .await
                .map_err(|error| {
                    Self::workspace_update_error(
                        error,
                        "session binding could not be loaded for update",
                    )
                })?
                .ok_or_else(|| KitError::invalid_state("session binding is missing"))?;
            let binding = Self::decode_binding(snapshot, request.id(), listed.id())?
                .with_workspace_revision(replacement.revision())?;
            let value = serde_json::to_value(binding)
                .map_err(|_| KitError::invalid_state("session binding could not be encoded"))?;
            transaction
                .put(storage_kit::state::record::StateRecord::new(key, value))
                .await
                .map_err(|error| {
                    Self::workspace_update_error(error, "session revision could not be stored")
                })?;
        }
        let mut catalog = Self::catalog_in_transaction(&mut transaction, agent_id).await?;
        catalog.remove(request.id())?;
        Self::store_catalog_in_transaction(&mut transaction, agent_id, catalog).await?;
        transaction.commit().await.map_err(|error| {
            Self::workspace_update_error(error, "workspace update transaction could not commit")
        })?;
        self.notifications.invalidate_workspace(request.id());
        Ok(replacement)
    }

    pub(super) async fn delete_workspace(
        &self,
        agent_id: &AgentId,
        id: &WorkspaceId,
        expected_revision: u64,
        cleanup: Option<&OpenCodeCleanup>,
        deadline: tokio::time::Instant,
    ) -> Result<OpenCodeWorkspaceDeleteDisposition, KitError> {
        let store = self.storage.store();
        let conversations = ConversationRecord::list(store, id).await?;
        let mut continuation_keys = Vec::new();
        for conversation in &conversations {
            for continuation in self.prompts.continuations(conversation.id()).await? {
                continuation_keys.push(continuation.state_key()?);
            }
        }
        let workspace_key = WorkspaceRecord::key(agent_id, id)?;
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::workspace_delete_error(error, "workspace delete transaction could not begin")
        })?;
        let snapshot = transaction
            .load(workspace_key.clone())
            .await
            .map_err(|error| {
                Self::workspace_delete_error(error, "workspace could not be loaded for delete")
            })?
            .ok_or_else(|| KitError::not_found("workspace does not exist"))?;
        let workspace = WorkspaceRecord::decode(snapshot, agent_id, Some(id))?;
        if workspace.revision() != expected_revision {
            return Err(KitError::conflict("workspace revision is stale"));
        }
        let routing = Self::workspace_routing_in_transaction(&mut transaction, &workspace).await?;
        let mut bindings = Vec::new();
        for conversation in conversations {
            if transaction
                .load(OpenCodePromptRecord::key(conversation.id())?)
                .await
                .map_err(|error| {
                    Self::workspace_delete_error(
                        error,
                        "prompt state could not be checked for delete",
                    )
                })?
                .is_some()
            {
                return Err(KitError::busy(
                    "workspace contains active or unreconciled prompt work",
                ));
            }
            let binding_key = Self::binding_key(id, conversation.id())?;
            let snapshot = transaction
                .load(binding_key.clone())
                .await
                .map_err(|error| {
                    Self::workspace_delete_error(
                        error,
                        "session binding could not be loaded for delete",
                    )
                })?
                .ok_or_else(|| KitError::invalid_state("session binding is missing"))?;
            let binding = Self::decode_binding(snapshot, id, conversation.id())?;
            binding.verify_owner(&conversation, &workspace, &routing)?;
            bindings.push(binding);
            self.stage_knowledge_forget(
                &mut transaction,
                &KnowledgeOwnerId::Conversation {
                    agent_id: agent_id.clone(),
                    workspace_id: id.clone(),
                    conversation_id: conversation.id().clone(),
                },
            )
            .await
            .map_err(|error| match error {
                AgentStorageKnowledgeError::TransactionConflict(phase) => {
                    KitError::conflict("workspace deletion conflicted")
                        .with_detail(ErrorDetail::TransactionConflict)
                        .in_phase(phase)
                }
                AgentStorageKnowledgeError::Failure(error) => error,
            })?;
            transaction
                .delete(ConversationRecord::key(id, conversation.id())?)
                .await
                .map_err(|error| {
                    Self::workspace_delete_error(error, "conversation could not be deleted")
                })?;
            transaction.delete(binding_key).await.map_err(|error| {
                Self::workspace_delete_error(error, "session binding could not be deleted")
            })?;
            NotificationStore::<S>::delete_in_transaction(&mut transaction, conversation.id())
                .await
                .map_err(NotificationDeleteError::into_sdk)?;
        }
        for key in continuation_keys {
            transaction.delete(key).await.map_err(|error| {
                Self::workspace_delete_error(error, "continuation could not be deleted")
            })?;
        }
        self.stage_knowledge_forget(
            &mut transaction,
            &KnowledgeOwnerId::Workspace {
                agent_id: agent_id.clone(),
                workspace_id: id.clone(),
            },
        )
        .await
        .map_err(|error| match error {
            AgentStorageKnowledgeError::TransactionConflict(phase) => {
                KitError::conflict("workspace deletion conflicted")
                    .with_detail(ErrorDetail::TransactionConflict)
                    .in_phase(phase)
            }
            AgentStorageKnowledgeError::Failure(error) => error,
        })?;
        transaction
            .delete(workspace_key.clone())
            .await
            .map_err(|error| {
                Self::workspace_delete_error(error, "workspace could not be deleted")
            })?;
        transaction
            .delete(OpenCodeWorkspaceRouting::key(agent_id, id)?)
            .await
            .map_err(|error| {
                Self::workspace_delete_error(error, "workspace routing could not be deleted")
            })?;
        match (bindings.is_empty(), cleanup) {
            (true, None) => {}
            (false, Some(cleanup)) if cleanup.matches_workspace(agent_id, id, &bindings) => {}
            _ => {
                return Err(KitError::invalid_state(
                    "workspace cleanup contradicts its durable bindings",
                ));
            }
        }
        if let Some(cleanup) = cleanup {
            transaction
                .put(storage_kit::state::record::StateRecord::new(
                    Self::cleanup_key(cleanup)?,
                    serde_json::to_value(cleanup).map_err(|_| {
                        KitError::invalid_state("cleanup obligation could not be encoded")
                    })?,
                ))
                .await
                .map_err(|error| {
                    Self::workspace_delete_error(error, "cleanup obligation could not be stored")
                })?;
        }
        let mut catalog = Self::catalog_in_transaction(&mut transaction, agent_id).await?;
        catalog.remove(id)?;
        Self::store_catalog_in_transaction(&mut transaction, agent_id, catalog).await?;
        match transaction.commit().await {
            Ok(()) => {}
            Err(error @ storage_kit::StateStoreError::TransactionConflict { .. }) => {
                return Err(Self::workspace_delete_error(
                    error,
                    "workspace delete transaction could not commit",
                ));
            }
            Err(source) => {
                let error = KitError::from_state_store(
                    source,
                    "workspace delete transaction could not commit",
                )
                .in_phase(ErrorPhase::StateCommit);
                let mut last_error = None;
                loop {
                    let observation =
                        tokio::time::timeout_at(deadline, store.load(workspace_key.clone())).await;
                    match observation {
                        Ok(Ok(None)) => break,
                        Ok(Ok(Some(_))) => return Err(error),
                        Ok(Err(source)) => {
                            let secondary = KitError::from_state_store(
                                source,
                                "workspace delete outcome could not be checked",
                            )
                            .in_phase(ErrorPhase::Reconciliation);
                            if tokio::time::Instant::now() < deadline {
                                last_error = Some(secondary);
                                tokio::task::yield_now().await;
                                continue;
                            }
                            self.notifications.invalidate_workspace(id);
                            return Ok(OpenCodeWorkspaceDeleteDisposition::RecoveryRequired(
                                error.with_supplementary(secondary),
                            ));
                        }
                        Err(_) => {
                            self.notifications.invalidate_workspace(id);
                            let timeout = KitError::timeout(
                                "workspace delete reconciliation deadline elapsed",
                            )
                            .at_phase(ErrorPhase::Reconciliation);
                            let timeout = match last_error {
                                Some(attempt) => timeout.with_supplementary(attempt),
                                None => timeout,
                            };
                            return Ok(OpenCodeWorkspaceDeleteDisposition::RecoveryRequired(
                                error.with_supplementary(timeout),
                            ));
                        }
                    }
                }
            }
        }
        self.notifications.invalidate_workspace(id);
        Ok(OpenCodeWorkspaceDeleteDisposition::Settled)
    }

    pub(super) async fn close_conversation(
        &self,
        agent_id: &AgentId,
        expected: &ConversationRecord,
        cleanup: &OpenCodeCleanup,
        deadline: tokio::time::Instant,
        lifecycle_timeout: Duration,
    ) -> Result<OpenCodeConversationMutationDisposition, KitError> {
        let workspace_id = expected.workspace_id();
        let workspace_revision = expected.workspace_revision();
        let id = expected.id();
        let store = self.storage.store();
        let continuations = tokio::time::timeout_at(deadline, self.prompts.continuations(id))
            .await
            .map_err(|_| KitError::timeout("conversation close deadline elapsed"))??;
        if tokio::time::Instant::now() >= deadline {
            return Err(KitError::timeout("conversation close deadline elapsed"));
        }
        let mut transaction = store
            .begin_transaction()
            .await
            .map_err(|error| match error {
                storage_kit::StateStoreError::TransactionConflict { .. } => {
                    KitError::conflict("conversation close transaction conflicted")
                        .with_detail(ErrorDetail::TransactionConflict)
                }
                error => KitError::from_state_store(
                    error,
                    "conversation close transaction could not begin",
                )
                .in_phase(ErrorPhase::StateWrite),
            })?;
        let mutation = async {
            let workspace_key = WorkspaceRecord::key(agent_id, workspace_id)
                .map_err(OpenCodeConversationCloseError::Failure)?;
            let workspace = transaction
                .load(workspace_key)
                .await
                .map_err(|error| {
                    OpenCodeConversationCloseError::storage(
                        error,
                        "workspace could not be loaded for close",
                        ErrorPhase::StateRead,
                    )
                })?
                .ok_or_else(|| {
                    OpenCodeConversationCloseError::Failure(KitError::not_found(
                        "workspace does not exist",
                    ))
                })?;
            let workspace = WorkspaceRecord::decode(workspace, agent_id, Some(workspace_id))
                .map_err(OpenCodeConversationCloseError::Failure)?;
            if workspace.revision() != workspace_revision {
                return Err(OpenCodeConversationCloseError::Failure(KitError::conflict(
                    "workspace handle is stale",
                )));
            }
            let conversation_key = ConversationRecord::key(workspace_id, id)
                .map_err(OpenCodeConversationCloseError::Failure)?;
            let conversation = transaction
                .load(conversation_key.clone())
                .await
                .map_err(|error| {
                    OpenCodeConversationCloseError::storage(
                        error,
                        "conversation could not be loaded for close",
                        ErrorPhase::StateRead,
                    )
                })?
                .ok_or_else(|| {
                    OpenCodeConversationCloseError::Failure(KitError::not_found(
                        "conversation does not exist",
                    ))
                })?;
            let conversation = ConversationRecord::decode(conversation, workspace_id, Some(id))
                .map_err(OpenCodeConversationCloseError::Failure)?;
            if conversation.workspace_revision() != workspace_revision {
                return Err(OpenCodeConversationCloseError::Failure(KitError::conflict(
                    "conversation handle is stale",
                )));
            }
            let prompt_key =
                OpenCodePromptRecord::key(id).map_err(OpenCodeConversationCloseError::Failure)?;
            if transaction
                .load(prompt_key)
                .await
                .map_err(|error| {
                    OpenCodeConversationCloseError::storage(
                        error,
                        "prompt state could not be checked",
                        ErrorPhase::StateRead,
                    )
                })?
                .is_some()
            {
                return Err(OpenCodeConversationCloseError::Failure(KitError::busy(
                    "conversation retains active or unreconciled prompt work",
                )));
            }
            let binding_key = Self::binding_key(workspace_id, id)
                .map_err(OpenCodeConversationCloseError::Failure)?;
            let binding = transaction
                .load(binding_key.clone())
                .await
                .map_err(|error| {
                    OpenCodeConversationCloseError::storage(
                        error,
                        "session binding could not be loaded for close",
                        ErrorPhase::StateRead,
                    )
                })?
                .ok_or_else(|| {
                    OpenCodeConversationCloseError::Failure(KitError::invalid_state(
                        "session binding is missing",
                    ))
                })?;
            let binding = Self::decode_binding(binding, workspace_id, id)
                .map_err(OpenCodeConversationCloseError::Failure)?;
            if !cleanup.matches_conversation(agent_id, id, &binding) {
                return Err(OpenCodeConversationCloseError::Failure(
                    KitError::invalid_state("conversation cleanup contradicts its durable binding"),
                ));
            }
            let cleanup_key =
                Self::cleanup_key(cleanup).map_err(OpenCodeConversationCloseError::Failure)?;
            if transaction
                .load(cleanup_key.clone())
                .await
                .map_err(|error| {
                    OpenCodeConversationCloseError::storage(
                        error,
                        "cleanup obligation could not be checked",
                        ErrorPhase::StateRead,
                    )
                })?
                .is_some()
            {
                return Err(OpenCodeConversationCloseError::Failure(
                    KitError::invalid_state("conversation cleanup obligation already exists"),
                ));
            }
            match self
                .stage_knowledge_forget(
                    &mut transaction,
                    &KnowledgeOwnerId::Conversation {
                        agent_id: agent_id.clone(),
                        workspace_id: workspace_id.clone(),
                        conversation_id: id.clone(),
                    },
                )
                .await
            {
                Ok(()) => {}
                Err(AgentStorageKnowledgeError::TransactionConflict(_)) => {
                    return Err(OpenCodeConversationCloseError::TransactionConflict);
                }
                Err(AgentStorageKnowledgeError::Failure(error)) => {
                    return Err(OpenCodeConversationCloseError::Failure(error));
                }
            }
            transaction
                .delete(conversation_key)
                .await
                .map_err(|error| {
                    OpenCodeConversationCloseError::storage(
                        error,
                        "conversation could not be closed",
                        ErrorPhase::StateWrite,
                    )
                })?;
            transaction.delete(binding_key).await.map_err(|error| {
                OpenCodeConversationCloseError::storage(
                    error,
                    "session binding could not be removed",
                    ErrorPhase::StateWrite,
                )
            })?;
            let cleanup_value = serde_json::to_value(cleanup).map_err(|_| {
                OpenCodeConversationCloseError::Failure(KitError::invalid_state(
                    "cleanup obligation could not be encoded",
                ))
            })?;
            transaction
                .put(storage_kit::state::record::StateRecord::new(
                    cleanup_key,
                    cleanup_value,
                ))
                .await
                .map_err(|error| {
                    OpenCodeConversationCloseError::storage(
                        error,
                        "cleanup obligation could not be stored",
                        ErrorPhase::StateWrite,
                    )
                })?;
            let closure = match self
                .notifications
                .close_in_transaction(&mut transaction, id)
                .await
            {
                Ok(cancelled) => cancelled,
                Err(NotificationCloseError::TransactionConflict(_)) => {
                    return Err(OpenCodeConversationCloseError::TransactionConflict);
                }
                Err(NotificationCloseError::Failure(error)) => {
                    return Err(OpenCodeConversationCloseError::Failure(error));
                }
            };
            if continuations.iter().any(|continuation| {
                continuation.is_unresolved() && !closure.contains(continuation.notification_id())
            }) {
                return Err(OpenCodeConversationCloseError::Failure(KitError::conflict(
                    "interaction resolution won the conversation-close race",
                )));
            }
            for continuation in &continuations {
                let key = OpenCodeContinuation::key(
                    continuation.conversation_id(),
                    continuation.request_id(),
                )
                .map_err(OpenCodeConversationCloseError::Failure)?;
                if !continuation.is_unresolved() || closure.contains(continuation.notification_id())
                {
                    transaction.delete(key).await.map_err(|error| {
                        OpenCodeConversationCloseError::storage(
                            error,
                            "continuation could not be removed",
                            ErrorPhase::StateWrite,
                        )
                    })?;
                }
            }
            Ok((binding, closure))
        }
        .await;
        let (binding, closure) = match mutation {
            Ok(staged) => staged,
            Err(OpenCodeConversationCloseError::TransactionConflict) => {
                let _ = transaction.rollback().await;
                return Err(KitError::conflict(
                    "conversation close transaction conflicted",
                ));
            }
            Err(OpenCodeConversationCloseError::Failure(error)) => {
                let rollback = transaction.rollback().await;
                return Err(match rollback {
                    Ok(()) => error,
                    Err(secondary) => error.with_supplementary(
                        KitError::from_state_store(secondary, "transaction rollback failed")
                            .at_phase(ErrorPhase::Cleanup),
                    ),
                });
            }
        };
        match transaction.commit().await {
            Ok(_) => {
                self.notifications.finish_close(id);
                Ok(OpenCodeConversationMutationDisposition::Settled)
            }
            Err(storage_kit::StateStoreError::TransactionConflict { .. }) => Err(
                KitError::conflict("conversation close transaction conflicted")
                    .with_detail(ErrorDetail::TransactionConflict),
            ),
            Err(source) => {
                let error = KitError::from_state_store(
                    source,
                    "conversation close transaction could not commit",
                )
                .in_phase(ErrorPhase::StateCommit);
                let mut last_error = None;
                let reconciliation_deadline = tokio::time::Instant::now() + lifecycle_timeout;
                loop {
                    let observation = tokio::time::timeout_at(
                        reconciliation_deadline,
                        self.observe_conversation_close(
                            expected,
                            &binding,
                            cleanup,
                            &continuations,
                            &closure,
                        ),
                    )
                    .await;
                    match observation {
                        Ok(Ok(NotificationMutationObservation::Expected)) => {
                            self.notifications.finish_close(id);
                            return Ok(OpenCodeConversationMutationDisposition::Settled);
                        }
                        Ok(Ok(NotificationMutationObservation::Unchanged)) => return Err(error),
                        Ok(Ok(NotificationMutationObservation::Contradictory)) => {
                            self.notifications.invalidate_conversation(id);
                            return Ok(OpenCodeConversationMutationDisposition::RecoveryRequired(
                                error,
                            ));
                        }
                        Ok(Err(secondary))
                            if tokio::time::Instant::now() < reconciliation_deadline =>
                        {
                            last_error = Some(secondary);
                            tokio::task::yield_now().await;
                        }
                        Ok(Err(secondary)) => {
                            self.notifications.invalidate_conversation(id);
                            return Ok(OpenCodeConversationMutationDisposition::RecoveryRequired(
                                error.with_supplementary(
                                    secondary.in_phase(ErrorPhase::Reconciliation),
                                ),
                            ));
                        }
                        Err(_) => {
                            self.notifications.invalidate_conversation(id);
                            let timeout = KitError::timeout(
                                "conversation close reconciliation deadline elapsed",
                            )
                            .at_phase(ErrorPhase::Reconciliation);
                            let timeout = match last_error {
                                Some(attempt) => timeout.with_supplementary(attempt),
                                None => timeout,
                            };
                            return Ok(OpenCodeConversationMutationDisposition::RecoveryRequired(
                                error.with_supplementary(timeout),
                            ));
                        }
                    }
                }
            }
        }
    }

    async fn observe_conversation_close(
        &self,
        expected: &ConversationRecord,
        binding: &OpenCodeSessionBinding,
        cleanup: &OpenCodeCleanup,
        continuations: &[OpenCodeContinuation],
        closure: &NotificationClosure,
    ) -> Result<NotificationMutationObservation, KitError> {
        let store = self.storage.store();
        let conversation = store
            .load(ConversationRecord::key(
                expected.workspace_id(),
                expected.id(),
            )?)
            .await
            .map_err(|error| {
                KitError::from_state_store(error, "conversation close could not be observed")
                    .in_phase(ErrorPhase::StateWrite)
            })?
            .map(|snapshot| {
                ConversationRecord::decode(snapshot, expected.workspace_id(), Some(expected.id()))
            })
            .transpose()?;
        let observed_binding = store
            .load(Self::binding_key(expected.workspace_id(), expected.id())?)
            .await
            .map_err(|error| {
                KitError::from_state_store(error, "session binding close could not be observed")
                    .in_phase(ErrorPhase::StateWrite)
            })?
            .map(|snapshot| Self::decode_binding(snapshot, expected.workspace_id(), expected.id()))
            .transpose()?;
        let cleanup_key = Self::cleanup_key(cleanup)?;
        let observed_cleanup = store
            .load(cleanup_key.clone())
            .await
            .map_err(|error| {
                KitError::from_state_store(error, "cleanup obligation could not be observed")
                    .in_phase(ErrorPhase::StateWrite)
            })?
            .map(|snapshot| {
                let key = snapshot.record.key.key().to_owned();
                let observed: OpenCodeCleanup =
                    serde::Deserialize::deserialize(snapshot.record.value.as_ref())
                        .map_err(|_| KitError::invalid_state("stored cleanup is malformed"))?;
                observed.validate()?;
                if observed.key_fragment() != key || key != cleanup_key.key() {
                    return Err(KitError::invalid_state(
                        "stored cleanup contradicts its key",
                    ));
                }
                Ok(observed)
            })
            .transpose()?;
        let observed_continuations = self.prompts.continuations(expected.id()).await?;
        let notification = closure.observe(store).await?;
        if conversation.is_none()
            && observed_binding.is_none()
            && observed_cleanup.as_ref() == Some(cleanup)
            && observed_continuations.is_empty()
            && notification == NotificationMutationObservation::Expected
        {
            Ok(NotificationMutationObservation::Expected)
        } else if conversation.as_ref() == Some(expected)
            && observed_binding.as_ref() == Some(binding)
            && observed_cleanup.is_none()
            && observed_continuations == continuations
            && notification == NotificationMutationObservation::Unchanged
        {
            Ok(NotificationMutationObservation::Unchanged)
        } else {
            Ok(NotificationMutationObservation::Contradictory)
        }
    }

    pub(crate) async fn bindings_for_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<OpenCodeSessionBinding>, KitError> {
        let workspace = self.workspace(&self.agent_id, workspace_id).await?;
        let routing = self.workspace_routing(&workspace).await?;
        let store = self.storage.store();
        let conversations = self.conversations(workspace_id).await?;
        let mut bindings = Vec::with_capacity(conversations.len());
        for conversation in conversations {
            let binding = Self::stored_binding(
                store,
                workspace_id,
                conversation.id(),
                OpenCodeMissingBinding::InventoryChanged,
            )
            .await?;
            binding.verify_owner(&conversation, &workspace, &routing)?;
            if !binding.is_bound() {
                return Err(KitError::invalid_state("conversation session is not bound"));
            }
            bindings.push(binding);
        }
        Ok(bindings)
    }

    pub(crate) async fn bindings_for_agent(
        &self,
        agent_id: &AgentId,
    ) -> Result<Vec<OpenCodeSessionBinding>, KitError> {
        let workspaces = self.workspaces(agent_id).await?;
        let mut bindings = Vec::new();
        for workspace in workspaces {
            bindings.extend(self.bindings_for_workspace(workspace.id()).await?);
        }
        bindings.sort_by(|left, right| left.session_id().cmp(&right.session_id()));
        Ok(bindings)
    }

    pub(crate) async fn binding_for_session(
        &self,
        agent_id: &AgentId,
        session_id: &str,
    ) -> Result<Option<OpenCodeSessionBinding>, KitError> {
        if session_id.is_empty() || session_id.trim() != session_id {
            return Err(KitError::protocol("event session identity is malformed"));
        }
        Ok(self
            .bindings_for_agent(agent_id)
            .await?
            .into_iter()
            .find(|binding| binding.session_id() == Some(session_id)))
    }

    pub(crate) async fn cleanup_obligations(
        &self,
        agent_id: &AgentId,
    ) -> Result<Vec<OpenCodeCleanup>, KitError> {
        let store = self.storage.store();
        let mut records = Vec::new();
        let mut cursor = None;
        for _ in 0..MAX_STATE_PAGES {
            let prefix =
                storage_kit::state::record::StateKeyPrefix::new(format!("{}:", agent_id.as_str()))
                    .map_err(|_| KitError::invalid_state("cleanup prefix is invalid"))?;
            let mut request =
                storage_kit::state::record::StateListRequest::new(OPENCODE_CLEANUP_NAMESPACE)
                    .map_err(|_| KitError::invalid_state("cleanup request is invalid"))?
                    .with_key_prefix(prefix)
                    .with_limit(storage_kit::state::record::StateListRequest::MAX_LIMIT)
                    .map_err(|_| KitError::invalid_state("cleanup limit is invalid"))?;
            if let Some(next) = cursor.take() {
                request = request
                    .start_after(next)
                    .map_err(|_| KitError::invalid_state("cleanup cursor is invalid"))?;
            }
            let page = store.list(request).await.map_err(|error| {
                KitError::from_state_store(error, "cleanup inventory could not be loaded")
                    .in_phase(ErrorPhase::StateRead)
            })?;
            for snapshot in page.records {
                let key = snapshot.record.key.key().to_owned();
                let cleanup: OpenCodeCleanup =
                    serde::Deserialize::deserialize(snapshot.record.value.as_ref())
                        .map_err(|_| KitError::invalid_state("stored cleanup is malformed"))?;
                cleanup.validate()?;
                if cleanup.agent_id() != agent_id || cleanup.key_fragment() != key {
                    return Err(KitError::invalid_state(
                        "stored cleanup ownership contradicts its key",
                    ));
                }
                records.push(cleanup);
            }
            let Some(next) = page.next_start_after else {
                records.sort_by_key(OpenCodeCleanup::key_fragment);
                return Ok(records);
            };
            cursor = Some(next);
        }
        Err(KitError::invalid_state(
            "cleanup inventory exceeds its private page bound",
        ))
    }

    pub(crate) async fn finish_cleanup(&self, cleanup: &OpenCodeCleanup) -> Result<(), KitError> {
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            KitError::from_state_store(error, "cleanup transaction could not begin")
                .in_phase(ErrorPhase::StateWrite)
        })?;
        let cleanup_key = Self::cleanup_key(cleanup)?;
        let Some(snapshot) = transaction
            .load(cleanup_key.clone())
            .await
            .map_err(|error| {
                KitError::from_state_store(error, "cleanup obligation could not be loaded")
                    .in_phase(ErrorPhase::StateRead)
            })?
        else {
            return Ok(());
        };
        let stored: OpenCodeCleanup =
            serde::Deserialize::deserialize(snapshot.record.value.as_ref())
                .map_err(|_| KitError::invalid_state("stored cleanup is malformed"))?;
        stored.validate()?;
        if &stored != cleanup || stored.key_fragment() != cleanup_key.key() {
            return Err(KitError::invalid_state(
                "cleanup obligation changed before completion",
            ));
        }
        let progress_key = Self::event_progress_key(cleanup.agent_id())?;
        let snapshot = transaction
            .load(progress_key.clone())
            .await
            .map_err(|error| {
                KitError::from_state_store(error, "event progress could not be loaded")
                    .in_phase(ErrorPhase::StateRead)
            })?
            .ok_or_else(|| KitError::invalid_state("event progress is missing"))?;
        let mut progress: OpenCodeEventProgress =
            serde::Deserialize::deserialize(snapshot.record.value.as_ref())
                .map_err(|_| KitError::invalid_state("stored event progress is malformed"))?;
        progress.validate(cleanup.agent_id())?;
        progress.retire_aggregates(cleanup.session_ids())?;
        transaction
            .put(storage_kit::state::record::StateRecord::new(
                progress_key.clone(),
                serde_json::to_value(&progress)
                    .map_err(|_| KitError::invalid_state("event progress could not be encoded"))?,
            ))
            .await
            .map_err(|error| {
                KitError::from_state_store(error, "event progress could not be stored")
                    .in_phase(ErrorPhase::StateWrite)
            })?;
        transaction
            .delete(cleanup_key.clone())
            .await
            .map_err(|error| {
                KitError::from_state_store(error, "cleanup obligation could not be removed")
                    .in_phase(ErrorPhase::StateWrite)
            })?;
        let intended_progress = progress;
        match transaction.commit().await {
            Ok(()) => Ok(()),
            Err(storage_kit::StateStoreError::TransactionConflict { .. }) => {
                Err(KitError::conflict("cleanup transaction conflicted")
                    .with_detail(ErrorDetail::TransactionConflict))
            }
            Err(_) => {
                async {
                    if store
                        .load(cleanup_key)
                        .await
                        .map_err(|error| {
                            KitError::from_state_store(
                                error,
                                "cleanup outcome could not be checked",
                            )
                            .in_phase(ErrorPhase::StateRead)
                        })?
                        .is_some()
                    {
                        return Err(KitError::integration(
                            "cleanup transaction could not commit",
                        ));
                    }
                    let snapshot = store
                        .load(progress_key)
                        .await
                        .map_err(|error| {
                            KitError::from_state_store(
                                error,
                                "cleanup outcome could not be checked",
                            )
                            .in_phase(ErrorPhase::StateRead)
                        })?
                        .ok_or_else(|| KitError::invalid_state("event progress is missing"))?;
                    let observed: OpenCodeEventProgress = serde::Deserialize::deserialize(
                        snapshot.record.value.as_ref(),
                    )
                    .map_err(|_| KitError::invalid_state("stored event progress is malformed"))?;
                    observed.validate(cleanup.agent_id())?;
                    if observed != intended_progress {
                        return Err(KitError::invalid_state("cleanup outcome is contradictory"));
                    }
                    Ok(())
                }
                .await
            }
        }
    }

    fn key(id: &crate::AgentId) -> Result<storage_kit::state::record::StateKey, KitError> {
        storage_kit::state::record::StateKey::new(OPENCODE_ATTACHMENT_NAMESPACE, id.as_str())
            .map_err(|_| KitError::invalid_state("attachment key is invalid"))
    }

    fn catalog_key(id: &AgentId) -> Result<storage_kit::state::record::StateKey, KitError> {
        storage_kit::state::record::StateKey::new(OPENCODE_CATALOG_NAMESPACE, id.as_str())
            .map_err(|_| KitError::invalid_state("catalog state key is invalid"))
    }

    fn event_progress_key(id: &AgentId) -> Result<storage_kit::state::record::StateKey, KitError> {
        storage_kit::state::record::StateKey::new(OPENCODE_EVENT_PROGRESS_NAMESPACE, id.as_str())
            .map_err(|_| KitError::invalid_state("event progress key is invalid"))
    }

    fn decode_catalog(
        snapshot: storage_kit::state::record::StateRecordSnapshot,
        agent_id: &AgentId,
    ) -> Result<OpenCodeCatalogState, KitError> {
        if snapshot.record.key != Self::catalog_key(agent_id)? {
            return Err(KitError::invalid_state(
                "stored catalog state contradicts its key owner",
            ));
        }
        let state: OpenCodeCatalogState =
            serde::Deserialize::deserialize(snapshot.record.value.as_ref())
                .map_err(|_| KitError::invalid_state("stored catalog state is malformed"))?;
        state.validate(agent_id)?;
        Ok(state)
    }

    async fn catalog_in_transaction(
        transaction: &mut S::Transaction,
        agent_id: &AgentId,
    ) -> Result<OpenCodeCatalogState, KitError> {
        let snapshot = transaction
            .load(Self::catalog_key(agent_id)?)
            .await
            .map_err(|error| match error {
                storage_kit::StateStoreError::TransactionConflict { .. } => {
                    KitError::conflict("catalog state transaction conflicted")
                        .with_detail(ErrorDetail::TransactionConflict)
                }
                error => KitError::from_state_store(error, "catalog state could not be loaded")
                    .in_phase(ErrorPhase::StateRead),
            })?
            .ok_or_else(|| KitError::invalid_state("catalog state is missing"))?;
        Self::decode_catalog(snapshot, agent_id)
    }

    async fn store_catalog_in_transaction(
        transaction: &mut S::Transaction,
        agent_id: &AgentId,
        state: OpenCodeCatalogState,
    ) -> Result<(), KitError> {
        state.validate(agent_id)?;
        transaction
            .put(storage_kit::state::record::StateRecord::new(
                Self::catalog_key(agent_id)?,
                serde_json::to_value(state)
                    .map_err(|_| KitError::invalid_state("catalog state could not be encoded"))?,
            ))
            .await
            .map_err(|error| match error {
                storage_kit::StateStoreError::TransactionConflict { .. } => {
                    KitError::conflict("catalog state transaction conflicted")
                        .with_detail(ErrorDetail::TransactionConflict)
                }
                error => KitError::from_state_store(error, "catalog state could not be stored")
                    .in_phase(ErrorPhase::StateWrite),
            })
    }

    async fn stored_binding(
        store: &S,
        workspace_id: &WorkspaceId,
        id: &ConversationId,
        missing: OpenCodeMissingBinding,
    ) -> Result<OpenCodeSessionBinding, KitError> {
        let snapshot = store
            .load(Self::binding_key(workspace_id, id)?)
            .await
            .map_err(|error| {
                KitError::from_state_store(error, "session binding could not be loaded")
                    .in_phase(ErrorPhase::StateRead)
            })?
            .ok_or_else(|| missing.error())?;
        Self::decode_binding(snapshot, workspace_id, id)
    }

    fn binding_key(
        workspace_id: &WorkspaceId,
        id: &ConversationId,
    ) -> Result<storage_kit::state::record::StateKey, KitError> {
        storage_kit::state::record::StateKey::new(
            OPENCODE_SESSION_NAMESPACE,
            format!("{}:{}", workspace_id.as_str(), id.as_str()),
        )
        .map_err(|_| KitError::invalid_state("session binding key is invalid"))
    }

    fn decode_binding(
        snapshot: storage_kit::state::record::StateRecordSnapshot,
        workspace_id: &WorkspaceId,
        id: &ConversationId,
    ) -> Result<OpenCodeSessionBinding, KitError> {
        let key = snapshot.record.key.key().to_owned();
        let binding: OpenCodeSessionBinding =
            serde::Deserialize::deserialize(snapshot.record.value.as_ref())
                .map_err(|_| KitError::invalid_state("stored session binding is malformed"))?;
        binding.validate()?;
        if binding.workspace_id() != workspace_id || binding.conversation_id() != id {
            return Err(KitError::invalid_state(
                "stored session binding has a foreign owner",
            ));
        }
        if Self::binding_key(workspace_id, id)?.key() != key {
            return Err(KitError::invalid_state(
                "stored session binding contradicts its key",
            ));
        }
        Ok(binding)
    }

    fn cleanup_key(
        cleanup: &OpenCodeCleanup,
    ) -> Result<storage_kit::state::record::StateKey, KitError> {
        storage_kit::state::record::StateKey::new(
            OPENCODE_CLEANUP_NAMESPACE,
            cleanup.key_fragment(),
        )
        .map_err(|_| KitError::invalid_state("cleanup key is invalid"))
    }

    fn continuation_state(settlement: ResolutionSettlement) -> OpenCodeContinuationState {
        match settlement {
            ResolutionSettlement::Accepted => OpenCodeContinuationState::Accepted,
            ResolutionSettlement::Rejected => OpenCodeContinuationState::Pending,
            ResolutionSettlement::Ambiguous => OpenCodeContinuationState::Ambiguous,
            ResolutionSettlement::Cancelled => OpenCodeContinuationState::Cancelled,
        }
    }

    fn terminal_settlement(state: OpenCodeContinuationState) -> Option<ResolutionSettlement> {
        match state {
            OpenCodeContinuationState::Accepted => Some(ResolutionSettlement::Accepted),
            OpenCodeContinuationState::Cancelled
            | OpenCodeContinuationState::CancellationRequired
            | OpenCodeContinuationState::CancellationDispatched { .. } => {
                Some(ResolutionSettlement::Cancelled)
            }
            OpenCodeContinuationState::Pending
            | OpenCodeContinuationState::Ambiguous
            | OpenCodeContinuationState::Closing { .. } => None,
        }
    }
}
