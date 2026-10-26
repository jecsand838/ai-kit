mod mutation;
mod provisioning;
pub(crate) mod records;
mod terminal_drain;

pub(crate) use terminal_drain::CustomTerminalDrain;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use storage_sdk::{DatabaseBackend, StateStoreBackend, StateTransactionBackend};
use tokio::time::{Instant, timeout_at};

use crate::agent_storage::{
    AgentStorageClaim, AgentStorageCreationError, AgentStorageKnowledgeError,
};
use crate::backends::custom::knowledge_base::CustomKnowledgeBase;
use crate::core::notification_store::{
    NotificationCloseError, NotificationClosure, NotificationDeleteError,
    NotificationInitializationError, NotificationInteractionCheckError,
    NotificationMutationObservation, NotificationReconciliation, NotificationSettlementError,
    NotificationStore, ResolutionDisposition, ResolutionIntent, ResolutionSettlement,
};
use crate::core::state::{ConversationRecord, WorkspaceRecord};
use crate::{
    AgentId, AgentInfo, AgentStorage, ConversationId, KnowledgeOwnerId, NewWorkspace,
    NotificationId, NotificationResolution, SdkError, UpdateWorkspaceRequest, WorkspaceId,
    WorkspaceInfo,
};

use mutation::CustomMutationOwner;
use provisioning::CustomProvisioningOwners;
use records::{
    CustomAttachment, CustomCancellationIntent, CustomCompatibilityFloor, CustomContinuation,
    CustomContinuationDisposition, CustomGeneration,
};
use terminal_drain::CustomTerminalDrains;

pub(crate) struct CustomBuildClaimEscrow {
    claim: Mutex<CustomBuildClaimState>,
}

enum CustomBuildClaimState {
    Held(AgentStorageClaim),
    Reclaimed,
}

pub(crate) struct CustomStateStore<S: StateStoreBackend, D: DatabaseBackend> {
    agent_id: AgentId,
    storage: AgentStorage<S>,
    knowledge_enabled: bool,
    notifications: NotificationStore<S>,
    prompt_permits: Arc<tokio::sync::Semaphore>,
    default_prompt_timeout: Option<Duration>,
    instructions: Option<String>,
    mutations: Arc<CustomMutationOwner>,
    provisionings: Arc<CustomProvisioningOwners>,
    terminal_drains: Arc<CustomTerminalDrains<S, D>>,
    build_claim: Arc<CustomBuildClaimEscrow>,
}

pub(crate) enum CustomCascadeCancellation {
    Active(u64),
    Quiescent,
    Terminal,
}

enum CustomCascadeAttempt {
    Retry,
    Complete(Result<CustomCascadeCancellation, SdkError>),
}

enum CustomConversationCreationAttempt {
    Retry,
    Complete(Result<ConversationRecord, SdkError>),
}

struct CustomConversationCreation<'a, S: StateStoreBackend> {
    store: &'a S,
    record: &'a ConversationRecord,
    continuation: &'a CustomContinuation,
    conversation_key: &'a storage_sdk::state::record::StateKey,
    continuation_key: &'a storage_sdk::state::record::StateKey,
    conversation_value: &'a serde_json::Value,
    continuation_value: &'a serde_json::Value,
}

enum CustomConversationPromotionAttempt {
    Retry,
    Complete(Result<(), SdkError>),
}

#[derive(Clone, Copy)]
enum CustomProvisioningCleanup {
    Failed,
    Undelivered,
}

struct CustomReopenObservation {
    closing: bool,
    unresolved_acceptance: bool,
}

#[derive(Clone)]
pub(crate) struct CustomTerminalAttempt {
    state: CustomTerminalAttemptState,
}

#[derive(Clone)]
enum CustomTerminalAttemptState {
    Failure,
    Output { identity: [u8; 16], text: String },
}

impl CustomTerminalAttempt {
    pub(crate) fn new(final_text: Option<String>) -> Self {
        let state = match final_text {
            Some(text) => CustomTerminalAttemptState::Output {
                identity: uuid::Uuid::new_v4().into_bytes(),
                text,
            },
            None => CustomTerminalAttemptState::Failure,
        };
        Self { state }
    }

    pub(super) fn text(&self) -> Option<&str> {
        match &self.state {
            CustomTerminalAttemptState::Failure => None,
            CustomTerminalAttemptState::Output { text, .. } => Some(text),
        }
    }

    pub(super) fn identity(&self) -> Option<&[u8; 16]> {
        match &self.state {
            CustomTerminalAttemptState::Failure => None,
            CustomTerminalAttemptState::Output { identity, .. } => Some(identity),
        }
    }
}

impl CustomBuildClaimEscrow {
    pub(crate) fn new(claim: AgentStorageClaim) -> Self {
        Self {
            claim: Mutex::new(CustomBuildClaimState::Held(claim)),
        }
    }

    pub(crate) fn reclaim(&self) -> Result<AgentStorageClaim, SdkError> {
        let mut claim = self
            .claim
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match std::mem::replace(&mut *claim, CustomBuildClaimState::Reclaimed) {
            CustomBuildClaimState::Held(claim) => Ok(claim),
            CustomBuildClaimState::Reclaimed => {
                Err(SdkError::invalid_state("agent build claim escrow is empty"))
            }
        }
    }
}

impl<S, D> Clone for CustomStateStore<S, D>
where
    S: StateStoreBackend,
    D: DatabaseBackend,
{
    fn clone(&self) -> Self {
        Self {
            agent_id: self.agent_id.clone(),
            storage: self.storage.clone(),
            knowledge_enabled: self.knowledge_enabled,
            notifications: self.notifications.clone(),
            prompt_permits: self.prompt_permits.clone(),
            default_prompt_timeout: self.default_prompt_timeout,
            instructions: self.instructions.clone(),
            mutations: self.mutations.clone(),
            provisionings: self.provisionings.clone(),
            terminal_drains: self.terminal_drains.clone(),
            build_claim: self.build_claim.clone(),
        }
    }
}

impl<S, D> CustomStateStore<S, D>
where
    S: StateStoreBackend,
    D: DatabaseBackend,
{
    fn with_commit_evidence(error: SdkError, commit_error: Option<&SdkError>) -> SdkError {
        match commit_error {
            Some(commit) => error.with_supplementary(commit.clone()),
            None => error,
        }
    }

    fn workspace_update_error(
        error: storage_sdk::StateStoreError,
        integration_message: &'static str,
        phase: crate::error::ErrorPhase,
    ) -> SdkError {
        match error {
            storage_sdk::StateStoreError::TransactionConflict { .. } => {
                SdkError::conflict("workspace update conflicted")
                    .with_detail(crate::error::ErrorDetail::TransactionConflict)
                    .in_phase(phase)
            }
            error => SdkError::from_state_store(error, integration_message).in_phase(phase),
        }
    }

    fn workspace_delete_error(
        error: storage_sdk::StateStoreError,
        integration_message: &'static str,
        phase: crate::error::ErrorPhase,
    ) -> SdkError {
        match error {
            storage_sdk::StateStoreError::TransactionConflict { .. } => {
                SdkError::conflict("workspace deletion conflicted")
                    .with_detail(crate::error::ErrorDetail::TransactionConflict)
                    .in_phase(phase)
            }
            error => SdkError::from_state_store(error, integration_message).in_phase(phase),
        }
    }

    pub(crate) fn new(
        agent_id: AgentId,
        storage: AgentStorage<S>,
        knowledge_enabled: bool,
        prompt_limit: usize,
        default_prompt_timeout: Option<Duration>,
        instructions: Option<String>,
        build_claim: Arc<CustomBuildClaimEscrow>,
    ) -> Result<Self, SdkError> {
        let notifications = NotificationStore::<S>::new(storage.clone())?;
        Ok(Self {
            agent_id,
            notifications,
            storage,
            knowledge_enabled,
            prompt_permits: Arc::new(tokio::sync::Semaphore::new(prompt_limit)),
            default_prompt_timeout,
            instructions,
            mutations: Arc::new(CustomMutationOwner::new()),
            provisionings: Arc::new(CustomProvisioningOwners::new()),
            terminal_drains: Arc::new(CustomTerminalDrains::new()),
            build_claim,
        })
    }

    pub(crate) fn notifications(&self) -> &NotificationStore<S> {
        &self.notifications
    }

    pub(crate) fn terminal_drain(
        &self,
        record: &ConversationRecord,
        knowledge: CustomKnowledgeBase<S, D>,
        lifecycle_timeout: Duration,
    ) -> CustomTerminalDrain<S, D> {
        CustomTerminalDrain::new(self.clone(), record, knowledge, lifecycle_timeout)
    }

    pub(crate) fn default_prompt_timeout(&self) -> Option<Duration> {
        self.default_prompt_timeout
    }

    pub(crate) fn instructions(&self) -> Option<&str> {
        self.instructions.as_deref()
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

    pub(crate) fn prompt_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, SdkError> {
        self.prompt_permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| SdkError::busy("agent prompt concurrency is saturated"))
    }

    pub(crate) async fn settle_mutations_before(&self, deadline: Instant) -> Result<(), SdkError> {
        self.mutations.settle_before(deadline, None).await
    }

    pub(crate) fn begin_conversation_provisioning(
        &self,
        workspace_id: &WorkspaceId,
        conversation_id: &ConversationId,
    ) -> Result<(), SdkError> {
        self.provisionings.begin(workspace_id, conversation_id)
    }

    pub(crate) fn finish_conversation_provisioning(
        &self,
        workspace_id: &WorkspaceId,
        conversation_id: &ConversationId,
    ) {
        self.provisionings.finish(workspace_id, conversation_id);
    }

    pub(crate) fn require_conversation_provisioning_recovery(
        &self,
        workspace_id: &WorkspaceId,
        conversation_id: &ConversationId,
    ) {
        self.provisionings
            .require_recovery(workspace_id, conversation_id);
    }

    pub(crate) async fn settle_conversation_provisioning_before(
        &self,
        workspace_id: &WorkspaceId,
        deadline: Instant,
    ) -> Result<(), SdkError> {
        self.provisionings
            .settle_before(Some(workspace_id), deadline)
            .await
    }

    pub(crate) async fn settle_all_owners_before(&self, deadline: Instant) -> Result<(), SdkError> {
        let mut first_error = None;
        if let Err(error) = self.provisionings.settle_before(None, deadline).await {
            SdkError::record_cleanup(&mut first_error, error);
        }
        if let Err(error) = self.mutations.settle_before(deadline, None).await {
            SdkError::record_cleanup(&mut first_error, error);
        }
        if let Err(error) = self.notifications.seal_reclamations_before(deadline).await {
            SdkError::record_cleanup(&mut first_error, error);
        }
        if let Err(error) = self.terminal_drains.settle_all_before(deadline).await {
            SdkError::record_cleanup(&mut first_error, error);
        }
        first_error.map_or(Ok(()), Err)
    }

    pub(crate) async fn settle_terminal_drains_before(
        &self,
        workspace_id: &WorkspaceId,
        deadline: Instant,
    ) -> Result<(), SdkError> {
        self.terminal_drains
            .settle_before(workspace_id, deadline)
            .await
    }

    pub(crate) async fn ensure_quiescent_open_before(
        &self,
        conversation_id: ConversationId,
        deadline: Instant,
    ) -> Result<(), SdkError> {
        let state = self.clone();
        self.mutations
            .run_before(deadline, Some(deadline), async move {
                state.ensure_quiescent_open(&conversation_id).await
            })
            .await
    }

    pub(crate) async fn request_generation_cancellation_before(
        &self,
        conversation_id: ConversationId,
        generation: u64,
        intent: CustomCancellationIntent,
        deadline: Instant,
    ) -> Result<bool, SdkError> {
        let state = self.clone();
        self.mutations
            .run_before(deadline, Some(deadline), async move {
                state
                    .request_generation_cancellation(&conversation_id, generation, intent, deadline)
                    .await
            })
            .await
    }

    pub(crate) async fn request_active_generation_cancellation_before(
        &self,
        conversation_id: ConversationId,
        intent: CustomCancellationIntent,
        deadline: Instant,
    ) -> Result<Option<u64>, SdkError> {
        let state = self.clone();
        self.mutations
            .run_before(deadline, None, async move {
                state
                    .request_active_generation_cancellation(&conversation_id, intent, deadline)
                    .await
            })
            .await
    }

    pub(crate) async fn request_cascade_cancellation_before(
        &self,
        expected: ConversationRecord,
        intent: CustomCancellationIntent,
        deadline: Instant,
    ) -> Result<CustomCascadeCancellation, SdkError> {
        let state = self.clone();
        self.mutations
            .run_before(deadline, Some(deadline), async move {
                state
                    .request_cascade_cancellation(&expected, intent, deadline)
                    .await
            })
            .await
    }

    pub(crate) async fn begin_close_before(
        &self,
        conversation_id: ConversationId,
        deadline: Instant,
    ) -> Result<Option<u64>, SdkError> {
        let state = self.clone();
        self.mutations
            .run_before(deadline, None, async move {
                state.begin_close(&conversation_id, deadline).await
            })
            .await
    }

    pub(crate) async fn cancel_interactions_before<F>(
        &self,
        conversation_id: ConversationId,
        deadline: Instant,
        observation_deadline: Option<Instant>,
        completion: F,
    ) -> Result<(), SdkError>
    where
        F: FnOnce() + Send + 'static,
    {
        let state = self.clone();
        self.mutations
            .run_before(deadline, observation_deadline, async move {
                state.cancel_interactions(&conversation_id).await?;
                completion();
                Ok(())
            })
            .await
    }

    pub(crate) async fn close_conversation_before(
        &self,
        workspace_id: WorkspaceId,
        workspace_revision: u64,
        conversation_id: ConversationId,
        knowledge: CustomKnowledgeBase<S, D>,
        deadline: Instant,
        observation_deadline: Option<Instant>,
    ) -> Result<(), SdkError> {
        let state = self.clone();
        let stage_workspace = workspace_id.clone();
        let stage_conversation = conversation_id.clone();
        let notification_closure = self
            .mutations
            .run_before(deadline, observation_deadline, async move {
                state
                    .stage_conversation_close(
                        &stage_workspace,
                        workspace_revision,
                        &stage_conversation,
                    )
                    .await
            })
            .await?;
        if notification_closure.is_some() {
            self.notifications.finish_close(&conversation_id);
        }
        if notification_closure
            .as_ref()
            .is_some_and(|closure| closure.retains_delivery())
        {
            return Ok(());
        }
        let owner = KnowledgeOwnerId::Conversation {
            agent_id: self.agent_id.clone(),
            workspace_id: workspace_id.clone(),
            conversation_id: conversation_id.clone(),
        };
        knowledge
            .settle_forget_before(owner, deadline, observation_deadline)
            .await?;
        if notification_closure.is_none() {
            return Ok(());
        }
        let state = self.clone();
        self.mutations
            .run_before(deadline, observation_deadline, async move {
                state
                    .remove_closed_conversation(&workspace_id, workspace_revision, &conversation_id)
                    .await
            })
            .await
    }

    pub(crate) async fn create_before(
        &self,
        info: AgentInfo,
        compatibility: CustomCompatibilityFloor,
        deadline: Instant,
    ) -> Result<(), SdkError> {
        let state = self.clone();
        let build_claim = Arc::clone(&self.build_claim);
        self.mutations
            .run_before(deadline, Some(deadline), async move {
                let outcome = state.create(&info, compatibility, deadline).await;
                drop(build_claim);
                outcome
            })
            .await
    }

    pub(crate) async fn create(
        &self,
        info: &AgentInfo,
        compatibility: CustomCompatibilityFloor,
        deadline: Instant,
    ) -> Result<(), SdkError> {
        if info.id() != &self.agent_id {
            return Err(SdkError::invalid_state(
                "agent state owner contradicts the requested record",
            ));
        }
        let attachment = CustomAttachment::new(info.id().clone(), compatibility)?;
        let expected_attachment = attachment.clone();
        let key = CustomAttachment::key(info.id())?;
        let attachment_value = attachment.encode()?;
        let store = self.storage.store();
        loop {
            if Instant::now() >= deadline {
                return Err(SdkError::timeout("agent state update deadline elapsed"));
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
                        "agent state update could not begin",
                    )
                    .in_phase(crate::error::ErrorPhase::StateWrite));
                }
            };
            let mutation = async {
                if transaction
                    .load(key.clone())
                    .await
                    .map_err(|error| match error {
                        storage_sdk::StateStoreError::TransactionConflict { .. } => {
                            AgentStorageCreationError::TransactionConflict
                        }
                        error => AgentStorageCreationError::Failure(
                            SdkError::from_state_store(error, "agent state could not be checked")
                                .in_phase(crate::error::ErrorPhase::StateRead),
                        ),
                    })?
                    .is_some()
                {
                    return Err(AgentStorageCreationError::Failure(SdkError::conflict(
                        "agent state already exists",
                    )));
                }
                self.storage
                    .create_agent_in_transaction(&mut transaction, info)
                    .await?;
                transaction
                    .put(storage_sdk::state::record::StateRecord::new(
                        key.clone(),
                        attachment_value.clone(),
                    ))
                    .await
                    .map_err(|error| match error {
                        storage_sdk::StateStoreError::TransactionConflict { .. } => {
                            AgentStorageCreationError::TransactionConflict
                        }
                        error => AgentStorageCreationError::Failure(
                            SdkError::from_state_store(error, "agent state could not be stored")
                                .in_phase(crate::error::ErrorPhase::StateWrite),
                        ),
                    })
            }
            .await;
            match mutation {
                Ok(()) => {}
                Err(AgentStorageCreationError::TransactionConflict) => {
                    let _ = transaction.rollback().await;
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(AgentStorageCreationError::Failure(error)) => {
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
            }
            let commit_result = transaction.commit().await;
            let commit_error = match commit_result {
                Ok(_) => return Ok(()),
                Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(error) => {
                    SdkError::from_state_store(error, "agent state update could not commit")
                        .in_phase(crate::error::ErrorPhase::StateCommit)
                }
            };
            let identity = match self.storage.agent_info(self.agent_id.clone()).await {
                Ok(identity) => Some(identity),
                Err(error) if error.kind() == crate::error::ErrorKind::NotFound => None,
                Err(error) => return Err(error.with_supplementary(commit_error)),
            };
            let attachment = self
                .attachment()
                .await
                .map_err(|error| error.with_supplementary(commit_error.clone()))?;
            match (identity, attachment) {
                (None, None) => return Err(commit_error),
                (Some(identity), Some(attachment))
                    if identity == *info && attachment == expected_attachment =>
                {
                    return Ok(());
                }
                (Some(_), Some(_)) | (Some(_), None) | (None, Some(_)) => {
                    return Err(SdkError::invalid_state(
                        "agent identity commit outcome is contradictory",
                    )
                    .with_supplementary(commit_error));
                }
            }
        }
    }

    async fn attachment(&self) -> Result<Option<CustomAttachment>, SdkError> {
        let store = self.storage.store();
        store
            .load(CustomAttachment::key(&self.agent_id)?)
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "agent state could not be observed")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })?
            .map(|snapshot| CustomAttachment::decode(snapshot, &self.agent_id))
            .transpose()
    }

    pub(crate) async fn verify(
        &self,
        compatibility: &CustomCompatibilityFloor,
    ) -> Result<(), SdkError> {
        let store = self.storage.store();
        let snapshot = store
            .load(CustomAttachment::key(&self.agent_id)?)
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "agent state could not be loaded")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })?
            .ok_or_else(|| {
                SdkError::conflict("stored agent configuration is incompatible")
                    .with_detail(crate::error::ErrorDetail::AttachmentMismatch)
            })?;
        let expected = CustomAttachment::new(self.agent_id.clone(), compatibility.clone())?;
        let actual = CustomAttachment::decode(snapshot, &self.agent_id)?;
        if actual != expected {
            return Err(SdkError::conflict(
                "current configuration differs from stored agent state",
            )
            .with_detail(crate::error::ErrorDetail::AttachmentMismatch));
        }
        Ok(())
    }

    pub(crate) async fn reconcile_reopen(
        &self,
        knowledge: &CustomKnowledgeBase<S, D>,
        deadline: Instant,
    ) -> Result<(), SdkError> {
        let workspaces = timeout_at(deadline, self.workspaces())
            .await
            .map_err(|_| SdkError::timeout("workspace recovery inventory timed out"))??;
        for workspace in workspaces {
            knowledge
                .verify_before(knowledge.workspace_owner(workspace.id()), deadline)
                .await?;
            let conversations = timeout_at(deadline, self.conversations(workspace.id()))
                .await
                .map_err(|_| SdkError::timeout("conversation recovery inventory timed out"))??;
            for conversation in conversations {
                let state = self.clone();
                let recovery_conversation = conversation.clone();
                let observation = self
                    .mutations
                    .run_before(deadline, Some(deadline), async move {
                        state
                            .reconcile_reopen_conversation(&recovery_conversation)
                            .await
                    })
                    .await?;
                if observation.unresolved_acceptance {
                    self.notifications.wake_receivers();
                    return Err(SdkError::busy(
                        "interaction settlement is still in progress",
                    ));
                }
                if observation.closing {
                    self.notifications.finish_reconciliation(conversation.id());
                    self.close_conversation_before(
                        workspace.id().clone(),
                        workspace.revision(),
                        conversation.id().clone(),
                        knowledge.clone(),
                        deadline,
                        Some(deadline),
                    )
                    .await?;
                    continue;
                }
                knowledge
                    .verify_before(
                        knowledge.conversation_owner(workspace.id(), conversation.id()),
                        deadline,
                    )
                    .await?;
                self.notifications.finish_reconciliation(conversation.id());
            }
        }
        Ok(())
    }

    async fn reconcile_reopen_conversation(
        &self,
        conversation: &ConversationRecord,
    ) -> Result<CustomReopenObservation, SdkError> {
        let key = CustomContinuation::key(conversation.id())?;
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            SdkError::from_state_store(error, "recovery state update could not begin")
                .in_phase(crate::error::ErrorPhase::StateWrite)
        })?;
        let snapshot = transaction
            .load(key.clone())
            .await
            .map_err(|error| {
                SdkError::from_state_store(
                    error,
                    "conversation state could not be loaded for recovery",
                )
                .in_phase(crate::error::ErrorPhase::StateRead)
            })?
            .ok_or_else(|| SdkError::invalid_state("conversation state is missing"))?;
        let before = CustomContinuation::decode(snapshot, conversation.id())?;
        let mut continuation = before.clone();
        let continuation_changed = continuation.reconcile_reopen()?;
        let closing = continuation.is_closing();
        if continuation_changed {
            transaction
                .put(storage_sdk::state::record::StateRecord::new(
                    key,
                    continuation.encode()?,
                ))
                .await
                .map_err(|error| {
                    SdkError::from_state_store(
                        error,
                        "recovered conversation state could not be stored",
                    )
                    .in_phase(crate::error::ErrorPhase::StateWrite)
                })?;
        }
        let notification_reconciliation = if closing {
            NotificationStore::<S>::reconcile_closing_interactions_in_transaction(
                &mut transaction,
                conversation.id(),
            )
            .await?
        } else {
            NotificationStore::<S>::reconcile_lost_interactions_in_transaction(
                &mut transaction,
                conversation.id(),
            )
            .await?
        };
        let unresolved_acceptance = notification_reconciliation.unresolved_acceptance();
        let commit_error = transaction.commit().await.err().map(|error| {
            SdkError::from_state_store(error, "state update could not commit")
                .in_phase(crate::error::ErrorPhase::StateCommit)
        });
        self.observe_reopen_commit(
            store,
            conversation.id(),
            &before,
            &continuation,
            &notification_reconciliation,
            commit_error,
        )
        .await?;
        Ok(CustomReopenObservation {
            closing,
            unresolved_acceptance,
        })
    }

    async fn observe_reopen_commit(
        &self,
        store: &S,
        conversation_id: &ConversationId,
        before: &CustomContinuation,
        expected: &CustomContinuation,
        notifications: &NotificationReconciliation,
        commit_error: Option<SdkError>,
    ) -> Result<(), SdkError> {
        let observed = self
            .continuation_optional(store, conversation_id)
            .await
            .map_err(|error| Self::with_commit_evidence(error, commit_error.as_ref()))?;
        let notification_observation = notifications
            .observe(store)
            .await
            .map_err(|error| Self::with_commit_evidence(error, commit_error.as_ref()))?;
        if observed.as_ref() == Some(expected)
            && matches!(
                notification_observation,
                NotificationMutationObservation::Expected
            )
        {
            return Ok(());
        }
        let continuation_unchanged = observed.as_ref() == Some(before);
        let notifications_unchanged = if notifications.changes_state() {
            matches!(
                notification_observation,
                NotificationMutationObservation::Unchanged
            )
        } else {
            matches!(
                notification_observation,
                NotificationMutationObservation::Expected
            )
        };
        if continuation_unchanged && notifications_unchanged {
            return match commit_error {
                None => Err(SdkError::invalid_state(
                    "recovery state update was not durably observed",
                )),
                Some(error) => Err(error),
            };
        }
        Err(Self::with_commit_evidence(
            SdkError::invalid_state("recovery state commit outcome is contradictory"),
            commit_error.as_ref(),
        ))
    }

    pub(crate) async fn create_workspace(
        &self,
        request: &NewWorkspace,
    ) -> Result<WorkspaceRecord, SdkError> {
        let record = WorkspaceRecord::new(self.agent_id.clone(), WorkspaceId::new(), request);
        let key = WorkspaceRecord::key(&self.agent_id, record.id())?;
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            SdkError::from_state_store(error, "workspace state update could not begin")
                .in_phase(crate::error::ErrorPhase::StateWrite)
        })?;
        if transaction
            .load(key.clone())
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "workspace state could not be checked")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })?
            .is_some()
        {
            return Err(SdkError::conflict("workspace identity already exists"));
        }
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                key,
                record.encode()?,
            ))
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "workspace state could not be stored")
                    .in_phase(crate::error::ErrorPhase::StateWrite)
            })?;
        let commit_error = transaction.commit().await.err().map(|error| {
            SdkError::from_state_store(error, "state update could not commit")
                .in_phase(crate::error::ErrorPhase::StateCommit)
        });
        self.observe_workspace_creation(store, &record, commit_error)
            .await?;
        Ok(record)
    }

    async fn observe_workspace_creation(
        &self,
        store: &S,
        expected: &WorkspaceRecord,
        commit_error: Option<SdkError>,
    ) -> Result<(), SdkError> {
        match self
            .workspace_record(store, expected.id())
            .await
            .map_err(|error| Self::with_commit_evidence(error, commit_error.as_ref()))?
        {
            Some(observed) if observed == *expected => Ok(()),
            None if commit_error.is_none() => Err(SdkError::invalid_state(
                "workspace creation was not durably observed",
            )),
            None => Err(commit_error.unwrap_or_else(|| {
                SdkError::invalid_state("workspace commit evidence is missing")
            })),
            Some(_) => Err(Self::with_commit_evidence(
                SdkError::invalid_state("workspace creation commit outcome is contradictory"),
                commit_error.as_ref(),
            )),
        }
    }

    pub(crate) async fn workspaces(&self) -> Result<Vec<WorkspaceRecord>, SdkError> {
        let store = self.storage.store();
        WorkspaceRecord::list(store, &self.agent_id).await
    }

    pub(crate) async fn workspace(&self, id: &WorkspaceId) -> Result<WorkspaceRecord, SdkError> {
        let store = self.storage.store();
        let snapshot = store
            .load(WorkspaceRecord::key(&self.agent_id, id)?)
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "workspace state could not be loaded")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })?
            .ok_or_else(|| SdkError::not_found("workspace does not exist"))?;
        WorkspaceRecord::decode(snapshot, &self.agent_id, Some(id))
    }

    pub(crate) async fn create_conversation_before(
        &self,
        record: ConversationRecord,
        deadline: Instant,
    ) -> Result<ConversationRecord, SdkError> {
        let state = self.clone();
        self.mutations
            .run_before(deadline, Some(deadline), async move {
                state.create_conversation(&record, deadline).await
            })
            .await
    }

    async fn create_conversation(
        &self,
        record: &ConversationRecord,
        deadline: Instant,
    ) -> Result<ConversationRecord, SdkError> {
        let conversation_key = ConversationRecord::key(record.workspace_id(), record.id())?;
        let continuation = CustomContinuation::provisioning(record.id().clone());
        let continuation_key = CustomContinuation::key(record.id())?;
        let conversation_value = record.encode()?;
        let continuation_value = continuation.encode()?;
        let store = self.storage.store();
        let creation = CustomConversationCreation {
            store,
            record,
            continuation: &continuation,
            conversation_key: &conversation_key,
            continuation_key: &continuation_key,
            conversation_value: &conversation_value,
            continuation_value: &continuation_value,
        };
        loop {
            if Instant::now() >= deadline {
                return Err(SdkError::timeout(
                    "conversation creation conflict did not settle before its deadline",
                ));
            }
            match self.create_conversation_attempt(&creation).await {
                CustomConversationCreationAttempt::Retry => {
                    self.yield_for_continuation_retry(deadline).await?;
                }
                CustomConversationCreationAttempt::Complete(outcome) => return outcome,
            }
        }
    }

    async fn create_conversation_attempt(
        &self,
        creation: &CustomConversationCreation<'_, S>,
    ) -> CustomConversationCreationAttempt {
        let store = creation.store;
        let record = creation.record;
        let continuation = creation.continuation;
        let conversation_key = creation.conversation_key;
        let continuation_key = creation.continuation_key;
        let conversation_value = creation.conversation_value;
        let continuation_value = creation.continuation_value;
        let transaction = store.begin_transaction().await;
        let mut transaction = match transaction {
            Ok(transaction) => transaction,
            Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                return CustomConversationCreationAttempt::Retry;
            }
            Err(error) => {
                return CustomConversationCreationAttempt::Complete(Err(
                    SdkError::from_state_store(error, "conversation state update could not begin")
                        .in_phase(crate::error::ErrorPhase::StateWrite),
                ));
            }
        };
        let workspace_key = match WorkspaceRecord::key(&self.agent_id, record.workspace_id()) {
            Ok(key) => key,
            Err(error) => {
                return self
                    .close_conversation_creation_attempt(
                        transaction,
                        CustomConversationCreationAttempt::Complete(Err(error)),
                    )
                    .await;
            }
        };
        let workspace = match transaction.load(workspace_key).await {
            Ok(Some(workspace)) => workspace,
            Ok(None) => {
                return self
                    .close_conversation_creation_attempt(
                        transaction,
                        CustomConversationCreationAttempt::Complete(Err(SdkError::not_found(
                            "workspace does not exist",
                        ))),
                    )
                    .await;
            }
            Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                return self
                    .close_conversation_creation_attempt(
                        transaction,
                        CustomConversationCreationAttempt::Retry,
                    )
                    .await;
            }
            Err(error) => {
                return self
                    .close_conversation_creation_attempt(
                        transaction,
                        CustomConversationCreationAttempt::Complete(Err(
                            SdkError::from_state_store(
                                error,
                                "workspace state could not be checked for conversation",
                            )
                            .in_phase(crate::error::ErrorPhase::StateRead),
                        )),
                    )
                    .await;
            }
        };
        let workspace =
            match WorkspaceRecord::decode(workspace, &self.agent_id, Some(record.workspace_id())) {
                Ok(workspace) => workspace,
                Err(error) => {
                    return self
                        .close_conversation_creation_attempt(
                            transaction,
                            CustomConversationCreationAttempt::Complete(Err(error)),
                        )
                        .await;
                }
            };
        if workspace.revision() != record.workspace_revision() {
            return self
                .close_conversation_creation_attempt(
                    transaction,
                    CustomConversationCreationAttempt::Complete(Err(SdkError::conflict(
                        "workspace handle is stale",
                    )
                    .with_detail(crate::error::ErrorDetail::StaleRevision))),
                )
                .await;
        }
        let conversation = match transaction.load(conversation_key.clone()).await {
            Ok(conversation) => conversation,
            Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                return self
                    .close_conversation_creation_attempt(
                        transaction,
                        CustomConversationCreationAttempt::Retry,
                    )
                    .await;
            }
            Err(error) => {
                return self
                    .close_conversation_creation_attempt(
                        transaction,
                        CustomConversationCreationAttempt::Complete(Err(
                            SdkError::from_state_store(
                                error,
                                "conversation state could not be checked",
                            )
                            .in_phase(crate::error::ErrorPhase::StateRead),
                        )),
                    )
                    .await;
            }
        };
        let stored_continuation = match transaction.load(continuation_key.clone()).await {
            Ok(continuation) => continuation,
            Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                return self
                    .close_conversation_creation_attempt(
                        transaction,
                        CustomConversationCreationAttempt::Retry,
                    )
                    .await;
            }
            Err(error) => {
                return self
                    .close_conversation_creation_attempt(
                        transaction,
                        CustomConversationCreationAttempt::Complete(Err(
                            SdkError::from_state_store(
                                error,
                                "conversation history could not be checked",
                            )
                            .in_phase(crate::error::ErrorPhase::StateRead),
                        )),
                    )
                    .await;
            }
        };
        if conversation.is_some() || stored_continuation.is_some() {
            return self
                .close_conversation_creation_attempt(
                    transaction,
                    CustomConversationCreationAttempt::Complete(Err(SdkError::conflict(
                        "conversation identity already exists",
                    ))),
                )
                .await;
        }
        match transaction
            .put(storage_sdk::state::record::StateRecord::new(
                conversation_key.clone(),
                conversation_value.clone(),
            ))
            .await
        {
            Ok(()) => {}
            Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                return self
                    .close_conversation_creation_attempt(
                        transaction,
                        CustomConversationCreationAttempt::Retry,
                    )
                    .await;
            }
            Err(error) => {
                return self
                    .close_conversation_creation_attempt(
                        transaction,
                        CustomConversationCreationAttempt::Complete(Err(
                            SdkError::from_state_store(
                                error,
                                "conversation state could not be stored",
                            )
                            .in_phase(crate::error::ErrorPhase::StateWrite),
                        )),
                    )
                    .await;
            }
        }
        match transaction
            .put(storage_sdk::state::record::StateRecord::new(
                continuation_key.clone(),
                continuation_value.clone(),
            ))
            .await
        {
            Ok(()) => {}
            Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                return self
                    .close_conversation_creation_attempt(
                        transaction,
                        CustomConversationCreationAttempt::Retry,
                    )
                    .await;
            }
            Err(error) => {
                return self
                    .close_conversation_creation_attempt(
                        transaction,
                        CustomConversationCreationAttempt::Complete(Err(
                            SdkError::from_state_store(
                                error,
                                "conversation history could not be stored",
                            )
                            .in_phase(crate::error::ErrorPhase::StateWrite),
                        )),
                    )
                    .await;
            }
        }
        match NotificationStore::<S>::initialize_in_transaction(
            &mut transaction,
            record.workspace_id(),
            record.id(),
        )
        .await
        {
            Ok(()) => {}
            Err(NotificationInitializationError::TransactionConflict) => {
                return self
                    .close_conversation_creation_attempt(
                        transaction,
                        CustomConversationCreationAttempt::Retry,
                    )
                    .await;
            }
            Err(NotificationInitializationError::Failure(error)) => {
                return self
                    .close_conversation_creation_attempt(
                        transaction,
                        CustomConversationCreationAttempt::Complete(Err(error)),
                    )
                    .await;
            }
        }
        let commit_result = transaction.commit().await;
        let commit_error = match commit_result {
            Ok(_) => None,
            Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                return CustomConversationCreationAttempt::Retry;
            }
            Err(error) => Some(
                SdkError::from_state_store(error, "conversation creation could not commit")
                    .in_phase(crate::error::ErrorPhase::StateCommit),
            ),
        };
        let observed_conversation = store.load(conversation_key.clone()).await.map_err(|error| {
            SdkError::from_state_store(error, "conversation state could not be observed")
                .in_phase(crate::error::ErrorPhase::StateRead)
        });
        let observed_conversation = match observed_conversation {
            Ok(observed) => observed,
            Err(error) => {
                return CustomConversationCreationAttempt::Complete(Err(
                    Self::with_commit_evidence(error, commit_error.as_ref()),
                ));
            }
        }
        .map(|snapshot| {
            ConversationRecord::decode(snapshot, record.workspace_id(), Some(record.id()))
        })
        .transpose();
        let observed_conversation = match observed_conversation {
            Ok(observed) => observed,
            Err(error) => {
                return CustomConversationCreationAttempt::Complete(Err(
                    Self::with_commit_evidence(error, commit_error.as_ref()),
                ));
            }
        };
        let observed_continuation = store.load(continuation_key.clone()).await.map_err(|error| {
            SdkError::from_state_store(error, "conversation history could not be observed")
                .in_phase(crate::error::ErrorPhase::StateRead)
        });
        let observed_continuation = match observed_continuation {
            Ok(observed) => observed,
            Err(error) => {
                return CustomConversationCreationAttempt::Complete(Err(
                    Self::with_commit_evidence(error, commit_error.as_ref()),
                ));
            }
        }
        .map(|snapshot| CustomContinuation::decode(snapshot, record.id()))
        .transpose();
        let observed_continuation = match observed_continuation {
            Ok(observed) => observed,
            Err(error) => {
                return CustomConversationCreationAttempt::Complete(Err(
                    Self::with_commit_evidence(error, commit_error.as_ref()),
                ));
            }
        };
        let notification_initialized = match NotificationStore::<S>::initialization_matches(
            store,
            record.workspace_id(),
            record.id(),
        )
        .await
        {
            Ok(initialized) => initialized,
            Err(error) => {
                return CustomConversationCreationAttempt::Complete(Err(
                    Self::with_commit_evidence(error, commit_error.as_ref()),
                ));
            }
        };
        let outcome = match (
            observed_conversation,
            observed_continuation,
            notification_initialized,
        ) {
            (Some(observed_conversation), Some(observed_continuation), true)
                if observed_conversation == *record && observed_continuation == *continuation =>
            {
                Ok(record.clone())
            }
            (None, None, false) => Err(commit_error.unwrap_or_else(|| {
                SdkError::invalid_state("conversation state update was not durably observed")
            })),
            _ => Err(Self::with_commit_evidence(
                SdkError::conflict("conversation state changed while creation was reconciling"),
                commit_error.as_ref(),
            )),
        };
        CustomConversationCreationAttempt::Complete(outcome)
    }

    async fn close_conversation_creation_attempt(
        &self,
        transaction: S::Transaction,
        attempt: CustomConversationCreationAttempt,
    ) -> CustomConversationCreationAttempt {
        match transaction.rollback().await {
            Ok(_) => attempt,
            Err(error @ storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                match attempt {
                    CustomConversationCreationAttempt::Complete(Err(primary)) => {
                        CustomConversationCreationAttempt::Complete(Err(primary
                            .with_supplementary(
                                SdkError::from_state_store(
                                    error,
                                    "state transaction could not close",
                                )
                                .at_phase(crate::error::ErrorPhase::Cleanup),
                            )))
                    }
                    CustomConversationCreationAttempt::Retry
                    | CustomConversationCreationAttempt::Complete(Ok(_)) => {
                        CustomConversationCreationAttempt::Retry
                    }
                }
            }
            Err(error) => match attempt {
                CustomConversationCreationAttempt::Complete(Err(primary)) => {
                    CustomConversationCreationAttempt::Complete(Err(primary.with_supplementary(
                        SdkError::from_state_store(error, "state transaction could not close")
                            .at_phase(crate::error::ErrorPhase::Cleanup),
                    )))
                }
                CustomConversationCreationAttempt::Retry
                | CustomConversationCreationAttempt::Complete(Ok(_)) => {
                    CustomConversationCreationAttempt::Complete(Err(SdkError::from_state_store(
                        error,
                        "conversation creation transaction could not close",
                    )
                    .at_phase(crate::error::ErrorPhase::Cleanup)))
                }
            },
        }
    }

    pub(crate) async fn discard_provisioning_before(
        &self,
        expected: ConversationRecord,
        knowledge: CustomKnowledgeBase<S, D>,
        deadline: Instant,
    ) -> Result<(), SdkError> {
        let state = self.clone();
        let owner = knowledge.conversation_owner(expected.workspace_id(), expected.id());
        self.mutations
            .run_before(deadline, Some(deadline), async move {
                state
                    .discard_provisioning(&expected, CustomProvisioningCleanup::Failed)
                    .await
            })
            .await?;
        knowledge
            .settle_forget_before(owner, deadline, Some(deadline))
            .await
    }

    pub(crate) async fn discard_undelivered_conversation_before(
        &self,
        expected: ConversationRecord,
        knowledge: CustomKnowledgeBase<S, D>,
        deadline: Instant,
    ) -> Result<(), SdkError> {
        let state = self.clone();
        let owner = knowledge.conversation_owner(expected.workspace_id(), expected.id());
        self.mutations
            .run_before(deadline, Some(deadline), async move {
                state
                    .discard_provisioning(&expected, CustomProvisioningCleanup::Undelivered)
                    .await
            })
            .await?;
        knowledge
            .settle_forget_before(owner, deadline, Some(deadline))
            .await
    }

    async fn discard_provisioning(
        &self,
        expected: &ConversationRecord,
        cleanup: CustomProvisioningCleanup,
    ) -> Result<(), SdkError> {
        let owner = KnowledgeOwnerId::Conversation {
            agent_id: self.agent_id.clone(),
            workspace_id: expected.workspace_id().clone(),
            conversation_id: expected.id().clone(),
        };
        let conversation_key = ConversationRecord::key(expected.workspace_id(), expected.id())?;
        let continuation_key = CustomContinuation::key(expected.id())?;
        let expected_continuation = CustomContinuation::provisioning(expected.id().clone());
        let mut expected_promoted = expected_continuation.clone();
        expected_promoted.promote()?;
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            SdkError::from_state_store(error, "conversation cleanup could not begin")
                .in_phase(crate::error::ErrorPhase::StateWrite)
        })?;
        let conversation = transaction
            .load(conversation_key.clone())
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "conversation state could not be loaded")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })?;
        let continuation = transaction
            .load(continuation_key.clone())
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "conversation history could not be loaded")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })?;
        let (conversation, continuation) = match (conversation, continuation) {
            (Some(conversation), Some(continuation)) => (
                ConversationRecord::decode(
                    conversation,
                    expected.workspace_id(),
                    Some(expected.id()),
                )?,
                CustomContinuation::decode(continuation, expected.id())?,
            ),
            (None, None) => {
                drop(transaction);
                if !NotificationStore::<S>::initialization_is_absent(store, expected.id()).await? {
                    return Err(SdkError::invalid_state(
                        "conversation provisioning cleanup is incomplete",
                    ));
                }
                return Ok(());
            }
            (Some(_), None) | (None, Some(_)) => {
                return Err(SdkError::invalid_state(
                    "conversation provisioning state is incomplete",
                ));
            }
        };
        if conversation == *expected
            && matches!(cleanup, CustomProvisioningCleanup::Failed)
            && continuation.is_reopenable()
        {
            drop(transaction);
            return Ok(());
        }
        let continuation_matches = continuation == expected_continuation
            || matches!(cleanup, CustomProvisioningCleanup::Undelivered)
                && continuation == expected_promoted;
        if conversation != *expected || !continuation_matches {
            return Err(SdkError::conflict(
                "conversation changed during provisioning cleanup",
            ));
        }
        self.stage_knowledge_forget(&mut transaction, &owner)
            .await
            .map_err(AgentStorageKnowledgeError::into_sdk)?;
        NotificationStore::<S>::delete_initialization_in_transaction(
            &mut transaction,
            expected.workspace_id(),
            expected.id(),
        )
        .await?;
        transaction
            .delete(conversation_key.clone())
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "conversation state could not be deleted")
                    .in_phase(crate::error::ErrorPhase::StateWrite)
            })?;
        transaction
            .delete(continuation_key.clone())
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "conversation history could not be deleted")
                    .in_phase(crate::error::ErrorPhase::StateWrite)
            })?;
        let commit_error = transaction.commit().await.err().map(|error| {
            SdkError::from_state_store(error, "conversation cleanup could not commit")
                .in_phase(crate::error::ErrorPhase::StateCommit)
        });
        let conversation_absent = store
            .load(conversation_key)
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "conversation cleanup could not be observed")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })
            .map_err(|error| Self::with_commit_evidence(error, commit_error.as_ref()))?
            .is_none();
        let continuation_absent = store
            .load(continuation_key)
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "conversation cleanup could not be observed")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })
            .map_err(|error| Self::with_commit_evidence(error, commit_error.as_ref()))?
            .is_none();
        let notification_absent =
            NotificationStore::<S>::initialization_is_absent(store, expected.id())
                .await
                .map_err(|error| Self::with_commit_evidence(error, commit_error.as_ref()))?;
        if !(conversation_absent && continuation_absent && notification_absent) {
            return Err(commit_error.unwrap_or_else(|| {
                SdkError::conflict("conversation cleanup could not be confirmed")
            }));
        }
        self.notifications.finish_close(expected.id());
        Ok(())
    }

    pub(crate) async fn promote_conversation_before(
        &self,
        expected: ConversationRecord,
        deadline: Instant,
    ) -> Result<(), SdkError> {
        let state = self.clone();
        self.mutations
            .run_before(deadline, Some(deadline), async move {
                state.promote_conversation(&expected, deadline).await
            })
            .await
    }

    async fn promote_conversation(
        &self,
        expected: &ConversationRecord,
        deadline: Instant,
    ) -> Result<(), SdkError> {
        let continuation_key = CustomContinuation::key(expected.id())?;
        let store = self.storage.store();
        loop {
            if Instant::now() >= deadline {
                return Err(SdkError::timeout(
                    "conversation setup conflict did not settle before its deadline",
                ));
            }
            match self
                .promote_conversation_attempt(store, expected, &continuation_key)
                .await
            {
                CustomConversationPromotionAttempt::Retry => {
                    self.yield_for_continuation_retry(deadline).await?;
                }
                CustomConversationPromotionAttempt::Complete(outcome) => return outcome,
            }
        }
    }

    async fn promote_conversation_attempt(
        &self,
        store: &S,
        expected: &ConversationRecord,
        continuation_key: &storage_sdk::state::record::StateKey,
    ) -> CustomConversationPromotionAttempt {
        let transaction = store.begin_transaction().await;
        let mut transaction = match transaction {
            Ok(transaction) => transaction,
            Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                return CustomConversationPromotionAttempt::Retry;
            }
            Err(error) => {
                return CustomConversationPromotionAttempt::Complete(Err(
                    SdkError::from_state_store(error, "conversation setup could not begin")
                        .in_phase(crate::error::ErrorPhase::StateWrite),
                ));
            }
        };
        let workspace_key = match WorkspaceRecord::key(&self.agent_id, expected.workspace_id()) {
            Ok(key) => key,
            Err(error) => {
                return self
                    .close_conversation_promotion_attempt(
                        transaction,
                        CustomConversationPromotionAttempt::Complete(Err(error)),
                    )
                    .await;
            }
        };
        let workspace = match transaction.load(workspace_key).await {
            Ok(Some(workspace)) => workspace,
            Ok(None) => {
                return self
                    .close_conversation_promotion_attempt(
                        transaction,
                        CustomConversationPromotionAttempt::Complete(Err(SdkError::not_found(
                            "workspace does not exist",
                        ))),
                    )
                    .await;
            }
            Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                return self
                    .close_conversation_promotion_attempt(
                        transaction,
                        CustomConversationPromotionAttempt::Retry,
                    )
                    .await;
            }
            Err(error) => {
                return self
                    .close_conversation_promotion_attempt(
                        transaction,
                        CustomConversationPromotionAttempt::Complete(Err(
                            SdkError::from_state_store(
                                error,
                                "workspace state could not be loaded",
                            )
                            .in_phase(crate::error::ErrorPhase::StateRead),
                        )),
                    )
                    .await;
            }
        };
        let workspace =
            match WorkspaceRecord::decode(workspace, &self.agent_id, Some(expected.workspace_id()))
            {
                Ok(workspace) => workspace,
                Err(error) => {
                    return self
                        .close_conversation_promotion_attempt(
                            transaction,
                            CustomConversationPromotionAttempt::Complete(Err(error)),
                        )
                        .await;
                }
            };
        if workspace.revision() != expected.workspace_revision() {
            return self
                .close_conversation_promotion_attempt(
                    transaction,
                    CustomConversationPromotionAttempt::Complete(Err(SdkError::conflict(
                        "workspace handle is stale",
                    )
                    .with_detail(crate::error::ErrorDetail::StaleRevision))),
                )
                .await;
        }
        let conversation_key = match ConversationRecord::key(expected.workspace_id(), expected.id())
        {
            Ok(key) => key,
            Err(error) => {
                return self
                    .close_conversation_promotion_attempt(
                        transaction,
                        CustomConversationPromotionAttempt::Complete(Err(error)),
                    )
                    .await;
            }
        };
        let conversation = match transaction.load(conversation_key).await {
            Ok(Some(conversation)) => conversation,
            Ok(None) => {
                return self
                    .close_conversation_promotion_attempt(
                        transaction,
                        CustomConversationPromotionAttempt::Complete(Err(SdkError::not_found(
                            "conversation does not exist",
                        ))),
                    )
                    .await;
            }
            Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                return self
                    .close_conversation_promotion_attempt(
                        transaction,
                        CustomConversationPromotionAttempt::Retry,
                    )
                    .await;
            }
            Err(error) => {
                return self
                    .close_conversation_promotion_attempt(
                        transaction,
                        CustomConversationPromotionAttempt::Complete(Err(
                            SdkError::from_state_store(
                                error,
                                "conversation state could not be loaded",
                            )
                            .in_phase(crate::error::ErrorPhase::StateRead),
                        )),
                    )
                    .await;
            }
        };
        let conversation = match ConversationRecord::decode(
            conversation,
            expected.workspace_id(),
            Some(expected.id()),
        ) {
            Ok(conversation) => conversation,
            Err(error) => {
                return self
                    .close_conversation_promotion_attempt(
                        transaction,
                        CustomConversationPromotionAttempt::Complete(Err(error)),
                    )
                    .await;
            }
        };
        if &conversation != expected {
            return self
                .close_conversation_promotion_attempt(
                    transaction,
                    CustomConversationPromotionAttempt::Complete(Err(SdkError::conflict(
                        "conversation changed before setup completed",
                    ))),
                )
                .await;
        }
        let continuation = match transaction.load(continuation_key.clone()).await {
            Ok(Some(continuation)) => continuation,
            Ok(None) => {
                return self
                    .close_conversation_promotion_attempt(
                        transaction,
                        CustomConversationPromotionAttempt::Complete(Err(SdkError::invalid_state(
                            "conversation state is missing",
                        ))),
                    )
                    .await;
            }
            Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                return self
                    .close_conversation_promotion_attempt(
                        transaction,
                        CustomConversationPromotionAttempt::Retry,
                    )
                    .await;
            }
            Err(error) => {
                return self
                    .close_conversation_promotion_attempt(
                        transaction,
                        CustomConversationPromotionAttempt::Complete(Err(
                            SdkError::from_state_store(
                                error,
                                "conversation state could not be loaded",
                            )
                            .in_phase(crate::error::ErrorPhase::StateRead),
                        )),
                    )
                    .await;
            }
        };
        let before = match CustomContinuation::decode(continuation, expected.id()) {
            Ok(continuation) => continuation,
            Err(error) => {
                return self
                    .close_conversation_promotion_attempt(
                        transaction,
                        CustomConversationPromotionAttempt::Complete(Err(error)),
                    )
                    .await;
            }
        };
        let mut promoted = before.clone();
        if let Err(error) = promoted.promote() {
            return self
                .close_conversation_promotion_attempt(
                    transaction,
                    CustomConversationPromotionAttempt::Complete(Err(error)),
                )
                .await;
        }
        match NotificationStore::<S>::check_no_unresolved_interaction_in_transaction(
            &mut transaction,
            expected.id(),
        )
        .await
        {
            Ok(()) => {}
            Err(NotificationInteractionCheckError::TransactionConflict) => {
                return self
                    .close_conversation_promotion_attempt(
                        transaction,
                        CustomConversationPromotionAttempt::Retry,
                    )
                    .await;
            }
            Err(NotificationInteractionCheckError::Failure(error)) => {
                return self
                    .close_conversation_promotion_attempt(
                        transaction,
                        CustomConversationPromotionAttempt::Complete(Err(error)),
                    )
                    .await;
            }
        }
        let promoted_value = match promoted.encode() {
            Ok(value) => value,
            Err(error) => {
                return self
                    .close_conversation_promotion_attempt(
                        transaction,
                        CustomConversationPromotionAttempt::Complete(Err(error)),
                    )
                    .await;
            }
        };
        match transaction
            .put(storage_sdk::state::record::StateRecord::new(
                continuation_key.clone(),
                promoted_value,
            ))
            .await
        {
            Ok(()) => {}
            Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                return self
                    .close_conversation_promotion_attempt(
                        transaction,
                        CustomConversationPromotionAttempt::Retry,
                    )
                    .await;
            }
            Err(error) => {
                return self
                    .close_conversation_promotion_attempt(
                        transaction,
                        CustomConversationPromotionAttempt::Complete(Err(
                            SdkError::from_state_store(
                                error,
                                "conversation setup could not be stored",
                            )
                            .in_phase(crate::error::ErrorPhase::StateWrite),
                        )),
                    )
                    .await;
            }
        }
        let commit_result = transaction.commit().await;
        if matches!(
            &commit_result,
            Err(storage_sdk::StateStoreError::TransactionConflict { .. })
        ) {
            return CustomConversationPromotionAttempt::Retry;
        }
        let commit_error = commit_result.err().map(|error| {
            SdkError::from_state_store(error, "conversation setup could not commit")
                .in_phase(crate::error::ErrorPhase::StateCommit)
        });
        let observed = match self.continuation_optional(store, expected.id()).await {
            Ok(observed) => observed,
            Err(error) => {
                return CustomConversationPromotionAttempt::Complete(Err(
                    Self::with_commit_evidence(error, commit_error.as_ref()),
                ));
            }
        };
        let outcome = match observed {
            Some(observed) if observed == promoted => Ok(()),
            Some(observed) if observed == before => match commit_error {
                None => Err(SdkError::invalid_state(
                    "conversation setup was not durably observed",
                )),
                Some(error) => Err(error),
            },
            Some(_) | None => Err(Self::with_commit_evidence(
                SdkError::conflict("conversation state changed while setup was reconciling"),
                commit_error.as_ref(),
            )),
        };
        CustomConversationPromotionAttempt::Complete(outcome)
    }

    async fn close_conversation_promotion_attempt(
        &self,
        transaction: S::Transaction,
        attempt: CustomConversationPromotionAttempt,
    ) -> CustomConversationPromotionAttempt {
        match transaction.rollback().await {
            Ok(_) => attempt,
            Err(error @ storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                match attempt {
                    CustomConversationPromotionAttempt::Complete(Err(primary)) => {
                        CustomConversationPromotionAttempt::Complete(Err(primary
                            .with_supplementary(
                                SdkError::from_state_store(
                                    error,
                                    "state transaction could not close",
                                )
                                .at_phase(crate::error::ErrorPhase::Cleanup),
                            )))
                    }
                    CustomConversationPromotionAttempt::Retry
                    | CustomConversationPromotionAttempt::Complete(Ok(())) => {
                        CustomConversationPromotionAttempt::Retry
                    }
                }
            }
            Err(error) => match attempt {
                CustomConversationPromotionAttempt::Complete(Err(primary)) => {
                    CustomConversationPromotionAttempt::Complete(Err(primary.with_supplementary(
                        SdkError::from_state_store(error, "state transaction could not close")
                            .at_phase(crate::error::ErrorPhase::Cleanup),
                    )))
                }
                CustomConversationPromotionAttempt::Retry
                | CustomConversationPromotionAttempt::Complete(Ok(())) => {
                    CustomConversationPromotionAttempt::Complete(Err(SdkError::from_state_store(
                        error,
                        "conversation setup transaction could not close",
                    )
                    .at_phase(crate::error::ErrorPhase::Cleanup)))
                }
            },
        }
    }

    pub(crate) async fn conversations(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<ConversationRecord>, SdkError> {
        let store = self.storage.store();
        ConversationRecord::list(store, workspace_id).await
    }

    pub(crate) async fn reopenable_conversations(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<ConversationRecord>, SdkError> {
        let store = self.storage.store();
        let records = ConversationRecord::list(store, workspace_id).await?;
        let mut reopenable = Vec::with_capacity(records.len());
        for record in records {
            let continuation = self.continuation(store, record.id()).await?;
            if continuation.is_reopenable() {
                reopenable.push(record);
            }
        }
        Ok(reopenable)
    }

    pub(crate) async fn conversation(
        &self,
        workspace_id: &WorkspaceId,
        id: &ConversationId,
    ) -> Result<ConversationRecord, SdkError> {
        let store = self.storage.store();
        let snapshot = store
            .load(ConversationRecord::key(workspace_id, id)?)
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "conversation state could not be loaded")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })?
            .ok_or_else(|| SdkError::not_found("conversation does not exist"))?;
        let record = ConversationRecord::decode(snapshot, workspace_id, Some(id))?;
        let continuation = self.continuation(store, id).await?;
        if continuation.is_provisioning() {
            return Err(SdkError::not_found("conversation does not exist"));
        }
        if !continuation.is_reopenable() {
            return Err(SdkError::closed("conversation is closing"));
        }
        Ok(record)
    }

    pub(crate) async fn ensure_quiescent_open(
        &self,
        conversation_id: &ConversationId,
    ) -> Result<(), SdkError> {
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            SdkError::from_state_store(error, "conversation reopen check could not begin")
                .in_phase(crate::error::ErrorPhase::StateWrite)
        })?;
        let snapshot = transaction
            .load(CustomContinuation::key(conversation_id)?)
            .await
            .map_err(|error| match error {
                storage_sdk::StateStoreError::TransactionConflict { .. } => {
                    SdkError::conflict("conversation state transaction conflicted")
                        .with_detail(crate::error::ErrorDetail::TransactionConflict)
                }
                error => {
                    SdkError::from_state_store(error, "conversation state could not be checked")
                        .in_phase(crate::error::ErrorPhase::StateRead)
                }
            })?
            .ok_or_else(|| SdkError::invalid_state("conversation state is missing"))?;
        CustomContinuation::decode(snapshot, conversation_id)?.ensure_quiescent_open()?;
        NotificationStore::<S>::ensure_no_unresolved_interaction_in_transaction(
            &mut transaction,
            conversation_id,
        )
        .await?;
        transaction.commit().await.map(|_| ()).map_err(|error| {
            SdkError::from_state_store(error, "conversation reopen check could not commit")
                .in_phase(crate::error::ErrorPhase::StateCommit)
        })
    }

    pub(crate) async fn start_generation(
        &self,
        conversation_id: &ConversationId,
        caller_text: String,
        structured: Option<&crate::StructuredOutputRequest>,
        tool_selection: &crate::ToolSelection,
    ) -> Result<CustomGeneration, SdkError> {
        let key = CustomContinuation::key(conversation_id)?;
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            SdkError::from_state_store(error, "prompt state update could not begin")
                .in_phase(crate::error::ErrorPhase::StateWrite)
        })?;
        let snapshot = transaction
            .load(key.clone())
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "conversation state could not be loaded")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })?
            .ok_or_else(|| SdkError::invalid_state("conversation state is missing"))?;
        let before = CustomContinuation::decode(snapshot, conversation_id)?;
        let mut expected = before.clone();
        NotificationStore::<S>::ensure_no_unresolved_interaction_in_transaction(
            &mut transaction,
            conversation_id,
        )
        .await?;
        let generation = expected.begin(caller_text, structured, tool_selection)?;
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                key,
                expected.encode()?,
            ))
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "prompt state could not be stored")
                    .in_phase(crate::error::ErrorPhase::StateWrite)
            })?;
        let commit_error = transaction.commit().await.err().map(|error| {
            SdkError::from_state_store(error, "state update could not commit")
                .in_phase(crate::error::ErrorPhase::StateCommit)
        });
        let observed = self
            .continuation(store, conversation_id)
            .await
            .map_err(|error| Self::with_commit_evidence(error, commit_error.as_ref()))?;
        if observed == expected {
            return Ok(generation);
        }
        if observed == before {
            return match commit_error {
                None => Err(SdkError::invalid_state(
                    "prompt state update was not durably observed",
                )),
                Some(error) => Err(error),
            };
        }
        Err(Self::with_commit_evidence(
            SdkError::conflict("conversation state changed while prompt admission was reconciling"),
            commit_error.as_ref(),
        ))
    }

    pub(crate) async fn mark_generation_accepted(
        &self,
        conversation_id: &ConversationId,
        generation: u64,
        deadline: Instant,
    ) -> Result<(), SdkError> {
        self.mutate_continuation(conversation_id, deadline, |continuation| {
            continuation.accept(generation)
        })
        .await
    }

    pub(crate) async fn finish_generation(
        &self,
        conversation_id: &ConversationId,
        generation: u64,
        attempt: CustomTerminalAttempt,
        deadline: Instant,
    ) -> Result<CustomContinuationDisposition, SdkError> {
        let mut disposition = CustomContinuationDisposition::Ready;
        self.mutate_continuation(conversation_id, deadline, |continuation| {
            disposition = continuation.finish(generation, &attempt)?;
            Ok(())
        })
        .await?;
        Ok(disposition)
    }

    pub(crate) async fn request_generation_cancellation(
        &self,
        conversation_id: &ConversationId,
        generation: u64,
        intent: CustomCancellationIntent,
        deadline: Instant,
    ) -> Result<bool, SdkError> {
        let mut active = false;
        self.mutate_continuation(conversation_id, deadline, |continuation| {
            active = continuation.request_cancellation(generation, intent)?;
            Ok(())
        })
        .await?;
        Ok(active)
    }

    pub(crate) async fn request_active_generation_cancellation(
        &self,
        conversation_id: &ConversationId,
        intent: CustomCancellationIntent,
        deadline: Instant,
    ) -> Result<Option<u64>, SdkError> {
        let mut generation = None;
        self.mutate_continuation(conversation_id, deadline, |continuation| {
            generation = continuation.request_active_cancellation(intent)?;
            Ok(())
        })
        .await?;
        Ok(generation)
    }

    pub(crate) async fn request_cascade_cancellation(
        &self,
        expected: &ConversationRecord,
        intent: CustomCancellationIntent,
        deadline: Instant,
    ) -> Result<CustomCascadeCancellation, SdkError> {
        let conversation_key = ConversationRecord::key(expected.workspace_id(), expected.id())?;
        let continuation_key = CustomContinuation::key(expected.id())?;
        let store = self.storage.store();
        loop {
            if Instant::now() >= deadline {
                return Err(SdkError::timeout(
                    "conversation cancellation conflict did not settle before its deadline",
                ));
            }
            let attempt = self
                .request_cascade_cancellation_attempt(
                    store,
                    expected,
                    intent,
                    &conversation_key,
                    &continuation_key,
                )
                .await;
            match attempt {
                CustomCascadeAttempt::Retry => {
                    self.yield_for_continuation_retry(deadline).await?;
                }
                CustomCascadeAttempt::Complete(outcome) => return outcome,
            }
        }
    }

    async fn request_cascade_cancellation_attempt(
        &self,
        store: &S,
        expected: &ConversationRecord,
        intent: CustomCancellationIntent,
        conversation_key: &storage_sdk::state::record::StateKey,
        continuation_key: &storage_sdk::state::record::StateKey,
    ) -> CustomCascadeAttempt {
        let mut transaction = match store.begin_transaction().await {
            Ok(transaction) => transaction,
            Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                return CustomCascadeAttempt::Retry;
            }
            Err(error) => {
                return CustomCascadeAttempt::Complete(Err(SdkError::from_state_store(
                    error,
                    "conversation state update could not begin",
                )
                .in_phase(crate::error::ErrorPhase::StateWrite)));
            }
        };
        let conversation = match transaction.load(conversation_key.clone()).await {
            Ok(conversation) => conversation,
            Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                return self
                    .close_cascade_attempt(transaction, CustomCascadeAttempt::Retry)
                    .await;
            }
            Err(error) => {
                return self
                    .close_cascade_attempt(
                        transaction,
                        CustomCascadeAttempt::Complete(Err(SdkError::from_state_store(
                            error,
                            "conversation state could not be loaded",
                        )
                        .in_phase(crate::error::ErrorPhase::StateRead))),
                    )
                    .await;
            }
        };
        let continuation = match transaction.load(continuation_key.clone()).await {
            Ok(continuation) => continuation,
            Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                return self
                    .close_cascade_attempt(transaction, CustomCascadeAttempt::Retry)
                    .await;
            }
            Err(error) => {
                return self
                    .close_cascade_attempt(
                        transaction,
                        CustomCascadeAttempt::Complete(Err(SdkError::from_state_store(
                            error,
                            "conversation state could not be loaded",
                        )
                        .in_phase(crate::error::ErrorPhase::StateRead))),
                    )
                    .await;
            }
        };
        let (conversation, continuation) = match (conversation, continuation) {
            (Some(conversation), Some(continuation)) => (conversation, continuation),
            (None, None) => {
                return self
                    .close_cascade_attempt(
                        transaction,
                        CustomCascadeAttempt::Complete(Ok(CustomCascadeCancellation::Terminal)),
                    )
                    .await;
            }
            (Some(_), None) | (None, Some(_)) => {
                return self
                    .close_cascade_attempt(
                        transaction,
                        CustomCascadeAttempt::Complete(Err(SdkError::invalid_state(
                            "conversation state is incomplete",
                        ))),
                    )
                    .await;
            }
        };
        let conversation = match ConversationRecord::decode(
            conversation,
            expected.workspace_id(),
            Some(expected.id()),
        ) {
            Ok(conversation) => conversation,
            Err(error) => {
                return self
                    .close_cascade_attempt(transaction, CustomCascadeAttempt::Complete(Err(error)))
                    .await;
            }
        };
        if &conversation != expected {
            return self
                .close_cascade_attempt(
                    transaction,
                    CustomCascadeAttempt::Complete(Err(SdkError::conflict(
                        "conversation changed before parent close",
                    ))),
                )
                .await;
        }
        let before = match CustomContinuation::decode(continuation, expected.id()) {
            Ok(continuation) => continuation,
            Err(error) => {
                return self
                    .close_cascade_attempt(transaction, CustomCascadeAttempt::Complete(Err(error)))
                    .await;
            }
        };
        let mut continuation = before.clone();
        if continuation.is_closing() {
            return self
                .close_cascade_attempt(
                    transaction,
                    CustomCascadeAttempt::Complete(Ok(CustomCascadeCancellation::Terminal)),
                )
                .await;
        }
        let generation = match continuation.request_active_cancellation(intent) {
            Ok(Some(generation)) => generation,
            Ok(None) => {
                return self
                    .close_cascade_attempt(
                        transaction,
                        CustomCascadeAttempt::Complete(Ok(CustomCascadeCancellation::Quiescent)),
                    )
                    .await;
            }
            Err(error) => {
                return self
                    .close_cascade_attempt(transaction, CustomCascadeAttempt::Complete(Err(error)))
                    .await;
            }
        };
        let expected_continuation = continuation.clone();
        let continuation = match continuation.encode() {
            Ok(continuation) => continuation,
            Err(error) => {
                return self
                    .close_cascade_attempt(transaction, CustomCascadeAttempt::Complete(Err(error)))
                    .await;
            }
        };
        match transaction
            .put(storage_sdk::state::record::StateRecord::new(
                continuation_key.clone(),
                continuation,
            ))
            .await
        {
            Ok(()) => {}
            Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                return self
                    .close_cascade_attempt(transaction, CustomCascadeAttempt::Retry)
                    .await;
            }
            Err(error) => {
                return self
                    .close_cascade_attempt(
                        transaction,
                        CustomCascadeAttempt::Complete(Err(SdkError::from_state_store(
                            error,
                            "conversation state could not be stored",
                        )
                        .in_phase(crate::error::ErrorPhase::StateWrite))),
                    )
                    .await;
            }
        }
        let commit_error = match transaction.commit().await {
            Ok(_) => None,
            Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                return CustomCascadeAttempt::Retry;
            }
            Err(error) => Some(
                SdkError::from_state_store(error, "state update could not commit")
                    .in_phase(crate::error::ErrorPhase::StateCommit),
            ),
        };
        let observed = match self.continuation(store, expected.id()).await {
            Ok(observed) => observed,
            Err(error) => {
                return CustomCascadeAttempt::Complete(Err(Self::with_commit_evidence(
                    error,
                    commit_error.as_ref(),
                )));
            }
        };
        if observed == expected_continuation {
            return CustomCascadeAttempt::Complete(Ok(CustomCascadeCancellation::Active(
                generation,
            )));
        }
        if observed == before {
            return CustomCascadeAttempt::Complete(match commit_error {
                None => Err(SdkError::invalid_state(
                    "conversation cancellation was not durably observed",
                )),
                Some(error) => Err(error),
            });
        }
        CustomCascadeAttempt::Complete(Err(Self::with_commit_evidence(
            SdkError::conflict("conversation state changed while cancellation was reconciling"),
            commit_error.as_ref(),
        )))
    }

    async fn close_cascade_attempt(
        &self,
        transaction: S::Transaction,
        attempt: CustomCascadeAttempt,
    ) -> CustomCascadeAttempt {
        match transaction.rollback().await {
            Ok(_) => attempt,
            Err(error @ storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                match attempt {
                    CustomCascadeAttempt::Complete(Err(primary)) => {
                        CustomCascadeAttempt::Complete(Err(primary.with_supplementary(
                            SdkError::from_state_store(error, "state transaction could not close")
                                .at_phase(crate::error::ErrorPhase::Cleanup),
                        )))
                    }
                    CustomCascadeAttempt::Retry | CustomCascadeAttempt::Complete(Ok(_)) => {
                        CustomCascadeAttempt::Retry
                    }
                }
            }
            Err(error) => match attempt {
                CustomCascadeAttempt::Complete(Err(primary)) => {
                    CustomCascadeAttempt::Complete(Err(primary.with_supplementary(
                        SdkError::from_state_store(error, "state transaction could not close")
                            .at_phase(crate::error::ErrorPhase::Cleanup),
                    )))
                }
                CustomCascadeAttempt::Retry | CustomCascadeAttempt::Complete(Ok(_)) => {
                    CustomCascadeAttempt::Complete(Err(SdkError::from_state_store(
                        error,
                        "conversation cancellation read could not close",
                    )
                    .at_phase(crate::error::ErrorPhase::Cleanup)))
                }
            },
        }
    }

    pub(crate) async fn settle_generation_waiter(
        &self,
        conversation_id: &ConversationId,
        generation: u64,
        delivered: bool,
        deadline: Instant,
    ) -> Result<(), SdkError> {
        self.mutate_continuation(conversation_id, deadline, |continuation| {
            continuation.settle_waiter(generation, delivered)
        })
        .await
    }

    pub(crate) async fn begin_close(
        &self,
        conversation_id: &ConversationId,
        deadline: Instant,
    ) -> Result<Option<u64>, SdkError> {
        let mut generation = None;
        self.mutate_continuation(conversation_id, deadline, |continuation| {
            generation = continuation.begin_close()?;
            Ok(())
        })
        .await?;
        Ok(generation)
    }

    pub(crate) async fn abort_target(
        &self,
        conversation_id: &ConversationId,
    ) -> Result<Option<u64>, SdkError> {
        let store = self.storage.store();
        let snapshot = store
            .load(CustomContinuation::key(conversation_id)?)
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "conversation state could not be loaded")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })?
            .ok_or_else(|| SdkError::invalid_state("conversation state is missing"))?;
        CustomContinuation::decode(snapshot, conversation_id)?.abort_target()
    }

    pub(crate) async fn update_workspace(
        &self,
        request: &UpdateWorkspaceRequest,
    ) -> Result<(WorkspaceInfo, Vec<ConversationId>), SdkError> {
        let store = self.storage.store();
        let conversations = ConversationRecord::list(store, request.id()).await?;
        let conversation_ids = conversations
            .iter()
            .map(|conversation| conversation.id().clone())
            .collect();
        let workspace_key = WorkspaceRecord::key(&self.agent_id, request.id())?;
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::workspace_update_error(
                error,
                "workspace state update could not begin",
                crate::error::ErrorPhase::StateWrite,
            )
        })?;
        let snapshot = transaction
            .load(workspace_key.clone())
            .await
            .map_err(|error| {
                Self::workspace_update_error(
                    error,
                    "workspace state could not be loaded for update",
                    crate::error::ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::not_found("workspace does not exist"))?;
        let current = WorkspaceRecord::decode(snapshot, &self.agent_id, Some(request.id()))?;
        for conversation in &conversations {
            Self::ensure_continuation_idle_in_transaction(&mut transaction, conversation.id())
                .await?;
            NotificationStore::<S>::ensure_no_unresolved_interaction_in_transaction(
                &mut transaction,
                conversation.id(),
            )
            .await?;
        }
        let replacement = current.replace(request)?;
        let info = replacement.info()?;
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                workspace_key,
                replacement.encode()?,
            ))
            .await
            .map_err(|error| {
                Self::workspace_update_error(
                    error,
                    "updated workspace state could not be stored",
                    crate::error::ErrorPhase::StateWrite,
                )
            })?;
        let mut updated_conversations = Vec::with_capacity(conversations.len());
        for conversation in &conversations {
            let key = ConversationRecord::key(request.id(), conversation.id())?;
            let snapshot = transaction
                .load(key.clone())
                .await
                .map_err(|error| {
                    Self::workspace_update_error(
                        error,
                        "conversation state could not be loaded for update",
                        crate::error::ErrorPhase::StateRead,
                    )
                })?
                .ok_or_else(|| {
                    SdkError::invalid_state("conversation state disappeared during update")
                })?;
            let updated =
                ConversationRecord::decode(snapshot, request.id(), Some(conversation.id()))?
                    .with_workspace_revision(replacement.revision())?;
            transaction
                .put(storage_sdk::state::record::StateRecord::new(
                    key,
                    updated.encode()?,
                ))
                .await
                .map_err(|error| {
                    Self::workspace_update_error(
                        error,
                        "conversation revision could not be stored",
                        crate::error::ErrorPhase::StateWrite,
                    )
                })?;
            updated_conversations.push(updated);
        }
        let commit_error = match transaction.commit().await {
            Ok(_) => None,
            Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                return Err(SdkError::conflict("workspace update conflicted")
                    .with_detail(crate::error::ErrorDetail::TransactionConflict)
                    .in_phase(crate::error::ErrorPhase::StateCommit));
            }
            Err(error) => Some(
                SdkError::from_state_store(error, "state update could not commit")
                    .in_phase(crate::error::ErrorPhase::StateCommit),
            ),
        };
        self.observe_workspace_update(
            store,
            &current,
            &replacement,
            &conversations,
            &updated_conversations,
            commit_error,
        )
        .await?;
        self.notifications.invalidate_workspace(request.id());
        Ok((info, conversation_ids))
    }

    async fn observe_workspace_update(
        &self,
        store: &S,
        before: &WorkspaceRecord,
        expected: &WorkspaceRecord,
        before_conversations: &[ConversationRecord],
        expected_conversations: &[ConversationRecord],
        commit_error: Option<SdkError>,
    ) -> Result<(), SdkError> {
        let observed_workspace = self
            .workspace_record(store, expected.id())
            .await
            .map_err(|error| Self::with_commit_evidence(error, commit_error.as_ref()))?;
        let observed_conversations = ConversationRecord::list(store, expected.id())
            .await
            .map_err(|error| Self::with_commit_evidence(error, commit_error.as_ref()))?;
        if observed_workspace.as_ref() == Some(expected)
            && observed_conversations == expected_conversations
        {
            return Ok(());
        }
        if observed_workspace.as_ref() == Some(before)
            && observed_conversations == before_conversations
        {
            return match commit_error {
                None => Err(SdkError::invalid_state(
                    "workspace update was not durably observed",
                )),
                Some(error) => Err(error),
            };
        }
        Err(Self::with_commit_evidence(
            SdkError::invalid_state("workspace update commit outcome is contradictory"),
            commit_error.as_ref(),
        ))
    }

    pub(crate) async fn workspace_has_unsettled_work(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<bool, SdkError> {
        if self.provisionings.contains_workspace(workspace_id) {
            return Ok(true);
        }
        for conversation in self.conversations(workspace_id).await? {
            let store = self.storage.store();
            let continuation = self.continuation(store, conversation.id()).await?;
            if continuation.is_closing() || continuation.active_generation().is_some() {
                return Ok(true);
            }
            match self
                .notifications
                .ensure_no_unresolved_interaction(conversation.id())
                .await
            {
                Ok(()) => {}
                Err(error) if error.kind() == crate::error::ErrorKind::Busy => return Ok(true),
                Err(error) => return Err(error),
            }
        }
        Ok(false)
    }

    pub(crate) async fn delete_workspace(
        &self,
        id: &WorkspaceId,
        expected_revision: u64,
    ) -> Result<Vec<ConversationId>, SdkError> {
        let store = self.storage.store();
        let conversations = ConversationRecord::list(store, id).await?;
        let workspace_key = WorkspaceRecord::key(&self.agent_id, id)?;
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::workspace_delete_error(
                error,
                "workspace deletion could not begin",
                crate::error::ErrorPhase::StateWrite,
            )
        })?;
        let snapshot = transaction
            .load(workspace_key.clone())
            .await
            .map_err(|error| {
                Self::workspace_delete_error(
                    error,
                    "workspace state could not be loaded for deletion",
                    crate::error::ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::not_found("workspace does not exist"))?;
        let workspace = WorkspaceRecord::decode(snapshot, &self.agent_id, Some(id))?;
        if workspace.revision() != expected_revision {
            return Err(SdkError::conflict("workspace revision is stale")
                .with_detail(crate::error::ErrorDetail::StaleRevision));
        }
        let mut removed = Vec::with_capacity(conversations.len());
        for conversation in &conversations {
            Self::ensure_continuation_idle_in_transaction(&mut transaction, conversation.id())
                .await?;
            self.stage_knowledge_forget(
                &mut transaction,
                &KnowledgeOwnerId::Conversation {
                    agent_id: self.agent_id.clone(),
                    workspace_id: id.clone(),
                    conversation_id: conversation.id().clone(),
                },
            )
            .await
            .map_err(|error| match error {
                AgentStorageKnowledgeError::TransactionConflict(phase) => {
                    SdkError::conflict("workspace deletion conflicted")
                        .with_detail(crate::error::ErrorDetail::TransactionConflict)
                        .in_phase(phase)
                }
                AgentStorageKnowledgeError::Failure(error) => error,
            })?;
            NotificationStore::<S>::delete_in_transaction(&mut transaction, conversation.id())
                .await
                .map_err(NotificationDeleteError::into_sdk)?;
            transaction
                .delete(ConversationRecord::key(id, conversation.id())?)
                .await
                .map_err(|error| {
                    Self::workspace_delete_error(
                        error,
                        "conversation state could not be deleted",
                        crate::error::ErrorPhase::StateWrite,
                    )
                })?;
            transaction
                .delete(CustomContinuation::key(conversation.id())?)
                .await
                .map_err(|error| {
                    Self::workspace_delete_error(
                        error,
                        "conversation history could not be deleted",
                        crate::error::ErrorPhase::StateWrite,
                    )
                })?;
            removed.push(conversation.id().clone());
        }
        self.stage_knowledge_forget(
            &mut transaction,
            &KnowledgeOwnerId::Workspace {
                agent_id: self.agent_id.clone(),
                workspace_id: id.clone(),
            },
        )
        .await
        .map_err(|error| match error {
            AgentStorageKnowledgeError::TransactionConflict(phase) => {
                SdkError::conflict("workspace deletion conflicted")
                    .with_detail(crate::error::ErrorDetail::TransactionConflict)
                    .in_phase(phase)
            }
            AgentStorageKnowledgeError::Failure(error) => error,
        })?;
        transaction.delete(workspace_key).await.map_err(|error| {
            Self::workspace_delete_error(
                error,
                "workspace state could not be deleted",
                crate::error::ErrorPhase::StateWrite,
            )
        })?;
        let commit_error = match transaction.commit().await {
            Ok(_) => None,
            Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                return Err(SdkError::conflict("workspace deletion conflicted")
                    .with_detail(crate::error::ErrorDetail::TransactionConflict)
                    .in_phase(crate::error::ErrorPhase::StateCommit));
            }
            Err(error) => Some(
                SdkError::from_state_store(error, "state update could not commit")
                    .in_phase(crate::error::ErrorPhase::StateCommit),
            ),
        };
        self.observe_workspace_deletion(store, &workspace, &conversations, commit_error)
            .await?;
        self.notifications.invalidate_workspace(id);
        Ok(removed)
    }

    async fn observe_workspace_deletion(
        &self,
        store: &S,
        before: &WorkspaceRecord,
        before_conversations: &[ConversationRecord],
        commit_error: Option<SdkError>,
    ) -> Result<(), SdkError> {
        let observed_workspace = self
            .workspace_record(store, before.id())
            .await
            .map_err(|error| Self::with_commit_evidence(error, commit_error.as_ref()))?;
        let observed_conversations = ConversationRecord::list(store, before.id())
            .await
            .map_err(|error| Self::with_commit_evidence(error, commit_error.as_ref()))?;
        let mut descendants_absent = true;
        let mut descendants_present = true;
        for conversation in before_conversations {
            let continuation = self
                .continuation_optional(store, conversation.id())
                .await
                .map_err(|error| Self::with_commit_evidence(error, commit_error.as_ref()))?;
            let notification_absent =
                NotificationStore::<S>::initialization_is_absent(store, conversation.id())
                    .await
                    .map_err(|error| Self::with_commit_evidence(error, commit_error.as_ref()))?;
            descendants_absent &= continuation.is_none() && notification_absent;
            descendants_present &= continuation.is_some() && !notification_absent;
        }
        if observed_workspace.is_none() && observed_conversations.is_empty() && descendants_absent {
            return Ok(());
        }
        if observed_workspace.as_ref() == Some(before)
            && observed_conversations == before_conversations
            && descendants_present
        {
            return match commit_error {
                None => Err(SdkError::invalid_state(
                    "workspace deletion was not durably observed",
                )),
                Some(error) => Err(error),
            };
        }
        Err(Self::with_commit_evidence(
            SdkError::invalid_state("workspace deletion commit outcome is contradictory"),
            commit_error.as_ref(),
        ))
    }

    async fn stage_conversation_close(
        &self,
        workspace_id: &WorkspaceId,
        workspace_revision: u64,
        conversation_id: &ConversationId,
    ) -> Result<Option<NotificationClosure>, SdkError> {
        let owner = KnowledgeOwnerId::Conversation {
            agent_id: self.agent_id.clone(),
            workspace_id: workspace_id.clone(),
            conversation_id: conversation_id.clone(),
        };
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            SdkError::from_state_store(error, "conversation close could not begin")
                .in_phase(crate::error::ErrorPhase::StateWrite)
        })?;
        let workspace = transaction
            .load(WorkspaceRecord::key(&self.agent_id, workspace_id)?)
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "workspace state could not be loaded for close")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })?
            .ok_or_else(|| SdkError::not_found("workspace does not exist"))?;
        let workspace = WorkspaceRecord::decode(workspace, &self.agent_id, Some(workspace_id))?;
        if workspace.revision() != workspace_revision {
            return Err(SdkError::conflict("workspace handle is stale")
                .with_detail(crate::error::ErrorDetail::StaleRevision));
        }
        let conversation_key = ConversationRecord::key(workspace_id, conversation_id)?;
        let conversation = transaction.load(conversation_key).await.map_err(|error| {
            SdkError::from_state_store(error, "conversation state could not be loaded for close")
                .in_phase(crate::error::ErrorPhase::StateRead)
        })?;
        let continuation_key = CustomContinuation::key(conversation_id)?;
        let continuation = transaction.load(continuation_key).await.map_err(|error| {
            SdkError::from_state_store(error, "conversation state could not be checked")
                .in_phase(crate::error::ErrorPhase::StateRead)
        })?;
        let (conversation, continuation) = match (conversation, continuation) {
            (None, None) => return Ok(None),
            (Some(conversation), Some(continuation)) => (conversation, continuation),
            (Some(_), None) | (None, Some(_)) => {
                return Err(SdkError::invalid_state("conversation state is incomplete"));
            }
        };
        let conversation =
            ConversationRecord::decode(conversation, workspace_id, Some(conversation_id))?;
        if conversation.workspace_revision() != workspace_revision {
            return Err(SdkError::conflict("conversation handle is stale")
                .with_detail(crate::error::ErrorDetail::StaleRevision));
        }
        let continuation = CustomContinuation::decode(continuation, conversation_id)?;
        if !continuation.is_closing() {
            return Err(SdkError::invalid_state(
                "conversation close state is inconsistent",
            ));
        }
        if continuation.active_generation().is_some() {
            return Err(SdkError::busy("conversation has active prompt work"));
        }
        self.stage_knowledge_forget(&mut transaction, &owner)
            .await
            .map_err(AgentStorageKnowledgeError::into_sdk)?;
        let notification_closure = self
            .notifications
            .close_in_transaction(&mut transaction, conversation_id)
            .await
            .map_err(NotificationCloseError::into_sdk)?;
        let commit_error = transaction.commit().await.err().map(|error| {
            SdkError::from_state_store(error, "state update could not commit")
                .in_phase(crate::error::ErrorPhase::StateCommit)
        });
        let observed_workspace = self
            .workspace_record(store, workspace_id)
            .await
            .map_err(|error| Self::with_commit_evidence(error, commit_error.as_ref()))?;
        let observed_conversation = self
            .conversation_record(store, workspace_id, conversation_id)
            .await
            .map_err(|error| Self::with_commit_evidence(error, commit_error.as_ref()))?;
        let observed_continuation = self
            .continuation_optional(store, conversation_id)
            .await
            .map_err(|error| Self::with_commit_evidence(error, commit_error.as_ref()))?;
        let notification_observation = notification_closure
            .observe(store)
            .await
            .map_err(|error| Self::with_commit_evidence(error, commit_error.as_ref()))?;
        if observed_workspace.as_ref() == Some(&workspace)
            && observed_conversation.as_ref() == Some(&conversation)
            && observed_continuation.as_ref() == Some(&continuation)
            && matches!(
                notification_observation,
                NotificationMutationObservation::Expected
            )
        {
            return Ok(Some(notification_closure));
        }
        if observed_workspace.as_ref() == Some(&workspace)
            && observed_conversation.as_ref() == Some(&conversation)
            && observed_continuation.as_ref() == Some(&continuation)
            && notification_closure.changes_state()
            && matches!(
                notification_observation,
                NotificationMutationObservation::Unchanged
            )
        {
            return match commit_error {
                None => Err(SdkError::invalid_state(
                    "conversation close was not durably observed",
                )),
                Some(error) => Err(error),
            };
        }
        Err(Self::with_commit_evidence(
            SdkError::invalid_state("conversation close commit outcome is contradictory"),
            commit_error.as_ref(),
        ))
    }

    async fn remove_closed_conversation(
        &self,
        workspace_id: &WorkspaceId,
        workspace_revision: u64,
        conversation_id: &ConversationId,
    ) -> Result<(), SdkError> {
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            SdkError::from_state_store(error, "conversation removal could not begin")
                .in_phase(crate::error::ErrorPhase::StateWrite)
        })?;
        let workspace = transaction
            .load(WorkspaceRecord::key(&self.agent_id, workspace_id)?)
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "workspace state could not be loaded for removal")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })?
            .ok_or_else(|| SdkError::not_found("workspace does not exist"))?;
        let workspace = WorkspaceRecord::decode(workspace, &self.agent_id, Some(workspace_id))?;
        if workspace.revision() != workspace_revision {
            return Err(SdkError::conflict("workspace handle is stale")
                .with_detail(crate::error::ErrorDetail::StaleRevision));
        }
        let conversation_key = ConversationRecord::key(workspace_id, conversation_id)?;
        let continuation_key = CustomContinuation::key(conversation_id)?;
        let conversation = transaction
            .load(conversation_key.clone())
            .await
            .map_err(|error| {
                SdkError::from_state_store(
                    error,
                    "conversation state could not be loaded for removal",
                )
                .in_phase(crate::error::ErrorPhase::StateRead)
            })?;
        let continuation = transaction
            .load(continuation_key.clone())
            .await
            .map_err(|error| {
                SdkError::from_state_store(
                    error,
                    "conversation state could not be loaded for removal",
                )
                .in_phase(crate::error::ErrorPhase::StateRead)
            })?;
        let (conversation, continuation) = match (conversation, continuation) {
            (None, None) => return Ok(()),
            (Some(conversation), Some(continuation)) => (conversation, continuation),
            (Some(_), None) | (None, Some(_)) => {
                return Err(SdkError::invalid_state("conversation state is incomplete"));
            }
        };
        let conversation =
            ConversationRecord::decode(conversation, workspace_id, Some(conversation_id))?;
        if conversation.workspace_revision() != workspace_revision {
            return Err(SdkError::conflict("conversation handle is stale")
                .with_detail(crate::error::ErrorDetail::StaleRevision));
        }
        let continuation = CustomContinuation::decode(continuation, conversation_id)?;
        if !continuation.is_closing() {
            return Err(SdkError::invalid_state(
                "conversation close state is inconsistent",
            ));
        }
        transaction
            .delete(conversation_key)
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "conversation state could not be closed")
                    .in_phase(crate::error::ErrorPhase::StateWrite)
            })?;
        transaction
            .delete(continuation_key)
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "conversation history could not be removed")
                    .in_phase(crate::error::ErrorPhase::StateWrite)
            })?;
        let commit_error = transaction.commit().await.err().map(|error| {
            SdkError::from_state_store(error, "state update could not commit")
                .in_phase(crate::error::ErrorPhase::StateCommit)
        });
        self.observe_conversation_removal(
            store,
            &workspace,
            &conversation,
            &continuation,
            commit_error,
        )
        .await
    }

    async fn observe_conversation_removal(
        &self,
        store: &S,
        workspace: &WorkspaceRecord,
        conversation: &ConversationRecord,
        continuation: &CustomContinuation,
        commit_error: Option<SdkError>,
    ) -> Result<(), SdkError> {
        let observed_workspace = self
            .workspace_record(store, workspace.id())
            .await
            .map_err(|error| Self::with_commit_evidence(error, commit_error.as_ref()))?;
        let observed_conversation = self
            .conversation_record(store, workspace.id(), conversation.id())
            .await
            .map_err(|error| Self::with_commit_evidence(error, commit_error.as_ref()))?;
        let observed_continuation = self
            .continuation_optional(store, conversation.id())
            .await
            .map_err(|error| Self::with_commit_evidence(error, commit_error.as_ref()))?;
        let notification_absent =
            NotificationStore::<S>::initialization_is_absent(store, conversation.id())
                .await
                .map_err(|error| Self::with_commit_evidence(error, commit_error.as_ref()))?;
        if observed_workspace.as_ref() == Some(workspace)
            && observed_conversation.is_none()
            && observed_continuation.is_none()
            && notification_absent
        {
            return Ok(());
        }
        if observed_workspace.as_ref() == Some(workspace)
            && observed_conversation.as_ref() == Some(conversation)
            && observed_continuation.as_ref() == Some(continuation)
            && notification_absent
        {
            return match commit_error {
                None => Err(SdkError::invalid_state(
                    "conversation removal was not durably observed",
                )),
                Some(error) => Err(error),
            };
        }
        Err(Self::with_commit_evidence(
            SdkError::invalid_state("conversation removal commit outcome is contradictory"),
            commit_error.as_ref(),
        ))
    }

    pub(crate) async fn publish_interaction(
        &self,
        conversation_id: &ConversationId,
        notification_id: NotificationId,
        kind: crate::ConversationNotificationKind,
        deadline: Instant,
    ) -> Result<(), SdkError> {
        self.notifications
            .publish_with_id_before(conversation_id, notification_id, kind, deadline)
            .await
    }

    pub(crate) async fn begin_resolution(
        &self,
        conversation_id: &ConversationId,
        notification_id: NotificationId,
        resolution: NotificationResolution,
        deadline: Instant,
    ) -> Result<ResolutionIntent, SdkError> {
        self.notifications
            .begin_resolution(conversation_id, notification_id, resolution, deadline)
            .await
    }

    pub(crate) async fn settle_resolution(
        &self,
        intent: &mut ResolutionIntent,
        settlement: ResolutionSettlement,
        deadline: Instant,
    ) -> Result<ResolutionDisposition, SdkError> {
        let store = self.storage.store();
        loop {
            let (commit_result, disposition) = if let Some(commit_result) =
                intent.settlement_commit()
            {
                if Instant::now() >= deadline {
                    return Err(Self::with_commit_evidence(
                        SdkError::conflict("interaction settlement confirmation requires recovery")
                            .with_detail(crate::error::ErrorDetail::AcceptanceUnknown)
                            .in_phase(crate::error::ErrorPhase::Reconciliation),
                        commit_result.as_ref().err(),
                    ));
                }
                let disposition = if settlement == ResolutionSettlement::Accepted {
                    ResolutionDisposition::AlreadyAccepted
                } else {
                    ResolutionDisposition::Applied(settlement)
                };
                (commit_result, disposition)
            } else {
                if Instant::now() >= deadline {
                    return Err(SdkError::timeout(
                        "interaction settlement conflict did not settle before its deadline",
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
                            "interaction state update could not begin",
                        )
                        .in_phase(crate::error::ErrorPhase::StateWrite));
                    }
                };
                if Instant::now() >= deadline {
                    let error = SdkError::timeout(
                        "interaction settlement conflict did not settle before its deadline",
                    );
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
                let disposition = match NotificationStore::<S>::settle_resolution_in_transaction(
                    &mut transaction,
                    intent,
                    settlement,
                )
                .await
                {
                    Ok(disposition) => disposition,
                    Err(NotificationSettlementError::TransactionConflict) => {
                        let _ = transaction.rollback().await;
                        tokio::task::yield_now().await;
                        continue;
                    }
                    Err(NotificationSettlementError::Failure(error)) => {
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
                let commit_result = match transaction.commit().await {
                    Ok(()) => {
                        intent.record_settlement_commit(Some(Ok(())));
                        Ok(())
                    }
                    Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    Err(error) => {
                        let error = SdkError::from_state_store(
                            error,
                            "interaction settlement could not commit",
                        )
                        .in_phase(crate::error::ErrorPhase::StateCommit);
                        intent.record_settlement_commit(Some(Err(error.clone())));
                        Err(error)
                    }
                };
                (commit_result, disposition)
            };
            loop {
                match intent.observe_settlement(store, settlement).await {
                    Ok(NotificationMutationObservation::Expected) => {
                        self.notifications.wake_receivers();
                        return Ok(disposition);
                    }
                    Ok(NotificationMutationObservation::Unchanged) => {
                        if commit_result.is_err() {
                            intent.record_settlement_commit(None);
                        }
                        return Err(commit_result.err().unwrap_or_else(|| {
                            SdkError::invalid_state(
                                "interaction settlement was not durably observed",
                            )
                        }));
                    }
                    Ok(NotificationMutationObservation::Contradictory) => {
                        return Err(Self::with_commit_evidence(
                            SdkError::conflict(
                                "interaction settlement commit outcome requires recovery",
                            )
                            .with_detail(crate::error::ErrorDetail::AcceptanceUnknown)
                            .in_phase(crate::error::ErrorPhase::Reconciliation),
                            commit_result.as_ref().err(),
                        ));
                    }
                    Err(error) => {
                        let evidence = match error {
                            NotificationSettlementError::TransactionConflict => {
                                SdkError::conflict("state transaction conflicted")
                                    .with_detail(crate::error::ErrorDetail::TransactionConflict)
                            }
                            NotificationSettlementError::Failure(error)
                                if matches!(
                                    error.kind(),
                                    crate::error::ErrorKind::Integration
                                        | crate::error::ErrorKind::Timeout
                                ) =>
                            {
                                error
                            }
                            NotificationSettlementError::Failure(error) => {
                                return Err(Self::with_commit_evidence(
                                    error,
                                    commit_result.as_ref().err(),
                                ));
                            }
                        };

                        if Instant::now() >= deadline {
                            return Err(Self::with_commit_evidence(
                                SdkError::conflict(
                                    "interaction settlement confirmation requires recovery",
                                )
                                .with_detail(crate::error::ErrorDetail::AcceptanceUnknown)
                                .in_phase(crate::error::ErrorPhase::Reconciliation),
                                commit_result.as_ref().err(),
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

    pub(crate) async fn cancel_interactions(
        &self,
        conversation_id: &ConversationId,
    ) -> Result<(), SdkError> {
        let unresolved_acceptance = self
            .notifications
            .cancel_pending_interactions(conversation_id)
            .await?;
        if unresolved_acceptance {
            self.notifications.wake_receivers();
            Err(SdkError::conflict(
                "interaction settlement is still in progress",
            ))
        } else {
            self.notifications.finish_reconciliation(conversation_id);
            Ok(())
        }
    }

    async fn mutate_continuation(
        &self,
        conversation_id: &ConversationId,
        deadline: Instant,
        mut mutation: impl FnMut(&mut CustomContinuation) -> Result<(), SdkError>,
    ) -> Result<(), SdkError> {
        let key = CustomContinuation::key(conversation_id)?;
        let store = self.storage.store();
        loop {
            if Instant::now() >= deadline {
                return Err(SdkError::timeout(
                    "conversation state conflict did not settle before its deadline",
                ));
            }
            let mut transaction = match store.begin_transaction().await {
                Ok(transaction) => transaction,
                Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                    self.yield_for_continuation_retry(deadline).await?;
                    continue;
                }
                Err(error) => {
                    return Err(SdkError::from_state_store(
                        error,
                        "conversation state update could not begin",
                    )
                    .in_phase(crate::error::ErrorPhase::StateWrite));
                }
            };
            let snapshot = match transaction.load(key.clone()).await {
                Ok(Some(snapshot)) => snapshot,
                Ok(None) => {
                    return Err(SdkError::invalid_state("conversation state is missing"));
                }
                Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                    drop(transaction);
                    self.yield_for_continuation_retry(deadline).await?;
                    continue;
                }
                Err(error) => {
                    return Err(SdkError::from_state_store(
                        error,
                        "conversation state could not be loaded",
                    )
                    .in_phase(crate::error::ErrorPhase::StateRead));
                }
            };
            let before = CustomContinuation::decode(snapshot, conversation_id)?;
            let mut continuation = before.clone();
            mutation(&mut continuation)?;
            if continuation == before {
                return transaction.rollback().await.map_err(|error| {
                    SdkError::from_state_store(error, "unchanged state transaction could not close")
                        .in_phase(crate::error::ErrorPhase::Cleanup)
                });
            }
            match transaction
                .put(storage_sdk::state::record::StateRecord::new(
                    key.clone(),
                    continuation.encode()?,
                ))
                .await
            {
                Ok(()) => {}
                Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                    drop(transaction);
                    self.yield_for_continuation_retry(deadline).await?;
                    continue;
                }
                Err(error) => {
                    return Err(SdkError::from_state_store(
                        error,
                        "conversation state could not be stored",
                    )
                    .in_phase(crate::error::ErrorPhase::StateWrite));
                }
            }
            let commit_error = match transaction.commit().await {
                Ok(_) => None,
                Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                    self.yield_for_continuation_retry(deadline).await?;
                    continue;
                }
                Err(error) => Some(
                    SdkError::from_state_store(error, "state update could not commit")
                        .in_phase(crate::error::ErrorPhase::StateCommit),
                ),
            };
            let observed = self
                .continuation(store, conversation_id)
                .await
                .map_err(|error| Self::with_commit_evidence(error, commit_error.as_ref()))?;
            if observed == continuation {
                return Ok(());
            }
            if observed == before {
                return match commit_error {
                    None => Err(SdkError::invalid_state(
                        "conversation state update was not durably observed",
                    )),
                    Some(error) => Err(error),
                };
            }
            return Err(Self::with_commit_evidence(
                SdkError::conflict("conversation state changed while an update was reconciling"),
                commit_error.as_ref(),
            ));
        }
    }

    async fn yield_for_continuation_retry(&self, deadline: Instant) -> Result<(), SdkError> {
        if Instant::now() >= deadline {
            return Err(SdkError::timeout(
                "conversation state conflict did not settle before its deadline",
            ));
        }
        tokio::task::yield_now().await;
        if Instant::now() >= deadline {
            Err(SdkError::timeout(
                "conversation state conflict did not settle before its deadline",
            ))
        } else {
            Ok(())
        }
    }

    async fn ensure_continuation_idle_in_transaction(
        transaction: &mut S::Transaction,
        conversation_id: &ConversationId,
    ) -> Result<(), SdkError> {
        let snapshot = transaction
            .load(CustomContinuation::key(conversation_id)?)
            .await
            .map_err(|error| match error {
                storage_sdk::StateStoreError::TransactionConflict { .. } => {
                    SdkError::conflict("conversation state transaction conflicted")
                        .with_detail(crate::error::ErrorDetail::TransactionConflict)
                }
                error => {
                    SdkError::from_state_store(error, "conversation state could not be checked")
                        .in_phase(crate::error::ErrorPhase::StateRead)
                }
            })?
            .ok_or_else(|| SdkError::invalid_state("conversation state is missing"))?;
        if CustomContinuation::decode(snapshot, conversation_id)?
            .active_generation()
            .is_some()
        {
            return Err(SdkError::busy("conversation has active prompt work"));
        }
        Ok(())
    }

    async fn continuation(
        &self,
        store: &S,
        conversation_id: &ConversationId,
    ) -> Result<CustomContinuation, SdkError> {
        self.continuation_optional(store, conversation_id)
            .await?
            .ok_or_else(|| SdkError::invalid_state("conversation state is missing"))
    }

    async fn continuation_optional(
        &self,
        store: &S,
        conversation_id: &ConversationId,
    ) -> Result<Option<CustomContinuation>, SdkError> {
        store
            .load(CustomContinuation::key(conversation_id)?)
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "conversation state could not be loaded")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })?
            .map(|snapshot| CustomContinuation::decode(snapshot, conversation_id))
            .transpose()
    }

    async fn workspace_record(
        &self,
        store: &S,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<WorkspaceRecord>, SdkError> {
        store
            .load(WorkspaceRecord::key(&self.agent_id, workspace_id)?)
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "workspace state could not be observed")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })?
            .map(|snapshot| WorkspaceRecord::decode(snapshot, &self.agent_id, Some(workspace_id)))
            .transpose()
    }

    async fn conversation_record(
        &self,
        store: &S,
        workspace_id: &WorkspaceId,
        conversation_id: &ConversationId,
    ) -> Result<Option<ConversationRecord>, SdkError> {
        store
            .load(ConversationRecord::key(workspace_id, conversation_id)?)
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "conversation state could not be observed")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })?
            .map(|snapshot| {
                ConversationRecord::decode(snapshot, workspace_id, Some(conversation_id))
            })
            .transpose()
    }
}
