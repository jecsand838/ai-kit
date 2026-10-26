use std::time::Duration;

use storage_sdk::state::record::StateRecord;
use storage_sdk::{StateStoreBackend, StateTransactionBackend};
use tokio::time::Instant;

use crate::backends::opencode::state_store::OpenCodeStateStore;
use crate::backends::opencode::state_store::prompt::OpenCodePromptDurability;
use crate::backends::opencode::state_store::records::{
    OpenCodeCancellationOwner, OpenCodeContinuation, OpenCodeContinuationState,
    OpenCodePromptRecord, OpenCodeSessionBinding,
};
use crate::core::notification_store::{NotificationStore, ResolutionSettlement};
use crate::core::state::{ConversationRecord, WorkspaceRecord};
use crate::error::{ErrorDetail, ErrorKind, ErrorPhase};
use crate::{ConversationId, SdkError};

impl<S: StateStoreBackend> OpenCodeStateStore<S> {
    pub(crate) async fn native_continuations(
        &self,
        conversation: &ConversationId,
    ) -> Result<Vec<OpenCodeContinuation>, SdkError> {
        self.prompts.continuations(conversation).await
    }

    pub(crate) async fn close_requires_interaction_preparation(
        &self,
        conversation: &ConversationId,
    ) -> Result<bool, SdkError> {
        self.storage
            .store()
            .load(OpenCodePromptRecord::key(conversation)?)
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "closing prompt could not be observed")
                    .at_phase(ErrorPhase::StateRead)
            })?
            .map(|snapshot| {
                OpenCodePromptRecord::decode(snapshot, conversation)
                    .map(|prompt| prompt.is_terminal())
            })
            .transpose()
            .map(|terminal| terminal.unwrap_or(false))
    }

    pub(crate) async fn native_cancellation_parent(
        &self,
        continuation: &OpenCodeContinuation,
    ) -> Result<OpenCodePromptRecord, SdkError> {
        let snapshot = self
            .storage
            .store()
            .load(OpenCodePromptRecord::key(continuation.conversation_id())?)
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "interaction generation could not be observed")
                    .at_phase(ErrorPhase::StateRead)
            })?
            .ok_or_else(|| {
                SdkError::conflict("interaction cancellation has no retained generation")
            })?;
        let prompt = OpenCodePromptRecord::decode(snapshot, continuation.conversation_id())?;
        if continuation.generation_id() != Some(prompt.generation_id())
            || !(prompt.is_cancelled() || prompt.interaction_cleanup_ready())
        {
            return Err(SdkError::conflict(
                "interaction cancellation does not own the terminal generation",
            ));
        }
        Ok(prompt)
    }

    pub(crate) async fn reserve_native_cancellation(
        &self,
        binding: &OpenCodeSessionBinding,
        original: &[OpenCodeContinuation],
        closing: Option<&ConversationRecord>,
        deadline: Instant,
    ) -> Result<Vec<OpenCodeContinuation>, SdkError> {
        let owner = OpenCodeCancellationOwner::fresh();
        let expected = original
            .iter()
            .map(|continuation| {
                let state = match continuation.state() {
                    OpenCodeContinuationState::Pending if closing.is_some() => {
                        OpenCodeContinuationState::Closing {
                            owner: owner.clone(),
                        }
                    }
                    OpenCodeContinuationState::CancellationRequired => {
                        OpenCodeContinuationState::CancellationDispatched {
                            owner: owner.clone(),
                        }
                    }
                    _ => {
                        return Err(SdkError::conflict(
                            "interaction cancellation is already owned or settled",
                        ));
                    }
                };
                continuation.with_state(state)
            })
            .collect::<Result<Vec<_>, _>>()?;
        for continuation in original
            .iter()
            .filter(|request| request.retains_cancelled_generation())
        {
            // Recovered runtime state is reconstructed only for the exact retained
            // durable generation; no native effect is authorized by this registry.
            self.prompts.retain_native_cleanup(continuation)?;
            self.native_cleanup_failure(continuation, deadline)?;
        }
        if let Err(error) = self
            .persist_native_cancellation(binding, original, &expected, closing, deadline)
            .await
        {
            for continuation in original
                .iter()
                .filter(|request| request.retains_cancelled_generation())
            {
                self.prompts
                    .observe_native_cleanup(
                        continuation,
                        OpenCodePromptDurability::CleanupFailed {
                            pending: original
                                .iter()
                                .filter(|request| {
                                    request.retains_cancelled_generation()
                                        && request.generation_id() == continuation.generation_id()
                                })
                                .map(|request| request.notification_id().clone())
                                .collect(),
                            error: error.clone(),
                        },
                    )
                    .map_err(|secondary| error.clone().with_supplementary(secondary))?;
            }
            return Err(error);
        }
        Ok(expected)
    }

    pub(crate) async fn settle_native_cancellation(
        &self,
        binding: &OpenCodeSessionBinding,
        original: &[OpenCodeContinuation],
        deadline: Instant,
    ) -> Result<(), SdkError> {
        let expected = original
            .iter()
            .map(|continuation| continuation.with_state(OpenCodeContinuationState::Cancelled))
            .collect::<Result<Vec<_>, _>>()?;
        self.persist_native_cancellation(binding, original, &expected, None, deadline)
            .await?;
        for continuation in original {
            // Only acknowledged native settlement clears a parked failure.
            if continuation.generation_id().is_some() {
                self.prompts
                    .observe_native_cleanup(continuation, OpenCodePromptDurability::Reserved)?;
            }
        }
        Ok(())
    }

    pub(crate) fn native_cleanup_failure(
        &self,
        continuation: &OpenCodeContinuation,
        deadline: Instant,
    ) -> Result<(), SdkError> {
        if let Some(generation) = continuation.generation_id() {
            self.prompts.native_cleanup_failure(
                continuation.conversation_id(),
                generation,
                deadline,
            )?;
        }
        Ok(())
    }

    async fn persist_native_cancellation(
        &self,
        binding: &OpenCodeSessionBinding,
        original: &[OpenCodeContinuation],
        expected: &[OpenCodeContinuation],
        closing: Option<&ConversationRecord>,
        deadline: Instant,
    ) -> Result<(), SdkError> {
        if original.is_empty() || original.len() != expected.len() {
            return Err(SdkError::invalid_state(
                "interaction cancellation has no exact owned set",
            ));
        }
        let store = self.storage.store();
        let mut last_conflict = None;
        loop {
            if Instant::now() >= deadline {
                let error =
                    SdkError::timeout("interaction cancellation persistence deadline elapsed")
                        .at_phase(ErrorPhase::Reconciliation);
                return Err(match last_conflict {
                    Some(previous) => error.with_supplementary(previous),
                    None => error,
                });
            }
            let mut transaction = match store.begin_transaction().await {
                Ok(transaction) => transaction,
                Err(error) => {
                    let error = SdkError::from_state_store(
                        error,
                        "interaction cancellation could not begin",
                    )
                    .at_phase(ErrorPhase::StateWrite);
                    if Self::native_cancellation_conflict(&error) {
                        for continuation in original
                            .iter()
                            .filter(|request| request.retains_cancelled_generation())
                        {
                            self.prompts.observe_native_cleanup(
                                continuation,
                                OpenCodePromptDurability::CleanupRetrying {
                                    pending: original
                                        .iter()
                                        .filter(|request| {
                                            request.retains_cancelled_generation()
                                                && request.generation_id()
                                                    == continuation.generation_id()
                                        })
                                        .map(|request| request.notification_id().clone())
                                        .collect(),
                                    last_error: error.clone(),
                                },
                            )?;
                        }
                        last_conflict = Some(error);
                        tokio::time::sleep_until(
                            deadline.min(Instant::now() + Duration::from_millis(25)),
                        )
                        .await;
                        continue;
                    }
                    return Err(error);
                }
            };
            let staging = async {
                // Revalidate the complete binding after network observations. The
                // consuming Close command retains its unique hierarchy claim.
                let workspace = transaction
                    .load(WorkspaceRecord::key(
                        &self.agent_id,
                        binding.workspace_id(),
                    )?)
                    .await
                    .map_err(|error| {
                        SdkError::from_state_store(
                            error,
                            "interaction workspace could not be checked",
                        )
                        .at_phase(ErrorPhase::StateRead)
                    })?
                    .ok_or_else(|| SdkError::closed("interaction workspace is absent"))?;
                let workspace = WorkspaceRecord::decode(
                    workspace,
                    &self.agent_id,
                    Some(binding.workspace_id()),
                )?;
                let conversation = transaction
                    .load(ConversationRecord::key(
                        binding.workspace_id(),
                        binding.conversation_id(),
                    )?)
                    .await
                    .map_err(|error| {
                        SdkError::from_state_store(
                            error,
                            "interaction conversation could not be checked",
                        )
                        .at_phase(ErrorPhase::StateRead)
                    })?
                    .ok_or_else(|| SdkError::closed("interaction conversation is absent"))?;
                let conversation = ConversationRecord::decode(
                    conversation,
                    binding.workspace_id(),
                    Some(binding.conversation_id()),
                )?;
                let routing =
                    Self::workspace_routing_in_transaction(&mut transaction, &workspace).await?;
                binding.verify_owner(&conversation, &workspace, &routing)?;
                if closing.is_some_and(|record| record != &conversation) {
                    return Err(SdkError::conflict(
                        "closing conversation changed before interaction cleanup",
                    ));
                }
                if closing.is_some()
                    && let Some(snapshot) = transaction
                        .load(OpenCodePromptRecord::key(binding.conversation_id())?)
                        .await
                        .map_err(|error| {
                            SdkError::from_state_store(error, "closing prompt could not be checked")
                                .at_phase(ErrorPhase::StateRead)
                        })?
                    && !OpenCodePromptRecord::decode(snapshot, binding.conversation_id())?
                        .is_terminal()
                {
                    return Err(SdkError::busy(
                        "closing interaction still has an active prompt",
                    ));
                }
                let stored_binding = transaction
                    .load(Self::binding_key(
                        binding.workspace_id(),
                        binding.conversation_id(),
                    )?)
                    .await
                    .map_err(|error| {
                        SdkError::from_state_store(
                            error,
                            "interaction session could not be checked",
                        )
                        .at_phase(ErrorPhase::StateRead)
                    })?
                    .ok_or_else(|| SdkError::closed("interaction session is absent"))?;
                if Self::decode_binding(
                    stored_binding,
                    binding.workspace_id(),
                    binding.conversation_id(),
                )? != *binding
                {
                    return Err(SdkError::conflict(
                        "interaction session changed before cleanup",
                    ));
                }
                for (before, after) in original.iter().zip(expected) {
                    let snapshot = transaction
                        .load(before.state_key()?)
                        .await
                        .map_err(|error| {
                            SdkError::from_state_store(
                                error,
                                "interaction cancellation could not be loaded",
                            )
                            .at_phase(ErrorPhase::StateRead)
                        })?
                        .ok_or_else(|| {
                            SdkError::conflict("interaction cancellation owner disappeared")
                        })?;
                    let current =
                        OpenCodeContinuation::decode(snapshot, binding.conversation_id())?;
                    if current == *after {
                        continue;
                    }
                    if current != *before || before.session_id() != binding.require_session_id()? {
                        return Err(SdkError::conflict(
                            "interaction cancellation authority changed",
                        ));
                    }
                    if matches!(after.state(), OpenCodeContinuationState::Closing { .. }) {
                        NotificationStore::<S>::ensure_interaction_pending_in_transaction(
                            &mut transaction,
                            before.conversation_id(),
                            before.notification_id(),
                        )
                        .await?;
                    }
                    if after.state() != OpenCodeContinuationState::Cancelled && closing.is_none() {
                        let snapshot = transaction
                            .load(OpenCodePromptRecord::key(binding.conversation_id())?)
                            .await
                            .map_err(|error| {
                                SdkError::from_state_store(
                                    error,
                                    "interaction generation could not be checked",
                                )
                                .at_phase(ErrorPhase::StateRead)
                            })?
                            .ok_or_else(|| {
                                SdkError::conflict("interaction generation was released")
                            })?;
                        let prompt =
                            OpenCodePromptRecord::decode(snapshot, binding.conversation_id())?;
                        if before.generation_id() != Some(prompt.generation_id())
                            || !(prompt.is_cancelled() || prompt.interaction_cleanup_ready())
                        {
                            return Err(SdkError::conflict(
                                "interaction cancellation belongs to another generation",
                            ));
                        }
                    }
                    if after.state() == OpenCodeContinuationState::Cancelled
                        && matches!(before.state(), OpenCodeContinuationState::Closing { .. })
                    {
                        NotificationStore::<S>::reconcile_native_resolution_in_transaction(
                            &mut transaction,
                            before.conversation_id(),
                            before.notification_id(),
                            ResolutionSettlement::Cancelled,
                        )
                        .await
                        .map_err(SdkError::from)?;
                    }
                    transaction
                        .put(StateRecord::new(
                            before.state_key()?,
                            serde_json::to_value(after).map_err(|_| {
                                SdkError::invalid_state(
                                    "interaction cancellation could not be encoded",
                                )
                            })?,
                        ))
                        .await
                        .map_err(|error| {
                            SdkError::from_state_store(
                                error,
                                "interaction cancellation could not be stored",
                            )
                            .at_phase(ErrorPhase::StateWrite)
                        })?;
                }
                Ok::<_, SdkError>(())
            }
            .await;
            if let Err(error) = staging {
                if let Err(cleanup) = transaction.rollback().await {
                    return Err(error.with_supplementary(
                        SdkError::from_state_store(
                            cleanup,
                            "interaction transaction rollback failed",
                        )
                        .at_phase(ErrorPhase::Cleanup),
                    ));
                }
                if Self::native_cancellation_conflict(&error) {
                    for continuation in original
                        .iter()
                        .filter(|request| request.retains_cancelled_generation())
                    {
                        self.prompts.observe_native_cleanup(
                            continuation,
                            OpenCodePromptDurability::CleanupRetrying {
                                pending: original
                                    .iter()
                                    .filter(|request| {
                                        request.retains_cancelled_generation()
                                            && request.generation_id()
                                                == continuation.generation_id()
                                    })
                                    .map(|request| request.notification_id().clone())
                                    .collect(),
                                last_error: error.clone(),
                            },
                        )?;
                    }
                    last_conflict = Some(error);
                    tokio::time::sleep_until(
                        deadline.min(Instant::now() + Duration::from_millis(25)),
                    )
                    .await;
                    continue;
                }
                return Err(error);
            }
            match transaction.commit().await {
                Ok(()) => break,
                Err(error) => {
                    let error = SdkError::from_state_store(
                        error,
                        "interaction cancellation could not commit",
                    )
                    .at_phase(ErrorPhase::StateCommit);
                    if Self::native_cancellation_conflict(&error) {
                        for continuation in original
                            .iter()
                            .filter(|request| request.retains_cancelled_generation())
                        {
                            self.prompts.observe_native_cleanup(
                                continuation,
                                OpenCodePromptDurability::CleanupRetrying {
                                    pending: original
                                        .iter()
                                        .filter(|request| {
                                            request.retains_cancelled_generation()
                                                && request.generation_id()
                                                    == continuation.generation_id()
                                        })
                                        .map(|request| request.notification_id().clone())
                                        .collect(),
                                    last_error: error.clone(),
                                },
                            )?;
                        }
                        last_conflict = Some(error);
                        tokio::time::sleep_until(
                            deadline.min(Instant::now() + Duration::from_millis(25)),
                        )
                        .await;
                        continue;
                    }
                    // From here only exact receipt observation is permitted.
                    for after in expected {
                        let observation =
                            store.load(after.state_key()?).await.map_err(|secondary| {
                                error.clone().with_supplementary(
                                    SdkError::from_state_store(
                                        secondary,
                                        "interaction cancellation receipt could not be observed",
                                    )
                                    .at_phase(ErrorPhase::Reconciliation),
                                )
                            })?;
                        if !observation
                            .map(|snapshot| {
                                OpenCodeContinuation::decode(snapshot, after.conversation_id())
                            })
                            .transpose()
                            .map_err(|secondary| {
                                error.clone().with_supplementary(
                                    secondary.at_phase(ErrorPhase::Reconciliation),
                                )
                            })?
                            .is_some_and(|actual| actual == *after)
                        {
                            return Err(error);
                        }
                    }
                    break;
                }
            }
        }
        self.prompts.mark_changed();
        self.notifications.wake_receivers();
        Ok(())
    }

    fn native_cancellation_conflict(error: &SdkError) -> bool {
        error.kind() == ErrorKind::Conflict
            && error.detail() == Some(&ErrorDetail::TransactionConflict)
            && error.context().phase != ErrorPhase::Cleanup
            && !error
                .supplementary()
                .iter()
                .any(|error| error.context().phase == ErrorPhase::Cleanup)
    }
}
