use crate::error::ErrorPhase;
use crate::error::{ErrorDetail, ErrorKind};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::{Arc, Mutex};

use storage_sdk::{StateStoreBackend, StateTransactionBackend};
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::backends::opencode::notifications::OpenCodePromptStatus;
use crate::backends::opencode::protocol::inbound::OpenCodeKnownEvent;
use crate::core::hierarchy::HierarchyAdmission;
use crate::core::notification_store::{
    NotificationInteractionCheckError, NotificationSettlementError, NotificationStore,
    ResolutionSettlement,
};
use crate::core::state::{ConversationRecord, WorkspaceRecord};
use crate::identity::NotificationId;
use crate::prompt::PromptOutput;
use crate::{AgentId, AgentStorage, ConversationId, PromptResponse, SdkError};

use super::records::{
    OpenCodeCancellation, OpenCodeCancellationOwner, OpenCodeContinuation,
    OpenCodeContinuationState, OpenCodePromptEffective, OpenCodePromptRecord,
    OpenCodeSessionBinding,
};

const MAX_CONTINUATION_PAGES: usize = 1_024;

pub(super) struct OpenCodePromptState<S: StateStoreBackend> {
    storage: AgentStorage<S>,
    notifications: NotificationStore<S>,
    permits: Arc<tokio::sync::Semaphore>,
    changes: tokio::sync::watch::Sender<u64>,
    statuses: tokio::sync::broadcast::Sender<(ConversationId, OpenCodePromptStatusSignal)>,
    registrations: Arc<Mutex<BTreeMap<ConversationId, OpenCodePromptReservation>>>,
    tasks: Arc<Mutex<OpenCodePromptTasks>>,
}

enum OpenCodePromptTasks {
    Open {
        running: JoinSet<()>,
        prior_failure: bool,
        revoked: bool,
    },
    Sealed,
}

pub(crate) struct OpenCodePromptGeneration<S: StateStoreBackend> {
    prompts: OpenCodePromptState<S>,
    conversation_id: ConversationId,
    generation_id: String,
}

enum OpenCodePromptSettlement<'a> {
    Response {
        cancellation_owner: Option<&'a OpenCodeCancellationOwner>,
        result: &'a Result<PromptResponse, SdkError>,
    },
    InteractionCleanup,
}

impl OpenCodePromptSettlement<'_> {
    fn cancels_interactions(&self) -> bool {
        match self {
            Self::Response { result, .. } => {
                matches!(result, Err(error) if error.kind() == ErrorKind::Cancelled)
            }
            Self::InteractionCleanup => true,
        }
    }

    fn prepare(&self, record: &mut OpenCodePromptRecord) -> Result<(), SdkError> {
        match self {
            Self::Response {
                cancellation_owner: Some(owner),
                result,
            } => record.terminal_without_cancellation_dispatch(owner, result),
            Self::Response {
                cancellation_owner: None,
                result,
            } if record.cancellation_accepted()
                && matches!(result, Err(error) if error.kind() == ErrorKind::Cancelled) =>
            {
                record.terminalize_accepted_cancellation()
            }
            Self::Response { result, .. } => record.terminal(result),
            Self::InteractionCleanup
                if record.interaction_cleanup_ready()
                    || (record.is_cancelled() && record.clearable()) =>
            {
                Ok(())
            }
            Self::InteractionCleanup => Err(SdkError::invalid_state(
                "interaction cleanup contradicts durable prompt ownership",
            )),
        }
    }

    fn confirms(
        &self,
        current: &mut OpenCodePromptRecord,
        expected: &OpenCodePromptRecord,
    ) -> bool {
        match self {
            Self::Response { result, .. } => {
                current.is_terminal() && current.terminal(result).is_ok()
            }
            // This unchanged parent is only the generation fence. The changed
            // continuation receipts below must also confirm the atomic commit.
            Self::InteractionCleanup => current == expected,
        }
    }
}

pub(crate) enum OpenCodePromptStartError {
    TransactionConflict,
    Failure(SdkError),
}

struct OpenCodePromptReservation {
    generation_id: String,
    runtime: OpenCodePromptRuntimeState,
    durability: OpenCodePromptDurability,
    abort_joiners: usize,
    abort: OpenCodeAbortProgress,
    observation: Option<OpenCodePromptObservation>,
}

const MAX_PROMPT_OUTPUT_OWNERS: usize = 1_024;

struct OpenCodePromptObservation {
    session_id: String,
    request_part_id: String,
    output: Arc<PromptOutput>,
    messages: BTreeMap<String, OpenCodeObservedMessage>,
    parts: BTreeMap<String, String>,
    state: OpenCodePromptObservationState,
}

enum OpenCodePromptObservationState {
    Observing,
    ExecutionFailed(SdkError),
    Interrupted,
}

enum OpenCodeObservedMessage {
    User,
    Assistant { parent_id: String, completed: bool },
}

impl OpenCodePromptObservation {
    fn new(session_id: String, request_part_id: String, output: Arc<PromptOutput>) -> Self {
        Self {
            session_id,
            request_part_id,
            output,
            messages: BTreeMap::new(),
            parts: BTreeMap::new(),
            state: OpenCodePromptObservationState::Observing,
        }
    }

    fn retain_execution_error(&mut self, error: &SdkError) {
        if matches!(self.state, OpenCodePromptObservationState::Observing) {
            self.state = OpenCodePromptObservationState::ExecutionFailed(error.clone());
        }
    }

    fn interrupt(&mut self, error: &SdkError) {
        self.messages.clear();
        self.parts.clear();
        if matches!(self.state, OpenCodePromptObservationState::Observing) {
            self.state = OpenCodePromptObservationState::Interrupted;
            self.output.finish(Err(error
                .clone()
                .at_operation(crate::error::Operation::Prompt)));
        }
        // A normalized execution failure remains owned by the foreground,
        // which publishes it with its terminal-cleanup diagnostics.
    }

    fn owns_assistant(&self, message_id: &str) -> bool {
        let Some(OpenCodeObservedMessage::Assistant { parent_id, .. }) =
            self.messages.get(message_id)
        else {
            return false;
        };
        self.parts.get(&self.request_part_id) == Some(parent_id)
            && matches!(
                self.messages.get(parent_id),
                Some(OpenCodeObservedMessage::User)
            )
    }

    fn complete(&self, message_id: &str) -> bool {
        !matches!(self.state, OpenCodePromptObservationState::Observing)
            || (self.owns_assistant(message_id)
                && matches!(
                    self.messages.get(message_id),
                    Some(OpenCodeObservedMessage::Assistant {
                        completed: true,
                        ..
                    })
                ))
    }

    fn observe(&mut self, event: &OpenCodeKnownEvent, live: bool, fresh: bool) {
        if !matches!(self.state, OpenCodePromptObservationState::Observing)
            || event.session_id() != Some(self.session_id.as_str())
        {
            return;
        }
        match event {
            OpenCodeKnownEvent::MessageUpdated(event) => {
                if let Some(evidence) = event.prompt_message_evidence() {
                    if matches!(
                        self.messages.get(evidence.message_id()),
                        Some(OpenCodeObservedMessage::Assistant { .. })
                    ) {
                        self.interrupt(&SdkError::protocol("prompt message ownership changed"));
                        return;
                    }
                    self.messages.insert(
                        evidence.message_id().to_owned(),
                        OpenCodeObservedMessage::User,
                    );
                } else if let Some((id, parent_id, completed)) = event.assistant_observation() {
                    match self.messages.get_mut(id) {
                        Some(OpenCodeObservedMessage::Assistant {
                            parent_id: owned_parent,
                            completed: owned_completed,
                        }) if owned_parent == parent_id => {
                            *owned_completed |= live && completed;
                        }
                        Some(_) => {
                            self.interrupt(&SdkError::protocol("prompt message ownership changed"))
                        }
                        None => {
                            self.messages.insert(
                                id.to_owned(),
                                OpenCodeObservedMessage::Assistant {
                                    parent_id: parent_id.to_owned(),
                                    completed: live && completed,
                                },
                            );
                        }
                    }
                }
            }
            OpenCodeKnownEvent::MessagePartUpdated(event) => {
                let (part_id, message_id, observable) = event.observation_part();
                if self
                    .parts
                    .get(part_id)
                    .is_some_and(|owner| owner != message_id)
                {
                    self.interrupt(&SdkError::protocol("prompt part ownership changed"));
                    return;
                }
                if observable {
                    self.parts.insert(part_id.to_owned(), message_id.to_owned());
                } else {
                    self.parts.remove(part_id);
                }
            }
            OpenCodeKnownEvent::MessageRemoved(event) => {
                let id = event.message_id();
                if self.owns_assistant(id)
                    || self
                        .parts
                        .get(&self.request_part_id)
                        .is_some_and(|parent| parent == id)
                {
                    self.interrupt(&SdkError::integration("prompt output owner was removed"));
                } else {
                    self.messages.remove(id);
                    self.parts.retain(|_, owner| owner != id);
                }
            }
            OpenCodeKnownEvent::MessagePartRemoved(event) => {
                if self
                    .parts
                    .get(event.part_id())
                    .is_some_and(|owner| owner == event.message_id())
                {
                    if event.part_id() == self.request_part_id
                        || self.owns_assistant(event.message_id())
                    {
                        self.interrupt(&SdkError::integration("prompt output part was removed"));
                    } else {
                        self.parts.remove(event.part_id());
                    }
                }
            }
            OpenCodeKnownEvent::MessagePartDelta(delta) if live && fresh => {
                if self.owns_assistant(delta.message_id())
                    && self
                        .parts
                        .get(delta.part_id())
                        .is_some_and(|message| message == delta.message_id())
                    && let Some(text) = delta.text()
                {
                    self.output.fragment(text);
                }
            }
            OpenCodeKnownEvent::MessagePartDelta(_)
            | OpenCodeKnownEvent::Connected
            | OpenCodeKnownEvent::Heartbeat
            | OpenCodeKnownEvent::Disposed
            | OpenCodeKnownEvent::InstanceDisposed(_)
            | OpenCodeKnownEvent::SessionCreated(_)
            | OpenCodeKnownEvent::SessionUpdated(_)
            | OpenCodeKnownEvent::SessionDeleted(_)
            | OpenCodeKnownEvent::SessionCompacted(_)
            | OpenCodeKnownEvent::SessionIdle(_)
            | OpenCodeKnownEvent::SessionStatus(_)
            | OpenCodeKnownEvent::SessionDiff(_)
            | OpenCodeKnownEvent::SessionError(_)
            | OpenCodeKnownEvent::PermissionAsked(_)
            | OpenCodeKnownEvent::PermissionReplied(_)
            | OpenCodeKnownEvent::PermissionV2Asked(_)
            | OpenCodeKnownEvent::PermissionV2Replied(_)
            | OpenCodeKnownEvent::QuestionAsked(_)
            | OpenCodeKnownEvent::QuestionReplied(_)
            | OpenCodeKnownEvent::QuestionRejected(_)
            | OpenCodeKnownEvent::QuestionV2Asked(_)
            | OpenCodeKnownEvent::QuestionV2Replied(_)
            | OpenCodeKnownEvent::QuestionV2Rejected(_)
            | OpenCodeKnownEvent::SessionNext(_)
            | OpenCodeKnownEvent::PrivateSession(_)
            | OpenCodeKnownEvent::TodoUpdated(_)
            | OpenCodeKnownEvent::PrivateGlobal(_)
            | OpenCodeKnownEvent::UnknownFuture => {}
        }
        if self.messages.len().saturating_add(self.parts.len()) > MAX_PROMPT_OUTPUT_OWNERS {
            self.interrupt(&SdkError::integration(
                "prompt output ownership exceeds its bound",
            ));
        }
    }
}

pub(super) enum OpenCodePromptDurability {
    Reserved,
    CleanupRetrying {
        pending: BTreeSet<NotificationId>,
        last_error: SdkError,
    },
    CleanupFailed {
        pending: BTreeSet<NotificationId>,
        error: SdkError,
    },
    Releasable,
}

enum OpenCodePromptRuntimeState {
    Live {
        owner: OpenCodePromptLocalOwner,
        cancellation: OpenCodeLiveCancellation,
    },
    Detached {
        owner: OpenCodePromptLocalOwner,
        cancellation: OpenCodeDetachedCancellation,
    },
    Recovery {
        cancellation: OpenCodeRecoveryCancellation,
    },
}

struct OpenCodePromptLocalOwner {
    admission: HierarchyAdmission,
    permit: tokio::sync::OwnedSemaphorePermit,
}

enum OpenCodeLiveCancellation {
    Available,
    Requested,
}

enum OpenCodeDetachedCancellation {
    Available,
    Requested,
    Owned(OpenCodeCancellationOwner),
}

enum OpenCodeRecoveryCancellation {
    Requested,
    Owned(OpenCodeCancellationOwner),
}

enum OpenCodePromptRelease {
    Unstarted,
    Terminal,
}

pub(crate) struct OpenCodePromptRegistration<S: StateStoreBackend> {
    generation: OpenCodePromptGeneration<S>,
}

pub(crate) struct OpenCodePromptRecovery<S: StateStoreBackend> {
    generation: OpenCodePromptGeneration<S>,
    owner: OpenCodeCancellationOwner,
}

pub(crate) struct OpenCodeAbortJoin<S: StateStoreBackend> {
    generation: OpenCodePromptGeneration<S>,
}

pub(crate) enum OpenCodeAbort<S: StateStoreBackend> {
    Idle,
    Join(OpenCodeAbortJoin<S>),
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum OpenCodeAbortOutcome {
    Complete,
    RepairedInvalidState,
}

#[derive(Clone)]
pub(crate) enum OpenCodeAbortProgress {
    Pending,
    Deletion {
        outcome: OpenCodeAbortOutcome,
        failure: Option<SdkError>,
    },
    Settling(OpenCodeAbortOutcome),
    Settled(OpenCodeAbortOutcome),
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum OpenCodeNativePromptState {
    Active,
    Idle,
}

impl OpenCodePromptTasks {
    fn spawn<F>(&mut self, future: F) -> Result<(), F>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        match self {
            Self::Open {
                running,
                prior_failure,
                revoked: false,
            } => {
                while let Some(result) = running.try_join_next() {
                    *prior_failure |= result.is_err();
                }
                running.spawn(future);
                Ok(())
            }
            Self::Open { revoked: true, .. } | Self::Sealed => Err(future),
        }
    }

    fn revoke(&mut self) {
        if let Self::Open {
            running, revoked, ..
        } = self
        {
            *revoked = true;
            running.abort_all();
        }
    }

    fn seal(&mut self) -> Option<(JoinSet<()>, bool, bool)> {
        let state = std::mem::replace(self, Self::Sealed);
        match state {
            Self::Open {
                mut running,
                mut prior_failure,
                revoked,
            } => {
                while let Some(result) = running.try_join_next() {
                    prior_failure |= result.is_err_and(|error| !revoked || !error.is_cancelled());
                }
                Some((running, prior_failure, revoked))
            }
            Self::Sealed => None,
        }
    }
}

impl OpenCodePromptDurability {
    fn is_releasable(&self) -> bool {
        matches!(self, Self::Releasable)
    }

    fn settle_native_cleanup(&mut self, notification: &NotificationId) {
        match self {
            Self::CleanupRetrying { pending, .. } | Self::CleanupFailed { pending, .. } => {
                pending.remove(notification);
                if pending.is_empty() {
                    *self = Self::Reserved;
                }
            }
            Self::Reserved | Self::Releasable => {}
        }
    }
}

impl OpenCodePromptStartError {
    fn storage(error: storage_sdk::StateStoreError, integration_message: &'static str) -> Self {
        match error {
            storage_sdk::StateStoreError::TransactionConflict { .. } => Self::TransactionConflict,
            _ => Self::Failure(SdkError::integration(integration_message)),
        }
    }
}

impl From<SdkError> for OpenCodePromptStartError {
    fn from(error: SdkError) -> Self {
        Self::Failure(error)
    }
}

impl<S: StateStoreBackend> Clone for OpenCodePromptState<S> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            notifications: self.notifications.clone(),
            permits: self.permits.clone(),
            changes: self.changes.clone(),
            statuses: self.statuses.clone(),
            registrations: self.registrations.clone(),
            tasks: self.tasks.clone(),
        }
    }
}

impl<S: StateStoreBackend> Clone for OpenCodePromptGeneration<S> {
    fn clone(&self) -> Self {
        Self {
            prompts: self.prompts.clone(),
            conversation_id: self.conversation_id.clone(),
            generation_id: self.generation_id.clone(),
        }
    }
}

impl<S: StateStoreBackend> OpenCodePromptState<S> {
    pub(super) fn new(
        storage: AgentStorage<S>,
        notifications: NotificationStore<S>,
        prompt_limit: usize,
    ) -> Self {
        Self {
            storage,
            notifications,
            permits: Arc::new(tokio::sync::Semaphore::new(prompt_limit)),
            changes: tokio::sync::watch::channel(0).0,
            statuses: tokio::sync::broadcast::channel(prompt_limit.max(1)).0,
            registrations: Arc::new(Mutex::new(BTreeMap::new())),
            tasks: Arc::new(Mutex::new(OpenCodePromptTasks::Open {
                running: JoinSet::new(),
                prior_failure: false,
                revoked: false,
            })),
        }
    }

    fn spawn_task<F>(&self, future: F) -> Result<(), F>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .spawn(future)
    }

    pub(super) fn abort_tasks(&self) {
        // Reject racing admission and revoke tasks without taking their JoinSet:
        // explicit shutdown can still join them and report the event owner's error.
        self.tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .revoke();
    }

    pub(super) async fn shutdown_tasks(&self, deadline: Instant) -> Result<(), SdkError> {
        let Some((mut running, mut prior_failure, revoked)) = self
            .tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .seal()
        else {
            return Err(SdkError::invalid_state(
                "prompt task owner is already sealed",
            ));
        };
        loop {
            while let Some(result) = running.try_join_next() {
                prior_failure |= result.is_err_and(|error| !revoked || !error.is_cancelled());
            }
            if running.is_empty() {
                return if prior_failure {
                    Err(SdkError::integration("prompt task terminated unexpectedly")
                        .with_detail(ErrorDetail::TaskTerminated)
                        .at_phase(ErrorPhase::Cleanup))
                } else {
                    Ok(())
                };
            }
            match tokio::time::timeout_at(deadline, running.join_next()).await {
                Ok(Some(result)) => {
                    prior_failure |= result.is_err_and(|error| !revoked || !error.is_cancelled());
                }
                Ok(None) => {}
                Err(_) => {
                    running.abort_all();
                    while let Some(result) = running.join_next().await {
                        prior_failure |= result.is_err_and(|error| !error.is_cancelled());
                    }
                    return if prior_failure {
                        Err(SdkError::integration("prompt task terminated unexpectedly")
                            .with_detail(ErrorDetail::TaskTerminated)
                            .at_phase(ErrorPhase::Cleanup))
                    } else {
                        Err(SdkError::timeout("prompt task shutdown deadline elapsed"))
                    };
                }
            }
        }
    }

    pub(super) fn permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, SdkError> {
        self.permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| SdkError::busy("agent prompt concurrency is saturated"))
    }

    pub(super) fn changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.changes.subscribe()
    }

    pub(super) fn statuses(
        &self,
    ) -> tokio::sync::broadcast::Receiver<(ConversationId, OpenCodePromptStatusSignal)> {
        self.statuses.subscribe()
    }

    pub(super) fn mark_changed(&self) {
        self.changes
            .send_modify(|version| *version = version.wrapping_add(1));
    }

    pub(super) fn publish_status(&self, status: (ConversationId, OpenCodePromptStatusSignal)) {
        let _ = self.statuses.send(status);
    }

    pub(super) async fn continuations(
        &self,
        conversation_id: &ConversationId,
    ) -> Result<Vec<OpenCodeContinuation>, SdkError> {
        let prefix = storage_sdk::state::record::StateKeyPrefix::new(format!(
            "{}:",
            conversation_id.as_str()
        ))
        .map_err(|_| SdkError::invalid_state("continuation prefix is invalid"))?;
        let mut continuations = Vec::new();
        let mut cursor = None;
        for _ in 0..MAX_CONTINUATION_PAGES {
            let mut request =
                storage_sdk::state::record::StateListRequest::new(OpenCodeContinuation::NAMESPACE)
                    .map_err(|_| SdkError::invalid_state("continuation request is invalid"))?
                    .with_key_prefix(prefix.clone())
                    .with_limit(storage_sdk::state::record::StateListRequest::MAX_LIMIT)
                    .map_err(|_| SdkError::invalid_state("continuation limit is invalid"))?;
            if let Some(next) = cursor.take() {
                request = request
                    .start_after(next)
                    .map_err(|_| SdkError::invalid_state("continuation cursor is invalid"))?;
            }
            let page = self.storage.store().list(request).await.map_err(|error| {
                SdkError::from_state_store(error, "continuations could not be listed")
                    .in_phase(ErrorPhase::StateRead)
            })?;
            for snapshot in page.records {
                continuations.push(OpenCodeContinuation::decode(snapshot, conversation_id)?);
            }
            let Some(next) = page.next_start_after else {
                return Ok(continuations);
            };
            cursor = Some(next);
        }
        Err(SdkError::invalid_state(
            "continuation inventory exceeds its private page bound",
        ))
    }

    pub(super) async fn continuation_reconciliation_matches(
        &self,
        continuation: &OpenCodeContinuation,
    ) -> Result<bool, SdkError> {
        let mut transaction = self
            .storage
            .store()
            .begin_transaction()
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "continuation healing could not begin")
                    .in_phase(ErrorPhase::StateWrite)
            })?;
        let Some(snapshot) =
            transaction
                .load(continuation.state_key()?)
                .await
                .map_err(|error| {
                    SdkError::from_state_store(error, "continuation could not be loaded")
                        .in_phase(ErrorPhase::StateRead)
                })?
        else {
            return Ok(false);
        };
        if OpenCodeContinuation::decode(snapshot, continuation.conversation_id())? != *continuation
        {
            return Ok(false);
        }
        match NotificationStore::<S>::ensure_no_resolving_interaction_in_transaction(
            &mut transaction,
            continuation.conversation_id(),
        )
        .await
        {
            Ok(()) => Ok(true),
            Err(error) if matches!(error.kind(), ErrorKind::Busy | ErrorKind::Closed) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub(super) async fn continuation_matches(
        &self,
        expected: &OpenCodeContinuation,
    ) -> Result<bool, SdkError> {
        let Some(snapshot) = self
            .storage
            .store()
            .load(expected.state_key()?)
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "continuation could not be checked")
                    .in_phase(ErrorPhase::StateRead)
            })?
        else {
            return Ok(false);
        };
        Ok(OpenCodeContinuation::decode(snapshot, expected.conversation_id())? == *expected)
    }

    pub(super) fn generation(
        &self,
        conversation_id: &ConversationId,
        generation_id: &str,
    ) -> OpenCodePromptGeneration<S> {
        OpenCodePromptGeneration {
            prompts: self.clone(),
            conversation_id: conversation_id.clone(),
            generation_id: generation_id.to_owned(),
        }
    }

    pub(super) fn observe_output(
        &self,
        conversation_id: &ConversationId,
        event: &OpenCodeKnownEvent,
        live: bool,
        fresh: bool,
    ) {
        if let Some(reservation) = self
            .registrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get_mut(conversation_id)
            && let Some(observation) = &mut reservation.observation
        {
            observation.observe(event, live, fresh);
        }
        self.mark_changed();
    }

    pub(super) fn interrupt_outputs(&self, error: &SdkError) {
        for reservation in self
            .registrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values_mut()
        {
            if let Some(observation) = &mut reservation.observation {
                observation.interrupt(error);
            }
        }
        self.mark_changed();
    }

    pub(super) fn register(
        &self,
        conversation_id: ConversationId,
        generation_id: String,
        admission: HierarchyAdmission,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<OpenCodePromptRegistration<S>, SdkError> {
        let mut registrations = match self.registrations.lock() {
            Ok(registrations) => registrations,
            Err(_) => {
                return admission.publish(Err(SdkError::integration(
                    "prompt generation registry could not be locked",
                )));
            }
        };
        if registrations.contains_key(&conversation_id) {
            drop(registrations);
            return admission.publish(Err(SdkError::busy(
                "conversation already has an active prompt",
            )));
        }
        registrations.insert(
            conversation_id.clone(),
            OpenCodePromptReservation {
                generation_id: generation_id.clone(),
                runtime: OpenCodePromptRuntimeState::Live {
                    owner: OpenCodePromptLocalOwner { admission, permit },
                    cancellation: OpenCodeLiveCancellation::Available,
                },
                durability: OpenCodePromptDurability::Reserved,
                abort_joiners: 0,
                abort: OpenCodeAbortProgress::Pending,
                observation: None,
            },
        );
        Ok(OpenCodePromptRegistration {
            generation: self.generation(&conversation_id, &generation_id),
        })
    }

    pub(super) fn release_runtime(&self, conversation_id: &ConversationId) {
        let owner = {
            let mut registrations = match self.registrations.lock() {
                Ok(registrations) => registrations,
                Err(poisoned) => poisoned.into_inner(),
            };
            let Some(reservation) = registrations.get_mut(conversation_id) else {
                return;
            };
            if reservation.abort.deletion_pending() {
                return;
            }
            reservation.durability = OpenCodePromptDurability::Releasable;
            reservation.abort.settle();
            reservation.clear_recovery_claim();
            if reservation.abort_joiners == 0 {
                registrations
                    .remove(conversation_id)
                    .and_then(OpenCodePromptReservation::into_local_owner)
            } else {
                reservation.release_local_for_recovery()
            }
        };
        self.mark_changed();
        if let Some(owner) = owner {
            owner.release();
        }
    }

    pub(super) fn detached_generation(
        &self,
        conversation_id: &ConversationId,
    ) -> Result<Option<String>, SdkError> {
        let registrations = self.registrations.lock().map_err(|_| {
            SdkError::integration("prompt generation registry could not be locked")
                .in_phase(ErrorPhase::Admission)
        })?;
        Ok(registrations.get(conversation_id).and_then(|reservation| {
            reservation
                .is_detached()
                .then(|| reservation.generation_id.clone())
        }))
    }

    pub(super) fn retain_native_cleanup(
        &self,
        continuation: &OpenCodeContinuation,
    ) -> Result<(), SdkError> {
        let generation = continuation.generation_id().ok_or_else(|| {
            SdkError::conflict("native cleanup has no retained prompt generation")
        })?;
        let mut registrations = self
            .registrations
            .lock()
            .map_err(|_| SdkError::integration("prompt generation registry could not be locked"))?;
        match registrations.get(continuation.conversation_id()) {
            Some(reservation) if reservation.generation_id == generation => Ok(()),
            Some(_) => Err(SdkError::conflict("native cleanup generation changed")),
            None => {
                registrations.insert(
                    continuation.conversation_id().clone(),
                    OpenCodePromptReservation {
                        generation_id: generation.to_owned(),
                        runtime: OpenCodePromptRuntimeState::Recovery {
                            cancellation: OpenCodeRecoveryCancellation::Requested,
                        },
                        durability: OpenCodePromptDurability::Reserved,
                        abort_joiners: 0,
                        abort: OpenCodeAbortProgress::Pending,
                        observation: None,
                    },
                );
                Ok(())
            }
        }
    }

    pub(super) fn observe_native_cleanup(
        &self,
        continuation: &OpenCodeContinuation,
        progress: OpenCodePromptDurability,
    ) -> Result<(), SdkError> {
        let mut registrations = self
            .registrations
            .lock()
            .map_err(|_| SdkError::integration("prompt generation registry could not be locked"))?;
        let Some(reservation) = registrations
            .get_mut(continuation.conversation_id())
            .filter(|reservation| {
                Some(reservation.generation_id.as_str()) == continuation.generation_id()
            })
        else {
            if matches!(progress, OpenCodePromptDurability::Reserved) {
                return Ok(());
            }
            return Err(SdkError::conflict(
                "native cleanup observation lost its exact generation",
            ));
        };
        if matches!(progress, OpenCodePromptDurability::Reserved) {
            // Native acknowledgement only settles its own obligation. A sibling
            // from the same generation cannot erase another request's uncertainty.
            reservation
                .durability
                .settle_native_cleanup(continuation.notification_id());
        } else if !matches!(
            reservation.durability,
            OpenCodePromptDurability::CleanupFailed { .. }
        ) {
            reservation.durability = progress;
        }
        drop(registrations);
        self.mark_changed();
        Ok(())
    }

    pub(super) fn native_cleanup_failure(
        &self,
        conversation: &ConversationId,
        generation: &str,
        deadline: Instant,
    ) -> Result<(), SdkError> {
        let registrations = self
            .registrations
            .lock()
            .map_err(|_| SdkError::integration("prompt generation registry could not be locked"))?;
        let Some(reservation) = registrations
            .get(conversation)
            .filter(|reservation| reservation.generation_id == generation)
        else {
            return Ok(());
        };
        match &reservation.durability {
            OpenCodePromptDurability::CleanupFailed { error, .. } => Err(error.clone()),
            OpenCodePromptDurability::CleanupRetrying { last_error, .. }
                if Instant::now() >= deadline =>
            {
                Err(
                    SdkError::timeout("native interaction cleanup deadline elapsed")
                        .at_phase(ErrorPhase::Reconciliation)
                        .with_supplementary(last_error.clone()),
                )
            }
            _ => Ok(()),
        }
    }

    pub(super) fn registered_abort(
        &self,
        conversation_id: &ConversationId,
    ) -> Result<Option<OpenCodeAbort<S>>, SdkError> {
        let target = {
            let mut registrations = self.registrations.lock().map_err(|_| {
                SdkError::integration("prompt generation registry could not be locked")
                    .in_phase(ErrorPhase::Admission)
            })?;
            match registrations.get_mut(conversation_id) {
                Some(reservation)
                    if reservation.durability.is_releasable()
                        && matches!(
                            reservation.abort,
                            OpenCodeAbortProgress::Settled(
                                OpenCodeAbortOutcome::RepairedInvalidState
                            )
                        ) =>
                {
                    Some(OpenCodeAbort::Idle)
                }
                Some(reservation) => {
                    reservation.abort_joiners =
                        reservation.abort_joiners.checked_add(1).ok_or_else(|| {
                            SdkError::busy("prompt cancellation join capacity is exhausted")
                        })?;
                    reservation.request_cancellation();
                    Some(OpenCodeAbort::Join(OpenCodeAbortJoin {
                        generation: self.generation(conversation_id, &reservation.generation_id),
                    }))
                }
                None => None,
            }
        };
        if matches!(&target, Some(OpenCodeAbort::Join(_))) {
            self.mark_changed();
        }
        Ok(target)
    }

    pub(super) fn register_recovered_abort(
        &self,
        conversation_id: &ConversationId,
        durable_generation_id: Option<String>,
    ) -> Result<OpenCodeAbort<S>, SdkError> {
        let generation_id = {
            let mut registrations = self.registrations.lock().map_err(|_| {
                SdkError::integration("prompt generation registry could not be locked")
                    .in_phase(ErrorPhase::Admission)
            })?;
            match registrations.get_mut(conversation_id) {
                Some(reservation) => {
                    reservation.abort_joiners =
                        reservation.abort_joiners.checked_add(1).ok_or_else(|| {
                            SdkError::busy("prompt cancellation join capacity is exhausted")
                        })?;
                    reservation.request_cancellation();
                    Some(reservation.generation_id.clone())
                }
                None => durable_generation_id.inspect(|generation_id| {
                    registrations.insert(
                        conversation_id.clone(),
                        OpenCodePromptReservation {
                            generation_id: generation_id.clone(),
                            runtime: OpenCodePromptRuntimeState::Recovery {
                                cancellation: OpenCodeRecoveryCancellation::Requested,
                            },
                            durability: OpenCodePromptDurability::Reserved,
                            abort_joiners: 1,
                            abort: OpenCodeAbortProgress::Pending,
                            observation: None,
                        },
                    );
                }),
            }
        };
        let Some(generation_id) = generation_id else {
            return Ok(OpenCodeAbort::Idle);
        };
        self.mark_changed();
        Ok(OpenCodeAbort::Join(OpenCodeAbortJoin {
            generation: self.generation(conversation_id, &generation_id),
        }))
    }

    pub(super) fn mark_prompt_releasable(
        &self,
        conversation_id: &ConversationId,
        generation_id: &str,
    ) {
        let released = {
            let mut registrations = match self.registrations.lock() {
                Ok(registrations) => registrations,
                Err(poisoned) => poisoned.into_inner(),
            };
            let Some(reservation) = registrations.get_mut(conversation_id) else {
                return;
            };
            if reservation.generation_id != generation_id || reservation.abort.deletion_pending() {
                return;
            }
            reservation.durability = OpenCodePromptDurability::Releasable;
            reservation.abort.settle();
            if !reservation.releases_with_durable_state() {
                None
            } else if reservation.abort_joiners == 0 {
                registrations
                    .remove(conversation_id)
                    .and_then(OpenCodePromptReservation::into_local_owner)
            } else if reservation.is_detached() {
                reservation.release_local_for_recovery()
            } else {
                None
            }
        };
        self.mark_changed();
        if let Some(ownership) = released {
            ownership.release();
        }
    }

    fn abort_progress(
        &self,
        conversation_id: &ConversationId,
        generation_id: &str,
    ) -> Result<Option<OpenCodeAbortProgress>, SdkError> {
        let registrations = self.registrations.lock().map_err(|_| {
            SdkError::integration("prompt generation registry could not be locked")
                .in_phase(ErrorPhase::Admission)
        })?;
        Ok(registrations
            .get(conversation_id)
            .filter(|reservation| reservation.generation_id == generation_id)
            .map(|reservation| reservation.abort.clone()))
    }

    fn publish_abort_outcome(
        &self,
        conversation_id: &ConversationId,
        generation_id: &str,
        outcome: OpenCodeAbortOutcome,
    ) -> Result<(), SdkError> {
        let mut registrations = self.registrations.lock().map_err(|_| {
            SdkError::integration("prompt generation registry could not be locked")
                .in_phase(ErrorPhase::Admission)
        })?;
        let Some(reservation) = registrations.get_mut(conversation_id) else {
            return Ok(());
        };
        if reservation.generation_id != generation_id {
            return Err(SdkError::conflict(
                "prompt abort completion changed generation ownership",
            ));
        }
        reservation.abort.publish(outcome)?;
        drop(registrations);
        self.mark_changed();
        Ok(())
    }
}

impl OpenCodeAbortProgress {
    fn publish(&mut self, outcome: OpenCodeAbortOutcome) -> Result<(), SdkError> {
        match self {
            Self::Pending => *self = Self::Settling(outcome),
            Self::Deletion {
                outcome: current, ..
            } if *current == outcome => {
                *self = Self::Settling(outcome);
            }
            Self::Settling(current) | Self::Settled(current) if *current == outcome => {}
            Self::Deletion { .. } | Self::Settling(_) | Self::Settled(_) => {
                return Err(SdkError::invalid_state(
                    "prompt abort completion contradicts its exact prior outcome",
                ));
            }
        }
        Ok(())
    }

    fn settle(&mut self) {
        match self {
            Self::Pending => *self = Self::Settled(OpenCodeAbortOutcome::Complete),
            Self::Settling(outcome) => *self = Self::Settled(*outcome),
            Self::Deletion { .. } | Self::Settled(_) => {}
        }
    }

    fn deletion_pending(&self) -> bool {
        matches!(self, Self::Deletion { .. })
    }

    fn settled(&self) -> Option<OpenCodeAbortOutcome> {
        match self {
            Self::Settled(outcome) => Some(*outcome),
            _ => None,
        }
    }

    fn outcome(self) -> Option<OpenCodeAbortOutcome> {
        match self {
            Self::Pending | Self::Deletion { .. } => None,
            Self::Settling(outcome) | Self::Settled(outcome) => Some(outcome),
        }
    }
}

impl OpenCodeNativePromptState {
    pub(crate) fn from_active(active: bool) -> Self {
        if active { Self::Active } else { Self::Idle }
    }

    pub(crate) fn from_status(status: OpenCodePromptStatus) -> Self {
        match status {
            OpenCodePromptStatus::Active => Self::Active,
            OpenCodePromptStatus::Idle => Self::Idle,
        }
    }

    fn is_idle(self) -> bool {
        self == Self::Idle
    }
}

impl<S: StateStoreBackend> OpenCodePromptGeneration<S> {
    pub(crate) fn conversation_id(&self) -> &ConversationId {
        &self.conversation_id
    }

    pub(crate) fn observe_output(
        &self,
        session_id: String,
        part_id: String,
        output: Arc<PromptOutput>,
    ) -> Result<(), SdkError> {
        let mut registrations = self
            .prompts
            .registrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let reservation = registrations
            .get_mut(&self.conversation_id)
            .filter(|reservation| reservation.generation_id == self.generation_id)
            .ok_or_else(|| SdkError::closed("prompt output generation is no longer owned"))?;
        if reservation.observation.is_some() {
            return Err(SdkError::conflict(
                "prompt output observation already exists",
            ));
        }
        reservation.observation = Some(OpenCodePromptObservation::new(session_id, part_id, output));
        Ok(())
    }

    pub(crate) fn retain_execution_error(&self, error: &SdkError) {
        let mut registrations = self
            .prompts
            .registrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(observation) = registrations
            .get_mut(&self.conversation_id)
            .filter(|reservation| reservation.generation_id == self.generation_id)
            .and_then(|reservation| reservation.observation.as_mut())
        {
            observation.retain_execution_error(error);
        }
    }

    pub(crate) fn retained_execution_error(&self) -> Option<SdkError> {
        self.prompts
            .registrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&self.conversation_id)
            .filter(|reservation| reservation.generation_id == self.generation_id)
            .and_then(|reservation| reservation.observation.as_ref())
            .and_then(|observation| match &observation.state {
                OpenCodePromptObservationState::ExecutionFailed(error) => Some(error.clone()),
                OpenCodePromptObservationState::Observing
                | OpenCodePromptObservationState::Interrupted => None,
            })
    }

    pub(crate) fn output_complete(&self, message_id: &str) -> bool {
        self.prompts
            .registrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&self.conversation_id)
            .filter(|reservation| reservation.generation_id == self.generation_id)
            .and_then(|reservation| reservation.observation.as_ref())
            .is_none_or(|observation| observation.complete(message_id))
    }

    pub(crate) fn spawn_task<F>(&self, future: F) -> Result<(), F>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.prompts.spawn_task(future)
    }

    pub(crate) async fn start(
        &self,
        agent_id: &AgentId,
        binding: &OpenCodeSessionBinding,
        request_part_id: String,
        effective: OpenCodePromptEffective,
        timeout: Option<std::time::Duration>,
    ) -> Result<(), OpenCodePromptStartError> {
        if binding.conversation_id() != &self.conversation_id {
            return Err(OpenCodePromptStartError::Failure(SdkError::invalid_state(
                "prompt binding contradicts its generation owner",
            )));
        }
        let workspace_id = binding.workspace_id();
        let workspace_revision = binding.workspace_revision();
        let record = OpenCodePromptRecord::prepared(
            self.conversation_id.clone(),
            self.generation_id.clone(),
            request_part_id,
            effective,
            timeout,
        )?;
        let key = OpenCodePromptRecord::key(&self.conversation_id)?;
        let store = self.prompts.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            OpenCodePromptStartError::storage(error, "prompt transaction could not begin")
        })?;
        let workspace = transaction
            .load(WorkspaceRecord::key(agent_id, workspace_id)?)
            .await
            .map_err(|error| {
                OpenCodePromptStartError::storage(error, "prompt workspace could not be checked")
            })?
            .ok_or_else(|| {
                OpenCodePromptStartError::Failure(SdkError::not_found("workspace does not exist"))
            })?;
        let workspace = WorkspaceRecord::decode(workspace, agent_id, Some(workspace_id))?;
        if workspace.revision() != workspace_revision {
            return Err(OpenCodePromptStartError::Failure(SdkError::conflict(
                "conversation handle is stale",
            )));
        }
        let conversation = transaction
            .load(ConversationRecord::key(
                workspace_id,
                &self.conversation_id,
            )?)
            .await
            .map_err(|error| {
                OpenCodePromptStartError::storage(error, "prompt conversation could not be checked")
            })?
            .ok_or_else(|| {
                OpenCodePromptStartError::Failure(SdkError::not_found(
                    "conversation does not exist",
                ))
            })?;
        let conversation =
            ConversationRecord::decode(conversation, workspace_id, Some(&self.conversation_id))?;
        if conversation.workspace_revision() != workspace_revision {
            return Err(OpenCodePromptStartError::Failure(SdkError::conflict(
                "conversation handle is stale",
            )));
        }
        if let Some(snapshot) = transaction.load(key.clone()).await.map_err(|error| {
            OpenCodePromptStartError::storage(error, "prompt state could not be checked")
        })? {
            let existing: OpenCodePromptRecord =
                serde::Deserialize::deserialize(snapshot.record.value.as_ref()).map_err(|_| {
                    OpenCodePromptStartError::Failure(SdkError::invalid_state(
                        "stored prompt state is malformed",
                    ))
                })?;
            existing.validate()?;
            if existing.conversation_id() != &self.conversation_id {
                return Err(OpenCodePromptStartError::Failure(SdkError::invalid_state(
                    "stored prompt contradicts its key owner",
                )));
            }
            if existing.matches_prepared(&record) {
                return Ok(());
            }
            return Err(OpenCodePromptStartError::Failure(SdkError::busy(
                if existing.is_terminal() {
                    "conversation has unreconciled terminal prompt state"
                } else {
                    "conversation already has an active prompt"
                },
            )));
        }
        match NotificationStore::<S>::check_no_unresolved_interaction_in_transaction(
            &mut transaction,
            &self.conversation_id,
        )
        .await
        {
            Ok(()) => {}
            Err(NotificationInteractionCheckError::TransactionConflict) => {
                return Err(OpenCodePromptStartError::TransactionConflict);
            }
            Err(NotificationInteractionCheckError::Failure(error)) => {
                return Err(OpenCodePromptStartError::Failure(error));
            }
        }
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                key,
                serde_json::to_value(record)
                    .map_err(|_| SdkError::invalid_state("prompt could not be encoded"))?,
            ))
            .await
            .map_err(|error| {
                OpenCodePromptStartError::storage(error, "prompt state could not be stored")
            })?;
        transaction.commit().await.map_err(|error| {
            OpenCodePromptStartError::storage(error, "prompt transaction could not commit")
        })?;
        self.prompts.mark_changed();
        Ok(())
    }

    pub(crate) async fn structured_parent_proof(
        &self,
        expected_session_id: &str,
        expected_message_id: &str,
        expected_part_id: &str,
    ) -> Result<bool, SdkError> {
        let snapshot = self
            .prompts
            .storage
            .store()
            .load(OpenCodePromptRecord::key(&self.conversation_id)?)
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "structured-parent state could not be loaded")
                    .in_phase(ErrorPhase::StateRead)
            })?
            .ok_or_else(|| SdkError::invalid_state("prompt state is missing"))?;
        let record = OpenCodePromptRecord::decode(snapshot, &self.conversation_id)?;
        if record.generation_id() != self.generation_id {
            return Err(SdkError::conflict(
                "structured-parent proof belongs to another generation",
            ));
        }
        record.structured_parent_proof(expected_session_id, expected_message_id, expected_part_id)
    }

    pub(crate) async fn claim_structured_parent_recovery(
        &self,
        expected_session_id: &str,
        expected_message_id: &str,
        expected_part_id: &str,
    ) -> Result<bool, SdkError> {
        let key = OpenCodePromptRecord::key(&self.conversation_id)?;
        let store = self.prompts.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            SdkError::from_state_store(error, "structured-parent recovery could not begin")
                .in_phase(ErrorPhase::StateWrite)
        })?;
        let snapshot = transaction
            .load(key.clone())
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "structured-parent state could not be loaded")
                    .in_phase(ErrorPhase::StateRead)
            })?
            .ok_or_else(|| SdkError::invalid_state("prompt state is missing"))?;
        let mut record = OpenCodePromptRecord::decode(snapshot, &self.conversation_id)?;
        if record.generation_id() != self.generation_id {
            return Err(SdkError::conflict(
                "structured-parent recovery belongs to another generation",
            ));
        }
        if record.structured_parent_proof(
            expected_session_id,
            expected_message_id,
            expected_part_id,
        )? {
            return Ok(false);
        }
        if !record.claim_structured_parent_recovery()? {
            return Err(SdkError::invalid_state(
                "structured-parent recovery was already claimed",
            ));
        }
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                key.clone(),
                serde_json::to_value(&record).map_err(|_| {
                    SdkError::invalid_state("structured-parent recovery could not be encoded")
                })?,
            ))
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "structured-parent recovery could not be stored")
                    .in_phase(ErrorPhase::StateWrite)
            })?;
        let commit = transaction.commit().await;
        let claimed = if commit.is_ok() {
            true
        } else {
            let snapshot = store
                .load(key)
                .await
                .map_err(|error| {
                    SdkError::from_state_store(
                        error,
                        "structured-parent recovery outcome could not be checked",
                    )
                    .in_phase(ErrorPhase::StateRead)
                })?
                .ok_or_else(|| SdkError::invalid_state("prompt state is missing"))?;
            let current = OpenCodePromptRecord::decode(snapshot, &self.conversation_id)?;
            if current.generation_id() != self.generation_id {
                return Err(SdkError::conflict(
                    "structured-parent recovery changed generation ownership",
                ));
            }
            if current.structured_parent_proof(
                expected_session_id,
                expected_message_id,
                expected_part_id,
            )? {
                false
            } else if current.structured_parent_recovery_claimed() {
                true
            } else {
                return Err(SdkError::integration(
                    "structured-parent recovery could not commit",
                ));
            }
        };
        self.prompts.mark_changed();
        Ok(claimed)
    }

    async fn cancel_continuations_in_transaction(
        &self,
        transaction: &mut S::Transaction,
        continuations: Vec<OpenCodeContinuation>,
    ) -> Result<Vec<OpenCodeContinuation>, SdkError> {
        let mut reconciled = Vec::new();
        for continuation in continuations {
            let continuation_key =
                OpenCodeContinuation::key(&self.conversation_id, continuation.request_id())?;
            let snapshot = transaction
                .load(continuation_key.clone())
                .await
                .map_err(|error| {
                    SdkError::from_state_store(error, "continuation could not be loaded")
                        .in_phase(ErrorPhase::StateRead)
                })?
                .ok_or_else(|| SdkError::invalid_state("continuation is missing"))?;
            if OpenCodeContinuation::decode(snapshot, &self.conversation_id)? != continuation {
                return Err(SdkError::invalid_state(
                    "continuation changed before prompt termination",
                ));
            }
            if continuation.state() == OpenCodeContinuationState::Pending {
                NotificationStore::<S>::ensure_interaction_pending_in_transaction(
                    transaction,
                    &self.conversation_id,
                    continuation.notification_id(),
                )
                .await?;
                NotificationStore::<S>::reconcile_native_resolution_in_transaction(
                    transaction,
                    &self.conversation_id,
                    continuation.notification_id(),
                    ResolutionSettlement::Cancelled,
                )
                .await
                .map_err(NotificationSettlementError::into_reconciliation_sdk)?;
                let cancelled =
                    continuation.with_state(OpenCodeContinuationState::CancellationRequired)?;
                transaction
                    .put(storage_sdk::state::record::StateRecord::new(
                        continuation_key,
                        serde_json::to_value(&cancelled).map_err(|_| {
                            SdkError::invalid_state("cancelled continuation could not be encoded")
                        })?,
                    ))
                    .await
                    .map_err(|error| {
                        SdkError::from_state_store(
                            error,
                            "cancelled continuation could not be stored",
                        )
                        .in_phase(ErrorPhase::StateWrite)
                    })?;
                reconciled.push(cancelled);
            } else {
                reconciled.push(continuation);
            }
        }
        Ok(reconciled)
    }

    pub(crate) fn claim_recovery(
        &self,
        owner: &OpenCodeCancellationOwner,
    ) -> Result<Option<OpenCodePromptRecovery<S>>, SdkError> {
        {
            let mut registrations = self.prompts.registrations.lock().map_err(|_| {
                SdkError::integration("prompt generation registry could not be locked")
                    .in_phase(ErrorPhase::Admission)
            })?;
            match registrations.get_mut(&self.conversation_id) {
                Some(reservation) if reservation.generation_id == self.generation_id => {
                    if !reservation.claim_recovery(owner) {
                        return Ok(None);
                    }
                }
                Some(_) => return Ok(None),
                None => {
                    registrations.insert(
                        self.conversation_id.clone(),
                        OpenCodePromptReservation {
                            generation_id: self.generation_id.clone(),
                            runtime: OpenCodePromptRuntimeState::Recovery {
                                cancellation: OpenCodeRecoveryCancellation::Owned(owner.clone()),
                            },
                            durability: OpenCodePromptDurability::Reserved,
                            abort_joiners: 0,
                            abort: OpenCodeAbortProgress::Pending,
                            observation: None,
                        },
                    );
                }
            }
        }
        self.prompts.mark_changed();
        Ok(Some(OpenCodePromptRecovery {
            generation: self.clone(),
            owner: owner.clone(),
        }))
    }

    pub(crate) async fn request_abort(&self, deadline: Instant) -> Result<(), SdkError> {
        let key = OpenCodePromptRecord::key(&self.conversation_id)?;
        let store = self.prompts.storage.store();
        let mut last_error = None;
        loop {
            if Instant::now() >= deadline {
                let timeout = SdkError::timeout("prompt cancellation deadline elapsed")
                    .at_phase(ErrorPhase::Reconciliation);
                return Err(match last_error {
                    Some(error) => timeout.with_supplementary(error),
                    None => timeout,
                });
            }
            let failure = match store.begin_transaction().await {
                Err(error) => SdkError::from_state_store(error, "cancellation could not begin")
                    .in_phase(ErrorPhase::StateWrite),
                Ok(mut transaction) => {
                    let staged: Result<bool, SdkError> = async {
                        let Some(snapshot) =
                            transaction.load(key.clone()).await.map_err(|error| {
                                SdkError::from_state_store(
                                    error,
                                    "prompt state could not be loaded",
                                )
                                .in_phase(ErrorPhase::StateRead)
                            })?
                        else {
                            return Ok(false);
                        };
                        let mut record =
                            OpenCodePromptRecord::decode(snapshot, &self.conversation_id)?;
                        if record.generation_id() != self.generation_id
                            || !record.begin_cancellation()?
                        {
                            return Ok(false);
                        }
                        transaction
                            .put(storage_sdk::state::record::StateRecord::new(
                                key.clone(),
                                serde_json::to_value(record).map_err(|_| {
                                    SdkError::invalid_state(
                                        "cancellation state could not be encoded",
                                    )
                                })?,
                            ))
                            .await
                            .map_err(|error| {
                                SdkError::from_state_store(
                                    error,
                                    "cancellation state could not be stored",
                                )
                                .in_phase(ErrorPhase::StateWrite)
                            })?;
                        Ok(true)
                    }
                    .await;
                    match staged {
                        Err(primary) => primary.with_cleanup_result(
                            transaction.rollback().await.map_err(|error| {
                                SdkError::from_state_store(
                                    error,
                                    "cancellation transaction could not close",
                                )
                                .at_phase(ErrorPhase::Cleanup)
                            }),
                        ),
                        Ok(false) => {
                            transaction.rollback().await.map_err(|error| {
                                SdkError::from_state_store(
                                    error,
                                    "unchanged cancellation transaction could not close",
                                )
                                .at_phase(ErrorPhase::Cleanup)
                            })?;
                            return Ok(());
                        }
                        Ok(true) => match transaction.commit().await {
                            Ok(()) => {
                                self.prompts.mark_changed();
                                return Ok(());
                            }
                            Err(
                                error @ storage_sdk::StateStoreError::TransactionConflict { .. },
                            ) => SdkError::from_state_store(
                                error,
                                "cancellation transaction conflicted",
                            )
                            .in_phase(ErrorPhase::StateCommit),
                            Err(error) => {
                                // This admitted commit may have published. Only
                                // observation may confirm its intent; neither
                                // caller can re-enter the mutation after this.
                                return self
                                    .observe_cancellation_intent(
                                        deadline,
                                        SdkError::from_state_store(
                                            error,
                                            "cancellation could not commit",
                                        )
                                        .in_phase(ErrorPhase::StateCommit),
                                    )
                                    .await;
                            }
                        },
                    }
                }
            };
            if failure.context().phase == ErrorPhase::Cleanup
                || failure
                    .supplementary()
                    .iter()
                    .any(|error| error.context().phase == ErrorPhase::Cleanup)
                || !((failure.kind() == ErrorKind::Conflict
                    && failure.detail() == Some(&ErrorDetail::TransactionConflict))
                    || super::OpenCodeStateStore::<S>::retryable_state_error(&failure))
            {
                return Err(failure);
            }
            last_error = Some(failure);
            tokio::time::sleep_until(
                deadline.min(Instant::now() + std::time::Duration::from_millis(25)),
            )
            .await;
        }
    }

    async fn observe_cancellation_intent(
        &self,
        deadline: Instant,
        primary: SdkError,
    ) -> Result<(), SdkError> {
        let key = OpenCodePromptRecord::key(&self.conversation_id)?;
        let store = self.prompts.storage.store();
        let mut last_error = None;
        loop {
            if Instant::now() >= deadline {
                let mut timeout =
                    SdkError::timeout("cancellation commit observation deadline elapsed")
                        .at_phase(ErrorPhase::Reconciliation);
                if let Some(error) = last_error {
                    timeout = timeout.with_supplementary(error);
                }
                return Err(primary.with_supplementary(timeout));
            }
            match store.load(key.clone()).await {
                Ok(Some(snapshot)) => {
                    let record = OpenCodePromptRecord::decode(snapshot, &self.conversation_id)
                        .map_err(|error| primary.clone().with_supplementary(error))?;
                    if record.generation_id() == self.generation_id
                        && record.cancellation_intent_recorded()
                    {
                        self.prompts.mark_changed();
                        return Ok(());
                    }
                    return Err(primary.with_supplementary(
                        SdkError::integration("cancellation intent was not confirmed")
                            .at_phase(ErrorPhase::Reconciliation),
                    ));
                }
                Ok(None) => {
                    return Err(primary.with_supplementary(
                        SdkError::integration(
                            "prompt state is missing during cancellation observation",
                        )
                        .at_phase(ErrorPhase::Reconciliation),
                    ));
                }
                Err(error) => {
                    let observation = SdkError::from_state_store(
                        error,
                        "cancellation intent could not be observed",
                    )
                    .at_phase(ErrorPhase::Reconciliation);
                    if observation.detail() != Some(&ErrorDetail::TransactionConflict)
                        && !super::OpenCodeStateStore::<S>::retryable_state_error(&observation)
                    {
                        return Err(primary.with_supplementary(observation));
                    }
                    last_error = Some(observation);
                    tokio::time::sleep_until(
                        deadline.min(Instant::now() + std::time::Duration::from_millis(25)),
                    )
                    .await;
                }
            }
        }
    }

    pub(crate) async fn claim_delivery(&self, deadline: Instant) -> Result<bool, SdkError> {
        self.mutate(deadline, |record| record.claim_waiter_delivery())
            .await
    }

    pub(crate) async fn mark_dispatched(&self, deadline: Instant) -> Result<bool, SdkError> {
        self.mutate(deadline, |record| {
            record.mark_dispatched_awaiting_admission()
        })
        .await
    }

    pub(crate) async fn stop_before_dispatch(&self) -> Result<bool, SdkError> {
        let key = OpenCodePromptRecord::key(&self.conversation_id)?;
        let store = self.prompts.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            SdkError::from_state_store(error, "pre-dispatch prompt cleanup could not begin")
                .in_phase(ErrorPhase::StateWrite)
        })?;
        let snapshot = transaction.load(key.clone()).await.map_err(|error| {
            SdkError::from_state_store(error, "prompt state could not be loaded")
                .in_phase(ErrorPhase::StateRead)
        })?;
        let Some(snapshot) = snapshot else {
            drop(transaction);
            if self.is_registered()? {
                self.mark_releasable();
                return Ok(false);
            }
            return Err(SdkError::invalid_state("prompt state is missing"));
        };
        let mut record = OpenCodePromptRecord::decode(snapshot, &self.conversation_id)?;
        if record.generation_id() != self.generation_id {
            return Err(SdkError::conflict(
                "pre-dispatch prompt cleanup belongs to another generation",
            ));
        }
        let cancelled = record.stop_before_dispatch()?;
        if cancelled {
            transaction
                .put(storage_sdk::state::record::StateRecord::new(
                    key,
                    serde_json::to_value(record).map_err(|_| {
                        SdkError::invalid_state("cancelled prompt could not be encoded")
                    })?,
                ))
                .await
                .map_err(|error| {
                    SdkError::from_state_store(error, "cancelled prompt could not be stored")
                        .in_phase(ErrorPhase::StateWrite)
                })?;
        } else {
            transaction.delete(key.clone()).await.map_err(|error| {
                SdkError::from_state_store(error, "pre-dispatch prompt could not be removed")
                    .in_phase(ErrorPhase::StateWrite)
            })?;
        }
        transaction.commit().await.map_err(|error| {
            SdkError::from_state_store(error, "pre-dispatch prompt cleanup could not commit")
                .in_phase(ErrorPhase::StateCommit)
        })?;
        if !cancelled {
            self.mark_releasable();
        }
        self.prompts.mark_changed();
        Ok(cancelled)
    }

    pub(crate) async fn mark_admitted(&self, deadline: Instant) -> Result<bool, SdkError> {
        self.mutate(deadline, |record| record.observe_active_status())
            .await
    }

    pub(crate) async fn admission_observed(&self) -> Result<bool, SdkError> {
        let Some(snapshot) = self
            .prompts
            .storage
            .store()
            .load(OpenCodePromptRecord::key(&self.conversation_id)?)
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "prompt state could not be loaded")
                    .in_phase(ErrorPhase::StateRead)
            })?
        else {
            return Ok(false);
        };
        let record = OpenCodePromptRecord::decode(snapshot, &self.conversation_id)?;
        Ok(record.generation_id() == self.generation_id && record.admission_observed())
    }

    pub(crate) async fn mark_waiter_lost(&self, deadline: Instant) -> Result<(), SdkError> {
        self.mutate(deadline, |record| record.mark_waiter_lost().map(|_| ()))
            .await
    }

    pub(crate) async fn mark_timed_out(&self) -> Result<Option<bool>, SdkError> {
        let key = OpenCodePromptRecord::key(&self.conversation_id)?;
        let store = self.prompts.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            SdkError::from_state_store(error, "prompt timeout transition could not begin")
                .in_phase(ErrorPhase::StateWrite)
        })?;
        let Some(snapshot) = transaction.load(key.clone()).await.map_err(|error| {
            SdkError::from_state_store(error, "prompt state could not be loaded")
                .in_phase(ErrorPhase::StateRead)
        })?
        else {
            return Ok(None);
        };
        let mut record = OpenCodePromptRecord::decode(snapshot, &self.conversation_id)?;
        if record.generation_id() != self.generation_id {
            return Err(SdkError::conflict(
                "prompt timeout belongs to another generation",
            ));
        }
        let result = record.mark_waiter_timed_out()?;
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                key.clone(),
                serde_json::to_value(record)
                    .map_err(|_| SdkError::invalid_state("prompt timeout could not be encoded"))?,
            ))
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "prompt timeout could not be stored")
                    .in_phase(ErrorPhase::StateWrite)
            })?;
        let commit = transaction.commit().await;
        let result = if commit.is_ok() {
            result
        } else {
            let Some(snapshot) = store.load(key).await.map_err(|error| {
                SdkError::from_state_store(error, "prompt timeout outcome could not be checked")
                    .in_phase(ErrorPhase::StateRead)
            })?
            else {
                return Ok(None);
            };
            let committed = OpenCodePromptRecord::decode(snapshot, &self.conversation_id)?;
            if committed.generation_id() != self.generation_id {
                return Err(SdkError::conflict(
                    "prompt timeout changed generation ownership",
                ));
            }
            committed.committed_timeout_result().ok_or_else(|| {
                SdkError::integration("prompt timeout transition could not commit")
            })?
        };
        self.prompts.mark_changed();
        Ok(Some(result))
    }

    pub(crate) async fn lose_response(&self, deadline: Instant) -> Result<(), SdkError> {
        self.mutate(deadline, OpenCodePromptRecord::lose_response)
            .await
    }

    pub(crate) fn native_cleanup_failure(&self, deadline: Instant) -> Result<(), SdkError> {
        self.prompts
            .native_cleanup_failure(&self.conversation_id, &self.generation_id, deadline)
    }

    pub(crate) async fn requires_native_cleanup(&self) -> Result<bool, SdkError> {
        Ok(self
            .prompts
            .continuations(&self.conversation_id)
            .await?
            .iter()
            .any(OpenCodeContinuation::requires_native_cleanup))
    }

    pub(crate) async fn clear(&self) -> Result<(), SdkError> {
        let store = self.prompts.storage.store();
        if self
            .prompts
            .continuations(&self.conversation_id)
            .await?
            .iter()
            .any(OpenCodeContinuation::blocks_prompt_release)
        {
            return Err(SdkError::busy("prompt interactions remain unresolved"));
        }
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            SdkError::from_state_store(error, "prompt cleanup could not begin")
                .in_phase(ErrorPhase::StateWrite)
        })?;
        let key = OpenCodePromptRecord::key(&self.conversation_id)?;
        let Some(snapshot) = transaction.load(key.clone()).await.map_err(|error| {
            SdkError::from_state_store(error, "prompt state could not be loaded")
                .in_phase(ErrorPhase::StateRead)
        })?
        else {
            drop(transaction);
            let runtime_generation = {
                let registrations = self.prompts.registrations.lock().map_err(|_| {
                    SdkError::integration("prompt generation registry could not be locked")
                        .in_phase(ErrorPhase::Admission)
                })?;
                registrations
                    .get(&self.conversation_id)
                    .map(|reservation| reservation.generation_id.clone())
            };
            return match runtime_generation {
                Some(runtime_generation) if runtime_generation == self.generation_id => {
                    self.mark_releasable();
                    Ok(())
                }
                Some(_) => Err(SdkError::conflict(
                    "prompt cleanup belongs to another generation",
                )),
                None => Err(SdkError::invalid_state(
                    "prompt cleanup has no durable or live generation owner",
                )),
            };
        };
        let record = OpenCodePromptRecord::decode(snapshot, &self.conversation_id)?;
        if record.generation_id() != self.generation_id {
            return Err(SdkError::conflict(
                "prompt cleanup belongs to another generation",
            ));
        }
        if !record.clearable() {
            return Err(SdkError::busy("prompt is not ready for durable cleanup"));
        }
        transaction.delete(key).await.map_err(|error| {
            SdkError::from_state_store(error, "prompt state could not be cleared")
                .in_phase(ErrorPhase::StateWrite)
        })?;
        transaction.commit().await.map_err(|error| {
            SdkError::from_state_store(error, "prompt cleanup could not commit")
                .in_phase(ErrorPhase::StateCommit)
        })?;
        self.mark_releasable();
        self.prompts.mark_changed();
        Ok(())
    }

    pub(crate) async fn terminalize(
        &self,
        result: &Result<PromptResponse, SdkError>,
        deadline: Instant,
    ) -> Result<(), SdkError> {
        self.settle_prompt(
            OpenCodePromptSettlement::Response {
                cancellation_owner: None,
                result,
            },
            deadline,
        )
        .await
    }

    pub(crate) async fn terminalize_without_abort_dispatch(
        &self,
        owner: &OpenCodeCancellationOwner,
        result: &Result<PromptResponse, SdkError>,
        deadline: Instant,
    ) -> Result<(), SdkError> {
        self.settle_prompt(
            OpenCodePromptSettlement::Response {
                cancellation_owner: Some(owner),
                result,
            },
            deadline,
        )
        .await
    }

    async fn retry_terminal_before(error: SdkError, deadline: Instant) -> Result<(), SdkError> {
        if error.context().phase == ErrorPhase::Cleanup
            || error
                .supplementary()
                .iter()
                .any(|error| error.context().phase == ErrorPhase::Cleanup)
            || !((error.kind() == ErrorKind::Conflict
                && error.detail() == Some(&ErrorDetail::TransactionConflict))
                || super::OpenCodeStateStore::<S>::retryable_state_error(&error))
        {
            return Err(error);
        }
        tokio::time::sleep_until(
            deadline.min(Instant::now() + std::time::Duration::from_millis(25)),
        )
        .await;
        if Instant::now() >= deadline {
            return Err(SdkError::timeout("prompt persistence deadline elapsed")
                .at_phase(ErrorPhase::Settlement)
                .with_supplementary(error));
        }
        Ok(())
    }

    pub(crate) async fn prepare_live_interaction_cleanup(
        &self,
        deadline: Instant,
    ) -> Result<(), SdkError> {
        if !self.is_live()? {
            return Err(SdkError::invalid_state(
                "interaction cleanup lacks its live prompt owner",
            ));
        }
        self.settle_prompt(OpenCodePromptSettlement::InteractionCleanup, deadline)
            .await
    }

    async fn settle_prompt(
        &self,
        intent: OpenCodePromptSettlement<'_>,
        deadline: Instant,
    ) -> Result<(), SdkError> {
        let key = OpenCodePromptRecord::key(&self.conversation_id)?;
        let store = self.prompts.storage.store();
        // Before commit, every failed attempt must confirm rollback and recompute
        // the prompt and continuation decisions together in a fresh transaction.
        let (commit_error, expected_prompt, cancelled_continuations) = loop {
            if Instant::now() >= deadline {
                return Err(SdkError::timeout("prompt persistence deadline elapsed")
                    .at_phase(ErrorPhase::Settlement));
            }
            let continuations = if intent.cancels_interactions() {
                match self.prompts.continuations(&self.conversation_id).await {
                    Ok(continuations) => continuations,
                    Err(error) => {
                        Self::retry_terminal_before(error, deadline).await?;
                        continue;
                    }
                }
            } else {
                Vec::new()
            };
            if matches!(intent, OpenCodePromptSettlement::InteractionCleanup)
                && !continuations
                    .iter()
                    .any(|continuation| continuation.state() == OpenCodeContinuationState::Pending)
            {
                // Existing native obligations are reconciled by their current
                // owners. No unchanged commit is used as a preparation receipt.
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(SdkError::timeout("prompt persistence deadline elapsed")
                    .at_phase(ErrorPhase::Settlement));
            }
            let mut transaction = match store.begin_transaction().await {
                Ok(transaction) => transaction,
                Err(error) => {
                    Self::retry_terminal_before(
                        SdkError::from_state_store(
                            error,
                            "prompt terminal transaction could not begin",
                        )
                        .in_phase(ErrorPhase::StateWrite),
                        deadline,
                    )
                    .await?;
                    continue;
                }
            };
            let prepared = async {
                let snapshot = transaction
                    .load(key.clone())
                    .await
                    .map_err(|error| {
                        SdkError::from_state_store(error, "prompt state could not be loaded")
                            .in_phase(ErrorPhase::StateRead)
                    })?
                    .ok_or_else(|| SdkError::invalid_state("prompt state is missing"))?;
                let mut record = OpenCodePromptRecord::decode(snapshot, &self.conversation_id)?;
                if record.generation_id() != self.generation_id {
                    return Err(SdkError::invalid_state(
                        "prompt terminal contradicts the active prompt",
                    ));
                }
                intent.prepare(&mut record)?;
                if matches!(intent, OpenCodePromptSettlement::InteractionCleanup)
                    && record.is_cancelled()
                    && continuations.iter().any(|continuation| {
                        continuation.state() == OpenCodeContinuationState::Pending
                            && continuation.generation_id() != Some(self.generation_id.as_str())
                    })
                {
                    return Err(SdkError::invalid_state(
                        "interaction cleanup contradicts the cancelled prompt generation",
                    ));
                }
                if matches!(intent, OpenCodePromptSettlement::Response { .. }) {
                    transaction
                        .put(storage_sdk::state::record::StateRecord::new(
                            key.clone(),
                            serde_json::to_value(&record).map_err(|_| {
                                SdkError::invalid_state("prompt terminal could not be encoded")
                            })?,
                        ))
                        .await
                        .map_err(|error| {
                            SdkError::from_state_store(error, "prompt terminal could not be stored")
                                .in_phase(ErrorPhase::StateWrite)
                        })?;
                }
                let continuations = self
                    .cancel_continuations_in_transaction(&mut transaction, continuations)
                    .await?;
                Ok((record, continuations))
            }
            .await;
            let (expected_prompt, cancelled_continuations) = match prepared {
                Ok(continuations) => continuations,
                Err(primary) => {
                    let error = primary.with_cleanup_result(transaction.rollback().await.map_err(
                        |error| {
                            SdkError::from_state_store(
                                error,
                                "prompt terminal transaction could not close",
                            )
                            .at_phase(ErrorPhase::Cleanup)
                        },
                    ));
                    Self::retry_terminal_before(error, deadline).await?;
                    continue;
                }
            };
            match transaction.commit().await {
                Ok(()) => break (None, expected_prompt, cancelled_continuations),
                Err(error @ storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                    Self::retry_terminal_before(
                        SdkError::from_state_store(error, "prompt terminal transaction conflicted")
                            .in_phase(ErrorPhase::StateCommit),
                        deadline,
                    )
                    .await?;
                }
                Err(error) => {
                    break (
                        Some(
                            SdkError::from_state_store(
                                error,
                                "prompt terminal transaction could not commit",
                            )
                            .in_phase(ErrorPhase::StateCommit),
                        ),
                        expected_prompt,
                        cancelled_continuations,
                    );
                }
            }
        };
        // After a possibly committed transaction, this owner can only observe.
        // Neither a missing receipt nor a transient read failure authorizes writes.
        if let Some(primary) = &commit_error {
            loop {
                if Instant::now() >= deadline {
                    return Err(primary.clone().with_supplementary(
                        SdkError::timeout("prompt terminal observation deadline elapsed")
                            .at_phase(ErrorPhase::Reconciliation),
                    ));
                }
                match store.load(key.clone()).await {
                    Ok(Some(snapshot)) => {
                        let mut current =
                            OpenCodePromptRecord::decode(snapshot, &self.conversation_id)
                                .map_err(|error| primary.clone().with_supplementary(error))?;
                        if current.generation_id() == self.generation_id
                            && intent.confirms(&mut current, &expected_prompt)
                        {
                            break;
                        }
                        return Err(primary.clone());
                    }
                    Ok(None) => return Err(primary.clone()),
                    Err(error) => {
                        let observation = SdkError::from_state_store(
                            error,
                            "prompt terminal outcome could not be checked",
                        )
                        .at_phase(ErrorPhase::Reconciliation);
                        if observation.detail() != Some(&ErrorDetail::TransactionConflict)
                            && !super::OpenCodeStateStore::<S>::retryable_state_error(&observation)
                        {
                            return Err(primary.clone().with_supplementary(observation));
                        }
                        tokio::time::sleep_until(
                            deadline.min(Instant::now() + std::time::Duration::from_millis(25)),
                        )
                        .await;
                    }
                }
            }
        }
        for continuation in cancelled_continuations {
            let mut last_observation = None;
            loop {
                if Instant::now() >= deadline {
                    let mut timeout =
                        SdkError::timeout("prompt terminal observation deadline elapsed")
                            .at_phase(ErrorPhase::Reconciliation);
                    if let Some(error) = last_observation {
                        timeout = timeout.with_supplementary(error);
                    }
                    return Err(match &commit_error {
                        Some(primary) => primary.clone().with_supplementary(timeout),
                        None => timeout,
                    });
                }
                let observation = match self.finish_continuation_reconciliation(&continuation).await
                {
                    Ok(true) => break,
                    Ok(false) => SdkError::integration(
                        "prompt terminal continuation is not exactly confirmed",
                    )
                    .at_phase(ErrorPhase::Reconciliation),
                    Err(error) => error.at_phase(ErrorPhase::Reconciliation),
                };
                let transient = observation.detail() == Some(&ErrorDetail::TransactionConflict)
                    || super::OpenCodeStateStore::<S>::retryable_state_error(&observation);
                if !transient {
                    return Err(match &commit_error {
                        Some(primary) => primary.clone().with_supplementary(observation),
                        None => observation,
                    });
                }
                last_observation = Some(observation);
                tokio::time::sleep_until(
                    deadline.min(Instant::now() + std::time::Duration::from_millis(25)),
                )
                .await;
            }
        }
        self.prompts.mark_changed();
        Ok(())
    }

    async fn finish_continuation_reconciliation(
        &self,
        continuation: &OpenCodeContinuation,
    ) -> Result<bool, SdkError> {
        if !self
            .prompts
            .continuation_reconciliation_matches(continuation)
            .await?
        {
            return Ok(false);
        }
        self.prompts
            .notifications
            .finish_reconciliation(continuation.conversation_id());
        Ok(true)
    }

    pub(crate) async fn settle_delivery(
        &self,
        delivered: bool,
        deadline: Instant,
    ) -> Result<(), SdkError> {
        self.mutate(deadline, |record| record.settle_waiter_delivery(delivered))
            .await
    }

    async fn mutate<T>(
        &self,
        deadline: Instant,
        mutation: impl FnOnce(&mut OpenCodePromptRecord) -> Result<T, SdkError>,
    ) -> Result<T, SdkError> {
        if Instant::now() >= deadline {
            return Err(SdkError::timeout("prompt persistence deadline elapsed")
                .at_phase(ErrorPhase::Settlement));
        }
        let key = OpenCodePromptRecord::key(&self.conversation_id)?;
        let store = self.prompts.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            SdkError::from_state_store(error, "prompt state transition could not begin")
                .in_phase(ErrorPhase::StateWrite)
        })?;
        let prepared = async {
            let snapshot = transaction
                .load(key.clone())
                .await
                .map_err(|error| {
                    SdkError::from_state_store(error, "prompt state could not be loaded")
                        .in_phase(ErrorPhase::StateRead)
                })?
                .ok_or_else(|| SdkError::invalid_state("prompt state is missing"))?;
            let before = OpenCodePromptRecord::decode(snapshot, &self.conversation_id)?;
            if before.generation_id() != self.generation_id {
                return Err(SdkError::conflict(
                    "prompt transition belongs to another generation",
                ));
            }
            let mut record = before.clone();
            let result = mutation(&mut record)?;
            let changed = record != before;
            if changed {
                transaction
                    .put(storage_sdk::state::record::StateRecord::new(
                        key.clone(),
                        serde_json::to_value(&record).map_err(|_| {
                            SdkError::invalid_state("prompt transition could not be encoded")
                        })?,
                    ))
                    .await
                    .map_err(|error| {
                        SdkError::from_state_store(error, "prompt transition could not be stored")
                            .in_phase(ErrorPhase::StateWrite)
                    })?;
            }
            Ok((result, changed, record))
        }
        .await;
        let (result, changed, expected) = match prepared {
            Ok(prepared) => prepared,
            Err(primary) => {
                return Err(
                    primary.with_cleanup_result(transaction.rollback().await.map_err(|error| {
                        SdkError::from_state_store(
                            error,
                            "prompt state transaction could not close",
                        )
                        .at_phase(ErrorPhase::Cleanup)
                    })),
                );
            }
        };
        if !changed {
            transaction.rollback().await.map_err(|error| {
                SdkError::from_state_store(error, "unchanged prompt transaction could not close")
                    .at_phase(ErrorPhase::Cleanup)
            })?;
            return Ok(result);
        }
        // The caller retains this admitted commit future through its stage deadline.
        // Only a definite conflict permits a new read/modify/write attempt.
        match transaction.commit().await {
            Ok(()) => {}
            Err(error @ storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                return Err(SdkError::from_state_store(
                    error,
                    "prompt state transition conflicted",
                )
                .in_phase(ErrorPhase::StateCommit));
            }
            Err(error) => {
                let primary =
                    SdkError::from_state_store(error, "prompt state transition could not commit")
                        .in_phase(ErrorPhase::StateCommit);
                loop {
                    if Instant::now() >= deadline {
                        return Err(primary.with_supplementary(
                            SdkError::timeout("prompt commit observation deadline elapsed")
                                .at_phase(ErrorPhase::Reconciliation),
                        ));
                    }
                    // Ambiguous commit outcomes retry only observation, never the mutation
                    // or any native prompt/tool effect.
                    let observed = store.load(key.clone()).await;
                    match observed {
                        Ok(Some(snapshot)) => {
                            let observed =
                                OpenCodePromptRecord::decode(snapshot, &self.conversation_id)
                                    .map_err(|error| primary.clone().with_supplementary(error))?;
                            if observed == expected {
                                break;
                            }
                            return Err(primary.with_supplementary(
                                SdkError::integration(
                                    "prompt state transition was not exactly confirmed",
                                )
                                .at_phase(ErrorPhase::Reconciliation),
                            ));
                        }
                        Ok(None) => {
                            return Err(primary.with_supplementary(
                                SdkError::integration(
                                    "prompt state is missing during commit observation",
                                )
                                .at_phase(ErrorPhase::Reconciliation),
                            ));
                        }
                        Err(error) => {
                            let observation = SdkError::from_state_store(
                                error,
                                "prompt commit outcome could not be observed",
                            )
                            .at_phase(ErrorPhase::StateRead);
                            if !(observation.detail() == Some(&ErrorDetail::TransactionConflict)
                                || super::OpenCodeStateStore::<S>::retryable_state_error(
                                    &observation,
                                ))
                            {
                                return Err(primary.with_supplementary(observation));
                            }
                            if Instant::now() >= deadline {
                                return Err(primary.with_supplementary(observation));
                            }
                            tokio::time::sleep_until(
                                deadline.min(Instant::now() + std::time::Duration::from_millis(25)),
                            )
                            .await;
                        }
                    }
                }
            }
        }
        self.prompts.mark_changed();
        Ok(result)
    }

    fn progress(&self) -> Result<Option<OpenCodeAbortProgress>, SdkError> {
        self.prompts
            .abort_progress(&self.conversation_id, &self.generation_id)
    }

    fn publish_abort(&self, outcome: OpenCodeAbortOutcome) -> Result<(), SdkError> {
        self.prompts
            .publish_abort_outcome(&self.conversation_id, &self.generation_id, outcome)
    }

    fn mark_releasable(&self) {
        self.prompts
            .mark_prompt_releasable(&self.conversation_id, &self.generation_id);
    }

    async fn settle_abort(
        &self,
        outcome: OpenCodeAbortOutcome,
        continuations: Vec<OpenCodeContinuation>,
    ) -> Result<OpenCodeAbortProgress, SdkError> {
        self.publish_abort(outcome)?;
        for continuation in continuations {
            if !self
                .prompts
                .continuation_reconciliation_matches(&continuation)
                .await?
            {
                return Err(SdkError::integration(
                    "abort continuation reconciliation remains pending",
                ));
            }
            self.prompts
                .notifications
                .finish_reconciliation(continuation.conversation_id());
        }
        self.mark_releasable();
        match self.progress()? {
            Some(progress @ OpenCodeAbortProgress::Settled(current)) if current == outcome => {
                Ok(progress)
            }
            Some(_) => Err(SdkError::invalid_state(
                "abort progress did not reach its exact settled outcome",
            )),
            None => Ok(OpenCodeAbortProgress::Settled(outcome)),
        }
    }

    pub(crate) async fn cancel_prepared(&self, deadline: Instant) -> Result<bool, SdkError> {
        if self.is_live()?
            || self
                .progress()?
                .is_some_and(|progress| progress.deletion_pending())
        {
            return Ok(false);
        }
        let key = OpenCodePromptRecord::key(&self.conversation_id)?;
        let store = self.prompts.storage.store();
        let mut last_error = None;
        loop {
            if Instant::now() >= deadline {
                let timeout = SdkError::timeout("prepared cancellation deadline elapsed")
                    .at_phase(ErrorPhase::Reconciliation);
                return Err(match last_error {
                    Some(error) => timeout.with_supplementary(error),
                    None => timeout,
                });
            }
            let failure = match store.begin_transaction().await {
                Err(error) => SdkError::from_state_store(
                    error,
                    "prepared prompt cancellation could not begin",
                )
                .in_phase(ErrorPhase::StateWrite),
                Ok(mut transaction) => {
                    let staged = async {
                        let Some(snapshot) =
                            transaction.load(key.clone()).await.map_err(|error| {
                                SdkError::from_state_store(
                                    error,
                                    "prompt state could not be loaded",
                                )
                                .in_phase(ErrorPhase::StateRead)
                            })?
                        else {
                            return Ok(None);
                        };
                        let record = OpenCodePromptRecord::decode(snapshot, &self.conversation_id)?;
                        if record.generation_id() != self.generation_id {
                            return Ok(None);
                        }
                        if record.is_prepared() {
                            transaction.delete(key.clone()).await.map_err(|error| {
                                SdkError::from_state_store(
                                    error,
                                    "prepared prompt could not be removed",
                                )
                                .in_phase(ErrorPhase::StateWrite)
                            })?;
                        }
                        Ok::<_, SdkError>(Some(record))
                    }
                    .await;
                    match staged {
                        Err(primary) => primary.with_cleanup_result(
                            transaction.rollback().await.map_err(|error| {
                                SdkError::from_state_store(
                                    error,
                                    "prepared cancellation transaction could not close",
                                )
                                .at_phase(ErrorPhase::Cleanup)
                            }),
                        ),
                        Ok(record)
                            if record.as_ref().is_none_or(|record| !record.is_prepared()) =>
                        {
                            transaction.rollback().await.map_err(|error| {
                                SdkError::from_state_store(
                                    error,
                                    "unchanged prepared cancellation transaction could not close",
                                )
                                .at_phase(ErrorPhase::Cleanup)
                            })?;
                            let released = record.is_none();
                            if released {
                                self.mark_releasable();
                            }
                            return Ok(released);
                        }
                        Ok(_) => match transaction.commit().await {
                            Ok(()) => {
                                self.mark_releasable();
                                self.prompts.mark_changed();
                                return Ok(true);
                            }
                            Err(
                                error @ storage_sdk::StateStoreError::TransactionConflict { .. },
                            ) => SdkError::from_state_store(
                                error,
                                "prepared prompt cancellation conflicted",
                            )
                            .in_phase(ErrorPhase::StateCommit),
                            Err(error) => {
                                // Consuming commit may have deleted the record. This
                                // invocation never restages deletion after uncertainty.
                                return self
                                    .observe_prepared_cancellation(
                                        deadline,
                                        SdkError::from_state_store(
                                            error,
                                            "prepared prompt cancellation could not commit",
                                        )
                                        .in_phase(ErrorPhase::StateCommit),
                                    )
                                    .await;
                            }
                        },
                    }
                }
            };
            if failure.context().phase == ErrorPhase::Cleanup
                || failure
                    .supplementary()
                    .iter()
                    .any(|error| error.context().phase == ErrorPhase::Cleanup)
                || !((failure.kind() == ErrorKind::Conflict
                    && failure.detail() == Some(&ErrorDetail::TransactionConflict))
                    || super::OpenCodeStateStore::<S>::retryable_state_error(&failure))
            {
                return Err(failure);
            }
            last_error = Some(failure);
            tokio::time::sleep_until(
                deadline.min(Instant::now() + std::time::Duration::from_millis(25)),
            )
            .await;
        }
    }

    async fn observe_prepared_cancellation(
        &self,
        deadline: Instant,
        primary: SdkError,
    ) -> Result<bool, SdkError> {
        let key = OpenCodePromptRecord::key(&self.conversation_id)?;
        let store = self.prompts.storage.store();
        let mut last_error = None;
        loop {
            if Instant::now() >= deadline {
                let mut timeout =
                    SdkError::timeout("prepared cancellation observation deadline elapsed")
                        .at_phase(ErrorPhase::Reconciliation);
                if let Some(error) = last_error {
                    timeout = timeout.with_supplementary(error);
                }
                return Err(primary.with_supplementary(timeout));
            }
            match store.load(key.clone()).await {
                Ok(None) => {
                    self.mark_releasable();
                    self.prompts.mark_changed();
                    return Ok(true);
                }
                Ok(Some(snapshot)) => {
                    OpenCodePromptRecord::decode(snapshot, &self.conversation_id)
                        .map_err(|error| primary.clone().with_supplementary(error))?;
                    // A foreign record is an initial-read no-op, but it cannot
                    // confirm this owner's potentially committed deletion.
                    return Err(primary.with_supplementary(
                        SdkError::integration("prepared prompt deletion was not confirmed")
                            .at_phase(ErrorPhase::Reconciliation),
                    ));
                }
                Err(error) => {
                    let observation = SdkError::from_state_store(
                        error,
                        "prepared cancellation could not be observed",
                    )
                    .at_phase(ErrorPhase::StateRead);
                    if observation.detail() != Some(&ErrorDetail::TransactionConflict)
                        && !super::OpenCodeStateStore::<S>::retryable_state_error(&observation)
                    {
                        return Err(primary.with_supplementary(observation));
                    }
                    last_error = Some(observation);
                    tokio::time::sleep_until(
                        deadline.min(Instant::now() + std::time::Duration::from_millis(25)),
                    )
                    .await;
                }
            }
        }
    }

    pub(crate) async fn persist_abort(
        &self,
        outcome: OpenCodeCancellation,
        deadline: Instant,
    ) -> Result<(), SdkError> {
        self.mutate(deadline, |record| record.settle_cancellation(outcome))
            .await
    }

    pub(crate) async fn claim_abort(
        &self,
        owner: &OpenCodeCancellationOwner,
        deadline: Instant,
    ) -> Result<bool, SdkError> {
        self.claim_abort_owner(owner, OpenCodeAbortClaim::LiveOwner, deadline)
            .await
    }

    pub(crate) async fn release_abort_claim(
        &self,
        owner: &OpenCodeCancellationOwner,
    ) -> Result<bool, SdkError> {
        let key = OpenCodePromptRecord::key(&self.conversation_id)?;
        let store = self.prompts.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            SdkError::from_state_store(error, "cancellation release could not begin")
                .in_phase(ErrorPhase::StateWrite)
        })?;
        let snapshot = transaction
            .load(key.clone())
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "prompt state could not be loaded")
                    .in_phase(ErrorPhase::StateRead)
            })?
            .ok_or_else(|| SdkError::invalid_state("prompt state is missing"))?;
        let mut record = OpenCodePromptRecord::decode(snapshot, &self.conversation_id)?;
        if record.generation_id() != self.generation_id {
            return Err(SdkError::conflict(
                "cancellation release changed prompt generation",
            ));
        }
        if record.cancellation_is_requested() {
            return Ok(true);
        }
        if !record.release_cancellation_claim(owner)? {
            return Ok(false);
        }
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                key.clone(),
                serde_json::to_value(record).map_err(|_| {
                    SdkError::invalid_state("cancellation release could not be encoded")
                })?,
            ))
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "cancellation release could not be stored")
                    .in_phase(ErrorPhase::StateWrite)
            })?;
        let commit = transaction.commit().await;
        if commit.is_err() {
            let released = store
                .load(key)
                .await
                .map_err(|error| {
                    SdkError::from_state_store(error, "cancellation release could not be checked")
                        .in_phase(ErrorPhase::StateRead)
                })?
                .map(|snapshot| OpenCodePromptRecord::decode(snapshot, &self.conversation_id))
                .transpose()?
                .is_some_and(|current| {
                    current.generation_id() == self.generation_id
                        && current.cancellation_is_requested()
                });
            if !released {
                return Err(SdkError::integration(
                    "cancellation release could not commit",
                ));
            }
        }
        self.prompts.mark_changed();
        Ok(true)
    }

    pub(crate) async fn begin_abort_dispatch(
        &self,
        owner: &OpenCodeCancellationOwner,
        deadline: Instant,
    ) -> Result<bool, SdkError> {
        self.mutate(deadline, |record| {
            if record.cancellation_dispatch_is_owned_by(owner) {
                return Ok(true);
            }
            record.begin_cancellation_dispatch(owner)
        })
        .await
    }

    pub(crate) async fn abort_recovery_pending(&self) -> Result<bool, SdkError> {
        if self.is_live()? {
            return Ok(false);
        }
        let store = self.prompts.storage.store();
        let Some(snapshot) = store
            .load(OpenCodePromptRecord::key(&self.conversation_id)?)
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "prompt state could not be loaded")
                    .in_phase(ErrorPhase::StateRead)
            })?
        else {
            return Ok(false);
        };
        let record = OpenCodePromptRecord::decode(snapshot, &self.conversation_id)?;
        Ok(record.generation_id() == self.generation_id && record.cancellation_dispatch_pending())
    }

    pub(crate) async fn claim_recovered_abort(
        &self,
        owner: &OpenCodeCancellationOwner,
        deadline: Instant,
    ) -> Result<bool, SdkError> {
        if self.is_live()? {
            return Ok(false);
        }
        self.claim_abort_owner(owner, OpenCodeAbortClaim::ObservedAdmission, deadline)
            .await
    }

    async fn claim_abort_owner(
        &self,
        owner: &OpenCodeCancellationOwner,
        claim: OpenCodeAbortClaim,
        deadline: Instant,
    ) -> Result<bool, SdkError> {
        self.mutate(deadline, |record| {
            if record.cancellation_is_reserved_by(owner) {
                return Ok(true);
            }
            match claim {
                OpenCodeAbortClaim::LiveOwner => record.reserve_cancellation(owner),
                OpenCodeAbortClaim::ObservedAdmission => {
                    record.reserve_recovered_cancellation(owner)
                }
            }
        })
        .await
    }

    pub(crate) async fn reconcile_live_idle_response_loss(&self) -> Result<(), SdkError> {
        if !self.is_live()? {
            return Err(SdkError::invalid_state(
                "idle response-loss repair lacks its live prompt owner",
            ));
        }

        let key = OpenCodePromptRecord::key(&self.conversation_id)?;
        let store = self.prompts.storage.store();
        let continuations = self.prompts.continuations(&self.conversation_id).await?;
        if continuations
            .iter()
            .any(OpenCodeContinuation::blocks_prompt_release)
        {
            return Err(SdkError::busy(
                "idle response-loss interactions remain unresolved",
            ));
        }
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            SdkError::from_state_store(error, "idle response-loss repair could not begin")
                .in_phase(ErrorPhase::StateWrite)
        })?;
        let Some(snapshot) = transaction.load(key.clone()).await.map_err(|error| {
            SdkError::from_state_store(error, "prompt state could not be loaded")
                .in_phase(ErrorPhase::StateRead)
        })?
        else {
            drop(transaction);
            let outcome = self
                .progress()?
                .and_then(OpenCodeAbortProgress::outcome)
                .ok_or_else(|| {
                    SdkError::invalid_state(
                        "idle response-loss repair lacks its exact completion outcome",
                    )
                })?;
            let _ = self.settle_abort(outcome, continuations).await?;
            return Ok(());
        };
        let mut record = OpenCodePromptRecord::decode(snapshot, &self.conversation_id)?;
        if record.generation_id() != self.generation_id {
            return Err(SdkError::conflict(
                "idle response-loss repair changed prompt generation",
            ));
        }
        if !record.has_lost_response_after_admission() {
            return Err(SdkError::invalid_state(
                "idle response-loss repair contradicts durable prompt state",
            ));
        }
        let cancellation_accepted = record.cancellation_accepted();
        let delivered_ambiguous_response_loss = record.delivered_ambiguous_response_loss();
        let _ = record.reconcile_quiescent_cancellation()?;
        if !record.clearable() {
            return Err(SdkError::invalid_state(
                "idle response-loss repair retained a live waiter",
            ));
        }
        let reconciled = self
            .cancel_continuations_in_transaction(&mut transaction, continuations)
            .await?;
        transaction.delete(key.clone()).await.map_err(|error| {
            SdkError::from_state_store(error, "idle response-loss repair could not be stored")
                .in_phase(ErrorPhase::StateWrite)
        })?;
        let commit = transaction.commit().await;
        if commit.is_err()
            && store
                .load(key)
                .await
                .map_err(|error| {
                    SdkError::from_state_store(
                        error,
                        "idle response-loss repair outcome could not be checked",
                    )
                    .in_phase(ErrorPhase::StateRead)
                })?
                .is_some()
        {
            return Err(SdkError::integration(
                "idle response-loss repair could not commit",
            ));
        }
        let outcome = if cancellation_accepted || delivered_ambiguous_response_loss {
            OpenCodeAbortOutcome::Complete
        } else {
            OpenCodeAbortOutcome::RepairedInvalidState
        };
        let _ = self.settle_abort(outcome, reconciled).await?;
        Ok(())
    }

    pub(crate) async fn abort_complete(&self) -> Result<Option<OpenCodeAbortProgress>, SdkError> {
        let registered = {
            let registrations = self.prompts.registrations.lock().map_err(|_| {
                SdkError::integration("prompt generation registry could not be locked")
                    .in_phase(ErrorPhase::Admission)
            })?;
            registrations
                .get(&self.conversation_id)
                .filter(|reservation| reservation.generation_id == self.generation_id)
                .map(|reservation| {
                    (
                        reservation.is_live(),
                        reservation
                            .abort
                            .settled()
                            .map(OpenCodeAbortProgress::Settled),
                    )
                })
        };
        if registered.as_ref().is_some_and(|(live, _)| *live) {
            return Ok(None);
        }
        if let Some((_, Some(outcome))) = registered {
            return Ok(Some(outcome));
        }
        let deletion = self.progress()?;
        let observed = async {
            let store = self.prompts.storage.store();
            let continuations = self.prompts.continuations(&self.conversation_id).await?;
            let record = store
                .load(OpenCodePromptRecord::key(&self.conversation_id)?)
                .await
                .map_err(|error| {
                    SdkError::from_state_store(error, "prompt state could not be loaded")
                        .in_phase(ErrorPhase::StateRead)
                })?
                .map(|snapshot| OpenCodePromptRecord::decode(snapshot, &self.conversation_id))
                .transpose()?;
            Ok::<_, SdkError>((continuations, record))
        }
        .await;
        let (continuations, record) = observed.map_err(|error| {
            if let Some(OpenCodeAbortProgress::Deletion {
                failure: Some(primary),
                ..
            }) = &deletion
            {
                primary.clone().with_supplementary(error)
            } else {
                error
            }
        })?;
        if record
            .as_ref()
            .is_some_and(|record| record.generation_id() != self.generation_id)
            && self
                .progress()?
                .is_some_and(|progress| progress.deletion_pending())
        {
            return Err(self.deletion_failure()?.with_supplementary(
                SdkError::conflict("prompt deletion observation changed generation")
                    .at_phase(ErrorPhase::Reconciliation),
            ));
        }
        let record = record.filter(|record| record.generation_id() == self.generation_id);
        if record.is_none() {
            let progress = self.progress()?;
            let outcome = match progress {
                Some(OpenCodeAbortProgress::Deletion { outcome, .. }) => outcome,
                other => other
                    .and_then(OpenCodeAbortProgress::outcome)
                    .unwrap_or(OpenCodeAbortOutcome::Complete),
            };
            return self.settle_abort(outcome, continuations).await.map(Some);
        }
        if let Some(OpenCodeAbortProgress::Deletion {
            failure: Some(error),
            ..
        }) = self.progress()?
        {
            return Err(error);
        }
        Ok(None)
    }

    pub(crate) async fn abort_recovery_ready(&self) -> Result<bool, SdkError> {
        if self.is_live()? {
            return Ok(false);
        }
        let store = self.prompts.storage.store();
        let Some(snapshot) = store
            .load(OpenCodePromptRecord::key(&self.conversation_id)?)
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "prompt state could not be loaded")
                    .in_phase(ErrorPhase::StateRead)
            })?
        else {
            return Ok(false);
        };
        let record = OpenCodePromptRecord::decode(snapshot, &self.conversation_id)?;
        if record.generation_id() != self.generation_id {
            return Ok(false);
        }
        Ok(record.idle_cancellation_repairable())
    }

    pub(super) fn is_live(&self) -> Result<bool, SdkError> {
        let registrations = self.prompts.registrations.lock().map_err(|_| {
            SdkError::integration("prompt generation registry could not be locked")
                .in_phase(ErrorPhase::Admission)
        })?;
        Ok(registrations
            .get(&self.conversation_id)
            .is_some_and(|reservation| {
                reservation.generation_id == self.generation_id && reservation.is_live()
            }))
    }

    fn is_registered(&self) -> Result<bool, SdkError> {
        let registrations = self.prompts.registrations.lock().map_err(|_| {
            SdkError::integration("prompt generation registry could not be locked")
                .in_phase(ErrorPhase::Admission)
        })?;
        Ok(registrations
            .get(&self.conversation_id)
            .is_some_and(|reservation| reservation.generation_id == self.generation_id))
    }

    pub(crate) async fn reconcile_abort(
        &self,
        native_state: Option<OpenCodeNativePromptState>,
        deadline: Instant,
    ) -> Result<OpenCodeAbortProgress, SdkError> {
        match self.progress()? {
            Some(progress @ OpenCodeAbortProgress::Settled(_)) => return Ok(progress),
            Some(OpenCodeAbortProgress::Deletion { outcome, .. }) => {
                return self
                    .observe_abort_deletion(outcome, self.deletion_failure()?, deadline)
                    .await;
            }
            _ => {}
        }
        if self.is_live()? {
            return Ok(OpenCodeAbortProgress::Pending);
        }
        if Instant::now() >= deadline {
            return Err(
                SdkError::timeout("prompt cancellation reconciliation deadline elapsed")
                    .at_phase(ErrorPhase::Reconciliation),
            );
        }
        let key = OpenCodePromptRecord::key(&self.conversation_id)?;
        let store = self.prompts.storage.store();
        let continuations = self.prompts.continuations(&self.conversation_id).await?;
        if Instant::now() >= deadline {
            return Err(
                SdkError::timeout("prompt cancellation reconciliation deadline elapsed")
                    .at_phase(ErrorPhase::Reconciliation),
            );
        }
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            SdkError::from_state_store(error, "prompt cancellation reconciliation could not begin")
                .at_phase(ErrorPhase::StateWrite)
        })?;
        let loaded = async {
            transaction
                .load(key.clone())
                .await
                .map_err(|error| {
                    SdkError::from_state_store(error, "prompt state could not be loaded")
                        .at_phase(ErrorPhase::StateRead)
                })?
                .map(|snapshot| OpenCodePromptRecord::decode(snapshot, &self.conversation_id))
                .transpose()
        }
        .await;
        let mut record = match loaded {
            Ok(Some(record)) if record.generation_id() == self.generation_id => record,
            Ok(_) => {
                transaction.rollback().await.map_err(|error| {
                    SdkError::from_state_store(
                        error,
                        "unchanged cancellation reconciliation could not close",
                    )
                    .at_phase(ErrorPhase::Cleanup)
                })?;
                let outcome = self
                    .progress()?
                    .and_then(OpenCodeAbortProgress::outcome)
                    .unwrap_or(OpenCodeAbortOutcome::Complete);
                return self.settle_abort(outcome, continuations).await;
            }
            Err(primary) => {
                return Err(
                    primary.with_cleanup_result(transaction.rollback().await.map_err(|error| {
                        SdkError::from_state_store(
                            error,
                            "cancellation reconciliation rollback failed",
                        )
                        .at_phase(ErrorPhase::Cleanup)
                    })),
                );
            }
        };
        let staged = async {
            let waiter_changed = record.mark_waiter_lost()?;
            if record.is_terminal() {
                let cancellation_changed =
                    if !record.cancellation_releasable() && record.idle_cancellation_repairable() {
                        record.reconcile_quiescent_cancellation()?
                    } else {
                        false
                    };
                if !record.clearable()
                    || continuations
                        .iter()
                        .any(OpenCodeContinuation::blocks_prompt_release)
                {
                    return Ok((None, waiter_changed || cancellation_changed, continuations));
                }
                Ok((Some(OpenCodeAbortOutcome::Complete), false, continuations))
            } else if (!record.cancellation_accepted()
                && !native_state.is_some_and(OpenCodeNativePromptState::is_idle))
                || !record.idle_cancellation_repairable()
            {
                Ok((None, waiter_changed, continuations))
            } else {
                let outcome = if record.cancellation_accepted()
                    || record.delivered_ambiguous_response_loss()
                {
                    OpenCodeAbortOutcome::Complete
                } else {
                    OpenCodeAbortOutcome::RepairedInvalidState
                };
                record.reconcile_quiescent_cancellation()?;
                let reconciled = self
                    .cancel_continuations_in_transaction(&mut transaction, continuations)
                    .await?;
                Ok::<_, SdkError>((Some(outcome), false, reconciled))
            }
        }
        .await;
        let (outcome, changed, continuations) = match staged {
            Ok(staged) => staged,
            Err(primary) => {
                return Err(
                    primary.with_cleanup_result(transaction.rollback().await.map_err(|error| {
                        SdkError::from_state_store(
                            error,
                            "cancellation reconciliation rollback failed",
                        )
                        .at_phase(ErrorPhase::Cleanup)
                    })),
                );
            }
        };
        let Some(outcome) = outcome else {
            if changed {
                self.commit_recovered_prompt(transaction, &record, deadline)
                    .await?;
            } else {
                transaction.rollback().await.map_err(|error| {
                    SdkError::from_state_store(
                        error,
                        "unchanged cancellation reconciliation could not close",
                    )
                    .at_phase(ErrorPhase::Cleanup)
                })?;
            }
            return Ok(OpenCodeAbortProgress::Pending);
        };
        let prepared = async {
            transaction.delete(key).await.map_err(|error| {
                SdkError::from_state_store(error, "reconciled prompt could not be cleared")
                    .at_phase(ErrorPhase::StateWrite)
            })?;
            // Retain the intended outcome before invoking the consuming commit.
            // This does not claim access to the backend's internal admission.
            self.retain_abort_deletion(outcome, None)
        }
        .await;
        if let Err(primary) = prepared {
            return Err(
                primary.with_cleanup_result(transaction.rollback().await.map_err(|error| {
                    SdkError::from_state_store(error, "cancellation reconciliation rollback failed")
                        .at_phase(ErrorPhase::Cleanup)
                })),
            );
        }
        match transaction.commit().await {
            Ok(()) => self.settle_abort(outcome, continuations).await,
            Err(error @ storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                let primary =
                    SdkError::from_state_store(error, "prompt recovery transaction conflicted")
                        .at_phase(ErrorPhase::StateCommit);
                self.reject_abort_deletion(outcome)
                    .map_err(|error| primary.clone().with_supplementary(error))?;
                Err(primary)
            }
            Err(error) => {
                let primary =
                    SdkError::from_state_store(error, "reconciled prompt cleanup could not commit")
                        .at_phase(ErrorPhase::StateCommit);
                self.retain_abort_deletion(outcome, Some(primary.clone()))
                    .map_err(|error| primary.clone().with_supplementary(error))?;
                self.observe_abort_deletion(outcome, primary, deadline)
                    .await
            }
        }
    }

    fn retain_abort_deletion(
        &self,
        outcome: OpenCodeAbortOutcome,
        failure: Option<SdkError>,
    ) -> Result<(), SdkError> {
        let mut registrations = self.prompts.registrations.lock().map_err(|_| {
            SdkError::integration("prompt generation registry could not be locked")
                .at_phase(ErrorPhase::Reconciliation)
        })?;
        let Some(reservation) = registrations.get_mut(&self.conversation_id) else {
            // Standalone reopen retains its single attempt and exact receipt on
            // the construction stack; it has no participating Abort observers.
            return Ok(());
        };
        if reservation.generation_id != self.generation_id {
            return Err(SdkError::conflict(
                "prompt deletion lost its bound generation",
            ));
        }
        match &mut reservation.abort {
            OpenCodeAbortProgress::Pending => {
                reservation.abort = OpenCodeAbortProgress::Deletion { outcome, failure };
            }
            OpenCodeAbortProgress::Deletion {
                outcome: current,
                failure: original,
            } if *current == outcome => {
                if original.is_none() {
                    *original = failure;
                }
            }
            OpenCodeAbortProgress::Settling(current) | OpenCodeAbortProgress::Settled(current)
                if *current == outcome => {}
            _ => {
                return Err(SdkError::conflict(
                    "prompt deletion contradicts its retained outcome",
                ));
            }
        }
        drop(registrations);
        self.prompts.mark_changed();
        Ok(())
    }

    fn reject_abort_deletion(&self, outcome: OpenCodeAbortOutcome) -> Result<(), SdkError> {
        let mut registrations = self.prompts.registrations.lock().map_err(|_| {
            SdkError::integration("prompt generation registry could not be locked")
                .at_phase(ErrorPhase::Reconciliation)
        })?;
        if let Some(reservation) = registrations.get_mut(&self.conversation_id)
            && reservation.generation_id == self.generation_id
            && matches!(reservation.abort, OpenCodeAbortProgress::Deletion { outcome: current, failure: None } if current == outcome)
        {
            reservation.abort = OpenCodeAbortProgress::Pending;
        }
        Ok(())
    }

    fn deletion_failure(&self) -> Result<SdkError, SdkError> {
        match self.progress()? {
            Some(OpenCodeAbortProgress::Deletion {
                failure: Some(error),
                ..
            }) => Ok(error),
            _ => Ok(
                SdkError::conflict("prompt deletion outcome remains unconfirmed")
                    .at_phase(ErrorPhase::Reconciliation),
            ),
        }
    }

    async fn observe_abort_deletion(
        &self,
        outcome: OpenCodeAbortOutcome,
        primary: SdkError,
        deadline: Instant,
    ) -> Result<OpenCodeAbortProgress, SdkError> {
        if let Some(progress @ OpenCodeAbortProgress::Settled(_)) = self.progress()? {
            return Ok(progress);
        }
        let key = OpenCodePromptRecord::key(&self.conversation_id)?;
        let mut last_error = None;
        loop {
            if Instant::now() >= deadline {
                let mut timeout = SdkError::timeout("prompt deletion observation deadline elapsed")
                    .at_phase(ErrorPhase::Reconciliation);
                if let Some(error) = last_error {
                    timeout = timeout.with_supplementary(error);
                }
                return Err(primary.with_supplementary(timeout));
            }
            let observed =
                tokio::time::timeout_at(deadline, self.prompts.storage.store().load(key.clone()))
                    .await;
            match observed {
                Ok(Ok(None)) => {
                    if Instant::now() >= deadline {
                        return Err(primary.with_supplementary(
                            SdkError::timeout("prompt deletion observation deadline elapsed")
                                .at_phase(ErrorPhase::Reconciliation),
                        ));
                    }
                    // These are receipt checks only. No admitted mutation is
                    // cancelled when the original observation budget expires.
                    let settlement = tokio::time::timeout_at(deadline, async {
                        let continuations =
                            self.prompts.continuations(&self.conversation_id).await?;
                        self.settle_abort(outcome, continuations).await
                    })
                    .await;
                    return match settlement {
                        Ok(result) => result.map_err(|error| primary.with_supplementary(error)),
                        Err(_) => Err(primary.with_supplementary(
                            SdkError::timeout("prompt deletion observation deadline elapsed")
                                .at_phase(ErrorPhase::Reconciliation),
                        )),
                    };
                }
                Ok(Ok(Some(snapshot))) => {
                    OpenCodePromptRecord::decode(snapshot, &self.conversation_id)
                        .map_err(|error| primary.clone().with_supplementary(error))?;
                    return Err(primary.with_supplementary(
                        SdkError::integration("prompt deletion was not exactly confirmed")
                            .at_phase(ErrorPhase::Reconciliation),
                    ));
                }
                Ok(Err(error)) => {
                    let error =
                        SdkError::from_state_store(error, "prompt deletion could not be observed")
                            .at_phase(ErrorPhase::StateRead);
                    if error.detail() != Some(&ErrorDetail::TransactionConflict)
                        && !super::OpenCodeStateStore::<S>::retryable_state_error(&error)
                    {
                        return Err(primary.with_supplementary(error));
                    }
                    last_error = Some(error);
                }
                Err(_) => {
                    return Err(primary.with_supplementary(
                        SdkError::timeout("prompt deletion observation deadline elapsed")
                            .at_phase(ErrorPhase::Reconciliation),
                    ));
                }
            }
            tokio::time::sleep_until(
                deadline.min(Instant::now() + std::time::Duration::from_millis(25)),
            )
            .await;
        }
    }

    pub(super) async fn commit_recovered_prompt(
        &self,
        mut transaction: S::Transaction,
        record: &OpenCodePromptRecord,
        deadline: Instant,
    ) -> Result<(), SdkError> {
        let prepared = async {
            if record.conversation_id() != &self.conversation_id
                || record.generation_id() != self.generation_id
            {
                return Err(SdkError::invalid_state(
                    "recovered prompt contradicts its bound generation",
                ));
            }
            let key = OpenCodePromptRecord::key(&self.conversation_id)?;
            let value = serde_json::to_value(record)
                .map_err(|_| SdkError::invalid_state("recovered prompt could not be encoded"))?;
            transaction
                .put(storage_sdk::state::record::StateRecord::new(
                    key.clone(),
                    value,
                ))
                .await
                .map_err(|error| {
                    SdkError::from_state_store(error, "recovered prompt could not be stored")
                        .at_phase(ErrorPhase::StateWrite)
                })?;
            Ok::<_, SdkError>(key)
        }
        .await;
        let key = match prepared {
            Ok(key) => key,
            Err(primary) => {
                return Err(
                    primary.with_cleanup_result(transaction.rollback().await.map_err(|error| {
                        SdkError::from_state_store(error, "recovered prompt rollback failed")
                            .at_phase(ErrorPhase::Cleanup)
                    })),
                );
            }
        };
        match transaction.commit().await {
            Ok(()) => {}
            Err(error @ storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                return Err(SdkError::from_state_store(
                    error,
                    "prompt recovery transaction conflicted",
                )
                .at_phase(ErrorPhase::StateCommit));
            }
            Err(error) => {
                let primary =
                    SdkError::from_state_store(error, "recovered prompt could not commit")
                        .at_phase(ErrorPhase::StateCommit);
                let mut last_error = None;
                loop {
                    if Instant::now() >= deadline {
                        let mut timeout =
                            SdkError::timeout("recovered prompt observation deadline elapsed")
                                .at_phase(ErrorPhase::Reconciliation);
                        if let Some(error) = last_error {
                            timeout = timeout.with_supplementary(error);
                        }
                        return Err(primary.with_supplementary(timeout));
                    }
                    match tokio::time::timeout_at(
                        deadline,
                        self.prompts.storage.store().load(key.clone()),
                    )
                    .await
                    {
                        Ok(Ok(Some(snapshot))) => {
                            let observed =
                                OpenCodePromptRecord::decode(snapshot, &self.conversation_id)
                                    .map_err(|error| primary.clone().with_supplementary(error))?;
                            if observed == *record {
                                break;
                            }
                            return Err(primary.with_supplementary(
                                SdkError::integration(
                                    "recovered prompt update was not exactly confirmed",
                                )
                                .at_phase(ErrorPhase::Reconciliation),
                            ));
                        }
                        Ok(Ok(None)) => {
                            return Err(primary.with_supplementary(
                                SdkError::integration("recovered prompt update is missing")
                                    .at_phase(ErrorPhase::Reconciliation),
                            ));
                        }
                        Ok(Err(error)) => {
                            let error = SdkError::from_state_store(
                                error,
                                "recovered prompt could not be observed",
                            )
                            .at_phase(ErrorPhase::StateRead);
                            if error.detail() != Some(&ErrorDetail::TransactionConflict)
                                && !super::OpenCodeStateStore::<S>::retryable_state_error(&error)
                            {
                                return Err(primary.with_supplementary(error));
                            }
                            last_error = Some(error);
                        }
                        Err(_) => {
                            return Err(primary.with_supplementary(
                                SdkError::timeout("recovered prompt observation deadline elapsed")
                                    .at_phase(ErrorPhase::Reconciliation),
                            ));
                        }
                    }
                    tokio::time::sleep_until(
                        deadline.min(Instant::now() + std::time::Duration::from_millis(25)),
                    )
                    .await;
                }
            }
        }
        self.prompts.mark_changed();
        Ok(())
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum OpenCodePromptStatusSignal {
    Observed(OpenCodePromptStatus),
    Refresh,
}

enum OpenCodeAbortClaim {
    LiveOwner,
    ObservedAdmission,
}

impl<S: StateStoreBackend> OpenCodePromptRegistration<S> {
    pub(crate) fn generation(&self) -> &OpenCodePromptGeneration<S> {
        &self.generation
    }

    pub(crate) fn is_active(&self) -> Result<bool, SdkError> {
        let registrations = self.generation.prompts.registrations.lock().map_err(|_| {
            SdkError::integration("prompt generation registry could not be locked")
                .in_phase(ErrorPhase::Admission)
        })?;
        Ok(registrations
            .get(&self.generation.conversation_id)
            .is_some_and(|reservation| {
                reservation.generation_id == self.generation.generation_id && reservation.is_live()
            }))
    }

    pub(crate) fn cancellation_requested(&self) -> Result<bool, SdkError> {
        let registrations = self.generation.prompts.registrations.lock().map_err(|_| {
            SdkError::integration("prompt generation registry could not be locked")
                .in_phase(ErrorPhase::Admission)
        })?;
        let reservation = registrations
            .get(&self.generation.conversation_id)
            .ok_or_else(|| SdkError::invalid_state("prompt generation registration is missing"))?;
        if reservation.generation_id != self.generation.generation_id {
            return Err(SdkError::conflict(
                "prompt generation registration changed ownership",
            ));
        }
        if !reservation.is_live() {
            return Err(SdkError::invalid_state(
                "prompt generation registration is detached",
            ));
        }
        reservation.live_cancellation_requested()
    }

    pub(crate) fn begin_dispatch(&self) -> Result<bool, SdkError> {
        let registrations = self.generation.prompts.registrations.lock().map_err(|_| {
            SdkError::integration("prompt generation registry could not be locked")
                .in_phase(ErrorPhase::Admission)
        })?;
        let reservation = registrations
            .get(&self.generation.conversation_id)
            .ok_or_else(|| SdkError::invalid_state("prompt generation registration is missing"))?;
        if reservation.generation_id != self.generation.generation_id {
            return Err(SdkError::conflict(
                "prompt generation registration changed ownership",
            ));
        }
        if !reservation.is_live() {
            return Err(SdkError::invalid_state(
                "prompt generation registration is detached",
            ));
        }
        reservation
            .live_cancellation_requested()
            .map(|requested| !requested)
    }

    pub(crate) fn publish_unstarted<T>(
        mut self,
        result: Result<T, SdkError>,
    ) -> Result<T, SdkError> {
        match self.take(OpenCodePromptRelease::Unstarted) {
            Ok(ownership) => ownership.publish(result),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn publish_terminal<T>(
        mut self,
        result: Result<T, SdkError>,
    ) -> Result<T, SdkError> {
        match self.take(OpenCodePromptRelease::Terminal) {
            Ok(ownership) => ownership.publish(result),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn release_terminal(mut self) {
        if let Ok(ownership) = self.take(OpenCodePromptRelease::Terminal) {
            ownership.release();
        }
    }

    fn take(
        &mut self,
        release: OpenCodePromptRelease,
    ) -> Result<OpenCodePromptLocalOwner, SdkError> {
        let owner = {
            let mut registrations = match self.generation.prompts.registrations.lock() {
                Ok(registrations) => registrations,
                Err(poisoned) => poisoned.into_inner(),
            };
            let reservation = registrations
                .get_mut(&self.generation.conversation_id)
                .ok_or_else(|| SdkError::closed("prompt generation runtime was invalidated"))?;
            if reservation.generation_id != self.generation.generation_id {
                return Err(SdkError::conflict(
                    "prompt generation runtime changed ownership",
                ));
            }
            let releasable = match release {
                OpenCodePromptRelease::Unstarted => !reservation.durability.is_releasable(),
                OpenCodePromptRelease::Terminal => reservation.durability.is_releasable(),
            };
            if !reservation.is_live() || !releasable || reservation.abort.deletion_pending() {
                return Err(SdkError::invalid_state(
                    "prompt generation runtime is not publishable",
                ));
            }
            reservation.durability = OpenCodePromptDurability::Releasable;
            reservation.abort.settle();
            if reservation.abort_joiners == 0 {
                registrations
                    .remove(&self.generation.conversation_id)
                    .ok_or_else(|| SdkError::invalid_state("prompt generation runtime is missing"))?
                    .into_live_owner()?
            } else {
                reservation.release_live_for_recovery()?
            }
        };
        self.generation.prompts.mark_changed();
        Ok(owner)
    }
}

impl OpenCodePromptReservation {
    fn is_live(&self) -> bool {
        matches!(&self.runtime, OpenCodePromptRuntimeState::Live { .. })
    }

    fn is_detached(&self) -> bool {
        matches!(&self.runtime, OpenCodePromptRuntimeState::Detached { .. })
    }

    fn releases_with_durable_state(&self) -> bool {
        !self.abort.deletion_pending()
            && matches!(
                &self.runtime,
                OpenCodePromptRuntimeState::Detached { .. }
                    | OpenCodePromptRuntimeState::Recovery { .. }
            )
    }

    fn live_cancellation_requested(&self) -> Result<bool, SdkError> {
        match &self.runtime {
            OpenCodePromptRuntimeState::Live {
                cancellation: OpenCodeLiveCancellation::Available,
                ..
            } => Ok(false),
            OpenCodePromptRuntimeState::Live {
                cancellation: OpenCodeLiveCancellation::Requested,
                ..
            } => Ok(true),
            OpenCodePromptRuntimeState::Detached { .. }
            | OpenCodePromptRuntimeState::Recovery { .. } => Err(SdkError::invalid_state(
                "prompt generation registration is detached",
            )),
        }
    }

    fn request_cancellation(&mut self) {
        match &mut self.runtime {
            OpenCodePromptRuntimeState::Live { cancellation, .. } => {
                *cancellation = OpenCodeLiveCancellation::Requested;
            }
            OpenCodePromptRuntimeState::Detached { cancellation, .. } => {
                if matches!(cancellation, OpenCodeDetachedCancellation::Available) {
                    *cancellation = OpenCodeDetachedCancellation::Requested;
                }
            }
            OpenCodePromptRuntimeState::Recovery { .. } => {}
        }
    }

    fn claim_recovery(&mut self, owner: &OpenCodeCancellationOwner) -> bool {
        match &mut self.runtime {
            OpenCodePromptRuntimeState::Detached {
                cancellation: cancellation @ OpenCodeDetachedCancellation::Requested,
                ..
            } => {
                *cancellation = OpenCodeDetachedCancellation::Owned(owner.clone());
                true
            }
            OpenCodePromptRuntimeState::Recovery {
                cancellation: cancellation @ OpenCodeRecoveryCancellation::Requested,
            } => {
                *cancellation = OpenCodeRecoveryCancellation::Owned(owner.clone());
                true
            }
            OpenCodePromptRuntimeState::Live { .. }
            | OpenCodePromptRuntimeState::Detached { .. }
            | OpenCodePromptRuntimeState::Recovery { .. } => false,
        }
    }

    fn release_recovery(&mut self, owner: &OpenCodeCancellationOwner) -> bool {
        let owns_detached = matches!(
            &self.runtime,
            OpenCodePromptRuntimeState::Detached {
                cancellation: OpenCodeDetachedCancellation::Owned(current),
                ..
            } if current == owner
        );
        if owns_detached {
            let runtime = std::mem::replace(
                &mut self.runtime,
                OpenCodePromptRuntimeState::Recovery {
                    cancellation: OpenCodeRecoveryCancellation::Requested,
                },
            );
            return match runtime {
                OpenCodePromptRuntimeState::Detached { owner, .. } => {
                    self.runtime = OpenCodePromptRuntimeState::Detached {
                        owner,
                        cancellation: OpenCodeDetachedCancellation::Requested,
                    };
                    true
                }
                runtime => {
                    self.runtime = runtime;
                    false
                }
            };
        }
        let owns_recovery = matches!(
            &self.runtime,
            OpenCodePromptRuntimeState::Recovery {
                cancellation: OpenCodeRecoveryCancellation::Owned(current),
            } if current == owner
        );
        if owns_recovery {
            self.runtime = OpenCodePromptRuntimeState::Recovery {
                cancellation: OpenCodeRecoveryCancellation::Requested,
            };
        }
        owns_recovery
    }

    fn clear_recovery_claim(&mut self) {
        if matches!(
            &self.runtime,
            OpenCodePromptRuntimeState::Detached {
                cancellation: OpenCodeDetachedCancellation::Owned(_),
                ..
            }
        ) {
            let runtime = std::mem::replace(
                &mut self.runtime,
                OpenCodePromptRuntimeState::Recovery {
                    cancellation: OpenCodeRecoveryCancellation::Requested,
                },
            );
            if let OpenCodePromptRuntimeState::Detached { owner, .. } = runtime {
                self.runtime = OpenCodePromptRuntimeState::Detached {
                    owner,
                    cancellation: OpenCodeDetachedCancellation::Requested,
                };
            } else {
                self.runtime = runtime;
            }
            return;
        }
        if matches!(
            &self.runtime,
            OpenCodePromptRuntimeState::Recovery {
                cancellation: OpenCodeRecoveryCancellation::Owned(_),
            }
        ) {
            self.runtime = OpenCodePromptRuntimeState::Recovery {
                cancellation: OpenCodeRecoveryCancellation::Requested,
            };
        }
    }

    fn detach_live(&mut self) -> Result<(), SdkError> {
        let runtime = std::mem::replace(
            &mut self.runtime,
            OpenCodePromptRuntimeState::Recovery {
                cancellation: OpenCodeRecoveryCancellation::Requested,
            },
        );
        self.runtime = match runtime {
            OpenCodePromptRuntimeState::Live {
                owner,
                cancellation: OpenCodeLiveCancellation::Available,
            } => OpenCodePromptRuntimeState::Detached {
                owner,
                cancellation: OpenCodeDetachedCancellation::Available,
            },
            OpenCodePromptRuntimeState::Live {
                owner,
                cancellation: OpenCodeLiveCancellation::Requested,
            } => OpenCodePromptRuntimeState::Detached {
                owner,
                cancellation: OpenCodeDetachedCancellation::Requested,
            },
            runtime => {
                self.runtime = runtime;
                return Err(SdkError::invalid_state(
                    "prompt generation runtime is not live",
                ));
            }
        };
        Ok(())
    }

    fn release_live_for_recovery(&mut self) -> Result<OpenCodePromptLocalOwner, SdkError> {
        let runtime = std::mem::replace(
            &mut self.runtime,
            OpenCodePromptRuntimeState::Recovery {
                cancellation: OpenCodeRecoveryCancellation::Requested,
            },
        );
        match runtime {
            OpenCodePromptRuntimeState::Live {
                owner,
                cancellation: OpenCodeLiveCancellation::Requested,
            } => Ok(owner),
            runtime => {
                self.runtime = runtime;
                Err(SdkError::invalid_state(
                    "prompt cancellation joiners lack a live cancellation request",
                ))
            }
        }
    }

    fn release_local_for_recovery(&mut self) -> Option<OpenCodePromptLocalOwner> {
        let runtime = std::mem::replace(
            &mut self.runtime,
            OpenCodePromptRuntimeState::Recovery {
                cancellation: OpenCodeRecoveryCancellation::Requested,
            },
        );
        match runtime {
            OpenCodePromptRuntimeState::Detached {
                owner,
                cancellation: OpenCodeDetachedCancellation::Requested,
            } => Some(owner),
            OpenCodePromptRuntimeState::Detached {
                owner,
                cancellation: OpenCodeDetachedCancellation::Owned(recovery_owner),
            } => {
                self.runtime = OpenCodePromptRuntimeState::Recovery {
                    cancellation: OpenCodeRecoveryCancellation::Owned(recovery_owner),
                };
                Some(owner)
            }
            runtime => {
                self.runtime = runtime;
                None
            }
        }
    }

    fn into_live_owner(self) -> Result<OpenCodePromptLocalOwner, SdkError> {
        match self.runtime {
            OpenCodePromptRuntimeState::Live { owner, .. } => Ok(owner),
            OpenCodePromptRuntimeState::Detached { .. }
            | OpenCodePromptRuntimeState::Recovery { .. } => Err(SdkError::invalid_state(
                "prompt generation runtime is not live",
            )),
        }
    }

    fn into_local_owner(self) -> Option<OpenCodePromptLocalOwner> {
        match self.runtime {
            OpenCodePromptRuntimeState::Live { owner, .. }
            | OpenCodePromptRuntimeState::Detached { owner, .. } => Some(owner),
            OpenCodePromptRuntimeState::Recovery { .. } => None,
        }
    }

    fn publish<T>(self, result: Result<T, SdkError>) -> Result<T, SdkError> {
        let Self {
            generation_id: _,
            runtime,
            durability: _,
            abort_joiners: _,
            abort: _,
            observation: _,
        } = self;
        runtime.publish(result)
    }

    fn release(self) {
        let _ = self.publish(Ok(()));
    }
}

impl OpenCodePromptRuntimeState {
    fn publish<T>(self, result: Result<T, SdkError>) -> Result<T, SdkError> {
        match self {
            Self::Live { owner, .. } | Self::Detached { owner, .. } => owner.publish(result),
            Self::Recovery { .. } => result,
        }
    }
}

impl OpenCodePromptLocalOwner {
    fn publish<T>(self, result: Result<T, SdkError>) -> Result<T, SdkError> {
        let result = self.admission.publish(result);
        drop(self.permit);
        result
    }

    fn release(self) {
        let _ = self.publish(Ok(()));
    }
}

impl<S: StateStoreBackend> OpenCodePromptRecovery<S> {
    pub(crate) async fn prepare_interaction_cleanup(
        &self,
        native_state: Option<OpenCodeNativePromptState>,
        deadline: Instant,
    ) -> Result<(), SdkError> {
        if Instant::now() >= deadline {
            return Err(SdkError::timeout("prompt recovery deadline elapsed")
                .at_phase(ErrorPhase::Reconciliation));
        }
        let snapshot = self
            .generation
            .prompts
            .storage
            .store()
            .load(OpenCodePromptRecord::key(&self.generation.conversation_id)?)
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "recovery prompt could not be loaded")
                    .at_phase(ErrorPhase::StateRead)
            })?;
        let Some(snapshot) = snapshot else {
            return Ok(());
        };
        let record = OpenCodePromptRecord::decode(snapshot, &self.generation.conversation_id)?;
        if record.generation_id() != self.generation.generation_id {
            return Err(SdkError::conflict(
                "prompt recovery belongs to another generation",
            ));
        }
        if record.interaction_cleanup_ready()
            && (record.cancellation_accepted()
                || native_state.is_some_and(OpenCodeNativePromptState::is_idle))
        {
            // This retained recovery claim excludes the old foreground owner.
            // Preparation keeps its original outcome facts until native cleanup.
            self.generation
                .settle_prompt(OpenCodePromptSettlement::InteractionCleanup, deadline)
                .await?;
        }
        Ok(())
    }
}

impl<S: StateStoreBackend> Drop for OpenCodePromptRecovery<S> {
    fn drop(&mut self) {
        let changed = {
            let mut registrations = match self.generation.prompts.registrations.lock() {
                Ok(registrations) => registrations,
                Err(poisoned) => poisoned.into_inner(),
            };
            let Some(reservation) = registrations.get_mut(&self.generation.conversation_id) else {
                return;
            };
            if reservation.generation_id != self.generation.generation_id
                || !reservation.release_recovery(&self.owner)
            {
                return;
            }
            if matches!(
                &reservation.runtime,
                OpenCodePromptRuntimeState::Recovery { .. }
            ) && reservation.abort_joiners == 0
                && reservation.durability.is_releasable()
                && !reservation.abort.deletion_pending()
            {
                registrations.remove(&self.generation.conversation_id);
            }
            true
        };
        if changed {
            self.generation.prompts.mark_changed();
        }
    }
}

impl<S: StateStoreBackend> Drop for OpenCodePromptRegistration<S> {
    fn drop(&mut self) {
        let released = {
            let mut registrations = match self.generation.prompts.registrations.lock() {
                Ok(registrations) => registrations,
                Err(poisoned) => poisoned.into_inner(),
            };
            let Some(reservation) = registrations.get_mut(&self.generation.conversation_id) else {
                return;
            };
            if reservation.generation_id != self.generation.generation_id {
                return;
            }
            if reservation.durability.is_releasable() && !reservation.abort.deletion_pending() {
                reservation.abort.settle();
                if reservation.abort_joiners == 0 {
                    registrations
                        .remove(&self.generation.conversation_id)
                        .and_then(OpenCodePromptReservation::into_local_owner)
                } else {
                    reservation.release_local_for_recovery()
                }
            } else {
                let _ = reservation.detach_live();
                None
            }
        };
        self.generation.prompts.mark_changed();
        if let Some(ownership) = released {
            ownership.release();
        }
    }
}

impl<S: StateStoreBackend> OpenCodeAbortJoin<S> {
    pub(crate) fn generation(&self) -> &OpenCodePromptGeneration<S> {
        &self.generation
    }
}

impl<S: StateStoreBackend> Drop for OpenCodeAbortJoin<S> {
    fn drop(&mut self) {
        let released = {
            let mut registrations = match self.generation.prompts.registrations.lock() {
                Ok(registrations) => registrations,
                Err(poisoned) => poisoned.into_inner(),
            };
            let Some(reservation) = registrations.get_mut(&self.generation.conversation_id) else {
                return;
            };
            if reservation.generation_id != self.generation.generation_id
                || reservation.abort_joiners == 0
            {
                return;
            }
            reservation.abort_joiners -= 1;
            if reservation.abort_joiners == 0
                && reservation.durability.is_releasable()
                && reservation.releases_with_durable_state()
            {
                registrations.remove(&self.generation.conversation_id)
            } else {
                None
            }
        };
        self.generation.prompts.mark_changed();
        if let Some(reservation) = released {
            reservation.release();
        }
    }
}
