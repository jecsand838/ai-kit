use crate::error::{DeadlineKind, ErrorDetail, ErrorKind, ErrorPhase, Operation};
use std::future::Future as _;
use std::sync::Arc;
use std::task::Poll;

use tokio::sync::oneshot;

use crate::backends::opencode::agent::events::OpenCodeEventHandle;
use crate::backends::opencode::protocol::http::{OpenCodeHttp, OpenCodeMutationError};
use crate::backends::opencode::protocol::requests::{OpenCodeMessageRequest, OpenCodeRouting};
use crate::backends::opencode::protocol::responses::OpenCodeMessageResponse;
use crate::backends::opencode::state_store::records::{
    OpenCodeCancellation, OpenCodeCancellationOwner, OpenCodePromptEffective, OpenCodePromptModel,
    OpenCodeSessionBinding,
};
use crate::backends::opencode::state_store::{
    OpenCodeAbort, OpenCodeAbortOutcome, OpenCodeAbortProgress, OpenCodeNativePromptState,
    OpenCodePromptGeneration, OpenCodePromptRegistration, OpenCodePromptStartError,
    OpenCodePromptStatusSignal, OpenCodeStateStore,
};
use crate::core::hierarchy::{HierarchyAdmission, HierarchyClaim};
use crate::core::state::ConversationRecord;
use crate::prompt::PromptOutput;
use crate::{AgentId, PromptResponse, SdkError};
use storage_sdk::StateStoreBackend;

const PROMPT_STATE_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(25);

pub(crate) struct OpenCodeConversationRuntime<S: StateStoreBackend> {
    agent_id: AgentId,
    state: OpenCodeStateStore<S>,
    http: OpenCodeHttp,
    events: OpenCodeEventHandle,
    prompt_policy: OpenCodePromptPolicy,
    binding: OpenCodeSessionBinding,
}

#[derive(Clone)]
pub(crate) struct OpenCodePromptPolicy {
    default_timeout: Option<std::time::Duration>,
    instructions: Option<Arc<str>>,
}

struct OpenCodeForegroundParent<'a, S: StateStoreBackend> {
    generation: &'a OpenCodePromptGeneration<S>,
    http: &'a OpenCodeHttp,
    events: &'a OpenCodeEventHandle,
    routing: &'a OpenCodeRouting,
    session_id: &'a str,
    message_id: &'a str,
    part_id: &'a str,
    structured: bool,
    deadline: Option<tokio::time::Instant>,
}

struct OpenCodePromptSession<S: StateStoreBackend> {
    events: OpenCodeEventHandle,
    ownership: OpenCodePromptOwnership<S>,
    waiter: OpenCodePromptWaiterState,
    cancellation_owner: OpenCodeCancellationOwner,
}

enum OpenCodePromptOwnership<S: StateStoreBackend> {
    Reserved(OpenCodePromptRegistration<S>),
    Durable(OpenCodePromptRegistration<S>),
    Released,
}

pub(super) enum OpenCodePromptWaiterState {
    Waiting(oneshot::Sender<oneshot::Receiver<Result<PromptResponse, SdkError>>>),
    Completing(oneshot::Sender<Result<PromptResponse, SdkError>>),
    Streaming(Arc<PromptOutput>),
    Released,
}

#[derive(Clone, Copy)]
enum OpenCodePersistencePolicy {
    Lifecycle,
    Generation,
    Reopen,
    Retained,
}

impl OpenCodePersistencePolicy {
    fn retryable<S: StateStoreBackend>(self, error: &SdkError) -> bool {
        match self {
            Self::Retained => false,
            Self::Lifecycle => OpenCodeStateStore::<S>::retryable_state_error(error),
            Self::Generation | Self::Reopen => {
                error.context().phase != ErrorPhase::Cleanup
                    && !error
                        .supplementary()
                        .iter()
                        .any(|cleanup| cleanup.context().phase == ErrorPhase::Cleanup)
                    && ((matches!(self, Self::Generation)
                        && error.kind() == crate::error::ErrorKind::Conflict
                        && error.detail() == Some(&ErrorDetail::TransactionConflict))
                        || (error.context().phase != ErrorPhase::StateCommit
                            && OpenCodeStateStore::<S>::retryable_state_error(error)))
            }
        }
    }
}

enum OpenCodePromptDelivery {
    Terminal,
    ResponseLost,
}

enum OpenCodePromptActivity {
    Native(Box<Result<OpenCodeMessageResponse, OpenCodeMutationError>>),
    StatusSignal(
        Result<
            (crate::ConversationId, OpenCodePromptStatusSignal),
            tokio::sync::broadcast::error::RecvError,
        >,
    ),
    StateChanged,
    StateClosed,
    CallerClosed,
}

impl OpenCodePromptPolicy {
    pub(crate) fn new(
        default_timeout: Option<std::time::Duration>,
        instructions: Option<&str>,
    ) -> Self {
        Self {
            default_timeout,
            instructions: instructions.map(Arc::from),
        }
    }

    pub(super) fn default_timeout(&self) -> Option<std::time::Duration> {
        self.default_timeout
    }

    pub(super) fn instructions(&self) -> Option<&str> {
        self.instructions.as_deref()
    }
}

impl<S: StateStoreBackend> OpenCodePromptOwnership<S> {
    fn registration(&self) -> Result<&OpenCodePromptRegistration<S>, SdkError> {
        match self {
            Self::Reserved(registration) | Self::Durable(registration) => Ok(registration),
            Self::Released => Err(SdkError::invalid_state(
                "prompt generation ownership is released",
            )),
        }
    }

    fn mark_durable(&mut self) {
        let current = std::mem::replace(self, Self::Released);
        *self = match current {
            Self::Reserved(registration) | Self::Durable(registration) => {
                Self::Durable(registration)
            }
            Self::Released => Self::Released,
        };
    }

    fn take(&mut self) -> Result<OpenCodePromptRegistration<S>, SdkError> {
        match std::mem::replace(self, Self::Released) {
            Self::Reserved(registration) | Self::Durable(registration) => Ok(registration),
            Self::Released => Err(SdkError::invalid_state(
                "prompt generation ownership is released",
            )),
        }
    }
}

impl OpenCodePromptWaiterState {
    fn is_waiting(&self) -> bool {
        matches!(self, Self::Waiting(_))
    }

    fn output(&self) -> Option<Arc<PromptOutput>> {
        match self {
            Self::Streaming(output) => Some(Arc::clone(output)),
            _ => None,
        }
    }

    fn is_closed(&self) -> bool {
        match self {
            Self::Waiting(sender) => sender.is_closed(),
            Self::Streaming(_) | Self::Completing(_) => false,
            Self::Released => true,
        }
    }

    async fn closed(&mut self) {
        match self {
            Self::Waiting(sender) => sender.closed().await,
            _ => std::future::pending().await,
        }
    }

    fn discard_response(&mut self) {
        if matches!(self, Self::Waiting(_) | Self::Completing(_)) {
            *self = Self::Released;
        }
    }

    fn begin_completion(&mut self) -> bool {
        if !matches!(self, Self::Waiting(_)) {
            return false;
        }
        let Self::Waiting(sender) = std::mem::replace(self, Self::Released) else {
            return false;
        };
        let (completion, completed) = oneshot::channel();
        if sender.send(completed).is_err() {
            return false;
        }
        *self = Self::Completing(completion);
        true
    }

    fn deliver(&mut self, result: Result<PromptResponse, SdkError>) -> bool {
        let result = result.map_err(|error| {
            error
                .at_operation(Operation::Prompt)
                .in_phase(ErrorPhase::ModelGeneration)
        });
        match std::mem::replace(self, Self::Released) {
            Self::Waiting(sender) => {
                let (completion, completed) = oneshot::channel();
                sender.send(completed).is_ok() && completion.send(result).is_ok()
            }
            Self::Completing(sender) => sender.send(result).is_ok(),
            Self::Streaming(output) => {
                output.finish(result);
                false
            }
            Self::Released => false,
        }
    }
}

impl<S: StateStoreBackend> OpenCodePromptSession<S> {
    fn new(
        waiter: OpenCodePromptWaiterState,
        registration: OpenCodePromptRegistration<S>,
        events: OpenCodeEventHandle,
    ) -> Self {
        Self {
            events,
            ownership: OpenCodePromptOwnership::Reserved(registration),
            waiter,
            cancellation_owner: OpenCodeCancellationOwner::fresh(),
        }
    }

    fn mark_durable(&mut self) {
        self.ownership.mark_durable();
    }

    fn generation(&self) -> Result<&OpenCodePromptGeneration<S>, SdkError> {
        self.ownership
            .registration()
            .map(OpenCodePromptRegistration::<S>::generation)
    }

    fn has_waiter(&self) -> bool {
        self.waiter.is_waiting()
    }

    fn caller_is_closed(&self) -> bool {
        self.waiter.is_closed()
    }

    fn cancellation_requested(&self) -> Result<bool, SdkError> {
        self.ownership.registration()?.cancellation_requested()
    }

    fn begin_dispatch(&self) -> Result<bool, SdkError> {
        self.ownership.registration()?.begin_dispatch()
    }

    fn runtime_active(&self) -> Result<bool, SdkError> {
        self.ownership.registration()?.is_active()
    }

    async fn persist_cancellation(&self, deadline: tokio::time::Instant) -> Result<(), SdkError> {
        self.await_settlement(
            OpenCodePersistencePolicy::Retained,
            None,
            deadline,
            || async { self.generation()?.request_abort(deadline).await },
        )
        .await
    }

    async fn persist_dispatch(&self, deadline: tokio::time::Instant) -> Result<bool, SdkError> {
        self.await_settlement(
            OpenCodePersistencePolicy::Generation,
            None,
            deadline,
            || async { self.generation()?.mark_dispatched(deadline).await },
        )
        .await
    }

    async fn admission_observed(&self) -> Result<bool, SdkError> {
        self.generation()?.admission_observed().await
    }

    async fn stop_before_dispatch(
        &mut self,
        result: Result<PromptResponse, SdkError>,
        deadline: tokio::time::Instant,
    ) -> Result<bool, SdkError> {
        let cleanup = self
            .await_settlement(
                OpenCodePersistencePolicy::Lifecycle,
                result.as_ref().err(),
                deadline,
                || async { self.generation()?.stop_before_dispatch().await },
            )
            .await;
        let cancelled = match cleanup {
            Ok(cancelled) => cancelled,
            Err(error) => return Err(result.err().unwrap_or(error)),
        };
        if cancelled {
            if self.caller_is_closed() {
                self.discard_waiter();
            }
            return Ok(true);
        }
        if self.caller_is_closed() {
            self.discard_waiter();
            self.release();
        } else {
            self.finish_releasable(result);
        }
        Ok(false)
    }

    async fn persist_admission(&self, deadline: tokio::time::Instant) -> Result<bool, SdkError> {
        self.await_settlement(
            OpenCodePersistencePolicy::Generation,
            None,
            deadline,
            || async { self.generation()?.mark_admitted(deadline).await },
        )
        .await
    }

    async fn persist_waiter_loss(&self, deadline: tokio::time::Instant) -> Result<(), SdkError> {
        self.await_settlement(
            OpenCodePersistencePolicy::Generation,
            None,
            deadline,
            || async { self.generation()?.mark_waiter_lost(deadline).await },
        )
        .await
    }

    async fn await_settlement<T, F>(
        &self,
        policy: OpenCodePersistencePolicy,
        primary: Option<&SdkError>,
        deadline: tokio::time::Instant,
        mut operation: impl FnMut() -> F,
    ) -> Result<T, SdkError>
    where
        F: std::future::Future<Output = Result<T, SdkError>>,
    {
        let work = async {
            let mut last_error = None;
            loop {
                if tokio::time::Instant::now() >= deadline {
                    let error = SdkError::timeout("prompt persistence deadline elapsed")
                        .at_phase(ErrorPhase::Settlement);
                    return Err(match last_error {
                        Some(attempt) => error.with_supplementary(attempt),
                        None => error,
                    });
                }
                match operation().await {
                    Err(error) if policy.retryable::<S>(&error) => {
                        last_error = Some(error);
                        tokio::time::sleep_until(
                            deadline.min(tokio::time::Instant::now() + PROMPT_STATE_RETRY_INTERVAL),
                        )
                        .await;
                    }
                    Err(error)
                        if last_error.is_some()
                            && tokio::time::Instant::now() >= deadline
                            && error.kind() == ErrorKind::Timeout
                            && error.context().phase == ErrorPhase::Settlement
                            && error.detail()
                                == Some(&ErrorDetail::DeadlineElapsed {
                                    kind: DeadlineKind::Operation,
                                    timeout: None,
                                })
                            && error.cause().is_none()
                            && error.http_status().is_none()
                            && error.supplementary().is_empty()
                            && !error.diagnostics_truncated() =>
                    {
                        // Mutation admission crossed the same bound after the
                        // loop check. Retain the prior attempt on its timeout.
                    }
                    result => return result,
                }
            }
        };
        let mut work = std::pin::pin!(work);
        tokio::select! {
            result = &mut work => return result,
            _ = tokio::time::sleep_until(deadline) => {
                if let Some(output) = self.waiter.output() {
                    output.finish(Err(primary
                        .cloned()
                        .map(|primary| primary.with_supplementary(
                            SdkError::timeout("prompt persistence deadline elapsed")
                                .at_phase(ErrorPhase::Settlement)))
                        .unwrap_or_else(|| SdkError::timeout("prompt persistence deadline elapsed")
                            .at_phase(ErrorPhase::Settlement))
                        .at_operation(Operation::Prompt)));
                }
            }
        }
        // Observation can expire, but the same owner retains each admitted
        // native future. Its outcome is read before any retry is considered.
        work.await
    }

    async fn persist_terminal(
        &self,
        result: &Result<PromptResponse, SdkError>,
        without_cancellation_dispatch: bool,
        deadline: tokio::time::Instant,
    ) -> Result<(), SdkError> {
        if let Err(error) = result {
            self.generation()?.retain_execution_error(error);
        }
        self.await_settlement(
            OpenCodePersistencePolicy::Retained,
            result.as_ref().err(),
            deadline,
            || async {
                if without_cancellation_dispatch {
                    self.generation()?
                        .terminalize_without_abort_dispatch(
                            &self.cancellation_owner,
                            result,
                            deadline,
                        )
                        .await
                } else {
                    self.generation()?.terminalize(result, deadline).await
                }
            },
        )
        .await
    }

    async fn persist_response_loss(
        &self,
        result: &Result<PromptResponse, SdkError>,
        deadline: tokio::time::Instant,
    ) -> Result<(), SdkError> {
        if let Err(error) = result {
            self.generation()?.retain_execution_error(error);
        }
        self.await_settlement(
            OpenCodePersistencePolicy::Generation,
            result.as_ref().err(),
            deadline,
            || async { self.generation()?.lose_response(deadline).await },
        )
        .await
    }

    async fn persist_cancellation_outcome(
        &self,
        outcome: OpenCodeCancellation,
        deadline: tokio::time::Instant,
    ) -> Result<(), SdkError> {
        self.await_settlement(
            OpenCodePersistencePolicy::Generation,
            None,
            deadline,
            || async {
                self.generation()?
                    .persist_abort(outcome.clone(), deadline)
                    .await
            },
        )
        .await
    }

    async fn claim_cancellation(&self, deadline: tokio::time::Instant) -> Result<bool, SdkError> {
        self.await_settlement(
            OpenCodePersistencePolicy::Generation,
            None,
            deadline,
            || async {
                self.generation()?
                    .claim_abort(&self.cancellation_owner, deadline)
                    .await
            },
        )
        .await
    }

    async fn begin_cancellation_dispatch(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), SdkError> {
        self.await_settlement(
            OpenCodePersistencePolicy::Generation,
            None,
            deadline,
            || async {
                if self
                    .generation()?
                    .begin_abort_dispatch(&self.cancellation_owner, deadline)
                    .await?
                {
                    Ok(())
                } else {
                    Err(SdkError::invalid_state(
                        "prompt cancellation reservation changed before dispatch",
                    ))
                }
            },
        )
        .await
    }

    async fn settle_delivery(
        &mut self,
        result: &Result<PromptResponse, SdkError>,
        delivery: OpenCodePromptDelivery,
        deadline: tokio::time::Instant,
    ) -> Result<(), SdkError> {
        let generation = self.generation()?.clone();
        let claimed = loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(SdkError::timeout(
                    "prompt terminal cleanup deadline elapsed",
                ));
            }
            match generation.claim_delivery(deadline).await {
                Ok(claimed) => break claimed,
                Err(error) if OpenCodePersistencePolicy::Generation.retryable::<S>(&error) => {
                    tokio::time::sleep_until(
                        deadline.min(tokio::time::Instant::now() + PROMPT_STATE_RETRY_INTERVAL),
                    )
                    .await;
                }
                Err(error) => return Err(error),
            }
        };
        let delivered = if claimed {
            self.waiter.begin_completion()
        } else {
            self.discard_waiter();
            false
        };
        if claimed {
            loop {
                if tokio::time::Instant::now() >= deadline {
                    return Err(SdkError::timeout(
                        "prompt terminal cleanup deadline elapsed",
                    ));
                }
                match generation.settle_delivery(delivered, deadline).await {
                    Ok(()) => break,
                    Err(error) if OpenCodePersistencePolicy::Generation.retryable::<S>(&error) => {
                        tokio::time::sleep_until(
                            deadline.min(tokio::time::Instant::now() + PROMPT_STATE_RETRY_INTERVAL),
                        )
                        .await;
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        if matches!(delivery, OpenCodePromptDelivery::ResponseLost) {
            return Ok(());
        }
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(SdkError::timeout(
                    "prompt terminal cleanup deadline elapsed",
                ));
            }
            if matches!(result, Err(error) if error.kind() == ErrorKind::Cancelled) {
                generation
                    .prepare_live_interaction_cleanup(deadline)
                    .await?;
            }
            if generation.requires_native_cleanup().await? {
                self.events
                    .synchronize(generation.conversation_id(), deadline)
                    .await?;
                continue;
            }
            match generation.clear().await {
                Ok(()) => break,
                Err(error)
                    if OpenCodeStateStore::<S>::retryable_state_error(&error)
                        || (error.context().phase == ErrorPhase::StateCommit
                            && OpenCodePersistencePolicy::Generation.retryable::<S>(&error)) =>
                {
                    // A consuming commit conflict rejected this deletion. Re-read
                    // its generation and clearability under the existing deadline;
                    // native prompt and interaction effects remain settled.
                    tokio::time::sleep_until(
                        deadline.min(tokio::time::Instant::now() + PROMPT_STATE_RETRY_INTERVAL),
                    )
                    .await;
                }
                Err(error) => return Err(error),
            }
        }
        self.release();
        Ok(())
    }

    async fn finish_response(
        &mut self,
        result: Result<PromptResponse, SdkError>,
        delivery: OpenCodePromptDelivery,
        deadline: tokio::time::Instant,
    ) -> bool {
        let output = self.waiter.output();
        let primary = result.as_ref().err().cloned();
        let completion = async {
            let cleanup = self.settle_delivery(&result, delivery, deadline).await;
            let settled = cleanup.is_ok();
            // The execution failure remains authoritative even when delivery cleanup fails.
            let result = match result {
                Err(error) => Err(match cleanup {
                    Ok(()) => error,
                    Err(cleanup) => error.with_supplementary(cleanup.at_phase(ErrorPhase::Cleanup)),
                }),
                Ok(response) => cleanup.map(|()| response),
            };
            let _ = self.deliver(result);
            settled
        };
        let mut completion = std::pin::pin!(completion);
        tokio::select! {
            settled = &mut completion => return settled,
            _ = tokio::time::sleep_until(deadline) => {
                if let Some(output) = output {
                    let cleanup = SdkError::timeout("prompt terminal cleanup deadline elapsed")
                        .at_phase(ErrorPhase::Cleanup).at_operation(Operation::Prompt);
                    output.finish(Err(match primary {
                        Some(primary) => primary.with_supplementary(cleanup).at_operation(Operation::Prompt),
                        None => cleanup,
                    }));
                }
            }
        }
        // An admitted native transaction keeps its commit authority after the observer
        // deadline. Continue polling it here; no later retry is admitted after the bound.
        completion.await
    }

    async fn finish_idle_response_loss(&mut self, deadline: tokio::time::Instant) {
        let generation = match self.generation() {
            Ok(generation) => generation.clone(),
            Err(error) => {
                self.finish_without_prompt(Err(error));
                return;
            }
        };
        let preparation = self
            .await_settlement(OpenCodePersistencePolicy::Retained, None, deadline, || {
                generation.prepare_live_interaction_cleanup(deadline)
            })
            .await;
        if let Err(error) = preparation {
            self.finish_without_prompt(Err(error));
            return;
        }
        let cleanup = async {
            if generation.requires_native_cleanup().await? {
                self.events
                    .synchronize(generation.conversation_id(), deadline)
                    .await?;
            }
            Ok::<(), SdkError>(())
        }
        .await;
        if let Err(error) = cleanup {
            self.finish_without_prompt(Err(error));
            return;
        }
        match self
            .await_settlement(OpenCodePersistencePolicy::Lifecycle, None, deadline, || {
                generation.reconcile_live_idle_response_loss()
            })
            .await
        {
            Ok(()) => self.release(),
            Err(error) => self.finish_without_prompt(Err(error)),
        }
    }

    async fn caller_closed(&mut self) {
        self.waiter.closed().await;
    }

    fn discard_waiter(&mut self) {
        self.waiter.discard_response();
    }

    fn deliver(&mut self, result: Result<PromptResponse, SdkError>) -> bool {
        self.waiter.deliver(result)
    }

    fn finish_without_prompt(&mut self, result: Result<PromptResponse, SdkError>) {
        let result = match std::mem::replace(&mut self.ownership, OpenCodePromptOwnership::Released)
        {
            OpenCodePromptOwnership::Reserved(registration) => {
                registration.publish_unstarted(result)
            }
            ownership @ OpenCodePromptOwnership::Durable(_) => {
                self.ownership = ownership;
                result
            }
            OpenCodePromptOwnership::Released => result,
        };
        let _ = self.deliver(result);
    }

    fn finish_releasable(&mut self, result: Result<PromptResponse, SdkError>) {
        let result = self.publish(result);
        let _ = self.deliver(result);
    }

    fn publish(
        &mut self,
        result: Result<PromptResponse, SdkError>,
    ) -> Result<PromptResponse, SdkError> {
        self.ownership
            .take()
            .and_then(|registration| registration.publish_terminal(result))
    }

    fn release(&mut self) {
        if let Ok(registration) = self.ownership.take() {
            registration.release_terminal();
        }
    }

    fn lose_waiter(&mut self) {
        self.discard_waiter();
    }
}

impl<S: StateStoreBackend> Drop for OpenCodePromptSession<S> {
    fn drop(&mut self) {
        let stopped = SdkError::closed("prompt owner stopped before delivery");
        let error = match self
            .generation()
            .ok()
            .and_then(OpenCodePromptGeneration::retained_execution_error)
        {
            Some(primary) => primary.with_supplementary(stopped.at_phase(ErrorPhase::Cleanup)),
            None => stopped,
        };
        self.finish_without_prompt(Err(error));
    }
}

impl<S: StateStoreBackend> OpenCodeConversationRuntime<S> {
    pub(crate) fn new(
        agent_id: AgentId,
        state: OpenCodeStateStore<S>,
        http: OpenCodeHttp,
        events: OpenCodeEventHandle,
        prompt_policy: OpenCodePromptPolicy,
        binding: OpenCodeSessionBinding,
    ) -> Self {
        Self {
            agent_id,
            state,
            http,
            events,
            prompt_policy,
            binding,
        }
    }

    pub(super) fn agent_id(&self) -> &AgentId {
        &self.agent_id
    }

    pub(super) fn state(&self) -> &OpenCodeStateStore<S> {
        &self.state
    }

    pub(super) fn http(&self) -> &OpenCodeHttp {
        &self.http
    }

    pub(super) fn prompt_policy(&self) -> &OpenCodePromptPolicy {
        &self.prompt_policy
    }

    pub(super) fn binding(&self) -> &OpenCodeSessionBinding {
        &self.binding
    }

    pub(super) async fn prepare_close(
        &self,
        record: ConversationRecord,
        claim: HierarchyClaim,
        admission: HierarchyAdmission,
        deadline: tokio::time::Instant,
    ) -> Result<(HierarchyClaim, HierarchyAdmission), SdkError> {
        let conversation_id = record.id().clone();
        let lease_id = self.state.notifications().receiver_lease(&conversation_id);
        let result = self
            .events
            .prepare_close(record, claim, admission, deadline)
            .await;
        if result.is_err()
            && let Some(lease_id) = lease_id
        {
            self.state
                .notifications()
                .fail_receiver(&conversation_id, lease_id);
        }
        result
    }

    pub(super) fn start_prompt(
        &self,
        request: OpenCodeMessageRequest,
        structured_schema: Option<serde_json::Value>,
        deadline: Option<tokio::time::Instant>,
        state_admission_deadline: tokio::time::Instant,
        admission: HierarchyAdmission,
        waiter: OpenCodePromptWaiterState,
    ) -> Result<OpenCodePromptGeneration<S>, SdkError> {
        if deadline.is_some_and(|deadline| deadline <= tokio::time::Instant::now()) {
            return admission.publish(Err(SdkError::timeout("caller prompt deadline elapsed")));
        }
        if request.structured_schema() != structured_schema.as_ref() {
            return admission.publish(Err(SdkError::invalid_state(
                "structured prompt state contradicts its native request",
            )));
        }
        let routing = match OpenCodeRouting::for_directory(self.binding.directory().to_path_buf()) {
            Ok(routing) => routing,
            Err(error) => return admission.publish(Err(error)),
        };
        let permit = match self.state.prompt_permit() {
            Ok(permit) => permit,
            Err(error) => return admission.publish(Err(error)),
        };
        let generation_id = format!("prompt_{}", uuid::Uuid::new_v4().simple());
        let prompt_part_id = request.part_id().to_owned();
        let requires_structured = request.requires_structured();
        let model = match request.model() {
            Some(model) => match OpenCodePromptModel::new(
                model.provider_id().to_owned(),
                model.model_id().to_owned(),
            ) {
                Ok(model) => Some(model),
                Err(error) => return admission.publish(Err(error)),
            },
            None => None,
        };
        let effective = match OpenCodePromptEffective::new(model, structured_schema.clone()) {
            Ok(effective) => effective,
            Err(error) => return admission.publish(Err(error)),
        };
        let prompt_timeout = match deadline {
            Some(deadline) => {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    return admission
                        .publish(Err(SdkError::timeout("caller prompt deadline elapsed")));
                }
                Some(remaining)
            }
            None => None,
        };
        let session_id = match self.binding.require_session_id() {
            Ok(session_id) => session_id.to_owned(),
            Err(error) => return admission.publish(Err(error)),
        };
        let registration = self.state.register_prompt(
            self.binding.conversation_id().clone(),
            generation_id,
            admission,
            permit,
        )?;
        let generation = registration.generation().clone();
        let owner_generation = generation.clone();
        let output = waiter.output();
        if let Some(output) = &output {
            generation.observe_output(
                session_id.clone(),
                prompt_part_id.clone(),
                Arc::clone(output),
            )?;
        }
        let state = self.state.clone();
        let agent_id = self.agent_id.clone();
        let http = self.http.clone();
        let events = self.events.clone();
        let binding = self.binding.clone();
        let session = OpenCodePromptSession::new(waiter, registration, self.events.clone());
        let task = Box::pin(async move {
            let mut session = session;
            loop {
                match owner_generation
                    .start(
                        &agent_id,
                        &binding,
                        prompt_part_id.clone(),
                        effective.clone(),
                        prompt_timeout,
                    )
                    .await
                {
                    Ok(()) => {
                        session.mark_durable();
                        break;
                    }
                    Err(OpenCodePromptStartError::TransactionConflict) => {
                        let now = tokio::time::Instant::now();
                        if now < state_admission_deadline {
                            tokio::time::sleep_until(
                                state_admission_deadline.min(now + PROMPT_STATE_RETRY_INTERVAL),
                            )
                            .await;
                            continue;
                        }
                        let error = if deadline.is_some_and(|deadline| deadline <= now) {
                            SdkError::timeout("caller prompt deadline elapsed")
                        } else {
                            SdkError::conflict("prompt transaction conflicted")
                        };
                        session.finish_without_prompt(Err(error));
                        return;
                    }
                    Err(OpenCodePromptStartError::Failure(error)) => {
                        session.finish_without_prompt(Err(error));
                        return;
                    }
                }
            }

            let cancellation_reserved = match session.cancellation_requested() {
                Ok(cancelled) => cancelled,
                Err(error) => {
                    session.finish_without_prompt(Err(error));
                    return;
                }
            };
            if cancellation_reserved
                && let Err(error) = session.persist_cancellation(events.deadline()).await
            {
                session.finish_without_prompt(Err(error));
                return;
            }

            let mut dispatch = true;
            let mut terminal = None;
            if !cancellation_reserved
                && (deadline.is_some_and(|deadline| deadline <= tokio::time::Instant::now())
                    || session.caller_is_closed())
            {
                match session
                    .stop_before_dispatch(
                        Err(SdkError::timeout(
                            "caller prompt deadline elapsed before dispatch",
                        )),
                        events.deadline(),
                    )
                    .await
                {
                    Ok(true) => {
                        dispatch = false;
                        terminal = Some(Err(SdkError::cancelled(
                            "foreground prompt was interrupted",
                        )));
                    }
                    Ok(false) => return,
                    Err(error) => {
                        session.finish_without_prompt(Err(error));
                        return;
                    }
                }
            }
            if dispatch && !cancellation_reserved {
                let idle = http
                    .session_status(&routing)
                    .await
                    .and_then(|statuses| statuses.require_idle(&session_id));
                if let Err(error) = idle {
                    match session
                        .stop_before_dispatch(Err(error), events.deadline())
                        .await
                    {
                        Ok(true) => {
                            dispatch = false;
                            terminal = Some(Err(SdkError::cancelled(
                                "foreground prompt was interrupted",
                            )));
                        }
                        Ok(false) => return,
                        Err(error) => {
                            session.finish_without_prompt(Err(error));
                            return;
                        }
                    }
                }
            }
            if dispatch && !cancellation_reserved {
                match session.begin_dispatch() {
                    Ok(true) => {}
                    Ok(false) => {
                        if let Err(error) = session.persist_cancellation(events.deadline()).await {
                            session.finish_without_prompt(Err(error));
                            return;
                        }
                    }
                    Err(error) => {
                        session.finish_without_prompt(Err(error));
                        return;
                    }
                }
            }
            let dispatch_deadline = events.deadline();
            if dispatch {
                let dispatch_result = session.persist_dispatch(dispatch_deadline).await;
                match dispatch_result {
                    Ok(true) => {}
                    Ok(false) => {
                        dispatch = false;
                        terminal = Some(Err(SdkError::cancelled(
                            "foreground prompt was interrupted",
                        )));
                    }
                    Err(error) => match session
                        .stop_before_dispatch(Err(error), events.deadline())
                        .await
                    {
                        Ok(true) => {
                            dispatch = false;
                            terminal = Some(Err(SdkError::cancelled(
                                "foreground prompt was interrupted",
                            )));
                        }
                        Ok(false) => return,
                        Err(cleanup_error) => {
                            session.finish_without_prompt(Err(cleanup_error));
                            return;
                        }
                    },
                }
            }
            if dispatch
                && (tokio::time::Instant::now() >= dispatch_deadline
                    || deadline.is_some_and(|deadline| deadline <= tokio::time::Instant::now())
                    || session.caller_is_closed())
            {
                match session
                    .stop_before_dispatch(
                        Err(SdkError::timeout(
                            "caller prompt deadline elapsed before dispatch",
                        )),
                        events.deadline(),
                    )
                    .await
                {
                    Ok(true) => {
                        dispatch = false;
                        terminal = Some(Err(SdkError::cancelled(
                            "foreground prompt was interrupted",
                        )));
                    }
                    Ok(false) => return,
                    Err(error) => {
                        session.finish_without_prompt(Err(error));
                        return;
                    }
                }
            }

            let mut changes = state.prompt_changes();
            let mut status_signals = state.prompt_statuses();
            let mut response =
                dispatch.then(|| Box::pin(http.message(&routing, &session_id, &request)));
            let mut admitted = false;
            let mut admission_observation = dispatch;
            let mut response_lost = false;
            let mut response_message_id = None;
            let mut awaiting_idle_repair = false;
            let mut idle_repair_observation = false;
            let mut cancellation_claim_without_dispatch = false;
            let mut ready_response = std::future::poll_fn(|context| {
                let polled =
                    response
                        .as_mut()
                        .and_then(|message| match message.as_mut().poll(context) {
                            Poll::Ready(result) => Some(result),
                            Poll::Pending => None,
                        });
                if polled.is_some() {
                    response = None;
                }
                Poll::Ready(polled)
            })
            .await;

            loop {
                if admitted && ready_response.is_none() && !awaiting_idle_repair {
                    ready_response = std::future::poll_fn(|context| {
                        let polled = response.as_mut().and_then(|message| {
                            match message.as_mut().poll(context) {
                                Poll::Ready(result) => Some(result),
                                Poll::Pending => None,
                            }
                        });
                        if polled.is_some() {
                            response = None;
                        }
                        Poll::Ready(polled)
                    })
                    .await;
                }
                if admitted && ready_response.is_none() {
                    match session.claim_cancellation(events.deadline()).await {
                        Ok(true) => {
                            let claimed_response = std::future::poll_fn(|context| {
                                let polled = response.as_mut().and_then(|message| {
                                    match message.as_mut().poll(context) {
                                        Poll::Ready(result) => Some(result),
                                        Poll::Pending => None,
                                    }
                                });
                                if polled.is_some() {
                                    response = None;
                                }
                                Poll::Ready(polled)
                            })
                            .await;
                            let exact_terminal = claimed_response.as_ref().is_some_and(|result| {
                                result
                                    .as_ref()
                                    .map(|_| true)
                                    .unwrap_or_else(|error| !error.is_ambiguous())
                            });
                            ready_response = claimed_response;
                            if exact_terminal {
                                cancellation_claim_without_dispatch = true;
                                continue;
                            }
                            if let Err(error) =
                                session.begin_cancellation_dispatch(events.deadline()).await
                            {
                                session.finish_without_prompt(Err(error));
                                return;
                            }
                            let cancellation = http.abort(&routing, &session_id).await;
                            let outcome = match cancellation {
                                Ok(()) => {
                                    OpenCodeCancellation::accepted(&session.cancellation_owner)
                                }
                                Err(error) if error.is_ambiguous() => {
                                    OpenCodeCancellation::ambiguous(&session.cancellation_owner)
                                }
                                Err(_) => {
                                    OpenCodeCancellation::rejected(&session.cancellation_owner)
                                }
                            };
                            let cancellation_accepted =
                                matches!(&outcome, OpenCodeCancellation::Accepted { .. });
                            let cancellation_rejected =
                                matches!(&outcome, OpenCodeCancellation::Rejected { .. });
                            if let Err(error) = session
                                .persist_cancellation_outcome(outcome, events.deadline())
                                .await
                            {
                                session.finish_without_prompt(Err(error));
                                return;
                            }
                            if cancellation_accepted {
                                let terminal_response = std::future::poll_fn(|context| {
                                    let polled =
                                        response.as_mut().and_then(|message| {
                                            match message.as_mut().poll(context) {
                                                Poll::Ready(result) => Some(result),
                                                Poll::Pending => None,
                                            }
                                        });
                                    if polled.is_some() {
                                        response = None;
                                    }
                                    Poll::Ready(polled)
                                })
                                .await;
                                if terminal_response.as_ref().is_some_and(|result| {
                                    result
                                        .as_ref()
                                        .map(|_| true)
                                        .unwrap_or_else(|error| !error.is_ambiguous())
                                }) {
                                    ready_response = terminal_response;
                                    continue;
                                }
                                response = None;
                                let result =
                                    Err(SdkError::cancelled("foreground prompt was interrupted"));
                                match session
                                    .persist_terminal(&result, false, events.deadline())
                                    .await
                                {
                                    Ok(()) => terminal = Some(result),
                                    Err(error) => {
                                        session.finish_without_prompt(result.and(Err(error)));
                                        return;
                                    }
                                }
                            } else if cancellation_rejected && response_lost {
                                awaiting_idle_repair = true;
                                idle_repair_observation = true;
                            }
                        }
                        Ok(false) => {}
                        Err(error) => {
                            session.finish_without_prompt(Err(error));
                            return;
                        }
                    }
                }

                if let Some(result) = terminal.take() {
                    if result.is_ok()
                        && let Some(output) = session.waiter.output()
                        && let Some(message_id) = response_message_id.as_deref()
                    {
                        let observation_deadline = deadline.unwrap_or_else(|| events.deadline());
                        while !owner_generation.output_complete(message_id) {
                            if tokio::time::timeout_at(observation_deadline, changes.changed())
                                .await
                                .is_err()
                            {
                                output.finish(Err(SdkError::timeout(
                                    "prompt output completion marker deadline elapsed",
                                )));
                                break;
                            }
                        }
                    }
                    session
                        .finish_response(
                            result,
                            OpenCodePromptDelivery::Terminal,
                            events.deadline(),
                        )
                        .await;
                    return;
                }

                if response_lost && admitted && !awaiting_idle_repair {
                    return;
                }

                if awaiting_idle_repair && idle_repair_observation {
                    idle_repair_observation = false;
                    if http
                        .session_status(&routing)
                        .await
                        .is_ok_and(|statuses| !statuses.is_active(&session_id))
                    {
                        session.finish_idle_response_loss(events.deadline()).await;
                        return;
                    }
                }

                let activity = if let Some(result) = ready_response.take() {
                    OpenCodePromptActivity::Native(Box::new(result))
                } else {
                    match response.as_mut() {
                        Some(message) => tokio::select! {
                            biased;
                            result = message => OpenCodePromptActivity::Native(Box::new(result)),
                            signal = status_signals.recv(), if admission_observation || awaiting_idle_repair => {
                                OpenCodePromptActivity::StatusSignal(signal)
                            }
                            changed = changes.changed() => if changed.is_ok() {
                                OpenCodePromptActivity::StateChanged
                            } else {
                                OpenCodePromptActivity::StateClosed
                            },
                            _ = session.caller_closed(), if session.has_waiter() => {
                                OpenCodePromptActivity::CallerClosed
                            }
                        },
                        None => tokio::select! {
                            signal = status_signals.recv(), if admission_observation || awaiting_idle_repair => {
                                OpenCodePromptActivity::StatusSignal(signal)
                            }
                            changed = changes.changed() => if changed.is_ok() {
                                OpenCodePromptActivity::StateChanged
                            } else {
                                OpenCodePromptActivity::StateClosed
                            },
                            _ = session.caller_closed(), if session.has_waiter() => {
                                OpenCodePromptActivity::CallerClosed
                            }
                        },
                    }
                };
                let native_result = match activity {
                    OpenCodePromptActivity::Native(result) => Some(*result),
                    OpenCodePromptActivity::StatusSignal(signal) => {
                        let native_state = match signal {
                            Ok((
                                conversation_id,
                                OpenCodePromptStatusSignal::Observed(_)
                                | OpenCodePromptStatusSignal::Refresh,
                            )) if &conversation_id == binding.conversation_id() => {
                                match http.session_status(&routing).await {
                                    Ok(statuses) => Some(OpenCodeNativePromptState::from_active(
                                        statuses.is_active(&session_id),
                                    )),
                                    Err(_) => None,
                                }
                            }
                            Ok(_) => None,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                match http.session_status(&routing).await {
                                    Ok(statuses) => Some(OpenCodeNativePromptState::from_active(
                                        statuses.is_active(&session_id),
                                    )),
                                    Err(_) => None,
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                admission_observation = false;
                                None
                            }
                        };
                        if native_state == Some(OpenCodeNativePromptState::Active) {
                            let observed = match session.persist_admission(events.deadline()).await
                            {
                                Ok(observed) => observed,
                                Err(error) => {
                                    session.finish_without_prompt(Err(error));
                                    return;
                                }
                            };
                            if observed {
                                admitted = true;
                                admission_observation = false;
                            }
                            if observed
                                && response_lost
                                && http
                                    .session_status(&routing)
                                    .await
                                    .is_ok_and(|statuses| !statuses.is_active(&session_id))
                            {
                                session.finish_idle_response_loss(events.deadline()).await;
                                return;
                            }
                        } else if native_state == Some(OpenCodeNativePromptState::Idle)
                            && admitted
                            && response_lost
                        {
                            session.finish_idle_response_loss(events.deadline()).await;
                            return;
                        }
                        None
                    }
                    OpenCodePromptActivity::StateChanged => match session.runtime_active() {
                        Ok(true) => {
                            match session.admission_observed().await {
                                Ok(true) => {
                                    admitted = true;
                                    admission_observation = false;
                                }
                                Ok(false) => {}
                                Err(error) => {
                                    session.finish_without_prompt(Err(error));
                                    return;
                                }
                            }
                            None
                        }
                        Ok(false) => {
                            session.finish_without_prompt(Err(SdkError::closed(
                                "prompt generation runtime was invalidated",
                            )));
                            return;
                        }
                        Err(error) => {
                            session.finish_without_prompt(Err(error));
                            return;
                        }
                    },
                    OpenCodePromptActivity::StateClosed => {
                        session.finish_without_prompt(Err(SdkError::closed(
                            "prompt state owner closed unexpectedly",
                        )));
                        return;
                    }
                    OpenCodePromptActivity::CallerClosed => {
                        session.lose_waiter();
                        if session
                            .persist_waiter_loss(events.deadline())
                            .await
                            .is_err()
                        {
                            return;
                        }
                        None
                    }
                };
                let Some(result) = native_result else {
                    continue;
                };
                response = None;
                match result {
                    Ok(response) => {
                        response_message_id = Some(response.message_id().to_owned());
                        let result = async {
                            let parent_id = response.parent_id(&session_id)?.to_owned();
                            let response =
                                response.into_prompt_response(&session_id, requires_structured)?;
                            let response = Self::validate_structured_response(
                                structured_schema.as_ref(),
                                response,
                            )?;
                            Self::verify_foreground_parent(
                                OpenCodeForegroundParent {
                                    generation: &owner_generation,
                                    http: &http,
                                    events: &events,
                                    routing: &routing,
                                    session_id: &session_id,
                                    message_id: &parent_id,
                                    part_id: &prompt_part_id,
                                    structured: requires_structured,
                                    deadline,
                                },
                                &mut changes,
                                &mut session,
                            )
                            .await?;
                            Ok(response)
                        }
                        .await;
                        let persisted = session
                            .persist_terminal(
                                &result,
                                cancellation_claim_without_dispatch,
                                events.deadline(),
                            )
                            .await;
                        match persisted {
                            Ok(()) => terminal = Some(result),
                            Err(error) => {
                                session.finish_without_prompt(result.and(Err(error)));
                                return;
                            }
                        }
                    }
                    Err(error) if error.is_ambiguous() => {
                        let result = Err(error.into_sdk_error());
                        match session
                            .persist_response_loss(&result, events.deadline())
                            .await
                        {
                            Ok(()) => {
                                if !session
                                    .finish_response(
                                        result,
                                        OpenCodePromptDelivery::ResponseLost,
                                        events.deadline(),
                                    )
                                    .await
                                {
                                    return;
                                }
                                response_lost = true;
                                match session.admission_observed().await {
                                    Ok(true) => {
                                        admitted = true;
                                        admission_observation = false;
                                    }
                                    Ok(false) => {}
                                    Err(state_error) => {
                                        session.finish_without_prompt(Err(state_error));
                                        return;
                                    }
                                }
                            }
                            Err(state_error) => {
                                session.finish_without_prompt(result.and(Err(state_error)));
                                return;
                            }
                        }
                    }
                    Err(error) => {
                        let result = Err(error.into_sdk_error());
                        let persisted = session
                            .persist_terminal(
                                &result,
                                cancellation_claim_without_dispatch,
                                events.deadline(),
                            )
                            .await;
                        match persisted {
                            Ok(()) => terminal = Some(result),
                            Err(error) => {
                                session.finish_without_prompt(result.and(Err(error)));
                                return;
                            }
                        }
                    }
                }
            }
        });
        let deadline_generation = generation.clone();
        let deadline_events = self.events.clone();
        let task = async move {
            let mut task = task;
            if let (Some(deadline), Some(output)) = (deadline, output) {
                tokio::select! {
                    _ = &mut task => return,
                    _ = tokio::time::sleep_until(deadline) => {
                        output.finish(Err(SdkError::timeout("caller prompt deadline elapsed")));
                        // The foreground may own a transaction needed by this timeout intent.
                        // Poll both in the same retained task, and never cancel an admitted
                        // native commit merely because the observer deadline has elapsed.
                        let intent_deadline = deadline_events.deadline();
                        let intent = async {
                            loop {
                                if tokio::time::Instant::now() >= intent_deadline {
                                    return;
                                }
                                match deadline_generation.mark_timed_out().await {
                                    Ok(_) => return,
                                    Err(error) if OpenCodeStateStore::<S>::retryable_state_error(&error) => {
                                        tokio::time::sleep_until(intent_deadline.min(tokio::time::Instant::now() + PROMPT_STATE_RETRY_INTERVAL)).await;
                                    }
                                    // A rejected durable intent cannot authorize native abort.
                                    // Keep the original, request-timeout-bounded HTTP owner
                                    // until its real result or reconciliation handoff.
                                    Err(_) => return,
                                }
                            }
                        };
                        tokio::join!(&mut task, intent);
                        return;
                    }
                }
            }
            task.await;
        };
        if let Err(task) = generation.spawn_task(task) {
            drop(task);
            return Err(SdkError::closed("prompt task admission is closed"));
        }
        Ok(generation)
    }

    pub(super) async fn await_prompt(
        &self,
        generation: OpenCodePromptGeneration<S>,
        receiver: oneshot::Receiver<oneshot::Receiver<Result<PromptResponse, SdkError>>>,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<PromptResponse, SdkError> {
        let mut receiver = receiver;
        match deadline {
            Some(deadline) => tokio::select! {
                biased;
                received = &mut receiver => Self::complete_prompt_result(
                    received.map_err(|_| SdkError::integration("prompt waiter was lost").with_detail(ErrorDetail::TaskTerminated))?,
                ).await,
                _ = tokio::time::sleep_until(deadline) => {
                    if let Ok(completed) = receiver.try_recv() {
                        return Self::complete_prompt_result(completed).await;
                    }
                    match generation.mark_timed_out().await {
                        Ok(Some(false)) => Self::receive_prompt_result(&mut receiver).await,
                        Ok(Some(true)) => Err(SdkError::timeout(
                            "caller prompt deadline elapsed",
                        )),
                        Ok(None) => match receiver.try_recv() {
                            Ok(completed) => Self::complete_prompt_result(completed).await,
                            Err(_) => Err(SdkError::timeout(
                                "caller prompt deadline elapsed",
                            )),
                        },
                        Err(error) => match receiver.try_recv() {
                            Ok(completed) => Self::complete_prompt_result(completed).await,
                            Err(_) => Err(error),
                        },
                    }
                }
            },
            None => Self::receive_prompt_result(&mut receiver).await,
        }
    }

    async fn receive_prompt_result(
        receiver: &mut oneshot::Receiver<oneshot::Receiver<Result<PromptResponse, SdkError>>>,
    ) -> Result<PromptResponse, SdkError> {
        let completed = receiver.await.map_err(|_| {
            SdkError::integration("prompt waiter was lost").with_detail(ErrorDetail::TaskTerminated)
        })?;
        Self::complete_prompt_result(completed).await
    }

    async fn complete_prompt_result(
        completed: oneshot::Receiver<Result<PromptResponse, SdkError>>,
    ) -> Result<PromptResponse, SdkError> {
        completed.await.map_err(|_| {
            SdkError::integration("prompt delivery was lost")
                .with_detail(ErrorDetail::TaskTerminated)
        })?
    }

    async fn abort_complete_before(
        &self,
        generation: &OpenCodePromptGeneration<S>,
        deadline: tokio::time::Instant,
    ) -> Result<bool, SdkError> {
        loop {
            generation.native_cleanup_failure(deadline)?;
            match generation.abort_complete().await {
                Ok(Some(OpenCodeAbortProgress::Settled(OpenCodeAbortOutcome::Complete))) => {
                    return Ok(true);
                }
                Ok(Some(OpenCodeAbortProgress::Settled(
                    OpenCodeAbortOutcome::RepairedInvalidState,
                ))) => {
                    return Err(SdkError::invalid_state(
                        "runtime work ended without a matching terminal response",
                    ));
                }
                Ok(
                    Some(
                        OpenCodeAbortProgress::Pending
                        | OpenCodeAbortProgress::Deletion { .. }
                        | OpenCodeAbortProgress::Settling(_),
                    )
                    | None,
                ) => return Ok(false),
                Err(error)
                    if OpenCodeStateStore::<S>::retryable_state_error(&error)
                        && tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep_until(
                        deadline.min(tokio::time::Instant::now() + PROMPT_STATE_RETRY_INTERVAL),
                    )
                    .await;
                }
                Err(error) if OpenCodeStateStore::<S>::retryable_state_error(&error) => {
                    return Err(SdkError::timeout("prompt cancellation deadline elapsed")
                        .at_phase(ErrorPhase::Reconciliation)
                        .with_supplementary(error));
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn exact_completion_or_error(
        &self,
        generation: &OpenCodePromptGeneration<S>,
        error: SdkError,
    ) -> Result<(), SdkError> {
        match generation.abort_complete().await {
            Ok(Some(OpenCodeAbortProgress::Settled(OpenCodeAbortOutcome::Complete))) => Ok(()),
            Ok(Some(OpenCodeAbortProgress::Settled(
                OpenCodeAbortOutcome::RepairedInvalidState,
            ))) => Err(SdkError::invalid_state(
                "runtime work ended without a matching terminal response",
            )),
            Ok(
                Some(
                    OpenCodeAbortProgress::Pending
                    | OpenCodeAbortProgress::Deletion { .. }
                    | OpenCodeAbortProgress::Settling(_),
                )
                | None,
            )
            | Err(_) => Err(error),
        }
    }

    async fn claim_recovered_abort_before(
        &self,
        generation: &OpenCodePromptGeneration<S>,
        owner: &OpenCodeCancellationOwner,
        deadline: tokio::time::Instant,
    ) -> Result<bool, SdkError> {
        let mut last_failure = None;
        loop {
            if tokio::time::Instant::now() >= deadline {
                let timeout = SdkError::timeout("prompt cancellation deadline elapsed")
                    .at_phase(ErrorPhase::Reconciliation);
                return Err(match last_failure {
                    Some(error) => timeout.with_supplementary(error),
                    None => timeout,
                });
            }
            match generation.claim_recovered_abort(owner, deadline).await {
                Ok(claimed) => return Ok(claimed),
                Err(error) if OpenCodePersistencePolicy::Generation.retryable::<S>(&error) => {
                    last_failure = Some(error);
                    tokio::time::sleep_until(
                        deadline.min(tokio::time::Instant::now() + PROMPT_STATE_RETRY_INTERVAL),
                    )
                    .await;
                }
                Err(error)
                    if last_failure.is_some()
                        && tokio::time::Instant::now() >= deadline
                        && error.kind() == ErrorKind::Timeout
                        && error.context().phase == ErrorPhase::Settlement
                        && error.detail()
                            == Some(&ErrorDetail::DeadlineElapsed {
                                kind: DeadlineKind::Operation,
                                timeout: None,
                            })
                        && error.cause().is_none()
                        && error.http_status().is_none()
                        && error.supplementary().is_empty()
                        && !error.diagnostics_truncated() =>
                {
                    // The same deadline crossed at the mutation's admission
                    // check. Keep the last storage attempt for the loop's timer.
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn release_recovered_abort_claim_before(
        &self,
        generation: &OpenCodePromptGeneration<S>,
        owner: &OpenCodeCancellationOwner,
        deadline: tokio::time::Instant,
    ) -> Result<(), SdkError> {
        loop {
            match generation.release_abort_claim(owner).await {
                Ok(true) => return Ok(()),
                Ok(false) => {
                    return Err(SdkError::invalid_state(
                        "prompt cancellation claim changed before dispatch",
                    ));
                }
                Err(error)
                    if OpenCodeStateStore::<S>::retryable_state_error(&error)
                        && tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep_until(
                        deadline.min(tokio::time::Instant::now() + PROMPT_STATE_RETRY_INTERVAL),
                    )
                    .await;
                }
                Err(error) if OpenCodeStateStore::<S>::retryable_state_error(&error) => {
                    return Err(SdkError::timeout("prompt cancellation deadline elapsed")
                        .at_phase(ErrorPhase::Reconciliation)
                        .with_supplementary(error));
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn begin_recovered_abort_dispatch_before(
        &self,
        generation: &OpenCodePromptGeneration<S>,
        owner: &OpenCodeCancellationOwner,
        deadline: tokio::time::Instant,
    ) -> Result<bool, SdkError> {
        let mut last_failure = None;
        loop {
            if tokio::time::Instant::now() >= deadline {
                let timeout = SdkError::timeout("prompt cancellation deadline elapsed")
                    .at_phase(ErrorPhase::Reconciliation);
                return Err(match last_failure {
                    Some(error) => timeout.with_supplementary(error),
                    None => timeout,
                });
            }
            match generation.begin_abort_dispatch(owner, deadline).await {
                Ok(ready) => return Ok(ready),
                Err(error) if OpenCodePersistencePolicy::Generation.retryable::<S>(&error) => {
                    last_failure = Some(error);
                    tokio::time::sleep_until(
                        deadline.min(tokio::time::Instant::now() + PROMPT_STATE_RETRY_INTERVAL),
                    )
                    .await;
                }
                Err(error)
                    if last_failure.is_some()
                        && tokio::time::Instant::now() >= deadline
                        && error.kind() == ErrorKind::Timeout
                        && error.context().phase == ErrorPhase::Settlement
                        && error.detail()
                            == Some(&ErrorDetail::DeadlineElapsed {
                                kind: DeadlineKind::Operation,
                                timeout: None,
                            })
                        && error.cause().is_none()
                        && error.http_status().is_none()
                        && error.supplementary().is_empty()
                        && !error.diagnostics_truncated() =>
                {
                    // The same deadline crossed at the mutation's admission
                    // check. Keep the last storage attempt for the loop's timer.
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn dispatch_recovered_abort(
        &self,
        routing: &OpenCodeRouting,
        session_id: &str,
        generation: &OpenCodePromptGeneration<S>,
        owner: &OpenCodeCancellationOwner,
        deadline: tokio::time::Instant,
        policy: OpenCodePersistencePolicy,
    ) -> Result<(), SdkError> {
        let http = self.http.clone();
        let routing = routing.clone();
        let session_id = session_id.to_owned();
        let task_generation = generation.clone();
        let owner = owner.clone();
        let (sender, receiver) = oneshot::channel();
        // The observer can expire before the owned task sends its final result.
        // Keep only its latest normalized persistence failure across that boundary.
        let (failure_sender, failure_receiver) = tokio::sync::watch::channel(None);
        let task = async move {
            let outcome = match http.abort(&routing, &session_id).await {
                Ok(()) => OpenCodeCancellation::accepted(&owner),
                Err(error) if error.is_ambiguous() => OpenCodeCancellation::ambiguous(&owner),
                Err(_) => OpenCodeCancellation::rejected(&owner),
            };
            let result = loop {
                if tokio::time::Instant::now() >= deadline {
                    let timeout = SdkError::timeout("cancellation persistence deadline elapsed")
                        .at_phase(ErrorPhase::Settlement);
                    break Err(match failure_sender.borrow().clone() {
                        Some(error) => timeout.with_supplementary(error),
                        None => timeout,
                    });
                }
                // The original native outcome and any admitted commit remain owned
                // here even if the borrowed caller stops observing this task.
                match task_generation
                    .persist_abort(outcome.clone(), deadline)
                    .await
                {
                    Ok(()) => break Ok(()),
                    Err(error)
                        if failure_sender.borrow().is_some()
                            && tokio::time::Instant::now() >= deadline
                            && error.kind() == ErrorKind::Timeout
                            && error.context().phase == ErrorPhase::Settlement
                            && error.detail()
                                == Some(&ErrorDetail::DeadlineElapsed {
                                    kind: DeadlineKind::Operation,
                                    timeout: None,
                                })
                            && error.cause().is_none()
                            && error.http_status().is_none()
                            && error.supplementary().is_empty()
                            && !error.diagnostics_truncated() =>
                    {
                        // The mutation's admission check crossed the same deadline;
                        // it must not overwrite the last actual storage failure.
                    }
                    Err(error) => {
                        failure_sender.send_replace(Some(error.clone()));
                        if policy.retryable::<S>(&error) {
                            tokio::time::sleep_until(
                                deadline
                                    .min(tokio::time::Instant::now() + PROMPT_STATE_RETRY_INTERVAL),
                            )
                            .await;
                        } else {
                            break Err(error);
                        }
                    }
                }
            };
            let _ = sender.send(result);
        };
        generation
            .spawn_task(task)
            .map_err(|_| SdkError::closed("prompt task admission is sealed"))?;
        match tokio::time::timeout_at(deadline, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(SdkError::integration(
                "cancellation dispatch owner was lost",
            )),
            Err(_) => {
                let timeout = SdkError::timeout("prompt cancellation deadline elapsed")
                    .at_phase(ErrorPhase::Reconciliation);
                Err(match failure_receiver.borrow().clone() {
                    Some(error) => timeout.with_supplementary(error),
                    None => timeout,
                })
            }
        }
    }

    pub(crate) async fn abort(&self, deadline: tokio::time::Instant) -> Result<(), SdkError> {
        self.abort_with_outcome_policy(deadline, OpenCodePersistencePolicy::Generation)
            .await
    }

    async fn abort_with_outcome_policy(
        &self,
        deadline: tokio::time::Instant,
        policy: OpenCodePersistencePolicy,
    ) -> Result<(), SdkError> {
        let mut changes = self.state.prompt_changes();
        let mut status_signals = self.state.prompt_statuses();
        let join = match self
            .state
            .request_abort_before(self.binding.conversation_id(), deadline)
            .await?
        {
            OpenCodeAbort::Idle => return Ok(()),
            OpenCodeAbort::Join(join) => join,
        };
        let generation = join.generation().clone();
        let _join = join;
        let recovery_owner = OpenCodeCancellationOwner::fresh();
        let routing = OpenCodeRouting::for_directory(self.binding.directory().to_path_buf())?;
        let session_id = self.binding.require_session_id()?;
        let mut recovery = None;
        let mut claimed_without_send = false;
        let mut native_state = None;
        let mut observe_status = true;
        let mut status_open = true;

        loop {
            if self.abort_complete_before(&generation, deadline).await? {
                return Ok(());
            }
            if recovery.is_none() {
                recovery = generation.claim_recovery(&recovery_owner)?;
                if recovery.is_some() {
                    let _ = changes.borrow_and_update();
                    observe_status = true;
                }
            }
            if recovery.is_none() {
                tokio::select! {
                    changed = changes.changed() => {
                        if changed.is_err() {
                            return Err(SdkError::closed(
                                "prompt state owner closed during cancellation",
                            ));
                        }
                    }
                    _ = tokio::time::sleep_until(deadline) => {
                        if self.abort_complete_before(&generation, deadline).await? {
                            return Ok(());
                        }
                        return Err(SdkError::timeout(
                            "prompt cancellation deadline elapsed",
                        ));
                    }
                }
                continue;
            }

            if generation.cancel_prepared(deadline).await? {
                return Ok(());
            }

            if let Some(recovery) = &recovery {
                recovery
                    .prepare_interaction_cleanup(native_state, deadline)
                    .await?;
            }
            if generation.requires_native_cleanup().await? {
                self.events
                    .synchronize(generation.conversation_id(), deadline)
                    .await?;
                continue;
            }

            let mut last_failure = None;
            let progress = loop {
                if tokio::time::Instant::now() >= deadline {
                    let timeout =
                        SdkError::timeout("prompt cancellation reconciliation deadline elapsed")
                            .at_phase(ErrorPhase::Reconciliation);
                    return Err(match last_failure {
                        Some(error) => timeout.with_supplementary(error),
                        None => timeout,
                    });
                }
                match generation.reconcile_abort(native_state, deadline).await {
                    Err(error) if OpenCodePersistencePolicy::Generation.retryable::<S>(&error) => {
                        last_failure = Some(error);
                        tokio::time::sleep_until(
                            deadline.min(tokio::time::Instant::now() + PROMPT_STATE_RETRY_INTERVAL),
                        )
                        .await;
                    }
                    Err(error)
                        if last_failure.is_some()
                            && tokio::time::Instant::now() >= deadline
                            && error.kind() == crate::error::ErrorKind::Timeout
                            && error.context().phase == ErrorPhase::Reconciliation
                            && error.detail()
                                == Some(&ErrorDetail::DeadlineElapsed {
                                    kind: crate::error::DeadlineKind::Operation,
                                    timeout: None,
                                })
                            && error.supplementary().is_empty()
                            && error.cause().is_none()
                            && error.http_status().is_none()
                            && !error.diagnostics_truncated() =>
                    {
                        return Err(error.with_supplementary(last_failure.unwrap()));
                    }
                    result => break result?,
                }
            };
            match progress {
                OpenCodeAbortProgress::Settled(OpenCodeAbortOutcome::Complete) => return Ok(()),
                OpenCodeAbortProgress::Settled(OpenCodeAbortOutcome::RepairedInvalidState) => {
                    return Err(SdkError::invalid_state(
                        "runtime work ended without a matching terminal response",
                    ));
                }
                OpenCodeAbortProgress::Pending
                | OpenCodeAbortProgress::Deletion { .. }
                | OpenCodeAbortProgress::Settling(_) => {}
            }

            let recovery_pending = match generation.abort_recovery_pending().await {
                Ok(pending) => pending,
                Err(error)
                    if OpenCodeStateStore::<S>::retryable_state_error(&error)
                        && tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep_until(
                        deadline.min(tokio::time::Instant::now() + PROMPT_STATE_RETRY_INTERVAL),
                    )
                    .await;
                    continue;
                }
                Err(error) if OpenCodeStateStore::<S>::retryable_state_error(&error) => {
                    if self.abort_complete_before(&generation, deadline).await? {
                        return Ok(());
                    }
                    return Err(SdkError::timeout("prompt cancellation deadline elapsed"));
                }
                Err(error) => return Err(error),
            };
            let recovery_ready = match generation.abort_recovery_ready().await {
                Ok(ready) => ready,
                Err(error)
                    if OpenCodeStateStore::<S>::retryable_state_error(&error)
                        && tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep_until(
                        deadline.min(tokio::time::Instant::now() + PROMPT_STATE_RETRY_INTERVAL),
                    )
                    .await;
                    continue;
                }
                Err(error) if OpenCodeStateStore::<S>::retryable_state_error(&error) => {
                    if self.abort_complete_before(&generation, deadline).await? {
                        return Ok(());
                    }
                    return Err(SdkError::timeout("prompt cancellation deadline elapsed"));
                }
                Err(error) => return Err(error),
            };

            if observe_status && (recovery_pending || recovery_ready || claimed_without_send) {
                observe_status = false;
                match tokio::time::timeout_at(deadline, self.http.session_status(&routing)).await {
                    Ok(Ok(statuses)) if !statuses.is_active(session_id) => {
                        if claimed_without_send {
                            if let Err(error) = self
                                .release_recovered_abort_claim_before(
                                    &generation,
                                    &recovery_owner,
                                    deadline,
                                )
                                .await
                            {
                                return self.exact_completion_or_error(&generation, error).await;
                            }
                            claimed_without_send = false;
                            let _ = changes.borrow_and_update();
                        }
                        native_state = Some(OpenCodeNativePromptState::Idle);
                        continue;
                    }
                    Ok(Ok(statuses)) if statuses.is_active(session_id) && recovery_pending => {
                        native_state = Some(OpenCodeNativePromptState::Active);
                        let claimed = match self
                            .claim_recovered_abort_before(&generation, &recovery_owner, deadline)
                            .await
                        {
                            Ok(claimed) => claimed,
                            Err(error) => {
                                return self.exact_completion_or_error(&generation, error).await;
                            }
                        };
                        if claimed {
                            let _ = changes.borrow_and_update();
                            match tokio::time::timeout_at(
                                deadline,
                                self.http.session_status(&routing),
                            )
                            .await
                            {
                                Ok(Ok(statuses)) if statuses.is_active(session_id) => {
                                    native_state = Some(OpenCodeNativePromptState::Active);
                                    match self
                                        .begin_recovered_abort_dispatch_before(
                                            &generation,
                                            &recovery_owner,
                                            deadline,
                                        )
                                        .await
                                    {
                                        Ok(true) => claimed_without_send = false,
                                        Ok(false) => {
                                            return self
                                                .exact_completion_or_error(
                                                    &generation,
                                                    SdkError::invalid_state(
                                                        "prompt cancellation reservation changed before dispatch",
                                                    ),
                                                )
                                                .await;
                                        }
                                        Err(error) => {
                                            return self
                                                .exact_completion_or_error(&generation, error)
                                                .await;
                                        }
                                    }
                                    if let Err(error) = self
                                        .dispatch_recovered_abort(
                                            &routing,
                                            session_id,
                                            &generation,
                                            &recovery_owner,
                                            deadline,
                                            policy,
                                        )
                                        .await
                                    {
                                        return self
                                            .exact_completion_or_error(&generation, error)
                                            .await;
                                    }
                                    let _ = changes.borrow_and_update();
                                    continue;
                                }
                                Ok(Ok(_)) => {
                                    if let Err(error) = self
                                        .release_recovered_abort_claim_before(
                                            &generation,
                                            &recovery_owner,
                                            deadline,
                                        )
                                        .await
                                    {
                                        return self
                                            .exact_completion_or_error(&generation, error)
                                            .await;
                                    }
                                    claimed_without_send = false;
                                    let _ = changes.borrow_and_update();
                                    native_state = Some(OpenCodeNativePromptState::Idle);
                                    continue;
                                }
                                Ok(Err(_)) | Err(_) => {
                                    if let Err(error) = self
                                        .release_recovered_abort_claim_before(
                                            &generation,
                                            &recovery_owner,
                                            deadline,
                                        )
                                        .await
                                    {
                                        return self
                                            .exact_completion_or_error(&generation, error)
                                            .await;
                                    }
                                    claimed_without_send = false;
                                    let _ = changes.borrow_and_update();
                                    observe_status = true;
                                    if tokio::time::Instant::now() >= deadline {
                                        if self.abort_complete_before(&generation, deadline).await?
                                        {
                                            return Ok(());
                                        }
                                        return Err(SdkError::timeout(
                                            "prompt cancellation deadline elapsed",
                                        ));
                                    }
                                    tokio::time::sleep_until(deadline.min(
                                        tokio::time::Instant::now() + PROMPT_STATE_RETRY_INTERVAL,
                                    ))
                                    .await;
                                    continue;
                                }
                            }
                        }
                    }
                    Ok(Ok(_)) => {}
                    Ok(Err(_)) | Err(_) => {
                        observe_status = true;
                        if tokio::time::Instant::now() < deadline {
                            tokio::time::sleep_until(
                                deadline
                                    .min(tokio::time::Instant::now() + PROMPT_STATE_RETRY_INTERVAL),
                            )
                            .await;
                            continue;
                        }
                    }
                }
            }

            tokio::select! {
                changed = changes.changed() => match changed {
                    Ok(()) => observe_status = true,
                    Err(_) => {
                        return Err(SdkError::closed(
                            "prompt state owner closed during cancellation",
                        ));
                    }
                },
                signal = status_signals.recv(), if status_open && (recovery_pending || recovery_ready || claimed_without_send) => {
                    observe_status = match signal {
                        Ok((
                            conversation_id,
                            OpenCodePromptStatusSignal::Observed(status),
                        ))
                            if &conversation_id == self.binding.conversation_id() =>
                        {
                            native_state = Some(OpenCodeNativePromptState::from_status(status));
                            true
                        }
                        Ok((conversation_id, OpenCodePromptStatusSignal::Refresh))
                            if &conversation_id == self.binding.conversation_id() =>
                        {
                            true
                        }
                        Ok(_) => false,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => true,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            status_open = false;
                            false
                        }
                    };
                },
                _ = tokio::time::sleep_until(deadline) => {
                    if self.abort_complete_before(&generation, deadline).await? {
                        return Ok(());
                    }
                    return Err(SdkError::timeout(
                        "prompt cancellation deadline elapsed",
                    ));
                }
            }
        }
    }

    async fn verify_foreground_parent(
        parent: OpenCodeForegroundParent<'_, S>,
        changes: &mut tokio::sync::watch::Receiver<u64>,
        prompt: &mut OpenCodePromptSession<S>,
    ) -> Result<(), SdkError> {
        let OpenCodeForegroundParent {
            generation,
            http,
            events,
            routing,
            session_id,
            message_id,
            part_id,
            structured,
            deadline,
        } = parent;
        if !structured {
            return http
                .message_by_id(routing, session_id, message_id)
                .await?
                .verify_prompt_parent(session_id, message_id, part_id);
        }

        let mut recovery_complete = false;
        loop {
            if generation
                .structured_parent_proof(session_id, message_id, part_id)
                .await?
            {
                return Ok(());
            }
            if recovery_complete {
                return Err(SdkError::invalid_state(
                    "structured parent remains unavailable after bounded recovery",
                ));
            }
            let claimed = generation
                .claim_structured_parent_recovery(session_id, message_id, part_id)
                .await?;
            if generation
                .structured_parent_proof(session_id, message_id, part_id)
                .await?
            {
                return Ok(());
            }
            if !claimed {
                return Err(SdkError::invalid_state(
                    "structured-parent recovery lost its exact owner",
                ));
            }

            let synchronization = events.synchronize(
                generation.conversation_id(),
                deadline.unwrap_or_else(|| events.deadline()),
            );
            tokio::pin!(synchronization);
            loop {
                tokio::select! {
                    biased;
                    changed = changes.changed() => {
                        if changed.is_err() {
                            return Err(SdkError::closed(
                                "structured-parent state owner closed unexpectedly",
                            ));
                        }
                        if !prompt.runtime_active()? {
                            return Err(SdkError::closed(
                                "structured-parent generation was invalidated",
                            ));
                        }
                    }
                    completed = &mut synchronization => {
                        completed?;
                        recovery_complete = true;
                        break;
                    }
                    _ = prompt.caller_closed(), if prompt.has_waiter() => {
                        prompt.lose_waiter();
                        prompt.persist_waiter_loss(events.deadline()).await?;
                    }
                }
            }
        }
    }

    fn validate_structured_response(
        schema: Option<&serde_json::Value>,
        response: PromptResponse,
    ) -> Result<PromptResponse, SdkError> {
        let Some(schema) = schema else {
            return Ok(response);
        };
        let value = response
            .structured_output()
            .ok_or_else(|| SdkError::invalid_state("structured response lacks its typed value"))?;
        let validator = jsonschema::draft202012::options()
            .build(schema)
            .map_err(|_| SdkError::invalid_state("structured schema could not compile"))?;
        if !validator.is_valid(value) {
            return Err(SdkError::protocol(
                "structured response does not satisfy the requested schema",
            ));
        }
        Ok(response)
    }

    pub(crate) async fn reconcile_reopen(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), SdkError> {
        let routing = OpenCodeRouting::for_directory(self.binding.directory().to_path_buf())?;
        let session_id = self.binding.require_session_id()?;
        let native = tokio::time::timeout_at(deadline, self.http.session(&routing, session_id))
            .await
            .map_err(|_| SdkError::timeout("prompt reconciliation deadline elapsed"))??
            .ok_or_else(|| SdkError::conflict("bound session does not exist"))?;
        native.verify(session_id, self.binding.directory())?;
        let active = tokio::time::timeout_at(deadline, self.http.session_status(&routing))
            .await
            .map_err(|_| SdkError::timeout("prompt reconciliation deadline elapsed"))??
            .is_active(session_id);
        let Some(_) = self
            .state
            .reconcile_reopen_prompt(self.binding.conversation_id(), active, deadline)
            .await?
        else {
            return Ok(());
        };
        self.abort_with_outcome_policy(deadline, OpenCodePersistencePolicy::Reopen)
            .await
    }
}
