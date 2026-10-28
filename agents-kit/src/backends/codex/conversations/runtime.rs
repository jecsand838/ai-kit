use std::sync::Arc;
use storage_kit::StateStoreBackend;
use tokio::sync::oneshot;
use tools_kit::ToolsetBackend;

use crate::backends::codex::adapters::prompt::CodexPrompt;
use crate::backends::codex::conversations::thread::CodexThreadBinding;
use crate::backends::codex::notifications::{CodexPromptOutput, CodexPromptTerminal};
use crate::backends::codex::protocol::requests::CodexTurnStartRequest;
use crate::backends::codex::protocol::responses::CodexServerResponse;
use crate::backends::codex::protocol::{CodexPromptTurn, CodexPromptTurnStatus, CodexProtocol};
use crate::backends::codex::server_requests::{CodexInteractionRequest, CodexServerRendering};
use crate::backends::codex::state_store::{
    CodexAbort, CodexPromptStartError, CodexStateStore, CodexTerminalDisposition,
};
use crate::backends::codex::tools::{
    CodexDynamicToolCall, CodexDynamicToolCallResponse, CodexToolGeneration, CodexTools,
};
use crate::core::hierarchy::HierarchyAdmission;
use crate::core::notification_store::ResolutionSettlement;
use crate::error::{ErrorDetail, ErrorKind, ErrorPhase, Operation};
use crate::prompt::{PromptOutput, PromptStream};
use crate::{
    ConversationId, ConversationNotificationKind, KitError, NotificationId, NotificationResolution,
    PromptResponse,
};

const PROMPT_STATE_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(25);

pub(crate) struct CodexConversationRuntime<S: StateStoreBackend, T: ToolsetBackend> {
    binding: CodexThreadBinding,
    state: CodexStateStore<S>,
    protocol: CodexProtocol,
    tools: CodexTools<S, T>,
}

struct CodexPromptSession {
    observation: CodexPromptObservation,
    admission: Option<HierarchyAdmission>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    lifecycle_timeout: std::time::Duration,
}

enum CodexPromptObservation {
    Response(Option<oneshot::Sender<oneshot::Receiver<Result<PromptResponse, KitError>>>>),
    Stream(Arc<PromptOutput>),
}

struct CodexTerminalDelivery {
    completion: Option<oneshot::Sender<Result<PromptResponse, KitError>>>,
    result: Result<PromptResponse, KitError>,
}

impl CodexTerminalDelivery {
    fn unclaimed(result: Result<PromptResponse, KitError>) -> Self {
        Self {
            completion: None,
            result,
        }
    }

    fn reached_waiter(&self) -> bool {
        self.completion.is_some()
    }

    fn fail(&mut self, error: KitError) {
        self.result = Err(match &self.result {
            Ok(_) => error,
            Err(primary) => primary
                .clone()
                .with_supplementary(error.in_phase(ErrorPhase::Cleanup)),
        });
    }

    fn finish(self, session: &mut CodexPromptSession) {
        if let Some(output) = session.output() {
            output.finish(session.publish(self.result));
        } else if let Some(completion) = self.completion {
            let _ = completion.send(session.publish(self.result));
        } else {
            session.finish_without_prompt(self.result);
        }
    }
}

impl CodexPromptSession {
    fn new(
        observation: CodexPromptObservation,
        admission: HierarchyAdmission,
        permit: tokio::sync::OwnedSemaphorePermit,
        lifecycle_timeout: std::time::Duration,
    ) -> Self {
        Self {
            observation,
            admission: Some(admission),
            permit: Some(permit),
            lifecycle_timeout,
        }
    }

    fn has_waiter(&self) -> bool {
        matches!(&self.observation, CodexPromptObservation::Response(Some(_)))
    }

    fn output(&self) -> Option<Arc<PromptOutput>> {
        match &self.observation {
            CodexPromptObservation::Response(_) => None,
            CodexPromptObservation::Stream(output) => Some(Arc::clone(output)),
        }
    }

    fn caller_is_closed(&self) -> bool {
        match &self.observation {
            CodexPromptObservation::Response(sender) => {
                sender.as_ref().is_none_or(oneshot::Sender::is_closed)
            }
            CodexPromptObservation::Stream(_) => false,
        }
    }

    async fn caller_closed(&mut self) {
        match &mut self.observation {
            CodexPromptObservation::Response(Some(sender)) => sender.closed().await,
            CodexPromptObservation::Response(None) | CodexPromptObservation::Stream(_) => {
                std::future::pending().await
            }
        }
    }

    fn send_early(&mut self, result: Result<PromptResponse, KitError>) {
        if let CodexPromptObservation::Stream(output) = &self.observation {
            output.finish(result);
            return;
        }
        let (completion, completed) = oneshot::channel();
        if let CodexPromptObservation::Response(sender) = &mut self.observation
            && let Some(sender) = sender.take()
        {
            let _ = sender.send(completed);
        }
        let _ = completion.send(result);
    }

    fn discard_waiter(&mut self) {
        if let CodexPromptObservation::Response(sender) = &mut self.observation {
            sender.take();
        }
    }

    fn begin_terminal_delivery(
        &mut self,
    ) -> Option<oneshot::Sender<Result<PromptResponse, KitError>>> {
        let sender = match &mut self.observation {
            CodexPromptObservation::Response(sender) => sender.take()?,
            CodexPromptObservation::Stream(_) => return None,
        };
        let (completion, completed) = oneshot::channel();
        sender.send(completed).ok().map(|_| completion)
    }

    fn finish_without_prompt(&mut self, result: Result<PromptResponse, KitError>) {
        let result = self.publish(result);
        self.send_early(result);
    }

    fn publish(
        &mut self,
        result: Result<PromptResponse, KitError>,
    ) -> Result<PromptResponse, KitError> {
        let result = match self.admission.take() {
            Some(admission) => admission.publish(result),
            None => result,
        };
        self.permit.take();
        result
    }

    async fn lose_waiter<S: StateStoreBackend>(
        &mut self,
        state: &CodexStateStore<S>,
        conversation_id: &ConversationId,
        client_message_id: &str,
    ) {
        self.discard_waiter();
        loop {
            match state
                .mark_prompt_waiter_lost(conversation_id, client_message_id)
                .await
            {
                Ok(()) => break,
                Err(error)
                    if matches!(error.kind(), ErrorKind::Integration | ErrorKind::Timeout) =>
                {
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                }
                Err(_) => {
                    state.invalidate_runtime();
                    break;
                }
            }
        }
    }

    fn release(&mut self) {
        if let Some(admission) = self.admission.take() {
            let _ = admission.publish(Ok(()));
        }
        self.permit.take();
    }

    async fn finish_terminal<S: StateStoreBackend>(
        &mut self,
        state: &CodexStateStore<S>,
        conversation_id: &ConversationId,
        client_message_id: &str,
        turn_id: &str,
        result: Result<PromptResponse, KitError>,
    ) {
        let deadline = tokio::time::Instant::now() + self.lifecycle_timeout;
        let output = self.output();
        let primary = result.as_ref().err().map(CodexProtocol::caller_safe_error);
        let mut delivery = CodexTerminalDelivery::unclaimed(result);
        let cleanup = async {
            let claimed = loop {
                if tokio::time::Instant::now() >= deadline {
                    return Err(KitError::timeout(
                        "prompt terminal cleanup deadline elapsed",
                    ));
                }
                match state
                    .claim_prompt_delivery(conversation_id, client_message_id)
                    .await
                {
                    Ok(claimed) => break claimed,
                    Err(error)
                        if matches!(error.kind(), ErrorKind::Integration | ErrorKind::Timeout) =>
                    {
                        tokio::time::sleep(PROMPT_STATE_RETRY_INTERVAL).await;
                    }
                    Err(error) => return Err(error),
                }
            };
            if claimed {
                delivery.completion = self.begin_terminal_delivery();
                let delivered = delivery.reached_waiter();
                loop {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(KitError::timeout(
                            "prompt terminal cleanup deadline elapsed",
                        ));
                    }
                    match state
                        .settle_prompt_delivery(conversation_id, client_message_id, delivered)
                        .await
                    {
                        Ok(()) => break,
                        Err(error)
                            if matches!(
                                error.kind(),
                                ErrorKind::Integration | ErrorKind::Timeout
                            ) =>
                        {
                            tokio::time::sleep(PROMPT_STATE_RETRY_INTERVAL).await;
                        }
                        Err(error) => return Err(error),
                    }
                }
            } else {
                self.discard_waiter();
            }
            loop {
                if tokio::time::Instant::now() >= deadline {
                    return Err(KitError::timeout(
                        "prompt terminal cleanup deadline elapsed",
                    ));
                }
                match state.clear_prompt(conversation_id, turn_id).await {
                    Ok(()) => return Ok(()),
                    Err(error)
                        if matches!(error.kind(), ErrorKind::Integration | ErrorKind::Timeout) =>
                    {
                        tokio::time::sleep(PROMPT_STATE_RETRY_INTERVAL).await;
                    }
                    Err(error) => return Err(error),
                }
            }
        };
        let result = {
            tokio::pin!(cleanup);
            tokio::select! {
                biased;
                result = &mut cleanup => result,
                _ = tokio::time::sleep_until(deadline) => {
                    if let Some(output) = output {
                        let timeout = KitError::timeout("prompt terminal cleanup deadline elapsed").in_phase(ErrorPhase::Cleanup);
                        output.finish(Err(match primary { Some(primary) => primary.with_supplementary(timeout), None => timeout }));
                    }
                    cleanup.await
                }
            }
        };
        if let Err(error) = result {
            state.invalidate_runtime();
            delivery.fail(error);
        }
        delivery.finish(self);
    }
}

impl<S: StateStoreBackend, T: ToolsetBackend> CodexConversationRuntime<S, T> {
    pub(crate) fn new(
        binding: CodexThreadBinding,
        state: CodexStateStore<S>,
        protocol: CodexProtocol,
        tools: CodexTools<S, T>,
    ) -> Self {
        Self {
            binding,
            state,
            protocol,
            tools,
        }
    }

    pub(crate) fn conversation_id(&self) -> &ConversationId {
        self.binding.conversation_id()
    }

    pub(crate) fn binding(&self) -> &CodexThreadBinding {
        &self.binding
    }

    pub(crate) fn state(&self) -> &CodexStateStore<S> {
        &self.state
    }

    pub(crate) fn protocol(&self) -> &CodexProtocol {
        &self.protocol
    }

    async fn recovered_terminal_result(
        state: &CodexStateStore<S>,
        conversation_id: &ConversationId,
        status: CodexPromptTurnStatus,
        output: Option<String>,
        error: Option<KitError>,
    ) -> Result<PromptResponse, KitError> {
        match status {
            CodexPromptTurnStatus::Completed => {
                state
                    .recover_completed_prompt(conversation_id, output)
                    .await
            }
            CodexPromptTurnStatus::Interrupted => {
                let cancelled = KitError::cancelled("foreground prompt was interrupted")
                    .with_context(Operation::Prompt, ErrorPhase::Cancellation);
                Err(match error {
                    Some(cause) => cancelled.with_cause(cause),
                    None => cancelled,
                })
            }
            CodexPromptTurnStatus::Failed => Err(error.unwrap_or_else(|| {
                KitError::protocol("failed prompt lacks authoritative failure data")
                    .with_context(Operation::Prompt, ErrorPhase::ResponseDecode)
                    .with_detail(ErrorDetail::MissingData)
            })),
            CodexPromptTurnStatus::InProgress => Err(KitError::invalid_state(
                "active prompt was classified as terminal",
            )),
        }
    }

    async fn persist_observed_turn(
        state: &CodexStateStore<S>,
        binding: &CodexThreadBinding,
        client_message_id: &str,
        observed: CodexPromptTurn,
    ) -> Result<bool, KitError> {
        match observed {
            CodexPromptTurn::Absent => Err(KitError::invalid_state(
                "prompt reconciliation did not observe a runtime prompt execution",
            )),
            CodexPromptTurn::Active { turn_id } => {
                state
                    .accept_prompt(binding.conversation_id(), client_message_id, &turn_id)
                    .await?;
                Ok(false)
            }
            CodexPromptTurn::Terminal {
                turn_id,
                status,
                output,
                error,
            } => {
                state
                    .accept_prompt(binding.conversation_id(), client_message_id, &turn_id)
                    .await?;
                if state
                    .prompt_terminal(binding.conversation_id(), client_message_id)
                    .await?
                    .is_none()
                {
                    let recovered = Self::recovered_terminal_result(
                        state,
                        binding.conversation_id(),
                        status,
                        output,
                        error,
                    )
                    .await;
                    if let Err(error) = state
                        .terminalize_prompt(binding.conversation_id(), &turn_id, &recovered)
                        .await
                        && state
                            .prompt_terminal(binding.conversation_id(), client_message_id)
                            .await?
                            .is_none()
                    {
                        return Err(error);
                    }
                }
                Ok(true)
            }
        }
    }

    async fn complete_prompt_result(
        completed: oneshot::Receiver<Result<PromptResponse, KitError>>,
    ) -> Result<PromptResponse, KitError> {
        completed
            .await
            .map_err(|_| KitError::integration("prompt cleanup ended before result publication"))?
    }

    async fn receive_prompt_result(
        receiver: &mut oneshot::Receiver<oneshot::Receiver<Result<PromptResponse, KitError>>>,
    ) -> Result<PromptResponse, KitError> {
        let received = receiver
            .await
            .map_err(|_| KitError::integration("prompt waiter was lost"))?;
        Self::complete_prompt_result(received).await
    }

    async fn remove_unaccepted_prompt(
        state: &CodexStateStore<S>,
        conversation_id: &ConversationId,
        client_message_id: &str,
    ) -> Result<(), KitError> {
        loop {
            match state
                .reject_unaccepted_prompt(conversation_id, client_message_id)
                .await
            {
                Ok(()) => return Ok(()),
                Err(error)
                    if matches!(error.kind(), ErrorKind::Integration | ErrorKind::Timeout) =>
                {
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                }
                Err(error) => {
                    state.invalidate_runtime();
                    return Err(error);
                }
            }
        }
    }

    pub(crate) async fn execute_tool(
        &self,
        call: CodexDynamicToolCall,
    ) -> Result<CodexDynamicToolCallResponse, KitError> {
        self.tools
            .execute(
                self.binding.workspace_id(),
                self.binding.conversation_id(),
                call,
            )
            .await
    }

    pub(crate) async fn cancel_tool_interactions(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), KitError> {
        self.tools
            .cancel_conversation(self.binding.conversation_id(), deadline)
            .await
    }

    pub(crate) async fn prompt(
        &self,
        prompt: CodexPrompt,
        admission: HierarchyAdmission,
        deadline: Option<tokio::time::Instant>,
        lifecycle_timeout: std::time::Duration,
    ) -> Result<PromptResponse, KitError> {
        let (sender, receiver) = oneshot::channel();
        let client_message_id = self.start_prompt(
            prompt,
            admission,
            deadline,
            lifecycle_timeout,
            CodexPromptObservation::Response(Some(sender)),
        )?;
        self.await_prompt(receiver, &client_message_id, deadline)
            .await
    }

    pub(crate) async fn prompt_stream(
        &self,
        prompt: CodexPrompt,
        admission: HierarchyAdmission,
        deadline: Option<tokio::time::Instant>,
        lifecycle_timeout: std::time::Duration,
    ) -> Result<PromptStream, KitError> {
        let (output, stream) = PromptOutput::gated_channel();
        self.start_prompt(
            prompt,
            admission,
            deadline,
            lifecycle_timeout,
            CodexPromptObservation::Stream(output),
        )?;
        Ok(stream)
    }

    fn start_prompt(
        &self,
        prompt: CodexPrompt,
        admission: HierarchyAdmission,
        deadline: Option<tokio::time::Instant>,
        lifecycle_timeout: std::time::Duration,
        observation: CodexPromptObservation,
    ) -> Result<String, KitError> {
        if deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
            return admission.publish(Err(KitError::timeout(
                "prompt deadline elapsed before dispatch",
            )));
        }
        let request = match CodexTurnStartRequest::from_prompt(&prompt) {
            Ok(request) => request,
            Err(error) => return admission.publish(Err(error)),
        };
        let permit = match self.state.prompt_permit() {
            Ok(permit) => permit,
            Err(error) => return admission.publish(Err(error)),
        };
        let client_message_id = prompt.client_message_id().to_owned();
        let structured_output = prompt.structured_output().cloned();
        let selection = prompt.selection().clone();
        let prompt_timeout = prompt.timeout();
        let binding = self.binding.clone();
        let state = self.state.clone();
        let protocol = self.protocol.clone();
        let owner_client_message_id = client_message_id.clone();
        let lifecycle_deadline = tokio::time::Instant::now() + lifecycle_timeout;
        let prompt_start_deadline = deadline.map_or(lifecycle_deadline, |deadline| {
            deadline.min(lifecycle_deadline)
        });

        let session = CodexPromptSession::new(observation, admission, permit, lifecycle_timeout);
        let output = session.output();
        let task = async move {
            let client_message_id = owner_client_message_id;
            let mut session = session;
            let mut changes = state.prompt_changes();
            let mut watch_open = true;
            let create_error = loop {
                match state
                    .create_prompt(
                        &binding,
                        client_message_id.clone(),
                        structured_output.clone(),
                        &selection,
                        prompt_timeout,
                    )
                    .await
                {
                    Ok(()) => break None,
                    Err(CodexPromptStartError::TransactionConflict) => {
                        let now = tokio::time::Instant::now();
                        if now >= prompt_start_deadline {
                            let error = if deadline.is_some_and(|deadline| deadline <= now) {
                                KitError::timeout("prompt admission deadline elapsed")
                            } else {
                                KitError::conflict("prompt admission transaction conflicted")
                            };
                            session.finish_without_prompt(Err(error));
                            return;
                        }
                        if session.caller_is_closed() {
                            session.discard_waiter();
                            session.release();
                            return;
                        }
                        tokio::time::sleep_until(
                            prompt_start_deadline.min(now + PROMPT_STATE_RETRY_INTERVAL),
                        )
                        .await;
                    }
                    Err(CodexPromptStartError::Failure(error)) => break Some(error),
                }
            };
            if let Some(error) = create_error {
                if !matches!(error.kind(), ErrorKind::Integration | ErrorKind::Timeout) {
                    session.finish_without_prompt(Err(error));
                    return;
                }
                let mut error = Some(error);
                loop {
                    match state.prompt_record(binding.conversation_id()).await {
                        Ok(Some(record))
                            if record.client_message_id() == client_message_id.as_str() =>
                        {
                            break;
                        }
                        Ok(_) => {
                            let result = match error.take() {
                                Some(error) => Err(error),
                                None => Err(KitError::invalid_state(
                                    "prompt creation lost its original failure",
                                )),
                            };
                            session.finish_without_prompt(result);
                            return;
                        }
                        Err(error)
                            if matches!(
                                error.kind(),
                                ErrorKind::Integration | ErrorKind::Timeout
                            ) =>
                        {
                            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                        }
                        Err(error) => {
                            state.invalidate_runtime();
                            session.finish_without_prompt(Err(error));
                            return;
                        }
                    }
                }
            }
            if let Err(error) = state.retain_prompt_context(
                binding.conversation_id(),
                &client_message_id,
                structured_output,
                session.output(),
            ) {
                state.invalidate_runtime();
                session.finish_without_prompt(Err(error));
                return;
            }
            let deadline_elapsed =
                deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline);
            if deadline_elapsed || session.caller_is_closed() {
                let cleanup = Self::remove_unaccepted_prompt(
                    &state,
                    binding.conversation_id(),
                    &client_message_id,
                )
                .await;
                if session.caller_is_closed() {
                    session.discard_waiter();
                    session.release();
                } else {
                    let primary = KitError::timeout("prompt deadline elapsed before dispatch")
                        .in_phase(ErrorPhase::Admission);
                    let result = Err(match cleanup {
                        Ok(()) => primary,
                        Err(error) => {
                            primary.with_supplementary(error.in_phase(ErrorPhase::Cleanup))
                        }
                    });
                    session.finish_without_prompt(result);
                }
                return;
            }
            if let Err(error) = state
                .mark_prompt_may_be_accepted(binding.conversation_id(), &client_message_id)
                .await
            {
                let error = match Self::remove_unaccepted_prompt(
                    &state,
                    binding.conversation_id(),
                    &client_message_id,
                )
                .await
                {
                    Ok(()) => error,
                    Err(cleanup_error) => {
                        error.with_supplementary(cleanup_error.in_phase(ErrorPhase::Cleanup))
                    }
                };
                session.finish_without_prompt(Err(error));
                return;
            }
            let deadline_elapsed =
                deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline);
            if deadline_elapsed || session.caller_is_closed() {
                let cleanup = Self::remove_unaccepted_prompt(
                    &state,
                    binding.conversation_id(),
                    &client_message_id,
                )
                .await;
                if session.caller_is_closed() {
                    session.discard_waiter();
                    session.release();
                } else {
                    let primary = KitError::timeout("prompt deadline elapsed before dispatch")
                        .in_phase(ErrorPhase::Admission);
                    let result = Err(match cleanup {
                        Ok(()) => primary,
                        Err(error) => {
                            primary.with_supplementary(error.in_phase(ErrorPhase::Cleanup))
                        }
                    });
                    session.finish_without_prompt(result);
                }
                return;
            }

            let mut acceptance_unknown = false;
            let mut native_reconciliation = false;
            let mut accepted_turn_id = None;
            let mut pending_error = None;
            let mut next_native_reconciliation =
                tokio::time::Instant::now() + std::time::Duration::from_millis(25);
            match protocol.start_turn(request).await {
                Ok(start) => {
                    accepted_turn_id = Some(start.turn_id().to_owned());
                    if let Err(error) = state
                        .accept_prompt(
                            binding.conversation_id(),
                            &client_message_id,
                            start.turn_id(),
                        )
                        .await
                    {
                        if matches!(error.kind(), ErrorKind::Integration | ErrorKind::Timeout) {
                            pending_error = Some(error);
                            acceptance_unknown = true;
                        } else {
                            state.invalidate_runtime();
                            session.finish_without_prompt(Err(error));
                            return;
                        }
                    }
                }
                Err(error) if protocol.rejected_before_dispatch(&error) => {
                    let error = CodexProtocol::caller_safe_request_error(error);
                    let result =
                        match Self::remove_unaccepted_prompt(
                            &state,
                            binding.conversation_id(),
                            &client_message_id,
                        )
                        .await
                        {
                            Ok(()) => Err(error),
                            Err(cleanup_error) => Err(error
                                .with_supplementary(cleanup_error.in_phase(ErrorPhase::Cleanup))),
                        };
                    session.finish_without_prompt(result);
                    return;
                }
                Err(error) => {
                    let start_error = CodexProtocol::caller_safe_request_error(error);
                    match protocol
                        .reconcile_prompt_start(binding.thread_id(), &client_message_id)
                        .await
                    {
                        Ok(observed @ CodexPromptTurn::Active { .. })
                        | Ok(observed @ CodexPromptTurn::Terminal { .. }) => {
                            accepted_turn_id = observed.turn_id().map(str::to_owned);
                            if let Err(error) = Self::persist_observed_turn(
                                &state,
                                &binding,
                                &client_message_id,
                                observed,
                            )
                            .await
                            {
                                if matches!(
                                    error.kind(),
                                    ErrorKind::Integration | ErrorKind::Timeout
                                ) {
                                    pending_error = Some(error);
                                    acceptance_unknown = true;
                                } else {
                                    state.invalidate_runtime();
                                    session.finish_without_prompt(Err(error));
                                    return;
                                }
                            }
                        }
                        Ok(CodexPromptTurn::Absent) => {
                            let result = match Self::remove_unaccepted_prompt(
                                &state,
                                binding.conversation_id(),
                                &client_message_id,
                            )
                            .await
                            {
                                Ok(()) => Err(start_error),
                                Err(cleanup_error) => Err(start_error.with_supplementary(
                                    cleanup_error.in_phase(ErrorPhase::Cleanup),
                                )),
                            };
                            session.finish_without_prompt(result);
                            return;
                        }
                        Err(_) => {
                            pending_error = Some(start_error);
                            acceptance_unknown = true;
                        }
                    }
                }
            }

            loop {
                let mut retry_ambiguous_cancellation = false;
                match state
                    .prompt_terminal(binding.conversation_id(), &client_message_id)
                    .await
                {
                    Ok(Some((turn_id, result))) => {
                        session
                            .finish_terminal(
                                &state,
                                binding.conversation_id(),
                                &client_message_id,
                                &turn_id,
                                result,
                            )
                            .await;
                        break;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        if !matches!(error.kind(), ErrorKind::Integration | ErrorKind::Timeout) {
                            state.invalidate_runtime();
                            session.finish_without_prompt(Err(error));
                            break;
                        }
                        if pending_error.is_none() {
                            pending_error = Some(error);
                        }
                        native_reconciliation = accepted_turn_id.is_some();
                        next_native_reconciliation =
                            tokio::time::Instant::now() + std::time::Duration::from_millis(25);
                        if !native_reconciliation && !acceptance_unknown {
                            let error = pending_error.take().unwrap_or_else(|| {
                                KitError::integration("prompt terminal state could not be read")
                            });
                            session.finish_without_prompt(Err(error));
                            break;
                        }
                    }
                }
                if acceptance_unknown {
                    match protocol
                        .reconcile_prompt_start(binding.thread_id(), &client_message_id)
                        .await
                    {
                        Ok(observed @ CodexPromptTurn::Active { .. })
                        | Ok(observed @ CodexPromptTurn::Terminal { .. }) => {
                            retry_ambiguous_cancellation =
                                matches!(&observed, CodexPromptTurn::Active { .. });
                            accepted_turn_id = observed.turn_id().map(str::to_owned);
                            match Self::persist_observed_turn(
                                &state,
                                &binding,
                                &client_message_id,
                                observed,
                            )
                            .await
                            {
                                Ok(_) => {
                                    acceptance_unknown = false;
                                    pending_error = None;
                                }
                                Err(error)
                                    if matches!(
                                        error.kind(),
                                        ErrorKind::Integration | ErrorKind::Timeout
                                    ) =>
                                {
                                    if pending_error.is_none() {
                                        pending_error = Some(error);
                                    }
                                }
                                Err(error) => {
                                    state.invalidate_runtime();
                                    session.finish_without_prompt(Err(error));
                                    break;
                                }
                            }
                        }
                        Ok(CodexPromptTurn::Absent) => {
                            match Self::remove_unaccepted_prompt(
                                &state,
                                binding.conversation_id(),
                                &client_message_id,
                            )
                            .await
                            {
                                Ok(()) => {
                                    let error = pending_error.take().unwrap_or_else(|| {
                                        KitError::integration(
                                            "prompt start could not be reconciled",
                                        )
                                    });
                                    session.finish_without_prompt(Err(error));
                                    break;
                                }
                                Err(_) => {
                                    if let Some(turn_id) = accepted_turn_id.as_deref() {
                                        let result = Err(KitError::integration(
                                            "accepted prompt disappeared during reconciliation",
                                        ));
                                        if state
                                            .terminalize_prompt(
                                                binding.conversation_id(),
                                                turn_id,
                                                &result,
                                            )
                                            .await
                                            .is_ok()
                                        {
                                            acceptance_unknown = false;
                                        }
                                    }
                                }
                            }
                        }
                        Err(error)
                            if !matches!(
                                error.kind(),
                                ErrorKind::Integration | ErrorKind::Timeout
                            ) =>
                        {
                            state.invalidate_runtime();
                            session.finish_without_prompt(Err(error));
                            break;
                        }
                        Err(_) => {}
                    }
                }
                if native_reconciliation
                    && !acceptance_unknown
                    && tokio::time::Instant::now() >= next_native_reconciliation
                    && let Some(turn_id) = accepted_turn_id.clone()
                {
                    match protocol
                        .prompt_turn(
                            binding.thread_id(),
                            Some(&client_message_id),
                            Some(&turn_id),
                        )
                        .await
                    {
                        Ok(observed @ CodexPromptTurn::Active { .. })
                        | Ok(observed @ CodexPromptTurn::Terminal { .. }) => {
                            retry_ambiguous_cancellation =
                                matches!(&observed, CodexPromptTurn::Active { .. });
                            accepted_turn_id = observed.turn_id().map(str::to_owned);
                            match Self::persist_observed_turn(
                                &state,
                                &binding,
                                &client_message_id,
                                observed,
                            )
                            .await
                            {
                                Ok(true) => native_reconciliation = false,
                                Ok(false) => {}
                                Err(error)
                                    if matches!(
                                        error.kind(),
                                        ErrorKind::Integration | ErrorKind::Timeout
                                    ) =>
                                {
                                    if pending_error.is_none() {
                                        pending_error = Some(error);
                                    }
                                }
                                Err(error) => {
                                    state.invalidate_runtime();
                                    session.finish_without_prompt(Err(error));
                                    break;
                                }
                            }
                        }
                        Ok(CodexPromptTurn::Absent) => {
                            let result = Err(KitError::integration(
                                "accepted prompt disappeared during reconciliation",
                            ));
                            if state
                                .terminalize_prompt(binding.conversation_id(), &turn_id, &result)
                                .await
                                .is_ok()
                            {
                                native_reconciliation = false;
                            }
                        }
                        Err(error)
                            if !matches!(
                                error.kind(),
                                ErrorKind::Integration | ErrorKind::Timeout
                            ) =>
                        {
                            state.invalidate_runtime();
                            session.finish_without_prompt(Err(error));
                            break;
                        }
                        Err(_) => {}
                    }
                    next_native_reconciliation =
                        tokio::time::Instant::now() + std::time::Duration::from_millis(25);
                }
                match state
                    .claim_abort(
                        binding.conversation_id(),
                        &client_message_id,
                        retry_ambiguous_cancellation,
                    )
                    .await
                {
                    Ok(Some(turn_id)) => {
                        accepted_turn_id = Some(turn_id.clone());
                        let cancellation =
                            protocol.interrupt_turn(binding.thread_id(), &turn_id).await;
                        let ambiguous = cancellation.is_err();
                        if let Err(error) = state
                            .settle_abort(
                                binding.conversation_id(),
                                &client_message_id,
                                &turn_id,
                                ambiguous,
                            )
                            .await
                        {
                            if !matches!(error.kind(), ErrorKind::Integration | ErrorKind::Timeout)
                            {
                                state.invalidate_runtime();
                                session.finish_without_prompt(Err(error));
                                break;
                            }
                            if pending_error.is_none() {
                                pending_error = Some(error);
                            }
                        }
                        native_reconciliation = true;
                        next_native_reconciliation =
                            tokio::time::Instant::now() + std::time::Duration::from_millis(25);
                    }
                    Ok(None) => {}
                    Err(error) => {
                        if !matches!(error.kind(), ErrorKind::Integration | ErrorKind::Timeout) {
                            state.invalidate_runtime();
                            session.finish_without_prompt(Err(error));
                            break;
                        }
                        if pending_error.is_none() {
                            pending_error = Some(error);
                        }
                        native_reconciliation = accepted_turn_id.is_some();
                        next_native_reconciliation =
                            tokio::time::Instant::now() + std::time::Duration::from_millis(25);
                    }
                }
                tokio::select! {
                    changed = changes.changed(), if watch_open => {
                        if changed.is_err() {
                            watch_open = false;
                            if pending_error.is_none() {
                                pending_error = Some(KitError::integration(
                                    "prompt state owner ended unexpectedly",
                                ));
                            }
                            native_reconciliation = accepted_turn_id.is_some();
                            next_native_reconciliation = tokio::time::Instant::now()
                                + std::time::Duration::from_millis(25);
                        }
                    }
                    _ = session.caller_closed(), if session.has_waiter() => {
                        session
                            .lose_waiter(
                                &state,
                                binding.conversation_id(),
                                &client_message_id,
                            )
                            .await;
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_millis(25)), if acceptance_unknown || !watch_open => {
                    }
                    _ = tokio::time::sleep_until(next_native_reconciliation), if native_reconciliation && !acceptance_unknown => {
                    }
                }
            }
        };
        let timeout_state = self.state.clone();
        let timeout_conversation_id = self.binding.conversation_id().clone();
        let timeout_client_message_id = client_message_id.clone();
        let task = async move {
            tokio::pin!(task);
            if let (Some(deadline), Some(output)) = (deadline, output) {
                tokio::select! {
                    biased;
                    _ = &mut task => return,
                    _ = tokio::time::sleep_until(deadline) => {}
                }
                output.finish(Err(KitError::timeout("prompt deadline elapsed")));
                let intent_deadline = tokio::time::Instant::now() + lifecycle_timeout;
                let intent = async {
                    let result = loop {
                        if tokio::time::Instant::now() >= intent_deadline {
                            break Err(KitError::timeout("prompt timeout intent deadline elapsed"));
                        }
                        match timeout_state
                            .mark_prompt_timed_out(
                                &timeout_conversation_id,
                                &timeout_client_message_id,
                            )
                            .await
                        {
                            Err(error)
                                if matches!(
                                    error.kind(),
                                    ErrorKind::Integration | ErrorKind::Timeout
                                ) =>
                            {
                                tokio::time::sleep(PROMPT_STATE_RETRY_INTERVAL).await;
                            }
                            result => break result,
                        }
                    };
                    if result.is_err() {
                        timeout_state.invalidate_runtime();
                    }
                };
                tokio::join!(&mut task, intent);
                return;
            }
            task.await;
        };
        if let Err(task) = self.state.spawn_prompt(task) {
            drop(task);
            return Err(KitError::closed("prompt task admission is closed"));
        }
        Ok(client_message_id)
    }

    async fn await_prompt(
        &self,
        receiver: oneshot::Receiver<oneshot::Receiver<Result<PromptResponse, KitError>>>,
        client_message_id: &str,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<PromptResponse, KitError> {
        let mut receiver = receiver;
        match deadline {
            Some(deadline) => {
                tokio::select! {
                    biased;
                    received = &mut receiver => {
                        let received = received
                            .map_err(|_| KitError::integration("prompt waiter was lost"))?;
                        Self::complete_prompt_result(received).await
                    }
                    _ = tokio::time::sleep_until(deadline) => {
                        if let Ok(received) = receiver.try_recv() {
                            return Self::complete_prompt_result(received).await;
                        }
                        match self
                            .state
                            .mark_prompt_timed_out(
                                self.binding.conversation_id(),
                                client_message_id,
                            )
                            .await
                        {
                            Ok(Some(false)) => Self::receive_prompt_result(&mut receiver).await,
                            Ok(Some(true)) => {
                                Err(KitError::timeout("prompt deadline elapsed"))
                            }
                            Ok(None) => match receiver.try_recv() {
                                Ok(completed) => Self::complete_prompt_result(completed).await,
                                Err(_) => Err(KitError::timeout("prompt deadline elapsed")),
                            },
                            Err(error) => match receiver.try_recv() {
                                Ok(received) => Self::complete_prompt_result(received).await,
                                Err(_) => Err(error),
                            },
                        }
                    }
                }
            }
            None => Self::receive_prompt_result(&mut receiver).await,
        }
    }

    pub(crate) async fn abort(&self, deadline: tokio::time::Instant) -> Result<(), KitError> {
        let mut changes = self.state.prompt_changes();
        let (target, turn_id) = match self
            .state
            .request_abort(self.binding.conversation_id(), deadline)
            .await?
        {
            CodexAbort::Idle => return Ok(()),
            CodexAbort::Join {
                client_message_id,
                turn_id,
            } => (client_message_id, turn_id),
        };
        let durable = async {
            loop {
                if self
                    .state
                    .abort_complete(self.binding.conversation_id(), &target)
                    .await?
                {
                    return Ok(());
                }
                match tokio::time::timeout_at(deadline, changes.changed()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => {
                        return Err(KitError::closed(
                            "prompt state owner closed during cancellation",
                        ));
                    }
                    Err(_) => {
                        return Err(KitError::timeout("prompt cancellation deadline elapsed"));
                    }
                }
            }
        };
        let Some(turn_id) = turn_id else {
            return durable.await;
        };
        let generation = CodexToolGeneration::new(self.binding.thread_id(), &turn_id);
        let (durable, tools) = tokio::join!(
            durable,
            self.tools.cancel_generation(generation.clone(), deadline),
        );
        if durable.is_ok() {
            self.tools.release_generation(generation);
        }
        durable.and(tools)
    }

    pub(crate) async fn reconcile_reopen(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), KitError> {
        let record = self
            .state
            .prompt_record(self.binding.conversation_id())
            .await?;
        let native = tokio::time::timeout_at(
            deadline,
            self.protocol.prompt_turn(
                self.binding.thread_id(),
                record.as_ref().map(|record| record.client_message_id()),
                record.as_ref().and_then(|record| record.turn_id()),
            ),
        )
        .await
        .map_err(|_| KitError::timeout("prompt reconciliation deadline elapsed"))??;
        let Some(record) = record else {
            return match native {
                CodexPromptTurn::Absent => Ok(()),
                CodexPromptTurn::Active { .. } | CodexPromptTurn::Terminal { .. } => {
                    Err(KitError::invalid_state(
                        "has active runtime work without a durable prompt owner",
                    ))
                }
            };
        };
        let client_message_id = record.client_message_id().to_owned();
        match native {
            CodexPromptTurn::Absent if record.is_terminal() => {
                return self
                    .state
                    .repair_reopen_prompt(self.binding.conversation_id(), &client_message_id)
                    .await;
            }
            CodexPromptTurn::Absent => {
                self.state
                    .repair_reopen_prompt(self.binding.conversation_id(), &client_message_id)
                    .await?;
                return Err(KitError::invalid_state(
                    "durable prompt has no matching runtime prompt execution",
                ));
            }
            terminal @ CodexPromptTurn::Terminal { .. } => {
                Self::persist_observed_turn(
                    &self.state,
                    &self.binding,
                    &client_message_id,
                    terminal,
                )
                .await?;
                return self
                    .state
                    .repair_reopen_prompt(self.binding.conversation_id(), &client_message_id)
                    .await;
            }
            CodexPromptTurn::Active { .. } if record.is_terminal() => {
                return Err(KitError::busy(
                    "runtime work remains active after durable terminal state",
                ));
            }
            CodexPromptTurn::Active { turn_id } => {
                if record.turn_id().is_none() {
                    self.state
                        .accept_prompt(self.binding.conversation_id(), &client_message_id, &turn_id)
                        .await?;
                }
            }
        }
        let target = match self
            .state
            .request_abort(self.binding.conversation_id(), deadline)
            .await?
        {
            CodexAbort::Idle => {
                let native = tokio::time::timeout_at(
                    deadline,
                    self.protocol.prompt_turn(
                        self.binding.thread_id(),
                        Some(&client_message_id),
                        record.turn_id(),
                    ),
                )
                .await
                .map_err(|_| KitError::timeout("prompt reconciliation deadline elapsed"))??;
                return match native {
                    CodexPromptTurn::Active { .. } => Err(KitError::busy(
                        "runtime prompt remains active during reconciliation",
                    )),
                    CodexPromptTurn::Absent | CodexPromptTurn::Terminal { .. } => {
                        self.state
                            .repair_reopen_prompt(
                                self.binding.conversation_id(),
                                &client_message_id,
                            )
                            .await
                    }
                };
            }
            CodexAbort::Join {
                client_message_id, ..
            } => client_message_id,
        };
        let Some(turn_id) = self
            .state
            .claim_abort(self.binding.conversation_id(), &target, true)
            .await?
        else {
            let current = self
                .state
                .prompt_record(self.binding.conversation_id())
                .await?
                .ok_or_else(|| {
                    KitError::invalid_state("prompt state disappeared during reconciliation")
                })?;
            let confirmation = tokio::time::timeout_at(
                deadline,
                self.protocol.prompt_turn(
                    self.binding.thread_id(),
                    Some(&target),
                    current.turn_id(),
                ),
            )
            .await
            .map_err(|_| KitError::timeout("prompt reconciliation deadline elapsed"))??;
            return match confirmation {
                CodexPromptTurn::Active { .. } => {
                    Err(KitError::busy("cancellation remains recovery-owned"))
                }
                CodexPromptTurn::Absent | CodexPromptTurn::Terminal { .. } => {
                    self.state
                        .repair_reopen_prompt(self.binding.conversation_id(), &target)
                        .await
                }
            };
        };
        let cancellation = tokio::time::timeout_at(
            deadline,
            self.protocol
                .interrupt_turn(self.binding.thread_id(), &turn_id),
        )
        .await;
        match cancellation {
            Err(_) => {
                self.state
                    .settle_abort(self.binding.conversation_id(), &target, &turn_id, true)
                    .await?;
                Err(KitError::busy("cancellation acceptance is ambiguous"))
            }
            Ok(Err(_)) => {
                self.state
                    .settle_abort(self.binding.conversation_id(), &target, &turn_id, true)
                    .await?;
                Err(KitError::busy("cancellation acceptance is ambiguous"))
            }
            Ok(Ok(())) => {
                self.state
                    .settle_abort(self.binding.conversation_id(), &target, &turn_id, false)
                    .await?;
                let confirmation = tokio::time::timeout_at(
                    deadline,
                    self.protocol.prompt_turn(
                        self.binding.thread_id(),
                        Some(&client_message_id),
                        Some(&turn_id),
                    ),
                )
                .await
                .map_err(|_| KitError::timeout("prompt reconciliation deadline elapsed"))??;
                match confirmation {
                    CodexPromptTurn::Active { .. } => Err(KitError::busy(
                        "runtime prompt remains active after cancellation",
                    )),
                    CodexPromptTurn::Terminal { .. } => {
                        self.state
                            .repair_reopen_prompt(
                                self.binding.conversation_id(),
                                &client_message_id,
                            )
                            .await
                    }
                    CodexPromptTurn::Absent => {
                        self.state
                            .repair_reopen_prompt(
                                self.binding.conversation_id(),
                                &client_message_id,
                            )
                            .await?;
                        Ok(())
                    }
                }
            }
        }
    }

    pub(crate) async fn observe_turn_metadata(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<(), KitError> {
        if thread_id != self.binding.thread_id() {
            // Routing has verified ancestry. Ephemeral and early child turns need
            // not have readable history; their native turn is not the root turn.
            return Ok(());
        }
        if let Some(prompt) = self
            .state
            .prompt_record(self.binding.conversation_id())
            .await?
            && prompt.turn_id().is_some_and(|owned| owned != turn_id)
        {
            return Err(KitError::protocol(
                "runtime metadata belongs to another prompt generation",
            ));
        }
        Ok(())
    }

    pub(crate) async fn observe_prompt_output(
        &self,
        output: &CodexPromptOutput,
    ) -> Result<(), KitError> {
        self.state
            .observe_prompt_output(
                self.binding.conversation_id(),
                output.turn_id(),
                output.text(),
                output.is_eligible_output(),
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn observe_terminal(
        &self,
        terminal: CodexPromptTerminal,
    ) -> Result<(), KitError> {
        let deadline = self.tools.cancellation_deadline();
        let mut expected_generation = None;
        loop {
            // The state operation owns any admitted commit through settlement;
            // only read-only historical reconciliation is observation-bounded.
            match self
                .state
                .terminalize_observed_prompt(
                    self.binding.conversation_id(),
                    terminal.clone(),
                    expected_generation.as_deref(),
                )
                .await?
            {
                CodexTerminalDisposition::Applied => return Ok(()),
                CodexTerminalDisposition::Reconcile(current) => {
                    let (history, unchanged) = tokio::time::timeout_at(deadline, async {
                        let history = self
                            .protocol
                            .terminal_history(self.binding.thread_id(), &terminal)
                            .await?;
                        let unchanged = self
                            .state
                            .confirm_historical_terminal(
                                self.binding.conversation_id(),
                                current.as_deref(),
                            )
                            .await?;
                        Ok::<_, KitError>((history, unchanged))
                    })
                    .await
                    .map_err(|_| KitError::timeout("historical prompt reconciliation elapsed"))??;
                    if unchanged {
                        expected_generation =
                            history.current_generation(&terminal, current.as_deref())?;
                        if expected_generation.is_none() {
                            return Ok(());
                        }
                    } else {
                        expected_generation = None;
                        tokio::time::sleep(
                            PROMPT_STATE_RETRY_INTERVAL.min(
                                deadline.saturating_duration_since(tokio::time::Instant::now()),
                            ),
                        )
                        .await;
                    }
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(KitError::timeout(
                    "historical prompt reconciliation elapsed",
                ));
            }
        }
    }

    pub(crate) async fn publish_notification(
        &self,
        kind: ConversationNotificationKind,
    ) -> Result<(), KitError> {
        self.state
            .notifications()
            .publish(self.binding.conversation_id(), kind)
            .await
            .map(|_| ())
    }

    pub(crate) async fn publish_interaction(
        &self,
        request: CodexInteractionRequest,
    ) -> Result<(), KitError> {
        let deadline = self.tools.cancellation_deadline();
        loop {
            match self
                .state
                .publish_interaction(self.binding.conversation_id(), request.clone(), deadline)
                .await
            {
                Ok(_) => return Ok(()),
                Err(error)
                    if error.detail() == Some(&ErrorDetail::TransactionConflict)
                        && error.context().phase != ErrorPhase::Cleanup
                        && error.supplementary().is_empty() =>
                {
                    // Only exact conflicts prove that publication did not commit.
                    // Staging has already rolled back; native response enqueue
                    // is outside this retry and remains non-replayable.
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    if remaining.is_zero() {
                        return Err(error);
                    }
                    tokio::time::sleep(PROMPT_STATE_RETRY_INTERVAL.min(remaining)).await;
                    if tokio::time::Instant::now() >= deadline {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn resolve_notification(
        &self,
        notification_id: NotificationId,
        resolution: NotificationResolution,
    ) -> Result<(), KitError> {
        let deadline = self.tools.cancellation_deadline();
        if self
            .tools
            .resolve(
                self.binding.conversation_id(),
                notification_id.clone(),
                resolution.clone(),
                deadline,
            )
            .await?
        {
            return Ok(());
        }
        let continuation = self
            .state
            .continuation(self.binding.conversation_id(), &notification_id)
            .await?;
        let response = CodexServerResponse::from_rendering(CodexServerRendering::interaction(
            &continuation,
            &resolution,
        )?);
        let intent = self
            .state
            .notifications()
            .begin_resolution(
                self.binding.conversation_id(),
                notification_id,
                resolution,
                deadline,
            )
            .await?;
        match self.protocol.respond(response).await {
            Ok(()) => {
                let actual = self
                    .state
                    .settle_interaction(&intent, &continuation, ResolutionSettlement::Ambiguous)
                    .await?;
                match actual {
                    ResolutionSettlement::Ambiguous => Err(KitError::conflict(
                        "response was enqueued but runtime acceptance is ambiguous",
                    )),
                    ResolutionSettlement::Accepted
                    | ResolutionSettlement::Rejected
                    | ResolutionSettlement::Cancelled => Err(KitError::invalid_state(
                        "interaction settled with an unexpected outcome",
                    )),
                }
            }
            Err(send_error) => {
                let actual = self
                    .state
                    .settle_interaction(&intent, &continuation, ResolutionSettlement::Rejected)
                    .await?;
                match actual {
                    ResolutionSettlement::Rejected => Err(send_error),
                    ResolutionSettlement::Ambiguous => {
                        Err(KitError::conflict("response acceptance remains ambiguous"))
                    }
                    ResolutionSettlement::Accepted | ResolutionSettlement::Cancelled => Err(
                        KitError::invalid_state("interaction settled with an unexpected outcome"),
                    ),
                }
            }
        }
    }
}
