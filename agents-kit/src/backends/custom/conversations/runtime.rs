use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::Duration;
use tools_kit::ToolsetBackend;

use providers_kit::ProviderBackend;
use tokio::sync::{Notify, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::backends::custom::adapters::files::CustomFileAdapter;
use crate::backends::custom::adapters::{CustomProviderCall, CustomProviderOperation};
use crate::backends::custom::knowledge_base::CustomKnowledgeBase;
use crate::backends::custom::server::CustomServer;
use crate::backends::custom::server_requests::CustomServerRequests;
use crate::backends::custom::state_store::records::{
    CustomCancellationIntent, CustomContinuationDisposition,
};
use crate::backends::custom::state_store::{CustomStateStore, CustomTerminalAttempt};
use crate::core::hierarchy::HierarchyAdmission;
use crate::prompt::{PromptOutput, PromptStream};
use crate::{
    ConversationId, FileRead, KitError, NotificationId, NotificationResolution, PromptRequest,
    PromptResponse, WorkspaceId, WorkspaceInfo,
};
use storage_kit::{DatabaseBackend, StateStoreBackend};

pub(crate) struct CustomConversationRuntime<
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
> {
    conversation_id: ConversationId,
    state: CustomStateStore<S, D>,
    server: CustomServer<S, D, P, T>,
    requests: CustomServerRequests<S, D, T>,
    knowledge: CustomKnowledgeBase<S, D>,
    fence: Arc<CustomGenerationFence>,
    close: Arc<CustomCloseOwner>,
}

struct CustomGenerationFence {
    state: Mutex<CustomGenerationState>,
    changed: Notify,
    terminal: watch::Sender<()>,
}

struct CustomCloseOwner {
    slot: Mutex<CustomCloseSlot>,
    changed: Notify,
}

enum CustomCloseSlot {
    Idle,
    Installing,
    Running {
        handle: JoinHandle<()>,
        phase: CustomClosePhase,
    },
    Poisoned,
}

enum CustomClosePhase {
    Running,
    Complete,
    Interrupted,
}

enum CustomCloseObservation {
    Complete,
    Notify,
    Yield,
}

struct CustomCloseGuard {
    owner: Arc<CustomCloseOwner>,
    state: CustomCloseGuardState,
}

#[derive(Clone, Copy)]
enum CustomCloseGuardState {
    Active,
    Complete,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CustomGenerationState {
    Idle,
    Starting { cancellation_requested: bool },
    Active { generation: u64 },
    Terminalizing { generation: u64 },
    Publishing { generation: u64 },
    Settling { generation: u64, delivered: bool },
}

#[derive(Clone, Copy)]
enum CustomSettlementScope {
    Retained,
    Quiescent,
}

enum CustomPromptDelivery {
    Immediate(Result<PromptResponse, KitError>),
    Terminal {
        generation: Option<u64>,
        result: Result<PromptResponse, KitError>,
    },
}

enum CustomPromptObservation {
    Response {
        sender: Option<oneshot::Sender<CustomPromptDelivery>>,
        acknowledgement: oneshot::Receiver<()>,
    },
    Stream {
        output: Arc<PromptOutput>,
        terminal: Option<Result<PromptResponse, KitError>>,
    },
}

impl CustomPromptObservation {
    fn output(&self) -> Option<Arc<PromptOutput>> {
        match self {
            Self::Response { .. } => None,
            Self::Stream { output, .. } => Some(Arc::clone(output)),
        }
    }
}

struct CustomPrompt<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend>
{
    runtime: CustomConversationRuntime<S, D, P, T>,
    admission: Option<HierarchyAdmission>,
    workspace_id: WorkspaceId,
    workspace_revision: u64,
    request: PromptRequest,
    deadline: Option<Instant>,
    timeout: Option<Duration>,
    deadline_elapsed: bool,
    delivered: bool,
    observation: CustomPromptObservation,
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend> Clone
    for CustomConversationRuntime<S, D, P, T>
{
    fn clone(&self) -> Self {
        Self {
            conversation_id: self.conversation_id.clone(),
            state: self.state.clone(),
            server: self.server.clone(),
            requests: self.requests.clone(),
            knowledge: self.knowledge.clone(),
            fence: Arc::clone(&self.fence),
            close: Arc::clone(&self.close),
        }
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend>
    CustomConversationRuntime<S, D, P, T>
{
    pub(crate) fn new(
        conversation_id: ConversationId,
        state: CustomStateStore<S, D>,
        server: CustomServer<S, D, P, T>,
        requests: CustomServerRequests<S, D, T>,
        knowledge: CustomKnowledgeBase<S, D>,
    ) -> Self {
        Self {
            conversation_id,
            state,
            server,
            requests,
            knowledge,
            fence: Arc::new(CustomGenerationFence {
                state: Mutex::new(CustomGenerationState::Idle),
                changed: Notify::new(),
                terminal: watch::channel(()).0,
            }),
            close: Arc::new(CustomCloseOwner::new()),
        }
    }

    pub(crate) async fn own_close_before(
        &self,
        deadline: Instant,
        future: Pin<Box<dyn Future<Output = Result<(), KitError>> + Send + 'static>>,
    ) -> Result<(), KitError> {
        self.close.run_before(deadline, future).await
    }

    pub(crate) async fn prompt(
        &self,
        admission: HierarchyAdmission,
        workspace_id: WorkspaceId,
        workspace_revision: u64,
        request: PromptRequest,
    ) -> Result<PromptResponse, KitError> {
        let (sender, receiver) = oneshot::channel();
        let (acknowledgement, waiter_acknowledgement) = oneshot::channel();
        self.start_prompt(
            admission,
            workspace_id,
            workspace_revision,
            request,
            CustomPromptObservation::Response {
                sender: Some(sender),
                acknowledgement: waiter_acknowledgement,
            },
        )
        .await?;
        let delivery = self
            .server
            .await_route_result(&self.conversation_id, async move {
                receiver.await.map_err(|_| {
                    KitError::invalid_state("prompt completion channel closed unexpectedly")
                })
            })
            .await?;
        let result = match delivery {
            CustomPromptDelivery::Immediate(result) => result,
            CustomPromptDelivery::Terminal { generation, result } => {
                self.server
                    .acknowledge_prompt(&self.conversation_id, generation)?;
                result
            }
        };
        let _ = acknowledgement.send(());
        result
    }

    pub(crate) async fn prompt_stream(
        &self,
        admission: HierarchyAdmission,
        workspace_id: WorkspaceId,
        workspace_revision: u64,
        request: PromptRequest,
    ) -> Result<PromptStream, KitError> {
        let (output, stream) = PromptOutput::channel();
        self.start_prompt(
            admission,
            workspace_id,
            workspace_revision,
            request,
            CustomPromptObservation::Stream {
                output,
                terminal: None,
            },
        )
        .await?;
        Ok(stream)
    }

    async fn start_prompt(
        &self,
        admission: HierarchyAdmission,
        workspace_id: WorkspaceId,
        workspace_revision: u64,
        request: PromptRequest,
        observation: CustomPromptObservation,
    ) -> Result<(), KitError> {
        let request_timeout = request.timeout();
        let timeout = request_timeout.or(self.state.default_prompt_timeout());
        let deadline = match timeout {
            Some(timeout) => Some(Instant::now().checked_add(timeout).ok_or_else(|| {
                if request_timeout.is_some() {
                    KitError::invalid_input("prompt timeout cannot be represented")
                } else {
                    KitError::invalid_config("default prompt timeout cannot be represented")
                }
            })?),
            None => None,
        };
        let lifecycle_deadline = self.server.lifecycle_deadline();
        let settlement_deadline = deadline.map_or(lifecycle_deadline, |deadline| {
            deadline.min(lifecycle_deadline)
        });
        if let Err(error) = self
            .server
            .await_prompt_settlement_before(&self.conversation_id, settlement_deadline)
            .await
        {
            return admission.publish(Err(error));
        }
        if let Err(error) = self
            .state
            .settle_mutations_before(settlement_deadline)
            .await
        {
            return admission.publish(Err(error));
        }
        if let Err(target) = self.fence.start() {
            if matches!(
                target,
                CustomGenerationState::Terminalizing { .. }
                    | CustomGenerationState::Publishing { .. }
                    | CustomGenerationState::Settling { .. }
            ) {
                if let Err(error) = self
                    .converge_settlement(
                        self.server.lifecycle_deadline(),
                        CustomSettlementScope::Quiescent,
                    )
                    .await
                {
                    return admission.publish(Err(error));
                }
                if self.fence.start().is_err() {
                    return admission.publish(Err(KitError::busy(
                        "conversation already has an active prompt",
                    )));
                }
            } else {
                return admission.publish(Err(KitError::busy(
                    "conversation already has an active prompt",
                )));
            }
        }
        let prompt = CustomPrompt {
            runtime: self.clone(),
            admission: Some(admission),
            workspace_id,
            workspace_revision,
            request,
            deadline,
            timeout,
            deadline_elapsed: false,
            delivered: false,
            observation,
        };
        let _completion = match self
            .server
            .spawn_prompt(&self.conversation_id, prompt.run())
        {
            Ok(completion) => completion,
            Err(error) => {
                self.fence.clear_starting();
                return Err(error);
            }
        };
        Ok(())
    }

    pub(crate) async fn abort(&self) -> Result<(), KitError> {
        let deadline = self.server.lifecycle_deadline();
        let (target, mut terminal) = self.fence.request_cancellation();
        let cancellation = match target {
            CustomGenerationState::Idle => {
                let durable = tokio::time::timeout_at(
                    deadline,
                    self.state.abort_target(&self.conversation_id),
                )
                .await
                .map_err(|_| KitError::timeout("prompt cancellation lookup timed out"))??;
                return if durable.is_some() {
                    Err(KitError::busy(
                        "accepted prompt lacks a live cancellation owner",
                    ))
                } else {
                    Ok(())
                };
            }
            CustomGenerationState::Publishing { .. } => return Ok(()),
            CustomGenerationState::Settling {
                generation,
                delivered,
            } => {
                self.state
                    .settle_generation_waiter(
                        &self.conversation_id,
                        generation,
                        delivered,
                        deadline,
                    )
                    .await?;
                self.fence.clear_settlement(generation, delivered);
                return Ok(());
            }
            CustomGenerationState::Starting { .. }
            | CustomGenerationState::Terminalizing { .. } => Ok(()),
            CustomGenerationState::Active { generation } => {
                let intent = self
                    .state
                    .request_generation_cancellation_before(
                        self.conversation_id.clone(),
                        generation,
                        CustomCancellationIntent::Caller,
                        deadline,
                    )
                    .await;
                let active = match intent {
                    Ok(active) => active,
                    Err(error)
                        if error.kind() == crate::error::ErrorKind::Conflict
                            && matches!(terminal.has_changed(), Ok(true)) =>
                    {
                        false
                    }
                    Err(error) => return Err(error),
                };
                if active {
                    // Keep the exact generation active through synchronous dispatch.
                    // Its prompt owner alone settles interactions before publishing
                    // the durable terminal; this observer must not touch a successor.
                    let state = self.fence.state();
                    if matches!(*state, CustomGenerationState::Active { generation: current } if current == generation)
                    {
                        self.server
                            .cancel(&self.conversation_id, generation)
                            .map(|_| ())
                    } else {
                        Ok(())
                    }
                } else {
                    Ok(())
                }
            }
        };
        let settlement = tokio::time::timeout_at(deadline, terminal.changed())
            .await
            .map_err(|_| KitError::timeout("prompt terminalization did not converge"))
            .and_then(|result| {
                result.map_err(|_| KitError::invalid_state("prompt terminalization owner closed"))
            });
        cancellation.and(settlement)
    }

    pub(crate) async fn read_file(
        &self,
        workspace: &WorkspaceInfo,
        path: PathBuf,
    ) -> Result<FileRead, KitError> {
        self.server.validate_workspace(workspace.context())?;
        let (relative, absolute) = workspace.context().resolve(&path)?;
        CustomFileAdapter::from_resolved(relative, absolute, workspace.context().roots())?
            .read()
            .await
    }

    pub(crate) async fn resolve(
        &self,
        id: NotificationId,
        resolution: NotificationResolution,
    ) -> Result<(), KitError> {
        let receiver = self
            .requests
            .start_resolution(&self.conversation_id, id, resolution)?;
        self.server
            .await_route_result(&self.conversation_id, async move {
                receiver.await.unwrap_or_else(|_| {
                    Err(KitError::invalid_state(
                        "interaction settlement ended unexpectedly",
                    ))
                })
            })
            .await
    }

    pub(crate) async fn begin_close(&self) -> Result<(), KitError> {
        let deadline = self.server.lifecycle_deadline();
        self.state
            .begin_close_before(self.conversation_id.clone(), deadline)
            .await?;
        self.converge_settlement(deadline, CustomSettlementScope::Retained)
            .await
    }

    pub(crate) async fn finish_close(&self) -> Result<(), KitError> {
        let deadline = self.server.lifecycle_deadline();
        let route_available = match self
            .server
            .seal_route_before(&self.conversation_id, deadline)
        {
            Ok(()) => true,
            Err(error) if error.kind() == crate::error::ErrorKind::Closed => false,
            Err(error) => return Err(error),
        };
        let _ = self.fence.request_cancellation();
        let generation = self
            .state
            .request_active_generation_cancellation_before(
                self.conversation_id.clone(),
                CustomCancellationIntent::ConversationClose,
                deadline,
            )
            .await?;
        if let Some(generation) = generation {
            if !route_available {
                return Err(KitError::invalid_state(
                    "active prompt lacks a live cancellation owner",
                ));
            }
            self.server.cancel(&self.conversation_id, generation)?;
        }
        self.requests
            .cancel_conversation(&self.conversation_id, deadline, None)
            .await?;
        self.converge_settlement(deadline, CustomSettlementScope::Quiescent)
            .await?;
        if route_available {
            self.server
                .remove_route_before(&self.conversation_id, deadline)
                .await
        } else {
            Ok(())
        }
    }

    pub(crate) fn lifecycle_deadline(&self) -> Instant {
        self.server.lifecycle_deadline()
    }

    pub(crate) fn lifecycle_timeout(&self) -> Duration {
        self.server.lifecycle_timeout()
    }

    async fn converge_settlement(
        &self,
        deadline: Instant,
        scope: CustomSettlementScope,
    ) -> Result<(), KitError> {
        loop {
            let target = self.fence.target();
            match target {
                CustomGenerationState::Idle => return Ok(()),
                CustomGenerationState::Starting { .. } | CustomGenerationState::Active { .. }
                    if matches!(scope, CustomSettlementScope::Retained) =>
                {
                    return Ok(());
                }
                CustomGenerationState::Starting { .. }
                | CustomGenerationState::Active { .. }
                | CustomGenerationState::Terminalizing { .. }
                | CustomGenerationState::Publishing { .. } => {
                    self.fence.await_change(target, deadline).await?;
                }
                CustomGenerationState::Settling {
                    generation,
                    delivered,
                } => {
                    self.state
                        .settle_generation_waiter(
                            &self.conversation_id,
                            generation,
                            delivered,
                            deadline,
                        )
                        .await?;
                    self.fence.clear_settlement(generation, delivered);
                }
            }
        }
    }
}

impl CustomCloseOwner {
    fn new() -> Self {
        Self {
            slot: Mutex::new(CustomCloseSlot::Idle),
            changed: Notify::new(),
        }
    }

    async fn run_before<F>(self: &Arc<Self>, deadline: Instant, future: F) -> Result<(), KitError>
    where
        F: Future<Output = Result<(), KitError>> + Send + 'static,
    {
        {
            let mut slot = self
                .slot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match *slot {
                CustomCloseSlot::Idle => *slot = CustomCloseSlot::Installing,
                CustomCloseSlot::Poisoned => {
                    return Err(KitError::invalid_state("terminal close owner is poisoned"));
                }
                CustomCloseSlot::Installing | CustomCloseSlot::Running { .. } => {
                    return Err(KitError::busy("terminal close is already running"));
                }
            }
        }
        let (outcome, result) = oneshot::channel();
        let owner = Arc::clone(self);
        let (start, ready) = oneshot::channel();
        let handle = tokio::spawn(async move {
            let guard = CustomCloseGuard {
                owner,
                state: CustomCloseGuardState::Active,
            };
            if ready.await.is_err() {
                return;
            }
            let result = future.await;
            let _ = outcome.send(result);
            guard.complete();
        });
        self.install(handle)?;
        start
            .send(())
            .map_err(|_| KitError::invalid_state("terminal close task could not start"))?;
        self.settle_before(deadline).await?;
        result
            .await
            .map_err(|_| KitError::invalid_state("terminal close completed without a result"))?
    }

    async fn settle_before(&self, deadline: Instant) -> Result<(), KitError> {
        loop {
            let mut changed = std::pin::pin!(self.changed.notified());
            changed.as_mut().enable();
            let observation = {
                let mut slot = self
                    .slot
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                match &*slot {
                    CustomCloseSlot::Idle => CustomCloseObservation::Complete,
                    CustomCloseSlot::Installing => CustomCloseObservation::Notify,
                    CustomCloseSlot::Poisoned => {
                        return Err(KitError::invalid_state("terminal close owner is poisoned"));
                    }
                    CustomCloseSlot::Running { handle, .. } if handle.is_finished() => {
                        let previous = std::mem::replace(&mut *slot, CustomCloseSlot::Poisoned);
                        let CustomCloseSlot::Running {
                            mut handle, phase, ..
                        } = previous
                        else {
                            return Err(KitError::invalid_state(
                                "terminal close owner changed during settlement",
                            ));
                        };
                        let waker = std::task::Waker::noop();
                        let mut context = Context::from_waker(waker);
                        let joined = matches!(
                            Pin::new(&mut handle).poll(&mut context),
                            Poll::Ready(Ok(()))
                        );
                        if !joined || !matches!(phase, CustomClosePhase::Complete) {
                            return Err(KitError::invalid_state(
                                "terminal close task terminated unexpectedly",
                            ));
                        }
                        *slot = CustomCloseSlot::Idle;
                        CustomCloseObservation::Complete
                    }
                    CustomCloseSlot::Running {
                        phase: CustomClosePhase::Complete | CustomClosePhase::Interrupted,
                        ..
                    } => CustomCloseObservation::Yield,
                    CustomCloseSlot::Running { .. } => CustomCloseObservation::Notify,
                }
            };
            match observation {
                CustomCloseObservation::Complete => {
                    self.changed.notify_waiters();
                    return Ok(());
                }
                CustomCloseObservation::Notify => {
                    tokio::time::timeout_at(deadline, changed)
                        .await
                        .map_err(|_| KitError::timeout("terminal close did not settle"))?;
                }
                CustomCloseObservation::Yield => {
                    if Instant::now() >= deadline {
                        return Err(KitError::timeout("terminal close did not settle"));
                    }
                    tokio::task::yield_now().await;
                }
            }
        }
    }

    fn install(&self, handle: JoinHandle<()>) -> Result<(), KitError> {
        let mut slot = self
            .slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !matches!(*slot, CustomCloseSlot::Installing) {
            return Err(KitError::invalid_state(
                "terminal close owner changed during task installation",
            ));
        }
        *slot = CustomCloseSlot::Running {
            handle,
            phase: CustomClosePhase::Running,
        };
        drop(slot);
        self.changed.notify_waiters();
        Ok(())
    }

    fn finish(&self, state: CustomCloseGuardState) {
        let mut slot = self
            .slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let CustomCloseSlot::Running { phase, .. } = &mut *slot
            && matches!(phase, CustomClosePhase::Running)
        {
            *phase = match state {
                CustomCloseGuardState::Active => CustomClosePhase::Interrupted,
                CustomCloseGuardState::Complete => CustomClosePhase::Complete,
            };
        }
        drop(slot);
        self.changed.notify_waiters();
    }
}

impl CustomCloseGuard {
    fn complete(mut self) {
        self.state = CustomCloseGuardState::Complete;
    }
}

impl Drop for CustomCloseGuard {
    fn drop(&mut self) {
        self.owner.finish(self.state);
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend>
    CustomPrompt<S, D, P, T>
{
    async fn run(mut self) {
        let _permit = match self.runtime.state.prompt_permit() {
            Ok(permit) => permit,
            Err(error) => {
                self.finish_starting(Err(error));
                return;
            }
        };
        let state = self.runtime.state.clone();
        let workspace_id = self.workspace_id.clone();
        let workspace = match self
            .await_operation(async move { state.workspace(&workspace_id).await })
            .await
        {
            Ok(workspace) => workspace,
            Err(error) => {
                self.finish_starting(Err(error));
                return;
            }
        };
        if workspace.revision() != self.workspace_revision {
            self.finish_starting(Err(KitError::conflict("conversation handle is stale")
                .with_detail(crate::error::ErrorDetail::StaleRevision)
                .in_phase(crate::error::ErrorPhase::Admission)));
            return;
        }
        if let Some(error) = self.starting_error() {
            self.finish_starting(Err(error));
            return;
        }
        let state = self.runtime.state.clone();
        let workspace_id = self.workspace_id.clone();
        let conversation_id = self.runtime.conversation_id.clone();
        let conversation = match self
            .await_operation(
                async move { state.conversation(&workspace_id, &conversation_id).await },
            )
            .await
        {
            Ok(conversation) => conversation,
            Err(error) => {
                self.finish_starting(Err(error));
                return;
            }
        };
        if conversation.workspace_revision() != workspace.revision() {
            self.finish_starting(Err(KitError::invalid_state(
                "conversation revision contradicts its workspace",
            )));
            return;
        }
        let conversation_defaults = match conversation.defaults() {
            Ok(defaults) => defaults,
            Err(error) => {
                self.finish_starting(Err(error));
                return;
            }
        };
        let workspace = match workspace.info() {
            Ok(workspace) => workspace,
            Err(error) => {
                self.finish_starting(Err(error));
                return;
            }
        };
        if let Err(error) = self.runtime.server.validate_workspace(workspace.context()) {
            self.finish_starting(Err(error));
            return;
        }
        if self
            .request
            .personality()
            .or_else(|| {
                conversation_defaults
                    .as_ref()
                    .and_then(crate::WorkspaceDefaults::personality)
            })
            .or_else(|| workspace.defaults().personality())
            .is_some()
        {
            self.finish_starting(Err(KitError::unsupported(
                "personality selection cannot be represented",
            )));
            return;
        }
        let model = self
            .request
            .model()
            .or_else(|| {
                conversation_defaults
                    .as_ref()
                    .and_then(crate::WorkspaceDefaults::model)
            })
            .or_else(|| workspace.defaults().model());
        let reasoning = self
            .request
            .reasoning_effort()
            .or_else(|| {
                conversation_defaults
                    .as_ref()
                    .and_then(crate::WorkspaceDefaults::reasoning_effort)
            })
            .or_else(|| workspace.defaults().reasoning_effort())
            .cloned();
        let selected_model = match self
            .runtime
            .server
            .model_selection(model, CustomProviderOperation::Request)
        {
            Ok(model) => model,
            Err(error) => {
                self.finish_starting(Err(error));
                return;
            }
        };
        let tool_selection = match self
            .runtime
            .server
            .tool_selection(self.request.tool_selection())
        {
            Ok(selection) => selection,
            Err(error) => {
                self.finish_starting(Err(error));
                return;
            }
        };
        if let Some(relative) = self.request.cwd()
            && let Err(error) = workspace.context().resolve(relative)
        {
            self.finish_starting(Err(error));
            return;
        }
        let knowledge = self.runtime.knowledge.clone();
        let workspace_id = self.workspace_id.clone();
        let conversation_id = self.runtime.conversation_id.clone();
        let conversation_policy = conversation_defaults
            .as_ref()
            .and_then(crate::WorkspaceDefaults::knowledge_policy)
            .cloned();
        let workspace_policy = workspace.defaults().knowledge_policy().cloned();
        let request = self.request.clone();
        let instructions = self
            .request
            .instructions()
            .map(str::to_owned)
            .or_else(|| self.runtime.state.instructions().map(str::to_owned));
        let system = match self
            .await_operation(async move {
                knowledge
                    .render_system(
                        &workspace_id,
                        &conversation_id,
                        conversation_policy.as_ref(),
                        workspace_policy.as_ref(),
                        &request,
                        instructions.as_deref(),
                    )
                    .await
            })
            .await
        {
            Ok(system) => system,
            Err(error) => {
                self.finish_starting(Err(error));
                return;
            }
        };
        if let Some(error) = self.starting_error() {
            self.finish_starting(Err(error));
            return;
        }
        let state = self.runtime.state.clone();
        let conversation_id = self.runtime.conversation_id.clone();
        let caller_text = self.request.text().to_owned();
        let structured = self.request.structured_output().cloned();
        let selected_tools = tool_selection.clone();
        let generation = match self
            .await_operation(async move {
                state
                    .start_generation(
                        &conversation_id,
                        caller_text,
                        structured.as_ref(),
                        &selected_tools,
                    )
                    .await
            })
            .await
        {
            Ok(generation) => generation,
            Err(error) => {
                self.finish_starting(Err(error));
                return;
            }
        };
        let generation_id = generation.id();
        if let Err(error) = self
            .runtime
            .server
            .bind_prompt(&self.runtime.conversation_id, generation_id)
        {
            let activation = self.runtime.fence.activate(generation_id);
            self.stop_generation(
                generation_id,
                Err(error.with_cleanup_result(activation.map(|_| ()))),
            )
            .await;
            return;
        }
        let cancelled = match self.runtime.fence.activate(generation_id) {
            Ok(cancelled) => cancelled,
            Err(error) => {
                let decline = self
                    .runtime
                    .server
                    .decline(&self.runtime.conversation_id, generation_id);
                self.stop_generation(generation_id, Err(error.with_cleanup_result(decline)))
                    .await;
                return;
            }
        };
        self.observe_deadline();
        if self.deadline_elapsed || cancelled {
            let decline = self
                .runtime
                .server
                .decline(&self.runtime.conversation_id, generation_id);
            let result = if self.deadline_elapsed {
                Err(self.deadline_error().with_cleanup_result(decline))
            } else {
                Err(
                    KitError::cancelled("prompt was cancelled before model acceptance")
                        .with_cleanup_result(decline),
                )
            };
            self.stop_generation(generation_id, result).await;
            return;
        }
        let call = CustomProviderCall::new(
            self.runtime.conversation_id.clone(),
            generation,
            selected_model,
            tool_selection,
            self.runtime.server.loop_limits(),
            self.observation.output(),
        )
        .with_prompt_overrides(
            system,
            self.request.structured_output(),
            reasoning.as_ref(),
        );
        let call = match call {
            Ok(call) => call,
            Err(error) => {
                let decline = self
                    .runtime
                    .server
                    .decline(&self.runtime.conversation_id, generation_id);
                self.stop_generation(generation_id, Err(error.with_cleanup_result(decline)))
                    .await;
                return;
            }
        };
        self.observe_deadline();
        if self.deadline_elapsed {
            let decline = self
                .runtime
                .server
                .decline(&self.runtime.conversation_id, generation_id);
            let failure = Err(self.deadline_error().with_cleanup_result(decline));
            self.stop_generation(generation_id, failure).await;
            return;
        }
        let mut run =
            match self
                .runtime
                .server
                .start(&self.runtime.conversation_id, generation_id, call)
            {
                Ok(run) => run,
                Err(error) => {
                    self.stop_generation(generation_id, Err(error)).await;
                    return;
                }
            };
        self.observe_deadline();
        if self.deadline_elapsed {
            let decline = run.decline();
            let failure = Err(self.deadline_error().with_cleanup_result(decline));
            self.stop_generation(generation_id, failure).await;
            return;
        }
        if let Err(error) = run.accept().await {
            self.stop_generation(generation_id, Err(error)).await;
            return;
        }
        let state = self.runtime.state.clone();
        let conversation_id = self.runtime.conversation_id.clone();
        let lifecycle_deadline = self.runtime.server.lifecycle_deadline();
        if let Err(error) = self
            .await_operation(async move {
                state
                    .mark_generation_accepted(&conversation_id, generation_id, lifecycle_deadline)
                    .await
            })
            .await
        {
            self.fail_accepted(generation_id, run, error).await;
            return;
        }
        self.complete_accepted(generation_id, run).await;
    }

    async fn await_operation<O, F>(&mut self, future: F) -> O
    where
        F: std::future::Future<Output = O>,
    {
        let mut future = std::pin::pin!(future);
        let Some(deadline) = self.deadline else {
            return future.await;
        };
        if self.deadline_elapsed || Instant::now() >= deadline {
            self.deliver_timeout();
            return future.await;
        }
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => {
                self.deliver_timeout();
                future.await
            }
            result = &mut future => result,
        }
    }

    async fn complete_accepted(
        mut self,
        generation: u64,
        run: crate::backends::custom::server::runtime::CustomProviderRun<S, D, P, T>,
    ) {
        let mut execution = Box::pin(run.finish());
        if !self.deadline_elapsed {
            let deadline = self.deadline;
            let observed = match deadline {
                Some(deadline) if Instant::now() < deadline => {
                    tokio::select! {
                        biased;
                        result = &mut execution => result.map(Some),
                        _ = tokio::time::sleep_until(deadline) => Ok(None),
                    }
                }
                Some(_) => Ok(None),
                None => execution.as_mut().await.map(Some),
            };
            match observed {
                Ok(Some(terminal)) => {
                    if let Some(result) = self.terminalize_provider(generation, Ok(terminal)).await
                    {
                        self.settle(generation, result).await;
                    }
                    return;
                }
                Err(error) => {
                    let deadline = self.runtime.server.lifecycle_deadline();
                    let interactions = self
                        .runtime
                        .requests
                        .cancel_conversation(
                            &self.runtime.conversation_id,
                            deadline,
                            Some(deadline),
                        )
                        .await;
                    if let Some(result) = self.terminalize_provider(generation, Err(error)).await {
                        self.settle(
                            generation,
                            match result {
                                Ok(response) => interactions.map(|_| response),
                                Err(primary) => Err(primary.with_cleanup_result(interactions)),
                            },
                        )
                        .await;
                    }
                    return;
                }
                Ok(None) => self.deadline_elapsed = true,
            }
        }
        let lifecycle_deadline = self.runtime.server.lifecycle_deadline();
        let state = self.runtime.state.clone();
        let conversation_id = self.runtime.conversation_id.clone();
        let cancellation = self
            .await_operation(async move {
                state
                    .request_generation_cancellation(
                        &conversation_id,
                        generation,
                        CustomCancellationIntent::Deadline,
                        lifecycle_deadline,
                    )
                    .await
            })
            .await;
        let active = match cancellation {
            Ok(active) => active,
            Err(error) => {
                let interactions = self
                    .runtime
                    .requests
                    .cancel_conversation(
                        &self.runtime.conversation_id,
                        lifecycle_deadline,
                        Some(lifecycle_deadline),
                    )
                    .await;
                let completion = execution.as_mut().await;
                if let Some(terminal) = self.terminalize_provider(generation, completion).await {
                    self.settle(
                        generation,
                        Err(error
                            .with_cleanup_result(interactions)
                            .with_cleanup_result(terminal.map(|_| ()))),
                    )
                    .await;
                }
                return;
            }
        };
        let route = if active {
            self.runtime
                .server
                .cancel(&self.runtime.conversation_id, generation)
                .map(|_| ())
        } else {
            Ok(())
        };
        self.deliver_timeout();
        let interactions = self
            .runtime
            .requests
            .cancel_conversation(
                &self.runtime.conversation_id,
                lifecycle_deadline,
                Some(lifecycle_deadline),
            )
            .await;
        let completion = execution.as_mut().await;
        if let Some(terminal) = self.terminalize_provider(generation, completion).await {
            self.settle(generation, route.and(interactions).and(terminal))
                .await;
        }
    }

    async fn fail_accepted(
        mut self,
        generation: u64,
        run: crate::backends::custom::server::runtime::CustomProviderRun<S, D, P, T>,
        error: KitError,
    ) {
        let lifecycle_deadline = self.runtime.server.lifecycle_deadline();
        let state = self.runtime.state.clone();
        let conversation_id = self.runtime.conversation_id.clone();
        let cancellation = self
            .await_operation(async move {
                state
                    .request_generation_cancellation(
                        &conversation_id,
                        generation,
                        CustomCancellationIntent::RuntimeFailure,
                        lifecycle_deadline,
                    )
                    .await
            })
            .await;
        let route = match &cancellation {
            Ok(true) => self
                .runtime
                .server
                .cancel(&self.runtime.conversation_id, generation)
                .map(|_| ()),
            Ok(false) => Ok(()),
            Err(_) => self
                .runtime
                .server
                .cancel(&self.runtime.conversation_id, generation)
                .map(|_| ()),
        };
        let interactions = self
            .runtime
            .requests
            .cancel_conversation(
                &self.runtime.conversation_id,
                lifecycle_deadline,
                Some(lifecycle_deadline),
            )
            .await;
        let completion = run.finish().await;
        if let Some(terminal) = self.terminalize_provider(generation, completion).await {
            self.settle(
                generation,
                Err(error
                    .with_cleanup_result(cancellation.map(|_| ()))
                    .with_cleanup_result(route)
                    .with_cleanup_result(interactions)
                    .with_cleanup_result(terminal.map(|_| ()))),
            )
            .await;
        }
    }

    async fn stop_generation(mut self, generation: u64, result: Result<PromptResponse, KitError>) {
        let decline = self
            .runtime
            .server
            .decline(&self.runtime.conversation_id, generation);
        let result = match result {
            Ok(response) => decline.map(|_| response),
            Err(primary) => Err(primary.with_cleanup_result(decline)),
        };
        if let Some(result) = self.terminalize(generation, None, result).await {
            self.settle(generation, result).await;
        }
    }

    async fn terminalize_provider(
        &mut self,
        generation: u64,
        terminal: Result<PromptResponse, KitError>,
    ) -> Option<Result<PromptResponse, KitError>> {
        match terminal {
            Ok(response) => {
                let final_text = response.output().to_owned();
                self.terminalize(generation, Some(final_text), Ok(response))
                    .await
            }
            Err(error) => self.terminalize(generation, None, Err(error)).await,
        }
    }

    async fn terminalize(
        &mut self,
        generation: u64,
        final_text: Option<String>,
        result: Result<PromptResponse, KitError>,
    ) -> Option<Result<PromptResponse, KitError>> {
        if self
            .runtime
            .fence
            .begin_terminalization(generation)
            .is_err()
        {
            return None;
        }
        let attempt = CustomTerminalAttempt::new(final_text);
        let mut retry_delay = Duration::from_millis(10);
        let lifecycle_deadline = self.runtime.server.lifecycle_deadline();
        let disposition = loop {
            let state = self.runtime.state.clone();
            let conversation_id = self.runtime.conversation_id.clone();
            let attempt = attempt.clone();
            let observed = self
                .await_operation(async move {
                    state
                        .finish_generation(
                            &conversation_id,
                            generation,
                            attempt,
                            lifecycle_deadline,
                        )
                        .await
                })
                .await;
            match observed {
                Ok(disposition) => break disposition,
                Err(error)
                    if error.kind() == crate::error::ErrorKind::Integration
                        || (error.kind() == crate::error::ErrorKind::Timeout
                            && Instant::now() < lifecycle_deadline) =>
                {
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = retry_delay.saturating_mul(2).min(Duration::from_secs(1));
                }
                Err(_) => return None,
            }
        };
        if self.runtime.fence.begin_publication(generation).is_err() {
            return None;
        }
        Some(match disposition {
            CustomContinuationDisposition::Ready => result,
            CustomContinuationDisposition::Sealed => Err(KitError::invalid_state(
                "conversation history exceeds the supported limit",
            )),
        })
    }

    async fn settle(mut self, generation: u64, result: Result<PromptResponse, KitError>) {
        let result = self
            .admission
            .take()
            .expect("prompt task owns its hierarchy admission")
            .publish(result);
        match &mut self.observation {
            CustomPromptObservation::Response {
                sender,
                acknowledgement,
            } => {
                if let Some(sender) = sender.take()
                    && sender
                        .send(CustomPromptDelivery::Terminal {
                            generation: Some(generation),
                            result,
                        })
                        .is_ok()
                {
                    self.delivered = acknowledgement.await.is_ok();
                }
            }
            CustomPromptObservation::Stream { terminal, .. } => {
                *terminal = Some(result);
            }
        }
        let runtime = self.runtime.clone();
        let delivered = self.delivered;
        let settlement = self
            .await_operation(async move {
                runtime.fence.record_settlement(generation, delivered)?;
                let mut retry_delay = Duration::from_millis(10);
                let lifecycle_deadline = runtime.server.lifecycle_deadline();
                loop {
                    match runtime
                        .state
                        .settle_generation_waiter(
                            &runtime.conversation_id,
                            generation,
                            delivered,
                            lifecycle_deadline,
                        )
                        .await
                    {
                        Ok(()) => {
                            runtime.fence.clear_settlement(generation, delivered);
                            return Ok(());
                        }
                        Err(error)
                            if error.kind() == crate::error::ErrorKind::Integration
                                || (error.kind() == crate::error::ErrorKind::Timeout
                                    && Instant::now() < lifecycle_deadline) =>
                        {
                            tokio::time::sleep(retry_delay).await;
                            retry_delay = retry_delay.saturating_mul(2).min(Duration::from_secs(1));
                        }
                        Err(error) => return Err(error),
                    }
                }
            })
            .await;
        self.observe_deadline();
        if let CustomPromptObservation::Stream { output, terminal } = &mut self.observation {
            let acknowledgement = self
                .runtime
                .server
                .acknowledge_prompt(&self.runtime.conversation_id, Some(generation));
            if let Some(result) = terminal.take() {
                let mut result = result;
                for cleanup in [settlement, acknowledgement] {
                    if let Err(error) = cleanup {
                        result = Err(match result {
                            Ok(_) => error,
                            Err(primary) => primary.with_supplementary(
                                error.in_phase(crate::error::ErrorPhase::Settlement),
                            ),
                        });
                    }
                }
                output.finish(result);
            }
        }
    }

    fn finish_starting(mut self, result: Result<PromptResponse, KitError>) {
        self.runtime.fence.clear_starting();
        let result = self
            .admission
            .take()
            .expect("prompt task owns its hierarchy admission")
            .publish(result);
        match &mut self.observation {
            CustomPromptObservation::Response { sender, .. } => {
                if let Some(sender) = sender.take() {
                    let _ = sender.send(CustomPromptDelivery::Terminal {
                        generation: None,
                        result,
                    });
                }
            }
            CustomPromptObservation::Stream { output, .. } => {
                let acknowledgement = self
                    .runtime
                    .server
                    .acknowledge_prompt(&self.runtime.conversation_id, None);
                output.finish(match (result, acknowledgement) {
                    (result, Ok(())) => result,
                    (Ok(_), Err(error)) => Err(error),
                    (Err(primary), Err(error)) => Err(primary
                        .with_supplementary(error.in_phase(crate::error::ErrorPhase::Settlement))),
                });
            }
        }
    }

    fn deliver_timeout(&mut self) {
        self.deadline_elapsed = true;
        let deadline_error = self.deadline_error();
        match &mut self.observation {
            CustomPromptObservation::Response { sender, .. } => {
                if let Some(sender) = sender.take() {
                    let _ = sender.send(CustomPromptDelivery::Immediate(Err(deadline_error)));
                }
            }
            CustomPromptObservation::Stream { output, terminal } => {
                let error = terminal
                    .take()
                    .and_then(Result::err)
                    .unwrap_or_else(|| deadline_error.clone());
                output.finish(Err(error));
            }
        }
    }

    fn observe_deadline(&mut self) {
        if !self.deadline_elapsed
            && self
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
        {
            self.deliver_timeout();
        }
    }

    fn starting_error(&mut self) -> Option<KitError> {
        self.observe_deadline();
        if self.deadline_elapsed {
            Some(self.deadline_error())
        } else if self.runtime.fence.starting_cancelled() {
            Some(KitError::cancelled(
                "prompt was cancelled before model acceptance",
            ))
        } else {
            None
        }
    }

    fn deadline_error(&self) -> KitError {
        KitError::timeout("prompt deadline elapsed")
            .with_context(
                crate::error::Operation::Prompt,
                crate::error::ErrorPhase::ModelGeneration,
            )
            .with_detail(crate::error::ErrorDetail::DeadlineElapsed {
                kind: crate::error::DeadlineKind::Operation,
                timeout: self.timeout,
            })
    }
}

impl CustomGenerationFence {
    fn start(&self) -> Result<(), CustomGenerationState> {
        let mut state = self.state();
        match *state {
            CustomGenerationState::Idle => {
                *state = CustomGenerationState::Starting {
                    cancellation_requested: false,
                };
            }
            active => return Err(active),
        }
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    fn activate(&self, generation: u64) -> Result<bool, KitError> {
        if generation == 0 {
            return Err(KitError::invalid_state("prompt identity is invalid"));
        }
        let mut state = self.state();
        let cancellation_requested = match *state {
            CustomGenerationState::Starting {
                cancellation_requested,
            } => cancellation_requested,
            CustomGenerationState::Idle => {
                return Err(KitError::invalid_state(
                    "prompt setup state is inconsistent",
                ));
            }
            CustomGenerationState::Active { .. }
            | CustomGenerationState::Terminalizing { .. }
            | CustomGenerationState::Publishing { .. }
            | CustomGenerationState::Settling { .. } => {
                return Err(KitError::busy("conversation already has an active prompt"));
            }
        };
        *state = CustomGenerationState::Active { generation };
        drop(state);
        self.changed.notify_waiters();
        Ok(cancellation_requested)
    }

    fn starting_cancelled(&self) -> bool {
        matches!(
            *self.state(),
            CustomGenerationState::Starting {
                cancellation_requested: true
            }
        )
    }

    fn request_cancellation(&self) -> (CustomGenerationState, watch::Receiver<()>) {
        let mut state = self.state();
        // Subscribe under the same lock that publishes terminal transitions.
        let terminal = self.terminal.subscribe();
        if let CustomGenerationState::Starting {
            cancellation_requested,
        } = &mut *state
        {
            *cancellation_requested = true;
        }
        let target = *state;
        drop(state);
        self.changed.notify_waiters();
        (target, terminal)
    }

    fn begin_terminalization(&self, generation: u64) -> Result<(), KitError> {
        let mut state = self.state();
        match *state {
            CustomGenerationState::Active { generation: active } if active == generation => {
                *state = CustomGenerationState::Terminalizing { generation };
            }
            _ => {
                return Err(KitError::invalid_state(
                    "prompt terminalization state is inconsistent",
                ));
            }
        }
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    fn begin_publication(&self, generation: u64) -> Result<(), KitError> {
        let mut state = self.state();
        match *state {
            CustomGenerationState::Terminalizing { generation: active } if active == generation => {
                *state = CustomGenerationState::Publishing { generation };
                self.terminal.send_replace(());
            }
            _ => {
                return Err(KitError::invalid_state(
                    "prompt publication state is inconsistent",
                ));
            }
        }
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    fn record_settlement(&self, generation: u64, delivered: bool) -> Result<(), KitError> {
        let mut state = self.state();
        match *state {
            CustomGenerationState::Publishing { generation: active } if active == generation => {
                *state = CustomGenerationState::Settling {
                    generation,
                    delivered,
                };
            }
            _ => {
                return Err(KitError::invalid_state(
                    "prompt settlement state is inconsistent",
                ));
            }
        }
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    fn clear_settlement(&self, generation: u64, delivered: bool) {
        let mut state = self.state();
        if matches!(
            *state,
            CustomGenerationState::Settling {
                generation: active,
                delivered: observed,
            } if active == generation && observed == delivered
        ) {
            *state = CustomGenerationState::Idle;
        }
        drop(state);
        self.changed.notify_waiters();
    }

    fn clear_starting(&self) {
        let mut state = self.state();
        if matches!(*state, CustomGenerationState::Starting { .. }) {
            *state = CustomGenerationState::Idle;
            self.terminal.send_replace(());
        }
        drop(state);
        self.changed.notify_waiters();
    }

    fn target(&self) -> CustomGenerationState {
        *self.state()
    }

    async fn await_change(
        &self,
        expected: CustomGenerationState,
        deadline: Instant,
    ) -> Result<(), KitError> {
        let mut changed = std::pin::pin!(self.changed.notified());
        changed.as_mut().enable();
        if self.target() != expected {
            return Ok(());
        }
        tokio::time::timeout_at(deadline, changed)
            .await
            .map_err(|_| KitError::timeout("prompt settlement did not converge"))
    }

    fn state(&self) -> MutexGuard<'_, CustomGenerationState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
