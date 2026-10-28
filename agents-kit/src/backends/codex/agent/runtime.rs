use crate::error::ErrorPhase;
use std::collections::BTreeSet;
use std::time::Duration;
use tools_kit::ToolsetBackend;

use storage_kit::StateStoreBackend;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::backends::codex::conversations::runtime::CodexConversationRuntime;
use crate::backends::codex::notifications::CodexNotificationAction;
use crate::backends::codex::protocol::inbound::CodexInbound;
use crate::backends::codex::protocol::notifications::CodexServerNotification;
use crate::backends::codex::protocol::responses::CodexServerResponse;
use crate::backends::codex::protocol::{CodexInboundReceiver, CodexProtocol};
use crate::backends::codex::server_requests::{CodexServerAction, CodexServerRendering};
use crate::backends::codex::state_store::CodexStateStore;
use crate::backends::codex::state_store::records::CodexToolAdmission;
use crate::backends::codex::tools::{CodexDynamicToolCall, CodexTools};
use crate::core::hierarchy::HierarchyRuntime;
use crate::{AgentId, CodexConfig, KitError};

const MAX_CODEX_ANCESTRY_DEPTH: usize = 256;

pub(crate) struct CodexRuntime<S: StateStoreBackend> {
    protocol: CodexProtocol,
    state: CodexStateStore<S>,
    stop: Option<oneshot::Sender<()>>,
    events: JoinHandle<Result<(), KitError>>,
}

impl<S: StateStoreBackend> Drop for CodexRuntime<S> {
    fn drop(&mut self) {
        self.state
            .fail_prompt_observers(&KitError::closed("prompt runtime was dropped"));
        self.state.invalidate_runtime();
        self.state.abort_prompt_tasks();
        self.events.abort();
    }
}

impl<S: StateStoreBackend> CodexRuntime<S> {
    pub(crate) async fn connect<T: ToolsetBackend>(
        config: &CodexConfig,
        agent_id: AgentId,
        state: CodexStateStore<S>,
        hierarchy: HierarchyRuntime,
        tools: CodexTools<S, T>,
    ) -> Result<Self, KitError> {
        let (protocol, inbound) = CodexProtocol::connect(config).await?;
        if let Err(error) = state.cancel_lost_continuations(&agent_id).await {
            let cleanup = protocol
                .clone()
                .shutdown(config.mode().request_timeout())
                .await;
            return Err(match cleanup {
                Ok(()) => error,
                Err(cleanup) => error.with_supplementary(cleanup.in_phase(ErrorPhase::Cleanup)),
            });
        }
        let (stop, stop_receiver) = oneshot::channel();
        let event_protocol = protocol.clone();
        let event_state = state.clone();
        let event_hierarchy = hierarchy.clone();
        let event_tools = tools.clone();
        let events = tokio::spawn(async move {
            let result = Self::run_events(
                agent_id,
                event_state.clone(),
                event_protocol,
                inbound,
                stop_receiver,
                event_hierarchy.clone(),
                event_tools,
            )
            .await;
            if let Err(error) = &result {
                event_state.fail_prompt_observers(error);
                event_hierarchy.invalidate();
                event_state.invalidate_runtime();
            }
            result
        });
        Ok(Self {
            protocol,
            state,
            stop: Some(stop),
            events,
        })
    }

    pub(crate) fn protocol(&self) -> CodexProtocol {
        self.protocol.clone()
    }

    pub(crate) async fn shutdown(mut self, timeout: Duration) -> Result<(), KitError> {
        let deadline = tokio::time::Instant::now() + timeout;
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let mut first_error = match tokio::time::timeout_at(deadline, &mut self.events).await {
            Ok(Ok(Ok(()))) => None,
            Ok(Ok(Err(error))) => Some(error),
            Ok(Err(_)) => Some(KitError::integration("event owner failed")),
            Err(_) => {
                self.events.abort();
                let _ = (&mut self.events).await;
                Some(KitError::timeout("event owner did not stop"))
            }
        };
        self.state.invalidate_runtime();
        if let Err(error) = self.state.join_prompt_tasks(deadline).await {
            first_error = Some(match first_error.take() {
                Some(primary) => primary.with_supplementary(error.in_phase(ErrorPhase::Cleanup)),
                None => error,
            });
        }
        if let Err(error) = self
            .protocol
            .clone()
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await
        {
            first_error = Some(match first_error.take() {
                Some(primary) => primary.with_supplementary(error.in_phase(ErrorPhase::Cleanup)),
                None => error,
            });
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn run_events<T: ToolsetBackend>(
        agent_id: AgentId,
        state: CodexStateStore<S>,
        protocol: CodexProtocol,
        mut inbound: CodexInboundReceiver,
        mut stop: oneshot::Receiver<()>,
        hierarchy: HierarchyRuntime,
        tools: CodexTools<S, T>,
    ) -> Result<(), KitError> {
        let mut tool_work = tokio::task::JoinSet::new();
        let result = loop {
            tokio::select! {
                _ = &mut stop => break Ok(()),
                result = state.prompt_task_exit() => {
                    if let Err(error) = result { break Err(error); }
                }
                completed = tool_work.join_next_with_id(), if !tool_work.is_empty() => {
                    let Some(completed) = completed else {
                        break Err(KitError::integration("managed tool work owner ended unexpectedly"));
                    };
                    match completed {
                        Ok((id, rendering)) => {
                            let admission = match tools.response_admission(id) {
                                Ok(admission) => admission,
                                Err(error) => break Err(error),
                            };
                            let response = if admission.is_some() {
                                match rendering {
                                    Some(rendering) => protocol
                                        .respond(CodexServerResponse::from_rendering(rendering))
                                        .await,
                                    None => Ok(()),
                                }
                            } else {
                                Ok(())
                            };
                            drop(admission);
                            let finished = tools.finish_task(id);
                            if let Err(error) = response.and(finished) {
                                break Err(error);
                            }
                        }
                        Err(error) => {
                            let id = error.id();
                            let expected = error.is_cancelled()
                                && tools.task_cancellation_expected(id);
                            if let Err(error) = tools.finish_task(id) {
                                break Err(error);
                            }
                            if !expected {
                                break Err(KitError::integration("managed tool work task failed"));
                            }
                        }
                    }
                }
                inbound = inbound.recv() => {
                    let inbound = match inbound {
                        Some(Ok(inbound)) => inbound,
                        Some(Err(error)) => break Err(error),
                        None => break Err(KitError::integration(
                            "inbound event stream ended unexpectedly",
                        )),
                    };
                    match inbound {
                        CodexInbound::Request(request) => {
                            let request_id = request.request_id().clone();
                            let action = match CodexServerAction::from_wire(request) {
                                Ok(action) => action,
                                Err(error) => {
                                    if let Err(error) = protocol
                                        .respond(CodexServerResponse::from_rendering(
                                            CodexServerRendering::from_error(request_id, &error),
                                        ))
                                        .await
                                    {
                                        break Err(error);
                                    }
                                    continue;
                                }
                            };
                            let (request_id, call) = match action {
                                CodexServerAction::DynamicTool { request_id, call } => {
                                    (request_id, call)
                                }
                                action => {
                                    let rendering = Self::apply_request(
                                        &agent_id,
                                        &state,
                                        &protocol,
                                        &tools,
                                        action,
                                    )
                                    .await;
                                    let rendering = match rendering {
                                        Ok(rendering) => rendering,
                                        Err(error) => Some(CodexServerRendering::from_error(
                                            request_id,
                                            &error,
                                        )),
                                    };
                                    if let Some(rendering) = rendering
                                        && let Err(error) = protocol
                                            .respond(CodexServerResponse::from_rendering(rendering))
                                            .await
                                    {
                                        break Err(error);
                                    }
                                    continue;
                                }
                            };
                            let generation = call.generation();
                            let runtime = match Self::conversation(
                                &agent_id,
                                &state,
                                &protocol,
                                &tools,
                                call.thread_id(),
                            )
                            .await
                            .and_then(|runtime| {
                                runtime.ok_or_else(|| {
                                    KitError::invalid_state(
                                        "tool request has no durable conversation owner",
                                    )
                                })
                            }) {
                                Ok(runtime) => runtime,
                                Err(error) => {
                                    let admission = match tools
                                        .generation_response_admission(&generation)
                                    {
                                        Ok(admission) => admission,
                                        Err(error) => break Err(error),
                                    };
                                    if admission.is_some() {
                                        let response = protocol
                                            .respond(CodexServerResponse::from_rendering(
                                                CodexServerRendering::from_error(
                                                    request_id,
                                                    &error,
                                                ),
                                            ))
                                            .await;
                                        drop(admission);
                                        if let Err(error) = response {
                                            break Err(error);
                                        }
                                    }
                                    continue;
                                }
                            };
                            let tool_admission = match state
                                .observe_prompt_turn(runtime.conversation_id(), call.turn_id())
                                .await
                            {
                                Ok(admission) => admission,
                                Err(error) => {
                                    let admission = match tools
                                        .generation_response_admission(&generation)
                                    {
                                        Ok(admission) => admission,
                                        Err(error) => break Err(error),
                                    };
                                    if admission.is_some() {
                                        let response = protocol
                                            .respond(CodexServerResponse::from_rendering(
                                                CodexServerRendering::from_error(
                                                    request_id,
                                                    &error,
                                                ),
                                            ))
                                            .await;
                                        drop(admission);
                                        if let Err(error) = response {
                                            break Err(error);
                                        }
                                    }
                                    continue;
                                }
                            };
                            if tool_admission == CodexToolAdmission::Suppress {
                                continue;
                            }
                            if tool_work.len() >= tools.work_limit() {
                                let error = KitError::busy(
                                    "managed tool concurrency is saturated",
                                );
                                let admission = match tools
                                    .generation_response_admission(&generation)
                                {
                                    Ok(admission) => admission,
                                    Err(error) => break Err(error),
                                };
                                if admission.is_some() {
                                    let response = protocol
                                        .respond(CodexServerResponse::from_rendering(
                                            CodexServerRendering::from_error(request_id, &error),
                                        ))
                                        .await;
                                    drop(admission);
                                    if let Err(error) = response {
                                        break Err(error);
                                    }
                                }
                                continue;
                            }
                            let workspace_id = runtime.binding().workspace_id().clone();
                            let conversation_id = runtime.conversation_id().clone();
                            let (start, started) = oneshot::channel();
                            let abort = tool_work.spawn(async move {
                                if started.await.is_err() {
                                    return None;
                                }
                                Self::apply_dynamic_tool(runtime, request_id, call).await
                            });
                            if let Err(error) = tools.register_task(
                                generation,
                                workspace_id,
                                conversation_id,
                                abort,
                                start,
                            ) {
                                break Err(error);
                            }
                        }
                        inbound => {
                            if let Err(error) = Self::apply_inbound(
                                &agent_id,
                                &state,
                                &protocol,
                                &hierarchy,
                                &tools,
                                inbound,
                            )
                            .await
                            {
                                break Err(error);
                            }
                        }
                    }
                }
            }
        };
        tool_work.abort_all();
        while let Some(completed) = tool_work.join_next_with_id().await {
            let id = match completed {
                Ok((id, _)) => id,
                Err(error) => error.id(),
            };
            let _ = tools.finish_task(id);
        }
        let cancellation_error = tools.cancel_all(tools.cancellation_deadline()).await.err();
        match (result, cancellation_error) {
            (Err(error), Some(cleanup)) => {
                Err(error.with_supplementary(cleanup.in_phase(ErrorPhase::Cleanup)))
            }
            (Err(error), None) | (Ok(()), Some(error)) => Err(error),
            (Ok(()), None) => Ok(()),
        }
    }

    async fn apply_dynamic_tool<T: ToolsetBackend>(
        runtime: CodexConversationRuntime<S, T>,
        request_id: serde_json::Value,
        call: CodexDynamicToolCall,
    ) -> Option<CodexServerRendering> {
        let result = runtime.execute_tool(call).await.map(|response| {
            Some(CodexServerRendering::dynamic_tool(
                request_id.clone(),
                response,
            ))
        });
        match result {
            Ok(rendering) => rendering,
            Err(error) => Some(CodexServerRendering::from_error(request_id, &error)),
        }
    }

    async fn apply_inbound<T: ToolsetBackend>(
        agent_id: &AgentId,
        state: &CodexStateStore<S>,
        protocol: &CodexProtocol,
        hierarchy: &HierarchyRuntime,
        tools: &CodexTools<S, T>,
        inbound: CodexInbound,
    ) -> Result<(), KitError> {
        match inbound {
            CodexInbound::MalformedRequest { request_id, error } => {
                protocol
                    .respond(CodexServerResponse::from_rendering(
                        CodexServerRendering::from_error(request_id, &error),
                    ))
                    .await
            }
            CodexInbound::Request(_) => Err(KitError::invalid_state(
                "request bypassed the bounded request scheduler",
            )),
            CodexInbound::Notification(notification) => {
                Self::apply_notification(agent_id, state, protocol, hierarchy, tools, notification)
                    .await
            }
        }
    }

    async fn apply_request<T: ToolsetBackend>(
        agent_id: &AgentId,
        state: &CodexStateStore<S>,
        protocol: &CodexProtocol,
        tools: &CodexTools<S, T>,
        action: CodexServerAction,
    ) -> Result<Option<CodexServerRendering>, KitError> {
        match action {
            CodexServerAction::Interaction(request) => {
                let runtime =
                    Self::conversation(agent_id, state, protocol, tools, request.thread_id())
                        .await?
                        .ok_or_else(|| {
                            KitError::invalid_state(
                                "server request has no durable conversation owner",
                            )
                        })?;
                runtime.publish_interaction(request).await.map(|()| None)
            }
            CodexServerAction::DynamicTool { request_id, call } => {
                let _ = (request_id, call);
                Err(KitError::invalid_state(
                    "dynamic tool request bypassed its runtime work owner",
                ))
            }
            CodexServerAction::CurrentTime {
                request_id,
                thread_id,
            } => {
                Self::conversation(agent_id, state, protocol, tools, &thread_id)
                    .await?
                    .ok_or_else(|| {
                        KitError::invalid_state("time request has no durable conversation owner")
                    })?;
                CodexServerRendering::current_time(request_id).map(Some)
            }
            CodexServerAction::Reply(rendering) => Ok(Some(rendering)),
        }
    }

    async fn apply_notification<T: ToolsetBackend>(
        agent_id: &AgentId,
        state: &CodexStateStore<S>,
        protocol: &CodexProtocol,
        hierarchy: &HierarchyRuntime,
        tools: &CodexTools<S, T>,
        notification: CodexServerNotification,
    ) -> Result<(), KitError> {
        match CodexNotificationAction::from_wire(notification)? {
            CodexNotificationAction::OutputDelta(delta) => {
                let Some((_, binding)) = state
                    .conversation_for_thread(agent_id, delta.thread_id())
                    .await?
                else {
                    return Ok(());
                };
                state
                    .observe_prompt_delta(binding.conversation_id(), delta.turn_id(), delta.text())
                    .await
            }
            CodexNotificationAction::PromptOutput(output) => {
                let runtime = Self::required_conversation(
                    agent_id,
                    state,
                    protocol,
                    tools,
                    output.thread_id(),
                )
                .await?;
                runtime.observe_prompt_output(&output).await
            }
            CodexNotificationAction::PromptTerminal(terminal) => {
                let runtime = Self::required_conversation(
                    agent_id,
                    state,
                    protocol,
                    tools,
                    terminal.thread_id(),
                )
                .await?;
                runtime.observe_terminal(terminal).await
            }
            CodexNotificationAction::RequestResolved {
                thread_id,
                request_id,
            } => {
                let _ = (thread_id, request_id);
                Ok(())
            }
            CodexNotificationAction::ThreadDeleted { thread_id } => {
                if let Some((conversation, binding)) =
                    state.conversation_for_thread(agent_id, &thread_id).await?
                {
                    state
                        .reconcile_deleted_thread(&conversation, &binding)
                        .await?;
                    hierarchy.close_conversation(conversation.workspace_id(), conversation.id());
                }
                Ok(())
            }
            CodexNotificationAction::ThreadStarted {
                thread_id,
                thread_source,
            } => {
                if let Some(thread_source) = thread_source.as_deref()
                    && state
                        .observe_thread_creation(agent_id, thread_source, &thread_id)
                        .await?
                {
                    return Ok(());
                }
                Self::required_conversation(agent_id, state, protocol, tools, &thread_id)
                    .await
                    .map(|_| ())
            }
            CodexNotificationAction::Publish { thread_id, kind } => {
                let runtime =
                    Self::required_conversation(agent_id, state, protocol, tools, &thread_id)
                        .await?;
                runtime.publish_notification(kind).await
            }
            CodexNotificationAction::HistoryReverted { thread_id } => {
                if Self::conversation(agent_id, state, protocol, tools, &thread_id)
                    .await?
                    .is_none()
                {
                    return Ok(());
                }
                Err(
                    KitError::invalid_state("owned conversation history was replaced")
                        .with_detail(crate::error::ErrorDetail::AttachmentMismatch),
                )
            }
            CodexNotificationAction::Observe { scope } => {
                use crate::backends::codex::protocol::notifications::CodexScope;
                match scope {
                    CodexScope::Connection => Ok(()),
                    CodexScope::Thread { thread_id } => {
                        Self::conversation(agent_id, state, protocol, tools, &thread_id)
                            .await
                            .map(|_| ())
                    }
                    CodexScope::Turn { thread_id, turn_id } => {
                        let Some(runtime) =
                            Self::conversation(agent_id, state, protocol, tools, &thread_id)
                                .await?
                        else {
                            return Ok(());
                        };
                        runtime.observe_turn_metadata(&thread_id, &turn_id).await
                    }
                }
            }
            CodexNotificationAction::State { thread_id } => {
                if let Some(thread_id) = thread_id.as_deref() {
                    Self::required_conversation(agent_id, state, protocol, tools, thread_id)
                        .await?;
                }
                Ok(())
            }
            CodexNotificationAction::Unsupported { thread_id } => {
                if let Some(thread_id) = thread_id.as_deref() {
                    Self::required_conversation(agent_id, state, protocol, tools, thread_id)
                        .await?;
                }
                Err(KitError::unsupported(
                    "emitted behavior outside the portable runtime",
                ))
            }
            CodexNotificationAction::Diagnostic { thread_id, method } => {
                if let Some(thread_id) = thread_id.as_deref() {
                    Self::required_conversation(agent_id, state, protocol, tools, thread_id)
                        .await?;
                }
                let _ = method;
                Ok(())
            }
        }
    }

    async fn required_conversation<T: ToolsetBackend>(
        agent_id: &AgentId,
        state: &CodexStateStore<S>,
        protocol: &CodexProtocol,
        tools: &CodexTools<S, T>,
        thread_id: &str,
    ) -> Result<CodexConversationRuntime<S, T>, KitError> {
        Self::conversation(agent_id, state, protocol, tools, thread_id)
            .await?
            .ok_or_else(|| KitError::invalid_state("event has no durable conversation owner"))
    }

    async fn conversation<T: ToolsetBackend>(
        agent_id: &AgentId,
        state: &CodexStateStore<S>,
        protocol: &CodexProtocol,
        tools: &CodexTools<S, T>,
        thread_id: &str,
    ) -> Result<Option<CodexConversationRuntime<S, T>>, KitError> {
        let mut current = thread_id.to_owned();
        let mut visited = BTreeSet::new();
        for _ in 0..MAX_CODEX_ANCESTRY_DEPTH {
            if !visited.insert(current.clone()) {
                return Err(KitError::protocol(
                    "runtime conversation ancestry contains a cycle",
                ));
            }
            if let Some((_, binding)) = state.conversation_for_thread(agent_id, &current).await? {
                return Ok(Some(CodexConversationRuntime::new(
                    binding,
                    state.clone(),
                    protocol.clone(),
                    tools.clone(),
                )));
            }
            let Some(parent) = protocol.parent_thread_id(&current).await? else {
                return Ok(None);
            };
            current = parent;
        }
        Err(KitError::integration(
            "runtime conversation ancestry exceeds the private bound",
        ))
    }
}
