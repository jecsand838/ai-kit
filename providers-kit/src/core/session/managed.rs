use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use futures_util::StreamExt;

use crate::KitError;
use crate::error::{ErrorKind, ErrorPhase, Operation, ValidationViolation};
use crate::generation::{
    ContentPart, FinishReason, FunctionResult, GenerateEvent, GenerateRequest, GenerateResponse,
    GenerationContinuation, Message, RetainedSize, Role,
};
use crate::models::ModelBackend;
use crate::provider::ProviderRuntime;
use crate::session::{
    CancelTarget, CancellationResolution, CommandId, ResponseId, ResponseOutcome, SessionCommand,
    SessionCommands, SessionConfig, SessionContinuity, SessionEvent, SessionProfile, UsageScope,
};

use super::runtime::{SessionControl, SessionEventPort};

pub(crate) struct ManagedConversation<M: ModelBackend<Continuation = GenerationContinuation>> {
    model: Arc<M>,
    control: Arc<SessionControl>,
    state: Arc<Mutex<ManagedState>>,
    timeout: Duration,
}

struct ManagedState {
    history: Option<GenerateRequest>,
    active: bool,
    required: BTreeSet<crate::session::ToolRequestId>,
}

pub(crate) struct ManagedCommands<'a, M: ModelBackend<Continuation = GenerationContinuation>> {
    owner: &'a ManagedConversation<M>,
    check: fn(&crate::ModelId, &FunctionResult) -> Result<(), KitError>,
}

impl<M: ModelBackend<Continuation = GenerationContinuation>> ManagedConversation<M> {
    pub(crate) fn new(
        model: M,
        config: SessionConfig,
        timeout: Duration,
    ) -> Result<Self, KitError> {
        if config.profile() != SessionProfile::Conversation
            || config.input_audio_format().is_some()
            || config.output_audio_format().is_some()
            || config.has_turn_detection()
        {
            return Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "session.config",
                    rule: "Conversation does not support realtime media or activity",
                },
                "Conversation sessions do not support realtime media or activity configuration",
            ));
        }
        // The fixed config is retained by control and copied once into the
        // authoritative request. Reserve both before accepting any user input.
        let mut retained = RetainedSize::new();
        retained.session_config(&config)?;
        let control = SessionControl::new(config, timeout)?;
        control.reserve_history(retained.bytes())?;
        Ok(Self {
            model: Arc::new(model),
            control,
            state: Arc::new(Mutex::new(ManagedState {
                history: None,
                active: false,
                required: BTreeSet::new(),
            })),
            timeout,
        })
    }

    pub(crate) fn control(&self) -> &Arc<SessionControl> {
        &self.control
    }
    pub(crate) fn model_id(&self) -> &crate::ModelId {
        self.model.model_id()
    }
    pub(crate) fn split(
        &mut self,
        check: fn(&crate::ModelId, &FunctionResult) -> Result<(), KitError>,
    ) -> (ManagedCommands<'_, M>, SessionEventPort<'_>) {
        (
            ManagedCommands { owner: self, check },
            SessionEventPort::new(&self.control),
        )
    }
    pub(crate) fn bind_provider(&self, provider: Arc<ProviderRuntime>) -> Result<(), KitError> {
        self.control.bind_provider(provider)
    }
    pub(crate) async fn close(self) -> Result<(), KitError> {
        self.control.close().await
    }

    fn state(&self) -> MutexGuard<'_, ManagedState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn request(
        &self,
        message: Message<GenerationContinuation>,
    ) -> Result<GenerateRequest, KitError> {
        let config = self.control.config();
        let mut messages = Vec::new();
        if let Some(instructions) = config.instructions() {
            messages.push(Message::system(vec![ContentPart::Text {
                text: instructions.into(),
            }])?);
        }
        messages.push(message);
        let mut request = GenerateRequest::new(messages)?
            .with_tools(config.tools().to_vec())?
            .with_generation(config.generation().clone())?;
        if let Some(format) = config.response_format() {
            request = request.with_response_format(format.clone());
        }
        if let Some(reasoning) = config.reasoning() {
            request = request.with_reasoning(reasoning.clone());
        }
        Ok(request)
    }

    fn start(&self, command: CommandId, request: GenerateRequest) -> Result<(), KitError> {
        let model = Arc::clone(&self.model);
        let control = Arc::clone(&self.control);
        let state = Arc::clone(&self.state);
        let timeout = self.timeout;
        self.control.spawn(async move {
            let admission=match control.generation_admission(){
                Ok(admission)=>admission,
                Err(error)=>{
                    let mut state=state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);state.history=Some(request);
                    control.publish(SessionEvent::CommandFailed{command,error,not_started:true})?;
                    Self::settle_controls(&control,control.continuity())?;
                    state.active=false;drop(state);
                    if control.is_sealed(){control.terminate(KitError::closed("provider sealed before session work began"),SessionContinuity::Closed);}
                    return Ok(());
                }
            };
            let response=control.next_response()?;
            control.publish(SessionEvent::ResponseStarted{command:Some(command.clone()),response:response.clone()})?;
            let mut request=request;
            let mut cancellation=None;
            let mut closing=false;
            let result={
                let drive=Self::drive(&*model,&request,&control,&response);
                tokio::pin!(drive);
                let deadline=tokio::time::sleep(timeout);
                tokio::pin!(deadline);
                loop {
                    tokio::select! {
                        biased;
                        _=control.stopped()=>{closing=true;break Err(KitError::closed("session closed during generation"));},
                        accepted=control.next_command()=>{
                            let Some((id,incoming))=accepted else {closing=true;break Err(KitError::closed("session commands drained"));};
                            match incoming {
                                SessionCommand::Cancel(target)=>{
                                    let matches=match &target{CancelTarget::Command(target)=>target==&command,CancelTarget::Response(target)=>target==&response};
                                    if matches {cancellation=Some((id,target));break Err(KitError::new(ErrorKind::Cancelled,Operation::SessionSend,ErrorPhase::SessionReceive));}
                                    control.publish(SessionEvent::Cancellation{command:id,target,resolution:CancellationResolution::AlreadySettled,continuity:SessionContinuity::Reusable})?;
                                },
                                _=>{control.publish(SessionEvent::CommandFailed{command:id,error:KitError::invalid_state("managed generation accepts only cancellation while active"),not_started:true})?;}
                            }
                        },
                        result=&mut drive=>break result,
                        _=&mut deadline=>break Err(KitError::new(ErrorKind::Timeout,Operation::SessionReceive,ErrorPhase::SessionReceive).with_timeout(timeout)),
                    }
                }
            };
            closing |= control.is_sealed();
            let mut requested=Vec::new();
            let (outcome,output)=match result {
                Ok(result)=>{
                    let outcome=Self::outcome(result.finish_reason.as_ref());
                    let complete=matches!(outcome,ResponseOutcome::Completed);
                    let mut retained=RetainedSize::new();retained.message(&result.message)?;
                    if let Some(replay)=result.message.continuation(){retained.add(replay.retained_bytes()?)?;}
                    retained.response(&result)?;
                    retained.add(std::mem::size_of::<SessionEvent>())?;
                    if let ResponseOutcome::Failed(error)|ResponseOutcome::Unresolved(error)=&outcome{retained.error(error)?;}
                    let calls=if complete{result.message.function_calls().len()}else{0};
                    if complete {for call in result.message.function_calls(){
                        retained.call(call)?; // Executable event payload.
                        retained.call(call)?; // Retained tool obligation.
                        retained.add(std::mem::size_of::<SessionEvent>())?;
                    }}
                    let events=calls.checked_add(1).ok_or_else(||KitError::invalid_state("completion observation count overflow"))?;
                    control.reserve_completion(retained.bytes(),events)?;
                    let (assistant,output)=result.into_session_parts();
                    let mut size=RetainedSize::new();size.message(&assistant)?;
                    if let Some(replay)=assistant.continuation(){size.add(replay.retained_bytes()?)?;}
                    control.reserve_history(size.bytes())?;
                    if complete {for call in assistant.function_calls(){requested.push((control.next_tool()?,call.clone()));}}
                    request.append_session(assistant);
                    (outcome,Some(Box::new(output)))
                },
                Err(error)=>{
                    (if cancellation.is_some(){ResponseOutcome::Cancelled}else{Self::failure(error)},None)
                },
            };
            let uncertain=output.as_ref().is_some_and(|output|matches!(outcome,ResponseOutcome::Unresolved(_)) || (!matches!(outcome,ResponseOutcome::Completed) && !output.message.function_calls().is_empty()));
            let mut state=state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            state.history=Some(request);
            for (request,_) in &requested {state.required.insert(request.clone());}
            // Authoritative assistant ownership is committed before any executable
            // tool or observational projection is made visible.
            for (request,call) in requested {control.publish(SessionEvent::ToolRequested{response:response.clone(),request,call})?;}
            let continuity=if closing{SessionContinuity::Closed}else if uncertain{SessionContinuity::Uncertain}else{SessionContinuity::Reusable};
            control.publish(SessionEvent::ResponseFinished{response:response.clone(),outcome,continuity,output})?;
            control.finish_completion();
            if let Some((command,target))=cancellation {
                control.invalidate_response(&response)?;
                control.publish(SessionEvent::Cancellation{command,target,resolution:CancellationResolution::LocallyStopped,continuity})?;
            }
            // Cancel admission holds the same state guard while deciding whether
            // this worker or the idle submitter owns its receipt. Drain before
            // releasing active ownership; no accepted control can fall between.
            Self::settle_controls(&control,continuity)?;
            state.active=false;
            drop(state);
            drop(admission);
            if closing {control.terminate(KitError::closed("session generation stopped during closure"),SessionContinuity::Closed);}
            Ok(())
        })
    }

    fn outcome(reason: Option<&FinishReason>) -> ResponseOutcome {
        match reason {
            Some(FinishReason::Stop | FinishReason::ToolCall) => ResponseOutcome::Completed,
            Some(FinishReason::Error) => ResponseOutcome::Failed(
                KitError::new(
                    ErrorKind::ProviderRejected,
                    Operation::GenerateStream,
                    ErrorPhase::Normalize,
                )
                .with_message("provider returned an error finish reason"),
            ),
            Some(reason) => ResponseOutcome::Partial {
                reason: Some(reason.clone()),
            },
            None => ResponseOutcome::Unresolved(
                KitError::invalid_state("generation omitted an affirmative terminal reason")
                    .at_operation(Operation::GenerateStream)
                    .at_phase(ErrorPhase::Normalize),
            ),
        }
    }

    fn failure(error: KitError) -> ResponseOutcome {
        if error.remote_failure().is_none()
            && matches!(
                error.kind(),
                ErrorKind::Transport
                    | ErrorKind::Timeout
                    | ErrorKind::Closed
                    | ErrorKind::InvalidResponse(_)
                    | ErrorKind::InvalidState
            )
        {
            ResponseOutcome::Unresolved(error)
        } else {
            ResponseOutcome::Failed(error)
        }
    }

    fn settle_controls(
        control: &SessionControl,
        continuity: SessionContinuity,
    ) -> Result<(), KitError> {
        while let Some((command, incoming)) = control.take_command() {
            match incoming {
                SessionCommand::Cancel(target) => control.publish(SessionEvent::Cancellation {
                    command,
                    target,
                    resolution: CancellationResolution::AlreadySettled,
                    continuity,
                })?,
                _ => control.publish(SessionEvent::CommandFailed {
                    command,
                    error: KitError::closed(
                        "session work settled before queued command could start",
                    ),
                    not_started: true,
                })?,
            }
        }
        Ok(())
    }

    async fn drive(
        model: &M,
        request: &GenerateRequest,
        control: &Arc<SessionControl>,
        response: &ResponseId,
    ) -> Result<GenerateResponse, KitError> {
        let mut stream = model.generate_stream(request).await?;
        loop {
            tokio::select! {
                _=control.demand()=>{},
                // Once sealed, admitted work must drain independently of an
                // observer so provider shutdown can preserve its native outcome.
                _=control.sealed()=>{},
            }
            loop {
                match stream.next().await {
                    Some(Ok(GenerateEvent::Started { .. } | GenerateEvent::HostedTool(_))) => {
                        continue;
                    }
                    Some(Ok(GenerateEvent::ContentDelta { delta })) => {
                        control.publish(SessionEvent::Output {
                            response: response.clone(),
                            segment: None,
                            content: ContentPart::Text { text: delta },
                        })?;
                        break;
                    }
                    Some(Ok(GenerateEvent::ThinkingDelta { delta })) => {
                        control.publish(SessionEvent::Output {
                            response: response.clone(),
                            segment: None,
                            content: ContentPart::Thinking { text: Some(delta) },
                        })?;
                        break;
                    }
                    Some(Ok(GenerateEvent::Usage(usage))) => {
                        control.publish(SessionEvent::Usage {
                            scope: UsageScope::Response(response.clone()),
                            usage,
                            finalized: false,
                        })?;
                        break;
                    }
                    Some(Ok(GenerateEvent::Completed(response))) => return Ok(*response),
                    Some(Err(error)) => return Err(error),
                    None => {
                        return Err(KitError::invalid_state(
                            "generation stream ended without an authoritative completion",
                        ));
                    }
                }
            }
        }
    }
}

impl<M: ModelBackend<Continuation = GenerationContinuation>> SessionCommands
    for ManagedCommands<'_, M>
{
    async fn submit(&mut self, command: SessionCommand) -> Result<CommandId, KitError> {
        if matches!(command, SessionCommand::Cancel(_)) {
            let state = self.owner.state();
            let id = self.owner.control.accept(command)?;
            if !state.active
                && let Err(error) = ManagedConversation::<M>::settle_controls(
                    &self.owner.control,
                    self.owner.control.continuity(),
                )
            {
                self.owner
                    .control
                    .terminate(error, SessionContinuity::Uncertain);
            }
            return Ok(id);
        }
        self.owner.control.ensure_open()?;
        if self.owner.state().active {
            return Err(KitError::invalid_state(
                "managed generation is already active",
            ));
        }
        match &command {
            SessionCommand::Input(message) => {
                if message.role() != Role::User || message.continuation().is_some() {
                    return Err(KitError::invalid_state(
                        "session input must be a fresh user message",
                    ));
                }
                message.validate_for_request()?;
                if !self.owner.state().required.is_empty() {
                    return Err(KitError::invalid_state(
                        "session requires outstanding tool results",
                    ));
                }
            }
            SessionCommand::ToolResults(results) => {
                self.owner.control.validate_admission(&command)?;
                for result in results {
                    (self.check)(self.owner.model_id(), result.result())?;
                }
            }
            _ => {
                return Err(KitError::unsupported(
                    ValidationViolation::Unsupported {
                        field: "session.command",
                        rule: "Conversation supports input, tool results and cancellation",
                    },
                    "Conversation sessions support user input, tool results and targeted cancellation",
                ));
            }
        }
        // Joining happens before acceptance. Dropping this future while waiting
        // cannot transfer the supplied command or cause a remote operation.
        self.owner.control.join().await?;
        let id = self.owner.control.accept(command)?;
        let Some((_, command)) = self.owner.control.take_history_command() else {
            return Err(KitError::invalid_state("accepted command lost ownership"));
        };
        let mut state = self.owner.state();
        match command {
            SessionCommand::Input(message) => {
                let message = message.try_map_continuation(&mut |_| {
                    Err(KitError::invalid_state("input cannot carry continuation"))
                })?;
                if let Some(history) = state.history.as_mut() {
                    history.append_session(message);
                } else {
                    state.history = Some(self.owner.request(message)?);
                }
            }
            SessionCommand::ToolResults(results) => {
                let mut batch = Vec::new();
                for result in results {
                    state.required.remove(result.request());
                    batch.push(result.into_result());
                }
                let message = Message::tool(batch)?;
                state
                    .history
                    .as_mut()
                    .ok_or_else(|| KitError::invalid_state("tool result has no assistant history"))?
                    .append_session(message);
                if !state.required.is_empty() {
                    drop(state);
                    if let Err(error) = self.owner.control.publish(SessionEvent::ControlApplied {
                        command: id.clone(),
                    }) {
                        self.owner
                            .control
                            .terminate(error, SessionContinuity::Uncertain);
                    }
                    return Ok(id);
                }
            }
            _ => {
                return Err(KitError::invalid_state(
                    "unsupported managed command entered admission",
                ));
            }
        }
        let request = state
            .history
            .take()
            .ok_or_else(|| KitError::invalid_state("accepted generation has no history"))?;
        state.active = true;
        drop(state);
        if let Err(error) = self.owner.start(id.clone(), request) {
            self.owner
                .control
                .terminate(error, SessionContinuity::Uncertain);
        }
        Ok(id)
    }
}

impl<M: ModelBackend<Continuation = GenerationContinuation>> Drop for ManagedConversation<M> {
    fn drop(&mut self) {
        self.control.stop();
    }
}
