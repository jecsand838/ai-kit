use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::KitError;
use crate::error::{Operation, ResponseViolation, ValidationViolation};
use crate::generation::{FunctionCall, ProviderToolSpec, RetainedSize, ToolChoice};
use crate::provider::{ProviderAdmission, ProviderRuntime};
use crate::session::{
    CancelTarget, CancellationResolution, CommandId, OutputSegmentId, ResponseId, ResponseOutcome,
    SessionCommand, SessionConfig, SessionContinuity, SessionEvent, SessionEvents, ToolRequestId,
};

const BYTE_LIMIT: usize = 32 * 1024 * 1024;
const CONTROL_BYTE_RESERVE: usize = 4 * 1024 * 1024;
const RECORD_LIMIT: usize = 4096;
const COMMAND_LIMIT: usize = 32;
const EVENT_LIMIT: usize = 128;

pub(crate) struct SessionControl {
    id: String,
    config: SessionConfig,
    timeout: Duration,
    state: Mutex<SessionState>,
    changed: Notify,
    commands_ready: Notify,
    demand_ready: Notify,
    stopped_ready: Notify,
    sealed_ready: Notify,
}

struct SessionState {
    provider: Weak<ProviderRuntime>,
    bound: bool,
    sequence: u64,
    commands: VecDeque<(CommandId, SessionCommand, usize)>,
    events: VecDeque<(SessionEvent, usize)>,
    terminal: Option<SessionEvent>,
    terminated: bool,
    stopped: bool,
    sealed: bool,
    demand: bool,
    continuity: SessionContinuity,
    retained: usize,
    completion_reserved: usize,
    transcription_retained: usize,
    records: usize,
    unfinished: BTreeSet<CommandId>,
    known_commands: BTreeSet<CommandId>,
    known_responses: BTreeSet<ResponseId>,
    generation_commands: BTreeSet<CommandId>,
    responses: BTreeMap<ResponseId, Option<CommandId>>,
    cancelled: BTreeSet<ResponseId>,
    tools: BTreeMap<ToolRequestId, ToolObligation>,
    task: Option<JoinHandle<Result<(), KitError>>>,
    joining: bool,
    joined: Option<Result<(), KitError>>,
}

struct ToolObligation {
    response: ResponseId,
    call: FunctionCall,
    delivered: bool,
    settled: bool,
}

pub(crate) struct SessionEventPort<'a> {
    control: &'a Arc<SessionControl>,
}

struct SessionJoinLease<'a> {
    control: &'a SessionControl,
    task: Option<JoinHandle<Result<(), KitError>>>,
}

impl SessionControl {
    pub(crate) fn new(config: SessionConfig, timeout: Duration) -> Result<Arc<Self>, KitError> {
        let mut retained = RetainedSize::new();
        retained.session_config(&config)?;
        if retained.bytes() > BYTE_LIMIT {
            return Err(Self::limit("session.config", BYTE_LIMIT));
        }
        Ok(Arc::new(Self {
            id: uuid::Uuid::new_v4().to_string(),
            config,
            timeout,
            state: Mutex::new(SessionState {
                provider: Weak::new(),
                bound: false,
                sequence: 0,
                commands: VecDeque::new(),
                events: VecDeque::new(),
                terminal: None,
                terminated: false,
                stopped: false,
                sealed: false,
                demand: false,
                continuity: SessionContinuity::Reusable,
                retained: retained.bytes(),
                completion_reserved: 0,
                transcription_retained: 0,
                records: 0,
                unfinished: BTreeSet::new(),
                known_commands: BTreeSet::new(),
                known_responses: BTreeSet::new(),
                generation_commands: BTreeSet::new(),
                responses: BTreeMap::new(),
                cancelled: BTreeSet::new(),
                tools: BTreeMap::new(),
                task: None,
                joining: false,
                joined: None,
            }),
            changed: Notify::new(),
            commands_ready: Notify::new(),
            demand_ready: Notify::new(),
            stopped_ready: Notify::new(),
            sealed_ready: Notify::new(),
        }))
    }

    pub(crate) fn id(&self) -> &str {
        &self.id
    }
    pub(crate) fn config(&self) -> &SessionConfig {
        &self.config
    }

    pub(crate) fn bind_provider(
        self: &Arc<Self>,
        provider: Arc<ProviderRuntime>,
    ) -> Result<(), KitError> {
        {
            let mut state = self.state();
            if state.bound {
                return Err(KitError::invalid_state(
                    "session already belongs to a provider",
                ));
            }
            state.bound = true;
            state.provider = Arc::downgrade(&provider);
        }
        provider.register_session(Arc::clone(self))
    }

    pub(crate) fn generation_admission(&self) -> Result<ProviderAdmission, KitError> {
        let provider = self
            .state()
            .provider
            .upgrade()
            .ok_or_else(|| KitError::closed("session provider is unavailable"))?;
        provider.admit(Operation::SessionSend)
    }

    pub(crate) fn ensure_open(&self) -> Result<(), KitError> {
        let state = self.state();
        if state.sealed || state.stopped || state.terminated {
            return Err(KitError::closed("session admission closed"));
        }
        if state.continuity != SessionContinuity::Reusable {
            return Err(KitError::invalid_state("session continuity is uncertain"));
        }
        Ok(())
    }

    pub(crate) fn accept(&self, command: SessionCommand) -> Result<CommandId, KitError> {
        let bytes = Self::command_bytes(&command)?;
        let mut state = self.state();
        let control = matches!(
            command,
            SessionCommand::Cancel(_)
                | SessionCommand::DiscardInput
                | SessionCommand::ReconcilePlayback { .. }
        );
        if state.terminated || state.stopped || (state.sealed && !control) {
            return Err(KitError::closed("session admission closed"));
        }
        if !control && state.continuity != SessionContinuity::Reusable {
            return Err(KitError::invalid_state("session continuity is uncertain"));
        }
        let max = if control {
            COMMAND_LIMIT + 8
        } else {
            COMMAND_LIMIT
        };
        if state.commands.len() >= max {
            return Err(Self::limit("session.commands", max));
        }
        // Each accepted command reserves bounded outcome space, independently
        // of consumer demand. A cancellation may also invalidate delivered calls.
        let observations = state
            .unfinished
            .len()
            .checked_mul(2)
            .and_then(|count| state.events.len().checked_add(count))
            .and_then(|count| count.checked_add(2));
        if observations.is_none_or(|count| count > EVENT_LIMIT + COMMAND_LIMIT + 8) {
            return Err(Self::limit(
                "session.observations",
                EVENT_LIMIT + COMMAND_LIMIT + 8,
            ));
        }
        if matches!(command, SessionCommand::Input(_))
            && (!state.responses.is_empty()
                || !state.generation_commands.is_empty()
                || state.tools.values().any(|tool| !tool.settled))
        {
            return Err(KitError::invalid_state(
                "one generation or required tool result set is already active",
            ));
        }
        if let SessionCommand::ToolResults(results) = &command {
            if results.is_empty() {
                return Err(KitError::invalid_state(
                    "tool result batch must not be empty",
                ));
            }
            let mut unique = BTreeSet::new();
            for result in results {
                if result.request().session_id() != self.id || !unique.insert(result.request()) {
                    return Err(KitError::invalid_state("duplicate or foreign tool request"));
                }
                let tool = state
                    .tools
                    .get(result.request())
                    .ok_or_else(|| KitError::invalid_state("unknown tool request"))?;
                if tool.settled
                    || tool.call.id() != result.result().call_id()
                    || tool.call.name() != result.result().name()
                {
                    return Err(KitError::invalid_state("stale or mismatched tool result"));
                }
            }
        }
        let retained = state
            .retained
            .checked_add(bytes)
            .ok_or_else(|| Self::limit("session.retained", BYTE_LIMIT))?;
        let byte_limit = if control {
            BYTE_LIMIT + CONTROL_BYTE_RESERVE
        } else {
            BYTE_LIMIT
        };
        let record_limit = if control {
            RECORD_LIMIT + 64
        } else {
            RECORD_LIMIT
        };
        if retained > byte_limit || state.records >= record_limit {
            return Err(Self::limit("session.retained", BYTE_LIMIT));
        }
        let id = CommandId::new(self.id.clone(), Self::sequence(&mut state)?)?;
        if let SessionCommand::ToolResults(results) = &command {
            for result in results {
                if let Some(tool) = state.tools.get_mut(result.request()) {
                    tool.settled = true;
                }
            }
        }
        if let SessionCommand::Cancel(target) = &command {
            match target {
                CancelTarget::Response(response) => {
                    if !state.known_responses.contains(response) {
                        return Err(KitError::invalid_state("unknown response cancellation"));
                    }
                }
                CancelTarget::Command(command) => {
                    if !state.known_commands.contains(command) {
                        return Err(KitError::invalid_state("unknown command cancellation"));
                    }
                }
            }
        }
        state.retained += bytes;
        state.records += 1;
        state.unfinished.insert(id.clone());
        state.known_commands.insert(id.clone());
        if matches!(command, SessionCommand::Input(_)) {
            state.generation_commands.insert(id.clone());
        }
        state.commands.push_back((id.clone(), command, bytes));
        drop(state);
        self.commands_ready.notify_one();
        self.changed.notify_waiters();
        Ok(id)
    }

    pub(crate) async fn next_command(&self) -> Option<(CommandId, SessionCommand)> {
        loop {
            let notified = self.commands_ready.notified();
            {
                let mut state = self.state();
                if let Some((id, command, bytes)) = state.commands.pop_front() {
                    state.retained -= bytes;
                    return Some((id, command));
                }
                if state.stopped || state.terminated {
                    return None;
                }
            }
            notified.await;
        }
    }

    pub(crate) fn take_command(&self) -> Option<(CommandId, SessionCommand)> {
        let mut state = self.state();
        state.commands.pop_front().map(|(id, command, bytes)| {
            state.retained -= bytes;
            (id, command)
        })
    }

    pub(crate) fn take_history_command(&self) -> Option<(CommandId, SessionCommand)> {
        self.state()
            .commands
            .pop_front()
            .map(|(id, command, _)| (id, command))
    }

    pub(crate) fn publish(&self, mut event: SessionEvent) -> Result<(), KitError> {
        if let SessionEvent::ToolRequested { call, .. } = &event {
            let declared=self.config.tools().iter().any(|tool|matches!(tool,ProviderToolSpec::Function(function) if function.name()==call.name()));
            let selected = match self.config.generation().tool_choice() {
                Some(ToolChoice::None) => false,
                Some(ToolChoice::Tool { name }) => name == call.name(),
                Some(ToolChoice::Allowed { names, .. }) => {
                    names.iter().any(|name| name == call.name())
                }
                _ => true,
            };
            if !declared || !selected {
                return Err(KitError::invalid_response(
                    ResponseViolation::ProtocolViolation {
                        rule: "session returned an undeclared or disabled function",
                    },
                ));
            }
        }
        let bytes = Self::event_bytes(&event)?;
        let mut obligation = RetainedSize::new();
        if let SessionEvent::ToolRequested { call, .. } = &event {
            obligation.call(call)?;
        }
        let required = bytes
            .checked_add(obligation.bytes())
            .ok_or_else(|| Self::limit("session.observations", BYTE_LIMIT))?;
        let mut state = self.state();
        if state.terminated {
            return Err(KitError::closed("session terminal already published"));
        }
        let normal = matches!(
            event,
            SessionEvent::Output { .. }
                | SessionEvent::Transcription { .. }
                | SessionEvent::ToolRequested { .. }
                | SessionEvent::Usage { .. }
                | SessionEvent::ResponseFinished {
                    output: Some(_),
                    ..
                }
        );
        let capacity = if normal {
            EVENT_LIMIT
        } else {
            EVENT_LIMIT + COMMAND_LIMIT + 8
        };
        let byte_limit = if normal {
            BYTE_LIMIT
        } else {
            BYTE_LIMIT + CONTROL_BYTE_RESERVE
        };
        let reserved = matches!(
            event,
            SessionEvent::ToolRequested { .. }
                | SessionEvent::ResponseFinished {
                    output: Some(_),
                    ..
                }
        ) && state.completion_reserved >= required;
        if state.events.len() >= capacity
            || (!reserved
                && state
                    .retained
                    .checked_add(required)
                    .is_none_or(|total| total > byte_limit))
        {
            return Err(Self::limit("session.observations", BYTE_LIMIT));
        }
        match &event {
            SessionEvent::ResponseStarted { command, response } => {
                if state.responses.contains_key(response) {
                    return Err(KitError::invalid_state("duplicate response start"));
                }
                state.responses.insert(response.clone(), command.clone());
            }
            SessionEvent::ResponseFinished {
                response,
                continuity,
                ..
            } => {
                if let Some(Some(command)) = state.responses.remove(response) {
                    state.unfinished.remove(&command);
                    state.generation_commands.remove(&command);
                }
                state.continuity = *continuity;
            }
            SessionEvent::ToolRequested {
                response,
                request,
                call,
            } => {
                if state.cancelled.contains(response) {
                    return Ok(());
                }
                if state.tools.len() >= RECORD_LIMIT || state.tools.contains_key(request) {
                    return Err(Self::limit("session.tools", RECORD_LIMIT));
                }
                state.tools.insert(
                    request.clone(),
                    ToolObligation {
                        response: response.clone(),
                        call: call.clone(),
                        delivered: false,
                        settled: false,
                    },
                );
            }
            SessionEvent::ToolsInvalidated { requests } => {
                let delivered = requests
                    .iter()
                    .filter(|id| {
                        state
                            .tools
                            .get(*id)
                            .is_some_and(|tool| tool.delivered && !tool.settled)
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                Self::invalidate_tools_locked(&mut state, requests);
                if delivered.is_empty() {
                    return Ok(());
                }
                event = SessionEvent::ToolsInvalidated {
                    requests: delivered,
                };
            }
            SessionEvent::CommandFailed { command, .. }
            | SessionEvent::ControlApplied { command } => {
                state.unfinished.remove(command);
                state.generation_commands.remove(command);
            }
            SessionEvent::Cancellation {
                command,
                continuity,
                ..
            } => {
                state.unfinished.remove(command);
                state.continuity = *continuity;
            }
            SessionEvent::Terminated { .. } => {
                return Err(KitError::invalid_state(
                    "session terminal must use terminal authority",
                ));
            }
            SessionEvent::Output { .. }
            | SessionEvent::Transcription { .. }
            | SessionEvent::Usage { .. }
            | SessionEvent::Interruption { .. }
            | SessionEvent::TurnEnded { .. } => {}
        }
        if reserved {
            state.completion_reserved -= required;
        } else {
            state.retained += required;
        }
        state.events.push_back((event, bytes));
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    pub(crate) fn next_response(&self) -> Result<ResponseId, KitError> {
        let mut state = self.state();
        if state.known_responses.len() >= RECORD_LIMIT {
            return Err(Self::limit("session.responses", RECORD_LIMIT));
        }
        let response = ResponseId::new(self.id.clone(), Self::sequence(&mut state)?)?;
        state.known_responses.insert(response.clone());
        Ok(response)
    }
    pub(crate) fn next_tool(&self) -> Result<ToolRequestId, KitError> {
        ToolRequestId::new(self.id.clone(), Self::sequence(&mut self.state())?)
    }
    pub(crate) fn next_segment(&self) -> Result<OutputSegmentId, KitError> {
        OutputSegmentId::new(self.id.clone(), Self::sequence(&mut self.state())?)
    }

    pub(crate) fn invalidate_response(&self, response: &ResponseId) -> Result<(), KitError> {
        let mut state = self.state();
        state.cancelled.insert(response.clone());
        Self::invalidate_locked(&mut state, response);
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    fn invalidate_locked(state: &mut SessionState, response: &ResponseId) {
        let requests = state
            .tools
            .iter()
            .filter(|(_, tool)| &tool.response == response && !tool.settled)
            .map(|(request, _)| request.clone())
            .collect::<Vec<_>>();
        let delivered = requests
            .iter()
            .filter(|id| state.tools.get(*id).is_some_and(|tool| tool.delivered))
            .cloned()
            .collect::<Vec<_>>();
        Self::invalidate_tools_locked(state, &requests);
        if !delivered.is_empty() {
            state.events.push_back((
                SessionEvent::ToolsInvalidated {
                    requests: delivered,
                },
                0,
            ));
        }
    }

    fn invalidate_tools_locked(state: &mut SessionState, requests: &[ToolRequestId]) {
        for request in requests {
            if let Some(tool) = state.tools.get_mut(request) {
                tool.settled = true;
            }
        }
        state.events.retain(|(event, bytes)| {
            let keep = !matches!(event, SessionEvent::ToolRequested { request,.. } if requests.contains(request));
            if !keep { state.retained -= bytes; }
            keep
        });
    }

    pub(crate) fn reserve_history(&self, bytes: usize) -> Result<(), KitError> {
        let mut state = self.state();
        if state.completion_reserved >= bytes {
            state.completion_reserved -= bytes;
            return Ok(());
        }
        let retained = state
            .retained
            .checked_add(bytes)
            .ok_or_else(|| Self::limit("session.history", BYTE_LIMIT))?;
        if retained > BYTE_LIMIT {
            return Err(Self::limit("session.history", BYTE_LIMIT));
        }
        state.retained = retained;
        Ok(())
    }

    pub(crate) fn reserve_transcription(&self, bytes: usize) -> Result<(), KitError> {
        let mut state = self.state();
        if state.terminated {
            return Err(KitError::closed("session terminal already published"));
        }
        let retained = state
            .retained
            .checked_add(bytes)
            .filter(|retained| *retained <= BYTE_LIMIT)
            .ok_or_else(|| Self::limit("session.transcription", BYTE_LIMIT))?;
        let transcription_retained = state
            .transcription_retained
            .checked_add(bytes)
            .ok_or_else(|| Self::limit("session.transcription", BYTE_LIMIT))?;
        state.retained = retained;
        state.transcription_retained = transcription_retained;
        Ok(())
    }

    pub(crate) fn release_transcription(&self, bytes: usize) -> Result<(), KitError> {
        Self::release_transcription_locked(&mut self.state(), bytes)
    }

    fn release_transcription_locked(
        state: &mut SessionState,
        bytes: usize,
    ) -> Result<(), KitError> {
        let transcription_retained = state
            .transcription_retained
            .checked_sub(bytes)
            .ok_or_else(|| KitError::invalid_state("transcription reservation is missing"))?;
        let retained = state
            .retained
            .checked_sub(bytes)
            .ok_or_else(|| KitError::invalid_state("transcription exceeds retained accounting"))?;
        state.transcription_retained = transcription_retained;
        state.retained = retained;
        Ok(())
    }

    pub(crate) fn publish_transcription_snapshot(
        &self,
        source: crate::session::TranscriptionSource,
        text: String,
    ) -> Result<(), KitError> {
        let mut state = self.state();
        // The String moves out of the driver's already charged buffer. Transfer
        // its credit under the same lock as admission to the observation queue;
        // any publication failure drops the String and leaves no orphan charge.
        Self::release_transcription_locked(&mut state, text.len())?;
        if state.terminated {
            return Err(KitError::closed("session terminal already published"));
        }
        let event = SessionEvent::Transcription {
            source,
            text,
            finalized: true,
        };
        let bytes = Self::event_bytes(&event)?;
        let retained = state
            .retained
            .checked_add(bytes)
            .filter(|retained| *retained <= BYTE_LIMIT)
            .ok_or_else(|| Self::limit("session.observations", BYTE_LIMIT))?;
        if state.events.len() >= EVENT_LIMIT {
            return Err(Self::limit("session.observations", BYTE_LIMIT));
        }
        state.retained = retained;
        state.events.push_back((event, bytes));
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    pub(crate) fn reserve_completion(&self, bytes: usize, events: usize) -> Result<(), KitError> {
        let mut state = self.state();
        if state.completion_reserved != 0 {
            return Err(KitError::invalid_state(
                "completion reservation is already active",
            ));
        }
        if state
            .retained
            .checked_add(bytes)
            .is_none_or(|total| total > BYTE_LIMIT)
            || state
                .events
                .len()
                .checked_add(events)
                .is_none_or(|total| total > EVENT_LIMIT)
        {
            return Err(Self::limit("session.retained", BYTE_LIMIT));
        }
        state.retained += bytes;
        state.completion_reserved = bytes;
        Ok(())
    }

    pub(crate) fn finish_completion(&self) {
        let mut state = self.state();
        state.retained -= state.completion_reserved;
        state.completion_reserved = 0;
    }

    pub(crate) fn seal(&self) {
        self.state().sealed = true;
        self.sealed_ready.notify_waiters();
        self.commands_ready.notify_waiters();
        self.changed.notify_waiters();
    }
    pub(crate) fn stop(&self) {
        self.state().stopped = true;
        self.stopped_ready.notify_waiters();
        self.commands_ready.notify_waiters();
        self.demand_ready.notify_waiters();
    }
    pub(crate) fn is_stopped(&self) -> bool {
        self.state().stopped
    }
    pub(crate) fn is_sealed(&self) -> bool {
        self.state().sealed
    }
    pub(crate) fn continuity(&self) -> SessionContinuity {
        let state = self.state();
        if state.sealed || state.stopped {
            SessionContinuity::Closed
        } else {
            state.continuity
        }
    }
    pub(crate) async fn stopped(&self) {
        loop {
            let notified = self.stopped_ready.notified();
            if self.is_stopped() {
                return;
            }
            notified.await;
        }
    }
    pub(crate) async fn sealed(&self) {
        loop {
            let notified = self.sealed_ready.notified();
            if self.is_sealed() {
                return;
            }
            notified.await;
        }
    }
    pub(crate) async fn demand(&self) {
        loop {
            let notified = self.demand_ready.notified();
            {
                let mut state = self.state();
                if state.demand {
                    state.demand = false;
                    return;
                }
            }
            notified.await;
        }
    }

    pub(crate) fn terminate(&self, reason: KitError, continuity: SessionContinuity) {
        let mut state = self.state();
        if state.terminated {
            return;
        }
        state.terminated = true;
        state.continuity = continuity;
        state.terminal = Some(SessionEvent::Terminated {
            reason,
            continuity,
            unfinished_commands: state.unfinished.iter().cloned().collect(),
            unfinished_responses: state.responses.keys().cloned().collect(),
            outstanding_tools: state
                .tools
                .iter()
                .filter(|(_, tool)| !tool.settled)
                .map(|(id, _)| id.clone())
                .collect(),
        });
        drop(state);
        self.changed.notify_waiters();
        self.commands_ready.notify_waiters();
    }

    pub(crate) fn spawn(
        self: &Arc<Self>,
        future: impl Future<Output = Result<(), KitError>> + Send + 'static,
    ) -> Result<(), KitError> {
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| KitError::invalid_state("sessions require a Tokio runtime"))?;
        let mut state = self.state();
        if state.task.is_some() || state.joining {
            return Err(KitError::invalid_state(
                "session task must be joined before replacement",
            ));
        }
        let control = Arc::clone(self);
        state.joined = None;
        state.task = Some(handle.spawn(async move {
            let result = future.await;
            if let Err(error) = &result {
                control.terminate(error.clone(), SessionContinuity::Uncertain);
            }
            control.changed.notify_waiters();
            result
        }));
        Ok(())
    }

    pub(crate) async fn join(&self) -> Result<(), KitError> {
        loop {
            let notified = self.changed.notified();
            let lease = {
                let mut state = self.state();
                if let Some(task) = state.task.take() {
                    state.joining = true;
                    Some(SessionJoinLease {
                        control: self,
                        task: Some(task),
                    })
                } else if !state.joining {
                    return state.joined.clone().unwrap_or(Ok(()));
                } else {
                    None
                }
            };
            let Some(mut lease) = lease else {
                notified.await;
                continue;
            };
            let task = lease
                .task
                .as_mut()
                .ok_or_else(|| KitError::invalid_state("session task lease missing"))?;
            let result = match tokio::time::timeout(self.timeout, &mut *task).await {
                Ok(Ok(result)) => result,
                Ok(Err(error)) if error.is_cancelled() && self.is_stopped() => Ok(()),
                Ok(Err(_)) => Err(KitError::invalid_state(
                    "session worker could not be joined",
                )),
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    Err(KitError::invalid_state(
                        "session cleanup exceeded its deadline",
                    ))
                }
            };
            lease.task = None;
            let mut state = self.state();
            state.joining = false;
            state.joined = Some(result.clone());
            drop(state);
            self.changed.notify_waiters();
            return result;
        }
    }

    pub(crate) async fn close(self: &Arc<Self>) -> Result<(), KitError> {
        self.stop();
        let result = self.join().await;
        self.terminate(
            KitError::closed("session closed"),
            SessionContinuity::Closed,
        );
        result
    }

    pub(crate) fn resource_settled(&self) -> bool {
        let state = self.state();
        state.terminated && state.task.is_none() && !state.joining
    }

    fn state(&self) -> MutexGuard<'_, SessionState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn sequence(state: &mut SessionState) -> Result<NonZeroU64, KitError> {
        state.sequence = state
            .sequence
            .checked_add(1)
            .ok_or_else(|| KitError::invalid_state("session token space exhausted"))?;
        NonZeroU64::new(state.sequence)
            .ok_or_else(|| KitError::invalid_state("session token is zero"))
    }
    fn limit(field: &'static str, limit: usize) -> KitError {
        KitError::invalid_input(
            ValidationViolation::LimitExceeded { field, limit },
            "session resource budget exhausted",
        )
    }

    fn command_bytes(command: &SessionCommand) -> Result<usize, KitError> {
        let mut size = RetainedSize::new();
        size.add(std::mem::size_of::<SessionCommand>())?;
        match command {
            SessionCommand::Input(message) => size.message(message)?,
            SessionCommand::ToolResults(results) => {
                for result in results {
                    size.result(result.result())?;
                }
            }
            SessionCommand::Audio { media_type, data } => {
                size.add(media_type.len())?;
                size.add(data.len())?;
            }
            SessionCommand::Cancel(_)
            | SessionCommand::StartInput
            | SessionCommand::EndInput
            | SessionCommand::EndAudioStream
            | SessionCommand::DiscardInput
            | SessionCommand::ReconcilePlayback { .. } => {}
        }
        Ok(size.bytes())
    }
    fn event_bytes(event: &SessionEvent) -> Result<usize, KitError> {
        let mut size = RetainedSize::new();
        size.add(std::mem::size_of::<SessionEvent>())?;
        match event {
            SessionEvent::Output { content, .. } => size.content(content)?,
            SessionEvent::Transcription { text, .. } => size.add(text.len())?,
            SessionEvent::ToolRequested { call, .. } => size.call(call)?,
            SessionEvent::ResponseFinished {
                output, outcome, ..
            } => {
                if let Some(output) = output {
                    size.response(output)?;
                }
                if let ResponseOutcome::Failed(error) | ResponseOutcome::Unresolved(error) = outcome
                {
                    size.error(error)?;
                }
            }
            SessionEvent::CommandFailed { error, .. } => size.error(error)?,
            SessionEvent::Cancellation {
                resolution: CancellationResolution::Unconfirmed(error),
                ..
            } => size.error(error)?,
            SessionEvent::Terminated { reason, .. } => size.error(reason)?,
            SessionEvent::Interruption { response } | SessionEvent::TurnEnded { response } => {
                if let Some(response) = response {
                    size.add(response.session_id().len())?;
                }
            }
            _ => {}
        }
        Ok(size.bytes())
    }
}

impl<'a> SessionEventPort<'a> {
    pub(crate) fn new(control: &'a Arc<SessionControl>) -> Self {
        Self { control }
    }
}

impl SessionEvents for SessionEventPort<'_> {
    async fn recv(&mut self) -> Option<SessionEvent> {
        loop {
            let notified = self.control.changed.notified();
            {
                let mut state = self.control.state();
                if let Some((event, bytes)) = state.events.pop_front() {
                    state.retained -= bytes;
                    if let SessionEvent::ToolRequested { request, .. } = &event
                        && let Some(tool) = state.tools.get_mut(request)
                    {
                        tool.delivered = true;
                    }
                    return Some(event);
                }
                if let Some(terminal) = state.terminal.take() {
                    return Some(terminal);
                }
                if state.terminated {
                    return None;
                }
                state.demand = true;
            }
            self.control.demand_ready.notify_one();
            notified.await;
        }
    }
}

impl Drop for SessionJoinLease<'_> {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            let mut state = self.control.state();
            state.task = Some(task);
            state.joining = false;
            drop(state);
            self.control.changed.notify_waiters();
        }
    }
}
