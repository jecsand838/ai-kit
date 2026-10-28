mod cancellation;

use crate::error::{DeadlineKind, ErrorDetail, ErrorKind, ErrorPhase};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::path::Path;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::backends::opencode::notifications::{OpenCodeEventSemantics, OpenCodeNotifications};
use crate::backends::opencode::protocol::http::OpenCodeHttp;
use crate::backends::opencode::protocol::inbound::{OpenCodeInboundEvent, OpenCodeKnownEvent};
use crate::backends::opencode::protocol::requests::OpenCodeRouting;
use crate::backends::opencode::protocol::sse::OpenCodeEventStream;
use crate::backends::opencode::protocol::sync::OpenCodeHistoryRequest;
use crate::backends::opencode::server_requests::OpenCodeServerRequests;
use crate::backends::opencode::state_store::records::{
    OpenCodeEventDisposition, OpenCodeEventIdentity, OpenCodeSessionBinding,
};
use crate::backends::opencode::state_store::{
    OpenCodeEventEffect, OpenCodeStateStore, OpenCodeTransactionError,
};
use crate::backends::opencode::tools::OpenCodeTools;
use crate::core::hierarchy::{HierarchyAdmission, HierarchyClaim, HierarchyRuntime};
use crate::core::state::ConversationRecord;
use crate::{AgentId, ConversationId, KitError};
use storage_kit::StateStoreBackend;

const MAX_LIVE_OVERLAP_EVENTS: usize = 1_024;
const MAX_LIVE_OVERLAP_BYTES: usize = 8 << 20;
const MAX_RETIRED_SESSIONS: usize = 4_096;
const EVENT_RECONNECT_BACKOFF: Duration = Duration::from_millis(100);

pub(super) struct OpenCodeEventPolicy {
    idle_stream_timeout: Duration,
    initial_deadline: Instant,
    lifecycle_timeout: Duration,
}

struct OpenCodeEventOwner<S: StateStoreBackend> {
    http: OpenCodeHttp,
    state: OpenCodeStateStore<S>,
    hierarchy: HierarchyRuntime,
    agent_id: AgentId,
    idle_stream_timeout: Duration,
    lifecycle_timeout: Duration,
    owned_sessions: BTreeSet<String>,
    session_bindings: BTreeMap<String, OpenCodeSessionBinding>,
    pending_deleted_sessions: BTreeSet<String>,
    terminal_sessions: BTreeSet<String>,
    retired_sessions: BTreeSet<String>,
    commands: mpsc::Receiver<OpenCodeEventCommand>,
    stop: Option<oneshot::Receiver<()>>,
    ready: Option<oneshot::Sender<Result<(), KitError>>>,
    initial_deadline: Instant,
    notifications: OpenCodeNotifications,
    tools: OpenCodeTools,
}

impl<S: StateStoreBackend> Drop for OpenCodeEventOwner<S> {
    fn drop(&mut self) {
        self.invalidate();
        self.state.abort_prompt_tasks();
    }
}

struct OpenCodeLiveEvents {
    stream: OpenCodeEventStream,
    buffered: VecDeque<OpenCodeInboundEvent>,
    buffered_bytes: usize,
}

enum OpenCodeEventPhase {
    Starting { deadline: Instant },
    Live(Box<OpenCodeLiveEvents>),
    Recovering(OpenCodeRecovery),
}

enum OpenCodeRecovery {
    Startup { deadline: Instant },
    Reconnect { deadline: Instant },
    Synchronize(Box<OpenCodeSameStreamRecovery>),
}

enum OpenCodeSameStreamRecovery {
    Application {
        deadline: Instant,
        live: OpenCodeLiveEvents,
    },
    Gap {
        deadline: Instant,
        live: OpenCodeLiveEvents,
    },
    Command {
        live: OpenCodeLiveEvents,
        command: OpenCodeEventCommand,
    },
}

#[derive(Clone, Copy)]
enum OpenCodeRecoveryReason {
    Startup,
    Reconnect,
    Application,
    Gap,
    Command,
}

enum OpenCodeEventActivity {
    Synchronize(OpenCodeEventCommand),
    Inbound(Box<Result<OpenCodeInboundEvent, KitError>>),
}

#[derive(Clone)]
enum OpenCodeRecoveryFailure {
    Parked(KitError),
    Retry(KitError),
    Reconnect(KitError),
    Stop(KitError),
}

pub(super) struct OpenCodeEvents {
    handle: OpenCodeEventHandle,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<(), KitError>>>,
}

#[derive(Clone)]
pub(crate) struct OpenCodeEventHandle {
    commands: mpsc::Sender<OpenCodeEventCommand>,
    lifecycle_timeout: Duration,
}

enum OpenCodeEventCommand {
    Synchronize {
        conversation_id: ConversationId,
        deadline: Instant,
        completion: oneshot::Sender<Result<(), KitError>>,
    },
    Close {
        deadline: Instant,
        claim: HierarchyClaim,
        admission: HierarchyAdmission,
        record: Box<ConversationRecord>,
        completion: oneshot::Sender<(HierarchyClaim, HierarchyAdmission, Result<(), KitError>)>,
    },
}

impl OpenCodeEventCommand {
    fn deadline(&self) -> Instant {
        match self {
            Self::Synchronize { deadline, .. } | Self::Close { deadline, .. } => *deadline,
        }
    }
    fn conversation_id(&self) -> &ConversationId {
        match self {
            Self::Synchronize {
                conversation_id, ..
            } => conversation_id,
            Self::Close { record, .. } => record.id(),
        }
    }
    fn close_record(&self) -> Option<&ConversationRecord> {
        match self {
            Self::Close { record, .. } => Some(record),
            Self::Synchronize { .. } => None,
        }
    }
    fn abandoned(&self) -> bool {
        match self {
            Self::Synchronize { completion, .. } => completion.is_closed(),
            Self::Close { .. } => false,
        }
    }
    fn complete(self, result: Result<(), KitError>) {
        match self {
            Self::Synchronize { completion, .. } => {
                let _ = completion.send(result);
            }
            Self::Close {
                claim,
                admission,
                completion,
                ..
            } => {
                let _ = completion.send((claim, admission, result));
            }
        }
    }
}

impl OpenCodeEvents {
    pub(super) async fn start<S: StateStoreBackend>(
        http: OpenCodeHttp,
        state: OpenCodeStateStore<S>,
        hierarchy: HierarchyRuntime,
        agent_id: AgentId,
        tools: OpenCodeTools,
        policy: OpenCodeEventPolicy,
    ) -> (Self, Result<(), KitError>) {
        let (commands, command_receiver) = mpsc::channel(16);
        let (stop, stop_receiver) = oneshot::channel();
        let (ready, readiness) = oneshot::channel();
        let lifecycle_timeout = policy.lifecycle_timeout;
        let readiness_deadline = policy.initial_deadline;
        let owner = OpenCodeEventOwner {
            http,
            state,
            hierarchy,
            agent_id,
            tools,
            idle_stream_timeout: policy.idle_stream_timeout,
            lifecycle_timeout,
            owned_sessions: BTreeSet::new(),
            session_bindings: BTreeMap::new(),
            pending_deleted_sessions: BTreeSet::new(),
            terminal_sessions: BTreeSet::new(),
            retired_sessions: BTreeSet::new(),
            commands: command_receiver,
            stop: Some(stop_receiver),
            ready: Some(ready),
            initial_deadline: policy.initial_deadline,
            notifications: OpenCodeNotifications::new(),
        };
        let events = Self {
            handle: OpenCodeEventHandle::new(commands, lifecycle_timeout),
            stop: Some(stop),
            task: Some(tokio::spawn(owner.run())),
        };
        let result = match tokio::time::timeout_at(readiness_deadline, readiness).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(
                KitError::integration("event startup task ended before readiness")
                    .with_detail(ErrorDetail::TaskTerminated)
                    .at_phase(ErrorPhase::EventReady),
            ),
            Err(_) => Err(KitError::timeout("event startup deadline elapsed")
                .at_phase(ErrorPhase::EventReady)
                .with_detail(ErrorDetail::DeadlineElapsed {
                    kind: DeadlineKind::Lifecycle,
                    timeout: Some(lifecycle_timeout),
                })),
        };
        (events, result)
    }

    pub(super) fn handle(&self) -> OpenCodeEventHandle {
        self.handle.clone()
    }

    pub(super) async fn shutdown(mut self, deadline: Instant) -> Result<(), KitError> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let Some(mut task) = self.task.take() else {
            return Ok(());
        };
        match tokio::time::timeout_at(deadline, &mut task).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(KitError::integration("event task could not be joined")),
            Err(_) => {
                task.abort();
                let _ = task.await;
                Err(KitError::timeout("event shutdown deadline elapsed"))
            }
        }
    }
}

impl Drop for OpenCodeEvents {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl OpenCodeEventHandle {
    fn new(commands: mpsc::Sender<OpenCodeEventCommand>, lifecycle_timeout: Duration) -> Self {
        Self {
            commands,
            lifecycle_timeout,
        }
    }

    pub(crate) fn deadline(&self) -> Instant {
        Instant::now() + self.lifecycle_timeout
    }

    pub(crate) async fn synchronize(
        &self,
        conversation_id: &ConversationId,
        deadline: Instant,
    ) -> Result<(), KitError> {
        if Instant::now() >= deadline {
            return Err(KitError::timeout("event synchronization deadline elapsed")
                .at_phase(ErrorPhase::EventSynchronization));
        }
        let permit = tokio::time::timeout_at(deadline, self.commands.reserve())
            .await
            .map_err(|_| {
                KitError::timeout("event synchronization deadline elapsed")
                    .at_phase(ErrorPhase::EventSynchronization)
            })?
            .map_err(|_| KitError::closed("event delivery is closed"))?;
        if Instant::now() >= deadline {
            return Err(KitError::timeout("event synchronization deadline elapsed")
                .at_phase(ErrorPhase::EventSynchronization));
        }
        let (completion, result) = oneshot::channel();
        permit.send(OpenCodeEventCommand::Synchronize {
            conversation_id: conversation_id.clone(),
            deadline,
            completion,
        });
        tokio::time::timeout_at(deadline, result)
            .await
            .map_err(|_| {
                KitError::timeout("event synchronization deadline elapsed")
                    .at_phase(ErrorPhase::EventSynchronization)
            })?
            .map_err(|_| KitError::closed("event synchronization completion was lost"))?
    }
    pub(crate) async fn prepare_close(
        &self,
        record: ConversationRecord,
        claim: HierarchyClaim,
        admission: HierarchyAdmission,
        deadline: Instant,
    ) -> Result<(HierarchyClaim, HierarchyAdmission), KitError> {
        if Instant::now() >= deadline {
            return Err(
                KitError::timeout("conversation cleanup admission deadline elapsed")
                    .at_phase(ErrorPhase::EventSynchronization),
            );
        }
        let permit = tokio::time::timeout_at(deadline, self.commands.reserve())
            .await
            .map_err(|_| {
                KitError::timeout("conversation cleanup admission deadline elapsed")
                    .at_phase(ErrorPhase::EventSynchronization)
            })?
            .map_err(|_| KitError::closed("event delivery is closed"))?;
        if Instant::now() >= deadline {
            return Err(
                KitError::timeout("conversation cleanup admission deadline elapsed")
                    .at_phase(ErrorPhase::EventSynchronization),
            );
        }
        let (completion, receiver) = oneshot::channel();
        permit.send(OpenCodeEventCommand::Close {
            deadline,
            record: Box::new(record),
            claim,
            admission,
            completion,
        });
        let (claim, admission, result) = tokio::time::timeout_at(deadline, receiver)
            .await
            .map_err(|_| {
                KitError::timeout("conversation interaction cleanup deadline elapsed")
                    .at_phase(ErrorPhase::EventSynchronization)
            })?
            .map_err(|_| KitError::closed("conversation cleanup owner ended"))?;
        match result {
            Ok(()) => Ok((claim, admission)),
            Err(error) => admission.publish(Err(error)),
        }
    }
}

impl OpenCodeEventPolicy {
    pub(super) fn new(
        idle_stream_timeout: Duration,
        initial_deadline: Instant,
        lifecycle_timeout: Duration,
    ) -> Self {
        Self {
            idle_stream_timeout,
            initial_deadline,
            lifecycle_timeout,
        }
    }
}

impl OpenCodeLiveEvents {
    fn new(stream: OpenCodeEventStream) -> Self {
        Self {
            stream,
            buffered: VecDeque::new(),
            buffered_bytes: 0,
        }
    }

    fn capture(&mut self, event: OpenCodeInboundEvent) -> Result<(), OpenCodeRecoveryFailure> {
        self.buffered_bytes = self
            .buffered_bytes
            .checked_add(event.encoded_len())
            .filter(|bytes| *bytes <= MAX_LIVE_OVERLAP_BYTES)
            .ok_or_else(|| {
                OpenCodeRecoveryFailure::Stop(KitError::integration(
                    "live event overlap exceeds its private byte bound",
                ))
            })?;
        if self.buffered.len() == MAX_LIVE_OVERLAP_EVENTS {
            return Err(OpenCodeRecoveryFailure::Stop(KitError::integration(
                "live event overlap exceeds its configured bound",
            )));
        }
        self.buffered.push_back(event);
        Ok(())
    }

    fn take_buffered(&mut self) -> Option<OpenCodeInboundEvent> {
        let event = self.buffered.pop_front()?;
        self.buffered_bytes = self.buffered_bytes.saturating_sub(event.encoded_len());
        Some(event)
    }

    async fn next(&mut self) -> Result<OpenCodeInboundEvent, KitError> {
        match self.take_buffered() {
            Some(event) => Ok(event),
            None => OpenCodeInboundEvent::from_global_frame(self.stream.next_complete().await?),
        }
    }

    async fn capture_while<T>(
        &mut self,
        operation: impl Future<Output = T>,
    ) -> Result<T, OpenCodeRecoveryFailure> {
        tokio::pin!(operation);
        loop {
            tokio::select! {
                biased;
                output = &mut operation => return Ok(output),
                frame = self.stream.next_complete() => {
                    let frame = frame.map_err(OpenCodeRecoveryFailure::stream)?;
                    self.capture(OpenCodeInboundEvent::from_global_frame(frame)?)?;
                }
            }
        }
    }

    async fn capture_until_settled<T, E: Clone + Into<OpenCodeRecoveryFailure>>(
        &mut self,
        operation: impl Future<Output = Result<T, E>>,
    ) -> Result<Result<T, E>, OpenCodeRecoveryFailure> {
        tokio::pin!(operation);
        loop {
            tokio::select! {
                biased;
                output = &mut operation => return Ok(output),
                frame = self.stream.next_complete() => {
                    let failure = match frame {
                        Ok(frame) => match OpenCodeInboundEvent::from_global_frame(frame) {
                            Ok(event) => match self.capture(event) {
                                Ok(()) => continue,
                                Err(failure) => failure,
                            },
                            Err(error) => error.into(),
                        },
                        Err(error) => OpenCodeRecoveryFailure::stream(error),
                    };
                    return match operation.await {
                        Err(error) => match error.clone().into() {
                            OpenCodeRecoveryFailure::Stop(_)
                            | OpenCodeRecoveryFailure::Parked(_) => Ok(Err(error)),
                            OpenCodeRecoveryFailure::Retry(_)
                            | OpenCodeRecoveryFailure::Reconnect(_) => Err(failure),
                        },
                        Ok(_) => Err(failure),
                    };
                }
            }
        }
    }

    async fn capture_until(&mut self, deadline: Instant) -> Result<(), OpenCodeRecoveryFailure> {
        loop {
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => return Ok(()),
                frame = self.stream.next_complete() => {
                    let frame = frame.map_err(OpenCodeRecoveryFailure::stream)?;
                    self.capture(OpenCodeInboundEvent::from_global_frame(frame)?)?;
                }
            }
        }
    }
}

impl<S: StateStoreBackend> OpenCodeEventOwner<S> {
    async fn run(mut self) -> Result<(), KitError> {
        let mut stop = self
            .stop
            .take()
            .ok_or_else(|| KitError::invalid_state("event stop owner is unavailable"))?;
        self.drive(&mut stop).await
    }

    async fn drive(&mut self, stop: &mut oneshot::Receiver<()>) -> Result<(), KitError> {
        let mut phase = OpenCodeEventPhase::Starting {
            deadline: self.initial_deadline,
        };
        loop {
            phase = match phase {
                OpenCodeEventPhase::Starting { deadline } => {
                    OpenCodeEventPhase::Recovering(OpenCodeRecovery::Startup { deadline })
                }
                OpenCodeEventPhase::Live(mut live) => {
                    let activity = tokio::select! {
                        biased;
                        _ = &mut *stop => return Ok(()),
                        command = self.commands.recv() => {
                            let Some(command) = command else {
                                self.invalidate();
                                return Err(KitError::closed(
                                    "event command owner closed unexpectedly",
                                ));
                            };
                            OpenCodeEventActivity::Synchronize(command)
                        }
                        event = live.next() => OpenCodeEventActivity::Inbound(Box::new(event)),
                    };
                    match activity {
                        OpenCodeEventActivity::Synchronize(command) => {
                            if command.abandoned() {
                                OpenCodeEventPhase::Live(live)
                            } else if Instant::now() >= command.deadline() {
                                command.complete(Err(KitError::timeout(
                                    "event synchronization deadline elapsed",
                                )
                                .at_phase(ErrorPhase::EventSynchronization)));
                                OpenCodeEventPhase::Live(live)
                            } else {
                                OpenCodeEventPhase::Recovering(OpenCodeRecovery::Synchronize(
                                    Box::new(OpenCodeSameStreamRecovery::Command {
                                        live: *live,
                                        command,
                                    }),
                                ))
                            }
                        }
                        OpenCodeEventActivity::Inbound(activity) => {
                            self.advance_live(live, *activity).await?
                        }
                    }
                }
                OpenCodeEventPhase::Recovering(recovery) => {
                    let reason = recovery.reason();
                    match recovery {
                        OpenCodeRecovery::Startup { deadline }
                        | OpenCodeRecovery::Reconnect { deadline } => {
                            let startup = matches!(reason, OpenCodeRecoveryReason::Startup);
                            let Some(result) =
                                self.connect_until(deadline, reason, stop, None).await
                            else {
                                if let Some(ready) = self.ready.take() {
                                    let _ = ready
                                        .send(Err(KitError::closed("event startup was cancelled")));
                                }
                                return Ok(());
                            };
                            match result {
                                Ok((live, synchronized)) => {
                                    if startup {
                                        let Some(ready) = self.ready.take() else {
                                            return Err(KitError::invalid_state(
                                                "event readiness owner is unavailable",
                                            ));
                                        };
                                        if ready.send(synchronized).is_err() {
                                            return Ok(());
                                        }
                                    }
                                    OpenCodeEventPhase::Live(Box::new(live))
                                }
                                Err(
                                    OpenCodeRecoveryFailure::Parked(error)
                                    | OpenCodeRecoveryFailure::Retry(error)
                                    | OpenCodeRecoveryFailure::Reconnect(error)
                                    | OpenCodeRecoveryFailure::Stop(error),
                                ) => {
                                    self.invalidate();
                                    if startup {
                                        if let Some(ready) = self.ready.take() {
                                            let _ = ready.send(Err(error));
                                        }
                                        return Ok(());
                                    }
                                    return Err(error);
                                }
                            }
                        }
                        OpenCodeRecovery::Synchronize(recovery) => {
                            let Some(phase) = self.finish_synchronization(*recovery, stop).await?
                            else {
                                return Ok(());
                            };
                            phase
                        }
                    }
                }
            };
        }
    }

    async fn advance_live(
        &mut self,
        live: Box<OpenCodeLiveEvents>,
        activity: Result<OpenCodeInboundEvent, KitError>,
    ) -> Result<OpenCodeEventPhase, KitError> {
        let event = match activity {
            Ok(event) => event,
            Err(error) => {
                self.state.interrupt_prompt_outputs(&error);
                return match OpenCodeRecoveryFailure::stream(error) {
                    OpenCodeRecoveryFailure::Reconnect(_) => {
                        drop(live);
                        Ok(OpenCodeEventPhase::Recovering(
                            OpenCodeRecovery::Reconnect {
                                deadline: Instant::now() + self.lifecycle_timeout,
                            },
                        ))
                    }
                    OpenCodeRecoveryFailure::Parked(error)
                    | OpenCodeRecoveryFailure::Retry(error)
                    | OpenCodeRecoveryFailure::Stop(error) => {
                        self.invalidate();
                        Err(error)
                    }
                };
            }
        };
        let deadline = Instant::now() + self.lifecycle_timeout;
        let disposition = self.apply_inbound(event, deadline).await;
        if matches!(&disposition, Ok(OpenCodeEventDisposition::Gap) | Err(_)) {
            self.state.interrupt_prompt_outputs(&KitError::integration(
                "prompt event continuity was lost",
            ));
        }
        match disposition {
            Ok(
                OpenCodeEventDisposition::Applied
                | OpenCodeEventDisposition::RememberedDuplicate
                | OpenCodeEventDisposition::Duplicate,
            ) => Ok(OpenCodeEventPhase::Live(live)),
            Ok(OpenCodeEventDisposition::Gap) => Ok(OpenCodeEventPhase::Recovering(
                OpenCodeRecovery::Synchronize(Box::new(OpenCodeSameStreamRecovery::Gap {
                    deadline: Instant::now() + self.lifecycle_timeout,
                    live: *live,
                })),
            )),
            Err(OpenCodeRecoveryFailure::Retry(_)) if Instant::now() < deadline => Ok(
                OpenCodeEventPhase::Recovering(OpenCodeRecovery::Synchronize(Box::new(
                    OpenCodeSameStreamRecovery::Application {
                        deadline,
                        live: *live,
                    },
                ))),
            ),
            Err(
                OpenCodeRecoveryFailure::Parked(error)
                | OpenCodeRecoveryFailure::Retry(error)
                | OpenCodeRecoveryFailure::Reconnect(error)
                | OpenCodeRecoveryFailure::Stop(error),
            ) => {
                self.invalidate();
                Err(error)
            }
        }
    }

    async fn finish_synchronization(
        &mut self,
        recovery: OpenCodeSameStreamRecovery,
        stop: &mut oneshot::Receiver<()>,
    ) -> Result<Option<OpenCodeEventPhase>, KitError> {
        let reason = recovery.reason();
        let (deadline, mut live, command) = match recovery {
            OpenCodeSameStreamRecovery::Application { deadline, live }
            | OpenCodeSameStreamRecovery::Gap { deadline, live } => (deadline, live, None),
            OpenCodeSameStreamRecovery::Command { live, command } => {
                (command.deadline(), live, Some(command))
            }
        };
        match self
            .synchronize_until(&mut live, deadline, reason, stop, command.as_ref())
            .await
        {
            None => Ok(None),
            Some(Ok(())) => {
                if let Some(command) = command {
                    command.complete(Ok(()));
                }
                Ok(Some(OpenCodeEventPhase::Live(Box::new(live))))
            }
            Some(Err(OpenCodeRecoveryFailure::Reconnect(error))) => {
                self.state.interrupt_prompt_outputs(&error);
                drop(live);
                let Some(result) = self
                    .connect_until(deadline, reason, stop, command.as_ref())
                    .await
                else {
                    return Ok(None);
                };
                match result {
                    Ok((replacement, synchronized)) => {
                        if let Some(command) = command {
                            command.complete(synchronized);
                        }
                        Ok(Some(OpenCodeEventPhase::Live(Box::new(replacement))))
                    }
                    Err(
                        OpenCodeRecoveryFailure::Parked(error)
                        | OpenCodeRecoveryFailure::Retry(error)
                        | OpenCodeRecoveryFailure::Reconnect(error)
                        | OpenCodeRecoveryFailure::Stop(error),
                    ) => {
                        if let Some(command) = command {
                            command.complete(Err(error.clone()));
                        }
                        self.invalidate();
                        Err(error)
                    }
                }
            }
            Some(Err(OpenCodeRecoveryFailure::Parked(error))) => {
                if let Some(command) = command {
                    command.complete(Err(error));
                }
                // This generation remains fenced; unrelated event delivery stays live.
                Ok(Some(OpenCodeEventPhase::Live(Box::new(live))))
            }
            Some(Err(OpenCodeRecoveryFailure::Retry(error))) => {
                if matches!(reason, OpenCodeRecoveryReason::Command) {
                    if let Some(command) = command {
                        command.complete(Err(error));
                    }
                    Ok(Some(OpenCodeEventPhase::Live(Box::new(live))))
                } else {
                    self.invalidate();
                    Err(error)
                }
            }
            Some(Err(OpenCodeRecoveryFailure::Stop(error))) => {
                self.invalidate();
                if let Some(command) = command {
                    command.complete(Err(error.clone()));
                }
                Err(error)
            }
        }
    }

    async fn connect_until(
        &mut self,
        deadline: Instant,
        reason: OpenCodeRecoveryReason,
        stop: &mut oneshot::Receiver<()>,
        command: Option<&OpenCodeEventCommand>,
    ) -> Option<Result<(OpenCodeLiveEvents, Result<(), KitError>), OpenCodeRecoveryFailure>> {
        let mut last_error = None;
        loop {
            if !matches!(stop.try_recv(), Err(oneshot::error::TryRecvError::Empty)) {
                return None;
            }
            if Instant::now() >= deadline {
                return Some(Err(reason.deadline_failure(last_error)));
            }
            let attempt = tokio::select! {
                biased;
                _ = &mut *stop => return None,
                attempt = tokio::time::timeout_at(deadline, self.connect()) => attempt,
            };
            match attempt {
                Ok(Ok(mut live)) => match self
                    .synchronize_until(&mut live, deadline, reason, stop, command)
                    .await
                {
                    Some(Ok(())) => return Some(Ok((live, Ok(())))),
                    Some(Err(OpenCodeRecoveryFailure::Parked(error)))
                        if !matches!(reason, OpenCodeRecoveryReason::Startup) =>
                    {
                        // Reconnection succeeded. Preserve that stream and the
                        // scoped failure separately; no mutation is re-entered.
                        return Some(Ok((live, Err(error))));
                    }
                    None => return None,
                    Some(Err(OpenCodeRecoveryFailure::Reconnect(error))) => {
                        last_error = Some(error);
                        drop(live);
                    }
                    Some(Err(
                        failure @ (OpenCodeRecoveryFailure::Parked(_)
                        | OpenCodeRecoveryFailure::Retry(_)
                        | OpenCodeRecoveryFailure::Stop(_)),
                    )) => return Some(Err(failure)),
                },
                Ok(Err(
                    failure @ (OpenCodeRecoveryFailure::Stop(_)
                    | OpenCodeRecoveryFailure::Parked(_)),
                )) => {
                    return Some(Err(failure));
                }
                Ok(Err(
                    OpenCodeRecoveryFailure::Retry(error)
                    | OpenCodeRecoveryFailure::Reconnect(error),
                )) => last_error = Some(error),
                Err(_) => return Some(Err(reason.deadline_failure(last_error))),
            }
            tokio::select! {
                biased;
                _ = &mut *stop => return None,
                _ = tokio::time::sleep_until(
                    (Instant::now() + EVENT_RECONNECT_BACKOFF).min(deadline)
                ) => {}
            }
        }
    }

    async fn connect(&mut self) -> Result<OpenCodeLiveEvents, OpenCodeRecoveryFailure> {
        let response = self.http.global_event().await?;
        let stream = OpenCodeEventStream::new(response, self.idle_stream_timeout)?;
        Ok(OpenCodeLiveEvents::new(stream))
    }

    async fn synchronize_until(
        &mut self,
        live: &mut OpenCodeLiveEvents,
        deadline: Instant,
        reason: OpenCodeRecoveryReason,
        stop: &mut oneshot::Receiver<()>,
        command: Option<&OpenCodeEventCommand>,
    ) -> Option<Result<(), OpenCodeRecoveryFailure>> {
        let mut last_error = None;
        loop {
            if !matches!(stop.try_recv(), Err(oneshot::error::TryRecvError::Empty)) {
                return None;
            }
            if Instant::now() >= deadline {
                return Some(Err(reason.deadline_failure(last_error)));
            }
            match self.synchronize_once(live, deadline, command).await {
                Ok(()) => return Some(Ok(())),
                Err(OpenCodeRecoveryFailure::Retry(error)) => last_error = Some(error),
                Err(
                    failure @ (OpenCodeRecoveryFailure::Parked(_)
                    | OpenCodeRecoveryFailure::Reconnect(_)
                    | OpenCodeRecoveryFailure::Stop(_)),
                ) => return Some(Err(failure)),
            }
            tokio::select! {
                biased;
                _ = &mut *stop => return None,
                result = live.capture_until(
                    (Instant::now() + EVENT_RECONNECT_BACKOFF).min(deadline)
                ) => if let Err(failure) = result {
                    return Some(Err(failure));
                },
            }
        }
    }

    async fn synchronize_once(
        &mut self,
        live: &mut OpenCodeLiveEvents,
        deadline: Instant,
        command: Option<&OpenCodeEventCommand>,
    ) -> Result<(), OpenCodeRecoveryFailure> {
        let pending_before_recovery = self.pending_deleted_sessions.clone();
        let rows = tokio::time::timeout_at(deadline, async {
            let high_water = live
                .capture_while(self.state.event_high_water(&self.agent_id))
                .await??;
            let request = OpenCodeHistoryRequest::new(high_water)?;
            let http = self.http.clone();
            let rows = live.capture_while(http.history(&request)).await??;
            Ok::<_, OpenCodeRecoveryFailure>(rows)
        })
        .await
        .map_err(|_| {
            OpenCodeRecoveryFailure::Retry(KitError::timeout("event recovery deadline elapsed"))
        })??;

        tokio::time::timeout_at(deadline, live.capture_while(self.refresh_ownership()))
            .await
            .map_err(|_| {
                OpenCodeRecoveryFailure::Retry(KitError::timeout("event recovery deadline elapsed"))
            })???;
        let mut history_by_aggregate = BTreeMap::<String, Vec<OpenCodeInboundEvent>>::new();
        for row in rows {
            let event = OpenCodeInboundEvent::from_history_row(row)?;
            let OpenCodeInboundEvent::History(sequenced) = &event else {
                return Err(KitError::invalid_state(
                    "history decoder produced a non-history event",
                )
                .into());
            };
            history_by_aggregate
                .entry(sequenced.aggregate_id().to_owned())
                .or_default()
                .push(event);
        }
        let mut first_error = None;
        for events in history_by_aggregate.values_mut() {
            events.sort_by_key(|event| match event {
                OpenCodeInboundEvent::History(sequenced) => sequenced.sequence(),
                OpenCodeInboundEvent::Normal(_) | OpenCodeInboundEvent::Sync(_) => 0,
            });
            for event in events.drain(..) {
                match live
                    .capture_until_settled(self.apply_inbound(event, deadline))
                    .await?
                {
                    Ok(
                        OpenCodeEventDisposition::Applied
                        | OpenCodeEventDisposition::RememberedDuplicate
                        | OpenCodeEventDisposition::Duplicate,
                    ) => {}
                    Ok(OpenCodeEventDisposition::Gap) => {
                        first_error.get_or_insert_with(|| {
                            KitError::busy("history retains an aggregate gap")
                        });
                        break;
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        self.apply_buffered_overlap(
            live,
            &mut first_error,
            "live overlap retains an aggregate gap",
            deadline,
        )
        .await?;
        if let Some(error) = first_error {
            return Err(error.into());
        }
        self.release_deleted_routes(&pending_before_recovery);
        self.reconcile_pending(live, deadline, command).await?;
        self.apply_buffered_overlap(
            live,
            &mut first_error,
            "live overlap after reconciliation retains an aggregate gap",
            deadline,
        )
        .await?;
        if let Some(error) = first_error {
            return Err(error.into());
        }
        // Recovery can finish without a status event or a changed continuation.
        // Wake each validated foreground owner to obtain its own fresh status;
        // successful reconciliation itself is not evidence of native Idle.
        let conversations = self
            .session_bindings
            .values()
            .map(OpenCodeSessionBinding::conversation_id)
            .filter(|conversation_id| {
                command.is_none_or(|command| command.conversation_id() == *conversation_id)
            })
            .collect::<BTreeSet<_>>();
        for conversation_id in conversations {
            self.state.refresh_prompt_status(conversation_id);
        }
        Ok(())
    }

    async fn apply_buffered_overlap(
        &mut self,
        live: &mut OpenCodeLiveEvents,
        first_error: &mut Option<KitError>,
        gap_message: &'static str,
        deadline: Instant,
    ) -> Result<(), OpenCodeRecoveryFailure> {
        while let Some(event) = live.take_buffered() {
            match live
                .capture_until_settled(self.apply_inbound(event, deadline))
                .await?
            {
                Ok(
                    OpenCodeEventDisposition::Applied
                    | OpenCodeEventDisposition::RememberedDuplicate
                    | OpenCodeEventDisposition::Duplicate,
                ) => {}
                Ok(OpenCodeEventDisposition::Gap) => {
                    first_error.get_or_insert_with(|| KitError::busy(gap_message));
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    async fn refresh_ownership(&mut self) -> Result<(), KitError> {
        for session_id in self.state.retired_sessions(&self.agent_id).await? {
            self.remember_retired_session(session_id)?;
        }
        let inventory = self.http.session_inventory().await?;
        if self
            .retired_sessions
            .iter()
            .any(|session_id| inventory.contains(session_id))
        {
            return Err(KitError::invalid_state(
                "retired session returned to native inventory",
            ));
        }
        let bindings = self.state.bindings_for_agent(&self.agent_id).await?;
        let cleanups = self.state.cleanup_obligations(&self.agent_id).await?;
        let mut candidate_owned = BTreeSet::new();
        let mut candidate_bindings = BTreeMap::new();
        let mut candidate_terminal = self.retired_sessions.clone();
        for cleanup in cleanups {
            candidate_terminal.extend(cleanup.session_ids().iter().cloned());
            candidate_owned.extend(cleanup.session_ids().iter().cloned());
        }
        for (session_id, binding) in &self.session_bindings {
            if binding
                .session_id()
                .is_some_and(|root| self.pending_deleted_sessions.contains(root))
            {
                if inventory.contains(session_id) {
                    return Err(KitError::invalid_state(
                        "deleted session returned to native inventory",
                    ));
                }
                candidate_owned.insert(session_id.clone());
                candidate_bindings.insert(session_id.clone(), binding.clone());
            }
        }
        for binding in bindings {
            let root = binding.require_session_id()?.to_owned();
            let mut sessions = vec![root.clone()];
            if inventory.contains(&root) {
                sessions = inventory.subtree(std::slice::from_ref(&root), binding.directory())?;
            }
            for session_id in sessions {
                if let Some(previous) =
                    candidate_bindings.insert(session_id.clone(), binding.clone())
                    && previous.conversation_id() != binding.conversation_id()
                {
                    return Err(KitError::invalid_state(
                        "native session belongs to multiple conversations",
                    ));
                }
                candidate_owned.insert(session_id);
            }
        }
        let newly_retired = self
            .session_bindings
            .keys()
            .filter(|session_id| {
                !inventory.contains(session_id) && !candidate_bindings.contains_key(*session_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        for session_id in newly_retired {
            self.remember_retired_session(session_id)?;
        }
        candidate_terminal.extend(self.retired_sessions.iter().cloned());
        if self
            .retired_sessions
            .iter()
            .any(|session_id| candidate_bindings.contains_key(session_id))
        {
            return Err(KitError::invalid_state(
                "retired session regained a durable binding",
            ));
        }
        candidate_owned.extend(self.retired_sessions.iter().cloned());
        if !inventory.contains_only(&candidate_owned) {
            return Err(KitError::busy(
                "event ownership is awaiting a durable binding",
            ));
        }
        self.owned_sessions = candidate_owned;
        self.session_bindings = candidate_bindings;
        self.terminal_sessions = candidate_terminal;
        Ok(())
    }

    fn remember_retired_session(&mut self, session_id: String) -> Result<(), KitError> {
        if !session_id.starts_with("ses") || session_id.trim() != session_id {
            return Err(KitError::invalid_state(
                "retired session identity is malformed",
            ));
        }
        if self.retired_sessions.contains(&session_id) {
            return Ok(());
        }
        if self.retired_sessions.len() == MAX_RETIRED_SESSIONS {
            return Err(KitError::integration(
                "retired session state exceeds its configured bound",
            ));
        }
        self.terminal_sessions.insert(session_id.clone());
        self.retired_sessions.insert(session_id);
        Ok(())
    }

    async fn reconcile_pending(
        &mut self,
        live: &mut OpenCodeLiveEvents,
        deadline: Instant,
        command: Option<&OpenCodeEventCommand>,
    ) -> Result<(), OpenCodeRecoveryFailure> {
        loop {
            if Instant::now() >= deadline {
                return Err(OpenCodeRecoveryFailure::Retry(KitError::timeout(
                    "event reconciliation deadline elapsed",
                )));
            }
            let (mut pending, mut native_pending) = tokio::time::timeout_at(deadline, async {
                let bindings = live
                    .capture_while(self.state.bindings_for_agent(&self.agent_id))
                    .await??;
                let directories = bindings
                    .iter()
                    .map(|binding| binding.directory().to_path_buf())
                    .collect::<BTreeSet<_>>();
                let mut pending = Vec::new();
                let mut native_pending = BTreeSet::new();
                for directory in directories {
                    let routing = OpenCodeRouting::for_directory(directory.clone())?;
                    let http = self.http.clone();
                    for request in live.capture_while(http.permissions(&routing)).await?? {
                        let request = request.validate_v1()?;
                        if !native_pending
                            .insert((request.session_id().to_owned(), request.id().to_owned()))
                        {
                            return Err(KitError::protocol(
                                "pending interaction identity is duplicated",
                            )
                            .into());
                        }
                        pending.push((
                            OpenCodeKnownEvent::PermissionAsked(request),
                            directory.clone(),
                        ));
                    }
                    let http = self.http.clone();
                    for request in live.capture_while(http.questions(&routing)).await?? {
                        let request = request.validate_v1()?;
                        if !native_pending
                            .insert((request.session_id().to_owned(), request.id().to_owned()))
                        {
                            return Err(KitError::protocol(
                                "pending interaction identity is duplicated",
                            )
                            .into());
                        }
                        pending.push((
                            OpenCodeKnownEvent::QuestionAsked(request),
                            directory.clone(),
                        ));
                    }
                }
                Ok::<_, OpenCodeRecoveryFailure>((pending, native_pending))
            })
            .await
            .map_err(|_| {
                OpenCodeRecoveryFailure::Retry(KitError::timeout(
                    "event reconciliation deadline elapsed",
                ))
            })??;
            // Validate the complete inventory before scoping mutable work. Otherwise
            // a changed session can be filtered out and mistaken for disappearance
            // of its original owned request during continuation reconciliation.
            for (event, directory) in &pending {
                let session = event.session_id().ok_or_else(|| {
                    KitError::protocol("pending interaction has no session ownership")
                })?;
                tokio::time::timeout_at(
                    deadline,
                    live.capture_while(self.binding_for_event(session, Some(directory))),
                )
                .await
                .map_err(|_| {
                    OpenCodeRecoveryFailure::Retry(KitError::timeout(
                        "pending interaction ownership deadline elapsed",
                    ))
                })???;
            }
            if let Some(command) = command {
                // A prompt or Close command reconciles only its conversation. Full
                // native inventory still fences duplicate and collateral identities.
                pending.retain(|(event, _)| {
                    event.session_id().is_some_and(|session| {
                        self.session_bindings.get(session).is_some_and(|binding| {
                            binding.conversation_id() == command.conversation_id()
                        })
                    })
                });
            }
            if self
                .cancel_pending_native(
                    live,
                    &mut pending,
                    &mut native_pending,
                    command.and_then(OpenCodeEventCommand::close_record),
                    deadline,
                )
                .await?
            {
                // Native settlement applies buffered asked events. Their absence
                // from the earlier inventory cannot settle those new obligations.
                continue;
            }
            for (event, directory) in pending {
                if Instant::now() >= deadline {
                    return Err(OpenCodeRecoveryFailure::Retry(KitError::timeout(
                        "event reconciliation deadline elapsed",
                    )));
                }
                live.capture_until_settled(self.apply_pending(event, &directory, deadline))
                    .await??;
                if Instant::now() >= deadline {
                    return Err(OpenCodeRecoveryFailure::Retry(KitError::timeout(
                        "event reconciliation deadline elapsed",
                    )));
                }
            }
            if Instant::now() >= deadline {
                return Err(OpenCodeRecoveryFailure::Retry(KitError::timeout(
                    "event reconciliation deadline elapsed",
                )));
            }
            live.capture_until_settled(self.state.reconcile_continuations(
                &self.agent_id,
                &native_pending,
                command.map(OpenCodeEventCommand::conversation_id),
                deadline,
            ))
            .await??;
            if Instant::now() >= deadline {
                return Err(OpenCodeRecoveryFailure::Retry(KitError::timeout(
                    "event reconciliation deadline elapsed",
                )));
            }
            return Ok(());
        }
    }

    async fn apply_pending(
        &mut self,
        event: OpenCodeKnownEvent,
        directory: &Path,
        deadline: Instant,
    ) -> Result<(), KitError> {
        let effect = tokio::time::timeout_at(deadline, async {
            let session_id = event
                .session_id()
                .ok_or_else(|| KitError::protocol("pending interaction has no session identity"))?;
            let binding = self
                .binding_for_event(session_id, Some(directory))
                .await?
                .ok_or_else(|| KitError::conflict("pending interaction has no durable binding"))?;
            let requests = OpenCodeServerRequests::for_binding(&binding)?;
            let semantics = match &event {
                OpenCodeKnownEvent::PermissionAsked(request) => requests.permission(request)?,
                OpenCodeKnownEvent::QuestionAsked(request) => requests.question(request)?,
                _ => {
                    return Err(KitError::invalid_state(
                        "pending reconciliation received another event",
                    ));
                }
            };
            Self::bound_effect(semantics, &binding, session_id)
        })
        .await
        .map_err(|_| KitError::timeout("event reconciliation deadline elapsed"))??;
        if Instant::now() >= deadline {
            return Err(KitError::timeout("event reconciliation deadline elapsed"));
        }
        self.state
            .reconcile_pending_interaction(effect, deadline)
            .await
    }

    async fn apply_inbound(
        &mut self,
        inbound: OpenCodeInboundEvent,
        deadline: Instant,
    ) -> Result<OpenCodeEventDisposition, OpenCodeRecoveryFailure> {
        let live = matches!(&inbound, OpenCodeInboundEvent::Normal(_));
        let history = matches!(&inbound, OpenCodeInboundEvent::History(_));
        let prepared = tokio::time::timeout_at(deadline, async {
            let (id, durable, directory, event_type, data, event) = match inbound {
                OpenCodeInboundEvent::Normal(normal) => (
                    normal.id().to_owned(),
                    None,
                    normal.routing().directory().map(str::to_owned),
                    normal.event_type().to_owned(),
                    serde_json::Value::Object(normal.data().clone()),
                    normal.event().clone(),
                ),
                OpenCodeInboundEvent::Sync(sequenced)
                | OpenCodeInboundEvent::History(sequenced) => (
                    sequenced.id().to_owned(),
                    Some((sequenced.aggregate_id().to_owned(), sequenced.sequence())),
                    sequenced
                        .routing()
                        .and_then(|routing| routing.directory())
                        .map(str::to_owned),
                    sequenced.event_type().to_owned(),
                    serde_json::Value::Object(sequenced.data().clone()),
                    sequenced.event().clone(),
                ),
            };
            let identity = match durable {
                Some((aggregate_id, sequence)) => OpenCodeEventIdentity::durable(
                    id,
                    aggregate_id,
                    sequence,
                    history,
                    event_type,
                    data,
                )?,
                None => OpenCodeEventIdentity::normal(id, event_type, data)?,
            };
            if let Some(session_id) = event.session_id()
                && self.belongs_to_terminal_conversation(session_id)
            {
                return Ok(None);
            }
            if let Some((aggregate_id, _)) = identity.durable_position()
                && !self.owned_sessions.contains(aggregate_id)
            {
                self.refresh_ownership().await?;
                if !self.owned_sessions.contains(aggregate_id) {
                    return Err(KitError::conflict(
                        "aggregate is outside the owned session closure",
                    ));
                }
            }
            let effect = self
                .effect(&event, directory.as_deref().map(Path::new))
                .await?;
            if matches!(&event, OpenCodeKnownEvent::SessionDeleted(_))
                && event
                    .session_id()
                    .is_some_and(|session_id| self.terminal_sessions.contains(session_id))
            {
                return Ok(None);
            }
            Ok::<_, KitError>(Some((identity, effect, event)))
        })
        .await
        .map_err(|_| {
            KitError::timeout("event application deadline elapsed")
                .at_phase(ErrorPhase::EventSynchronization)
        })??;
        let Some((identity, effect, event)) = prepared else {
            return Ok(OpenCodeEventDisposition::Duplicate);
        };
        let deleted = match &effect {
            OpenCodeEventEffect::ConversationDeleted {
                conversation_id,
                workspace_id,
                session_id,
            } => Some((
                workspace_id.clone(),
                conversation_id.clone(),
                session_id.clone(),
            )),
            OpenCodeEventEffect::None
            | OpenCodeEventEffect::CatalogInvalidated
            | OpenCodeEventEffect::Passive { .. }
            | OpenCodeEventEffect::Process { .. }
            | OpenCodeEventEffect::PromptStatus { .. }
            | OpenCodeEventEffect::PromptMessage { .. }
            | OpenCodeEventEffect::PromptPart { .. }
            | OpenCodeEventEffect::InteractionResolved { .. }
            | OpenCodeEventEffect::Interaction { .. } => None,
        };
        let mut last_conflict = None;
        let (disposition, deadline_elapsed) = loop {
            if Instant::now() >= deadline {
                let error = KitError::timeout("event application deadline elapsed")
                    .at_phase(ErrorPhase::EventSynchronization);
                return Err(match last_conflict {
                    Some(conflict) => error.with_supplementary(conflict).into(),
                    None => error.into(),
                });
            }
            let attempt = self
                .state
                .apply_event(&self.agent_id, identity.clone(), effect.clone(), deadline)
                .await;
            let deadline_elapsed = Instant::now() >= deadline;
            match (attempt, deadline_elapsed) {
                (Ok(disposition), elapsed) => break (disposition, elapsed),
                (Err(OpenCodeTransactionError::TerminalCleanup(error)), _) => {
                    // The event already committed. Neither its mutation nor an
                    // uncertain terminal deletion may be replayed by recovery.
                    return Err(OpenCodeRecoveryFailure::Stop(error));
                }
                (
                    Err(
                        OpenCodeTransactionError::TransactionConflict(error)
                        | OpenCodeTransactionError::Failure(error),
                    ),
                    true,
                ) => {
                    return Err(KitError::timeout("event application deadline elapsed")
                        .at_phase(ErrorPhase::EventSynchronization)
                        .with_supplementary(error)
                        .into());
                }
                (Err(OpenCodeTransactionError::TransactionConflict(error)), false) => {
                    last_conflict = Some(error);
                    tokio::task::yield_now().await;
                }
                (Err(OpenCodeTransactionError::Failure(error)), false) => return Err(error.into()),
            }
        };
        if disposition != OpenCodeEventDisposition::Gap
            && let Some(binding) = event
                .session_id()
                .and_then(|session_id| self.session_bindings.get(session_id))
        {
            self.state.observe_prompt_output(
                binding.conversation_id(),
                &event,
                live,
                disposition == OpenCodeEventDisposition::Applied,
            );
        }
        if disposition == OpenCodeEventDisposition::Applied
            && matches!(&effect, OpenCodeEventEffect::CatalogInvalidated)
        {
            self.tools.invalidate()?;
        }
        if matches!(
            disposition,
            OpenCodeEventDisposition::Applied | OpenCodeEventDisposition::RememberedDuplicate
        ) && let Some((workspace_id, conversation_id, session_id)) = deleted
        {
            self.hierarchy
                .close_conversation(&workspace_id, &conversation_id);
            self.state
                .notifications()
                .invalidate_conversation(&conversation_id);
            if disposition == OpenCodeEventDisposition::Applied {
                self.pending_deleted_sessions.insert(session_id.clone());
                self.terminal_sessions.insert(session_id);
            } else {
                self.release_deleted_route(&session_id);
            }
        }
        if deadline_elapsed {
            return Err(KitError::timeout("event application deadline elapsed")
                .at_phase(ErrorPhase::EventSynchronization)
                .into());
        }
        Ok(disposition)
    }

    fn belongs_to_terminal_conversation(&self, session_id: &str) -> bool {
        self.session_bindings
            .get(session_id)
            .is_some_and(|binding| {
                self.hierarchy
                    .conversation_is_terminal(binding.workspace_id(), binding.conversation_id())
            })
    }

    async fn effect(
        &mut self,
        event: &OpenCodeKnownEvent,
        routing_directory: Option<&Path>,
    ) -> Result<OpenCodeEventEffect, KitError> {
        let Some(session_id) = event.session_id() else {
            return Self::global_effect(self.notifications.classify(event, None)?);
        };
        if matches!(event, OpenCodeKnownEvent::SessionDeleted(_)) {
            self.refresh_ownership().await?;
            if self.terminal_sessions.contains(session_id) {
                return Ok(OpenCodeEventEffect::None);
            }
        }
        let Some(binding) = self
            .binding_for_event(session_id, routing_directory)
            .await?
        else {
            return Ok(OpenCodeEventEffect::None);
        };
        if matches!(
            event,
            OpenCodeKnownEvent::SessionCreated(event)
                | OpenCodeKnownEvent::SessionUpdated(event)
                if event.has_native_share()
        ) {
            return Err(KitError::invalid_state(
                "native session unexpectedly has a share binding",
            ));
        }
        let semantics = self
            .notifications
            .classify(event, Some(binding.directory()))?;
        Self::bound_effect(semantics, &binding, session_id)
    }

    fn global_effect(semantics: OpenCodeEventSemantics) -> Result<OpenCodeEventEffect, KitError> {
        match semantics {
            OpenCodeEventSemantics::Private | OpenCodeEventSemantics::UnknownFuture => {
                Ok(OpenCodeEventEffect::None)
            }
            OpenCodeEventSemantics::PromptStatus(_) => Err(KitError::protocol(
                "conversation-scoped prompt activity arrived without an owner",
            )),
            OpenCodeEventSemantics::PromptMessage { .. } => Err(KitError::protocol(
                "conversation-scoped prompt message arrived without an owner",
            )),
            OpenCodeEventSemantics::PromptPart { .. } => Err(KitError::protocol(
                "conversation-scoped prompt input arrived without an owner",
            )),
            OpenCodeEventSemantics::Process { .. } => Err(KitError::protocol(
                "conversation-scoped process state arrived without an owner",
            )),
            OpenCodeEventSemantics::CatalogInvalidated => {
                Ok(OpenCodeEventEffect::CatalogInvalidated)
            }
            OpenCodeEventSemantics::ConversationDeleted
            | OpenCodeEventSemantics::Notifications(_)
            | OpenCodeEventSemantics::Interaction { .. }
            | OpenCodeEventSemantics::InteractionResolved { .. } => Err(KitError::protocol(
                "global event has conversation-scoped semantics",
            )),
        }
    }

    fn bound_effect(
        semantics: OpenCodeEventSemantics,
        binding: &OpenCodeSessionBinding,
        session_id: &str,
    ) -> Result<OpenCodeEventEffect, KitError> {
        match semantics {
            OpenCodeEventSemantics::Private | OpenCodeEventSemantics::UnknownFuture => {
                Ok(OpenCodeEventEffect::None)
            }
            OpenCodeEventSemantics::PromptStatus(status) => Ok(OpenCodeEventEffect::PromptStatus {
                conversation_id: binding.conversation_id().clone(),
                status,
            }),
            OpenCodeEventSemantics::PromptMessage {
                session_id: evidence_session_id,
                message_id,
            } => {
                if evidence_session_id != session_id {
                    return Err(KitError::protocol(
                        "prompt message has contradictory session ownership",
                    ));
                }
                Ok(OpenCodeEventEffect::PromptMessage {
                    conversation_id: binding.conversation_id().clone(),
                    session_id: evidence_session_id,
                    message_id,
                })
            }
            OpenCodeEventSemantics::PromptPart {
                session_id: evidence_session_id,
                message_id,
                part_id,
            } => {
                if evidence_session_id != session_id {
                    return Err(KitError::protocol(
                        "prompt part has contradictory session ownership",
                    ));
                }
                Ok(OpenCodeEventEffect::PromptPart {
                    conversation_id: binding.conversation_id().clone(),
                    session_id: evidence_session_id,
                    message_id,
                    part_id,
                })
            }
            OpenCodeEventSemantics::CatalogInvalidated => Err(KitError::protocol(
                "session event has global catalog semantics",
            )),
            OpenCodeEventSemantics::Process { process, kind } => {
                if process.session_id() != session_id {
                    return Err(KitError::protocol(
                        "process state has contradictory session ownership",
                    ));
                }
                Ok(OpenCodeEventEffect::Process {
                    conversation_id: binding.conversation_id().clone(),
                    process,
                    kind,
                })
            }
            OpenCodeEventSemantics::ConversationDeleted => {
                if binding.session_id() == Some(session_id) {
                    Ok(OpenCodeEventEffect::ConversationDeleted {
                        conversation_id: binding.conversation_id().clone(),
                        workspace_id: binding.workspace_id().clone(),
                        session_id: session_id.to_owned(),
                    })
                } else {
                    Ok(OpenCodeEventEffect::None)
                }
            }
            OpenCodeEventSemantics::Notifications(kinds) => Ok(OpenCodeEventEffect::Passive {
                conversation_id: binding.conversation_id().clone(),
                kinds,
            }),
            OpenCodeEventSemantics::Interaction { kind, request } => {
                Ok(OpenCodeEventEffect::Interaction {
                    conversation_id: binding.conversation_id().clone(),
                    kind,
                    request,
                })
            }
            OpenCodeEventSemantics::InteractionResolved {
                request_id,
                kind,
                response_count,
                outcome,
            } => Ok(OpenCodeEventEffect::InteractionResolved {
                conversation_id: binding.conversation_id().clone(),
                session_id: session_id.to_owned(),
                request_id,
                kind,
                answer_count: response_count,
                outcome,
            }),
        }
    }

    async fn binding_for_event(
        &mut self,
        session_id: &str,
        routing_directory: Option<&Path>,
    ) -> Result<Option<OpenCodeSessionBinding>, KitError> {
        if let Some(binding) = self.session_bindings.get(session_id) {
            Self::verify_routing(binding, routing_directory)?;
            return Ok(Some(binding.clone()));
        }
        if self.owned_sessions.contains(session_id) {
            return Ok(None);
        }
        if let Some(binding) = self
            .state
            .binding_for_session(&self.agent_id, session_id)
            .await?
        {
            Self::verify_routing(&binding, routing_directory)?;
            self.owned_sessions.insert(session_id.to_owned());
            self.session_bindings
                .insert(session_id.to_owned(), binding.clone());
            return Ok(Some(binding));
        }
        self.refresh_ownership().await?;
        if let Some(binding) = self.session_bindings.get(session_id) {
            Self::verify_routing(binding, routing_directory)?;
            return Ok(Some(binding.clone()));
        }
        if self.owned_sessions.contains(session_id) {
            return Ok(None);
        }
        Err(KitError::conflict(
            "event belongs outside the owned session closure",
        ))
    }

    fn verify_routing(
        binding: &OpenCodeSessionBinding,
        routing_directory: Option<&Path>,
    ) -> Result<(), KitError> {
        if routing_directory.is_some_and(|directory| directory != binding.directory()) {
            return Err(KitError::conflict(
                "event routing contradicts its durable binding",
            ));
        }
        Ok(())
    }

    fn release_deleted_routes(&mut self, roots: &BTreeSet<String>) {
        for root in roots {
            if self.pending_deleted_sessions.contains(root) {
                self.release_deleted_route(root);
            }
        }
    }

    fn release_deleted_route(&mut self, root: &str) {
        self.pending_deleted_sessions.remove(root);
        let Some(binding) = self.session_bindings.get(root) else {
            self.owned_sessions.remove(root);
            return;
        };
        let conversation_id = binding.conversation_id().clone();
        let sessions = self
            .session_bindings
            .iter()
            .filter(|(_, candidate)| candidate.conversation_id() == &conversation_id)
            .map(|(session_id, _)| session_id.clone())
            .collect::<Vec<_>>();
        for session_id in sessions {
            self.session_bindings.remove(&session_id);
            self.owned_sessions.remove(&session_id);
        }
    }

    fn invalidate(&self) {
        self.state
            .interrupt_prompt_outputs(&KitError::closed("prompt event owner invalidated"));
        self.hierarchy.invalidate();
        self.state.notifications().fail_all();
    }
}

impl From<KitError> for OpenCodeRecoveryFailure {
    fn from(error: KitError) -> Self {
        if Self::recoverable(&error) {
            Self::Retry(error)
        } else {
            Self::Stop(error)
        }
    }
}

impl From<OpenCodeTransactionError> for OpenCodeRecoveryFailure {
    fn from(error: OpenCodeTransactionError) -> Self {
        match error {
            OpenCodeTransactionError::TerminalCleanup(error) => Self::Stop(error),
            OpenCodeTransactionError::TransactionConflict(error)
            | OpenCodeTransactionError::Failure(error) => Self::from(error),
        }
    }
}

impl OpenCodeRecoveryFailure {
    fn into_sdk(self) -> KitError {
        match self {
            Self::Parked(error)
            | Self::Retry(error)
            | Self::Reconnect(error)
            | Self::Stop(error) => error,
        }
    }

    fn recoverable(error: &KitError) -> bool {
        matches!(
            (error).kind(),
            ErrorKind::Busy | ErrorKind::Timeout | ErrorKind::Protocol | ErrorKind::Integration
        )
    }

    fn stream(error: KitError) -> Self {
        if Self::recoverable(&error) {
            Self::Reconnect(error)
        } else {
            Self::Stop(error)
        }
    }
}

impl OpenCodeRecovery {
    fn reason(&self) -> OpenCodeRecoveryReason {
        match self {
            Self::Startup { .. } => OpenCodeRecoveryReason::Startup,
            Self::Reconnect { .. } => OpenCodeRecoveryReason::Reconnect,
            Self::Synchronize(recovery) => recovery.reason(),
        }
    }
}

impl OpenCodeSameStreamRecovery {
    fn reason(&self) -> OpenCodeRecoveryReason {
        match self {
            Self::Application { .. } => OpenCodeRecoveryReason::Application,
            Self::Gap { .. } => OpenCodeRecoveryReason::Gap,
            Self::Command { .. } => OpenCodeRecoveryReason::Command,
        }
    }
}

impl OpenCodeRecoveryReason {
    fn deadline_failure(self, last_error: Option<KitError>) -> OpenCodeRecoveryFailure {
        let error = self.deadline_error(last_error);
        if matches!(self, Self::Command) {
            OpenCodeRecoveryFailure::Retry(error)
        } else {
            OpenCodeRecoveryFailure::Stop(error)
        }
    }

    fn deadline_error(self, last_error: Option<KitError>) -> KitError {
        if last_error
            .as_ref()
            .is_some_and(|error| error.kind() == ErrorKind::Busy)
            && matches!(self, Self::Startup | Self::Reconnect)
        {
            let error = KitError::conflict("event ownership is not exclusive")
                .with_detail(ErrorDetail::OwnershipConflict)
                .at_phase(ErrorPhase::EventSynchronization);
            return error.with_cause(last_error.expect("busy cause was checked"));
        }
        let error = match self {
            Self::Startup => {
                KitError::timeout("event startup deadline elapsed").at_phase(ErrorPhase::EventReady)
            }
            Self::Reconnect => KitError::timeout("event reconnect deadline elapsed")
                .at_phase(ErrorPhase::EventReady),
            Self::Application => KitError::timeout("event application deadline elapsed")
                .at_phase(ErrorPhase::EventSynchronization),
            Self::Gap => KitError::timeout("event gap deadline elapsed")
                .at_phase(ErrorPhase::EventSynchronization),
            Self::Command => KitError::timeout("event synchronization deadline elapsed")
                .at_phase(ErrorPhase::EventSynchronization),
        };
        match last_error {
            Some(attempt) => error.with_supplementary(attempt),
            None => error,
        }
    }
}
