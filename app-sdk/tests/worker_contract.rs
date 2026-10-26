//! Driver isolation and owned cleanup through existing state/Input workflows.
//! The captured runtime remains alive and driven until App cleanup completes.

mod owned_child;

use std::future::Future;
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread::ThreadId;
use std::time::Duration;

use agent_sdk::conversation::{ConversationInfo, NewConversation};
use agent_sdk::identity::{AgentId, AgentInfo, ConversationId, WorkspaceId as AgentWorkspaceId};
use agent_sdk::knowledge::IngestKnowledgeRequest;
use agent_sdk::model::ModelInfo;
use agent_sdk::tool::ToolInfo;
use agent_sdk::workspace::{
    DeleteWorkspaceRequest, NewWorkspace, UpdateWorkspaceRequest, WorkspaceInfo,
    WorkspaceUpdateReadiness,
};
use agent_sdk::{AgentBackend, SdkError, WorkspaceBackend};
use app_sdk::workflow::{
    ActionBackend, ActionDeclaration, ActionEvent, ActionMode, ActionRequest, CheckpointSelector,
    ExecutionOptions, Interaction, LifecycleEvent, LifecycleHooks, NoActions, NoConditions,
    Resolution, RestoreOptions, ResumePosition, RunId, RunSelector, RunStatus, StateChange,
    StateTyping, SyncExecution, WorkflowDefinition, WorkflowRun, WorkflowState,
};
use app_sdk::{
    AgentSpec, App, AppConfig, AppError, Shutdown, Workspace, WorkspaceConfig, WorkspaceId,
};
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, Stream, StreamExt};
use serde_json::{Map, json};
use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_sdk::state::record::{
    StateKey, StateKeyPrefix, StateListRequest, StateListResponse, StateRecord, StateRecordSnapshot,
};
use storage_sdk::{
    StateStore, StateStoreBackend, StateStoreError, StateTransaction, StateTransactionBackend,
};
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;
use tracing::instrument::WithSubscriber;

#[derive(Default)]
struct WorkerEvidence {
    source_events: Mutex<Vec<(RunId, &'static str, ThreadId)>>,
    native_read: Mutex<Option<NativeRead>>,
    native_read_faults: AtomicUsize,
    native_read_dropped: AtomicUsize,
    native_read_observed: Notify,
    native_read_release: Notify,
    native_lifecycle: Mutex<Vec<(&'static str, i64)>>,
    source_pending: Notify,
    source_complete: AtomicBool,
    source_waker: Mutex<Option<Waker>>,
    mutation_threads: Mutex<Vec<ThreadId>>,
    data_created: AtomicUsize,
    data_dropped: AtomicUsize,
    typing_dropped: AtomicUsize,
    blocking_loads: AtomicUsize,
    retention_armed: AtomicBool,
    retention_entered: Notify,
    retention_read_dropped: AtomicUsize,
    retention_read_failure: AtomicBool,
    retention_read_target: Mutex<Option<(StateKey, CapacityReadPhase)>>,
    retention_release: Notify,
    retention_applied: Notify,
    retention_scan_armed: AtomicBool,
    retention_scans: AtomicUsize,
    retention_after_marker: AtomicBool,
    capacity_marker: Mutex<Option<StateKey>>,
    capacity_marker_armed: AtomicBool,
    capacity_reads: AtomicUsize,
    reject_pre_once: AtomicBool,
    admission_burst: Mutex<Option<Arc<AdmissionGate>>>,
    admission_read: Mutex<Option<StateKey>>,
    admission_read_armed: AtomicBool,
    admission_read_entered: Notify,
    admission_read_release: Notify,
    admission_read_returned: AtomicUsize,
    readmission_head: Mutex<Option<StateKey>>,
    readmission_reader: Mutex<Option<(ThreadId, Option<tokio::task::Id>)>>,
    readmission_marker_seen: AtomicBool,
    readmission_reads: AtomicUsize,
    readmission_head_entered: Notify,
    readmission_head_release: Notify,
    delete_head: Mutex<Option<StateKey>>,
    delete_read_armed: AtomicBool,
    delete_read_entered: Notify,
    delete_read_release: Notify,
    delete_after_scan: AtomicBool,
    deletion_applied: Notify,
    deleted_heads: AtomicUsize,
    commit: Arc<CommitGate>,
    commit_task: Mutex<Option<JoinHandle<Result<(), StateStoreError>>>>,
    panic_read_armed: AtomicBool,
    panic_read_entered: Notify,
    panic_read_dropped: AtomicUsize,
    agent_shutdowns: [AtomicUsize; 2],
    gate_agent_cleanup: AtomicBool,
    agent_cleanup_entered: Notify,
    agent_cleanup_release: Notify,
    lower_workspaces: Arc<Mutex<[Option<WorkspaceInfo>; 2]>>,
    workspace_race: Mutex<Option<Arc<WorkspaceRace>>>,
}

#[derive(Clone, Copy)]
enum CapacityReadFault {
    Held,
    Permanent,
}

#[derive(Clone, Copy)]
enum CapacityReadPhase {
    Head,
    Marker,
    Application,
}

// Original public hooks select the next authority read by exact run identity;
// storage observation is free to run on the accepted publication's own task.
enum NativeRead {
    Initial,
    Next(StateKey),
    Bootstrap {
        fault: CapacityReadFault,
        panic_first: bool,
    },
    Authority {
        key: StateKey,
        fault: CapacityReadFault,
        panic_first: bool,
    },
}

#[derive(Default)]
struct WorkspaceRace {
    key: Mutex<Option<StateKey>>,
    lower_armed: AtomicBool,
    lower_calls: AtomicUsize,
    lower_owner: Mutex<Option<(ThreadId, Option<tokio::task::Id>)>>,
    lower_entered: Notify,
    lower_release: Notify,
    capture_armed: AtomicBool,
    capture_entered: Notify,
    capture_release: Notify,
    observation_armed: AtomicBool,
    observation_entered: Notify,
    observation_release: Notify,
    observed_reads: AtomicUsize,
    scan_armed: AtomicBool,
    scan_entered: Notify,
    scan_release: Notify,
}

impl WorkspaceRace {
    fn release(&self) {
        self.lower_release.notify_one();
        self.capture_release.notify_one();
        self.observation_release.notify_one();
        self.scan_release.notify_one();
    }
}

#[derive(Default)]
struct CommitGate {
    armed: AtomicBool,
    applied: Notify,
    release: Notify,
    observer_dropped: Notify,
    observed_losses: AtomicUsize,
}

// This guard models observation only. The separately retained task below owns
// the native transaction and its receipt even after this waiter is dropped.
enum WorkerObservation {
    Commit {
        gate: Arc<CommitGate>,
        waiting: bool,
    },
    Read(Arc<WorkerEvidence>),
    Retention(Arc<WorkerEvidence>),
    Authority(Arc<WorkerEvidence>),
    Admission(Arc<AdmissionGate>),
    Handoff(Option<oneshot::Sender<()>>),
}

impl Drop for WorkerObservation {
    fn drop(&mut self) {
        match self {
            Self::Handoff(sender) => {
                if let Some(sender) = sender.take() {
                    let _ = sender.send(());
                }
            }
            Self::Admission(gate) => {
                gate.departed.fetch_add(1, Ordering::SeqCst);
                gate.changed.notify_waiters();
            }
            Self::Commit {
                gate,
                waiting: true,
            } => {
                gate.observed_losses.fetch_add(1, Ordering::SeqCst);
                gate.observer_dropped.notify_one();
            }
            Self::Retention(evidence) => {
                evidence
                    .retention_read_dropped
                    .fetch_add(1, Ordering::SeqCst);
            }
            Self::Authority(evidence) => {
                evidence.native_read_dropped.fetch_add(1, Ordering::SeqCst);
            }
            Self::Read(evidence) => {
                evidence.panic_read_dropped.fetch_add(1, Ordering::SeqCst);
                panic!("worker read destructor private diagnostic");
            }
            Self::Commit { waiting: false, .. } => {}
        }
    }
}

// A task retains its tracing dispatcher through completion supervision. This
// observer fences that public ownership handoff without a sleep or private API.
impl tracing::Subscriber for WorkerObservation {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        false
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        unreachable!("the handoff observer disables spans")
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

impl WorkerEvidence {
    fn record_source(&self, run: &RunId, event: &'static str) {
        self.source_events
            .lock()
            .unwrap()
            .push((run.clone(), event, std::thread::current().id()));
    }

    fn release_source(&self) {
        self.source_complete.store(true, Ordering::SeqCst);
        let waker = self.source_waker.lock().unwrap().take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn assert_source(&self, run: &RunId, completed: bool, caller: ThreadId) -> ThreadId {
        let events = self.source_events.lock().unwrap().clone();
        let selected = events
            .iter()
            .filter(|(id, _, _)| id == run)
            .collect::<Vec<_>>();
        let thread = selected.first().expect("real source construction").2;
        assert_ne!(thread, caller, "background source ran on the caller thread");
        assert!(selected.iter().all(|(_, _, observed)| *observed == thread));
        for event in ["construct", "partial", "pending", "drop"] {
            assert_eq!(
                selected
                    .iter()
                    .filter(|(_, name, _)| *name == event)
                    .count(),
                1
            );
        }
        assert_eq!(
            selected
                .iter()
                .filter(|(_, name, _)| *name == "complete")
                .count(),
            usize::from(completed),
        );
        assert_eq!(selected.first().unwrap().1, "construct");
        assert_eq!(selected.last().unwrap().1, "drop");
        let partial = selected
            .iter()
            .position(|(_, event, _)| *event == "partial")
            .unwrap();
        let pending = selected
            .iter()
            .position(|(_, event, _)| *event == "pending")
            .unwrap();
        assert!(partial < pending);
        assert!(
            selected
                .iter()
                .filter(|(_, event, _)| *event == "poll")
                .count()
                >= 2
        );
        if completed {
            assert!(
                pending
                    < selected
                        .iter()
                        .position(|(_, event, _)| *event == "complete")
                        .unwrap()
            );
        }
        thread
    }

    async fn join_commit(&self) {
        self.commit.release.notify_one();
        let mut task = self
            .commit_task
            .lock()
            .unwrap()
            .take()
            .expect("accepted commit task");
        let result = match tokio::time::timeout(Duration::from_secs(3), &mut task).await {
            Ok(result) => result,
            Err(_) => {
                task.abort();
                let _ = task.await;
                panic!("test-owned native finalization did not join after receipt release");
            }
        };
        result
            .expect("native finalization task did not panic")
            .expect("real LanceDB commit succeeded");
    }
}

struct WorkerAction {
    declaration: ActionDeclaration,
    evidence: Arc<WorkerEvidence>,
}

struct WorkerSource {
    owner: Rc<ThreadId>,
    run: RunId,
    evidence: Arc<WorkerEvidence>,
    phase: u8,
    announced: bool,
}

impl WorkerAction {
    fn new(evidence: &Arc<WorkerEvidence>) -> Self {
        Self {
            declaration: ActionDeclaration {
                key: "local".into(),
                revision: "r1".into(),
                reads: vec!["item".into()],
                writes: vec!["item".into()],
                required_inputs: Vec::new(),
                optional_inputs: Vec::new(),
                bound_inputs: Map::new(),
                default_inputs: Map::new(),
                tags: Vec::new(),
                source: None,
                mode: ActionMode::Asynchronous,
            },
            evidence: Arc::clone(evidence),
        }
    }
    fn definition() -> WorkflowDefinition {
        WorkflowDefinition::parse(
            &json!({
                "schema":"app-sdk.workflow/v2", "id":"worker-source", "revision":"r1",
                "state":{"item":0}, "required":["item"], "reducers":{"item":"reject_concurrent"},
                "entry":"effect", "nodes":[{"id":"effect", "step":{
                    "kind":"action", "action":"local", "revision":"r1"
                }}], "routes":{}
            })
            .to_string(),
        )
        .unwrap()
    }
}

impl ActionBackend for WorkerAction {
    type Stream = WorkerSource;

    fn declaration(&self) -> &ActionDeclaration {
        &self.declaration
    }

    fn start(&self, request: ActionRequest) -> Self::Stream {
        assert_eq!(request.context.mode, ActionMode::Asynchronous);
        assert_eq!(request.state.values()["item"], 0);
        assert_eq!(request.context.action_ordinal, 0);
        self.evidence
            .record_source(&request.context.run, "construct");
        WorkerSource {
            owner: Rc::new(std::thread::current().id()),
            run: request.context.run,
            evidence: Arc::clone(&self.evidence),
            phase: 0,
            announced: false,
        }
    }
}

impl Stream for WorkerSource {
    type Item = Result<ActionEvent, AppError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        assert_eq!(*self.owner, std::thread::current().id());
        self.evidence.record_source(&self.run, "poll");
        match self.phase {
            0 => {
                self.phase = 1;
                self.evidence.record_source(&self.run, "partial");
                Poll::Ready(Some(Ok(ActionEvent::Partial {
                    result: Map::from_iter([("transient".into(), json!("worker-only"))]),
                })))
            }
            1 => {
                *self.evidence.source_waker.lock().unwrap() = Some(context.waker().clone());
                if !self.evidence.source_complete.load(Ordering::SeqCst) {
                    if !self.announced {
                        self.announced = true;
                        self.evidence.record_source(&self.run, "pending");
                        self.evidence.source_pending.notify_one();
                    }
                    return Poll::Pending;
                }
                self.phase = 2;
                self.evidence.record_source(&self.run, "complete");
                Poll::Ready(Some(Ok(ActionEvent::Completed {
                    result: Map::from_iter([("answer".into(), json!(1))]),
                    writes: vec![StateChange::Assign {
                        values: Map::from_iter([("item".into(), json!(1))]),
                    }],
                })))
            }
            _ => panic!("completed background source was polled again"),
        }
    }
}

impl Drop for WorkerSource {
    fn drop(&mut self) {
        assert_eq!(
            *self.owner,
            std::thread::current().id(),
            "local source migrated during cleanup"
        );
        self.evidence.source_waker.lock().unwrap().take();
        self.evidence.record_source(&self.run, "drop");
    }
}

// App materializes each configured lower workspace even for state-only runs.
// These direct Agents retain that real fixture state across App reopen, but no
// workflow invokes a conversation or fabricates Agent output.
struct WorkerAgent {
    index: usize,
    evidence: Arc<WorkerEvidence>,
}

struct WorkerWorkspace {
    index: usize,
    evidence: Arc<WorkerEvidence>,
}

impl WorkerAgent {
    fn workspace_id(&self) -> AgentWorkspaceId {
        format!("workspace_{:032x}", self.index + 1)
            .parse()
            .unwrap()
    }
}

impl AgentBackend for WorkerAgent {
    type Workspace = WorkerWorkspace;

    fn id(&self) -> AgentId {
        format!("agent_{:032x}", self.index + 1).parse().unwrap()
    }
    async fn info(&self) -> Result<AgentInfo, SdkError> {
        AgentInfo::new(self.id(), None)
    }
    async fn new_workspace(&self, request: NewWorkspace) -> Result<Self::Workspace, SdkError> {
        let mut workspaces = self.evidence.lower_workspaces.lock().unwrap();
        if workspaces[self.index].is_some() {
            return Err(SdkError::conflict("worker workspace already exists"));
        }
        workspaces[self.index] = Some(WorkspaceInfo::new(
            self.workspace_id(),
            request.name().to_owned(),
            request.context().clone(),
            request.defaults().clone(),
            1,
        )?);
        Ok(WorkerWorkspace {
            index: self.index,
            evidence: Arc::clone(&self.evidence),
        })
    }
    async fn list_workspaces(&self) -> Result<Vec<WorkspaceInfo>, SdkError> {
        Ok(self.evidence.lower_workspaces.lock().unwrap()[self.index]
            .iter()
            .cloned()
            .collect())
    }
    async fn workspace_info(&self, id: AgentWorkspaceId) -> Result<WorkspaceInfo, SdkError> {
        if id != self.workspace_id() {
            return Err(SdkError::not_found("worker workspace id"));
        }
        self.evidence.lower_workspaces.lock().unwrap()[self.index]
            .clone()
            .ok_or_else(|| SdkError::not_found("worker workspace absent"))
    }
    async fn get_workspace(&self, id: AgentWorkspaceId) -> Result<Self::Workspace, SdkError> {
        self.workspace_info(id).await?;
        Ok(WorkerWorkspace {
            index: self.index,
            evidence: Arc::clone(&self.evidence),
        })
    }
    fn check_workspace_update(
        &self,
        request: &UpdateWorkspaceRequest,
    ) -> impl std::future::Future<Output = Result<WorkspaceUpdateReadiness, SdkError>> + Send + '_
    {
        let id = request.id().clone();
        let revision = request.expected_revision();
        async move {
            if self.workspace_info(id).await?.revision() != revision {
                return Err(SdkError::conflict("worker workspace fence"));
            }
            Ok(WorkspaceUpdateReadiness::Ready)
        }
    }
    async fn update_workspace(
        &self,
        request: UpdateWorkspaceRequest,
    ) -> Result<WorkspaceInfo, SdkError> {
        let mut workspaces = self.evidence.lower_workspaces.lock().unwrap();
        let current = workspaces[self.index]
            .as_ref()
            .ok_or_else(|| SdkError::not_found("worker workspace absent"))?;
        if current.id() != request.id() || current.revision() != request.expected_revision() {
            return Err(SdkError::conflict("worker workspace fence"));
        }
        let info = WorkspaceInfo::new(
            request.id().clone(),
            request.name().to_owned(),
            request.context().clone(),
            request.defaults().clone(),
            current.revision() + 1,
        )?;
        workspaces[self.index] = Some(info.clone());
        Ok(info)
    }
    async fn delete_workspace(&self, request: DeleteWorkspaceRequest) -> Result<(), SdkError> {
        let race = self.evidence.workspace_race.lock().unwrap().clone();
        if let Some(race) = race {
            race.lower_calls.fetch_add(1, Ordering::SeqCst);
            if race.lower_armed.swap(false, Ordering::SeqCst) {
                *race.lower_owner.lock().unwrap() =
                    Some((std::thread::current().id(), tokio::task::try_id()));
                // The durable deletion already owns this exact lower call.
                race.lower_entered.notify_one();
                race.lower_release.notified().await;
            }
        }
        let mut workspaces = self.evidence.lower_workspaces.lock().unwrap();
        let current = workspaces[self.index]
            .as_ref()
            .ok_or_else(|| SdkError::not_found("worker workspace absent"))?;
        if current.id() != request.id() || current.revision() != request.expected_revision() {
            return Err(SdkError::conflict("worker workspace fence"));
        }
        workspaces[self.index] = None;
        Ok(())
    }
    async fn list_models(&self) -> Result<Vec<ModelInfo>, SdkError> {
        Ok(Vec::new())
    }
    async fn ingest_knowledge(&self, _: IngestKnowledgeRequest) -> Result<(), SdkError> {
        Err(SdkError::unsupported(
            "worker fixture has no knowledge source",
        ))
    }
    async fn shutdown(self) -> Result<(), SdkError> {
        if self.index == 0
            && self
                .evidence
                .gate_agent_cleanup
                .swap(false, Ordering::SeqCst)
        {
            self.evidence.agent_cleanup_entered.notify_one();
            self.evidence.agent_cleanup_release.notified().await;
        }
        self.evidence.agent_shutdowns[self.index].fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl WorkspaceBackend for WorkerWorkspace {
    type Conversation = agent_sdk::Conversation;

    fn id(&self) -> AgentWorkspaceId {
        format!("workspace_{:032x}", self.index + 1)
            .parse()
            .unwrap()
    }
    async fn info(&self) -> Result<WorkspaceInfo, SdkError> {
        self.evidence.lower_workspaces.lock().unwrap()[self.index]
            .clone()
            .ok_or_else(|| SdkError::not_found("worker workspace absent"))
    }
    async fn new_conversation(&self, _: NewConversation) -> Result<Self::Conversation, SdkError> {
        Err(SdkError::unsupported(
            "worker fixture has no conversation execution",
        ))
    }
    async fn list_conversations(&self) -> Result<Vec<ConversationInfo>, SdkError> {
        Ok(Vec::new())
    }
    async fn conversation_info(&self, _: ConversationId) -> Result<ConversationInfo, SdkError> {
        Err(SdkError::not_found("worker conversation absent"))
    }
    async fn get_conversation(&self, _: ConversationId) -> Result<Self::Conversation, SdkError> {
        Err(SdkError::not_found("worker conversation absent"))
    }
    async fn list_tools(&self) -> Result<Vec<ToolInfo>, SdkError> {
        Ok(Vec::new())
    }
    async fn ingest_knowledge(&self, _: IngestKnowledgeRequest) -> Result<(), SdkError> {
        Err(SdkError::unsupported(
            "worker fixture has no knowledge source",
        ))
    }
    async fn close(self) -> Result<(), SdkError> {
        Ok(())
    }
}

struct WorkerHooks(Arc<WorkerEvidence>);

impl LifecycleHooks for WorkerHooks {
    fn observe(&self, event: &LifecycleEvent<'_>) -> Result<(), AppError> {
        match event {
            LifecycleEvent::Registered {
                workspace,
                run,
                occurrence,
                ..
            } => {
                let mut probe = self.0.native_read.lock().unwrap();
                if let Some(NativeRead::Bootstrap { fault, panic_first }) = *probe {
                    *probe = Some(NativeRead::Authority {
                        key: StateKey::new(
                            "app-sdk",
                            format!("run/{workspace}/worker-state/{run}/{occurrence}"),
                        )
                        .unwrap(),
                        fault,
                        panic_first,
                    });
                }
            }
            LifecycleEvent::ExecuteStarted { .. } => {
                self.0
                    .native_lifecycle
                    .lock()
                    .unwrap()
                    .push(("sync-start", 0));
            }
            LifecycleEvent::ExecuteFinished { snapshot, .. } => {
                self.0.native_lifecycle.lock().unwrap().push((
                    "sync-finish",
                    snapshot.state.values()["item"].as_i64().unwrap(),
                ));
            }
            LifecycleEvent::StepStarted { context, state, .. } if context.node == "effect" => {
                let mut probe = self.0.native_read.lock().unwrap();
                if matches!(*probe, Some(NativeRead::Initial)) {
                    assert_eq!(context.checkpoint, 0);
                    assert_eq!(state.values()["item"], 0);
                    *probe = Some(NativeRead::Next(
                        StateKey::new(
                            "app-sdk",
                            format!(
                                "run/{}/{}/{}/{}",
                                context.workspace,
                                context.workflow,
                                context.run,
                                context.occurrence,
                            ),
                        )
                        .unwrap(),
                    ));
                }
            }
            LifecycleEvent::StepFinished {
                context,
                outcome: Some(Ok(_)),
                checkpoint,
            } if context.node == "first" => {
                assert_eq!(checkpoint.state.values()["item"], 1);
                assert_eq!(checkpoint.action_ordinal, Some(0));
                *self.0.native_read.lock().unwrap() = Some(NativeRead::Next(
                    StateKey::new(
                        "app-sdk",
                        format!(
                            "run/{}/{}/{}/{}",
                            context.workspace, context.workflow, context.run, context.occurrence
                        ),
                    )
                    .unwrap(),
                ));
            }
            _ => {}
        }
        Ok(())
    }

    async fn observe_async(&self, event: &LifecycleEvent<'_>) -> Result<(), AppError> {
        match event {
            LifecycleEvent::ExecuteStarted { .. } => {
                self.0
                    .native_lifecycle
                    .lock()
                    .unwrap()
                    .push(("async-start", 0));
            }
            LifecycleEvent::ExecuteFinished { snapshot, .. } => {
                self.0.native_lifecycle.lock().unwrap().push((
                    "async-finish",
                    snapshot.state.values()["item"].as_i64().unwrap(),
                ));
            }
            _ => {}
        }
        Ok(())
    }
}

struct WorkerTyping(Arc<WorkerEvidence>);

struct LocalData {
    state: Rc<WorkflowState>,
    created_on: ThreadId,
    evidence: Arc<WorkerEvidence>,
}

impl StateTyping for WorkerTyping {
    type Data = LocalData;

    fn construct_data(&self, state: &WorkflowState) -> Result<Self::Data, AppError> {
        self.0.data_created.fetch_add(1, Ordering::SeqCst);
        Ok(LocalData {
            state: Rc::new(state.clone()),
            created_on: std::thread::current().id(),
            evidence: Arc::clone(&self.0),
        })
    }

    fn construct_state(&self, data: &Self::Data) -> Result<WorkflowState, AppError> {
        assert_eq!(data.created_on, std::thread::current().id());
        Ok(data.state.as_ref().clone())
    }

    fn validate_pre(&self, _state: &WorkflowState, _change: &StateChange) -> Result<(), AppError> {
        self.0
            .mutation_threads
            .lock()
            .unwrap()
            .push(std::thread::current().id());
        if self.0.reject_pre_once.swap(false, Ordering::SeqCst) {
            return Err(AppError::InvalidInput("one worker state rejection".into()));
        }
        Ok(())
    }
}

impl Drop for LocalData {
    fn drop(&mut self) {
        assert_eq!(
            self.created_on,
            std::thread::current().id(),
            "local Data migrated during cleanup"
        );
        self.evidence.data_dropped.fetch_add(1, Ordering::SeqCst);
    }
}

impl Drop for WorkerTyping {
    fn drop(&mut self) {
        self.0.typing_dropped.fetch_add(1, Ordering::SeqCst);
    }
}

// This non-Clone caller store delegates all durability to real LanceDB. The tiny
// lower operation needs caller-runtime blocking capacity; a long-lived App
// worker or premature blocking thread join must not monopolize that capacity.
struct WorkerStore {
    inner: StateStore,
    evidence: Arc<WorkerEvidence>,
}

impl WorkerStore {
    fn is_checkpoint(record: &StateRecord) -> bool {
        let (Some(workspace), Some(workflow), Some(run), Some(occurrence), Some(sequence)) = (
            record
                .value
                .get("workspace_id")
                .and_then(serde_json::Value::as_str),
            record
                .value
                .get("workflow_id")
                .and_then(serde_json::Value::as_str),
            record.value.get("id").and_then(serde_json::Value::as_str),
            record
                .value
                .get("occurrence")
                .and_then(serde_json::Value::as_str),
            record
                .value
                .pointer("/checkpoint/sequence")
                .and_then(serde_json::Value::as_u64),
        ) else {
            return false;
        };
        record.key.key()
            == format!(
                "history/{workspace}/{workflow}/{run}/{occurrence}/checkpoint/{sequence:020}"
            )
    }
}

impl StateStoreBackend for WorkerStore {
    type Transaction = WorkerTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        let admission = self.evidence.admission_burst.lock().unwrap().clone();
        if let Some(gate) = admission.filter(|gate| {
            gate.key == key
                && gate.caller != (std::thread::current().id(), tokio::task::try_id())
                && *gate.exempt.lock().unwrap()
                    != Some((std::thread::current().id(), tokio::task::try_id()))
                && !gate.released.load(Ordering::SeqCst)
        }) {
            let _observation = WorkerObservation::Admission(Arc::clone(&gate));
            gate.hold().await;
        }
        let authority = {
            let mut probe = self.evidence.native_read.lock().unwrap();
            if matches!(&*probe, Some(NativeRead::Authority { key: selected, .. }) if selected == &key)
            {
                let Some(NativeRead::Authority {
                    key,
                    fault,
                    panic_first,
                }) = probe.take()
                else {
                    unreachable!()
                };
                if panic_first {
                    // Preserve the same actual authority key after the original
                    // backend panic; the tracked task owns reconciliation.
                    *probe = Some(NativeRead::Authority {
                        key,
                        fault,
                        panic_first: false,
                    });
                }
                Some((fault, panic_first))
            } else {
                None
            }
        };
        if let Some((fault, panic_first)) = authority {
            self.evidence
                .native_read_faults
                .fetch_add(1, Ordering::SeqCst);
            if panic_first {
                panic!("original bootstrap backend read panic");
            }
            let _observation = WorkerObservation::Authority(Arc::clone(&self.evidence));
            self.evidence.native_read_observed.notify_one();
            if matches!(fault, CapacityReadFault::Held) {
                self.evidence.native_read_release.notified().await;
            }
            return Err(StateStoreError::serialization(
                "selected worker authority is unreadable",
            ));
        }
        let selected = {
            let mut target = self.evidence.retention_read_target.lock().unwrap();
            match target.as_mut() {
                Some((selected, phase)) if selected == &key => {
                    if matches!(phase, CapacityReadPhase::Application) && key.key() != "app" {
                        // This actual capacity-marker read precedes the same
                        // owner's App-count read. No other App operation starts
                        // until the selected read has entered its gate.
                        *selected = StateKey::new("app-sdk", "app")?;
                        false
                    } else {
                        target.take();
                        true
                    }
                }
                _ => false,
            }
        };
        if selected {
            let _observation = WorkerObservation::Retention(Arc::clone(&self.evidence));
            self.evidence.retention_entered.notify_one();
            self.evidence.retention_release.notified().await;
            if self
                .evidence
                .retention_read_failure
                .swap(false, Ordering::SeqCst)
            {
                return Err(StateStoreError::serialization(
                    "selected capacity authority is unreadable",
                ));
            }
        }
        let fail_native_read = {
            let mut probe = self.evidence.native_read.lock().unwrap();
            if matches!(&*probe, Some(NativeRead::Next(selected)) if selected == &key) {
                *probe = None;
                true
            } else {
                false
            }
        };
        if fail_native_read {
            self.evidence
                .native_read_faults
                .fetch_add(1, Ordering::SeqCst);
            self.evidence.native_read_observed.notify_one();
            self.evidence.native_read_release.notified().await;
            return Err(StateStoreError::unavailable(
                "load",
                "one native ready observation was unavailable",
            ));
        }
        let race = self.evidence.workspace_race.lock().unwrap().clone();
        if let Some(race) = &race
            && race.key.lock().unwrap().as_ref() == Some(&key)
            && race.observation_armed.swap(false, Ordering::SeqCst)
        {
            // The reattached deletion now owns its registered operation. This
            // fresh read has not reached the actual store or acquired a writer.
            race.observation_entered.notify_one();
            race.observation_release.notified().await;
            race.observed_reads.fetch_add(1, Ordering::SeqCst);
        }
        if self.evidence.admission_read.lock().unwrap().as_ref() == Some(&key)
            && self
                .evidence
                .admission_read_armed
                .swap(false, Ordering::SeqCst)
        {
            // Admission already owns its exact execution lease, but no run
            // record or active marker has reached the native store yet.
            self.evidence.admission_read_entered.notify_one();
            self.evidence.admission_read_release.notified().await;
            self.evidence
                .admission_read_returned
                .fetch_add(1, Ordering::SeqCst);
        }
        if self.evidence.capacity_marker.lock().unwrap().as_ref() == Some(&key)
            && self
                .evidence
                .capacity_marker_armed
                .swap(false, Ordering::SeqCst)
        {
            // The active inventory page and this check's head are already
            // observed. Retention now commits before the native marker read.
            self.evidence.capacity_reads.fetch_add(1, Ordering::SeqCst);
            self.evidence.retention_release.notify_one();
            self.evidence.retention_applied.notified().await;
        }
        if self.evidence.delete_head.lock().unwrap().as_ref() == Some(&key)
            && self
                .evidence
                .delete_read_armed
                .swap(false, Ordering::SeqCst)
        {
            // Public delete already transferred its Operation owner, but this
            // first read has not reached native storage yet.
            self.evidence.delete_read_entered.notify_one();
            self.evidence.delete_read_release.notified().await;
        }
        let evidence = Arc::clone(&self.evidence);
        tokio::task::spawn_blocking(move || {
            evidence.blocking_loads.fetch_add(1, Ordering::SeqCst);
        })
        .await
        .expect("caller blocking operation remains owned");
        let snapshot = self.inner.load(key.clone()).await?;
        if let Some(race) = &race
            && race.key.lock().unwrap().as_ref() == Some(&key)
            && snapshot.as_ref().is_some_and(|snapshot| {
                snapshot.record.value.pointer("/state/state") == Some(&json!("deleting"))
            })
            && race.capture_armed.swap(false, Ordering::SeqCst)
        {
            // Preserve this actual read response while the original deletion
            // completes. No transaction stays open and no value is fabricated.
            race.capture_entered.notify_one();
            race.capture_release.notified().await;
        }
        let readmission_reader = self
            .evidence
            .readmission_reader
            .lock()
            .unwrap()
            .is_some_and(|reader| reader == (std::thread::current().id(), tokio::task::try_id()));
        if readmission_reader
            && self.evidence.capacity_marker.lock().unwrap().as_ref() == Some(&key)
            && snapshot.is_none()
        {
            self.evidence
                .readmission_marker_seen
                .store(true, Ordering::SeqCst);
        }
        if readmission_reader
            && self.evidence.readmission_head.lock().unwrap().as_ref() == Some(&key)
            && snapshot.as_ref().is_some_and(|head| {
                head.record.value.pointer("/retention/state") == Some(&json!("retained"))
            })
            && self
                .evidence
                .readmission_marker_seen
                .swap(false, Ordering::SeqCst)
        {
            // This reader has seen Retained, then no active marker, and now
            // the actual Retained recheck. Hold only its returned snapshot;
            // the public retry can acquire a new native writer independently.
            self.evidence
                .readmission_reads
                .fetch_add(1, Ordering::SeqCst);
            *self.evidence.readmission_reader.lock().unwrap() = None;
            self.evidence.readmission_head_entered.notify_one();
            self.evidence.readmission_head_release.notified().await;
        }
        let driver_thread = self
            .evidence
            .mutation_threads
            .lock()
            .unwrap()
            .contains(&std::thread::current().id());
        let current = if (driver_thread && self.evidence.panic_read_armed.load(Ordering::SeqCst)
            || self.evidence.retention_armed.load(Ordering::SeqCst))
            && let Some(head) = &snapshot
            && let Some(scope) = head.record.key.key().strip_prefix("run/")
            && let Some(sequence) = head
                .record
                .value
                .get("sequence")
                .and_then(serde_json::Value::as_u64)
        {
            // Preserve the selected head read as the held operation; inspect
            // only that head's immutable row, never an older terminal row.
            let row = self
                .inner
                .load(StateKey::new(
                    "app-sdk",
                    format!("history/{scope}/checkpoint/{sequence:020}"),
                )?)
                .await?;
            if let Some(row) = &row {
                assert!(Self::is_checkpoint(&row.record));
                for field in ["id", "workspace_id", "workflow_id"] {
                    assert_eq!(row.record.value[field], head.record.value[field]);
                }
                assert_eq!(row.record.value["checkpoint"]["sequence"], json!(sequence));
            }
            row
        } else {
            None
        };
        if snapshot.as_ref().is_some_and(|head| {
            head.record.value.pointer("/retention/state") == Some(&json!("active"))
        }) && current.as_ref().is_some_and(|snapshot| {
            matches!(
                snapshot
                    .record
                    .value
                    .pointer("/checkpoint/phase/state")
                    .and_then(serde_json::Value::as_str),
                Some("succeeded" | "failed")
            )
        }) && self.evidence.retention_armed.swap(false, Ordering::SeqCst)
        {
            // Hold the actual terminal head response before capacity release
            // acquires a native writer, regardless of the publication thread.
            let _observation = WorkerObservation::Retention(Arc::clone(&self.evidence));
            self.evidence.retention_entered.notify_one();
            self.evidence.retention_release.notified().await;
            if self
                .evidence
                .retention_read_failure
                .swap(false, Ordering::SeqCst)
            {
                return Err(StateStoreError::serialization(
                    "selected capacity authority is unreadable",
                ));
            }
        }
        if driver_thread
            && current.as_ref().is_some_and(|snapshot| {
                snapshot.record.value.pointer("/checkpoint/phase/state")
                    == Some(&json!("succeeded"))
            })
            && self.evidence.panic_read_armed.swap(false, Ordering::SeqCst)
        {
            let _observation = WorkerObservation::Read(Arc::clone(&self.evidence));
            self.evidence.panic_read_entered.notify_one();
            std::future::pending::<()>().await;
        }
        Ok(snapshot)
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        let workspace_page = request.prefix() == Some("workspace/");
        let page = self.inner.list(request).await?;
        let race = self.evidence.workspace_race.lock().unwrap().clone();
        if let Some(race) = race
            && workspace_page
            && page.records.is_empty()
            && race.scan_armed.swap(false, Ordering::SeqCst)
        {
            // Shutdown sampled the empty operation registry before this real
            // empty page. Reattachment is admitted while its response is held.
            race.scan_entered.notify_one();
            race.scan_release.notified().await;
        }
        if page
            .records
            .iter()
            .any(|record| record.record.key.key().starts_with("active/"))
            && self
                .evidence
                .retention_scan_armed
                .swap(false, Ordering::SeqCst)
        {
            // The actual scan saw an active marker. The marker-read case
            // returns this page before releasing retention; the other cases
            // wait for the selected real retention/deletion commit first.
            self.evidence.retention_scans.fetch_add(1, Ordering::SeqCst);
            if self.evidence.retention_after_marker.load(Ordering::SeqCst) {
                self.evidence
                    .capacity_marker_armed
                    .store(true, Ordering::SeqCst);
            } else if self.evidence.delete_after_scan.load(Ordering::SeqCst) {
                self.evidence.retention_release.notify_one();
                self.evidence.delete_read_release.notify_one();
                self.evidence.deletion_applied.notified().await;
            } else {
                self.evidence.retention_release.notify_one();
                self.evidence.retention_applied.notified().await;
            }
            if self.evidence.readmission_head.lock().unwrap().is_some() {
                // A root caller and a worker block_on can both have no task
                // ID. Select the exact thread and task that owns this scan.
                *self.evidence.readmission_reader.lock().unwrap() =
                    Some((std::thread::current().id(), tokio::task::try_id()));
            }
        }
        Ok(page)
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        Ok(WorkerTransaction {
            inner: self.inner.begin_transaction().await?,
            evidence: Arc::clone(&self.evidence),
            applies_effect: false,
            retains_run: false,
            deletes_run: false,
        })
    }
}

struct WorkerTransaction {
    inner: StateTransaction,
    evidence: Arc<WorkerEvidence>,
    applies_effect: bool,
    retains_run: bool,
    deletes_run: bool,
}

impl StateTransactionBackend for WorkerTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.inner.load(key).await
    }

    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        self.retains_run |= record.key.key().starts_with("run/")
            && record.value.pointer("/retention/state") == Some(&json!("retained"));
        self.applies_effect |= WorkerStore::is_checkpoint(&record)
            && record.value.pointer("/checkpoint/state/item") == Some(&json!(1));
        self.inner.put(record).await
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        self.deletes_run |= self.evidence.delete_head.lock().unwrap().as_ref() == Some(&key);
        self.inner.delete(key).await
    }

    async fn commit(self) -> Result<(), StateStoreError> {
        if self.retains_run || self.deletes_run {
            let result = self.inner.commit().await;
            if result.is_ok() {
                if self.retains_run {
                    self.evidence.retention_applied.notify_one();
                }
                if self.deletes_run {
                    self.evidence.deleted_heads.fetch_add(1, Ordering::SeqCst);
                    self.evidence.deletion_applied.notify_one();
                }
            }
            return result;
        }
        if !self.applies_effect || !self.evidence.commit.armed.swap(false, Ordering::SeqCst) {
            return self.inner.commit().await;
        }
        let mut observation = WorkerObservation::Commit {
            gate: Arc::clone(&self.evidence.commit),
            waiting: true,
        };
        let gate = Arc::clone(&self.evidence.commit);
        let (sender, receiver) = oneshot::channel();
        let task = tokio::spawn(async move {
            // The actual lower commit runs in the retained task before the
            // receipt gate. This is applied-receipt loss, never an uncommitted
            // transaction parked in the cancelled driver's own future.
            let result = self.inner.commit().await;
            gate.applied.notify_one();
            gate.release.notified().await;
            let _ = sender.send(result.clone());
            result
        });
        let previous = self.evidence.commit_task.lock().unwrap().replace(task);
        assert!(previous.is_none(), "one exactly selected finalization");
        let result = receiver
            .await
            .expect("owned native task retains its receipt");
        if let WorkerObservation::Commit { waiting, .. } = &mut observation {
            *waiting = false;
        }
        result
    }

    async fn rollback(self) -> Result<(), StateStoreError> {
        self.inner.rollback().await
    }
}

struct WorkerCase {
    app: App<WorkerStore, WorkerAgent, NoActions, NoConditions, WorkerTyping>,
    workspace: Workspace<WorkerStore, WorkerAgent, NoActions, NoConditions, WorkerTyping>,
    store: StateStore,
    evidence: Arc<WorkerEvidence>,
}

impl WorkerCase {
    async fn open() -> Self {
        Self::open_with_agents(0).await
    }

    async fn open_with_agents(agents: usize) -> Self {
        let evidence = Arc::new(WorkerEvidence::default());
        let inner = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("real transactional state");
        let app = App::open(Self::config(inner.clone(), &evidence, agents))
            .await
            .expect("caller-local state typing");
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().unwrap(),
            })
            .await
            .expect("workspace");
        Self {
            app,
            workspace,
            store: inner,
            evidence,
        }
    }

    fn config(
        store: StateStore,
        evidence: &Arc<WorkerEvidence>,
        agents: usize,
    ) -> AppConfig<WorkerStore, WorkerAgent, NoActions, NoConditions, WorkerTyping> {
        AppConfig::new(
            WorkerStore {
                inner: store,
                evidence: Arc::clone(evidence),
            },
            (0..agents)
                .map(|index| {
                    AgentSpec::new(
                        format!("cleanup-{index}"),
                        "r1".into(),
                        WorkerAgent {
                            index,
                            evidence: Arc::clone(evidence),
                        },
                    )
                })
                .collect(),
            Self::definitions(),
        )
        .with_state_typing(WorkerTyping(Arc::clone(evidence)))
    }

    async fn recover_effect(
        store: WorkerStore,
        workspace_id: WorkspaceId,
        run_id: RunId,
        agents: usize,
    ) {
        let evidence = Arc::new(WorkerEvidence {
            lower_workspaces: Arc::clone(&store.evidence.lower_workspaces),
            ..WorkerEvidence::default()
        });
        let mut app = App::open(Self::config(store.inner, &evidence, agents))
            .await
            .expect("fresh App with retained real lower authority");
        let workspace = app
            .workspace(workspace_id)
            .await
            .expect("durable workspace");
        let workflow = workspace
            .workflow("worker-state".parse().unwrap())
            .await
            .unwrap();
        let run = workflow
            .open_run(RunSelector::Latest(run_id))
            .await
            .expect("exact original run");
        let snapshot = tokio::time::timeout(Duration::from_secs(3), run.wait())
            .await
            .expect("recovery settles without replay")
            .expect("durable run truth");
        assert!(matches!(snapshot.status, RunStatus::Succeeded));
        assert_eq!(snapshot.state.values()["item"], 1);
        assert!(
            evidence.mutation_threads.lock().unwrap().is_empty(),
            "an already committed increment must not execute again"
        );
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        })
        .await
        .expect("fresh recovery App closes");
        for index in 0..agents {
            assert_eq!(evidence.agent_shutdowns[index].load(Ordering::SeqCst), 1);
        }
        drop(run);
        drop(workflow);
        drop(workspace);
        drop(app);
        tokio::time::timeout(Duration::from_secs(3), async {
            while evidence.typing_dropped.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fresh worker and runtime ownership release");
        assert_eq!(evidence.typing_dropped.load(Ordering::SeqCst), 1);
        assert_eq!(
            evidence.data_created.load(Ordering::SeqCst),
            evidence.data_dropped.load(Ordering::SeqCst)
        );
    }

    async fn observe_root_drop(fatal: bool) {
        let case = Self::open_with_agents(2).await;
        case.evidence
            .panic_read_armed
            .store(fatal, Ordering::SeqCst);
        case.evidence
            .gate_agent_cleanup
            .store(true, Ordering::SeqCst);
        let run = case.start("worker-state").await;
        let before = if fatal {
            tokio::time::timeout(
                Duration::from_secs(3),
                case.evidence.panic_read_entered.notified(),
            )
            .await
            .expect("exact driver read is retained before root drop");
            // Completion waits for this held driver observation. Read the real
            // committed boundary without waiting for its lifecycle cleanup.
            tokio::time::timeout(Duration::from_secs(3), run.snapshot())
                .await
                .expect("committed snapshot is bounded")
                .expect("committed snapshot remains readable before root drop")
        } else {
            tokio::time::timeout(Duration::from_secs(3), run.wait())
                .await
                .unwrap()
                .unwrap()
        };
        assert!(matches!(before.status, RunStatus::Succeeded));
        assert_eq!(before.state.values()["item"], 1);
        let Self {
            app,
            workspace,
            store,
            evidence,
        } = case;
        drop(workspace);
        drop(app);
        let during = AssertUnwindSafe(async {
            tokio::time::timeout(
                Duration::from_secs(3),
                evidence.agent_cleanup_entered.notified(),
            )
            .await
            .expect("drop finalizer reached owned Agent cleanup");
            assert_eq!(evidence.agent_shutdowns[0].load(Ordering::SeqCst), 0);
            assert_eq!(evidence.agent_shutdowns[1].load(Ordering::SeqCst), 0);
            assert!(
                matches!(
                    tokio::time::timeout(Duration::from_secs(1), run.wait())
                        .await
                        .unwrap(),
                    Err(AppError::Closed(_))
                ),
                "wait observation is revoked immediately on root drop"
            );
            Self::assert_drop_snapshot(&run, &before, fatal).await;
        })
        .catch_unwind()
        .await;
        // Every assertion path releases this already-entered, App-owned cleanup
        // future; the finalizer, not the test, owns consuming both Agents.
        evidence.agent_cleanup_release.notify_one();
        let after = AssertUnwindSafe(async {
            tokio::time::timeout(Duration::from_secs(3), async {
                while evidence
                    .agent_shutdowns
                    .iter()
                    .any(|count| count.load(Ordering::SeqCst) != 1)
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("both transferred Agent cleanup futures complete");
            // Observe across subsequent finalizer scheduling turns, not only
            // the instant the final consuming Agent returns.
            for _ in 0..8 {
                Self::assert_drop_snapshot(&run, &before, fatal).await;
                tokio::task::yield_now().await;
            }
            assert_eq!(
                evidence.panic_read_dropped.load(Ordering::SeqCst),
                usize::from(fatal)
            );
        })
        .catch_unwind()
        .await;
        let fresh_evidence = Arc::new(WorkerEvidence {
            lower_workspaces: Arc::clone(&evidence.lower_workspaces),
            ..WorkerEvidence::default()
        });
        let fresh = App::open(Self::config(store.clone(), &fresh_evidence, 2)).await;
        let fenced = AssertUnwindSafe(async {
            let mut app = fresh.expect("fresh App generation opens");
            let observation = AssertUnwindSafe(async {
                assert!(
                    matches!(run.snapshot().await, Err(AppError::Closed(_))),
                    "a new durable generation fences even an allowed drop snapshot"
                );
                assert!(matches!(run.wait().await, Err(AppError::Closed(_))));
            })
            .catch_unwind()
            .await;
            let closed = app
                .shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(3),
                })
                .await;
            drop(app);
            if let Err(failure) = observation {
                std::panic::resume_unwind(failure);
            }
            closed.expect("fresh App cleanup completes");
        })
        .catch_unwind()
        .await;
        drop(run);
        let released = AssertUnwindSafe(Self::released(&evidence))
            .catch_unwind()
            .await;
        for outcome in [during, after, fenced, released] {
            if let Err(failure) = outcome {
                std::panic::resume_unwind(failure);
            }
        }
        assert!(
            fresh_evidence.mutation_threads.lock().unwrap().is_empty(),
            "fresh generation must not replay the committed effect"
        );
        for shutdown in &fresh_evidence.agent_shutdowns {
            assert_eq!(shutdown.load(Ordering::SeqCst), 1);
        }
        drop(store);
    }

    async fn assert_drop_snapshot(
        run: &WorkflowRun<WorkerStore, WorkerAgent, NoActions, NoConditions, WorkerTyping>,
        before: &app_sdk::workflow::RunSnapshot,
        fatal: bool,
    ) {
        let observed = tokio::time::timeout(Duration::from_secs(1), run.snapshot())
            .await
            .expect("snapshot observation is bounded");
        if fatal {
            assert!(
                matches!(observed, Err(AppError::Closed(_))),
                "a fatal worker drop revokes snapshot reads"
            );
        } else {
            let snapshot = observed.expect("ordinary root drop preserves committed snapshot reads");
            assert!(matches!(snapshot.status, RunStatus::Succeeded));
            assert_eq!(snapshot.checkpoint, before.checkpoint);
            assert_eq!(snapshot.state, before.state);
            assert_eq!(snapshot.last_action_ordinal, before.last_action_ordinal);
            assert_eq!(snapshot.outcomes.len(), before.outcomes.len());
            for (actual, expected) in snapshot.outcomes.iter().zip(&before.outcomes) {
                assert_eq!(actual.node, expected.node);
                assert_eq!(actual.ordinal, expected.ordinal);
                assert_eq!(actual.result, expected.result);
            }
        }
    }

    fn definitions() -> Vec<WorkflowDefinition> {
        [
            json!({
                "schema":"app-sdk.workflow/v2", "id":"worker-state", "revision":"r1",
                "state":{"item":0}, "required":["item"], "reducers":{"item":"reject_concurrent"},
                "entry":"effect", "nodes":[{"id":"effect","step":{"kind":"state","mutations":[
                    {"kind":"increment","target":"item","value":{"kind":"literal","value":1}}
                ]}}], "routes":{}
            }),
            json!({
                "schema":"app-sdk.workflow/v2", "id":"worker-input", "revision":"r1",
                "state":{}, "required":[], "reducers":{}, "entry":"input", "nodes":[{"id":"input", "deadline_ms":30000,
                    "step":{"kind":"input","message":[{"kind":"text","text":"Keep this run parked"}]}
                }], "routes":{}
            }),
            json!({
                "schema":"app-sdk.workflow/v2", "id":"worker-capacity-input", "revision":"r1",
                "state":{}, "required":[], "reducers":{}, "entry":"input", "nodes":[{"id":"input",
                    "step":{"kind":"input","message":[{"kind":"text","text":"Retain the full capacity of live Input owners"}]}
                }], "routes":{}
            }),
            json!({
                "schema":"app-sdk.workflow/v2", "id":"worker-middle", "revision":"r1",
                "state":{"item":0,"items":[3,4],"collected":[]},
                "required":["item","items","collected"],
                "reducers":{"item":"reject_concurrent","items":"reject_concurrent","collected":"collect"},
                "entry":"map", "nodes":[{"id":"map","step":{"kind":"map","workflow":"worker-state",
                    "items":{"kind":"state","path":"items"},"input_key":"item","max_parallel":1,
                    "reductions":[{"source":"item","target":"collected","reducer":"collect"}]
                }}], "routes":{}
            }),
            json!({
                "schema":"app-sdk.workflow/v2", "id":"worker-parent", "revision":"r1",
                "state":{"items":[10,20],"collected":[]},
                "required":["items","collected"],
                "reducers":{"items":"reject_concurrent","collected":"collect"}, "entry":"map",
                "nodes":[{"id":"map","step":{"kind":"map","workflow":"worker-middle",
                    "items":{"kind":"state","path":"items"},"input_key":"item","max_parallel":1,
                    "reductions":[{"source":"collected","target":"collected","reducer":"collect"}]
                }}], "routes":{}
            }),
        ].into_iter().map(|value| WorkflowDefinition::parse(&value.to_string())
            .expect("existing state/Input/map workflow syntax")).collect()
    }

    async fn start(
        &self,
        name: &str,
    ) -> WorkflowRun<WorkerStore, WorkerAgent, NoActions, NoConditions, WorkerTyping> {
        self.workspace
            .workflow(name.parse().unwrap())
            .await
            .unwrap()
            .start(ExecutionOptions::default())
            .await
            .expect("background admission")
    }

    async fn complete(&self, name: &str) {
        let run = self.start(name).await;
        let snapshot = tokio::time::timeout(Duration::from_secs(5), run.wait())
            .await
            .expect("driver progress with caller reactor running")
            .expect("run observation");
        assert!(matches!(snapshot.status, RunStatus::Succeeded));
        if name == "worker-state" {
            assert_eq!(snapshot.state.values()["item"], 1);
        } else {
            assert_eq!(
                snapshot.state.values()["collected"],
                json!([[4, 5], [4, 5]])
            );
        }
    }

    fn assert_driver_thread(&self, caller: ThreadId) {
        let observed = self.evidence.mutation_threads.lock().unwrap();
        assert!(
            !observed.is_empty(),
            "the real mutation must execute before checking its lane"
        );
        assert!(
            observed.iter().all(|thread| *thread != caller),
            "driver ran on caller runtime thread"
        );
        assert!(
            observed.iter().all(|thread| thread == &observed[0]),
            "one App created multiple driver threads"
        );
    }

    async fn close(self) {
        let Self {
            mut app,
            workspace,
            store,
            evidence,
        } = self;
        drop(workspace);
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(5),
        })
        .await
        .expect("driver and lower cleanup join before shutdown returns");
        drop(app);
        Self::released(&evidence).await;
        drop(store);
    }

    async fn released(evidence: &WorkerEvidence) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while evidence.typing_dropped.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("App-derived ownership releases while runtime is driven");
        assert_eq!(evidence.typing_dropped.load(Ordering::SeqCst), 1);
        assert!(evidence.data_created.load(Ordering::SeqCst) > 0);
        assert_eq!(
            evidence.data_created.load(Ordering::SeqCst),
            evidence.data_dropped.load(Ordering::SeqCst)
        );
    }
}

#[tokio::test]
async fn background_state_uses_one_dedicated_driver_thread_and_keeps_data_local() {
    let case = WorkerCase::open().await;
    let caller = std::thread::current().id();
    for _ in 0..3 {
        case.complete("worker-state").await;
    }
    case.assert_driver_thread(caller);
    case.close().await;
}

#[tokio::test]
async fn parked_input_does_not_starve_other_drivers_or_cancellation() {
    let case = WorkerCase::open().await;
    let parked = case.start("worker-input").await;
    let snapshot = tokio::time::timeout(Duration::from_secs(3), parked.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        snapshot.interactions.as_slice(),
        [Interaction::Input { .. }]
    ));
    for _ in 0..8 {
        case.complete("worker-state").await;
    }
    let current = parked.snapshot().await.unwrap();
    assert!(matches!(
        current.interactions.as_slice(),
        [Interaction::Input { .. }]
    ));
    parked
        .cancel(current.checkpoint)
        .await
        .expect("control remains responsive");
    let cancelled = tokio::time::timeout(Duration::from_secs(3), parked.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(cancelled.status, RunStatus::Cancelled));
    drop(parked);
    case.assert_driver_thread(std::thread::current().id());
    case.close().await;
}

#[test]
fn nested_children_and_worker_join_leave_one_lower_blocking_slot_available() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let case = WorkerCase::open().await;
        case.complete("worker-parent").await;
        assert!(case.evidence.blocking_loads.load(Ordering::SeqCst) > 0);
        case.assert_driver_thread(std::thread::current().id());
        case.close().await;
    });
    // Runtime destruction follows completed App cleanup. Destroying its reactor
    // first is outside the supported lifetime contract and cannot prove a join.
    drop(runtime);
}

#[tokio::test]
async fn root_drop_after_a_real_effect_releases_local_ownership_and_parked_work() {
    let case = WorkerCase::open().await;
    case.complete("worker-state").await;
    let parked = case.start("worker-input").await;
    let snapshot = tokio::time::timeout(Duration::from_secs(3), parked.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        snapshot.interactions.as_slice(),
        [Interaction::Input { .. }]
    ));
    assert!(
        !case.evidence.mutation_threads.lock().unwrap().is_empty(),
        "effect-before-drop rendezvous"
    );
    let WorkerCase {
        app,
        workspace,
        store,
        evidence,
    } = case;
    drop(parked);
    drop(workspace);
    drop(app);
    WorkerCase::released(&evidence).await;
    drop(store);
}

#[tokio::test]
async fn accepted_native_commit_keeps_authority_after_driver_observer_cancellation() {
    let case = WorkerCase::open().await;
    case.evidence.commit.armed.store(true, Ordering::SeqCst);
    let workspace_id = case.workspace.id().clone();
    let mut admitted = None;
    // Capture assertion failures so every route below first revokes App work,
    // releases the selected lower receipt and joins its retained task. Resume
    // the original failure only after both lower and App cleanup were awaited.
    let applied = AssertUnwindSafe(async {
        admitted = Some(case.start("worker-state").await);
        let run = admitted.as_ref().unwrap();
        tokio::time::timeout(
            Duration::from_secs(3),
            case.evidence.commit.applied.notified(),
        )
        .await
        .expect("real native effect commit applies before observer cancellation");
        let snapshot = tokio::time::timeout(Duration::from_secs(3), run.snapshot())
            .await
            .expect("independent durable read is bounded")
            .expect("independent durable read while receipt is held");
        assert_eq!(snapshot.state.values()["item"], 1);
        assert_eq!(
            case.evidence.commit.observed_losses.load(Ordering::SeqCst),
            0
        );
    })
    .catch_unwind()
    .await;
    let run_id = admitted.as_ref().map(|run| run.id().clone());
    let WorkerCase {
        app,
        workspace,
        store,
        evidence,
    } = case;
    drop(admitted);
    drop(workspace);
    drop(app);
    let cancelled = AssertUnwindSafe(async {
        tokio::time::timeout(
            Duration::from_secs(3),
            evidence.commit.observer_dropped.notified(),
        )
        .await
        .expect("driver observation is cancelled while native receipt remains owned");
        assert_eq!(evidence.commit.observed_losses.load(Ordering::SeqCst), 1);
        let retained = evidence
            .commit_task
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|task| !task.is_finished());
        assert!(
            retained,
            "the lower finalization task is still retained at the receipt gate"
        );
    })
    .catch_unwind()
    .await;
    let joined = AssertUnwindSafe(evidence.join_commit())
        .catch_unwind()
        .await;
    let released = AssertUnwindSafe(WorkerCase::released(&evidence))
        .catch_unwind()
        .await;
    for outcome in [applied, cancelled, joined, released] {
        if let Err(failure) = outcome {
            std::panic::resume_unwind(failure);
        }
    }
    assert!(evidence.commit_task.lock().unwrap().is_none());
    assert_eq!(evidence.mutation_threads.lock().unwrap().len(), 1);
    WorkerCase::recover_effect(
        WorkerStore {
            inner: store,
            evidence,
        },
        workspace_id,
        run_id.unwrap(),
        0,
    )
    .await;
}

#[tokio::test]
async fn worker_read_drop_panic_during_shutdown_retains_failure_and_finishes_lower_cleanup() {
    let mut case = WorkerCase::open_with_agents(2).await;
    case.evidence.panic_read_armed.store(true, Ordering::SeqCst);
    let workspace_id = case.workspace.id().clone();
    let mut admitted = None;
    let observed = AssertUnwindSafe(async {
        admitted = Some(case.start("worker-state").await);
        let run = admitted.as_ref().unwrap();
        tokio::time::timeout(
            Duration::from_secs(3),
            case.evidence.panic_read_entered.notified(),
        )
        .await
        .expect("driver retains an actual terminal-record read after committing the effect");
        let before = tokio::time::timeout(Duration::from_secs(3), run.snapshot())
            .await
            .expect("committed read is bounded")
            .expect("caller can inspect committed truth");
        assert!(matches!(before.status, RunStatus::Succeeded));
        assert_eq!(before.state.values()["item"], 1);
        let error = case
            .app
            .shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(5),
            })
            .await
            .expect_err("worker destruction failure survives an already-started finalizer");
        assert!(
            matches!(error, AppError::Integration(_)),
            "unexpected shutdown category: {error}"
        );
        assert!(
            !error
                .to_string()
                .contains("worker read destructor private diagnostic")
        );
        assert_eq!(case.evidence.panic_read_dropped.load(Ordering::SeqCst), 1);
        for shutdown in &case.evidence.agent_shutdowns {
            assert_eq!(
                shutdown.load(Ordering::SeqCst),
                1,
                "each remaining Agent shuts down after the worker joins"
            );
        }
        match run.snapshot().await {
            Err(error) => assert!(
                matches!(error, AppError::Closed(_)),
                "a fatal worker stop invalidates the whole App; got {error}"
            ),
            Ok(snapshot) => panic!(
                "a fatal worker stop still allowed checkpoint {} with state {:?}",
                snapshot.checkpoint, snapshot.state
            ),
        }
        let replay = case
            .app
            .shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            })
            .await
            .expect_err("terminal shutdown failure is replayed");
        assert_eq!(error.to_string(), replay.to_string());
    })
    .catch_unwind()
    .await;
    let run_id = admitted.as_ref().map(|run| run.id().clone());
    let WorkerCase {
        app,
        workspace,
        store,
        evidence,
    } = case;
    drop(admitted);
    drop(workspace);
    drop(app);
    // Even a failed entry/shutdown assertion keeps the caller runtime driven
    // while the existing App supervisor joins the revoked worker and cleanup.
    let released = AssertUnwindSafe(WorkerCase::released(&evidence))
        .catch_unwind()
        .await;
    for outcome in [observed, released] {
        if let Err(failure) = outcome {
            std::panic::resume_unwind(failure);
        }
    }
    for shutdown in &evidence.agent_shutdowns {
        assert_eq!(shutdown.load(Ordering::SeqCst), 1);
    }
    WorkerCase::recover_effect(
        WorkerStore {
            inner: store,
            evidence,
        },
        workspace_id,
        run_id.unwrap(),
        2,
    )
    .await;
}

#[tokio::test]
async fn ordinary_root_drop_preserves_snapshot_until_a_new_generation_fences_it() {
    WorkerCase::observe_root_drop(false).await;
}

#[tokio::test]
async fn fatal_worker_root_drop_revokes_snapshot_during_and_after_lower_cleanup() {
    WorkerCase::observe_root_drop(true).await;
}

#[tokio::test]
async fn shutdown_retries_a_real_retention_change_after_its_active_inventory_scan() {
    // Drain reaches the quiescence read; CancelAndDrain first inventories
    // cancellation. Both must retain the same accepted finalizer on movement.
    for policy in [
        Shutdown::Drain {
            timeout: Duration::from_secs(4),
        },
        Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(4),
        },
    ] {
        let WorkerCase {
            mut app,
            workspace,
            store,
            evidence,
        } = WorkerCase::open().await;
        evidence.retention_armed.store(true, Ordering::SeqCst);
        let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(8), async {
            let run = workspace
                .workflow("worker-state".parse().unwrap())
                .await
                .unwrap()
                .start(ExecutionOptions::default())
                .await
                .unwrap();
            evidence.retention_entered.notified().await;
            // No sleep assumption: terminal state is durable while its actual
            // capacity-release operation remains before native begin behind this gate.
            evidence.retention_scan_armed.store(true, Ordering::SeqCst);
            app.shutdown(policy)
                .await
                .expect("typed recoverable inventory movement must not fail shutdown");
            assert_eq!(evidence.retention_scans.load(Ordering::SeqCst), 1);
            drop(run);
        }))
        .catch_unwind()
        .await;
        evidence.retention_release.notify_one();
        evidence.retention_applied.notify_one();
        let cleanup = tokio::time::timeout(
            Duration::from_secs(6),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(4),
            }),
        )
        .await;
        drop(workspace);
        drop(app);
        let released = AssertUnwindSafe(WorkerCase::released(&evidence))
            .catch_unwind()
            .await;
        drop(store);
        match observed {
            Ok(result) => result.expect("retention scan/shutdown remain bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        if let Err(panic) = released {
            std::panic::resume_unwind(panic);
        }
        cleanup.expect("all original owned cleanup joins").unwrap();
    }
}

#[tokio::test]
async fn shutdown_retries_when_its_scanned_active_run_is_retained_and_explicitly_deleted() {
    let WorkerCase {
        mut app,
        workspace,
        store,
        evidence,
    } = WorkerCase::open().await;
    evidence.retention_armed.store(true, Ordering::SeqCst);
    let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(10), async {
        let mut run = workspace.workflow("worker-state".parse().unwrap()).await.unwrap()
            .start(ExecutionOptions::default()).await.unwrap();
        evidence.retention_entered.notified().await;
        let terminal = run.snapshot().await.unwrap();
        assert!(matches!(terminal.status, RunStatus::Succeeded));
        let scope = format!("{}/worker-state/{}/{}", workspace.id(), run.id(), terminal.occurrence);
        let head = StateKey::new("app-sdk", format!("run/{scope}")).unwrap();
        *evidence.delete_head.lock().unwrap() = Some(head.clone());
        evidence.delete_read_armed.store(true, Ordering::SeqCst);
        let mut deletion = Box::pin(run.delete(terminal.checkpoint));
        tokio::select! {
            _ = evidence.delete_read_entered.notified() => {},
            result = &mut deletion => panic!("accepted public deletion must reach its held head read: {}", result.is_ok()),
            _ = tokio::time::sleep(Duration::from_secs(2)) => panic!("public deletion was not admitted"),
        }
        evidence.delete_after_scan.store(true, Ordering::SeqCst);
        evidence.retention_scan_armed.store(true, Ordering::SeqCst);
        let shutdown = app.shutdown(Shutdown::CancelAndDrain { timeout: Duration::from_secs(4) }).await;
        let deleted = tokio::time::timeout(Duration::from_secs(2), &mut deletion).await;
        drop(deletion);
        assert_eq!(evidence.retention_scans.load(Ordering::SeqCst), 1);
        assert_eq!(evidence.deleted_heads.load(Ordering::SeqCst), 1);
        assert!(store.load(head).await.unwrap().is_none());
        assert!(store.load(StateKey::new("app-sdk", format!("active/{scope}")).unwrap()).await.unwrap().is_none());
        shutdown.expect("a removed stale active marker is read movement, not corruption");
        deleted.expect("original deletion observer completes").expect("original deletion converges");
    })).catch_unwind().await;
    evidence.retention_release.notify_one();
    evidence.retention_applied.notify_one();
    evidence.delete_read_release.notify_one();
    evidence.deletion_applied.notify_one();
    let cleanup = tokio::time::timeout(
        Duration::from_secs(6),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(4),
        }),
    )
    .await;
    drop(workspace);
    drop(app);
    let released = AssertUnwindSafe(WorkerCase::released(&evidence))
        .catch_unwind()
        .await;
    drop(store);
    match observed {
        Ok(result) => result.expect("retention/deletion scan observation is bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    if let Err(panic) = released {
        std::panic::resume_unwind(panic);
    }
    cleanup
        .expect("accepted finalizer joins after either observation result")
        .unwrap();
}

#[tokio::test]
async fn a_dangling_active_marker_keeps_missing_head_corruption_permanent_during_shutdown() {
    let WorkerCase {
        mut app,
        workspace,
        store,
        evidence,
    } = WorkerCase::open().await;
    let mut repair = None;
    let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(8), async {
        let run = workspace
            .workflow("worker-state".parse().unwrap())
            .await
            .unwrap()
            .start(ExecutionOptions::default())
            .await
            .unwrap();
        let terminal = run.wait().await.unwrap();
        assert!(matches!(terminal.status, RunStatus::Succeeded));
        let scope = format!(
            "{}/worker-state/{}/{}",
            workspace.id(),
            run.id(),
            terminal.occurrence
        );
        let head = StateKey::new("app-sdk", format!("run/{scope}")).unwrap();
        let marker = StateKey::new("app-sdk", format!("active/{scope}")).unwrap();
        let original = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let row = store.load(head.clone()).await.unwrap().unwrap().record;
                if row.value.pointer("/retention/state") == Some(&json!("retained"))
                    && store.load(marker.clone()).await.unwrap().is_none()
                {
                    break row;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("actual capacity release precedes corruption");
        repair = Some((original.clone(), marker.clone()));
        let mut transaction = store.begin_transaction().await.unwrap();
        transaction.delete(head).await.unwrap();
        transaction
            .put(StateRecord::new(
                marker,
                json!({"epoch":original.value["epoch"]}),
            ))
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        assert!(
            matches!(
                app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(3)
                })
                .await,
                Err(AppError::Integration(_))
            ),
            "an existing dangling marker is permanent corruption, not a retryable missing run"
        );
    }))
    .catch_unwind()
    .await;
    if let Some((head, marker)) = repair {
        let mut transaction = store.begin_transaction().await.unwrap();
        transaction.put(head).await.unwrap();
        transaction.delete(marker).await.unwrap();
        transaction.commit().await.unwrap();
    }
    let cleanup = tokio::time::timeout(
        Duration::from_secs(6),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(4),
        }),
    )
    .await;
    drop(workspace);
    drop(app);
    let released = AssertUnwindSafe(WorkerCase::released(&evidence))
        .catch_unwind()
        .await;
    drop(store);
    match observed {
        Ok(result) => result.expect("permanent inventory failure remains bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    if let Err(panic) = released {
        std::panic::resume_unwind(panic);
    }
    assert!(matches!(
        cleanup.expect("failed finalizer cleanup joins"),
        Err(AppError::Integration(_))
    ));
}

#[tokio::test]
async fn shutdown_retries_retention_between_its_capacity_head_and_marker_reads() {
    let WorkerCase {
        mut app,
        workspace,
        store,
        evidence,
    } = WorkerCase::open().await;
    evidence.retention_armed.store(true, Ordering::SeqCst);
    let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(8), async {
        let run = workspace.workflow("worker-state".parse().unwrap()).await.unwrap()
            .start(ExecutionOptions::default()).await.unwrap();
        evidence.retention_entered.notified().await;
        let terminal = run.snapshot().await.unwrap();
        let scope = format!("{}/worker-state/{}/{}", workspace.id(), run.id(), terminal.occurrence);
        let head = StateKey::new("app-sdk", format!("run/{scope}")).unwrap();
        let marker = StateKey::new("app-sdk", format!("active/{scope}")).unwrap();
        assert_eq!(store.load(head.clone()).await.unwrap().unwrap().record.value.pointer("/retention/state"), Some(&json!("active")));
        assert!(store.load(marker.clone()).await.unwrap().is_some());
        *evidence.capacity_marker.lock().unwrap() = Some(marker.clone());
        evidence.retention_after_marker.store(true, Ordering::SeqCst);
        evidence.retention_scan_armed.store(true, Ordering::SeqCst);
        let shutdown = app.shutdown(Shutdown::CancelAndDrain { timeout: Duration::from_secs(4) }).await;
        assert_eq!(evidence.retention_scans.load(Ordering::SeqCst), 1);
        assert_eq!(evidence.capacity_reads.load(Ordering::SeqCst), 1);
        assert_eq!(store.load(head).await.unwrap().unwrap().record.value.pointer("/retention/state"), Some(&json!("retained")));
        assert!(store.load(marker).await.unwrap().is_none());
        shutdown.expect("a changed capacity head is recoverable even when its captured Active disagrees with the newer absent marker");
    })).catch_unwind().await;
    evidence.retention_release.notify_one();
    evidence.retention_applied.notify_one();
    let cleanup = tokio::time::timeout(
        Duration::from_secs(6),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(4),
        }),
    )
    .await;
    drop(workspace);
    drop(app);
    let released = AssertUnwindSafe(WorkerCase::released(&evidence))
        .catch_unwind()
        .await;
    drop(store);
    match observed {
        Ok(result) => result.expect("head/marker interleaving remains bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    if let Err(panic) = released {
        std::panic::resume_unwind(panic);
    }
    cleanup.expect("accepted shutdown cleanup joins").unwrap();
}

#[tokio::test]
async fn an_unchanged_active_head_without_its_marker_is_permanent_corruption() {
    let WorkerCase {
        mut app,
        workspace,
        store,
        evidence,
    } = WorkerCase::open().await;
    evidence.retention_armed.store(true, Ordering::SeqCst);
    let mut repair = None;
    let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(8), async {
        let mut run = workspace.workflow("worker-state".parse().unwrap()).await.unwrap()
            .start(ExecutionOptions::default()).await.unwrap();
        evidence.retention_entered.notified().await;
        let terminal = run.snapshot().await.unwrap();
        assert!(matches!(terminal.status, RunStatus::Succeeded));
        let scope = format!("{}/worker-state/{}/{}", workspace.id(), run.id(), terminal.occurrence);
        let head = StateKey::new("app-sdk", format!("run/{scope}")).unwrap();
        let marker = StateKey::new("app-sdk", format!("active/{scope}")).unwrap();
        let original_head = store.load(head.clone()).await.unwrap().unwrap();
        assert_eq!(original_head.record.value.pointer("/retention/state"), Some(&json!("active")));
        let original_marker = store.load(marker.clone()).await.unwrap().unwrap().record;
        repair = Some(original_marker);
        let mut transaction = store.begin_transaction().await.unwrap();
        transaction.delete(marker.clone()).await.unwrap();
        transaction.commit().await.unwrap();
        let deleted = tokio::time::timeout(Duration::from_secs(2), run.delete(terminal.checkpoint)).await;
        assert!(matches!(deleted, Ok(Err(AppError::Integration(_)))), "a stable inconsistent capacity pair must fail as corruption instead of retrying or deleting history");
        let unchanged = store.load(head).await.unwrap().unwrap();
        assert_eq!(unchanged.revision, original_head.revision);
        assert_eq!(unchanged.record.value, original_head.record.value);
        assert!(store.load(marker).await.unwrap().is_none());
        assert_eq!(run.snapshot().await.unwrap().checkpoint, terminal.checkpoint);
    })).catch_unwind().await;
    let repaired = match repair {
        Some(marker) => Some(
            tokio::time::timeout(Duration::from_secs(3), async {
                let mut transaction = store.begin_transaction().await?;
                transaction.put(marker).await?;
                transaction.commit().await
            })
            .await,
        ),
        None => None,
    };
    evidence.retention_release.notify_one();
    evidence.retention_applied.notify_one();
    let cleanup = tokio::time::timeout(
        Duration::from_secs(6),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(4),
        }),
    )
    .await;
    drop(workspace);
    drop(app);
    let released = AssertUnwindSafe(WorkerCase::released(&evidence))
        .catch_unwind()
        .await;
    drop(store);
    if let Some(repaired) = repaired {
        repaired
            .expect("marker restoration is bounded")
            .expect("original marker restored before retention release");
    }
    match observed {
        Ok(result) => result.expect("stable capacity corruption observation is bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    if let Err(panic) = released {
        std::panic::resume_unwind(panic);
    }
    cleanup
        .expect("original retention and App cleanup join")
        .unwrap();
}

#[tokio::test]
async fn capacity_scan_keeps_readmission_after_a_retained_recheck_retryable() {
    let WorkerCase {
        mut app,
        workspace,
        store,
        evidence,
    } = WorkerCase::open().await;
    evidence.reject_pre_once.store(true, Ordering::SeqCst);
    evidence.retention_armed.store(true, Ordering::SeqCst);
    let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(10), async {
        let workflow = workspace.workflow("worker-state".parse().unwrap()).await.unwrap();
        let mut run = workflow.start(ExecutionOptions::default()).await.unwrap();
        evidence.retention_entered.notified().await;
        let failed = run.snapshot().await.unwrap();
        assert!(matches!(failed.status, RunStatus::Failed(_)));
        assert_eq!(failed.state.values()["item"], 0);
        let original_workspace = workspace.snapshot().await.unwrap();
        let scope = format!("{}/worker-state/{}/{}", workspace.id(), run.id(), failed.occurrence);
        let head_key = StateKey::new("app-sdk", format!("run/{scope}")).unwrap();
        let marker_key = StateKey::new("app-sdk", format!("active/{scope}")).unwrap();
        assert!(store.load(marker_key.clone()).await.unwrap().is_some());
        *evidence.readmission_head.lock().unwrap() = Some(head_key.clone());
        *evidence.capacity_marker.lock().unwrap() = Some(marker_key.clone());
        evidence.retention_scan_armed.store(true, Ordering::SeqCst);
        let mut replacement = Box::pin(workspace.replace(
            original_workspace.revision, WorkspaceConfig {
                name: original_workspace.config.name.clone(),
                root: original_workspace.config.root.clone(),
            },
        ));
        tokio::select! {
            _ = evidence.readmission_head_entered.notified() => {},
            result = &mut replacement => panic!("workspace capacity observation did not reach its held Retained recheck: {:?}", result.err()),
        }
        assert_eq!(evidence.retention_scans.load(Ordering::SeqCst), 1);
        assert_eq!(evidence.readmission_reads.load(Ordering::SeqCst), 1);
        assert!(store.load(marker_key.clone()).await.unwrap().is_none());
        // The retained background owner may still be releasing its local lease.
        // Retry only pre-admission Busy; no source is ever polled by this test.
        loop {
            match run.execution(ExecutionOptions::default()).await {
                Err(AppError::Busy(_)) => tokio::task::yield_now().await,
                Err(error) => panic!("explicit readmission failed: {error}"),
                Ok(execution) => {
                    let head = store.load(head_key.clone()).await.unwrap().unwrap().record;
                    assert_eq!(head.value.pointer("/retention/state"), Some(&json!("active")));
                    assert_eq!(head.value["sequence"], json!(failed.checkpoint + 1));
                    assert!(store.load(marker_key.clone()).await.unwrap().is_some(),
                        "the real retry mutation must publish a new active marker first");
                    assert_eq!(evidence.mutation_threads.lock().unwrap().len(), 1);
                    evidence.readmission_head_release.notify_one();
                    let replaced = (&mut replacement).await;
                    assert!(matches!(replaced, Err(AppError::Busy(_))),
                        "retained capacity observed before new readmission is inventory movement, not corruption");
                    execution.abort().await.unwrap();
                    break;
                }
            }
        }
        drop(replacement);
        let unchanged_workspace = workspace.snapshot().await.unwrap();
        assert_eq!(unchanged_workspace.revision, original_workspace.revision);
        assert_eq!(unchanged_workspace.config.name, original_workspace.config.name);
        assert_eq!(unchanged_workspace.config.root, original_workspace.config.root);
        let paused = run.snapshot().await.unwrap();
        assert!(matches!(paused.status, RunStatus::Paused));
        assert_eq!(paused.state, failed.state);
        assert_eq!(paused.last_action_ordinal, failed.last_action_ordinal);
        assert!(paused.outcomes == failed.outcomes, "readmission preserves committed outcomes");
        assert_eq!(evidence.mutation_threads.lock().unwrap().len(), 1);
    })).catch_unwind().await;
    evidence.retention_release.notify_one();
    evidence.retention_applied.notify_one();
    evidence.readmission_head_release.notify_one();
    let cleanup = tokio::time::timeout(
        Duration::from_secs(6),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(4),
        }),
    )
    .await;
    drop(workspace);
    drop(app);
    let released = AssertUnwindSafe(WorkerCase::released(&evidence))
        .catch_unwind()
        .await;
    drop(store);
    match observed {
        Ok(result) => result.expect("retained readmission interleaving is bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    if let Err(panic) = released {
        std::panic::resume_unwind(panic);
    }
    cleanup.expect("readmission fixture cleanup joins").unwrap();
}

#[tokio::test]
async fn shutdown_observation_waits_for_accepted_admission_before_capacity_exists() {
    let WorkerCase {
        mut app,
        workspace,
        store,
        evidence,
    } = WorkerCase::open().await;
    let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(8), async {
        let workflow = workspace.workflow("worker-state".parse().unwrap()).await.unwrap();
        *evidence.admission_read.lock().unwrap() = Some(
            StateKey::new("app-sdk", format!("workspace/{}", workspace.id())).unwrap(),
        );
        evidence.admission_read_armed.store(true, Ordering::SeqCst);
        let mut admission = Box::pin(workflow.prepare(WorkflowState::default()));
        tokio::select! {
            _ = evidence.admission_read_entered.notified() => {},
            result = &mut admission => panic!("accepted admission did not enter the held pre-capacity read: {}", result.is_ok()),
        }
        let active = store.list(StateListRequest::new("app-sdk").unwrap()
            .with_key_prefix(StateKeyPrefix::new("active/").unwrap())
            .with_limit(1).unwrap()).await.unwrap();
        assert!(active.records.is_empty());
        assert!(evidence.mutation_threads.lock().unwrap().is_empty());
        assert_eq!(evidence.data_created.load(Ordering::SeqCst), 0);
        assert!(matches!(app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_millis(50),
        }).await, Err(AppError::Timeout(_))),
            "normal finalization must retain the accepted admission owner even before a durable run exists");
        assert_eq!(evidence.admission_read_returned.load(Ordering::SeqCst), 0);
        evidence.admission_read_release.notify_one();
        let admitted = (&mut admission).await;
        assert!(matches!(admitted, Err(AppError::Closed(_))),
            "released pre-capacity work observes already-sealed App admission");
        assert_eq!(evidence.admission_read_returned.load(Ordering::SeqCst), 1);
        assert!(evidence.mutation_threads.lock().unwrap().is_empty());
        assert_eq!(evidence.data_created.load(Ordering::SeqCst), 1,
            "released admission validates its initial state before observing sealed durable admission");
    })).catch_unwind().await;
    evidence.admission_read_release.notify_one();
    let cleanup = tokio::time::timeout(
        Duration::from_secs(6),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(4),
        }),
    )
    .await;
    drop(workspace);
    drop(app);
    let released = AssertUnwindSafe(WorkerCase::released(&evidence))
        .catch_unwind()
        .await;
    drop(store);
    match observed {
        Ok(result) => result.expect("held admission shutdown observation is bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    if let Err(panic) = released {
        std::panic::resume_unwind(panic);
    }
    cleanup
        .expect("accepted admission and App cleanup join")
        .unwrap();
}

#[tokio::test]
async fn shutdown_rechecks_workspace_deletion_reattached_after_its_empty_scan() {
    let case = WorkerCase::open_with_agents(2).await;
    case.complete("worker-state").await;
    let WorkerCase {
        mut app,
        mut workspace,
        store,
        evidence,
    } = case;
    let race = Arc::new(WorkspaceRace::default());
    let key = StateKey::new("app-sdk", format!("workspace/{}", workspace.id())).unwrap();
    *race.key.lock().unwrap() = Some(key.clone());
    *evidence.workspace_race.lock().unwrap() = Some(Arc::clone(&race));
    let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(10), async {
        // Public terminal observation settles hooks; capacity releases through
        // its independent native commit before this separate deletion race.
        evidence.retention_applied.notified().await;
        let mut reattacher = app.workspace(workspace.id().clone()).await.unwrap();
        let revision = workspace.snapshot().await.unwrap().revision;
        race.lower_armed.store(true, Ordering::SeqCst);
        let mut original = Box::pin(workspace.delete(revision));
        tokio::select! {
            _ = race.lower_entered.notified() => {},
            result = &mut original => panic!("original deletion did not reach its real lower gate: {result:?}"),
        }
        race.capture_armed.store(true, Ordering::SeqCst);
        let mut reattached = Box::pin(reattacher.delete(revision));
        tokio::select! {
            _ = race.capture_entered.notified() => {},
            result = &mut reattached => panic!("reattachment did not capture the real Deleting row: {result:?}"),
        }
        race.lower_release.notify_one();
        original.await.expect("original deletion completes and releases its registered owner");
        assert!(store.load(key.clone()).await.unwrap().is_none());
        assert!(evidence.lower_workspaces.lock().unwrap().iter().all(Option::is_none));

        race.scan_armed.store(true, Ordering::SeqCst);
        let mut shutdown = Box::pin(app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_millis(100),
        }));
        tokio::select! {
            _ = race.scan_entered.notified() => {},
            result = &mut shutdown => panic!("shutdown did not reach its empty post-sample workspace page: {result:?}"),
        }
        race.observation_armed.store(true, Ordering::SeqCst);
        race.capture_release.notify_one();
        tokio::select! {
            _ = race.observation_entered.notified() => {},
            result = &mut reattached => panic!("reattached operation did not reach its authoritative missing-row read: {result:?}"),
        }
        race.scan_release.notify_one();
        let shutdown = shutdown.await;
        assert!(matches!(shutdown, Err(AppError::Timeout(_))),
            "graceful finalization must retain a workspace operation accepted after its empty registry sample: {shutdown:?}");
        assert_eq!(race.observed_reads.load(Ordering::SeqCst), 0);
        assert!(evidence.agent_shutdowns.iter().all(|count| count.load(Ordering::SeqCst) == 0));
        race.observation_release.notify_one();
        reattached.await.expect("the accepted idempotent deletion publishes its own successful result");
        assert_eq!(race.observed_reads.load(Ordering::SeqCst), 1);
    })).catch_unwind().await;
    race.release();
    let cleanup = tokio::time::timeout(
        Duration::from_secs(6),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(4),
        }),
    )
    .await;
    drop(workspace);
    drop(app);
    let released = AssertUnwindSafe(WorkerCase::released(&evidence))
        .catch_unwind()
        .await;
    drop(store);
    match observed {
        Ok(result) => result.expect("workspace reattachment shutdown race is bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    if let Err(panic) = released {
        std::panic::resume_unwind(panic);
    }
    cleanup
        .expect("workspace reattachment and App cleanup join")
        .unwrap();
}

#[tokio::test]
async fn background_local_source_stays_on_one_worker_through_completion_cancel_and_shutdown() {
    let caller = std::thread::current().id();
    let evidence = Arc::new(WorkerEvidence::default());
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let definition = WorkerAction::definition();
    let config: AppConfig<WorkerStore, WorkerAgent> = AppConfig::new(
        WorkerStore {
            inner: store.clone(),
            evidence: Arc::clone(&evidence),
        },
        Vec::new(),
        vec![definition],
    );
    let mut app = App::open(
        config
            .with_actions(vec![WorkerAction::new(&evidence)])
            .with_state_typing(WorkerTyping(Arc::clone(&evidence))),
    )
    .await
    .unwrap();
    let mut runs = Vec::new();
    let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(15), async {
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().unwrap(),
            })
            .await
            .unwrap();
        let workflow = workspace
            .workflow("worker-source".parse().unwrap())
            .await
            .unwrap();
        for index in 0..5 {
            evidence.source_complete.store(false, Ordering::SeqCst);
            // The associated source contains a real Rc, not a Send wrapper.
            // Only start's intent crosses into the existing App worker.
            let run = workflow.start(ExecutionOptions::default()).await.unwrap();
            runs.push(run.id().clone());
            tokio::time::timeout(Duration::from_secs(2), evidence.source_pending.notified())
                .await
                .expect("the actual source emits a partial and enters Pending");
            let partial = run.snapshot().await.unwrap();
            assert_eq!(partial.state.values()["item"], 0);
            assert!(partial.last_action_ordinal.is_none() && partial.outcomes.is_empty());
            let partials = evidence
                .source_events
                .lock()
                .unwrap()
                .iter()
                .filter(|(id, event, _)| id == run.id() && *event == "partial")
                .count();
            assert_eq!(partials, 1);
            if index < 3 {
                evidence.release_source();
                let snapshot = tokio::time::timeout(Duration::from_secs(2), run.wait())
                    .await
                    .expect("background completion is bounded")
                    .unwrap();
                assert!(matches!(snapshot.status, RunStatus::Succeeded));
                assert_eq!(snapshot.state.values()["item"], 1);
                assert_eq!(snapshot.last_action_ordinal, Some(0));
                assert_eq!(snapshot.outcomes.len(), 1);
                assert_eq!(snapshot.outcomes[0].node, "effect");
                assert_eq!(snapshot.outcomes[0].result["answer"], 1);
            } else if index == 3 {
                run.cancel(partial.checkpoint).await.unwrap();
                let cancelled = tokio::time::timeout(Duration::from_secs(2), run.wait())
                    .await
                    .expect("pending local source cancellation is bounded")
                    .unwrap();
                assert!(matches!(cancelled.status, RunStatus::Cancelled));
                assert_eq!(cancelled.state.values()["item"], 0);
                assert!(cancelled.last_action_ordinal.is_none() && cancelled.outcomes.is_empty());
            } else {
                app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(3),
                })
                .await
                .expect("shutdown joins the pending source and worker");
            }
            evidence.assert_source(run.id(), index < 3, caller);
        }
        let events = evidence.source_events.lock().unwrap().clone();
        let mutations = evidence.mutation_threads.lock().unwrap().clone();
        let worker = events[0].2;
        assert!(
            events.iter().all(|(_, _, thread)| *thread == worker),
            "all five runs use the same existing worker"
        );
        assert!(mutations.iter().all(|thread| *thread == worker));
    }))
    .catch_unwind()
    .await;
    // Always release the cooperative source before attempting App cleanup,
    // including a failed partial/state assertion or an expired observation.
    evidence.release_source();
    let cleanup = AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(6),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(4),
        }),
    ))
    .catch_unwind()
    .await;
    drop(app);
    let released = AssertUnwindSafe(WorkerCase::released(&evidence))
        .catch_unwind()
        .await;
    drop(store);
    match observed {
        Ok(result) => result.expect("local source worker proof remains bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    cleanup
        .expect("cleanup does not panic")
        .expect("cleanup is bounded")
        .unwrap();
    if let Err(panic) = released {
        std::panic::resume_unwind(panic);
    }
    assert_eq!(runs.len(), 5);
    for (index, run) in runs.iter().enumerate() {
        evidence.assert_source(run, index < 3, caller);
    }
}

#[tokio::test]
async fn native_ready_read_retries_after_a_complete_initial_observation_without_replay() {
    let evidence = Arc::new(WorkerEvidence::default());
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let mut definitions = WorkerCase::definitions();
    definitions.push(WorkerAction::definition());
    let config: AppConfig<WorkerStore, WorkerAgent> = AppConfig::new(
        WorkerStore {
            inner: store.clone(),
            evidence: Arc::clone(&evidence),
        },
        Vec::new(),
        definitions,
    );
    let mut app = App::open(
        config
            .with_actions(vec![WorkerAction::new(&evidence)])
            .with_state_typing(WorkerTyping(Arc::clone(&evidence)))
            .with_hooks(WorkerHooks(Arc::clone(&evidence))),
    )
    .await
    .unwrap();
    let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(10), async {
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().unwrap(),
            })
            .await
            .unwrap();
        let source = workspace
            .workflow("worker-source".parse().unwrap())
            .await
            .unwrap();
        evidence.source_complete.store(true, Ordering::SeqCst);
        let warmup = source.start(ExecutionOptions::default()).await.unwrap();
        let complete = tokio::time::timeout(Duration::from_secs(3), warmup.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(complete.status, RunStatus::Succeeded));
        let source_events = evidence.source_events.lock().unwrap().clone();
        let worker = source_events
            .iter()
            .find(|(_, event, _)| *event == "construct")
            .unwrap()
            .2;
        assert_ne!(
            worker,
            std::thread::current().id(),
            "the actual non-Send source identifies the App worker"
        );
        let before = evidence.mutation_threads.lock().unwrap().len();
        *evidence.native_read.lock().unwrap() = Some(NativeRead::Initial);
        let workflow = workspace
            .workflow("worker-state".parse().unwrap())
            .await
            .unwrap();
        let run = workflow.start(ExecutionOptions::default()).await.unwrap();
        tokio::time::timeout(
            Duration::from_secs(2),
            evidence.native_read_observed.notified(),
        )
        .await
        .expect("the selected native read follows the real initial pre-hook");
        let initial = run.snapshot().await.unwrap();
        assert_eq!(initial.state.values()["item"], 0);
        assert_eq!(initial.checkpoint, 0);
        assert!(initial.last_action_ordinal.is_none() && initial.outcomes.is_empty());
        let unchanged = evidence.mutation_threads.lock().unwrap().len();
        assert_eq!(
            unchanged, before,
            "read failure occurs before the target State effect"
        );
        evidence.native_read_release.notify_one();
        let complete = tokio::time::timeout(Duration::from_secs(3), run.wait())
            .await
            .expect("one recoverable native read must not discard background ownership")
            .unwrap();
        assert!(matches!(complete.status, RunStatus::Succeeded));
        assert_eq!(complete.state.values()["item"], 1);
        assert_eq!(complete.last_action_ordinal, Some(0));
        assert_eq!(complete.outcomes.len(), 1);
        assert_eq!(evidence.native_read_faults.load(Ordering::SeqCst), 1);
        let after = evidence.mutation_threads.lock().unwrap().len();
        assert_eq!(
            after,
            before + 1,
            "the native State effect executes exactly once"
        );
    }))
    .catch_unwind()
    .await;
    *evidence.native_read.lock().unwrap() = None;
    evidence.native_read_release.notify_one();
    let cleanup = AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(6),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(4),
        }),
    ))
    .catch_unwind()
    .await;
    drop(app);
    let released = AssertUnwindSafe(WorkerCase::released(&evidence))
        .catch_unwind()
        .await;
    drop(store);
    match observed {
        Ok(result) => result.expect("native read ownership proof stays bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    cleanup
        .expect("cleanup does not panic")
        .expect("cleanup is bounded")
        .unwrap();
    if let Err(panic) = released {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn native_read_retry_after_committed_progress_retains_the_original_lifecycle() {
    let evidence = Arc::new(WorkerEvidence::default());
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let definition = WorkflowDefinition::parse(&json!({
        "schema":"app-sdk.workflow/v2", "id":"worker-progress", "revision":"r1",
        "state":{"item":0}, "required":["item"], "reducers":{"item":"reject_concurrent"},
        "entry":"first", "nodes":[
            {"id":"first", "step":{"kind":"state", "mutations":[
                {"kind":"increment", "target":"item", "value":{"kind":"literal","value":1}}
            ]}},
            {"id":"second", "step":{"kind":"state", "mutations":[
                {"kind":"increment", "target":"item", "value":{"kind":"literal","value":1}}
            ]}}
        ], "routes":{"first":{"mode":"first","branches":[{"target":{"kind":"node","id":"second"}}]}}
    }).to_string()).unwrap();
    let config: AppConfig<WorkerStore, WorkerAgent> = AppConfig::new(
        WorkerStore {
            inner: store.clone(),
            evidence: Arc::clone(&evidence),
        },
        Vec::new(),
        vec![WorkerAction::definition(), definition],
    );
    let mut app = App::open(
        config
            .with_actions(vec![WorkerAction::new(&evidence)])
            .with_state_typing(WorkerTyping(Arc::clone(&evidence)))
            .with_hooks(WorkerHooks(Arc::clone(&evidence))),
    )
    .await
    .unwrap();
    let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(10), async {
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().unwrap(),
            })
            .await
            .unwrap();
        let source = workspace
            .workflow("worker-source".parse().unwrap())
            .await
            .unwrap();
        evidence.source_complete.store(true, Ordering::SeqCst);
        let warmup = source.start(ExecutionOptions::default()).await.unwrap();
        let warmup = tokio::time::timeout(Duration::from_secs(3), warmup.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(warmup.status, RunStatus::Succeeded));
        let source_events = evidence.source_events.lock().unwrap().clone();
        let worker = source_events
            .iter()
            .find(|(_, event, _)| *event == "construct")
            .unwrap()
            .2;
        assert_ne!(worker, std::thread::current().id());
        let before = evidence.mutation_threads.lock().unwrap().len();
        evidence.native_lifecycle.lock().unwrap().clear();
        let workflow = workspace
            .workflow("worker-progress".parse().unwrap())
            .await
            .unwrap();
        let run = workflow.start(ExecutionOptions::default()).await.unwrap();
        tokio::time::timeout(
            Duration::from_secs(2),
            evidence.native_read_observed.notified(),
        )
        .await
        .expect("the next native read is held after a real first-step commit");
        let committed = run.snapshot().await.unwrap();
        assert_eq!(committed.state.values()["item"], 1);
        assert_eq!(committed.last_action_ordinal, Some(0));
        assert_eq!(committed.outcomes.len(), 1);
        assert_eq!(committed.outcomes[0].node, "first");
        let effects = evidence.mutation_threads.lock().unwrap().len();
        assert_eq!(effects, before + 1);
        let lifecycle = evidence.native_lifecycle.lock().unwrap().clone();
        assert_eq!(lifecycle, [("sync-start", 0), ("async-start", 0)]);
        evidence.native_read_release.notify_one();
        let complete = tokio::time::timeout(Duration::from_secs(3), run.wait())
            .await
            .expect("native progress resumes after the one recoverable read")
            .unwrap();
        assert!(matches!(complete.status, RunStatus::Succeeded));
        assert_eq!(complete.state.values()["item"], 2);
        assert_eq!(complete.last_action_ordinal, Some(1));
        assert_eq!(complete.outcomes.len(), 1);
        assert_eq!(complete.outcomes[0].node, "second");
        let effects = evidence.mutation_threads.lock().unwrap().len();
        assert_eq!(
            effects,
            before + 2,
            "each committed native effect executes once"
        );
        assert_eq!(evidence.native_read_faults.load(Ordering::SeqCst), 1);
        let lifecycle = evidence.native_lifecycle.lock().unwrap().clone();
        assert_eq!(
            lifecycle,
            [
                ("sync-start", 0),
                ("async-start", 0),
                ("sync-finish", 2),
                ("async-finish", 2)
            ],
            "a recoverable native read neither ends the original session nor opens another"
        );
    }))
    .catch_unwind()
    .await;
    *evidence.native_read.lock().unwrap() = None;
    evidence.native_read_release.notify_one();
    let cleanup = AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(6),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(4),
        }),
    ))
    .catch_unwind()
    .await;
    drop(app);
    let released = AssertUnwindSafe(WorkerCase::released(&evidence))
        .catch_unwind()
        .await;
    drop(store);
    match observed {
        Ok(result) => result.expect("post-progress native read proof stays bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    cleanup
        .expect("cleanup does not panic")
        .expect("cleanup is bounded")
        .unwrap();
    if let Err(panic) = released {
        std::panic::resume_unwind(panic);
    }
}

impl WorkerCase {
    async fn capacity_authority_read(phase: CapacityReadPhase, fault: CapacityReadFault) {
        let WorkerCase {
            mut app,
            workspace,
            store,
            evidence,
        } = WorkerCase::open_with_agents(2).await;
        evidence.retention_armed.store(true, Ordering::SeqCst);
        let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(36), async {
            let run = workspace
                .workflow("worker-state".parse().unwrap())
                .await
                .unwrap()
                .start(ExecutionOptions::default())
                .await
                .unwrap();
            evidence.retention_entered.notified().await;
            let terminal = run.snapshot().await.unwrap();
            let scope = format!(
                "{}/worker-state/{}/{}",
                workspace.id(),
                run.id(),
                terminal.occurrence
            );
            let head_key = StateKey::new("app-sdk", format!("run/{scope}")).unwrap();
            let marker_key = StateKey::new("app-sdk", format!("active/{scope}")).unwrap();
            let original = store.load(head_key.clone()).await.unwrap().unwrap();
            let checkpoint = store
                .load(
                    StateKey::new(
                        "app-sdk",
                        format!(
                            "history/{scope}/checkpoint/{:020}",
                            original.record.value["sequence"].as_u64().unwrap(),
                        ),
                    )
                    .unwrap(),
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                checkpoint.record.value["checkpoint"]["phase"]["state"],
                "succeeded"
            );
            assert_eq!(checkpoint.record.value["checkpoint"]["state"]["item"], 1);
            assert_eq!(
                original.record.value.pointer("/retention/state"),
                Some(&json!("active"))
            );
            assert!(store.load(marker_key.clone()).await.unwrap().is_some());
            let earlier_read = usize::from(!matches!(phase, CapacityReadPhase::Head));
            if earlier_read != 0 {
                // Spend this owner's first 20s before advancing to its later pure
                // read, so another fresh 30s ceiling cannot satisfy the oracle.
                if matches!(fault, CapacityReadFault::Held) {
                    tokio::time::sleep(Duration::from_secs(20)).await;
                }
                *evidence.retention_read_target.lock().unwrap() = Some((marker_key.clone(), phase));
                evidence.retention_release.notify_one();
                evidence.retention_entered.notified().await;
            }
            assert_eq!(
                evidence.retention_read_dropped.load(Ordering::SeqCst),
                earlier_read
            );
            if matches!(fault, CapacityReadFault::Permanent) {
                // Fail only the original selected read after its exact entry gate.
                // Other App inventory reads cannot consume this one-shot error.
                evidence
                    .retention_read_failure
                    .store(true, Ordering::SeqCst);
                evidence.retention_release.notify_one();
            }
            // Do not initiate shutdown: its observation timeout cannot substitute
            // for this original required capacity-read owner's finite disposition.
            let grace = if matches!(fault, CapacityReadFault::Permanent) {
                3
            } else if earlier_read == 0 {
                33
            } else {
                13
            };
            tokio::time::timeout(Duration::from_secs(grace), async {
                loop {
                    match app
                        .list_workspaces(None, NonZeroUsize::new(1).unwrap())
                        .await
                    {
                        Err(AppError::Closed(_)) => break,
                        Ok(_) => tokio::task::yield_now().await,
                        Err(error) => panic!("unexpected retention authority observation: {error}"),
                    }
                }
            })
            .await
            .expect("unreadable required capacity authority reaches bounded fatal cleanup");
            assert_eq!(
                evidence.retention_read_dropped.load(Ordering::SeqCst),
                earlier_read + 1,
                "fatal publication follows destruction of the original held read"
            );
            assert_eq!(
                store.load(head_key).await.unwrap().unwrap(),
                original,
                "unproven cleanup cannot rewrite the actual retained-capacity head"
            );
            assert!(store.load(marker_key).await.unwrap().is_some());
            assert_eq!(
                store
                    .load(StateKey::new("app-sdk", "app").unwrap())
                    .await
                    .unwrap()
                    .unwrap()
                    .record
                    .value["run_count"],
                1
            );
            assert!(matches!(run.snapshot().await, Err(AppError::Closed(_))));
        }))
        .catch_unwind()
        .await;
        evidence
            .retention_read_failure
            .store(false, Ordering::SeqCst);
        evidence.retention_read_target.lock().unwrap().take();
        evidence.retention_release.notify_one();
        let cleanup = tokio::time::timeout(
            Duration::from_secs(6),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(4),
            }),
        )
        .await;
        drop(workspace);
        drop(app);
        let released = AssertUnwindSafe(WorkerCase::released(&evidence))
            .catch_unwind()
            .await;
        drop(store);
        match observed {
            Ok(result) => result.expect("capacity authority proof remains bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        if let Err(panic) = released {
            std::panic::resume_unwind(panic);
        }
        assert!(matches!(
            cleanup.expect("failed capacity authority cleanup joins"),
            Err(AppError::Integration(_))
        ));
        assert!(
            evidence
                .agent_shutdowns
                .iter()
                .all(|count| count.load(Ordering::SeqCst) == 1)
        );
    }
}

#[tokio::test]
async fn held_capacity_authority_read_expires_without_releasing_unproven_retention() {
    WorkerCase::capacity_authority_read(CapacityReadPhase::Head, CapacityReadFault::Held).await;
}

#[tokio::test]
async fn held_capacity_marker_read_expires_under_the_original_preparation_budget() {
    WorkerCase::capacity_authority_read(CapacityReadPhase::Marker, CapacityReadFault::Held).await;
}

#[tokio::test]
async fn held_capacity_app_count_read_expires_under_the_original_preparation_budget() {
    WorkerCase::capacity_authority_read(CapacityReadPhase::Application, CapacityReadFault::Held)
        .await;
}

#[tokio::test]
async fn permanent_capacity_head_read_closes_without_releasing_unproven_retention() {
    WorkerCase::capacity_authority_read(CapacityReadPhase::Head, CapacityReadFault::Permanent)
        .await;
}

#[tokio::test]
async fn permanent_capacity_marker_read_closes_without_releasing_unproven_retention() {
    WorkerCase::capacity_authority_read(CapacityReadPhase::Marker, CapacityReadFault::Permanent)
        .await;
}

#[tokio::test]
async fn permanent_capacity_app_count_read_closes_without_releasing_unproven_retention() {
    WorkerCase::capacity_authority_read(
        CapacityReadPhase::Application,
        CapacityReadFault::Permanent,
    )
    .await;
}

impl WorkerCase {
    async fn unreadable_bootstrap(fault: CapacityReadFault, panic_first: bool) {
        let evidence = Arc::new(WorkerEvidence::default());
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let mut app = App::open(
            Self::config(store.clone(), &evidence, 2)
                .with_hooks(WorkerHooks(Arc::clone(&evidence))),
        )
        .await
        .unwrap();
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(37), async {
            let workspace = app.create_workspace(WorkspaceConfig {
                name: None, root: std::env::current_dir().unwrap(),
            }).await.unwrap();
            let workflow = workspace.workflow("worker-state".parse().unwrap()).await.unwrap();
            *evidence.native_read.lock().unwrap() = Some(NativeRead::Bootstrap { fault, panic_first });
            let run = workflow.start(ExecutionOptions::default()).await.unwrap();
            tokio::time::timeout(Duration::from_secs(2), evidence.native_read_observed.notified())
                .await.expect("successful registration reaches the original worker authority read");
            let heads = store.list(StateListRequest::new("app-sdk").unwrap()
                .with_key_prefix(StateKeyPrefix::new(format!("run/{}/worker-state/{}/", workspace.id(), run.id())).unwrap())
                .with_limit(2).unwrap()).await.unwrap();
            assert_eq!(heads.records.len(), 1, "the original bootstrap owns one exact occurrence");
            let head = heads.records.into_iter().next().unwrap().record;
            let head_key = head.key.clone();
            let occurrence = head.value["creation"].as_str().unwrap();
            assert_eq!(head.value["sequence"], 0);
            let checkpoint_key = StateKey::new("app-sdk", format!(
                "history/{}/worker-state/{}/{occurrence}/checkpoint/{:020}", workspace.id(), run.id(), 0,
            )).unwrap();
            let checkpoint = store.load(checkpoint_key.clone()).await.unwrap().unwrap().record;
            assert_eq!(checkpoint.value["checkpoint"]["state"]["item"], 0);
            assert!(checkpoint.value["checkpoint"]["position"].is_null());
            assert!(checkpoint.value["checkpoint"]["outcomes"].as_array().unwrap().is_empty());
            assert!(evidence.mutation_threads.lock().unwrap().is_empty());
            assert!(evidence.native_lifecycle.lock().unwrap().is_empty(),
                "unreadable bootstrap cannot open execution hooks");
            let validated_data = evidence.data_created.load(Ordering::SeqCst);
            assert_eq!(validated_data, evidence.data_dropped.load(Ordering::SeqCst),
                "only completed admission validation may construct caller data");
            tokio::time::timeout(Duration::from_secs(match fault {
                CapacityReadFault::Held => 33, CapacityReadFault::Permanent => 3,
            }), async {
                loop {
                    match app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await {
                        Err(AppError::Closed(_)) => break,
                        Ok(_) => tokio::task::yield_now().await,
                        Err(error) => panic!("worker authority must close without fabricating progress: {error:?}"),
                    }
                }
            }).await.expect("the owned worker authority read reaches fatal closure within its bound");
            assert_eq!(evidence.native_read_dropped.load(Ordering::SeqCst), 1,
                "the exact selected read is destroyed before fatal closure");
            assert_eq!(evidence.native_read_faults.load(Ordering::SeqCst), if panic_first { 2 } else { 1 });
            assert_eq!(evidence.data_created.load(Ordering::SeqCst), validated_data);
            assert_eq!(store.load(head_key).await.unwrap().unwrap().record.value, head.value);
            assert_eq!(store.load(checkpoint_key).await.unwrap().unwrap().record.value, checkpoint.value);
            assert!(evidence.mutation_threads.lock().unwrap().is_empty());
            assert!(evidence.native_lifecycle.lock().unwrap().is_empty());
            assert!(matches!(run.snapshot().await, Err(AppError::Closed(_))));
        })).catch_unwind().await;
        *evidence.native_read.lock().unwrap() = None;
        evidence.native_read_release.notify_one();
        let cleanup = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(6),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(4),
            }),
        ))
        .catch_unwind()
        .await;
        drop(app);
        let released = AssertUnwindSafe(Self::released(&evidence))
            .catch_unwind()
            .await;
        match checked {
            Ok(result) => result.expect("worker bootstrap case stays bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        assert!(matches!(
            cleanup
                .expect("worker fatal cleanup does not panic")
                .expect("worker fatal cleanup stays bounded"),
            Err(AppError::Integration(_))
        ));
        if let Err(panic) = released {
            std::panic::resume_unwind(panic);
        }
        for shutdown in &evidence.agent_shutdowns {
            assert_eq!(
                shutdown.load(Ordering::SeqCst),
                1,
                "both independent lower Agents join after original worker cleanup"
            );
        }
    }
}

#[tokio::test]
async fn permanent_background_bootstrap_read_closes_before_execution() {
    WorkerCase::unreadable_bootstrap(CapacityReadFault::Permanent, false).await;
}

#[tokio::test]
async fn held_background_bootstrap_read_is_destroyed_before_fatal_closure() {
    WorkerCase::unreadable_bootstrap(CapacityReadFault::Held, false).await;
}

#[tokio::test]
async fn permanent_task_failure_authority_retains_fatal_cleanup_without_effects() {
    WorkerCase::unreadable_bootstrap(CapacityReadFault::Permanent, true).await;
}

#[tokio::test]
async fn held_task_failure_authority_is_destroyed_before_fatal_closure() {
    WorkerCase::unreadable_bootstrap(CapacityReadFault::Held, true).await;
}

// The requested caller-operation budget is 1,024. This is a behavioral target,
// not a mirror of an implementation constant. Native storage remains real.
struct AdmissionGate {
    key: StateKey,
    caller: (ThreadId, Option<tokio::task::Id>),
    hold_limit: usize,
    exempt: Mutex<Option<(ThreadId, Option<tokio::task::Id>)>>,
    entered: AtomicUsize,
    departed: AtomicUsize,
    released: AtomicBool,
    changed: Notify,
    release_signal: Notify,
}

impl AdmissionGate {
    fn new(workspace: &WorkspaceId, hold_limit: usize) -> Arc<Self> {
        Arc::new(Self {
            key: StateKey::new("app-sdk", format!("workspace/{workspace}")).unwrap(),
            caller: (std::thread::current().id(), tokio::task::try_id()),
            hold_limit,
            exempt: Mutex::new(None),
            entered: AtomicUsize::new(0),
            departed: AtomicUsize::new(0),
            released: AtomicBool::new(false),
            changed: Notify::new(),
            release_signal: Notify::new(),
        })
    }

    async fn hold(&self) {
        let index = self.entered.fetch_add(1, Ordering::SeqCst) + 1;
        self.changed.notify_waiters();
        if index > self.hold_limit {
            return;
        }
        loop {
            let changed = self.release_signal.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.released.load(Ordering::SeqCst) {
                return;
            }
            changed.await;
        }
    }

    async fn entered(&self, count: usize) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.entered.load(Ordering::SeqCst) >= count {
                return;
            }
            changed.await;
        }
    }

    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.release_signal.notify_waiters();
    }

    async fn rejects<T>(
        &self,
        request: impl Future<Output = Result<T, AppError>>,
    ) -> Option<String> {
        let next = self.entered.load(Ordering::SeqCst) + 1;
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                result = request => match result {
                    Err(AppError::Busy(_)) => None,
                    Err(error) => Some(format!("returned {error} instead of Busy")),
                    Ok(_) => Some("accepted beyond caller-operation capacity".into()),
                },
                _ = self.entered(next) => Some("entered lower storage beyond caller-operation capacity".into()),
            }
        }).await.unwrap_or_else(|_| Some("did not reject within the entry guard".into()))
    }
}

impl WorkerCase {
    async fn admission_matrix(&mut self, drop_observers: bool) {
        let workflow = self
            .workspace
            .workflow("worker-state".parse().unwrap())
            .await
            .unwrap();
        let mut executable = workflow.prepare(WorkflowState::default()).await.unwrap();
        let terminal = workflow.start(ExecutionOptions::default()).await.unwrap();
        let completed = terminal.wait().await.unwrap();
        tokio::time::timeout(
            Duration::from_secs(10),
            self.evidence.retention_applied.notified(),
        )
        .await
        .expect("the setup terminal run releases its actual durable capacity");
        let mut deletable = workflow
            .open_run(RunSelector::Latest(terminal.id().clone()))
            .await
            .unwrap();
        let parked = self.start("worker-input").await;
        let pending = parked.wait().await.unwrap();
        let [
            Interaction::Input {
                sequence,
                generation,
                ..
            },
        ] = pending.interactions.as_slice()
        else {
            panic!("real input control is ready before saturation");
        };
        let cancellable = self.start("worker-input").await;
        let cancel_at = cancellable.wait().await.unwrap().checkpoint;
        let spare = self
            .app
            .create_workspace(WorkspaceConfig {
                name: Some("admission-spare".into()),
                root: std::env::current_dir().unwrap(),
            })
            .await
            .unwrap();
        let spare_revision = spare.snapshot().await.unwrap().revision;
        let mut deletable_workspace = self
            .app
            .create_workspace(WorkspaceConfig {
                name: Some("admission-delete-spare".into()),
                root: std::env::current_dir().unwrap(),
            })
            .await
            .unwrap();
        let delete_revision = deletable_workspace.snapshot().await.unwrap().revision;
        let gate = AdmissionGate::new(self.workspace.id(), usize::MAX);
        *self.evidence.admission_burst.lock().unwrap() = Some(Arc::clone(&gate));
        let mut requests = FuturesUnordered::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let (handoff, handoff_complete) = oneshot::channel();
        let dispatch = tracing::Dispatch::new(WorkerObservation::Handoff(Some(handoff)));
        let data_before = self.evidence.data_created.load(Ordering::SeqCst);
        let capacity_key = StateKey::new("app-sdk", "app").unwrap();
        let capacity_before = self
            .store
            .load(capacity_key.clone())
            .await
            .unwrap()
            .unwrap()
            .record
            .value["run_count"]
            .clone();
        let checked = AssertUnwindSafe(tokio::time::timeout_at(deadline, async {
            for _ in 0..1_023 { requests.push(workflow.prepare(WorkflowState::default())); }
            tokio::time::timeout(Duration::from_secs(10), async {
                tokio::select! {
                    _ = gate.entered(1_023) => {},
                    result = requests.next() => panic!("admission below capacity completed before its gate: {}", result.unwrap().is_ok()),
                }
            }).await.expect("1,023 real pre-capacity reads enter");
            requests.push(workflow.prepare(WorkflowState::default()));
            tokio::time::timeout(Duration::from_secs(10), async {
                tokio::select! {
                    _ = gate.entered(1_024) => {},
                    result = requests.next() => panic!("the 1,024th admission rejected or escaped its gate: {}", result.unwrap().is_ok()),
                }
            }).await.expect("the last available caller-operation slot is usable");
            assert_eq!(gate.entered.load(Ordering::SeqCst), 1_024);
            assert_eq!(self.evidence.data_created.load(Ordering::SeqCst), data_before,
                "held pre-capacity admission never constructs caller state");
            assert_eq!(self.store.load(capacity_key).await.unwrap().unwrap().record.value["run_count"], capacity_before,
                "pending caller ownership is counted before any durable run capacity exists");
            if drop_observers { requests.clear(); }
            let mut failures = Vec::new();
            for (name, result) in [
                ("prepare", gate.rejects(workflow.prepare(WorkflowState::default())).await),
                ("start", gate.rejects(workflow.start(ExecutionOptions::default())).await),
                ("restore", gate.rejects(workflow.restore(RestoreOptions {
                    source: RunSelector::Latest(terminal.id().clone()),
                    checkpoint: CheckpointSelector::Latest, overrides: WorkflowState::default(),
                    position: ResumePosition::Entrypoint, fallback: None,
                })).await),
                ("resolve", gate.rejects(parked.resolve(*sequence, *generation, Resolution::Input(json!(true)))).await),
                ("cancel", gate.rejects(cancellable.cancel(cancel_at)).await),
                ("delete", gate.rejects(deletable.delete(completed.checkpoint)).await),
                ("execution", gate.rejects(executable.execution(ExecutionOptions::default())).await),
                ("create_workspace", gate.rejects(self.app.create_workspace(WorkspaceConfig {
                    name: Some("overflow".into()), root: std::env::current_dir().unwrap(),
                })).await),
                ("replace_workspace", gate.rejects(spare.replace(spare_revision, WorkspaceConfig {
                    name: Some("overflow-replacement".into()), root: std::env::current_dir().unwrap(),
                })).await),
                ("delete_workspace", gate.rejects(deletable_workspace.delete(delete_revision)).await),
            ] {
                if let Some(failure) = result { failures.push(format!("{name}: {failure}")); }
            }
            if self.evidence.data_created.load(Ordering::SeqCst) != data_before {
                failures.push("overflow constructed caller state".into());
            }
            assert!(failures.is_empty(), "caller-operation capacity violations: {}", failures.join("; "));
        }).with_subscriber(dispatch)).catch_unwind().await;
        requests.clear();
        gate.release();
        *self.evidence.admission_burst.lock().unwrap() = None;
        let reconciled = AssertUnwindSafe(async {
            tokio::time::timeout_at(deadline, handoff_complete).await
                .expect("all admitted task completion callbacks finish within the original 60-second guard")
                .expect("the operation-scoped dispatcher reaches its final owner");
            assert_eq!(gate.departed.load(Ordering::SeqCst), gate.entered.load(Ordering::SeqCst));
            let fresh = tokio::time::timeout_at(deadline, workflow.prepare(WorkflowState::default())).await
                .expect("capacity is reusable while this App remains alive")
                .expect("supervised completion releases caller admission");
            assert!(matches!(fresh.snapshot().await.unwrap().status, RunStatus::Paused));
            drop(fresh);
        }).catch_unwind().await;
        let closed = tokio::time::timeout_at(
            deadline,
            self.app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(30),
            }),
        )
        .await;
        closed
            .expect("saturated work cleanup remains inside the 60-second total guard")
            .expect("saturated accepted work and internal cleanup join");
        if let Err(panic) = reconciled {
            std::panic::resume_unwind(panic);
        }
        match checked {
            Ok(result) => result.expect("the new capacity matrix has a 60-second total guard"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
}

#[tokio::test]
async fn admission_capacity_bounds_all_foreground_entrypoints_before_storage() {
    let mut case = WorkerCase::open().await;
    case.admission_matrix(false).await;
}

#[tokio::test]
async fn admission_capacity_is_retained_after_all_caller_observers_are_dropped() {
    let mut case = WorkerCase::open().await;
    case.admission_matrix(true).await;
}

#[test]
fn admission_capacity_bounds_sync_execution_outside_the_runtime() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut case = runtime.block_on(WorkerCase::open());
    let workflow = runtime
        .block_on(case.workspace.workflow("worker-state".parse().unwrap()))
        .unwrap();
    let mut run = runtime
        .block_on(workflow.prepare(WorkflowState::default()))
        .unwrap();
    let gate = runtime.block_on(async { AdmissionGate::new(case.workspace.id(), usize::MAX) });
    *case.evidence.admission_burst.lock().unwrap() = Some(Arc::clone(&gate));
    let mut requests = FuturesUnordered::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let checked = std::panic::catch_unwind(AssertUnwindSafe(|| {
        runtime.block_on(async {
            for _ in 0..1_024 { requests.push(workflow.prepare(WorkflowState::default())); }
            tokio::time::timeout(Duration::from_secs(10), async {
                tokio::select! {
                    _ = gate.entered(1_024) => {},
                    result = requests.next() => panic!("admission escaped before sync probe: {}", result.unwrap().is_ok()),
                }
            }).await.expect("all 1,024 accepted callers reach storage");
        });
        assert!(
            matches!(
                SyncExecution::open(&mut run, ExecutionOptions::default()),
                Err(AppError::Busy(_))
            ),
            "sync execution must reject global caller saturation before acquiring the run"
        );
    }));
    requests.clear();
    gate.release();
    *case.evidence.admission_burst.lock().unwrap() = None;
    runtime.block_on(async {
        tokio::time::timeout_at(
            deadline,
            case.app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(30),
            }),
        )
        .await
        .expect("sync saturation cleanup is bounded")
        .unwrap();
    });
    if let Err(panic) = checked {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn admission_capacity_preserves_exact_workspace_reattachment() {
    let mut case = WorkerCase::open_with_agents(1).await;
    let workflow = case
        .workspace
        .workflow("worker-state".parse().unwrap())
        .await
        .unwrap();
    let revision = case.workspace.snapshot().await.unwrap().revision;
    let mut reattacher = case
        .app
        .workspace(case.workspace.id().clone())
        .await
        .unwrap();
    let mut stale = case
        .app
        .workspace(case.workspace.id().clone())
        .await
        .unwrap();
    let race = Arc::new(WorkspaceRace::default());
    *race.key.lock().unwrap() =
        Some(StateKey::new("app-sdk", format!("workspace/{}", case.workspace.id())).unwrap());
    race.lower_armed.store(true, Ordering::SeqCst);
    *case.evidence.workspace_race.lock().unwrap() = Some(Arc::clone(&race));
    let gate = AdmissionGate::new(case.workspace.id(), usize::MAX);
    let mut original = Box::pin(case.workspace.delete(revision));
    let mut observer = Box::pin(reattacher.delete(revision));
    let mut requests = FuturesUnordered::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let checked = AssertUnwindSafe(tokio::time::timeout_at(deadline - Duration::from_secs(15), async {
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                _ = race.lower_entered.notified() => {},
                result = &mut original => panic!("deletion escaped its actual lower operation: {}", result.is_ok()),
            }
        }).await.expect("original deletion is owned before saturation");
        *gate.exempt.lock().unwrap() = *race.lower_owner.lock().unwrap();
        assert!(gate.exempt.lock().unwrap().is_some(), "the original lower operation identifies its actual caller");
        *case.evidence.admission_burst.lock().unwrap() = Some(Arc::clone(&gate));
        for _ in 0..1_023 { requests.push(workflow.prepare(WorkflowState::default())); }
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                _ = gate.entered(1_023) => {},
                result = requests.next() => panic!("admission escaped before workspace reattachment: {}", result.unwrap().is_ok()),
            }
        }).await.expect("one workspace plus 1,023 run owners fill caller capacity");
        let overflow = gate.rejects(workflow.prepare(WorkflowState::default())).await;
        assert!(matches!(stale.delete(revision + 1).await, Err(AppError::Conflict(_))),
            "the existing stale workspace fence remains Conflict at caller capacity");
        race.capture_armed.store(true, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                _ = race.capture_entered.notified() => {},
                result = &mut observer => panic!("same-fence observation did not reach the actual Deleting record: {result:?}"),
            }
        }).await.expect("reattachment must observe the existing native deletion before it finishes");
        race.capture_release.notify_one();
        assert!(observer.as_mut().now_or_never().is_none(),
            "same-fence observation remains attached instead of rejecting at full capacity");
        assert_eq!(race.lower_calls.load(Ordering::SeqCst), 1);
        race.lower_release.notify_one();
        (&mut original).await.unwrap();
        (&mut observer).await.unwrap();
        assert_eq!(race.lower_calls.load(Ordering::SeqCst), 1, "reattachment never repeats a lower deletion");
        assert!(case.evidence.lower_workspaces.lock().unwrap()[0].is_none());
        assert!(overflow.is_none(), "a new owner must reject: {overflow:?}");
    })).catch_unwind().await;
    drop(original);
    drop(observer);
    requests.clear();
    race.release();
    gate.release();
    *case.evidence.admission_burst.lock().unwrap() = None;
    let closed = tokio::time::timeout_at(
        deadline,
        case.app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(30),
        }),
    )
    .await
    .expect("new workload cleanup stays within its original 60-second total guard");
    closed.expect("deletion, admitted run tasks and lower shutdown all join");
    match checked {
        Ok(result) => result.expect("workspace reattachment observation is bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

#[tokio::test]
async fn admission_capacity_recharges_workspace_reattachment_after_owner_completion() {
    let mut case = WorkerCase::open_with_agents(1).await;
    let workflow = case
        .workspace
        .workflow("worker-state".parse().unwrap())
        .await
        .unwrap();
    let revision = case.workspace.snapshot().await.unwrap().revision;
    let mut reattacher = case
        .app
        .workspace(case.workspace.id().clone())
        .await
        .unwrap();
    let race = Arc::new(WorkspaceRace::default());
    let key = StateKey::new("app-sdk", format!("workspace/{}", case.workspace.id())).unwrap();
    *race.key.lock().unwrap() = Some(key.clone());
    race.lower_armed.store(true, Ordering::SeqCst);
    *case.evidence.workspace_race.lock().unwrap() = Some(Arc::clone(&race));
    let gate = AdmissionGate::new(case.workspace.id(), usize::MAX);
    let (handoff, handoff_complete) = oneshot::channel();
    let dispatch = tracing::Dispatch::new(WorkerObservation::Handoff(Some(handoff)));
    let mut original = Box::pin(case.workspace.delete(revision).with_subscriber(dispatch));
    let mut observer = Box::pin(reattacher.delete(revision));
    let mut requests = FuturesUnordered::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let checked = AssertUnwindSafe(tokio::time::timeout_at(deadline - Duration::from_secs(15), async {
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                _ = race.lower_entered.notified() => {},
                result = &mut original => panic!("original deletion escaped its native lower gate: {result:?}"),
            }
        }).await.expect("original deletion owns one caller reservation");
        race.capture_armed.store(true, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                _ = race.capture_entered.notified() => {},
                result = &mut observer => panic!("reattachment did not capture the actual Deleting row: {result:?}"),
            }
        }).await.expect("same-fence reattachment sees the original registered owner");
        *gate.exempt.lock().unwrap() = *race.lower_owner.lock().unwrap();
        assert!(gate.exempt.lock().unwrap().is_some());
        *case.evidence.admission_burst.lock().unwrap() = Some(Arc::clone(&gate));
        for _ in 0..1_023 { requests.push(workflow.prepare(WorkflowState::default())); }
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                _ = gate.entered(1_023) => {},
                result = requests.next() => panic!("admission escaped before the original owner completed: {}", result.unwrap().is_ok()),
            }
        }).await.expect("the original deletion and 1,023 pending admissions fill capacity");
        race.lower_release.notify_one();
        original.await.expect("original deletion completes its actual durable removal");
        tokio::time::timeout(Duration::from_secs(10), handoff_complete).await
            .expect("original deletion supervision finishes before replacing its reservation")
            .expect("the original operation dispatcher loses its final owner");
        assert!(case.store.load(key).await.unwrap().is_none());
        requests.push(workflow.prepare(WorkflowState::default()));
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                _ = gate.entered(1_024) => {},
                result = requests.next() => panic!("the original deletion's released slot was not reusable: {}", result.unwrap().is_ok()),
            }
        }).await.expect("a new caller occupies the original deletion's released reservation");
        race.capture_release.notify_one();
        let failure = gate.rejects(&mut observer).await;
        assert!(failure.is_none(), "a replacement deletion must reserve before its new owned work: {failure:?}");
        assert_eq!(gate.entered.load(Ordering::SeqCst), 1_024);
        assert_eq!(race.lower_calls.load(Ordering::SeqCst), 1);
    })).catch_unwind().await;
    drop(observer);
    requests.clear();
    race.release();
    gate.release();
    *case.evidence.admission_burst.lock().unwrap() = None;
    let closed = tokio::time::timeout_at(
        deadline,
        case.app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(30),
        }),
    )
    .await
    .expect("reattachment race cleanup remains within the original 60-second guard");
    closed.expect("all accepted work and lower shutdown join after releasing the gates");
    match checked {
        Ok(result) => result.expect("workspace reattachment capacity race is bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

#[tokio::test]
async fn admission_cleanup_survives_unpolled_and_rejected_callers_without_poisoning_app() {
    let mut case = WorkerCase::open().await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let checked = AssertUnwindSafe(tokio::time::timeout_at(deadline, async {
        let workflow = case
            .workspace
            .workflow("worker-state".parse().unwrap())
            .await
            .unwrap();
        let before = case.evidence.data_created.load(Ordering::SeqCst);
        for _ in 0..1_024 {
            drop(workflow.prepare(WorkflowState::default()));
        }
        assert_eq!(
            case.evidence.data_created.load(Ordering::SeqCst),
            before,
            "unpolled callers acquire no state or operation ownership"
        );
        let revision = case.workspace.snapshot().await.unwrap().revision;
        case.workspace
            .replace(
                revision,
                WorkspaceConfig {
                    name: Some("reject-stale-admission".into()),
                    root: std::env::current_dir().unwrap(),
                },
            )
            .await
            .unwrap();
        for _ in 0..1_024 {
            assert!(
                matches!(
                    workflow.prepare(WorkflowState::default()).await,
                    Err(AppError::Conflict(_))
                ),
                "stale captured workflow rejects after owned admission acquisition"
            );
        }
        let fresh = case
            .workspace
            .workflow("worker-state".parse().unwrap())
            .await
            .unwrap();
        let run = fresh
            .start(ExecutionOptions::default())
            .await
            .expect("1,024 rejected callers do not exhaust subsequent admission");
        assert!(matches!(
            run.wait().await.unwrap().status,
            RunStatus::Succeeded
        ));
    }))
    .catch_unwind()
    .await;
    let closed = tokio::time::timeout_at(
        deadline,
        case.app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(30),
        }),
    )
    .await;
    closed
        .expect("rejected-caller cleanup stays within its total bound")
        .unwrap();
    match checked {
        Ok(result) => result.expect("unpolled and rejected caller workload is bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

#[tokio::test]
async fn admission_cleanup_does_not_charge_lazy_execution_lifetimes() {
    let mut case = WorkerCase::open().await;
    let workflow = case
        .workspace
        .workflow("worker-capacity-input".parse().unwrap())
        .await
        .unwrap();
    let independent = case
        .workspace
        .workflow("worker-state".parse().unwrap())
        .await
        .unwrap();
    let started = tokio::time::Instant::now();
    let mut phase = "prepare";
    let mut prepared = 0;
    let mut parked = 0;
    let checked = AssertUnwindSafe(async {
        // Setup and observation guards do not change the workflow's existing
        // finite default deadline. Ordinary thirty-second Inputs expire before
        // this deliberately complete capacity population can become simultaneous.
        let mut runs = tokio::time::timeout(Duration::from_secs(600), async {
            let mut runs = Vec::with_capacity(1_024);
            for _ in 0..1_024 {
                runs.push(workflow.prepare(WorkflowState::default()).await.unwrap());
                prepared = runs.len();
            }
            runs
        })
        .await
        .expect("all 1,024 real run preparations finish within the setup guard");
        phase = "park Inputs";
        tokio::time::timeout(Duration::from_secs(7_200), async {
            let mut executions = Vec::with_capacity(1_024);
            let mut observers = Vec::with_capacity(1_024);
            for run in &mut runs {
                let observer = workflow
                    .open_run(RunSelector::Latest(run.id().clone()))
                    .await
                    .unwrap();
                let mut execution = run.execution(ExecutionOptions::default()).await.unwrap();
                assert!(
                    execution.next().await.unwrap().is_none(),
                    "the native Input parks the returned execution"
                );
                assert!(
                    matches!(
                        observer.snapshot().await.unwrap().interactions.as_slice(),
                        [Interaction::Input { .. }]
                    ),
                    "each retained owner waits on its real Input"
                );
                executions.push(execution);
                observers.push(observer);
                parked = executions.len();
                if parked % 128 == 0 {
                    eprintln!("lazy execution workload: parked={parked} elapsed_ms={}", started.elapsed().as_millis());
                }
            }
            phase = "validate simultaneous Inputs before progress";
            for (index, observer) in observers.iter().enumerate() {
                let snapshot = observer.snapshot().await.unwrap();
                assert!(
                    matches!(snapshot.status, RunStatus::Paused),
                    "retained Input owner {index} remains parked: checkpoint={} status={:?}",
                    snapshot.checkpoint,
                    std::mem::discriminant(&snapshot.status),
                );
                assert!(matches!(snapshot.interactions.as_slice(), [Interaction::Input { .. }]),
                    "all 1,024 retained owners remain on their original Input before independent progress");
            }
            assert_eq!(executions.len(), 1_024);
            assert_eq!(observers.len(), 1_024);
            phase = "independent progress";
            let next = independent
                .start(ExecutionOptions::default())
                .await
                .expect("returned Input-waiting executions release caller-admission slots");
            assert!(
                matches!(next.wait().await.unwrap().status, RunStatus::Succeeded),
                "independent execution progresses while all 1,024 Input owners remain alive"
            );
            phase = "revalidate simultaneous Inputs";
            for (index, observer) in observers.iter().enumerate() {
                let snapshot = observer.snapshot().await.unwrap();
                assert!(
                    matches!(snapshot.status, RunStatus::Paused),
                    "retained Input owner {index} remains parked: checkpoint={} status={:?}",
                    snapshot.checkpoint,
                    std::mem::discriminant(&snapshot.status),
                );
                assert!(matches!(snapshot.interactions.as_slice(), [Interaction::Input { .. }]),
                    "all 1,024 retained owners remain on their original Input after independent progress");
            }
            assert_eq!(executions.len(), 1_024);
            assert_eq!(observers.len(), 1_024);
            drop(next);
            drop(executions);
            phase = "body complete";
        })
        .await
    })
    .catch_unwind()
    .await;
    eprintln!(
        "lazy execution workload: phase={phase} prepared={prepared} parked={parked} elapsed_ms={} observation_timed_out={} panicked={}",
        started.elapsed().as_millis(),
        matches!(&checked, Ok(Err(_))),
        checked.is_err(),
    );
    let closed = tokio::time::timeout(
        Duration::from_secs(660),
        case.app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(600),
        }),
    )
    .await;
    if !matches!(&closed, Ok(Ok(()))) {
        eprintln!("lazy execution secondary cleanup result: {closed:?}");
    }
    // Only these CancelAndDrain outcomes establish completed owned joining.
    // Unexpected errors retain the App/runtime under the outer supervisor.
    if !matches!(&closed, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
        let mut observer = 1;
        loop {
            // The first receipt remains a failed budget assertion. Reattach to
            // its accepted finalizer on this runtime before reporting it.
            let joined = AssertUnwindSafe(case.app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(600),
            }))
            .catch_unwind()
            .await;
            eprintln!(
                "lazy execution cleanup observer={observer} elapsed_ms={} result={joined:?}",
                started.elapsed().as_millis()
            );
            // This fixture only requests valid CancelAndDrain; Integration is
            // a retained finalizer failure delivered after owned joining.
            if matches!(&joined, Ok(Ok(())) | Ok(Err(AppError::Integration(_)))) {
                break;
            }
            if !matches!(&joined, Ok(Err(AppError::Timeout(message)))
                if message == "application shutdown observation timed out")
            {
                // Other unexpected failures cannot establish a completed join. Keep
                // the runtime driven; the unchanged outer supervisor bounds it.
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            observer += 1;
        }
    }
    match checked {
        Ok(result) => result.expect("1,024 lazy execution owners fit the active-workload guard"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    closed
        .expect("lazy execution cleanup receives a fresh bounded observation")
        .expect("all lazy execution cleanup is joined");
}

#[tokio::test]
async fn admission_cleanup_children_and_native_publication_progress_while_callers_are_held() {
    let evidence = Arc::new(WorkerEvidence::default());
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let mut definitions = WorkerCase::definitions();
    definitions.push(WorkerAction::definition());
    definitions.push(WorkflowDefinition::parse(&json!({
        "schema":"app-sdk.workflow/v2", "id":"admission-children", "revision":"r1",
        "state":{"items":[0,0],"collected":[]}, "required":["items","collected"],
        "reducers":{"items":"reject_concurrent","collected":"collect"}, "entry":"map",
        "nodes":[{"id":"map","step":{"kind":"map", "workflow":"worker-source",
            "items":{"kind":"state","path":"items"},"input_key":"item","max_parallel":1,
            "reductions":[{"source":"item","target":"collected","reducer":"collect"}]}}], "routes":{}
    }).to_string()).unwrap());
    let config: AppConfig<WorkerStore, WorkerAgent> = AppConfig::new(
        WorkerStore {
            inner: store,
            evidence: Arc::clone(&evidence),
        },
        Vec::new(),
        definitions,
    );
    let mut app = App::open(
        config
            .with_actions(vec![WorkerAction::new(&evidence)])
            .with_state_typing(WorkerTyping(Arc::clone(&evidence))),
    )
    .await
    .unwrap();
    let workspace = app
        .create_workspace(WorkspaceConfig {
            name: None,
            root: std::env::current_dir().unwrap(),
        })
        .await
        .unwrap();
    let workflow = workspace
        .workflow("worker-state".parse().unwrap())
        .await
        .unwrap();
    let parent = workspace
        .workflow("admission-children".parse().unwrap())
        .await
        .unwrap()
        .start(ExecutionOptions::default())
        .await
        .unwrap();
    let gate = AdmissionGate::new(workspace.id(), 1_024);
    let mut requests = FuturesUnordered::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let checked = AssertUnwindSafe(tokio::time::timeout_at(deadline, async {
        tokio::time::timeout(Duration::from_secs(10), evidence.source_pending.notified()).await
            .expect("the first actual local child source is pending before caller saturation");
        *evidence.admission_burst.lock().unwrap() = Some(Arc::clone(&gate));
        for _ in 0..1_024 { requests.push(workflow.prepare(WorkflowState::default())); }
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                _ = gate.entered(1_024) => {},
                result = requests.next() => panic!("caller escaped saturation gate: {}", result.unwrap().is_ok()),
            }
        }).await.expect("all caller slots are held before internal child work continues");
        evidence.release_source();
        let completed = tokio::time::timeout(Duration::from_secs(10), parent.wait()).await
            .expect("child callbacks and native publication progress with all caller slots occupied").unwrap();
        assert!(matches!(completed.status, RunStatus::Succeeded));
        assert_eq!(completed.state.values()["collected"], json!([1,1]));
        assert_eq!(gate.entered.load(Ordering::SeqCst) - gate.departed.load(Ordering::SeqCst), 1_024,
            "internal progress did not require releasing a caller read");
        assert!(matches!(app.shutdown(Shutdown::CancelAndDrain { timeout: Duration::from_millis(50) }).await,
            Err(AppError::Timeout(_))), "shutdown retains accepted gated callers instead of detaching them");
    })).catch_unwind().await;
    requests.clear();
    gate.release();
    evidence.release_source();
    *evidence.admission_burst.lock().unwrap() = None;
    let closed = tokio::time::timeout_at(
        deadline,
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(30),
        }),
    )
    .await;
    closed
        .expect("all owned work joins within the original total bound")
        .unwrap();
    match checked {
        Ok(result) => result.expect("internal-progress preservation is bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

// These are deterministic infrastructure controls, outside workflow_live.

enum ChildStopObservation {
    Complete,
    Cancelled,
}

impl ChildStopObservation {
    async fn verify(self) {
        use std::io::Read;

        let name = match self {
            Self::Complete => "owned_child_stop_returns_actual_exit_receipt",
            Self::Cancelled => "cancelled_child_stop_observer_retains_reap_authority",
        };
        if std::env::var("APP_SDK_CHILD_STOP_CASE").ok().as_deref() == Some(name) {
            let ready = std::env::var_os("APP_SDK_CHILD_STOP_READY")
                .expect("the owned child receives its exact ready path");
            std::fs::write(ready, b"ready").expect("the actual child publishes readiness");
            let mut input = [0];
            std::io::stdin()
                .read_exact(&mut input)
                .expect("the parent retains stdin until real termination");
            panic!("the parent must terminate the owned child instead of sending input");
        }

        let root = std::env::temp_dir().join(format!("app-owned-child-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).expect("the unique child directory must be created");
        let ready = root.join("ready");
        let process = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", name, "--nocapture", "--test-threads=1"])
            .env("APP_SDK_CHILD_STOP_CASE", name)
            .env("APP_SDK_CHILD_STOP_READY", &ready)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::inherit())
            .spawn();
        let mut child = match process {
            Ok(child) => owned_child::OwnedChild::new(child),
            Err(error) => {
                if let Err(cleanup) = std::fs::remove_dir_all(&root) {
                    eprintln!("failed child startup directory cleanup: {cleanup}");
                }
                panic!("the real child must start: {error}");
            }
        };
        let observed = AssertUnwindSafe(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                while !ready.exists() {
                    assert!(
                        child.try_wait().unwrap().is_none(),
                        "child exited before readiness"
                    );
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the actual child reaches its stdin wait");
            assert!(child.try_wait().unwrap().is_none());
            if matches!(self, Self::Cancelled) {
                let stop = child.stop();
                tokio::pin!(stop);
                let pending = std::future::poll_fn(|cx| {
                    Poll::Ready(matches!(stop.as_mut().poll(cx), Poll::Pending))
                })
                .await;
                assert!(
                    pending,
                    "the real termination was signaled before observer cancellation"
                );
                // The future is dropped here. The child owner remains outside it.
            }
            let status = tokio::time::timeout(Duration::from_secs(12), child.stop())
                .await
                .expect("the same owned child must finish reaping")
                .expect("the same owned child returns an actual exit receipt");
            assert!(
                !status.success(),
                "this child was terminated while waiting for stdin"
            );
        })
        .catch_unwind()
        .await;
        let cleanup = tokio::time::timeout(Duration::from_secs(12), child.stop()).await;
        if !matches!(&cleanup, Ok(Ok(_))) {
            eprintln!("owned child independent cleanup: {cleanup:?}");
        }
        let reaped = child.settle_on_drop();
        drop(child);
        let paths = if reaped {
            Some(std::fs::remove_dir_all(&root))
        } else {
            eprintln!("owned child directory retained because reaping is unconfirmed");
            None
        };
        if let Some(Err(error)) = &paths {
            eprintln!("owned child directory cleanup failed: {error}");
        }
        if let Err(panic) = observed {
            std::panic::resume_unwind(panic);
        }
        cleanup
            .expect("independent child cleanup is bounded")
            .expect("independent child cleanup reaps");
        assert!(reaped, "path deletion requires actual child reaping");
        paths
            .expect("confirmed reaping admits path cleanup")
            .expect("the owned path is removed");
        assert!(!root.exists());
    }
}

#[tokio::test]
async fn owned_child_stop_returns_actual_exit_receipt() {
    ChildStopObservation::Complete.verify().await;
}

#[tokio::test]
async fn cancelled_child_stop_observer_retains_reap_authority() {
    ChildStopObservation::Cancelled.verify().await;
}
