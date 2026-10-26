//! Public action integration, using a caller-owned local stream and real memory
//! transactions. No Agent value is constructed or invoked by these workflows.
//! Source-derived oracle: rurr-core action/function.rs:34–76, :858–891 and
//! application/streaming/step_core.rs:401–420, :495–501 in the fixed archive.

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use agent_sdk::Agent;
use app_sdk::workflow::{
    ActionBackend, ActionDeclaration, ActionEvent, ActionMode, ActionRequest, ConditionBackend,
    ConditionDeclaration, ExecutionEvent, ExecutionOptions, Interaction, Resolution, RunSelector,
    RunSnapshot, RunStatus, StateChange, SyncExecution, Workflow, WorkflowDefinition,
    WorkflowFailure, WorkflowRun, WorkflowState,
};
use app_sdk::{AgentSpec, App, AppConfig, AppError, Shutdown, WorkspaceConfig};
use futures_util::{FutureExt, Stream};
use serde_json::{Map, Value, json};
use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_sdk::state::record::{
    StateKey, StateListRequest, StateListResponse, StateRecord, StateRecordSnapshot,
};
use storage_sdk::{
    StateStore, StateStoreBackend, StateStoreError, StateTransaction, StateTransactionBackend,
};
use tokio::sync::Notify;
use tracing::instrument::WithSubscriber;

pub mod observability_support;
use observability_support::InteractionCapture;

#[derive(Default)]
struct Observation {
    constructions: AtomicUsize,
    polls: AtomicUsize,
    drops: AtomicUsize,
    external_effects: AtomicUsize,
    condition_calls: AtomicUsize,
    calls: Mutex<Vec<Invocation>>,
    pending_entered: Notify,
    source_dropped: Notify,
}

struct Invocation {
    node: String,
    ordinal: u64,
    inputs: Map<String, Value>,
    state: Map<String, Value>,
    thread: std::thread::ThreadId,
    asynchronous: bool,
}

#[derive(Clone, Copy)]
enum Behavior {
    Increment,
    MissingTerminal,
    Error,
    PanicStart,
    PanicPoll,
    PanicAfterPartial,
    PanicDrop,
    PanicDropEof,
    PendingAfterPartial,
    InvalidMutation,
    InvalidPartial,
    AdditionalWrite,
}

struct CallerAction {
    declaration: ActionDeclaration,
    observation: Arc<Observation>,
    behavior: Behavior,
}

struct CallerCondition {
    declaration: ConditionDeclaration,
    observation: Arc<Observation>,
}

struct LocalSource {
    local: Rc<usize>,
    observation: Arc<Observation>,
    request: Option<ActionRequest>,
    events: VecDeque<Result<ActionEvent, AppError>>,
    behavior: Behavior,
    yielded: bool,
}

#[derive(Clone, Copy)]
enum PublicationBoundary {
    Acquisition { retry: bool },
    ChildPark,
    ChildAdmission,
    Stage,
    Barrier,
    Rollback,
    Cancellation,
}

#[derive(Clone, Copy)]
enum PublicationPoint {
    BeforeCommit,
    AppliedReceipt,
    Rejected,
    FollowingRead,
    ChildPreparationRead,
}

struct PublicationGate {
    selection: Mutex<(PublicationBoundary, PublicationPoint)>,
    receipt_returned: Notify,
    armed: AtomicBool,
    released: AtomicBool,
    entered: Notify,
    release: Notify,
    abandoned: AtomicUsize,
    read_key: Mutex<Option<StateKey>>,
}

struct PublicationStore {
    inner: StateStore,
    gate: Arc<PublicationGate>,
}

struct PublicationTransaction {
    inner: Option<StateTransaction>,
    gate: Arc<PublicationGate>,
    selected: bool,
    following_read: Option<StateKey>,
    held: bool,
}

struct PublicationCase {
    app: App<PublicationStore, Agent, CallerAction>,
    workflow: Workflow<PublicationStore, Agent, CallerAction>,
    run: WorkflowRun<PublicationStore, Agent, CallerAction>,
    gate: Arc<PublicationGate>,
    observation: Arc<Observation>,
    store: StateStore,
}

impl PublicationCase {
    fn config<S: StateStoreBackend>(
        store: S,
        observation: &Arc<Observation>,
    ) -> AppConfig<S, Agent, CallerAction> {
        AppConfig::new(
            store,
            Vec::new(),
            vec![CallerAction::definition(&["first", "second"])],
        )
        .with_actions(vec![
            CallerAction::new("first", observation, Behavior::Increment),
            CallerAction::new("second", observation, Behavior::Increment),
        ])
    }

    async fn new(boundary: PublicationBoundary, point: PublicationPoint) -> Self {
        let store = CallerAction::store().await;
        let gate = PublicationGate::new(boundary, point);
        if matches!(boundary, PublicationBoundary::Acquisition { .. }) {
            gate.armed.store(false, Ordering::SeqCst);
        }
        let observation = Arc::new(Observation::default());
        let wrapped = PublicationStore {
            inner: store.clone(),
            gate: Arc::clone(&gate),
        };
        let config = if let PublicationBoundary::Acquisition { retry } = boundary {
            AppConfig::new(
                wrapped,
                Vec::new(),
                vec![CallerAction::definition(&["first", "second"])],
            )
            .with_actions(vec![
                CallerAction::new(
                    "first",
                    &observation,
                    if retry {
                        Behavior::Error
                    } else {
                        Behavior::Increment
                    },
                ),
                CallerAction::new("second", &observation, Behavior::Increment),
            ])
        } else if matches!(boundary, PublicationBoundary::Cancellation) {
            AppConfig::new(
                wrapped,
                Vec::new(),
                vec![CallerAction::definition_with_deadline(
                    &["first"],
                    Some(500),
                )],
            )
            .with_actions(vec![CallerAction::new(
                "first",
                &observation,
                Behavior::PendingAfterPartial,
            )])
        } else {
            Self::config(wrapped, &observation)
        };
        let app = App::open(config)
            .await
            .expect("bind actions to real gated transactions");
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().unwrap(),
            })
            .await
            .expect("publication workspace");
        let workflow = workspace
            .workflow("callable".parse().unwrap())
            .await
            .unwrap();
        let run = workflow.prepare(WorkflowState::default()).await.unwrap();
        Self {
            app,
            workflow,
            run,
            gate,
            observation,
            store,
        }
    }

    async fn finish(
        &mut self,
        observed: Result<(), Box<dyn std::any::Any + Send>>,
        superseded: bool,
    ) {
        self.gate.release();
        let closed = tokio::time::timeout(
            Duration::from_secs(5),
            self.app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        )
        .await;
        if let Err(failure) = observed {
            std::panic::resume_unwind(failure);
        }
        match closed {
            Ok(Ok(())) => {}
            Ok(Err(AppError::Closed(_))) if superseded => {}
            other => panic!("publication fixture must join its App owner: {other:?}"),
        }
    }
}

impl PublicationGate {
    fn new(boundary: PublicationBoundary, point: PublicationPoint) -> Arc<Self> {
        Arc::new(Self {
            selection: Mutex::new((boundary, point)),
            receipt_returned: Notify::new(),
            armed: AtomicBool::new(true),
            released: AtomicBool::new(false),
            entered: Notify::new(),
            release: Notify::new(),
            abandoned: AtomicUsize::new(0),
            read_key: Mutex::new(None),
        })
    }

    fn checkpoint_scope(record: &StateRecord) -> Option<(String, u64)> {
        let workspace = record.value.get("workspace_id")?.as_str()?;
        let workflow = record.value.get("workflow_id")?.as_str()?;
        let run = record.value.get("id")?.as_str()?;
        let sequence = record.value.pointer("/checkpoint/sequence")?.as_u64()?;
        let occurrence = record.value.get("occurrence")?.as_str()?;
        let scope = format!("{workspace}/{workflow}/{run}/{occurrence}");
        (record.key.key() == format!("history/{scope}/checkpoint/{sequence:020}"))
            .then_some((scope, sequence))
    }

    fn selects(&self, record: &StateRecord, previous: Option<&Value>) -> bool {
        let Some(checkpoint) = record.value.get("checkpoint") else {
            return false;
        };
        match self.selection.lock().unwrap().0 {
            PublicationBoundary::Acquisition { .. } => {
                checkpoint.pointer("/admission/kind") == Some(&json!("prepared"))
                    && checkpoint
                        .get("sequence")
                        .and_then(Value::as_u64)
                        .is_some_and(|sequence| sequence > 0)
            }
            boundary @ (PublicationBoundary::ChildPark | PublicationBoundary::ChildAdmission) => {
                record.value.get("workflow_id") == Some(&json!("input-receipt-parent"))
                    && checkpoint
                        .get("controls")
                        .and_then(Value::as_object)
                        .is_some_and(|controls| {
                            controls.values().any(|control| {
                                control.pointer("/state/kind") == Some(&json!("child"))
                                    && control
                                        .pointer("/state/admitted")
                                        .and_then(Value::as_array)
                                        .is_some_and(|admitted| {
                                            admitted.is_empty()
                                                == matches!(
                                                    boundary,
                                                    PublicationBoundary::ChildPark
                                                )
                                        })
                            })
                        })
            }
            PublicationBoundary::Cancellation => {
                checkpoint.pointer("/phase/state") == Some(&json!("cancelling"))
            }
            PublicationBoundary::Stage => {
                checkpoint.pointer("/state/count") == Some(&json!(0))
                    && checkpoint
                        .get("staged")
                        .and_then(Value::as_array)
                        .is_some_and(|staged| {
                            staged.iter().any(|entry| {
                                entry.get("node") == Some(&json!("node0"))
                                    && entry.pointer("/outcome/state") == Some(&json!("complete"))
                            })
                        })
            }
            PublicationBoundary::Rollback => {
                checkpoint.pointer("/state/count") == Some(&json!(0))
                    && checkpoint
                        .get("staged")
                        .and_then(Value::as_array)
                        .is_some_and(Vec::is_empty)
                    && checkpoint
                        .get("outcomes")
                        .and_then(Value::as_array)
                        .is_some_and(Vec::is_empty)
                    && previous
                        .and_then(|previous| previous.pointer("/checkpoint/staged"))
                        .and_then(Value::as_array)
                        .is_some_and(|staged| !staged.is_empty())
            }
            PublicationBoundary::Barrier => {
                checkpoint.pointer("/state/count") == Some(&json!(1))
                    && checkpoint.pointer("/outcomes/0/node") == Some(&json!("node0"))
                    && checkpoint
                        .get("staged")
                        .and_then(Value::as_array)
                        .is_some_and(Vec::is_empty)
            }
        }
    }

    fn point(&self) -> PublicationPoint {
        self.selection.lock().unwrap().1
    }

    fn rearm(&self, boundary: PublicationBoundary, point: PublicationPoint) {
        *self.selection.lock().unwrap() = (boundary, point);
        self.released.store(false, Ordering::SeqCst);
        self.armed.store(true, Ordering::SeqCst);
    }

    async fn hold(&self) {
        self.entered.notify_one();
        loop {
            let notified = self.release.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.released.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }

    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.release.notify_waiters();
    }
}

impl PublicationTransaction {
    fn inner(&mut self) -> &mut StateTransaction {
        self.inner.as_mut().expect("test transaction remains owned")
    }
}

impl Drop for PublicationTransaction {
    fn drop(&mut self) {
        if self.held && !self.gate.released.load(Ordering::SeqCst) {
            self.gate.abandoned.fetch_add(1, Ordering::SeqCst);
        }
    }
}

impl StateStoreBackend for PublicationStore {
    type Transaction = PublicationTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        let selected = {
            let mut read_key = self.gate.read_key.lock().unwrap();
            if read_key.as_ref() == Some(&key) {
                read_key.take();
                true
            } else {
                false
            }
        };
        if selected {
            if matches!(self.gate.point(), PublicationPoint::ChildPreparationRead) {
                self.gate.entered.notify_one();
                return Err(StateStoreError::unavailable(
                    "load",
                    "selected child preparation read unavailable",
                ));
            }
            self.gate.hold().await;
        }
        self.inner.load(key).await
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        self.inner.list(request).await
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        Ok(PublicationTransaction {
            inner: Some(self.inner.begin_transaction().await?),
            gate: Arc::clone(&self.gate),
            selected: false,
            following_read: None,
            held: false,
        })
    }
}

impl StateTransactionBackend for PublicationTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.inner().load(key).await
    }

    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        let scope = PublicationGate::checkpoint_scope(&record);
        let rollback = matches!(
            self.gate.selection.lock().unwrap().0,
            PublicationBoundary::Rollback
        );
        let previous = if let Some((scope, sequence)) = &scope
            && rollback
            && self.gate.armed.load(Ordering::SeqCst)
            && let Some(previous) = sequence.checked_sub(1)
        {
            // The predecessor is a separate immutable row. Read it through the
            // same native transaction that will publish the rollback row.
            self.inner()
                .load(StateKey::new(
                    "app-sdk",
                    format!("history/{scope}/checkpoint/{previous:020}"),
                )?)
                .await?
        } else {
            None
        };
        let selected = scope.is_some()
            && self.gate.selects(
                &record,
                previous
                    .as_ref()
                    .map(|previous| previous.record.value.as_ref()),
            )
            && self.gate.armed.swap(false, Ordering::SeqCst);
        if selected && matches!(self.gate.point(), PublicationPoint::FollowingRead) {
            let (scope, _) = scope.expect("selected immutable checkpoint");
            self.following_read = Some(StateKey::new("app-sdk", format!("run/{scope}"))?);
        }
        if selected && matches!(self.gate.point(), PublicationPoint::ChildPreparationRead) {
            let workspace = record
                .value
                .get("workspace_id")
                .and_then(Value::as_str)
                .expect("selected real child parent workspace");
            self.following_read = Some(StateKey::new("app-sdk", format!("workspace/{workspace}"))?);
        }
        self.inner().put(record).await?;
        self.selected |= selected;
        if selected && matches!(self.gate.point(), PublicationPoint::Rejected) {
            return Err(StateStoreError::invalid_request(
                "test rejects one staged publication before commit",
            ));
        }
        if selected && matches!(self.gate.point(), PublicationPoint::BeforeCommit) {
            // This is still a nonterminal operation: native commit was not
            // invoked. A dropped transaction would roll back through real LanceDB.
            self.held = true;
            self.gate.hold().await;
            self.held = false;
        }
        Ok(())
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        self.inner().delete(key).await
    }

    async fn commit(mut self) -> Result<(), StateStoreError> {
        self.inner
            .take()
            .expect("owned native transaction")
            .commit()
            .await?;
        if self.selected
            && matches!(
                self.gate.point(),
                PublicationPoint::FollowingRead | PublicationPoint::ChildPreparationRead
            )
        {
            // Select only after the selected checkpoint is committed.
            // The held operation is the next authority read, never its mutation.
            *self.gate.read_key.lock().unwrap() = self.following_read.take();
        }
        if self.selected && matches!(self.gate.point(), PublicationPoint::AppliedReceipt) {
            // Native commit and cleanup already completed. Holding/losing this
            // receipt cannot revoke the real Store's finalization authority.
            self.held = true;
            self.gate.hold().await;
            self.held = false;
            self.gate.receipt_returned.notify_one();
            return Err(StateStoreError::backend_operation(
                "commit",
                "test obscures one applied publication receipt",
            ));
        }
        Ok(())
    }

    async fn rollback(mut self) -> Result<(), StateStoreError> {
        self.inner
            .take()
            .expect("owned native transaction")
            .rollback()
            .await
    }
}

impl CallerAction {
    fn new(key: &str, observation: &Arc<Observation>, behavior: Behavior) -> Self {
        Self {
            declaration: ActionDeclaration {
                key: key.to_owned(),
                revision: "action-r1".into(),
                reads: vec!["count".into()],
                writes: vec!["count".into()],
                required_inputs: vec!["question".into()],
                optional_inputs: vec!["format".into(), "bound".into()],
                bound_inputs: Map::from_iter([("bound".into(), json!("fixed"))]),
                default_inputs: Map::from_iter([("format".into(), json!("plain"))]),
                tags: vec!["callable".into()],
                source: Some("caller action fixture".into()),
                mode: ActionMode::Both,
            },
            observation: Arc::clone(observation),
            behavior,
        }
    }

    fn definition(keys: &[&str]) -> WorkflowDefinition {
        Self::definition_with_deadline(keys, None)
    }

    fn definition_with_deadline(keys: &[&str], deadline: Option<u64>) -> WorkflowDefinition {
        WorkflowDefinition::parse(&Self::document(keys, deadline).to_string())
            .expect("ordinary action syntax must compile before behavior binding")
    }

    fn document(keys: &[&str], deadline: Option<u64>) -> Value {
        let nodes = keys
            .iter()
            .enumerate()
            .map(|(index, key)| {
                json!({
                    "id": format!("node{index}"),
                    "step": {"kind":"action", "action":key, "revision":"action-r1"},
                    "deadline_ms": deadline,
                })
            })
            .collect::<Vec<_>>();
        let routes = (1..keys.len()).map(|index| (format!("node{}", index - 1), json!({
            "mode":"first", "branches":[{"target":{"kind":"node", "id":format!("node{index}")}}],
        }))).collect::<Map<_, _>>();
        json!({
            "schema":"app-sdk.workflow/v2", "id":"callable", "revision":"workflow-r1",
            "state":{"count":0}, "required":["count"],
            "reducers":{"count":"reject_concurrent"}, "entry":"node0", "nodes":nodes, "routes":routes,
        })
    }

    fn revision_definition(
        workflow_revision: &str,
        action_revision: &str,
        condition_revision: &str,
    ) -> WorkflowDefinition {
        let mut document = Self::document(&["first", "first"], None);
        document["revision"] = json!(workflow_revision);
        for node in document["nodes"].as_array_mut().expect("nodes") {
            node["step"]["revision"] = json!(action_revision);
        }
        document["routes"]["node0"]["branches"][0]["when"] = json!({
            "kind":"custom", "condition":"choose", "revision":condition_revision,
        });
        WorkflowDefinition::parse(&document.to_string()).expect("explicit revision selection")
    }

    fn at_revision(revision: &str, observation: &Arc<Observation>) -> Self {
        let mut action = Self::new("first", observation, Behavior::Increment);
        action.declaration.revision = revision.into();
        action
    }

    async fn store() -> StateStore {
        StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("real memory transaction store")
    }

    fn options() -> ExecutionOptions {
        ExecutionOptions {
            inputs: Map::from_iter([
                ("question".into(), json!("ephemeral secret")),
                ("ignored".into(), json!("not declared")),
            ]),
            ..ExecutionOptions::default()
        }
    }

    async fn prepared(
        observation: &Arc<Observation>,
        behavior: Behavior,
        deadline: Option<u64>,
    ) -> (
        App<StateStore, Agent, Self>,
        WorkflowRun<StateStore, Agent, Self>,
    ) {
        Self::new("first", observation, behavior)
            .prepare(deadline)
            .await
    }

    async fn prepare(
        self,
        deadline: Option<u64>,
    ) -> (
        App<StateStore, Agent, Self>,
        WorkflowRun<StateStore, Agent, Self>,
    ) {
        let config: AppConfig<StateStore, Agent> = AppConfig::new(
            Self::store().await,
            Vec::new(),
            vec![Self::definition_with_deadline(&["first"], deadline)],
        );
        let app = App::open(config.with_actions(vec![self]))
            .await
            .expect("bind local action");
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().expect("workspace directory"),
            })
            .await
            .expect("workspace");
        let workflow = workspace
            .workflow("callable".parse().expect("workflow id"))
            .await
            .expect("workflow");
        let run = workflow
            .prepare(WorkflowState::default())
            .await
            .expect("checkpoint zero");
        (app, run)
    }

    async fn close<S: StateStoreBackend, C: ConditionBackend>(
        app: &mut App<S, Agent, CallerAction, C>,
    ) {
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        })
        .await
        .expect("all accepted work must drain");
    }
}

impl CallerCondition {
    fn new(revision: &str, observation: &Arc<Observation>) -> Self {
        Self {
            declaration: ConditionDeclaration {
                key: "choose".into(),
                revision: revision.into(),
                reads: vec!["count".into()],
            },
            observation: Arc::clone(observation),
        }
    }
}

impl ConditionBackend for CallerCondition {
    fn declaration(&self) -> &ConditionDeclaration {
        &self.declaration
    }

    fn evaluate(&self, _: &WorkflowState) -> Result<bool, AppError> {
        self.observation
            .condition_calls
            .fetch_add(1, Ordering::SeqCst);
        Ok(self.declaration.revision == "condition-r1")
    }
}

impl ActionBackend for CallerAction {
    type Stream = LocalSource;

    fn declaration(&self) -> &ActionDeclaration {
        &self.declaration
    }

    fn start(&self, request: ActionRequest) -> Self::Stream {
        self.observation
            .constructions
            .fetch_add(1, Ordering::SeqCst);
        assert!(
            !matches!(self.behavior, Behavior::PanicStart),
            "caller start panic"
        );
        LocalSource {
            local: Rc::new(7),
            observation: Arc::clone(&self.observation),
            request: Some(request),
            events: VecDeque::new(),
            behavior: self.behavior,
            yielded: false,
        }
    }
}

impl Stream for LocalSource {
    type Item = Result<ActionEvent, AppError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.observation.polls.fetch_add(1, Ordering::SeqCst);
        assert!(
            !matches!(self.behavior, Behavior::PanicPoll),
            "caller poll panic"
        );
        if !self.yielded {
            self.yielded = true;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        assert_eq!(*self.local, 7, "local Rc must survive a suspended poll");
        if let Some(request) = self.request.take() {
            // This fixture-owned effect is deliberately outside workflow state.
            // App cannot reverse it or infer an acceptance receipt from it.
            self.observation
                .external_effects
                .fetch_add(1, Ordering::SeqCst);
            let next = request.state.values()["count"]
                .as_i64()
                .expect("declared read")
                + 1;
            self.observation
                .calls
                .lock()
                .expect("invocation mutex")
                .push(Invocation {
                    node: request.context.node,
                    ordinal: request.context.action_ordinal,
                    inputs: request.inputs.clone(),
                    state: request.state.values().clone(),
                    thread: std::thread::current().id(),
                    asynchronous: matches!(request.context.mode, ActionMode::Asynchronous),
                });
            let partial = if matches!(self.behavior, Behavior::InvalidPartial) {
                "x".repeat(1_048_576)
            } else {
                "part".to_owned()
            };
            self.events.push_back(Ok(ActionEvent::Partial {
                result: Map::from_iter([("delta".into(), json!(partial))]),
            }));
            match self.behavior {
                Behavior::MissingTerminal
                | Behavior::PanicDropEof
                | Behavior::PanicAfterPartial
                | Behavior::PendingAfterPartial => {}
                Behavior::Error => self
                    .events
                    .push_back(Err(AppError::Integration("caller failed".into()))),
                Behavior::Increment
                | Behavior::InvalidMutation
                | Behavior::InvalidPartial
                | Behavior::AdditionalWrite
                | Behavior::PanicStart
                | Behavior::PanicDrop
                | Behavior::PanicPoll => {
                    let mut writes = if matches!(self.behavior, Behavior::InvalidMutation) {
                        vec![StateChange::Increment {
                            values: vec![("count".into(), json!("not an integer"))],
                        }]
                    } else {
                        vec![StateChange::Assign {
                            values: Map::from_iter([("count".into(), json!(next))]),
                        }]
                    };
                    if matches!(self.behavior, Behavior::AdditionalWrite) {
                        writes.push(StateChange::Assign {
                            values: Map::from_iter([("other".into(), json!(99))]),
                        });
                    }
                    self.events.push_back(Ok(ActionEvent::Completed {
                        result: Map::from_iter([(
                            "transient".into(),
                            request.inputs["question"].clone(),
                        )]),
                        writes,
                    }));
                }
            }
        }
        if self.events.is_empty() {
            assert!(
                !matches!(self.behavior, Behavior::PanicAfterPartial),
                "panic after partial"
            );
            if matches!(self.behavior, Behavior::PendingAfterPartial) {
                self.observation.pending_entered.notify_one();
                return Poll::Pending;
            }
        }
        let next = self.events.pop_front();
        assert!(
            next.is_some()
                || matches!(
                    self.behavior,
                    Behavior::MissingTerminal | Behavior::PanicDropEof
                ),
            "the first terminal must close the source without polling its tail"
        );
        Poll::Ready(next)
    }
}

impl Drop for LocalSource {
    fn drop(&mut self) {
        self.observation.drops.fetch_add(1, Ordering::SeqCst);
        self.observation.source_dropped.notify_one();
        assert!(
            !matches!(self.behavior, Behavior::PanicDrop | Behavior::PanicDropEof),
            "caller source destructor secret"
        );
    }
}

#[tokio::test]
async fn caller_local_actions_are_lazy_and_keep_results_and_reused_inputs_separate_from_state() {
    let observation = Arc::new(Observation::default());
    let actions =
        ["first", "second"].map(|key| CallerAction::new(key, &observation, Behavior::Increment));
    let config: AppConfig<StateStore, Agent> = AppConfig::new(
        CallerAction::store().await,
        Vec::new(),
        vec![CallerAction::definition(&["first", "second"])],
    );
    let mut app = App::open(config.with_actions(Vec::from(actions)))
        .await
        .expect("bind caller actions");
    let workspace = app
        .create_workspace(WorkspaceConfig {
            name: None,
            root: std::env::current_dir().expect("workspace directory"),
        })
        .await
        .expect("state-only workspace");
    let workflow = workspace
        .workflow("callable".parse().expect("workflow id"))
        .await
        .expect("workflow");
    let mut run = workflow
        .prepare(WorkflowState::default())
        .await
        .expect("checkpoint zero");
    let observer = workflow
        .open_run(RunSelector::Latest(run.id().clone()))
        .await
        .expect("independent observer");
    let before = observer.snapshot().await.expect("initial snapshot");
    let mut execution = run
        .execution(CallerAction::options())
        .await
        .expect("exclusive local session");
    assert_eq!(observation.constructions.load(Ordering::SeqCst), 0);
    assert_eq!(observation.polls.load(Ordering::SeqCst), 0);
    let first_event = execution.next().await.expect("first partial");
    assert!(
        matches!(&first_event, Some(ExecutionEvent::Partial { node, result })
        if node == "node0" && result["delta"] == "part"),
        "expected the first source item; returned_none={}, constructions={}, polls={}",
        first_event.is_none(),
        observation.constructions.load(Ordering::SeqCst),
        observation.polls.load(Ordering::SeqCst),
    );
    let partial = observer
        .snapshot()
        .await
        .expect("committed snapshot while partial");
    assert_eq!(partial.state, before.state);
    assert!(partial.last_action_ordinal.is_none());
    assert!(partial.outcomes.is_empty());
    assert!(
        matches!(execution.step().await.expect("first commit"), Some(ExecutionEvent::Committed { outcomes, .. })
        if outcomes.len() == 1 && outcomes[0].ordinal == 0 && outcomes[0].result["transient"] == "ephemeral secret")
    );
    let first = observer.snapshot().await.expect("first committed state");
    assert_eq!(
        first.state.values(),
        &Map::from_iter([("count".into(), json!(1))])
    );
    let complete = execution.finish().await.expect("drain remaining action");
    assert!(matches!(complete.status, RunStatus::Succeeded));
    assert_eq!(
        complete.state.values(),
        &Map::from_iter([("count".into(), json!(2))])
    );
    assert_eq!(complete.last_action_ordinal, Some(1));
    assert_eq!(observation.drops.load(Ordering::SeqCst), 2);
    {
        let calls = observation.calls.lock().expect("invocation mutex");
        assert_eq!(calls.len(), 2);
        for (index, call) in calls.iter().enumerate() {
            assert_eq!(call.node, format!("node{index}"));
            assert_eq!(call.ordinal, index as u64);
            assert_eq!(call.state["count"], index);
            assert_eq!(
                call.inputs,
                Map::from_iter([
                    ("question".into(), json!("ephemeral secret")),
                    ("format".into(), json!("plain")),
                    ("bound".into(), json!("fixed")),
                    ("ignored".into(), json!("not declared")),
                ])
            );
            assert_eq!(call.thread, std::thread::current().id());
            assert!(call.asynchronous);
        }
    }
    CallerAction::close(&mut app).await;
}

#[tokio::test]
async fn failed_local_action_sources_never_publish_staged_state() {
    for behavior in [
        Behavior::Error,
        Behavior::PanicStart,
        Behavior::PanicPoll,
        Behavior::PanicAfterPartial,
        Behavior::InvalidMutation,
        Behavior::InvalidPartial,
    ] {
        let observation = Arc::new(Observation::default());
        let action = CallerAction::new("first", &observation, behavior);
        let config: AppConfig<StateStore, Agent> = AppConfig::new(
            CallerAction::store().await,
            Vec::new(),
            vec![CallerAction::definition(&["first"])],
        );
        let mut app = App::open(config.with_actions(vec![action]))
            .await
            .expect("bind action");
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().expect("workspace directory"),
            })
            .await
            .expect("workspace");
        let workflow = workspace
            .workflow("callable".parse().expect("workflow id"))
            .await
            .expect("workflow");
        let mut run = workflow
            .prepare(WorkflowState::default())
            .await
            .expect("checkpoint zero");
        let execution = run
            .execution(CallerAction::options())
            .await
            .expect("local session");
        let result = tokio::time::timeout(Duration::from_secs(3), execution.finish())
            .await
            .expect("failure must settle");
        let snapshot = run.snapshot().await.expect("durable failure state");
        assert!(result.is_err() || matches!(snapshot.status, RunStatus::Failed(_)));
        match behavior {
            Behavior::Error => assert!(matches!(
                snapshot.status,
                RunStatus::Failed(WorkflowFailure::Action(_))
            )),
            Behavior::PanicStart | Behavior::PanicPoll | Behavior::PanicAfterPartial => {
                assert!(matches!(
                    snapshot.status,
                    RunStatus::Failed(WorkflowFailure::Execution(_))
                ));
            }
            Behavior::InvalidMutation | Behavior::InvalidPartial => assert!(matches!(
                snapshot.status,
                RunStatus::Failed(WorkflowFailure::State(_))
            )),
            _ => unreachable!("only failure behaviors are selected"),
        }
        assert_eq!(
            observation.constructions.load(Ordering::SeqCst),
            1,
            "a terminal error must never redispatch"
        );
        assert_eq!(
            observation.drops.load(Ordering::SeqCst),
            usize::from(!matches!(behavior, Behavior::PanicStart))
        );
        assert_eq!(
            observation.external_effects.load(Ordering::SeqCst),
            usize::from(!matches!(
                behavior,
                Behavior::PanicStart | Behavior::PanicPoll
            ))
        );
        assert_eq!(
            snapshot.state.values(),
            &Map::from_iter([("count".into(), json!(0))])
        );
        assert!(snapshot.last_action_ordinal.is_none());
        assert!(snapshot.outcomes.is_empty());
        CallerAction::close(&mut app).await;
    }
}

#[tokio::test]
async fn source_eof_rolls_back_without_failure_and_the_same_action_can_be_retried() {
    // Pinned step_core.rs:157–167,361–365 returns successful no-output on EOF;
    // the prior state/position remain available to a subsequent step.
    let observation = Arc::new(Observation::default());
    let config: AppConfig<StateStore, Agent> = AppConfig::new(
        CallerAction::store().await,
        Vec::new(),
        vec![CallerAction::definition(&["first"])],
    );
    let mut app = App::open(config.with_actions(vec![CallerAction::new(
        "first",
        &observation,
        Behavior::MissingTerminal,
    )]))
    .await
    .expect("bind EOF source");
    let workspace = app
        .create_workspace(WorkspaceConfig {
            name: None,
            root: std::env::current_dir().expect("workspace directory"),
        })
        .await
        .expect("workspace");
    let workflow = workspace
        .workflow("callable".parse().expect("workflow id"))
        .await
        .expect("workflow");
    let mut run = workflow
        .prepare(WorkflowState::default())
        .await
        .expect("checkpoint zero");
    for attempt in 1..=2 {
        let mut execution = run
            .execution(CallerAction::options())
            .await
            .expect("retryable local session");
        let result = tokio::time::timeout(Duration::from_secs(3), execution.step())
            .await
            .expect("finite EOF source must stop")
            .expect("EOF is not an action error");
        assert!(result.is_none(), "EOF must not invent a committed outcome");
        execution
            .abort()
            .await
            .expect("release this uncommitted session");
        let snapshot = run.snapshot().await.expect("retryable saved position");
        assert!(matches!(
            snapshot.status,
            RunStatus::Running | RunStatus::Paused
        ));
        assert_eq!(
            snapshot.state.values(),
            &Map::from_iter([("count".into(), json!(0))])
        );
        assert!(snapshot.last_action_ordinal.is_none());
        assert!(snapshot.outcomes.is_empty());
        assert_eq!(observation.constructions.load(Ordering::SeqCst), attempt);
        assert_eq!(observation.drops.load(Ordering::SeqCst), attempt);
    }
    assert!(
        observation
            .calls
            .lock()
            .expect("invocations")
            .iter()
            .all(|call| call.ordinal == 0)
    );
    CallerAction::close(&mut app).await;
}

#[tokio::test]
async fn explicit_action_writes_are_not_limited_by_declaration_metadata() {
    // Pinned LocalAction::update applies every explicit ActionResult write.
    // Only FunctionAction's convenience result adapter selects declared keys.
    let observation = Arc::new(Observation::default());
    let config: AppConfig<StateStore, Agent> = AppConfig::new(
        CallerAction::store().await,
        Vec::new(),
        vec![CallerAction::definition(&["first"])],
    );
    let mut app = App::open(config.with_actions(vec![CallerAction::new(
        "first",
        &observation,
        Behavior::AdditionalWrite,
    )]))
    .await
    .expect("bind explicit writer");
    let workspace = app
        .create_workspace(WorkspaceConfig {
            name: None,
            root: std::env::current_dir().expect("workspace directory"),
        })
        .await
        .expect("workspace");
    let workflow = workspace
        .workflow("callable".parse().expect("workflow id"))
        .await
        .expect("workflow");
    let mut run = workflow
        .prepare(WorkflowState::default())
        .await
        .expect("checkpoint zero");
    let execution = run
        .execution(CallerAction::options())
        .await
        .expect("local session");
    let snapshot = tokio::time::timeout(Duration::from_secs(3), execution.finish())
        .await
        .expect("finite source must settle")
        .expect("valid explicit writes must commit");
    assert!(matches!(snapshot.status, RunStatus::Succeeded));
    assert_eq!(
        snapshot.state.values(),
        &Map::from_iter([("count".into(), json!(1)), ("other".into(), json!(99)),])
    );
    assert!(!snapshot.state.values().contains_key("transient"));
    CallerAction::close(&mut app).await;
}

#[tokio::test]
async fn dropping_an_unpolled_local_execution_never_constructs_an_action() {
    let observation = Arc::new(Observation::default());
    let config: AppConfig<StateStore, Agent> = AppConfig::new(
        CallerAction::store().await,
        Vec::new(),
        vec![CallerAction::definition(&["first"])],
    );
    let mut app = App::open(config.with_actions(vec![CallerAction::new(
        "first",
        &observation,
        Behavior::Increment,
    )]))
    .await
    .expect("bind action");
    let workspace = app
        .create_workspace(WorkspaceConfig {
            name: None,
            root: std::env::current_dir().expect("workspace directory"),
        })
        .await
        .expect("workspace");
    let workflow = workspace
        .workflow("callable".parse().expect("workflow id"))
        .await
        .expect("workflow");
    let mut run = workflow
        .prepare(WorkflowState::default())
        .await
        .expect("checkpoint zero");
    drop(
        run.execution(CallerAction::options())
            .await
            .expect("local session"),
    );
    assert_eq!(observation.constructions.load(Ordering::SeqCst), 0);
    assert_eq!(observation.polls.load(Ordering::SeqCst), 0);
    assert_eq!(observation.drops.load(Ordering::SeqCst), 0);
    assert_eq!(
        run.snapshot()
            .await
            .expect("unchanged state")
            .state
            .values()["count"],
        0
    );
    CallerAction::close(&mut app).await;
}

#[tokio::test]
async fn partial_action_drop_and_abort_roll_back_without_replaying_caller_effects() {
    // Pinned stream_result.rs:29–30,86–105 retains the committed base on drop.
    // The source's private I/O belongs to the caller; it has no App receipt.
    for explicit_abort in [false, true] {
        let observation = Arc::new(Observation::default());
        let (mut app, mut run) =
            CallerAction::prepared(&observation, Behavior::PendingAfterPartial, None).await;
        for attempt in 1..=2 {
            let mut execution = run
                .execution(CallerAction::options())
                .await
                .expect("explicit execution");
            assert!(matches!(
                execution.next().await.expect("one partial"),
                Some(ExecutionEvent::Partial { node, .. }) if node == "node0"
            ));
            assert_eq!(observation.external_effects.load(Ordering::SeqCst), attempt);
            if explicit_abort {
                tokio::time::timeout(Duration::from_secs(3), execution.abort())
                    .await
                    .expect("abort must not wait for the caller source")
                    .expect("abort releases the uncommitted source");
            } else {
                drop(execution);
            }
            assert_eq!(
                observation.drops.load(Ordering::SeqCst),
                attempt,
                "the local source drops exactly once"
            );
            let snapshot = run.snapshot().await.expect("committed base after release");
            assert!(matches!(
                snapshot.status,
                RunStatus::Running | RunStatus::Paused
            ));
            assert_eq!(snapshot.state.values()["count"], 0);
            assert!(snapshot.last_action_ordinal.is_none());
            assert!(snapshot.outcomes.is_empty());
            assert!(
                snapshot.interactions.is_empty(),
                "source polling alone cannot fabricate Unknown"
            );
            assert_eq!(
                observation.constructions.load(Ordering::SeqCst),
                attempt,
                "release cannot redispatch"
            );
            assert_eq!(
                observation.external_effects.load(Ordering::SeqCst),
                attempt,
                "rollback cannot undo caller I/O"
            );
        }
        assert!(
            observation
                .calls
                .lock()
                .expect("calls")
                .iter()
                .all(|call| call.ordinal == 0)
        );
        CallerAction::close(&mut app).await;
        assert_eq!(
            observation.constructions.load(Ordering::SeqCst),
            2,
            "drained cleanup must not redispatch"
        );
        assert_eq!(observation.external_effects.load(Ordering::SeqCst), 2);
        assert_eq!(observation.drops.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn an_action_deadline_after_partial_is_timeout_without_a_fabricated_unknown_effect() {
    let observation = Arc::new(Observation::default());
    let (mut app, mut run) =
        CallerAction::prepared(&observation, Behavior::PendingAfterPartial, Some(500)).await;
    let mut execution = run
        .execution(CallerAction::options())
        .await
        .expect("local session");
    assert!(matches!(
        execution.next().await.expect("partial"),
        Some(ExecutionEvent::Partial { .. })
    ));
    let result = tokio::time::timeout(Duration::from_secs(3), execution.finish())
        .await
        .expect("activation deadline must bound the pending caller source");
    let snapshot = run.snapshot().await.expect("terminal deadline state");
    assert!(result.is_err() || matches!(snapshot.status, RunStatus::Failed(_)));
    assert!(matches!(
        snapshot.status,
        RunStatus::Failed(WorkflowFailure::Timeout(_))
    ));
    assert_eq!(snapshot.state.values()["count"], 0);
    assert!(snapshot.last_action_ordinal.is_none());
    assert!(snapshot.outcomes.is_empty());
    assert!(snapshot.interactions.is_empty());
    assert_eq!(observation.constructions.load(Ordering::SeqCst), 1);
    assert_eq!(observation.external_effects.load(Ordering::SeqCst), 1);
    assert_eq!(observation.drops.load(Ordering::SeqCst), 1);
    CallerAction::close(&mut app).await;
}

#[tokio::test]
async fn reopened_runs_retain_exact_callable_revisions_while_new_definitions_select_new_ones() {
    // This is a deterministic new-generation reopen through a real shared Store,
    // not a fresh-process or strict-live durability claim.
    for reversed in [false, true] {
        let old_observed = Arc::new(Observation::default());
        let new_observed = Arc::new(Observation::default());
        let store = CallerAction::store().await;
        let config: AppConfig<StateStore, Agent> = AppConfig::new(
            store.clone(),
            Vec::new(),
            vec![CallerAction::revision_definition(
                "workflow-z-old",
                "action-r1",
                "condition-r1",
            )],
        );
        let old_app = App::open(
            config
                .with_actions(vec![CallerAction::at_revision("action-r1", &old_observed)])
                .with_conditions(vec![CallerCondition::new("condition-r1", &old_observed)]),
        )
        .await
        .expect("original revision inventory");
        let old_workspace = old_app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().expect("workspace root"),
            })
            .await
            .expect("original workspace");
        let workspace_id = old_workspace.id().clone();
        let old_workflow = old_workspace
            .workflow("callable".parse().expect("id"))
            .await
            .expect("original workflow");
        let old_run = old_workflow
            .prepare(WorkflowState::default())
            .await
            .expect("retained paused run");
        let old_id = old_run.id().clone();
        assert_eq!(old_observed.constructions.load(Ordering::SeqCst), 0);

        for missing in ["action", "condition"] {
            let config: AppConfig<StateStore, Agent> = AppConfig::new(
                store.clone(),
                Vec::new(),
                vec![
                    CallerAction::revision_definition(
                        "workflow-z-old",
                        "action-r1",
                        "condition-r1",
                    ),
                    CallerAction::revision_definition(
                        "workflow-a-new",
                        "action-r2",
                        "condition-r2",
                    ),
                ],
            );
            let mut actions = vec![
                CallerAction::at_revision("action-r1", &old_observed),
                CallerAction::at_revision("action-r2", &new_observed),
            ];
            let mut conditions = vec![
                CallerCondition::new("condition-r1", &old_observed),
                CallerCondition::new("condition-r2", &new_observed),
            ];
            if missing == "action" {
                actions.remove(0);
            } else {
                conditions.remove(0);
            }
            let result = App::open(config.with_actions(actions).with_conditions(conditions)).await;
            assert!(
                matches!(result, Err(AppError::InvalidInput(_))),
                "a newer {missing} cannot stand in for a retained exact binding"
            );
        }
        for (action, condition) in [("action-r2", "condition-r1"), ("action-r1", "condition-r2")] {
            let config: AppConfig<StateStore, Agent> = AppConfig::new(
                store.clone(),
                Vec::new(),
                vec![CallerAction::revision_definition(
                    "workflow-z-old",
                    action,
                    condition,
                )],
            );
            let result = App::open(
                config
                    .with_actions(vec![
                        CallerAction::at_revision("action-r1", &old_observed),
                        CallerAction::at_revision("action-r2", &new_observed),
                    ])
                    .with_conditions(vec![
                        CallerCondition::new("condition-r1", &old_observed),
                        CallerCondition::new("condition-r2", &new_observed),
                    ]),
            )
            .await;
            assert!(
                matches!(result, Err(AppError::Conflict(_))),
                "same definition revision cannot change either callable reference"
            );
        }
        let unchanged = old_run
            .snapshot()
            .await
            .expect("failed reopen leaves prior generation valid");
        assert_eq!(unchanged.checkpoint, 0);
        assert!(matches!(unchanged.status, RunStatus::Paused));
        for observation in [&old_observed, &new_observed] {
            assert_eq!(observation.constructions.load(Ordering::SeqCst), 0);
            assert_eq!(observation.condition_calls.load(Ordering::SeqCst), 0);
        }

        let config: AppConfig<StateStore, Agent> = AppConfig::new(
            store,
            Vec::new(),
            vec![
                CallerAction::revision_definition("workflow-z-old", "action-r1", "condition-r1"),
                CallerAction::revision_definition("workflow-a-new", "action-r2", "condition-r2"),
            ],
        );
        let mut actions = vec![
            CallerAction::at_revision("action-r1", &old_observed),
            CallerAction::at_revision("action-r2", &new_observed),
        ];
        let mut conditions = vec![
            CallerCondition::new("condition-r1", &old_observed),
            CallerCondition::new("condition-r2", &new_observed),
        ];
        if reversed {
            actions.reverse();
            conditions.reverse();
        }
        let mut app = App::open(config.with_actions(actions).with_conditions(conditions))
            .await
            .expect("coexisting retained and new revisions");
        drop(old_run);
        drop(old_workflow);
        drop(old_workspace);
        drop(old_app);
        let workspace = app
            .workspace(workspace_id)
            .await
            .expect("reopened workspace");
        let workflow = workspace
            .workflow("callable".parse().expect("id"))
            .await
            .expect("new-admission workflow revision");
        let mut retained = workflow
            .open_run(RunSelector::Latest(old_id))
            .await
            .expect("old run uses its recorded definition");
        let execution = retained
            .execution(CallerAction::options())
            .await
            .expect("old local session");
        let completed = tokio::time::timeout(Duration::from_secs(3), execution.finish())
            .await
            .expect("retained action completes")
            .expect("retained action outcome");
        assert!(matches!(completed.status, RunStatus::Succeeded));
        assert_eq!(
            completed.state.values()["count"],
            2,
            "retained predicate selects the second action"
        );
        assert_eq!(completed.last_action_ordinal, Some(1));
        assert_eq!(old_observed.constructions.load(Ordering::SeqCst), 2);
        assert_eq!(new_observed.constructions.load(Ordering::SeqCst), 0);
        assert_eq!(old_observed.condition_calls.load(Ordering::SeqCst), 1);
        assert_eq!(new_observed.condition_calls.load(Ordering::SeqCst), 0);

        let mut fresh = workflow
            .prepare(WorkflowState::default())
            .await
            .expect("new run");
        let execution = fresh
            .execution(CallerAction::options())
            .await
            .expect("new local session");
        let completed = tokio::time::timeout(Duration::from_secs(3), execution.finish())
            .await
            .expect("new action completes")
            .expect("new action outcome");
        assert!(matches!(completed.status, RunStatus::Succeeded));
        assert_eq!(
            completed.state.values()["count"],
            1,
            "new predicate ends before the second action"
        );
        assert_eq!(
            completed.last_action_ordinal,
            Some(0),
            "predicate evaluation is not an action"
        );
        assert_eq!(old_observed.constructions.load(Ordering::SeqCst), 2);
        assert_eq!(new_observed.constructions.load(Ordering::SeqCst), 1);
        assert_eq!(old_observed.condition_calls.load(Ordering::SeqCst), 1);
        assert_eq!(new_observed.condition_calls.load(Ordering::SeqCst), 1);
        CallerAction::close(&mut app).await;
    }
}

#[tokio::test]
async fn cancelled_step_observation_retains_the_same_stage_publication_and_source() {
    for point in [
        PublicationPoint::BeforeCommit,
        PublicationPoint::AppliedReceipt,
    ] {
        let mut case = PublicationCase::new(PublicationBoundary::Stage, point).await;
        let observed = AssertUnwindSafe(async {
            let mut observer = case.workflow.open_run(RunSelector::Latest(case.run.id().clone())).await.unwrap();
            let mut execution = case.run.execution(CallerAction::options()).await.unwrap();
            assert!(matches!(execution.next().await.unwrap(), Some(ExecutionEvent::Partial { .. })));
            let mut step = Box::pin(execution.step());
            tokio::time::timeout(Duration::from_secs(3), async {
                tokio::select! {
                    _ = case.gate.entered.notified() => {}
                    _ = &mut step => panic!("step returned before its selected Stage publication"),
                }
            }).await.expect("the actual Stage mutation must reach the gate");
            drop(step);
            assert_eq!(case.gate.abandoned.load(Ordering::SeqCst), 0,
                "cancelling a step observer cannot drop its admitted mutation");
            assert!(matches!(observer.execution(CallerAction::options()).await,
                Err(AppError::Busy(_))), "the original execution still owns this run");
            assert_eq!(case.observation.constructions.load(Ordering::SeqCst), 1);
            case.gate.release();
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(3), execution.step()).await
                    .expect("reattached step observer must finish").unwrap(),
                Some(ExecutionEvent::Committed { outcomes, .. })
                    if outcomes.len() == 1 && outcomes[0].node == "node0" && outcomes[0].ordinal == 0
            ));
            let snapshot = execution.abort().await.unwrap();
            assert_eq!(snapshot.state.values()["count"], 1);
            assert_eq!(snapshot.last_action_ordinal, Some(0));
            assert_eq!(case.observation.constructions.load(Ordering::SeqCst), 1,
                "resuming observation cannot reconstruct the first or next source");
        }).catch_unwind().await;
        case.finish(observed, false).await;
    }
}

#[tokio::test]
async fn owner_drop_during_stage_publication_blocks_replacement_until_rollback() {
    for point in [
        PublicationPoint::BeforeCommit,
        PublicationPoint::AppliedReceipt,
    ] {
        let mut case = PublicationCase::new(PublicationBoundary::Stage, point).await;
        let observed = AssertUnwindSafe(async {
            let mut observer = case
                .workflow
                .open_run(RunSelector::Latest(case.run.id().clone()))
                .await
                .unwrap();
            let mut execution = case.run.execution(CallerAction::options()).await.unwrap();
            assert!(matches!(
                execution.next().await.unwrap(),
                Some(ExecutionEvent::Partial { .. })
            ));
            let mut step = Box::pin(execution.step());
            tokio::time::timeout(Duration::from_secs(3), async {
                tokio::select! {
                    _ = case.gate.entered.notified() => {}
                    _ = &mut step => panic!("step returned before its selected Stage publication"),
                }
            })
            .await
            .expect("the actual Stage mutation must reach the gate");
            drop(step);
            drop(execution);
            assert_eq!(
                case.gate.abandoned.load(Ordering::SeqCst),
                0,
                "local source destruction cannot detach admitted publication"
            );
            assert!(
                matches!(
                    observer.execution(CallerAction::options()).await,
                    Err(AppError::Busy(_))
                ),
                "replacement must wait for the exact old publication"
            );
            case.gate.release();
            let snapshot = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    match observer.execution(CallerAction::options()).await {
                        Ok(execution) => break execution.abort().await.unwrap(),
                        Err(AppError::Busy(_)) => {
                            tokio::time::sleep(Duration::from_millis(5)).await
                        }
                        Err(error) => panic!("replacement admission failed unexpectedly: {error}"),
                    }
                }
            })
            .await
            .expect("rollback must release the old execution generation");
            assert_eq!(snapshot.state.values()["count"], 0);
            assert!(snapshot.outcomes.is_empty());
            assert_eq!(snapshot.last_action_ordinal, None);
            assert!(snapshot.interactions.is_empty());
            assert_eq!(case.observation.constructions.load(Ordering::SeqCst), 1);
            assert_eq!(case.observation.drops.load(Ordering::SeqCst), 1);
            assert_eq!(case.gate.abandoned.load(Ordering::SeqCst), 0);
        })
        .catch_unwind()
        .await;
        case.finish(observed, false).await;
    }
}

#[tokio::test]
async fn owner_drop_after_applied_barrier_keeps_the_commit_without_advancing_routing() {
    let mut case = PublicationCase::new(
        PublicationBoundary::Barrier,
        PublicationPoint::AppliedReceipt,
    )
    .await;
    let observed = AssertUnwindSafe(async {
        let mut observer = case
            .workflow
            .open_run(RunSelector::Latest(case.run.id().clone()))
            .await
            .unwrap();
        let mut execution = case.run.execution(CallerAction::options()).await.unwrap();
        assert!(matches!(
            execution.next().await.unwrap(),
            Some(ExecutionEvent::Partial { .. })
        ));
        let mut step = Box::pin(execution.step());
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                _ = case.gate.entered.notified() => {}
                _ = &mut step => panic!("step returned before its applied Barrier receipt"),
            }
        })
        .await
        .expect("the actual Barrier receipt must reach the gate");
        drop(step);
        drop(execution);
        let committed = observer.snapshot().await.unwrap();
        assert_eq!(committed.state.values()["count"], 1);
        assert_eq!(committed.last_action_ordinal, Some(0));
        assert_eq!(committed.outcomes.len(), 1);
        assert_eq!(committed.outcomes[0].node, "node0");
        assert!(
            matches!(
                observer.execution(CallerAction::options()).await,
                Err(AppError::Busy(_))
            ),
            "lost receipt still belongs to the old generation"
        );
        assert_eq!(case.gate.abandoned.load(Ordering::SeqCst), 0);
        case.gate.release();
        let snapshot = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match observer.execution(CallerAction::options()).await {
                    Ok(execution) => break execution.abort().await.unwrap(),
                    Err(AppError::Busy(_)) => tokio::time::sleep(Duration::from_millis(5)).await,
                    Err(error) => panic!("replacement admission failed unexpectedly: {error}"),
                }
            }
        })
        .await
        .expect("retained commit reconciliation must release its generation");
        assert_eq!(snapshot.state, committed.state);
        assert!(
            snapshot.outcomes == committed.outcomes,
            "receipt reconciliation must retain the exact committed outcomes"
        );
        assert_eq!(snapshot.last_action_ordinal, Some(0));
        assert_eq!(
            case.observation.constructions.load(Ordering::SeqCst),
            1,
            "neither receipt recovery nor unpolled replacement may enter node1"
        );
    })
    .catch_unwind()
    .await;
    case.finish(observed, false).await;
}

#[tokio::test]
async fn reopening_a_prepared_stage_or_barrier_parks_user_advancement_until_explicit_execution() {
    for boundary in [PublicationBoundary::Stage, PublicationBoundary::Barrier] {
        let mut case = PublicationCase::new(boundary, PublicationPoint::AppliedReceipt).await;
        let mut fresh = None;
        let observed =
            AssertUnwindSafe(async {
                let workspace_id = case
                    .app
                    .list_workspaces(None, std::num::NonZeroUsize::new(1).unwrap())
                    .await
                    .unwrap()[0]
                    .id
                    .clone();
                let run_id = case.run.id().clone();
                let mut execution = case.run.execution(CallerAction::options()).await.unwrap();
                assert!(matches!(
                    execution.next().await.unwrap(),
                    Some(ExecutionEvent::Partial { .. })
                ));
                let mut step = Box::pin(execution.step());
                tokio::time::timeout(Duration::from_secs(3), async {
                tokio::select! {
                    _ = case.gate.entered.notified() => {}
                    _ = &mut step => panic!("step returned before its applied publication receipt"),
                }
            }).await.expect("the selected durable boundary must be visible before reopen");
                drop(step);
                // The fresh App claims the real stored generation while the old
                // source owner still exists. No synthetic checkpoint is installed.
                fresh = Some(
                    App::open(PublicationCase::config(
                        case.store.clone(),
                        &case.observation,
                    ))
                    .await
                    .expect("fresh App claims this recorded boundary"),
                );
                drop(execution);
                case.gate.release();
                let app = fresh.as_mut().unwrap();
                let workspace = app.workspace(workspace_id).await.unwrap();
                let workflow = workspace
                    .workflow("callable".parse().unwrap())
                    .await
                    .unwrap();
                let mut run = workflow
                    .open_run(RunSelector::Latest(run_id))
                    .await
                    .unwrap();
                let parked = tokio::time::timeout(Duration::from_secs(3), run.wait())
                    .await
                    .expect("fresh ownership must settle to a public parked boundary")
                    .unwrap();
                let expected = u64::from(matches!(boundary, PublicationBoundary::Barrier));
                assert_eq!(parked.state.values()["count"], expected);
                assert_eq!(parked.last_action_ordinal, expected.checked_sub(1));
                assert_eq!(
                    case.observation.constructions.load(Ordering::SeqCst),
                    1,
                    "recovery cannot replay caller source or advance the next action"
                );
                let completed = run
                    .execution(CallerAction::options())
                    .await
                    .unwrap()
                    .finish()
                    .await
                    .unwrap();
                assert!(matches!(completed.status, RunStatus::Succeeded));
                assert_eq!(completed.state.values()["count"], 2);
                assert_eq!(completed.last_action_ordinal, Some(1));
                assert_eq!(
                    case.observation.constructions.load(Ordering::SeqCst),
                    3 - expected as usize
                );
                let calls = case.observation.calls.lock().unwrap();
                let first_calls = calls.iter().filter(|call| call.node == "node0").count();
                assert_eq!(
                    first_calls,
                    2 - expected as usize,
                    "only an explicit retry may repeat the uncommitted Stage action"
                );
                assert_eq!(calls.iter().filter(|call| call.node == "node1").count(), 1);
            })
            .catch_unwind()
            .await;
        case.gate.release();
        let superseded = fresh.is_some();
        let fresh_closed = if let Some(app) = fresh.as_mut() {
            Some(
                AssertUnwindSafe(CallerAction::close(app))
                    .catch_unwind()
                    .await,
            )
        } else {
            None
        };
        case.finish(observed, superseded).await;
        if let Some(Err(failure)) = fresh_closed {
            std::panic::resume_unwind(failure);
        }
    }
}

#[tokio::test]
async fn action_input_resolution_rejects_before_source_construction_and_preserves_exact_data() {
    // Pinned PreparedInputSpec resolves defaults, then bound values, then every
    // runtime entry; equal bound overrides are errors too. Read metadata is a
    // precondition, not a projection of the full action state.
    for case in [
        "missing-required",
        "equal-bound-override",
        "different-bound-override",
        "reserved-runtime",
        "missing-read",
        "unsupported-mode",
        "default",
        "override-default",
        "explicit-null",
        "required-bound",
        "repeated-inputs",
        "arbitrary-extra",
        "undeclared-state-visible",
    ] {
        let observed = Arc::new(Observation::default());
        let mut action = CallerAction::new("first", &observed, Behavior::Increment);
        let mut options = CallerAction::options();
        let accepted = match case {
            "missing-required" => {
                options.inputs.shift_remove("question");
                false
            }
            "equal-bound-override" | "different-bound-override" => {
                options.inputs.insert(
                    "bound".into(),
                    json!(if case == "equal-bound-override" {
                        "fixed"
                    } else {
                        "changed"
                    }),
                );
                false
            }
            "reserved-runtime" => {
                options.inputs.insert("__user".into(), json!(1));
                false
            }
            "missing-read" => {
                action.declaration.reads.push("missing".into());
                false
            }
            "unsupported-mode" => {
                action.declaration.mode = ActionMode::Synchronous;
                false
            }
            "override-default" => {
                options.inputs.insert("format".into(), json!("rich"));
                true
            }
            "explicit-null" => {
                options.inputs.insert("format".into(), Value::Null);
                true
            }
            "required-bound" => {
                options.inputs.shift_remove("question");
                action
                    .declaration
                    .bound_inputs
                    .insert("question".into(), json!("bound question"));
                true
            }
            "repeated-inputs" => {
                action.declaration.required_inputs.push("question".into());
                action
                    .declaration
                    .optional_inputs
                    .extend(["format".into(), "absent".into()]);
                true
            }
            "arbitrary-extra" => {
                options
                    .inputs
                    .insert("a/b ~ .\0".into(), json!({"nested": [1, null]}));
                true
            }
            "undeclared-state-visible" => {
                action.declaration.reads.clear();
                true
            }
            "default" => true,
            _ => unreachable!("finite input cases"),
        };
        let mut expected = action.declaration.default_inputs.clone();
        expected.extend(action.declaration.bound_inputs.clone());
        expected.extend(options.inputs.clone());
        let (mut app, mut run) = action.prepare(None).await;
        let checked = std::panic::AssertUnwindSafe(async {
            let result = match run.execution(options).await {
                Ok(mut execution) => tokio::time::timeout(Duration::from_secs(3), execution.step())
                    .await
                    .expect("input boundary is bounded"),
                Err(error) => Err(error),
            };
            let snapshot = run.snapshot().await.expect("input boundary snapshot");
            if accepted {
                assert!(result.expect("accepted inputs").is_some(), "{case}");
                assert_eq!(observed.constructions.load(Ordering::SeqCst), 1, "{case}");
                assert_eq!(snapshot.state.values()["count"], 1, "{case}");
                let calls = observed.calls.lock().expect("input observations");
                assert_eq!(calls.len(), 1, "{case}");
                assert_eq!(calls[0].inputs, expected, "{case}");
                assert_eq!(
                    calls[0].inputs.keys().collect::<Vec<_>>(),
                    expected.keys().collect::<Vec<_>>(),
                    "{case}: input order"
                );
                assert_eq!(calls[0].state, Map::from_iter([("count".into(), json!(0))]));
            } else {
                if case == "missing-required" {
                    assert!(
                        result
                            .expect("missing runtime inputs expose a binding target")
                            .is_none()
                    );
                    assert_eq!(CallerAction::input_target(&snapshot).2, ["question"]);
                } else {
                    assert!(
                        result.is_err() || matches!(snapshot.status, RunStatus::Failed(_)),
                        "{case} must reject"
                    );
                }
                assert_eq!(observed.constructions.load(Ordering::SeqCst), 0, "{case}");
                assert_eq!(
                    observed.external_effects.load(Ordering::SeqCst),
                    0,
                    "{case}"
                );
                assert_eq!(snapshot.state.values()["count"], 0, "{case}");
                assert!(snapshot.last_action_ordinal.is_none(), "{case}");
                assert!(snapshot.outcomes.is_empty(), "{case}");
            }
        })
        .catch_unwind()
        .await;
        CallerAction::close(&mut app).await;
        if let Err(panic) = checked {
            std::panic::resume_unwind(panic);
        }
    }
}

#[tokio::test]
async fn invalid_partial_cannot_be_followed_by_a_commit_on_the_same_execution() {
    let observed = Arc::new(Observation::default());
    let (mut app, mut run) =
        CallerAction::prepared(&observed, Behavior::InvalidPartial, None).await;
    let checked = std::panic::AssertUnwindSafe(async {
        let mut execution = run
            .execution(CallerAction::options())
            .await
            .expect("execution");
        let first = tokio::time::timeout(Duration::from_secs(3), execution.next())
            .await
            .expect("invalid partial is bounded");
        assert!(
            first.is_err() || matches!(first, Ok(None)),
            "invalid partial must not escape as an event"
        );
        let polls = observed.polls.load(Ordering::SeqCst);
        let next = tokio::time::timeout(Duration::from_secs(3), execution.next())
            .await
            .expect("closed source cannot resume");
        assert!(next.is_err() || matches!(next, Ok(None)));
        assert_eq!(
            observed.polls.load(Ordering::SeqCst),
            polls,
            "a validation error closes the source permanently"
        );
        assert_eq!(observed.drops.load(Ordering::SeqCst), 1);
        let _ = tokio::time::timeout(Duration::from_secs(3), execution.finish())
            .await
            .expect("failed output cleanup is bounded");
        let snapshot = run.snapshot().await.expect("failed output checkpoint");
        assert!(matches!(
            snapshot.status,
            RunStatus::Failed(WorkflowFailure::State(_))
        ));
        assert_eq!(snapshot.state.values()["count"], 0);
        assert!(snapshot.outcomes.is_empty() && snapshot.last_action_ordinal.is_none());
        assert_eq!(observed.constructions.load(Ordering::SeqCst), 1);
    })
    .catch_unwind()
    .await;
    CallerAction::close(&mut app).await;
    if let Err(panic) = checked {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn cancelled_abort_observation_retains_its_admitted_rollback_publication() {
    for point in [
        PublicationPoint::BeforeCommit,
        PublicationPoint::AppliedReceipt,
    ] {
        let mut case =
            PublicationCase::new(PublicationBoundary::Stage, PublicationPoint::AppliedReceipt)
                .await;
        let observed = AssertUnwindSafe(async {
            let mut observer = case
                .workflow
                .open_run(RunSelector::Latest(case.run.id().clone()))
                .await
                .unwrap();
            let mut execution = case.run.execution(CallerAction::options()).await.unwrap();
            assert!(matches!(
                execution.next().await.unwrap(),
                Some(ExecutionEvent::Partial { .. })
            ));
            let mut step = Box::pin(execution.step());
            tokio::time::timeout(Duration::from_secs(3), async {
                tokio::select! {
                    _ = case.gate.entered.notified() => {}
                    _ = &mut step => panic!("step escaped its applied Stage receipt"),
                }
            })
            .await
            .expect("real Stage commit must be held");
            drop(step);
            case.gate.release();
            tokio::time::timeout(
                Duration::from_secs(3),
                case.gate.receipt_returned.notified(),
            )
            .await
            .expect("Stage receipt is returned before rearming the same gate");
            case.gate.rearm(PublicationBoundary::Rollback, point);
            let mut abort = Box::pin(execution.abort());
            tokio::time::timeout(Duration::from_secs(3), async {
                tokio::select! {
                    _ = case.gate.entered.notified() => {}
                    _ = &mut abort => panic!("abort escaped its selected rollback publication"),
                }
            })
            .await
            .expect("rollback must admit a real transaction");
            drop(abort);
            assert_eq!(
                case.gate.abandoned.load(Ordering::SeqCst),
                0,
                "cancelling abort observation cannot drop an admitted rollback mutation"
            );
            assert!(
                matches!(
                    observer.execution(CallerAction::options()).await,
                    Err(AppError::Busy(_))
                ),
                "the rollback retains the same exclusive generation until its receipt settles"
            );
            case.gate.release();
            let snapshot = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    match observer.execution(CallerAction::options()).await {
                        Ok(execution) => break execution.abort().await.unwrap(),
                        Err(AppError::Busy(_)) => {
                            tokio::time::sleep(Duration::from_millis(5)).await
                        }
                        Err(error) => panic!("replacement failed: {error}"),
                    }
                }
            })
            .await
            .expect("retained rollback releases ownership after settlement");
            assert_eq!(snapshot.state.values()["count"], 0);
            assert!(snapshot.outcomes.is_empty());
            assert!(snapshot.last_action_ordinal.is_none());
            assert_eq!(case.observation.constructions.load(Ordering::SeqCst), 1);
            assert_eq!(case.observation.drops.load(Ordering::SeqCst), 1);
            assert_eq!(case.gate.abandoned.load(Ordering::SeqCst), 0);
        })
        .catch_unwind()
        .await;
        case.finish(observed, false).await;
    }
}

#[tokio::test]
async fn a_failed_publication_receipt_cannot_be_polled_twice_or_restart_its_source() {
    let mut case =
        PublicationCase::new(PublicationBoundary::Stage, PublicationPoint::Rejected).await;
    let observed = AssertUnwindSafe(async {
        let mut execution = case.run.execution(CallerAction::options()).await.unwrap();
        assert!(matches!(
            execution.next().await.unwrap(),
            Some(ExecutionEvent::Partial { .. })
        ));
        let result = tokio::time::timeout(Duration::from_secs(3), execution.step())
            .await
            .expect("definite publication rejection must return");
        assert!(
            result.is_err(),
            "a rejected Stage cannot synthesize a commit"
        );
        let resumed = tokio::time::timeout(
            Duration::from_secs(3),
            AssertUnwindSafe(execution.next()).catch_unwind(),
        )
        .await
        .expect("same-owner observation remains bounded")
        .expect("a consumed publication receipt must never panic on reobservation");
        assert!(
            resumed.is_err() || matches!(resumed, Ok(None)),
            "rejected publication seals this execution"
        );
        assert_eq!(case.observation.constructions.load(Ordering::SeqCst), 1);
        assert_eq!(case.observation.drops.load(Ordering::SeqCst), 1);
        drop(execution);
        let snapshot = case.run.snapshot().await.unwrap();
        assert_eq!(snapshot.state.values()["count"], 0);
        assert!(snapshot.outcomes.is_empty());
        assert!(snapshot.last_action_ordinal.is_none());
    })
    .catch_unwind()
    .await;
    case.finish(observed, false).await;
}

#[tokio::test]
async fn same_wave_caller_context_ordinals_match_the_canonical_committed_outcomes() {
    for reversed in [false, true] {
        let observed = Arc::new(Observation::default());
        let mut document = CallerAction::document(&["first", "second"], None);
        document["entry"] = json!("fork");
        document["reducers"]["count"] = json!("collect");
        document["nodes"].as_array_mut().unwrap().insert(
            0,
            json!({
                "id":"fork", "step":{"kind":"state","mutations":[]}
            }),
        );
        document["routes"] = json!({"fork":{"mode":"broadcast","branches":[
            {"target":{"kind":"node","id":"node1"}},
            {"target":{"kind":"node","id":"node0"}}
        ]}});
        let definition = WorkflowDefinition::parse(&document.to_string()).unwrap();
        let mut actions = Vec::from(
            ["first", "second"].map(|key| CallerAction::new(key, &observed, Behavior::Increment)),
        );
        // Move the original non-Clone values; inventory order is not execution order.
        if reversed {
            actions.reverse();
        }
        let config: AppConfig<StateStore, Agent> =
            AppConfig::new(CallerAction::store().await, Vec::new(), vec![definition]);
        let mut app = App::open(config.with_actions(actions)).await.unwrap();
        let checked = AssertUnwindSafe(async {
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().unwrap(),
                })
                .await
                .unwrap();
            let workflow = workspace
                .workflow("callable".parse().unwrap())
                .await
                .unwrap();
            let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
            let execution = run.execution(CallerAction::options()).await.unwrap();
            let snapshot = tokio::time::timeout(Duration::from_secs(3), execution.finish())
                .await
                .expect("one finite wave")
                .expect("successful caller wave");
            assert!(matches!(snapshot.status, RunStatus::Succeeded));
            assert_eq!(snapshot.state.values()["count"], json!([1, 1]));
            let calls = observed.calls.lock().unwrap();
            let mut invocation_ordinals = calls
                .iter()
                .map(|call| (call.node.as_str(), call.ordinal))
                .collect::<Vec<_>>();
            invocation_ordinals.sort_unstable();
            assert_eq!(invocation_ordinals, [("node0", 1), ("node1", 2)]);
            assert_eq!(
                snapshot
                    .outcomes
                    .iter()
                    .map(|outcome| (outcome.node.as_str(), outcome.ordinal))
                    .collect::<Vec<_>>(),
                [("node0", 1), ("node1", 2)]
            );
            assert_eq!(snapshot.last_action_ordinal, Some(2));
            assert_eq!(observed.constructions.load(Ordering::SeqCst), 2);
            assert_eq!(observed.drops.load(Ordering::SeqCst), 2);
        })
        .catch_unwind()
        .await;
        CallerAction::close(&mut app).await;
        if let Err(panic) = checked {
            std::panic::resume_unwind(panic);
        }
    }
}

#[tokio::test]
async fn caller_source_destructor_panics_are_contained_before_terminal_publication() {
    let mut failures = Vec::new();
    for (behavior, drop_after_partial) in [
        (Behavior::PanicDrop, false),
        (Behavior::PanicDropEof, false),
        (Behavior::PanicDrop, true),
    ] {
        let observed = Arc::new(Observation::default());
        let (mut app, mut run) = CallerAction::prepared(&observed, behavior, None).await;
        let checked = AssertUnwindSafe(async {
            let mut execution = run.execution(CallerAction::options()).await.unwrap();
            if drop_after_partial {
                assert!(matches!(
                    execution.next().await.unwrap(),
                    Some(ExecutionEvent::Partial { .. })
                ));
                let dropped = std::panic::catch_unwind(AssertUnwindSafe(|| drop(execution)));
                assert!(
                    dropped.is_ok(),
                    "caller destructor panic cannot escape owner Drop"
                );
                let snapshot = tokio::time::timeout(Duration::from_secs(3), async {
                    loop {
                        match run.execution(CallerAction::options()).await {
                            Ok(execution) => break execution.abort().await.unwrap(),
                            Err(AppError::Busy(_)) => {
                                tokio::time::sleep(Duration::from_millis(5)).await
                            }
                            Err(error) => {
                                panic!("replacement after contained Drop failed: {error}")
                            }
                        }
                    }
                })
                .await
                .expect("destructor panic must not strand exclusive ownership");
                assert_eq!(snapshot.state.values()["count"], 0);
                assert!(snapshot.outcomes.is_empty() && snapshot.last_action_ordinal.is_none());
                assert_eq!(observed.constructions.load(Ordering::SeqCst), 1);
                assert_eq!(observed.drops.load(Ordering::SeqCst), 1);
                assert_eq!(observed.external_effects.load(Ordering::SeqCst), 1);
                return;
            }
            let result = AssertUnwindSafe(tokio::time::timeout(
                Duration::from_secs(3),
                execution.finish(),
            ))
            .catch_unwind()
            .await;
            assert!(
                result.is_ok(),
                "caller destructor panic must not escape the execution boundary"
            );
            let _ = result
                .unwrap()
                .expect("destructor failure cleanup is bounded");
            let snapshot = tokio::time::timeout(Duration::from_secs(3), run.wait())
                .await
                .expect("failed source has a terminal owner")
                .unwrap();
            let RunStatus::Failed(WorkflowFailure::Execution(message)) = snapshot.status else {
                panic!("source destructor panic is an execution failure");
            };
            assert!(!message.contains("destructor secret"));
            assert_eq!(snapshot.state.values()["count"], 0);
            assert!(snapshot.outcomes.is_empty() && snapshot.last_action_ordinal.is_none());
            assert_eq!(observed.constructions.load(Ordering::SeqCst), 1);
            assert_eq!(observed.drops.load(Ordering::SeqCst), 1);
            assert_eq!(observed.external_effects.load(Ordering::SeqCst), 1);
        })
        .catch_unwind()
        .await;
        CallerAction::close(&mut app).await;
        if let Err(panic) = checked {
            failures.push(panic);
        }
    }
    if let Some(panic) = failures.into_iter().next() {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn notified_authority_read_cannot_extend_the_active_source_deadline() {
    let mut case = PublicationCase::new(
        PublicationBoundary::Cancellation,
        PublicationPoint::FollowingRead,
    )
    .await;
    let observed = AssertUnwindSafe(async {
        let observer = case.workflow.open_run(RunSelector::Latest(case.run.id().clone())).await.unwrap();
        let mut execution = case.run.execution(CallerAction::options()).await.unwrap();
        let source_started = tokio::time::Instant::now();
        assert!(matches!(
            execution.next().await.unwrap(),
            Some(ExecutionEvent::Partial { .. })
        ));
        let mut finish = Box::pin(execution.finish());
        tokio::select! {
            _ = case.observation.pending_entered.notified() => {}
            result = &mut finish => panic!("source finished before pending rendezvous: {}", result.is_ok()),
            _ = tokio::time::sleep(Duration::from_secs(2)) => panic!("source pending entry was not reached"),
        }
        let checkpoint = observer.snapshot().await.unwrap().checkpoint;
        let cancelled = tokio::select! {
            result = observer.cancel(checkpoint) => result.expect("exact cancellation intent"),
            result = &mut finish => panic!("source finished before cancellation: {}", result.is_ok()),
            _ = tokio::time::sleep(Duration::from_secs(2)) => panic!("cancellation intent did not commit"),
        };
        assert!(cancelled.checkpoint > checkpoint);
        tokio::select! {
            _ = case.gate.entered.notified() => {}
            result = &mut finish => panic!("source finished before authority-read gate: {}", result.is_ok()),
            _ = tokio::time::sleep(Duration::from_secs(2)) => panic!("post-cancellation authority read was not reached"),
        }
        assert!(!case.gate.released.load(Ordering::SeqCst));
        let completed = tokio::select! {
            _ = case.observation.source_dropped.notified() => None,
            result = &mut finish => Some(result),
            _ = tokio::time::sleep_until(source_started + Duration::from_millis(750)) => {
                panic!("authority-read wait extended the sampled action deadline");
            }
        };
        assert_eq!(case.observation.drops.load(Ordering::SeqCst), 1);
        assert!(!case.gate.released.load(Ordering::SeqCst));
        case.gate.release();
        let result = match completed {
            Some(result) => result,
            None => tokio::time::timeout(Duration::from_secs(3), &mut finish)
                .await
                .expect("released authority read and cleanup are bounded"),
        };
        match result {
            Ok(snapshot) => assert!(matches!(snapshot.status, RunStatus::Cancelled)),
            Err(AppError::Conflict(_)) => {
                // The earlier cancellation owns the durable fence. A late
                // timeout cannot replace that already accepted transition.
            }
            Err(error) => panic!("unexpected source settlement error: {error}"),
        }
        drop(finish);
        let snapshot = tokio::time::timeout(Duration::from_secs(3), observer.wait())
            .await
            .expect("accepted cancellation drains")
            .unwrap();
        assert!(matches!(snapshot.status, RunStatus::Cancelled));
        assert_eq!(snapshot.state.values()["count"], 0);
        assert!(snapshot.outcomes.is_empty() && snapshot.last_action_ordinal.is_none());
        assert_eq!(case.observation.constructions.load(Ordering::SeqCst), 1);
        assert_eq!(case.observation.external_effects.load(Ordering::SeqCst), 1);
        assert_eq!(case.observation.drops.load(Ordering::SeqCst), 1);
    })
    .catch_unwind()
    .await;
    case.finish(observed, false).await;
}

#[tokio::test]
async fn reopening_mixed_staging_preserves_resolved_input_and_discards_only_abandoned_caller_action()
 {
    let store = CallerAction::store().await;
    let gate = PublicationGate::new(PublicationBoundary::Stage, PublicationPoint::AppliedReceipt);
    let observation = Arc::new(Observation::default());
    let config = || {
        let definition = WorkflowDefinition::parse(&json!({
            "schema":"app-sdk.workflow/v2", "id":"callable", "revision":"mixed-r1",
            "state":{"count":0}, "required":["count"], "reducers":{"count":"collect"},
            "entry":"fork", "nodes":[
                {"id":"fork","step":{"kind":"state","mutations":[]}},
                {"id":"input","step":{"kind":"input","message":[{"kind":"text","text":"Native value"}],
                    "target":"count","schema":{"type":"integer"}}},
                {"id":"node0","step":{"kind":"action","action":"first","revision":"action-r1"}}
            ], "routes":{"fork":{"mode":"broadcast","branches":[
                {"target":{"kind":"node","id":"node0"}}, {"target":{"kind":"node","id":"input"}}
            ]}}
        }).to_string()).unwrap();
        AppConfig::new(
            PublicationStore {
                inner: store.clone(),
                gate: Arc::clone(&gate),
            },
            Vec::<AgentSpec<Agent>>::new(),
            vec![definition],
        )
        .with_actions(vec![CallerAction::new(
            "first",
            &observation,
            Behavior::Increment,
        )])
    };
    let mut app = App::open(config()).await.unwrap();
    let mut fresh = None;
    let progress = std::cell::Cell::new("collect initial native and transient inputs");
    let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(12), async {
        let workspace = app.create_workspace(WorkspaceConfig { name: None, root: std::env::current_dir().unwrap() }).await.unwrap();
        let workspace_id = workspace.id().clone();
        let workflow = workspace.workflow("callable".parse().unwrap()).await.unwrap();
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        let run_id = run.id().clone();
        let waiting = run.execution(ExecutionOptions::default()).await.unwrap().finish().await.unwrap();
        let [Interaction::Input { sequence, generation, .. }, Interaction::Inputs { required, .. }] =
            waiting.interactions.as_slice()
        else {
            panic!("the complete wave exposes native Input and the unstarted caller's missing inputs");
        };
        assert_eq!(required, &["question"]);
        assert_eq!(observation.constructions.load(Ordering::SeqCst), 0);
        progress.set("resolve original native Input");
        let resolved = run.resolve(*sequence, *generation, Resolution::Input(json!(5))).await.unwrap();
        assert_eq!(resolved.state.values()["count"], 0, "resolved input remains staged in the immutable wave");
        progress.set("acquire next caller execution");
        loop {
            let mut candidate = workflow.open_run(RunSelector::Latest(run_id.clone())).await.unwrap();
            let attempt = candidate.execution(ExecutionOptions::default()).await;
            match attempt {
                Ok(mut execution) => {
                    progress.set("expose the retained transient Inputs target");
                    assert!(execution.next().await.unwrap().is_none());
                    let pending = run.snapshot().await.unwrap();
                    let [Interaction::Inputs { sequence, generation, required, .. }] = pending.interactions.as_slice()
                        else { panic!("only the unstarted caller Inputs target remains after native Input settlement") };
                    assert_eq!(required, &["question"]);
                    assert_eq!(pending.state.values(), resolved.state.values());
                    assert_eq!(pending.last_action_ordinal, resolved.last_action_ordinal);
                    assert!(pending.outcomes == resolved.outcomes);
                    assert_eq!(observation.constructions.load(Ordering::SeqCst), 0);
                    let mut values = CallerAction::options().inputs;
                    values.retain(|key, _| required.contains(key));
                    progress.set("bind Inputs to the original reacquired execution");
                    let bound = run.resolve(*sequence, *generation, Resolution::Inputs(values)).await.unwrap();
                    assert!(bound.interactions.is_empty());
                    assert_eq!(bound.state.values(), resolved.state.values());
                    assert_eq!(observation.constructions.load(Ordering::SeqCst), 0);
                    progress.set("observe original caller partial");
                    assert!(matches!(execution.next().await.unwrap(), Some(ExecutionEvent::Partial { .. })));
                    progress.set("await original caller Stage receipt");
                    let mut step = Box::pin(execution.step());
                    tokio::select! {
                        _ = gate.entered.notified() => {},
                        result = &mut step => panic!("caller Stage did not reach its actual receipt: {}", result.is_ok()),
                    }
                    drop(step);
                    progress.set("reopen while Stage receipt is held");
                    fresh = Some(App::open(config()).await.unwrap());
                    drop(execution);
                    gate.release();
                    break;
                }
                Err(AppError::Busy(_)) => {
                    let snapshot = run.snapshot().await.unwrap();
                    assert_eq!(snapshot.state.values()["count"], 0);
                    assert_eq!(snapshot.last_action_ordinal, Some(0));
                    assert_eq!(observation.constructions.load(Ordering::SeqCst), 0);
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("mixed native settlement acquisition failed: {error}"),
            }
        }
        let workflow = fresh.as_ref().unwrap().workspace(workspace_id).await.unwrap()
            .workflow("callable".parse().unwrap()).await.unwrap();
        progress.set("observe reopened paused run");
        let mut resumed = workflow.open_run(RunSelector::Latest(run_id)).await.unwrap();
        assert!(matches!(resumed.wait().await.unwrap().status, RunStatus::Paused));
        assert_eq!(observation.constructions.load(Ordering::SeqCst), 1);
        let resumed_id = resumed.id().clone();
        progress.set("finish explicit resumed execution");
        let completed = loop {
            let attempt = resumed.execution(CallerAction::options()).await;
            match attempt {
                Ok(execution) => break execution.finish().await.unwrap(),
                Err(AppError::Busy(_)) => {
                    let observer = workflow.open_run(RunSelector::Latest(resumed_id.clone())).await.unwrap();
                    let snapshot = observer.snapshot().await.unwrap();
                    assert_eq!(snapshot.state.values()["count"], 0);
                    assert_eq!(snapshot.last_action_ordinal, Some(0));
                    assert_eq!(observation.constructions.load(Ordering::SeqCst), 1);
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("mixed explicit resume failed: {error}"),
            }
        };
        assert!(matches!(completed.status, RunStatus::Succeeded), "the already resolved native Input cannot reopen");
        assert_eq!(completed.state.values()["count"], json!([5,1]));
        assert_eq!(completed.last_action_ordinal, Some(2));
        assert_eq!(completed.outcomes.iter().map(|outcome| outcome.node.as_str()).collect::<Vec<_>>(), ["input","node0"]);
        assert_eq!(observation.constructions.load(Ordering::SeqCst), 2,
            "only the abandoned caller source is constructed again on explicit resume");
        let calls = observation.calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert!(calls.iter().all(|call| call.node == "node0" && call.ordinal == 2 && call.state["count"] == 0));
    })).catch_unwind().await;
    gate.release();
    let superseded = fresh.is_some();
    let fresh_closed = match fresh.as_mut() {
        Some(app) => Some(
            AssertUnwindSafe(tokio::time::timeout(
                Duration::from_secs(5),
                CallerAction::close(app),
            ))
            .catch_unwind()
            .await,
        ),
        None => None,
    };
    let old_closed = AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(5),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        }),
    ))
    .catch_unwind()
    .await;
    match observed {
        Ok(result) => result.unwrap_or_else(|error| {
            panic!(
                "mixed staged recovery stalled at {}: {error}",
                progress.get()
            )
        }),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    if let Some(result) = fresh_closed {
        result
            .expect("fresh cleanup does not panic")
            .expect("fresh cleanup joins");
    }
    match old_closed {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(AppError::Closed(_)))) if superseded => {}
        other => panic!("old mixed owner cleanup failed: {other:?}"),
    }
}

#[tokio::test]
async fn background_options_reject_before_admission_and_preserve_initial_state_and_halts() {
    let mut case =
        PublicationCase::new(PublicationBoundary::Stage, PublicationPoint::BeforeCommit).await;
    case.gate.release();
    let checked = AssertUnwindSafe(async {
        tokio::time::timeout(Duration::from_secs(12), async {
            let before = case.workflow.list_runs(None, NonZeroUsize::new(10).unwrap()).await.unwrap();
            assert_eq!(before.len(), 1);
            for invalid in ["unknown-before", "unknown-tag", "reserved-input"] {
                let mut options = CallerAction::options();
                match invalid {
                    "unknown-before" => options.halt_before.push("missing".into()),
                    "unknown-tag" => options.halt_after.push("@missing".into()),
                    "reserved-input" => { options.inputs.insert("__secret".into(), json!("not admitted")); },
                    _ => unreachable!(),
                }
                assert!(matches!(case.workflow.start(options).await, Err(AppError::InvalidInput(_))), "{invalid}");
                let after = case.workflow.list_runs(None, NonZeroUsize::new(10).unwrap()).await.unwrap();
                assert_eq!(after.len(), 1);
                assert_eq!(after[0].id, before[0].id);
                assert_eq!(after[0].checkpoint, before[0].checkpoint);
                assert_eq!(case.observation.constructions.load(Ordering::SeqCst), 0);
            }
            for before_halt in [true, false] {
                let mut options = CallerAction::options();
                options.state = WorkflowState::new(Map::from_iter([("count".into(), json!(10))])).unwrap();
                options.inputs.insert("ignored".into(), json!("background-input-only-credential"));
                if before_halt {
                    options.halt_before.push("node1".into());
                    options.halt_after.push("node0".into());
                } else {
                    options.max_steps = NonZeroUsize::new(1);
                }
                let run = case.workflow.start(options).await.unwrap();
                let halted = run.wait().await.unwrap();
                assert_eq!(halted.state.values(), &Map::from_iter([("count".into(), json!(11))]));
                assert_eq!(halted.last_action_ordinal, Some(0));
                assert_eq!(halted.outcomes.len(), 1);
                assert!(matches!(&halted.status, RunStatus::Halted { node, result }
                    if if before_halt { node == "node1" && result.is_none() }
                    else { node == "node0" && result.as_ref().is_some_and(|result| result["transient"] == "ephemeral secret") }));
                let initial = run.checkpoint(app_sdk::workflow::CheckpointSelector::Sequence(0)).await.unwrap();
                assert_eq!(initial.state.values(), &Map::from_iter([("count".into(), json!(10))]));
                let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
                let completed = loop {
                    let mut resumed = case.workflow.open_run(RunSelector::Latest(run.id().clone())).await.unwrap();
                    match resumed.execution(CallerAction::options()).await {
                        Ok(execution) => break execution.finish().await.unwrap(),
                        Err(AppError::Busy(_)) => {
                            assert!(tokio::time::Instant::now() < deadline, "background owner must release after halt");
                            let saved = run.snapshot().await.unwrap();
                            assert_eq!(saved.checkpoint, halted.checkpoint);
                            assert_eq!(saved.state, halted.state);
                            assert_eq!(saved.last_action_ordinal, halted.last_action_ordinal);
                            assert_eq!(case.observation.constructions.load(Ordering::SeqCst), if before_halt { 1 } else { 3 });
                            tokio::task::yield_now().await;
                        }
                        Err(error) => panic!("unexpected post-halt execution failure: {error}"),
                    }
                };
                assert!(matches!(completed.status, RunStatus::Succeeded));
                assert_eq!(completed.state.values()["count"], 12);
                assert_eq!(completed.last_action_ordinal, Some(1));
            }
            let calls = case.observation.calls.lock().unwrap().iter().map(|call| (
                call.node.clone(), call.ordinal, call.inputs.clone(), call.state.clone(), call.asynchronous,
            )).collect::<Vec<_>>();
            assert_eq!(calls.len(), 4);
            for (index, (node, ordinal, inputs, state, asynchronous)) in calls.iter().enumerate() {
                assert_eq!(*node, format!("node{}", index % 2));
                assert_eq!(*ordinal, (index % 2) as u64);
                assert_eq!(state["count"], 10 + index % 2);
                assert!(*asynchronous);
                assert_eq!(inputs["question"], "ephemeral secret");
                assert_eq!(inputs["ignored"], if index % 2 == 0 { "background-input-only-credential" } else { "not declared" });
            }
            let mut request = StateListRequest::new("app-sdk").unwrap().with_limit(100).unwrap();
            loop {
                let records = case.store.list(request.clone()).await.unwrap();
                assert!(!serde_json::to_string(&records.records.iter().map(|row| row.record.value.as_ref()).collect::<Vec<_>>()).unwrap()
                    .contains("background-input-only-credential"), "unprojected runtime inputs never enter persistence");
                let Some(cursor) = records.next_start_after else { break };
                request = request.start_after(cursor).unwrap();
            }
            assert_eq!(case.observation.constructions.load(Ordering::SeqCst), 4);
            assert_eq!(case.observation.drops.load(Ordering::SeqCst), 4);
        }).await.expect("background options and follow-up executions are bounded");
    }).catch_unwind().await;
    case.finish(checked, false).await;
}

impl CallerAction {
    fn input_target(snapshot: &RunSnapshot) -> (u64, u64, Vec<String>) {
        let [
            Interaction::Inputs {
                sequence,
                generation,
                required,
            },
        ] = snapshot.interactions.as_slice()
        else {
            panic!("one unstarted action exposes its exact missing-input target")
        };
        (*sequence, *generation, required.clone())
    }

    async fn assert_transient_input_binding(reopen: bool) {
        let capture = InteractionCapture::default();
        let mut trace_targets = Vec::new();
        let observation = Arc::new(Observation::default());
        let store = Self::store().await;
        let config: AppConfig<StateStore, Agent> = AppConfig::new(
            store.clone(),
            Vec::new(),
            vec![Self::definition_with_deadline(&["first"], Some(15000))],
        );
        let mut action = Self::new("first", &observation, Behavior::Increment);
        action.declaration.required_inputs.push("question".into());
        let mut app = App::open(config.with_actions(vec![action])).await.unwrap();
        let mut fresh = None;
        let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(12), async {
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().unwrap(),
                })
                .await
                .unwrap();
            let workspace_id = workspace.id().clone();
            let workflow = workspace
                .workflow("callable".parse().unwrap())
                .await
                .unwrap();
            let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
            let run_id = run.id().clone();
            let observer = workflow
                .open_run(RunSelector::Latest(run_id.clone()))
                .await
                .unwrap();
            let mut execution = run.execution(ExecutionOptions::default()).await.unwrap();
            assert!(execution.next().await.unwrap().is_none());
            let waiting = observer.snapshot().await.unwrap();
            let target = Self::input_target(&waiting);
            assert_eq!(target.2, ["question"]);
            assert!(target.0 > 0 && target.1 > 0);
            assert_eq!(
                waiting.state.values(),
                &Map::from_iter([("count".into(), json!(0))])
            );
            assert!(waiting.outcomes.is_empty() && waiting.last_action_ordinal.is_none());
            assert_eq!(observation.constructions.load(Ordering::SeqCst), 0);
            assert_eq!(observation.polls.load(Ordering::SeqCst), 0);
            for resolution in [
                Resolution::Input(json!("wrong kind")),
                Resolution::Inputs(Map::new()),
                Resolution::Inputs(Map::from_iter([
                    ("question".into(), json!("candidate")),
                    ("extra".into(), json!(1)),
                ])),
            ] {
                assert!(matches!(
                    observer.resolve(target.0, target.1, resolution).await,
                    Err(AppError::Conflict(_))
                ));
                let unchanged = observer.snapshot().await.unwrap();
                assert_eq!(Self::input_target(&unchanged), target);
                assert_eq!(unchanged.checkpoint, waiting.checkpoint);
                assert_eq!(unchanged.state.values(), waiting.state.values());
                assert_eq!(observation.constructions.load(Ordering::SeqCst), 0);
            }
            let original_value = "private-input-binding-original-question";
            let values = Map::from_iter([("question".into(), json!(original_value))]);
            let applied = observer
                .resolve(target.0, target.1, Resolution::Inputs(values.clone()))
                .await
                .unwrap();
            assert!(applied.interactions.is_empty());
            assert_eq!(applied.state.values(), waiting.state.values());
            assert!(applied.outcomes.is_empty() && applied.last_action_ordinal.is_none());
            let replay = observer
                .resolve(target.0, target.1, Resolution::Inputs(values.clone()))
                .await
                .unwrap();
            assert_eq!(replay.checkpoint, applied.checkpoint);
            assert_eq!(replay.state.values(), applied.state.values());
            assert!(replay.interactions.is_empty());
            assert!(replay.outcomes == applied.outcomes);
            assert_eq!(replay.last_action_ordinal, applied.last_action_ordinal);
            assert!(matches!(
                observer
                    .resolve(
                        target.0,
                        target.1,
                        Resolution::Inputs(Map::from_iter([("question".into(), json!("changed"))]))
                    )
                    .await,
                Err(AppError::Conflict(_))
            ));
            let bound = observer.snapshot().await.unwrap();
            assert_eq!(bound.checkpoint, applied.checkpoint);
            assert_eq!(observation.constructions.load(Ordering::SeqCst), 0);
            assert_eq!(observation.external_effects.load(Ordering::SeqCst), 0);
            let records = store
                .list(
                    StateListRequest::new("app-sdk")
                        .unwrap()
                        .with_limit(100)
                        .unwrap(),
                )
                .await
                .unwrap();
            assert!(
                records.next_start_after.is_none(),
                "inspect every bounded fixture record before the source starts"
            );
            let encoded = serde_json::to_string(
                &records
                    .records
                    .iter()
                    .map(|row| row.record.value.as_ref())
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            assert!(
                !encoded.contains(original_value),
                "binding evidence cannot persist raw input values before action execution"
            );
            drop(execution);
            drop(run);
            let workflow = if reopen {
                let config: AppConfig<StateStore, Agent> = AppConfig::new(
                    store.clone(),
                    Vec::new(),
                    vec![Self::definition_with_deadline(&["first"], Some(15000))],
                );
                let mut action = Self::new("first", &observation, Behavior::Increment);
                action.declaration.required_inputs.push("question".into());
                fresh = Some(App::open(config.with_actions(vec![action])).await.unwrap());
                fresh
                    .as_ref()
                    .unwrap()
                    .workspace(workspace_id)
                    .await
                    .unwrap()
                    .workflow("callable".parse().unwrap())
                    .await
                    .unwrap()
            } else {
                workflow
            };
            let observer = workflow
                .open_run(RunSelector::Latest(run_id.clone()))
                .await
                .unwrap();
            loop {
                let mut resumed = workflow
                    .open_run(RunSelector::Latest(run_id.clone()))
                    .await
                    .unwrap();
                let acquisition = resumed.execution(ExecutionOptions::default()).await;
                let mut execution = match acquisition {
                    Ok(execution) => execution,
                    Err(AppError::Busy(_)) => {
                        assert_eq!(
                            observation.constructions.load(Ordering::SeqCst),
                            0,
                            "disposing the old owner cannot start the bound caller source"
                        );
                        tokio::task::yield_now().await;
                        continue;
                    }
                    Err(error) => panic!("explicit input-owner reacquisition failed: {error}"),
                };
                assert!(execution.next().await.unwrap().is_none());
                let waiting_again = observer.snapshot().await.unwrap();
                let replacement = Self::input_target(&waiting_again);
                assert_eq!(replacement.2, target.2);
                assert_ne!((replacement.0, replacement.1), (target.0, target.1));
                assert!(
                    matches!(
                        observer
                            .resolve(target.0, target.1, Resolution::Inputs(values))
                            .await,
                        Err(AppError::Conflict(_))
                    ),
                    "disposed comparison evidence cannot attest an old binding replay"
                );
                let unchanged = observer.snapshot().await.unwrap();
                assert_eq!(Self::input_target(&unchanged), replacement);
                assert_eq!(unchanged.checkpoint, waiting_again.checkpoint);
                assert_eq!(unchanged.state.values(), waiting.state.values());
                assert_eq!(observation.constructions.load(Ordering::SeqCst), 0);
                let replacement_value = "private-input-binding-replacement-question";
                observer
                    .resolve(
                        replacement.0,
                        replacement.1,
                        Resolution::Inputs(Map::from_iter([(
                            "question".into(),
                            json!(replacement_value),
                        )])),
                    )
                    .await
                    .unwrap();
                let records = store
                    .list(
                        StateListRequest::new("app-sdk")
                            .unwrap()
                            .with_limit(100)
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert!(records.next_start_after.is_none());
                let encoded = serde_json::to_string(
                    &records
                        .records
                        .iter()
                        .map(|row| row.record.value.as_ref())
                        .collect::<Vec<_>>(),
                )
                .unwrap();
                assert!(!encoded.contains(original_value) && !encoded.contains(replacement_value));
                assert_eq!(observation.constructions.load(Ordering::SeqCst), 0);
                let complete = execution.finish().await.unwrap();
                assert!(matches!(complete.status, RunStatus::Succeeded));
                assert!(complete.interactions.is_empty());
                assert_eq!(complete.state.values()["count"], json!(1));
                assert_eq!(complete.last_action_ordinal, Some(0));
                assert_eq!(complete.outcomes.len(), 1);
                assert_eq!(
                    complete.outcomes[0].result["transient"],
                    json!(replacement_value)
                );
                let calls = observation
                    .calls
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|call| (call.node.clone(), call.ordinal, call.inputs.clone()))
                    .collect::<Vec<_>>();
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].0, "node0");
                assert_eq!(calls[0].1, 0);
                assert_eq!(
                    calls[0].2,
                    Map::from_iter([
                        ("format".into(), json!("plain")),
                        ("bound".into(), json!("fixed")),
                        ("question".into(), json!(replacement_value)),
                    ])
                );
                assert_eq!(observation.constructions.load(Ordering::SeqCst), 1);
                assert_eq!(observation.external_effects.load(Ordering::SeqCst), 1);
                assert_eq!(observation.drops.load(Ordering::SeqCst), 1);
                trace_targets.push(waiting_again);
                break;
            }
            trace_targets.push(waiting);
        }))
        .catch_unwind()
        .with_subscriber(capture.clone())
        .await;
        let superseded = fresh.is_some();
        let fresh_cleanup = if let Some(app) = fresh.as_mut() {
            Some(
                AssertUnwindSafe(tokio::time::timeout(
                    Duration::from_secs(5),
                    app.shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_secs(3),
                    }),
                ))
                .catch_unwind()
                .await,
            )
        } else {
            None
        };
        drop(fresh);
        let cleanup = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        ))
        .catch_unwind()
        .await;
        drop(app);
        match observed {
            Ok(result) => {
                result.expect("transient input binding and owner revocation remain bounded")
            }
            Err(panic) => std::panic::resume_unwind(panic),
        }
        if let Some(result) = fresh_cleanup {
            result
                .expect("fresh cleanup does not panic")
                .expect("fresh cleanup joins")
                .unwrap();
        }
        match cleanup
            .expect("original cleanup does not panic")
            .expect("original cleanup joins")
        {
            Ok(()) => {}
            Err(AppError::Closed(_)) if superseded => {}
            Err(error) => panic!("original input binding cleanup failed: {error}"),
        }
        for target in &trace_targets {
            capture.assert_target(target, &["pending", "applied"]);
        }
        assert_eq!(trace_targets.len(), 2);
        capture.assert_redacted(&[
            "private-input-binding-original-question",
            "private-input-binding-replacement-question",
        ]);
    }
}

#[tokio::test]
async fn transient_input_bindings_replay_only_while_the_original_execution_retains_values() {
    CallerAction::assert_transient_input_binding(false).await;
}

#[tokio::test]
async fn missing_action_inputs_expire_without_constructing_the_action() {
    let observation = Arc::new(Observation::default());
    let (mut app, mut run) =
        CallerAction::prepared(&observation, Behavior::Increment, Some(1000)).await;
    let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), async {
        let mut execution = run.execution(ExecutionOptions::default()).await.unwrap();
        assert!(execution.next().await.unwrap().is_none());
        let waiting = execution.finish().await.unwrap();
        let target = CallerAction::input_target(&waiting);
        assert_eq!(target.2, ["question"]);
        assert_eq!(waiting.state.values()["count"], 0);
        assert!(waiting.outcomes.is_empty() && waiting.last_action_ordinal.is_none());
        let expired = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let snapshot = run.snapshot().await.unwrap();
                if matches!(snapshot.status, RunStatus::Failed(_)) {
                    break snapshot;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("the original missing-input activation reaches its deadline");
        assert!(matches!(
            expired.status,
            RunStatus::Failed(WorkflowFailure::Timeout(_))
        ));
        assert_eq!(expired.state.values()["count"], 0);
        assert!(expired.outcomes.is_empty() && expired.last_action_ordinal.is_none());
        assert!(expired.interactions.is_empty());
        assert_eq!(observation.constructions.load(Ordering::SeqCst), 0);
        assert_eq!(observation.polls.load(Ordering::SeqCst), 0);
        assert_eq!(observation.drops.load(Ordering::SeqCst), 0);
        assert_eq!(observation.external_effects.load(Ordering::SeqCst), 0);
        assert!(
            app.list_workspaces(None, NonZeroUsize::new(1).unwrap())
                .await
                .is_ok(),
            "a valid control timeout leaves App active"
        );
    }))
    .catch_unwind()
    .await;
    let cleanup = AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(5),
        CallerAction::close(&mut app),
    ))
    .catch_unwind()
    .await;
    drop(app);
    match checked {
        Ok(result) => result.expect("missing-input deadline remains bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    cleanup
        .expect("cleanup does not panic")
        .expect("cleanup joins");
}

#[tokio::test]
async fn reopening_revokes_input_binding_values_and_requires_a_fresh_target() {
    CallerAction::assert_transient_input_binding(true).await;
}

#[derive(Clone, Copy)]
enum ChildInputCase {
    Child,
    Map,
    Pipeline,
    Tasks,
}

impl CallerAction {
    fn child_transient_inputs(case: ChildInputCase) {
        // Fixed rurr parallelism/types.rs:105-120,161-164,191-193 carries
        // each task's runtime inputs separately from its durable state.
        let first = json!({
            "workflow":"callable", "input":{"count":{"kind":"literal","value":10}},
            "inputs":{
                "question":{"kind":"literal","value":"first child"},
                "ignored":{"kind":"literal","value":"child-input-only-credential"}
            }
        });
        let mut step = match case {
            ChildInputCase::Tasks => json!({"kind":"map","max_parallel":2,"tasks":{
                "kind":"literal", "value":[
                    {"task_key":"first","workflow":"callable","revision":"workflow-r1","input":{"count":10},
                        "inputs":{"question":"first child","ignored":"child-input-only-credential",
                            "verbatim":{"kind":"state","path":"not_a_parent_expression"}},"history":"retain"},
                    {"task_key":"second","workflow":"callable","input":{"count":20},
                        "inputs":{"question":"second child","ignored":"child-input-only-credential",
                            "verbatim":{"kind":"state","path":"not_a_parent_expression"}}}
                ]
            }}),
            ChildInputCase::Pipeline => {
                let mut second = first.clone();
                second["input"] = json!({});
                second["inputs"]["question"]["value"] = json!("second child");
                json!({"kind":"pipeline","children":[first, second]})
            }
            ChildInputCase::Child | ChildInputCase::Map => {
                let mut step = first;
                step["kind"] = json!(if matches!(case, ChildInputCase::Child) {
                    "child"
                } else {
                    "map"
                });
                if matches!(case, ChildInputCase::Map) {
                    step["input"] = json!({});
                    step["items"] = json!({"kind":"literal","value":[10,20]});
                    step["input_key"] = json!("count");
                    step["max_parallel"] = json!(2);
                }
                step
            }
        };
        step["reductions"] = json!([{"source":"count","target":"collected","reducer":"collect"}]);
        let parent = WorkflowDefinition::parse(&json!({
            "schema":"app-sdk.workflow/v2", "id":"input-parent", "revision":"r1",
            "state":{"count":0,"collected":[]}, "required":[],
            "reducers":{"count":"reject_concurrent","collected":"collect"},
            "entry":"compose", "nodes":[{"id":"compose","deadline_ms":5000,"step":step}], "routes":{}
        }).to_string()).expect("transient input maps parse independently of durable child state");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let observation = Arc::new(Observation::default());
        let store = runtime.block_on(Self::store());
        let config: AppConfig<StateStore, Agent> = AppConfig::new(
            store.clone(),
            Vec::new(),
            vec![parent, Self::definition(&["first"])],
        );
        let mut app = runtime
            .block_on(App::open(config.with_actions(vec![Self::new(
                "first",
                &observation,
                Behavior::Increment,
            )])))
            .unwrap();
        let checked = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let workspace = runtime
                .block_on(app.create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().unwrap(),
                }))
                .unwrap();
            let workflow = runtime
                .block_on(workspace.workflow("input-parent".parse().unwrap()))
                .unwrap();
            let mut run = runtime
                .block_on(workflow.prepare(WorkflowState::default()))
                .unwrap();
            let snapshot = if matches!(case, ChildInputCase::Pipeline) {
                SyncExecution::open(&mut run, Self::options())
                    .unwrap()
                    .finish()
                    .unwrap()
            } else {
                runtime.block_on(async {
                    tokio::time::timeout(Duration::from_secs(7), async {
                        run.execution(Self::options())
                            .await
                            .unwrap()
                            .finish()
                            .await
                            .unwrap()
                    })
                    .await
                    .expect("concrete child execution is bounded")
                })
            };
            assert!(matches!(snapshot.status, RunStatus::Succeeded));
            let expected = match case {
                ChildInputCase::Child => vec![11],
                ChildInputCase::Map | ChildInputCase::Tasks => vec![11, 21],
                // WORKFLOW.md:409–418: explicit reductions use every ordered
                // child state; final_state remains separate action result data.
                ChildInputCase::Pipeline => vec![11, 12],
            };
            assert_eq!(snapshot.state.values()["collected"], json!(expected));
            assert_eq!(snapshot.state.values()["count"], 0);
            assert_eq!(snapshot.last_action_ordinal, Some(0));
            assert_eq!(snapshot.outcomes.len(), 1);
            if matches!(case, ChildInputCase::Pipeline) {
                // Pinned parallelism/executor.rs:123–138 returns all outputs;
                // task_action.rs:142–150 separately publishes the final state.
                assert_eq!(
                    snapshot.outcomes[0].result["child_states"],
                    json!([{"count":11},{"count":12}])
                );
                assert_eq!(
                    snapshot.outcomes[0].result["final_state"],
                    json!({"count":12})
                );
            }
            let mut calls = observation
                .calls
                .lock()
                .unwrap()
                .iter()
                .map(|call| {
                    (
                        call.state["count"].as_i64().unwrap(),
                        call.ordinal,
                        call.inputs.clone(),
                        call.asynchronous,
                    )
                })
                .collect::<Vec<_>>();
            calls.sort_by_key(|call| call.0);
            let before = match case {
                ChildInputCase::Child => vec![10],
                ChildInputCase::Map | ChildInputCase::Tasks => vec![10, 20],
                ChildInputCase::Pipeline => vec![10, 11],
            };
            assert_eq!(calls.len(), before.len());
            for (index, (count, ordinal, inputs, asynchronous)) in calls.iter().enumerate() {
                assert_eq!(
                    *count, before[index],
                    "pipeline state handoff is independent of transient input maps"
                );
                assert_eq!(*ordinal, 0);
                assert_eq!(*asynchronous, !matches!(case, ChildInputCase::Pipeline));
                assert_eq!(
                    inputs["question"],
                    if matches!(case, ChildInputCase::Pipeline | ChildInputCase::Tasks)
                        && index == 1
                    {
                        "second child"
                    } else {
                        "first child"
                    }
                );
                assert_eq!(inputs["ignored"], "child-input-only-credential");
                assert_eq!(inputs["format"], "plain");
                assert_eq!(inputs["bound"], "fixed");
                if matches!(case, ChildInputCase::Tasks) {
                    assert_eq!(
                        inputs["verbatim"],
                        json!({"kind":"state","path":"not_a_parent_expression"}),
                        "resolved tasks contain plain transient data, never expression syntax to evaluate again"
                    );
                }
                assert_eq!(
                    inputs.len(),
                    if matches!(case, ChildInputCase::Tasks) {
                        5
                    } else {
                        4
                    },
                    "root execution inputs do not replace a child's distinct map"
                );
            }
            let children = runtime
                .block_on(workspace.workflow("callable".parse().unwrap()))
                .unwrap();
            let retained = runtime
                .block_on(children.list_runs(None, NonZeroUsize::new(4).unwrap()))
                .unwrap();
            assert_eq!(retained.len(), before.len());
            assert!(
                retained
                    .iter()
                    .all(|child| matches!(child.status, RunStatus::Succeeded)
                        && child.last_action_ordinal == Some(0)
                        && child.state.values().len() == 1)
            );
            let mut request = StateListRequest::new("app-sdk")
                .unwrap()
                .with_limit(100)
                .unwrap();
            let mut authored_definitions = 0;
            loop {
                let records = runtime.block_on(store.list(request.clone())).unwrap();
                for row in &records.records {
                    let encoded = serde_json::to_string(row.record.value.as_ref()).unwrap();
                    if row.record.key.key().starts_with("definition/") {
                        if row.record.value["workflow_id"] == "input-parent" {
                            assert!(
                                encoded.contains("child-input-only-credential"),
                                "the original authored expression remains in its pinned definition"
                            );
                            authored_definitions += 1;
                        } else {
                            assert!(!encoded.contains("child-input-only-credential"));
                        }
                    } else {
                        assert!(
                            !encoded.contains("child-input-only-credential"),
                            "resolved transient child values cannot enter runtime records, checkpoints, results or receipts"
                        );
                    }
                }
                let Some(cursor) = records.next_start_after else {
                    break;
                };
                assert!(
                    request
                        .start_after_key()
                        .is_none_or(|previous| cursor.as_str() > previous),
                    "native fixture scan must advance through every bounded page"
                );
                request = request.start_after(cursor).unwrap();
            }
            assert_eq!(authored_definitions, 1);
            assert_eq!(
                observation.constructions.load(Ordering::SeqCst),
                before.len()
            );
            assert_eq!(observation.drops.load(Ordering::SeqCst), before.len());
        }));
        runtime.block_on(Self::close(&mut app));
        if let Err(panic) = checked {
            std::panic::resume_unwind(panic);
        }
    }
}

#[test]
fn child_action_receives_its_transient_inputs_without_persisting_them() {
    CallerAction::child_transient_inputs(ChildInputCase::Child);
}

#[test]
fn map_actions_receive_their_transient_inputs_without_persisting_them() {
    CallerAction::child_transient_inputs(ChildInputCase::Map);
}

#[test]
fn pipeline_preserves_each_transient_map_while_handing_off_only_state() {
    CallerAction::child_transient_inputs(ChildInputCase::Pipeline);
}

#[test]
fn resolved_map_tasks_keep_ordered_state_and_verbatim_distinct_transient_inputs() {
    CallerAction::child_transient_inputs(ChildInputCase::Tasks);
}

impl CallerAction {
    async fn child_transient_receipt(boundary: PublicationBoundary, point: PublicationPoint) {
        let parent = WorkflowDefinition::parse(&json!({
            "schema":"app-sdk.workflow/v2", "id":"input-receipt-parent", "revision":"r1",
            "state":{"count":0,"collected":[]}, "required":[],
            "reducers":{"count":"reject_concurrent","collected":"collect"}, "entry":"compose",
            "nodes":[{"id":"compose","deadline_ms":10000,"step":{
                "kind":"child","workflow":"callable","input":{"count":{"kind":"literal","value":10}},
                "inputs":{"question":{"kind":"literal","value":"original child input"},
                    "ignored":{"kind":"literal","value":"original private value"}},
                "reductions":[{"source":"count","target":"collected","reducer":"collect"}]
            }}],"routes":{}
        }).to_string()).expect("child transient expression map parses");
        let store = Self::store().await;
        let observation = Arc::new(Observation::default());
        let gate = PublicationGate::new(boundary, point);
        let config: AppConfig<PublicationStore, Agent> = AppConfig::new(
            PublicationStore {
                inner: store.clone(),
                gate: Arc::clone(&gate),
            },
            Vec::new(),
            vec![parent, Self::definition(&["first"])],
        );
        let mut app = App::open(config.with_actions(vec![Self::new(
            "first",
            &observation,
            Behavior::Increment,
        )]))
        .await
        .unwrap();
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(12), async {
            let workspace = app.create_workspace(WorkspaceConfig {
                name: None, root: std::env::current_dir().unwrap(),
            }).await.unwrap();
            let workflow = workspace.workflow("input-receipt-parent".parse().unwrap()).await.unwrap();
            let leaves = workspace.workflow("callable".parse().unwrap()).await.unwrap();
            let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
            let observer = workflow.open_run(RunSelector::Latest(run.id().clone())).await.unwrap();
            let mut execution = run.execution(Self::options()).await.unwrap();
            if matches!(point, PublicationPoint::ChildPreparationRead) {
                let error = execution.next().await.err().expect("a definitive preparation failure ends this original execution observation");
                assert!(matches!(error, AppError::Integration(_)));
                assert!(leaves.list_runs(None, NonZeroUsize::new(2).unwrap()).await.unwrap().is_empty());
                assert!(observation.calls.lock().unwrap().is_empty());
                drop(execution);
                // Owning Drop stops new child admission (WORKFLOW.md:865–871,
                // 1005–1013). Explicitly acquire and poll the replacement owner;
                // merely listing children cannot create a new execution.
                let final_parent = loop {
                    let mut resumed = workflow.open_run(RunSelector::Latest(run.id().clone())).await.unwrap();
                    let acquired = resumed.execution(ExecutionOptions::default()).await;
                    let execution = match acquired {
                        Ok(execution) => execution,
                        Err(AppError::Busy(_)) => {
                            assert!(observation.calls.lock().unwrap().is_empty());
                            tokio::task::yield_now().await;
                            continue;
                        }
                        Err(error) => panic!("replacement child execution failed: {error}"),
                    };
                    let mut finished = Box::pin(execution.finish());
                    let pending = loop {
                        let children = tokio::select! {
                            result = &mut finished => panic!("replacement owner finished before required child Inputs: {}", result.is_ok()),
                            children = leaves.list_runs(None, NonZeroUsize::new(2).unwrap()) => children.unwrap(),
                        };
                        if let Some(child) = children.into_iter().find(|child|
                            child.interactions.iter().any(|interaction| matches!(interaction, Interaction::Inputs { .. }))) {
                            break child;
                        }
                        assert!(observation.calls.lock().unwrap().is_empty(), "recovery cannot reevaluate lost transient inputs");
                        tokio::task::yield_now().await;
                    };
                    assert!(observation.calls.lock().unwrap().is_empty());
                    let child = leaves.open_run(RunSelector::Latest(pending.id)).await.unwrap();
                    let Interaction::Inputs { sequence, generation, .. } = &pending.interactions[0] else { unreachable!() };
                    child.resolve(*sequence, *generation, Resolution::Inputs(
                        Map::from_iter([("question".into(), json!("explicit rebound input"))]),
                    )).await.unwrap();
                    break finished.await.unwrap();
                };
                assert!(matches!(final_parent.status, RunStatus::Succeeded));
                assert_eq!(final_parent.state.values()["collected"], json!([11]));
                let calls = observation.calls.lock().unwrap();
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].inputs["question"], "explicit rebound input");
                assert!(!calls[0].inputs.contains_key("ignored"), "completed original owner cannot be reconstructed from definition expressions");
            } else {
                let mut next = Box::pin(execution.next());
                tokio::select! {
                    _ = gate.entered.notified() => {},
                    result = &mut next => panic!("original child publication was not held: {}", result.is_ok()),
                }
                assert!(observation.calls.lock().unwrap().is_empty());
                let parent = observer.snapshot().await.unwrap();
                assert_eq!(parent.state.values()["collected"], json!([]));
                assert_eq!(parent.last_action_ordinal, None);
                assert!(parent.outcomes.is_empty());
                drop(next);
                assert_eq!(gate.abandoned.load(Ordering::SeqCst), 0);
                gate.release();
                let final_parent = execution.finish().await.unwrap();
                assert!(matches!(final_parent.status, RunStatus::Succeeded));
                assert_eq!(final_parent.state.values()["collected"], json!([11]));
                assert_eq!(final_parent.last_action_ordinal, Some(0));
                assert_eq!(final_parent.outcomes.len(), 1);
                let calls = observation.calls.lock().unwrap();
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].inputs["question"], "original child input");
                assert_eq!(calls[0].inputs["ignored"], "original private value");
            }
            assert_eq!(observation.constructions.load(Ordering::SeqCst), 1);
            assert_eq!(observation.drops.load(Ordering::SeqCst), 1);
            assert_eq!(observation.external_effects.load(Ordering::SeqCst), 1);
            assert_eq!(gate.abandoned.load(Ordering::SeqCst), 0);
        })).catch_unwind().await;
        gate.release();
        gate.read_key.lock().unwrap().take();
        let closed = tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        )
        .await;
        drop(app);
        match checked {
            Ok(result) => result.expect("original child input receipt proof is bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        closed
            .expect("child input receipt cleanup is bounded")
            .expect("child input receipt cleanup joins");
    }
}

#[tokio::test]
async fn canceled_child_park_observer_keeps_original_transient_inputs() {
    CallerAction::child_transient_receipt(
        PublicationBoundary::ChildPark,
        PublicationPoint::AppliedReceipt,
    )
    .await;
}

#[tokio::test]
async fn canceled_child_admission_observer_keeps_original_transient_inputs() {
    CallerAction::child_transient_receipt(
        PublicationBoundary::ChildAdmission,
        PublicationPoint::AppliedReceipt,
    )
    .await;
}

#[tokio::test]
async fn failed_child_preparation_then_new_execution_rebinds_inputs_without_reevaluation() {
    CallerAction::child_transient_receipt(
        PublicationBoundary::ChildPark,
        PublicationPoint::ChildPreparationRead,
    )
    .await;
}

#[derive(Clone, Copy)]
enum ChildInputBound {
    Exact,
    Exceeded,
}

impl CallerAction {
    fn child_transient_bound(bound: ChildInputBound) {
        const LIMIT: usize = 1_048_576;
        let mut runtime_input = json!({"question":"child question","ignored":""});
        let empty_size = serde_json::to_vec(&vec![runtime_input.clone(); 3])
            .unwrap()
            .len();
        let payload_len =
            (LIMIT - empty_size) / 3 + usize::from(matches!(bound, ChildInputBound::Exceeded));
        runtime_input["ignored"] = json!("x".repeat(payload_len));
        let encoded = serde_json::to_vec(&vec![runtime_input.clone(); 3])
            .unwrap()
            .len();
        assert_eq!(
            encoded,
            LIMIT
                + if matches!(bound, ChildInputBound::Exceeded) {
                    3
                } else {
                    0
                }
        );
        let input = runtime_input
            .as_object()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), json!({"kind":"literal","value":value})))
            .collect::<Map<_, _>>();
        let parent = WorkflowDefinition::parse(
            &json!({
                "schema":"app-sdk.workflow/v2", "id":"input-limit-parent", "revision":"r1",
                "state":{"count":0,"collected":[]}, "required":[],
                "reducers":{"count":"reject_concurrent","collected":"collect"}, "entry":"compose",
                "nodes":[{"id":"compose","deadline_ms":8000,"step":{
                    "kind":"map","workflow":"callable","items":{"kind":"literal","value":[0,1,2]},
                    "input_key":"count","max_parallel":3,"inputs":input,
                    "reductions":[{"source":"count","target":"collected","reducer":"collect"}]
                }}],"routes":{}
            })
            .to_string(),
        )
        .expect("compact authored expression fits its independent document limit");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let observation = Arc::new(Observation::default());
        let store = runtime.block_on(Self::store());
        let config: AppConfig<StateStore, Agent> = AppConfig::new(
            store,
            Vec::new(),
            vec![parent, Self::definition(&["first"])],
        );
        let mut app = runtime
            .block_on(App::open(config.with_actions(vec![Self::new(
                "first",
                &observation,
                Behavior::Increment,
            )])))
            .unwrap();
        let checked = std::panic::catch_unwind(AssertUnwindSafe(|| {
            runtime.block_on(async {
                tokio::time::timeout(Duration::from_secs(10), async {
                    let workspace = app
                        .create_workspace(WorkspaceConfig {
                            name: None,
                            root: std::env::current_dir().unwrap(),
                        })
                        .await
                        .unwrap();
                    let workflow = workspace
                        .workflow("input-limit-parent".parse().unwrap())
                        .await
                        .unwrap();
                    let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
                    let snapshot = run
                        .execution(Self::options())
                        .await
                        .unwrap()
                        .finish()
                        .await
                        .unwrap();
                    let children = workspace
                        .workflow("callable".parse().unwrap())
                        .await
                        .unwrap()
                        .list_runs(None, NonZeroUsize::new(4).unwrap())
                        .await
                        .unwrap();
                    if matches!(bound, ChildInputBound::Exceeded) {
                        assert!(matches!(
                            snapshot.status,
                            RunStatus::Failed(WorkflowFailure::Limit(_))
                        ));
                        assert_eq!(snapshot.state.values()["collected"], json!([]));
                        assert_eq!(snapshot.last_action_ordinal, None);
                        assert!(snapshot.outcomes.is_empty());
                        assert!(
                            children.is_empty(),
                            "the complete resolved batch rejects before any child admission"
                        );
                        assert!(observation.calls.lock().unwrap().is_empty());
                    } else {
                        assert!(matches!(snapshot.status, RunStatus::Succeeded));
                        assert_eq!(snapshot.state.values()["collected"], json!([1, 2, 3]));
                        assert_eq!(snapshot.last_action_ordinal, Some(0));
                        assert_eq!(snapshot.outcomes.len(), 1);
                        assert_eq!(children.len(), 3);
                        let calls = observation.calls.lock().unwrap();
                        assert_eq!(calls.len(), 3);
                        for call in calls.iter() {
                            assert_eq!(call.inputs["ignored"], runtime_input["ignored"]);
                            assert_eq!(call.inputs["question"], runtime_input["question"]);
                        }
                    }
                })
                .await
                .expect("child transient byte boundary execution is bounded")
            })
        }));
        let closed = runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(5),
                app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(3),
                }),
            )
            .await
        });
        drop(app);
        if let Err(panic) = checked {
            std::panic::resume_unwind(panic);
        }
        closed
            .expect("child input limit cleanup is bounded")
            .expect("child input limit cleanup joins");
    }
}

#[test]
fn child_transient_batch_accepts_the_exact_independent_byte_limit() {
    CallerAction::child_transient_bound(ChildInputBound::Exact);
}

#[test]
fn child_transient_batch_rejects_over_limit_before_any_child_effect() {
    CallerAction::child_transient_bound(ChildInputBound::Exceeded);
}

#[tokio::test]
async fn invalid_resolved_child_tasks_reject_the_whole_batch_before_child_admission() {
    let definition = WorkflowDefinition::parse(
        &json!({
            "schema":"app-sdk.workflow/v2", "id":"invalid-task-parent", "revision":"r1",
            "state":{"tasks":[],"collected":[]}, "required":["tasks"],
            "reducers":{"tasks":"reject_concurrent","collected":"collect"},
            "entry":"compose", "nodes":[{"id":"compose","deadline_ms":5000,"step":{
                "kind":"map","tasks":{"kind":"state","path":"tasks"},"max_parallel":2,
                "reductions":[{"source":"count","target":"collected","reducer":"collect"}]
            }}], "routes":{}
        })
        .to_string(),
    )
    .expect("tasks source syntax is compiled before its resolved data exists");
    let observation = Arc::new(Observation::default());
    let store = CallerAction::store().await;
    let config: AppConfig<StateStore, Agent> = AppConfig::new(
        store,
        Vec::new(),
        vec![definition, CallerAction::definition(&["first"])],
    );
    let mut app = App::open(config.with_actions(vec![CallerAction::new(
        "first",
        &observation,
        Behavior::Increment,
    )]))
    .await
    .unwrap();
    let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(12), async {
        let workspace = app.create_workspace(WorkspaceConfig {
            name: None, root: std::env::current_dir().unwrap(),
        }).await.unwrap();
        let workflow = workspace.workflow("invalid-task-parent".parse().unwrap()).await.unwrap();
        let children = workspace.workflow("callable".parse().unwrap()).await.unwrap();
        let valid = json!({"task_key":"valid-first","workflow":"callable","input":{"count":10},
            "inputs":{"question":"must not execute"}});
        for invalid in [
            Value::Null,
            json!({"workflow":"callable","input":{"count":20}}),
            json!({"task_key":false,"workflow":"callable","input":{"count":20}}),
            json!({"task_key":"bad","workflow":"absent","input":{"count":20}}),
            json!({"task_key":"bad","workflow":"callable","revision":"absent","input":{"count":20}}),
            json!({"task_key":"bad","workflow":"callable","input":[]}),
            json!({"task_key":"bad","workflow":"callable","input":{"count":20},"inputs":[]}),
            json!({"task_key":"bad","workflow":"callable","input":{"count":20},"inputs":{"__private":1}}),
            json!({"task_key":"bad","workflow":"callable","input":{"count":20},"history":"erase"}),
            json!({"task_key":"bad","workflow":"callable","input":{"count":20},"unknown":true}),
        ] {
            let tasks = json!([valid, invalid]);
            let mut run = workflow.prepare(WorkflowState::new(Map::from_iter([
                ("tasks".into(), tasks.clone()),
            ])).unwrap()).await.unwrap();
            let failed = run.execution(CallerAction::options()).await.unwrap().finish().await.unwrap();
            assert!(matches!(failed.status, RunStatus::Failed(_)), "invalid resolved task: {tasks}");
            assert_eq!(failed.state.values()["tasks"], tasks);
            assert_eq!(failed.state.values()["collected"], json!([]));
            assert!(failed.outcomes.is_empty() && failed.last_action_ordinal.is_none());
            assert!(children.list_runs(None, NonZeroUsize::new(1).unwrap()).await.unwrap().is_empty(),
                "a valid earlier task cannot admit a partial batch before later validation fails");
            assert_eq!(observation.constructions.load(Ordering::SeqCst), 0);
            assert_eq!(observation.external_effects.load(Ordering::SeqCst), 0);
            assert!(observation.calls.lock().unwrap().is_empty());
        }
    })).catch_unwind().await;
    let closed = tokio::time::timeout(
        Duration::from_secs(5),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        }),
    )
    .await;
    drop(app);
    match checked {
        Ok(result) => result.expect("resolved child task validation remains bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    closed.expect("resolved task cleanup joins").unwrap();
}

#[derive(Clone, Copy)]
enum ChildTaskMetadataBound {
    One,
    Repeated,
}

impl CallerAction {
    fn child_task_metadata_bound(case: ChildTaskMetadataBound) {
        const RECORD_LIMIT: usize = 4_194_304;
        let task_key = "k".repeat(32_768);
        let count = if matches!(case, ChildTaskMetadataBound::Repeated) {
            256
        } else {
            1
        };
        let items = (0..count).collect::<Vec<_>>();
        // Both ordinary vector byte domains fit. Only normalized repeated
        // task metadata exceeds the existing durable-row ceiling.
        assert!(serde_json::to_vec(&items).unwrap().len() < 1_048_576);
        assert!(serde_json::to_vec(&vec![json!({}); count]).unwrap().len() < 1_048_576);
        assert_eq!(
            task_key.len() * count > RECORD_LIMIT,
            matches!(case, ChildTaskMetadataBound::Repeated)
        );
        let document = json!({
            "schema":"app-sdk.workflow/v2", "id":"task-metadata-parent", "revision":"r1",
            "state":{"count":0,"collected":[]}, "required":[],
            "reducers":{"count":"reject_concurrent","collected":"collect"}, "entry":"compose",
            "nodes":[{"id":"compose","deadline_ms":8000,"step":{
                "kind":"map","workflow":"callable","items":{"kind":"literal","value":items},
                "task_key":{"kind":"literal","value":task_key},
                "inputs":{"question":{"kind":"literal","value":"child metadata input"}},
                "input_key":"count","max_parallel":2,
                "reductions":[{"source":"count","target":"collected","reducer":"collect"}]
            }}],"routes":{}
        })
        .to_string();
        assert!(
            document.len() < 1_048_576,
            "the original declaration is independently valid and compact"
        );
        let parent = WorkflowDefinition::parse(&document)
            .expect("authored task keys parse before expansion");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let observation = Arc::new(Observation::default());
        let store = runtime.block_on(Self::store());
        let config: AppConfig<StateStore, Agent> = AppConfig::new(
            store,
            Vec::new(),
            vec![parent, Self::definition(&["first"])],
        );
        let mut app = runtime
            .block_on(App::open(config.with_actions(vec![Self::new(
                "first",
                &observation,
                Behavior::Increment,
            )])))
            .unwrap();
        let checked = std::panic::catch_unwind(AssertUnwindSafe(|| {
            runtime.block_on(async {
                tokio::time::timeout(Duration::from_secs(10), async {
                    let workspace = app
                        .create_workspace(WorkspaceConfig {
                            name: None,
                            root: std::env::current_dir().unwrap(),
                        })
                        .await
                        .unwrap();
                    let workflow = workspace
                        .workflow("task-metadata-parent".parse().unwrap())
                        .await
                        .unwrap();
                    let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
                    let snapshot = run
                        .execution(Self::options())
                        .await
                        .unwrap()
                        .finish()
                        .await
                        .unwrap();
                    let children = workspace
                        .workflow("callable".parse().unwrap())
                        .await
                        .unwrap()
                        .list_runs(None, NonZeroUsize::new(2).unwrap())
                        .await
                        .unwrap();
                    if matches!(case, ChildTaskMetadataBound::Repeated) {
                        assert!(matches!(
                            snapshot.status,
                            RunStatus::Failed(WorkflowFailure::Limit(_))
                        ));
                        assert_eq!(snapshot.state.values()["collected"], json!([]));
                        assert_eq!(snapshot.last_action_ordinal, None);
                        assert!(snapshot.outcomes.is_empty());
                        assert!(
                            children.is_empty(),
                            "oversized resolved metadata cannot admit a fitting prefix"
                        );
                        assert!(observation.calls.lock().unwrap().is_empty());
                    } else {
                        assert!(matches!(snapshot.status, RunStatus::Succeeded));
                        assert_eq!(snapshot.state.values()["collected"], json!([1]));
                        assert_eq!(snapshot.last_action_ordinal, Some(0));
                        assert_eq!(children.len(), 1);
                        let calls = observation.calls.lock().unwrap();
                        assert_eq!(calls.len(), 1);
                        assert_eq!(calls[0].inputs["question"], "child metadata input");
                    }
                })
                .await
                .expect("task metadata bound has a bounded public outcome")
            })
        }));
        let closed = runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(5),
                app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(3),
                }),
            )
            .await
        });
        drop(app);
        if let Err(panic) = checked {
            std::panic::resume_unwind(panic);
        }
        closed
            .expect("metadata bound cleanup is bounded")
            .expect("metadata bound cleanup joins");
    }
}

#[test]
fn one_large_child_task_key_preserves_existing_admission_capacity() {
    CallerAction::child_task_metadata_bound(ChildTaskMetadataBound::One);
}

#[test]
fn repeated_child_task_metadata_rejects_the_entire_oversized_expansion() {
    CallerAction::child_task_metadata_bound(ChildTaskMetadataBound::Repeated);
}

impl PublicationCase {
    async fn initial_acquisition(retry: bool) {
        for point in [
            PublicationPoint::BeforeCommit,
            PublicationPoint::AppliedReceipt,
        ] {
            // Keep the gate disarmed through every real setup transition.
            let mut case = Self::new(PublicationBoundary::Acquisition { retry }, point).await;
            let phase = std::cell::Cell::new("prepare an initial Retry/Pause boundary");
            let checked = AssertUnwindSafe(async {
                tokio::time::timeout(Duration::from_secs(12), async {
                if retry {
                    let failure = case.run.execution(CallerAction::options()).await.unwrap().finish().await;
                    assert!(failure.is_err() || matches!(&failure, Ok(snapshot) if matches!(snapshot.status, RunStatus::Failed(_))), "the original action really fails before retry acquisition");
                    assert!(matches!(case.run.snapshot().await.unwrap().status, RunStatus::Failed(_)));
                } else {
                    let mut options = CallerAction::options();
                    options.halt_before = vec!["node1".into()];
                    let halted = case.run.execution(options).await.unwrap().finish().await.unwrap();
                    assert!(matches!(&halted.status, RunStatus::Halted { node, .. } if node == "node1"),
                        "local setup must reach the actual halt at checkpoint {} with {} interactions",
                        halted.checkpoint, halted.interactions.len());
                    assert_eq!(halted.state.values()["count"], json!(1),
                        "the first action commits before the second action's halt");
                }
                phase.set("capture initial authority");
                let before = case.run.snapshot().await.unwrap();
                let constructions = case.observation.constructions.load(Ordering::SeqCst);
                phase.set("reach the accepted initial publication");
                case.gate.rearm(PublicationBoundary::Acquisition { retry }, point);
                let mut opening = Box::pin(case.run.execution(CallerAction::options()));
                tokio::time::timeout(Duration::from_secs(3), async {
                    tokio::select! {
                        _ = case.gate.entered.notified() => {},
                        result = &mut opening => panic!("initial publication escaped its native gate: {}", result.is_ok()),
                    }
                }).await.expect("initial Retry/Pause publication reaches its real native boundary");
                drop(opening);
                assert_eq!(case.gate.abandoned.load(Ordering::SeqCst), 0,
                    "dropping acquisition observation preserves its accepted transaction future");
                assert_eq!(case.observation.constructions.load(Ordering::SeqCst), constructions,
                    "acquisition does not start or replay the action");
                // Release the native writer before loading or reconciling its run.
                case.gate.release();
                phase.set("reconcile released initial publication");
                tokio::time::timeout(Duration::from_secs(3), async {
                    loop {
                        match case.run.execution(CallerAction::options()).await {
                            Ok(execution) => { drop(execution); break; },
                            Err(AppError::Busy(_)) => tokio::task::yield_now().await,
                            Err(error) => panic!("accepted acquisition did not reconcile: {error}"),
                        }
                    }
                }).await.expect("the original publication settles and releases execution ownership");
                let after = case.run.snapshot().await.unwrap();
                assert_eq!(after.state, before.state);
                assert_eq!(case.gate.abandoned.load(Ordering::SeqCst), 0);
                assert_eq!(case.observation.constructions.load(Ordering::SeqCst), constructions);
                }).await.unwrap_or_else(|_| panic!("initial acquisition stalled during {}", phase.get()));
            }).catch_unwind().await;
            case.finish(checked, false).await;
        }
    }
}

#[tokio::test]
async fn execution_open_retains_initial_pause_publication_after_observer_drop() {
    PublicationCase::initial_acquisition(false).await;
}

#[tokio::test]
async fn execution_open_retains_initial_retry_publication_after_observer_drop() {
    PublicationCase::initial_acquisition(true).await;
}
