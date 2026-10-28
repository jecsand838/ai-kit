//! Serial halt contracts through public execution methods and real LanceDB state.
//!
//! Source-derived oracle from rurr commit 005bfe3ece631e64b06dac2d42ef84235f727a4f,
//! archive SHA-256 5b5f20eb1d233467c600dc9dd249feaaa9046c5a303ae4e937040be5b6ecf25e:
//! application/session.rs:169–210,335–374,489–514; streaming/step_lifecycle.rs:
//! 237–251,441–448; tests/contract/application_run_contract.rs:428–465,546–583;
//! tests/contract/application_streaming_contract/resumed_halts.rs:11–115.
//! App's mixed-method mapping and retained status tuple follow SPECIFICATIONS
//! and WORKFLOW. These are source-derived cases, not executed-rurr evidence.
//! Multiple matching BSP siblings and fresh-process recovery are separate cases.

use std::mem;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use agents_kit::Agent;
use app_kit::workflow::{
    ActionBackend, ActionDeclaration, ActionEvent, ActionMode, ActionRequest, Execution,
    ExecutionEvent, ExecutionOptions, JsonState, LifecycleEvent, LifecycleHooks, NoConditions,
    RunSelector, RunSnapshot, RunStatus, StateChange, SyncExecution, Workflow, WorkflowDefinition,
    WorkflowRun, WorkflowState,
};
use app_kit::{App, AppConfig, AppError, Shutdown, WorkspaceConfig};
use futures_util::{FutureExt, Stream};
use serde_json::{Map, Value, json};
use storage_kit::StateStore;
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
use tokio::sync::futures::OwnedNotified;
use tokio::sync::{Notify, oneshot};

#[derive(Clone, Copy)]
enum Output {
    Counter,
    Empty,
    EofOnce,
    Pending,
}

#[derive(Clone, Copy)]
enum Advance {
    Streaming,
    Ordinary,
}

#[derive(Clone, Copy, Debug)]
enum BlockingMethod {
    Next,
    Step,
    Finish,
    Abort,
}

#[derive(Clone, Debug, PartialEq)]
struct Invocation {
    node: String,
    ordinal: u64,
    mode: ActionMode,
    thread: std::thread::ThreadId,
}

struct CounterAction {
    declaration: ActionDeclaration,
    calls: Arc<Mutex<Vec<Invocation>>>,
    output: Output,
    evidence: Arc<ControlEvidence>,
}

struct CounterSource {
    progress: Progress,
    calls: Arc<Mutex<Vec<Invocation>>>,
    output: Output,
    owner_thread: Rc<std::thread::ThreadId>,
    evidence: Arc<ControlEvidence>,
    pending: Option<Pin<Box<OwnedNotified>>>,
}

enum Progress {
    Ready(Box<ActionRequest>),
    Terminal {
        result: Map<String, Value>,
        writes: Vec<StateChange>,
    },
    Eof,
    Done,
}

struct Controls {
    app: App<StateStore, Agent, CounterAction, NoConditions, JsonState, ControlHooks>,
    workflow: Workflow<StateStore, Agent, CounterAction, NoConditions, JsonState, ControlHooks>,
    calls: Arc<Mutex<Vec<Invocation>>>,
    hooks: Arc<AtomicUsize>,
    async_hooks: Arc<AtomicUsize>,
    evidence: Arc<ControlEvidence>,
}

#[derive(Default)]
struct ControlEvidence {
    sources: Mutex<Vec<(&'static str, std::thread::ThreadId)>>,
    hook_modes: Mutex<Vec<ActionMode>>,
    steps: Mutex<Vec<(String, u64)>>,
    pending: Notify,
    release: Arc<Notify>,
}

impl ControlEvidence {
    fn constructions(&self) -> usize {
        self.sources
            .lock()
            .unwrap()
            .iter()
            .filter(|(event, _)| *event == "construct")
            .count()
    }

    fn source(&self, event: &'static str) {
        self.sources
            .lock()
            .unwrap()
            .push((event, std::thread::current().id()));
    }
}

struct ControlHooks {
    calls: Arc<AtomicUsize>,
    async_calls: Arc<AtomicUsize>,
    evidence: Arc<ControlEvidence>,
}

impl LifecycleHooks for ControlHooks {
    fn observe(&self, event: &LifecycleEvent<'_>) -> Result<(), AppError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let LifecycleEvent::StepStarted { context, .. }
        | LifecycleEvent::StepFinished { context, .. } = event
        {
            self.evidence.hook_modes.lock().unwrap().push(context.mode);
        }
        if let LifecycleEvent::StepStarted { context, .. } = event {
            self.evidence
                .steps
                .lock()
                .unwrap()
                .push((context.node.clone(), context.action_ordinal));
        }
        Ok(())
    }

    async fn observe_async<'a>(&'a self, _event: &'a LifecycleEvent<'a>) -> Result<(), AppError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.async_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl CounterAction {
    fn new(
        calls: &Arc<Mutex<Vec<Invocation>>>,
        output: Output,
        evidence: &Arc<ControlEvidence>,
    ) -> Self {
        Self {
            declaration: ActionDeclaration {
                key: "counter".into(),
                revision: "r1".into(),
                reads: vec!["count".into()],
                writes: vec!["count".into()],
                required_inputs: Vec::new(),
                optional_inputs: Vec::new(),
                bound_inputs: Map::new(),
                default_inputs: Map::new(),
                tags: Vec::new(),
                source: None,
                mode: ActionMode::Both,
            },
            calls: Arc::clone(calls),
            output,
            evidence: Arc::clone(evidence),
        }
    }
}

impl ActionBackend for CounterAction {
    type Stream = CounterSource;

    fn declaration(&self) -> &ActionDeclaration {
        &self.declaration
    }

    fn start(&self, request: ActionRequest) -> Self::Stream {
        self.evidence.source("construct");
        CounterSource {
            progress: Progress::Ready(Box::new(request)),
            calls: Arc::clone(&self.calls),
            output: self.output,
            owner_thread: Rc::new(std::thread::current().id()),
            evidence: Arc::clone(&self.evidence),
            pending: matches!(self.output, Output::Pending)
                .then(|| Box::pin(Arc::clone(&self.evidence.release).notified_owned())),
        }
    }
}

impl Stream for CounterSource {
    type Item = Result<ActionEvent, AppError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        assert_eq!(*self.owner_thread, std::thread::current().id());
        self.evidence.source("poll");
        match mem::replace(&mut self.progress, Progress::Done) {
            Progress::Ready(request) => {
                let first = {
                    let mut calls = self.calls.lock().expect("invocation observations");
                    let first = calls.is_empty();
                    calls.push(Invocation {
                        node: request.context.node.clone(),
                        ordinal: request.context.action_ordinal,
                        mode: request.context.mode,
                        thread: *self.owner_thread,
                    });
                    first
                };
                let count = request.state.values()["count"]
                    .as_u64()
                    .expect("counter state")
                    + 1;
                self.progress = if matches!(self.output, Output::EofOnce) && first {
                    Progress::Eof
                } else {
                    Progress::Terminal {
                        result: if matches!(self.output, Output::Empty) {
                            Map::new()
                        } else {
                            Controls::result(&request.context.node, count)
                        },
                        writes: vec![StateChange::Assign {
                            values: Map::from_iter([("count".into(), json!(count))]),
                        }],
                    }
                };
                self.evidence.source("partial");
                Poll::Ready(Some(Ok(ActionEvent::Partial {
                    result: Map::from_iter([("node".into(), json!(request.context.node))]),
                })))
            }
            Progress::Terminal { result, writes } => {
                if let Some(pending) = self.pending.as_mut() {
                    if pending.as_mut().poll(cx).is_pending() {
                        self.progress = Progress::Terminal { result, writes };
                        self.evidence.source("pending");
                        self.evidence.pending.notify_one();
                        return Poll::Pending;
                    }
                    self.pending = None;
                }
                self.evidence.source("complete");
                Poll::Ready(Some(Ok(ActionEvent::Completed { result, writes })))
            }
            Progress::Eof => Poll::Ready(None),
            Progress::Done => panic!("a closed source must never be polled again"),
        }
    }
}

impl Drop for CounterSource {
    fn drop(&mut self) {
        self.evidence.source("drop");
        assert_eq!(*self.owner_thread, std::thread::current().id());
    }
}

impl Advance {
    async fn commit(
        self,
        execution: &mut Execution<
            '_,
            StateStore,
            Agent,
            CounterAction,
            NoConditions,
            JsonState,
            ControlHooks,
        >,
        node: &str,
        ordinal: u64,
    ) {
        let event = match self {
            Self::Streaming => {
                Controls::partial(execution.next().await.expect("partial event"), node);
                execution.next().await.expect("commit event")
            }
            Self::Ordinary => execution.step().await.expect("drained commit event"),
        };
        Controls::committed(event, node, ordinal);
    }

    fn expected(self) -> (&'static str, Option<Map<String, Value>>) {
        match self {
            Self::Streaming => ("a", Some(Controls::result("a", 1))),
            Self::Ordinary => ("b", None),
        }
    }
}

impl Controls {
    async fn new(nodes: &[&str], cycle: bool, output: Output) -> Self {
        Self::open(Self::definition(nodes, cycle), output).await
    }

    async fn open(definition: WorkflowDefinition, output: Output) -> Self {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let hooks = Arc::new(AtomicUsize::new(0));
        let async_hooks = Arc::new(AtomicUsize::new(0));
        let evidence = Arc::new(ControlEvidence::default());
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("real memory transactions");
        let config: AppConfig<StateStore, Agent> =
            AppConfig::new(store, Vec::new(), vec![definition]);
        let app = App::open(
            config
                .with_actions(vec![CounterAction::new(&calls, output, &evidence)])
                .with_hooks(ControlHooks {
                    calls: Arc::clone(&hooks),
                    async_calls: Arc::clone(&async_hooks),
                    evidence: Arc::clone(&evidence),
                }),
        )
        .await
        .expect("bind the caller-owned counter without constructing an Agent");
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().expect("workspace root"),
            })
            .await
            .expect("App workspace");
        let workflow = workspace
            .workflow("controls".parse().expect("workflow identity"))
            .await
            .expect("configured workflow");
        Self {
            app,
            workflow,
            calls,
            hooks,
            async_hooks,
            evidence,
        }
    }

    fn definition(nodes: &[&str], cycle: bool) -> WorkflowDefinition {
        let routes = nodes
            .iter()
            .enumerate()
            .filter_map(|(index, node)| {
                let target = nodes
                    .get(index + 1)
                    .copied()
                    .or_else(|| cycle.then_some(nodes[0]))?;
                Some((
                    (*node).to_owned(),
                    json!({
                        "mode":"first", "branches":[{"target":{"kind":"node","id":target}}]
                    }),
                ))
            })
            .collect::<Map<_, _>>();
        let nodes = nodes.iter().enumerate().map(|(index, node)| json!({
            "id":node, "tags": if index == 0 { vec!["all", "first"] } else { vec!["all", "later"] },
            "step":{"kind":"action", "action":"counter", "revision":"r1"}
        })).collect::<Vec<_>>();
        WorkflowDefinition::parse(
            &json!({
                "schema":"app-kit.workflow/v2", "id":"controls", "revision":"r1",
                "state":{"count":0}, "required":["count"], "reducers":{"count":"reject_concurrent"},
                "entry":"a", "nodes":nodes, "routes":routes
            })
            .to_string(),
        )
        .expect("serial action graph")
    }

    fn options(before: &[&str], after: &[&str]) -> ExecutionOptions {
        ExecutionOptions {
            state: WorkflowState::default(),
            inputs: Map::new(),
            halt_before: before.iter().map(|value| (*value).into()).collect(),
            halt_after: after.iter().map(|value| (*value).into()).collect(),
            max_steps: None,
        }
    }

    async fn native_halt(maximum: bool) {
        let nodes = ["a", "b", "c"].map(|id| {
            json!({
                "id":id, "step":{"kind":"state", "mutations":[
                    {"kind":"increment", "target":"count", "value":{"kind":"literal", "value":1}}
                ]}
            })
        });
        let definition = WorkflowDefinition::parse(
            &json!({
                "schema":"app-kit.workflow/v2", "id":"controls", "revision":"r1",
                "state":{"count":0}, "required":["count"],
                "reducers":{"count":"reject_concurrent"}, "entry":"a",
                "nodes":nodes,
                "routes":{
                    "a":{"mode":"first", "branches":[{"target":{"kind":"node", "id":"b"}}]},
                    "b":{"mode":"first", "branches":[{"target":{"kind":"node", "id":"c"}}]}
                }
            })
            .to_string(),
        )
        .expect("native serial state graph");
        let mut fixture = Self::open(definition, Output::Counter).await;
        let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(12), async {
            let mut options = Self::options(if maximum { &[] } else { &["b"] }, &[]);
            if maximum {
                options.max_steps = Some(std::num::NonZeroUsize::MIN);
            }
            let run = fixture
                .workflow
                .start(options)
                .await
                .expect("native background admission");
            let halted = tokio::time::timeout(Duration::from_secs(3), run.wait())
                .await
                .expect("native background halt is bounded")
                .expect("native halt snapshot");
            let result = maximum.then(Map::new);
            let node = if maximum { "a" } else { "b" };
            Self::halted_snapshot(&halted, node, &result, 1);
            assert_eq!(halted.outcomes[0].node, "a");
            assert!(halted.outcomes[0].result.is_empty());
            let steps = fixture.evidence.steps.lock().unwrap().clone();
            assert_eq!(steps, vec![("a".into(), 0)]);
            assert_eq!(fixture.evidence.constructions(), 0);
            fixture.calls(&[]);

            // Completion may still own its final receipt when wait exposes the
            // durable halt. Busy cannot alter it or admit another native step.
            let finished = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let mut resumed = fixture
                        .workflow
                        .open_run(RunSelector::Latest(run.id().clone()))
                        .await
                        .unwrap();
                    let acquisition = resumed.execution(Self::options(&[], &[])).await;
                    match acquisition {
                        Ok(execution) => {
                            break execution
                                .finish()
                                .await
                                .expect("explicit native continuation");
                        }
                        Err(AppError::Busy(_)) => {
                            Self::unchanged(&run.snapshot().await.unwrap(), &halted);
                            let steps = fixture.evidence.steps.lock().unwrap().clone();
                            assert_eq!(steps, vec![("a".into(), 0)]);
                            assert_eq!(fixture.evidence.constructions(), 0);
                            tokio::task::yield_now().await;
                        }
                        Err(error) => panic!("unexpected native continuation error: {error}"),
                    }
                }
            })
            .await
            .expect("native ownership handoff and continuation stay bounded");
            assert!(matches!(finished.status, RunStatus::Succeeded));
            assert_eq!(finished.state.values()["count"], 3);
            assert_eq!(finished.last_action_ordinal, Some(2));
            assert_eq!(finished.outcomes.len(), 1);
            assert_eq!(finished.outcomes[0].node, "c");
            assert_eq!(finished.outcomes[0].ordinal, 2);
            assert!(finished.outcomes[0].result.is_empty());
            let steps = fixture.evidence.steps.lock().unwrap().clone();
            assert_eq!(
                steps,
                vec![("a".into(), 0), ("b".into(), 1), ("c".into(), 2)]
            );
            assert_eq!(fixture.evidence.constructions(), 0);
            fixture.calls(&[]);
        }))
        .catch_unwind()
        .await;
        let closed = AssertUnwindSafe(fixture.close()).catch_unwind().await;
        match observed {
            Ok(result) => result.expect("native state halt journey stays bounded"),
            Err(failure) => std::panic::resume_unwind(failure),
        }
        if let Err(failure) = closed {
            std::panic::resume_unwind(failure);
        }
    }

    async fn prepare(
        &self,
    ) -> WorkflowRun<StateStore, Agent, CounterAction, NoConditions, JsonState, ControlHooks> {
        self.workflow
            .prepare(WorkflowState::default())
            .await
            .expect("checkpoint zero")
    }

    async fn seed_a(
        &self,
        run: &mut WorkflowRun<
            StateStore,
            Agent,
            CounterAction,
            NoConditions,
            JsonState,
            ControlHooks,
        >,
    ) {
        let execution = run
            .execution(Self::options(&[], &["a"]))
            .await
            .expect("seed session");
        let snapshot = tokio::time::timeout(Duration::from_secs(3), execution.finish())
            .await
            .expect("bounded first action")
            .expect("seed commit");
        Self::halted_snapshot(&snapshot, "a", &Some(Self::result("a", 1)), 1);
    }

    fn result(node: &str, count: u64) -> Map<String, Value> {
        Map::from_iter([("node".into(), json!(node)), ("count".into(), json!(count))])
    }

    fn partial(event: Option<ExecutionEvent>, expected: &str) {
        assert!(
            matches!(event, Some(ExecutionEvent::Partial { node, result })
            if node == expected && result["node"] == expected)
        );
    }

    fn committed(event: Option<ExecutionEvent>, expected: &str, ordinal: u64) {
        assert!(
            matches!(event, Some(ExecutionEvent::Committed { outcomes, .. })
            if outcomes.len() == 1 && outcomes[0].node == expected && outcomes[0].ordinal == ordinal)
        );
    }

    fn halted_event(
        event: Option<ExecutionEvent>,
        expected: &str,
        result: &Option<Map<String, Value>>,
    ) {
        assert!(
            matches!(event, Some(ExecutionEvent::Halted { node, result: actual })
            if node == expected && &actual == result)
        );
    }

    fn halted_snapshot(
        snapshot: &RunSnapshot,
        expected: &str,
        result: &Option<Map<String, Value>>,
        count: u64,
    ) {
        assert!(
            matches!(&snapshot.status, RunStatus::Halted { node, result: actual }
            if node == expected && actual == result)
        );
        assert_eq!(snapshot.state.values()["count"], count);
        assert_eq!(snapshot.last_action_ordinal, Some(count - 1));
        assert_eq!(
            snapshot.outcomes.len(),
            1,
            "halt cannot fabricate a pending action outcome"
        );
        assert_eq!(snapshot.outcomes[0].ordinal, count - 1);
        assert!(
            snapshot.interactions.is_empty(),
            "halt is not an Input or Unknown interaction"
        );
    }

    fn calls(&self, expected: &[(&str, u64)]) {
        let calls = self.calls.lock().expect("calls").clone();
        assert_eq!(
            calls
                .iter()
                .map(|call| (call.node.as_str(), call.ordinal))
                .collect::<Vec<_>>(),
            expected
        );
    }

    fn unchanged(actual: &RunSnapshot, before: &RunSnapshot) {
        assert_eq!(actual.checkpoint, before.checkpoint);
        assert_eq!(actual.state.values(), before.state.values());
        assert_eq!(actual.last_action_ordinal, before.last_action_ordinal);
        assert_eq!(actual.interactions.len(), before.interactions.len());
        assert_eq!(actual.outcomes.len(), before.outcomes.len());
        for (actual, expected) in actual.outcomes.iter().zip(&before.outcomes) {
            assert_eq!(actual.node, expected.node);
            assert_eq!(actual.ordinal, expected.ordinal);
            assert_eq!(actual.result, expected.result);
        }
        match (&actual.status, &before.status) {
            (RunStatus::Paused, RunStatus::Paused) => {}
            (
                RunStatus::Halted { node, result },
                RunStatus::Halted {
                    node: prior,
                    result: prior_result,
                },
            ) => {
                assert_eq!(node, prior);
                assert_eq!(result, prior_result);
            }
            _ => panic!("rejected initial state changed the saved execution boundary"),
        }
    }

    async fn close(&mut self) {
        tokio::time::timeout(
            Duration::from_secs(5),
            self.app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        )
        .await
        .expect("cleanup observation remains bounded")
        .expect("owned work drains");
    }
}

#[tokio::test]
async fn native_background_halt_before_stops_before_the_next_state_activation() {
    Controls::native_halt(false).await;
}

#[tokio::test]
async fn native_background_max_steps_counts_committed_state_activations() {
    Controls::native_halt(true).await;
}

#[tokio::test]
async fn existing_async_run_rejects_initial_state_before_hooks_source_or_generation_changes() {
    for historical in [false, true] {
        let mut fixture = Controls::new(&["a", "b"], false, Output::Counter).await;
        let observed = AssertUnwindSafe(async {
            let mut run = fixture.prepare().await;
            if historical {
                fixture.seed_a(&mut run).await;
            }
            let before = run.snapshot().await.expect("saved committed boundary");
            let constructions = fixture.evidence.constructions();
            let hooks = fixture.hooks.load(Ordering::SeqCst);
            for values in [
                before.state.values().clone(),
                Map::from_iter([("count".into(), json!(99))]),
                Map::from_iter([("new_key".into(), Value::Null)]),
            ] {
                let mut options = Controls::options(&[], &[]);
                options.state = WorkflowState::new(values).expect("valid nonempty initial state");
                assert!(matches!(
                    run.execution(options).await,
                    Err(AppError::InvalidInput(_))
                ));
                Controls::unchanged(&run.snapshot().await.expect("unchanged saved run"), &before);
                assert_eq!(fixture.evidence.constructions(), constructions);
                assert_eq!(fixture.hooks.load(Ordering::SeqCst), hooks);
            }
            let mut resumed = fixture
                .workflow
                .open_run(RunSelector::Latest(run.id().clone()))
                .await
                .expect("another handle remains usable");
            let execution = resumed
                .execution(Controls::options(&[], &[]))
                .await
                .expect("rejection must not strand an exclusive generation");
            let snapshot = tokio::time::timeout(Duration::from_secs(3), execution.finish())
                .await
                .expect("subsequent empty-state execution stays bounded")
                .expect("complete from the saved position");
            assert!(matches!(snapshot.status, RunStatus::Succeeded));
            assert_eq!(snapshot.state.values()["count"], 2);
            assert_eq!(snapshot.last_action_ordinal, Some(1));
            fixture.calls(&[("a", 0), ("b", 1)]);
            assert_eq!(fixture.evidence.constructions(), 2);
            assert!(fixture.hooks.load(Ordering::SeqCst) > hooks);
        })
        .catch_unwind()
        .await;
        let closed = AssertUnwindSafe(fixture.close()).catch_unwind().await;
        for result in [observed, closed] {
            if let Err(failure) = result {
                std::panic::resume_unwind(failure);
            }
        }
    }
}

#[test]
fn existing_sync_run_rejects_initial_state_before_hooks_source_or_generation_changes() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("caller lower runtime remains driven");
    for historical in [false, true] {
        let mut fixture = runtime.block_on(Controls::new(&["a", "b"], false, Output::Counter));
        let observed = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let mut run = runtime.block_on(fixture.prepare());
            if historical {
                runtime.block_on(fixture.seed_a(&mut run));
            }
            let before = runtime
                .block_on(run.snapshot())
                .expect("saved committed boundary");
            let constructions = fixture.evidence.constructions();
            let hooks = fixture.hooks.load(Ordering::SeqCst);
            for values in [
                before.state.values().clone(),
                Map::from_iter([("count".into(), json!(99))]),
                Map::from_iter([("new_key".into(), Value::Null)]),
            ] {
                let mut options = Controls::options(&[], &[]);
                options.state = WorkflowState::new(values).expect("valid nonempty initial state");
                assert!(matches!(
                    SyncExecution::open(&mut run, options),
                    Err(AppError::InvalidInput(_))
                ));
                Controls::unchanged(
                    &runtime
                        .block_on(run.snapshot())
                        .expect("unchanged saved run"),
                    &before,
                );
                assert_eq!(fixture.evidence.constructions(), constructions);
                assert_eq!(fixture.hooks.load(Ordering::SeqCst), hooks);
            }
            let mut resumed = runtime
                .block_on(
                    fixture
                        .workflow
                        .open_run(RunSelector::Latest(run.id().clone())),
                )
                .expect("another handle remains usable");
            let execution = SyncExecution::open(&mut resumed, Controls::options(&[], &[]))
                .expect("rejection must not strand an exclusive generation");
            let snapshot = execution
                .finish()
                .expect("complete from the saved position");
            assert!(matches!(snapshot.status, RunStatus::Succeeded));
            assert_eq!(snapshot.state.values()["count"], 2);
            assert_eq!(snapshot.last_action_ordinal, Some(1));
            fixture.calls(&[("a", 0), ("b", 1)]);
            let calls = fixture.calls.lock().unwrap().clone();
            assert_eq!(
                calls[0].mode,
                if historical {
                    ActionMode::Asynchronous
                } else {
                    ActionMode::Synchronous
                }
            );
            assert_eq!(calls[1].mode, ActionMode::Synchronous);
            assert_eq!(fixture.evidence.constructions(), 2);
            assert!(fixture.hooks.load(Ordering::SeqCst) > hooks);
        }));
        let closed =
            std::panic::catch_unwind(AssertUnwindSafe(|| runtime.block_on(fixture.close())));
        for result in [observed, closed] {
            if let Err(failure) = result {
                std::panic::resume_unwind(failure);
            }
        }
    }
}

#[tokio::test]
async fn next_and_step_keep_the_two_pinned_halt_orders_and_exact_reopened_status() {
    for advance in [Advance::Streaming, Advance::Ordinary] {
        let mut fixture = Controls::new(&["a", "b"], false, Output::Counter).await;
        let mut run = fixture.prepare().await;
        let mut execution = run
            .execution(Controls::options(&["b"], &["a"]))
            .await
            .expect("execution");
        advance.commit(&mut execution, "a", 0).await;
        let (node, result) = advance.expected();
        Controls::halted_event(
            execution.next().await.expect("retained halt"),
            node,
            &result,
        );
        assert!(
            execution
                .next()
                .await
                .expect("halt event is emitted once")
                .is_none()
        );
        assert!(
            execution
                .step()
                .await
                .expect("changing methods cannot unhalt")
                .is_none()
        );
        let snapshot = execution.finish().await.expect("finish the retained halt");
        Controls::halted_snapshot(&snapshot, node, &result, 1);
        assert_eq!(snapshot.outcomes[0].node, "a");
        assert_eq!(snapshot.outcomes[0].result, Controls::result("a", 1));
        let reopened = fixture
            .workflow
            .open_run(RunSelector::Latest(run.id().clone()))
            .await
            .expect("new run handle");
        let readback = reopened.snapshot().await.expect("checkpoint readback");
        Controls::halted_snapshot(&readback, node, &result, 1);
        assert_eq!(readback.checkpoint, snapshot.checkpoint);
        fixture.calls(&[("a", 0)]);
        fixture.close().await;
    }
}

#[tokio::test]
async fn finish_and_background_wait_return_the_pending_halt_tuple_without_fabricating_an_outcome() {
    for background in [false, true] {
        let mut fixture = Controls::new(&["a", "b"], false, Output::Counter).await;
        let snapshot = if background {
            let run = fixture
                .workflow
                .start(Controls::options(&["b"], &["a"]))
                .await
                .expect("background admission");
            tokio::time::timeout(Duration::from_secs(3), run.wait())
                .await
                .expect("bounded halt")
                .expect("background snapshot")
        } else {
            let mut run = fixture.prepare().await;
            let execution = run
                .execution(Controls::options(&["b"], &["a"]))
                .await
                .expect("foreground admission");
            tokio::time::timeout(Duration::from_secs(3), execution.finish())
                .await
                .expect("bounded halt")
                .expect("finish snapshot")
        };
        Controls::halted_snapshot(&snapshot, "b", &None, 1);
        assert_eq!(snapshot.outcomes[0].node, "a");
        fixture.calls(&[("a", 0)]);
        fixture.close().await;
    }
}

#[tokio::test]
async fn historical_halts_do_not_suppress_the_first_current_session_action() {
    for advance in [Advance::Streaming, Advance::Ordinary] {
        for (before, after) in [
            (vec!["b"], vec![]),
            (vec![], vec!["a"]),
            (vec!["b"], vec!["a"]),
        ] {
            let mut fixture = Controls::new(&["a", "b"], false, Output::Counter).await;
            let mut run = fixture.prepare().await;
            fixture.seed_a(&mut run).await;
            let mut resumed = run
                .execution(Controls::options(&before, &after))
                .await
                .expect("new execution session");
            advance.commit(&mut resumed, "b", 1).await;
            assert!(resumed.next().await.expect("terminal frontier").is_none());
            let snapshot = resumed.finish().await.expect("resumed completion");
            assert!(matches!(snapshot.status, RunStatus::Succeeded));
            assert_eq!(snapshot.state.values()["count"], 2);
            assert_eq!(snapshot.last_action_ordinal, Some(1));
            fixture.calls(&[("a", 0), ("b", 1)]);
            fixture.close().await;
        }
    }
}

#[tokio::test]
async fn resumed_halts_become_active_after_the_first_current_session_commit() {
    for advance in [Advance::Streaming, Advance::Ordinary] {
        let mut fixture = Controls::new(&["a", "b", "c"], false, Output::Counter).await;
        let mut run = fixture.prepare().await;
        fixture.seed_a(&mut run).await;
        let mut resumed = run
            .execution(Controls::options(&["c"], &["b"]))
            .await
            .expect("resumed session");
        advance.commit(&mut resumed, "b", 1).await;
        let (node, result) = match advance {
            Advance::Streaming => ("b", Some(Controls::result("b", 2))),
            Advance::Ordinary => ("c", None),
        };
        Controls::halted_event(
            resumed
                .next()
                .await
                .expect("halt after this session's commit"),
            node,
            &result,
        );
        let snapshot = resumed.finish().await.expect("retained halt");
        Controls::halted_snapshot(&snapshot, node, &result, 2);
        fixture.calls(&[("a", 0), ("b", 1)]);
        fixture.close().await;
    }
}

#[tokio::test]
async fn the_method_committing_a_partially_observed_action_selects_and_then_retains_halt_order() {
    for ordinary_finish in [false, true] {
        let mut fixture = Controls::new(&["a", "b"], false, Output::Counter).await;
        let mut run = fixture.prepare().await;
        let mut execution = run
            .execution(Controls::options(&["b"], &["a"]))
            .await
            .expect("execution");
        Controls::partial(
            execution.next().await.expect("initial streaming partial"),
            "a",
        );
        let snapshot = if ordinary_finish {
            execution
                .finish()
                .await
                .expect("ordinary finish commits the boundary")
        } else {
            Controls::committed(
                execution
                    .step()
                    .await
                    .expect("ordinary step commits the boundary"),
                "a",
                0,
            );
            Controls::halted_event(
                execution
                    .next()
                    .await
                    .expect("streaming next cannot reclassify it"),
                "b",
                &None,
            );
            execution
                .finish()
                .await
                .expect("finish keeps pending-node halt")
        };
        Controls::halted_snapshot(&snapshot, "b", &None, 1);
        fixture.calls(&[("a", 0)]);
        fixture.close().await;
    }

    let mut fixture = Controls::new(&["a", "b"], false, Output::Counter).await;
    let mut run = fixture.prepare().await;
    let mut execution = run
        .execution(Controls::options(&["b"], &["a"]))
        .await
        .expect("execution");
    Advance::Streaming.commit(&mut execution, "a", 0).await;
    let result = Some(Controls::result("a", 1));
    Controls::halted_event(
        execution
            .step()
            .await
            .expect("ordinary step cannot reclassify retained streaming halt"),
        "a",
        &result,
    );
    Controls::halted_snapshot(
        &execution.finish().await.expect("retained decision"),
        "a",
        &result,
        1,
    );
    fixture.calls(&[("a", 0)]);
    fixture.close().await;
}

#[tokio::test]
async fn tag_expansion_and_duplicate_selectors_preserve_the_same_serial_boundaries() {
    for advance in [Advance::Streaming, Advance::Ordinary] {
        let mut fixture = Controls::new(&["a", "b", "c"], false, Output::Counter).await;
        let mut run = fixture.prepare().await;
        let mut execution = run
            .execution(Controls::options(
                &["@later", "b", "@later"],
                &["@first", "a", "@first"],
            ))
            .await
            .expect("ordered tag selectors");
        advance.commit(&mut execution, "a", 0).await;
        let (node, result) = advance.expected();
        Controls::halted_event(
            execution.next().await.expect("selected tagged boundary"),
            node,
            &result,
        );
        Controls::halted_snapshot(
            &execution.finish().await.expect("tagged halt"),
            node,
            &result,
            1,
        );
        fixture.calls(&[("a", 0)]);
        fixture.close().await;
    }
}

#[tokio::test]
async fn unknown_explicit_and_tag_selectors_fail_before_any_action_is_polled() {
    for selector in ["missing", "@missing"] {
        for before in [false, true] {
            let mut fixture = Controls::new(&["a", "b"], false, Output::Counter).await;
            let mut run = fixture.prepare().await;
            let options = if before {
                Controls::options(&[selector], &[])
            } else {
                Controls::options(&[], &[selector])
            };
            match run.execution(options).await {
                Err(error) => assert!(matches!(error, AppError::InvalidInput(_))),
                Ok(mut execution) => assert!(matches!(
                    execution.next().await,
                    Err(AppError::InvalidInput(_))
                )),
            }
            let snapshot = run.snapshot().await.expect("no-effect state");
            assert_eq!(snapshot.state.values()["count"], 0);
            assert!(snapshot.last_action_ordinal.is_none());
            assert!(snapshot.outcomes.is_empty());
            fixture.calls(&[]);
            fixture.close().await;
        }
    }
}

#[tokio::test]
async fn first_entry_bypass_distinguishes_a_self_loop_from_an_exhausted_frontier() {
    for advance in [Advance::Streaming, Advance::Ordinary] {
        for cycle in [false, true] {
            let mut fixture = Controls::new(&["a"], cycle, Output::Counter).await;
            let mut run = fixture.prepare().await;
            let mut execution = run
                .execution(Controls::options(&["a"], &[]))
                .await
                .expect("first-entry selector");
            advance.commit(&mut execution, "a", 0).await;
            if cycle {
                Controls::halted_event(
                    execution.next().await.expect("halt before repeated action"),
                    "a",
                    &None,
                );
                Controls::halted_snapshot(
                    &execution.finish().await.expect("retained loop halt"),
                    "a",
                    &None,
                    1,
                );
            } else {
                assert!(
                    execution
                        .next()
                        .await
                        .expect("no next action exists")
                        .is_none()
                );
                let snapshot = execution.finish().await.expect("terminal completion");
                assert!(matches!(snapshot.status, RunStatus::Succeeded));
                assert_eq!(snapshot.state.values()["count"], 1);
                assert_eq!(snapshot.last_action_ordinal, Some(0));
            }
            fixture.calls(&[("a", 0)]);
            fixture.close().await;
        }
    }
}

#[tokio::test]
async fn halt_after_an_exhausted_action_preserves_some_empty_result() {
    for advance in [Advance::Streaming, Advance::Ordinary] {
        let mut fixture = Controls::new(&["a"], false, Output::Empty).await;
        let mut run = fixture.prepare().await;
        let mut execution = run
            .execution(Controls::options(&["a"], &["a"]))
            .await
            .expect("terminal halt selectors");
        advance.commit(&mut execution, "a", 0).await;
        let result = Some(Map::new());
        Controls::halted_event(
            execution
                .next()
                .await
                .expect("after wins when no pending action exists"),
            "a",
            &result,
        );
        Controls::halted_snapshot(
            &execution
                .finish()
                .await
                .expect("empty result is not absent"),
            "a",
            &result,
            1,
        );
        fixture.calls(&[("a", 0)]);
        fixture.close().await;
    }
}

#[tokio::test]
async fn partial_then_eof_does_not_enable_historical_halts_or_advance_position() {
    let mut fixture = Controls::new(&["a", "b"], false, Output::EofOnce).await;
    let mut run = fixture.prepare().await;
    let observer = fixture
        .workflow
        .open_run(RunSelector::Latest(run.id().clone()))
        .await
        .expect("read-only observer handle");
    let mut execution = run
        .execution(Controls::options(&["a"], &[]))
        .await
        .expect("session with first-action halt-before");
    Controls::partial(execution.next().await.expect("uncommitted partial"), "a");
    assert!(
        execution
            .next()
            .await
            .expect("successful no-output EOF")
            .is_none()
    );
    let snapshot = observer.snapshot().await.expect("EOF rollback readback");
    assert_eq!(snapshot.state.values()["count"], 0);
    assert!(snapshot.last_action_ordinal.is_none());
    assert!(snapshot.outcomes.is_empty());
    assert!(!matches!(
        snapshot.status,
        RunStatus::Halted { .. } | RunStatus::Failed(_)
    ));
    // This explicit advancement retries the saved position; no hidden drain
    // after EOF is allowed to construct or commit that second source for us.
    fixture.calls(&[("a", 0)]);
    Advance::Streaming.commit(&mut execution, "a", 0).await;
    Advance::Streaming.commit(&mut execution, "b", 1).await;
    let snapshot = execution
        .finish()
        .await
        .expect("explicit retry and remaining action complete");
    assert!(matches!(snapshot.status, RunStatus::Succeeded));
    fixture.calls(&[("a", 0), ("a", 0), ("b", 1)]);
    fixture.close().await;
}

#[test]
fn synchronous_next_and_step_use_the_same_two_halt_orders_outside_a_tokio_task() {
    // The multi-thread runtime remains driven while this ordinary test thread
    // invokes the sync facade. No sync operation runs inside Runtime::block_on.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("still-driven runtime");
    for advance in [Advance::Streaming, Advance::Ordinary] {
        let mut fixture = runtime.block_on(Controls::new(&["a", "b"], false, Output::Counter));
        let observed = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let mut run = runtime.block_on(fixture.prepare());
            let asynchronous = fixture.async_hooks.load(Ordering::SeqCst);
            let hooks = fixture.hooks.load(Ordering::SeqCst);
            let mut execution = SyncExecution::open(&mut run, Controls::options(&["b"], &["a"]))
                .expect("sync execution outside runtime task");
            assert_eq!(fixture.evidence.constructions(), 0);
            let event = match advance {
                Advance::Streaming => {
                    Controls::partial(execution.try_next().expect("sync partial"), "a");
                    execution.try_next().expect("sync commit")
                }
                Advance::Ordinary => execution.step().expect("sync drained commit"),
            };
            Controls::committed(event, "a", 0);
            let (node, result) = advance.expected();
            Controls::halted_event(
                execution.try_next().expect("sync retained halt"),
                node,
                &result,
            );
            assert!(execution.step().expect("sync halt is fused").is_none());
            Controls::halted_snapshot(
                &execution.finish().expect("sync finalization"),
                node,
                &result,
                1,
            );
            fixture.calls(&[("a", 0)]);
            let calls = fixture.calls.lock().unwrap().clone();
            assert!(calls.iter().all(|call| call.mode == ActionMode::Synchronous
                && call.thread == std::thread::current().id()));
            assert!(fixture.hooks.load(Ordering::SeqCst) > hooks);
            assert_eq!(fixture.async_hooks.load(Ordering::SeqCst), asynchronous);
        }));
        let closed = runtime.block_on(AssertUnwindSafe(fixture.close()).catch_unwind());
        if let Err(panic) = observed {
            std::panic::resume_unwind(panic);
        }
        closed.expect("sync cleanup does not panic");
    }
}

#[test]
fn synchronous_resumption_and_partial_to_finish_preserve_first_session_and_ordinary_rules() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("still-driven runtime");
    let mut fixture = runtime.block_on(Controls::new(&["a", "b", "c"], false, Output::Counter));
    let observed = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let mut run = runtime.block_on(fixture.prepare());
        let asynchronous = fixture.async_hooks.load(Ordering::SeqCst);
        let mut initial =
            SyncExecution::open(&mut run, Controls::options(&["@later"], &["@first"]))
                .expect("sync execution");
        Controls::partial(initial.try_next().expect("sync partial"), "a");
        Controls::halted_snapshot(
            &initial.finish().expect("finish selects ordinary order"),
            "b",
            &None,
            1,
        );
        let mut resumed =
            SyncExecution::open(&mut run, Controls::options(&["b", "c"], &["a", "b"]))
                .expect("new sync session");
        Controls::partial(
            resumed
                .try_next()
                .expect("historical halt cannot suppress B"),
            "b",
        );
        Controls::committed(
            resumed.try_next().expect("first current-session commit"),
            "b",
            1,
        );
        let result = Some(Controls::result("b", 2));
        Controls::halted_event(
            resumed
                .step()
                .expect("retained streaming choice cannot be reclassified"),
            "b",
            &result,
        );
        Controls::halted_snapshot(
            &resumed.finish().expect("sync retained output"),
            "b",
            &result,
            2,
        );
        fixture.calls(&[("a", 0), ("b", 1)]);
        let calls = fixture.calls.lock().unwrap().clone();
        assert!(calls.iter().all(|call| call.mode == ActionMode::Synchronous
            && call.thread == std::thread::current().id()));
        assert_eq!(fixture.async_hooks.load(Ordering::SeqCst), asynchronous);
    }));
    let closed = runtime.block_on(AssertUnwindSafe(fixture.close()).catch_unwind());
    if let Err(panic) = observed {
        std::panic::resume_unwind(panic);
    }
    closed.expect("sync cleanup does not panic");
}

#[test]
fn every_sync_method_rejects_entered_context_without_polling_or_committing() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    for method in [
        BlockingMethod::Next,
        BlockingMethod::Step,
        BlockingMethod::Finish,
        BlockingMethod::Abort,
    ] {
        let mut fixture = runtime.block_on(Controls::new(&["a"], false, Output::Counter));
        let observed = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let mut run = runtime.block_on(fixture.prepare());
            let id = run.id().clone();
            let observer = runtime
                .block_on(fixture.workflow.open_run(RunSelector::Latest(id.clone())))
                .unwrap();
            let mut execution = SyncExecution::open(&mut run, Controls::options(&[], &[])).unwrap();
            Controls::partial(execution.try_next().unwrap(), "a");
            let before = runtime.block_on(observer.snapshot()).unwrap();
            let sources = fixture.evidence.sources.lock().unwrap().clone();
            let hooks = fixture.hooks.load(Ordering::SeqCst);
            let asynchronous = fixture.async_hooks.load(Ordering::SeqCst);
            assert_eq!(fixture.evidence.constructions(), 1);
            assert_eq!(before.state.values()["count"], 0);
            assert!(before.last_action_ordinal.is_none() && before.outcomes.is_empty());

            let snapshot = match method {
                BlockingMethod::Next | BlockingMethod::Step => {
                    let rejected = {
                        // Merely entering a runtime is enough: this is not
                        // limited to calls currently polled by a Tokio task.
                        let _entered = runtime.enter();
                        match method {
                            BlockingMethod::Next => execution.try_next(),
                            BlockingMethod::Step => execution.step(),
                            _ => unreachable!(),
                        }
                    };
                    assert!(
                        matches!(rejected, Err(AppError::InvalidInput(_))),
                        "{method:?}"
                    );
                    let after_sources = fixture.evidence.sources.lock().unwrap().clone();
                    assert_eq!(
                        after_sources, sources,
                        "rejection cannot poll or drop the retained source"
                    );
                    assert_eq!(fixture.hooks.load(Ordering::SeqCst), hooks);
                    Controls::unchanged(&runtime.block_on(observer.snapshot()).unwrap(), &before);
                    // A borrowed rejection keeps the exact existing source.
                    execution
                        .finish()
                        .expect("same owning session remains usable")
                }
                BlockingMethod::Finish | BlockingMethod::Abort => {
                    let rejected = {
                        let _entered = runtime.enter();
                        match method {
                            BlockingMethod::Finish => execution.finish(),
                            BlockingMethod::Abort => execution.abort(),
                            _ => unreachable!(),
                        }
                    };
                    assert!(
                        matches!(rejected, Err(AppError::InvalidInput(_))),
                        "{method:?}"
                    );
                    let after_sources = fixture.evidence.sources.lock().unwrap().clone();
                    let mut dropped = sources.clone();
                    dropped.push(("drop", std::thread::current().id()));
                    assert_eq!(
                        after_sources, dropped,
                        "consuming rejection only drops the uncommitted source"
                    );
                    assert_eq!(fixture.async_hooks.load(Ordering::SeqCst), asynchronous);
                    // Consuming rejection may revoke the generation. The
                    // original App cleanup may briefly retain its reservation.
                    let deadline = std::time::Instant::now() + Duration::from_secs(3);
                    loop {
                        let saved = runtime.block_on(observer.snapshot()).unwrap();
                        assert_eq!(saved.state.values(), before.state.values());
                        assert!(saved.last_action_ordinal.is_none() && saved.outcomes.is_empty());
                        assert_eq!(fixture.evidence.constructions(), 1);
                        let mut resumed = runtime
                            .block_on(fixture.workflow.open_run(RunSelector::Latest(id.clone())))
                            .unwrap();
                        match SyncExecution::open(&mut resumed, Controls::options(&[], &[])) {
                            Ok(execution) => {
                                break execution
                                    .finish()
                                    .expect("a fresh session uses the unchanged saved position");
                            }
                            Err(AppError::Busy(_)) => {
                                assert!(
                                    std::time::Instant::now() < deadline,
                                    "consumed rejection retains ownership past bounded cleanup"
                                );
                                let current = fixture.evidence.sources.lock().unwrap().clone();
                                assert_eq!(
                                    current, dropped,
                                    "handoff cannot replay the abandoned source"
                                );
                                std::thread::yield_now();
                            }
                            Err(error) => {
                                panic!("unexpected post-rejection acquisition failure: {error}")
                            }
                        }
                    }
                }
            };
            assert!(matches!(snapshot.status, RunStatus::Succeeded));
            assert_eq!(snapshot.state.values()["count"], 1);
            assert_eq!(snapshot.last_action_ordinal, Some(0));
            assert_eq!(snapshot.outcomes.len(), 1);
            let attempts = if matches!(method, BlockingMethod::Finish | BlockingMethod::Abort) {
                2
            } else {
                1
            };
            assert_eq!(fixture.evidence.constructions(), attempts);
            let calls = fixture.calls.lock().unwrap().clone();
            assert_eq!(calls.len(), attempts);
            assert!(calls.iter().all(|call| call.node == "a"
                && call.ordinal == 0
                && call.mode == ActionMode::Synchronous
                && call.thread == std::thread::current().id()));
            let modes = fixture.evidence.hook_modes.lock().unwrap().clone();
            assert!(!modes.is_empty() && modes.iter().all(|mode| *mode == ActionMode::Synchronous));
            assert_eq!(fixture.async_hooks.load(Ordering::SeqCst), asynchronous);
        }));
        let closed = runtime.block_on(AssertUnwindSafe(fixture.close()).catch_unwind());
        if let Err(panic) = observed {
            std::panic::resume_unwind(panic);
        }
        closed.expect("rejection cleanup does not panic");
    }
}

#[test]
fn synchronous_abort_discards_partial_work_and_releases_the_run_for_explicit_resumption() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("caller lower runtime remains driven");
    for committed_prefix in [false, true] {
        let mut fixture = runtime.block_on(Controls::new(&["a", "b"], false, Output::Counter));
        let observed = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let caller = std::thread::current().id();
            let mut run = runtime.block_on(fixture.prepare());
            let observer = runtime
                .block_on(
                    fixture
                        .workflow
                        .open_run(RunSelector::Latest(run.id().clone())),
                )
                .unwrap();
            let asynchronous = fixture.async_hooks.load(Ordering::SeqCst);
            let mut execution = SyncExecution::open(&mut run, Controls::options(&[], &[])).unwrap();
            assert_eq!(fixture.evidence.constructions(), 0);
            if committed_prefix {
                Controls::committed(execution.step().unwrap(), "a", 0);
            }
            let node = if committed_prefix { "b" } else { "a" };
            Controls::partial(execution.try_next().unwrap(), node);
            let before = runtime.block_on(observer.snapshot()).unwrap();
            let sources = fixture.evidence.sources.lock().unwrap().clone();
            let hooks = fixture.hooks.load(Ordering::SeqCst);
            assert_eq!(before.state.values()["count"], u64::from(committed_prefix));
            assert_eq!(before.last_action_ordinal, committed_prefix.then_some(0));
            assert_eq!(before.outcomes.len(), usize::from(committed_prefix));

            // Abort returns on this ordinary thread. It must drop the retained
            // partial source without polling its ready terminal writes.
            let paused = execution.abort().expect("successful synchronous abort");
            assert!(matches!(paused.status, RunStatus::Paused));
            assert_eq!(paused.state, before.state);
            assert_eq!(paused.last_action_ordinal, before.last_action_ordinal);
            assert!(paused.outcomes == before.outcomes);
            assert!(paused.interactions.is_empty());
            let mut dropped = sources;
            dropped.push(("drop", caller));
            assert_eq!(fixture.evidence.sources.lock().unwrap().as_slice(), dropped);
            assert_eq!(fixture.hooks.load(Ordering::SeqCst), hooks + 3);
            assert_eq!(fixture.async_hooks.load(Ordering::SeqCst), asynchronous);
            let modes = fixture.evidence.hook_modes.lock().unwrap().clone();
            assert_eq!(
                modes,
                vec![ActionMode::Synchronous; 2 + 2 * usize::from(committed_prefix)]
            );
            // The consumed owner releases the original mutable run borrow.
            Controls::unchanged(&runtime.block_on(run.snapshot()).unwrap(), &paused);

            let complete = SyncExecution::open(&mut run, Controls::options(&[], &[]))
                .expect("abort releases the original execution ownership")
                .finish()
                .expect("explicit continuation completes from the saved boundary");
            assert!(matches!(complete.status, RunStatus::Succeeded));
            assert_eq!(complete.state.values()["count"], 2);
            assert_eq!(complete.last_action_ordinal, Some(1));
            assert_eq!(fixture.async_hooks.load(Ordering::SeqCst), asynchronous);
        }));
        let closed = runtime.block_on(AssertUnwindSafe(fixture.close()).catch_unwind());
        if let Err(panic) = observed {
            std::panic::resume_unwind(panic);
        }
        closed.expect("synchronous abort cleanup joins");
    }
}

#[test]
fn synchronous_pending_source_stays_on_caller_with_driven_current_thread_runtime() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let handle = runtime.handle().clone();
    let mut fixture = runtime.block_on(Controls::new(&["a"], false, Output::Pending));
    let caller = std::thread::current().id();
    // This is the caller's lower runtime, not a SyncExecution-created thread.
    // Its real LanceDB/App operations stay on this same captured runtime.
    std::thread::scope(|scope| {
        let (stop, stopped) = oneshot::channel();
        let driver = scope.spawn(|| {
            runtime.block_on(async {
                let _ = stopped.await;
            })
        });
        let evidence = Arc::clone(&fixture.evidence);
        let mut release = handle.spawn(async move {
            let pending =
                tokio::time::timeout(Duration::from_secs(3), evidence.pending.notified()).await;
            evidence.release.notify_one();
            pending.expect("real source must return Pending before it is released");
        });
        let observed = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let mut run = handle.block_on(fixture.prepare());
            let observer = handle
                .block_on(
                    fixture
                        .workflow
                        .open_run(RunSelector::Latest(run.id().clone())),
                )
                .unwrap();
            let asynchronous = fixture.async_hooks.load(Ordering::SeqCst);
            let mut execution = SyncExecution::open(&mut run, Controls::options(&[], &[])).unwrap();
            Controls::partial(execution.try_next().unwrap(), "a");
            let partial = handle.block_on(observer.snapshot()).unwrap();
            assert_eq!(partial.state.values()["count"], 0);
            assert!(partial.last_action_ordinal.is_none() && partial.outcomes.is_empty());
            Controls::committed(
                execution
                    .step()
                    .expect("pending local source wakes on the captured runtime"),
                "a",
                0,
            );
            let snapshot = execution.finish().unwrap();
            assert!(matches!(snapshot.status, RunStatus::Succeeded));
            assert_eq!(snapshot.state.values()["count"], 1);
            assert_eq!(snapshot.last_action_ordinal, Some(0));
            assert_eq!(snapshot.outcomes.len(), 1);
            fixture.calls(&[("a", 0)]);
            let sources = fixture.evidence.sources.lock().unwrap().clone();
            assert!(sources.iter().all(|(_, thread)| *thread == caller));
            assert_eq!(sources.first().unwrap().0, "construct");
            assert_eq!(sources.last().unwrap().0, "drop");
            for event in ["construct", "partial", "complete", "drop"] {
                assert_eq!(
                    sources.iter().filter(|(name, _)| *name == event).count(),
                    1,
                    "{event}"
                );
            }
            let pending = sources
                .iter()
                .position(|(event, _)| *event == "pending")
                .expect("a real Pending poll");
            let complete = sources
                .iter()
                .position(|(event, _)| *event == "complete")
                .unwrap();
            assert!(pending < complete);
            let calls = fixture.calls.lock().unwrap().clone();
            assert_eq!(calls[0].mode, ActionMode::Synchronous);
            let modes = fixture.evidence.hook_modes.lock().unwrap().clone();
            assert_eq!(modes, [ActionMode::Synchronous, ActionMode::Synchronous]);
            assert_eq!(fixture.async_hooks.load(Ordering::SeqCst), asynchronous);
        }));
        fixture.evidence.release.notify_one();
        let released = handle.block_on(async {
            match tokio::time::timeout(Duration::from_secs(4), &mut release).await {
                Ok(result) => result,
                Err(_) => {
                    release.abort();
                    release.await
                }
            }
        });
        let closed = handle.block_on(AssertUnwindSafe(fixture.close()).catch_unwind());
        // Keep the runtime driven until both the App and test-owned wake task
        // have joined, even when a body assertion or cleanup fails.
        let _ = stop.send(());
        let joined = driver.join();
        if let Err(panic) = observed {
            std::panic::resume_unwind(panic);
        }
        released.expect("source release task joins without panic or cancellation");
        closed.expect("current-thread App cleanup does not panic");
        joined.expect("caller-owned runtime driver joins");
    });
}

#[tokio::test]
async fn synchronous_execution_inside_a_tokio_task_rejects_before_admission() {
    let mut case = Controls::new(&["a"], false, Output::Counter).await;
    let mut run = case.prepare().await;
    let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), async {
        let before = run.snapshot().await.unwrap();
        let hooks = case.hooks.load(Ordering::SeqCst);
        assert!(SyncExecution::open(&mut run, Controls::options(&[], &[])).is_err());
        Controls::unchanged(&run.snapshot().await.unwrap(), &before);
        assert_eq!(case.hooks.load(Ordering::SeqCst), hooks);
        assert_eq!(case.evidence.constructions(), 0);
        let completed = run
            .execution(Controls::options(&[], &[]))
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        assert!(matches!(completed.status, RunStatus::Succeeded));
        case.calls(&[("a", 0)]);
        let calls = case.calls.lock().unwrap().clone();
        assert_eq!(calls[0].mode, ActionMode::Asynchronous);
    }))
    .catch_unwind()
    .await;
    let closed = AssertUnwindSafe(case.close()).catch_unwind().await;
    drop(case);
    match observed {
        Ok(result) => result.expect("rejected sync acquisition and async continuation are bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    closed.expect("rejected sync acquisition leaves cleanup usable");
}
