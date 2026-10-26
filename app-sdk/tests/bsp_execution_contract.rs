//! App-native BSP halt extension, not a rurr BSP claim. The single-action
//! projection keeps WORKFLOW's pinned ordinary/streaming halt precedence.
//! Gates force both sibling admissions and either exact terminal order; no
//! sleep or completion-order assumption selects the canonical halt winner.

use std::collections::{BTreeMap, BTreeSet};
use std::mem;
use std::panic::{AssertUnwindSafe, resume_unwind};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use agent_sdk::Agent;
use app_sdk::workflow::{
    ActionBackend, ActionContext, ActionDeclaration, ActionEvent, ActionMode, ActionRequest,
    Checkpoint, CheckpointKind, CheckpointSelector, Execution, ExecutionEvent, ExecutionOptions,
    Interaction, JsonState, LifecycleEvent, LifecycleHooks, NoConditions, Resolution, RunSelector,
    RunSnapshot, RunStatus, StateChange, Workflow, WorkflowDefinition, WorkflowFailure,
    WorkflowState,
};
use app_sdk::{App, AppConfig, AppError, Shutdown, WorkspaceConfig, WorkspaceId};
use futures_util::{FutureExt, Stream};
use serde_json::{Map, Value, json};
use storage_sdk::StateStore;
use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
use tokio::sync::Notify;

#[derive(Default)]
struct WaveEvidence {
    constructed: Vec<String>,
    started: Vec<String>,
    completed: Vec<String>,
    terminals: Vec<(String, SourceEnding)>,
    endings: BTreeMap<String, SourceEnding>,
    dropped: Vec<String>,
    observations: Vec<Value>,
    released: BTreeSet<String>,
    held_terminals: BTreeSet<String>,
    held_hook: Option<Arc<HeldHook>>,
    wakers: BTreeMap<String, Waker>,
}

struct HeldHook {
    entered: Notify,
    release: Notify,
    finished: AtomicUsize,
    fail: bool,
    item: bool,
}

struct WaveAction {
    declaration: ActionDeclaration,
    evidence: Arc<Mutex<WaveEvidence>>,
}
struct WaveSource {
    context: ActionContext,
    phase: SourcePhase,
    evidence: Arc<Mutex<WaveEvidence>>,
}
enum SourcePhase {
    Ready,
    Waiting,
    Terminal,
    Closed,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SourceEnding {
    Complete,
    Error,
    Eof,
}
#[derive(Clone, Copy)]
enum Lane {
    Streaming,
    Ordinary,
}

impl WaveAction {
    fn new(evidence: &Arc<Mutex<WaveEvidence>>) -> Self {
        Self {
            declaration: ActionDeclaration {
                key: "wave".into(),
                revision: "r1".into(),
                reads: Vec::new(),
                writes: vec!["values".into()],
                required_inputs: Vec::new(),
                optional_inputs: Vec::new(),
                bound_inputs: Map::new(),
                default_inputs: Map::new(),
                tags: Vec::new(),
                source: None,
                mode: ActionMode::Both,
            },
            evidence: Arc::clone(evidence),
        }
    }

    fn observe_event(&self, event: &LifecycleEvent<'_>, lane: &str) {
        if let LifecycleEvent::ExecuteFinished { snapshot, failure } = event {
            self.evidence.lock().unwrap().observations.push(json!({
                "event":"execute-finish", "lane":lane,
                "checkpoint":snapshot.checkpoint, "state":snapshot.state.values(),
                "ordinal":snapshot.last_action_ordinal,
                "failure":Self::failure(*failure),
            }));
            return;
        }
        let (name, context, detail) = match event {
            LifecycleEvent::StepStarted { context, state, .. } => {
                ("step-start", context, json!({"state":state.values()}))
            }
            LifecycleEvent::StepFinished {
                context, outcome, ..
            } => {
                let outcome = match outcome {
                    Some(Ok(outcome)) => json!({"kind":"complete","node":outcome.node,
                        "ordinal":outcome.ordinal,"result":outcome.result}),
                    Some(Err(WorkflowFailure::Action(message))) => {
                        json!({"kind":"action","message":message})
                    }
                    Some(Err(WorkflowFailure::Timeout(message))) => {
                        json!({"kind":"timeout","message":message})
                    }
                    Some(Err(_)) => json!({"kind":"other_failure"}),
                    None => Value::Null,
                };
                ("step-finish", context, outcome)
            }
            LifecycleEvent::StreamStarted { context, .. } => ("stream-start", context, Value::Null),
            LifecycleEvent::StreamItem {
                context,
                index,
                result,
                ..
            } => (
                "stream-item",
                context,
                json!({"index":index,"result":result}),
            ),
            LifecycleEvent::StreamFinished { context, items, .. } => {
                ("stream-finish", context, json!({"items":items}))
            }
            _ => return,
        };
        let row = json!({"event":name,"lane":lane,"node":context.node,
            "checkpoint":context.checkpoint,"ordinal":context.action_ordinal,
            "asynchronous":context.mode == ActionMode::Asynchronous,"detail":detail});
        self.evidence.lock().unwrap().observations.push(row);
    }

    fn failure(failure: Option<&WorkflowFailure>) -> Value {
        match failure {
            Some(WorkflowFailure::Action(message)) => json!({"kind":"action","message":message}),
            Some(WorkflowFailure::Timeout(message)) => json!({"kind":"timeout","message":message}),
            Some(WorkflowFailure::Hook { message, cause }) => json!({
                "kind":"hook", "message":message, "cause":Self::failure(cause.as_deref()),
            }),
            Some(_) => json!({"kind":"other"}),
            None => Value::Null,
        }
    }
}
impl LifecycleHooks for WaveAction {
    fn observe(&self, event: &LifecycleEvent<'_>) -> Result<(), AppError> {
        self.observe_event(event, "sync");
        Ok(())
    }

    async fn observe_async<'a>(&'a self, event: &'a LifecycleEvent<'a>) -> Result<(), AppError> {
        self.observe_event(event, "async");
        let held = self.evidence.lock().unwrap().held_hook.clone();
        if let Some(held) = held
            && match event {
                LifecycleEvent::StreamItem { context, .. } => held.item && context.node == "z",
                LifecycleEvent::StepStarted { context, .. } => !held.item && context.node == "z",
                _ => false,
            }
        {
            held.entered.notify_one();
            held.release.notified().await;
            held.finished.fetch_add(1, Ordering::SeqCst);
            if held.fail {
                let message = if held.item {
                    "held z item hook failure"
                } else {
                    "held z pre-hook failure"
                };
                return Err(AppError::Integration(message.into()));
            }
        }
        Ok(())
    }
}
impl ActionBackend for WaveAction {
    type Stream = WaveSource;
    fn declaration(&self) -> &ActionDeclaration {
        &self.declaration
    }
    fn start(&self, request: ActionRequest) -> Self::Stream {
        self.evidence
            .lock()
            .unwrap()
            .constructed
            .push(request.context.node.clone());
        WaveSource {
            context: request.context,
            phase: SourcePhase::Ready,
            evidence: Arc::clone(&self.evidence),
        }
    }
}
impl WaveSource {
    fn result(&self) -> Map<String, Value> {
        let mut result = WaveCase::result(&self.context.node);
        if self.context.node == "shared" {
            result.insert("ordinal".into(), json!(self.context.action_ordinal));
        }
        result
    }
}
impl Stream for WaveSource {
    type Item = Result<ActionEvent, AppError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let node = self.context.node.clone();
        match mem::replace(&mut self.phase, SourcePhase::Closed) {
            SourcePhase::Ready => {
                let row = json!({"event":"source","lane":"source","node":node,
                    "checkpoint":self.context.checkpoint,"ordinal":self.context.action_ordinal,
                    "asynchronous":self.context.mode == ActionMode::Asynchronous});
                let mut evidence = self.evidence.lock().unwrap();
                evidence.started.push(node.clone());
                evidence.observations.push(row);
            }
            SourcePhase::Waiting => {}
            SourcePhase::Terminal => {
                let held = {
                    let mut evidence = self.evidence.lock().unwrap();
                    if evidence.held_terminals.contains(&node) {
                        evidence.wakers.insert(node.clone(), cx.waker().clone());
                        true
                    } else {
                        false
                    }
                };
                if held {
                    self.phase = SourcePhase::Terminal;
                    return Poll::Pending;
                }
                let ending = {
                    let mut evidence = self.evidence.lock().unwrap();
                    let ending = evidence
                        .endings
                        .get(&node)
                        .copied()
                        .unwrap_or(SourceEnding::Complete);
                    evidence.terminals.push((node.clone(), ending));
                    if ending == SourceEnding::Complete {
                        evidence.completed.push(node.clone());
                    }
                    ending
                };
                match ending {
                    SourceEnding::Error => {
                        return Poll::Ready(Some(Err(AppError::Integration(format!(
                            "{node} source failure"
                        )))));
                    }
                    SourceEnding::Eof => return Poll::Ready(None),
                    SourceEnding::Complete => {}
                }
                let writes = if node == "seed" {
                    Vec::new()
                } else {
                    vec![StateChange::Assign {
                        values: Map::from_iter([("values".into(), json!(node))]),
                    }]
                };
                return Poll::Ready(Some(Ok(ActionEvent::Completed {
                    result: self.result(),
                    writes,
                })));
            }
            SourcePhase::Closed => panic!("terminal source was polled after completion"),
        }
        let ready = {
            let mut evidence = self.evidence.lock().unwrap();
            if matches!(node.as_str(), "a" | "z") && !evidence.released.contains(&node) {
                evidence.wakers.insert(node.clone(), cx.waker().clone());
                false
            } else {
                true
            }
        };
        if !ready {
            self.phase = SourcePhase::Waiting;
            return Poll::Pending;
        }
        self.phase = SourcePhase::Terminal;
        Poll::Ready(Some(Ok(ActionEvent::Partial {
            result: self.result(),
        })))
    }
}
impl Drop for WaveSource {
    fn drop(&mut self) {
        self.evidence
            .lock()
            .unwrap()
            .dropped
            .push(self.context.node.clone());
    }
}

struct WaveCase {
    app: App<StateStore, Agent, WaveAction, NoConditions, JsonState, WaveAction>,
    workflow: Workflow<StateStore, Agent, WaveAction, NoConditions, JsonState, WaveAction>,
    evidence: Arc<Mutex<WaveEvidence>>,
    store: StateStore,
    workspace: WorkspaceId,
}
impl WaveCase {
    fn definition(earlier_deadline: Option<u64>) -> WorkflowDefinition {
        let nodes = ["seed", "z", "a", "y", "b"].map(|id| {
            let tags = if matches!(id, "a" | "z") {
                vec!["completed"]
            } else if matches!(id, "b" | "y") {
                vec!["pending"]
            } else {
                Vec::new()
            };
            let mut node = json!({"id":id,"tags":tags,"step":{"kind":"action","action":"wave","revision":"r1"}});
            if id == "a" && let Some(deadline) = earlier_deadline {
                node["deadline_ms"] = json!(deadline);
            }
            node
        });
        WorkflowDefinition::parse(&json!({
            "schema":"app-sdk.workflow/v2","id":"wave","revision":"r1","state":{"values":[]},"required":["values"],"reducers":{"values":"collect"},"entry":"seed",
            "nodes":nodes,
            "routes":{
                "seed":{"mode":"broadcast","branches":[{"target":{"kind":"node","id":"z"}},{"target":{"kind":"node","id":"a"}}]},
                "a":{"mode":"first","branches":[{"target":{"kind":"node","id":"y"}}]},
                "z":{"mode":"first","branches":[{"target":{"kind":"node","id":"b"}}]}
            }
        }).to_string()).unwrap()
    }

    async fn open() -> Self {
        Self::from_definition(Self::definition(None)).await
    }

    async fn from_definition(definition: WorkflowDefinition) -> Self {
        let evidence = Arc::new(Mutex::new(WaveEvidence::default()));
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let config: AppConfig<StateStore, Agent> =
            AppConfig::new(store.clone(), Vec::new(), vec![definition]);
        let app = App::open(
            config
                .with_actions(vec![WaveAction::new(&evidence)])
                .with_hooks(WaveAction::new(&evidence)),
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
        let workflow = workspace.workflow("wave".parse().unwrap()).await.unwrap();
        Self {
            app,
            workflow,
            evidence,
            store,
            workspace: workspace.id().clone(),
        }
    }
    fn result(node: &str) -> Map<String, Value> {
        Map::from_iter([("node".into(), json!(node))])
    }

    fn inputs(snapshot: &RunSnapshot) -> Vec<(u64, u64, String)> {
        snapshot
            .interactions
            .iter()
            .map(|interaction| match interaction {
                Interaction::Input {
                    sequence,
                    generation,
                    message,
                    ..
                } => (*sequence, *generation, message.clone()),
                _ => panic!("this graph exposes only its two native Input targets"),
            })
            .collect()
    }
    fn release(evidence: &Arc<Mutex<WaveEvidence>>, node: &str) {
        let waker = {
            let mut evidence = evidence.lock().unwrap();
            evidence.released.insert(node.into());
            evidence.wakers.remove(node)
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
    fn release_terminal(evidence: &Arc<Mutex<WaveEvidence>>, node: &str) {
        let waker = {
            let mut evidence = evidence.lock().unwrap();
            evidence.held_terminals.remove(node);
            evidence.wakers.remove(node)
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
    async fn release_in_order(evidence: &Arc<Mutex<WaveEvidence>>, order: [&str; 2]) {
        loop {
            let both = {
                let evidence = evidence.lock().unwrap();
                ["a", "z"]
                    .iter()
                    .all(|node| evidence.started.iter().any(|started| started == node))
            };
            if both {
                break;
            }
            tokio::task::yield_now().await;
        }
        Self::release(evidence, order[0]);
        loop {
            if evidence
                .lock()
                .unwrap()
                .terminals
                .iter()
                .any(|(node, _)| node == order[0])
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        Self::release(evidence, order[1]);
    }
    fn wave(event: Option<ExecutionEvent>) {
        let Some(ExecutionEvent::Committed { outcomes, .. }) = event else {
            panic!("one complete wave boundary required")
        };
        assert_eq!(outcomes.len(), 2);
        for (outcome, (node, ordinal)) in outcomes.iter().zip([("a", 1), ("z", 2)]) {
            assert_eq!(outcome.node, node);
            assert_eq!(outcome.ordinal, ordinal);
            assert_eq!(outcome.result, Self::result(node));
        }
    }

    fn assert_contexts(&self, base: u64, failed: Option<&Checkpoint>) {
        let observations = self.evidence.lock().unwrap().observations.clone();
        for (node, ordinal) in [("a", 1), ("z", 2)] {
            let rows = observations
                .iter()
                .filter(|row| row["node"] == node)
                .collect::<Vec<_>>();
            assert_eq!(
                rows.len(),
                11,
                "one source and five paired lifecycle events for {node}: {rows:?}"
            );
            for row in &rows {
                assert_eq!(row["checkpoint"], base, "{row}");
                assert_eq!(
                    row["ordinal"], ordinal,
                    "the sampled context must never be renumbered: {row}"
                );
                assert_eq!(row["asynchronous"], true);
            }
            assert_eq!(
                rows.iter().filter(|row| row["event"] == "source").count(),
                1
            );
            for event in [
                "step-start",
                "stream-start",
                "stream-item",
                "step-finish",
                "stream-finish",
            ] {
                let pair = rows
                    .iter()
                    .filter(|row| row["event"] == event)
                    .collect::<Vec<_>>();
                assert_eq!(pair.len(), 2, "{node} {event}");
                assert_eq!(pair[0]["lane"], "sync");
                assert_eq!(pair[1]["lane"], "async");
                match event {
                    "step-start" => assert_eq!(pair[0]["detail"]["state"], json!({"values":[]})),
                    "stream-item" => assert_eq!(
                        pair[0]["detail"],
                        json!({"index":0,"result":Self::result(node)})
                    ),
                    "stream-finish" => assert_eq!(pair[0]["detail"]["items"], 1),
                    "step-finish" => {
                        let expected = match failed {
                            Some(checkpoint) => match (&checkpoint.kind, &checkpoint.status) {
                                (
                                    CheckpointKind::Failed { node: selected, .. },
                                    RunStatus::Failed(WorkflowFailure::Action(message)),
                                ) if selected == node => json!({"kind":"action","message":message}),
                                _ => Value::Null,
                            },
                            None => {
                                json!({"kind":"complete","node":node,"ordinal":ordinal,"result":Self::result(node)})
                            }
                        };
                        for row in pair {
                            assert_eq!(
                                row["detail"], expected,
                                "{node} retains only its authoritative outcome"
                            );
                        }
                    }
                    _ => {}
                }
            }
        }
        let dropped = self.evidence.lock().unwrap().dropped.clone();
        for node in ["seed", "a", "z"] {
            assert_eq!(
                dropped
                    .iter()
                    .filter(|dropped| dropped.as_str() == node)
                    .count(),
                1
            );
        }
    }
    async fn boundary(
        lane: Lane,
        execution: &mut Execution<
            '_,
            StateStore,
            Agent,
            WaveAction,
            NoConditions,
            JsonState,
            WaveAction,
        >,
    ) -> Option<ExecutionEvent> {
        match lane {
            Lane::Ordinary => execution.step().await.unwrap(),
            Lane::Streaming => loop {
                match execution.next().await.unwrap() {
                    Some(ExecutionEvent::Partial { .. }) => {}
                    event => break event,
                }
            },
        }
    }
    async fn held_item_failure(drop_owner: bool) {
        let mut case = Self::open().await;
        let held = Arc::new(HeldHook {
            entered: Notify::new(),
            release: Notify::new(),
            finished: AtomicUsize::new(0),
            fail: !drop_owner,
            item: true,
        });
        {
            let mut evidence = case.evidence.lock().unwrap();
            evidence.endings.insert("a".into(), SourceEnding::Error);
            evidence.held_terminals.insert("a".into());
            evidence.held_hook = Some(Arc::clone(&held));
        }
        let mut reopened = None;
        let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(15), async {
            let mut run = case.workflow.prepare(WorkflowState::default()).await.unwrap();
            let id = run.id().clone();
            let observer = case.workflow.open_run(RunSelector::Latest(id.clone())).await.unwrap();
            let mut execution = run.execution(ExecutionOptions::default()).await.unwrap();
            let (base, seed_outcomes) = match execution.step().await.unwrap() {
                Some(ExecutionEvent::Committed { checkpoint, outcomes }) => {
                    assert_eq!(outcomes.len(), 1);
                    assert_eq!(outcomes[0].node, "seed");
                    assert_eq!(outcomes[0].ordinal, 0);
                    (checkpoint, outcomes)
                }
                _ => panic!("the seed commits before the held sibling wave"),
            };
            // Prime a's partial before holding z's item receipt. Its separately
            // gated terminal then occupies the sole prefetch slot directly.
            let (first, ()) = tokio::time::timeout(Duration::from_secs(3),
                futures_util::future::join(execution.next(), async {
                    loop {
                        let both = {
                            let evidence = case.evidence.lock().unwrap();
                            ["a", "z"].iter().all(|node| evidence.started.iter().any(|seen| seen == node))
                        };
                        if both { break; }
                        tokio::task::yield_now().await;
                    }
                    Self::release(&case.evidence, "a");
                }),
            ).await.expect("both sources enter before a's first partial");
            assert!(matches!(first.unwrap(), Some(ExecutionEvent::Partial { node, result })
                if node == "a" && result == Self::result("a")));
            Self::release(&case.evidence, "z");
            let mut next = Box::pin(execution.next());
            tokio::time::timeout(Duration::from_secs(3), async {
                tokio::select! {
                    _ = held.entered.notified() => {}
                    _ = &mut next => panic!("z's admitted item hook must remain held"),
                }
                Self::release_terminal(&case.evidence, "a");
                loop {
                    let captured = {
                        let evidence = case.evidence.lock().unwrap();
                        evidence.terminals.contains(&("a".into(), SourceEnding::Error))
                            && evidence.dropped.iter().any(|node| node == "a")
                    };
                    if captured { break; }
                    tokio::select! {
                        _ = &mut next => panic!("a's real error cannot discard z's original hook receipt"),
                        _ = tokio::task::yield_now() => {}
                    }
                }
            }).await.expect("a's terminal error is observed while z's original hook is pending");
            drop(next);
            assert_eq!(held.finished.load(Ordering::SeqCst), 0);
            let before_release = observer.snapshot().await.unwrap();
            assert_eq!(before_release.state.values()["values"], json!([]));
            assert_eq!(before_release.last_action_ordinal, Some(0));
            assert!(before_release.outcomes == seed_outcomes);
            let rows = case.evidence.lock().unwrap().observations.clone();
            for (node, ordinal) in [("a", 1), ("z", 2)] {
                for row in rows.iter().filter(|row| row["node"] == node) {
                    assert_eq!(row["checkpoint"], base);
                    assert_eq!(row["ordinal"], ordinal);
                    assert_eq!(row["asynchronous"], true);
                }
                for event in ["source", "step-start", "stream-start", "stream-item"] {
                    assert_eq!(rows.iter().filter(|row| row["node"] == node && row["event"] == event).count(),
                        if event == "source" { 1 } else { 2 });
                }
            }
            assert!(!rows.iter().any(|row| row["node"] == "z" && row["event"] == "step-finish"));

            let (snapshot, failed) = if drop_owner {
                drop(execution);
                let dropped = case.evidence.lock().unwrap().dropped.clone();
                assert_eq!(dropped.iter().filter(|node| node.as_str() == "a").count(), 1);
                assert_eq!(dropped.iter().filter(|node| node.as_str() == "z").count(), 1);
                assert_eq!(held.finished.load(Ordering::SeqCst), 0);
                assert!(matches!(case.app.shutdown(Shutdown::Drain {
                    timeout: Duration::from_millis(150),
                }).await, Err(AppError::Timeout(_))),
                    "owning Drop cannot release the original pending hook publication");
                assert_eq!(held.finished.load(Ordering::SeqCst), 0);
                held.release.notify_one();
                case.app.shutdown(Shutdown::Drain { timeout: Duration::from_secs(3) }).await
                    .expect("the original hook receipt settles before App shutdown finishes");
                assert_eq!(held.finished.load(Ordering::SeqCst), 1);

                // Shutdown closes live projections. Reopen through the public
                // App path to verify the retained failure, without replay.
                let config: AppConfig<StateStore, Agent> = AppConfig::new(
                    case.store.clone(), Vec::new(), vec![Self::definition(None)],
                );
                reopened = Some(App::open(config
                    .with_actions(vec![WaveAction::new(&case.evidence)])
                    .with_hooks(WaveAction::new(&case.evidence)),
                ).await.unwrap());
                let workspace = reopened.as_ref().unwrap().workspace(case.workspace.clone()).await.unwrap();
                let workflow = workspace.workflow("wave".parse().unwrap()).await.unwrap();
                let run = workflow.open_run(RunSelector::Latest(id)).await.unwrap();
                assert!(matches!(run.checkpoint(CheckpointSelector::Action(2)).await, Err(AppError::NotFound(_))),
                    "the uncommitted z hook cannot fabricate a second failure boundary");
                (run.snapshot().await.unwrap(), run.checkpoint(CheckpointSelector::Action(1)).await.unwrap())
            } else {
                held.release.notify_one();
                assert!(matches!(execution.finish().await, Err(AppError::Integration(message))
                    if message.contains("held z item hook failure")),
                    "resuming the cancelled observation retains the actual later hook error");
                assert_eq!(held.finished.load(Ordering::SeqCst), 1);
                assert!(matches!(observer.checkpoint(CheckpointSelector::Action(2)).await, Err(AppError::NotFound(_))),
                    "the later hook remains a completion cause, not another action boundary");
                (observer.snapshot().await.unwrap(), observer.checkpoint(CheckpointSelector::Action(1)).await.unwrap())
            };
            assert!(matches!(&snapshot.status, RunStatus::Failed(WorkflowFailure::Action(message))
                if message == "caller action execution failed"));
            assert_eq!(snapshot.state.values()["values"], json!([]));
            assert_eq!(snapshot.last_action_ordinal, Some(0));
            assert!(snapshot.outcomes == seed_outcomes);
            assert!(matches!(&failed.kind, CheckpointKind::Failed { node, ordinal: 1 } if node == "a"));
            assert!(matches!(&failed.status, RunStatus::Failed(WorkflowFailure::Action(message))
                if message == "caller action execution failed"));
            assert_eq!(failed.state, snapshot.state);
            assert_eq!(failed.action_ordinal, Some(0));
            assert_eq!(failed.position.as_deref(), Some("seed"));
            assert!(failed.outcomes.is_empty());
            let evidence = case.evidence.lock().unwrap();
            let (started, terminals, dropped, rows) = (evidence.started.clone(),
                evidence.terminals.clone(), evidence.dropped.clone(), evidence.observations.clone());
            drop(evidence);
            assert_eq!(started.len(), 3, "neither continuation nor reopen may replay either source");
            assert_eq!(terminals, vec![("seed".into(), SourceEnding::Complete), ("a".into(), SourceEnding::Error)]);
            for node in ["seed", "a", "z"] {
                assert_eq!(dropped.iter().filter(|seen| seen.as_str() == node).count(), 1);
            }
            for (node, ordinal) in [("a", 1), ("z", 2)] {
                for row in rows.iter().filter(|row| row["node"] == node) {
                    assert_eq!(row["checkpoint"], base);
                    assert_eq!(row["ordinal"], ordinal);
                }
                for event in ["step-finish", "stream-finish"] {
                    let finished = rows.iter().filter(|row| row["node"] == node && row["event"] == event).collect::<Vec<_>>();
                    assert_eq!(finished.len(), 1, "one synchronous remaining guard for {node}/{event}");
                    assert_eq!(finished[0]["lane"], "sync");
                    if event == "step-finish" {
                        let expected = if node == "a" {
                            json!({"kind":"action","message":"caller action execution failed"})
                        } else { Value::Null };
                        assert_eq!(finished[0]["detail"], expected);
                    } else {
                        assert_eq!(finished[0]["detail"]["items"],
                            if node == "a" || drop_owner { 1 } else { 0 });
                    }
                }
            }
            let completions = rows.iter().filter(|row| row["event"] == "execute-finish").collect::<Vec<_>>();
            assert_eq!(completions.len(), if drop_owner { 1 } else { 2 });
            for (index, row) in completions.iter().enumerate() {
                assert_eq!(row["lane"], if index == 0 { "sync" } else { "async" });
                assert_eq!(row["state"]["values"], json!([]));
                assert_eq!(row["ordinal"], 0);
                let primary = if drop_owner { &row["failure"] } else {
                    assert_eq!(row["failure"]["kind"], "hook");
                    assert!(row["failure"]["message"].as_str().unwrap().contains("held z item hook failure"));
                    &row["failure"]["cause"]
                };
                assert_eq!(primary["kind"], "action");
                assert_eq!(primary["message"], "caller action execution failed");
            }
        })).catch_unwind().await;
        held.release.notify_one();
        Self::release(&case.evidence, "a");
        Self::release(&case.evidence, "z");
        Self::release_terminal(&case.evidence, "a");
        let restored_cleanup = if let Some(mut reopened) = reopened {
            let closed = AssertUnwindSafe(tokio::time::timeout(
                Duration::from_secs(5),
                reopened.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(3),
                }),
            ))
            .catch_unwind()
            .await;
            drop(reopened);
            Some(closed)
        } else {
            None
        };
        let cleanup = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), case.close()))
            .catch_unwind()
            .await;
        match observed {
            Ok(result) => result.expect("held item/source failure journey is bounded"),
            Err(panic) => resume_unwind(panic),
        }
        if let Some(closed) = restored_cleanup {
            closed
                .expect("restored cleanup does not panic")
                .expect("restored cleanup is bounded")
                .expect("restored cleanup joins");
        }
        cleanup
            .expect("held item cleanup does not panic")
            .expect("held item cleanup is bounded");
    }

    async fn close(mut self) {
        Self::release(&self.evidence, "a");
        Self::release(&self.evidence, "z");
        Self::release_terminal(&self.evidence, "a");
        Self::release_terminal(&self.evidence, "z");
        let held = self.evidence.lock().unwrap().held_hook.clone();
        if let Some(held) = held {
            held.release.notify_one();
        }
        let before = self.evidence.lock().unwrap().started.clone();
        self.app
            .shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            })
            .await
            .unwrap();
        let after = self.evidence.lock().unwrap().started.clone();
        assert_eq!(after, before, "cleanup cannot admit the halted next wave");
    }
}

#[tokio::test]
async fn canonical_bsp_halt_winner_is_independent_of_completion_and_selector_order() {
    for lane in [Lane::Streaming, Lane::Ordinary] {
        for order in [["a", "z"], ["z", "a"]] {
            for reverse_selectors in [false, true] {
                for (before, after) in [(true, false), (false, true), (true, true)] {
                    let case = WaveCase::open().await;
                    let observed = AssertUnwindSafe(tokio::time::timeout(
                        Duration::from_secs(12),
                        async {
                            let mut run = case
                                .workflow
                                .prepare(WorkflowState::default())
                                .await
                                .unwrap();
                            let options = ExecutionOptions {
                                halt_before: if before {
                                    if reverse_selectors {
                                        vec!["y".into(), "@pending".into(), "b".into()]
                                    } else {
                                        vec!["b".into(), "y".into(), "@pending".into()]
                                    }
                                } else {
                                    Vec::new()
                                },
                                halt_after: if after {
                                    if reverse_selectors {
                                        vec!["z".into(), "@completed".into(), "a".into()]
                                    } else {
                                        vec!["a".into(), "z".into(), "@completed".into()]
                                    }
                                } else {
                                    Vec::new()
                                },
                                ..ExecutionOptions::default()
                            };
                            let mut execution = run.execution(options).await.unwrap();
                            let seed = WaveCase::boundary(lane, &mut execution).await;
                            let base = match &seed {
                                Some(ExecutionEvent::Committed { checkpoint, .. }) => *checkpoint,
                                _ => panic!("seed commits before the sibling wave"),
                            };
                            assert!(
                                matches!(seed,Some(ExecutionEvent::Committed {outcomes,..}) if outcomes.len()==1 && outcomes[0].node=="seed" && outcomes[0].ordinal==0)
                            );
                            let (event, ()) = tokio::time::timeout(
                                Duration::from_secs(3),
                                futures_util::future::join(
                                    WaveCase::boundary(lane, &mut execution),
                                    WaveCase::release_in_order(&case.evidence, order),
                                ),
                            )
                            .await
                            .expect("both sibling sources enter and settle in the forced order");
                            WaveCase::wave(event);
                            let use_after = after && (!before || matches!(lane, Lane::Streaming));
                            let expected_node = if use_after { "a" } else { "b" };
                            let expected_result = use_after.then(|| WaveCase::result("a"));
                            assert!(
                                matches!(execution.next().await.unwrap(),Some(ExecutionEvent::Halted {node,result}) if node==expected_node && result==expected_result)
                            );
                            assert!(execution.next().await.unwrap().is_none());
                            assert!(
                                execution.step().await.unwrap().is_none(),
                                "a retained decision cannot be reclassified"
                            );
                            let snapshot = execution.finish().await.unwrap();
                            assert!(
                                matches!(&snapshot.status,RunStatus::Halted {node,result} if node==expected_node && result==&expected_result)
                            );
                            assert_eq!(snapshot.last_action_ordinal, Some(2));
                            assert_eq!(snapshot.state.values()["values"], json!(["a", "z"]));
                            assert_eq!(snapshot.outcomes.len(), 2);
                            assert_eq!(snapshot.outcomes[1].node, "z");
                            let readback = case
                                .workflow
                                .open_run(RunSelector::Latest(run.id().clone()))
                                .await
                                .unwrap()
                                .snapshot()
                                .await
                                .unwrap();
                            assert_eq!(readback.checkpoint, snapshot.checkpoint);
                            assert!(
                                matches!(readback.status,RunStatus::Halted {node,result} if node==expected_node && result==expected_result)
                            );
                            let (completed, started) = {
                                let evidence = case.evidence.lock().unwrap();
                                (evidence.completed.clone(), evidence.started.len())
                            };
                            assert_eq!(completed, vec!["seed", order[0], order[1]]);
                            assert_eq!(started, 3, "halt-before excludes all next-wave effects");
                            case.assert_contexts(base, None);
                        },
                    ))
                    .catch_unwind()
                    .await;
                    let cleanup = AssertUnwindSafe(tokio::time::timeout(
                        Duration::from_secs(5),
                        case.close(),
                    ))
                    .catch_unwind()
                    .await;
                    match observed {
                        Ok(result) => result.expect("bounded complete BSP halt journey"),
                        Err(panic) => resume_unwind(panic),
                    }
                    cleanup.unwrap().expect("bounded BSP shutdown");
                }
            }
        }
    }
}

#[tokio::test]
async fn later_source_error_waits_for_the_earlier_sibling_and_retains_sampled_failure_position() {
    for earlier in [
        SourceEnding::Error,
        SourceEnding::Complete,
        SourceEnding::Eof,
    ] {
        let case = WaveCase::open().await;
        {
            let mut evidence = case.evidence.lock().unwrap();
            evidence.endings.insert("a".into(), earlier);
            evidence.endings.insert("z".into(), SourceEnding::Error);
        }
        let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(12), async {
            let mut run = case.workflow.prepare(WorkflowState::default()).await.unwrap();
            let mut execution = run.execution(ExecutionOptions::default()).await.unwrap();
            let (base, seed_outcomes) = match execution.step().await.unwrap() {
                Some(ExecutionEvent::Committed { checkpoint, outcomes }) => {
                    assert_eq!(outcomes.len(), 1);
                    assert_eq!(outcomes[0].node, "seed");
                    assert_eq!(outcomes[0].ordinal, 0);
                    (checkpoint, outcomes)
                }
                _ => panic!("the seed commits before either sibling starts"),
            };
            // The second release follows the actual z terminal poll, not a
            // timer or a guessed scheduling interval. Both sources first
            // prove their admission by polling while held.
            let (snapshot, ()) = tokio::time::timeout(Duration::from_secs(3),
                futures_util::future::join(
                    execution.finish(),
                    WaveCase::release_in_order(&case.evidence, ["z", "a"]),
                ),
            ).await.expect("both real terminals settle in the required order");
            let snapshot = snapshot.expect("ordinary source failure remains a durable run outcome");
            let (terminals, started) = {
                let evidence = case.evidence.lock().unwrap();
                (evidence.terminals.clone(), evidence.started.clone())
            };
            assert_eq!(terminals, vec![
                ("seed".into(), SourceEnding::Complete),
                ("z".into(), SourceEnding::Error),
                ("a".into(), earlier),
            ], "the earlier source must reach its own result after z fails");
            assert_eq!(started.len(), 3, "failure cannot route to another wave or replay a source");
            assert!(matches!(&snapshot.status, RunStatus::Failed(WorkflowFailure::Action(message))
                if message == "caller action execution failed"),
                "a settled source error wins over later EOF without inventing Paused success");
            assert_eq!(snapshot.state.values(), &Map::from_iter([("values".into(), json!([]))]));
            assert_eq!(snapshot.last_action_ordinal, Some(0));
            assert!(snapshot.outcomes == seed_outcomes);
            let (node, ordinal) = if earlier == SourceEnding::Error { ("a", 1) } else { ("z", 2) };
            let failed = run.checkpoint(CheckpointSelector::Action(ordinal)).await.unwrap();
            assert!(matches!(&failed.kind, CheckpointKind::Failed { node: actual, ordinal: actual_ordinal }
                if actual == node && *actual_ordinal == ordinal),
                "canonical primary selection retains its original attempted position");
            assert!(matches!(&failed.status, RunStatus::Failed(WorkflowFailure::Action(message))
                if message == "caller action execution failed"));
            assert_eq!(failed.state, snapshot.state);
            assert_eq!(failed.action_ordinal, Some(0));
            assert_eq!(failed.position.as_deref(), Some("seed"));
            assert!(failed.outcomes.is_empty());
            if node == "z" {
                assert!(matches!(run.checkpoint(CheckpointSelector::Action(1)).await,
                    Err(AppError::NotFound(_))),
                    "uncommitted a cannot fill the sparse attempted-ordinal gap");
            }
            case.assert_contexts(base, Some(&failed));
        })).catch_unwind().await;
        let cleanup = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), case.close()))
            .catch_unwind()
            .await;
        match observed {
            Ok(result) => result.expect("bounded canonical source-failure journey"),
            Err(panic) => resume_unwind(panic),
        }
        cleanup
            .expect("source-failure cleanup does not panic")
            .expect("source-failure cleanup is bounded");
    }
}

#[tokio::test]
async fn resumed_first_wave_bypasses_historical_halts_then_stops_before_the_next_wave() {
    let case = WaveCase::open().await;
    let observed = AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(12),
        async {
            for lane in [Lane::Streaming, Lane::Ordinary] {
                // Each lane needs a fresh saved boundary, not replay of a completed wave.
                let mut fresh = case
                    .workflow
                    .prepare(WorkflowState::default())
                    .await
                    .unwrap();
                let first = fresh
                    .execution(ExecutionOptions {
                        halt_after: vec!["seed".into()],
                        ..ExecutionOptions::default()
                    })
                    .await
                    .unwrap();
                let saved = first.finish().await.unwrap();
                assert!(matches!(saved.status,RunStatus::Halted {node,..} if node=="seed"));
                WaveCase::release(&case.evidence, "a");
                WaveCase::release(&case.evidence, "z");
                let mut resumed = fresh
                    .execution(ExecutionOptions {
                        halt_before: vec!["@completed".into(), "@pending".into()],
                        halt_after: vec!["seed".into()],
                        ..ExecutionOptions::default()
                    })
                    .await
                    .unwrap();
                WaveCase::wave(
                    tokio::time::timeout(
                        Duration::from_secs(3),
                        WaveCase::boundary(lane, &mut resumed),
                    )
                    .await
                    .unwrap(),
                );
                assert!(
                    matches!(resumed.next().await.unwrap(),Some(ExecutionEvent::Halted {node,result:None}) if node=="b")
                );
                let end = resumed.finish().await.unwrap();
                assert_eq!(end.last_action_ordinal, Some(2));
            }
        },
    ))
    .catch_unwind()
    .await;
    let cleanup = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), case.close()))
        .catch_unwind()
        .await;
    match observed {
        Ok(result) => result.expect("bounded BSP resume journey"),
        Err(panic) => resume_unwind(panic),
    }
    cleanup.unwrap().expect("bounded BSP shutdown");
}

#[tokio::test]
async fn earlier_source_error_survives_a_later_held_item_hook_failure() {
    WaveCase::held_item_failure(false).await;
}

#[tokio::test]
async fn owning_drop_retains_a_buffered_source_error_through_the_original_item_receipt() {
    WaveCase::held_item_failure(true).await;
}

#[tokio::test]
async fn earlier_source_deadline_survives_a_later_held_pre_hook_failure() {
    let case = WaveCase::from_definition(WaveCase::definition(Some(1_000))).await;
    let held = Arc::new(HeldHook {
        entered: Notify::new(),
        release: Notify::new(),
        finished: AtomicUsize::new(0),
        fail: true,
        item: false,
    });
    case.evidence.lock().unwrap().held_hook = Some(Arc::clone(&held));
    let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(12), async {
        let mut run = case.workflow.prepare(WorkflowState::default()).await.unwrap();
        let observer = case.workflow.open_run(RunSelector::Latest(run.id().clone())).await.unwrap();
        let mut execution = run.execution(ExecutionOptions::default()).await.unwrap();
        let (base, seed_outcomes) = match execution.step().await.unwrap() {
            Some(ExecutionEvent::Committed { checkpoint, outcomes }) => {
                assert_eq!(outcomes.len(), 1);
                assert_eq!(outcomes[0].node, "seed");
                assert_eq!(outcomes[0].ordinal, 0);
                (checkpoint, outcomes)
            }
            _ => panic!("the seed commits before the deadline/pre-hook wave"),
        };
        let mut next = Box::pin(execution.next());
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                _ = held.entered.notified() => {},
                _ = &mut next => panic!("the later pre-hook must remain admitted and held"),
            }
            loop {
                let dropped = case.evidence.lock().unwrap().dropped.iter().any(|node| node == "a");
                if dropped { break; }
                tokio::select! {
                    _ = &mut next => panic!("the earlier deadline cannot discard the later pre-hook receipt"),
                    _ = tokio::task::yield_now() => {},
                }
            }
        }).await.expect("the earlier source expires while the later pre-hook is held");
        drop(next);
        assert_eq!(held.finished.load(Ordering::SeqCst), 0);
        let (constructed, started, terminals, dropped, rows) = {
            let evidence = case.evidence.lock().unwrap();
            (evidence.constructed.clone(), evidence.started.clone(), evidence.terminals.clone(),
                evidence.dropped.clone(), evidence.observations.clone())
        };
        assert_eq!(constructed, ["seed", "a"], "z cannot construct before its pre-hook settles");
        assert_eq!(started, ["seed"], "complete admission precedes every caller source poll");
        assert_eq!(terminals, [("seed".into(), SourceEnding::Complete)]);
        assert_eq!(dropped, ["seed", "a"], "a's actual unpolled source is destroyed at its deadline");
        for (node, ordinal) in [("a", 1), ("z", 2)] {
            let steps = rows.iter().filter(|row| row["node"] == node && row["event"] == "step-start").collect::<Vec<_>>();
            assert_eq!(steps.len(), 2);
            for (index, row) in steps.iter().enumerate() {
                assert_eq!(row["lane"], if index == 0 { "sync" } else { "async" });
                assert_eq!(row["checkpoint"], base);
                assert_eq!(row["ordinal"], ordinal);
                assert_eq!(row["asynchronous"], true);
            }
        }
        assert_eq!(rows.iter().filter(|row| row["node"] == "a" && row["event"] == "stream-start").count(), 2);
        assert!(!rows.iter().any(|row| row["node"] == "z" && row["event"] == "stream-start"));
        assert!(!rows.iter().any(|row| (row["node"] == "a" || row["node"] == "z") && row["event"] == "stream-item"));
        let before_release = observer.snapshot().await.unwrap();
        assert_eq!(before_release.state.values()["values"], json!([]));
        assert_eq!(before_release.last_action_ordinal, Some(0));
        assert!(before_release.outcomes == seed_outcomes);
        let mut finished = Box::pin(execution.finish());
        tokio::select! {
            biased;
            _ = &mut finished => panic!("resuming must await the original pre-hook receipt"),
            _ = tokio::task::yield_now() => {},
        }
        assert_eq!(held.finished.load(Ordering::SeqCst), 0);
        held.release.notify_one();
        assert!(matches!(finished.await, Err(AppError::Integration(message))
            if message.contains("held z pre-hook failure")));
        assert_eq!(held.finished.load(Ordering::SeqCst), 1);
        let snapshot = observer.snapshot().await.unwrap();
        let timeout_message = match &snapshot.status {
            RunStatus::Failed(WorkflowFailure::Timeout(message)) => message,
            _ => panic!("the earlier source deadline remains primary"),
        };
        assert_eq!(snapshot.state, before_release.state);
        assert_eq!(snapshot.last_action_ordinal, Some(0));
        assert!(snapshot.outcomes == seed_outcomes);
        let failed = observer.checkpoint(CheckpointSelector::Action(1)).await.unwrap();
        assert!(matches!(&failed.kind, CheckpointKind::Failed { node, ordinal: 1 } if node == "a"));
        assert!(matches!(&failed.status, RunStatus::Failed(WorkflowFailure::Timeout(message))
            if message == timeout_message));
        assert_eq!(failed.state, snapshot.state);
        assert_eq!(failed.action_ordinal, Some(0));
        assert_eq!(failed.position.as_deref(), Some("seed"));
        assert!(failed.outcomes.is_empty());
        assert!(matches!(observer.checkpoint(CheckpointSelector::Action(2)).await, Err(AppError::NotFound(_))));
        let (constructed, started, terminals, dropped, rows) = {
            let evidence = case.evidence.lock().unwrap();
            (evidence.constructed.clone(), evidence.started.clone(), evidence.terminals.clone(),
                evidence.dropped.clone(), evidence.observations.clone())
        };
        assert_eq!(constructed, ["seed", "a"]);
        assert_eq!(started, ["seed"]);
        assert_eq!(terminals, [("seed".into(), SourceEnding::Complete)]);
        assert_eq!(dropped, ["seed", "a"]);
        assert!(!rows.iter().any(|row| row["node"] == "z" && (row["event"] == "step-finish" || row["event"] == "stream-start" || row["event"] == "stream-finish")));
        for event in ["step-finish", "stream-finish"] {
            let finished = rows.iter().filter(|row| row["node"] == "a" && row["event"] == event).collect::<Vec<_>>();
            assert_eq!(finished.len(), 1);
            assert_eq!(finished[0]["lane"], "sync");
            assert_eq!(finished[0]["checkpoint"], base);
            assert_eq!(finished[0]["ordinal"], 1);
            if event == "step-finish" {
                assert_eq!(finished[0]["detail"]["kind"], "timeout");
                assert_eq!(finished[0]["detail"]["message"], *timeout_message);
            }
            else { assert_eq!(finished[0]["detail"]["items"], 0); }
        }
        let completions = rows.iter().filter(|row| row["event"] == "execute-finish").collect::<Vec<_>>();
        assert_eq!(completions.len(), 2);
        for (index, row) in completions.iter().enumerate() {
            assert_eq!(row["lane"], if index == 0 { "sync" } else { "async" });
            assert_eq!(row["state"]["values"], json!([]));
            assert_eq!(row["ordinal"], 0);
            assert_eq!(row["failure"]["kind"], "hook");
            assert!(row["failure"]["message"].as_str().unwrap().contains("held z pre-hook failure"));
            assert_eq!(row["failure"]["cause"]["kind"], "timeout");
            assert_eq!(row["failure"]["cause"]["message"], *timeout_message);
        }
    })).catch_unwind().await;
    held.release.notify_one();
    WaveCase::release(&case.evidence, "a");
    WaveCase::release(&case.evidence, "z");
    let cleanup = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), case.close()))
        .catch_unwind()
        .await;
    match observed {
        Ok(result) => result.expect("deadline/pre-hook failure journey is bounded"),
        Err(panic) => resume_unwind(panic),
    }
    cleanup
        .expect("deadline/pre-hook cleanup does not panic")
        .expect("deadline/pre-hook cleanup joins");
}

#[tokio::test]
async fn sibling_inputs_keep_independent_generations_and_commit_one_complete_wave() {
    let definition = WorkflowDefinition::parse(
        &json!({
            "schema":"app-sdk.workflow/v2", "id":"wave", "revision":"r1",
            "state":{"values":[]}, "required":["values"], "reducers":{"values":"collect"},
            "entry":"seed", "nodes":[
                {"id":"seed", "step":{"kind":"state", "mutations":[]}},
                {"id":"a", "step":{"kind":"input", "target":"values",
                    "message":[{"kind":"text", "text":"first"}]}},
                {"id":"z", "step":{"kind":"input", "target":"values",
                    "message":[{"kind":"text", "text":"second"}]}}
            ],
            "routes":{"seed":{"mode":"broadcast", "branches":[
                {"target":{"kind":"node", "id":"z"}},
                {"target":{"kind":"node", "id":"a"}}
            ]}}
        })
        .to_string(),
    )
    .unwrap();
    let case = WaveCase::from_definition(definition).await;
    let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(12), async {
        let run = case
            .workflow
            .start(ExecutionOptions::default())
            .await
            .unwrap();
        let ready = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let snapshot = run.snapshot().await.unwrap();
                if snapshot.interactions.len() == 2 {
                    break snapshot;
                }
                assert!(
                    !matches!(snapshot.status, RunStatus::Failed(_) | RunStatus::Succeeded),
                    "both independent targets must be admitted before an outcome"
                );
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("a parked first Input cannot block its ready sibling");
        let targets = WaveCase::inputs(&ready);
        assert_eq!(
            targets
                .iter()
                .map(|(_, _, message)| message.as_str())
                .collect::<Vec<_>>(),
            ["first", "second"],
            "authored branch order cannot reorder the canonical ready set"
        );
        assert_ne!(targets[0].0, targets[1].0);
        assert_eq!(ready.state.values()["values"], json!([]));
        assert_eq!(ready.last_action_ordinal, Some(0));
        assert_eq!(ready.outcomes.len(), 1);
        assert_eq!(ready.outcomes[0].node, "seed");

        assert!(matches!(
            run.resolve(
                targets[0].0,
                targets[0].1.wrapping_add(1),
                Resolution::Input(json!("stale"))
            )
            .await,
            Err(AppError::Conflict(_))
        ));
        let rejected = run.snapshot().await.unwrap();
        assert_eq!(rejected.checkpoint, ready.checkpoint);
        assert_eq!(WaveCase::inputs(&rejected), targets);
        assert_eq!(rejected.state.values(), ready.state.values());
        assert_eq!(rejected.last_action_ordinal, ready.last_action_ordinal);
        assert!(rejected.outcomes == ready.outcomes);

        let first = run
            .resolve(targets[0].0, targets[0].1, Resolution::Input(json!("a")))
            .await
            .expect("first target settles under its own generation");
        let remaining = run.snapshot().await.unwrap();
        assert_eq!(
            WaveCase::inputs(&remaining),
            vec![targets[1].clone()],
            "settling one target cannot replace or invalidate its sibling generation"
        );
        assert_eq!(remaining.state.values()["values"], json!([]));
        assert_eq!(remaining.last_action_ordinal, Some(0));
        assert!(remaining.outcomes == ready.outcomes);
        let replay = run
            .resolve(targets[0].0, targets[0].1, Resolution::Input(json!("a")))
            .await
            .expect("an exact answer replays the original applied receipt");
        assert_eq!(replay.checkpoint, first.checkpoint);
        assert_eq!(replay.state.values(), first.state.values());
        assert_eq!(WaveCase::inputs(&replay), WaveCase::inputs(&first));
        assert_eq!(replay.last_action_ordinal, first.last_action_ordinal);
        assert!(replay.outcomes == first.outcomes);
        assert!(matches!(
            run.resolve(
                targets[0].0,
                targets[0].1,
                Resolution::Input(json!("changed"))
            )
            .await,
            Err(AppError::Conflict(_))
        ));
        let unchanged = run.snapshot().await.unwrap();
        assert_eq!(unchanged.checkpoint, remaining.checkpoint);
        assert_eq!(WaveCase::inputs(&unchanged), vec![targets[1].clone()]);
        run.resolve(targets[1].0, targets[1].1, Resolution::Input(json!("z")))
            .await
            .expect("the sibling's original generation is still authoritative");
        let completed = tokio::time::timeout(Duration::from_secs(3), run.wait())
            .await
            .expect("complete wave settlement is bounded")
            .unwrap();
        assert!(matches!(completed.status, RunStatus::Succeeded));
        assert!(completed.interactions.is_empty());
        assert_eq!(completed.state.values()["values"], json!(["a", "z"]));
        assert_eq!(completed.last_action_ordinal, Some(2));
        assert_eq!(
            completed
                .outcomes
                .iter()
                .map(|outcome| (outcome.node.as_str(), outcome.ordinal))
                .collect::<Vec<_>>(),
            [("a", 1), ("z", 2)]
        );
        let calls = case.evidence.lock().unwrap().started.clone();
        assert!(
            calls.is_empty(),
            "native Input execution never invokes the configured caller Action"
        );
    }))
    .catch_unwind()
    .await;
    let cleanup = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), case.close()))
        .catch_unwind()
        .await;
    match observed {
        Ok(result) => result.expect("independent Input settlement stays bounded"),
        Err(panic) => resume_unwind(panic),
    }
    cleanup
        .expect("Input cleanup does not panic")
        .expect("Input cleanup is bounded");
}

#[tokio::test]
async fn shared_action_keeps_each_original_lineage_context_and_outcome() {
    let definition = WorkflowDefinition::parse(
        &json!({
            "schema":"app-sdk.workflow/v2", "id":"wave", "revision":"r1",
            "state":{"values":[]}, "required":["values"], "reducers":{"values":"collect"},
            "entry":"seed", "nodes":[
                {"id":"seed", "step":{"kind":"state", "mutations":[]}},
                {"id":"left", "step":{"kind":"state", "mutations":[]}},
                {"id":"right", "step":{"kind":"state", "mutations":[]}},
                {"id":"shared", "step":{"kind":"action", "action":"wave", "revision":"r1"}}
            ],
            "routes":{
                "seed":{"mode":"broadcast", "branches":[
                    {"target":{"kind":"node", "id":"right"}},
                    {"target":{"kind":"node", "id":"left"}}
                ]},
                "left":{"mode":"first", "branches":[{"target":{"kind":"node", "id":"shared"}}]},
                "right":{"mode":"first", "branches":[{"target":{"kind":"node", "id":"shared"}}]}
            }
        })
        .to_string(),
    )
    .unwrap();
    let case = WaveCase::from_definition(definition).await;
    let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(8), async {
        let mut run = case.workflow.prepare(WorkflowState::default()).await.unwrap();
        let mut execution = run.execution(ExecutionOptions::default()).await.unwrap();
        assert!(matches!(execution.step().await.unwrap(), Some(ExecutionEvent::Committed { outcomes, .. })
            if outcomes.len() == 1 && outcomes[0].node == "seed" && outcomes[0].ordinal == 0));
        let Some(ExecutionEvent::Committed { checkpoint: base, outcomes }) = execution.step().await.unwrap()
            else { panic!("both distinct predecessor activations commit before the shared wave") };
        assert_eq!(outcomes.iter().map(|outcome| (outcome.node.as_str(), outcome.ordinal)).collect::<Vec<_>>(),
            [("left", 1), ("right", 2)]);
        let mut partials = Vec::new();
        let committed = loop {
            match execution.next().await.unwrap() {
                Some(ExecutionEvent::Partial { node, result }) => {
                    assert_eq!(node, "shared");
                    partials.push(result);
                }
                Some(ExecutionEvent::Committed { checkpoint, outcomes }) => break (checkpoint, outcomes),
                _ => panic!("both live shared activations must settle one complete wave"),
            }
        };
        assert_eq!(partials.len(), 2);
        for ordinal in [3, 4] {
            let result = Map::from_iter([("node".into(), json!("shared")), ("ordinal".into(), json!(ordinal))]);
            assert_eq!(partials.iter().filter(|partial| **partial == result).count(), 1);
            assert_eq!(committed.1.iter().filter(|outcome| outcome.node == "shared"
                && outcome.ordinal == ordinal && outcome.result == result).count(), 1);
        }
        assert_eq!(committed.1.iter().map(|outcome| outcome.ordinal).collect::<Vec<_>>(), [3,4]);
        let snapshot = execution.finish().await.unwrap();
        assert!(matches!(snapshot.status, RunStatus::Succeeded));
        assert_eq!(snapshot.last_action_ordinal, Some(4));
        assert_eq!(snapshot.state.values()["values"], json!(["shared", "shared"]));
        assert!(snapshot.outcomes == committed.1);
        for ordinal in [3,4] {
            let checkpoint = run.checkpoint(CheckpointSelector::Action(ordinal)).await.unwrap();
            assert!(matches!(checkpoint.kind, CheckpointKind::Action));
            assert_eq!(checkpoint.sequence, committed.0);
            assert_eq!(checkpoint.action_ordinal, Some(4));
            assert!(checkpoint.outcomes == snapshot.outcomes);
            assert_eq!(checkpoint.state, snapshot.state);
        }
        let (constructed, started, completed, dropped, observations) = {
            let evidence = case.evidence.lock().unwrap();
            (evidence.constructed.clone(), evidence.started.clone(), evidence.completed.clone(),
                evidence.dropped.clone(), evidence.observations.clone())
        };
        for invocations in [constructed, started, completed, dropped] {
            assert_eq!(invocations, ["shared", "shared"], "both exact source owners are counted, never deduplicated by node");
        }
        let sources = observations.iter().filter(|row| row["event"] == "source").collect::<Vec<_>>();
        assert_eq!(sources.len(), 2);
        for ordinal in [3,4] {
            let rows = observations.iter().filter(|row| row["node"] == "shared" && row["ordinal"] == ordinal).collect::<Vec<_>>();
            assert_eq!(rows.len(), 11, "one source and five paired guard events for this original activation");
            assert!(rows.iter().all(|row| row["checkpoint"] == base && row["asynchronous"] == true));
            let source = rows.iter().filter(|row| row["event"] == "source").collect::<Vec<_>>();
            assert_eq!(source.len(), 1);
            for event in ["step-start", "stream-start", "stream-item", "stream-finish", "step-finish"] {
                let pair = rows.iter().filter(|row| row["event"] == event).collect::<Vec<_>>();
                assert_eq!(pair.len(), 2, "each original activation retains exactly one paired {event}");
                assert_eq!(pair[0]["lane"], "sync");
                assert_eq!(pair[1]["lane"], "async");
                for row in pair {
                    match event {
                        "step-start" => assert_eq!(row["detail"]["state"], json!({"values":[]})),
                        "stream-item" => assert_eq!(row["detail"], json!({"index":0,"result":{"node":"shared","ordinal":ordinal}})),
                        "stream-finish" => assert_eq!(row["detail"]["items"], 1),
                        "step-finish" => assert_eq!(row["detail"], json!({"kind":"complete","node":"shared","ordinal":ordinal,"result":{"node":"shared","ordinal":ordinal}})),
                        _ => {}
                    }
                }
            }
        }
    })).catch_unwind().await;
    let cleanup = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), case.close()))
        .catch_unwind()
        .await;
    match observed {
        Ok(result) => result.expect("two shared lineage activations settle within the bound"),
        Err(panic) => resume_unwind(panic),
    }
    cleanup
        .expect("shared action cleanup does not panic")
        .expect("shared action cleanup joins");
}
