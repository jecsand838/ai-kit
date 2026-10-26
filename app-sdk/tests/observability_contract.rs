//! Captured operational tracing through public App APIs and real LanceDB commits.
//! Host dispatcher scopes and Agent attempts share the same public workflow proof.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::{Future, poll_fn};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::thread::ThreadId;
use std::time::Duration;

use agent_sdk::prompt::PromptResponse;
use app_sdk::workflow::{ExecutionOptions, Interaction, Resolution, RunSnapshot, RunStatus};
use app_sdk::{App, AppConfig, AppError, Shutdown, WorkspaceConfig};
use futures_util::FutureExt;
use serde_json::{Value, json};
use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_sdk::state::record::{
    StateKey, StateListRequest, StateListResponse, StateRecord, StateRecordSnapshot,
};
use storage_sdk::{
    StateStore, StateStoreBackend, StateStoreError, StateTransaction, StateTransactionBackend,
};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

pub mod observability_support;
use observability_support::{TraceKind, TraceRecord};

pub mod native_support;
use native_support::{NativeAgent, NativeCase};

const PRIVATE_CONTENT: &str = "private-tracing-state-input-resolution";

#[derive(Clone, Default)]
struct TraceCapture {
    state: Arc<Mutex<TraceState>>,
}

#[derive(Default)]
struct TraceState {
    records: Vec<TraceRecord>,
    entered: HashMap<ThreadId, Vec<u64>>,
    native_commits: BTreeSet<TraceCommit>,
    violations: Vec<&'static str>,
    pending_polls: usize,
    failure: TraceFailure,
    source: TraceSource,
    source_calls: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TraceCallback {
    Enabled,
    NewSpan,
    EventEnabled,
    Event,
    Enter,
    Exit,
    Close,
}

#[derive(Clone, Copy, Default)]
enum TraceFailure {
    #[default]
    None,
    Armed(TraceCallback, &'static str),
    Fired(TraceCallback),
}

#[derive(Clone, Copy, Default)]
enum TraceSource {
    #[default]
    Native,
    Pending,
    Panic,
}

#[derive(Eq, PartialEq, Ord, PartialOrd)]
enum TraceCommit {
    Checkpoint {
        run: String,
        occurrence: String,
        sequence: u64,
    },
    Deleted {
        run: String,
        occurrence: String,
    },
}

#[derive(Clone, Copy)]
enum TraceCompletion {
    Resolve,
    CancelAndDelete,
}

impl Subscriber for TraceCapture {
    fn register_callsite(&self, _: &'static Metadata<'static>) -> tracing::subscriber::Interest {
        // Filtering remains dynamic so enabled is exercised during App work,
        // rather than while the host installs its subscriber.
        tracing::subscriber::Interest::sometimes()
    }
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        self.fail_selected_callback(TraceCallback::Enabled, metadata.name());
        metadata.target() == "app_sdk" || metadata.target().starts_with("app_sdk::")
    }
    fn new_span(&self, attributes: &Attributes<'_>) -> Id {
        self.fail_selected_callback(TraceCallback::NewSpan, attributes.metadata().name());
        let mut record = TraceRecord {
            name: attributes.metadata().name(),
            fields: BTreeMap::new(),
            kind: TraceKind::Span {
                enters: 0,
                exits: 0,
            },
        };
        attributes.record(&mut record);
        let mut state = self.state.lock().unwrap();
        state.records.push(record);
        Id::from_u64(state.records.len() as u64)
    }
    fn record(&self, span: &Id, values: &Record<'_>) {
        values.record(&mut self.state.lock().unwrap().records[span.into_u64() as usize - 1]);
    }
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event_enabled(&self, event: &Event<'_>) -> bool {
        let record = TraceRecord::from_event(event);
        if record.fields.get("event").and_then(Value::as_str) == Some("app.workflow.checkpoint") {
            self.fail_selected_callback(TraceCallback::EventEnabled, "app.workflow.checkpoint");
        }
        true
    }
    fn event(&self, event: &Event<'_>) {
        let record = TraceRecord::from_event(event);
        if record.fields.get("event").and_then(Value::as_str) == Some("app.workflow.checkpoint") {
            self.fail_selected_callback(TraceCallback::Event, "app.workflow.checkpoint");
        }
        let mut state = self.state.lock().unwrap();
        if matches!(
            record.fields.get("event").and_then(Value::as_str),
            Some(
                "app.workflow.activate"
                    | "app.workflow.checkpoint"
                    | "app.workflow.superstep"
                    | "app.workflow.interaction"
            )
        ) {
            match (
                record.fields.get("run_id").and_then(Value::as_str),
                record.fields.get("occurrence").and_then(Value::as_str),
                record.fields.get("checkpoint").and_then(Value::as_u64),
            ) {
                (Some(run), Some(occurrence), Some(checkpoint))
                    if state.native_commits.contains(&TraceCommit::Checkpoint {
                        run: run.into(),
                        occurrence: occurrence.into(),
                        sequence: checkpoint,
                    }) => {}
                _ => state
                    .violations
                    .push("committed event preceded its exact native checkpoint commit"),
            }
        }
        if record.fields.get("event").and_then(Value::as_str) == Some("app.workflow.delete") {
            match (
                record.fields.get("run_id").and_then(Value::as_str),
                record.fields.get("occurrence").and_then(Value::as_str),
            ) {
                (Some(run), Some(occurrence))
                    if state.native_commits.contains(&TraceCommit::Deleted {
                        run: run.into(),
                        occurrence: occurrence.into(),
                    }) => {}
                _ => state
                    .violations
                    .push("completed deletion event preceded its actual native head deletion"),
            }
        }
        state.records.push(record);
    }
    fn enter(&self, span: &Id) {
        let name = self.state.lock().unwrap().records[span.into_u64() as usize - 1].name;
        self.fail_selected_callback(TraceCallback::Enter, name);
        let mut state = self.state.lock().unwrap();
        if let TraceKind::Span { enters, .. } =
            &mut state.records[span.into_u64() as usize - 1].kind
        {
            *enters += 1;
        }
        state
            .entered
            .entry(std::thread::current().id())
            .or_default()
            .push(span.into_u64());
    }
    fn try_close(&self, span: Id) -> bool {
        let name = self.state.lock().unwrap().records[span.into_u64() as usize - 1].name;
        self.fail_selected_callback(TraceCallback::Close, name);
        true
    }
    fn exit(&self, span: &Id) {
        let name = self.state.lock().unwrap().records[span.into_u64() as usize - 1].name;
        self.fail_selected_callback(TraceCallback::Exit, name);
        let mut state = self.state.lock().unwrap();
        let popped = state
            .entered
            .entry(std::thread::current().id())
            .or_default()
            .pop();
        if popped != Some(span.into_u64()) {
            state
                .violations
                .push("span exited outside its original nested poll scope");
        }
        if let TraceKind::Span { exits, .. } = &mut state.records[span.into_u64() as usize - 1].kind
        {
            *exits += 1;
        }
    }
}

impl TraceCapture {
    fn fail_selected_callback(&self, callback: TraceCallback, name: &str) {
        let fired = {
            let mut state = self.state.lock().unwrap();
            if matches!(state.failure, TraceFailure::Armed(expected, target) if expected == callback && target == name)
            {
                state.failure = TraceFailure::Fired(callback);
                true
            } else {
                false
            }
        };
        // One intentional host-callback panic, outside the capture mutex. A
        // second panic during unwinding would test process abort instead.
        assert!(
            !fired,
            "selected host tracing callback panicked: {callback:?}"
        );
    }

    fn assert_callback_isolated(&self, expected: TraceCallback) {
        assert!(
            matches!(self.state.lock().unwrap().failure,
            TraceFailure::Fired(actual) if actual == expected),
            "the selected App operation never exercised {expected:?}"
        );
        // The owning scenario separately proves its native outcome and joined
        // cleanup. A failed subscriber's own bookkeeping is not asserted to
        // remain well-formed after its callback panics.
    }

    async fn outside_app_polls<F: Future>(&self, future: F) -> F::Output {
        let mut future = Box::pin(future);
        poll_fn(|context| {
            self.check_outside_scope(false);
            let polled = future.as_mut().poll(context);
            self.check_outside_scope(polled.is_pending());
            polled
        })
        .await
    }

    fn check_outside_scope(&self, pending: bool) {
        let mut state = self.state.lock().unwrap();
        if state
            .entered
            .get(&std::thread::current().id())
            .is_some_and(|spans| !spans.is_empty())
        {
            state
                .violations
                .push("an App span remained entered after returning to its caller poll");
        }
        state.pending_polls += usize::from(pending);
    }

    async fn exercise(failure: TraceFailure, completion: TraceCompletion) -> Self {
        let capture = Self::default();
        capture.state.lock().unwrap().failure = failure;
        // The test runtime is current-thread. This is a subscriber scope, not
        // an entered span guard; App tasks on this thread share the capture.
        let _subscriber = tracing::subscriber::set_default(capture.clone());
        capture.outside_app_polls(async {
            let nodes = vec![
                json!({"id":"increment","step":{"kind":"state","mutations":[
                    {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
                ]}}),
                json!({"id":"input","step":{"kind":"input","message":[
                    {"kind":"text","text":PRIVATE_CONTENT}
                ],"target":"private_value","schema":{"type":"string","description":PRIVATE_CONTENT}}}),
            ];
            let definition = NativeCase::definition("native", json!({"count":0,"private_value":PRIVATE_CONTENT}),
                nodes.clone(), NativeCase::linear(&nodes),
                json!({"count":"reject_concurrent","private_value":"reject_concurrent"}));
            let native = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
                .await.unwrap();
            let mut app = App::open(AppConfig::<TraceStore, NativeAgent>::new(
                TraceStore { native, capture: capture.clone() }, Vec::new(), vec![definition],
            )).await.unwrap();
            let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(8), async {
                let workspace = app.create_workspace(WorkspaceConfig {
                    name: Some(PRIVATE_CONTENT.into()), root: std::env::current_dir().unwrap(),
                }).await.unwrap();
                let workflow = workspace.workflow("native".parse().unwrap()).await.unwrap();
                let mut run = workflow.start(ExecutionOptions::default()).await.unwrap();
                let waiting = run.wait().await.unwrap();
                assert_eq!(waiting.state.values()["count"], 1);
                let [Interaction::Input { sequence, generation, .. }] = waiting.interactions.as_slice() else {
                    panic!("real workflow must reach its native Input control");
                };
                // Ensure the host polls independently while this real App
                // operation is parked; no App span may remain entered here.
                tokio::task::yield_now().await;
                capture.check_outside_scope(false);
                let complete = match completion {
                    TraceCompletion::Resolve => {
                        run.resolve(*sequence, *generation, Resolution::Input(json!(PRIVATE_CONTENT))).await.unwrap();
                        let complete = run.wait().await.unwrap();
                        assert!(matches!(complete.status, RunStatus::Succeeded));
                        assert_eq!(complete.last_action_ordinal, Some(1));
                        assert_eq!(complete.outcomes.len(), 1);
                        assert_eq!(complete.outcomes[0].node, "input");
                        assert_eq!(complete.outcomes[0].ordinal, 1);
                        complete
                    }
                    TraceCompletion::CancelAndDelete => {
                        run.cancel(waiting.checkpoint).await.unwrap();
                        let complete = run.wait().await.unwrap();
                        assert!(matches!(complete.status, RunStatus::Cancelled));
                        assert_eq!(complete.last_action_ordinal, Some(0));
                        assert_eq!(complete.outcomes.len(), 1);
                        assert_eq!(complete.outcomes[0].node, "increment");
                        assert_eq!(complete.outcomes[0].ordinal, 0);
                        run.delete(complete.checkpoint).await.unwrap();
                        assert!(matches!(run.snapshot().await, Err(AppError::NotFound(_))));
                        complete
                    }
                };
                assert_eq!(complete.state.values()["count"], 1);
                assert_eq!(complete.state.values()["private_value"], PRIVATE_CONTENT);
                (waiting, complete)
            })).catch_unwind().await;
            let closed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5),
                app.shutdown(Shutdown::CancelAndDrain { timeout: Duration::from_secs(3) }),
            )).catch_unwind().await;
            drop(app);
            let (waiting, complete) = match observed {
                Ok(result) => result.expect("tracing workload is bounded"),
                Err(panic) => std::panic::resume_unwind(panic),
            };
            match closed {
                Ok(result) => result.expect("tracing cleanup is bounded").expect("tracing cleanup joins"),
                Err(panic) => std::panic::resume_unwind(panic),
            }
            if matches!(failure, TraceFailure::None) {
                capture.assert_interaction_target(&waiting, "pending");
                match completion {
                    TraceCompletion::Resolve => capture.assert_interaction_target(&waiting, "applied"),
                    TraceCompletion::CancelAndDelete => capture.assert_cancelled_and_deleted(&complete),
                }
            }
        }).await;
        capture
    }

    fn assert_interaction_target(&self, waiting: &RunSnapshot, status: &str) {
        let state = self.state.lock().unwrap();
        assert!(
            state
                .records
                .iter()
                .any(|record| record.matches_target(waiting, status)),
            "missing {status} metadata for the exact public interaction target"
        );
    }

    fn assert_cancelled_and_deleted(&self, complete: &RunSnapshot) {
        let run = complete.id.to_string();
        let state = self.state.lock().unwrap();
        assert!(
            state
                .records
                .iter()
                .any(|record| matches!(record.kind, TraceKind::Event)
                    && record.fields.get("event").and_then(Value::as_str)
                        == Some("app.workflow.checkpoint")
                    && record.fields.get("run_id").and_then(Value::as_str) == Some(run.as_str())
                    && record.fields.get("transition").and_then(Value::as_str)
                        == Some("cancelled")
                    && record.fields.get("status").and_then(Value::as_str) == Some("cancelled")
                    && record.fields.get("checkpoint").and_then(Value::as_u64)
                        == Some(complete.checkpoint)),
            "cancellation event must describe the exact committed terminal checkpoint"
        );
        assert!(
            state
                .records
                .iter()
                .any(|record| matches!(record.kind, TraceKind::Event)
                    && record.fields.get("event").and_then(Value::as_str)
                        == Some("app.workflow.delete")
                    && record.fields.get("run_id").and_then(Value::as_str) == Some(run.as_str())
                    && record.fields.get("transition").and_then(Value::as_str)
                        == Some("completed")),
            "bounded native deletion must emit its proven completion"
        );
        assert!(
            state.violations.is_empty(),
            "metadata preceded native authority: {:?}",
            state.violations
        );
    }

    async fn exercise_caller_dispatchers() {
        let nodes = vec![
            json!({"id":"increment","step":{"kind":"state","mutations":[
                {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
            ]}}),
            json!({"id":"agent","step":{"kind":"agent","agent":"native","prompt":[
                {"kind":"text","text":PRIVATE_CONTENT}
            ],"text_target":"answer"}}),
        ];
        let definition = NativeCase::definition(
            "native",
            json!({"count":0,"answer":"before"}),
            nodes.clone(),
            NativeCase::linear(&nodes),
            json!({"count":"reject_concurrent","answer":"reject_concurrent"}),
        );
        // Open before either caller installs its dispatcher. An App-open or
        // first-driver capture must not become the context for later callers.
        let case = NativeCase::open(
            vec![definition],
            true,
            PromptResponse::new(PRIVATE_CONTENT.into(), None),
        )
        .await;
        let captures = [Self::default(), Self::default()];
        let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(8), async {
            let mut runs = Vec::new();
            for capture in &captures {
                let _subscriber = tracing::subscriber::set_default(capture.clone());
                let run = case
                    .workflow
                    .start(ExecutionOptions::default())
                    .await
                    .unwrap();
                let complete = run.wait().await.unwrap();
                assert!(matches!(complete.status, RunStatus::Succeeded));
                assert_eq!(complete.state.values()["count"], 1);
                assert_eq!(complete.state.values()["answer"], PRIVATE_CONTENT);
                assert_eq!(complete.last_action_ordinal, Some(1));
                assert_eq!(complete.outcomes.len(), 1);
                assert_eq!(complete.outcomes[0].node, "agent");
                assert_eq!(complete.outcomes[0].ordinal, 1);
                runs.push(run.id().to_string());
            }
            assert_ne!(runs[0], runs[1]);
            assert_eq!(case.peer.ordinary_calls.load(Ordering::SeqCst), 2);
            assert_eq!(case.peer.streaming_calls.load(Ordering::SeqCst), 0);
            runs
        }))
        .catch_unwind()
        .await;
        let closed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), case.close()))
            .catch_unwind()
            .await;
        let runs = match observed {
            Ok(result) => result.expect("two dispatcher workloads are bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        };
        match closed {
            Ok(result) => result.expect("Agent and App cleanup join"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        for (index, capture) in captures.iter().enumerate() {
            let state = capture.state.lock().unwrap();
            for name in [
                "app.workflow.activate",
                "app.workflow.checkpoint",
                "app.workflow.superstep",
                "app.workflow.attempt",
            ] {
                assert!(state.records.iter().any(|record| record.name == name
                    && record.fields.get("run_id").and_then(Value::as_str) == Some(runs[index].as_str())
                    && matches!(record.kind, TraceKind::Span { enters, exits } if enters > 0 && enters == exits)),
                    "caller {index} did not observe its completed {name} span");
            }
            for record in &state.records {
                assert_ne!(
                    record.fields.get("run_id").and_then(Value::as_str),
                    Some(runs[1 - index].as_str()),
                    "another caller's workflow crossed dispatcher scopes"
                );
                assert!(
                    !serde_json::to_string(&record.fields)
                        .unwrap()
                        .contains(PRIVATE_CONTENT),
                    "Agent prompt or result leaked into operational tracing"
                );
            }
            assert!(
                state.entered.values().all(Vec::is_empty),
                "Agent span escaped joined cleanup"
            );
            // Native commit ordering is covered with TraceStore above. This
            // case uses the existing concrete Agent fixture to prove actual
            // per-caller background execution and two accepted prompt calls.
        }
    }

    async fn exercise_open_source(source: TraceSource) {
        let native = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let capture = Self::default();
        capture.state.lock().unwrap().source = source;
        let _subscriber = tracing::subscriber::set_default(capture.clone());
        let opening = App::open(AppConfig::<TraceStore, NativeAgent>::new(
            TraceStore {
                native: native.clone(),
                capture: capture.clone(),
            },
            Vec::new(),
            Vec::new(),
        ));
        let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(3), async {
            match source {
                TraceSource::Pending => {
                    let mut opening = Box::pin(opening);
                    // Earlier native reads may suspend before the selected
                    // list source. Cancel only after that source is entered;
                    // until then, retain its real wakeups and the same future.
                    match poll_fn(|context| {
                        let result = opening.as_mut().poll(context);
                        if result.is_pending() && capture.state.lock().unwrap().source_calls == 0 {
                            Poll::Pending
                        } else {
                            Poll::Ready(result)
                        }
                    })
                    .await
                    {
                        Poll::Pending => {}
                        Poll::Ready(Ok(mut app)) => {
                            app.shutdown(Shutdown::CancelAndDrain {
                                timeout: Duration::from_secs(1),
                            })
                            .await
                            .unwrap();
                            panic!("selected read did not suspend App open");
                        }
                        Poll::Ready(Err(error)) => {
                            panic!("App open did not reach its read: {error}")
                        }
                    }
                    capture.state.lock().unwrap().failure =
                        TraceFailure::Armed(TraceCallback::Close, "app.open");
                    let dropped = std::panic::catch_unwind(AssertUnwindSafe(|| drop(opening)));
                    assert!(
                        dropped.is_ok(),
                        "subscriber close panic escaped cancellation of the App future"
                    );
                    capture.assert_callback_isolated(TraceCallback::Close);
                }
                TraceSource::Panic => {
                    let original = AssertUnwindSafe(opening).catch_unwind().await;
                    match original {
                        Err(payload) => assert_eq!(
                            payload.downcast_ref::<&'static str>(),
                            Some(&"original App storage future panic"),
                            "observation replaced the primary panic"
                        ),
                        Ok(Ok(mut app)) => {
                            app.shutdown(Shutdown::CancelAndDrain {
                                timeout: Duration::from_secs(1),
                            })
                            .await
                            .unwrap();
                            panic!("observation swallowed the original storage panic");
                        }
                        Ok(Err(error)) => {
                            panic!("observation rewrote the original storage panic: {error}")
                        }
                    }
                }
                TraceSource::Native => {
                    panic!("this proof requires an actual pending or panicking source")
                }
            }
        }))
        .catch_unwind()
        .await;
        {
            let mut state = capture.state.lock().unwrap();
            state.source = TraceSource::Native;
            state.failure = TraceFailure::None;
        }
        // This fault precedes admission. Cancellation or panic must not claim
        // an App generation; the same actual native store remains usable.
        let records = native
            .list(StateListRequest::new("app-sdk").unwrap())
            .await
            .unwrap();
        let mut reopened = tokio::time::timeout(
            Duration::from_secs(5),
            App::open(AppConfig::<TraceStore, NativeAgent>::new(
                TraceStore {
                    native,
                    capture: capture.clone(),
                },
                Vec::new(),
                Vec::new(),
            )),
        )
        .await
        .expect("the original native store remains usable")
        .unwrap();
        let closed = tokio::time::timeout(
            Duration::from_secs(5),
            reopened.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        )
        .await;
        drop(reopened);
        match observed {
            Ok(result) => result.expect("original App poll remains bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        closed
            .expect("fresh App cleanup remains bounded")
            .expect("fresh App cleanup joins");
        assert_eq!(
            capture.state.lock().unwrap().source_calls,
            1,
            "an original source cannot be replayed"
        );
        assert!(
            records.records.is_empty(),
            "observation changed native state before admission"
        );
    }

    fn assert_safe_committed_events(&self) {
        let state = self.state.lock().unwrap();
        let workspace_path = std::env::current_dir()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        assert!(
            !state.records.is_empty(),
            "capture must contain actual App tracing"
        );
        for record in &state.records {
            record.assert_redacted(&[PRIVATE_CONTENT, &workspace_path]);
        }
        for name in [
            "app.workflow.activate",
            "app.workflow.checkpoint",
            "app.workflow.superstep",
            "app.workflow.interaction",
        ] {
            assert!(
                state
                    .records
                    .iter()
                    .any(|record| matches!(record.kind, TraceKind::Event)
                        && record.fields.get("event").and_then(Value::as_str) == Some(name)),
                "performed operation omitted its committed event: {name}"
            );
        }
        assert!(
            state.violations.is_empty(),
            "tracing authority violations: {:?}",
            state.violations
        );
    }

    fn assert_spans(&self) {
        let state = self.state.lock().unwrap();
        for name in [
            "app.open",
            "app.operation",
            "app.workflow.activate",
            "app.workflow.superstep",
            "app.workflow.checkpoint",
            "app.workflow.interaction",
        ] {
            assert!(state.records.iter().any(|record| record.name == name
                && matches!(record.kind, TraceKind::Span { enters, exits } if enters > 0 && enters == exits)),
                "performed operation omitted its entered, completed promised span: {name}");
        }
        assert!(
            state.pending_polls > 0,
            "scope probe must include actual async pending polls"
        );
        assert!(
            state.entered.values().all(Vec::is_empty),
            "span guard escaped completed App cleanup"
        );
        assert!(
            state.violations.is_empty(),
            "tracing scope violations: {:?}",
            state.violations
        );
    }
}

struct TraceStore {
    native: StateStore,
    capture: TraceCapture,
}

struct TraceTransaction {
    native: StateTransaction,
    capture: TraceCapture,
    commits: Vec<TraceCommit>,
}

impl StateStoreBackend for TraceStore {
    type Transaction = TraceTransaction;
    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.native.load(key).await
    }
    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        let source = {
            let mut state = self.capture.state.lock().unwrap();
            if !matches!(state.source, TraceSource::Native) {
                state.source_calls += 1;
            }
            state.source
        };
        match source {
            TraceSource::Native => self.native.list(request).await,
            TraceSource::Pending => std::future::pending().await,
            TraceSource::Panic => panic!("original App storage future panic"),
        }
    }
    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        Ok(TraceTransaction {
            native: self.native.begin_transaction().await?,
            capture: self.capture.clone(),
            commits: Vec::new(),
        })
    }
}

impl StateTransactionBackend for TraceTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.native.load(key).await
    }
    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        let checkpoint = if record.key.namespace().as_str() == "app-sdk"
            && record.key.key().starts_with("history/")
        {
            record
                .value
                .get("id")
                .and_then(Value::as_str)
                .zip(
                    record
                        .value
                        .pointer("/checkpoint/sequence")
                        .and_then(Value::as_u64),
                )
                .zip(record.value.get("occurrence").and_then(Value::as_str))
                .map(|((run, sequence), occurrence)| TraceCommit::Checkpoint {
                    run: run.to_owned(),
                    occurrence: occurrence.to_owned(),
                    sequence,
                })
        } else {
            None
        };
        self.native.put(record).await?;
        if let Some(checkpoint) = checkpoint {
            self.commits.push(checkpoint);
        }
        Ok(())
    }
    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        let deleted = (key.namespace().as_str() == "app-sdk" && key.key().starts_with("run/"))
            .then(|| {
                let mut address = key.key().rsplit('/');
                let occurrence = address.next().unwrap().to_owned();
                let run = address.next().unwrap().to_owned();
                TraceCommit::Deleted { run, occurrence }
            });
        self.native.delete(key).await?;
        if let Some(deleted) = deleted {
            self.commits.push(deleted);
        }
        Ok(())
    }
    async fn commit(self) -> Result<(), StateStoreError> {
        self.native.commit().await?;
        // There is no await between the real commit receipt and recording it.
        // Events captured before this point cannot borrow a later success.
        self.capture
            .state
            .lock()
            .unwrap()
            .native_commits
            .extend(self.commits);
        Ok(())
    }
    async fn rollback(self) -> Result<(), StateStoreError> {
        self.native.rollback().await
    }
}

#[tokio::test]
async fn state_and_input_operations_emit_their_promised_poll_scoped_spans() {
    let capture = TraceCapture::exercise(TraceFailure::None, TraceCompletion::Resolve).await;
    capture.assert_spans();
    capture.assert_safe_committed_events();
}

#[tokio::test]
async fn subscriber_new_span_panic_does_not_change_workflow_outcome() {
    TraceCapture::exercise(
        TraceFailure::Armed(TraceCallback::NewSpan, "app.workflow.checkpoint"),
        TraceCompletion::Resolve,
    )
    .await
    .assert_callback_isolated(TraceCallback::NewSpan);
}

#[tokio::test]
async fn subscriber_event_panic_does_not_change_committed_workflow_outcome() {
    TraceCapture::exercise(
        TraceFailure::Armed(TraceCallback::Event, "app.workflow.checkpoint"),
        TraceCompletion::Resolve,
    )
    .await
    .assert_callback_isolated(TraceCallback::Event);
}

#[tokio::test]
async fn subscriber_enter_panic_does_not_change_workflow_outcome() {
    TraceCapture::exercise(
        TraceFailure::Armed(TraceCallback::Enter, "app.workflow.checkpoint"),
        TraceCompletion::Resolve,
    )
    .await
    .assert_callback_isolated(TraceCallback::Enter);
}

#[tokio::test]
async fn subscriber_exit_panic_does_not_change_workflow_outcome() {
    TraceCapture::exercise(
        TraceFailure::Armed(TraceCallback::Exit, "app.workflow.checkpoint"),
        TraceCompletion::Resolve,
    )
    .await
    .assert_callback_isolated(TraceCallback::Exit);
}

#[tokio::test]
async fn each_callers_background_work_and_agent_attempt_keep_the_admitting_dispatcher() {
    TraceCapture::exercise_caller_dispatchers().await;
}

#[tokio::test]
async fn subscriber_enabled_panic_does_not_change_workflow_outcome() {
    TraceCapture::exercise(
        TraceFailure::Armed(TraceCallback::Enabled, "app.workflow.checkpoint"),
        TraceCompletion::Resolve,
    )
    .await
    .assert_callback_isolated(TraceCallback::Enabled);
}

#[tokio::test]
async fn subscriber_event_enabled_panic_does_not_change_workflow_outcome() {
    TraceCapture::exercise(
        TraceFailure::Armed(TraceCallback::EventEnabled, "app.workflow.checkpoint"),
        TraceCompletion::Resolve,
    )
    .await
    .assert_callback_isolated(TraceCallback::EventEnabled);
}

#[tokio::test]
async fn subscriber_close_panic_does_not_change_workflow_outcome() {
    TraceCapture::exercise(
        TraceFailure::Armed(TraceCallback::Close, "app.workflow.checkpoint"),
        TraceCompletion::Resolve,
    )
    .await
    .assert_callback_isolated(TraceCallback::Close);
}

#[tokio::test]
async fn cancelling_an_app_future_contains_its_subscriber_close_panic() {
    TraceCapture::exercise_open_source(TraceSource::Pending).await;
}

#[tokio::test]
async fn operational_observation_preserves_an_actual_inner_future_panic_without_repolling() {
    TraceCapture::exercise_open_source(TraceSource::Panic).await;
}

#[tokio::test]
async fn dispatched_attempt_event_correlates_with_its_actual_unknown_control() {
    let case = NativeCase::open(
        vec![NativeCase::agent_definition(false, true)],
        true,
        PromptResponse::new(PRIVATE_CONTENT.into(), None),
    )
    .await;
    let peer = Arc::clone(&case.peer);
    peer.fail_next_prompt.store(true, Ordering::SeqCst);
    let capture = TraceCapture::default();
    let _subscriber = tracing::subscriber::set_default(capture.clone());
    let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(8), async {
        let run = case
            .workflow
            .start(ExecutionOptions::default())
            .await
            .unwrap();
        let unknown = run.wait().await.unwrap();
        let [
            Interaction::Unknown {
                sequence,
                generation,
                ..
            },
        ] = unknown.interactions.as_slice()
        else {
            panic!("accepted prompt failure must retain its actual Unknown target");
        };
        assert!(unknown.last_action_ordinal.is_none() && unknown.outcomes.is_empty());
        assert_eq!(unknown.state.values()["text"], "before");
        run.resolve(
            *sequence,
            *generation,
            Resolution::Accept(serde_json::Map::from_iter([
                ("text".into(), json!(PRIVATE_CONTENT)),
                ("structured".into(), json!({"accepted":true})),
            ])),
        )
        .await
        .unwrap();
        let complete = run.wait().await.unwrap();
        assert!(matches!(complete.status, RunStatus::Succeeded));
        assert_eq!(complete.state.values()["text"], PRIVATE_CONTENT);
        assert_eq!(
            complete.state.values()["structured"],
            json!({"accepted":true})
        );
        assert_eq!(complete.last_action_ordinal, Some(0));
        assert_eq!(complete.outcomes.len(), 1);
        assert_eq!(complete.outcomes[0].node, "agent");
        assert_eq!(complete.outcomes[0].ordinal, 0);
        assert!(
            complete.outcomes[0].result.is_empty(),
            "acceptance cannot invent a prompt result"
        );
        (run.id().to_string(), *sequence, unknown)
    }))
    .catch_unwind()
    .await;
    let closed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), case.close()))
        .catch_unwind()
        .await;
    let (run, sequence, unknown) = match observed {
        Ok(result) => result.expect("accepted failure tracing remains bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    };
    match closed {
        Ok(result) => result.expect("Agent and App cleanup join"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    assert_eq!(
        peer.ordinary_calls.load(Ordering::SeqCst),
        1,
        "tracing cannot replay the accepted prompt"
    );
    capture.assert_interaction_target(&unknown, "pending");
    capture.assert_interaction_target(&unknown, "applied");
    let state = capture.state.lock().unwrap();
    let dispatched = state
        .records
        .iter()
        .find(|record| {
            matches!(record.kind, TraceKind::Event)
                && record.fields.get("event").and_then(Value::as_str)
                    == Some("app.workflow.attempt")
                && record.fields.get("transition").and_then(Value::as_str) == Some("dispatched")
        })
        .expect("real dispatch must emit its metadata event");
    assert_eq!(
        dispatched.fields.get("run_id").and_then(Value::as_str),
        Some(run.as_str())
    );
    assert_eq!(
        dispatched.fields.get("control").and_then(Value::as_u64),
        Some(sequence),
        "the event must identify the exact native control later exposed as Unknown"
    );
    assert_eq!(
        dispatched.fields.get("attempt").and_then(Value::as_u64),
        Some(1)
    );
    for record in &state.records {
        let fields = serde_json::to_string(&record.fields).unwrap();
        assert!(!fields.contains(PRIVATE_CONTENT));
        assert!(
            !fields.contains("accepted fixture prompt lost its receipt"),
            "lower errors remain private"
        );
    }
}

#[tokio::test]
async fn cancelled_checkpoint_and_completed_deletion_events_follow_actual_native_commits() {
    TraceCapture::exercise(TraceFailure::None, TraceCompletion::CancelAndDelete)
        .await
        .assert_safe_committed_events();
}
