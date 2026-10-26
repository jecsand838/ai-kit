//! Caller-observed native Agent deltas and authoritative terminal results.
//! Non-streaming native result cases use the same direct lower owner separately.

pub mod native_support;

use agent_sdk::prompt::{PromptResponse, UsageInfo};
use app_sdk::AppError;
use app_sdk::workflow::{
    ExecutionEvent, ExecutionOptions, Interaction, LifecycleEvent, LifecycleHooks, Resolution,
    RunSelector, RunSnapshot, RunStatus, WorkflowFailure, WorkflowState,
};
use futures_util::FutureExt;
use native_support::{NativeCase, NativeStream};
use serde_json::{Value, json};
use std::any::Any;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

struct NativeHooks {
    events: Arc<Mutex<Vec<Value>>>,
    completed: Arc<Notify>,
}

impl NativeHooks {
    fn record(&self, event: &LifecycleEvent<'_>, lane: &str) {
        let mut row = match event {
            LifecycleEvent::StepStarted { .. } => json!({"event":"step-start"}),
            LifecycleEvent::StepFinished {
                outcome,
                checkpoint,
                ..
            } => json!({
                "event":"step-finish",
                "outcome":match outcome { None => "none", Some(Ok(_)) => "success", Some(Err(_)) => "failure" },
                "state":checkpoint.state.values(), "ordinal":checkpoint.action_ordinal
            }),
            LifecycleEvent::StreamStarted { .. } => json!({"event":"stream-start"}),
            LifecycleEvent::StreamItem { index, .. } => {
                json!({"event":"stream-item","index":index})
            }
            LifecycleEvent::StreamFinished { items, .. } => {
                json!({"event":"stream-finish","items":items})
            }
            LifecycleEvent::ExecuteFinished { snapshot, .. } => json!({
                "event":"execute-finish", "state":snapshot.state.values(),
                "ordinal":snapshot.last_action_ordinal, "outcomes":snapshot.outcomes.len()
            }),
            LifecycleEvent::Registered { .. }
            | LifecycleEvent::ChildFinished { .. }
            | LifecycleEvent::ExecuteStarted { .. }
            | LifecycleEvent::SpanStarted { .. }
            | LifecycleEvent::SpanAttribute { .. }
            | LifecycleEvent::SpanFinished { .. } => return,
        };
        row["lane"] = lane.into();
        self.events.lock().unwrap().push(row);
        if matches!(event, LifecycleEvent::ExecuteFinished { .. }) {
            self.completed.notify_one();
        }
    }
}

impl LifecycleHooks for NativeHooks {
    fn observe(&self, event: &LifecycleEvent<'_>) -> Result<(), AppError> {
        self.record(event, "sync");
        Ok(())
    }

    async fn observe_async<'a>(&'a self, event: &'a LifecycleEvent<'a>) -> Result<(), AppError> {
        self.record(event, "async");
        Ok(())
    }
}

struct NativeResults;
impl NativeResults {
    async fn unknown(behavior: NativeStream) {
        // Every arm returns a real stream and delivers two actual items before
        // the error/EOF gate opens. No App cancellation is requested, including
        // the lower Cancelled item: its spelling is not terminal evidence.
        let case = NativeCase::open(
            vec![NativeCase::agent_definition(true, true)],
            true,
            PromptResponse::from_parts(
                "unproven terminal must not commit".into(),
                Some(Value::Null),
                None,
            ),
        )
        .await;
        case.peer.configure_stream(behavior);
        let peer = Arc::clone(&case.peer);
        let observed =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(10), async {
                let mut run = case
                    .workflow
                    .prepare(WorkflowState::default())
                    .await
                    .unwrap();
                let mut execution = run.execution(ExecutionOptions::default()).await.unwrap();
                for text in ["draft ", "not the final answer"] {
                    NativeResults::partial(execution.next().await.unwrap(), text);
                }
                case.peer.terminal.add_permits(1);
                let unknown = execution.finish().await.unwrap();
                NativeResults::unchanged(&unknown);
                let [
                    Interaction::Unknown {
                        sequence,
                        generation,
                        retry_safe,
                    },
                ] = unknown.interactions.as_slice()
                else {
                    panic!("an accepted stream without terminal truth remains Unknown");
                };
                assert!(!*retry_safe);
                assert_eq!(peer.stream_drops.load(Ordering::SeqCst), 1);
                assert_eq!(peer.ordinary_calls.load(Ordering::SeqCst), 0);
                assert_eq!(peer.streaming_calls.load(Ordering::SeqCst), 1);
                assert_eq!(peer.accepted_streams.load(Ordering::SeqCst), 1);
                // The target generation survives unrelated abort checkpoints.
                let settled = run
                    .resolve(*sequence, *generation, Resolution::Fail)
                    .await
                    .expect("exact Unknown failure needs no checkpoint-refresh retry");
                NativeResults::unchanged(&settled);
                assert!(
                    matches!(settled.status, RunStatus::Paused),
                    "Prepared settlement requires resumed execution, got {}",
                    NativeResults::status(&settled.status)
                );
                assert_eq!(peer.streaming_calls.load(Ordering::SeqCst), 1);
                // The original stream captured its behavior before acceptance and
                // has now dropped. A forbidden new prompt must fail promptly, not
                // hide behind another terminal gate while the staged failure is lost.
                peer.configure_stream(NativeStream::Unsupported);
                // Resolution commits control truth before the existing cleanup and
                // hook owner releases. Busy is an ownership handoff, not permission
                // to resubmit the resolution or replay the accepted prompt.
                let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
                let failed = loop {
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "settled native cleanup must release exclusive execution"
                    );
                    let mut resumed = case
                        .workflow
                        .open_run(RunSelector::Latest(run.id().clone()))
                        .await
                        .unwrap();
                    let acquisition = resumed.execution(ExecutionOptions::default()).await;
                    match acquisition {
                        Ok(execution) => break execution.finish().await.unwrap(),
                        Err(AppError::Busy(_)) => {
                            let current = run.snapshot().await.unwrap();
                            assert!(matches!(current.status, RunStatus::Paused));
                            assert_eq!(current.checkpoint, settled.checkpoint);
                            assert_eq!(current.state.values(), settled.state.values());
                            assert_eq!(current.last_action_ordinal, settled.last_action_ordinal);
                            assert!(current.outcomes == settled.outcomes);
                            assert_eq!(peer.streaming_calls.load(Ordering::SeqCst), 1);
                            assert_eq!(peer.accepted_streams.load(Ordering::SeqCst), 1);
                            assert_eq!(peer.stream_drops.load(Ordering::SeqCst), 1);
                            assert_eq!(peer.ordinary_calls.load(Ordering::SeqCst), 0);
                            tokio::task::yield_now().await;
                        }
                        Err(error) => panic!("settled native execution failed: {error}"),
                    }
                };
                assert_eq!(
                    peer.streaming_calls.load(Ordering::SeqCst),
                    1,
                    "resuming a staged caller failure must not open another prompt"
                );
                assert!(
                    matches!(failed.status, RunStatus::Failed(_)),
                    "explicit failure returned checkpoint {} as {}",
                    failed.checkpoint,
                    NativeResults::status(&failed.status)
                );
                NativeResults::unchanged(&failed);
            }))
            .catch_unwind()
            .await;
        NativeResults::close_after(case, observed).await;
        assert_eq!(peer.streaming_calls.load(Ordering::SeqCst), 1);
        assert_eq!(peer.stream_drops.load(Ordering::SeqCst), 1);
    }

    fn unchanged(snapshot: &RunSnapshot) {
        assert_eq!(snapshot.last_action_ordinal, None);
        assert!(snapshot.outcomes.is_empty());
        assert_eq!(snapshot.state.values()["sentinel"], 7);
        assert_eq!(snapshot.state.values()["text"], "before");
        assert_eq!(snapshot.state.values()["structured"], "before");
    }

    fn status(status: &RunStatus) -> String {
        match status {
            RunStatus::Running => "Running".into(),
            RunStatus::Paused => "Paused".into(),
            RunStatus::Halted { .. } => "Halted".into(),
            RunStatus::Cancelling => "Cancelling".into(),
            RunStatus::Succeeded => "Succeeded".into(),
            RunStatus::Failed(_) => "Failed".into(),
            RunStatus::Cancelled => "Cancelled".into(),
            _ => "future public status".into(),
        }
    }

    fn partial(event: Option<ExecutionEvent>, text: &str) {
        let Some(ExecutionEvent::Partial { node, result }) = event else {
            panic!("the accepted native stream must expose each real delta");
        };
        assert_eq!(node, "agent");
        assert_eq!(Value::Object(result), json!({"output_delta":text}));
    }

    fn committed(event: Option<ExecutionEvent>, node: &str, ordinal: u64, result: Value) {
        let Some(ExecutionEvent::Committed { outcomes, .. }) = event else {
            panic!("native terminal requires one committed event")
        };
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].node, node);
        assert_eq!(outcomes[0].ordinal, ordinal);
        assert_eq!(Value::Object(outcomes[0].result.clone()), result);
    }

    async fn close_after<H: LifecycleHooks>(
        case: NativeCase<H>,
        observed: Result<Result<(), tokio::time::error::Elapsed>, Box<dyn Any + Send>>,
    ) {
        // Release even when an assertion or timeout drops the Execution early.
        // The actual accepted lower owner is joined before propagating failure.
        case.peer.stream_return.add_permits(1);
        case.peer.terminal.add_permits(1);
        let closed = std::panic::AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(5),
            case.close(),
        ))
        .catch_unwind()
        .await;
        match observed {
            Ok(result) => result.expect("native streaming scenario is bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        match closed {
            Ok(result) => result.expect("native streaming App cleanup joins"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
}

#[tokio::test]
async fn delayed_native_stream_return_retains_dropped_owner_guards_without_late_async_callbacks() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let completed = Arc::new(Notify::new());
    let case = NativeCase::open_with_hooks(
        vec![NativeCase::agent_definition(true, true)],
        true,
        PromptResponse::from_parts("accepted lower answer".into(), Some(Value::Null), None),
        NativeHooks {
            events: Arc::clone(&events),
            completed: Arc::clone(&completed),
        },
    )
    .await;
    let peer = Arc::clone(&case.peer);
    peer.hold_stream_return.store(true, Ordering::SeqCst);
    let observed = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(10), async {
        let mut run = case.workflow.prepare(WorkflowState::default()).await.unwrap();
        let observer = case.workflow.open_run(RunSelector::Latest(run.id().clone())).await.unwrap();
        let mut competing = case.workflow.open_run(RunSelector::Latest(run.id().clone())).await.unwrap();
        let mut execution = run.execution(ExecutionOptions::default()).await.unwrap();
        let mut next = Box::pin(execution.next());
        tokio::select! {
            _ = peer.stream_return_entered.notified() => {},
            result = &mut next => panic!("native next returned before the held source: {}", result.is_ok()),
        }
        assert_eq!(peer.streaming_calls.load(Ordering::SeqCst), 1);
        assert_eq!(peer.accepted_streams.load(Ordering::SeqCst), 1);
        assert_eq!(peer.produced_deltas.load(Ordering::SeqCst), 0);
        assert_eq!(peer.stream_drops.load(Ordering::SeqCst), 0);
        let before_drop = events.lock().unwrap().clone();
        assert_eq!(before_drop.iter().filter(|row| row["event"] == "step-start").count(), 2);
        assert!(!before_drop.iter().any(|row| row["event"] == "stream-start" || row["event"] == "stream-item"));
        drop(next);
        drop(execution);
        assert!(!events.lock().unwrap().iter().any(|row|
            row["event"] == "step-finish" || row["event"] == "stream-finish" || row["event"] == "execute-finish"),
            "accepted native publication keeps its guards until the real lower call settles");
        assert!(matches!(competing.execution(ExecutionOptions::default()).await, Err(AppError::Busy(_))));

        // Either terminal observation or the original abort owner may settle
        // accepted work. Drop does not require one particular drain strategy.
        peer.stream_return.add_permits(1);
        peer.terminal.add_permits(1);
        completed.notified().await;
        // The synchronous completion callback precedes the retained native
        // cleanup handoff. Observe its public settlement under the same deadline.
        let snapshot = loop {
            let snapshot = observer.snapshot().await.unwrap();
            NativeResults::unchanged(&snapshot);
            if matches!(snapshot.status, RunStatus::Running) && snapshot.interactions.is_empty() {
                tokio::task::yield_now().await;
            } else {
                break snapshot;
            }
        };
        assert!(matches!(snapshot.status, RunStatus::Paused)
            || matches!(snapshot.interactions.as_slice(), [Interaction::Unknown { retry_safe: false, .. }]),
            "uncommitted native cleanup returned {}", NativeResults::status(&snapshot.status));
        let after = events.lock().unwrap().clone();
        assert!(!after.iter().any(|row| row["lane"] == "async"
            && (row["event"] == "stream-start" || row["event"] == "stream-item")),
            "owner Drop cannot start new asynchronous stream callbacks");
        for name in ["step-finish", "execute-finish"] {
            let rows = after.iter().filter(|row| row["event"] == name).collect::<Vec<_>>();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0]["lane"], "sync");
            assert_eq!(rows[0]["ordinal"], Value::Null);
            assert_eq!(rows[0]["state"], json!({"sentinel":7,"text":"before","structured":"before"}));
        }
        let step = after.iter().find(|row| row["event"] == "step-finish").unwrap();
        assert_eq!(step["outcome"], "none", "accepted lower text is not a committed Action outcome");
        assert!(after.iter().filter(|row| row["event"] == "stream-finish").all(|row| row["lane"] == "sync"));
        assert_eq!(peer.streaming_calls.load(Ordering::SeqCst), 1);
        assert_eq!(peer.accepted_streams.load(Ordering::SeqCst), 1);
        assert_eq!(peer.ordinary_calls.load(Ordering::SeqCst), 0);
    })).catch_unwind().await;
    NativeResults::close_after(case, observed).await;
    assert_eq!(peer.streaming_calls.load(Ordering::SeqCst), 1);
    assert_eq!(peer.accepted_streams.load(Ordering::SeqCst), 1);
    assert_eq!(peer.stream_drops.load(Ordering::SeqCst), 1);
    assert!(peer.produced_deltas.load(Ordering::SeqCst) <= 2);
    assert!(
        peer.aborts.load(Ordering::SeqCst) <= 1,
        "definite abort cannot be replayed"
    );
}

#[tokio::test]
async fn agent_partials_are_exact_deltas_and_terminal_response_remains_authoritative() {
    for targets in [false, true] {
        let expected = json!({"output":"final answer","structured_output":{"answer":42},"usage":{"input_tokens":11,"output_tokens":7}});
        let case = NativeCase::open(
            vec![NativeCase::agent_definition(true, targets)],
            true,
            PromptResponse::from_parts(
                "final answer".into(),
                Some(json!({"answer":42})),
                Some(UsageInfo::new(11, 7)),
            ),
        )
        .await;
        let observed =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(10), async {
                let mut run = case
                    .workflow
                    .prepare(WorkflowState::default())
                    .await
                    .unwrap();
                let observer = case
                    .workflow
                    .open_run(RunSelector::Latest(run.id().clone()))
                    .await
                    .unwrap();
                let mut execution = run.execution(ExecutionOptions::default()).await.unwrap();
                for text in ["draft ", "not the final answer"] {
                    let event = tokio::time::timeout(Duration::from_secs(3), execution.next())
                        .await
                        .expect("a real delta must arrive before terminal release")
                        .unwrap();
                    NativeResults::partial(event, text);
                    let snapshot = observer.snapshot().await.unwrap();
                    assert_eq!(snapshot.last_action_ordinal, None);
                    assert!(snapshot.outcomes.is_empty());
                    assert_eq!(snapshot.state.values()["text"], "before");
                    assert_eq!(snapshot.state.values()["structured"], "before");
                }
                // Cancel this observation poll while the lower accepted result is held;
                // a later next must continue the same generation and lose no terminal.
                assert!(
                    tokio::time::timeout(Duration::from_millis(50), execution.next())
                        .await
                        .is_err()
                );
                case.peer.terminal.add_permits(1);
                let event = tokio::time::timeout(Duration::from_secs(3), execution.next())
                    .await
                    .expect("released terminal commits")
                    .unwrap();
                NativeResults::committed(event, "agent", 0, expected);
                assert!(execution.next().await.unwrap().is_none());
                let snapshot = execution.finish().await.unwrap();
                assert_eq!(snapshot.last_action_ordinal, Some(0));
                assert_eq!(
                    snapshot.state.values()["text"],
                    if targets { "final answer" } else { "before" }
                );
                assert_eq!(
                    snapshot.state.values()["structured"],
                    if targets {
                        json!({"answer":42})
                    } else {
                        json!("before")
                    }
                );
                assert_eq!(case.peer.ordinary_calls.load(Ordering::SeqCst), 0);
                assert_eq!(case.peer.streaming_calls.load(Ordering::SeqCst), 1);
            }))
            .catch_unwind()
            .await;
        NativeResults::close_after(case, observed).await;
    }
}

#[tokio::test]
async fn explicit_nonstreaming_agent_uses_unary_without_synthetic_partials() {
    let case = NativeCase::open(
        vec![NativeCase::agent_definition(false, true)],
        true,
        PromptResponse::from_parts("unary terminal".into(), Some(Value::Null), None),
    )
    .await;
    let observed =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(10), async {
            let mut run = case
                .workflow
                .prepare(WorkflowState::default())
                .await
                .unwrap();
            let mut execution = run.execution(ExecutionOptions::default()).await.unwrap();
            NativeResults::committed(
                execution.next().await.unwrap(),
                "agent",
                0,
                json!({"output":"unary terminal","structured_output":null}),
            );
            assert!(execution.next().await.unwrap().is_none());
            let snapshot = execution.finish().await.unwrap();
            assert!(matches!(snapshot.status, RunStatus::Succeeded));
            assert_eq!(snapshot.state.values()["text"], "unary terminal");
            assert_eq!(snapshot.state.values()["structured"], Value::Null);
            assert_eq!(case.peer.ordinary_calls.load(Ordering::SeqCst), 1);
            assert_eq!(case.peer.streaming_calls.load(Ordering::SeqCst), 0);
            assert_eq!(case.peer.accepted_streams.load(Ordering::SeqCst), 0);
            assert_eq!(case.peer.produced_deltas.load(Ordering::SeqCst), 0);
        }))
        .catch_unwind()
        .await;
    NativeResults::close_after(case, observed).await;
}

#[tokio::test]
async fn unsupported_stream_opening_preserves_uncertainty_and_redacts_lower_detail() {
    let case = NativeCase::open(
        vec![NativeCase::agent_definition(true, true)],
        true,
        PromptResponse::from_parts(
            "must not use unary fallback".into(),
            Some(Value::Null),
            None,
        ),
    )
    .await;
    case.peer.configure_stream(NativeStream::Unsupported);
    let observed =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(10), async {
            let mut run = case
                .workflow
                .prepare(WorkflowState::default())
                .await
                .unwrap();
            let execution = run.execution(ExecutionOptions::default()).await.unwrap();
            let snapshot = execution.finish().await.unwrap();
            NativeResults::unchanged(&snapshot);
            let [
                Interaction::Unknown {
                    sequence,
                    generation,
                    retry_safe,
                },
            ] = snapshot.interactions.as_slice()
            else {
                panic!("an opening error cannot establish lower prompt acceptance");
            };
            assert!(!*retry_safe);
            assert_eq!(case.peer.ordinary_calls.load(Ordering::SeqCst), 0);
            assert_eq!(case.peer.streaming_calls.load(Ordering::SeqCst), 1);
            assert_eq!(case.peer.accepted_streams.load(Ordering::SeqCst), 0);
            assert_eq!(case.peer.produced_deltas.load(Ordering::SeqCst), 0);
            assert_eq!(case.peer.stream_drops.load(Ordering::SeqCst), 0);
            // The fixture knows it returned before constructing a stream. App
            // receives no admission receipt carrying that private knowledge.
            let settled = run
                .resolve(*sequence, *generation, Resolution::Fail)
                .await
                .expect("the caller can fail the exact uncertain opening");
            NativeResults::unchanged(&settled);
            assert!(matches!(settled.status, RunStatus::Paused));
            let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
            let failed = loop {
                assert!(tokio::time::Instant::now() < deadline);
                let mut resumed = case
                    .workflow
                    .open_run(RunSelector::Latest(run.id().clone()))
                    .await
                    .unwrap();
                match resumed.execution(ExecutionOptions::default()).await {
                    Ok(execution) => break execution.finish().await.unwrap(),
                    Err(AppError::Busy(_)) => {
                        let current = run.snapshot().await.unwrap();
                        assert!(matches!(current.status, RunStatus::Paused));
                        assert_eq!(current.checkpoint, settled.checkpoint);
                        NativeResults::unchanged(&current);
                        assert_eq!(case.peer.streaming_calls.load(Ordering::SeqCst), 1);
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("settled opening cleanup failed: {error}"),
                }
            };
            NativeResults::unchanged(&failed);
            let RunStatus::Failed(WorkflowFailure::Agent(message)) = failed.status else {
                panic!("explicit caller failure must become the terminal Agent outcome");
            };
            assert!(!message.contains("private unsupported stream detail"));
            assert_eq!(case.peer.ordinary_calls.load(Ordering::SeqCst), 0);
            assert_eq!(case.peer.streaming_calls.load(Ordering::SeqCst), 1);
            assert_eq!(case.peer.accepted_streams.load(Ordering::SeqCst), 0);
            assert_eq!(case.peer.produced_deltas.load(Ordering::SeqCst), 0);
            assert_eq!(case.peer.stream_drops.load(Ordering::SeqCst), 0);
        }))
        .catch_unwind()
        .await;
    NativeResults::close_after(case, observed).await;
}

#[tokio::test]
async fn accepted_native_stream_error_and_eof_preserve_unknown_without_replay() {
    for behavior in [NativeStream::AcceptedError, NativeStream::Eof] {
        NativeResults::unknown(behavior).await;
    }
}

#[tokio::test]
async fn accepted_native_stream_unsupported_item_preserves_unknown() {
    NativeResults::unknown(NativeStream::AcceptedUnsupported).await;
}

#[tokio::test]
async fn accepted_native_stream_invalid_input_item_preserves_unknown() {
    NativeResults::unknown(NativeStream::AcceptedInvalidInput).await;
}

#[tokio::test]
async fn accepted_native_stream_closed_item_preserves_unknown() {
    NativeResults::unknown(NativeStream::AcceptedClosed).await;
}

#[tokio::test]
async fn accepted_native_stream_cancelled_item_preserves_unknown_without_app_cancellation() {
    NativeResults::unknown(NativeStream::AcceptedCancelled).await;
}

#[tokio::test]
async fn stalled_native_output_is_bounded_and_public_cancellation_drops_its_source_once() {
    let case = NativeCase::open(
        vec![NativeCase::agent_definition(true, true)],
        true,
        PromptResponse::from_parts(
            "cancelled terminal must not commit".into(),
            Some(Value::Null),
            None,
        ),
    )
    .await;
    case.peer
        .configure_stream(NativeStream::ManyDeltas { count: 256 });
    let peer = Arc::clone(&case.peer);
    let observed =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(10), async {
            let mut run = case
                .workflow
                .prepare(WorkflowState::default())
                .await
                .unwrap();
            let observer = case.workflow.open_run(RunSelector::Latest(run.id().clone())).await.unwrap();
            let mut execution = run.execution(ExecutionOptions::default()).await.unwrap();
            NativeResults::partial(execution.next().await.unwrap(), "delta-0");
            // Give the accepted producer time to reach its bounded output
            // handoff while this caller deliberately does not request more.
            tokio::time::sleep(Duration::from_millis(50)).await;
            let produced = peer.produced_deltas.load(Ordering::SeqCst);
            assert!(
                (1..256).contains(&produced),
                "stalled output eagerly drained all {produced} deltas"
            );
            let current = observer.snapshot().await.unwrap();
            NativeResults::unchanged(&current);
            let cancellation =
                tokio::time::timeout(Duration::from_secs(3), observer.cancel(current.checkpoint))
                    .await
                    .expect("cancellation is responsive while output is stalled")
                    .unwrap();
            assert!(matches!(
                cancellation.status,
                RunStatus::Cancelling | RunStatus::Cancelled
            ));
            drop(execution);
            let cancelled = tokio::time::timeout(Duration::from_secs(3), observer.wait())
                .await
                .expect("owned stream cleanup converges after explicit cancellation")
                .unwrap();
            assert!(matches!(cancelled.status, RunStatus::Cancelled),
                "explicit cancellation returned checkpoint {} as {}; lower aborts={}, source drops={}",
                cancelled.checkpoint, NativeResults::status(&cancelled.status),
                peer.aborts.load(Ordering::SeqCst), peer.stream_drops.load(Ordering::SeqCst));
            NativeResults::unchanged(&cancelled);
            assert_eq!(peer.ordinary_calls.load(Ordering::SeqCst), 0);
            assert_eq!(peer.streaming_calls.load(Ordering::SeqCst), 1);
            assert_eq!(peer.accepted_streams.load(Ordering::SeqCst), 1);
            assert_eq!(peer.aborts.load(Ordering::SeqCst), 1);
            assert_eq!(peer.stream_drops.load(Ordering::SeqCst), 1);
        }))
        .catch_unwind()
        .await;
    NativeResults::close_after(case, observed).await;
    assert_eq!(peer.streaming_calls.load(Ordering::SeqCst), 1);
    assert_eq!(peer.stream_drops.load(Ordering::SeqCst), 1);
}

#[derive(Clone, Copy)]
enum OccurrenceCopyCase {
    SavedAction,
    NoSavedAction,
}

impl NativeResults {
    async fn occurrence_copy_uncertainty(selected: OccurrenceCopyCase) {
        let root = NativeCase::definition(
            "native",
            json!({"count":0,"collected":[]}),
            vec![
                json!({"id":"compose","deadline_ms":10000,"step":{"kind":"map","max_parallel":3,
                "tasks":{"kind":"literal","value":[
                    {"task_key":"same","workflow":"occurrence-leaf","revision":"saved","input":{"count":10}},
                    {"task_key":"same","workflow":"occurrence-leaf","revision":"unknown","input":{"count":20}},
                    {"task_key":"same","workflow":"occurrence-leaf","revision":"fresh","input":{"count":30}}
                ]},"reductions":[{"source":"count","target":"collected","reducer":"collect"}]}}),
            ],
            json!({}),
            json!({"count":"reject_concurrent","collected":"collect"}),
        );
        let mut definitions = vec![root];
        for (revision, step) in [
            (
                "saved",
                match selected {
                    OccurrenceCopyCase::SavedAction => {
                        json!({"kind":"state","mutations":[{"kind":"increment","target":"count","value":{"kind":"literal","value":1}}]})
                    }
                    OccurrenceCopyCase::NoSavedAction => {
                        json!({"kind":"input","message":[{"kind":"text","text":"No saved action"}]})
                    }
                },
            ),
            (
                "unknown",
                json!({"kind":"agent","agent":"native","stream":true,"prompt":[{"kind":"text","text":"Retain the original accepted uncertainty"}]}),
            ),
            (
                "fresh",
                json!({"kind":"input","message":[{"kind":"text","text":"Fresh control only"}]}),
            ),
        ] {
            definitions.push(app_sdk::workflow::WorkflowDefinition::parse(&json!({
                "schema":"app-sdk.workflow/v2","id":"occurrence-leaf","revision":revision,
                "state":{"count":0},"required":["count"],"reducers":{"count":"reject_concurrent"},
                "entry":"work","nodes":[{"id":"work","deadline_ms":10000,"step":step}],"routes":{}
            }).to_string()).unwrap());
        }
        let case = NativeCase::open(
            definitions,
            true,
            PromptResponse::from_parts("unproven".into(), None, None),
        )
        .await;
        case.peer.configure_stream(NativeStream::AcceptedError);
        case.peer.terminal.add_permits(1);
        let observed = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(8), async {
            let workspace = case.app.list_workspaces(None, std::num::NonZeroUsize::new(1).unwrap()).await.unwrap()
                .into_iter().next().unwrap();
            let workspace = case.app.workspace(workspace.id).await.unwrap();
            let leaf = workspace.workflow("occurrence-leaf".parse().unwrap()).await.unwrap();
            let parent = case.workflow.start(ExecutionOptions::default()).await.unwrap();
            let mut rows = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let mut rows = leaf.list_runs(None, std::num::NonZeroUsize::new(4).unwrap()).await.unwrap();
                    rows.sort_by_key(|row| row.state.values()["count"].as_i64().unwrap());
                    if rows.len() == 3
                        && matches!(rows[1].interactions.as_slice(), [Interaction::Unknown { retry_safe:false, .. }])
                        && matches!(rows[2].interactions.as_slice(), [Interaction::Input { .. }])
                        && match selected {
                            OccurrenceCopyCase::SavedAction => matches!(rows[0].status, RunStatus::Succeeded),
                            OccurrenceCopyCase::NoSavedAction => matches!(rows[0].interactions.as_slice(), [Interaction::Input { .. }]),
                        }
                        && case.peer.aborts.load(Ordering::SeqCst) == 1
                    { break rows; }
                    tokio::task::yield_now().await;
                }
            }).await.expect("A, the original accepted Unknown B, and control-only C remain independent");
            assert_eq!(rows[0].id, rows[1].id);
            assert_eq!(rows[1].id, rows[2].id);
            let mut exact = Vec::new();
            for row in &rows {
                exact.push(leaf.open_run(RunSelector::Exact { id:row.id.clone(), occurrence:row.occurrence.clone() }).await.unwrap());
            }
            if matches!(selected, OccurrenceCopyCase::NoSavedAction) {
                exact[0].cancel(rows[0].checkpoint).await.unwrap();
                rows[0] = exact[0].wait().await.unwrap();
            }
            exact[2].cancel(rows[2].checkpoint).await.unwrap();
            rows[2] = exact[2].wait().await.unwrap();
            assert!(matches!(rows[2].status, RunStatus::Cancelled));
            assert!(rows[2].last_action_ordinal.is_none() && rows[2].outcomes.is_empty());
            let control_latest = leaf.open_run(RunSelector::Latest(rows[0].id.clone())).await.unwrap().snapshot().await.unwrap();
            assert_eq!(control_latest.occurrence, rows[2].occurrence,
                "a later clean C control is the logical App head while B remains Unknown");
            let latest_options = || app_sdk::workflow::RestoreOptions {
                source:RunSelector::Latest(rows[0].id.clone()), checkpoint:app_sdk::workflow::CheckpointSelector::Latest,
                overrides:WorkflowState::default(), position:app_sdk::workflow::ResumePosition::Entrypoint,
                fallback:Some(WorkflowState::new(serde_json::Map::from_iter([("count".into(),json!(99))])).unwrap()),
            };
            assert!(matches!(leaf.restore(latest_options()).await, Err(AppError::Conflict(_))),
                "aggregate implicit Latest cannot bypass B through A, clean C, or the absent-save fallback");
            let mut exact_b = latest_options();
            exact_b.source = RunSelector::Exact { id:rows[1].id.clone(), occurrence:rows[1].occurrence.clone() };
            assert!(matches!(leaf.restore(exact_b).await, Err(AppError::Conflict(_))),
                "Exact B still requires clean current authority for implicit Latest");
            if matches!(selected, OccurrenceCopyCase::SavedAction) {
                let mut exact_a = latest_options();
                exact_a.source = RunSelector::Exact { id:rows[0].id.clone(), occurrence:rows[0].occurrence.clone() };
                let copied = leaf.restore(exact_a).await.unwrap().checkpoint(app_sdk::workflow::CheckpointSelector::Sequence(0)).await.unwrap();
                assert_eq!(copied.state.values()["count"],11);
                assert_eq!(copied.lineage.as_ref().unwrap().source_occurrence,rows[0].occurrence);
                let mut historical = latest_options();
                historical.checkpoint = app_sdk::workflow::CheckpointSelector::Action(0);
                let copied = leaf.restore(historical).await.unwrap().checkpoint(app_sdk::workflow::CheckpointSelector::Sequence(0)).await.unwrap();
                assert_eq!(copied.state.values()["count"],11,
                    "explicit historical Action authorization can select A despite B's current uncertainty");
            }
            let mut historical_b = latest_options();
            historical_b.source = RunSelector::Exact { id:rows[1].id.clone(), occurrence:rows[1].occurrence.clone() };
            historical_b.checkpoint = app_sdk::workflow::CheckpointSelector::Sequence(0);
            let copied = leaf.restore(historical_b).await.unwrap().checkpoint(app_sdk::workflow::CheckpointSelector::Sequence(0)).await.unwrap();
            assert_eq!(copied.state.values()["count"],20);
            assert_eq!(copied.lineage.as_ref().unwrap().source_occurrence,rows[1].occurrence);
            let unknown = exact[1].snapshot().await.unwrap();
            assert_eq!(unknown.state.values()["count"],20);
            assert!(unknown.last_action_ordinal.is_none() && unknown.outcomes.is_empty());
            let [Interaction::Unknown { sequence,generation,.. }] = unknown.interactions.as_slice() else { panic!("copy requests cannot settle original B") };
            assert_eq!(case.peer.streaming_calls.load(Ordering::SeqCst),1);
            assert_eq!(case.peer.accepted_streams.load(Ordering::SeqCst),1);
            assert_eq!(case.peer.produced_deltas.load(Ordering::SeqCst),2);
            assert_eq!(case.peer.stream_drops.load(Ordering::SeqCst),1);
            assert_eq!(case.peer.ordinary_calls.load(Ordering::SeqCst),0);
            let unchanged = parent.snapshot().await.unwrap();
            assert_eq!(unchanged.state.values()["collected"],json!([]));
            assert!(unchanged.last_action_ordinal.is_none() && unchanged.outcomes.is_empty());
            exact[1].resolve(*sequence,*generation,Resolution::Fail).await.unwrap();
            let failed = parent.wait().await.unwrap();
            assert!(matches!(failed.status,RunStatus::Failed(_)));
            assert_eq!(failed.state,unchanged.state);
            assert_eq!(case.peer.streaming_calls.load(Ordering::SeqCst),1);
        })).catch_unwind().await;
        NativeResults::close_after(case, observed).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aggregate_latest_cannot_bypass_an_unknown_occurrence_through_a_cleaner_later_head() {
    NativeResults::occurrence_copy_uncertainty(OccurrenceCopyCase::SavedAction).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aggregate_latest_absent_save_fallback_is_guarded_by_every_current_occurrence() {
    NativeResults::occurrence_copy_uncertainty(OccurrenceCopyCase::NoSavedAction).await;
}
