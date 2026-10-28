//! App-native result, ordinal and synchronous locality contracts.
//! These are not executed-rurr or live-Agent proofs.

pub mod native_support;

use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agents_kit::prompt::{PromptResponse, UsageInfo};
use app_kit::AppError;
use app_kit::workflow::{
    ActionMode, ExecutionEvent, ExecutionOptions, Interaction, LifecycleEvent, LifecycleHooks,
    Resolution, RunSelector, RunSnapshot, RunStatus, StateChange, StateTyping, SyncExecution,
    WorkflowFailure, WorkflowState,
};
use futures_util::FutureExt;
use native_support::NativeCase;
use serde_json::{Map, Value, json};

struct NativeResults;

#[test]
fn synchronous_native_state_keeps_typed_data_on_the_calling_thread() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("caller-owned lower runtime remains driven");
    let locality = NativeLocality::default();
    let definition = NativeCase::definition(
        "native",
        json!({"count":0}),
        vec![
            json!({"id":"state", "deadline_ms":1000, "step":{"kind":"state", "mutations":[
                {"kind":"increment", "target":"count", "value":{"kind":"literal", "value":1}}
            ]}}),
        ],
        json!({}),
        json!({"count":"reject_concurrent"}),
    );
    let case = runtime.block_on(NativeCase::open_with_typing(
        vec![definition],
        false,
        PromptResponse::new(String::new(), None),
        locality.clone(),
        locality.clone(),
    ));
    let observed = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let caller = std::thread::current().id();
        let mut run = runtime
            .block_on(case.workflow.prepare(WorkflowState::default()))
            .unwrap();
        // Initial admission validates typing through its own accepted owner.
        // Only the subsequently acquired Sync execution is caller-thread work.
        let prior_data = locality.data.lock().unwrap().len();
        let prior_modes = locality.modes.lock().unwrap().len();
        let prior_async = locality.async_hooks.load(Ordering::SeqCst);
        let mut execution = SyncExecution::open(&mut run, ExecutionOptions::default()).unwrap();
        let committed = execution.try_next().expect("native Sync activation");
        assert!(
            matches!(committed, Some(ExecutionEvent::Committed { outcomes, .. })
            if outcomes.len() == 1 && outcomes[0].node == "state" && outcomes[0].ordinal == 0
                && outcomes[0].result.is_empty())
        );
        let finished = execution.finish().expect("native Sync finalization");
        assert!(matches!(finished.status, RunStatus::Succeeded));
        assert_eq!(
            finished.state.values(),
            &Map::from_iter([("count".into(), json!(1))])
        );
        NativeResults::latest(&finished, "state", 0, json!({}));
        let data = locality.data.lock().unwrap()[prior_data..].to_vec();
        let modes = locality.modes.lock().unwrap()[prior_modes..].to_vec();
        assert!(
            !data.is_empty(),
            "native execution must actually construct caller Data"
        );
        assert!(
            data.iter().all(|(_, thread)| *thread == caller),
            "native Data construction, serialization, mutation and Drop stay on the Sync caller: {data:?}"
        );
        let constructed = data
            .iter()
            .filter(|(event, _)| *event == "construct")
            .count();
        assert!(constructed > 0);
        assert_eq!(
            data.iter()
                .filter(|(event, _)| *event == "serialize")
                .count(),
            constructed
        );
        assert_eq!(
            data.iter().filter(|(event, _)| *event == "drop").count(),
            constructed
        );
        assert_eq!(
            data.iter().filter(|(event, _)| *event == "mutate").count(),
            1
        );
        assert_eq!(
            modes,
            vec![ActionMode::Synchronous, ActionMode::Synchronous]
        );
        assert_eq!(locality.async_hooks.load(Ordering::SeqCst), prior_async);
        assert_eq!(case.peer.ordinary_calls.load(Ordering::SeqCst), 0);
        assert_eq!(case.peer.streaming_calls.load(Ordering::SeqCst), 0);
    }));
    let closed = runtime.block_on(async {
        AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), case.close()))
            .catch_unwind()
            .await
    });
    if let Err(failure) = observed {
        std::panic::resume_unwind(failure);
    }
    match closed {
        Ok(result) => result.expect("native Sync cleanup is bounded"),
        Err(failure) => std::panic::resume_unwind(failure),
    }
}

#[derive(Clone, Default)]
struct NativeLocality {
    data: Arc<Mutex<Vec<(&'static str, std::thread::ThreadId)>>>,
    modes: Arc<Mutex<Vec<ActionMode>>>,
    async_hooks: Arc<AtomicUsize>,
}

struct NativeData {
    state: Rc<WorkflowState>,
    locality: NativeLocality,
}

impl NativeLocality {
    fn record(&self, event: &'static str) {
        self.data
            .lock()
            .unwrap()
            .push((event, std::thread::current().id()));
    }
}

impl StateTyping for NativeLocality {
    type Data = NativeData;

    fn construct_data(&self, state: &WorkflowState) -> Result<Self::Data, AppError> {
        self.record("construct");
        Ok(NativeData {
            state: Rc::new(state.clone()),
            locality: self.clone(),
        })
    }

    fn construct_state(&self, data: &Self::Data) -> Result<WorkflowState, AppError> {
        self.record("serialize");
        Ok(data.state.as_ref().clone())
    }

    fn validate_pre(&self, _state: &WorkflowState, _change: &StateChange) -> Result<(), AppError> {
        self.record("mutate");
        Ok(())
    }
}

impl Drop for NativeData {
    fn drop(&mut self) {
        self.locality.record("drop");
    }
}

impl LifecycleHooks for NativeLocality {
    fn observe(&self, event: &LifecycleEvent<'_>) -> Result<(), AppError> {
        if let LifecycleEvent::StepStarted { context, .. }
        | LifecycleEvent::StepFinished { context, .. } = event
        {
            self.modes.lock().unwrap().push(context.mode);
        }
        Ok(())
    }

    async fn observe_async<'a>(&'a self, _event: &'a LifecycleEvent<'a>) -> Result<(), AppError> {
        self.async_hooks.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl NativeResults {
    async fn natural_structured_result(structured: Value) {
        let expected = json!({"output":"final", "structured_output":structured});
        assert!(serde_json::to_vec(&expected).unwrap().len() < 1_048_576);
        let case = NativeCase::open(
            vec![NativeCase::agent_definition(false, false)],
            true,
            PromptResponse::from_parts(
                "final".into(),
                Some(expected["structured_output"].clone()),
                None,
            ),
        )
        .await;
        let observed = AssertUnwindSafe(async {
            let snapshot = Self::terminal(&case).await;
            Self::latest(&snapshot, "agent", 0, expected);
            assert_eq!(
                snapshot.state.values().clone(),
                Map::from_iter([
                    ("sentinel".into(), json!(7)),
                    ("text".into(), json!("before")),
                    ("structured".into(), json!("before")),
                ])
            );
            assert_eq!(case.peer.ordinary_calls.load(Ordering::SeqCst), 1);
            assert_eq!(case.peer.streaming_calls.load(Ordering::SeqCst), 0);
        })
        .catch_unwind()
        .await;
        Self::close_after(case, observed).await;
    }

    async fn close_after(case: NativeCase, observed: std::thread::Result<()>) {
        let closed = AssertUnwindSafe(case.close()).catch_unwind().await;
        for result in [observed, closed] {
            if let Err(failure) = result {
                std::panic::resume_unwind(failure);
            }
        }
    }

    fn latest(snapshot: &RunSnapshot, node: &str, ordinal: u64, result: Value) {
        assert_eq!(snapshot.last_action_ordinal, Some(ordinal));
        assert_eq!(snapshot.outcomes.len(), 1);
        assert_eq!(snapshot.outcomes[0].node, node);
        assert_eq!(snapshot.outcomes[0].ordinal, ordinal);
        assert_eq!(Value::Object(snapshot.outcomes[0].result.clone()), result);
    }
    async fn terminal(case: &NativeCase) -> RunSnapshot {
        let run = case
            .workflow
            .start(ExecutionOptions::default())
            .await
            .expect("background admission");
        let waited = tokio::time::timeout(Duration::from_secs(5), run.wait()).await;
        if waited.is_err() {
            // Failure-only public evidence; the original five-second oracle
            // still fails even if a later read sees completed publication.
            let diagnostic = tokio::time::timeout(Duration::from_secs(2), run.snapshot()).await;
            match diagnostic {
                Ok(Ok(snapshot)) => eprintln!(
                    "native timeout: checkpoint={}, ordinal={:?}, outcomes={}, succeeded={}, failed={}, interactions={}, ordinary_calls={}",
                    snapshot.checkpoint,
                    snapshot.last_action_ordinal,
                    snapshot.outcomes.len(),
                    matches!(snapshot.status, RunStatus::Succeeded),
                    matches!(snapshot.status, RunStatus::Failed(_)),
                    snapshot.interactions.len(),
                    case.peer.ordinary_calls.load(Ordering::SeqCst),
                ),
                Ok(Err(error)) => eprintln!("native timeout snapshot error: {error}"),
                Err(_) => eprintln!("native timeout snapshot read remained pending"),
            }
        }
        let snapshot = waited.expect("native terminal progress").unwrap();
        assert!(matches!(snapshot.status, RunStatus::Succeeded));
        let readback = case
            .workflow
            .open_run(RunSelector::Latest(run.id().clone()))
            .await
            .unwrap()
            .snapshot()
            .await
            .unwrap();
        assert_eq!(readback.last_action_ordinal, snapshot.last_action_ordinal);
        assert_eq!(readback.outcomes.len(), snapshot.outcomes.len());
        for (actual, expected) in readback.outcomes.iter().zip(&snapshot.outcomes) {
            assert_eq!(actual.node, expected.node);
            assert_eq!(actual.ordinal, expected.ordinal);
            assert_eq!(actual.result, expected.result);
        }
        snapshot
    }
}

#[tokio::test]
async fn untargeted_agent_result_preserves_object_keys_beyond_state_key_grammar() {
    NativeResults::natural_structured_result(Value::Object(Map::from_iter([(
        "k".repeat(129),
        json!("exact result key"),
    )])))
    .await;
}

#[tokio::test]
async fn untargeted_agent_result_preserves_json_depth_beyond_state_structure_limits() {
    let mut structured = json!("leaf");
    for _ in 0..65 {
        structured = Value::Array(vec![structured]);
    }
    NativeResults::natural_structured_result(structured).await;
}

#[tokio::test]
async fn untargeted_agent_result_preserves_json_items_within_its_byte_budget() {
    NativeResults::natural_structured_result(Value::Array(vec![json!(0); 262_145])).await;
}

#[tokio::test]
async fn every_committed_state_join_and_sleep_activation_consumes_one_ordinal() {
    let all = [
        json!({"id":"state","step":{"kind":"state","mutations":[
            {"kind":"increment","target":"count","value":{"kind":"literal","value":1}},
            {"kind":"increment","target":"count","value":{"kind":"literal","value":2}}
        ]}}),
        json!({"id":"empty","step":{"kind":"state","mutations":[]}}),
        json!({"id":"join-all","step":{"kind":"join_all"}}),
        json!({"id":"join-first","step":{"kind":"join_first"}}),
        json!({"id":"sleep","step":{"kind":"sleep","message":"Brief wait","delay_ms":10}}),
    ];
    // Each prefix makes that native kind the latest committed wave, so no
    // historical API is required to verify its exact result and ordinal.
    for length in 1..=all.len() {
        let nodes = all[..length].to_vec();
        let definition = NativeCase::definition(
            "native",
            json!({"count":0}),
            nodes.clone(),
            NativeCase::linear(&nodes),
            json!({"count":"reject_concurrent"}),
        );
        let case = NativeCase::open(
            vec![definition],
            false,
            PromptResponse::new(String::new(), None),
        )
        .await;
        let snapshot = NativeResults::terminal(&case).await;
        NativeResults::latest(
            &snapshot,
            all[length - 1]["id"].as_str().unwrap(),
            (length - 1) as u64,
            json!({}),
        );
        assert_eq!(snapshot.state.values()["count"], 3);
        case.close().await;
    }
}

#[tokio::test]
async fn input_returns_exact_value_without_implicit_state_target_or_waiting_ordinal() {
    for target in [false, true] {
        let mut step =
            json!({"kind":"input","message":[{"kind":"text","text":"Provide an object"}]});
        if target {
            step["target"] = json!("answer");
        }
        let definition = NativeCase::definition(
            "native",
            json!({"answer":"before","sentinel":7}),
            vec![json!({"id":"input","step":step})],
            json!({}),
            json!({"answer":"reject_concurrent","sentinel":"reject_concurrent"}),
        );
        let case = NativeCase::open(
            vec![definition],
            false,
            PromptResponse::new(String::new(), None),
        )
        .await;
        let run = case
            .workflow
            .start(ExecutionOptions::default())
            .await
            .unwrap();
        let waiting = tokio::time::timeout(Duration::from_secs(3), run.wait())
            .await
            .unwrap()
            .unwrap();
        let [
            Interaction::Input {
                sequence,
                generation,
                ..
            },
        ] = waiting.interactions.as_slice()
        else {
            panic!("native input must park")
        };
        assert_eq!(waiting.last_action_ordinal, None);
        assert!(waiting.outcomes.is_empty());
        assert_eq!(waiting.state.values()["answer"], "before");
        let answer = json!({"nested":[null,42],"secret":"explicit input result"});
        run.resolve(*sequence, *generation, Resolution::Input(answer.clone()))
            .await
            .unwrap();
        let snapshot = tokio::time::timeout(Duration::from_secs(3), run.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(snapshot.status, RunStatus::Succeeded));
        NativeResults::latest(&snapshot, "input", 0, json!({"value":answer}));
        assert_eq!(
            snapshot.state.values()["answer"],
            if target { answer } else { json!("before") }
        );
        assert_eq!(snapshot.state.values()["sentinel"], 7);
        case.close().await;
    }
}

#[tokio::test]
async fn unary_agent_result_preserves_optional_null_usage_and_explicit_targets() {
    for (structured, usage, targets, expected) in [
        (None, None, false, json!({"output":"final"})),
        (
            Some(Value::Null),
            Some(UsageInfo::new(11, 7)),
            true,
            json!({"output":"final","structured_output":null,"usage":{"input_tokens":11,"output_tokens":7}}),
        ),
        (
            Some(json!({"answer":42})),
            None,
            true,
            json!({"output":"final","structured_output":{"answer":42}}),
        ),
    ] {
        let case = NativeCase::open(
            vec![NativeCase::agent_definition(false, targets)],
            true,
            PromptResponse::from_parts("final".into(), structured.clone(), usage),
        )
        .await;
        let snapshot = NativeResults::terminal(&case).await;
        NativeResults::latest(&snapshot, "agent", 0, expected.clone());
        assert_eq!(
            snapshot.outcomes[0].result.keys().collect::<Vec<_>>(),
            expected.as_object().unwrap().keys().collect::<Vec<_>>(),
            "native fields retain their specified order"
        );
        assert_eq!(
            snapshot.state.values()["text"],
            if targets { "final" } else { "before" }
        );
        assert_eq!(
            snapshot.state.values()["structured"],
            if targets {
                structured.unwrap()
            } else {
                json!("before")
            }
        );
        assert_eq!(snapshot.state.values()["sentinel"], 7);
        assert!(!snapshot.state.values().contains_key("output"));
        assert!(!snapshot.state.values().contains_key("usage"));
        assert_eq!(case.peer.ordinary_calls.load(Ordering::SeqCst), 1);
        assert_eq!(case.peer.streaming_calls.load(Ordering::SeqCst), 0);
        case.close().await;
    }
}

#[tokio::test]
async fn child_map_and_empty_map_each_have_one_parent_outcome_and_local_child_ordinals() {
    let all = [
        json!({"id":"child","step":{"kind":"child","workflow":"leaf","input":{"item":{"kind":"literal","value":7}},"reductions":[{"source":"item","target":"single","reducer":"reject_concurrent"}]}}),
        json!({"id":"map","step":{"kind":"map","workflow":"leaf","items":{"kind":"state","path":"items"},"input_key":"item","max_parallel":2,"reductions":[{"source":"item","target":"collected","reducer":"collect"}]}}),
        json!({"id":"empty-map","step":{"kind":"map","workflow":"leaf","items":{"kind":"literal","value":[]},"input_key":"item","max_parallel":1,"reductions":[]}}),
    ];
    for (index, step) in all.iter().enumerate() {
        // Distinct parent runs keep this result test independent of the pinned
        // cross-action default child-task identity reuse contract.
        let nodes = vec![
            json!({"id":"seed","step":{"kind":"state","mutations":[]}}),
            step.clone(),
        ];
        let parent = NativeCase::definition(
            "native",
            json!({"items":[1,2],"single":0,"collected":[]}),
            nodes.clone(),
            NativeCase::linear(&nodes),
            json!({"items":"reject_concurrent","single":"reject_concurrent","collected":"collect"}),
        );
        let leaf = NativeCase::definition(
            "leaf",
            json!({"item":0}),
            vec![
                json!({"id":"effect","step":{"kind":"state","mutations":[{"kind":"increment","target":"item","value":{"kind":"literal","value":1}}]}}),
            ],
            json!({}),
            json!({"item":"reject_concurrent"}),
        );
        let case = NativeCase::open(
            vec![parent, leaf],
            false,
            PromptResponse::new(String::new(), None),
        )
        .await;
        let snapshot = NativeResults::terminal(&case).await;
        NativeResults::latest(&snapshot, step["id"].as_str().unwrap(), 1, json!({}));
        assert_eq!(
            snapshot.state.values()["single"],
            if index == 0 { 8 } else { 0 }
        );
        assert_eq!(
            snapshot.state.values()["collected"],
            if index == 1 { json!([2, 3]) } else { json!([]) }
        );
        let workspaces = case
            .app
            .list_workspaces(None, NonZeroUsize::new(2).unwrap())
            .await
            .unwrap();
        let workspace = case.app.workspace(workspaces[0].id.clone()).await.unwrap();
        let leaf = workspace.workflow("leaf".parse().unwrap()).await.unwrap();
        let children = leaf
            .list_runs(None, NonZeroUsize::new(10).unwrap())
            .await
            .unwrap();
        assert_eq!(children.len(), [1, 2, 0][index]);
        for child in children {
            NativeResults::latest(&child, "effect", 0, json!({}));
        }
        case.close().await;
    }
}

#[tokio::test]
async fn rejected_parallel_wave_retains_prior_outcome_and_consumes_no_ordinal() {
    let nodes = vec![
        json!({"id":"seed","step":{"kind":"state","mutations":[]}}),
        json!({"id":"left","step":{"kind":"state","mutations":[{"kind":"assign","target":"count","value":{"kind":"literal","value":1}}]}}),
        json!({"id":"right","step":{"kind":"state","mutations":[{"kind":"assign","target":"count","value":{"kind":"literal","value":2}}]}}),
    ];
    let definition = NativeCase::definition(
        "native",
        json!({"count":0}),
        nodes,
        json!({"seed":{"mode":"broadcast","branches":[{"target":{"kind":"node","id":"right"}},{"target":{"kind":"node","id":"left"}}]}}),
        json!({"count":"reject_concurrent"}),
    );
    let case = NativeCase::open(
        vec![definition],
        false,
        PromptResponse::new(String::new(), None),
    )
    .await;
    let run = case
        .workflow
        .start(ExecutionOptions::default())
        .await
        .unwrap();
    let snapshot = tokio::time::timeout(Duration::from_secs(3), run.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        snapshot.status,
        RunStatus::Failed(WorkflowFailure::State(_))
    ));
    assert_eq!(snapshot.state.values()["count"], 0);
    NativeResults::latest(&snapshot, "seed", 0, json!({}));
    case.close().await;
}

#[tokio::test]
async fn successful_parallel_wave_retains_canonical_native_outcomes() {
    let definition = NativeCase::definition(
        "native",
        json!({"values":[]}),
        vec![
            json!({"id":"seed","step":{"kind":"state","mutations":[]}}),
            json!({"id":"z","step":{"kind":"state","mutations":[{"kind":"assign","target":"values","value":{"kind":"literal","value":"z"}}]}}),
            json!({"id":"a","step":{"kind":"state","mutations":[{"kind":"assign","target":"values","value":{"kind":"literal","value":"a"}}]}}),
        ],
        json!({"seed":{"mode":"broadcast","branches":[{"target":{"kind":"node","id":"z"}},{"target":{"kind":"node","id":"a"}}]}}),
        json!({"values":"collect"}),
    );
    let case = NativeCase::open(
        vec![definition],
        false,
        PromptResponse::new(String::new(), None),
    )
    .await;
    let snapshot = NativeResults::terminal(&case).await;
    assert_eq!(snapshot.state.values()["values"], json!(["a", "z"]));
    assert_eq!(snapshot.last_action_ordinal, Some(2));
    assert_eq!(snapshot.outcomes.len(), 2);
    for (outcome, (node, ordinal)) in snapshot.outcomes.iter().zip([("a", 1), ("z", 2)]) {
        assert_eq!(outcome.node, node);
        assert_eq!(outcome.ordinal, ordinal);
        assert!(outcome.result.is_empty());
    }
    case.close().await;
}

#[tokio::test]
async fn manually_accepted_unknown_agent_has_empty_result_and_never_replays_prompt() {
    let case = NativeCase::open(
        vec![NativeCase::agent_definition(false, true)],
        true,
        PromptResponse::new("must not be fabricated from manual targets".into(), None),
    )
    .await;
    let peer = Arc::clone(&case.peer);
    let observed = AssertUnwindSafe(async {
        case.peer.fail_next_prompt.store(true, Ordering::SeqCst);
        let run = case
            .workflow
            .start(ExecutionOptions::default())
            .await
            .unwrap();
        let unknown = tokio::time::timeout(Duration::from_secs(3), run.wait())
            .await
            .expect("unknown prompt receipt remains observable")
            .unwrap();
        let [
            Interaction::Unknown {
                sequence,
                generation,
                ..
            },
        ] = unknown.interactions.as_slice()
        else {
            panic!("lost lower prompt receipt must preserve Unknown")
        };
        assert_eq!(unknown.last_action_ordinal, None);
        assert!(unknown.outcomes.is_empty());
        let accepted = Map::from_iter([
            ("text".into(), json!("manual state")),
            ("structured".into(), json!({"manual":true})),
        ]);
        // The original target generation survives unrelated abort checkpoints.
        tokio::time::timeout(
            Duration::from_secs(3),
            run.resolve(*sequence, *generation, Resolution::Accept(accepted)),
        )
        .await
        .expect("explicit caller settlement remains bounded")
        .expect("the exact Unknown target needs no checkpoint-refresh retry");
        let snapshot = tokio::time::timeout(Duration::from_secs(3), run.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(snapshot.status, RunStatus::Succeeded));
        NativeResults::latest(&snapshot, "agent", 0, json!({}));
        assert_eq!(snapshot.state.values()["text"], "manual state");
        assert_eq!(
            snapshot.state.values()["structured"],
            json!({"manual":true})
        );
        assert_eq!(case.peer.ordinary_calls.load(Ordering::SeqCst), 1);
    })
    .catch_unwind()
    .await;
    NativeResults::close_after(case, observed).await;
    assert_eq!(peer.ordinary_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancellation_of_an_incomplete_wave_discards_staged_state_and_preserves_prior_outcome() {
    let definition = NativeCase::definition(
        "native",
        json!({"count":0}),
        vec![
            json!({"id":"seed","step":{"kind":"state","mutations":[]}}),
            json!({"id":"a-state","step":{"kind":"state","mutations":[{"kind":"assign","target":"count","value":{"kind":"literal","value":99}}]}}),
            json!({"id":"z-input","step":{"kind":"input","message":[{"kind":"text","text":"Keep this required sibling unresolved"}]}}),
        ],
        json!({"seed":{"mode":"broadcast","branches":[{"target":{"kind":"node","id":"a-state"}},{"target":{"kind":"node","id":"z-input"}}]}}),
        json!({"count":"reject_concurrent"}),
    );
    let case = NativeCase::open(
        vec![definition],
        false,
        PromptResponse::new(String::new(), None),
    )
    .await;
    let run = case
        .workflow
        .start(ExecutionOptions::default())
        .await
        .unwrap();
    let waiting = tokio::time::timeout(Duration::from_secs(3), run.wait())
        .await
        .expect("independent Input sibling parks")
        .unwrap();
    assert!(matches!(
        waiting.interactions.as_slice(),
        [Interaction::Input { .. }]
    ));
    assert_eq!(waiting.state.values()["count"], 0);
    NativeResults::latest(&waiting, "seed", 0, json!({}));
    run.cancel(waiting.checkpoint)
        .await
        .expect("explicit cancellation admission");
    let terminal = tokio::time::timeout(Duration::from_secs(3), run.wait())
        .await
        .expect("local cancellation settles")
        .unwrap();
    assert!(matches!(terminal.status, RunStatus::Cancelled));
    assert_eq!(
        terminal.state.values()["count"],
        0,
        "an incomplete wave cannot publish successful sibling state during cancellation"
    );
    NativeResults::latest(&terminal, "seed", 0, json!({}));
    case.close().await;
}

#[tokio::test]
async fn nested_overlapping_join_names_preserve_cycles_and_suppress_late_siblings() {
    // The child node and leaf JoinFirst are both named race. One and two
    // ancestors therefore contribute nonzero offsets to the same join name.
    // This public equivalence case must not depend on persisted counter values.
    for depth in 0..=2 {
        let leaf_id = if depth == 0 { "native" } else { "leaf" };
        let leaf = NativeCase::definition(
            leaf_id,
            json!({"visits":0,"loser_visits":0}),
            vec![
                json!({"id":"split","step":{"kind":"state","mutations":[]}}),
                json!({"id":"short","step":{"kind":"state","mutations":[]}}),
                json!({"id":"long-one","step":{"kind":"state","mutations":[]}}),
                json!({"id":"long-two","step":{"kind":"state","mutations":[
                    {"kind":"assign","target":"loser_visits","value":{"kind":"literal","value":1}}
                ]}}),
                json!({"id":"race","step":{"kind":"join_first"}}),
                json!({"id":"visit","step":{"kind":"state","mutations":[
                    {"kind":"assign","target":"visits","value":{"kind":"literal","value":1}}
                ]}}),
            ],
            json!({
                "split":{"mode":"broadcast","branches":[
                    {"target":{"kind":"node","id":"short"}},
                    {"target":{"kind":"node","id":"long-one"}}
                ]},
                "short":{"mode":"first","branches":[{"target":{"kind":"node","id":"race"}}]},
                "long-one":{"mode":"first","branches":[{"target":{"kind":"node","id":"long-two"}}]},
                "long-two":{"mode":"first","branches":[{"target":{"kind":"node","id":"race"}}]},
                "race":{"mode":"first","branches":[{"target":{"kind":"node","id":"visit"}}]},
                "visit":{"mode":"first","branches":[
                    {"when":{"kind":"less_than","left":{"kind":"state","path":"visits"},
                        "right":{"kind":"literal","value":2}},"target":{"kind":"node","id":"race"}},
                    {"target":{"kind":"end"}}
                ]}
            }),
            json!({"visits":"sum_integer","loser_visits":"sum_integer"}),
        );
        let mut definitions = vec![leaf];
        for level in 0..depth {
            let id = if level + 1 == depth {
                "native"
            } else {
                "middle"
            };
            let child = if level == 0 { "leaf" } else { "middle" };
            definitions.push(NativeCase::definition(
                id,
                json!({"visits":0,"loser_visits":0}),
                vec![json!({"id":"race","step":{"kind":"child","workflow":child,"input":{},
                    "reductions":[
                        {"source":"visits","target":"visits","reducer":"reject_concurrent"},
                        {"source":"loser_visits","target":"loser_visits","reducer":"reject_concurrent"}
                    ]}})],
                json!({}),
                json!({"visits":"reject_concurrent","loser_visits":"reject_concurrent"}),
            ));
        }
        let case =
            NativeCase::open(definitions, false, PromptResponse::new(String::new(), None)).await;
        let snapshot = NativeResults::terminal(&case).await;
        assert_eq!(
            snapshot.state.values().clone(),
            Map::from_iter([
                ("visits".into(), json!(2)),
                ("loser_visits".into(), json!(0))
            ]),
            "ancestor depth {depth}"
        );
        if depth == 0 {
            NativeResults::latest(&snapshot, "visit", 6, json!({}));
        } else {
            NativeResults::latest(&snapshot, "race", 0, json!({}));
            let workspaces = case
                .app
                .list_workspaces(None, NonZeroUsize::new(2).unwrap())
                .await
                .unwrap();
            let workspace = case.app.workspace(workspaces[0].id.clone()).await.unwrap();
            let leaf = workspace.workflow("leaf".parse().unwrap()).await.unwrap();
            let children = leaf
                .list_runs(None, NonZeroUsize::new(2).unwrap())
                .await
                .unwrap();
            assert_eq!(children.len(), 1);
            assert!(matches!(children[0].status, RunStatus::Succeeded));
            assert_eq!(children[0].state, snapshot.state);
            NativeResults::latest(&children[0], "visit", 6, json!({}));
            // Reattachment validates the actual descendant checkpoint without
            // executing either its winning cycle or its suppressed sibling.
            let reread = leaf
                .open_run(RunSelector::Exact {
                    id: children[0].id.clone(),
                    occurrence: children[0].occurrence.clone(),
                })
                .await
                .unwrap()
                .snapshot()
                .await
                .unwrap();
            assert_eq!(reread.state, children[0].state);
            NativeResults::latest(&reread, "visit", 6, json!({}));
        }
        case.close().await;
    }
}
