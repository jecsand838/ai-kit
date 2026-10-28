//! Tracking projections of real public checkpoint/history/restore values.
//!
//! Source-derived, not executed-rurr evidence: rurr-main.zip SHA256
//! 5b5f20eb1d233467c600dc9dd249feaaa9046c5a303ae4e937040be5b6ecf25e,
//! commit 005bfe3ece631e64b06dac2d42ef84235f727a4f; rurr-core/src/
//! application/step_txn.rs, tracking/lifecycle_events.rs:652-672,
//! tracking/client/registration.rs:45-108 and state.rs:316-365.
//! These cases need the approved public history/restore APIs. Until those are
//! wired, their missing-API compile failure is not runtime tracking evidence.
//! Native v1/start(Map) setup follows the currently compiled grammar; it moves
//! with the single planned grammar/start cutover, without a compatibility lane.
//! Hook dispatch is separate: the events here borrow actual returned history.

use std::num::NonZeroUsize;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use agents_kit::Agent;
use app_kit::workflow::{
    ActionContext, ActionMode, Checkpoint, CheckpointKind, CheckpointSelector, ExecutionOptions,
    GraphDescription, LifecycleEvent, RestoreOptions, ResumePosition, RunSelector, RunSnapshot,
    RunStatus, StateSerialization, WorkflowDefinition, WorkflowState,
};
use app_kit::{App, AppConfig, AppError, Shutdown, WorkspaceConfig, WorkspaceId};
use futures_util::FutureExt;
use serde_json::{Value, json};
use storage_kit::StateStore;
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};

#[derive(Clone, Copy)]
enum NativeSteps {
    Success,
    FailedAfterA,
    FailedInitially,
}

struct TrackingHistory {
    workspace: WorkspaceId,
    snapshot: RunSnapshot,
    checkpoints: Vec<Checkpoint>,
    graph: GraphDescription,
    restored: Option<Checkpoint>,
}

impl TrackingHistory {
    fn time() -> SystemTime {
        UNIX_EPOCH + Duration::new(951_782_400, 123_456_789)
    }

    fn definition(steps: NativeSteps) -> WorkflowDefinition {
        let bad = json!({"kind":"state", "mutations":[
            {"kind":"assign", "target":"count", "value":{"kind":"literal","value":99}},
            {"kind":"assign", "target":"count", "value":{"kind":"state","path":"missing"}}
        ]});
        let good = json!({"kind":"state", "mutations":[
            {"kind":"assign", "target":"count", "value":{"kind":"literal","value":2}}
        ]});
        WorkflowDefinition::parse(&json!({
            "schema":"app-kit.workflow/v2", "id":"tracking-history", "revision":"r1",
            "state":{"count":0,"sentinel":"kept","__SEQUENCE_ID":900,"__PRIOR_STEP":"user-origin"},
            "required":["count","sentinel"],
            "reducers":{"count":"reject_concurrent","sentinel":"reject_concurrent",
                "__SEQUENCE_ID":"reject_concurrent","__PRIOR_STEP":"reject_concurrent"},
            "entry":if matches!(steps, NativeSteps::FailedInitially) {"b"} else {"a"},
            "nodes":if matches!(steps, NativeSteps::FailedInitially) {
                json!([{"id":"b","step":bad}])
            } else {
                json!([
                    {"id":"a","step":{"kind":"state","mutations":[
                        {"kind":"assign","target":"count","value":{"kind":"literal","value":1}}
                    ]}},
                    {"id":"b","step":if matches!(steps, NativeSteps::Success) {good} else {bad}}
                ])
            },
            "routes":if matches!(steps, NativeSteps::FailedInitially) {json!({})} else {
                json!({"a":{"mode":"first","branches":[{"target":{"kind":"node","id":"b"}}]}})
            }
        }).to_string()).expect("ordinary native State definitions")
    }

    async fn capture(steps: NativeSteps, restore: bool) -> Self {
        let definition = Self::definition(steps);
        let graph = definition.graph();
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("real transactional store");
        let config: AppConfig<StateStore, Agent> =
            AppConfig::new(store, Vec::new(), vec![definition]);
        let mut app = App::open(config).await.expect("native state-only App");
        let observed = std::panic::AssertUnwindSafe(async {
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().unwrap(),
                })
                .await
                .expect("workspace");
            let workflow = workspace
                .workflow("tracking-history".parse().unwrap())
                .await
                .expect("workflow");
            let run = workflow
                .start(ExecutionOptions::default())
                .await
                .expect("ordinary native admission");
            let snapshot = tokio::time::timeout(Duration::from_secs(5), run.wait())
                .await
                .expect("bounded native execution")
                .expect("terminal snapshot");
            assert!(matches!(
                &snapshot.status,
                RunStatus::Succeeded | RunStatus::Failed(_)
            ));
            let initial = run
                .checkpoint(CheckpointSelector::Sequence(0))
                .await
                .expect("public initial checkpoint");
            assert!(matches!(initial.kind, CheckpointKind::Initial));
            assert_eq!(initial.state.values()["count"], 0);
            let mut after = None;
            let mut checkpoints = Vec::new();
            for page_index in 0..32 {
                let page = run
                    .history(after, NonZeroUsize::new(2).unwrap())
                    .await
                    .expect("public bounded history page");
                assert!(
                    !page.checkpoints.is_empty(),
                    "native history must make progress"
                );
                for checkpoint in page.checkpoints {
                    if let Some(previous) = checkpoints.last() {
                        let previous: &Checkpoint = previous;
                        assert!(checkpoint.sequence > previous.sequence);
                    }
                    checkpoints.push(checkpoint);
                }
                after = page.next;
                if after.is_none() {
                    break;
                }
                assert!(
                    page_index < 31,
                    "finite native workflow history must terminate"
                );
            }
            assert_eq!(checkpoints.first().unwrap().sequence, 0);
            assert_eq!(checkpoints.last().unwrap().sequence, snapshot.checkpoint);
            let restored = if restore {
                let source = checkpoints
                    .iter()
                    .find(|checkpoint| {
                        matches!(checkpoint.kind, CheckpointKind::Action)
                            && checkpoint.position.as_deref() == Some("a")
                    })
                    .expect("actual committed A boundary");
                let fork = workflow
                    .restore(RestoreOptions {
                        source: RunSelector::Latest(run.id().clone()),
                        checkpoint: CheckpointSelector::Sequence(source.sequence),
                        overrides: WorkflowState::default(),
                        position: ResumePosition::AfterCompleted,
                        fallback: None,
                    })
                    .await
                    .expect("actual restore creates a new run");
                assert_ne!(fork.id(), run.id());
                let checkpoint = fork
                    .checkpoint(CheckpointSelector::Sequence(0))
                    .await
                    .expect("real restored initial checkpoint");
                assert_eq!(checkpoint.state, source.state);
                assert_eq!(checkpoint.position, source.position);
                assert_eq!(checkpoint.action_ordinal, source.action_ordinal);
                assert!(
                    checkpoint.outcomes.is_empty(),
                    "restore invents no local result"
                );
                let original = run
                    .checkpoint(CheckpointSelector::Sequence(source.sequence))
                    .await
                    .expect("source history remains readable");
                assert_eq!(original.state, source.state);
                assert!(original.outcomes == source.outcomes);
                Some(checkpoint)
            } else {
                None
            };
            Self {
                workspace: workspace.id().clone(),
                snapshot,
                checkpoints,
                graph,
                restored,
            }
        })
        .catch_unwind()
        .await;
        let closed = app
            .shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            })
            .await;
        drop(app);
        let history = match observed {
            Ok(history) => history,
            Err(failure) => std::panic::resume_unwind(failure),
        };
        closed.expect("drain App before any global codec is replaced");
        history
    }

    fn action(&self, node: &str) -> &Checkpoint {
        self.checkpoints
            .iter()
            .find(|checkpoint| {
                matches!(checkpoint.kind, CheckpointKind::Action)
                    && checkpoint.position.as_deref() == Some(node)
            })
            .expect("real action checkpoint")
    }

    fn initial(&self) -> &Checkpoint {
        self.checkpoints.first().expect("initial history row")
    }

    fn context(&self, base: &Checkpoint, node: &str, ordinal: u64) -> ActionContext {
        ActionContext {
            workspace: self.workspace.clone(),
            workflow: base.workflow.clone(),
            run: base.run.clone(),
            occurrence: base.occurrence.clone(),
            checkpoint: base.sequence,
            action_ordinal: ordinal,
            node: node.into(),
            attempt: 0,
            mode: ActionMode::Asynchronous,
            parent: None,
            parent_occurrence: None,
        }
    }

    fn event<'a>(
        &'a self,
        context: &'a ActionContext,
        checkpoint: &'a Checkpoint,
    ) -> LifecycleEvent<'a> {
        let outcome = match &self.snapshot.status {
            RunStatus::Succeeded => Ok(checkpoint
                .outcomes
                .iter()
                .find(|outcome| {
                    outcome.node == context.node && outcome.ordinal == context.action_ordinal
                })
                .expect("actual committed outcome")),
            RunStatus::Failed(failure) => Err(failure),
            _ => panic!("fixture requires actual terminal truth"),
        };
        LifecycleEvent::StepFinished {
            context,
            outcome: Some(outcome),
            checkpoint,
        }
    }

    fn expected(&self, ordinal: u64, prior: Option<&str>, count: u64) -> Value {
        let mut state = self.initial().state.values().clone();
        state.insert("count".into(), count.into());
        state.insert("__SEQUENCE_ID".into(), ordinal.into());
        match prior {
            Some(prior) => {
                state.insert("__PRIOR_STEP".into(), prior.into());
            }
            None => {
                state.remove("__PRIOR_STEP");
            }
        }
        json!(state)
    }

    fn assert_original(
        checkpoint: &Checkpoint,
        count: u64,
        prior: Option<&str>,
        ordinal: Option<u64>,
    ) {
        assert_eq!(checkpoint.state.values()["count"], count);
        assert_eq!(checkpoint.state.values()["__SEQUENCE_ID"], 900);
        assert_eq!(checkpoint.state.values()["__PRIOR_STEP"], "user-origin");
        assert_eq!(checkpoint.position.as_deref(), prior);
        assert_eq!(checkpoint.action_ordinal, ordinal);
    }
}

// A process, rather than a reset/unregister hook, isolates fixed reserved names.
// The parent always owns, bounds and reaps it, including panic/timeout paths.
struct CodecProcess {
    child: Option<Child>,
}

impl CodecProcess {
    fn start(test: &str) -> Option<Self> {
        if std::env::var("AI_KIT_TRACKING_CHECKPOINT_CODEC")
            .ok()
            .as_deref()
            == Some(test)
        {
            return None;
        }
        Some(Self {
            child: Some(
                Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", test, "--nocapture"])
                    .env("AI_KIT_TRACKING_CHECKPOINT_CODEC", test)
                    .spawn()
                    .expect("owned isolated tracking codec process"),
            ),
        })
    }

    async fn wait(mut self) {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(status) = self.child.as_mut().unwrap().try_wait().unwrap() {
                self.child.take();
                assert!(
                    status.success(),
                    "isolated tracking codec process failed: {status}"
                );
                return;
            }
            assert!(
                Instant::now() < deadline,
                "tracking codec process exceeded its bound"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

impl Drop for CodecProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[tokio::test]
async fn successful_b_exports_its_real_result_and_committed_position_after_a() {
    let history = TrackingHistory::capture(NativeSteps::Success, false).await;
    let a = history.action("a");
    let b = history.action("b");
    let context = history.context(a, "b", 1);
    let event = history.event(&context, b);
    let records = event.tracking_records(TrackingHistory::time()).unwrap();
    assert_eq!(
        records,
        vec![json!({"type":"end_entry", "action":"b", "sequence_id":1,
        "end_time":"2000-02-29T00:00:00.123456789Z", "result":{}, "exception":null,
        "state":history.expected(1, Some("b"), 2)})]
    );
    assert_eq!(
        event.tracking_records(TrackingHistory::time()).unwrap(),
        records
    );
    TrackingHistory::assert_original(a, 1, Some("a"), Some(0));
    TrackingHistory::assert_original(b, 2, Some("b"), Some(1));
}

#[tokio::test]
async fn failed_b_exports_attempted_ordinal_with_a_state_and_no_invented_result() {
    let history = TrackingHistory::capture(NativeSteps::FailedAfterA, false).await;
    let a = history.action("a");
    let context = history.context(a, "b", 1);
    let records = history
        .event(&context, a)
        .tracking_records(TrackingHistory::time())
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["action"], "b");
    assert_eq!(records[0]["sequence_id"], 1);
    assert_eq!(records[0]["state"], history.expected(1, Some("a"), 1));
    assert!(records[0]["result"].is_null());
    let RunStatus::Failed(app_kit::workflow::WorkflowFailure::Evaluation(message)) =
        &history.snapshot.status
    else {
        panic!("real expression failure")
    };
    assert_eq!(records[0]["exception"], message.as_str());
    assert_eq!(history.snapshot.last_action_ordinal, Some(0));
    TrackingHistory::assert_original(a, 1, Some("a"), Some(0));
}

#[tokio::test]
async fn initial_failure_removes_user_prior_marker_only_from_the_export() {
    let history = TrackingHistory::capture(NativeSteps::FailedInitially, false).await;
    let initial = history.initial();
    let context = history.context(initial, "b", 0);
    let records = history
        .event(&context, initial)
        .tracking_records(TrackingHistory::time())
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["state"], history.expected(0, None, 0));
    assert_eq!(records[0]["sequence_id"], 0);
    assert!(records[0]["result"].is_null());
    assert!(!records[0]["exception"].as_str().unwrap().is_empty());
    assert_eq!(history.snapshot.last_action_ordinal, None);
    TrackingHistory::assert_original(initial, 0, None, None);
}

#[tokio::test]
async fn reserved_field_codecs_receive_attempted_and_committed_metadata_before_encoding() {
    let test = "reserved_field_codecs_receive_attempted_and_committed_metadata_before_encoding";
    if let Some(process) = CodecProcess::start(test) {
        process.wait().await;
        return;
    }
    let success = TrackingHistory::capture(NativeSteps::Success, false).await;
    let failed = TrackingHistory::capture(NativeSteps::FailedAfterA, false).await;
    let initial = TrackingHistory::capture(NativeSteps::FailedInitially, false).await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    for field in ["__SEQUENCE_ID", "__PRIOR_STEP"] {
        let seen = Arc::clone(&seen);
        StateSerialization::global()
            .register_field(
                field.into(),
                move |value| {
                    seen.lock().unwrap().push((field, value.clone()));
                    Ok::<_, &'static str>(json!({"encoded":value}))
                },
                |value| Ok::<_, &'static str>(value.clone()),
            )
            .unwrap();
    }
    for (history, checkpoint, ordinal, prior, count) in [
        (&success, success.action("b"), 1, Some("b"), 2),
        (&failed, failed.action("a"), 1, Some("a"), 1),
        (&initial, initial.initial(), 0, None, 0),
    ] {
        seen.lock().unwrap().clear();
        let context = history.context(checkpoint, "b", ordinal);
        let records = history
            .event(&context, checkpoint)
            .tracking_records(TrackingHistory::time())
            .unwrap();
        assert_eq!(records[0]["sequence_id"], ordinal);
        assert_eq!(
            records[0]["state"]["__SEQUENCE_ID"],
            json!({"encoded":ordinal})
        );
        let mut expected = vec![("__SEQUENCE_ID", json!(ordinal))];
        if let Some(prior) = prior {
            expected.push(("__PRIOR_STEP", json!(prior)));
            assert_eq!(
                records[0]["state"]["__PRIOR_STEP"],
                json!({"encoded":prior})
            );
        } else {
            assert!(records[0]["state"].get("__PRIOR_STEP").is_none());
        }
        assert_eq!(*seen.lock().unwrap(), expected);
        TrackingHistory::assert_original(
            checkpoint,
            count,
            prior,
            match prior {
                Some("b") => Some(1),
                Some(_) => Some(0),
                None => None,
            },
        );
    }
}

#[tokio::test]
async fn one_tracking_export_keeps_one_registry_snapshot_during_reentrant_replacement() {
    let test = "one_tracking_export_keeps_one_registry_snapshot_during_reentrant_replacement";
    if let Some(process) = CodecProcess::start(test) {
        process.wait().await;
        return;
    }
    let history = TrackingHistory::capture(NativeSteps::Success, false).await;
    let b = history.action("b");
    StateSerialization::global()
        .register_field(
            "__PRIOR_STEP".into(),
            |value| Ok::<_, &'static str>(json!({"version":"old","value":value})),
            |value| Ok::<_, &'static str>(value.clone()),
        )
        .unwrap();
    let first = AtomicBool::new(true);
    StateSerialization::global()
        .register_field(
            "__SEQUENCE_ID".into(),
            move |value| {
                assert_eq!(value, &json!(1));
                if first.swap(false, Ordering::SeqCst) {
                    StateSerialization::global()
                        .register_field(
                            "__PRIOR_STEP".into(),
                            |value| Ok::<_, &'static str>(json!({"version":"new","value":value})),
                            |value| Ok::<_, &'static str>(value.clone()),
                        )
                        .unwrap();
                }
                Ok::<_, &'static str>(json!({"value":value}))
            },
            |value| Ok::<_, &'static str>(value.clone()),
        )
        .unwrap();
    let context = history.context(history.action("a"), "b", 1);
    let event = history.event(&context, b);
    let old = event.tracking_records(TrackingHistory::time()).unwrap();
    let new = event.tracking_records(TrackingHistory::time()).unwrap();
    assert_eq!(
        old[0]["state"]["__PRIOR_STEP"],
        json!({"version":"old","value":"b"})
    );
    assert_eq!(
        new[0]["state"]["__PRIOR_STEP"],
        json!({"version":"new","value":"b"})
    );
    assert_eq!(old[0]["sequence_id"], 1);
    assert_eq!(new[0]["sequence_id"], 1);
    TrackingHistory::assert_original(b, 2, Some("b"), Some(1));
}

#[tokio::test]
async fn checkpoint_tracking_preserves_codec_error_context_without_fallback_success() {
    let test = "checkpoint_tracking_preserves_codec_error_context_without_fallback_success";
    if let Some(process) = CodecProcess::start(test) {
        process.wait().await;
        return;
    }
    let history = TrackingHistory::capture(NativeSteps::Success, false).await;
    StateSerialization::global()
        .register_field(
            "__SEQUENCE_ID".into(),
            |_| Err::<Value, _>("checkpoint codec cause"),
            |value| Ok::<_, &'static str>(value.clone()),
        )
        .unwrap();
    let b = history.action("b");
    let context = history.context(history.action("a"), "b", 1);
    let error = history
        .event(&context, b)
        .tracking_records(TrackingHistory::time())
        .unwrap_err();
    let AppError::InvalidInput(message) = error else {
        panic!("existing codec error category")
    };
    assert!(
        message.contains("__SEQUENCE_ID")
            && message.contains("field serialize handler")
            && message.contains("checkpoint codec cause")
    );
    TrackingHistory::assert_original(b, 2, Some("b"), Some(1));
}

#[tokio::test]
async fn checkpoint_tracking_codec_panic_unwinds_without_mutating_history() {
    let test = "checkpoint_tracking_codec_panic_unwinds_without_mutating_history";
    if let Some(process) = CodecProcess::start(test) {
        process.wait().await;
        return;
    }
    let history = TrackingHistory::capture(NativeSteps::Success, false).await;
    StateSerialization::global()
        .register_field(
            "__SEQUENCE_ID".into(),
            |_| -> Result<Value, &'static str> {
                panic!("checkpoint codec panic");
            },
            |value| Ok::<_, &'static str>(value.clone()),
        )
        .unwrap();
    let b = history.action("b");
    let context = history.context(history.action("a"), "b", 1);
    let event = history.event(&context, b);
    let error = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        event.tracking_records(TrackingHistory::time())
    }))
    .expect_err("the opt-in serializer does not swallow caller panic");
    assert_eq!(
        error.downcast_ref::<&str>(),
        Some(&"checkpoint codec panic")
    );
    TrackingHistory::assert_original(b, 2, Some("b"), Some(1));
}

#[tokio::test]
async fn checkpoint_tracking_rejects_every_nonobject_field_encoding() {
    let test = "checkpoint_tracking_rejects_every_nonobject_field_encoding";
    if let Some(process) = CodecProcess::start(test) {
        process.wait().await;
        return;
    }
    let history = TrackingHistory::capture(NativeSteps::Success, false).await;
    let b = history.action("b");
    let context = history.context(history.action("a"), "b", 1);
    for output in [
        json!(null),
        json!(false),
        json!(3),
        json!("text"),
        json!([]),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let recorded = Arc::clone(&calls);
        StateSerialization::global()
            .register_field(
                "__SEQUENCE_ID".into(),
                move |_| {
                    recorded.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, &'static str>(output.clone())
                },
                |value| Ok::<_, &'static str>(value.clone()),
            )
            .unwrap();
        assert!(matches!(
            history
                .event(&context, b)
                .tracking_records(TrackingHistory::time()),
            Err(AppError::InvalidInput(_))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        TrackingHistory::assert_original(b, 2, Some("b"), Some(1));
    }
}

#[tokio::test]
async fn checkpoint_tracking_bounds_encoded_state_and_the_complete_record_separately() {
    let test = "checkpoint_tracking_bounds_encoded_state_and_the_complete_record_separately";
    if let Some(process) = CodecProcess::start(test) {
        process.wait().await;
        return;
    }
    let history = TrackingHistory::capture(NativeSteps::Success, false).await;
    let b = history.action("b");
    let context = history.context(history.action("a"), "b", 1);
    StateSerialization::global()
        .register_field(
            "__SEQUENCE_ID".into(),
            |_| Ok::<_, &'static str>(json!({"payload":"x".repeat(1_048_576)})),
            |value| Ok::<_, &'static str>(value.clone()),
        )
        .unwrap();
    assert!(matches!(
        history
            .event(&context, b)
            .tracking_records(TrackingHistory::time()),
        Err(AppError::InvalidInput(_))
    ));

    // A state exactly at the public byte limit is valid on its own; the end
    // entry surrounding that same state must still obey the output-list limit.
    let mut expected = history
        .expected(1, Some("b"), 2)
        .as_object()
        .unwrap()
        .clone();
    expected.insert("__SEQUENCE_ID".into(), json!({"payload":""}));
    let padding = 1_048_576 - serde_json::to_vec(&expected).unwrap().len();
    StateSerialization::global()
        .register_field(
            "__SEQUENCE_ID".into(),
            move |_| Ok::<_, &'static str>(json!({"payload":"x".repeat(padding)})),
            |value| Ok::<_, &'static str>(value.clone()),
        )
        .unwrap();
    let projected = WorkflowState::new(
        history
            .expected(1, Some("b"), 2)
            .as_object()
            .unwrap()
            .clone(),
    )
    .unwrap();
    let encoded = projected
        .serialize()
        .expect("encoded state alone reaches its inclusive byte bound");
    assert_eq!(serde_json::to_vec(&encoded).unwrap().len(), 1_048_576);
    assert!(matches!(
        history
            .event(&context, b)
            .tracking_records(TrackingHistory::time()),
        Err(AppError::InvalidInput(_))
    ));
    TrackingHistory::assert_original(b, 2, Some("b"), Some(1));
}

#[tokio::test]
async fn restored_checkpoint_projects_actual_lineage_and_optional_spawning_pointer_in_order() {
    let history = TrackingHistory::capture(NativeSteps::Success, true).await;
    let spawning = TrackingHistory::capture(NativeSteps::Success, false).await;
    let source = history.action("a");
    let restored = history.restored.as_ref().unwrap();
    let lineage = restored
        .lineage
        .as_ref()
        .expect("restore returns actual lineage");
    assert_eq!(lineage.source, source.run);
    assert_eq!(lineage.source_occurrence, source.occurrence);
    assert_eq!(lineage.checkpoint, source.sequence);
    assert_eq!(lineage.action_ordinal, Some(0));
    let parent = spawning.context(spawning.action("a"), "b", 1);
    assert_ne!(parent.run, source.run);
    assert_ne!(parent.workspace, history.workspace);
    for parent in [None, Some(&parent)] {
        // The optional public context exercises projection composition. It does
        // not claim runtime delivery of a restored child's spawning relation.
        let records = LifecycleEvent::Registered {
            workspace: &history.workspace,
            run: &restored.run,
            occurrence: &restored.occurrence,
            graph: &history.graph,
            parent,
            lineage: Some(lineage),
        }
        .tracking_records(TrackingHistory::time())
        .unwrap();
        assert_eq!(records.len(), if parent.is_some() { 4 } else { 3 });
        assert_eq!(records[0], history.graph.tracking_record().unwrap());
        assert_eq!(
            records[1],
            json!({"type":"application_metadata", "partition_key":history.workspace,
            "parent_pointer":{"type":"pointer_data","app_id":source.run,"sequence_id":0,
                "partition_key":history.workspace},
            "spawning_parent_pointer":parent.map(|parent| json!({"type":"pointer_data", "app_id":parent.run,
                "sequence_id":parent.action_ordinal,"partition_key":parent.workspace}))})
        );
        for (index, event_type) in ["fork", "spawn_start"]
            .into_iter()
            .take(records.len() - 2)
            .enumerate()
        {
            assert_eq!(
                records[index + 2],
                json!({"type":"child_application_data", "event_type":event_type,
                "sequence_id":if index == 0 {0} else {1},"event_time":"2000-02-29T00:00:00.123456789Z",
                "child":{"type":"pointer_data","app_id":restored.run,"sequence_id":null,
                    "partition_key":history.workspace}})
            );
        }
    }
    TrackingHistory::assert_original(source, 1, Some("a"), Some(0));
    TrackingHistory::assert_original(restored, 1, Some("a"), Some(0));
}
