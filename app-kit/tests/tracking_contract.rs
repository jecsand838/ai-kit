//! Pure opt-in tracking projections; this does not prove runtime hook dispatch.
//! Source-derived from pinned rurr tracking/models/{metadata,log,children}.rs,
//! tracking/lifecycle_events.rs:629–739 and rurr-persistence/src/wire.rs:25–43.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agents_kit::Agent;
use app_kit::workflow::{
    ActionContext, ActionMode, ExecutionOptions, GraphFormat, LifecycleEvent, WorkflowDefinition,
    WorkflowState,
};
use app_kit::{App, AppConfig, AppError, Shutdown, WorkspaceConfig};
use serde_json::{Map, Value, json};
use storage_kit::StateStore;
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};

struct Tracking;

impl Tracking {
    fn context() -> ActionContext {
        ActionContext {
            workspace: "appws_8a7c251e-b5b8-4d9e-8f0b-123456789abc"
                .parse()
                .unwrap(),
            workflow: "tracking".parse().unwrap(),
            run: "run_8a7c251e-b5b8-4d9e-8f0b-123456789abc".parse().unwrap(),
            occurrence: "8a7c251eb5b84d9e8f0b123456789abc".into(),
            checkpoint: 17,
            action_ordinal: 3,
            node: "current".into(),
            attempt: 2,
            mode: ActionMode::Asynchronous,
            parent: None,
            parent_occurrence: None,
        }
    }

    fn time() -> SystemTime {
        UNIX_EPOCH + Duration::new(951_782_400, 123_456_789)
    }

    fn timestamp() -> &'static str {
        "2000-02-29T00:00:00.123456789Z"
    }

    fn one(event: &LifecycleEvent<'_>, expected: Value) {
        let first = event
            .tracking_records(Self::time())
            .expect("tracking projection");
        assert_eq!(first, vec![expected]);
        assert_eq!(event.tracking_records(Self::time()).unwrap(), first);
    }

    fn graph() -> WorkflowDefinition {
        WorkflowDefinition::parse(
            &json!({
                "schema":"app-kit.workflow/v2","id":"tracking","revision":"r1",
                "state":{},"required":[],"reducers":{},"entry":"current",
                "nodes":[{"id":"current","step":{"kind":"state","mutations":[]}}],
                "routes":{}
            })
            .to_string(),
        )
        .unwrap()
    }
}

#[test]
fn registration_exports_the_same_graph_and_exact_metadata_without_side_effects() {
    let context = Tracking::context();
    let definition = Tracking::graph();
    let graph = definition.graph();
    let event = LifecycleEvent::Registered {
        workspace: &context.workspace,
        run: &context.run,
        occurrence: &context.occurrence,
        graph: &graph,
        parent: None,
        lineage: None,
    };
    let records = event.tracking_records(Tracking::time()).unwrap();
    assert_eq!(
        records,
        vec![
            graph.tracking_record().unwrap(),
            json!({
                "type":"application_metadata", "partition_key":context.workspace.to_string(),
                "parent_pointer":null, "spawning_parent_pointer":null,
            })
        ]
    );
    assert_eq!(event.tracking_records(Tracking::time()).unwrap(), records);
    assert!(
        definition
            .export(GraphFormat::Json)
            .unwrap()
            .contains("tracking")
    );
}

#[test]
fn child_registration_projects_spawn_pointer_and_relationship_without_action_results() {
    let parent = Tracking::context();
    let child = "run_6a7c251e-b5b8-4d9e-8f0b-123456789abc".parse().unwrap();
    let graph = Tracking::graph().graph();
    let records = LifecycleEvent::Registered {
        workspace: &parent.workspace,
        run: &child,
        occurrence: "6a7c251eb5b84d9e8f0b123456789abc",
        graph: &graph,
        parent: Some(&parent),
        lineage: None,
    }
    .tracking_records(Tracking::time())
    .unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0], graph.tracking_record().unwrap());
    assert_eq!(
        records[1],
        json!({
            "type":"application_metadata", "partition_key":parent.workspace.to_string(),
            "parent_pointer":null,
            "spawning_parent_pointer":{"type":"pointer_data", "app_id":parent.run.to_string(),
                "sequence_id":3, "partition_key":parent.workspace.to_string()},
        })
    );
    assert_eq!(
        records[2],
        json!({
            "type":"child_application_data", "event_type":"spawn_start",
            "sequence_id":3, "event_time":Tracking::timestamp(),
            "child":{"type":"pointer_data", "app_id":child.to_string(),
                "sequence_id":null, "partition_key":parent.workspace.to_string()},
        })
    );
}

#[test]
fn step_begin_exports_transient_inputs_and_semantic_ordinal_without_mutating_state() {
    let context = Tracking::context();
    let state = WorkflowState::new(Map::from_iter([("persistent".into(), json!(4))])).unwrap();
    let inputs = Map::from_iter([("question".into(), json!("explicit caller content"))]);
    Tracking::one(
        &LifecycleEvent::StepStarted {
            context: &context,
            state: &state,
            inputs: &inputs,
        },
        json!({"type":"begin_entry", "start_time":Tracking::timestamp(),
        "action":"current", "inputs":inputs, "sequence_id":3}),
    );
    assert_eq!(
        state.values(),
        &Map::from_iter([("persistent".into(), json!(4))])
    );
}

#[test]
fn span_projections_preserve_explicit_identity_parent_dependencies_and_string_tags() {
    let context = Tracking::context();
    let dependencies = vec![
        "upstream-b".into(),
        "upstream-a".into(),
        "upstream-b".into(),
    ];
    Tracking::one(
        &LifecycleEvent::SpanStarted {
            context: &context,
            id: "child",
            name: "Repeated name",
            parent: Some("parent"),
            dependencies: &dependencies,
        },
        json!({"type":"begin_span", "start_time":Tracking::timestamp(),
        "action_sequence_id":3, "span_id":"child", "span_name":"Repeated name",
        "parent_span_id":"parent", "span_dependencies":dependencies}),
    );
    let value = json!({"explicit":"caller content"});
    let tags = Map::from_iter([("source".into(), json!("test"))]);
    for span in [None, Some("parent")] {
        Tracking::one(
            &LifecycleEvent::SpanAttribute {
                context: &context,
                span,
                key: "details",
                value: &value,
                tags: &tags,
            },
            json!({"type":"attribute", "key":"details", "action_sequence_id":3,
            "span_id":span, "value":value, "tags":tags,
            "time_logged":Tracking::timestamp()}),
        );
    }
    Tracking::one(
        &LifecycleEvent::SpanFinished {
            context: &context,
            id: "child",
        },
        json!({"type":"end_span", "end_time":Tracking::timestamp(),
            "action_sequence_id":3,"span_id":"child"}),
    );
    for invalid in [json!(1), json!(null), json!(true), json!([]), json!({})] {
        let tags = Map::from_iter([("invalid".into(), invalid)]);
        assert!(matches!(
            LifecycleEvent::SpanAttribute {
                context: &context,
                span: None,
                key: "details",
                value: &value,
                tags: &tags,
            }
            .tracking_records(Tracking::time()),
            Err(AppError::InvalidInput(_))
        ));
    }
}

#[test]
fn stream_projections_emit_only_the_pinned_first_item_and_exact_terminal_count() {
    let context = Tracking::context();
    let result = Map::from_iter([("partial".into(), json!("content remains with hook"))]);
    for span in [None, Some("span")] {
        Tracking::one(
            &LifecycleEvent::StreamStarted {
                context: &context,
                span,
            },
            json!({"type":"begin_stream","action_sequence_id":3,"span_id":span,
                "stream_init_time":Tracking::timestamp()}),
        );
        Tracking::one(
            &LifecycleEvent::StreamItem {
                context: &context,
                span,
                index: 0,
                result: &result,
            },
            json!({"type":"first_item_stream","action_sequence_id":3,"span_id":span,
                "first_item_time":Tracking::timestamp()}),
        );
        for index in [1, 7, usize::MAX] {
            assert!(
                LifecycleEvent::StreamItem {
                    context: &context,
                    span,
                    index,
                    result: &result
                }
                .tracking_records(Tracking::time())
                .unwrap()
                .is_empty()
            );
        }
        Tracking::one(
            &LifecycleEvent::StreamFinished {
                context: &context,
                span,
                items: 8,
                failure: None,
            },
            json!({"type":"end_stream","action_sequence_id":3,"span_id":span,
                "end_time":Tracking::timestamp(),"items_streamed":8}),
        );
    }
    assert!(
        LifecycleEvent::ExecuteStarted {
            run: &context.run,
            occurrence: &context.occurrence
        }
        .tracking_records(UNIX_EPOCH - Duration::from_nanos(1))
        .unwrap()
        .is_empty()
    );
}

#[test]
fn tracking_timestamps_follow_fixed_nanosecond_utc_without_clamping_or_wraparound() {
    let context = Tracking::context();
    let event = LifecycleEvent::StreamStarted {
        context: &context,
        span: None,
    };
    for (seconds, nanos, expected) in [
        (0, 123_456_789, "1970-01-01T00:00:00.123456789Z"),
        (951_782_400, 0, "2000-02-29T00:00:00.000000000Z"),
        (4_107_542_400, 9, "2100-03-01T00:00:00.000000009Z"),
        (13_574_563_200, 0, "2400-02-29T00:00:00.000000000Z"),
        (
            253_402_300_799,
            999_999_999,
            "9999-12-31T23:59:59.999999999Z",
        ),
    ] {
        let records = event
            .tracking_records(UNIX_EPOCH + Duration::new(seconds, nanos))
            .unwrap();
        assert_eq!(records[0]["stream_init_time"], expected);
    }
    assert!(matches!(
        event.tracking_records(UNIX_EPOCH - Duration::from_nanos(1)),
        Err(AppError::InvalidInput(_))
    ));
    assert!(matches!(
        event.tracking_records(UNIX_EPOCH + Duration::from_secs(253_402_300_800)),
        Err(AppError::InvalidInput(_))
    ));
}

#[test]
fn tracking_content_uses_output_bytes_without_app_state_key_restrictions() {
    let context = Tracking::context();
    let state = WorkflowState::default();
    let inputs = Map::from_iter([("long".repeat(40), json!({"payload":"visible"}))]);
    let event = LifecycleEvent::StepStarted {
        context: &context,
        state: &state,
        inputs: &inputs,
    };
    let records = event.tracking_records(Tracking::time()).unwrap();
    assert_eq!(records[0]["inputs"], json!(inputs));
    let oversized = Map::from_iter([("payload".into(), json!("x".repeat(1_048_576)))]);
    assert!(matches!(
        LifecycleEvent::StepStarted {
            context: &context,
            state: &state,
            inputs: &oversized
        }
        .tracking_records(Tracking::time()),
        Err(AppError::InvalidInput(_))
    ));
}

#[tokio::test]
async fn terminal_lifecycle_events_without_pinned_wire_rows_return_no_records() {
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let mut app =
        App::<StateStore, Agent>::open(AppConfig::new(store, Vec::new(), vec![Tracking::graph()]))
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
        .workflow("tracking".parse().unwrap())
        .await
        .unwrap();
    let run = workflow.start(ExecutionOptions::default()).await.unwrap();
    let snapshot = tokio::time::timeout(Duration::from_secs(3), run.wait())
        .await
        .expect("owned terminal result")
        .unwrap();
    app.shutdown(Shutdown::CancelAndDrain {
        timeout: Duration::from_secs(3),
    })
    .await
    .unwrap();
    assert!(matches!(
        snapshot.status,
        app_kit::workflow::RunStatus::Succeeded
    ));
    let state = snapshot.state.clone();
    let parent = Tracking::context();
    // A real snapshot permits a pure event projection; this does not claim
    // parent-child admission or H dispatch was exercised by this state-only run.
    for event in [
        LifecycleEvent::ChildFinished {
            parent: &parent,
            snapshot: &snapshot,
        },
        LifecycleEvent::ExecuteFinished {
            snapshot: &snapshot,
            failure: None,
        },
    ] {
        assert!(
            event
                .tracking_records(UNIX_EPOCH - Duration::from_nanos(1))
                .unwrap()
                .is_empty()
        );
    }
    assert_eq!(snapshot.state, state);
}
