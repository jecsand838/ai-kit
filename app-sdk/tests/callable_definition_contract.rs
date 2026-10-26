//! Callable revision selection is an App deployment extension. Metadata follows
//! pinned rurr action/function.rs:505–509,755–762 and tracking/models/graph.rs:31–53.
//! This staged target uses v1 so the existing parser reaches the new grammar;
//! the coherent v2/v5 cutover remains a separate acceptance obligation.
//! No execution API is required to prove parse, binding and registration facts.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_sdk::Agent;
use app_sdk::workflow::{
    ActionBackend, ActionDeclaration, ActionEvent, ActionMode, ActionRequest, ConditionBackend,
    ConditionDeclaration, ExecutionOptions, GraphComposition, GraphImport, JsonState,
    LifecycleEvent, LifecycleHooks, RunStatus, WorkflowDefinition, WorkflowState,
};
use app_sdk::{App, AppConfig, AppError, Shutdown, WorkspaceConfig};
use futures_util::stream::{self, Empty};
use serde_json::{Map, Value, json};
use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_sdk::state::record::StateListRequest;
use storage_sdk::{StateStore, StateStoreBackend};

#[derive(Default)]
struct Observation {
    actions: Arc<AtomicUsize>,
    conditions: Arc<AtomicUsize>,
    registered: Arc<Mutex<Vec<Value>>>,
}

struct Callable {
    declaration: ActionDeclaration,
    calls: Arc<AtomicUsize>,
}

struct Predicate {
    declaration: ConditionDeclaration,
    calls: Arc<AtomicUsize>,
}

impl Callable {
    fn new(revision: &str, observed: &Observation) -> Self {
        Self {
            declaration: ActionDeclaration {
                key: "shared".into(),
                revision: revision.into(),
                reads: if revision == "r1" {
                    vec!["count".into()]
                } else {
                    vec!["gate".into(), "count".into()]
                },
                writes: vec!["count".into()],
                required_inputs: vec![format!("question-{revision}")],
                optional_inputs: vec!["format".into()],
                bound_inputs: Map::new(),
                default_inputs: Map::from_iter([("format".into(), json!("plain"))]),
                tags: vec![format!("decl-{revision}"), "shared".into(), "shared".into()],
                source: Some(format!("actual source {revision}")),
                mode: ActionMode::Both,
            },
            calls: Arc::clone(&observed.actions),
        }
    }
}

impl ActionBackend for Callable {
    type Stream = Empty<Result<ActionEvent, AppError>>;

    fn declaration(&self) -> &ActionDeclaration {
        &self.declaration
    }

    fn start(&self, _: ActionRequest) -> Self::Stream {
        self.calls.fetch_add(1, Ordering::SeqCst);
        stream::empty()
    }
}

impl Predicate {
    fn new(revision: &str, observed: &Observation) -> Self {
        Self {
            declaration: ConditionDeclaration {
                key: "choose".into(),
                revision: revision.into(),
                reads: vec!["gate".into()],
            },
            calls: Arc::clone(&observed.conditions),
        }
    }
}

impl ConditionBackend for Predicate {
    fn declaration(&self) -> &ConditionDeclaration {
        &self.declaration
    }

    fn evaluate(&self, _: &WorkflowState) -> Result<bool, AppError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.declaration.revision == "c1")
    }
}

impl LifecycleHooks for Observation {
    fn observe(&self, event: &LifecycleEvent<'_>) -> Result<(), AppError> {
        if let LifecycleEvent::Registered { graph, .. } = event {
            let graph_value = serde_json::to_value(graph)
                .map_err(|error| AppError::Integration(error.to_string()))?;
            let tracking = graph.tracking_record()?;
            self.registered
                .lock()
                .expect("registration observations")
                .push(json!({
                    "graph": graph_value,
                    "tracking": tracking,
                }));
        }
        Ok(())
    }
}

impl Observation {
    fn document() -> Value {
        json!({
            "schema":"app-sdk.workflow/v2", "id":"binding", "revision":"workflow-r1",
            "state":{"count":0,"gate":true}, "required":["count","gate"],
            "reducers":{"count":"reject_concurrent","gate":"reject_concurrent"},
            "entry":"first",
            "nodes":[
                {"id":"first","name":"Displayed first","description":"authored description",
                 "step":{"kind":"action","action":"shared","revision":"r1"}},
                {"id":"second","tags":["override","override"],
                 "step":{"kind":"action","action":"shared","revision":"r2"}},
                {"id":"third","tags":[],
                 "step":{"kind":"action","action":"shared","revision":"r1"}}
            ],
            "routes":{
                "first":{"mode":"first","branches":[{
                    "when":{"kind":"custom","condition":"choose","revision":"c1"},
                    "target":{"kind":"node","id":"second"}
                }]},
                "second":{"mode":"first","branches":[{
                    "target":{"kind":"node","id":"third"}
                }]}
            }
        })
    }

    fn definition(document: &Value) -> WorkflowDefinition {
        WorkflowDefinition::parse(&document.to_string())
            .expect("exact callable references must parse without an inventory")
    }

    fn predicate_document(revision: &str) -> Value {
        json!({
            "schema":"app-sdk.workflow/v2", "id":"predicate", "revision":"workflow-r1",
            "state":{"count":0,"gate":true}, "required":["count","gate"],
            "reducers":{"count":"reject_concurrent","gate":"reject_concurrent"},
            "entry":"first", "nodes":[
                {"id":"first","step":{"kind":"state","mutations":[]}},
                {"id":"selected","step":{"kind":"state","mutations":[{
                    "kind":"assign","target":"count","value":{"kind":"literal","value":1}
                }]}}
            ],
            "routes":{"first":{"mode":"first","branches":[{
                "when":{"kind":"custom","condition":"choose","revision":revision},
                "target":{"kind":"node","id":"selected"}
            }]}}
        })
    }

    async fn store() -> StateStore {
        StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("real memory transactions")
    }

    fn inventory(&self) -> (Vec<Callable>, Vec<Predicate>) {
        (
            vec![Callable::new("r1", self), Callable::new("r2", self)],
            vec![Predicate::new("c1", self), Predicate::new("c2", self)],
        )
    }
}

#[tokio::test]
async fn custom_condition_revision_controls_state_only_routes_independent_of_inventory_order() {
    for selected in ["c1", "c2"] {
        for reversed in [false, true] {
            let observed = Observation::default();
            let mut conditions = vec![
                Predicate::new("c1", &observed),
                Predicate::new("c2", &observed),
            ];
            if reversed {
                conditions.reverse();
            }
            let config: AppConfig<StateStore, Agent> = AppConfig::new(
                Observation::store().await,
                Vec::new(),
                vec![Observation::definition(&Observation::predicate_document(
                    selected,
                ))],
            );
            let mut app = App::open(config.with_conditions(conditions))
                .await
                .expect("bind the exact predicate revision");
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().expect("workspace root"),
                })
                .await
                .expect("workspace");
            let workflow = workspace
                .workflow("predicate".parse().expect("id"))
                .await
                .expect("state-only graph with callable condition");
            let run = workflow
                .start(ExecutionOptions::default())
                .await
                .expect("state-only run");
            let snapshot = tokio::time::timeout(Duration::from_secs(3), run.wait())
                .await
                .expect("condition evaluation is bounded")
                .expect("completed state");
            assert!(matches!(snapshot.status, RunStatus::Succeeded));
            assert_eq!(
                snapshot.state.values()["count"],
                if selected == "c1" { 1 } else { 0 }
            );
            assert_eq!(observed.conditions.load(Ordering::SeqCst), 1);
            assert_eq!(observed.actions.load(Ordering::SeqCst), 0);
            app.shutdown(Shutdown::Drain {
                timeout: Duration::from_secs(3),
            })
            .await
            .expect("state-only work drained");
        }
    }
}

#[test]
fn callable_references_require_explicit_exact_revisions() {
    let valid = Observation::document();
    let _ = Observation::definition(&valid);
    for (kind, field, replacement) in [
        ("action", "revision", None),
        ("action", "revision", Some(json!(""))),
        ("action", "revision", Some(json!(" r1"))),
        ("action", "revision", Some(Value::Null)),
        ("condition", "revision", None),
        ("condition", "revision", Some(json!(""))),
        ("condition", "revision", Some(json!("c1\n"))),
        ("condition", "revision", Some(Value::Null)),
    ] {
        let mut document = valid.clone();
        let reference = if kind == "action" {
            &mut document["nodes"][0]["step"]
        } else {
            &mut document["routes"]["first"]["branches"][0]["when"]
        };
        let fields = reference.as_object_mut().expect("reference object");
        match replacement {
            Some(value) => {
                fields.insert(field.into(), value);
            }
            None => {
                fields.remove(field);
            }
        }
        assert!(
            matches!(
                WorkflowDefinition::parse(&document.to_string()),
                Err(AppError::InvalidInput(_))
            ),
            "{kind} reference must reject a missing or invalid revision"
        );
    }
}

#[test]
fn unbound_callable_graph_cannot_fabricate_declaration_or_tracking_source() {
    let definition = Observation::definition(&Observation::document());
    let graph = definition.graph();
    assert_eq!(graph.nodes.len(), 3);
    for node in &graph.nodes {
        assert_eq!(node.kind, "action");
        assert!(
            node.source.is_none(),
            "reference JSON is not callable source"
        );
        assert!(node.reads.is_empty() && node.writes.is_empty());
        assert!(node.required_inputs.is_empty() && node.optional_inputs.is_empty());
    }
    assert_eq!(graph.nodes[0].name.as_deref(), Some("Displayed first"));
    assert_eq!(
        graph.nodes[0].description.as_deref(),
        Some("authored description")
    );
    assert!(graph.nodes[0].tags.is_empty());
    assert_eq!(graph.nodes[1].tags, ["override", "override"]);
    assert!(graph.nodes[2].tags.is_empty());
    assert!(matches!(
        graph.tracking_record(),
        Err(AppError::InvalidInput(_))
    ));
}

#[tokio::test]
async fn missing_or_duplicate_callable_pairs_reject_before_app_publication() {
    for case in [
        "action-revision",
        "condition-revision",
        "action-key",
        "condition-key",
        "duplicate-action",
        "duplicate-condition",
    ] {
        let observed = Observation::default();
        let store = Observation::store().await;
        let retained_store = store.clone();
        let mut document = Observation::document();
        let (mut actions, mut conditions) = observed.inventory();
        match case {
            "action-revision" => document["nodes"][0]["step"]["revision"] = json!("r3"),
            "condition-revision" => {
                document["routes"]["first"]["branches"][0]["when"]["revision"] = json!("c3")
            }
            "action-key" => document["nodes"][0]["step"]["action"] = json!("missing"),
            "condition-key" => {
                document["routes"]["first"]["branches"][0]["when"]["condition"] = json!("missing")
            }
            "duplicate-action" => actions.push(Callable::new("r1", &observed)),
            "duplicate-condition" => conditions.push(Predicate::new("c1", &observed)),
            _ => unreachable!("selected preflight case"),
        }
        let action_calls = Arc::clone(&observed.actions);
        let condition_calls = Arc::clone(&observed.conditions);
        let registered = Arc::clone(&observed.registered);
        let config: AppConfig<StateStore, Agent> =
            AppConfig::new(store, Vec::new(), vec![Observation::definition(&document)]);
        let result = App::open(
            config
                .with_actions(actions)
                .with_conditions(conditions)
                .with_hooks(observed),
        )
        .await;
        assert!(
            matches!(result, Err(AppError::InvalidInput(_))),
            "{case} must reject the exact unresolved pair"
        );
        assert_eq!(action_calls.load(Ordering::SeqCst), 0);
        assert_eq!(condition_calls.load(Ordering::SeqCst), 0);
        assert!(registered.lock().expect("registrations").is_empty());
        assert!(
            retained_store
                .list(StateListRequest::new("app-sdk").expect("namespace"))
                .await
                .expect("visible App records")
                .records
                .is_empty(),
            "{case} must not publish App records"
        );
    }
}

#[tokio::test]
async fn registration_resolves_exact_metadata_independent_of_inventory_order_and_prefixes() {
    for prefixed in [false, true] {
        let mut prior = None;
        for reversed in [false, true] {
            let observed = Observation::default();
            let action_calls = Arc::clone(&observed.actions);
            let condition_calls = Arc::clone(&observed.conditions);
            let registered = Arc::clone(&observed.registered);
            let (mut actions, mut conditions) = observed.inventory();
            if reversed {
                actions.reverse();
                conditions.reverse();
            }
            let definition = Observation::definition(&Observation::document());
            let definition = if prefixed {
                WorkflowDefinition::compose(GraphComposition {
                    id: "binding".parse().expect("id"),
                    revision: "workflow-r1".into(),
                    entry: "p_first".into(),
                    imports: vec![GraphImport {
                        definition,
                        prefix: "p_".into(),
                    }],
                    connections: Vec::new(),
                })
                .expect("composition preserves callable pairs")
            } else {
                definition
            };
            let config: AppConfig<StateStore, Agent> =
                AppConfig::new(Observation::store().await, Vec::new(), vec![definition]);
            let mut app: App<StateStore, Agent, Callable, Predicate, JsonState, Observation> =
                App::open(
                    config
                        .with_actions(actions)
                        .with_conditions(conditions)
                        .with_hooks(observed),
                )
                .await
                .expect("bind exact revisions");
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().expect("workspace root"),
                })
                .await
                .expect("workspace");
            let workflow = workspace
                .workflow("binding".parse().expect("id"))
                .await
                .expect("workflow");
            let run = workflow
                .prepare(WorkflowState::default())
                .await
                .expect("register paused run");
            assert_eq!(run.snapshot().await.expect("checkpoint").checkpoint, 0);
            let records = registered.lock().expect("registrations").clone();
            assert_eq!(
                records.len(),
                1,
                "one run registration supplies one bound graph"
            );
            let record = &records[0];
            let nodes = record["graph"]["nodes"].as_array().expect("graph nodes");
            let prefix = if prefixed { "p_" } else { "" };
            for (index, (node, revision)) in [("first", "r1"), ("second", "r2"), ("third", "r1")]
                .into_iter()
                .enumerate()
            {
                assert_eq!(nodes[index]["id"], format!("{prefix}{node}"));
                assert_eq!(nodes[index]["source"], format!("actual source {revision}"));
                assert_eq!(nodes[index]["writes"], json!(["count"]));
                assert_eq!(
                    nodes[index]["required_inputs"],
                    json!([format!("question-{revision}")])
                );
                assert_eq!(nodes[index]["optional_inputs"], json!(["format"]));
                assert_eq!(
                    record["tracking"]["actions"][index]["code"],
                    format!("actual source {revision}")
                );
            }
            assert_eq!(nodes[0]["reads"], json!(["count"]));
            assert_eq!(nodes[1]["reads"], json!(["gate", "count"]));
            assert_eq!(nodes[0]["tags"], json!(["decl-r1", "shared", "shared"]));
            assert_eq!(nodes[1]["tags"], json!(["override", "override"]));
            assert_eq!(nodes[2]["tags"], json!([]));
            assert_eq!(action_calls.load(Ordering::SeqCst), 0);
            assert_eq!(condition_calls.load(Ordering::SeqCst), 0);
            if let Some(prior) = prior {
                assert_eq!(records, prior, "inventory order cannot select a revision");
            }
            prior = Some(records);
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            })
            .await
            .expect("drain the paused run");
        }
    }
}
