//! Direct, non-Clone predicate bindings in routes and nested value expressions.
//! Real LanceDB proves App durability; caller predicates prove only the public seam.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agents_kit::Agent;
use app_kit::workflow::{
    ActionBackend, ActionDeclaration, ActionEvent, ActionMode, ActionRequest, ConditionBackend,
    ConditionDeclaration, ExecutionOptions, JsonState, LifecycleEvent, LifecycleHooks, NoActions,
    RunSnapshot, RunStatus, StateChange, StateTyping, Workflow, WorkflowDefinition,
    WorkflowFailure, WorkflowState,
};
use app_kit::{App, AppConfig, AppError, Shutdown, WorkspaceConfig};
use futures_util::FutureExt;
use serde_json::{Map, Value, json};
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_kit::state::record::StateListRequest;
use storage_kit::{StateStore, StateStoreBackend};

#[derive(Default)]
struct Evidence {
    calls: Mutex<Vec<(String, Map<String, Value>)>>,
    graphs: Mutex<Vec<Value>>,
}

struct Observed(Arc<Evidence>);

impl LifecycleHooks for Observed {
    fn observe(&self, event: &LifecycleEvent<'_>) -> Result<(), AppError> {
        if let LifecycleEvent::Registered { graph, .. } = event {
            self.0.graphs.lock().unwrap().push(
                serde_json::to_value(graph)
                    .map_err(|_| AppError::Integration("test graph serialization".into()))?,
            );
        }
        Ok(())
    }
}

enum PredicateRule {
    Constant(bool),
    Equals { key: String, value: Value },
    Error(Mutex<Option<AppError>>),
    Panic,
}

struct Predicate {
    declaration: ConditionDeclaration,
    rule: PredicateRule,
    evidence: Arc<Evidence>,
}

impl Predicate {
    fn new(
        key: &str,
        revision: &str,
        reads: &[&str],
        rule: PredicateRule,
        evidence: &Arc<Evidence>,
    ) -> Self {
        Self {
            declaration: ConditionDeclaration {
                key: key.into(),
                revision: revision.into(),
                reads: reads.iter().map(|key| (*key).into()).collect(),
            },
            rule,
            evidence: Arc::clone(evidence),
        }
    }

    fn reference(key: &str, revision: &str) -> Value {
        json!({"kind":"custom", "condition":key, "revision":revision})
    }

    fn choose(when: Value, value: Value) -> Value {
        json!({"kind":"conditional", "when":when,
            "then":{"kind":"literal", "value":value},
            "else":{"kind":"state", "path":"unchosen.missing"}})
    }
}

impl ConditionBackend for Predicate {
    fn declaration(&self) -> &ConditionDeclaration {
        &self.declaration
    }

    fn evaluate(&self, state: &WorkflowState) -> Result<bool, AppError> {
        self.evidence.calls.lock().unwrap().push((
            format!("{}@{}", self.declaration.key, self.declaration.revision),
            state.values().clone(),
        ));
        match &self.rule {
            PredicateRule::Constant(value) => Ok(*value),
            PredicateRule::Equals { key, value } => Ok(state.values().get(key) == Some(value)),
            PredicateRule::Error(error) => Err(error
                .lock()
                .unwrap()
                .take()
                .expect("a failed predicate is never replayed")),
            PredicateRule::Panic => panic!("private predicate panic"),
        }
    }
}

struct ConditionCase {
    app: App<StateStore, Agent, NoActions, Predicate, JsonState, Observed>,
    workflow: Workflow<StateStore, Agent, NoActions, Predicate, JsonState, Observed>,
}

impl ConditionCase {
    fn definition(id: &str, state: Value, nodes: Vec<Value>, routes: Value) -> WorkflowDefinition {
        let reducers = state
            .as_object()
            .expect("test state object")
            .keys()
            .map(|key| (key.clone(), json!("reject_concurrent")))
            .collect::<Map<String, Value>>();
        WorkflowDefinition::parse(&json!({
            "schema":"app-kit.workflow/v2", "id":id, "revision":"r1", "state":state,
            "required":[], "reducers":reducers, "entry":nodes[0]["id"], "nodes":nodes, "routes":routes,
        }).to_string()).expect("nested custom predicate syntax")
    }

    fn mutation(state: Value, mutations: Vec<Value>) -> WorkflowDefinition {
        Self::definition(
            "condition",
            state,
            vec![json!({"id":"effect", "step":{"kind":"state", "mutations":mutations}})],
            json!({}),
        )
    }

    fn route(when: Value) -> WorkflowDefinition {
        Self::definition(
            "condition",
            json!({"count":0,"gate":true,"sentinel":91}),
            vec![
                json!({"id":"seed","step":{"kind":"state","mutations":[{"kind":"assign","target":"count","value":{"kind":"literal","value":7}}]}}),
                json!({"id":"selected","step":{"kind":"state","mutations":[{"kind":"assign","target":"count","value":{"kind":"literal","value":9}}]}}),
            ],
            json!({"seed":{"mode":"first","branches":[{"when":when,"target":{"kind":"node","id":"selected"}}]}}),
        )
    }

    fn child() -> WorkflowDefinition {
        Self::definition(
            "leaf",
            json!({"item":0,"tag":0,"result":0}),
            vec![json!({
                "id":"sum","step":{"kind":"state","mutations":[{
                    "kind":"assign","target":"result","value":{"kind":"add",
                        "left":{"kind":"state","path":"item"},"right":{"kind":"state","path":"tag"}}
                }]}
            })],
            json!({}),
        )
    }

    fn parent(step: Value) -> WorkflowDefinition {
        WorkflowDefinition::parse(
            &json!({
                "schema":"app-kit.workflow/v2","id":"condition","revision":"r1",
                "state":{"count":0,"gate":true,"collected":[]},"required":[],
                "reducers":{"count":"reject_concurrent","gate":"reject_concurrent","collected":"collect"},"entry":"children",
                "nodes":[{"id":"children","step":step}],"routes":{}
            })
            .to_string(),
        )
        .expect("expression-valued child input syntax")
    }

    fn placements(when: Value) -> Vec<(&'static str, Vec<WorkflowDefinition>)> {
        let reductions = json!([{"source":"result","target":"collected","reducer":"collect"}]);
        vec![
            (
                "state",
                vec![Self::mutation(
                    json!({"count":0,"gate":true}),
                    vec![json!({
                        "kind":"assign","target":"count","value":Predicate::choose(when.clone(),json!(7))
                    })],
                )],
            ),
            ("route", vec![Self::route(when.clone())]),
            (
                "child-input",
                vec![
                    Self::parent(json!({"kind":"child","workflow":"leaf",
                        "input":{"item":Predicate::choose(when.clone(),json!(7))},"reductions":reductions.clone()
                    })),
                    Self::child(),
                ],
            ),
            (
                "map-items",
                vec![
                    Self::parent(json!({"kind":"map","workflow":"leaf",
                        "items":Predicate::choose(when.clone(),json!([1,2])),"input_key":"item","input":{},
                        "max_parallel":2,"reductions":reductions.clone()
                    })),
                    Self::child(),
                ],
            ),
            (
                "map-input",
                vec![
                    Self::parent(json!({"kind":"map","workflow":"leaf",
                        "items":{"kind":"literal","value":[1,2]},"input_key":"item",
                        "input":{"tag":Predicate::choose(when,json!(7))},"max_parallel":2,"reductions":reductions
                    })),
                    Self::child(),
                ],
            ),
        ]
    }

    async fn store() -> StateStore {
        StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap()
    }

    async fn open(
        definitions: Vec<WorkflowDefinition>,
        conditions: Vec<Predicate>,
        evidence: &Arc<Evidence>,
    ) -> Self {
        let config: AppConfig<StateStore, Agent> =
            AppConfig::new(Self::store().await, Vec::new(), definitions);
        let app = App::open(
            config
                .with_conditions(conditions)
                .with_hooks(Observed(Arc::clone(evidence))),
        )
        .await
        .unwrap();
        assert!(
            evidence.calls.lock().unwrap().is_empty(),
            "binding never evaluates a predicate"
        );
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().unwrap(),
            })
            .await
            .unwrap();
        let workflow = workspace
            .workflow("condition".parse().unwrap())
            .await
            .unwrap();
        Self { app, workflow }
    }

    async fn finish(mut self) -> RunSnapshot {
        let run = self
            .workflow
            .start(ExecutionOptions::default())
            .await
            .unwrap();
        let observed = tokio::time::timeout(Duration::from_secs(5), run.wait()).await;
        let closed = self
            .app
            .shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            })
            .await;
        closed.expect("join condition test's accepted work");
        observed
            .expect("condition execution is bounded")
            .expect("durable result")
    }

    fn failed(snapshot: &RunSnapshot, panic: bool, committed: bool) {
        assert!(if panic {
            matches!(
                snapshot.status,
                RunStatus::Failed(WorkflowFailure::Execution(_))
            )
        } else {
            matches!(
                snapshot.status,
                RunStatus::Failed(WorkflowFailure::Evaluation(_))
            )
        });
        assert_eq!(
            snapshot.state.values()["count"],
            if committed { 7 } else { 0 }
        );
        assert_eq!(snapshot.last_action_ordinal, committed.then_some(0));
        assert_eq!(snapshot.outcomes.len(), usize::from(committed));
        if committed {
            assert_eq!(snapshot.outcomes[0].node, "seed");
            assert!(snapshot.outcomes[0].result.is_empty());
        }
    }
}

#[tokio::test]
async fn exact_condition_revisions_ignore_inventory_order_and_preserve_full_state() {
    for revision in ["yes", "no"] {
        for reverse in [false, true] {
            let evidence = Arc::new(Evidence::default());
            let mut predicates = vec![
                Predicate::new(
                    "choose",
                    "yes",
                    &["gate"],
                    PredicateRule::Constant(true),
                    &evidence,
                ),
                Predicate::new(
                    "choose",
                    "no",
                    &["gate"],
                    PredicateRule::Constant(false),
                    &evidence,
                ),
            ];
            if reverse {
                predicates.reverse();
            }
            let case = ConditionCase::open(
                vec![ConditionCase::route(Predicate::reference(
                    "choose", revision,
                ))],
                predicates,
                &evidence,
            )
            .await;
            let snapshot = case.finish().await;
            assert!(matches!(snapshot.status, RunStatus::Succeeded));
            assert_eq!(
                snapshot.state.values()["count"],
                if revision == "yes" { 9 } else { 7 }
            );
            let calls = evidence.calls.lock().unwrap();
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].0, format!("choose@{revision}"));
            assert_eq!(
                calls[0].1["count"], 7,
                "outgoing condition observes committed state"
            );
            assert_eq!(
                calls[0].1["sentinel"], 91,
                "declared reads do not filter state"
            );
        }
    }
}

#[tokio::test]
async fn custom_predicates_reach_each_state_mutation_value_and_nested_expression() {
    let when = Predicate::reference("choose", "r1");
    let cases = [
        (
            json!({"kind":"assign","target":"value","value":Predicate::choose(when.clone(), json!(4))}),
            json!(4),
        ),
        (
            json!({"kind":"append","target":"items","value":Predicate::choose(when.clone(), json!(4))}),
            json!([1, 4]),
        ),
        (
            json!({"kind":"extend","target":"items","values":Predicate::choose(when.clone(), json!([4,5]))}),
            json!([1, 4, 5]),
        ),
        (
            json!({"kind":"increment","target":"value","value":Predicate::choose(when.clone(), json!(4))}),
            json!(6),
        ),
        (
            json!({"kind":"merge","state":Predicate::choose(when.clone(), json!({"value":4}))}),
            json!(4),
        ),
        (
            json!({"kind":"assign","target":"value","value":{"kind":"add","left":{"kind":"length","value":Predicate::choose(when, json!([8,9]))},"right":{"kind":"literal","value":3}}}),
            json!(5),
        ),
    ];
    for (mutation, expected) in cases {
        let target = if matches!(mutation["kind"].as_str(), Some("append" | "extend")) {
            "items"
        } else {
            "value"
        };
        let evidence = Arc::new(Evidence::default());
        let predicate = Predicate::new(
            "choose",
            "r1",
            &["gate"],
            PredicateRule::Constant(true),
            &evidence,
        );
        let case = ConditionCase::open(
            vec![ConditionCase::mutation(
                json!({"gate":true,"value":2,"items":[1]}),
                vec![mutation],
            )],
            vec![predicate],
            &evidence,
        )
        .await;
        let snapshot = case.finish().await;
        assert!(matches!(snapshot.status, RunStatus::Succeeded));
        assert_eq!(snapshot.state.values()[target], expected);
        assert_eq!(snapshot.last_action_ordinal, Some(0));
        assert_eq!(evidence.calls.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn predicates_observe_prior_local_mutations_and_isolate_sibling_state() {
    let evidence = Arc::new(Evidence::default());
    let predicates = vec![
        Predicate::new(
            "one",
            "r1",
            &["flag"],
            PredicateRule::Equals {
                key: "flag".into(),
                value: json!(1),
            },
            &evidence,
        ),
        Predicate::new(
            "zero",
            "r1",
            &["flag"],
            PredicateRule::Equals {
                key: "flag".into(),
                value: json!(0),
            },
            &evidence,
        ),
    ];
    let definition = ConditionCase::definition(
        "condition",
        json!({"flag":0,"local":false,"sibling":false}),
        vec![
            json!({"id":"seed","step":{"kind":"state","mutations":[]}}),
            json!({"id":"a","step":{"kind":"state","mutations":[
                {"kind":"assign","target":"flag","value":{"kind":"literal","value":1}},
                {"kind":"assign","target":"local","value":Predicate::choose(Predicate::reference("one","r1"),json!(true))}
            ]}}),
            json!({"id":"b","step":{"kind":"state","mutations":[
                {"kind":"assign","target":"sibling","value":Predicate::choose(Predicate::reference("zero","r1"),json!(true))}
            ]}}),
        ],
        json!({"seed":{"mode":"broadcast","branches":[{"target":{"kind":"node","id":"b"}},{"target":{"kind":"node","id":"a"}}]}}),
    );
    let snapshot = ConditionCase::open(vec![definition], predicates, &evidence)
        .await
        .finish()
        .await;
    assert!(matches!(snapshot.status, RunStatus::Succeeded));
    assert_eq!(
        snapshot.state.values().clone(),
        json!({"flag":1,"local":true,"sibling":true})
            .as_object()
            .unwrap()
            .clone()
    );
    let calls = evidence.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(
        calls.iter().find(|(key, _)| key == "one@r1").unwrap().1["flag"],
        1
    );
    assert_eq!(
        calls.iter().find(|(key, _)| key == "zero@r1").unwrap().1["flag"],
        0
    );
}

#[tokio::test]
async fn declared_keys_remain_literal_while_builtin_reads_decode_paths() {
    for present in [true, false] {
        let evidence = Arc::new(Evidence::default());
        let predicate = Predicate::new(
            "literal",
            "r1",
            &["a.b", "/x"],
            PredicateRule::Constant(true),
            &evidence,
        );
        let mut state = json!({"count":0,"a":{"b":false},"x":false,"/x":true});
        if present {
            state["a.b"] = json!(true);
        }
        let when = json!({"kind":"all","conditions":[Predicate::reference("literal","r1"),
            {"kind":"equal","left":{"kind":"state","path":"a/b"},"right":{"kind":"literal","value":false}},
            {"kind":"equal","left":{"kind":"state","path":"x"},"right":{"kind":"literal","value":false}}
        ]});
        let definition = ConditionCase::mutation(
            state,
            vec![
                json!({"kind":"assign","target":"count","value":Predicate::choose(when,json!(1))}),
            ],
        );
        let snapshot = ConditionCase::open(vec![definition], vec![predicate], &evidence)
            .await
            .finish()
            .await;
        if present {
            assert!(matches!(snapshot.status, RunStatus::Succeeded));
            assert_eq!(snapshot.state.values()["count"], 1);
            assert_eq!(evidence.calls.lock().unwrap().len(), 1);
        } else {
            ConditionCase::failed(&snapshot, false, false);
            assert!(evidence.calls.lock().unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn route_read_preflight_includes_skipped_custom_and_nested_expression_reads() {
    for when in [
        json!({"kind":"all","conditions":[{"kind":"constant","value":false},Predicate::reference("missing","r1")]}),
        json!({"kind":"any","conditions":[{"kind":"constant","value":true},Predicate::reference("missing","r1")]}),
        json!({"kind":"all","conditions":[{"kind":"constant","value":false},
            {"kind":"equal","left":{"kind":"conditional","when":Predicate::reference("missing","r1"),"then":{"kind":"literal","value":1},"else":{"kind":"literal","value":0}},"right":{"kind":"literal","value":1}}
        ]}),
    ] {
        let evidence = Arc::new(Evidence::default());
        let predicate = Predicate::new(
            "missing",
            "r1",
            &["absent"],
            PredicateRule::Constant(true),
            &evidence,
        );
        let snapshot =
            ConditionCase::open(vec![ConditionCase::route(when)], vec![predicate], &evidence)
                .await
                .finish()
                .await;
        ConditionCase::failed(&snapshot, false, true);
        assert!(evidence.calls.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn bare_expression_short_circuit_skips_uninvoked_predicate_read_preflight() {
    let evidence = Arc::new(Evidence::default());
    let predicate = Predicate::new(
        "missing",
        "r1",
        &["absent"],
        PredicateRule::Panic,
        &evidence,
    );
    let expression = json!({"kind":"conditional","when":{"kind":"all","conditions":[
        {"kind":"constant","value":false},Predicate::reference("missing","r1")
    ]},"then":{"kind":"state","path":"unchosen"},"else":{"kind":"literal","value":4}});
    let snapshot = ConditionCase::open(
        vec![ConditionCase::mutation(
            json!({"count":0}),
            vec![json!({"kind":"assign","target":"count","value":expression})],
        )],
        vec![predicate],
        &evidence,
    )
    .await
    .finish()
    .await;
    assert!(matches!(snapshot.status, RunStatus::Succeeded));
    assert_eq!(snapshot.state.values()["count"], 4);
    assert!(evidence.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn explicit_condition_errors_and_panics_preserve_the_owning_commit_boundary() {
    for panic in [false, true] {
        for committed in [false, true] {
            let evidence = Arc::new(Evidence::default());
            let predicate = Predicate::new(
                "failure",
                "r1",
                &[],
                if panic {
                    PredicateRule::Panic
                } else {
                    PredicateRule::Error(Mutex::new(Some(AppError::Integration(
                        "private predicate error".into(),
                    ))))
                },
                &evidence,
            );
            let when = Predicate::reference("failure", "r1");
            let definition = if committed {
                ConditionCase::route(when)
            } else {
                ConditionCase::mutation(
                    json!({"count":0}),
                    vec![
                        json!({"kind":"assign","target":"count","value":{"kind":"literal","value":99}}),
                        json!({"kind":"assign","target":"count","value":Predicate::choose(when,json!(1))}),
                    ],
                )
            };
            let snapshot = ConditionCase::open(vec![definition], vec![predicate], &evidence)
                .await
                .finish()
                .await;
            ConditionCase::failed(&snapshot, panic, committed);
            assert_eq!(
                evidence.calls.lock().unwrap().len(),
                1,
                "no predicate replay"
            );
        }
    }
}

#[tokio::test]
async fn custom_conditions_resolve_child_input_map_items_and_additional_map_input() {
    for (label, definitions) in ConditionCase::placements(Predicate::reference("choose", "r1")) {
        if matches!(label, "state" | "route") {
            continue;
        }
        let evidence = Arc::new(Evidence::default());
        let predicate = Predicate::new(
            "choose",
            "r1",
            &["gate"],
            PredicateRule::Constant(true),
            &evidence,
        );
        let snapshot = ConditionCase::open(definitions, vec![predicate], &evidence)
            .await
            .finish()
            .await;
        assert!(matches!(snapshot.status, RunStatus::Succeeded), "{label}");
        assert_eq!(
            snapshot.state.values()["collected"],
            match label {
                "child-input" => json!([7]),
                "map-items" => json!([1, 2]),
                "map-input" => json!([8, 9]),
                _ => unreachable!("only child expression contexts"),
            }
        );
        assert_eq!(snapshot.last_action_ordinal, Some(0));
        assert_eq!(
            evidence.calls.lock().unwrap().len(),
            1,
            "each parent expression runs once: {label}"
        );
    }
}

#[tokio::test]
async fn every_nested_condition_binding_is_checked_before_app_publication() {
    for wrapped in [false, true] {
        let mut when = Predicate::reference("choose", "missing");
        if wrapped {
            when = json!({"kind":"all","conditions":[{"kind":"constant","value":false},when]});
        }
        for (label, definitions) in ConditionCase::placements(when) {
            let evidence = Arc::new(Evidence::default());
            let store = ConditionCase::store().await;
            let predicate =
                Predicate::new("choose", "existing", &[], PredicateRule::Panic, &evidence);
            let config: AppConfig<StateStore, Agent> =
                AppConfig::new(store.clone(), Vec::new(), definitions);
            let opened = App::open(
                config
                    .with_conditions(vec![predicate])
                    .with_hooks(Observed(Arc::clone(&evidence))),
            )
            .await;
            let error = match opened {
                Ok(mut app) => {
                    app.shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_secs(3),
                    })
                    .await
                    .unwrap();
                    panic!("missing nested binding was accepted: {label}, wrapped={wrapped}");
                }
                Err(error) => error,
            };
            assert!(
                matches!(error, AppError::InvalidInput(_)),
                "{label}: {error}"
            );
            assert!(evidence.calls.lock().unwrap().is_empty());
            assert!(evidence.graphs.lock().unwrap().is_empty());
            let records = store
                .list(StateListRequest::new("app-kit").unwrap())
                .await
                .unwrap();
            assert!(
                records.records.is_empty(),
                "preflight published App records: {label}"
            );
            assert!(records.next_start_after.is_none());
        }
    }
}

#[tokio::test]
async fn missing_and_duplicate_condition_pairs_fail_without_selecting_by_key_alone() {
    for duplicate in [false, true] {
        let evidence = Arc::new(Evidence::default());
        let store = ConditionCase::store().await;
        let definitions = vec![ConditionCase::route(Predicate::reference(
            "choose", "required",
        ))];
        let mut predicates = vec![Predicate::new(
            "choose",
            if duplicate { "required" } else { "other" },
            &[],
            PredicateRule::Panic,
            &evidence,
        )];
        if duplicate {
            predicates.push(Predicate::new(
                "choose",
                "required",
                &[],
                PredicateRule::Constant(false),
                &evidence,
            ));
        }
        let config: AppConfig<StateStore, Agent> =
            AppConfig::new(store.clone(), Vec::new(), definitions);
        match App::open(config.with_conditions(predicates)).await {
            Err(error) => assert!(matches!(error, AppError::InvalidInput(_))),
            Ok(mut app) => {
                app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(3),
                })
                .await
                .unwrap();
                panic!("invalid exact-pair inventory accepted");
            }
        }
        assert!(evidence.calls.lock().unwrap().is_empty());
        assert!(
            store
                .list(StateListRequest::new("app-kit").unwrap())
                .await
                .unwrap()
                .records
                .is_empty()
        );
    }
}

#[tokio::test]
async fn child_expression_errors_and_panics_do_not_commit_a_parent_outcome() {
    for panic in [false, true] {
        for (label, definitions) in ConditionCase::placements(Predicate::reference("failure", "r1"))
        {
            if matches!(label, "state" | "route") {
                continue;
            }
            let evidence = Arc::new(Evidence::default());
            let predicate = Predicate::new(
                "failure",
                "r1",
                &[],
                if panic {
                    PredicateRule::Panic
                } else {
                    PredicateRule::Error(Mutex::new(Some(AppError::Integration(
                        "private predicate error".into(),
                    ))))
                },
                &evidence,
            );
            let snapshot = ConditionCase::open(definitions, vec![predicate], &evidence)
                .await
                .finish()
                .await;
            ConditionCase::failed(&snapshot, panic, false);
            assert_eq!(snapshot.state.values()["collected"], json!([]));
            assert_eq!(
                evidence.calls.lock().unwrap().len(),
                1,
                "no replay in {label}"
            );
        }
    }
}

#[tokio::test]
async fn registered_graph_uses_exact_condition_reads_without_evaluation_or_route_leakage() {
    let evidence = Arc::new(Evidence::default());
    let definition = ConditionCase::definition(
        "condition",
        json!({
            "read.only":true,"extra":true,"route-only":true,"a":{"b":4},"count":0
        }),
        vec![json!({"id":"effect","step":{"kind":"state","mutations":[{
            "kind":"assign","target":"count","value":{"kind":"conditional",
                "when":Predicate::reference("choose","selected"),
                "then":{"kind":"state","path":"a/b"},"else":{"kind":"literal","value":0}}
        }]}})],
        json!({"effect":{"mode":"first","branches":[{
            "when":Predicate::reference("route","r1"),"target":{"kind":"end"}
        }]}}),
    );
    assert_eq!(
        definition.graph().nodes[0].reads,
        vec!["a"],
        "authored graph cannot guess callable declarations"
    );
    let predicates = vec![
        Predicate::new(
            "choose",
            "other",
            &["wrong-revision"],
            PredicateRule::Panic,
            &evidence,
        ),
        Predicate::new(
            "choose",
            "selected",
            &["read.only", "extra"],
            PredicateRule::Panic,
            &evidence,
        ),
        Predicate::new(
            "route",
            "r1",
            &["route-only"],
            PredicateRule::Panic,
            &evidence,
        ),
    ];
    let mut case = ConditionCase::open(vec![definition], predicates, &evidence).await;
    let prepared = case.workflow.prepare(WorkflowState::default()).await;
    let graphs = evidence.graphs.lock().unwrap().clone();
    let closed = case
        .app
        .shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        })
        .await;
    closed.expect("prepared metadata work drains");
    let prepared = prepared.expect("prepare registers the bound graph");
    assert!(!prepared.id().to_string().is_empty());
    assert_eq!(graphs.len(), 1);
    assert_eq!(
        graphs[0]["nodes"][0]["reads"],
        json!(["read.only", "extra", "a"])
    );
    assert_eq!(graphs[0]["nodes"][0]["writes"], json!(["count"]));
    assert!(
        evidence.calls.lock().unwrap().is_empty(),
        "inspection cannot execute predicates"
    );
}

#[tokio::test]
async fn caller_condition_error_variants_are_redacted_without_reclassifying_or_replaying() {
    const PRIVATE: &str = "private-predicate-prompt-token-/caller/secret";
    for committed in [false, true] {
        for (label, error) in [
            ("invalid-input", AppError::InvalidInput(PRIVATE.into())),
            ("not-found", AppError::NotFound(PRIVATE.into())),
            ("conflict", AppError::Conflict(PRIVATE.into())),
            ("busy", AppError::Busy(PRIVATE.into())),
            ("closed", AppError::Closed(PRIVATE.into())),
            ("timeout", AppError::Timeout(PRIVATE.into())),
            ("integration", AppError::Integration(PRIVATE.into())),
        ] {
            let evidence = Arc::new(Evidence::default());
            let predicate = Predicate::new(
                "failure",
                "r1",
                &[],
                PredicateRule::Error(Mutex::new(Some(error))),
                &evidence,
            );
            let when = Predicate::reference("failure", "r1");
            let definition = if committed {
                ConditionCase::route(when)
            } else {
                ConditionCase::mutation(
                    json!({"count":0}),
                    vec![
                        json!({"kind":"assign","target":"count","value":{"kind":"literal","value":99}}),
                        json!({"kind":"assign","target":"count","value":Predicate::choose(when,json!(1))}),
                    ],
                )
            };
            let snapshot = ConditionCase::open(vec![definition], vec![predicate], &evidence)
                .await
                .finish()
                .await;
            ConditionCase::failed(&snapshot, false, committed);
            let RunStatus::Failed(WorkflowFailure::Evaluation(message)) = snapshot.status else {
                panic!("caller predicate failure must remain an Evaluation failure: {label}");
            };
            assert!(!message.contains(PRIVATE), "caller error leaked: {label}");
            assert!(message.len() <= 1024, "safe error is bounded: {label}");
            assert_eq!(
                evidence.calls.lock().unwrap().len(),
                1,
                "no replay: {label}"
            );
        }
    }
}

// These callers execute on the local foreground owner; background Driver panic
// supervision cannot establish this boundary's behavior.
struct ForegroundAction {
    declaration: ActionDeclaration,
    calls: Arc<AtomicUsize>,
}

impl ForegroundAction {
    fn new(key: &str, calls: Arc<AtomicUsize>) -> Self {
        Self {
            declaration: ActionDeclaration {
                key: key.into(),
                revision: "r1".into(),
                reads: vec!["count".into()],
                writes: vec!["count".into()],
                required_inputs: Vec::new(),
                optional_inputs: Vec::new(),
                bound_inputs: Map::new(),
                default_inputs: Map::new(),
                tags: Vec::new(),
                source: None,
                mode: ActionMode::Asynchronous,
            },
            calls,
        }
    }
}

impl ActionBackend for ForegroundAction {
    type Stream = futures_util::stream::Iter<std::vec::IntoIter<Result<ActionEvent, AppError>>>;

    fn declaration(&self) -> &ActionDeclaration {
        &self.declaration
    }

    fn start(&self, request: ActionRequest) -> Self::Stream {
        self.calls.fetch_add(1, Ordering::SeqCst);
        futures_util::stream::iter(vec![Ok(ActionEvent::Completed {
            result: Map::new(),
            writes: vec![StateChange::Assign {
                values: Map::from_iter([(
                    "count".into(),
                    json!(request.state.values()["count"].as_i64().unwrap() + 1),
                )]),
            }],
        })])
    }
}

struct ForegroundTyping(Arc<AtomicUsize>);

impl StateTyping for ForegroundTyping {
    type Data = WorkflowState;

    fn construct_data(&self, state: &WorkflowState) -> Result<Self::Data, AppError> {
        Ok(state.clone())
    }
    fn construct_state(&self, data: &Self::Data) -> Result<WorkflowState, AppError> {
        Ok(data.clone())
    }

    fn validate_pre(&self, _: &WorkflowState, _: &StateChange) -> Result<(), AppError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("private foreground typing panic")
    }
}

#[tokio::test]
async fn foreground_condition_panic_preserves_the_preceding_action_commit() {
    let evidence = Arc::new(Evidence::default());
    let first = Arc::new(AtomicUsize::new(0));
    let second = Arc::new(AtomicUsize::new(0));
    let definition = WorkflowDefinition::parse(
        &json!({
            "schema":"app-kit.workflow/v2", "id":"condition", "revision":"r1",
            "state":{"count":0}, "required":["count"], "reducers":{"count":"reject_concurrent"},
            "entry":"a", "nodes":[
                {"id":"a","step":{"kind":"action","action":"first","revision":"r1"}},
                {"id":"b","step":{"kind":"action","action":"second","revision":"r1"}}
            ], "routes":{"a":{"mode":"first","branches":[
                {"when":Predicate::reference("fail","r1"),"target":{"kind":"node","id":"b"}}
            ]}}
        })
        .to_string(),
    )
    .unwrap();
    let config: AppConfig<StateStore, Agent> =
        AppConfig::new(ConditionCase::store().await, Vec::new(), vec![definition]);
    let mut app = App::open(
        config
            .with_actions(vec![
                ForegroundAction::new("first", Arc::clone(&first)),
                ForegroundAction::new("second", Arc::clone(&second)),
            ])
            .with_conditions(vec![Predicate::new(
                "fail",
                "r1",
                &[],
                PredicateRule::Panic,
                &evidence,
            )]),
    )
    .await
    .unwrap();
    let checked = std::panic::AssertUnwindSafe(async {
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().unwrap(),
            })
            .await
            .unwrap();
        let workflow = workspace
            .workflow("condition".parse().unwrap())
            .await
            .unwrap();
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        let execution = run.execution(ExecutionOptions::default()).await.unwrap();
        let outcome = std::panic::AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(3),
            execution.finish(),
        ))
        .catch_unwind()
        .await;
        assert!(
            outcome.is_ok(),
            "foreground predicate panic must not escape"
        );
        let _ = outcome
            .unwrap()
            .expect("foreground failure observation is bounded");
        let snapshot = tokio::time::timeout(Duration::from_secs(3), run.wait())
            .await
            .expect("failure settles")
            .unwrap();
        let RunStatus::Failed(WorkflowFailure::Execution(message)) = snapshot.status else {
            panic!("predicate panic must retain Execution provenance")
        };
        assert!(!message.contains("private predicate panic"));
        assert_eq!(snapshot.state.values()["count"], 1);
        assert_eq!(snapshot.last_action_ordinal, Some(0));
        assert_eq!(snapshot.outcomes.len(), 1);
        assert_eq!(snapshot.outcomes[0].node, "a");
        assert_eq!(first.load(Ordering::SeqCst), 1);
        assert_eq!(second.load(Ordering::SeqCst), 0);
        assert_eq!(evidence.calls.lock().unwrap().len(), 1);
    })
    .catch_unwind()
    .await;
    let closed = tokio::time::timeout(
        Duration::from_secs(5),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        }),
    )
    .await;
    if let Err(panic) = checked {
        std::panic::resume_unwind(panic);
    }
    closed
        .expect("foreground caller cleanup is bounded")
        .expect("foreground caller work drains");
}

#[tokio::test]
async fn foreground_native_typing_panic_cannot_escape_or_commit_a_mutation() {
    let calls = Arc::new(AtomicUsize::new(0));
    let definition = ConditionCase::mutation(
        json!({"count":0}),
        vec![json!({
            "kind":"assign", "target":"count", "value":{"kind":"literal","value":99}
        })],
    );
    let config: AppConfig<StateStore, Agent> =
        AppConfig::new(ConditionCase::store().await, Vec::new(), vec![definition]);
    let mut app = App::open(config.with_state_typing(ForegroundTyping(Arc::clone(&calls))))
        .await
        .unwrap();
    let checked = std::panic::AssertUnwindSafe(async {
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().unwrap(),
            })
            .await
            .unwrap();
        let workflow = workspace
            .workflow("condition".parse().unwrap())
            .await
            .unwrap();
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "fault is not an initial-state callback"
        );
        let execution = run.execution(ExecutionOptions::default()).await.unwrap();
        let outcome = std::panic::AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(3),
            execution.finish(),
        ))
        .catch_unwind()
        .await;
        assert!(outcome.is_ok(), "foreground typing panic must not escape");
        let _ = outcome
            .unwrap()
            .expect("foreground native failure is bounded");
        let snapshot = tokio::time::timeout(Duration::from_secs(3), run.wait())
            .await
            .expect("failure settles")
            .unwrap();
        let RunStatus::Failed(WorkflowFailure::Execution(message)) = snapshot.status else {
            panic!("typing panic must retain Execution provenance")
        };
        assert!(!message.contains("private foreground typing panic"));
        assert_eq!(snapshot.state.values()["count"], 0);
        assert!(snapshot.outcomes.is_empty() && snapshot.last_action_ordinal.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    })
    .catch_unwind()
    .await;
    let closed = tokio::time::timeout(
        Duration::from_secs(5),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        }),
    )
    .await;
    if let Err(panic) = checked {
        std::panic::resume_unwind(panic);
    }
    closed
        .expect("foreground native cleanup is bounded")
        .expect("foreground native work drains");
}
