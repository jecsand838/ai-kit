//! Pipeline composition through the public App API and real LanceDB records.
//! The oracle is the pinned rurr executor/task-action source, not an executed
//! rurr package. Explicit reduction replacement and input overrides are App
//! extensions owned by WORKFLOW.md.

pub mod native_support;

use std::collections::BTreeSet;
use std::future::Future;
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::ThreadId;
use std::time::{Duration, Instant};

use app_sdk::workflow::{
    ActionMode, CheckpointSelector, ChildBatch, ChildCompletion, ChildExecutor, ExecutionOptions,
    Interaction, LifecycleEvent, LifecycleHooks, NoActions, NoConditions, Resolution, RunSelector,
    RunSnapshot, RunStatus, StateChange, StateTyping, SyncExecution, WorkflowDefinition,
    WorkflowFailure, WorkflowState,
};
use app_sdk::{App, AppConfig, AppError, Shutdown, Workspace, WorkspaceConfig};
use futures_util::FutureExt;
use native_support::{NativeAgent, NativeCase};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_sdk::state::record::StateKey;
use storage_sdk::{StateStore, StateStoreBackend, StateTransactionBackend};

#[derive(Clone, Copy)]
enum PipelineCase {
    Handoff,
    Reductions,
    Overrides,
    Failure,
    HistoryDeletion,
    FailedHistoryDeletion,
}

#[derive(Clone, Copy)]
enum StaticChild {
    Child,
    Map,
    Pipeline,
}

#[derive(Clone, Copy)]
enum RevisionCase {
    Available,
    MissingPair,
    HistoricalParent,
    Dynamic,
}

#[derive(Clone, Copy)]
enum CompositionHold {
    SavedCount(i64),
    SecondRegistration,
}

#[derive(Clone, Default)]
struct CompositionProbe {
    data: Arc<Mutex<Vec<(&'static str, ThreadId)>>>,
    steps: Arc<Mutex<Vec<(String, i64, ActionMode)>>>,
    registered_children: Arc<AtomicUsize>,
    executor_calls: Arc<AtomicUsize>,
    mutations: Arc<AtomicUsize>,
    hold: Arc<Mutex<Option<CompositionHold>>>,
    hold_entered: Arc<tokio::sync::Notify>,
    hold_release: Arc<tokio::sync::Notify>,
}

struct LocalState {
    state: Rc<WorkflowState>,
    created: ThreadId,
    probe: CompositionProbe,
}

impl CompositionProbe {
    fn data_event(&self, event: &'static str) {
        self.data
            .lock()
            .unwrap()
            .push((event, std::thread::current().id()));
    }

    fn assert_local(&self, after: usize, caller: ThreadId) {
        let data = self.data.lock().unwrap()[after..].to_vec();
        assert!(
            !data.is_empty(),
            "the pipeline constructs actual non-Send Data"
        );
        assert!(data.iter().all(|(_, thread)| *thread == caller), "{data:?}");
        let created = data
            .iter()
            .filter(|(event, _)| *event == "construct")
            .count();
        assert!(created > 0);
        assert_eq!(
            data.iter()
                .filter(|(event, _)| *event == "serialize")
                .count(),
            created
        );
        assert_eq!(
            data.iter().filter(|(event, _)| *event == "drop").count(),
            created
        );
    }
}

impl StateTyping for CompositionProbe {
    type Data = LocalState;

    fn construct_data(&self, state: &WorkflowState) -> Result<Self::Data, AppError> {
        if state.values().get("count").is_some_and(Value::is_null) {
            return Err(AppError::InvalidInput("typed count cannot be null".into()));
        }
        self.data_event("construct");
        Ok(LocalState {
            state: Rc::new(state.clone()),
            created: std::thread::current().id(),
            probe: self.clone(),
        })
    }

    fn construct_state(&self, data: &Self::Data) -> Result<WorkflowState, AppError> {
        assert_eq!(data.created, std::thread::current().id());
        self.data_event("serialize");
        Ok(data.state.as_ref().clone())
    }

    fn validate_pre(&self, _state: &WorkflowState, _change: &StateChange) -> Result<(), AppError> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl Drop for LocalState {
    fn drop(&mut self) {
        assert_eq!(self.created, std::thread::current().id());
        self.probe.data_event("drop");
    }
}

impl LifecycleHooks for CompositionProbe {
    fn observe(&self, event: &LifecycleEvent<'_>) -> Result<(), AppError> {
        match event {
            LifecycleEvent::Registered {
                parent: Some(_), ..
            } => {
                self.registered_children.fetch_add(1, Ordering::SeqCst);
            }
            LifecycleEvent::StepStarted { context, state, .. } => {
                let observed = (
                    context.node.clone(),
                    state.values()["count"].as_i64().unwrap(),
                    context.mode,
                );
                self.steps.lock().unwrap().push(observed);
            }
            _ => {}
        }
        Ok(())
    }

    async fn observe_async<'a>(&'a self, event: &'a LifecycleEvent<'a>) -> Result<(), AppError> {
        let selected = {
            let mut hold = self.hold.lock().unwrap();
            let selected = match (&*hold, event) {
                (
                    Some(CompositionHold::SavedCount(count)),
                    LifecycleEvent::StepFinished {
                        context,
                        checkpoint,
                        outcome: Some(Ok(_)),
                        ..
                    },
                ) => {
                    context.node == "effect"
                        && checkpoint.state.values()["count"].as_i64() == Some(*count)
                }
                (
                    Some(CompositionHold::SecondRegistration),
                    LifecycleEvent::Registered {
                        parent: Some(_), ..
                    },
                ) => self.registered_children.load(Ordering::SeqCst) == 2,
                _ => false,
            };
            if selected {
                hold.take();
            }
            selected
        };
        if selected {
            self.hold_entered.notify_one();
            self.hold_release.notified().await;
        }
        Ok(())
    }
}

impl ChildExecutor for CompositionProbe {
    type Execution<'a, F>
        = ChildBatch<F>
    where
        Self: 'a,
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a;

    fn execute<'a, F>(&'a self, batch: ChildBatch<F>) -> Self::Execution<'a, F>
    where
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a,
    {
        self.executor_calls.fetch_add(1, Ordering::SeqCst);
        batch
    }
}

#[derive(Clone, Copy)]
enum HistoryPolicyCase {
    Retain,
    DeleteAfterReduction,
    ParentCancelled,
    ChildCancelled,
}

#[derive(Clone, Copy)]
enum StagedHistoryCase {
    Child,
    LargeMap,
    CorruptOwnership,
    CorruptChildDepth,
    DeleteAfterReduction,
}

struct ChildComposition {
    store: StateStore,
    app: App<
        StateStore,
        NativeAgent,
        NoActions,
        NoConditions,
        CompositionProbe,
        CompositionProbe,
        CompositionProbe,
    >,
    workspace: Workspace<
        StateStore,
        NativeAgent,
        NoActions,
        NoConditions,
        CompositionProbe,
        CompositionProbe,
        CompositionProbe,
    >,
    probe: CompositionProbe,
}

impl ChildComposition {
    fn revision_parent(
        id: &str,
        kind: StaticChild,
        revisions: &[Option<&str>],
        case: RevisionCase,
    ) -> WorkflowDefinition {
        let mut state = json!({"count":7,"collected":[]});
        let mut reducers = json!({"count":"reject_concurrent","collected":"collect"});
        let children = revisions
            .iter()
            .enumerate()
            .map(|(index, revision)| {
                let mut child = json!({"workflow":"revision-leaf","input":{}});
                if let Some(revision) = revision {
                    child["revision"] = json!(revision);
                }
                if matches!(case, RevisionCase::Dynamic) {
                    let mut selected = json!({"workflow":"revision-leaf"});
                    if let Some(revision) = revision {
                        selected["revision"] = json!(revision);
                    }
                    let key = format!("selection{index}");
                    state[&key] = selected;
                    reducers[&key] = json!("reject_concurrent");
                    child.as_object_mut().unwrap().remove("workflow");
                    child.as_object_mut().unwrap().remove("revision");
                    child["select"] = json!({"kind":"state","path":key});
                }
                child
            })
            .collect::<Vec<_>>();
        let mut step = match kind {
            StaticChild::Pipeline => json!({"kind":"pipeline","children":children}),
            StaticChild::Child | StaticChild::Map => {
                assert_eq!(children.len(), 1);
                let mut step = children.into_iter().next().unwrap();
                if matches!(kind, StaticChild::Map) {
                    step["kind"] = json!("map");
                    step["items"] = json!({"kind":"literal","value":[0,100]});
                    step["input_key"] = json!("count");
                    step["max_parallel"] = json!(2);
                } else {
                    step["kind"] = json!("child");
                }
                step
            }
        };
        step["reductions"] = json!([{"source":"count","target":"collected","reducer":"collect"}]);
        NativeCase::definition(
            id,
            state,
            vec![json!({"id":"select","deadline_ms":3000,"step":step})],
            json!({}),
            reducers,
        )
    }

    fn revision_definitions(
        kind: StaticChild,
        selected_new: bool,
        case: RevisionCase,
    ) -> Vec<WorkflowDefinition> {
        let mut definitions = Vec::new();
        if matches!(case, RevisionCase::HistoricalParent) {
            let parents = if selected_new {
                [("z-old", "legacy"), ("a-new", "current")]
            } else {
                [("a-new", "current"), ("z-old", "legacy")]
            };
            for (revision, key) in parents {
                definitions.push(WorkflowDefinition::parse(&json!({
                    "schema":"app-sdk.workflow/v2", "id":"captured", "revision":revision,
                    "state":{"count":7,"collected":[]}, "required":[],
                    "reducers":{"count":"reject_concurrent","collected":"collect"},
                    "entry":"select", "nodes":[{"id":"select","deadline_ms":3000,"step":{
                        "kind":"child","workflow":"revision-leaf",
                        "input":{key:{"kind":"literal","value":99}},
                        "reductions":[{"source":"count","target":"collected","reducer":"collect"}]
                    }}], "routes":{}
                }).to_string()).expect("historical parent has its own child input schema"));
            }
        } else {
            definitions.push(Self::revision_parent("captured", kind, &[None], case));
        }
        if matches!(kind, StaticChild::Pipeline) {
            definitions.push(Self::revision_parent(
                "explicit",
                kind,
                &[Some("z-old"), Some("a-new")],
                case,
            ));
        } else {
            // Keep these separate parent invocations: cross-action task-key
            // reuse is a different contract from exact revision selection.
            definitions.push(Self::revision_parent(
                "explicit-old",
                kind,
                &[Some("z-old")],
                case,
            ));
            definitions.push(Self::revision_parent(
                "explicit-new",
                kind,
                &[Some("a-new")],
                case,
            ));
        }
        let revisions = if selected_new {
            [("z-old", "old-effect", 1), ("a-new", "new-effect", 10)]
        } else {
            [("a-new", "new-effect", 10), ("z-old", "old-effect", 1)]
        };
        for (revision, node, delta) in revisions {
            let mut state = json!({"count":0});
            let mut reducers = json!({"count":"reject_concurrent"});
            if matches!(case, RevisionCase::HistoricalParent) {
                let key = if revision == "z-old" {
                    "legacy"
                } else {
                    "current"
                };
                state[key] = json!(0);
                reducers[key] = json!("reject_concurrent");
            }
            definitions.push(
                WorkflowDefinition::parse(
                    &json!({
                        "schema":"app-sdk.workflow/v2", "id":"revision-leaf", "revision":revision,
                        "state":state, "required":[], "reducers":reducers,
                        "entry":node, "nodes":[{"id":node,"deadline_ms":1000,"step":{
                            "kind":"state","mutations":[{"kind":"increment","target":"count",
                                "value":{"kind":"literal","value":delta}}]
                        }}], "routes":{}
                    })
                    .to_string(),
                )
                .expect("same workflow identity at two exact revisions"),
            );
        }
        if matches!(case, RevisionCase::Dynamic) {
            definitions.push(NativeCase::definition(
                "unrelated-selection", json!({"label":"different schema"}),
                vec![json!({"id":"unused","step":{"kind":"input","message":[{"kind":"text","text":"unused"}]}})],
                json!({}), json!({"label":"reject_concurrent"}),
            ));
        }
        definitions
    }

    fn revisions(kind: StaticChild, case: RevisionCase) {
        let missing_pair = matches!(case, RevisionCase::MissingPair);
        // Keep syntax rejection outside every App/runtime owner. Runtime
        // assertions become reachable only after the static selector parses.
        let original_definitions = Self::revision_definitions(kind, false, case);
        let mut deployed = Self::revision_definitions(kind, true, case);
        if missing_pair {
            // a-new is present in the authored selectors but absent from this
            // inventory; the same WorkflowId still has its valid z-old entry.
            deployed.pop();
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let mut owner = runtime.block_on(Self::open(original_definitions));
        let mut reopened = None;
        let observed = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let workflow = runtime
                .block_on(async {
                    tokio::time::timeout(
                        Duration::from_secs(3),
                        owner.workspace.workflow("captured".parse().unwrap()),
                    )
                    .await
                })
                .unwrap()
                .unwrap();
            let saved = runtime
                .block_on(async {
                    tokio::time::timeout(
                        Duration::from_secs(3),
                        workflow.prepare(WorkflowState::default()),
                    )
                    .await
                })
                .unwrap()
                .unwrap();
            let initial = runtime
                .block_on(async {
                    tokio::time::timeout(Duration::from_secs(3), saved.snapshot()).await
                })
                .unwrap()
                .unwrap();
            let saved_id = saved.id().clone();
            let leaves = runtime
                .block_on(async {
                    tokio::time::timeout(Duration::from_secs(3), async {
                        owner
                            .workspace
                            .workflow("revision-leaf".parse().unwrap())
                            .await
                            .unwrap()
                            .list_runs(None, NonZeroUsize::new(16).unwrap())
                            .await
                            .unwrap()
                    })
                    .await
                })
                .unwrap();
            assert!(
                leaves.is_empty(),
                "root capture precedes the first child admission"
            );
            assert_eq!(owner.probe.registered_children.load(Ordering::SeqCst), 0);
            assert_eq!(owner.probe.executor_calls.load(Ordering::SeqCst), 0);
            assert_eq!(owner.probe.mutations.load(Ordering::SeqCst), 0);

            let config: AppConfig<StateStore, NativeAgent> =
                AppConfig::new(owner.store.clone(), Vec::new(), deployed);
            let opened = runtime
                .block_on(async {
                    tokio::time::timeout(
                        Duration::from_secs(3),
                        App::open(
                            config
                                .with_state_typing(owner.probe.clone())
                                .with_hooks(owner.probe.clone())
                                .with_child_executor(owner.probe.clone()),
                        ),
                    )
                    .await
                })
                .expect("revision deployment is bounded");
            match opened {
                Ok(app) => reopened = Some(app),
                Err(error) if missing_pair => {
                    assert!(
                        matches!(error, AppError::InvalidInput(_)),
                        "missing authored revision rejects inventory validation: {error}"
                    );
                    let unchanged = runtime
                        .block_on(async {
                            tokio::time::timeout(Duration::from_secs(3), saved.snapshot()).await
                        })
                        .unwrap()
                        .unwrap();
                    assert_eq!(unchanged.checkpoint, initial.checkpoint);
                    assert_eq!(unchanged.state, initial.state);
                    assert_eq!(unchanged.last_action_ordinal, initial.last_action_ordinal);
                    assert!(unchanged.outcomes == initial.outcomes);
                    assert_eq!(owner.probe.registered_children.load(Ordering::SeqCst), 0);
                    assert_eq!(owner.probe.executor_calls.load(Ordering::SeqCst), 0);
                    assert_eq!(owner.probe.mutations.load(Ordering::SeqCst), 0);
                    let leaves = runtime
                        .block_on(async {
                            tokio::time::timeout(Duration::from_secs(3), async {
                                owner
                                    .workspace
                                    .workflow("revision-leaf".parse().unwrap())
                                    .await
                                    .unwrap()
                                    .list_runs(None, NonZeroUsize::new(16).unwrap())
                                    .await
                                    .unwrap()
                            })
                            .await
                        })
                        .unwrap();
                    assert!(
                        leaves.is_empty(),
                        "missing revision cannot admit a child row"
                    );
                    return;
                }
                Err(error) => panic!("both exact revisions remain available on reopen: {error}"),
            }
            assert!(
                !missing_pair,
                "an available WorkflowId cannot substitute for its missing revision"
            );
            assert_eq!(owner.probe.registered_children.load(Ordering::SeqCst), 0);
            assert_eq!(owner.probe.executor_calls.load(Ordering::SeqCst), 0);
            assert_eq!(owner.probe.mutations.load(Ordering::SeqCst), 0);
            let workspace = runtime
                .block_on(async {
                    tokio::time::timeout(
                        Duration::from_secs(3),
                        reopened
                            .as_ref()
                            .unwrap()
                            .workspace(owner.workspace.id().clone()),
                    )
                    .await
                })
                .unwrap()
                .unwrap();
            let expectations = match kind {
                StaticChild::Pipeline => vec![("explicit", vec![1, 11])],
                StaticChild::Child => vec![("explicit-old", vec![1]), ("explicit-new", vec![10])],
                StaticChild::Map => vec![
                    ("explicit-old", vec![1, 101]),
                    ("explicit-new", vec![10, 110]),
                ],
            };
            let captured_old = if matches!(kind, StaticChild::Map) {
                vec![1, 101]
            } else {
                vec![1]
            };
            let captured_new = if matches!(kind, StaticChild::Map) {
                vec![10, 110]
            } else {
                vec![10]
            };
            let mut journeys = expectations
                .into_iter()
                .map(|(id, values)| (id, None, values))
                .collect::<Vec<_>>();
            journeys.push(("captured", Some(saved_id), captured_old));
            journeys.push(("captured", None, captured_new));
            let mut expected_children = Vec::new();
            for (id, saved_id, expected) in journeys {
                let mut run = runtime
                    .block_on(async {
                        tokio::time::timeout(Duration::from_secs(3), async {
                            let workflow = workspace.workflow(id.parse().unwrap()).await.unwrap();
                            match saved_id {
                                Some(id) => workflow.open_run(RunSelector::Latest(id)).await,
                                None => workflow.prepare(WorkflowState::default()).await,
                            }
                        })
                        .await
                    })
                    .unwrap()
                    .unwrap();
                let prior_data = owner.probe.data.lock().unwrap().len();
                let snapshot = SyncExecution::open(&mut run, ExecutionOptions::default())
                    .unwrap()
                    .finish()
                    .unwrap();
                owner
                    .probe
                    .assert_local(prior_data, std::thread::current().id());
                assert!(
                    matches!(snapshot.status, RunStatus::Succeeded),
                    "{id}: checkpoint {}, state {:?}",
                    snapshot.checkpoint,
                    snapshot.state.values()
                );
                assert_eq!(snapshot.state.values()["count"], 7);
                assert_eq!(snapshot.state.values()["collected"], json!(expected));
                assert_eq!(snapshot.last_action_ordinal, Some(0));
                assert_eq!(snapshot.outcomes.len(), 1);
                assert_eq!(snapshot.outcomes[0].node, "select");
                assert_eq!(snapshot.outcomes[0].ordinal, 0);
                if matches!(kind, StaticChild::Pipeline) {
                    assert_eq!(
                        Value::Object(snapshot.outcomes[0].result.clone()),
                        json!({
                            "child_states":expected.iter().map(|count| json!({"count":count})).collect::<Vec<_>>(),
                            "final_state":{"count":expected.last().unwrap()},
                        })
                    );
                }
                expected_children.extend(expected);
            }
            let leaves = runtime
                .block_on(async {
                    tokio::time::timeout(Duration::from_secs(3), async {
                        workspace
                            .workflow("revision-leaf".parse().unwrap())
                            .await
                            .unwrap()
                            .list_runs(None, NonZeroUsize::new(16).unwrap())
                            .await
                            .unwrap()
                    })
                    .await
                })
                .unwrap();
            assert_eq!(leaves.len(), expected_children.len());
            assert_eq!(
                leaves
                    .iter()
                    .map(|run| &run.id)
                    .collect::<BTreeSet<_>>()
                    .len(),
                leaves.len()
            );
            let mut actual = Vec::new();
            for child in &leaves {
                assert!(matches!(child.status, RunStatus::Succeeded));
                assert_eq!(child.last_action_ordinal, Some(0));
                assert_eq!(child.outcomes.len(), 1);
                let count = child.state.values()["count"].as_i64().unwrap();
                assert_eq!(
                    child.outcomes[0].node,
                    if [1, 101].contains(&count) {
                        "old-effect"
                    } else {
                        "new-effect"
                    }
                );
                actual.push(count);
            }
            actual.sort_unstable();
            expected_children.sort_unstable();
            assert_eq!(actual, expected_children);
            if matches!(case, RevisionCase::HistoricalParent) {
                // The saved parent and fresh parent each bind only the key in
                // their captured child schema; unused historical definitions
                // need not accept the newly selected default's schema.
                for key in ["legacy", "current"] {
                    assert_eq!(
                        leaves
                            .iter()
                            .filter(
                                |child| child.state.values().get(key).and_then(Value::as_i64)
                                    == Some(99)
                            )
                            .count(),
                        1
                    );
                    assert_eq!(
                        leaves
                            .iter()
                            .filter(
                                |child| child.state.values().get(key).and_then(Value::as_i64)
                                    == Some(0)
                            )
                            .count(),
                        1
                    );
                }
            }
            assert_eq!(
                owner.probe.registered_children.load(Ordering::SeqCst),
                leaves.len()
            );
            assert_eq!(
                owner.probe.executor_calls.load(Ordering::SeqCst),
                if matches!(kind, StaticChild::Pipeline) {
                    0
                } else {
                    4
                }
            );
        }));
        let fresh_cleanup = reopened.as_mut().map(|app| {
            runtime.block_on(async {
                AssertUnwindSafe(tokio::time::timeout(
                    Duration::from_secs(5),
                    app.shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_secs(3),
                    }),
                ))
                .catch_unwind()
                .await
            })
        });
        let original_cleanup = runtime.block_on(async {
            AssertUnwindSafe(tokio::time::timeout(
                Duration::from_secs(5),
                owner.app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(3),
                }),
            ))
            .catch_unwind()
            .await
        });
        drop(reopened);
        drop(owner);
        if let Err(panic) = observed {
            std::panic::resume_unwind(panic);
        }
        if let Some(cleanup) = fresh_cleanup {
            match cleanup {
                Ok(result) => result
                    .expect("new generation cleanup is bounded")
                    .expect("new generation joins"),
                Err(panic) => std::panic::resume_unwind(panic),
            }
        }
        match original_cleanup {
            Ok(result) => assert!(
                matches!(result, Ok(Ok(()) | Err(AppError::Closed(_)))),
                "original generation cleanup joins: {result:?}"
            ),
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }

    fn independent_groups() -> Vec<WorkflowDefinition> {
        vec![
            NativeCase::definition(
                "independent-groups",
                json!({"count":0,"collected":[]}),
                vec![
                    json!({"id":"seed","deadline_ms":1000,"step":{"kind":"state",
                        "mutations":[{"kind":"increment","target":"count",
                            "value":{"kind":"literal","value":1}}]}}),
                    json!({"id":"z","deadline_ms":8000,"step":{"kind":"child",
                        "workflow":"group-leaf","input":{"count":{"kind":"literal","value":20}},
                        "reductions":[{"source":"count","target":"collected","reducer":"collect"}]}}),
                    json!({"id":"a","deadline_ms":8000,"step":{"kind":"child",
                        "workflow":"group-leaf","input":{"count":{"kind":"literal","value":10}},
                        "reductions":[{"source":"count","target":"collected","reducer":"collect"}]}}),
                ],
                json!({"seed":{"mode":"broadcast","branches":[
                    {"target":{"kind":"node","id":"z"}},
                    {"target":{"kind":"node","id":"a"}}
                ]}}),
                json!({"count":"reject_concurrent","collected":"collect"}),
            ),
            NativeCase::definition(
                "group-leaf",
                json!({"count":0}),
                vec![
                    json!({"id":"release","deadline_ms":8000,"step":{"kind":"input",
                        "message":[{"kind":"text","text":"release this child"}]}}),
                    json!({"id":"increment","deadline_ms":1000,"step":{"kind":"state",
                        "mutations":[{"kind":"increment","target":"count",
                            "value":{"kind":"literal","value":1}}]}}),
                ],
                json!({"release":{"mode":"first","branches":[
                    {"target":{"kind":"node","id":"increment"}}
                ]}}),
                json!({"count":"reject_concurrent"}),
            ),
        ]
    }

    fn definitions(case: PipelineCase) -> Vec<WorkflowDefinition> {
        let children = ["first", "second", "third"]
            .into_iter()
            .map(|id| {
                let input = if matches!(case, PipelineCase::Overrides) && id == "second" {
                    json!({"count":{"kind":"state","path":"count"}})
                } else {
                    json!({})
                };
                let mut child = json!({"workflow":id,"input":input});
                if matches!(
                    case,
                    PipelineCase::HistoryDeletion | PipelineCase::FailedHistoryDeletion
                ) {
                    child["history"] = json!("delete_after_reduction");
                }
                child
            })
            .collect::<Vec<_>>();
        let reductions = if matches!(case, PipelineCase::Reductions) {
            json!([{"source":"count","target":"collected","reducer":"collect"}])
        } else {
            json!([])
        };
        let mut pipeline = json!({"kind":"pipeline","children":children});
        if !matches!(case, PipelineCase::Handoff) {
            pipeline["reductions"] = reductions;
        }
        let mut definitions = vec![NativeCase::definition(
            "composition",
            json!({"count":7,"collected":[]}),
            vec![json!({"id":"pipeline","deadline_ms":3000,"step":pipeline})],
            json!({}),
            json!({"count":"reject_concurrent","collected":"collect"}),
        )];
        for (id, seed) in [("first", 0), ("second", 100), ("third", 200)] {
            let delta = if matches!(
                case,
                PipelineCase::Failure | PipelineCase::FailedHistoryDeletion
            ) && id == "second"
            {
                json!("invalid increment")
            } else {
                json!(1)
            };
            definitions.push(NativeCase::definition(
                id, json!({"count":seed}),
                vec![json!({"id":id,"deadline_ms":1000,"step":{"kind":"state","mutations":[
                    {"kind":"increment","target":"count","value":{"kind":"literal","value":delta}}
                ]}})], json!({}), json!({"count":"reject_concurrent"}),
            ));
        }
        definitions
    }

    async fn open(definitions: Vec<WorkflowDefinition>) -> Self {
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let probe = CompositionProbe::default();
        let config: AppConfig<StateStore, NativeAgent> =
            AppConfig::new(store.clone(), Vec::new(), definitions);
        let mut app = App::open(
            config
                .with_state_typing(probe.clone())
                .with_hooks(probe.clone())
                .with_child_executor(probe.clone()),
        )
        .await
        .unwrap();
        let workspace = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(3), async {
            app.create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().unwrap(),
            })
            .await
        }))
        .catch_unwind()
        .await;
        let workspace = match workspace {
            Ok(Ok(Ok(workspace))) => workspace,
            observed => {
                let _cleanup = AssertUnwindSafe(tokio::time::timeout(
                    Duration::from_secs(5),
                    app.shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_secs(3),
                    }),
                ))
                .catch_unwind()
                .await;
                drop(app);
                match observed {
                    Ok(Ok(Err(error))) => panic!("composition workspace setup: {error}"),
                    Ok(Err(error)) => panic!("composition workspace setup is bounded: {error}"),
                    Err(panic) => std::panic::resume_unwind(panic),
                    Ok(Ok(Ok(_))) => unreachable!(),
                }
            }
        };
        Self {
            store,
            app,
            workspace,
            probe,
        }
    }

    async fn children(&self) -> Vec<RunSnapshot> {
        let mut children = Vec::new();
        for id in ["first", "second", "third"] {
            let runs = self
                .workspace
                .workflow(id.parse().unwrap())
                .await
                .unwrap()
                .list_runs(None, NonZeroUsize::new(4).unwrap())
                .await
                .unwrap();
            assert!(runs.len() <= 1, "a pipeline child must never be replayed");
            children.extend(runs);
        }
        children
    }

    fn run(case: PipelineCase, mode: ActionMode) {
        // Parse before constructing native/runtime owners: the first red is
        // the adopted pipeline syntax, before any App effect can occur.
        let definitions = Self::definitions(case);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let mut owner = runtime.block_on(Self::open(definitions));
        let observed = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let workflow = runtime
                .block_on(owner.workspace.workflow("composition".parse().unwrap()))
                .unwrap();
            let mut run = runtime
                .block_on(workflow.prepare(WorkflowState::default()))
                .unwrap();
            let initial = runtime.block_on(run.snapshot()).unwrap();
            if mode == ActionMode::Asynchronous {
                let rejected = runtime.block_on(async {
                    tokio::time::timeout(Duration::from_secs(5), async {
                        run.execution(ExecutionOptions::default())
                            .await?
                            .finish()
                            .await
                    })
                    .await
                });
                assert!(
                    matches!(rejected, Ok(Err(AppError::InvalidInput(_)))),
                    "Async pipeline must reject at mode validation"
                );
                let unchanged = runtime.block_on(run.snapshot()).unwrap();
                assert_eq!(unchanged.state, initial.state);
                assert_eq!(unchanged.last_action_ordinal, initial.last_action_ordinal);
                assert!(unchanged.outcomes == initial.outcomes);
                assert!(runtime.block_on(owner.children()).is_empty());
                assert_eq!(owner.probe.registered_children.load(Ordering::SeqCst), 0);
                assert_eq!(owner.probe.executor_calls.load(Ordering::SeqCst), 0);
                assert_eq!(owner.probe.mutations.load(Ordering::SeqCst), 0);
                let steps = owner.probe.steps.lock().unwrap().clone();
                assert!(
                    steps.is_empty(),
                    "mode rejects before the selected node's pre-hooks"
                );
            }
            let caller = std::thread::current().id();
            let prior_data = owner.probe.data.lock().unwrap().len();
            // An async rejection may still be releasing its exact execution
            // owner. Retry only Busy, without advancing or admitting children.
            let started = Instant::now();
            let snapshot = loop {
                match SyncExecution::open(&mut run, ExecutionOptions::default()) {
                    Ok(execution) => break execution.finish().expect("Sync pipeline execution"),
                    Err(AppError::Busy(_)) if mode == ActionMode::Asynchronous => {
                        assert!(
                            started.elapsed() < Duration::from_secs(3),
                            "rejected owner releases"
                        );
                        assert_eq!(owner.probe.registered_children.load(Ordering::SeqCst), 0);
                        assert_eq!(owner.probe.executor_calls.load(Ordering::SeqCst), 0);
                        assert_eq!(owner.probe.mutations.load(Ordering::SeqCst), 0);
                        runtime.block_on(async { tokio::task::yield_now().await });
                    }
                    Err(error) => panic!("Sync pipeline acquisition: {error}"),
                }
            };
            let children = runtime
                .block_on(async {
                    tokio::time::timeout(Duration::from_secs(3), async {
                        loop {
                            let children = owner.children().await;
                            if !matches!(case, PipelineCase::HistoryDeletion) || children.is_empty()
                            {
                                break children;
                            }
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                })
                .expect("public child observations remain bounded");
            assert_eq!(
                owner.probe.executor_calls.load(Ordering::SeqCst),
                0,
                "pipeline bypasses the supplied ChildExecutor in both modes"
            );
            let ids = children
                .iter()
                .map(|child| child.id.clone())
                .collect::<BTreeSet<_>>();
            assert_eq!(ids.len(), children.len());
            if matches!(
                case,
                PipelineCase::Failure | PipelineCase::FailedHistoryDeletion
            ) {
                assert!(matches!(
                    snapshot.status,
                    RunStatus::Failed(WorkflowFailure::Child(_))
                ));
                assert_eq!(snapshot.state, initial.state);
                assert_eq!(snapshot.last_action_ordinal, None);
                assert!(snapshot.outcomes.is_empty());
                assert_eq!(
                    children.len(),
                    2,
                    "no successor admission after child failure"
                );
                assert!(matches!(children[0].status, RunStatus::Succeeded));
                assert!(matches!(
                    children[1].status,
                    RunStatus::Failed(WorkflowFailure::State(_))
                ));
                assert_eq!(children[1].last_action_ordinal, None);
                assert!(children[1].outcomes.is_empty());
                assert_eq!(
                    owner.probe.mutations.load(Ordering::SeqCst),
                    2,
                    "first increment succeeds and second increment reaches primitive validation"
                );
                assert_eq!(children[0].state.values()["count"], 1);
                assert_eq!(children[1].state.values()["count"], 1);
            } else {
                let counts = if matches!(case, PipelineCase::Overrides) {
                    [1, 8, 9]
                } else {
                    [1, 2, 3]
                };
                let final_count = counts[2];
                assert!(matches!(snapshot.status, RunStatus::Succeeded));
                assert_eq!(snapshot.last_action_ordinal, Some(0));
                assert_eq!(snapshot.outcomes.len(), 1);
                let outcome = &snapshot.outcomes[0];
                assert_eq!(outcome.node, "pipeline");
                assert_eq!(outcome.ordinal, 0);
                assert_eq!(
                    outcome
                        .result
                        .keys()
                        .map(String::as_str)
                        .collect::<Vec<_>>(),
                    ["child_states", "final_state"]
                );
                assert_eq!(
                    Value::Object(outcome.result.clone()),
                    json!({
                        "child_states":counts.map(|count| json!({"count":count})),
                        "final_state":{"count":final_count},
                    })
                );
                let expected = if matches!(case, PipelineCase::Reductions) {
                    // App-native explicit reductions replace automatic writes;
                    // the result still retains the exact final child state.
                    json!({"count":7,"collected":[1,2,3]})
                } else {
                    json!({"count":final_count,"collected":[]})
                };
                assert_eq!(Value::Object(snapshot.state.values().clone()), expected);
                assert_eq!(
                    children.len(),
                    if matches!(case, PipelineCase::HistoryDeletion) {
                        0
                    } else {
                        3
                    }
                );
                for (child, count) in children.iter().zip(counts) {
                    assert!(matches!(child.status, RunStatus::Succeeded));
                    assert_eq!(child.state.values()["count"], count);
                    assert_eq!(child.last_action_ordinal, Some(0));
                    assert_eq!(child.outcomes.len(), 1);
                }
            }
            assert_eq!(
                owner.probe.registered_children.load(Ordering::SeqCst),
                if matches!(case, PipelineCase::HistoryDeletion) {
                    3
                } else {
                    children.len()
                }
            );
            let steps = owner.probe.steps.lock().unwrap().clone();
            let child_steps = steps
                .iter()
                .filter(|(node, _, _)| node != "pipeline")
                .collect::<Vec<_>>();
            let expected = if matches!(
                case,
                PipelineCase::Failure | PipelineCase::FailedHistoryDeletion
            ) {
                vec![("first", 0), ("second", 1)]
            } else if matches!(case, PipelineCase::Overrides) {
                vec![("first", 0), ("second", 7), ("third", 8)]
            } else {
                vec![("first", 0), ("second", 1), ("third", 2)]
            };
            assert_eq!(
                child_steps
                    .iter()
                    .map(|(node, count, _)| (node.as_str(), *count))
                    .collect::<Vec<_>>(),
                expected
            );
            assert!(
                steps
                    .iter()
                    .all(|(_, _, mode)| *mode == ActionMode::Synchronous)
            );
            owner.probe.assert_local(prior_data, caller);
        }));
        let closed = runtime.block_on(async {
            AssertUnwindSafe(tokio::time::timeout(
                Duration::from_secs(5),
                owner.app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(3),
                }),
            ))
            .catch_unwind()
            .await
        });
        drop(owner);
        if let Err(panic) = observed {
            std::panic::resume_unwind(panic);
        }
        match closed {
            Ok(result) => result
                .expect("composition cleanup remains bounded")
                .expect("composition cleanup joins"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
}

#[test]
fn synchronous_pipeline_hands_off_state_and_preserves_distinct_result_and_writes() {
    ChildComposition::run(PipelineCase::Handoff, ActionMode::Synchronous);
}

#[test]
fn pipeline_explicit_reductions_replace_automatic_final_state_writes() {
    ChildComposition::run(PipelineCase::Reductions, ActionMode::Synchronous);
}

#[test]
fn pipeline_successor_overrides_are_captured_from_the_original_parent() {
    ChildComposition::run(PipelineCase::Overrides, ActionMode::Synchronous);
}

#[test]
fn pipeline_child_failure_prevents_successor_admission_and_parent_commit() {
    ChildComposition::run(PipelineCase::Failure, ActionMode::Synchronous);
}

#[test]
fn asynchronous_pipeline_rejects_before_children_and_can_resume_synchronously() {
    ChildComposition::run(PipelineCase::Handoff, ActionMode::Asynchronous);
}

#[test]
fn static_child_revisions_and_omitted_capture_survive_deployment_and_reopen() {
    ChildComposition::revisions(StaticChild::Child, RevisionCase::Available);
}

#[test]
fn static_map_revisions_and_omitted_capture_survive_deployment_and_reopen() {
    ChildComposition::revisions(StaticChild::Map, RevisionCase::Available);
}

#[test]
fn pipeline_selects_two_exact_revisions_and_preserves_omitted_capture_on_reopen() {
    ChildComposition::revisions(StaticChild::Pipeline, RevisionCase::Available);
}

#[test]
fn dynamic_child_selection_uses_original_default_after_reopen_and_exact_revision_when_present() {
    ChildComposition::revisions(StaticChild::Child, RevisionCase::Dynamic);
}

#[test]
fn dynamic_map_selection_uses_original_default_after_reopen_and_exact_revision_when_present() {
    ChildComposition::revisions(StaticChild::Map, RevisionCase::Dynamic);
}

#[test]
fn dynamic_pipeline_selection_keeps_per_child_revision_and_captured_default() {
    ChildComposition::revisions(StaticChild::Pipeline, RevisionCase::Dynamic);
}

#[test]
fn static_child_missing_exact_revision_rejects_before_registration_or_effects() {
    ChildComposition::revisions(StaticChild::Child, RevisionCase::MissingPair);
}

#[test]
fn static_map_missing_exact_revision_rejects_before_registration_or_effects() {
    ChildComposition::revisions(StaticChild::Map, RevisionCase::MissingPair);
}

#[test]
fn pipeline_missing_exact_revision_rejects_before_registration_or_effects() {
    ChildComposition::revisions(StaticChild::Pipeline, RevisionCase::MissingPair);
}

#[test]
fn retained_parent_uses_saved_child_schema_while_unused_revision_ignores_new_default() {
    ChildComposition::revisions(StaticChild::Child, RevisionCase::HistoricalParent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_child_groups_keep_both_index_zero_members_and_commit_one_wave() {
    let mut owner = ChildComposition::open(ChildComposition::independent_groups()).await;
    let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(12), async {
        let workflow = owner
            .workspace
            .workflow("independent-groups".parse().unwrap())
            .await
            .unwrap();
        let parent = workflow.start(ExecutionOptions::default()).await.unwrap();
        let leaves = owner
            .workspace
            .workflow("group-leaf".parse().unwrap())
            .await
            .unwrap();
        let children = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let mut children = leaves
                    .list_runs(None, NonZeroUsize::new(4).unwrap())
                    .await
                    .unwrap();
                assert!(
                    children.len() <= 2,
                    "each single-child group admits exactly one member"
                );
                assert!(children.iter().all(|child| !matches!(
                    child.status,
                    RunStatus::Succeeded | RunStatus::Failed(_) | RunStatus::Cancelled
                )));
                if children.len() == 2
                    && children.iter().all(|child| {
                        child.interactions.len() == 1
                            && matches!(child.interactions[0], Interaction::Input { .. })
                    })
                {
                    children.sort_by_key(|child| child.state.values()["count"].as_i64().unwrap());
                    break children;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("a waiting child in one group cannot block admission of its ready sibling group");
        assert_eq!(
            children[0].id, children[1].id,
            "both index-zero groups use the stable default task key"
        );
        assert_ne!(
            children[0].occurrence, children[1].occurrence,
            "distinct parent activation membership retains independent occurrences"
        );
        for (child, count) in children.iter().zip([10, 20]) {
            assert_eq!(child.state.values()["count"], count);
            assert_eq!(child.last_action_ordinal, None);
            assert!(child.outcomes.is_empty());
            assert_eq!(child.interactions.len(), 1);
        }
        assert_eq!(owner.probe.registered_children.load(Ordering::SeqCst), 2);
        assert_eq!(owner.probe.executor_calls.load(Ordering::SeqCst), 2);
        let seed = parent.snapshot().await.unwrap();
        assert_eq!(seed.state.values()["count"], 1);
        assert_eq!(seed.state.values()["collected"], json!([]));
        assert_eq!(seed.last_action_ordinal, Some(0));
        assert_eq!(seed.outcomes.len(), 1);
        assert_eq!(seed.outcomes[0].node, "seed");
        assert_eq!(seed.outcomes[0].ordinal, 0);

        // Resolve the later canonical group's real child first. Its result
        // must remain staged until the independently waiting sibling completes.
        let later = leaves
            .open_run(RunSelector::Exact {
                id: children[1].id.clone(),
                occurrence: children[1].occurrence.clone(),
            })
            .await
            .unwrap();
        let Interaction::Input {
            sequence,
            generation,
            ..
        } = &children[1].interactions[0]
        else {
            panic!("the later child retains its own input reservation");
        };
        later
            .resolve(*sequence, *generation, Resolution::Input(Value::Null))
            .await
            .unwrap();
        let completed_later = tokio::time::timeout(Duration::from_secs(3), later.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(completed_later.status, RunStatus::Succeeded));
        assert_eq!(completed_later.state.values()["count"], 21);
        assert_eq!(completed_later.last_action_ordinal, Some(1));
        assert_eq!(completed_later.outcomes.len(), 1);
        assert_eq!(completed_later.outcomes[0].node, "increment");
        let partial = parent.snapshot().await.unwrap();
        assert_eq!(partial.state, seed.state);
        assert_eq!(partial.last_action_ordinal, seed.last_action_ordinal);
        assert!(partial.outcomes == seed.outcomes);
        let earlier = leaves
            .open_run(RunSelector::Exact {
                id: children[0].id.clone(),
                occurrence: children[0].occurrence.clone(),
            })
            .await
            .unwrap();
        let waiting = earlier.snapshot().await.unwrap();
        assert_eq!(waiting.interactions.len(), 1);
        match (&waiting.interactions[0], &children[0].interactions[0]) {
            (
                Interaction::Input {
                    sequence,
                    generation,
                    message,
                    schema,
                },
                Interaction::Input {
                    sequence: prior_sequence,
                    generation: prior_generation,
                    message: prior_message,
                    schema: prior_schema,
                },
            ) => {
                assert_eq!(
                    (sequence, generation, message, schema),
                    (
                        prior_sequence,
                        prior_generation,
                        prior_message,
                        prior_schema
                    )
                );
            }
            _ => panic!("the earlier child's exact input target remains unchanged"),
        }
        assert_eq!(waiting.checkpoint, children[0].checkpoint);
        assert_eq!(waiting.state, children[0].state);
        assert_eq!(waiting.last_action_ordinal, None);
        assert!(waiting.outcomes.is_empty());
        let Interaction::Input {
            sequence,
            generation,
            ..
        } = &children[0].interactions[0]
        else {
            panic!("the earlier child's original input reservation remains valid");
        };
        earlier
            .resolve(*sequence, *generation, Resolution::Input(Value::Null))
            .await
            .unwrap();
        let completed = tokio::time::timeout(Duration::from_secs(3), parent.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(completed.status, RunStatus::Succeeded));
        assert_eq!(completed.state.values()["count"], 1);
        assert_eq!(completed.state.values()["collected"], json!([11, 21]));
        assert_eq!(completed.last_action_ordinal, Some(2));
        assert_eq!(
            completed
                .outcomes
                .iter()
                .map(|outcome| (outcome.node.as_str(), outcome.ordinal))
                .collect::<Vec<_>>(),
            [("a", 1), ("z", 2)]
        );
        let mut final_children = leaves
            .list_runs(None, NonZeroUsize::new(4).unwrap())
            .await
            .unwrap();
        final_children.sort_by_key(|child| child.state.values()["count"].as_i64().unwrap());
        assert_eq!(final_children.len(), 2);
        for ((child, original), count) in final_children.iter().zip(&children).zip([11, 21]) {
            assert_eq!(child.id, original.id);
            assert_eq!(child.occurrence, original.occurrence);
            assert!(matches!(child.status, RunStatus::Succeeded));
            assert_eq!(child.state.values()["count"], count);
            assert_eq!(child.last_action_ordinal, Some(1));
            assert_eq!(child.outcomes.len(), 1);
            assert_eq!(child.outcomes[0].node, "increment");
            assert_eq!(child.outcomes[0].ordinal, 1);
        }
        assert_eq!(owner.probe.registered_children.load(Ordering::SeqCst), 2);
        assert_eq!(owner.probe.executor_calls.load(Ordering::SeqCst), 2);
    }))
    .catch_unwind()
    .await;
    let cleanup = AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(5),
        owner.app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        }),
    ))
    .catch_unwind()
    .await;
    drop(owner);
    match observed {
        Ok(result) => result.expect("independent child groups are bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    match cleanup {
        Ok(result) => result
            .expect("group cleanup is bounded")
            .expect("group cleanup joins"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

impl ChildComposition {
    fn history_document(kind: StaticChild, history: Value) -> Value {
        let child = json!({"workflow":"history-leaf","input":{},"history":history});
        let mut step = match kind {
            StaticChild::Pipeline => json!({"kind":"pipeline","children":[child]}),
            StaticChild::Child | StaticChild::Map => {
                let mut step = child;
                step["kind"] = json!(if matches!(kind, StaticChild::Map) {
                    "map"
                } else {
                    "child"
                });
                if matches!(kind, StaticChild::Map) {
                    step["items"] = json!({"kind":"literal","value":[10,20]});
                    step["input_key"] = json!("count");
                    step["max_parallel"] = json!(2);
                }
                step
            }
        };
        step["reductions"] = json!([{"source":"count","target":"collected","reducer":"collect"}]);
        json!({
            "schema":"app-sdk.workflow/v2", "id":"history-parent", "revision":"r1",
            "state":{"count":0,"collected":[]}, "required":[],
            "reducers":{"count":"reject_concurrent","collected":"collect"}, "entry":"compose",
            "nodes":[
                {"id":"compose","deadline_ms":8000,"step":step},
                {"id":"after","deadline_ms":8000,"step":{"kind":"input","message":[{"kind":"text","text":"parent result committed"}]}}
            ],
            "routes":{"compose":{"mode":"first","branches":[{"target":{"kind":"node","id":"after"}}]}}
        })
    }

    async fn history_policy(case: HistoryPolicyCase) {
        let history = if matches!(case, HistoryPolicyCase::Retain) {
            "retain"
        } else {
            "delete_after_reduction"
        };
        // App-native WORKFLOW.md 296-298: deletion is authorized only after
        // the parent's copied result commits, never by child completion alone.
        // Pinned rurr types.rs 105-120 supplies per-task persistence ownership;
        // the sealed-delete mapping is App's policy, not a rurr API assertion.
        let parent = WorkflowDefinition::parse(
            &Self::history_document(StaticChild::Map, json!(history)).to_string(),
        )
        .expect("authored child history policy parses before any runtime owner exists");
        let leaf = NativeCase::definition(
            "history-leaf",
            json!({"count":0}),
            vec![
                json!({"id":"release","deadline_ms":8000,"step":{"kind":"input","message":[{"kind":"text","text":"release child"}]}}),
                json!({"id":"increment","deadline_ms":1000,"step":{"kind":"state","mutations":[{"kind":"increment","target":"count","value":{"kind":"literal","value":1}}]}}),
            ],
            json!({"release":{"mode":"first","branches":[{"target":{"kind":"node","id":"increment"}}]}}),
            json!({"count":"reject_concurrent"}),
        );
        let mut owner = Self::open(vec![parent, leaf]).await;
        let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(12), async {
            let parent = owner.workspace.workflow("history-parent".parse().unwrap()).await.unwrap()
                .start(ExecutionOptions::default()).await.unwrap();
            let leaves = owner.workspace.workflow("history-leaf".parse().unwrap()).await.unwrap();
            let children = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let mut children = leaves.list_runs(None, NonZeroUsize::new(4).unwrap()).await.unwrap();
                    assert!(children.len() <= 2);
                    if children.len() == 2 && children.iter().all(|child| matches!(child.interactions.as_slice(), [Interaction::Input { .. }])) {
                        children.sort_by_key(|child| child.state.values()["count"].as_i64().unwrap());
                        break children;
                    }
                    tokio::task::yield_now().await;
                }
            }).await.expect("both original children reach their own input");
            let first = leaves.open_run(RunSelector::Exact { id: children[0].id.clone(), occurrence: children[0].occurrence.clone() }).await.unwrap();
            let Interaction::Input { sequence, generation, .. } = &children[0].interactions[0] else { unreachable!() };
            first.resolve(*sequence, *generation, Resolution::Input(Value::Null)).await.unwrap();
            let complete = tokio::time::timeout(Duration::from_secs(3), first.wait()).await.unwrap().unwrap();
            assert!(matches!(complete.status, RunStatus::Succeeded));
            assert_eq!(complete.state.values()["count"], 11);
            assert!(!first.history(None, NonZeroUsize::new(1).unwrap()).await.unwrap().checkpoints.is_empty(),
                "completed history stays readable while its sibling prevents parent reduction");
            assert_eq!(leaves.list_runs(None, NonZeroUsize::new(4).unwrap()).await.unwrap().len(), 2);
            let uncommitted = parent.snapshot().await.unwrap();
            assert_eq!(uncommitted.state.values()["collected"], json!([]));
            assert_eq!(uncommitted.last_action_ordinal, None);
            assert!(uncommitted.outcomes.is_empty());
            let second = leaves.open_run(RunSelector::Exact { id: children[1].id.clone(), occurrence: children[1].occurrence.clone() }).await.unwrap();
            if matches!(case, HistoryPolicyCase::ParentCancelled | HistoryPolicyCase::ChildCancelled) {
                let target = if matches!(case, HistoryPolicyCase::ParentCancelled) { &parent } else { &second };
                loop {
                    let current = target.snapshot().await.unwrap();
                    match target.cancel(current.checkpoint).await {
                        Ok(_) => break,
                        Err(AppError::Conflict(_)) => tokio::task::yield_now().await,
                        Err(error) => panic!("the original live control accepts cancellation: {error}"),
                    }
                }
                let stopped = parent.wait().await.unwrap();
                if matches!(case, HistoryPolicyCase::ParentCancelled) {
                    assert!(matches!(stopped.status, RunStatus::Cancelled));
                } else {
                    assert!(matches!(stopped.status, RunStatus::Failed(_)));
                }
                assert_eq!(stopped.state.values()["collected"], json!([]));
                assert_eq!(stopped.last_action_ordinal, None);
                assert!(stopped.outcomes.is_empty(), "no child output crossed a successful parent barrier");
                assert_eq!(leaves.list_runs(None, NonZeroUsize::new(4).unwrap()).await.unwrap().len(), 2);
                for original in &children {
                    let child = leaves.open_run(RunSelector::Exact { id: original.id.clone(), occurrence: original.occurrence.clone() }).await.unwrap();
                    assert!(!child.history(None, NonZeroUsize::new(1).unwrap()).await.unwrap().checkpoints.is_empty(),
                        "successful and cancelled child histories survive a failed or cancelled parent wave");
                }
                assert!(matches!(first.snapshot().await.unwrap().status, RunStatus::Succeeded));
                assert!(matches!(second.snapshot().await.unwrap().status, RunStatus::Cancelled));
                assert_eq!(owner.probe.registered_children.load(Ordering::SeqCst), 2);
                assert_eq!(owner.probe.executor_calls.load(Ordering::SeqCst), 1);
                return;
            }
            let Interaction::Input { sequence, generation, .. } = &children[1].interactions[0] else { unreachable!() };
            second.resolve(*sequence, *generation, Resolution::Input(Value::Null)).await.unwrap();
            let committed = tokio::time::timeout(Duration::from_secs(3), parent.wait()).await.unwrap().unwrap();
            assert!(matches!(committed.interactions.as_slice(), [Interaction::Input { .. }]));
            assert_eq!(committed.state.values()["collected"], json!([11,21]));
            assert_eq!(committed.last_action_ordinal, Some(0));
            assert_eq!(committed.outcomes.len(), 1);
            assert_eq!(committed.outcomes[0].node, "compose");
            if history == "delete_after_reduction" {
                tokio::time::timeout(Duration::from_secs(3), async {
                    for child in &children {
                        loop {
                            match leaves.open_run(RunSelector::Exact { id: child.id.clone(), occurrence: child.occurrence.clone() }).await {
                                Err(AppError::NotFound(_)) => break,
                                Ok(_) | Err(AppError::Closed(_)) => tokio::task::yield_now().await,
                                Err(error) => panic!("unexpected child deletion observation: {error}"),
                            }
                        }
                    }
                }).await.expect("sealed child deletion completes after copied parent result commits");
                assert!(leaves.list_runs(None, NonZeroUsize::new(4).unwrap()).await.unwrap().is_empty());
            } else {
                let retained = leaves.list_runs(None, NonZeroUsize::new(4).unwrap()).await.unwrap();
                assert_eq!(retained.len(), 2);
                for child in &children {
                    let retained = leaves.open_run(RunSelector::Exact { id: child.id.clone(), occurrence: child.occurrence.clone() }).await.unwrap();
                    assert!(!retained.history(None, NonZeroUsize::new(1).unwrap()).await.unwrap().checkpoints.is_empty());
                    assert!(matches!(retained.snapshot().await.unwrap().status, RunStatus::Succeeded));
                }
            }
            let unchanged = parent.snapshot().await.unwrap();
            assert_eq!(unchanged.state, committed.state);
            assert_eq!(unchanged.last_action_ordinal, committed.last_action_ordinal);
            assert!(unchanged.outcomes == committed.outcomes);
            assert_eq!(owner.probe.registered_children.load(Ordering::SeqCst), 2);
            assert_eq!(owner.probe.executor_calls.load(Ordering::SeqCst), 1);
        })).catch_unwind().await;
        let closed = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(5),
            owner.app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        ))
        .catch_unwind()
        .await;
        drop(owner);
        match observed {
            Ok(result) => result.expect("child history policy proof is bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        match closed {
            Ok(result) => result
                .expect("history cleanup is bounded")
                .expect("history cleanup joins"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
}

#[test]
fn child_history_policy_is_strict_for_child_map_and_pipeline() {
    for kind in [StaticChild::Child, StaticChild::Map, StaticChild::Pipeline] {
        for history in [json!("retain"), json!("delete_after_reduction")] {
            WorkflowDefinition::parse(
                &ChildComposition::history_document(kind, history).to_string(),
            )
            .expect("both documented history policies parse in every authored child form");
        }
        for history in [
            Value::Null,
            json!(false),
            json!({}),
            json!("delete"),
            json!("Retain"),
        ] {
            assert!(matches!(
                WorkflowDefinition::parse(
                    &ChildComposition::history_document(kind, history).to_string()
                ),
                Err(AppError::InvalidInput(_))
            ));
        }
    }
}

#[test]
fn documented_pipeline_history_fields_parse_without_rewriting_the_example() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("docs/examples/pipeline.json");
    WorkflowDefinition::parse(&std::fs::read_to_string(path).unwrap())
        .expect("the authored documentation fixture is an executable public definition");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_child_history_remains_after_parent_reduction() {
    ChildComposition::history_policy(HistoryPolicyCase::Retain).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_history_deletion_waits_for_the_parent_reduction_commit() {
    ChildComposition::history_policy(HistoryPolicyCase::DeleteAfterReduction).await;
}

impl ChildComposition {
    async fn staged_child_history(case: StagedHistoryCase) {
        let large_inputs = matches!(case, StagedHistoryCase::LargeMap);
        let corrupt_membership = matches!(
            case,
            StagedHistoryCase::CorruptOwnership | StagedHistoryCase::CorruptChildDepth
        );
        let payload = if large_inputs {
            "x".repeat(270_000)
        } else {
            String::new()
        };
        let expected = if large_inputs { vec![11, 21] } else { vec![11] };
        let mut child_step = json!({"kind":"child", "workflow":"staged-history-leaf",
            "input":{"count":{"kind":"literal","value":10},"payload":{"kind":"literal","value":payload}},
            "reductions":[{"source":"count","target":"collected","reducer":"collect"}]
        });
        if matches!(case, StagedHistoryCase::DeleteAfterReduction) {
            child_step["history"] = json!("delete_after_reduction");
        }
        if large_inputs {
            child_step["kind"] = json!("map");
            child_step["input"].as_object_mut().unwrap().remove("count");
            child_step["reductions"]
                .as_array_mut()
                .unwrap()
                .push(json!({"source":"payload","target":"payloads","reducer":"collect"}));
            child_step["items"] = json!({"kind":"literal","value":[10,20]});
            child_step["input_key"] = json!("count");
            child_step["max_parallel"] = json!(2);
            let admitted_states = [
                json!({"count":10,"payload":payload}),
                json!({"count":20,"payload":payload}),
            ];
            let input_bytes = serde_json::to_vec(&admitted_states).unwrap().len();
            let result_bytes = serde_json::to_vec(&[payload.as_str(), payload.as_str()])
                .unwrap()
                .len();
            assert!(input_bytes < 1_048_576 && result_bytes < 1_048_576);
            assert!(
                input_bytes + result_bytes > 1_048_576,
                "separately valid admitted inputs and staged writes cannot share one byte charge"
            );
        }
        let definitions = || {
            let parent = NativeCase::definition(
                "staged-history-parent",
                json!({"count":0,"collected":[],"payloads":[]}),
                vec![
                    json!({"id":"seed","deadline_ms":1000,"step":{"kind":"state","mutations":[
                        {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
                    ]}}),
                    json!({"id":"compose","deadline_ms":8000,"step":child_step.clone()}),
                    json!({"id":"hold","deadline_ms":8000,"step":{"kind":"input",
                        "message":[{"kind":"text","text":"hold the parent barrier"}]
                    }}),
                ],
                json!({"seed":{"mode":"broadcast","branches":[
                    {"target":{"kind":"node","id":"compose"}},
                    {"target":{"kind":"node","id":"hold"}}
                ]}}),
                json!({"count":"reject_concurrent","collected":"collect","payloads":"collect"}),
            );
            let leaf = NativeCase::definition(
                "staged-history-leaf",
                json!({"count":0,"payload":""}),
                vec![
                    json!({"id":"release","deadline_ms":8000,"step":{"kind":"input",
                        "message":[{"kind":"text","text":"release the original child"}]
                    }}),
                    json!({"id":"increment","deadline_ms":1000,"step":{"kind":"state","mutations":[
                        {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
                    ]}}),
                ],
                json!({"release":{"mode":"first","branches":[{"target":{"kind":"node","id":"increment"}}]}}),
                json!({"count":"reject_concurrent","payload":"reject_concurrent"}),
            );
            vec![parent, leaf]
        };
        let mut owner = ChildComposition::open(definitions()).await;
        let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(10), async {
        let parent = owner.workspace.workflow("staged-history-parent".parse().unwrap()).await.unwrap()
            .start(ExecutionOptions::default()).await.unwrap();
        let leaves = owner.workspace.workflow("staged-history-leaf".parse().unwrap()).await.unwrap();
        let (before, original) = loop {
            let mut children = leaves.list_runs(None, NonZeroUsize::new(3).unwrap()).await.unwrap();
            assert!(children.len() <= expected.len());
            let before = parent.snapshot().await.unwrap();
            if children.len() == expected.len()
                && children.iter().all(|child| matches!(child.interactions.as_slice(), [Interaction::Input { .. }]))
                && matches!(before.interactions.as_slice(), [Interaction::Input { .. }])
            {
                children.sort_by_key(|child| child.state.values()["count"].as_i64().unwrap());
                break (before, children);
            }
            tokio::task::yield_now().await;
        };
        assert_eq!(before.state.values()["collected"], json!([]));
        assert_eq!(before.last_action_ordinal, Some(0));
        let mut completed = Vec::new();
        for (original, expected) in original.iter().zip(&expected) {
            let child = leaves.open_run(RunSelector::Exact { id: original.id.clone(), occurrence: original.occurrence.clone() }).await.unwrap();
            let Interaction::Input { sequence, generation, .. } = original.interactions[0] else { unreachable!() };
            child.resolve(sequence, generation, Resolution::Input(Value::Null)).await.unwrap();
            let complete = child.wait().await.unwrap();
            assert!(matches!(complete.status, RunStatus::Succeeded));
            assert_eq!(complete.state.values()["count"], *expected);
            assert_eq!(complete.state.values()["payload"], payload);
            completed.push((child, complete.checkpoint));
        }
        // Both controls were publicly visible before releasing the child. No
        // caller has changed the parent's remaining Input, so its next durable
        // advance accepts this original child result while the wave stays held.
        let staged = loop {
            let current = parent.snapshot().await.unwrap();
            if current.checkpoint > before.checkpoint {
                break current;
            }
            tokio::task::yield_now().await;
        };
        assert_eq!(staged.state.values()["collected"], json!([]));
        assert_eq!(staged.last_action_ordinal, Some(0));
        assert_eq!(staged.outcomes[0].node, "seed");
        assert!(matches!(staged.interactions.as_slice(), [Interaction::Input { .. }]));
        if corrupt_membership {
            let workflow = owner.workspace.workflow("staged-history-parent".parse().unwrap()).await.unwrap();
            loop {
            let mut driver = workflow.open_run(RunSelector::Latest(parent.id().clone())).await.unwrap();
            let idle = match driver.execution(ExecutionOptions::default()).await {
                Ok(execution) => execution,
                Err(AppError::Busy(_)) => { tokio::task::yield_now().await; continue; }
                Err(error) => panic!("cannot own the original staged boundary: {error}"),
            };
            let staged = parent.snapshot().await.unwrap();
            let corrupt_depth = matches!(case, StagedHistoryCase::CorruptChildDepth);
            let (workflow_id, run_id, occurrence, checkpoint) = if corrupt_depth {
                ("staged-history-leaf", completed[0].0.id(), &original[0].occurrence, completed[0].1)
            } else { ("staged-history-parent", parent.id(), &staged.occurrence, staged.checkpoint) };
            let original = owner.store.load(StateKey::new("app-sdk", format!(
                "history/{}/{workflow_id}/{run_id}/{occurrence}/checkpoint/{checkpoint:020}", owner.workspace.id(),
            )).unwrap()).await.unwrap().unwrap().record;
            let predecessor = if corrupt_depth {
                Some(owner.store.load(StateKey::new("app-sdk", format!(
                    "history/{}/{workflow_id}/{run_id}/{occurrence}/checkpoint/{:020}",
                    owner.workspace.id(), checkpoint.checked_sub(1).expect("real completed child has a predecessor"),
                )).unwrap()).await.unwrap().unwrap().record)
            } else { None };
            let mut altered = original.clone();
            if corrupt_depth {
                // The real child belongs directly to this root, so depth two
                // is a valid integer with false ancestry, not a range error.
                Arc::make_mut(&mut altered.value)["parent"]["depth"] = json!(2);
            } else {
                let Interaction::Input { sequence, .. } = staged.interactions[0] else { unreachable!() };
                let retained = Arc::make_mut(&mut altered.value)["checkpoint"]["staged"].as_array_mut().unwrap()
                    .iter_mut().find(|activation| activation["node"] == "compose").unwrap();
                // Change only the original sequence, preserving exact compact
                // membership and copied output in the real native row.
                retained["children"][0] = json!(sequence);
            }
            let mut reopened = None;
            let rejected = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(4), async {
                let mut write = owner.store.begin_transaction().await.unwrap();
                assert_eq!(write.load(original.key.clone()).await.unwrap().unwrap().record, original);
                write.put(altered).await.unwrap();
                if let Some(predecessor) = &predecessor {
                    assert_eq!(write.load(predecessor.key.clone()).await.unwrap().unwrap().record, *predecessor);
                    let mut altered = predecessor.clone();
                    Arc::make_mut(&mut altered.value)["parent"]["depth"] = json!(2);
                    write.put(altered).await.unwrap();
                }
                write.commit().await.unwrap();
                let projection = if corrupt_depth { None } else {
                    Some(matches!(parent.snapshot().await, Err(AppError::Integration(_))))
                };
                let config: AppConfig<StateStore, NativeAgent> =
                    AppConfig::new(owner.store.clone(), Vec::new(), definitions());
                let result = App::open(config.with_state_typing(owner.probe.clone())
                    .with_hooks(owner.probe.clone()).with_child_executor(owner.probe.clone())).await;
                let recovery = matches!(&result, Err(AppError::Integration(_)));
                reopened = result.ok();
                (projection, recovery)
            })).catch_unwind().await;
            // Reopen may already have fenced the old App. Restore the exact
            // real row, then drain an accepted or freshly reopened root before
            // reporting a failed oracle. No fabricated checkpoint survives.
            let mut write = owner.store.begin_transaction().await.unwrap();
            write.load(original.key.clone()).await.unwrap().unwrap();
            write.put(original).await.unwrap();
            if let Some(predecessor) = predecessor {
                write.load(predecessor.key.clone()).await.unwrap().unwrap();
                write.put(predecessor).await.unwrap();
            }
            write.commit().await.unwrap();
            let mut repaired = match reopened {
                Some(app) => app,
                None => {
                    let config: AppConfig<StateStore, NativeAgent> =
                        AppConfig::new(owner.store.clone(), Vec::new(), definitions());
                    App::open(config.with_state_typing(owner.probe.clone())
                        .with_hooks(owner.probe.clone()).with_child_executor(owner.probe.clone()))
                        .await.expect("restored real membership reopens")
                }
            };
            let closed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5),
                repaired.shutdown(Shutdown::CancelAndDrain { timeout: Duration::from_secs(3) }),
            )).catch_unwind().await;
            drop(repaired);
            let (projection, recovery) = match rejected {
                Ok(result) => result.expect("malformed membership observations are bounded"),
                Err(panic) => std::panic::resume_unwind(panic),
            };
            match closed {
                Ok(result) => result.expect("repaired membership cleanup is bounded").expect("cleanup joins"),
                Err(panic) => std::panic::resume_unwind(panic),
            }
            drop(idle);
            if let Some(projection) = projection {
                assert!(projection, "snapshot cannot project a staged child owned by another real control sequence");
            }
            assert!(recovery, "reopen cannot accept false child control or ancestry evidence");
            assert_eq!(owner.probe.registered_children.load(Ordering::SeqCst), 1);
            assert_eq!(owner.probe.executor_calls.load(Ordering::SeqCst), 1);
            break;
            }
            return;
        }
        for (child, checkpoint) in &mut completed {
            assert!(matches!(child.delete(*checkpoint).await, Err(AppError::Busy(_))),
                "an accepted child result remains a dependency until its parent barrier commits");
            assert!(!child.history(None, NonZeroUsize::new(1).unwrap()).await.unwrap().checkpoints.is_empty());
        }
        let Interaction::Input { sequence, generation, .. } = staged.interactions[0] else { unreachable!() };
        parent.resolve(sequence, generation, Resolution::Input(Value::Null)).await.unwrap();
        let committed = parent.wait().await.unwrap();
        assert!(matches!(committed.status, RunStatus::Succeeded));
        assert_eq!(committed.state.values()["collected"], json!(expected));
        assert_eq!(committed.state.values()["payloads"], if large_inputs { json!([payload,payload]) } else { json!([]) });
        for (mut child, checkpoint) in completed {
            if matches!(case, StagedHistoryCase::DeleteAfterReduction) {
                loop {
                    match child.snapshot().await {
                        Err(AppError::NotFound(_)) => break,
                        Ok(_) | Err(AppError::Closed(_)) => tokio::task::yield_now().await,
                        Err(error) => panic!("automatic child history deletion: {error}"),
                    }
                }
            } else {
                child.delete(checkpoint).await.unwrap();
            }
        }
        assert_eq!(owner.probe.registered_children.load(Ordering::SeqCst), expected.len());
        assert_eq!(owner.probe.executor_calls.load(Ordering::SeqCst), 1);
    })).catch_unwind().await;
        let closed = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(5),
            owner.app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        ))
        .catch_unwind()
        .await;
        drop(owner);
        match observed {
            Ok(result) => result.expect("staged child dependency proof is bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        match closed {
            Ok(result) => {
                let result = result.expect("staged dependency cleanup is bounded");
                if corrupt_membership {
                    assert!(
                        matches!(result, Ok(()) | Err(AppError::Closed(_))),
                        "reopened original owner joins: {result:?}"
                    );
                } else {
                    result.expect("cleanup joins");
                }
            }
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
}

#[tokio::test]
async fn staged_child_history_remains_owned_until_its_parent_wave_commits() {
    ChildComposition::staged_child_history(StagedHistoryCase::Child).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_dependency_metadata_does_not_recharge_admitted_state_as_staged_output() {
    ChildComposition::staged_child_history(StagedHistoryCase::LargeMap).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_staged_child_membership_rejects_projection_and_recovery() {
    ChildComposition::staged_child_history(StagedHistoryCase::CorruptOwnership).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_parent_wave_retains_every_child_despite_delete_after_reduction() {
    ChildComposition::history_policy(HistoryPolicyCase::ParentCancelled).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_parent_wave_retains_its_successful_and_cancelled_children() {
    ChildComposition::history_policy(HistoryPolicyCase::ChildCancelled).await;
}

#[test]
fn synchronous_pipeline_deletes_only_after_copying_its_final_state_and_child_results() {
    ChildComposition::run(PipelineCase::HistoryDeletion, ActionMode::Synchronous);
}

#[test]
fn failed_partial_pipeline_retains_its_admitted_child_histories() {
    ChildComposition::run(PipelineCase::FailedHistoryDeletion, ActionMode::Synchronous);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_child_history_deletion_waits_until_its_sibling_parent_control_settles() {
    ChildComposition::staged_child_history(StagedHistoryCase::DeleteAfterReduction).await;
}

#[test]
fn authored_child_selectors_task_keys_and_initializers_parse_in_all_child_forms() {
    for kind in [StaticChild::Child, StaticChild::Map, StaticChild::Pipeline] {
        let mut document = ChildComposition::history_document(kind, json!("retain"));
        let step = &mut document["nodes"][0]["step"];
        let child = if matches!(kind, StaticChild::Pipeline) {
            &mut step["children"][0]
        } else {
            step
        };
        child.as_object_mut().unwrap().remove("workflow");
        child["select"] =
            json!({"kind":"literal","value":{"workflow":"history-leaf","revision":"r1"}});
        child["task_key"] = json!({"kind":"literal","value":"same logical child"});
        child["initializer"] = json!({
            "source":{"kind":"literal","value":"run_00000000-0000-4000-8000-000000000000"},
            "occurrence":{"kind":"literal","value":"00000000000040008000000000000000"},
            "checkpoint":{"kind":"action","value":0},"position":"at_saved",
            "overrides":{"count":{"kind":"literal","value":9}},
            "fallback":{"kind":"literal","value":{"count":2}}
        });
        WorkflowDefinition::parse(&document.to_string())
            .expect("the complete authored child selection contract parses");
    }
}

#[test]
fn authored_child_selection_rejects_ambiguous_sources_and_malformed_expression_maps() {
    for kind in [StaticChild::Child, StaticChild::Map, StaticChild::Pipeline] {
        let original = ChildComposition::history_document(kind, json!("retain"));
        #[derive(Debug)]
        enum InvalidChildSyntax {
            AmbiguousSource,
            MissingSource,
            RawTaskKey,
            NonMapOverrides,
            RawOccurrence,
        }
        for invalid in [
            InvalidChildSyntax::AmbiguousSource,
            InvalidChildSyntax::MissingSource,
            InvalidChildSyntax::RawTaskKey,
            InvalidChildSyntax::NonMapOverrides,
            InvalidChildSyntax::RawOccurrence,
        ] {
            let mut document = original.clone();
            let step = &mut document["nodes"][0]["step"];
            let child = if matches!(kind, StaticChild::Pipeline) {
                &mut step["children"][0]
            } else {
                step
            };
            match invalid {
                InvalidChildSyntax::AmbiguousSource => {
                    child["select"] = json!({"kind":"literal","value":{"workflow":"history-leaf"}});
                }
                InvalidChildSyntax::MissingSource => {
                    child.as_object_mut().unwrap().remove("workflow");
                }
                InvalidChildSyntax::RawTaskKey => {
                    child["task_key"] = json!("this must be an expression");
                }
                InvalidChildSyntax::NonMapOverrides => {
                    child["initializer"] = json!({
                        "source":{"kind":"literal","value":"run_00000000-0000-4000-8000-000000000000"},
                        "checkpoint":{"kind":"latest"},"position":"entrypoint",
                        "overrides":{"kind":"literal","value":{"count":9}}
                    });
                }
                InvalidChildSyntax::RawOccurrence => {
                    child["initializer"] = json!({
                        "source":{"kind":"literal","value":"run_00000000-0000-4000-8000-000000000000"},
                        "occurrence":"this must be an expression",
                        "checkpoint":{"kind":"latest"},"position":"entrypoint"
                    });
                }
            }
            assert!(
                matches!(
                    WorkflowDefinition::parse(&document.to_string()),
                    Err(AppError::InvalidInput(_))
                ),
                "ambiguous/malformed child declaration case {invalid:?} must reject"
            );
        }
    }
}

#[test]
fn tasks_mode_has_one_source_and_defers_resolved_data_validation() {
    let mut document = ChildComposition::history_document(StaticChild::Map, json!("retain"));
    document["nodes"][0]["step"] = json!({"kind":"map","max_parallel":2,
        "tasks":{"kind":"literal","value":[{"this":"is invalid resolved task data"}]},
        "reductions":[{"source":"count","target":"collected","reducer":"collect"}]
    });
    WorkflowDefinition::parse(&document.to_string())
        .expect("tasks expression syntax parses before its resolved data is validated");
    for (field, value) in [
        ("workflow", json!("history-leaf")),
        (
            "select",
            json!({"kind":"literal","value":{"workflow":"history-leaf"}}),
        ),
        ("items", json!({"kind":"literal","value":[]})),
        ("input_key", json!("count")),
        ("index_key", json!("index")),
        ("input", json!({})),
        ("inputs", json!({})),
        ("task_key", json!({"kind":"literal","value":"key"})),
        ("initializer", json!({})),
        ("history", json!("retain")),
    ] {
        let mut invalid = document.clone();
        invalid["nodes"][0]["step"][field] = value;
        assert!(
            matches!(
                WorkflowDefinition::parse(&invalid.to_string()),
                Err(AppError::InvalidInput(_))
            ),
            "tasks mode cannot retain an ignored node-level {field}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_depth_recovery_rejects_an_in_range_value_inconsistent_with_its_real_parent() {
    ChildComposition::staged_child_history(StagedHistoryCase::CorruptChildDepth).await;
}

enum RecursiveDepthCase {
    Leaf(u64),
    EmptyMapAtLimit,
}

impl ChildComposition {
    async fn recursive_child_depth(case: RecursiveDepthCase) {
        let (target, leaf) = match case {
            RecursiveDepthCase::Leaf(target) => (
                target,
                json!({"kind":"state","mutations":[
                    {"kind":"increment","target":"result","value":{"kind":"literal","value":1}}
                ]}),
            ),
            RecursiveDepthCase::EmptyMapAtLimit => (
                64,
                json!({
                    "kind":"map","select":{"kind":"literal","value":{"workflow":"recursive-child"}},
                    "items":{"kind":"literal","value":[]},"input_key":"count","input":{},
                    "max_parallel":1,"reductions":[]
                }),
            ),
        };
        let definition = WorkflowDefinition::parse(&json!({
            "schema":"app-sdk.workflow/v2", "id":"recursive-child", "revision":"r1",
            "state":{"count":0,"target":target,"result":0}, "required":["count","target","result"],
            "reducers":{"count":"reject_concurrent","target":"reject_concurrent","result":"reject_concurrent"},
            "entry":"decide", "nodes":[
                {"id":"decide","step":{"kind":"state","mutations":[]}},
                {"id":"descend","deadline_ms":60000,"step":{
                    "kind":"child","select":{"kind":"literal","value":{"workflow":"recursive-child"}},
                    "input":{
                        "count":{"kind":"add","left":{"kind":"state","path":"count"},"right":{"kind":"literal","value":1}},
                        "target":{"kind":"state","path":"target"}
                    },"reductions":[{"source":"result","target":"result","reducer":"reject_concurrent"}]
                }},
                {"id":"leaf","step":leaf}
            ], "routes":{"decide":{"mode":"first","branches":[
                {"when":{"kind":"less_than","left":{"kind":"state","path":"count"},"right":{"kind":"state","path":"target"}},
                    "target":{"kind":"node","id":"descend"}},
                {"target":{"kind":"node","id":"leaf"}}
            ]}}
        }).to_string()).expect("dynamic recursive selection is valid graph syntax");
        let mut owner = Self::open(vec![definition]).await;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(70), async {
            let workflow = owner
                .workspace
                .workflow("recursive-child".parse().unwrap())
                .await
                .unwrap();
            let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
            let completed = run
                .execution(ExecutionOptions::default())
                .await
                .unwrap()
                .finish()
                .await
                .unwrap();
            let admitted = target.min(64) as usize;
            let rows = workflow
                .list_runs(None, NonZeroUsize::new(128).unwrap())
                .await
                .unwrap();
            assert_eq!(rows.len(), admitted + 1);
            let mut counts = rows
                .iter()
                .map(|row| row.state.values()["count"].as_u64().unwrap())
                .collect::<Vec<_>>();
            counts.sort_unstable();
            assert_eq!(
                counts,
                (0..=admitted as u64).collect::<Vec<_>>(),
                "no occurrence beyond the supported depth is admitted"
            );
            assert_eq!(
                owner.probe.registered_children.load(Ordering::SeqCst),
                admitted
            );
            let empty_map = matches!(case, RecursiveDepthCase::EmptyMapAtLimit);
            assert_eq!(
                owner.probe.executor_calls.load(Ordering::SeqCst),
                admitted + usize::from(empty_map),
                "each real Child and the terminal empty Map use the original executor once"
            );
            let leaf_steps = owner
                .probe
                .steps
                .lock()
                .unwrap()
                .iter()
                .filter(|(node, _, _)| node == "leaf")
                .count();
            if target <= 64 {
                assert!(matches!(completed.status, RunStatus::Succeeded));
                assert!(
                    rows.iter()
                        .all(|row| matches!(row.status, RunStatus::Succeeded))
                );
                assert_eq!(
                    completed.state.values()["result"],
                    if empty_map { json!(0) } else { json!(1) }
                );
                assert_eq!(
                    leaf_steps, 1,
                    "the deepest leaf action runs once, including an empty Map"
                );
            } else {
                assert!(matches!(
                    completed.status,
                    RunStatus::Failed(WorkflowFailure::Child(_))
                ));
                let deepest = rows
                    .iter()
                    .find(|row| row.state.values()["count"] == json!(64))
                    .unwrap();
                assert!(matches!(
                    deepest.status,
                    RunStatus::Failed(WorkflowFailure::Limit(_))
                ));
                assert!(
                    rows.iter()
                        .all(|row| matches!(row.status, RunStatus::Failed(_)))
                );
                assert_eq!(completed.state.values()["result"], 0);
                assert_eq!(completed.last_action_ordinal, Some(0));
                assert_eq!(completed.outcomes.len(), 1);
                assert_eq!(completed.outcomes[0].node, "decide");
                assert_eq!(
                    leaf_steps, 0,
                    "rejected depth cannot start an unadmitted leaf effect"
                );
            }
        }))
        .catch_unwind()
        .await;
        let cleanup = tokio::time::timeout(
            Duration::from_secs(5),
            owner.app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        )
        .await;
        drop(owner);
        match checked {
            Ok(result) => result.expect("recursive child execution is bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup.expect("recursive child cleanup joins").unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dynamic_child_recursion_terminates_without_rejecting_repeated_workflow_identity() {
    ChildComposition::recursive_child_depth(RecursiveDepthCase::Leaf(3)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_depth_sixty_three_succeeds() {
    ChildComposition::recursive_child_depth(RecursiveDepthCase::Leaf(63)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_depth_sixty_four_succeeds() {
    ChildComposition::recursive_child_depth(RecursiveDepthCase::Leaf(64)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_depth_sixty_five_rejects_before_registration_or_leaf_effects() {
    ChildComposition::recursive_child_depth(RecursiveDepthCase::Leaf(65)).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_map_at_child_depth_sixty_four_succeeds_without_admitting_a_deeper_child() {
    ChildComposition::recursive_child_depth(RecursiveDepthCase::EmptyMapAtLimit).await;
}

#[derive(Clone, Copy)]
enum InitializerComposition {
    Child,
    Items,
    Tasks,
    Pipeline,
}

#[derive(Debug)]
enum InitializerCase {
    LoadedIgnoresDefaultInput,
    TargetAfterCompleted,
    TargetAtSaved,
    TargetEntrypoint,
    TargetRenamedEntrypoint,
    TargetMissingPosition,
    AfterCompleted,
    AtSaved,
    Entrypoint,
    MissingSource,
    MissingSourceFallback,
    MissingCheckpoint,
    MissingCheckpointFallback,
}

impl ChildComposition {
    fn child_initializer(kind: InitializerComposition) {
        for case in [
            InitializerCase::LoadedIgnoresDefaultInput,
            InitializerCase::TargetAfterCompleted,
            InitializerCase::TargetAtSaved,
            InitializerCase::TargetEntrypoint,
            InitializerCase::TargetRenamedEntrypoint,
            InitializerCase::TargetMissingPosition,
            InitializerCase::AfterCompleted,
            InitializerCase::AtSaved,
            InitializerCase::Entrypoint,
            InitializerCase::MissingSource,
            InitializerCase::MissingSourceFallback,
            InitializerCase::MissingCheckpoint,
            InitializerCase::MissingCheckpointFallback,
        ] {
            let new_target = matches!(
                case,
                InitializerCase::TargetAfterCompleted
                    | InitializerCase::TargetAtSaved
                    | InitializerCase::TargetEntrypoint
                    | InitializerCase::TargetRenamedEntrypoint
                    | InitializerCase::TargetMissingPosition
            );
            let missing_source = matches!(
                case,
                InitializerCase::MissingSource | InitializerCase::MissingSourceFallback
            );
            let missing_checkpoint = matches!(
                case,
                InitializerCase::MissingCheckpoint | InitializerCase::MissingCheckpointFallback
            );
            let fallback = matches!(
                case,
                InitializerCase::MissingSourceFallback | InitializerCase::MissingCheckpointFallback
            );
            let position = match case {
                InitializerCase::AtSaved
                | InitializerCase::TargetAtSaved
                | InitializerCase::TargetMissingPosition => "at_saved",
                InitializerCase::Entrypoint
                | InitializerCase::TargetEntrypoint
                | InitializerCase::TargetRenamedEntrypoint => "entrypoint",
                _ => "after_completed",
            };
            let mut initializer = json!({
                "source":{"kind":"state","path":"source"},
                "checkpoint":{"kind":"action","value":if missing_checkpoint {999} else {1}},
                "position":position,"overrides":{"count":{"kind":"literal","value":40}}
            });
            if fallback {
                initializer["fallback"] = json!({"kind":"literal","value":{"count":7}});
            }
            let pipeline_default = matches!(kind, InitializerComposition::Pipeline)
                && (missing_source || missing_checkpoint)
                && !fallback;
            let default_input = if matches!(case, InitializerCase::LoadedIgnoresDefaultInput) {
                Value::Null
            } else {
                json!(100)
            };
            let input = if pipeline_default {
                json!({})
            } else {
                json!({"count":{"kind":"literal","value":default_input}})
            };
            let mut child =
                json!({"workflow":"initializer-leaf","input":input,"initializer":initializer});
            if new_target {
                child["revision"] = json!("r2");
            }
            let reductions = json!([{"source":"count","target":"collected","reducer":"collect"}]);
            let mut step = match kind {
                InitializerComposition::Child => {
                    let mut step = child;
                    step["kind"] = json!("child");
                    step["reductions"] = reductions;
                    step
                }
                InitializerComposition::Items => json!({"kind":"map","workflow":"initializer-leaf",
                    "items":{"kind":"literal","value":[default_input]},"input_key":"count","max_parallel":1,
                    "initializer":initializer,"reductions":reductions}),
                InitializerComposition::Tasks => {
                    json!({"kind":"map","tasks":{"kind":"state","path":"tasks"},
                    "max_parallel":1,"reductions":reductions})
                }
                InitializerComposition::Pipeline => json!({"kind":"pipeline","children":[
                    {"workflow":"initializer-leaf","input":{"count":{"kind":"literal","value":10}}}, child
                ],"reductions":reductions}),
            };
            if new_target && matches!(kind, InitializerComposition::Items) {
                step["revision"] = json!("r2");
            }
            let parent = WorkflowDefinition::parse(&json!({
                "schema":"app-sdk.workflow/v2","id":"initializer-parent","revision":"r1",
                "state":{"count":0,"source":"","tasks":[],"collected":[]},"required":[],
                "reducers":{"count":"reject_concurrent","source":"reject_concurrent","tasks":"reject_concurrent","collected":"collect"},
                "entry":"compose","nodes":[{"id":"compose","deadline_ms":10000,"step":step}],"routes":{}
            }).to_string()).expect("adopted initializer syntax must parse");
            let leaf_document = json!({
                "schema":"app-sdk.workflow/v2","id":"initializer-leaf","revision":"r1",
                "state":{"count":0},"required":["count"],"reducers":{"count":"reject_concurrent"},
                "entry":"first","nodes":[
                    {"id":"first","step":{"kind":"state","mutations":[{"kind":"increment","target":"count","value":{"kind":"literal","value":1}}]}},
                    {"id":"second","step":{"kind":"state","mutations":[{"kind":"increment","target":"count","value":{"kind":"literal","value":10}}]}},
                    {"id":"third","step":{"kind":"state","mutations":[{"kind":"increment","target":"count","value":{"kind":"literal","value":100}}]}}
                ],"routes":{
                    "first":{"mode":"first","branches":[{"target":{"kind":"node","id":"second"}}]},
                    "second":{"mode":"first","branches":[{"target":{"kind":"node","id":"third"}}]}
                }
            });
            let leaf = WorkflowDefinition::parse(&leaf_document.to_string()).unwrap();
            let mut definitions = vec![parent, leaf];
            if new_target {
                let mut target = leaf_document;
                target["revision"] = json!("r2");
                target["nodes"][2]["step"]["mutations"][0]["value"]["value"] = json!(1000);
                target["nodes"].as_array_mut().unwrap().push(json!({"id":"finish","step":{"kind":"state","mutations":[{"kind":"increment","target":"count","value":{"kind":"literal","value":2000}}]}}));
                target["routes"]["first"]["branches"][0]["target"]["id"] = json!("third");
                target["routes"]["third"] =
                    json!({"mode":"first","branches":[{"target":{"kind":"node","id":"second"}}]});
                target["routes"]["second"]["branches"][0]["target"]["id"] = json!("finish");
                if matches!(case, InitializerCase::TargetMissingPosition) {
                    target["nodes"][1]["id"] = json!("replacement");
                    target["routes"]["third"]["branches"][0]["target"]["id"] = json!("replacement");
                    let route = target["routes"]
                        .as_object_mut()
                        .unwrap()
                        .remove("second")
                        .unwrap();
                    target["routes"]["replacement"] = route;
                }
                if matches!(case, InitializerCase::TargetRenamedEntrypoint) {
                    target["entry"] = json!("target-first");
                    for node in target["nodes"].as_array_mut().unwrap() {
                        node["id"] = json!(format!("target-{}", node["id"].as_str().unwrap()));
                    }
                    let routes = std::mem::take(target["routes"].as_object_mut().unwrap());
                    for (node, mut route) in routes {
                        for branch in route["branches"].as_array_mut().unwrap() {
                            let target_id = branch["target"]["id"].as_str().unwrap();
                            branch["target"]["id"] = json!(format!("target-{target_id}"));
                        }
                        target["routes"][format!("target-{node}")] = route;
                    }
                }
                definitions.insert(1, WorkflowDefinition::parse(&target.to_string()).unwrap());
            }
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let mut owner = runtime.block_on(Self::open(definitions));
            let checked = std::panic::catch_unwind(AssertUnwindSafe(|| {
                let leaf = runtime
                    .block_on(
                        owner
                            .workspace
                            .workflow("initializer-leaf".parse().unwrap()),
                    )
                    .unwrap();
                let mut source = runtime
                    .block_on(
                        leaf.prepare(
                            WorkflowState::new(serde_json::Map::from_iter([(
                                "count".into(),
                                json!(1),
                            )]))
                            .unwrap(),
                        ),
                    )
                    .unwrap();
                let original = SyncExecution::open(&mut source, ExecutionOptions::default())
                    .unwrap()
                    .finish()
                    .unwrap();
                assert_eq!(original.state.values()["count"], 112);
                let selected = runtime
                    .block_on(source.checkpoint(CheckpointSelector::Action(1)))
                    .unwrap();
                assert_eq!(selected.state.values()["count"], 12);
                let source_id = if missing_source {
                    "run_00000000-0000-4000-8000-000000000000".to_owned()
                } else {
                    source.id().to_string()
                };
                let mut raw_initializer = json!({"source":source_id,"checkpoint":initializer["checkpoint"],
                    "position":position,"overrides":{"count":40}});
                if fallback {
                    raw_initializer["fallback"] = json!({"count":7});
                }
                let mut task = json!({"task_key":"loaded","workflow":"initializer-leaf","input":{"count":default_input},"initializer":raw_initializer});
                if new_target {
                    task["revision"] = json!("r2");
                }
                let workflow = runtime
                    .block_on(
                        owner
                            .workspace
                            .workflow("initializer-parent".parse().unwrap()),
                    )
                    .unwrap();
                let mut run = runtime
                    .block_on(
                        workflow.prepare(
                            WorkflowState::new(serde_json::Map::from_iter([
                                ("source".into(), json!(source_id)),
                                ("tasks".into(), json!([task])),
                            ]))
                            .unwrap(),
                        ),
                    )
                    .unwrap();
                let finished = SyncExecution::open(&mut run, ExecutionOptions::default())
                    .unwrap()
                    .finish();
                if matches!(case, InitializerCase::TargetMissingPosition) {
                    assert!(
                        matches!(&finished, Err(AppError::InvalidInput(_))),
                        "incompatible saved position must preserve the original admission rejection"
                    );
                    let observed = runtime.block_on(async {
                        tokio::time::timeout(Duration::from_secs(5), run.wait())
                            .await
                            .expect("rejected child admission must settle within the cleanup bound")
                            .expect("rejected child admission must publish its parent failure")
                    });
                    assert!(
                        matches!(observed.status, RunStatus::Failed(_)),
                        "incompatible saved position must fail before target admission"
                    );
                    assert_eq!(observed.state.values()["collected"], json!([]));
                    assert!(observed.outcomes.is_empty());
                    assert_eq!(observed.last_action_ordinal, None);
                    let predecessor = usize::from(matches!(kind, InitializerComposition::Pipeline));
                    assert_eq!(
                        owner.probe.registered_children.load(Ordering::SeqCst),
                        predecessor
                    );
                    assert_eq!(owner.probe.executor_calls.load(Ordering::SeqCst), 0);
                    let rows = runtime
                        .block_on(leaf.list_runs(None, NonZeroUsize::new(8).unwrap()))
                        .unwrap();
                    assert_eq!(
                        rows.len(),
                        predecessor + 1,
                        "no incompatible target occurrence was admitted"
                    );
                    let saved = runtime
                        .block_on(source.checkpoint(CheckpointSelector::Action(1)))
                        .unwrap();
                    assert_eq!(saved.sequence, selected.sequence);
                    assert_eq!(saved.state, selected.state);
                    assert!(saved.outcomes == selected.outcomes);
                    let after = runtime.block_on(source.snapshot()).unwrap();
                    assert_eq!(after.state, original.state);
                    assert_eq!(after.checkpoint, original.checkpoint);
                    assert!(after.outcomes == original.outcomes);
                    return;
                }
                let observed = finished.unwrap();
                let expected = match case {
                    InitializerCase::TargetAfterCompleted => 2040,
                    InitializerCase::TargetAtSaved => 2050,
                    InitializerCase::TargetEntrypoint
                    | InitializerCase::TargetRenamedEntrypoint => 3051,
                    InitializerCase::AfterCompleted
                    | InitializerCase::LoadedIgnoresDefaultInput => 140,
                    InitializerCase::AtSaved => 150,
                    InitializerCase::Entrypoint => 151,
                    _ if fallback => 118,
                    _ if pipeline_default => 232,
                    _ => 211,
                };
                let counts = if matches!(kind, InitializerComposition::Pipeline) {
                    vec![121, expected]
                } else {
                    vec![expected]
                };
                assert!(
                    matches!(observed.status, RunStatus::Succeeded),
                    "{case:?}: initializer composition must succeed"
                );
                assert_eq!(
                    observed.state.values()["collected"],
                    json!(counts),
                    "{case:?}"
                );
                assert_eq!(observed.last_action_ordinal, Some(0));
                assert_eq!(observed.outcomes.len(), 1);
                assert_eq!(observed.outcomes[0].node, "compose");
                if matches!(kind, InitializerComposition::Pipeline) {
                    assert_eq!(
                        Value::Object(observed.outcomes[0].result.clone()),
                        json!({
                            "child_states":counts.iter().map(|count|json!({"count":count})).collect::<Vec<_>>(),
                            "final_state":{"count":expected}
                        })
                    );
                }
                assert_eq!(
                    owner.probe.registered_children.load(Ordering::SeqCst),
                    counts.len()
                );
                assert_eq!(
                    owner.probe.executor_calls.load(Ordering::SeqCst),
                    usize::from(!matches!(kind, InitializerComposition::Pipeline))
                );
                let rows = runtime
                    .block_on(leaf.list_runs(None, NonZeroUsize::new(8).unwrap()))
                    .unwrap();
                assert_eq!(
                    rows.len(),
                    counts.len() + 1,
                    "one fresh child per original task; source not reused"
                );
                let after = runtime.block_on(source.snapshot()).unwrap();
                assert_eq!(after.checkpoint, original.checkpoint);
                assert_eq!(after.state, original.state);
                assert_eq!(after.last_action_ordinal, original.last_action_ordinal);
                assert!(after.outcomes == original.outcomes);
                let saved = runtime
                    .block_on(source.checkpoint(CheckpointSelector::Action(1)))
                    .unwrap();
                assert_eq!(saved.sequence, selected.sequence);
                assert_eq!(saved.state, selected.state);
                assert!(saved.outcomes == selected.outcomes);
            }));
            let cleanup = runtime.block_on(async {
                tokio::time::timeout(
                    Duration::from_secs(5),
                    owner.app.shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_secs(3),
                    }),
                )
                .await
            });
            drop(owner);
            if let Err(panic) = checked {
                std::panic::resume_unwind(panic);
            }
            cleanup
                .expect("initializer cleanup is bounded")
                .expect("initializer cleanup joins");
        }
    }
}

#[test]
fn child_initializer_restores_positions_and_missing_state_precedence() {
    ChildComposition::child_initializer(InitializerComposition::Child);
}

#[test]
fn items_map_initializer_restores_positions_and_missing_state_precedence() {
    ChildComposition::child_initializer(InitializerComposition::Items);
}

#[test]
fn resolved_tasks_initializer_restores_positions_and_missing_state_precedence() {
    ChildComposition::child_initializer(InitializerComposition::Tasks);
}

#[test]
fn pipeline_initializer_replaces_handoff_only_when_saved_state_exists() {
    ChildComposition::child_initializer(InitializerComposition::Pipeline);
}

#[derive(Clone, Copy)]
enum EqualKeyCase {
    Cancel,
    ExactControls,
    DeleteLatest,
    SavedOrder,
    FreshAdmission,
}

impl ChildComposition {
    async fn equal_key_occurrences(case: EqualKeyCase) {
        let (nodes, routes, reducer) = if matches!(case, EqualKeyCase::FreshAdmission) {
            (
                json!([
                    {"id":"compose","deadline_ms":10000,"step":{"kind":"child","workflow":"equal-key-leaf",
                        "task_key":{"kind":"literal","value":"same"},"input":{"count":{"kind":"literal","value":10}},
                        "reductions":[{"source":"completed","target":"collected","reducer":"append_array"}]}},
                    {"id":"second","deadline_ms":10000,"step":{"kind":"child","workflow":"equal-key-leaf",
                        "task_key":{"kind":"literal","value":"same"},"input":{"count":{"kind":"literal","value":20}},
                        "reductions":[{"source":"completed","target":"collected","reducer":"append_array"}]}}
                ]),
                json!({"compose":{"mode":"first","branches":[{"target":{"kind":"node","id":"second"}}]}}),
                "append_array",
            )
        } else {
            (
                json!([{"id":"compose","deadline_ms":10000,"step":{"kind":"map","max_parallel":2,
                "tasks":{"kind":"literal","value":[
                    {"task_key":"same","workflow":"equal-key-leaf","input":{"count":10}},
                    {"task_key":"same","workflow":"equal-key-leaf","input":{"count":20}}
                ]},"reductions":[{"source":"count","target":"collected","reducer":"collect"}]}}]),
                json!({}),
                "collect",
            )
        };
        let parent = WorkflowDefinition::parse(
            &json!({
                "schema":"app-sdk.workflow/v2","id":"equal-key-parent","revision":"r1",
                "state":{"count":0,"collected":[]},"required":[],
                "reducers":{"count":"reject_concurrent","collected":reducer},"entry":"compose",
                "nodes":nodes,"routes":routes
            })
            .to_string(),
        )
        .expect("equal task keys are valid resolved task input");
        let mut leaf = json!({
        "schema":"app-sdk.workflow/v2","id":"equal-key-leaf","revision":"r1",
        "state":{"count":0},"required":["count"],"reducers":{"count":"reject_concurrent"},"entry":"input",
        "nodes":[
            {"id":"input","deadline_ms":10000,"step":{"kind":"input","message":[{"kind":"text","text":"Each task owns its input"}]}},
            {"id":"effect","step":{"kind":"state","mutations":[{"kind":"increment","target":"count","value":{"kind":"literal","value":1}}]}}
        ],"routes":{"input":{"mode":"first","branches":[{"target":{"kind":"node","id":"effect"}}]}}
        });
        if matches!(case, EqualKeyCase::FreshAdmission) {
            // These Child nodes commit separate waves. Collect replaces a wave's
            // contributions; append_array retains both completed occurrences.
            leaf["state"]["completed"] = json!([]);
            leaf["reducers"]["completed"] = json!("reject_concurrent");
            leaf["nodes"][1]["step"]["mutations"]
                .as_array_mut()
                .unwrap()
                .push(json!({
                    "kind":"append","target":"completed","value":{"kind":"state","path":"count"}
                }));
        }
        let leaf = WorkflowDefinition::parse(&leaf.to_string()).unwrap();
        let mut owner = ChildComposition::open(vec![parent, leaf]).await;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(8), async {
            let workflow = owner
                .workspace
                .workflow("equal-key-parent".parse().unwrap())
                .await
                .unwrap();
            if matches!(case, EqualKeyCase::FreshAdmission) {
                *owner.probe.hold.lock().unwrap() = Some(CompositionHold::SecondRegistration);
            }
            let parent = workflow.start(ExecutionOptions::default()).await.unwrap();
            let expected_child = format!("{:x}", Sha256::digest(format!("{}:same", parent.id()).as_bytes()));
            let leaf = owner
                .workspace
                .workflow("equal-key-leaf".parse().unwrap())
                .await
                .unwrap();
            if matches!(case, EqualKeyCase::FreshAdmission) {
                let first_pending = tokio::time::timeout(Duration::from_secs(2), async {
                    loop {
                        let rows = leaf.list_runs(None, NonZeroUsize::new(3).unwrap()).await.unwrap();
                        if rows.len() == 1 && matches!(rows[0].interactions.as_slice(), [Interaction::Input { .. }]) {
                            break rows.into_iter().next().unwrap();
                        }
                        tokio::task::yield_now().await;
                    }
                }).await.expect("the first Child occurrence owns its real Input");
                let first = leaf.open_run(RunSelector::Exact {
                    id: first_pending.id.clone(), occurrence: first_pending.occurrence.clone(),
                }).await.unwrap();
                let Interaction::Input { sequence, generation, .. } = first_pending.interactions[0] else { unreachable!() };
                first.resolve(sequence, generation, Resolution::Input(Value::Null)).await.unwrap();
                tokio::time::timeout(Duration::from_secs(2), owner.probe.hold_entered.notified()).await
                    .expect("the next equal-key occurrence is durably admitted before its original registration returns");
                let first_done = first.wait().await.unwrap();
                assert!(matches!(first_done.status, RunStatus::Succeeded));
                assert_eq!(first_done.state.values()["completed"], json!([11]));
                assert_eq!(parent.snapshot().await.unwrap().state.values()["collected"], json!([11]),
                    "the first parent wave is committed before the second registration returns");
                let first_action = first.checkpoint(CheckpointSelector::Action(1)).await.unwrap();
                let mut rows = leaf.list_runs(None, NonZeroUsize::new(3).unwrap()).await.unwrap();
                assert_eq!(rows.len(), 2);
                rows.sort_by_key(|row| row.state.values()["count"].as_i64().unwrap());
                assert_eq!(rows[0].id, rows[1].id);
                assert_eq!(rows[0].id.to_string(), expected_child);
                assert_eq!(rows[0].occurrence, first_pending.occurrence);
                let fresh = &rows[1];
                assert_eq!(fresh.checkpoint, 0);
                assert_eq!(fresh.state.values()["count"], 20);
                assert!(fresh.interactions.is_empty() && fresh.outcomes.is_empty() && fresh.last_action_ordinal.is_none());
                let mut observing = leaf.open_run(RunSelector::Latest(fresh.id.clone())).await.unwrap();
                let latest = observing.snapshot().await.unwrap();
                assert_eq!(latest.occurrence, fresh.occurrence);
                assert!(matches!(observing.execution(ExecutionOptions::default()).await,
                    Err(AppError::Busy(_))), "observation cannot take the original registration's execution owner");
                let after_rejected_execution = observing.snapshot().await.unwrap();
                assert_eq!(after_rejected_execution.checkpoint, latest.checkpoint);
                assert_eq!(after_rejected_execution.state, latest.state);
                assert!(after_rejected_execution.interactions.is_empty());
                assert_eq!(after_rejected_execution.last_action_ordinal, latest.last_action_ordinal);
                assert!(after_rejected_execution.outcomes == latest.outcomes);
                let copied = leaf.restore(app_sdk::workflow::RestoreOptions {
                    source: RunSelector::Latest(fresh.id.clone()), checkpoint: CheckpointSelector::Latest,
                    overrides: WorkflowState::default(), position: app_sdk::workflow::ResumePosition::AfterCompleted,
                    fallback: None,
                }).await.unwrap().checkpoint(CheckpointSelector::Sequence(0)).await.unwrap();
                assert_eq!(copied.state.values()["count"], 11,
                    "fresh B checkpoint zero cannot hide A's actual saved action");
                let lineage = copied.lineage.as_ref().unwrap();
                assert_eq!(lineage.source_occurrence, first_pending.occurrence);
                assert_eq!(lineage.checkpoint, first_action.sequence);
                assert!(matches!(leaf.restore(app_sdk::workflow::RestoreOptions {
                    source: RunSelector::Exact { id: fresh.id.clone(), occurrence: fresh.occurrence.clone() },
                    checkpoint: CheckpointSelector::Latest, overrides: WorkflowState::default(),
                    position: app_sdk::workflow::ResumePosition::AfterCompleted, fallback: None,
                }).await, Err(AppError::NotFound(_))), "Exact B has no saved action yet");
                let second = leaf.open_run(RunSelector::Exact {
                    id: fresh.id.clone(), occurrence: fresh.occurrence.clone(),
                }).await.unwrap();
                owner.probe.hold_release.notify_one();
                let pending = tokio::time::timeout(Duration::from_secs(2), async {
                    loop {
                        let snapshot = second.snapshot().await.unwrap();
                        if matches!(snapshot.interactions.as_slice(), [Interaction::Input { .. }]) { break snapshot; }
                        tokio::task::yield_now().await;
                    }
                }).await.expect("the original second child proceeds after its registration receipt");
                let Interaction::Input { sequence, generation, .. } = pending.interactions[0] else { unreachable!() };
                second.resolve(sequence, generation, Resolution::Input(Value::Null)).await.unwrap();
                let committed = parent.wait().await.unwrap();
                assert!(matches!(committed.status, RunStatus::Succeeded));
                assert_eq!(committed.state.values()["collected"], json!([11, 21]));
                assert_eq!(first.snapshot().await.unwrap().checkpoint, first_done.checkpoint);
                assert_eq!(owner.probe.registered_children.load(Ordering::SeqCst), 2);
                assert_eq!(owner.probe.executor_calls.load(Ordering::SeqCst), 2);
                assert_eq!(owner.probe.steps.lock().unwrap().iter().filter(|(node, _, _)| node == "effect").count(), 2);
                return;
            }
            let mut children = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let rows = leaf
                        .list_runs(None, NonZeroUsize::new(4).unwrap())
                        .await
                        .unwrap();
                    if rows.len() == 2
                        && rows.iter().all(|row| {
                            matches!(row.interactions.as_slice(), [Interaction::Input { .. }])
                        })
                    {
                        break rows;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect(
                "both equal-key occurrences must reach their own Input before either is resolved",
            );
            assert_eq!(
                children[0].id, children[1].id,
                "input/index do not salt the stable task identity"
            );
            assert_eq!(children[0].id.to_string(), expected_child);
            let mut counts = children
                .iter()
                .map(|row| row.state.values()["count"].as_i64().unwrap())
                .collect::<Vec<_>>();
            counts.sort_unstable();
            assert_eq!(counts, [10, 20]);
            assert!(
                children
                    .iter()
                    .all(|row| row.last_action_ordinal.is_none() && row.outcomes.is_empty())
            );
            assert_eq!(owner.probe.registered_children.load(Ordering::SeqCst), 2);
            assert_eq!(owner.probe.executor_calls.load(Ordering::SeqCst), 1);
            let before = parent.snapshot().await.unwrap();
            assert_eq!(before.state.values()["collected"], json!([]));
            assert!(before.last_action_ordinal.is_none() && before.outcomes.is_empty());
            if matches!(case, EqualKeyCase::SavedOrder) {
                children.sort_by_key(|child| child.state.values()["count"].as_i64().unwrap());
                let first = leaf.open_run(RunSelector::Exact {
                    id: children[0].id.clone(), occurrence: children[0].occurrence.clone(),
                }).await.unwrap();
                let second = leaf.open_run(RunSelector::Exact {
                    id: children[1].id.clone(), occurrence: children[1].occurrence.clone(),
                }).await.unwrap();
                *owner.probe.hold.lock().unwrap() = Some(CompositionHold::SavedCount(11));
                let Interaction::Input { sequence, generation, .. } = children[0].interactions[0] else { unreachable!() };
                first.resolve(sequence, generation, Resolution::Input(Value::Null)).await.unwrap();
                tokio::time::timeout(Duration::from_secs(2), owner.probe.hold_entered.notified()).await
                    .expect("A's actual saved action reaches its original post-hook");
                let first_action = first.checkpoint(CheckpointSelector::Action(1)).await.unwrap();
                assert_eq!(first_action.state.values()["count"], 11);
                let Interaction::Input { sequence, generation, .. } = children[1].interactions[0] else { unreachable!() };
                second.resolve(sequence, generation, Resolution::Input(Value::Null)).await.unwrap();
                let second_done = second.wait().await.unwrap();
                assert!(matches!(second_done.status, RunStatus::Succeeded));
                let second_action = second.checkpoint(CheckpointSelector::Action(1)).await.unwrap();
                assert_eq!(second_action.state.values()["count"], 21);
                owner.probe.hold_release.notify_one();
                let first_done = first.wait().await.unwrap();
                assert!(matches!(first_done.status, RunStatus::Succeeded));
                assert!(first_done.checkpoint > first_action.sequence);
                assert_eq!(first_done.last_action_ordinal, Some(1));
                assert_eq!(first_done.state.values()["count"], 11);
                let control_latest = leaf.open_run(RunSelector::Latest(children[0].id.clone())).await.unwrap()
                    .snapshot().await.unwrap();
                assert_eq!(control_latest.occurrence, children[0].occurrence,
                    "A's later control publication owns the App checkpoint projection");
                let saved_latest = leaf.restore(app_sdk::workflow::RestoreOptions {
                    source: RunSelector::Latest(children[0].id.clone()), checkpoint: CheckpointSelector::Latest,
                    overrides: WorkflowState::default(), position: app_sdk::workflow::ResumePosition::AfterCompleted,
                    fallback: None,
                }).await.unwrap().checkpoint(CheckpointSelector::Sequence(0)).await.unwrap();
                assert_eq!(saved_latest.state.values()["count"], 21,
                    "A's later controls cannot supersede B's later actual action save");
                let lineage = saved_latest.lineage.as_ref().unwrap();
                assert_eq!(lineage.source_occurrence, children[1].occurrence);
                assert_eq!(lineage.checkpoint, second_action.sequence);
                let exact_first = leaf.restore(app_sdk::workflow::RestoreOptions {
                    source: RunSelector::Exact { id: children[0].id.clone(), occurrence: children[0].occurrence.clone() },
                    checkpoint: CheckpointSelector::Latest, overrides: WorkflowState::default(),
                    position: app_sdk::workflow::ResumePosition::AfterCompleted, fallback: None,
                }).await.unwrap().checkpoint(CheckpointSelector::Sequence(0)).await.unwrap();
                assert_eq!(exact_first.state.values()["count"], 11);
                assert_eq!(exact_first.lineage.as_ref().unwrap().checkpoint, first_action.sequence);
                let committed = parent.wait().await.unwrap();
                assert_eq!(committed.state.values()["collected"], json!([11, 21]));
                assert_eq!(first.snapshot().await.unwrap().checkpoint, first_done.checkpoint);
                assert_eq!(second.snapshot().await.unwrap().checkpoint, second_done.checkpoint);
                assert_eq!(owner.probe.registered_children.load(Ordering::SeqCst), 2);
                assert_eq!(owner.probe.executor_calls.load(Ordering::SeqCst), 1);
                assert_eq!(owner.probe.steps.lock().unwrap().iter().filter(|(node, _, _)| node == "effect").count(), 2);
                return;
            }
            if matches!(
                case,
                EqualKeyCase::ExactControls | EqualKeyCase::DeleteLatest
            ) {
                assert_ne!(children[0].occurrence, children[1].occurrence);
                assert!(
                    children[0].occurrence < children[1].occurrence,
                    "inventory orders equal logical ids by exact occurrence token"
                );
                for child in &children {
                    assert_eq!(child.occurrence.len(), 32);
                    assert!(
                        child
                            .occurrence
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                    );
                }
                let page = leaf
                    .list_runs(
                        Some(RunSelector::Exact {
                            id: children[0].id.clone(),
                            occurrence: children[0].occurrence.clone(),
                        }),
                        NonZeroUsize::new(1).unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(page.len(), 1);
                assert_eq!(page[0].occurrence, children[1].occurrence);
                assert!(
                    leaf.list_runs(
                        Some(RunSelector::Latest(children[0].id.clone())),
                        NonZeroUsize::new(2).unwrap()
                    )
                    .await
                    .unwrap()
                    .is_empty(),
                    "Latest inventory cursor excludes the complete logical identity"
                );
                for occurrence in [
                    String::new(),
                    "A".repeat(32),
                    "a".repeat(31),
                    "g".repeat(32),
                ] {
                    assert!(matches!(
                        leaf.open_run(RunSelector::Exact {
                            id: children[0].id.clone(),
                            occurrence
                        })
                        .await,
                        Err(AppError::InvalidInput(_))
                    ));
                }
                assert!(
                    matches!(
                        leaf.open_run(RunSelector::Exact {
                            id: children[0].id.clone(),
                            occurrence: before.occurrence.clone(),
                        })
                        .await,
                        Err(AppError::NotFound(_))
                    ),
                    "another real occurrence token cannot cross the child identity scope"
                );
                children.sort_by_key(|child| child.state.values()["count"].as_i64().unwrap());
                let mut first = leaf
                    .open_run(RunSelector::Exact {
                        id: children[0].id.clone(),
                        occurrence: children[0].occurrence.clone(),
                    })
                    .await
                    .unwrap();
                let mut second = leaf
                    .open_run(RunSelector::Exact {
                        id: children[1].id.clone(),
                        occurrence: children[1].occurrence.clone(),
                    })
                    .await
                    .unwrap();
                let Interaction::Input {
                    sequence,
                    generation,
                    ..
                } = children[0].interactions[0]
                else {
                    unreachable!()
                };
                first
                    .resolve(sequence, generation, Resolution::Input(Value::Null))
                    .await
                    .unwrap();
                let first_done = first.wait().await.unwrap();
                assert!(matches!(first_done.status, RunStatus::Succeeded));
                assert_eq!(first_done.occurrence, children[0].occurrence);
                assert_eq!(first_done.state.values()["count"], 11);
                let untouched = second.snapshot().await.unwrap();
                assert!(matches!(
                    untouched.interactions.as_slice(),
                    [Interaction::Input { .. }]
                ));
                assert_eq!(untouched.occurrence, children[1].occurrence);
                assert_eq!(untouched.state.values()["count"], 20);
                assert!(untouched.last_action_ordinal.is_none() && untouched.outcomes.is_empty());
                assert_eq!(
                    parent.snapshot().await.unwrap().state.values()["collected"],
                    json!([])
                );
                assert!(
                    matches!(
                        first.delete(first_done.checkpoint).await,
                        Err(AppError::Busy(_))
                    ),
                    "exact completed occurrence remains needed until original parent reduction"
                );
                let history = first
                    .history(None, NonZeroUsize::new(1).unwrap())
                    .await
                    .unwrap();
                assert_eq!(history.checkpoints[0].occurrence, children[0].occurrence);
                assert!(
                    matches!(
                        second
                            .history(
                                Some(
                                    history.next.expect(
                                        "completed occurrence has retained later checkpoints"
                                    )
                                ),
                                NonZeroUsize::new(1).unwrap()
                            )
                            .await,
                        Err(AppError::InvalidInput(_))
                    ),
                    "same logical RunId does not make another occurrence's cursor portable"
                );
                let Interaction::Input {
                    sequence,
                    generation,
                    ..
                } = untouched.interactions[0]
                else {
                    unreachable!()
                };
                second
                    .resolve(sequence, generation, Resolution::Input(Value::Null))
                    .await
                    .unwrap();
                let committed = parent.wait().await.unwrap();
                assert!(matches!(committed.status, RunStatus::Succeeded));
                assert_eq!(committed.state.values()["collected"], json!([11, 21]));
                assert_eq!(committed.last_action_ordinal, Some(0));
                assert_eq!(committed.outcomes.len(), 1);
                assert_eq!(
                    first.snapshot().await.unwrap().state.values()["count"],
                    11,
                    "later equal-key publication cannot retarget an already returned handle"
                );
                if matches!(case, EqualKeyCase::DeleteLatest) {
                    let second_done = second.snapshot().await.unwrap();
                    let newest = leaf
                        .restore(app_sdk::workflow::RestoreOptions {
                            source: RunSelector::Latest(children[0].id.clone()),
                            checkpoint: CheckpointSelector::Latest,
                            overrides: WorkflowState::default(),
                            position: app_sdk::workflow::ResumePosition::AfterCompleted,
                            fallback: None,
                        })
                        .await
                        .unwrap();
                    let copied = newest
                        .checkpoint(CheckpointSelector::Sequence(0))
                        .await
                        .unwrap();
                    assert_eq!(copied.state.values()["count"], 21);
                    assert_eq!(
                        copied.lineage.as_ref().unwrap().source_occurrence,
                        children[1].occurrence
                    );
                    second.delete(second_done.checkpoint).await.unwrap();
                    assert!(matches!(
                        leaf.open_run(RunSelector::Exact {
                            id: children[1].id.clone(),
                            occurrence: children[1].occurrence.clone(),
                        })
                        .await,
                        Err(AppError::NotFound(_))
                    ));
                    let latest = leaf
                        .open_run(RunSelector::Latest(children[0].id.clone()))
                        .await
                        .unwrap()
                        .snapshot()
                        .await
                        .unwrap();
                    assert_eq!(latest.occurrence, children[0].occurrence);
                    assert_eq!(latest.state.values()["count"], 11);
                    let earlier = leaf
                        .restore(app_sdk::workflow::RestoreOptions {
                            source: RunSelector::Latest(children[0].id.clone()),
                            checkpoint: CheckpointSelector::Latest,
                            overrides: WorkflowState::default(),
                            position: app_sdk::workflow::ResumePosition::AfterCompleted,
                            fallback: None,
                        })
                        .await
                        .unwrap();
                    let copied = earlier
                        .checkpoint(CheckpointSelector::Sequence(0))
                        .await
                        .unwrap();
                    assert_eq!(copied.state.values()["count"], 11);
                    assert_eq!(
                        copied.lineage.as_ref().unwrap().source_occurrence,
                        children[0].occurrence
                    );
                    assert_eq!(
                        first.snapshot().await.unwrap().checkpoint,
                        first_done.checkpoint
                    );
                    assert_eq!(owner.probe.registered_children.load(Ordering::SeqCst), 2);
                    assert_eq!(owner.probe.executor_calls.load(Ordering::SeqCst), 1);
                    assert_eq!(
                        owner
                            .probe
                            .steps
                            .lock()
                            .unwrap()
                            .iter()
                            .filter(|(node, _, _)| node == "effect")
                            .count(),
                        2
                    );
                    return;
                }
                first.delete(first_done.checkpoint).await.unwrap();
                assert!(matches!(
                    leaf.open_run(RunSelector::Exact {
                        id: children[0].id.clone(),
                        occurrence: children[0].occurrence.clone(),
                    })
                    .await,
                    Err(AppError::NotFound(_))
                ));
                let remaining = leaf
                    .list_runs(None, NonZeroUsize::new(2).unwrap())
                    .await
                    .unwrap();
                assert_eq!(remaining.len(), 1);
                assert_eq!(remaining[0].occurrence, children[1].occurrence);
                let latest = leaf
                    .open_run(RunSelector::Latest(children[1].id.clone()))
                    .await
                    .unwrap()
                    .snapshot()
                    .await
                    .unwrap();
                assert_eq!(latest.occurrence, children[1].occurrence);
                assert_eq!(latest.state.values()["count"], 21);
                assert_eq!(owner.probe.registered_children.load(Ordering::SeqCst), 2);
                assert_eq!(owner.probe.executor_calls.load(Ordering::SeqCst), 1);
                assert_eq!(
                    owner
                        .probe
                        .steps
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|(node, _, _)| node == "effect")
                        .count(),
                    2
                );
                return;
            }
            parent.cancel(before.checkpoint).await.unwrap();
            let cancelled = parent.wait().await.unwrap();
            assert!(matches!(cancelled.status, RunStatus::Cancelled));
            assert_eq!(cancelled.state, before.state);
            assert!(cancelled.last_action_ordinal.is_none() && cancelled.outcomes.is_empty());
            let rows = leaf
                .list_runs(None, NonZeroUsize::new(4).unwrap())
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert!(
                rows.iter()
                    .all(|row| matches!(row.status, RunStatus::Cancelled))
            );
            assert_eq!(rows[0].id, rows[1].id);
            assert_eq!(owner.probe.registered_children.load(Ordering::SeqCst), 2);
            assert_eq!(owner.probe.executor_calls.load(Ordering::SeqCst), 1);
            assert!(
                owner
                    .probe
                    .steps
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|(node, _, _)| node != "effect")
            );
        }))
        .catch_unwind()
        .await;
        *owner.probe.hold.lock().unwrap() = None;
        owner.probe.hold_release.notify_one();
        let cleanup = tokio::time::timeout(
            Duration::from_secs(5),
            owner.app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        )
        .await;
        drop(owner);
        match checked {
            Ok(result) => result.expect("equal-key occurrence observations are bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup
            .expect("equal-key cleanup is bounded")
            .expect("equal-key cleanup joins");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn equal_task_keys_admit_independent_input_occurrences_without_serializing_the_batch() {
    ChildComposition::equal_key_occurrences(EqualKeyCase::Cancel).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_equal_key_occurrence_controls_history_and_deletion_do_not_retarget_siblings() {
    ChildComposition::equal_key_occurrences(EqualKeyCase::ExactControls).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deleting_latest_occurrence_restores_the_prior_retained_checkpoint_and_saved_action() {
    ChildComposition::equal_key_occurrences(EqualKeyCase::DeleteLatest).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn later_equal_key_control_does_not_replace_the_latest_saved_action() {
    ChildComposition::equal_key_occurrences(EqualKeyCase::SavedOrder).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_equal_key_occurrence_does_not_hide_the_previous_saved_action() {
    ChildComposition::equal_key_occurrences(EqualKeyCase::FreshAdmission).await;
}
