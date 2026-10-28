//! App must retain and invoke the caller's exact typing implementation at state
//! admission, mutation and reduction boundaries, without retaining local Data.

use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agents_kit::Agent;
use app_kit::workflow::{
    ExecutionOptions, RunStatus, StateChange, StateTyping, WorkflowDefinition, WorkflowFailure,
    WorkflowState,
};
use app_kit::{App, AppConfig, AppError, Shutdown, WorkspaceConfig};
use serde_json::{Map, json};
use storage_kit::StateStore;
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};

#[derive(Clone, Copy)]
enum Policy {
    Accept,
    RejectInitial,
    PanicInitial,
    PanicRoundtrip,
    RejectPost,
    PanicPost,
    RejectReduced,
}

struct Typing {
    policy: Policy,
    observed: Arc<Mutex<Vec<String>>>,
}

impl Typing {
    fn definition(parallel: bool) -> WorkflowDefinition {
        let (nodes, routes) = if parallel {
            (
                json!([
                    {"id":"first", "step":{"kind":"state", "mutations":[]}},
                    {"id":"left", "step":{"kind":"state", "mutations":[{"kind":"assign", "target":"count", "value":{"kind":"literal", "value":10}}]}},
                    {"id":"right", "step":{"kind":"state", "mutations":[{"kind":"assign", "target":"count", "value":{"kind":"literal", "value":10}}]}}
                ]),
                json!({"first":{"mode":"broadcast", "branches":[
                    {"target":{"kind":"node", "id":"left"}},
                    {"target":{"kind":"node", "id":"right"}}
                ]}}),
            )
        } else {
            (
                json!([{ "id":"first", "step":{"kind":"state", "mutations":[
                    {"kind":"increment", "target":"count", "value":{"kind":"literal", "value":1}}
                ]}}]),
                json!({}),
            )
        };
        WorkflowDefinition::parse(
            &json!({
                "schema":"app-kit.workflow/v2", "id":"typed", "revision":"r1",
                "state":{"count":0}, "required":["count"],
                "reducers":{"count": if parallel { "sum_integer" } else { "reject_concurrent" }},
                "entry":"first", "nodes":nodes, "routes":routes
            })
            .to_string(),
        )
        .expect("typed workflow")
    }

    async fn app(
        policy: Policy,
        parallel: bool,
        observed: &Arc<Mutex<Vec<String>>>,
    ) -> App<
        storage_kit::StateStore,
        Agent,
        app_kit::workflow::NoActions,
        app_kit::workflow::NoConditions,
        Self,
    > {
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("memory store");
        let config = AppConfig::new(store, Vec::new(), vec![Self::definition(parallel)]);
        App::open(config.with_state_typing(Self {
            policy,
            observed: Arc::clone(observed),
        }))
        .await
        .expect("App retains caller typing")
    }

    fn record(&self, phase: &str, state: &WorkflowState) {
        self.observed
            .lock()
            .expect("typing events")
            .push(format!("{phase}:{}", state.values()["count"]));
    }
}

impl StateTyping for Typing {
    type Data = Rc<WorkflowState>;

    fn construct_data(&self, state: &WorkflowState) -> Result<Self::Data, AppError> {
        self.record("data", state);
        assert!(
            !(matches!(self.policy, Policy::PanicRoundtrip) && state.values()["count"] == 7),
            "PRIVATE_ROUNDTRIP_TYPING_PANIC"
        );
        Ok(Rc::new(state.clone()))
    }

    fn construct_state(&self, data: &Self::Data) -> Result<WorkflowState, AppError> {
        self.record("reconstruct", data);
        Ok(data.as_ref().clone())
    }

    fn validate_pre(&self, state: &WorkflowState, _: &StateChange) -> Result<(), AppError> {
        self.record("pre", state);
        Ok(())
    }

    fn validate_post(
        &self,
        _: &WorkflowState,
        after: &WorkflowState,
        _: &StateChange,
    ) -> Result<(), AppError> {
        self.record("post", after);
        assert!(
            !matches!(self.policy, Policy::PanicPost),
            "caller typing post panic"
        );
        if matches!(self.policy, Policy::RejectPost) {
            Err(AppError::InvalidInput("caller rejects candidate".into()))
        } else {
            Ok(())
        }
    }

    fn validate_state(&self, state: &WorkflowState) -> Result<(), AppError> {
        self.record("state", state);
        assert!(
            !(matches!(self.policy, Policy::PanicInitial) && state.values()["count"] == 7),
            "PRIVATE_INITIAL_TYPING_PANIC"
        );
        if matches!(self.policy, Policy::RejectInitial) && state.values()["count"] == 7
            || matches!(self.policy, Policy::RejectReduced)
                && state.values()["count"].as_i64().expect("count") > 10
        {
            Err(AppError::InvalidInput("caller state invariant".into()))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn workflow_mutations_use_exact_caller_typing_and_keep_local_data_local() {
    for policy in [Policy::Accept, Policy::RejectPost, Policy::PanicPost] {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let mut app = Typing::app(policy, false, &observed).await;
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().expect("root"),
            })
            .await
            .expect("workspace");
        let workflow = workspace
            .workflow("typed".parse().expect("id"))
            .await
            .expect("workflow");
        let run = workflow
            .start(ExecutionOptions::default())
            .await
            .expect("accepted run");
        let snapshot = tokio::time::timeout(Duration::from_secs(3), run.wait())
            .await
            .expect("typing outcome must settle")
            .expect("snapshot");
        match policy {
            Policy::Accept => {
                assert!(matches!(snapshot.status, RunStatus::Succeeded));
                assert_eq!(snapshot.state.values()["count"], 1);
            }
            Policy::RejectPost => {
                assert!(matches!(
                    snapshot.status,
                    RunStatus::Failed(WorkflowFailure::State(_))
                ));
                assert_eq!(snapshot.state.values()["count"], 0);
            }
            Policy::PanicPost => {
                assert!(matches!(
                    snapshot.status,
                    RunStatus::Failed(WorkflowFailure::Execution(_))
                ));
                assert_eq!(snapshot.state.values()["count"], 0);
            }
            _ => unreachable!("selected mutation policies"),
        }
        let events = observed.lock().expect("events").clone();
        let pre = events
            .iter()
            .position(|event| event == "pre:0")
            .expect("caller pre validation");
        assert_eq!(events[pre + 1], "post:1");
        assert_eq!(
            events
                .iter()
                .filter(|event| event.as_str() == "pre:0")
                .count(),
            1,
            "no automatic mutation retry"
        );
        if matches!(policy, Policy::Accept) {
            assert_eq!(
                &events[pre + 2..pre + 5],
                ["state:1", "data:1", "reconstruct:1"]
            );
        } else {
            assert!(
                !events
                    .iter()
                    .any(|event| ["state:1", "data:1", "reconstruct:1"].contains(&event.as_str())),
                "post failure stops candidate validation"
            );
        }
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        })
        .await
        .expect("drain");
    }
}

#[tokio::test]
async fn typing_rejects_a_combined_reducer_result_before_committing_the_barrier() {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let mut app = Typing::app(Policy::RejectReduced, true, &observed).await;
    let workspace = app
        .create_workspace(WorkspaceConfig {
            name: None,
            root: std::env::current_dir().expect("root"),
        })
        .await
        .expect("workspace");
    let workflow = workspace
        .workflow("typed".parse().expect("id"))
        .await
        .expect("workflow");
    let run = workflow
        .start(ExecutionOptions::default())
        .await
        .expect("accepted run");
    let snapshot = tokio::time::timeout(Duration::from_secs(3), run.wait())
        .await
        .expect("barrier outcome must settle")
        .expect("snapshot");
    assert!(matches!(
        snapshot.status,
        RunStatus::Failed(WorkflowFailure::State(_))
    ));
    assert_eq!(
        snapshot.state.values()["count"],
        0,
        "neither sibling candidate is committed"
    );
    let events = observed.lock().expect("events").clone();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.as_str() == "post:10")
            .count(),
        2
    );
    assert!(
        events.iter().any(|event| event == "state:20"),
        "the actual reduced candidate reaches typing"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.starts_with("pre:"))
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.starts_with("post:"))
            .count(),
        2
    );
    assert!(
        !events
            .iter()
            .any(|event| ["post:20", "data:20", "reconstruct:20"].contains(&event.as_str())),
        "a reduced snapshot has no primitive post hook, and failed state validation stops roundtrip"
    );
    app.shutdown(Shutdown::CancelAndDrain {
        timeout: Duration::from_secs(3),
    })
    .await
    .expect("drain");
}

#[tokio::test]
async fn typing_rejects_initial_state_before_run_publication() {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let mut app = Typing::app(Policy::RejectInitial, false, &observed).await;
    let workspace = app
        .create_workspace(WorkspaceConfig {
            name: None,
            root: std::env::current_dir().expect("root"),
        })
        .await
        .expect("workspace");
    let workflow = workspace
        .workflow("typed".parse().expect("id"))
        .await
        .expect("workflow");
    assert!(matches!(
        workflow
            .start(ExecutionOptions {
                state: WorkflowState::new(Map::from_iter([("count".into(), json!(7))])).unwrap(),
                ..ExecutionOptions::default()
            })
            .await,
        Err(AppError::InvalidInput(_))
    ));
    assert!(
        workflow
            .list_runs(None, std::num::NonZeroUsize::new(10).expect("limit"))
            .await
            .expect("list")
            .is_empty()
    );
    let events = observed.lock().expect("events").clone();
    assert!(events.iter().any(|event| event == "state:7"));
    assert!(
        !events
            .iter()
            .any(|event| ["data:7", "reconstruct:7"].contains(&event.as_str()))
    );
    app.shutdown(Shutdown::CancelAndDrain {
        timeout: Duration::from_secs(3),
    })
    .await
    .expect("drain");
}

#[tokio::test]
async fn typing_panics_before_admission_are_integration_errors_and_leave_app_usable() {
    for policy in [Policy::PanicInitial, Policy::PanicRoundtrip] {
        for prepared in [false, true] {
            let observed = Arc::new(Mutex::new(Vec::new()));
            let mut app = Typing::app(policy, false, &observed).await;
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().expect("root"),
                })
                .await
                .expect("workspace");
            let workflow = workspace
                .workflow("typed".parse().expect("id"))
                .await
                .expect("workflow");
            let input = Map::from_iter([("count".into(), json!(7))]);
            let result = if prepared {
                workflow
                    .prepare(WorkflowState::new(input).expect("initial state"))
                    .await
            } else {
                workflow
                    .start(ExecutionOptions {
                        state: WorkflowState::new(input).unwrap(),
                        ..ExecutionOptions::default()
                    })
                    .await
            };
            let Err(AppError::Integration(message)) = result else {
                panic!("a caller typing panic before publication is Integration, not a closed App");
            };
            assert!(
                !message.contains("PRIVATE_"),
                "panic payload is not portable error text"
            );
            assert!(
                workflow
                    .list_runs(None, std::num::NonZeroUsize::new(10).expect("limit"))
                    .await
                    .expect("unpublished run inventory")
                    .is_empty()
            );
            let healthy = workflow
                .start(ExecutionOptions::default())
                .await
                .expect("the same App still accepts valid state");
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(3), healthy.wait())
                    .await
                    .expect("valid action settles")
                    .expect("valid outcome")
                    .status,
                RunStatus::Succeeded
            ));
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            })
            .await
            .expect("drain valid work");
        }
    }
}
