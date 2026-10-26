//! Real App composition over direct, process-local caller backends.
//! Reconstruction here is same-process state retention, never durability evidence.
use app_sdk::workflow::{
    CheckpointSelector, ExecutionOptions, RestoreOptions, ResumePosition, RunSelector, RunStatus,
    WorkflowDefinition, WorkflowState,
};
use app_sdk::{AgentSpec, App, AppConfig, AppError, Shutdown, WorkspaceConfig};
use serde_json::{Map, Value, json};
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use storage_sdk::state::record::{StateKey, StateRecord};
use storage_sdk::{StateStoreBackend, StateTransactionBackend};
pub mod mock_agents;
pub mod mock_state;
pub mod mock_workflow;
use mock_agents::{AgentChoice, CallerAgent, Peer};
use mock_state::{CommitFault, Memory, ReadCommit, Store, StoreChoice};
use mock_workflow::{
    ActionChoice, ActionPlan, ActionScript, CallerCondition, CallerHooks, CallerTyping, ChildTrace,
    ConditionChoice, ExecutorChoice, ExpectedAction, ForwardExecutor, HookEvent, HookTrace,
    HooksChoice, ReverseExecutor, TypingChoice, TypingTrace,
};

struct Cases;
impl Cases {
    fn config() -> WorkspaceConfig {
        WorkspaceConfig {
            name: Some("mock workspace".into()),
            root: std::env::current_dir().unwrap(),
        }
    }
    fn definition(document: Value) -> WorkflowDefinition {
        WorkflowDefinition::parse(&document.to_string()).unwrap()
    }
    fn counter() -> WorkflowDefinition {
        Self::definition(
            json!({"schema":"app-sdk.workflow/v2","id":"counter","revision":"r1","entry":"increment","state":{"count":0},"required":["count"],"reducers":{"count":"reject_concurrent"},"nodes":[{"id":"increment","step":{"kind":"state","mutations":[{"kind":"increment","target":"count","value":{"kind":"literal","value":1}}]}}],"routes":{}}),
        )
    }
    fn action() -> WorkflowDefinition {
        Self::definition(
            json!({"schema":"app-sdk.workflow/v2","id":"action","revision":"r1","entry":"invoke","state":{"count":0},"required":["count"],"reducers":{"count":"reject_concurrent"},"nodes":[{"id":"invoke","step":{"kind":"action","action":"change","revision":"r1"}}],"routes":{}}),
        )
    }
    fn chain() -> WorkflowDefinition {
        Self::definition(
            json!({"schema":"app-sdk.workflow/v2","id":"chain","revision":"r1","entry":"first","state":{"count":0},"required":["count"],"reducers":{"count":"reject_concurrent"},"nodes":[{"id":"first","step":{"kind":"state","mutations":[{"kind":"increment","target":"count","value":{"kind":"literal","value":1}}]}},{"id":"invoke","step":{"kind":"action","action":"change","revision":"r1"}},{"id":"last","step":{"kind":"state","mutations":[{"kind":"increment","target":"count","value":{"kind":"literal","value":1}}]}}],"routes":{"first":{"mode":"first","branches":[{"target":{"kind":"node","id":"invoke"}}]},"invoke":{"mode":"first","branches":[{"when":{"kind":"custom","condition":"choose","revision":"r1"},"target":{"kind":"node","id":"last"}}]}}}),
        )
    }
    fn agents() -> WorkflowDefinition {
        Self::definition(
            json!({"schema":"app-sdk.workflow/v2","id":"agent","revision":"r1","entry":"answer","state":{"answer":""},"required":["answer"],"reducers":{"answer":"reject_concurrent"},"nodes":[{"id":"answer","deadline_ms":2000,"step":{"kind":"agent","agent":"writer","prompt":[{"kind":"text","text":"reply exactly"}],"text_target":"answer"}}],"routes":{}}),
        )
    }
    fn children() -> Vec<WorkflowDefinition> {
        vec![
            Self::definition(
                json!({"schema":"app-sdk.workflow/v2","id":"child","revision":"r1","entry":"increment","state":{"item":0},"required":["item"],"reducers":{"item":"reject_concurrent"},"nodes":[{"id":"increment","step":{"kind":"state","mutations":[{"kind":"increment","target":"item","value":{"kind":"literal","value":1}}]}}],"routes":{}}),
            ),
            Self::definition(
                json!({"schema":"app-sdk.workflow/v2","id":"parent","revision":"r1","entry":"map","state":{"items":[10,20],"collected":[]},"required":["items","collected"],"reducers":{"items":"reject_concurrent","collected":"collect"},"nodes":[{"id":"map","deadline_ms":3000,"step":{"kind":"map","workflow":"child","items":{"kind":"state","path":"items"},"input_key":"item","max_parallel":2,"reductions":[{"source":"item","target":"collected","reducer":"collect"}]}}],"routes":{}}),
            ),
        ]
    }
    async fn bounded<F: Future>(future: F) -> F::Output {
        tokio::time::timeout(Duration::from_secs(8), future)
            .await
            .expect("mock operation settles within eight seconds")
    }
    async fn app(memory: &Arc<Memory>) -> App<Store, CallerAgent> {
        App::open(AppConfig::new(
            memory.store(),
            Vec::new(),
            vec![Self::counter()],
        ))
        .await
        .unwrap()
    }
    fn shutdown() -> Shutdown {
        Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        }
    }
    async fn shutdown_reobserves_conflicting_read(policy: Shutdown) {
        let memory = Arc::new(Memory::default());
        let action_gate = Arc::new(mock_workflow::ActionGate::default());
        let script = ActionScript::new(vec![ExpectedAction::new(
            0,
            ActionPlan::Gated(Arc::clone(&action_gate), 7),
        )]);
        let config: AppConfig<Store, CallerAgent> =
            AppConfig::new(memory.store(), Vec::new(), vec![Self::action()]);
        let mut app = App::open(config.with_actions(vec![script.action("change")]))
            .await
            .unwrap();
        let workspace = app.create_workspace(Self::config()).await.unwrap();
        let workflow = workspace.workflow("action".parse().unwrap()).await.unwrap();
        let run = workflow.start(ExecutionOptions::default()).await.unwrap();
        Self::bounded(action_gate.entered()).await;

        let read_gate = Arc::new(mock_state::CommitGate::default());
        memory.read_commit_as(ReadCommit::Gated(Arc::clone(&read_gate)));
        let mut shutdown = Box::pin(app.shutdown(policy));
        let entered = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                () = read_gate.admitted() => None,
                outcome = &mut shutdown => Some(outcome),
            }
        })
        .await;

        // A real action publication changes the observed run while shutdown's
        // read-only transaction is held. Release both gates before assertions.
        action_gate.release();
        let terminal = tokio::time::timeout(Duration::from_secs(3), run.wait()).await;
        read_gate.release();
        let (observed_read, outcome) = match entered {
            Ok(Some(outcome)) => (false, Ok(outcome)),
            entered => (
                entered.is_ok(),
                tokio::time::timeout(Duration::from_secs(5), &mut shutdown).await,
            ),
        };
        drop(shutdown);

        let outcome = outcome.expect("shutdown observation settles after gate release");
        let terminal = terminal
            .expect("action reaches terminal while shutdown's read is held")
            .expect("action completion");
        assert!(observed_read, "shutdown did not reach the gated read");
        assert!(matches!(terminal.status, RunStatus::Succeeded));
        assert_eq!(terminal.state.values()["count"], 7);
        assert_eq!(read_gate.conflicts(), 1, "real read revision conflict");
        assert_eq!(script.observations().len(), 1, "no action replay");
        assert_eq!(script.drops(), 1, "action source settled");
        script.assert_drained();
        memory.assert_faults_consumed();
        outcome.expect("shutdown reobserves a conflicting read-only transaction");
    }
}

#[tokio::test]
async fn drain_shutdown_reobserves_a_conflicting_read_only_transaction() {
    Cases::shutdown_reobserves_conflicting_read(Shutdown::Drain {
        timeout: Duration::from_secs(5),
    })
    .await;
}

#[tokio::test]
async fn cancelling_shutdown_reobserves_a_conflicting_read_only_transaction() {
    Cases::shutdown_reobserves_conflicting_read(Shutdown::CancelAndDrain {
        timeout: Duration::from_secs(5),
    })
    .await;
}

#[tokio::test]
async fn shutdown_retains_a_permanent_read_commit_failure_without_retry() {
    let memory = Arc::new(Memory::default());
    let mut app = Cases::app(&memory).await;
    let workspace = app.create_workspace(Cases::config()).await.unwrap();
    let workflow = workspace
        .workflow("counter".parse().unwrap())
        .await
        .unwrap();
    let _run = workflow.prepare(WorkflowState::default()).await.unwrap();
    memory.read_commit_as(ReadCommit::SerializationFailure);
    let first = Cases::bounded(app.shutdown(Cases::shutdown())).await;
    let repeated = Cases::bounded(app.shutdown(Cases::shutdown())).await;
    memory.assert_faults_consumed();
    let first = first.expect_err("permanent read failure must remain visible");
    let repeated = repeated.expect_err("the finalizer retains its original failure");
    assert!(matches!(&first, AppError::Integration(_)), "{first:?}");
    assert!(
        matches!(&repeated, AppError::Integration(_)),
        "{repeated:?}"
    );
    assert_eq!(first.to_string(), repeated.to_string());
}

#[tokio::test]
async fn read_commit_gate_is_one_shot_and_detects_real_revision_conflicts() {
    let memory = Arc::new(Memory::default());
    let store = memory.store();
    let key = StateKey::new("fixture", "observed").unwrap();
    let mut seed = store.begin_transaction().await.unwrap();
    seed.put(StateRecord::new(key.clone(), json!(1)))
        .await
        .unwrap();
    seed.commit().await.unwrap();
    let gate = Arc::new(mock_state::CommitGate::default());
    memory.read_commit_as(ReadCommit::Gated(Arc::clone(&gate)));

    // Empty and writing transactions cannot consume the read-only expectation.
    store
        .begin_transaction()
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
    let mut writer = store.begin_transaction().await.unwrap();
    writer
        .put(StateRecord::new(
            StateKey::new("fixture", "unrelated").unwrap(),
            json!(0),
        ))
        .await
        .unwrap();
    writer.commit().await.unwrap();
    let mut read = store.begin_transaction().await.unwrap();
    let retained = read.load(key.clone()).await.unwrap().unwrap();
    let mut commit = Box::pin(read.commit());
    let entered = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::select! {
            () = gate.admitted() => None,
            outcome = &mut commit => Some(outcome),
        }
    })
    .await;
    let mut writer = store.begin_transaction().await.unwrap();
    writer
        .put(StateRecord::new(key.clone(), json!(2)))
        .await
        .unwrap();
    let written = writer.commit().await;
    gate.release();
    let (observed_gate, outcome) = match entered {
        Ok(Some(outcome)) => (false, Ok(outcome)),
        entered => (
            entered.is_ok(),
            tokio::time::timeout(Duration::from_secs(3), &mut commit).await,
        ),
    };
    drop(commit);
    written.expect("concurrent writer commits before read validation");
    assert!(observed_gate, "read commit expectation was not selected");
    assert!(matches!(
        outcome.expect("read commit settles"),
        Err(storage_sdk::StateStoreError::TransactionConflict { .. })
    ));
    assert_eq!(gate.conflicts(), 1);
    assert_eq!(*retained.record.value, json!(1));
    let mut fresh = store.begin_transaction().await.unwrap();
    assert_eq!(
        *fresh.load(key).await.unwrap().unwrap().record.value,
        json!(2)
    );
    fresh.commit().await.unwrap();
    memory.assert_faults_consumed();
}
#[tokio::test]
async fn state_workflow_retains_checkpoints_restores_independently_and_deletes_explicitly() {
    let memory = Arc::new(Memory::default());
    let mut app = Cases::app(&memory).await;
    let workspace = app.create_workspace(Cases::config()).await.unwrap();
    let workflow = workspace
        .workflow("counter".parse().unwrap())
        .await
        .unwrap();
    let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
    let terminal = Cases::bounded(
        run.execution(ExecutionOptions::default())
            .await
            .unwrap()
            .finish(),
    )
    .await
    .unwrap();
    assert!(matches!(terminal.status, RunStatus::Succeeded));
    assert_eq!(terminal.state.values()["count"], 1);
    let initial = run
        .checkpoint(CheckpointSelector::Sequence(0))
        .await
        .unwrap();
    assert_eq!(initial.state.values()["count"], 0);
    let action = run.checkpoint(CheckpointSelector::Action(0)).await.unwrap();
    assert_eq!(action.state.values()["count"], 1);
    let page = run
        .history(None, NonZeroUsize::new(1).unwrap())
        .await
        .unwrap();
    assert_eq!(page.checkpoints.len(), 1);
    assert_eq!(page.checkpoints[0].sequence, 0);
    let tail = run
        .history(page.next, NonZeroUsize::new(100).unwrap())
        .await
        .unwrap();
    assert!(!tail.checkpoints.is_empty());
    assert!(tail.checkpoints.iter().all(|c| c.sequence > 0));
    let copy = workflow
        .restore(RestoreOptions {
            source: RunSelector::Latest(run.id().clone()),
            checkpoint: CheckpointSelector::Action(0),
            overrides: WorkflowState::default(),
            position: ResumePosition::AfterCompleted,
            fallback: None,
        })
        .await
        .unwrap();
    assert_ne!(copy.id(), run.id());
    assert_eq!(
        copy.checkpoint(CheckpointSelector::Sequence(0))
            .await
            .unwrap()
            .state
            .values()["count"],
        1
    );
    assert!(matches!(
        run.delete(terminal.checkpoint + 1).await,
        Err(AppError::Conflict(_))
    ));
    let deleted = run.id().clone();
    run.delete(terminal.checkpoint).await.unwrap();
    assert!(matches!(
        workflow.open_run(RunSelector::Latest(deleted)).await,
        Err(AppError::NotFound(_))
    ));
    assert_eq!(copy.snapshot().await.unwrap().state.values()["count"], 1);
    assert_eq!(
        initial.state.values()["count"],
        0,
        "retained immutable observation"
    );
    app.shutdown(Cases::shutdown()).await.unwrap();
}
#[tokio::test]
async fn same_process_reconstruction_preserves_terminal_history_without_reexecution() {
    let memory = Arc::new(Memory::default());
    let mut app = Cases::app(&memory).await;
    let workspace = app.create_workspace(Cases::config()).await.unwrap();
    let workspace_id = workspace.id().clone();
    let workflow = workspace
        .workflow("counter".parse().unwrap())
        .await
        .unwrap();
    let mut run = workflow
        .prepare(WorkflowState::new(Map::from_iter([("count".into(), 9.into())])).unwrap())
        .await
        .unwrap();
    let run_id = run.id().clone();
    let terminal = Cases::bounded(
        run.execution(ExecutionOptions::default())
            .await
            .unwrap()
            .finish(),
    )
    .await
    .unwrap();
    assert!(matches!(terminal.status, RunStatus::Succeeded));
    assert_eq!(terminal.state.values()["count"], 10);
    app.shutdown(Shutdown::Drain {
        timeout: Duration::from_secs(3),
    })
    .await
    .unwrap();
    drop(run);
    drop(workflow);
    drop(workspace);
    drop(app);
    let mut reopened = Cases::app(&memory).await;
    let workspace = reopened.workspace(workspace_id).await.unwrap();
    let workflow = workspace
        .workflow("counter".parse().unwrap())
        .await
        .unwrap();
    let run = workflow
        .open_run(RunSelector::Latest(run_id))
        .await
        .unwrap();
    let retained = run.snapshot().await.unwrap();
    assert!(matches!(retained.status, RunStatus::Succeeded));
    assert_eq!(retained.state.values()["count"], 10);
    assert_eq!(
        run.checkpoint(CheckpointSelector::Sequence(0))
            .await
            .unwrap()
            .state
            .values()["count"],
        9
    );
    let mut copy = workflow
        .restore(RestoreOptions {
            source: RunSelector::Latest(run.id().clone()),
            checkpoint: CheckpointSelector::Action(0),
            overrides: WorkflowState::default(),
            position: ResumePosition::Entrypoint,
            fallback: None,
        })
        .await
        .unwrap();
    let advanced = Cases::bounded(
        copy.execution(ExecutionOptions::default())
            .await
            .unwrap()
            .finish(),
    )
    .await
    .unwrap();
    assert_eq!(advanced.state.values()["count"], 11);
    assert_eq!(run.snapshot().await.unwrap().state.values()["count"], 10);
    reopened.shutdown(Cases::shutdown()).await.unwrap();
}
#[tokio::test]
async fn struct_workflow_values_drive_real_app_with_local_stream_and_typing_data() {
    let memory = Arc::new(Memory::default());
    let script = ActionScript::new(vec![ExpectedAction::new(
        1,
        ActionPlan::PartialThenComplete(2),
    )]);
    let typing = Arc::new(TypingTrace::default());
    let hooks = Arc::new(HookTrace::default());
    let config: AppConfig<Store, CallerAgent> =
        AppConfig::new(memory.store(), Vec::new(), vec![Cases::chain()]);
    let mut app = App::open(
        config
            .with_actions(vec![script.action("change")])
            .with_conditions(vec![CallerCondition::new(2, Ok(true))])
            .with_state_typing(CallerTyping::new(&typing, Some(10)))
            .with_hooks(CallerHooks::new(&hooks, None)),
    )
    .await
    .unwrap();
    let workspace = app.create_workspace(Cases::config()).await.unwrap();
    let workflow = workspace.workflow("chain".parse().unwrap()).await.unwrap();
    let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
    let terminal = Cases::bounded(
        run.execution(ExecutionOptions::default())
            .await
            .unwrap()
            .finish(),
    )
    .await
    .unwrap();
    assert!(matches!(terminal.status, RunStatus::Succeeded));
    assert_eq!(terminal.state.values()["count"], 3);
    assert_eq!(script.observations()[0].state["count"], 1);
    script.assert_drained();
    assert_eq!(script.drops(), 1);
    assert!(typing.calls().contains(&"construct_state"));
    let events = hooks.events();
    assert!(events.contains(&(false, HookEvent::Registered)));
    assert!(events.contains(&(true, HookEvent::Registered)));
    assert!(events.contains(&(false, HookEvent::StreamItem)));
    assert!(events.contains(&(true, HookEvent::ExecuteFinished)));
    app.shutdown(Cases::shutdown()).await.unwrap();
}
#[tokio::test]
async fn both_workflow_enum_arms_compose_with_both_storage_enum_arms() {
    for second in [false, true] {
        let memory = Arc::new(Memory::default());
        let script = ActionScript::new(vec![ExpectedAction::new(1, ActionPlan::Complete(2))]);
        let typing = Arc::new(TypingTrace::default());
        let hooks = Arc::new(HookTrace::default());
        let store = if second {
            StoreChoice::Second(memory.store())
        } else {
            StoreChoice::First(memory.store())
        };
        let action = if second {
            ActionChoice::Second(script.action("change"))
        } else {
            ActionChoice::First(script.action("change"))
        };
        let condition = if second {
            ConditionChoice::Second(CallerCondition::new(2, Ok(true)))
        } else {
            ConditionChoice::First(CallerCondition::new(2, Ok(true)))
        };
        let typed = if second {
            TypingChoice::Second(CallerTyping::new(&typing, Some(10)))
        } else {
            TypingChoice::First(CallerTyping::new(&typing, Some(10)))
        };
        let observed = if second {
            HooksChoice::Second(CallerHooks::new(&hooks, None))
        } else {
            HooksChoice::First(CallerHooks::new(&hooks, None))
        };
        let config: AppConfig<StoreChoice, CallerAgent> =
            AppConfig::new(store, Vec::new(), vec![Cases::chain()]);
        let mut app = App::open(
            config
                .with_actions(vec![action])
                .with_conditions(vec![condition])
                .with_state_typing(typed)
                .with_hooks(observed),
        )
        .await
        .unwrap();
        let workspace = app.create_workspace(Cases::config()).await.unwrap();
        let workflow = workspace.workflow("chain".parse().unwrap()).await.unwrap();
        let run = workflow.start(ExecutionOptions::default()).await.unwrap();
        let terminal = Cases::bounded(run.wait()).await.unwrap();
        assert!(matches!(terminal.status, RunStatus::Succeeded));
        assert_eq!(terminal.state.values()["count"], 3);
        script.assert_drained();
        app.shutdown(Cases::shutdown()).await.unwrap();
    }
}
#[tokio::test]
async fn caller_agent_struct_materializes_and_executes_without_supplied_backends() {
    let memory = Arc::new(Memory::default());
    let peer = Peer::new(1);
    peer.expect_prompt("reply exactly", "caller response");
    let mut app = App::open(AppConfig::new(
        memory.store(),
        vec![AgentSpec::new("writer".into(), "r1".into(), peer.agent())],
        vec![Cases::agents()],
    ))
    .await
    .unwrap();
    let workspace = app.create_workspace(Cases::config()).await.unwrap();
    assert_eq!(peer.creations(), 1);
    let workflow = workspace.workflow("agent".parse().unwrap()).await.unwrap();
    let run = workflow.start(ExecutionOptions::default()).await.unwrap();
    let terminal = Cases::bounded(run.wait()).await.unwrap();
    assert!(matches!(terminal.status, RunStatus::Succeeded));
    assert_eq!(terminal.state.values()["answer"], "caller response");
    peer.assert_drained();
    assert_eq!(peer.prompts(), ["reply exactly"]);
    app.shutdown(Cases::shutdown()).await.unwrap();
    assert_eq!(peer.shutdowns(), 1);
    assert!(
        matches!(agent_sdk::AgentBackend::info(&peer.agent()).await,Err(error) if error.kind()==agent_sdk::error::ErrorKind::Closed)
    );
}
#[tokio::test]
async fn exhaustive_agent_workspace_conversation_enum_paths_preserve_associated_children() {
    for second in [false, true] {
        let memory = Arc::new(Memory::default());
        let peer = Peer::new(if second { 3 } else { 2 });
        peer.expect_prompt("reply exactly", "enum response");
        let agent = if second {
            AgentChoice::Second(peer.agent())
        } else {
            AgentChoice::First(peer.agent())
        };
        let mut app = App::open(AppConfig::new(
            memory.store(),
            vec![AgentSpec::new("writer".into(), "r1".into(), agent)],
            vec![Cases::agents()],
        ))
        .await
        .unwrap();
        let workspace = app.create_workspace(Cases::config()).await.unwrap();
        let workflow = workspace.workflow("agent".parse().unwrap()).await.unwrap();
        let run = workflow.start(ExecutionOptions::default()).await.unwrap();
        let terminal = Cases::bounded(run.wait()).await.unwrap();
        assert!(matches!(terminal.status, RunStatus::Succeeded));
        assert_eq!(terminal.state.values()["answer"], "enum response");
        peer.assert_drained();
        app.shutdown(Cases::shutdown()).await.unwrap();
        assert_eq!(peer.shutdowns(), 1);
    }
}
#[tokio::test]
async fn actual_child_batches_preserve_sibling_isolation_and_canonical_reduction_for_both_polling_strategies()
 {
    for reverse in [false, true] {
        let memory = Arc::new(Memory::default());
        let trace = Arc::new(ChildTrace::default());
        let executor = if reverse {
            ExecutorChoice::Reverse(ReverseExecutor::new(&trace))
        } else {
            ExecutorChoice::Forward(ForwardExecutor::new(&trace))
        };
        let config: AppConfig<Store, CallerAgent> =
            AppConfig::new(memory.store(), Vec::new(), Cases::children());
        let mut app = App::open(config.with_child_executor(executor))
            .await
            .unwrap();
        let workspace = app.create_workspace(Cases::config()).await.unwrap();
        let workflow = workspace.workflow("parent".parse().unwrap()).await.unwrap();
        let run = workflow.start(ExecutionOptions::default()).await.unwrap();
        let terminal = Cases::bounded(run.wait()).await.unwrap();
        assert!(matches!(terminal.status, RunStatus::Succeeded));
        assert_eq!(terminal.state.values()["collected"], json!([11, 21]));
        assert!(trace.batches().iter().any(|(_, count)| *count == 2));
        app.shutdown(Cases::shutdown()).await.unwrap();
    }
}
#[tokio::test]
async fn validation_rejects_invalid_initial_state_before_action_effects() {
    let memory = Arc::new(Memory::default());
    let script = ActionScript::new(Vec::new());
    let typing = Arc::new(TypingTrace::default());
    let config: AppConfig<Store, CallerAgent> =
        AppConfig::new(memory.store(), Vec::new(), vec![Cases::action()]);
    let mut app = App::open(
        config
            .with_actions(vec![script.action("change")])
            .with_state_typing(CallerTyping::new(&typing, Some(5))),
    )
    .await
    .unwrap();
    let workspace = app.create_workspace(Cases::config()).await.unwrap();
    let workflow = workspace.workflow("action".parse().unwrap()).await.unwrap();
    assert!(matches!(
        workflow
            .prepare(WorkflowState::new(Map::from_iter([("count".into(), 10.into())])).unwrap())
            .await,
        Err(AppError::InvalidInput(_))
    ));
    assert!(script.observations().is_empty());
    app.shutdown(Cases::shutdown()).await.unwrap();
}
#[tokio::test]
async fn explicitly_incomplete_action_rolls_back_and_can_be_retried() {
    let memory = Arc::new(Memory::default());
    let script = ActionScript::new(vec![
        ExpectedAction::new(0, ActionPlan::MalformedEof),
        ExpectedAction::new(0, ActionPlan::Complete(1)),
    ]);
    let config: AppConfig<Store, CallerAgent> =
        AppConfig::new(memory.store(), Vec::new(), vec![Cases::action()]);
    let mut app = App::open(config.with_actions(vec![script.action("change")]))
        .await
        .unwrap();
    let workspace = app.create_workspace(Cases::config()).await.unwrap();
    let workflow = workspace.workflow("action".parse().unwrap()).await.unwrap();
    let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
    let mut execution = run.execution(ExecutionOptions::default()).await.unwrap();
    assert!(Cases::bounded(execution.step()).await.unwrap().is_none());
    execution.abort().await.unwrap();
    assert_eq!(run.snapshot().await.unwrap().state.values()["count"], 0);
    let terminal = Cases::bounded(
        run.execution(ExecutionOptions::default())
            .await
            .unwrap()
            .finish(),
    )
    .await
    .unwrap();
    assert!(matches!(terminal.status, RunStatus::Succeeded));
    assert_eq!(terminal.state.values()["count"], 1);
    script.assert_drained();
    assert_eq!(script.drops(), 2);
    app.shutdown(Cases::shutdown()).await.unwrap();
}
#[tokio::test]
async fn prepared_run_observation_drop_is_independent_from_cancellation_authority() {
    let memory = Arc::new(Memory::default());
    let mut app = Cases::app(&memory).await;
    let workspace = app.create_workspace(Cases::config()).await.unwrap();
    let workflow = workspace
        .workflow("counter".parse().unwrap())
        .await
        .unwrap();
    let run = workflow.prepare(WorkflowState::default()).await.unwrap();
    let observer = workflow
        .open_run(RunSelector::Latest(run.id().clone()))
        .await
        .unwrap();
    drop(observer);
    assert!(matches!(
        run.snapshot().await.unwrap().status,
        RunStatus::Paused
    ));
    assert!(matches!(run.cancel(99).await, Err(AppError::Conflict(_))));
    run.cancel(0).await.unwrap();
    let cancelled = Cases::bounded(run.wait()).await.unwrap();
    assert!(matches!(cancelled.status, RunStatus::Cancelled));
    assert_eq!(cancelled.state.values()["count"], 0);
    app.shutdown(Cases::shutdown()).await.unwrap();
}
#[tokio::test]
async fn workspace_revision_fences_and_shutdown_seal_new_admission() {
    let memory = Arc::new(Memory::default());
    let mut app = Cases::app(&memory).await;
    let workspace = app.create_workspace(Cases::config()).await.unwrap();
    let initial = workspace.snapshot().await.unwrap();
    let updated = workspace
        .replace(
            initial.revision,
            WorkspaceConfig {
                name: Some("replacement".into()),
                root: initial.config.root.clone(),
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.revision, initial.revision + 1);
    assert!(matches!(
        workspace.replace(initial.revision, Cases::config()).await,
        Err(AppError::Conflict(_))
    ));
    assert_eq!(
        workspace.snapshot().await.unwrap().config.name.as_deref(),
        Some("replacement")
    );
    app.shutdown(Cases::shutdown()).await.unwrap();
    assert!(matches!(
        app.create_workspace(Cases::config()).await,
        Err(AppError::Closed(_))
    ));
}
#[tokio::test]
async fn memory_fixture_conflicts_rollback_and_ambiguous_commit_do_not_forge_visibility() {
    let memory = Arc::new(Memory::default());
    let store = memory.store();
    let key = StateKey::new("fixture", "one").unwrap();
    let mut first = store.begin_transaction().await.unwrap();
    let mut second = store.begin_transaction().await.unwrap();
    assert!(first.load(key.clone()).await.unwrap().is_none());
    assert!(second.load(key.clone()).await.unwrap().is_none());
    first
        .put(StateRecord::new(key.clone(), json!(1)))
        .await
        .unwrap();
    first.commit().await.unwrap();
    second
        .put(StateRecord::new(key.clone(), json!(2)))
        .await
        .unwrap();
    assert!(second.commit().await.is_err());
    assert_eq!(
        *store.load(key.clone()).await.unwrap().unwrap().record.value,
        json!(1)
    );
    let mut dropped = store.begin_transaction().await.unwrap();
    dropped
        .put(StateRecord::new(key.clone(), json!(3)))
        .await
        .unwrap();
    drop(dropped);
    assert_eq!(
        store
            .load(key.clone())
            .await
            .unwrap()
            .unwrap()
            .revision
            .get(),
        1
    );
    memory.fail_next_commit(CommitFault::BeforePublication);
    let mut failed = store.begin_transaction().await.unwrap();
    failed
        .put(StateRecord::new(key.clone(), json!(4)))
        .await
        .unwrap();
    assert!(failed.commit().await.is_err());
    assert_eq!(
        store
            .load(key.clone())
            .await
            .unwrap()
            .unwrap()
            .revision
            .get(),
        1
    );
    memory.fail_next_commit(CommitFault::AfterPublication);
    let mut ambiguous = store.begin_transaction().await.unwrap();
    ambiguous
        .put(StateRecord::new(key.clone(), json!(5)))
        .await
        .unwrap();
    assert!(ambiguous.commit().await.is_err());
    let current = store.load(key).await.unwrap().unwrap();
    assert_eq!(*current.record.value, json!(5));
    assert_eq!(current.revision.get(), 2);
    memory.assert_faults_consumed();
}

#[tokio::test]
async fn sibling_actions_read_the_same_base_and_conflicting_writes_publish_no_partial_wave() {
    let memory = Arc::new(Memory::default());
    let left = ActionScript::new(vec![ExpectedAction::new(0, ActionPlan::Complete(1))]);
    let right = ActionScript::new(vec![ExpectedAction::new(0, ActionPlan::Complete(2))]);
    let definition = Cases::definition(
        json!({"schema":"app-sdk.workflow/v2","id":"barrier","revision":"r1","entry":"fork","state":{"count":0},"required":["count"],"reducers":{"count":"reject_concurrent"},"nodes":[{"id":"fork","step":{"kind":"state","mutations":[]}},{"id":"left","step":{"kind":"action","action":"left","revision":"r1"}},{"id":"right","step":{"kind":"action","action":"right","revision":"r1"}}],"routes":{"fork":{"mode":"broadcast","branches":[{"target":{"kind":"node","id":"left"}},{"target":{"kind":"node","id":"right"}}]}}}),
    );
    let config: AppConfig<Store, CallerAgent> =
        AppConfig::new(memory.store(), Vec::new(), vec![definition]);
    let mut app = App::open(config.with_actions(vec![left.action("left"), right.action("right")]))
        .await
        .unwrap();
    let workspace = app.create_workspace(Cases::config()).await.unwrap();
    let workflow = workspace
        .workflow("barrier".parse().unwrap())
        .await
        .unwrap();
    let run = workflow.start(ExecutionOptions::default()).await.unwrap();
    let terminal = Cases::bounded(run.wait()).await.unwrap();
    assert!(matches!(terminal.status, RunStatus::Failed(_)));
    assert_eq!(terminal.state.values()["count"], 0);
    assert_eq!(left.observations()[0].state, right.observations()[0].state);
    left.assert_drained();
    right.assert_drained();
    let history = run
        .history(None, NonZeroUsize::new(100).unwrap())
        .await
        .unwrap();
    assert!(
        history
            .checkpoints
            .iter()
            .all(|c| c.state.values()["count"] == 0)
    );
    app.shutdown(Cases::shutdown()).await.unwrap();
}

#[tokio::test]
async fn missing_real_child_completion_is_rejected_before_parent_reduction() {
    let memory = Arc::new(Memory::default());
    let trace = Arc::new(ChildTrace::default());
    let config: AppConfig<Store, CallerAgent> =
        AppConfig::new(memory.store(), Vec::new(), Cases::children());
    let mut app = App::open(
        config.with_child_executor(ReverseExecutor::malformed_missing_completion(&trace)),
    )
    .await
    .unwrap();
    let workspace = app.create_workspace(Cases::config()).await.unwrap();
    let workflow = workspace.workflow("parent".parse().unwrap()).await.unwrap();
    let run = workflow.start(ExecutionOptions::default()).await.unwrap();
    let terminal = Cases::bounded(run.wait()).await.unwrap();
    assert!(matches!(terminal.status, RunStatus::Failed(_)));
    assert_eq!(terminal.state.values()["collected"], json!([]));
    assert!(trace.batches().iter().any(|(_, len)| *len == 2));
    app.shutdown(Cases::shutdown()).await.unwrap();
}

#[tokio::test]
async fn dropping_an_observer_preserves_accepted_gated_work_and_does_not_duplicate_effects() {
    let memory = Arc::new(Memory::default());
    let gate = Arc::new(mock_workflow::ActionGate::default());
    let script = ActionScript::new(vec![ExpectedAction::new(
        0,
        ActionPlan::Gated(Arc::clone(&gate), 7),
    )]);
    let config: AppConfig<Store, CallerAgent> =
        AppConfig::new(memory.store(), Vec::new(), vec![Cases::action()]);
    let mut app = App::open(config.with_actions(vec![script.action("change")]))
        .await
        .unwrap();
    let workspace = app.create_workspace(Cases::config()).await.unwrap();
    let workflow = workspace.workflow("action".parse().unwrap()).await.unwrap();
    let run = workflow.start(ExecutionOptions::default()).await.unwrap();
    Cases::bounded(gate.entered()).await;
    let observer = workflow
        .open_run(RunSelector::Latest(run.id().clone()))
        .await
        .unwrap();
    drop(observer);
    assert_eq!(run.snapshot().await.unwrap().state.values()["count"], 0);
    assert_eq!(script.drops(), 0);
    gate.release();
    let terminal = Cases::bounded(run.wait()).await.unwrap();
    assert!(matches!(terminal.status, RunStatus::Succeeded));
    assert_eq!(terminal.state.values()["count"], 7);
    assert_eq!(script.observations().len(), 1);
    script.assert_drained();
    assert_eq!(script.drops(), 1);
    app.shutdown(Cases::shutdown()).await.unwrap();
}

#[tokio::test]
async fn explicit_cancellation_drops_uncommitted_action_source_without_publication() {
    let memory = Arc::new(Memory::default());
    let gate = Arc::new(mock_workflow::ActionGate::default());
    let script = ActionScript::new(vec![ExpectedAction::new(
        0,
        ActionPlan::Gated(Arc::clone(&gate), 7),
    )]);
    let config: AppConfig<Store, CallerAgent> =
        AppConfig::new(memory.store(), Vec::new(), vec![Cases::action()]);
    let mut app = App::open(config.with_actions(vec![script.action("change")]))
        .await
        .unwrap();
    let workspace = app.create_workspace(Cases::config()).await.unwrap();
    let workflow = workspace.workflow("action".parse().unwrap()).await.unwrap();
    let run = workflow.start(ExecutionOptions::default()).await.unwrap();
    Cases::bounded(gate.entered()).await;
    let current = run.snapshot().await.unwrap();
    run.cancel(current.checkpoint).await.unwrap();
    let terminal = Cases::bounded(run.wait()).await.unwrap();
    assert!(matches!(terminal.status, RunStatus::Cancelled));
    assert_eq!(terminal.state.values()["count"], 0);
    assert_eq!(script.drops(), 1);
    script.assert_drained();
    app.shutdown(Cases::shutdown()).await.unwrap();
}

#[test]
fn synchronous_execution_uses_only_synchronous_hooks_with_local_action_values() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let memory = Arc::new(Memory::default());
    let script = ActionScript::new(vec![ExpectedAction::new(
        0,
        ActionPlan::PartialThenComplete(1),
    )]);
    let hooks = Arc::new(HookTrace::default());
    let (mut app, mut run) = runtime.block_on(async {
        let config: AppConfig<Store, CallerAgent> =
            AppConfig::new(memory.store(), Vec::new(), vec![Cases::action()]);
        let app = App::open(
            config
                .with_actions(vec![script.action("change")])
                .with_hooks(CallerHooks::new(&hooks, None)),
        )
        .await
        .unwrap();
        let workspace = app.create_workspace(Cases::config()).await.unwrap();
        let workflow = workspace.workflow("action".parse().unwrap()).await.unwrap();
        let run = workflow.prepare(WorkflowState::default()).await.unwrap();
        (app, run)
    });
    let before = hooks.events().len();
    let terminal = app_sdk::workflow::SyncExecution::open(&mut run, ExecutionOptions::default())
        .unwrap()
        .finish()
        .unwrap();
    assert!(matches!(terminal.status, RunStatus::Succeeded));
    assert_eq!(terminal.state.values()["count"], 1);
    assert!(
        hooks.events()[before..]
            .iter()
            .all(|(asynchronous, _)| !*asynchronous)
    );
    assert_eq!(
        script.observations()[0].mode,
        app_sdk::workflow::ActionMode::Synchronous
    );
    script.assert_drained();
    runtime.block_on(app.shutdown(Cases::shutdown())).unwrap();
}

#[tokio::test]
async fn admission_reconciles_publication_reply_loss_without_duplicate_runs() {
    let memory = Arc::new(Memory::default());
    let mut app = Cases::app(&memory).await;
    let workspace = app.create_workspace(Cases::config()).await.unwrap();
    let workflow = workspace
        .workflow("counter".parse().unwrap())
        .await
        .unwrap();
    memory.fail_next_commit(CommitFault::AfterPublication);
    let mut run = workflow
        .prepare(WorkflowState::default())
        .await
        .expect("exact readback reconciles committed checkpoint-zero admission");
    memory.assert_faults_consumed();
    let runs = workflow
        .list_runs(None, NonZeroUsize::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].id, *run.id());
    assert_eq!(run.snapshot().await.unwrap().checkpoint, 0);
    let terminal = Cases::bounded(
        run.execution(ExecutionOptions::default())
            .await
            .unwrap()
            .finish(),
    )
    .await
    .unwrap();
    assert!(matches!(terminal.status, RunStatus::Succeeded));
    assert_eq!(terminal.state.values()["count"], 1);
    app.shutdown(Cases::shutdown()).await.unwrap();
}

#[tokio::test]
async fn admission_failure_before_publication_leaves_no_phantom_run() {
    let memory = Arc::new(Memory::default());
    let mut app = Cases::app(&memory).await;
    let workspace = app.create_workspace(Cases::config()).await.unwrap();
    let workflow = workspace
        .workflow("counter".parse().unwrap())
        .await
        .unwrap();
    memory.fail_next_commit(CommitFault::BeforePublication);
    assert!(matches!(
        workflow.prepare(WorkflowState::default()).await,
        Err(AppError::Integration(_))
    ));
    memory.assert_faults_consumed();
    assert!(
        workflow
            .list_runs(None, NonZeroUsize::new(10).unwrap())
            .await
            .unwrap()
            .is_empty()
    );
    let run = workflow.prepare(WorkflowState::default()).await.unwrap();
    assert_eq!(run.snapshot().await.unwrap().checkpoint, 0);
    app.shutdown(Cases::shutdown()).await.unwrap();
}

#[tokio::test]
async fn lower_workspace_rejection_compensates_prior_updates_and_preserves_app_revision() {
    let memory = Arc::new(Memory::default());
    let first = Peer::new(10);
    let second = Peer::new(11);
    let mut app = App::open(AppConfig::new(
        memory.store(),
        vec![
            AgentSpec::new("first".into(), "r1".into(), first.agent()),
            AgentSpec::new("second".into(), "r1".into(), second.agent()),
        ],
        vec![Cases::counter()],
    ))
    .await
    .unwrap();
    let workspace = app.create_workspace(Cases::config()).await.unwrap();
    let before = workspace.snapshot().await.unwrap();
    let first_before = first.workspace_observation();
    let second_before = second.workspace_observation();
    second.reject_next_update();
    assert!(
        workspace
            .replace(
                before.revision,
                WorkspaceConfig {
                    name: Some("rejected replacement".into()),
                    root: before.config.root.clone()
                }
            )
            .await
            .is_err()
    );
    let after = workspace.snapshot().await.unwrap();
    assert_eq!(after.revision, before.revision);
    assert_eq!(after.config.name, before.config.name);
    assert_eq!(first.workspace_observation().name(), first_before.name());
    assert_eq!(second.workspace_observation().name(), second_before.name());
    app.shutdown(Cases::shutdown()).await.unwrap();
    assert_eq!(first.shutdowns(), 1);
    assert_eq!(second.shutdowns(), 1);
}

#[tokio::test]
async fn uncertain_agent_effect_is_retained_for_explicit_resolution_without_automatic_retry() {
    let memory = Arc::new(Memory::default());
    let peer = Peer::new(20);
    peer.expect_uncertain_prompt("reply exactly");
    let mut app = App::open(AppConfig::new(
        memory.store(),
        vec![AgentSpec::new("writer".into(), "r1".into(), peer.agent())],
        vec![Cases::agents()],
    ))
    .await
    .unwrap();
    let workspace = app.create_workspace(Cases::config()).await.unwrap();
    let workflow = workspace.workflow("agent".parse().unwrap()).await.unwrap();
    let run = workflow.start(ExecutionOptions::default()).await.unwrap();
    let unknown = Cases::bounded(run.wait()).await.unwrap();
    let [
        app_sdk::workflow::Interaction::Unknown {
            sequence,
            generation,
            ..
        },
    ] = unknown.interactions.as_slice()
    else {
        panic!("uncertain accepted effect must be retained for explicit settlement")
    };
    assert_eq!(unknown.state.values()["answer"], "");
    assert!(unknown.outcomes.is_empty());
    assert_eq!(peer.prompts().len(), 1);
    let observed = run.snapshot().await.unwrap();
    assert_eq!(observed.checkpoint, unknown.checkpoint);
    assert_eq!(peer.prompts().len(), 1);
    assert!(matches!(
        run.resolve(*sequence, *generation, app_sdk::workflow::Resolution::Retry)
            .await,
        Err(AppError::Conflict(_))
    ));
    run.resolve(*sequence, *generation, app_sdk::workflow::Resolution::Fail)
        .await
        .unwrap();
    let terminal = Cases::bounded(run.wait()).await.unwrap();
    assert!(matches!(
        terminal.status,
        RunStatus::Failed(app_sdk::workflow::WorkflowFailure::Agent(_))
    ));
    assert_eq!(peer.prompts().len(), 1);
    peer.assert_drained();
    app.shutdown(Cases::shutdown()).await.unwrap();
    assert_eq!(peer.shutdowns(), 1);
}

#[tokio::test]
async fn finalization_hook_failure_keeps_the_primary_action_failure_as_its_cause() {
    let memory = Arc::new(Memory::default());
    let script = ActionScript::new(vec![ExpectedAction::new(
        0,
        ActionPlan::Failure("primary action failure"),
    )]);
    let hooks = Arc::new(HookTrace::default());
    let config: AppConfig<Store, CallerAgent> =
        AppConfig::new(memory.store(), Vec::new(), vec![Cases::action()]);
    let mut app = App::open(
        config
            .with_actions(vec![script.action("change")])
            .with_hooks(CallerHooks::new(&hooks, Some(HookEvent::StepFinished))),
    )
    .await
    .unwrap();
    let workspace = app.create_workspace(Cases::config()).await.unwrap();
    let workflow = workspace.workflow("action".parse().unwrap()).await.unwrap();
    let run = workflow.start(ExecutionOptions::default()).await.unwrap();
    assert!(
        matches!(Cases::bounded(run.wait()).await,Err(AppError::Integration(message)) if message.contains("mock synchronous hook failure"))
    );
    let terminal = run.snapshot().await.unwrap();
    assert!(matches!(
        terminal.status,
        RunStatus::Failed(app_sdk::workflow::WorkflowFailure::Action(_))
    ));
    assert_eq!(terminal.state.values()["count"], 0);
    let completions = hooks.completions();
    assert_eq!(
        completions.len(),
        2,
        "both completion lanes receive the structured primary cause"
    );
    assert!(
        completions
            .iter()
            .all(|failure| failure["kind"] == "hook" && failure["cause"]["kind"] == "action")
    );
    script.assert_drained();
    assert_eq!(script.drops(), 1);
    app.shutdown(Cases::shutdown()).await.unwrap();
}

#[tokio::test]
async fn fixture_admitted_commit_retains_authority_after_waiter_loss_and_joins_worker() {
    let memory = Arc::new(Memory::default());
    let gate = Arc::new(mock_state::CommitGate::default());
    let store = memory.store();
    let key = StateKey::new("fixture", "admitted").unwrap();
    let mut transaction = store.begin_transaction().await.unwrap();
    transaction
        .put(StateRecord::new(key.clone(), json!("published")))
        .await
        .unwrap();
    memory.fail_next_commit(CommitFault::AdmittedAfterWaiterLoss(Arc::clone(&gate)));
    let mut waiting = Box::pin(transaction.commit());
    Cases::bounded(async {tokio::select!{_ = gate.admitted()=>{},outcome=&mut waiting=>panic!("commit replied before release: {outcome:?}")}}).await;
    drop(waiting);
    assert!(store.load(key.clone()).await.unwrap().is_none());
    gate.release();
    memory.join_admitted().await;
    assert_eq!(
        *store.load(key).await.unwrap().unwrap().record.value,
        json!("published")
    );
    memory.assert_faults_consumed();
}

#[tokio::test]
async fn app_checkpoint_zero_commit_remains_authoritative_after_admission_waiter_loss() {
    let memory = Arc::new(Memory::default());
    let mut app = Cases::app(&memory).await;
    let workspace = app.create_workspace(Cases::config()).await.unwrap();
    let workflow = workspace
        .workflow("counter".parse().unwrap())
        .await
        .unwrap();
    let gate = Arc::new(mock_state::CommitGate::default());
    memory.fail_next_commit(CommitFault::AdmittedAfterWaiterLoss(Arc::clone(&gate)));
    let mut waiting = Box::pin(workflow.prepare(WorkflowState::default()));
    Cases::bounded(async {tokio::select!{_=gate.admitted()=>{},_= &mut waiting=>panic!("prepare returned before admitted native commit completed")}}).await;
    drop(waiting);
    gate.release();
    memory.join_admitted().await;
    let runs = workflow
        .list_runs(None, NonZeroUsize::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(
        runs.len(),
        1,
        "admitted checkpoint-zero publication is recoverable despite observer loss"
    );
    let mut recovered = workflow
        .open_run(RunSelector::Latest(runs[0].id.clone()))
        .await
        .unwrap();
    assert_eq!(
        recovered.snapshot().await.unwrap().state.values()["count"],
        0
    );
    let terminal = Cases::bounded(
        recovered
            .execution(ExecutionOptions::default())
            .await
            .unwrap()
            .finish(),
    )
    .await
    .unwrap();
    assert!(matches!(terminal.status, RunStatus::Succeeded));
    assert_eq!(terminal.state.values()["count"], 1);
    memory.assert_faults_consumed();
    app.shutdown(Cases::shutdown()).await.unwrap();
}

#[tokio::test]
async fn single_child_uses_actual_batch_and_completion_with_concrete_executor() {
    let memory = Arc::new(Memory::default());
    let trace = Arc::new(ChildTrace::default());
    let mut definitions = Cases::children();
    definitions.push(Cases::definition(json!({"schema":"app-sdk.workflow/v2","id":"single","revision":"r1","entry":"child","state":{"item":0},"required":["item"],"reducers":{"item":"reject_concurrent"},"nodes":[{"id":"child","deadline_ms":3000,"step":{"kind":"child","workflow":"child","input":{"item":{"kind":"literal","value":41}},"reductions":[{"source":"item","target":"item","reducer":"reject_concurrent"}]}}],"routes":{}})));
    let config: AppConfig<Store, CallerAgent> =
        AppConfig::new(memory.store(), Vec::new(), definitions);
    let mut app = App::open(config.with_child_executor(ForwardExecutor::new(&trace)))
        .await
        .unwrap();
    let workspace = app.create_workspace(Cases::config()).await.unwrap();
    let workflow = workspace.workflow("single".parse().unwrap()).await.unwrap();
    let run = workflow.start(ExecutionOptions::default()).await.unwrap();
    let terminal = Cases::bounded(run.wait()).await.unwrap();
    assert!(matches!(terminal.status, RunStatus::Succeeded));
    assert_eq!(terminal.state.values()["item"], 42);
    assert!(trace.batches().iter().any(|(_, len)| *len == 1));
    app.shutdown(Cases::shutdown()).await.unwrap();
}

#[tokio::test]
async fn runtime_input_acceptance_is_fenced_and_precedes_action_dispatch() {
    let memory = Arc::new(Memory::default());
    let input = Map::from_iter([("payload".into(), json!({"value":7}))]);
    let script = ActionScript::new(vec![
        ExpectedAction::new(0, ActionPlan::Complete(1)).with_inputs(input.clone()),
    ]);
    let config: AppConfig<Store, CallerAgent> =
        AppConfig::new(memory.store(), Vec::new(), vec![Cases::action()]);
    let mut app =
        App::open(config.with_actions(vec![script.action("change").requiring("payload")]))
            .await
            .unwrap();
    let workspace = app.create_workspace(Cases::config()).await.unwrap();
    let workflow = workspace.workflow("action".parse().unwrap()).await.unwrap();
    let run = workflow.start(ExecutionOptions::default()).await.unwrap();
    let missing = Cases::bounded(run.wait()).await.unwrap();
    let [
        app_sdk::workflow::Interaction::Inputs {
            sequence,
            generation,
            required,
        },
    ] = missing.interactions.as_slice()
    else {
        panic!("required input must be exposed before dispatch")
    };
    assert_eq!(required.as_slice(), ["payload"]);
    assert!(script.observations().is_empty());
    assert!(matches!(
        run.resolve(
            *sequence,
            generation + 1,
            app_sdk::workflow::Resolution::Inputs(input.clone())
        )
        .await,
        Err(AppError::Conflict(_))
    ));
    assert!(
        run.resolve(
            *sequence,
            *generation,
            app_sdk::workflow::Resolution::Inputs(Map::new())
        )
        .await
        .is_err()
    );
    assert!(script.observations().is_empty());
    run.resolve(
        *sequence,
        *generation,
        app_sdk::workflow::Resolution::Inputs(input.clone()),
    )
    .await
    .unwrap();
    let terminal = Cases::bounded(run.wait()).await.unwrap();
    assert!(matches!(terminal.status, RunStatus::Succeeded));
    assert_eq!(script.observations()[0].inputs, input);
    assert_eq!(script.observations().len(), 1);
    script.assert_drained();
    app.shutdown(Cases::shutdown()).await.unwrap();
}

#[tokio::test]
async fn dropping_execution_owner_revokes_uncommitted_source_and_allows_explicit_retry() {
    let memory = Arc::new(Memory::default());
    let gate = Arc::new(mock_workflow::ActionGate::default());
    let script = ActionScript::new(vec![
        ExpectedAction::new(0, ActionPlan::Gated(Arc::clone(&gate), 8)),
        ExpectedAction::new(0, ActionPlan::Complete(1)),
    ]);
    let config: AppConfig<Store, CallerAgent> =
        AppConfig::new(memory.store(), Vec::new(), vec![Cases::action()]);
    let mut app = App::open(config.with_actions(vec![script.action("change")]))
        .await
        .unwrap();
    let workspace = app.create_workspace(Cases::config()).await.unwrap();
    let workflow = workspace.workflow("action".parse().unwrap()).await.unwrap();
    let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
    let mut execution = run.execution(ExecutionOptions::default()).await.unwrap();
    let mut step = Box::pin(execution.step());
    Cases::bounded(async {tokio::select!{_=gate.entered()=>{},_= &mut step=>panic!("gated source should remain uncommitted")}}).await;
    drop(step);
    drop(execution);
    let rolled_back = Cases::bounded(async {
        loop {
            match run.execution(ExecutionOptions::default()).await {
                Ok(execution) => break execution.abort().await.unwrap(),
                Err(AppError::Busy(_)) => tokio::task::yield_now().await,
                Err(error) => panic!("unexpected retry admission error: {error}"),
            }
        }
    })
    .await;
    assert_eq!(rolled_back.state.values()["count"], 0);
    assert!(rolled_back.last_action_ordinal.is_none());
    assert_eq!(script.drops(), 1);
    let terminal = Cases::bounded(
        run.execution(ExecutionOptions::default())
            .await
            .unwrap()
            .finish(),
    )
    .await
    .unwrap();
    assert!(matches!(terminal.status, RunStatus::Succeeded));
    assert_eq!(terminal.state.values()["count"], 1);
    assert_eq!(script.observations().len(), 2);
    script.assert_drained();
    app.shutdown(Cases::shutdown()).await.unwrap();
}

#[tokio::test]
async fn caller_agent_fixture_preserves_child_metadata_independence_and_lifecycle() {
    use agent_sdk::conversation::NewConversation;
    use agent_sdk::{AgentBackend, ConversationBackend, WorkspaceBackend};
    let memory = Arc::new(Memory::default());
    let peer = Peer::new(30);
    let probe = peer.agent();
    let mut app = App::open(AppConfig::new(
        memory.store(),
        vec![AgentSpec::new("writer".into(), "r1".into(), peer.agent())],
        vec![Cases::counter()],
    ))
    .await
    .unwrap();
    app.create_workspace(Cases::config()).await.unwrap();
    let workspace_id = peer.workspace_observation().id().clone();
    let workspace = probe.get_workspace(workspace_id.clone()).await.unwrap();
    let first = workspace
        .new_conversation(NewConversation::new(Some("first".into()), None, Vec::new()).unwrap())
        .await
        .unwrap();
    let second = workspace
        .new_conversation(NewConversation::new(Some("second".into()), None, Vec::new()).unwrap())
        .await
        .unwrap();
    let first_id = first.id();
    let second_id = second.id();
    assert_ne!(first_id, second_id);
    assert_eq!(first.info().await.unwrap().title(), Some("first"));
    assert_eq!(peer.conversation_requests()[1].title(), Some("second"));
    first.close().await.unwrap();
    assert!(
        matches!(workspace.get_conversation(first_id).await,Err(error) if error.kind()==agent_sdk::error::ErrorKind::Closed)
    );
    assert!(second.info().await.is_ok());
    workspace.close().await.unwrap();
    assert!(
        matches!(second.info().await,Err(error) if error.kind()==agent_sdk::error::ErrorKind::Closed)
    );
    let reopened = probe.get_workspace(workspace_id).await.unwrap();
    let retained = reopened.get_conversation(second_id).await.unwrap();
    assert_eq!(retained.info().await.unwrap().title(), Some("second"));
    app.shutdown(Cases::shutdown()).await.unwrap();
    assert!(
        matches!(probe.info().await,Err(error) if error.kind()==agent_sdk::error::ErrorKind::Closed)
    );
    assert!(
        matches!(retained.info().await,Err(error) if error.kind()==agent_sdk::error::ErrorKind::Closed)
    );
    assert_eq!(peer.shutdowns(), 1);
}

#[tokio::test]
async fn memory_fixture_overflow_invalidates_transaction_and_publishes_nothing() {
    let memory = Arc::new(Memory::default());
    let key = StateKey::new("fixture", "exhausted").unwrap();
    memory.retain_at_revision(
        StateRecord::new(key.clone(), json!(1)),
        storage_sdk::state::record::StateRevision::new(u64::MAX).unwrap(),
    );
    let store = memory.store();
    let before = store.load(key.clone()).await.unwrap().unwrap();
    let mut transaction = store.begin_transaction().await.unwrap();
    let other = StateKey::new("fixture", "other").unwrap();
    transaction
        .put(StateRecord::new(other.clone(), json!(2)))
        .await
        .unwrap();
    assert!(matches!(
        transaction
            .put(StateRecord::new(key.clone(), json!(3)))
            .await,
        Err(storage_sdk::StateStoreError::InvalidRequest(_))
    ));
    assert!(transaction.load(key.clone()).await.is_err());
    assert!(transaction.commit().await.is_err());
    assert_eq!(store.load(key).await.unwrap().unwrap(), before);
    assert!(store.load(other).await.unwrap().is_none());
}

#[tokio::test]
async fn caller_agent_fixture_stales_old_children_after_app_workspace_revision_change() {
    use agent_sdk::{AgentBackend, ConversationBackend, WorkspaceBackend};
    let memory = Arc::new(Memory::default());
    let peer = Peer::new(31);
    let probe = peer.agent();
    let mut app = App::open(AppConfig::new(
        memory.store(),
        vec![AgentSpec::new("writer".into(), "r1".into(), peer.agent())],
        vec![Cases::counter()],
    ))
    .await
    .unwrap();
    let application_workspace = app.create_workspace(Cases::config()).await.unwrap();
    let lower_id = peer.workspace_observation().id().clone();
    let old = probe.get_workspace(lower_id.clone()).await.unwrap();
    let conversation = old
        .new_conversation(
            agent_sdk::conversation::NewConversation::new(
                Some("retained".into()),
                None,
                Vec::new(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let id = conversation.id();
    let prior = application_workspace.snapshot().await.unwrap();
    application_workspace
        .replace(
            prior.revision,
            WorkspaceConfig {
                name: Some("updated".into()),
                root: prior.config.root,
            },
        )
        .await
        .unwrap();
    assert!(
        matches!(old.info().await,Err(error) if error.kind()==agent_sdk::error::ErrorKind::Conflict)
    );
    assert!(
        matches!(conversation.info().await,Err(error) if error.kind()==agent_sdk::error::ErrorKind::Conflict)
    );
    let reopened = probe.get_workspace(lower_id).await.unwrap();
    let current = reopened.get_conversation(id).await.unwrap();
    assert_eq!(current.info().await.unwrap().title(), Some("retained"));
    current.close().await.unwrap();
    reopened.close().await.unwrap();
    app.shutdown(Cases::shutdown()).await.unwrap();
}
