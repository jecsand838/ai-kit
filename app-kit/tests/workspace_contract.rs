mod workspace_support;

use std::sync::atomic::{AtomicBool, Ordering};
use std::{error::Error, num::NonZeroUsize, path::PathBuf, sync::Arc, time::Duration};

use agents_kit::Agent;
use app_kit::workflow::{
    CheckpointKind, CheckpointSelector, ExecutionOptions, Interaction, Resolution, RestoreOptions,
    ResumePosition, RunSelector, RunSnapshot, RunStatus, WorkflowDefinition, WorkflowId,
    WorkflowRun, WorkflowState,
};
use app_kit::{App, AppConfig, AppError, Shutdown, WorkspaceConfig, WorkspaceId};
use futures_util::FutureExt;
use serde_json::{Map, Value};
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_kit::state::record::{
    StateKey, StateListRequest, StateListResponse, StateRecord, StateRecordSnapshot,
};
use storage_kit::{
    StateStore, StateStoreBackend, StateStoreError, StateTransaction, StateTransactionBackend,
};

struct WorkspaceJourney {
    root: PathBuf,
}

impl WorkspaceJourney {
    fn new() -> Self {
        let stem =
            std::env::temp_dir().join(format!("ai-kit-app-workspace-{}", uuid::Uuid::new_v4()));
        let root = stem.with_extension("workspace");
        std::fs::create_dir_all(&root).expect("workspace fixture root must be created");
        Self { root }
    }

    fn config(&self, state: StateStore) -> AppConfig<StateStore, Agent> {
        let workflow = WorkflowDefinition::parse(WORKFLOW).expect("fixture workflow must parse");
        AppConfig::new(state, Vec::new(), vec![workflow])
    }

    fn history_config(&self, state: StateStore) -> AppConfig<StateStore, Agent> {
        let workflow = WorkflowDefinition::parse(WORKFLOW).expect("fixture workflow must parse");
        let unused = WorkflowDefinition::parse(&WORKFLOW.replace("workspace-flow", "unused-flow"))
            .expect("configured workflow without retained runs must parse");
        AppConfig::new(state, Vec::new(), vec![workflow, unused])
    }

    fn workspace(&self, name: &str) -> WorkspaceConfig {
        WorkspaceConfig {
            name: Some(name.to_owned()),
            root: self.root.clone(),
        }
    }

    async fn shutdown(app: &mut App<StateStore, Agent>) -> Result<(), AppError> {
        app.shutdown(Shutdown::Drain {
            timeout: Duration::from_secs(10),
        })
        .await
    }

    async fn wait(run: &WorkflowRun<StateStore, Agent>) -> Result<RunSnapshot, AppError> {
        let observed = tokio::time::timeout(Duration::from_secs(10), run.wait()).await;
        observed.map_err(|_| AppError::Timeout("workflow observation timed out".to_owned()))?
    }

    async fn workspace_count(&self, app: &App<StateStore, Agent>) -> Result<usize, AppError> {
        let mut count = 0;
        let mut after = None;
        let limit = NonZeroUsize::new(1_000).expect("the page limit must be nonzero");
        loop {
            let page = app.list_workspaces(after, limit).await?;
            let complete = page.len() < limit.get();
            count += page.len();
            after = page.last().map(|workspace| workspace.id.clone());
            if complete {
                return Ok(count);
            }
        }
    }
}

impl Drop for WorkspaceJourney {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

const WORKFLOW: &str = r#"
{
  "schema": "app-kit.workflow/v2",
  "id": "workspace-flow",
  "revision": "v1",
  "state": {"answer": null},
  "required": ["answer"],
  "reducers": {"answer": "reject_concurrent"},
  "entry": "ask",
  "nodes": [
    {
      "id": "ask",
      "step": {
        "kind": "input",
        "message": [{"kind": "text", "text": "Continue?"}],
        "schema": {"type": "string"},
        "target": "answer"
      }
    }
  ],
  "routes": {
    "ask": {"mode": "first", "branches": [{"target": {"kind": "end"}}]}
  }
}
"#;

#[tokio::test]
async fn workspace_configuration_is_prevalidated_revision_fenced_and_reopens_by_identity()
-> Result<(), Box<dyn Error + Send + Sync>> {
    WorkspaceJourney::new()
        .reopen_case(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
}

#[tokio::test]
async fn lance_state_memory_workspace_revision_fencing_and_reopen()
-> Result<(), Box<dyn Error + Send + Sync>> {
    WorkspaceJourney::new()
        .reopen_case(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
}

#[tokio::test]
async fn lance_state_directory_workspace_revision_fencing_and_reopen()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let fixture = WorkspaceJourney::new();
    let config =
        StateStoreConfig::Lance(LanceStateStoreConfig::directory(fixture.root.join("state")));
    fixture.reopen_case(config).await
}

impl WorkspaceJourney {
    async fn reopen_case(
        &self,
        config: StateStoreConfig,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let fixture = self;
        let store = StateStore::connect(config).await?;
        let app = App::open(fixture.config(store.clone())).await?;
        let (root, separator) = (fixture.root.display(), std::path::MAIN_SEPARATOR);
        let mut invalid = fixture.workspace("invalid-root");
        invalid.root = PathBuf::from(format!("{root}{separator}{separator}nested"));
        let invalid_root = app.create_workspace(invalid).await;
        assert!(matches!(invalid_root, Err(AppError::InvalidInput(_))));
        assert_eq!(fixture.workspace_count(&app).await?, 0);

        let workspace = app.create_workspace(fixture.workspace("before")).await?;
        let id: WorkspaceId = workspace.id().clone();
        let initial = workspace.snapshot().await?;
        let workflow_id: WorkflowId = "workspace-flow".parse()?;

        workspace
            .replace(initial.revision, fixture.workspace("after"))
            .await?;
        let stale = workspace
            .replace(initial.revision, fixture.workspace("stale"))
            .await;
        assert!(matches!(stale, Err(AppError::Conflict(_))));

        let workflow = workspace.workflow(workflow_id.clone()).await?;
        let run = workflow.start(ExecutionOptions::default()).await?;
        let input = |snapshot: RunSnapshot| match snapshot.interactions.as_slice() {
            [
                Interaction::Input {
                    sequence,
                    generation,
                    ..
                },
            ] => (*sequence, *generation),
            _ => panic!("the retention workflow must request its configured input"),
        };
        let (sequence, generation) = input(WorkspaceJourney::wait(&run).await?);
        let run_id = run.id().clone();
        drop(app);
        assert_eq!(input(run.snapshot().await?), (sequence, generation));
        drop((run, workflow, workspace));

        let mut app = App::open(fixture.config(store)).await?;
        let workspace = app.workspace(id).await?;
        let workflow = workspace.workflow(workflow_id).await?;
        let mut run = workflow
            .open_run(RunSelector::Latest(run_id.clone()))
            .await?;
        assert_eq!(input(run.snapshot().await?), (sequence, generation));
        let oversized = Value::String("x".repeat(1_048_576));
        let rejected = run
            .resolve(sequence, generation, Resolution::Input(oversized))
            .await;
        assert!(matches!(rejected, Err(AppError::InvalidInput(_))));
        assert_eq!(input(run.snapshot().await?), (sequence, generation));

        let competing = workflow
            .open_run(RunSelector::Latest(run_id.clone()))
            .await?;
        let answer = Value::String("retained".to_owned());
        let (left, right) = tokio::join!(
            run.resolve(sequence, generation, Resolution::Input(answer.clone())),
            competing.resolve(sequence, generation, Resolution::Input(answer)),
        );
        let left = left.expect("first exact decision settles or reattaches");
        let right = right.expect("concurrent identical decision replays the same receipt");
        assert_eq!(left.checkpoint, right.checkpoint);
        assert_eq!(left.state, right.state);
        assert_eq!(left.last_action_ordinal, right.last_action_ordinal);
        assert!(left.interactions.is_empty() && right.interactions.is_empty());
        let terminal = WorkspaceJourney::wait(&run).await?;
        assert!(matches!(terminal.status, RunStatus::Succeeded));
        let retained = workflow.list_runs(None, NonZeroUsize::MIN).await?;
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].id, run_id);
        assert!(matches!(retained[0].status, RunStatus::Succeeded));
        let action = run.checkpoint(CheckpointSelector::Action(0)).await?;
        assert_eq!(action.state.values()["answer"], "retained");
        tokio::time::timeout(Duration::from_secs(5), run.delete(terminal.checkpoint))
            .await
            .expect("explicit terminal run deletion must converge after reopen")?;
        assert!(
            workflow
                .list_runs(None, NonZeroUsize::MIN)
                .await?
                .is_empty()
        );
        assert!(matches!(run.snapshot().await, Err(AppError::NotFound(_))));
        WorkspaceJourney::shutdown(&mut app).await?;
        Ok(())
    }
}

#[tokio::test]
async fn recovery_rejects_workspace_inventory_above_the_private_bound()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let fixture = WorkspaceJourney::new();
    let store =
        StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    let app = App::open(fixture.config(store.clone())).await?;
    let workspace = app
        .create_workspace(fixture.workspace("inventory-template"))
        .await?;
    let source_id = workspace.id().to_string();
    let source_key = StateKey::new("app-kit", format!("workspace/{source_id}"))?;
    let template = store
        .load(source_key)
        .await?
        .expect("the public workspace admission must create its durable record")
        .record
        .value;
    drop(workspace);
    drop(app);

    let mut next = 0_usize;
    while next < 10_000 {
        let end = next.saturating_add(256).min(10_000);
        let mut transaction = store.begin_transaction().await?;
        for index in next..end {
            let id = format!("appws_00000000-0000-4000-8000-{index:012x}");
            assert_ne!(
                id, source_id,
                "the deterministic corruption id must be unique"
            );
            let mut value = template.as_ref().clone();
            *value
                .pointer_mut("/id")
                .expect("the public workspace record must contain its identity") =
                Value::String(id.clone());
            transaction
                .put(StateRecord::new(
                    StateKey::new("app-kit", format!("workspace/{id}"))?,
                    value,
                ))
                .await?;
        }
        transaction.commit().await?;
        next = end;
    }

    let error = match App::open(fixture.config(store)).await {
        Ok(_) => panic!("a durable workspace inventory above the private bound must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        AppError::Integration(ref message)
            if message == "durable workspace inventory exceeds the private bound"
    ));
    Ok(())
}

// This controlled lower-boundary timeout proves App error handling. Native
// Lance admission/deadline guarantees are exercised by Storage's own tests.
struct DeletionTimeoutStore {
    inner: StateStore,
    timeout_once: Arc<AtomicBool>,
}

impl StateStoreBackend for DeletionTimeoutStore {
    type Transaction = StateTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.inner.load(key).await
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        self.inner.list(request).await
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        if self.timeout_once.swap(false, Ordering::SeqCst) {
            return Err(StateStoreError::timed_out("begin_transaction", 50));
        }
        self.inner.begin_transaction().await
    }
}

#[tokio::test]
async fn workspace_delete_is_fenced_bounded_and_removes_only_app_coordination()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let fixture = WorkspaceJourney::new();
    let timeout_once = Arc::new(AtomicBool::new(false));
    let store =
        StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    let mut app = App::open(AppConfig::<_, Agent>::new(
        DeletionTimeoutStore {
            inner: store,
            timeout_once: Arc::clone(&timeout_once),
        },
        Vec::new(),
        vec![WorkflowDefinition::parse(WORKFLOW)?],
    ))
    .await?;
    let mut workspace = app.create_workspace(fixture.workspace("delete")).await?;
    let id = workspace.id().clone();
    let revision = workspace.snapshot().await?.revision;
    assert!(matches!(
        workspace.delete(revision + 1).await,
        Err(AppError::Conflict(_))
    ));
    timeout_once.store(true, Ordering::SeqCst);
    let exhausted = tokio::time::timeout(Duration::from_secs(5), workspace.delete(revision)).await;
    assert!(matches!(exhausted, Ok(Err(AppError::Timeout(_)))));
    assert!(
        !timeout_once.load(Ordering::SeqCst),
        "deletion observed the selected lower timeout"
    );
    assert_eq!(workspace.snapshot().await?.revision, revision);
    workspace.delete(revision).await?;
    let missing = app.workspace(id).await;
    assert!(matches!(missing, Err(AppError::NotFound(_))));
    app.shutdown(Shutdown::Drain {
        timeout: Duration::from_secs(10),
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn workspace_replacement_rejects_run_admitted_during_advisory_preflight() {
    workspace_support::AgentRevisionCase::replacement_rechecks_admission_after_preflight().await;
}

#[tokio::test]
async fn workspace_replacement_fences_run_admitted_before_native_snapshot() {
    workspace_support::AgentRevisionCase::replacement_preserves_fence_across_snapshot(
        workspace_support::AdmissionBoundary::BeforeSnapshot,
    )
    .await;
}

#[tokio::test]
async fn workspace_replacement_fences_run_admitted_after_native_snapshot() {
    workspace_support::AgentRevisionCase::replacement_preserves_fence_across_snapshot(
        workspace_support::AdmissionBoundary::AfterSnapshot,
    )
    .await;
}

#[tokio::test]
async fn workspace_mutations_and_run_admission_share_one_durable_fence()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let fixture = WorkspaceJourney::new();
    let mut app = App::open(
        fixture.config(
            StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
                .await
                .expect("caller memory store"),
        ),
    )
    .await?;
    let workspace = app.create_workspace(fixture.workspace("fenced")).await?;
    let competing = app.workspace(workspace.id().clone()).await?;
    let workflow_id: WorkflowId = "workspace-flow".parse()?;
    let workflow = workspace.workflow(workflow_id.clone()).await?;
    let current = workspace.snapshot().await?;

    let (started, replaced) = tokio::join!(
        workflow.start(ExecutionOptions::default()),
        competing.replace(current.revision, fixture.workspace("replaced")),
    );
    match (started, replaced) {
        (Ok(run), Err(AppError::Busy(_) | AppError::Conflict(_))) => {
            let snapshot = WorkspaceJourney::wait(&run).await?;
            run.cancel(snapshot.checkpoint).await?;
        }
        (Err(AppError::Busy(_) | AppError::Conflict(_)), Ok(_)) => {}
        (started, replaced) => {
            panic!(
                "exactly one workspace mutation or run admission must commit; started={}, replaced={}",
                started.is_ok(),
                replaced.is_ok(),
            );
        }
    }

    let mut workspace = app
        .create_workspace(fixture.workspace("delete-fence"))
        .await?;
    let workflow = workspace.workflow(workflow_id).await?;
    let revision = workspace.snapshot().await?.revision;

    let (started, deleted) = tokio::join!(
        workflow.start(ExecutionOptions::default()),
        workspace.delete(revision)
    );
    match (started, deleted) {
        (
            Err(
                AppError::Busy(_)
                | AppError::Conflict(_)
                | AppError::Closed(_)
                | AppError::NotFound(_),
            ),
            Ok(()),
        ) => {}
        (Ok(run), Err(AppError::Busy(_) | AppError::Conflict(_))) => {
            let snapshot = WorkspaceJourney::wait(&run).await?;
            run.cancel(snapshot.checkpoint).await?;
            workspace.delete(revision).await?;
        }
        (Ok(run), Ok(())) => {
            assert!(matches!(run.snapshot().await?.status, RunStatus::Cancelled));
        }
        (started, deleted) => {
            panic!(
                "workspace deletion race returned an invalid outcome; started={}, deleted={}",
                started.is_ok(),
                deleted.is_ok(),
            );
        }
    }

    WorkspaceJourney::shutdown(&mut app).await?;
    Ok(())
}

#[tokio::test]
async fn accepted_workspace_deletion_outlives_its_observer()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let fixture = WorkspaceJourney::new();
    let mut app = App::open(
        fixture.config(
            StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
                .await
                .expect("caller memory store"),
        ),
    )
    .await?;
    let mut workspace = app
        .create_workspace(fixture.workspace("owned-delete"))
        .await?;
    let observer = app.workspace(workspace.id().clone()).await?;
    let mut reattacher = app.workspace(workspace.id().clone()).await?;
    let workflow_id: WorkflowId = "workspace-flow".parse()?;
    let workflow = observer.workflow(workflow_id).await?;
    let run = workflow.start(ExecutionOptions::default()).await?;
    assert!(matches!(
        WorkspaceJourney::wait(&run).await?.interactions.as_slice(),
        [Interaction::Input { .. }]
    ));
    let revision = workspace.snapshot().await?.revision;

    let seal_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut deletion = Box::pin(workspace.delete(revision));
    let mut reattached = Box::pin(reattacher.delete(revision));
    let sealed = async {
        loop {
            match observer.snapshot().await {
                Ok(_) => {
                    assert!(
                        tokio::time::Instant::now() < seal_deadline,
                        "workspace deletion must publish its seal in time",
                    );
                    tokio::task::yield_now().await;
                }
                Err(AppError::Closed(_)) => break,
                Err(error) => panic!("workspace seal observation failed: {error}"),
            }
        }
    };
    tokio::pin!(sealed);
    tokio::select! {
        biased;
        () = &mut sealed => {}
        result = &mut deletion => {
            result?;
            panic!("workspace deletion completed before its observer was dropped");
        }
        result = &mut reattached => {
            result?;
            panic!("same-fence reattachment completed before the owned deletion");
        }
    }
    drop(deletion);
    reattached.await?;

    let history_workspace = app.workspace(workspace.id().clone()).await?;
    assert!(matches!(
        history_workspace.snapshot().await,
        Err(AppError::NotFound(_))
    ));
    assert_eq!(fixture.workspace_count(&app).await?, 0);
    let retained = history_workspace
        .workflow("workspace-flow".parse()?)
        .await?
        .open_run(RunSelector::Latest(run.id().clone()))
        .await?;
    assert!(matches!(
        retained.snapshot().await?.status,
        RunStatus::Cancelled
    ));
    WorkspaceJourney::shutdown(&mut app).await?;
    Ok(())
}

#[tokio::test]
async fn compensation_preserves_foreign_revision() -> Result<(), Box<dyn std::error::Error>> {
    workspace_support::CompensationCase::preserves_foreign_change("foreign").await
}

#[tokio::test]
async fn agent_revisions_coexist_and_caller_order_selects_new_work() {
    workspace_support::AgentRevisionCase::caller_order_selects_new_work().await;
}

#[tokio::test]
async fn agent_revision_deployment_preserves_prepared_and_restored_bindings() {
    workspace_support::AgentRevisionCase::deployment_preserves_prepared_and_restored_bindings()
        .await;
}

#[tokio::test]
async fn interrupted_agent_revision_extension_preserves_applied_old_bindings() {
    workspace_support::AgentRevisionCase::interrupted_deployment_preserves_applied_bindings().await;
}

#[tokio::test]
async fn missing_agent_revision_rejects_before_claim_or_lower_effects() {
    workspace_support::AgentRevisionCase::missing_pin_rejects_before_claim_or_lower_effects().await;
}

#[tokio::test]
async fn compensation_preserves_equal_configuration_foreign_revision()
-> Result<(), Box<dyn std::error::Error>> {
    workspace_support::CompensationCase::preserves_foreign_change("next").await
}

#[tokio::test]
async fn compensation_does_not_adopt_foreign_prior_configuration()
-> Result<(), Box<dyn std::error::Error>> {
    workspace_support::CompensationCase::preserves_foreign_change("prior").await
}

#[tokio::test]
async fn replacement_does_not_adopt_foreign_matching_write_after_rejection()
-> Result<(), Box<dyn std::error::Error>> {
    workspace_support::CompensationCase::rejects_foreign_forward_receipt().await
}

#[tokio::test]
async fn workspace_operation_panic_settles_exact_observer_and_cleanup() -> Result<(), Box<dyn Error>>
{
    workspace_support::CompensationCase::operation_panic_is_observed().await
}

#[tokio::test]
async fn deleted_workspace_reopens_only_as_retained_history_without_admission() {
    let fixture = WorkspaceJourney::new();
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let mut app = App::open(fixture.history_config(store.clone()))
        .await
        .unwrap();
    let observed =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(10), async {
            let mut workspace = app
                .create_workspace(fixture.workspace("retained-history"))
                .await
                .unwrap();
            let workspace_id = workspace.id().clone();
            let workspace_revision = workspace.snapshot().await.unwrap().revision;
            let workflow = workspace
                .workflow("workspace-flow".parse().unwrap())
                .await
                .unwrap();
            let run = workflow.start(ExecutionOptions::default()).await.unwrap();
            let waiting = WorkspaceJourney::wait(&run).await.unwrap();
            let [
                Interaction::Input {
                    sequence,
                    generation,
                    ..
                },
            ] = waiting.interactions.as_slice()
            else {
                panic!("real input establishes the retained result");
            };
            run.resolve(
                *sequence,
                *generation,
                Resolution::Input(Value::String("retained".into())),
            )
            .await
            .unwrap();
            let terminal = WorkspaceJourney::wait(&run).await.unwrap();
            assert!(matches!(terminal.status, RunStatus::Succeeded));
            assert_eq!(terminal.state.values()["answer"], "retained");
            let scope = format!(
                "{}/workspace-flow/{}/{}",
                workspace_id,
                run.id(),
                terminal.occurrence
            );
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let head = store
                        .load(StateKey::new("app-kit", format!("run/{scope}")).unwrap())
                        .await
                        .unwrap()
                        .expect("terminal head retained");
                    if head.record.value.pointer("/retention/state")
                        == Some(&Value::String("retained".into()))
                        && store
                            .load(StateKey::new("app-kit", format!("active/{scope}")).unwrap())
                            .await
                            .unwrap()
                            .is_none()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .expect("terminal execution releases capacity before workspace deletion");
            let history = run
                .history(None, NonZeroUsize::new(1000).unwrap())
                .await
                .unwrap();
            assert!(history.next.is_none());
            assert!(history.checkpoints.len() >= 3);
            assert_eq!(
                history
                    .checkpoints
                    .iter()
                    .filter(|row| matches!(row.kind, CheckpointKind::Action))
                    .count(),
                1
            );
            let retained = history
                .checkpoints
                .into_iter()
                .map(|row| {
                    (
                        row.sequence,
                        row.action_ordinal,
                        row.position,
                        row.state.values().clone(),
                        row.outcomes,
                    )
                })
                .collect::<Vec<_>>();
            workspace.delete(workspace_revision).await.unwrap();
            assert_eq!(fixture.workspace_count(&app).await.unwrap(), 0);
            assert!(
                fixture.root.is_dir(),
                "workspace deletion preserves caller files"
            );
            assert_eq!(
                run.snapshot().await.unwrap().checkpoint,
                terminal.checkpoint
            );
            (
                workspace_id,
                workspace_revision,
                run.id().clone(),
                terminal.checkpoint,
                retained,
            )
        }))
        .catch_unwind()
        .await;
    let closed = tokio::time::timeout(
        Duration::from_secs(5),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        }),
    )
    .await;
    drop(app);
    let (workspace_id, workspace_revision, run_id, checkpoint, retained) = match observed {
        Ok(result) => result.expect("workspace deletion and retained history setup are bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    };
    closed.expect("old App joins").expect("old App cleanup");

    let mut fresh = tokio::time::timeout(
        Duration::from_secs(5),
        App::open(fixture.history_config(store.clone())),
    )
    .await
    .expect("fresh App admission is bounded")
    .expect("retained history survives App reopen");
    let mut orphaned_head = None;
    let observed =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(10), async {
            assert_eq!(fixture.workspace_count(&fresh).await.unwrap(), 0);
            let missing: WorkspaceId = format!("appws_{}", uuid::Uuid::new_v4())
                .parse()
                .unwrap();
            assert!(matches!(
                fresh.workspace(missing).await,
                Err(AppError::NotFound(_))
            ));
            let workspace = fresh
                .workspace(workspace_id.clone())
                .await
                .expect("retained run heads prove the deleted workspace's history scope");
            assert!(matches!(
                workspace.snapshot().await,
                Err(AppError::NotFound(_) | AppError::Closed(_))
            ));
            assert!(matches!(
                workspace
                    .replace(workspace_revision, fixture.workspace("forbidden-replace"))
                    .await,
                Err(AppError::NotFound(_) | AppError::Closed(_))
            ));
            assert!(
                matches!(
                    workspace.workflow("unused-flow".parse().unwrap()).await,
                    Err(AppError::NotFound(_))
                ),
                "a configured definition without retained runs cannot borrow another workflow's history scope"
            );
            let workflow = workspace
                .workflow("workspace-flow".parse().unwrap())
                .await
                .unwrap();
            let run = workflow.open_run(RunSelector::Latest(run_id)).await.unwrap();
            let snapshot = run.snapshot().await.unwrap();
            assert!(matches!(snapshot.status, RunStatus::Succeeded));
            assert_eq!(snapshot.checkpoint, checkpoint);
            assert_eq!(snapshot.state.values()["answer"], "retained");
            let action = run.checkpoint(CheckpointSelector::Action(0)).await.unwrap();
            assert!(matches!(action.kind, CheckpointKind::Action));
            assert_eq!(action.position.as_deref(), Some("ask"));
            assert_eq!(action.state.values()["answer"], "retained");
            assert_eq!(action.outcomes.len(), 1);
            let history = run
                .history(None, NonZeroUsize::new(1000).unwrap())
                .await
                .unwrap();
            assert!(history.next.is_none());
            assert_eq!(
                history
                    .checkpoints
                    .iter()
                    .filter(|row| matches!(row.kind, CheckpointKind::Action))
                    .count(),
                1
            );
            let actual = history
                .checkpoints
                .into_iter()
                .map(|row| {
                    (
                        row.sequence,
                        row.action_ordinal,
                        row.position,
                        row.state.values().clone(),
                        row.outcomes,
                    )
                })
                .collect::<Vec<_>>();
            assert!(
                actual == retained,
                "deleted workspace keeps exact immutable run history"
            );
            assert!(matches!(
                workflow.start(ExecutionOptions::default()).await,
                Err(AppError::NotFound(_) | AppError::Closed(_))
            ));
            let initial =
                WorkflowState::new(Map::from_iter([("answer".into(), Value::Null)])).unwrap();
            assert!(matches!(
                workflow.prepare(initial).await,
                Err(AppError::NotFound(_) | AppError::Closed(_))
            ));
            assert!(matches!(
                workflow
                    .restore(RestoreOptions {
                        source: RunSelector::Latest(run.id().clone()),
                        checkpoint: CheckpointSelector::Action(0),
                        overrides: WorkflowState::default(),
                        position: ResumePosition::AfterCompleted,
                        fallback: None,
                    })
                    .await,
                Err(AppError::NotFound(_) | AppError::Closed(_))
            ));
            let listed = workflow.list_runs(None, NonZeroUsize::MIN).await.unwrap();
            assert_eq!(listed.len(), 1);
            assert_eq!(&listed[0].id, run.id());
            assert!(matches!(listed[0].status, RunStatus::Succeeded));
            assert_eq!(run.snapshot().await.unwrap().checkpoint, checkpoint);
            assert_eq!(fixture.workspace_count(&fresh).await.unwrap(), 0);
            assert!(
                store
                    .load(StateKey::new("app-kit", format!("workspace/{workspace_id}")).unwrap())
                    .await
                    .unwrap()
                    .is_none(),
                "read-only attachment and rejected admission never recreate workspace coordination"
            );
            let unchanged = run
                .history(None, NonZeroUsize::new(1000).unwrap())
                .await
                .unwrap();
            assert!(unchanged.next.is_none());
            assert!(
                unchanged
                    .checkpoints
                    .into_iter()
                    .map(|row| (
                        row.sequence,
                        row.action_ordinal,
                        row.position,
                        row.state.values().clone(),
                        row.outcomes,
                    ))
                    .collect::<Vec<_>>()
                    == retained,
                "history reads and rejected start, prepare and restore produce no new action or state effects"
            );

            // An orphaned active head is not authority to recreate a history
            // scope. Restore the exact original bytes outside the timed body.
            let scope = format!("{}/workspace-flow/{}/{}", workspace_id, run.id(), snapshot.occurrence);
            let head = store
                .load(StateKey::new("app-kit", format!("run/{scope}")).unwrap())
                .await
                .unwrap()
                .unwrap()
                .record;
            let marker = StateKey::new("app-kit", format!("active/{scope}")).unwrap();
            assert!(store.load(marker.clone()).await.unwrap().is_none());
            assert_eq!(
                head.value.pointer("/retention/state"),
                Some(&Value::String("retained".into()))
            );
            orphaned_head = Some((head.clone(), marker.clone()));
            let mut active = head;
            Arc::make_mut(&mut active.value)["retention"] = serde_json::json!({"state":"active"});
            let mut transaction = store.begin_transaction().await.unwrap();
            transaction
                .put(StateRecord::new(marker, serde_json::json!({"epoch":active.value["epoch"]})))
                .await
                .unwrap();
            transaction.put(active).await.unwrap();
            transaction.commit().await.unwrap();
            assert!(
                matches!(
                    fresh.workspace(workspace_id).await,
                    Err(AppError::NotFound(_) | AppError::Closed(_) | AppError::Integration(_))
                ),
                "missing coordination plus an active head cannot grant a history-only scope"
            );
        }))
        .catch_unwind()
        .await;
    let repaired = match orphaned_head {
        Some((head, marker)) => Some(
            tokio::time::timeout(Duration::from_secs(5), async {
                let mut transaction = store.begin_transaction().await?;
                transaction.put(head).await?;
                transaction.delete(marker).await?;
                transaction.commit().await
            })
            .await,
        ),
        None => None,
    };
    let closed = tokio::time::timeout(
        Duration::from_secs(5),
        fresh.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        }),
    )
    .await;
    drop(fresh);
    if let Some(repaired) = repaired {
        repaired
            .expect("fault restoration is bounded")
            .expect("exact head restored before cleanup completes");
    }
    match observed {
        Ok(result) => result.expect("history-only scope observations remain bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    closed.expect("fresh App joins").expect("fresh App cleanup");
}

#[tokio::test]
async fn interrupted_agent_deployment_rejects_pending_and_unknown_required_old_pairs() {
    workspace_support::AgentRevisionCase::interrupted_deployment_rejects_unproven_bindings().await;
}

#[tokio::test]
async fn prepared_parent_pins_transitive_agent_children_across_revision_deployment() {
    workspace_support::AgentRevisionCase::transitive_children_preserve_agent_bindings().await;
}
