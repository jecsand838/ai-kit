//! Preparing a run admits a durable boundary without executing its entry node.
//! These tests exercise the existing admission transaction and run owners.

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::Duration;

use agents_kit::Agent;
use app_kit::workflow::{RunSelector, RunStatus, WorkflowDefinition, WorkflowState};
use app_kit::{App, AppConfig, Shutdown, WorkspaceConfig, WorkspaceId};
use serde_json::{Map, json};
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_kit::{StateStore, StateStoreBackend, StateTransactionBackend};

struct Preparation;

// This process exits after durable admission without running App/StateStore
// destructors. The parent always reaps it, including assertion/error paths.
struct PreparationProcess {
    directory: PathBuf,
    child: Option<Child>,
}

impl PreparationProcess {
    fn start() -> Self {
        Self::start_case("prepared_checkpoint_zero_survives_process_loss", None)
    }

    fn start_case(name: &str, boundary: Option<CrashBoundary>) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "app-prepared-recovery-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir(&directory).expect("isolated recovery directory");
        let child = Command::new(std::env::current_exe().expect("test executable"))
            .args(["--exact", name, "--nocapture"])
            .env("AI_KIT_PREPARED_RECOVERY_CHILD", &directory)
            .env(
                "AI_KIT_APP_CRASH_BOUNDARY",
                boundary.map(CrashBoundary::name).unwrap_or("prepared"),
            )
            .spawn()
            .expect("start prepared recovery child");
        Self {
            directory,
            child: Some(child),
        }
    }

    async fn wait(&mut self) {
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if let Some(status) = self
                    .child
                    .as_mut()
                    .expect("owned child")
                    .try_wait()
                    .expect("observe recovery child")
                {
                    self.child.take();
                    assert!(status.success(), "recovery child failed: {status}");
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("recovery child exits within its bound");
    }

    async fn app(directory: &Path) -> App<StateStore, Agent> {
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::directory(
            directory.join("app-state"),
        )))
        .await
        .expect("real persistent transactions");
        App::open(AppConfig::new(
            store,
            Vec::new(),
            vec![Preparation::definition()],
        ))
        .await
        .expect("open persistent App")
    }
}

impl Drop for PreparationProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            if let Err(error) = child.wait() {
                eprintln!(
                    "prepared recovery child reaping failed; retaining its directory: {error}"
                );
                return;
            }
        }
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

impl Preparation {
    fn definition() -> WorkflowDefinition {
        WorkflowDefinition::parse(
            &json!({
                "schema":"app-kit.workflow/v2", "id":"prepared", "revision":"r1",
                "state":{"count":0}, "required":["count"],
                "reducers":{"count":"reject_concurrent"}, "entry":"first",
                "nodes":[{"id":"first", "step":{"kind":"state", "mutations":[
                    {"kind":"assign", "target":"count", "value":{"kind":"literal", "value":99}}
                ]}}], "routes":{}
            })
            .to_string(),
        )
        .expect("state-only definition")
    }

    async fn app() -> App<StateStore, Agent> {
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("real memory transactions");
        App::open(AppConfig::new(store, Vec::new(), vec![Self::definition()]))
            .await
            .expect("state-only App")
    }
}

#[tokio::test]
async fn prepare_keeps_checkpoint_zero_parked_through_observers_and_cancellation() {
    let mut app = Preparation::app().await;
    let workspace = app
        .create_workspace(WorkspaceConfig {
            name: None,
            root: std::env::current_dir().expect("workspace path"),
        })
        .await
        .expect("workspace");
    let workflow = workspace
        .workflow("prepared".parse().expect("id"))
        .await
        .expect("workflow");
    let run = workflow
        .prepare(
            WorkflowState::new(Map::from_iter([("count".into(), json!(7))]))
                .expect("initial patch"),
        )
        .await
        .expect("durable checkpoint zero");
    let initial = run.snapshot().await.expect("prepared snapshot");
    assert_eq!(initial.checkpoint, 0);
    assert!(matches!(initial.status, RunStatus::Paused));
    assert_eq!(initial.state.values()["count"], 7);
    let observer = workflow
        .open_run(RunSelector::Latest(run.id().clone()))
        .await
        .expect("independent observer");
    let listed = workflow
        .list_runs(None, NonZeroUsize::new(10).expect("limit"))
        .await
        .expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].checkpoint, 0);
    assert!(matches!(listed[0].status, RunStatus::Paused));
    let waiting = tokio::time::timeout(Duration::from_secs(3), observer.wait())
        .await
        .expect("wait exposes the parked boundary")
        .expect("parked snapshot");
    assert_eq!(waiting.checkpoint, 0);
    assert_eq!(waiting.state.values()["count"], 7);
    assert!(matches!(waiting.status, RunStatus::Paused));
    // Give already accepted background/recovery work an opportunity to run.
    // A prepared boundary must remain stable under repeated public observation.
    assert!(
        tokio::time::timeout(Duration::from_millis(150), async {
            loop {
                let snapshot = observer.snapshot().await.expect("parked readback");
                assert_eq!(snapshot.checkpoint, 0);
                assert_eq!(snapshot.state.values()["count"], 7);
                assert!(matches!(snapshot.status, RunStatus::Paused));
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_err()
    );
    let accepted = run.cancel(0).await.expect("cancel prepared boundary");
    assert!(accepted.checkpoint > 0, "cancellation intent is committed");
    assert_eq!(accepted.state.values()["count"], 7);
    let cancelled = tokio::time::timeout(Duration::from_secs(3), observer.wait())
        .await
        .expect("accepted cancellation cleanup must settle")
        .expect("cancelled state");
    assert!(matches!(cancelled.status, RunStatus::Cancelled));
    assert_eq!(cancelled.state.values()["count"], 7);
    app.shutdown(Shutdown::CancelAndDrain {
        timeout: Duration::from_secs(3),
    })
    .await
    .expect("owned cancellation drains");
}

#[tokio::test]
async fn prepare_rejects_a_stale_workspace_before_allocating_a_visible_run() {
    let mut app = Preparation::app().await;
    let workspace = app
        .create_workspace(WorkspaceConfig {
            name: None,
            root: std::env::current_dir().expect("workspace path"),
        })
        .await
        .expect("workspace");
    let workflow = workspace
        .workflow("prepared".parse().expect("id"))
        .await
        .expect("workflow");
    let before = workspace
        .snapshot()
        .await
        .expect("current workspace revision");
    workspace
        .replace(
            before.revision,
            WorkspaceConfig {
                name: Some("new revision".into()),
                root: std::env::current_dir().expect("workspace path"),
            },
        )
        .await
        .expect("replace workspace");
    let result = workflow.prepare(WorkflowState::default()).await;
    assert!(matches!(result, Err(app_kit::AppError::Conflict(_))));
    let current = workspace
        .workflow("prepared".parse().expect("id"))
        .await
        .expect("current workflow");
    assert!(
        current
            .list_runs(None, NonZeroUsize::new(10).expect("limit"))
            .await
            .expect("list")
            .is_empty()
    );
    app.shutdown(Shutdown::CancelAndDrain {
        timeout: Duration::from_secs(3),
    })
    .await
    .expect("empty App drains");
}

#[tokio::test]
async fn prepared_checkpoint_zero_survives_process_loss() {
    if let Some(directory) = std::env::var_os("AI_KIT_PREPARED_RECOVERY_CHILD") {
        let directory = PathBuf::from(directory);
        let app = PreparationProcess::app(&directory).await;
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: directory.clone(),
            })
            .await
            .expect("durable workspace");
        let workflow = workspace
            .workflow("prepared".parse().expect("workflow id"))
            .await
            .expect("workflow");
        let run = workflow
            .prepare(
                WorkflowState::new(Map::from_iter([("count".into(), json!(7))]))
                    .expect("initial patch"),
            )
            .await
            .expect("durably prepared run");
        let snapshot = run.snapshot().await.expect("committed checkpoint zero");
        assert_eq!(snapshot.checkpoint, 0);
        assert_eq!(snapshot.state.values()["count"], 7);
        assert!(matches!(snapshot.status, RunStatus::Paused));
        std::fs::write(
            directory.join("identity.json"),
            json!({
                "workspace": workspace.id().to_string(), "run": run.id().to_string(),
                "occurrence": snapshot.occurrence,
            })
            .to_string(),
        )
        .expect("persist test-owned public identities");
        // Process loss is deliberate. Normal handle drop would signal shutdown
        // and would therefore test a different lifecycle transition.
        std::process::exit(0);
    }

    let mut process = PreparationProcess::start();
    process.wait().await;
    let identities: serde_json::Value = serde_json::from_slice(
        &std::fs::read(process.directory.join("identity.json")).expect("child identities"),
    )
    .expect("identity JSON");
    let workspace_id: WorkspaceId = identities["workspace"]
        .as_str()
        .expect("workspace id")
        .parse()
        .expect("validated workspace id");
    let run_id = identities["run"]
        .as_str()
        .expect("run id")
        .parse()
        .expect("validated run id");
    let mut app = PreparationProcess::app(&process.directory).await;
    let workspace = app
        .workspace(workspace_id)
        .await
        .expect("recovered workspace");
    let workflow = workspace
        .workflow("prepared".parse().expect("workflow id"))
        .await
        .expect("recovered workflow");
    let run = workflow
        .open_run(RunSelector::Exact {
            id: run_id,
            occurrence: identities["occurrence"]
                .as_str()
                .expect("exact occurrence")
                .to_owned(),
        })
        .await
        .expect("same durable run");
    let snapshot = run
        .wait()
        .await
        .expect("prepared recovery exposes checkpoint zero");
    assert_eq!(snapshot.checkpoint, 0);
    assert_eq!(snapshot.state.values()["count"], 7);
    assert!(matches!(snapshot.status, RunStatus::Paused));
    assert!(
        tokio::time::timeout(Duration::from_millis(150), async {
            loop {
                let snapshot = run.snapshot().await.expect("recovered parked truth");
                assert_eq!(snapshot.checkpoint, 0);
                assert_eq!(snapshot.state.values()["count"], 7);
                assert!(matches!(snapshot.status, RunStatus::Paused));
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_err()
    );
    run.cancel(0)
        .await
        .expect("explicit cancellation after recovery");
    let cancelled = tokio::time::timeout(Duration::from_secs(3), run.wait())
        .await
        .expect("recovered cancellation settles")
        .expect("cancelled snapshot");
    assert!(matches!(cancelled.status, RunStatus::Cancelled));
    assert_eq!(cancelled.state.values()["count"], 7);
    app.shutdown(Shutdown::CancelAndDrain {
        timeout: Duration::from_secs(3),
    })
    .await
    .expect("reopened App drains");
    drop(run);
    drop(workflow);
    drop(workspace);
    drop(app);
}

#[derive(Clone, Copy)]
enum CrashBoundary {
    Staged,
    InputStaged,
    BeforeBarrier,
    AfterBarrier,
}

impl CrashBoundary {
    fn name(self) -> &'static str {
        match self {
            Self::Staged => "staged",
            Self::InputStaged => "input-staged",
            Self::BeforeBarrier => "before-barrier",
            Self::AfterBarrier => "after-barrier",
        }
    }

    fn selects(self, record: &storage_kit::state::record::StateRecord) -> bool {
        if !record.key.key().starts_with("history/") || record.value["workflow_id"] != "prepared" {
            return false;
        }
        match self {
            Self::Staged | Self::InputStaged => record
                .value
                .pointer("/checkpoint/staged")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|staged| !staged.is_empty()),
            Self::BeforeBarrier | Self::AfterBarrier => {
                record
                    .value
                    .pointer("/checkpoint/phase/state")
                    .and_then(serde_json::Value::as_str)
                    == Some("routing")
                    && record
                        .value
                        .pointer("/checkpoint/outcomes")
                        .and_then(serde_json::Value::as_array)
                        .is_some_and(|outcomes| outcomes.len() == 1)
            }
        }
    }

    fn definition(self) -> WorkflowDefinition {
        if !matches!(self, Self::InputStaged) {
            return Preparation::definition();
        }
        WorkflowDefinition::parse(
            &json!({
                "schema":"app-kit.workflow/v2","id":"prepared","revision":"r1",
                "state":{"count":0},"required":["count"],"reducers":{"count":"reject_concurrent"},
                "entry":"input","nodes":[{"id":"input","step":{"kind":"input",
                    "message":[{"kind":"text","text":"Supply the retained input"}],
                    "schema":{"type":"integer"},"target":"count"}}],"routes":{}
            })
            .to_string(),
        )
        .unwrap()
    }

    async fn hold(self, directory: &std::path::Path) {
        use std::io::Write;
        let mut ready = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(directory.join("crash-ready.tmp"))
            .expect("one selected native boundary");
        ready.write_all(self.name().as_bytes()).unwrap();
        ready.sync_all().unwrap();
        std::fs::rename(
            directory.join("crash-ready.tmp"),
            directory.join("crash-ready"),
        )
        .unwrap();
        // The parent force-kills this process. No synthetic commit/error or
        // graceful Drop is allowed to substitute for that native crash.
        std::future::pending::<()>().await;
    }
}

struct CrashStore {
    inner: StateStore,
    boundary: Option<CrashBoundary>,
    directory: PathBuf,
}

struct CrashTransaction {
    inner: storage_kit::StateTransaction,
    boundary: Option<CrashBoundary>,
    directory: PathBuf,
    selected: bool,
}

impl StateStoreBackend for CrashStore {
    type Transaction = CrashTransaction;

    async fn load(
        &self,
        key: storage_kit::state::record::StateKey,
    ) -> Result<Option<storage_kit::state::record::StateRecordSnapshot>, storage_kit::StateStoreError>
    {
        self.inner.load(key).await
    }

    async fn list(
        &self,
        request: storage_kit::state::record::StateListRequest,
    ) -> Result<storage_kit::state::record::StateListResponse, storage_kit::StateStoreError> {
        self.inner.list(request).await
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, storage_kit::StateStoreError> {
        Ok(CrashTransaction {
            inner: self.inner.begin_transaction().await?,
            boundary: self.boundary,
            directory: self.directory.clone(),
            selected: false,
        })
    }
}

impl StateTransactionBackend for CrashTransaction {
    async fn load(
        &mut self,
        key: storage_kit::state::record::StateKey,
    ) -> Result<Option<storage_kit::state::record::StateRecordSnapshot>, storage_kit::StateStoreError>
    {
        self.inner.load(key).await
    }

    async fn put(
        &mut self,
        record: storage_kit::state::record::StateRecord,
    ) -> Result<(), storage_kit::StateStoreError> {
        self.selected |= self
            .boundary
            .is_some_and(|boundary| boundary.selects(&record));
        self.inner.put(record).await
    }

    async fn delete(
        &mut self,
        key: storage_kit::state::record::StateKey,
    ) -> Result<(), storage_kit::StateStoreError> {
        self.inner.delete(key).await
    }

    async fn commit(self) -> Result<(), storage_kit::StateStoreError> {
        if self.selected
            && let Some(boundary @ CrashBoundary::BeforeBarrier) = self.boundary
        {
            boundary.hold(&self.directory).await;
        }
        self.inner.commit().await?;
        if self.selected
            && let Some(boundary) = self.boundary
        {
            boundary.hold(&self.directory).await;
        }
        Ok(())
    }

    async fn rollback(self) -> Result<(), storage_kit::StateStoreError> {
        self.inner.rollback().await
    }
}

struct CrashTyping {
    directory: PathBuf,
}

impl app_kit::workflow::StateTyping for CrashTyping {
    type Data = WorkflowState;

    fn construct_data(&self, state: &WorkflowState) -> Result<Self::Data, app_kit::AppError> {
        Ok(state.clone())
    }

    fn construct_state(&self, data: &Self::Data) -> Result<WorkflowState, app_kit::AppError> {
        Ok(data.clone())
    }

    fn validate_post(
        &self,
        _: &WorkflowState,
        _: &WorkflowState,
        _: &app_kit::workflow::StateChange,
    ) -> Result<(), app_kit::AppError> {
        use std::io::Write;
        let mut effects = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.directory.join("effects"))
            .unwrap();
        writeln!(effects, "state-action").unwrap();
        effects.sync_all().unwrap();
        Ok(())
    }
}

impl PreparationProcess {
    async fn crash_app(
        directory: &std::path::Path,
        boundary: CrashBoundary,
        armed: bool,
    ) -> App<
        CrashStore,
        Agent,
        app_kit::workflow::NoActions,
        app_kit::workflow::NoConditions,
        CrashTyping,
    > {
        let config =
            StateStoreConfig::Lance(LanceStateStoreConfig::directory(directory.join("state")));
        let inner = StateStore::connect(config)
            .await
            .expect("selected real crash store");
        App::open(
            AppConfig::new(
                CrashStore {
                    inner,
                    boundary: armed.then_some(boundary),
                    directory: directory.to_owned(),
                },
                Vec::new(),
                vec![boundary.definition()],
            )
            .with_state_typing(CrashTyping {
                directory: directory.to_owned(),
            }),
        )
        .await
        .unwrap()
    }

    async fn force_at_boundary(&mut self, boundary: CrashBoundary) {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                assert!(
                    self.child.as_mut().unwrap().try_wait().unwrap().is_none(),
                    "crash child cannot exit before the selected native boundary"
                );
                if let Ok(ready) = std::fs::read_to_string(self.directory.join("crash-ready")) {
                    assert_eq!(ready, boundary.name());
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("real transaction reaches the selected crash boundary");
        let child = self.child.as_mut().unwrap();
        child.kill().unwrap();
        let status = child.wait().unwrap();
        self.child.take();
        assert!(!status.success(), "parent actually force-kills the child");
    }
}

impl PreparationProcess {
    async fn crash_case(test_name: &str) {
        use app_kit::workflow::{CheckpointKind, ExecutionOptions};
        use futures_util::FutureExt;
        if let Some(directory) = std::env::var_os("AI_KIT_PREPARED_RECOVERY_CHILD") {
            let directory = PathBuf::from(directory);
            let boundary = match std::env::var("AI_KIT_APP_CRASH_BOUNDARY").unwrap().as_str() {
                "staged" => CrashBoundary::Staged,
                "input-staged" => CrashBoundary::InputStaged,
                "before-barrier" => CrashBoundary::BeforeBarrier,
                "after-barrier" => CrashBoundary::AfterBarrier,
                _ => panic!("unknown crash boundary"),
            };
            let app = PreparationProcess::crash_app(&directory, boundary, true).await;
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: None,
                    root: directory.clone(),
                })
                .await
                .unwrap();
            let workflow = workspace
                .workflow("prepared".parse().unwrap())
                .await
                .unwrap();
            let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
            let prepared = run.snapshot().await.unwrap();
            std::fs::write(
                directory.join("identity.json"),
                json!({
                    "workspace": workspace.id().to_string(), "run": run.id().to_string(),
                    "occurrence": prepared.occurrence,
                })
                .to_string(),
            )
            .unwrap();
            let observer = workflow
                .open_run(RunSelector::Exact {
                    id: run.id().clone(),
                    occurrence: prepared.occurrence,
                })
                .await
                .unwrap();
            let completed = run
                .execution(ExecutionOptions::default())
                .await
                .unwrap()
                .finish()
                .await
                .unwrap();
            if matches!(boundary, CrashBoundary::InputStaged) {
                assert!(matches!(completed.status, RunStatus::Paused));
                let [
                    app_kit::workflow::Interaction::Input {
                        sequence,
                        generation,
                        ..
                    },
                ] = completed.interactions.as_slice()
                else {
                    panic!("prepared Input exposes its original target before resolution");
                };
                // Hold the actual Resolve Stage receipt after native commit.
                observer
                    .resolve(
                        *sequence,
                        *generation,
                        app_kit::workflow::Resolution::Input(json!(99)),
                    )
                    .await
                    .unwrap();
            }
            panic!("selected native boundary must remain held until force termination");
        }
        for boundary in [
            CrashBoundary::Staged,
            CrashBoundary::InputStaged,
            CrashBoundary::BeforeBarrier,
            CrashBoundary::AfterBarrier,
        ] {
            let mut process = PreparationProcess::start_case(test_name, Some(boundary));
            process.force_at_boundary(boundary).await;
            let first_effects = if matches!(boundary, CrashBoundary::InputStaged) {
                ""
            } else {
                "state-action\n"
            };
            assert_eq!(
                std::fs::read_to_string(process.directory.join("effects")).unwrap_or_default(),
                first_effects
            );
            let identity: serde_json::Value = serde_json::from_slice(
                &std::fs::read(process.directory.join("identity.json")).unwrap(),
            )
            .unwrap();
            let mut app = tokio::time::timeout(
                Duration::from_secs(10),
                PreparationProcess::crash_app(&process.directory, boundary, false),
            )
            .await
            .expect("native store and App reopen within the recovery budget");
            let observed = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(10), async {
            let workspace = app.workspace(identity["workspace"].as_str().unwrap().parse().unwrap()).await.unwrap();
            let workflow = workspace.workflow("prepared".parse().unwrap()).await.unwrap();
            let mut run = workflow.open_run(RunSelector::Exact { id: identity["run"].as_str().unwrap().parse().unwrap(), occurrence: identity["occurrence"].as_str().unwrap().to_owned() }).await.unwrap();
            let before = run.snapshot().await.unwrap();
            assert_eq!(before.state.values()["count"], if matches!(boundary, CrashBoundary::AfterBarrier) {99} else {0});
            assert_eq!(before.last_action_ordinal, if matches!(boundary, CrashBoundary::AfterBarrier) {Some(0)} else {None});
            let complete = run.execution(ExecutionOptions::default()).await.unwrap().finish().await.unwrap();
            assert!(matches!(complete.status, RunStatus::Succeeded));
            assert_eq!(complete.state.values()["count"], 99);
            assert_eq!(complete.last_action_ordinal, Some(0));
            let history = run.history(None, NonZeroUsize::new(100).unwrap()).await.unwrap();
            assert!(history.next.is_none());
            assert_eq!(history.checkpoints.iter().filter(|row| matches!(row.kind, CheckpointKind::Action)).count(), 1,
                "head/history/receipt reconciliation cannot duplicate the committed action");
            let expected_effects = match boundary {
                CrashBoundary::InputStaged => "",
                CrashBoundary::Staged | CrashBoundary::BeforeBarrier => "state-action\nstate-action\n",
                CrashBoundary::AfterBarrier => "state-action\n",
            };
            assert_eq!(std::fs::read_to_string(process.directory.join("effects")).unwrap_or_default(), expected_effects,
                "a fresh Execution discards recomputable State stages, but committed actions never replay");
            if matches!(boundary, CrashBoundary::InputStaged) {
                assert!(complete.interactions.is_empty());
                assert_eq!(complete.outcomes.len(), 1);
                assert_eq!(complete.outcomes[0].result["value"], 99,
                    "the originally accepted answer survives without another Input resolution");
            }
        })).catch_unwind().await;
            let closed = tokio::time::timeout(
                Duration::from_secs(5),
                app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(3),
                }),
            )
            .await;
            drop(app);
            match observed {
                Ok(result) => result.expect("recovered stage/barrier settles within its bound"),
                Err(panic) => std::panic::resume_unwind(panic),
            }
            closed.expect("reopened native owners join").unwrap();
        }
    }
}

#[tokio::test]
async fn staged_action_and_atomic_barrier_survive_forced_process_loss() {
    PreparationProcess::crash_case("staged_action_and_atomic_barrier_survive_forced_process_loss")
        .await;
}
