//! Copy retained state into a parked fork, delete its source, and reopen the fork.
//! Run with `cargo run --locked -p app-kit --example history_and_recovery`.
//!
//! With no arguments, reopen over the same caller-owned memory store. To retain
//! the completed result across processes, run `create DIRECTORY`, then
//! `reopen DIRECTORY`. Both modes use embedded Lance without an external service.

use std::error::Error;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use agents_kit::Agent;
use app_kit::workflow::{
    CheckpointKind, CheckpointSelector, ExecutionOptions, RestoreOptions, ResumePosition, RunId,
    RunSelector, RunStatus, WorkflowDefinition, WorkflowId, WorkflowState,
};
use app_kit::{App, AppConfig, AppError, Shutdown, WorkspaceConfig, WorkspaceId};
use serde::{Deserialize, Serialize};
use serde_json::{Map, json};
use storage_kit::StateStore;
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};

enum Command {
    Memory,
    Create(PathBuf),
    Reopen(PathBuf),
}

impl Command {
    fn parse() -> Result<Self, AppError> {
        let arguments: Vec<_> = std::env::args_os().skip(1).collect();
        match arguments.as_slice() {
            [] => Ok(Self::Memory),
            [command, directory] if !directory.is_empty() && command == "create" => {
                Ok(Self::Create(PathBuf::from(directory)))
            }
            [command, directory] if !directory.is_empty() && command == "reopen" => {
                Ok(Self::Reopen(PathBuf::from(directory)))
            }
            _ => Err(AppError::Integration(
                "usage: history_and_recovery [create DIRECTORY | reopen DIRECTORY]".into(),
            )),
        }
    }

    fn run(self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let result = self.execute(&runtime);
        // An observation timeout does not cancel App's accepted finalizer.
        // After all command handles drop, keep this dedicated runtime driven
        // until its owned tasks settle, including on the error path. Native
        // cleanup may outlive the observation timeout; do not mistake a
        // retained terminal Timeout for permission to retry finalization.
        let metrics = runtime.metrics();
        runtime.block_on(async {
            while metrics.num_alive_tasks() != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        result
    }

    fn execute(
        self,
        runtime: &tokio::runtime::Runtime,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self {
            Self::Memory => runtime.block_on(async {
                HistoryExample::open(LanceStateStoreConfig::memory())
                    .await?
                    .run()
                    .await
            }),
            Self::Create(directory) => {
                let config = LanceStateStoreConfig::directory(directory.join("app-state"));
                config.validate()?;
                // Claim a new directory before connecting; never reuse or reset
                // caller data, including an existing empty directory.
                std::fs::create_dir(&directory)?;
                let receipt = runtime.block_on(async {
                    let example = HistoryExample::open(config).await?;
                    let created = example.create().await;
                    drop(example);
                    created
                })?;
                // All App and store owners have settled before publishing the
                // caller's lookup receipt. File I/O stays outside the runtime.
                receipt.save(&directory)?;
                println!("Saved fork {} at count 41", receipt.fork_id);
                Ok(())
            }
            Self::Reopen(directory) => {
                let receipt = SavedRun::read(&directory)?;
                let storage = directory.join("app-state");
                if !storage.is_dir() || std::fs::read_dir(&storage)?.next().is_none() {
                    return Err(AppError::Integration(
                        "persistent app-state storage is missing or empty".into(),
                    )
                    .into());
                }
                runtime.block_on(async {
                    let example =
                        HistoryExample::open(LanceStateStoreConfig::directory(storage)).await?;
                    let observed = example.reopen(&receipt).await;
                    drop(example);
                    observed
                })
            }
        }
    }
}

// Caller-owned public lookup identities, not a copy of App's storage records.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SavedRun {
    workspace: WorkspaceId,
    workflow: WorkflowId,
    source_id: RunId,
    source_occurrence: String,
    fork_id: RunId,
    fork_occurrence: String,
}

impl SavedRun {
    fn validate(&self) -> Result<(), AppError> {
        if self.workflow != "history-counter".parse()? {
            return Err(AppError::Integration(
                "saved workflow does not match this example".into(),
            ));
        }
        for occurrence in [&self.source_occurrence, &self.fork_occurrence] {
            if occurrence.len() != 32
                || !occurrence
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err(AppError::Integration(
                    "saved run occurrence must contain 32 lowercase hexadecimal digits".into(),
                ));
            }
        }
        Ok(())
    }

    fn read(directory: &Path) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let mut bytes = Vec::new();
        File::open(directory.join("run.json"))?
            .take(4097)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 4096 {
            return Err(AppError::Integration("saved run receipt is too large".into()).into());
        }
        let receipt: Self = serde_json::from_slice(&bytes)?;
        receipt.validate()?;
        Ok(receipt)
    }

    fn save(&self, directory: &Path) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.validate()?;
        let mut file = File::options()
            .write(true)
            .create_new(true)
            .open(directory.join("run.json"))?;
        file.write_all(&serde_json::to_vec_pretty(self)?)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        Ok(())
    }
}

struct HistoryExample {
    store: StateStore,
}

impl HistoryExample {
    async fn open(config: LanceStateStoreConfig) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let store = StateStore::connect(StateStoreConfig::Lance(config)).await?;
        Ok(Self { store })
    }

    async fn open_app(&self) -> Result<App<StateStore, Agent>, AppError> {
        let definition = WorkflowDefinition::parse(
            &json!({
                "schema":"app-kit.workflow/v2", "id":"history-counter", "revision":"r1",
                "state":{"count":0}, "required":["count"],
                "reducers":{"count":"reject_concurrent"}, "entry":"first",
                "nodes":[
                    {"id":"first", "step":{"kind":"state", "mutations":[{
                        "kind":"increment", "target":"count",
                        "value":{"kind":"literal", "value":1}
                    }]}},
                    {"id":"second", "step":{"kind":"state", "mutations":[{
                        "kind":"increment", "target":"count",
                        "value":{"kind":"literal", "value":1}
                    }]}}
                ],
                "routes":{"first":{"mode":"first", "branches":[{
                    "target":{"kind":"node", "id":"second"}
                }]}}
            })
            .to_string(),
        )?;
        // Agent supplies the static type for an empty inventory. No Agent value
        // is constructed, and these native State nodes need no lower service.
        App::open(AppConfig::<_, Agent>::new(
            self.store.clone(),
            Vec::new(),
            vec![definition],
        ))
        .await
    }

    async fn create_fork(
        &self,
        app: &App<StateStore, Agent>,
    ) -> Result<SavedRun, Box<dyn Error + Send + Sync>> {
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: Some("history example".into()),
                root: std::env::current_dir()?,
            })
            .await?;
        let workflow = workspace.workflow("history-counter".parse()?).await?;
        let mut source = workflow.prepare(WorkflowState::default()).await?;
        let completed = source
            .execution(ExecutionOptions::default())
            .await?
            .finish()
            .await?;
        if !matches!(completed.status, RunStatus::Succeeded)
            || completed.state.values()["count"] != 2
        {
            return Err(
                AppError::Integration("source did not complete both increments".into()).into(),
            );
        }

        // An action ordinal selects the whole committed boundary containing it;
        // it is independent of the checkpoint sequence and storage revision.
        let first = source.checkpoint(CheckpointSelector::Action(0)).await?;
        if !matches!(first.kind, CheckpointKind::Action)
            || first.position.as_deref() != Some("first")
            || first.state.values()["count"] != 1
        {
            return Err(
                AppError::Integration("first action history is inconsistent".into()).into(),
            );
        }
        let source_selector = RunSelector::Exact {
            id: source.id().clone(),
            occurrence: first.occurrence.clone(),
        };
        let mut fork = workflow
            .restore(RestoreOptions {
                source: source_selector.clone(),
                checkpoint: CheckpointSelector::Action(0),
                overrides: WorkflowState::new(Map::from_iter([("count".into(), json!(40))]))?,
                position: ResumePosition::AfterCompleted,
                fallback: None,
            })
            .await?;
        let parked = fork.snapshot().await?;
        if !matches!(parked.status, RunStatus::Paused)
            || parked.checkpoint != 0
            || parked.state.values()["count"] != 40
            || !parked.outcomes.is_empty()
        {
            return Err(
                AppError::Integration("restore did not create a parked clean copy".into()).into(),
            );
        }

        source.delete(completed.checkpoint).await?;
        if !matches!(source.snapshot().await, Err(AppError::NotFound(_))) {
            return Err(
                AppError::Integration("explicit source deletion did not finish".into()).into(),
            );
        }
        // Deletion does not invalidate the copied state. AfterCompleted skips
        // the first increment, so only the second changes the override: 40 -> 41.
        let completed = fork
            .execution(ExecutionOptions::default())
            .await?
            .finish()
            .await?;
        if !matches!(completed.status, RunStatus::Succeeded)
            || completed.state.values()["count"] != 41
            || completed.last_action_ordinal != Some(1)
        {
            return Err(
                AppError::Integration("fork resumed from the wrong boundary".into()).into(),
            );
        }
        println!("Source count: 2; copied count: 40; completed fork count: 41");
        Ok(SavedRun {
            workspace: workspace.id().clone(),
            workflow: "history-counter".parse()?,
            source_id: source.id().clone(),
            source_occurrence: first.occurrence,
            fork_id: fork.id().clone(),
            fork_occurrence: completed.occurrence,
        })
    }

    async fn read_reopened(
        &self,
        app: &App<StateStore, Agent>,
        receipt: &SavedRun,
    ) -> Result<(), AppError> {
        let workspace = app.workspace(receipt.workspace.clone()).await?;
        let workflow = workspace.workflow(receipt.workflow.clone()).await?;
        let fork = workflow
            .open_run(RunSelector::Exact {
                id: receipt.fork_id.clone(),
                occurrence: receipt.fork_occurrence.clone(),
            })
            .await?;
        let snapshot = fork.snapshot().await?;
        let committed = fork.checkpoint(CheckpointSelector::Action(1)).await?;
        let lineage = committed
            .lineage
            .as_ref()
            .ok_or_else(|| AppError::Integration("reopened fork lost its source lineage".into()))?;
        if !matches!(snapshot.status, RunStatus::Succeeded)
            || snapshot.state.values()["count"] != 41
            || snapshot.occurrence != receipt.fork_occurrence
            || committed.state.values()["count"] != 41
            || lineage.source != receipt.source_id
            || lineage.source_occurrence != receipt.source_occurrence
            || lineage.action_ordinal != Some(0)
        {
            return Err(AppError::Integration(
                "reopened fork history changed".into(),
            ));
        }
        println!(
            "Reopened fork {} at count 41; source lineage remains readable",
            fork.id()
        );
        Ok(())
    }

    async fn close(&self, app: &mut App<StateStore, Agent>) -> Result<(), AppError> {
        tokio::time::timeout(
            Duration::from_secs(15),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(10),
            }),
        )
        .await
        .map_err(|_| AppError::Timeout("example App cleanup timed out".into()))?
    }

    async fn create(&self) -> Result<SavedRun, Box<dyn Error + Send + Sync>> {
        let mut app = self.open_app().await?;
        let created = tokio::time::timeout(Duration::from_secs(15), self.create_fork(&app))
            .await
            .map_err(|_| AppError::Timeout("example execution timed out".into()));
        // Always join the admitted App before propagating an operation error.
        // Keep this Tokio runtime driven until cleanup finishes.
        let closed = self.close(&mut app).await;
        drop(app);
        if let Err(error) = &closed {
            eprintln!("App shutdown: {error}");
        }
        let receipt = created??;
        closed?;
        Ok(receipt)
    }

    async fn reopen(&self, receipt: &SavedRun) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut reopened = self.open_app().await?;
        let observed = tokio::time::timeout(
            Duration::from_secs(15),
            self.read_reopened(&reopened, receipt),
        )
        .await
        .map_err(|_| AppError::Timeout("example history read timed out".into()));
        let closed = self.close(&mut reopened).await;
        drop(reopened);
        if let Err(error) = &closed {
            eprintln!("App shutdown: {error}");
        }
        observed??;
        closed?;
        Ok(())
    }

    async fn run(self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let receipt = self.create().await?;
        self.reopen(&receipt).await
    }
}

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    Command::parse()?.run()
}
