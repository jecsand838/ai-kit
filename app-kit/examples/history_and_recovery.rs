//! Copy retained state into a parked fork, delete its source, and reopen the fork.
//! Run with `cargo run -p app-kit --example history_and_recovery`.
//!
//! Memory LanceDB keeps this example service-free. Reopening the App uses the same
//! caller-owned store; surviving process exit requires a persistent store instead.

use std::error::Error;
use std::time::Duration;

use agents_kit::Agent;
use app_kit::workflow::{
    CheckpointKind, CheckpointSelector, ExecutionOptions, RestoreOptions, ResumePosition,
    RunSelector, RunStatus, WorkflowDefinition, WorkflowState,
};
use app_kit::{App, AppConfig, AppError, Shutdown, WorkspaceConfig, WorkspaceId};
use serde_json::{Map, json};
use storage_kit::StateStore;
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};

struct HistoryExample {
    store: StateStore,
}

impl HistoryExample {
    async fn open() -> Result<Self, Box<dyn Error + Send + Sync>> {
        let store =
            StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
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
    ) -> Result<(WorkspaceId, RunSelector, RunSelector), Box<dyn Error + Send + Sync>> {
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
        Ok((
            workspace.id().clone(),
            source_selector,
            RunSelector::Exact {
                id: fork.id().clone(),
                occurrence: completed.occurrence,
            },
        ))
    }

    async fn read_reopened(
        &self,
        app: &App<StateStore, Agent>,
        workspace: WorkspaceId,
        source: RunSelector,
        fork: RunSelector,
    ) -> Result<(), AppError> {
        let workspace = app.workspace(workspace).await?;
        let workflow = workspace.workflow("history-counter".parse()?).await?;
        let fork = workflow.open_run(fork).await?;
        let committed = fork.checkpoint(CheckpointSelector::Action(1)).await?;
        let lineage = committed
            .lineage
            .as_ref()
            .ok_or_else(|| AppError::Integration("reopened fork lost its source lineage".into()))?;
        if committed.state.values()["count"] != 41
            || !matches!(&source, RunSelector::Exact { id, occurrence }
                if &lineage.source == id && &lineage.source_occurrence == occurrence)
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

    async fn run(self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut app = self.open_app().await?;
        let created = tokio::time::timeout(Duration::from_secs(15), self.create_fork(&app))
            .await
            .map_err(|_| AppError::Timeout("example execution timed out".into()));
        // Always join the admitted App before propagating an operation error.
        // Keep this Tokio runtime driven until cleanup finishes.
        let closed = self.close(&mut app).await;
        drop(app);
        let (workspace, source, fork) = created??;
        closed?;

        let mut reopened = self.open_app().await?;
        let observed = tokio::time::timeout(
            Duration::from_secs(15),
            self.read_reopened(&reopened, workspace, source, fork),
        )
        .await
        .map_err(|_| AppError::Timeout("example history read timed out".into()));
        let closed = self.close(&mut reopened).await;
        drop(reopened);
        observed??;
        closed?;
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    HistoryExample::open().await?.run().await
}
