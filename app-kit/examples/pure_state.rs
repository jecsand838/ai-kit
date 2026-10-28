//! Execute two native State nodes without constructing an Agent.
//! Run with `cargo run -p app-kit --example pure_state`.
//!
//! The caller supplies an in-memory LanceDB store. Its records live only while
//! that store remains alive; use a persistent caller-owned store for recovery
//! across process exit.

use std::error::Error;
use std::time::Duration;

use agents_kit::Agent;
use app_kit::workflow::{ExecutionOptions, RunStatus, WorkflowDefinition, WorkflowState};
use app_kit::{App, AppConfig, AppError, Shutdown, WorkspaceConfig};
use storage_kit::StateStore;
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};

struct PureStateExample {
    app: App<StateStore, Agent>,
}

impl PureStateExample {
    async fn open() -> Result<Self, Box<dyn Error + Send + Sync>> {
        let definition =
            WorkflowDefinition::parse(include_str!("../docs/examples/linear_state.json"))?;
        let store =
            StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
        // Agent supplies only the static type of the empty inventory. No Agent
        // value, credentials, provider, or external service is constructed.
        let app = App::open(AppConfig::<_, Agent>::new(
            store,
            Vec::new(),
            vec![definition],
        ))
        .await?;
        Ok(Self { app })
    }

    async fn execute(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let workspace = self
            .app
            .create_workspace(WorkspaceConfig {
                name: Some("pure state example".into()),
                root: std::env::current_dir()?,
            })
            .await?;
        let workflow = workspace.workflow("linear_state".parse()?).await?;
        let mut run = workflow.prepare(WorkflowState::default()).await?;
        // prepare persists checkpoint zero. The explicit execution advances
        // the two State nodes through their ordinary validation and commits.
        let completed = run
            .execution(ExecutionOptions::default())
            .await?
            .finish()
            .await?;
        if !matches!(completed.status, RunStatus::Succeeded)
            || completed.state.values()["count"] != 2
            || completed.state.values()["label"] != "complete"
        {
            return Err(AppError::Integration(
                "the linear workflow did not reach its expected committed state".into(),
            )
            .into());
        }
        println!("count: 2; label: complete");
        Ok(())
    }

    async fn run(mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let executed = tokio::time::timeout(Duration::from_secs(15), self.execute())
            .await
            .map_err(|_| AppError::Timeout("example execution timed out".into()));
        // Capture the execution result and keep the runtime driven through
        // owned cleanup before propagating either a failure or a timeout.
        let closed = tokio::time::timeout(
            Duration::from_secs(15),
            self.app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(10),
            }),
        )
        .await
        .map_err(|_| AppError::Timeout("example App cleanup timed out".into()));
        drop(self.app);
        executed??;
        closed??;
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    PureStateExample::open().await?.run().await
}
