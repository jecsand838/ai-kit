//! Hand a structured artifact from one configured OpenCode Agent to another.
//!
//! Run with two already-running, model-configured services and an existing directory:
//! `cargo run -p app-kit --example multi_agent -- http://127.0.0.1:4096 http://127.0.0.1:4097 /path/to/workspace`
//! The caller owns those services. Separate memory stores make this a fresh-run
//! example; use persistent stores and explicit reopen for process recovery.

use std::error::Error;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use agents_kit::config::{AgentConfig, OpenCodeConfig};
use agents_kit::{Agent, AgentBackend, AgentBuilder, AgentStorage};
use app_kit::workflow::{ExecutionOptions, RunStatus, WorkflowDefinition};
use app_kit::{AgentSpec, App, AppConfig, AppError, Shutdown, WorkspaceConfig};
use storage_kit::StateStore;
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};

struct HandoffExample {
    endpoints: [String; 2],
    root: PathBuf,
}

impl HandoffExample {
    fn from_args() -> Result<Self, Box<dyn Error + Send + Sync>> {
        let arguments = std::env::args().skip(1).collect::<Vec<_>>();
        let [author, reviewer, root] = arguments.as_slice() else {
            return Err(AppError::InvalidInput(
                "expected AUTHOR_URL REVIEWER_URL EXISTING_WORKSPACE_DIRECTORY".into(),
            )
            .into());
        };
        let root = PathBuf::from(root).canonicalize()?;
        if !root.is_dir() {
            return Err(AppError::InvalidInput("workspace must be a directory".into()).into());
        }
        Ok(Self {
            endpoints: [author.clone(), reviewer.clone()],
            root,
        })
    }

    async fn agent(&self, index: usize) -> Result<Agent, Box<dyn Error + Send + Sync>> {
        let backend = OpenCodeConfig::external(
            &self.endpoints[index],
            Duration::from_secs(10),
            Duration::from_secs(120),
            Duration::from_secs(30),
        )?;
        let config = AgentConfig::new(
            backend.into(),
            Duration::from_secs(30),
            NonZeroUsize::new(1).unwrap(),
        )?;
        let store =
            StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
        Ok(AgentBuilder::create(config, AgentStorage::new(store))
            .build()
            .await?)
    }

    async fn run(self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let definition = WorkflowDefinition::parse(include_str!(
            "../docs/examples/multi_agent_sequential.json"
        ))?;
        let store =
            StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
        let author = self.agent(0).await?;
        let reviewer = match self.agent(1).await {
            Ok(agent) => agent,
            Err(error) => {
                // The author has not transferred to App yet.
                if let Err(cleanup) = author.shutdown().await {
                    eprintln!("Agent cleanup also failed: {cleanup}");
                }
                return Err(error);
            }
        };
        let agents = vec![
            AgentSpec::new("author".into(), "r1".into(), author),
            AgentSpec::new("reviewer".into(), "r1".into(), reviewer),
        ];
        // App consumes the already-built Agents through AgentBackend. Its open
        // failure path also owns cleanup after this transfer.
        let mut app = App::open(AppConfig::new(store, agents, vec![definition])).await?;
        let observed = tokio::time::timeout(Duration::from_secs(300), async {
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: Some("two-agent handoff".into()),
                    root: self.root,
                })
                .await?;
            let workflow = workspace.workflow("handoff".parse()?).await?;
            let run = workflow.start(ExecutionOptions::default()).await?;
            let snapshot = run.wait().await?;
            if !matches!(snapshot.status, RunStatus::Succeeded) {
                return Err(AppError::Integration(
                    "handoff did not finish; inspect its failure or resolve its caller interaction"
                        .into(),
                ));
            }
            let state = snapshot.state.values();
            if state["artifact"]
                .get("summary")
                .and_then(serde_json::Value::as_str)
                .is_none_or(str::is_empty)
                || state["review"].as_str().is_none_or(str::is_empty)
            {
                return Err(AppError::Integration(
                    "handoff returned an empty artifact or review".into(),
                ));
            }
            println!(
                "Author artifact: {}\nReviewer response: {}",
                state["artifact"], state["review"]
            );
            Ok::<(), AppError>(())
        })
        .await;
        let closed = tokio::time::timeout(
            Duration::from_secs(130),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(120),
            }),
        )
        .await;
        drop(app);
        observed??;
        closed??;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    HandoffExample::from_args()?.run().await
}
