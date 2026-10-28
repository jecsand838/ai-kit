//! Inspect a partial, halt after a commit, then resume with new transient inputs.
//! Run with `cargo run -p app-kit --example controls_and_streaming`.

mod invoice;

use std::error::Error;
use std::path::PathBuf;
use std::time::Duration;

use agents_kit::Agent;
use app_kit::workflow::{
    ExecutionEvent, ExecutionOptions, RunSelector, RunStatus, WorkflowDefinition, WorkflowState,
};
use app_kit::{App, AppConfig, AppError, Shutdown, WorkspaceConfig};
use invoice::InvoiceAction;
use serde_json::{Map, json};
use storage_kit::StateStore;
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};

struct ControlsExample;

impl ControlsExample {
    async fn run() -> Result<(), Box<dyn Error + Send + Sync>> {
        let definition =
            WorkflowDefinition::parse(include_str!("../docs/examples/invoice_controls.json"))?;
        let store =
            StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
        let mut app = App::open(
            AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition])
                .with_actions(vec![InvoiceAction::new()]),
        )
        .await?;
        let observed = tokio::time::timeout(Duration::from_secs(15), async {
            let workspace = app.create_workspace(WorkspaceConfig {
                name: Some("execution controls example".into()),
                root: PathBuf::from(env!("CARGO_MANIFEST_DIR")),
            }).await?;
            let workflow = workspace.workflow("invoice_controls".parse()?).await?;
            let mut run = workflow.prepare(WorkflowState::default()).await?;
            let observer = workflow.open_run(RunSelector::Latest(run.id().clone())).await?;
            let mut execution = run.execution(ExecutionOptions {
                inputs: Map::from_iter([("quantity".into(), json!(6))]),
                halt_after: vec!["calculate".into()],
                ..ExecutionOptions::default()
            }).await?;
            if !matches!(execution.next().await?, Some(ExecutionEvent::Partial { node, result })
                if node == "calculate" && result.get("progress") == Some(&json!("calculated")))
                || observer.snapshot().await?.state.values()["total"] != 0
            {
                return Err(AppError::Integration("a partial must leave committed total unchanged".into()));
            }
            if !matches!(execution.next().await?, Some(ExecutionEvent::Committed { outcomes, .. })
                if outcomes.len() == 1 && outcomes[0].result.get("total") == Some(&json!(42)))
            {
                return Err(AppError::Integration("expected the first invoice commit".into()));
            }
            if !matches!(execution.next().await?, Some(ExecutionEvent::Halted { node, result: Some(result) })
                if node == "calculate" && result.get("total") == Some(&json!(42)))
            {
                return Err(AppError::Integration("expected the selected after halt and its result".into()));
            }
            // finish releases this execution's authority. The saved next node
            // remains pending, and new inputs belong only to the next session.
            let halted = execution.finish().await?;
            if !matches!(halted.status, RunStatus::Halted { .. })
                || halted.state.values()["total"] != 42
            {
                return Err(AppError::Integration("the halt must retain the committed total".into()));
            }
            let terminal = run.execution(ExecutionOptions {
                inputs: Map::from_iter([("quantity".into(), json!(7))]),
                ..ExecutionOptions::default()
            }).await?.finish().await?;
            if !matches!(terminal.status, RunStatus::Succeeded)
                || terminal.state.values()["total"] != 49
                || terminal.outcomes.len() != 1
                || terminal.outcomes[0].node != "recalculate"
            {
                return Err(AppError::Integration("resume must commit the pending node with new inputs".into()));
            }
            println!("Partial kept total 0; first commit and halt retained 42; resumed total is 49");
            Ok::<(), AppError>(())
        }).await;
        let closed = tokio::time::timeout(
            Duration::from_secs(15),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(10),
            }),
        )
        .await;
        drop(app);
        observed.map_err(|_| AppError::Timeout("controls execution timed out".into()))??;
        closed.map_err(|_| AppError::Timeout("controls cleanup timed out".into()))??;
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    ControlsExample::run().await
}
