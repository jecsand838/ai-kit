//! A local caller action with reusable inputs, a partial and explicit state writes.
//! Run with `cargo run -p app-sdk --example actions_and_inputs`.

mod invoice;

use invoice::InvoiceAction;
use std::error::Error;
use std::path::PathBuf;
use std::time::Duration;

use agent_sdk::Agent;
use app_sdk::workflow::{
    ExecutionEvent, ExecutionOptions, RunStatus, WorkflowDefinition, WorkflowState,
};
use app_sdk::{App, AppConfig, AppError, Shutdown, WorkspaceConfig};
use serde_json::{Map, Value, json};
use storage_sdk::StateStore;
use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};

struct InvoiceExample;

impl InvoiceExample {
    async fn run() -> Result<(), Box<dyn Error + Send + Sync>> {
        let definition = WorkflowDefinition::parse(
            &json!({
                "schema":"app-sdk.workflow/v2", "id":"invoice", "revision":"r1",
                "state":{"unit_price":7,"total":0}, "required":["unit_price","total"],
                "reducers":{"unit_price":"reject_concurrent","total":"reject_concurrent"},
                "entry":"calculate",
                "nodes":[{"id":"calculate","step":{
                    "kind":"action","action":"invoice","revision":"r1","project_result":false
                }}], "routes":{}
            })
            .to_string(),
        )?;
        let store =
            StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
        // Native caller actions need no Agent value or provider service.
        let mut app = App::open(
            AppConfig::<_, Agent>::new(store, Vec::new(), vec![definition])
                .with_actions(vec![InvoiceAction::new()]),
        )
        .await?;
        let observed = tokio::time::timeout(Duration::from_secs(15), async {
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: Some("invoice example".into()),
                    root: PathBuf::from(env!("CARGO_MANIFEST_DIR")),
                })
                .await?;
            let workflow = workspace.workflow("invoice".parse()?).await?;
            let mut run = workflow.prepare(WorkflowState::default()).await?;
            let mut execution = run
                .execution(ExecutionOptions {
                    inputs: Map::from_iter([
                        ("quantity".into(), json!(6)),
                        ("trace".into(), json!("sample")),
                    ]),
                    ..ExecutionOptions::default()
                })
                .await?;
            let Some(ExecutionEvent::Partial { result, .. }) = execution.next().await? else {
                return Err(AppError::Integration(
                    "expected the real action partial".into(),
                ));
            };
            if result.get("progress") != Some(&json!("calculated")) {
                return Err(AppError::Integration("partial changed".into()));
            }
            let Some(ExecutionEvent::Committed { outcomes, .. }) = execution.step().await? else {
                return Err(AppError::Integration(
                    "expected one committed boundary".into(),
                ));
            };
            let expected =
                json!({"quantity":6,"label":"order","currency":"USD","trace":"sample","total":42});
            if outcomes.len() != 1 || Value::Object(outcomes[0].result.clone()) != expected {
                return Err(AppError::Integration(
                    "resolved inputs or action result changed".into(),
                ));
            }
            let terminal = execution.finish().await?;
            if !matches!(terminal.status, RunStatus::Succeeded)
                || terminal.state.values()
                    != json!({"unit_price":7,"total":42}).as_object().unwrap()
            {
                return Err(AppError::Integration(
                    "only the explicit total write should persist".into(),
                ));
            }
            println!(
                "Partial: calculated; committed total: 42; transient inputs stayed out of state"
            );
            Ok::<(), AppError>(())
        })
        .await;
        let closed = tokio::time::timeout(
            Duration::from_secs(15),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(10),
            }),
        )
        .await;
        drop(app);
        observed.map_err(|_| AppError::Timeout("invoice execution timed out".into()))??;
        closed.map_err(|_| AppError::Timeout("invoice cleanup timed out".into()))??;
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    InvoiceExample::run().await
}
