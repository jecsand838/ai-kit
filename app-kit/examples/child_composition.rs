//! Run resolved child tasks with a concrete executor, then a synchronous Pipeline.
//!
//! `cargo run -p app-kit --example child_composition`
//! No Agent, external service or credentials are constructed. The caller's
//! runtime stays alive while Pipeline is driven outside an entered Tokio context.

use std::error::Error;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use agents_kit::Agent;
use app_kit::workflow::{
    ActionMode, ChildBatch, ChildCompletion, ChildExecutor, ExecutionOptions, RunSelector,
    RunStatus, SyncExecution, WorkflowDefinition, WorkflowState,
};
use app_kit::{App, AppConfig, AppError, Shutdown, WorkspaceConfig};
use serde_json::json;
use storage_kit::StateStore;
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};

struct ReverseExecutor {
    completed_batches: Arc<Mutex<Vec<Vec<usize>>>>,
}

struct ReverseChildren<F> {
    tasks: Vec<Pin<Box<F>>>,
    completed: Vec<ChildCompletion>,
    completed_batches: Arc<Mutex<Vec<Vec<usize>>>>,
}

impl ChildExecutor for ReverseExecutor {
    type Execution<'a, F>
        = ReverseChildren<F>
    where
        Self: 'a,
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a;

    fn execute<'a, F>(&'a self, batch: ChildBatch<F>) -> Self::Execution<'a, F>
    where
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a,
    {
        assert_eq!(batch.mode(), ActionMode::Asynchronous);
        ReverseChildren {
            tasks: batch
                .into_tasks()
                .expect("App supplies the original unpolled batch"),
            completed: Vec::new(),
            completed_batches: Arc::clone(&self.completed_batches),
        }
    }
}

impl<F: Future<Output = Result<ChildCompletion, AppError>>> Future for ReverseChildren<F> {
    type Output = Result<Vec<ChildCompletion>, AppError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let owner = self.get_mut();
        while let Some(task) = owner.tasks.last_mut() {
            let completed = match task.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(completed)) => completed,
            };
            drop(owner.tasks.pop());
            owner.completed.push(completed);
        }
        owner.completed_batches.lock().unwrap().push(
            owner
                .completed
                .iter()
                .map(ChildCompletion::ordinal)
                .collect(),
        );
        Poll::Ready(Ok(std::mem::take(&mut owner.completed)))
    }
}

struct CompositionExample;

impl CompositionExample {
    fn run(runtime: &tokio::runtime::Runtime) -> Result<(), AppError> {
        let completed_batches = Arc::new(Mutex::new(Vec::new()));
        let mut app = runtime.block_on(async {
            let definitions = [
                include_str!("../docs/examples/child_counter.json"),
                include_str!("../docs/examples/tasks.json"),
                include_str!("../docs/examples/pipeline.json"),
            ]
            .into_iter()
            .map(WorkflowDefinition::parse)
            .collect::<Result<Vec<_>, _>>()?;
            let store =
                StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
                    .await?;
            App::open(
                AppConfig::<_, Agent>::new(store, Vec::new(), definitions).with_child_executor(
                    ReverseExecutor {
                        completed_batches: Arc::clone(&completed_batches),
                    },
                ),
            )
            .await
        })?;

        let observed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let workspace = runtime.block_on(app.create_workspace(WorkspaceConfig {
                name: Some("child composition example".into()),
                root: std::env::current_dir().map_err(|_| {
                    AppError::InvalidInput("workspace directory is unavailable".into())
                })?,
            }))?;
            runtime.block_on(async {
                let workflow = workspace.workflow("tasks".parse()?).await?;
                let run = workflow.start(ExecutionOptions::default()).await?;
                let terminal = run.wait().await?;
                assert!(matches!(terminal.status, RunStatus::Succeeded));
                assert_eq!(terminal.state.values()["values"], json!([11, 21]));
                assert_eq!(terminal.last_action_ordinal, Some(0));
                assert_eq!(
                    *completed_batches.lock().unwrap(),
                    [vec![1, 0]],
                    "the executor's completion order differs from parent reduction order"
                );

                let children = workspace.workflow("child_counter".parse()?).await?;
                let first = children
                    .list_runs(None, NonZeroUsize::new(1).unwrap())
                    .await?
                    .pop()
                    .expect("first exact occurrence");
                let second = children
                    .list_runs(
                        Some(RunSelector::Exact {
                            id: first.id.clone(),
                            occurrence: first.occurrence.clone(),
                        }),
                        NonZeroUsize::new(1).unwrap(),
                    )
                    .await?
                    .pop()
                    .expect("second exact occurrence with the same logical task identity");
                assert_eq!(first.id, second.id);
                assert_ne!(first.occurrence, second.occurrence);
                assert!(
                    children
                        .list_runs(
                            Some(RunSelector::Exact {
                                id: second.id.clone(),
                                occurrence: second.occurrence.clone(),
                            }),
                            NonZeroUsize::new(1).unwrap()
                        )
                        .await?
                        .is_empty()
                );
                let mut values = Vec::new();
                for snapshot in [first, second] {
                    let child = children
                        .open_run(RunSelector::Exact {
                            id: snapshot.id.clone(),
                            occurrence: snapshot.occurrence.clone(),
                        })
                        .await?;
                    let exact = child.snapshot().await?;
                    assert_eq!(exact.occurrence, snapshot.occurrence);
                    assert!(matches!(exact.status, RunStatus::Succeeded));
                    assert_eq!(exact.last_action_ordinal, Some(0));
                    values.push(exact.state.values()["value"].as_i64().unwrap());
                }
                values.sort_unstable();
                assert_eq!(values, [11, 21]);
                Ok::<(), AppError>(())
            })?;

            let mut pipeline = runtime.block_on(async {
                workspace
                    .workflow("pipeline".parse()?)
                    .await?
                    .prepare(WorkflowState::default())
                    .await
            })?;
            // This call is outside Runtime::block_on. The caller-created
            // multithread runtime continues driving the storage reactor.
            let terminal =
                SyncExecution::open(&mut pipeline, ExecutionOptions::default())?.finish()?;
            assert!(matches!(terminal.status, RunStatus::Succeeded));
            assert_eq!(terminal.state.values()["sum"], 11);
            assert_eq!(terminal.last_action_ordinal, Some(0));
            assert_eq!(terminal.outcomes.len(), 1);
            assert_eq!(
                &terminal.outcomes[0].result,
                json!({
                    "child_states":[{"value":5},{"value":6}], "final_state":{"value":6},
                })
                .as_object()
                .unwrap()
            );
            assert_eq!(
                completed_batches.lock().unwrap().len(),
                1,
                "synchronous Pipeline bypasses the custom batch executor"
            );
            println!(
                "Tasks completed [1, 0], reduced [11, 21]; Pipeline handed off 5 → 6 and reduced 11"
            );
            Ok::<(), AppError>(())
        }));
        let closed = runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(15),
                app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(10),
                }),
            )
            .await
        });
        drop(app);
        match observed {
            Ok(result) => result?,
            Err(panic) => std::panic::resume_unwind(panic),
        }
        closed.map_err(|_| AppError::Timeout("child example cleanup timed out".into()))??;
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    CompositionExample::run(&runtime)?;
    Ok(())
}
