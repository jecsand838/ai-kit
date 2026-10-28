//! Explicit result projection through the existing action/state boundaries.
//! Pinned rurr-main.zip SHA256 5b5f20eb1d233467c600dc9dd249feaaa9046c5a303ae4e937040be5b6ecf25e,
//! function.rs259–268,309–333,512–523: present declared keys, separate full result.
//! Projection-before-explicit writes and finite combined counts are App translations.

use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use agents_kit::Agent;
use app_kit::workflow::{
    ActionBackend, ActionDeclaration, ActionEvent, ActionMode, ActionRequest, ExecutionEvent,
    ExecutionOptions, NoConditions, RunStatus, StateChange, StateTyping, WorkflowDefinition,
    WorkflowFailure, WorkflowRun, WorkflowState,
};
use app_kit::{App, AppConfig, AppError, Shutdown, WorkspaceConfig};
use futures_util::{FutureExt, Stream};
use serde_json::{Map, Value, json};
use storage_kit::StateStore;
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};

#[derive(Clone, Copy)]
enum SourceMode {
    Terminal,
    PartialThenTerminal,
    PartialThenPending,
    PartialThenEof,
}

#[derive(Default)]
struct ProjectionObservations {
    constructed: AtomicUsize,
    dropped: AtomicUsize,
    operations: Mutex<Vec<Value>>,
}

struct ProjectionAction {
    declaration: ActionDeclaration,
    result: Map<String, Value>,
    writes: Mutex<Option<Vec<StateChange>>>,
    mode: SourceMode,
    observed: Arc<ProjectionObservations>,
}

struct ProjectionStream {
    result: Option<Map<String, Value>>,
    writes: Option<Vec<StateChange>>,
    mode: SourceMode,
    partial: bool,
    local: Rc<()>,
    observed: Arc<ProjectionObservations>,
}

impl ActionBackend for ProjectionAction {
    type Stream = ProjectionStream;

    fn declaration(&self) -> &ActionDeclaration {
        &self.declaration
    }

    fn start(&self, _: ActionRequest) -> Self::Stream {
        self.observed.constructed.fetch_add(1, Ordering::SeqCst);
        ProjectionStream {
            result: Some(self.result.clone()),
            writes: Some(
                self.writes
                    .lock()
                    .unwrap()
                    .take()
                    .expect("one source admission"),
            ),
            mode: self.mode,
            partial: !matches!(self.mode, SourceMode::Terminal),
            local: Rc::new(()),
            observed: Arc::clone(&self.observed),
        }
    }
}

impl Stream for ProjectionStream {
    type Item = Result<ActionEvent, AppError>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        assert_eq!(Rc::strong_count(&self.local), 1);
        if self.partial {
            self.partial = false;
            return Poll::Ready(Some(Ok(ActionEvent::Partial {
                result: self.result.as_ref().unwrap().clone(),
            })));
        }
        match self.mode {
            SourceMode::PartialThenPending => Poll::Pending,
            SourceMode::PartialThenEof => Poll::Ready(None),
            SourceMode::Terminal | SourceMode::PartialThenTerminal => {
                Poll::Ready(self.result.take().map(|result| {
                    Ok(ActionEvent::Completed {
                        result,
                        writes: self.writes.take().unwrap(),
                    })
                }))
            }
        }
    }
}

impl Drop for ProjectionStream {
    fn drop(&mut self) {
        self.observed.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

struct ProjectionTyping {
    observed: Arc<ProjectionObservations>,
    reject_negative: bool,
}

impl StateTyping for ProjectionTyping {
    type Data = Rc<WorkflowState>;

    fn construct_data(&self, state: &WorkflowState) -> Result<Self::Data, AppError> {
        Ok(Rc::new(state.clone()))
    }

    fn construct_state(&self, data: &Self::Data) -> Result<WorkflowState, AppError> {
        Ok(data.as_ref().clone())
    }

    fn validate_pre(&self, _: &WorkflowState, change: &StateChange) -> Result<(), AppError> {
        let operation = match change {
            StateChange::Assign { values } => {
                json!({"kind":"assign", "keys":values.keys().collect::<Vec<_>>()})
            }
            StateChange::Increment { values } => {
                json!({"kind":"increment", "keys":values.iter().map(|(key, _)| key).collect::<Vec<_>>()})
            }
            StateChange::Remove { keys } => json!({"kind":"remove", "keys":keys}),
            StateChange::Append { .. }
            | StateChange::Extend { .. }
            | StateChange::Wipe { .. }
            | StateChange::Merge { .. }
            | StateChange::Subset { .. } => {
                panic!("the example action supplies no other operations")
            }
        };
        self.observed.operations.lock().unwrap().push(operation);
        Ok(())
    }

    fn validate_post(
        &self,
        _: &WorkflowState,
        after: &WorkflowState,
        _: &StateChange,
    ) -> Result<(), AppError> {
        if self.reject_negative
            && after.values()["count"]
                .as_i64()
                .is_some_and(|count| count < 0)
        {
            return Err(AppError::InvalidInput(
                "negative projection is forbidden".into(),
            ));
        }
        Ok(())
    }
}

struct ProjectionCase {
    app: App<StateStore, Agent, ProjectionAction, NoConditions, ProjectionTyping>,
    run: WorkflowRun<StateStore, Agent, ProjectionAction, NoConditions, ProjectionTyping>,
    observed: Arc<ProjectionObservations>,
}

impl ProjectionCase {
    fn document(projection: Option<Value>) -> Value {
        let mut step = json!({"kind":"action", "action":"project", "revision":"r1"});
        if let Some(projection) = projection {
            step["project_result"] = projection;
        }
        json!({
            "schema":"app-kit.workflow/v2", "id":"projection", "revision":"r1",
            "state":{"count":0,"keep":"base"}, "required":["count","keep"],
            "reducers":{"count":"reject_concurrent","keep":"reject_concurrent"},
            "entry":"copy", "nodes":[{"id":"copy","step":step}], "routes":{}
        })
    }

    async fn open(
        projection: Option<bool>,
        declared: Vec<String>,
        result: Map<String, Value>,
        writes: Vec<StateChange>,
        mode: SourceMode,
        reject_negative: bool,
    ) -> Self {
        let observed = Arc::new(ProjectionObservations::default());
        let definition =
            WorkflowDefinition::parse(&Self::document(projection.map(Value::Bool)).to_string())
                .expect("declared project_result grammar");
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let config: AppConfig<StateStore, Agent> =
            AppConfig::new(store, Vec::new(), vec![definition]);
        let mut app = App::open(
            config
                .with_actions(vec![ProjectionAction {
                    declaration: ActionDeclaration {
                        key: "project".into(),
                        revision: "r1".into(),
                        reads: Vec::new(),
                        writes: declared,
                        required_inputs: Vec::new(),
                        optional_inputs: Vec::new(),
                        bound_inputs: Map::new(),
                        default_inputs: Map::new(),
                        tags: Vec::new(),
                        source: Some("projection fixture".into()),
                        mode: ActionMode::Asynchronous,
                    },
                    result,
                    writes: Mutex::new(Some(writes)),
                    mode,
                    observed: Arc::clone(&observed),
                }])
                .with_state_typing(ProjectionTyping {
                    observed: Arc::clone(&observed),
                    reject_negative,
                }),
        )
        .await
        .unwrap();
        let prepared = std::panic::AssertUnwindSafe(async {
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().unwrap(),
                })
                .await
                .unwrap();
            let workflow = workspace
                .workflow("projection".parse().unwrap())
                .await
                .unwrap();
            workflow.prepare(WorkflowState::default()).await.unwrap()
        })
        .catch_unwind()
        .await;
        let run = match prepared {
            Ok(run) => run,
            Err(failure) => {
                let _ = app
                    .shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_secs(3),
                    })
                    .await;
                std::panic::resume_unwind(failure);
            }
        };
        Self { app, run, observed }
    }

    async fn step(&mut self) -> Result<Option<ExecutionEvent>, AppError> {
        let mut execution = self.run.execution(ExecutionOptions::default()).await?;
        let result = tokio::time::timeout(Duration::from_secs(3), execution.step())
            .await
            .expect("bounded action projection");
        // Retain the same owner through joined cleanup. An implicit Drop here
        // would make the following snapshot race admitted failure publication.
        let cleanup = tokio::time::timeout(Duration::from_secs(3), execution.abort())
            .await
            .expect("projection finalization is bounded");
        match result {
            Err(error) => Err(error),
            Ok(event) => cleanup.map(|_| event),
        }
    }

    async fn close(mut self, checked: std::thread::Result<()>) {
        let closed = self
            .app
            .shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            })
            .await;
        if let Err(failure) = checked {
            std::panic::resume_unwind(failure);
        }
        closed.expect("projection work drains");
        assert_eq!(self.observed.constructed.load(Ordering::SeqCst), 1);
        assert_eq!(self.observed.dropped.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn action_projection_is_an_optional_strict_boolean() {
    for projection in [None, Some(json!(false)), Some(json!(true))] {
        WorkflowDefinition::parse(&ProjectionCase::document(projection).to_string()).unwrap();
    }
    for projection in [Value::Null, json!("true"), json!(1), json!([]), json!({})] {
        assert!(
            WorkflowDefinition::parse(&ProjectionCase::document(Some(projection)).to_string())
                .is_err()
        );
    }
}

#[tokio::test]
async fn projection_selects_present_declared_keys_then_preserves_every_explicit_operation() {
    let result = Map::from_iter([
        ("unselected".into(), json!({"content":"retained result"})),
        ("count".into(), json!(3)),
        ("nullable".into(), Value::Null),
        ("new".into(), json!(2)),
    ]);
    let mut case = ProjectionCase::open(
        Some(true),
        ["new", "nullable", "absent", "new", "count"]
            .map(String::from)
            .into(),
        result.clone(),
        vec![
            StateChange::Increment {
                values: vec![("count".into(), json!(4))],
            },
            StateChange::Assign {
                values: Map::from_iter([("extra".into(), json!(7))]),
            },
            StateChange::Remove {
                keys: vec!["new".into()],
            },
        ],
        SourceMode::PartialThenTerminal,
        false,
    )
    .await;
    let checked = std::panic::AssertUnwindSafe(async {
        let mut execution = case.run.execution(ExecutionOptions::default()).await.unwrap();
        assert!(matches!(tokio::time::timeout(Duration::from_secs(3), execution.next()).await.unwrap().unwrap(), Some(ExecutionEvent::Partial { result:partial, .. }) if partial == result));
        assert!(case.observed.operations.lock().unwrap().is_empty());
        let Some(ExecutionEvent::Committed { outcomes, .. }) = tokio::time::timeout(Duration::from_secs(3), execution.step()).await.unwrap().unwrap() else { panic!("committed projection"); };
        assert_eq!(outcomes[0].result, result);
        drop(execution);
        let snapshot = case.run.snapshot().await.unwrap();
        assert_eq!(snapshot.state.values(), &Map::from_iter([
            ("count".into(), json!(7)), ("keep".into(), json!("base")),
            ("nullable".into(), Value::Null), ("extra".into(), json!(7)),
        ]));
        assert_eq!(snapshot.state.values().keys().map(String::as_str).collect::<Vec<_>>(), ["count","keep","nullable","extra"]);
        assert_eq!(*case.observed.operations.lock().unwrap(), [
            json!({"kind":"assign","keys":["new","nullable","count"]}),
            json!({"kind":"increment","keys":["count"]}),
            json!({"kind":"assign","keys":["extra"]}),
            json!({"kind":"remove","keys":["new"]}),
        ]);
    }).catch_unwind().await;
    case.close(checked).await;
}

#[tokio::test]
async fn disabled_projection_adds_no_operation_and_enabled_empty_projection_still_validates() {
    for projection in [None, Some(false), Some(true)] {
        let declared = if projection == Some(true) {
            "missing"
        } else {
            "count"
        };
        let mut case = ProjectionCase::open(
            projection,
            vec![declared.into()],
            Map::from_iter([("count".into(), json!(9))]),
            Vec::new(),
            SourceMode::Terminal,
            false,
        )
        .await;
        let checked = std::panic::AssertUnwindSafe(async {
            let Some(ExecutionEvent::Committed { outcomes, .. }) = case.step().await.unwrap()
            else {
                panic!("empty projection still completes");
            };
            assert_eq!(outcomes[0].result["count"], 9);
            assert_eq!(
                case.run.snapshot().await.unwrap().state.values()["count"],
                0
            );
            let operations = case.observed.operations.lock().unwrap();
            if projection == Some(true) {
                assert_eq!(*operations, [json!({"kind":"assign","keys":[]})]);
            } else {
                assert!(operations.is_empty());
            }
        })
        .catch_unwind()
        .await;
        case.close(checked).await;
    }
}

#[tokio::test]
async fn invalid_projection_cannot_be_repaired_by_later_explicit_writes() {
    for invalid in [
        json!(-1),
        Value::Object(Map::from_iter([(
            "x".repeat(129),
            json!("invalid state key"),
        )])),
    ] {
        let mut case = ProjectionCase::open(
            Some(true),
            vec!["count".into()],
            Map::from_iter([("count".into(), invalid)]),
            vec![StateChange::Assign {
                values: Map::from_iter([("count".into(), json!(5))]),
            }],
            SourceMode::Terminal,
            true,
        )
        .await;
        let checked = std::panic::AssertUnwindSafe(async {
            let _ = case.step().await;
            let snapshot = case.run.snapshot().await.unwrap();
            assert!(matches!(
                snapshot.status,
                RunStatus::Failed(WorkflowFailure::State(_))
            ));
            assert_eq!(snapshot.state.values()["count"], 0);
            assert!(snapshot.outcomes.is_empty());
            assert!(snapshot.last_action_ordinal.is_none());
            assert_eq!(
                case.observed.operations.lock().unwrap().len(),
                1,
                "later explicit operation never validates"
            );
        })
        .catch_unwind()
        .await;
        case.close(checked).await;
    }
}

#[tokio::test]
async fn unselected_result_data_does_not_acquire_state_structure_restrictions() {
    let result = Map::from_iter([("x".repeat(129), json!(1)), ("count".into(), json!(2))]);
    let mut case = ProjectionCase::open(
        Some(true),
        vec!["count".into()],
        result.clone(),
        Vec::new(),
        SourceMode::Terminal,
        false,
    )
    .await;
    let checked = std::panic::AssertUnwindSafe(async {
        let Some(ExecutionEvent::Committed { outcomes, .. }) = case.step().await.unwrap() else {
            panic!("byte-valid full result");
        };
        assert_eq!(outcomes[0].result, result);
        assert_eq!(
            case.run.snapshot().await.unwrap().state.values()["count"],
            2
        );
    })
    .catch_unwind()
    .await;
    case.close(checked).await;
}

#[tokio::test]
async fn projection_counts_as_one_operation_including_when_empty() {
    for (projection, explicit, accepted) in [
        (Some(false), 4096, true),
        (Some(true), 4095, true),
        (Some(true), 4096, false),
    ] {
        let writes = (0..explicit)
            .map(|_| StateChange::Assign { values: Map::new() })
            .collect();
        let mut case = ProjectionCase::open(
            projection,
            Vec::new(),
            Map::new(),
            writes,
            SourceMode::Terminal,
            false,
        )
        .await;
        let checked = std::panic::AssertUnwindSafe(async {
            let event = case.step().await;
            let snapshot = case.run.snapshot().await.unwrap();
            assert_eq!(snapshot.state.values()["count"], 0);
            if accepted {
                assert!(matches!(
                    event.unwrap(),
                    Some(ExecutionEvent::Committed { .. })
                ));
                assert_eq!(case.observed.operations.lock().unwrap().len(), 4096);
            } else {
                assert!(event.is_err() || matches!(snapshot.status, RunStatus::Failed(_)));
                assert!(snapshot.outcomes.is_empty());
                assert!(snapshot.last_action_ordinal.is_none());
                assert!(case.observed.operations.lock().unwrap().is_empty());
            }
        })
        .catch_unwind()
        .await;
        case.close(checked).await;
    }
}

#[tokio::test]
async fn partial_drop_abort_and_eof_never_apply_result_projection() {
    for ending in [0, 1, 2] {
        let mode = if ending == 2 {
            SourceMode::PartialThenEof
        } else {
            SourceMode::PartialThenPending
        };
        let mut case = ProjectionCase::open(
            Some(true),
            vec!["count".into()],
            Map::from_iter([("count".into(), json!(99))]),
            Vec::new(),
            mode,
            false,
        )
        .await;
        let checked = std::panic::AssertUnwindSafe(async {
            let mut execution = case
                .run
                .execution(ExecutionOptions::default())
                .await
                .unwrap();
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(3), execution.next())
                    .await
                    .unwrap()
                    .unwrap(),
                Some(ExecutionEvent::Partial { .. })
            ));
            match ending {
                0 => drop(execution),
                1 => {
                    tokio::time::timeout(Duration::from_secs(3), execution.abort())
                        .await
                        .unwrap()
                        .unwrap();
                }
                2 => {
                    assert!(
                        tokio::time::timeout(Duration::from_secs(3), execution.step())
                            .await
                            .unwrap()
                            .unwrap()
                            .is_none()
                    );
                    drop(execution);
                }
                _ => unreachable!(),
            }
            let snapshot = case.run.snapshot().await.unwrap();
            assert_eq!(snapshot.state.values()["count"], 0);
            assert!(snapshot.outcomes.is_empty());
            assert!(snapshot.last_action_ordinal.is_none());
            assert!(snapshot.interactions.is_empty());
            assert!(case.observed.operations.lock().unwrap().is_empty());
        })
        .catch_unwind()
        .await;
        case.close(checked).await;
    }
}
