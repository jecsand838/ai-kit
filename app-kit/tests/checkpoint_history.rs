//! Public retained-history behavior, using real LanceDB and native State nodes.
//!
//! Source-derived selector oracle: pinned rurr commit
//! 005bfe3ece631e64b06dac2d42ef84235f727a4f, archive SHA256
//! 5b5f20eb1d233467c600dc9dd249feaaa9046c5a303ae4e937040be5b6ecf25e;
//! rurr-testkit/src/persistence_cases.rs:615-671 and
//! rurr-core/src/application/build/initializer.rs:243-259.
//! BSP attribution and checkpoint cursors are explicit App extensions.
//! This target uses public history/restore APIs and exact retained-record faults.
//! It does not claim process-crash or real-service durability proof.

use std::cell::Cell;
use std::future::Future;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agents_kit::Agent;
use app_kit::workflow::{
    Checkpoint, CheckpointKind, CheckpointSelector, ExecutionEvent, ExecutionOptions,
    HistoryCursor, NoActions, NoConditions, RestoreOptions, ResumePosition, RunId, RunSelector,
    RunStatus, StateChange, StateTyping, Workflow, WorkflowDefinition, WorkflowFailure,
    WorkflowRun, WorkflowState,
};
use app_kit::{App, AppConfig, AppError, Shutdown, WorkspaceConfig, WorkspaceId};
use futures_util::FutureExt;
use serde_json::{Value, json};
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_kit::state::record::{StateKey, StateListRequest, StateRecordSnapshot};
use storage_kit::{StateStore, StateStoreBackend, StateTransactionBackend};

struct HistoryTyping {
    reject_once: Option<i64>,
    reject_next: Arc<Mutex<Option<i64>>>,
    rejected: Arc<AtomicBool>,
    posts: Arc<Mutex<Vec<i64>>>,
    reject_reduced: bool,
}

impl StateTyping for HistoryTyping {
    type Data = Rc<WorkflowState>;

    fn construct_data(&self, state: &WorkflowState) -> Result<Self::Data, AppError> {
        Ok(Rc::new(state.clone()))
    }

    fn construct_state(&self, data: &Self::Data) -> Result<WorkflowState, AppError> {
        Ok(data.as_ref().clone())
    }

    fn validate_post(
        &self,
        _: &WorkflowState,
        after: &WorkflowState,
        _: &StateChange,
    ) -> Result<(), AppError> {
        let count = after.values()["count"]
            .as_i64()
            .expect("integer fixture state");
        self.posts.lock().unwrap().push(count);
        let selected = {
            let mut next = self.reject_next.lock().unwrap();
            if *next == Some(count) {
                *next = None;
                true
            } else {
                false
            }
        };
        if selected
            || self.reject_once == Some(count) && !self.rejected.swap(true, Ordering::SeqCst)
        {
            Err(AppError::InvalidInput(
                "one explicit state rejection".into(),
            ))
        } else {
            Ok(())
        }
    }

    fn validate_state(&self, state: &WorkflowState) -> Result<(), AppError> {
        if self.reject_reduced
            && state.values()["count"] == 30
            && !self.rejected.swap(true, Ordering::SeqCst)
        {
            Err(AppError::InvalidInput("combined candidate rejected".into()))
        } else {
            Ok(())
        }
    }
}

struct HistoryCase {
    app: App<StateStore, Agent, NoActions, NoConditions, HistoryTyping>,
    store: StateStore,
    workspace: WorkspaceId,
    posts: Arc<Mutex<Vec<i64>>>,
    reject_next: Arc<Mutex<Option<i64>>>,
}

#[derive(Clone, Copy)]
enum RetainedStateFault {
    MissingRequired,
    Oversized,
}

impl HistoryCase {
    fn checkpoint_facts(row: &Checkpoint) -> Value {
        let kind = match &row.kind {
            CheckpointKind::Initial => json!({"kind":"initial"}),
            CheckpointKind::Control => json!({"kind":"control"}),
            CheckpointKind::Action => json!({"kind":"action"}),
            CheckpointKind::Failed { node, ordinal } => {
                json!({"kind":"failed","node":node,"ordinal":ordinal})
            }
            CheckpointKind::Terminal => json!({"kind":"terminal"}),
        };
        let status = match &row.status {
            RunStatus::Running => "running",
            RunStatus::Paused => "paused",
            RunStatus::Succeeded => "succeeded",
            _ => {
                panic!("successful state-only fixture has no halt, cancellation or failure")
            }
        };
        let lineage = row.lineage.as_ref().map(|lineage| {
            json!({
                "source":lineage.source.to_string(), "occurrence":lineage.source_occurrence,
                "checkpoint":lineage.checkpoint, "ordinal":lineage.action_ordinal,
            })
        });
        json!({
            "run":row.run.to_string(), "occurrence":row.occurrence,
            "sequence":row.sequence, "kind":kind, "workflow":row.workflow.to_string(),
            "revision":row.revision, "ordinal":row.action_ordinal, "position":row.position,
            "state":row.state.values(), "status":status, "outcomes":row.outcomes,
            "lineage":lineage,
        })
    }

    // Each round constructs fresh Lance 13 and App handles after the preceding
    // owners settle. Reopening observes persisted history without replaying work.
    async fn persistence_round(directory: &std::path::Path, expected: Option<&Value>) -> Value {
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::directory(
            directory.join("state"),
        )))
        .await
        .expect("Lance 13 directory store");
        let posts = Arc::new(Mutex::new(Vec::new()));
        let config: AppConfig<StateStore, Agent> =
            AppConfig::new(store, Vec::new(), vec![Self::linear(2)]);
        let mut app = App::open(config.with_state_typing(HistoryTyping {
            reject_once: None,
            reject_next: Arc::new(Mutex::new(None)),
            rejected: Arc::new(AtomicBool::new(false)),
            posts: Arc::clone(&posts),
            reject_reduced: false,
        }))
        .await
        .expect("open v7 App without schema migration");
        let observed =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(20), async {
                let workspace = match expected {
                    Some(expected) => app
                        .workspace(expected["workspace"].as_str().unwrap().parse().unwrap())
                        .await
                        .unwrap(),
                    None => app
                        .create_workspace(WorkspaceConfig {
                            name: Some("Lance directory history".into()),
                            root: directory.to_owned(),
                        })
                        .await
                        .unwrap(),
                };
                let workflow = workspace
                    .workflow("history".parse().unwrap())
                    .await
                    .unwrap();
                let mut histories = Vec::new();
                if let Some(expected) = expected {
                    for prior in expected["runs"].as_array().unwrap() {
                        let run = workflow
                            .open_run(RunSelector::Latest(
                                prior["run"].as_str().unwrap().parse().unwrap(),
                            ))
                            .await
                            .unwrap();
                        let rows = Self::pages(&run, None, 2).await;
                        let facts: Vec<_> = rows.iter().map(Self::checkpoint_facts).collect();
                        assert_eq!(
                            json!(facts),
                            prior["history"],
                            "all retained checkpoint facts survive cold reopen"
                        );
                        assert_eq!(
                            run.checkpoint(CheckpointSelector::Action(0))
                                .await
                                .unwrap()
                                .state
                                .values()["count"],
                            1
                        );
                        assert_eq!(
                            run.checkpoint(CheckpointSelector::Action(1))
                                .await
                                .unwrap()
                                .state
                                .values()["count"],
                            2
                        );
                        assert!(matches!(
                            run.snapshot().await.unwrap().status,
                            RunStatus::Succeeded
                        ));
                        histories.push(prior.clone());
                    }
                    assert!(
                        posts.lock().unwrap().is_empty(),
                        "reopen never executes prior actions"
                    );
                }
                // Create one run in each of the first two rounds. The final
                // fresh-handle round only observes both persisted histories.
                if histories.len() < 2 {
                    let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
                    assert!(matches!(
                        Self::finish(&mut run).await.status,
                        RunStatus::Succeeded
                    ));
                    let rows = Self::pages(&run, None, 2).await;
                    assert_eq!(
                        rows.iter()
                            .filter(|row| matches!(row.kind, CheckpointKind::Action))
                            .count(),
                        2
                    );
                    assert_eq!(*posts.lock().unwrap(), vec![1, 2]);
                    histories.push(json!({"run":run.id().to_string(),
                        "history":rows.iter().map(Self::checkpoint_facts).collect::<Vec<_>>()}));
                }
                json!({"workspace":workspace.id().to_string(), "runs":histories})
            }))
            .catch_unwind()
            .await;
        let closed = std::panic::AssertUnwindSafe(app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(4),
        }))
        .catch_unwind()
        .await;
        drop(app);
        if !matches!(&closed, Ok(Ok(()))) {
            eprintln!(
                "cold-reopen App cleanup failed or panicked; original observation remains primary"
            );
        }
        let facts = match observed {
            Ok(result) => result.expect("bounded directory checkpoint/history proof"),
            Err(panic) => std::panic::resume_unwind(panic),
        };
        closed
            .expect("cold-reopen App shutdown must not panic")
            .expect("App and native state owners settle before reopening");
        facts
    }

    async fn assert_corrupt_state(predecessor: bool, fault: RetainedStateFault) {
        let (case, workflow) = Self::new(Self::linear(2), None, false).await;
        let store = case.store.clone();
        let workspace = case.workspace.clone();
        let posts = Arc::clone(&case.posts);
        case.close_after(async {
            let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
            Self::finish(&mut run).await;
            let latest = run.checkpoint(CheckpointSelector::Latest).await.unwrap();
            assert!(latest.sequence > 0);
            let sequence = latest.sequence - u64::from(predecessor);
            let scope = format!("{workspace}/history/{}/{}", run.id(), latest.occurrence);
            let head_key = StateKey::new("app-kit", format!("run/{scope}")).unwrap();
            let head = store.load(head_key.clone()).await.unwrap().unwrap();
            let key = StateKey::new(
                "app-kit",
                format!("history/{scope}/checkpoint/{sequence:020}"),
            )
            .unwrap();
            let original = store.load(key).await.unwrap().unwrap().record;
            let mut corrupted = original.clone();
            let state = Arc::make_mut(&mut corrupted.value)["checkpoint"]["state"]
                .as_object_mut()
                .unwrap();
            match fault {
                RetainedStateFault::MissingRequired => {
                    assert_eq!(state.remove("count"), Some(json!(2)));
                }
                RetainedStateFault::Oversized => {
                    state.insert("oversized".into(), Value::String("x".repeat(1_048_576)));
                }
            }
            let mut transaction = store.begin_transaction().await.unwrap();
            transaction.put(corrupted).await.unwrap();
            transaction.commit().await.unwrap();
            // This mutates only one real immutable row. Head identity/sequence
            // and the other predecessor/current record remain unchanged.
            let observed =
                std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(3), async {
                    let unchanged = store.load(head_key).await.unwrap().unwrap();
                    assert_eq!(unchanged.record.value, head.record.value);
                    assert_eq!(unchanged.revision, head.revision);
                    assert!(
                        matches!(
                            run.checkpoint(CheckpointSelector::Latest).await,
                            Err(AppError::Integration(_))
                        ),
                        "corrupt retained state must not project as a valid checkpoint"
                    );
                    assert!(
                        matches!(
                            run.history(None, NonZeroUsize::new(1_000).unwrap()).await,
                            Err(AppError::Integration(_))
                        ),
                        "history validates selected and predecessor state"
                    );
                    assert_eq!(
                        *posts.lock().unwrap(),
                        vec![1, 2],
                        "reads do not re-run state mutations"
                    );
                }))
                .catch_unwind()
                .await;
            // Restore exact original bytes before propagating any red result,
            // so owned App cleanup never depends on the corrupted fixture.
            let mut transaction = store.begin_transaction().await.unwrap();
            transaction.put(original).await.unwrap();
            transaction.commit().await.unwrap();
            match observed {
                Ok(result) => result.expect("corrupt history observations remain bounded"),
                Err(panic) => std::panic::resume_unwind(panic),
            }
            assert_eq!(
                run.checkpoint(CheckpointSelector::Latest)
                    .await
                    .unwrap()
                    .state
                    .values()["count"],
                2
            );
        })
        .await;
    }

    fn assign(count: usize) -> Value {
        json!({"kind":"state","mutations":[
            {"kind":"assign","target":"count","value":{"kind":"literal","value":count}}
        ]})
    }

    fn linear(length: usize) -> WorkflowDefinition {
        let nodes: Vec<_> = (0..length)
            .map(|index| {
                json!({
                    "id":format!("n{index:03}"), "step":Self::assign(index + 1)
                })
            })
            .collect();
        let routes: serde_json::Map<String, Value> = (0..length - 1)
            .map(|index| {
                (
                    format!("n{index:03}"),
                    json!({"mode":"first","branches":[
                        {"target":{"kind":"node","id":format!("n{:03}", index + 1)}}
                    ]}),
                )
            })
            .collect();
        WorkflowDefinition::parse(
            &json!({
                "schema":"app-kit.workflow/v2","id":"history","revision":"r1",
                "state":{"count":0},"required":["count"],
                "reducers":{"count":"reject_concurrent"},"entry":"n000",
                "nodes":nodes,"routes":routes
            })
            .to_string(),
        )
        .expect("native linear definition")
    }

    fn parallel(failed_sources: &[&str]) -> WorkflowDefinition {
        let failure = json!({"kind":"state","mutations":[
            {"kind":"assign","target":"count","value":{"kind":"state","path":"absent"}}
        ]});
        let last = if failed_sources.contains(&"z") {
            failure.clone()
        } else {
            Self::assign(20)
        };
        WorkflowDefinition::parse(
            &json!({
                "schema":"app-kit.workflow/v2","id":"history","revision":"r1",
                "state":{"count":0},"required":["count"],
                "reducers":{"count":"sum_integer"},"entry":"seed",
                "nodes":[
                    {"id":"seed","step":{"kind":"state","mutations":[]}},
                    {"id":"z","step":last}, {"id":"a","step":if failed_sources.contains(&"a") {failure} else {Self::assign(10)}}
                ],
                "routes":{"seed":{"mode":"broadcast","branches":[
                    {"target":{"kind":"node","id":"z"}},
                    {"target":{"kind":"node","id":"a"}}
                ]}}
            })
            .to_string(),
        )
        .expect("canonical order differs from authored/route order")
    }

    async fn new(
        definition: WorkflowDefinition,
        reject_once: Option<i64>,
        reject_reduced: bool,
    ) -> (
        Self,
        Workflow<StateStore, Agent, NoActions, NoConditions, HistoryTyping>,
    ) {
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("real LanceDB");
        let posts = Arc::new(Mutex::new(Vec::new()));
        let reject_next = Arc::new(Mutex::new(None));
        let config: AppConfig<StateStore, Agent> =
            AppConfig::new(store.clone(), Vec::new(), vec![definition]);
        let app = App::open(config.with_state_typing(HistoryTyping {
            reject_once,
            reject_next: Arc::clone(&reject_next),
            rejected: Arc::new(AtomicBool::new(false)),
            posts: Arc::clone(&posts),
            reject_reduced,
        }))
        .await
        .expect("state-only App");
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().unwrap(),
            })
            .await
            .expect("workspace");
        let workspace_id = workspace.id().clone();
        let workflow = workspace
            .workflow("history".parse().unwrap())
            .await
            .expect("workflow");
        (
            Self {
                app,
                store,
                workspace: workspace_id,
                posts,
                reject_next,
            },
            workflow,
        )
    }

    async fn close_after(mut self, observed: impl Future<Output = ()>) {
        let result =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(20), observed))
                .catch_unwind()
                .await;
        let closed = self
            .app
            .shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(4),
            })
            .await;
        drop(self.app);
        match result {
            Ok(result) => result.expect("bounded public history scenario"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        closed.expect("owned App cleanup");
    }

    async fn finish(
        run: &mut WorkflowRun<StateStore, Agent, NoActions, NoConditions, HistoryTyping>,
    ) -> app_kit::workflow::RunSnapshot {
        run.execution(ExecutionOptions::default())
            .await
            .expect("exclusive explicit execution")
            .finish()
            .await
            .expect("settled execution snapshot")
    }

    async fn records(store: &StateStore) -> Vec<StateRecordSnapshot> {
        let mut after = None;
        let mut rows = Vec::new();
        for _ in 0..64 {
            let mut request = StateListRequest::new("app-kit")
                .unwrap()
                .with_limit(256)
                .unwrap();
            if let Some(cursor) = after.take() {
                request = request.start_after(cursor).unwrap();
            }
            let page = store.list(request).await.unwrap();
            rows.extend(page.records);
            after = page.next_start_after;
            if after.is_none() {
                return rows;
            }
        }
        panic!("finite native history inventory did not terminate");
    }

    async fn pages(
        run: &WorkflowRun<StateStore, Agent, NoActions, NoConditions, HistoryTyping>,
        mut after: Option<HistoryCursor>,
        limit: usize,
    ) -> Vec<Checkpoint> {
        let mut rows = Vec::new();
        for _ in 0..1024 {
            let page = run
                .history(after, NonZeroUsize::new(limit).unwrap())
                .await
                .expect("bounded immutable history page");
            assert!(page.checkpoints.len() <= limit);
            for row in page.checkpoints {
                if let Some(prior) = rows.last() {
                    let prior: &Checkpoint = prior;
                    assert!(row.sequence > prior.sequence, "strict page progress");
                }
                rows.push(row);
            }
            after = page.next;
            if after.is_none() {
                return rows;
            }
        }
        panic!("finite history traversal did not terminate");
    }

    fn failed(row: &Checkpoint, node: &str, ordinal: u64, prior: Option<(&str, u64)>, count: i64) {
        assert!(
            matches!(&row.kind, CheckpointKind::Failed { node: actual, ordinal: actual_ordinal }
            if actual == node && *actual_ordinal == ordinal)
        );
        assert_eq!(row.position.as_deref(), prior.map(|(node, _)| node));
        assert_eq!(row.action_ordinal, prior.map(|(_, ordinal)| ordinal));
        assert!(
            row.outcomes.is_empty(),
            "a failed attempt invents no committed result"
        );
        assert!(matches!(&row.status, RunStatus::Failed(_)));
        assert_eq!(row.state.values()["count"], count);
    }
}

#[tokio::test]
async fn action_selector_ignores_terminal_and_control_rows_but_preserves_each_commit() {
    let (case, workflow) = HistoryCase::new(HistoryCase::linear(3), None, false).await;
    let posts = Arc::clone(&case.posts);
    case.close_after(async {
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        assert!(matches!(
            run.history(None, NonZeroUsize::new(1_001).unwrap()).await,
            Err(AppError::InvalidInput(_))
        ));
        assert!(posts.lock().unwrap().is_empty());
        let initial = run.checkpoint(CheckpointSelector::Latest).await.unwrap();
        assert!(matches!(initial.kind, CheckpointKind::Initial));
        assert!(initial.action_ordinal.is_none());
        assert!(matches!(
            run.checkpoint(CheckpointSelector::Action(0)).await,
            Err(AppError::NotFound(_))
        ));
        let result = HistoryCase::finish(&mut run).await;
        assert!(matches!(result.status, RunStatus::Succeeded));
        let mut last = 0;
        for ordinal in 0..3 {
            let row = run
                .checkpoint(CheckpointSelector::Action(ordinal))
                .await
                .unwrap();
            assert!(matches!(row.kind, CheckpointKind::Action));
            assert_eq!(row.action_ordinal, Some(ordinal));
            assert_eq!(row.position, Some(format!("n{ordinal:03}")));
            assert_eq!(row.state.values()["count"], ordinal + 1);
            assert_eq!(row.outcomes.len(), 1);
            assert!(
                row.outcomes[0].result.is_empty(),
                "native State transient result"
            );
            assert!(row.sequence > last);
            last = row.sequence;
        }
        let latest = run.checkpoint(CheckpointSelector::Latest).await.unwrap();
        assert!(matches!(latest.kind, CheckpointKind::Terminal));
        assert!(latest.sequence > last);
        assert!(latest.outcomes.is_empty());
        assert_eq!(latest.action_ordinal, Some(2));
        assert!(matches!(
            run.checkpoint(CheckpointSelector::Action(3)).await,
            Err(AppError::NotFound(_))
        ));
        let rows = HistoryCase::pages(&run, None, 2).await;
        let largest = run
            .history(None, NonZeroUsize::new(1_000).unwrap())
            .await
            .unwrap();
        assert!(largest.next.is_none());
        assert_eq!(largest.checkpoints.len(), rows.len());
        assert_eq!(rows.first().unwrap().sequence, 0);
        assert_eq!(rows.last().unwrap().sequence, latest.sequence);
        assert!(
            rows.iter()
                .any(|row| matches!(row.kind, CheckpointKind::Control)),
            "the fixture must observe real state-preserving publication/routing controls"
        );
        for control in rows
            .iter()
            .filter(|row| matches!(row.kind, CheckpointKind::Control))
        {
            assert!(
                control.outcomes.is_empty(),
                "a control invents no action observation"
            );
        }
        assert_eq!(
            rows.iter()
                .filter(|row| matches!(row.kind, CheckpointKind::Action))
                .count(),
            3
        );
        assert_eq!(
            *posts.lock().unwrap(),
            vec![1, 2, 3],
            "reads never replay typing"
        );
    })
    .await;
}

#[tokio::test]
async fn failed_observations_keep_attempted_identity_separate_from_prior_success() {
    for (reject, prior) in [(1, None), (2, Some(("n000", 0)))] {
        let (case, workflow) = HistoryCase::new(HistoryCase::linear(2), Some(reject), false).await;
        let posts = Arc::clone(&case.posts);
        case.close_after(async {
            let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
            let result = HistoryCase::finish(&mut run).await;
            assert!(matches!(
                result.status,
                RunStatus::Failed(WorkflowFailure::State(_))
            ));
            let ordinal = (reject - 1) as u64;
            let row = run
                .checkpoint(CheckpointSelector::Action(ordinal))
                .await
                .unwrap();
            HistoryCase::failed(&row, &format!("n{ordinal:03}"), ordinal, prior, reject - 1);
            let rows = HistoryCase::pages(&run, None, 1).await;
            assert_eq!(
                rows.iter()
                    .filter(|row| matches!(row.kind, CheckpointKind::Failed { .. }))
                    .count(),
                1
            );
            let same = run
                .checkpoint(CheckpointSelector::Sequence(row.sequence))
                .await
                .unwrap();
            HistoryCase::failed(&same, &format!("n{ordinal:03}"), ordinal, prior, reject - 1);
            assert!(matches!(
                run.checkpoint(CheckpointSelector::Action(ordinal + 1))
                    .await,
                Err(AppError::NotFound(_))
            ));
            assert_eq!(
                posts.lock().unwrap().len(),
                reject as usize,
                "no read-triggered retry"
            );
        })
        .await;
    }
}

#[tokio::test]
async fn cancellation_history_reads_lifecycle_without_replaying_work() {
    let (case, workflow) = HistoryCase::new(HistoryCase::linear(1), None, false).await;
    let posts = Arc::clone(&case.posts);
    case.close_after(async {
        let run = workflow.prepare(WorkflowState::default()).await.unwrap();
        let checkpoint = run.snapshot().await.unwrap().checkpoint;
        run.cancel(checkpoint).await.unwrap();
        let terminal = tokio::time::timeout(Duration::from_secs(3), run.wait())
            .await
            .expect("accepted cancellation settles")
            .unwrap();
        assert!(matches!(terminal.status, RunStatus::Cancelled));
        let rows = HistoryCase::pages(&run, None, 1).await;
        let cancellation: Vec<_> = rows
            .iter()
            .filter(|row| matches!(row.status, RunStatus::Cancelling))
            .collect();
        assert_eq!(cancellation.len(), 1, "one accepted cancellation boundary");
        assert!(matches!(cancellation[0].kind, CheckpointKind::Control));
        assert!(matches!(rows.last().unwrap().status, RunStatus::Cancelled));
        for row in rows {
            assert_eq!(row.state.values()["count"], 0);
            assert!(row.outcomes.is_empty());
            assert!(row.action_ordinal.is_none() && row.position.is_none());
        }
        assert!(
            posts.lock().unwrap().is_empty(),
            "history does not invoke typing"
        );
    })
    .await;
}

#[tokio::test]
async fn explicit_failed_retry_supersedes_same_ordinal_selection_without_erasing_failure() {
    let (case, workflow) = HistoryCase::new(HistoryCase::linear(2), Some(2), false).await;
    let posts = Arc::clone(&case.posts);
    case.close_after(async {
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        let failed = HistoryCase::finish(&mut run).await;
        assert_eq!(failed.last_action_ordinal, Some(0));
        let old = run.checkpoint(CheckpointSelector::Action(1)).await.unwrap();
        HistoryCase::failed(&old, "n001", 1, Some(("n000", 0)), 1);
        let result = HistoryCase::finish(&mut run).await;
        assert!(matches!(result.status, RunStatus::Succeeded));
        assert_eq!(result.last_action_ordinal, Some(1));
        let selected = run.checkpoint(CheckpointSelector::Action(1)).await.unwrap();
        assert!(matches!(selected.kind, CheckpointKind::Action));
        assert!(selected.sequence > old.sequence);
        assert_eq!(selected.position.as_deref(), Some("n001"));
        assert_eq!(selected.state.values()["count"], 2);
        HistoryCase::failed(
            &run.checkpoint(CheckpointSelector::Sequence(old.sequence))
                .await
                .unwrap(),
            "n001",
            1,
            Some(("n000", 0)),
            1,
        );
        assert_eq!(
            *posts.lock().unwrap(),
            vec![1, 2, 2],
            "only explicit failed action reruns"
        );
    })
    .await;
}

#[tokio::test]
async fn serialized_cursor_keeps_its_head_cutoff_during_append_and_rejects_another_run() {
    for malformed in ["null", "{}", "[]", "0", "\"native-cursor\""] {
        assert!(serde_json::from_str::<HistoryCursor>(malformed).is_err());
    }
    let (case, workflow) = HistoryCase::new(HistoryCase::linear(6), None, false).await;
    case.close_after(async {
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        let observer = workflow
            .open_run(RunSelector::Latest(run.id().clone()))
            .await
            .unwrap();
        let other = workflow.prepare(WorkflowState::default()).await.unwrap();
        let mut execution = run.execution(ExecutionOptions::default()).await.unwrap();
        assert!(matches!(
            execution.step().await.unwrap(),
            Some(ExecutionEvent::Committed { .. })
        ));
        let cutoff = observer
            .checkpoint(CheckpointSelector::Latest)
            .await
            .unwrap()
            .sequence;
        let first = observer
            .history(None, NonZeroUsize::new(1).unwrap())
            .await
            .unwrap();
        assert_eq!(first.checkpoints.len(), 1);
        let cursor = first
            .next
            .expect("initial plus first action need another page");
        let cursor: HistoryCursor =
            serde_json::from_slice(&serde_json::to_vec(&cursor).unwrap()).unwrap();
        assert!(
            other
                .history(Some(cursor.clone()), NonZeroUsize::new(1).unwrap())
                .await
                .is_err(),
            "a real cursor cannot cross run scope"
        );
        assert!(matches!(
            execution.finish().await.unwrap().status,
            RunStatus::Succeeded
        ));
        let mut old = first.checkpoints;
        old.extend(HistoryCase::pages(&observer, Some(cursor), 2).await);
        assert_eq!(old.last().unwrap().sequence, cutoff);
        assert!(old.iter().all(|row| row.sequence <= cutoff));
        let current = HistoryCase::pages(&observer, None, 2).await;
        assert!(current.last().unwrap().sequence > cutoff);
        let sequences: Vec<_> = current
            .iter()
            .filter(|row| row.sequence <= cutoff)
            .map(|row| row.sequence)
            .collect();
        assert_eq!(
            old.iter().map(|row| row.sequence).collect::<Vec<_>>(),
            sequences
        );
    })
    .await;
}

#[tokio::test]
async fn bsp_success_selects_the_whole_wave_for_each_sibling_ordinal() {
    let (case, workflow) = HistoryCase::new(HistoryCase::parallel(&[]), None, false).await;
    case.close_after(async {
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        assert!(matches!(
            HistoryCase::finish(&mut run).await.status,
            RunStatus::Succeeded
        ));
        let earlier = run.checkpoint(CheckpointSelector::Action(1)).await.unwrap();
        let later = run.checkpoint(CheckpointSelector::Action(2)).await.unwrap();
        assert_eq!(earlier.sequence, later.sequence);
        assert!(earlier.outcomes == later.outcomes);
        assert_eq!(earlier.state.values()["count"], 30);
        assert_eq!(earlier.position.as_deref(), Some("z"));
        assert_eq!(earlier.action_ordinal, Some(2));
        assert_eq!(
            earlier
                .outcomes
                .iter()
                .map(|outcome| (outcome.node.as_str(), outcome.ordinal))
                .collect::<Vec<_>>(),
            vec![("a", 1), ("z", 2)]
        );
    })
    .await;
}

#[tokio::test]
async fn bsp_activation_failure_uses_its_proposed_ordinal_and_rolls_back_successful_sibling() {
    let (case, workflow) = HistoryCase::new(HistoryCase::parallel(&["z"]), None, false).await;
    case.close_after(async {
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        let result = HistoryCase::finish(&mut run).await;
        assert_eq!(result.last_action_ordinal, Some(0));
        assert_eq!(result.state.values()["count"], 0);
        assert!(matches!(
            result.status,
            RunStatus::Failed(WorkflowFailure::Evaluation(_))
        ));
        HistoryCase::failed(
            &run.checkpoint(CheckpointSelector::Action(2)).await.unwrap(),
            "z",
            2,
            Some(("seed", 0)),
            0,
        );
        assert!(
            matches!(
                run.checkpoint(CheckpointSelector::Action(1)).await,
                Err(AppError::NotFound(_))
            ),
            "staged successful a did not commit"
        );
    })
    .await;
}

#[tokio::test]
async fn bsp_barrier_typing_failure_is_attributed_to_first_canonical_contributor() {
    let (case, workflow) = HistoryCase::new(HistoryCase::parallel(&[]), None, true).await;
    let posts = Arc::clone(&case.posts);
    case.close_after(async {
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        let result = HistoryCase::finish(&mut run).await;
        assert!(matches!(
            result.status,
            RunStatus::Failed(WorkflowFailure::State(_))
        ));
        assert_eq!(result.last_action_ordinal, Some(0));
        HistoryCase::failed(
            &run.checkpoint(CheckpointSelector::Action(1)).await.unwrap(),
            "a",
            1,
            Some(("seed", 0)),
            0,
        );
        assert!(matches!(
            run.checkpoint(CheckpointSelector::Action(2)).await,
            Err(AppError::NotFound(_))
        ));
        let mut effects = posts.lock().unwrap().clone();
        effects.sort_unstable();
        assert_eq!(
            effects,
            vec![10, 20],
            "both actual candidates preceded aggregate validation"
        );
    })
    .await;
}

#[tokio::test]
async fn restore_is_parked_and_failed_saved_position_differs_from_prior_success() {
    let (case, workflow) = HistoryCase::new(HistoryCase::linear(2), Some(2), false).await;
    let posts = Arc::clone(&case.posts);
    case.close_after(async {
        let mut source = workflow.prepare(WorkflowState::default()).await.unwrap();
        HistoryCase::finish(&mut source).await;
        let selected = source
            .checkpoint(CheckpointSelector::Action(1))
            .await
            .unwrap();
        HistoryCase::failed(&selected, "n001", 1, Some(("n000", 0)), 1);
        let mut fork = workflow
            .restore(RestoreOptions {
                source: RunSelector::Latest(source.id().clone()),
                checkpoint: CheckpointSelector::Action(1),
                overrides: WorkflowState::default(),
                position: ResumePosition::AtSaved,
                fallback: None,
            })
            .await
            .unwrap();
        assert_ne!(fork.id(), source.id());
        let zero = fork
            .checkpoint(CheckpointSelector::Sequence(0))
            .await
            .unwrap();
        assert!(matches!(zero.kind, CheckpointKind::Initial));
        assert!(matches!(zero.status, RunStatus::Paused));
        assert_eq!(zero.state, selected.state);
        assert_eq!(zero.action_ordinal, Some(0));
        assert_eq!(zero.position.as_deref(), Some("n000"));
        assert!(zero.outcomes.is_empty());
        let lineage = zero.lineage.as_ref().unwrap();
        assert_eq!(&lineage.source, source.id());
        assert_eq!(lineage.checkpoint, selected.sequence);
        assert_eq!(lineage.action_ordinal, Some(1));
        assert_eq!(
            *posts.lock().unwrap(),
            vec![1, 2],
            "restore does not execute work"
        );
        let finished = HistoryCase::finish(&mut fork).await;
        assert!(matches!(finished.status, RunStatus::Succeeded));
        assert_eq!(finished.last_action_ordinal, Some(1));
        assert_eq!(*posts.lock().unwrap(), vec![1, 2, 2]);
        let unchanged = source
            .checkpoint(CheckpointSelector::Sequence(selected.sequence))
            .await
            .unwrap();
        HistoryCase::failed(&unchanged, "n001", 1, Some(("n000", 0)), 1);
        let source_head = source.snapshot().await.unwrap().checkpoint;
        source.delete(source_head).await.unwrap();
        assert!(matches!(
            source.checkpoint(CheckpointSelector::Latest).await,
            Err(AppError::NotFound(_))
        ));
        assert_eq!(
            fork.checkpoint(CheckpointSelector::Sequence(0))
                .await
                .unwrap()
                .state,
            zero.state,
            "copied fork state has no live source-history dependency"
        );
    })
    .await;
}

#[tokio::test]
async fn completed_restore_resumes_after_saved_action_and_missing_fallback_has_no_forged_ordinal() {
    let (case, workflow) = HistoryCase::new(HistoryCase::linear(2), None, false).await;
    let posts = Arc::clone(&case.posts);
    case.close_after(async {
        let mut source = workflow.prepare(WorkflowState::default()).await.unwrap();
        HistoryCase::finish(&mut source).await;
        let mut fork = workflow
            .restore(RestoreOptions {
                source: RunSelector::Latest(source.id().clone()),
                checkpoint: CheckpointSelector::Action(0),
                overrides: WorkflowState::default(),
                position: ResumePosition::AfterCompleted,
                fallback: None,
            })
            .await
            .unwrap();
        assert!(matches!(
            fork.snapshot().await.unwrap().status,
            RunStatus::Paused
        ));
        assert_eq!(*posts.lock().unwrap(), vec![1, 2]);
        HistoryCase::finish(&mut fork).await;
        assert_eq!(
            *posts.lock().unwrap(),
            vec![1, 2, 2],
            "completed n000 is not replayed"
        );
        assert!(matches!(
            workflow
                .restore(RestoreOptions {
                    source: RunSelector::Latest(source.id().clone()),
                    checkpoint: CheckpointSelector::Action(999),
                    overrides: WorkflowState::default(),
                    position: ResumePosition::Entrypoint,
                    fallback: None,
                })
                .await,
            Err(AppError::NotFound(_))
        ));
        let fallback = workflow
            .restore(RestoreOptions {
                source: RunSelector::Latest(source.id().clone()),
                checkpoint: CheckpointSelector::Action(999),
                overrides: WorkflowState::default(),
                position: ResumePosition::Entrypoint,
                fallback: Some(
                    WorkflowState::new(serde_json::Map::from_iter([("count".into(), json!(40))]))
                        .unwrap(),
                ),
            })
            .await
            .unwrap();
        let zero = fallback
            .checkpoint(CheckpointSelector::Sequence(0))
            .await
            .unwrap();
        assert!(matches!(zero.status, RunStatus::Paused));
        assert_eq!(zero.state.values()["count"], 40);
        assert!(zero.action_ordinal.is_none());
        assert!(zero.position.is_none());
        assert!(
            zero.lineage.is_none(),
            "missing selector cannot fabricate a source boundary"
        );
        assert!(zero.outcomes.is_empty());
    })
    .await;
}

#[tokio::test]
async fn retained_history_crosses_storage_pages_and_survives_old_expiry_and_reopen() {
    let (case, workflow) = HistoryCase::new(HistoryCase::linear(270), None, false).await;
    let store = case.store.clone();
    let definition = HistoryCase::linear(270);
    let posts = Arc::clone(&case.posts);
    let workspace_id = case.workspace.clone();
    let stage = Cell::new("preparation");
    // This scenario explicitly joins the first App before claiming a new generation.
    // Its 270 durable actions need a larger harness budget than the small cases;
    // this is history correctness coverage, not a throughput requirement.
    let observed =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(180), async {
            let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
            let run_id = run.id().clone();
            stage.set("execution");
            HistoryCase::finish(&mut run).await;
            stage.set("first history page");
            let page = run
                .history(None, NonZeroUsize::new(1).unwrap())
                .await
                .unwrap();
            let cursor = page.next.unwrap();
            stage.set("old expiry interval");
            tokio::time::sleep(Duration::from_millis(1100)).await;
            stage.set("paged history traversal");
            let rows = HistoryCase::pages(&run, None, 31).await;
            assert!(rows.len() > 256, "cross the native storage page boundary");
            assert_eq!(
                rows.iter()
                    .filter(|row| matches!(row.kind, CheckpointKind::Action))
                    .count(),
                270
            );
            stage.set("final action selector");
            assert_eq!(
                run.checkpoint(CheckpointSelector::Action(269))
                    .await
                    .unwrap()
                    .state
                    .values()["count"],
                270
            );
            (cursor, run_id)
        }))
        .catch_unwind()
        .await;
    // Capture at the observation boundary, before cleanup may change effects.
    let observed_stage = stage.get();
    let observed_effects = {
        let posts = posts.lock().unwrap();
        (posts.len(), posts.last().copied())
    };
    let mut app = case.app;
    let closed = app
        .shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(4),
        })
        .await;
    drop(app);
    let (cursor, run_id) = match observed {
        Ok(result) => result.unwrap_or_else(|_| {
            panic!("bounded retained traversal timed out at {observed_stage}; mutation count/last: {observed_effects:?}")
        }),
        Err(panic) => std::panic::resume_unwind(panic),
    };
    closed.expect("old App fully drained before reopen");
    assert_eq!(posts.lock().unwrap().len(), 270);
    let fresh_posts = Arc::new(Mutex::new(Vec::new()));
    let config: AppConfig<StateStore, Agent> = AppConfig::new(store, Vec::new(), vec![definition]);
    let mut reopened = App::open(config.with_state_typing(HistoryTyping {
        reject_once: None,
        reject_next: Arc::new(Mutex::new(None)),
        rejected: Arc::new(AtomicBool::new(false)),
        posts: Arc::clone(&fresh_posts),
        reject_reduced: false,
    }))
    .await
    .unwrap();
    let observed =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(20), async {
            let workspace = reopened.workspace(workspace_id).await.unwrap();
            let workflow = workspace
                .workflow("history".parse().unwrap())
                .await
                .unwrap();
            let run = workflow
                .open_run(RunSelector::Latest(run_id))
                .await
                .unwrap();
            assert_eq!(
                run.checkpoint(CheckpointSelector::Action(269))
                    .await
                    .unwrap()
                    .state
                    .values()["count"],
                270
            );
            assert!(
                run.history(Some(cursor), NonZeroUsize::new(1).unwrap())
                    .await
                    .is_err(),
                "cursor from the prior App generation cannot be reused"
            );
            assert!(
                fresh_posts.lock().unwrap().is_empty(),
                "reopen/history never replay completed actions"
            );
        }))
        .catch_unwind()
        .await;
    let closed = reopened
        .shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(4),
        })
        .await;
    drop(reopened);
    match observed {
        Ok(result) => result.expect("bounded reopened history observations"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    closed.expect("fresh App cleanup");
}

#[tokio::test]
async fn mixed_epoch_in_later_history_page_rejects_reopen_without_mutation() {
    let (case, workflow) = HistoryCase::new(HistoryCase::linear(100), None, false).await;
    let store = case.store.clone();
    let posts = Arc::clone(&case.posts);
    let workspace = case.workspace.clone();
    let mut saved = None;
    case.close_after(async {
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        let terminal = HistoryCase::finish(&mut run).await;
        assert!(matches!(terminal.status, RunStatus::Succeeded));
        saved = Some((run.id().clone(), terminal.occurrence, terminal.checkpoint));
    })
    .await;
    drop(workflow);
    let (run, occurrence, terminal) = saved.unwrap();
    let scope = format!("{workspace}/history/{run}/{occurrence}");
    let prefix = format!("history/{scope}/checkpoint/");
    let mut before = HistoryCase::records(&store).await;
    assert!(
        before.len() > 256,
        "retained history crosses the preflight page"
    );
    let original = before
        .iter()
        .skip(256)
        .find(|row| {
            row.record.key.key().starts_with(&prefix)
                && row.record.value["checkpoint"]["sequence"]
                    .as_u64()
                    .is_some_and(|sequence| sequence + 1 < terminal)
        })
        .expect("later page contains an old checkpoint, excluding current and predecessor")
        .clone();
    assert!(original.record.key.key() > before[255].record.key.key());
    assert!(
        before
            .iter()
            .all(|row| row.record.value["epoch"] == "app/v7")
    );
    assert!(
        before.iter().any(|row| row.record.key.key() == "app"
            && row.record.value["generation"].as_u64().is_some())
    );
    assert!(
        before
            .iter()
            .any(|row| row.record.key.key() == format!("run/{scope}")
                && row.record.value["sequence"] == terminal)
    );
    let mut corrupt = original.record.clone();
    Arc::make_mut(&mut corrupt.value)["epoch"] = json!("app/v5");
    let mut transaction = store.begin_transaction().await.unwrap();
    transaction.put(corrupt).await.unwrap();
    transaction.commit().await.unwrap();
    let corrupted = store
        .load(original.record.key.clone())
        .await
        .unwrap()
        .unwrap();
    *before
        .iter_mut()
        .find(|row| row.record.key == original.record.key)
        .unwrap() = corrupted;

    let fresh_posts = Arc::new(Mutex::new(Vec::new()));
    let mut accepted = None;
    let observed =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(20), async {
            // No Agent is supplied: the existing native-State fixture has no lower
            // runtime to dispatch. Its typing counter independently detects replay.
            let config: AppConfig<StateStore, Agent> =
                AppConfig::new(store.clone(), Vec::new(), vec![HistoryCase::linear(100)]);
            let error = match App::open(config.with_state_typing(HistoryTyping {
                reject_once: None,
                reject_next: Arc::new(Mutex::new(None)),
                rejected: Arc::new(AtomicBool::new(false)),
                posts: Arc::clone(&fresh_posts),
                reject_reduced: false,
            }))
            .await
            {
                Ok(app) => {
                    accepted = Some(app);
                    None
                }
                Err(error) => Some(error),
            };
            let after = HistoryCase::records(&store).await;
            (error, after == before)
        }))
        .catch_unwind()
        .await;
    // Restore the actual row before any unexpectedly admitted App is joined.
    let mut transaction = store.begin_transaction().await.unwrap();
    transaction.put(original.record).await.unwrap();
    transaction.commit().await.unwrap();
    let closed = match accepted.as_mut() {
        Some(app) => Some(
            tokio::time::timeout(
                Duration::from_secs(6),
                app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(4),
                }),
            )
            .await,
        ),
        None => None,
    };
    drop(accepted);
    if let Some(closed) = closed {
        closed
            .expect("unexpected reopened App cleanup is bounded")
            .expect("unexpected reopened App joins");
    }
    let (error, unchanged) = match observed {
        Ok(result) => result.expect("mixed-epoch preflight is bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    };
    assert!(
        matches!(error, Some(AppError::Integration(message)) if message == "unsupported app record schema version")
    );
    assert!(
        unchanged,
        "rejected reopen preserves every native value and revision, including the App generation"
    );
    assert_eq!(*posts.lock().unwrap(), (1..=100).collect::<Vec<i64>>());
    assert!(
        fresh_posts.lock().unwrap().is_empty(),
        "rejected reopen cannot replay actions"
    );
}

#[tokio::test]
async fn multiple_bsp_failures_choose_canonical_first_failed_activation() {
    let (case, workflow) = HistoryCase::new(HistoryCase::parallel(&["a", "z"]), None, false).await;
    case.close_after(async {
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        let result = HistoryCase::finish(&mut run).await;
        assert!(matches!(
            result.status,
            RunStatus::Failed(WorkflowFailure::Evaluation(_))
        ));
        HistoryCase::failed(
            &run.checkpoint(CheckpointSelector::Action(1)).await.unwrap(),
            "a",
            1,
            Some(("seed", 0)),
            0,
        );
        assert!(matches!(
            run.checkpoint(CheckpointSelector::Action(2)).await,
            Err(AppError::NotFound(_))
        ));
    })
    .await;
}

#[tokio::test]
async fn restoring_a_failed_bsp_representative_retries_the_entire_uncommitted_wave() {
    let (case, workflow) = HistoryCase::new(HistoryCase::parallel(&[]), None, true).await;
    let posts = Arc::clone(&case.posts);
    case.close_after(async {
        let mut source = workflow.prepare(WorkflowState::default()).await.unwrap();
        HistoryCase::finish(&mut source).await;
        let failed = source
            .checkpoint(CheckpointSelector::Action(1))
            .await
            .unwrap();
        HistoryCase::failed(&failed, "a", 1, Some(("seed", 0)), 0);
        let mut fork = workflow
            .restore(RestoreOptions {
                source: RunSelector::Latest(source.id().clone()),
                checkpoint: CheckpointSelector::Sequence(failed.sequence),
                overrides: WorkflowState::default(),
                position: ResumePosition::AtSaved,
                fallback: None,
            })
            .await
            .unwrap();
        let zero = fork
            .checkpoint(CheckpointSelector::Sequence(0))
            .await
            .unwrap();
        assert!(matches!(zero.status, RunStatus::Paused));
        assert_eq!(zero.action_ordinal, Some(0));
        assert_eq!(zero.state.values()["count"], 0);
        assert_eq!(posts.lock().unwrap().len(), 2, "no restore-driven effect");
        let result = HistoryCase::finish(&mut fork).await;
        assert!(matches!(result.status, RunStatus::Succeeded));
        assert_eq!(
            result.state.values()["count"],
            30,
            "both rolled-back contributors are retried"
        );
        assert_eq!(result.last_action_ordinal, Some(2));
        let row = fork
            .checkpoint(CheckpointSelector::Action(1))
            .await
            .unwrap();
        assert_eq!(
            row.outcomes
                .iter()
                .map(|outcome| (outcome.node.as_str(), outcome.ordinal))
                .collect::<Vec<_>>(),
            vec![("a", 1), ("z", 2)]
        );
        let mut effects = posts.lock().unwrap().clone();
        effects.sort_unstable();
        assert_eq!(effects, vec![10, 10, 20, 20]);
        HistoryCase::failed(
            &source
                .checkpoint(CheckpointSelector::Sequence(failed.sequence))
                .await
                .unwrap(),
            "a",
            1,
            Some(("seed", 0)),
            0,
        );
    })
    .await;
}

#[tokio::test]
async fn lineage_preserves_earlier_sibling_selector_separately_from_copied_position() {
    let (case, workflow) = HistoryCase::new(HistoryCase::parallel(&[]), None, false).await;
    case.close_after(async {
        let mut source = workflow.prepare(WorkflowState::default()).await.unwrap();
        HistoryCase::finish(&mut source).await;
        let wave = source
            .checkpoint(CheckpointSelector::Action(1))
            .await
            .unwrap();
        for (selector, pointer) in [
            (CheckpointSelector::Action(1), 1),
            (CheckpointSelector::Sequence(wave.sequence), 2),
        ] {
            let fork = workflow
                .restore(RestoreOptions {
                    source: RunSelector::Latest(source.id().clone()),
                    checkpoint: selector,
                    overrides: WorkflowState::default(),
                    position: ResumePosition::AfterCompleted,
                    fallback: None,
                })
                .await
                .unwrap();
            let zero = fork
                .checkpoint(CheckpointSelector::Sequence(0))
                .await
                .unwrap();
            let lineage = zero.lineage.as_ref().unwrap();
            assert_eq!(&lineage.source, source.id());
            assert_eq!(lineage.checkpoint, wave.sequence);
            assert_eq!(lineage.action_ordinal, Some(pointer));
            assert_eq!(zero.action_ordinal, Some(2));
            assert_eq!(zero.position.as_deref(), Some("z"));
            assert_eq!(zero.state, wave.state);
            assert!(zero.outcomes.is_empty());
        }
    })
    .await;
}

#[tokio::test]
async fn failed_sequence_lineage_points_at_attempt_while_copied_state_retains_prior_commit() {
    let (case, workflow) = HistoryCase::new(HistoryCase::linear(2), Some(2), false).await;
    case.close_after(async {
        let mut source = workflow.prepare(WorkflowState::default()).await.unwrap();
        HistoryCase::finish(&mut source).await;
        let failed = source
            .checkpoint(CheckpointSelector::Action(1))
            .await
            .unwrap();
        let fork = workflow
            .restore(RestoreOptions {
                source: RunSelector::Latest(source.id().clone()),
                checkpoint: CheckpointSelector::Sequence(failed.sequence),
                overrides: WorkflowState::default(),
                position: ResumePosition::AtSaved,
                fallback: None,
            })
            .await
            .unwrap();
        let zero = fork
            .checkpoint(CheckpointSelector::Sequence(0))
            .await
            .unwrap();
        let lineage = zero.lineage.as_ref().unwrap();
        assert_eq!(lineage.action_ordinal, Some(1));
        assert_eq!(lineage.checkpoint, failed.sequence);
        assert_eq!(zero.action_ordinal, Some(0));
        assert_eq!(zero.position.as_deref(), Some("n000"));
        assert_eq!(zero.state.values()["count"], 1);
    })
    .await;
}

#[tokio::test]
async fn missing_source_restore_uses_explicit_fallback_without_forged_lineage() {
    let (case, workflow) = HistoryCase::new(HistoryCase::linear(1), None, false).await;
    let posts = Arc::clone(&case.posts);
    case.close_after(async {
        let missing: RunId = "run_00000000-0000-4000-8000-000000000777".parse().unwrap();
        assert!(matches!(
            workflow
                .restore(RestoreOptions {
                    source: RunSelector::Latest(missing.clone()),
                    checkpoint: CheckpointSelector::Latest,
                    overrides: WorkflowState::default(),
                    position: ResumePosition::Entrypoint,
                    fallback: None,
                })
                .await,
            Err(AppError::NotFound(_))
        ));
        let fork = workflow
            .restore(RestoreOptions {
                source: RunSelector::Latest(missing),
                checkpoint: CheckpointSelector::Latest,
                overrides: WorkflowState::default(),
                position: ResumePosition::Entrypoint,
                fallback: Some(
                    WorkflowState::new(serde_json::Map::from_iter([("count".into(), json!(40))]))
                        .unwrap(),
                ),
            })
            .await
            .expect("a missing source is a missing loaded state for explicit fallback");
        let zero = fork.checkpoint(CheckpointSelector::Latest).await.unwrap();
        assert_eq!(zero.sequence, 0);
        assert!(matches!(zero.kind, CheckpointKind::Initial));
        assert!(matches!(zero.status, RunStatus::Paused));
        assert_eq!(zero.state.values()["count"], 40);
        assert!(zero.lineage.is_none() && zero.action_ordinal.is_none() && zero.position.is_none());
        assert!(zero.outcomes.is_empty() && posts.lock().unwrap().is_empty());
    })
    .await;
}

#[tokio::test]
async fn missing_selector_fallback_ignores_overrides_for_loaded_state() {
    let (case, workflow) = HistoryCase::new(HistoryCase::linear(1), None, false).await;
    case.close_after(async {
        let source = workflow.prepare(WorkflowState::default()).await.unwrap();
        // Fixed initializer.rs:221–230 returns default_state immediately on
        // None; the override branch at238–242 only runs for loaded state.
        let fork = workflow
            .restore(RestoreOptions {
                source: RunSelector::Latest(source.id().clone()),
                checkpoint: CheckpointSelector::Action(99),
                overrides: WorkflowState::new(serde_json::Map::from_iter([(
                    "count".into(),
                    json!(99),
                )]))
                .unwrap(),
                position: ResumePosition::Entrypoint,
                fallback: Some(
                    WorkflowState::new(serde_json::Map::from_iter([("count".into(), json!(40))]))
                        .unwrap(),
                ),
            })
            .await
            .unwrap();
        let zero = fork.checkpoint(CheckpointSelector::Latest).await.unwrap();
        assert_eq!(zero.state.values()["count"], 40);
        assert!(matches!(zero.status, RunStatus::Paused));
        assert!(zero.lineage.is_none() && zero.action_ordinal.is_none() && zero.position.is_none());
        assert!(zero.outcomes.is_empty());
    })
    .await;
}

#[tokio::test]
async fn missing_state_fallback_is_complete_state_without_definition_defaults() {
    let (case, workflow) = HistoryCase::new(HistoryCase::linear(1), None, false).await;
    let posts = Arc::clone(&case.posts);
    case.close_after(async {
        assert!(
            workflow
                .list_runs(None, NonZeroUsize::new(10).unwrap())
                .await
                .unwrap()
                .is_empty()
        );
        // Fixed initializer.rs:221–230 returns the supplied default_state as-is.
        // It is a complete fallback snapshot, not the fresh-run initial patch.
        let restored = workflow
            .restore(RestoreOptions {
                source: RunSelector::Latest(
                    "run_00000000-0000-4000-8000-000000000778".parse().unwrap(),
                ),
                checkpoint: CheckpointSelector::Latest,
                overrides: WorkflowState::default(),
                position: ResumePosition::Entrypoint,
                fallback: Some(WorkflowState::default()),
            })
            .await;
        assert!(
            matches!(restored, Err(AppError::InvalidInput(_))),
            "fallback missing required count cannot silently acquire the definition's count=0"
        );
        assert!(
            workflow
                .list_runs(None, NonZeroUsize::new(10).unwrap())
                .await
                .unwrap()
                .is_empty(),
            "invalid fallback must not admit a parked run"
        );
        assert!(
            posts.lock().unwrap().is_empty(),
            "invalid fallback invokes no mutation callback"
        );
    })
    .await;
}

#[tokio::test]
async fn retained_current_state_requires_declared_required_keys() {
    HistoryCase::assert_corrupt_state(false, RetainedStateFault::MissingRequired).await;
}

#[tokio::test]
async fn retained_predecessor_state_requires_declared_required_keys() {
    HistoryCase::assert_corrupt_state(true, RetainedStateFault::MissingRequired).await;
}

#[tokio::test]
async fn retained_current_state_keeps_workflow_state_byte_limit() {
    HistoryCase::assert_corrupt_state(false, RetainedStateFault::Oversized).await;
}

#[tokio::test]
async fn retained_predecessor_state_keeps_workflow_state_byte_limit() {
    HistoryCase::assert_corrupt_state(true, RetainedStateFault::Oversized).await;
}

impl HistoryCase {
    async fn missing_observation(
        store: &StateStore,
        workspace: &WorkspaceId,
        run: &WorkflowRun<StateStore, Agent, NoActions, NoConditions, HistoryTyping>,
        ordinal: u64,
    ) {
        let selected = run
            .checkpoint(CheckpointSelector::Action(ordinal))
            .await
            .unwrap();
        let key = StateKey::new(
            "app-kit",
            format!(
                "history/{workspace}/history/{}/{}/action/{ordinal:020}/{:020}",
                run.id(),
                selected.occurrence,
                u64::MAX - selected.sequence,
            ),
        )
        .unwrap();
        let original = store
            .load(key.clone())
            .await
            .unwrap()
            .expect("actual emitted index")
            .record;
        let before = run.snapshot().await.unwrap();
        let mut transaction = store.begin_transaction().await.unwrap();
        transaction.delete(key).await.unwrap();
        transaction.commit().await.unwrap();
        let observed = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(3), async {
            assert!(matches!(run.checkpoint(CheckpointSelector::Action(ordinal)).await,
                Err(AppError::Integration(_))),
                "a missing real observation index cannot mean absent history or an older observation");
            let after = run.snapshot().await.unwrap();
            assert_eq!(after.checkpoint, before.checkpoint);
            assert_eq!(after.state, before.state);
            assert_eq!(after.last_action_ordinal, before.last_action_ordinal);
            assert!(after.outcomes == before.outcomes, "selector failure does not advance execution");
        })).catch_unwind().await;
        let mut transaction = store.begin_transaction().await.unwrap();
        transaction.put(original).await.unwrap();
        transaction.commit().await.unwrap();
        match observed {
            Ok(result) => result.expect("missing observation read is bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        assert_eq!(
            run.checkpoint(CheckpointSelector::Action(ordinal))
                .await
                .unwrap()
                .sequence,
            selected.sequence
        );
    }
}

#[tokio::test]
async fn a_missing_earlier_success_index_is_corruption_with_a_later_committed_position() {
    let (case, workflow) = HistoryCase::new(HistoryCase::linear(3), None, false).await;
    let store = case.store.clone();
    let workspace = case.workspace.clone();
    let posts = Arc::clone(&case.posts);
    case.close_after(async {
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        let terminal = HistoryCase::finish(&mut run).await;
        assert_eq!(terminal.last_action_ordinal, Some(2));
        HistoryCase::missing_observation(&store, &workspace, &run, 0).await;
        assert_eq!(*posts.lock().unwrap(), vec![1, 2, 3]);
    })
    .await;
}

#[tokio::test]
async fn inherited_position_has_no_local_index_but_later_local_commits_require_theirs() {
    let (case, workflow) = HistoryCase::new(HistoryCase::linear(3), None, false).await;
    let store = case.store.clone();
    let workspace = case.workspace.clone();
    let posts = Arc::clone(&case.posts);
    case.close_after(async {
        let mut source = workflow.prepare(WorkflowState::default()).await.unwrap();
        HistoryCase::finish(&mut source).await;
        let mut fork = workflow
            .restore(RestoreOptions {
                source: RunSelector::Latest(source.id().clone()),
                checkpoint: CheckpointSelector::Action(0),
                overrides: WorkflowState::default(),
                position: ResumePosition::AfterCompleted,
                fallback: None,
            })
            .await
            .unwrap();
        let initial = fork.checkpoint(CheckpointSelector::Latest).await.unwrap();
        assert!(matches!(initial.kind, CheckpointKind::Initial));
        assert_eq!(initial.action_ordinal, Some(0));
        assert!(initial.outcomes.is_empty());
        assert!(matches!(
            fork.checkpoint(CheckpointSelector::Action(0)).await,
            Err(AppError::NotFound(_))
        ));
        let terminal = HistoryCase::finish(&mut fork).await;
        assert_eq!(terminal.last_action_ordinal, Some(2));
        assert!(matches!(
            fork.checkpoint(CheckpointSelector::Action(0)).await,
            Err(AppError::NotFound(_))
        ));
        assert!(matches!(
            fork.checkpoint(CheckpointSelector::Action(999)).await,
            Err(AppError::NotFound(_))
        ));
        HistoryCase::missing_observation(&store, &workspace, &fork, 1).await;
        assert_eq!(*posts.lock().unwrap(), vec![1, 2, 3, 2, 3]);
    })
    .await;
}

#[tokio::test]
async fn a_missing_failed_attempt_index_is_not_an_unattempted_ordinal() {
    let (case, workflow) = HistoryCase::new(HistoryCase::linear(2), Some(2), false).await;
    let store = case.store.clone();
    let workspace = case.workspace.clone();
    let posts = Arc::clone(&case.posts);
    case.close_after(async {
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        let failed = HistoryCase::finish(&mut run).await;
        assert!(matches!(
            failed.status,
            RunStatus::Failed(WorkflowFailure::State(_))
        ));
        assert_eq!(failed.last_action_ordinal, Some(0));
        assert!(matches!(
            run.checkpoint(CheckpointSelector::Action(1))
                .await
                .unwrap()
                .kind,
            CheckpointKind::Failed { ordinal: 1, .. }
        ));
        HistoryCase::missing_observation(&store, &workspace, &run, 1).await;
        assert_eq!(*posts.lock().unwrap(), vec![1, 2]);
    })
    .await;
}

#[tokio::test]
async fn a_missing_retry_success_index_cannot_silently_select_its_older_failed_attempt() {
    let (case, workflow) = HistoryCase::new(HistoryCase::linear(2), Some(2), false).await;
    let store = case.store.clone();
    let workspace = case.workspace.clone();
    let posts = Arc::clone(&case.posts);
    case.close_after(async {
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        HistoryCase::finish(&mut run).await;
        let failed = run.checkpoint(CheckpointSelector::Action(1)).await.unwrap();
        assert!(matches!(
            failed.kind,
            CheckpointKind::Failed { ordinal: 1, .. }
        ));
        let terminal = HistoryCase::finish(&mut run).await;
        assert!(matches!(terminal.status, RunStatus::Succeeded));
        let success = run.checkpoint(CheckpointSelector::Action(1)).await.unwrap();
        assert!(matches!(success.kind, CheckpointKind::Action));
        assert!(success.sequence > failed.sequence);
        HistoryCase::missing_observation(&store, &workspace, &run, 1).await;
        assert_eq!(*posts.lock().unwrap(), vec![1, 2, 2]);
    })
    .await;
}

#[tokio::test]
async fn retry_control_preserves_a_failed_observation_after_clearing_the_current_failure() {
    let (case, workflow) = HistoryCase::new(HistoryCase::linear(2), Some(2), false).await;
    let store = case.store.clone();
    let workspace = case.workspace.clone();
    let posts = Arc::clone(&case.posts);
    case.close_after(async {
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        HistoryCase::finish(&mut run).await;
        let failed = run.checkpoint(CheckpointSelector::Action(1)).await.unwrap();
        assert!(matches!(
            failed.kind,
            CheckpointKind::Failed { ordinal: 1, .. }
        ));
        let execution = run.execution(ExecutionOptions::default()).await.unwrap();
        let paused = execution.abort().await.unwrap();
        assert!(matches!(paused.status, RunStatus::Paused));
        assert_eq!(paused.last_action_ordinal, Some(0));
        assert_eq!(paused.state.values()["count"], 1);
        let latest = run.checkpoint(CheckpointSelector::Latest).await.unwrap();
        assert!(matches!(latest.kind, CheckpointKind::Control));
        assert!(latest.sequence > failed.sequence);
        assert_eq!(
            run.checkpoint(CheckpointSelector::Action(1))
                .await
                .unwrap()
                .sequence,
            failed.sequence
        );
        HistoryCase::missing_observation(&store, &workspace, &run, 1).await;
        assert_eq!(*posts.lock().unwrap(), vec![1, 2]);
    })
    .await;
}

#[tokio::test]
async fn sparse_failed_sibling_index_survives_a_later_lower_ordinal_failure() {
    let (case, workflow) = HistoryCase::new(HistoryCase::parallel(&[]), None, false).await;
    let store = case.store.clone();
    let workspace = case.workspace.clone();
    let posts = Arc::clone(&case.posts);
    let reject_next = Arc::clone(&case.reject_next);
    case.close_after(async {
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        *reject_next.lock().unwrap() = Some(20);
        let first = HistoryCase::finish(&mut run).await;
        assert!(matches!(
            first.status,
            RunStatus::Failed(WorkflowFailure::State(_))
        ));
        assert_eq!(first.last_action_ordinal, Some(0));
        let high = run.checkpoint(CheckpointSelector::Action(2)).await.unwrap();
        HistoryCase::failed(&high, "z", 2, Some(("seed", 0)), 0);
        assert!(
            matches!(
                run.checkpoint(CheckpointSelector::Action(1)).await,
                Err(AppError::NotFound(_))
            ),
            "a rolled-back successful sibling has no action observation"
        );
        *reject_next.lock().unwrap() = Some(10);
        HistoryCase::finish(&mut run).await;
        let low = run.checkpoint(CheckpointSelector::Action(1)).await.unwrap();
        HistoryCase::failed(&low, "a", 1, Some(("seed", 0)), 0);
        assert!(low.sequence > high.sequence);
        assert_eq!(
            run.checkpoint(CheckpointSelector::Action(2))
                .await
                .unwrap()
                .sequence,
            high.sequence
        );
        HistoryCase::missing_observation(&store, &workspace, &run, 2).await;
        assert_eq!(*posts.lock().unwrap(), vec![10, 20, 10, 20]);
    })
    .await;
}

#[tokio::test]
async fn a_missing_repeated_sparse_failure_cannot_select_its_older_same_ordinal_failure() {
    let (case, workflow) = HistoryCase::new(HistoryCase::parallel(&[]), None, false).await;
    let store = case.store.clone();
    let workspace = case.workspace.clone();
    let posts = Arc::clone(&case.posts);
    let reject_next = Arc::clone(&case.reject_next);
    case.close_after(async {
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        *reject_next.lock().unwrap() = Some(20);
        HistoryCase::finish(&mut run).await;
        let earlier = run.checkpoint(CheckpointSelector::Action(2)).await.unwrap();
        *reject_next.lock().unwrap() = Some(10);
        HistoryCase::finish(&mut run).await;
        *reject_next.lock().unwrap() = Some(20);
        let latest = HistoryCase::finish(&mut run).await;
        assert!(matches!(
            latest.status,
            RunStatus::Failed(WorkflowFailure::State(_))
        ));
        assert_eq!(latest.last_action_ordinal, Some(0));
        let repeated = run.checkpoint(CheckpointSelector::Action(2)).await.unwrap();
        HistoryCase::failed(&repeated, "z", 2, Some(("seed", 0)), 0);
        assert!(repeated.sequence > earlier.sequence);
        let lower = run.checkpoint(CheckpointSelector::Action(1)).await.unwrap();
        HistoryCase::failed(&lower, "a", 1, Some(("seed", 0)), 0);
        assert!(earlier.sequence < lower.sequence && lower.sequence < repeated.sequence);
        HistoryCase::missing_observation(&store, &workspace, &run, 2).await;
        assert_eq!(*posts.lock().unwrap(), vec![10, 20, 10, 20, 10, 20]);
    })
    .await;
}

#[tokio::test]
async fn committed_retry_supersedes_sparse_failures_while_a_restore_has_no_local_observations() {
    let (case, workflow) = HistoryCase::new(HistoryCase::parallel(&[]), None, false).await;
    let posts = Arc::clone(&case.posts);
    let reject_next = Arc::clone(&case.reject_next);
    case.close_after(async {
        let mut source = workflow.prepare(WorkflowState::default()).await.unwrap();
        *reject_next.lock().unwrap() = Some(20);
        HistoryCase::finish(&mut source).await;
        *reject_next.lock().unwrap() = Some(10);
        HistoryCase::finish(&mut source).await;
        let failed = source.checkpoint(CheckpointSelector::Action(1)).await.unwrap();
        let fork = workflow.restore(RestoreOptions {
            source: RunSelector::Latest(source.id().clone()), checkpoint: CheckpointSelector::Sequence(failed.sequence),
            overrides: WorkflowState::default(), position: ResumePosition::AtSaved, fallback: None,
        }).await.unwrap();
        let zero = fork.checkpoint(CheckpointSelector::Latest).await.unwrap();
        assert!(matches!(zero.kind, CheckpointKind::Initial));
        assert_eq!(zero.action_ordinal, Some(0));
        assert!(zero.outcomes.is_empty());
        for ordinal in [0, 1, 2] {
            assert!(matches!(fork.checkpoint(CheckpointSelector::Action(ordinal)).await, Err(AppError::NotFound(_))), "copied inherited position and failed source observations are not local observations");
        }
        let succeeded = HistoryCase::finish(&mut source).await;
        assert!(matches!(succeeded.status, RunStatus::Succeeded));
        assert_eq!(succeeded.state.values()["count"], 30);
        assert_eq!(succeeded.last_action_ordinal, Some(2));
        let one = source.checkpoint(CheckpointSelector::Action(1)).await.unwrap();
        let two = source.checkpoint(CheckpointSelector::Action(2)).await.unwrap();
        assert!(matches!(one.kind, CheckpointKind::Action));
        assert!(matches!(two.kind, CheckpointKind::Action));
        assert_eq!(one.sequence, two.sequence);
        assert!(one.sequence > failed.sequence);
        assert_eq!(one.outcomes.iter().map(|outcome| outcome.ordinal).collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(*posts.lock().unwrap(), vec![10, 20, 10, 20, 10, 20]);
    }).await;
}
#[tokio::test]
async fn unresolved_failure_evidence_rejects_oversized_maps_and_invalid_bounds() {
    let (case, workflow) = HistoryCase::new(HistoryCase::linear(2), Some(2), false).await;
    let store = case.store.clone();
    let workspace = case.workspace.clone();
    let posts = Arc::clone(&case.posts);
    case.close_after(async {
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        HistoryCase::finish(&mut run).await;
        let failed = run.checkpoint(CheckpointSelector::Action(1)).await.unwrap();
        let head_key = StateKey::new(
            "app-kit",
            format!("run/{workspace}/history/{}/{}", run.id(), failed.occurrence),
        )
        .unwrap();
        let head = store.load(head_key).await.unwrap().unwrap().record;
        let sequence = head.value["sequence"].as_u64().unwrap();
        let oversized: serde_json::Map<String, Value> = (1..=4097)
            .map(|ordinal| (ordinal.to_string(), json!(failed.sequence)))
            .collect();
        for (label, evidence) in [
            ("map count", Value::Object(oversized)),
            ("ordinal distance", json!({"4097":failed.sequence})),
            ("committed ordinal", json!({"0":failed.sequence})),
            (
                "duplicate failed sequence",
                json!({"1":failed.sequence,"2":failed.sequence}),
            ),
            ("initial sequence", json!({"1":0})),
            ("future sequence", json!({"1":sequence + 1})),
        ] {
            let mut forged = head.clone();
            Arc::make_mut(&mut forged.value)["unresolved_failures"] = evidence;
            let mut transaction = store.begin_transaction().await.unwrap();
            transaction.put(forged).await.unwrap();
            transaction.commit().await.unwrap();
            let observed =
                std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(3), async {
                    assert!(
                        matches!(
                            run.checkpoint(CheckpointSelector::Action(1)).await,
                            Err(AppError::Integration(_))
                        ),
                        "invalid {label} must not grant an action observation"
                    );
                }))
                .catch_unwind()
                .await;
            let mut transaction = store.begin_transaction().await.unwrap();
            transaction.put(head.clone()).await.unwrap();
            transaction.commit().await.unwrap();
            match observed {
                Ok(result) => result.expect("bounded index-evidence validation"),
                Err(panic) => std::panic::resume_unwind(panic),
            }
        }
        assert_eq!(
            run.checkpoint(CheckpointSelector::Action(1))
                .await
                .unwrap()
                .sequence,
            failed.sequence
        );
        assert_eq!(*posts.lock().unwrap(), vec![1, 2]);
    })
    .await;
}

#[tokio::test]
async fn unresolved_failure_evidence_cannot_replace_latest_failure_with_an_older_one() {
    let (case, workflow) = HistoryCase::new(HistoryCase::linear(2), None, false).await;
    let store = case.store.clone();
    let workspace = case.workspace.clone();
    let posts = Arc::clone(&case.posts);
    let reject_next = Arc::clone(&case.reject_next);
    case.close_after(async {
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        *reject_next.lock().unwrap() = Some(2);
        HistoryCase::finish(&mut run).await;
        let earlier = run.checkpoint(CheckpointSelector::Action(1)).await.unwrap();
        *reject_next.lock().unwrap() = Some(2);
        HistoryCase::finish(&mut run).await;
        let latest = run.checkpoint(CheckpointSelector::Action(1)).await.unwrap();
        assert!(latest.sequence > earlier.sequence);
        let head_key = StateKey::new(
            "app-kit",
            format!("run/{workspace}/history/{}/{}", run.id(), latest.occurrence),
        )
        .unwrap();
        let head = store.load(head_key).await.unwrap().unwrap().record;
        let mut forged = head.clone();
        Arc::make_mut(&mut forged.value)["unresolved_failures"] = json!({"1":earlier.sequence});
        let mut transaction = store.begin_transaction().await.unwrap();
        transaction.put(forged).await.unwrap();
        transaction.commit().await.unwrap();
        let observed =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(3), async {
                assert!(
                    matches!(
                        run.checkpoint(CheckpointSelector::Action(1)).await,
                        Err(AppError::Integration(_))
                    ),
                    "head evidence cannot downgrade the existing newer failure index"
                );
            }))
            .catch_unwind()
            .await;
        let mut transaction = store.begin_transaction().await.unwrap();
        transaction.put(head).await.unwrap();
        transaction.commit().await.unwrap();
        match observed {
            Ok(result) => result.expect("bounded stale-evidence validation"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        assert_eq!(
            run.checkpoint(CheckpointSelector::Action(1))
                .await
                .unwrap()
                .sequence,
            latest.sequence
        );
        assert_eq!(*posts.lock().unwrap(), vec![1, 2, 2]);
    })
    .await;
}

#[tokio::test]
async fn a_coordinated_failure_index_and_head_cannot_relabel_a_successful_action_row() {
    let (case, workflow) = HistoryCase::new(HistoryCase::linear(2), Some(2), false).await;
    let store = case.store.clone();
    let workspace = case.workspace.clone();
    let posts = Arc::clone(&case.posts);
    case.close_after(async {
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        HistoryCase::finish(&mut run).await;
        let success = run.checkpoint(CheckpointSelector::Action(0)).await.unwrap();
        let failed = run.checkpoint(CheckpointSelector::Action(1)).await.unwrap();
        assert!(success.sequence < failed.sequence);
        let scope = format!("{workspace}/history/{}/{}", run.id(), success.occurrence);
        let head = store.load(StateKey::new("app-kit", format!("run/{scope}")).unwrap()).await.unwrap().unwrap().record;
        let original = store.load(StateKey::new("app-kit", format!("history/{scope}/action/{:020}/{:020}", 1, u64::MAX-failed.sequence)).unwrap()).await.unwrap().unwrap().record;
        let mut forged_index = original.clone();
        forged_index.key = StateKey::new("app-kit", format!("history/{scope}/action/{:020}/{:020}", 1, u64::MAX-success.sequence)).unwrap();
        assert!(store.load(forged_index.key.clone()).await.unwrap().is_none());
        let mut forged_head = head.clone();
        Arc::make_mut(&mut forged_head.value)["unresolved_failures"] = json!({"1":success.sequence});
        let mut transaction = store.begin_transaction().await.unwrap();
        transaction.delete(original.key.clone()).await.unwrap();
        transaction.put(forged_index.clone()).await.unwrap();
        transaction.put(forged_head).await.unwrap();
        transaction.commit().await.unwrap();
        let observed = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(3), async {
            assert!(matches!(run.checkpoint(CheckpointSelector::Action(1)).await, Err(AppError::Integration(_))), "matching forged index and head still require an actual Failed observation of ordinal one");
        })).catch_unwind().await;
        let mut transaction = store.begin_transaction().await.unwrap();
        transaction.delete(forged_index.key).await.unwrap();
        transaction.put(original).await.unwrap();
        transaction.put(head).await.unwrap();
        transaction.commit().await.unwrap();
        match observed { Ok(result) => result.expect("bounded coordinated-index validation"), Err(panic) => std::panic::resume_unwind(panic) }
        assert_eq!(run.checkpoint(CheckpointSelector::Action(1)).await.unwrap().sequence, failed.sequence);
        assert_eq!(*posts.lock().unwrap(), vec![1, 2]);
    }).await;
}

#[tokio::test]
async fn active_and_retained_head_variants_reject_unknown_fields() {
    for retained in [false, true] {
        let (case, workflow) = HistoryCase::new(HistoryCase::linear(1), None, false).await;
        let store = case.store.clone();
        let workspace = case.workspace.clone();
        case.close_after(async {
            let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
            if retained {
                HistoryCase::finish(&mut run).await;
            }
            let before = run.snapshot().await.unwrap();
            let scope = format!("{workspace}/history/{}/{}", run.id(), before.occurrence);
            let head_key = StateKey::new("app-kit", format!("run/{scope}")).unwrap();
            let marker_key = StateKey::new("app-kit", format!("active/{scope}")).unwrap();
            let original = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let head = store.load(head_key.clone()).await.unwrap().unwrap().record;
                    let expected = if retained { "retained" } else { "active" };
                    if head.value.pointer("/retention/state") == Some(&json!(expected))
                        && store.load(marker_key.clone()).await.unwrap().is_some() != retained
                    {
                        break head;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("exact capacity state precedes the corruption");
            let mut forged = original.clone();
            Arc::make_mut(&mut forged.value)["retention"]["unexpected"] = json!(true);
            let mut transaction = store.begin_transaction().await.unwrap();
            transaction.put(forged).await.unwrap();
            transaction.commit().await.unwrap();
            let observed =
                std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(3), async {
                    assert!(
                        matches!(
                            run.checkpoint(CheckpointSelector::Latest).await,
                            Err(AppError::Integration(_))
                        ),
                        "a {retained:?} retention variant cannot discard unexpected durable fields"
                    );
                }))
                .catch_unwind()
                .await;
            let mut transaction = store.begin_transaction().await.unwrap();
            transaction.put(original).await.unwrap();
            transaction.commit().await.unwrap();
            match observed {
                Ok(result) => result.expect("retention decoding observation is bounded"),
                Err(panic) => std::panic::resume_unwind(panic),
            }
            let after = run.snapshot().await.unwrap();
            assert_eq!(after.checkpoint, before.checkpoint);
            assert_eq!(after.state, before.state);
            assert_eq!(after.last_action_ordinal, before.last_action_ordinal);
            assert!(
                after.outcomes == before.outcomes,
                "retention repair preserves exact outcomes"
            );
        })
        .await;
    }
}

#[tokio::test]
async fn completed_at_saved_restore_reexecutes_the_saved_node_before_its_successor() {
    let (case, workflow) = HistoryCase::new(HistoryCase::linear(2), None, false).await;
    let posts = Arc::clone(&case.posts);
    case.close_after(async {
        let mut source = workflow.prepare(WorkflowState::default()).await.unwrap();
        let original = HistoryCase::finish(&mut source).await;
        let saved = source
            .checkpoint(CheckpointSelector::Action(0))
            .await
            .unwrap();
        let mut fork = workflow
            .restore(RestoreOptions {
                source: RunSelector::Latest(source.id().clone()),
                checkpoint: CheckpointSelector::Action(0),
                overrides: WorkflowState::default(),
                position: ResumePosition::AtSaved,
                fallback: None,
            })
            .await
            .unwrap();
        let initial = fork
            .checkpoint(CheckpointSelector::Sequence(0))
            .await
            .unwrap();
        assert!(matches!(initial.status, RunStatus::Paused));
        assert_eq!(initial.state, saved.state);
        assert!(initial.outcomes.is_empty());
        assert_eq!(
            *posts.lock().unwrap(),
            vec![1, 2],
            "restoration does not execute a target"
        );
        let complete = HistoryCase::finish(&mut fork).await;
        assert!(matches!(complete.status, RunStatus::Succeeded));
        assert_eq!(
            *posts.lock().unwrap(),
            vec![1, 2, 1, 2],
            "at_saved executes n000, then routes n001"
        );
        assert_eq!(complete.state.values()["count"], 2);
        let retained = source.snapshot().await.unwrap();
        assert_eq!(retained.checkpoint, original.checkpoint);
        assert_eq!(retained.state, original.state);
        assert!(retained.outcomes == original.outcomes);
        let retained_saved = source
            .checkpoint(CheckpointSelector::Action(0))
            .await
            .unwrap();
        assert_eq!(retained_saved.sequence, saved.sequence);
        assert_eq!(retained_saved.state, saved.state);
        assert!(retained_saved.outcomes == saved.outcomes);
    })
    .await;
}

#[tokio::test]
async fn successful_at_saved_selects_an_earlier_bsp_sibling_without_replaying_its_peers() {
    let (case, workflow) = HistoryCase::new(HistoryCase::parallel(&[]), None, false).await;
    let posts = Arc::clone(&case.posts);
    case.close_after(async {
        let mut source = workflow.prepare(WorkflowState::default()).await.unwrap();
        let original = HistoryCase::finish(&mut source).await;
        let saved = source
            .checkpoint(CheckpointSelector::Action(1))
            .await
            .unwrap();
        assert_eq!(
            saved
                .outcomes
                .iter()
                .map(|outcome| (outcome.node.as_str(), outcome.ordinal))
                .collect::<Vec<_>>(),
            vec![("a", 1), ("z", 2)]
        );
        let mut fork = workflow
            .restore(RestoreOptions {
                source: RunSelector::Latest(source.id().clone()),
                checkpoint: CheckpointSelector::Action(1),
                overrides: WorkflowState::default(),
                position: ResumePosition::AtSaved,
                fallback: None,
            })
            .await
            .unwrap();
        let initial = fork
            .checkpoint(CheckpointSelector::Sequence(0))
            .await
            .unwrap();
        assert!(matches!(initial.status, RunStatus::Paused));
        assert_eq!(initial.state, saved.state);
        assert_eq!(
            initial.action_ordinal,
            Some(2),
            "source committed ordinal is independent of selected sibling"
        );
        assert_eq!(
            posts.lock().unwrap().len(),
            2,
            "restore does not execute a target"
        );
        let complete = HistoryCase::finish(&mut fork).await;
        assert!(matches!(complete.status, RunStatus::Succeeded));
        let mut effects = posts.lock().unwrap().clone();
        effects.sort_unstable();
        assert_eq!(effects, vec![10, 10, 20], "only selected a is replayed");
        assert_eq!(complete.last_action_ordinal, Some(3));
        let retained = source.snapshot().await.unwrap();
        assert_eq!(retained.checkpoint, original.checkpoint);
        assert_eq!(retained.state, original.state);
        assert!(retained.outcomes == original.outcomes);
    })
    .await;
}

#[tokio::test]
async fn successful_at_saved_preserves_an_unrelated_pending_join_frontier() {
    let definition = WorkflowDefinition::parse(&json!({
        "schema":"app-kit.workflow/v2","id":"history","revision":"r1",
        "state":{"count":0,"route_to_join":true},"required":["count","route_to_join"],
        "reducers":{"count":"reject_concurrent","route_to_join":"reject_concurrent"},"entry":"seed",
        "nodes":[
            {"id":"seed","step":{"kind":"state","mutations":[]}},
            {"id":"a","step":HistoryCase::assign(10)},
            {"id":"b","step":{"kind":"state","mutations":[]}},
            {"id":"c","step":HistoryCase::assign(20)},
            {"id":"join","step":{"kind":"join_all"}},
            {"id":"done","step":HistoryCase::assign(100)}
        ],"routes":{
            "seed":{"mode":"broadcast","branches":[{"target":{"kind":"node","id":"a"}},{"target":{"kind":"node","id":"b"}}]},
            "a":{"mode":"first","branches":[{"target":{"kind":"node","id":"join"}}]},
            "b":{"mode":"first","branches":[{"target":{"kind":"node","id":"c"}}]},
            "c":{"mode":"first","branches":[{"when":{"kind":"equal","left":{"kind":"state","path":"route_to_join"},"right":{"kind":"literal","value":true}},"target":{"kind":"node","id":"join"}}]},
            "join":{"mode":"first","branches":[{"target":{"kind":"node","id":"done"}}]}
        }
    }).to_string()).unwrap();
    let (case, workflow) = HistoryCase::new(definition, None, false).await;
    let posts = Arc::clone(&case.posts);
    case.close_after(async {
        let mut source = workflow.prepare(WorkflowState::default()).await.unwrap();
        let original = HistoryCase::finish(&mut source).await;
        assert!(matches!(original.status, RunStatus::Succeeded));
        let saved = source
            .checkpoint(CheckpointSelector::Action(3))
            .await
            .unwrap();
        assert_eq!(saved.outcomes[0].node, "c");
        let mut fork = workflow
            .restore(RestoreOptions {
                source: RunSelector::Latest(source.id().clone()),
                checkpoint: CheckpointSelector::Action(3),
                overrides: WorkflowState::new(serde_json::Map::from_iter([(
                    "route_to_join".into(),
                    json!(false),
                )]))
                .unwrap(),
                position: ResumePosition::AtSaved,
                fallback: None,
            })
            .await
            .unwrap();
        assert!(matches!(
            fork.snapshot().await.unwrap().status,
            RunStatus::Paused
        ));
        assert_eq!(*posts.lock().unwrap(), vec![10, 20, 100]);
        let complete = HistoryCase::finish(&mut fork).await;
        assert!(matches!(complete.status, RunStatus::Succeeded));
        assert_eq!(
            *posts.lock().unwrap(),
            vec![10, 20, 100, 20, 100],
            "c replays without routing to join; the original waiting join must survive"
        );
        assert_eq!(complete.state.values()["count"], 100);
        let retained = source.snapshot().await.unwrap();
        assert_eq!(retained.checkpoint, original.checkpoint);
        assert_eq!(retained.state, original.state);
        assert!(retained.outcomes == original.outcomes);
    })
    .await;
}

#[tokio::test]
async fn at_saved_sequence_after_cyclic_join_routing_replays_the_original_activation() {
    let definition = WorkflowDefinition::parse(&json!({
        "schema":"app-kit.workflow/v2","id":"history","revision":"r1",
        "state":{"count":0},"required":["count"],"reducers":{"count":"reject_concurrent"},"entry":"work",
        "nodes":[
            {"id":"work","step":{"kind":"state","mutations":[{"kind":"increment","target":"count","value":{"kind":"literal","value":1}}]}},
            {"id":"join","step":{"kind":"join_first"}}
        ],"routes":{
            "work":{"mode":"first","branches":[{"target":{"kind":"node","id":"join"}}]},
            "join":{"mode":"first","branches":[{"when":{"kind":"less_than","left":{"kind":"state","path":"count"},"right":{"kind":"literal","value":2}},"target":{"kind":"node","id":"work"}}]}
        }
    }).to_string()).unwrap();
    let (case, workflow) = HistoryCase::new(definition, None, false).await;
    let posts = Arc::clone(&case.posts);
    case.close_after(async {
        let mut source = workflow.prepare(WorkflowState::default()).await.unwrap();
        let original = HistoryCase::finish(&mut source).await;
        assert!(matches!(original.status, RunStatus::Succeeded));
        assert_eq!(*posts.lock().unwrap(), vec![1, 2]);
        let saved = source
            .checkpoint(CheckpointSelector::Action(2))
            .await
            .unwrap();
        assert_eq!(saved.position.as_deref(), Some("work"));
        let page = source
            .history(None, NonZeroUsize::new(100).unwrap())
            .await
            .unwrap();
        assert!(page.next.is_none());
        let after_routing = page
            .checkpoints
            .iter()
            .find(|row| {
                row.sequence > saved.sequence
                    && row.action_ordinal == Some(2)
                    && matches!(row.kind, CheckpointKind::Control)
            })
            .expect("real state-preserving routing checkpoint before the next join");
        assert_eq!(after_routing.state, saved.state);
        let mut fork = workflow
            .restore(RestoreOptions {
                source: RunSelector::Latest(source.id().clone()),
                checkpoint: CheckpointSelector::Sequence(after_routing.sequence),
                overrides: WorkflowState::default(),
                position: ResumePosition::AtSaved,
                fallback: None,
            })
            .await
            .unwrap();
        let initial = fork
            .checkpoint(CheckpointSelector::Sequence(0))
            .await
            .unwrap();
        assert!(matches!(initial.status, RunStatus::Paused));
        assert_eq!(initial.state, after_routing.state);
        assert_eq!(
            initial.lineage.as_ref().unwrap().checkpoint,
            after_routing.sequence
        );
        let complete = HistoryCase::finish(&mut fork).await;
        assert!(matches!(complete.status, RunStatus::Succeeded));
        assert_eq!(
            *posts.lock().unwrap(),
            vec![1, 2, 3],
            "the saved second work activation must replay before its join successor"
        );
        let replay = fork
            .checkpoint(CheckpointSelector::Action(3))
            .await
            .unwrap();
        assert_eq!(replay.outcomes[0].node, "work");
        assert_eq!(complete.last_action_ordinal, Some(4));
        let final_join = source
            .checkpoint(CheckpointSelector::Action(3))
            .await
            .unwrap();
        assert_eq!(final_join.position.as_deref(), Some("join"));
        assert!(
            original.checkpoint > final_join.sequence + 1,
            "terminal selection lies beyond the original Action and its routing successor"
        );
        let mut join_fork = workflow
            .restore(RestoreOptions {
                source: RunSelector::Latest(source.id().clone()),
                checkpoint: CheckpointSelector::Sequence(original.checkpoint),
                overrides: WorkflowState::default(),
                position: ResumePosition::AtSaved,
                fallback: None,
            })
            .await
            .unwrap();
        let initial = join_fork
            .checkpoint(CheckpointSelector::Sequence(0))
            .await
            .unwrap();
        assert!(matches!(initial.status, RunStatus::Paused));
        assert_eq!(initial.state, original.state);
        assert_eq!(
            initial.lineage.as_ref().unwrap().checkpoint,
            original.checkpoint
        );
        let complete = HistoryCase::finish(&mut join_fork).await;
        assert!(matches!(complete.status, RunStatus::Succeeded));
        assert_eq!(
            complete.last_action_ordinal,
            Some(4),
            "the selected final join visit must replay once"
        );
        let replay = join_fork
            .checkpoint(CheckpointSelector::Action(4))
            .await
            .unwrap();
        assert_eq!(replay.outcomes.len(), 1);
        assert_eq!(replay.outcomes[0].node, "join");
        assert_eq!(complete.state, original.state);
        assert_eq!(
            *posts.lock().unwrap(),
            vec![1, 2, 3],
            "join replay must not repeat either source work action"
        );
        let retained = source.snapshot().await.unwrap();
        assert_eq!(retained.checkpoint, original.checkpoint);
        assert_eq!(retained.state, original.state);
        assert!(retained.outcomes == original.outcomes);
    })
    .await;
}

#[derive(Clone, Copy)]
enum CopyAuthorityFault {
    SequenceTail,
    HistoricalIdentity,
}

impl HistoryCase {
    async fn rejects_corrupt_copy_authority(fault: CopyAuthorityFault) {
        let (case, workflow) = Self::new(Self::linear(2), None, false).await;
        let store = case.store.clone();
        let workspace = case.workspace.clone();
        let posts = Arc::clone(&case.posts);
        case.close_after(async {
            let mut source = workflow.prepare(WorkflowState::default()).await.unwrap();
            let terminal = Self::finish(&mut source).await;
            let saved = source.checkpoint(CheckpointSelector::Action(0)).await.unwrap();
            assert!(saved.sequence > 0 && saved.sequence + 1 < terminal.checkpoint);
            let scope = format!("{workspace}/history/{}/{}", source.id(), saved.occurrence);
            let originals = match fault {
                CopyAuthorityFault::SequenceTail => {
                    let key = StateKey::new("app-kit", format!(
                        "projection/{workspace}/history/{}/sequence/{:020}", source.id(), saved.sequence,
                    )).unwrap();
                    let row = store.load(key).await.unwrap().unwrap().record;
                    assert_eq!(row.value["last"]["occurrence"], saved.occurrence);
                    assert_eq!(row.value["last"]["sequence"], saved.sequence);
                    vec![row]
                }
                CopyAuthorityFault::HistoricalIdentity => {
                    let mut rows = Vec::new();
                    for sequence in [saved.sequence - 1, saved.sequence] {
                        rows.push(store.load(StateKey::new("app-kit", format!(
                            "history/{scope}/checkpoint/{sequence:020}",
                        )).unwrap()).await.unwrap().unwrap().record);
                    }
                    assert_eq!(rows[0].value["workspace_revision"], rows[1].value["workspace_revision"]);
                    rows
                }
            };
            let mut write = store.begin_transaction().await.unwrap();
            for original in &originals {
                let mut altered = original.clone();
                match fault {
                    CopyAuthorityFault::SequenceTail => {
                        Arc::make_mut(&mut altered.value)["last"]["sequence"] = json!(saved.sequence + 1);
                    }
                    CopyAuthorityFault::HistoricalIdentity => {
                        let revision = original.value["workspace_revision"].as_u64().unwrap();
                        Arc::make_mut(&mut altered.value)["workspace_revision"] = json!(revision.checked_add(1).unwrap());
                    }
                }
                write.put(altered).await.unwrap();
            }
            write.commit().await.unwrap();
            let observed = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(3), async {
                let source_selector = match fault {
                    CopyAuthorityFault::SequenceTail => RunSelector::Latest(source.id().clone()),
                    CopyAuthorityFault::HistoricalIdentity => RunSelector::Exact {
                        id: source.id().clone(), occurrence: saved.occurrence.clone(),
                    },
                };
                assert!(matches!(workflow.restore(RestoreOptions {
                    source: source_selector, checkpoint: CheckpointSelector::Sequence(saved.sequence),
                    overrides: WorkflowState::default(), position: ResumePosition::Entrypoint, fallback: None,
                }).await, Err(AppError::Integration(_))),
                    "copy selection requires the exact requested sequence and the captured occurrence's identity");
            })).catch_unwind().await;
            // Both immutable source rows are restored before any assertion can
            // unwind cleanup. The current head and native source values never move.
            let mut write = store.begin_transaction().await.unwrap();
            for original in originals { write.put(original).await.unwrap(); }
            write.commit().await.unwrap();
            match observed {
                Ok(result) => result.expect("corrupt copy selection remains bounded"),
                Err(panic) => std::panic::resume_unwind(panic),
            }
            assert_eq!(workflow.list_runs(None, NonZeroUsize::new(4).unwrap()).await.unwrap().len(), 1,
                "invalid source authority cannot admit a fresh copy");
            let unchanged = source.snapshot().await.unwrap();
            assert_eq!(unchanged.checkpoint, terminal.checkpoint);
            assert_eq!(unchanged.state, terminal.state);
            assert!(unchanged.outcomes == terminal.outcomes);
            assert_eq!(*posts.lock().unwrap(), vec![1, 2]);
        }).await;
    }
}

#[tokio::test]
async fn aggregate_sequence_copy_rejects_a_tail_pointing_to_another_real_checkpoint() {
    HistoryCase::rejects_corrupt_copy_authority(CopyAuthorityFault::SequenceTail).await;
}

#[tokio::test]
async fn historical_copy_rejects_coordinated_source_and_predecessor_identity_drift() {
    HistoryCase::rejects_corrupt_copy_authority(CopyAuthorityFault::HistoricalIdentity).await;
}

#[tokio::test]
async fn prepared_checkpoint_requires_explicit_sequence_when_no_action_has_been_saved() {
    let (case, workflow) = HistoryCase::new(HistoryCase::linear(2), None, false).await;
    let posts = Arc::clone(&case.posts);
    case.close_after(async {
        let source = workflow.prepare(WorkflowState::default()).await.unwrap();
        let initial = source.snapshot().await.unwrap();
        assert_eq!(initial.checkpoint, 0);
        for selector in [
            RunSelector::Latest(source.id().clone()),
            RunSelector::Exact {
                id: source.id().clone(),
                occurrence: initial.occurrence.clone(),
            },
        ] {
            assert!(
                matches!(
                    workflow
                        .restore(RestoreOptions {
                            source: selector,
                            checkpoint: CheckpointSelector::Latest,
                            overrides: WorkflowState::default(),
                            position: ResumePosition::Entrypoint,
                            fallback: None,
                        })
                        .await,
                    Err(AppError::NotFound(_))
                ),
                "initial/control observations are not saved actions for implicit copying"
            );
        }
        let fork = workflow
            .restore(RestoreOptions {
                source: RunSelector::Exact {
                    id: source.id().clone(),
                    occurrence: initial.occurrence.clone(),
                },
                checkpoint: CheckpointSelector::Sequence(0),
                overrides: WorkflowState::default(),
                position: ResumePosition::Entrypoint,
                fallback: None,
            })
            .await
            .unwrap();
        let copied = fork
            .checkpoint(CheckpointSelector::Sequence(0))
            .await
            .unwrap();
        assert_eq!(copied.state, initial.state);
        let lineage = copied.lineage.as_ref().unwrap();
        assert_eq!(&lineage.source, source.id());
        assert_eq!(lineage.source_occurrence, initial.occurrence);
        assert_eq!(lineage.checkpoint, 0);
        assert!(copied.action_ordinal.is_none() && copied.outcomes.is_empty());
        assert_eq!(
            workflow
                .list_runs(None, NonZeroUsize::new(4).unwrap())
                .await
                .unwrap()
                .len(),
            2
        );
        assert_eq!(source.snapshot().await.unwrap().checkpoint, 0);
        assert!(posts.lock().unwrap().is_empty());
    })
    .await;
}

#[tokio::test]
async fn retained_failure_record_observes_exact_compact_json_byte_boundary() {
    let mut integers = Vec::new();
    for exponent in 0..=19 {
        let power = 10_u64.checked_pow(exponent).unwrap();
        for value in [power - 1, power, power + 1] {
            integers.push(Value::from(value));
            if let Ok(signed) = i64::try_from(value) {
                integers.push(Value::from(signed));
                integers.push(Value::from(-signed));
            }
        }
    }
    let controls = (0_u8..=31).map(char::from).collect::<String>();
    let escaped = format!("{controls}\"\\/\u{7f}é水🦀\u{2028}\u{2029}");
    let corpus = Value::Object(serde_json::Map::from_iter([
        ("z".into(), json!([null, true, false, [], {}])),
        (
            escaped.clone(),
            json!({"nested":[{"":escaped.clone()}, [1, -1]]}),
        ),
        (
            "a".into(),
            json!([i64::MIN, u64::MAX, -0.0_f64, 1.25, 1e-300, 1e300]),
        ),
        ("integers".into(), Value::Array(integers)),
    ]));
    let definition = WorkflowDefinition::parse(
        &json!({
            "schema":"app-kit.workflow/v2", "id":"history", "revision":"r1",
            "state":{"count":0,"oracle":null}, "required":["count","oracle"],
            "reducers":{"count":"reject_concurrent","oracle":"reject_concurrent"},
            "entry":"n000", "nodes":[
                {"id":"n000","step":HistoryCase::assign(1)},
                {"id":"n001","step":HistoryCase::assign(2)}
            ], "routes":{"n000":{"mode":"first","branches":[
                {"target":{"kind":"node","id":"n001"}}
            ]}}
        })
        .to_string(),
    )
    .unwrap();
    let (case, workflow) = HistoryCase::new(definition, Some(2), false).await;
    let store = case.store.clone();
    let workspace = case.workspace.clone();
    let posts = Arc::clone(&case.posts);
    case.close_after(async {
        let mut run = workflow.prepare(WorkflowState::new(serde_json::Map::from_iter([
            ("oracle".into(), corpus.clone()),
        ])).unwrap()).await.unwrap();
        let prepared = run.checkpoint(CheckpointSelector::Sequence(0)).await.unwrap();
        assert_eq!(prepared.state.values()["oracle"], corpus,
            "public admission persists the string, key, numeric and nested Value corpus");
        let terminal = HistoryCase::finish(&mut run).await;
        assert!(matches!(terminal.status, RunStatus::Failed(WorkflowFailure::State(_))));
        let selected = run.checkpoint(CheckpointSelector::Sequence(terminal.checkpoint)).await.unwrap();
        let scope = format!("{workspace}/history/{}/{}", run.id(), terminal.occurrence);
        let key = StateKey::new("app-kit", format!("history/{scope}/checkpoint/{:020}", terminal.checkpoint)).unwrap();
        let original = store.load(key).await.unwrap().unwrap().record;
        assert_eq!(original.value["checkpoint"]["phase"]["state"], "failed");
        assert_eq!(original.value["checkpoint"]["controls"], json!({}));
        assert_eq!(original.value["checkpoint"]["staged"], json!([]));
        assert_eq!(original.value["checkpoint"]["state"]["oracle"], corpus);
        let request = StateListRequest::new("app-kit").unwrap().with_limit(256).unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let page = store.list(request.clone()).await.unwrap();
                assert!(page.next_start_after.is_none());
                if page.records.iter().any(|row| row.record.key.key() == format!("run/{scope}")
                    && row.record.value["retention"]["state"] == "retained")
                    && !page.records.iter().any(|row| row.record.key.key() == format!("active/{scope}"))
                {
                    // Applied receipts may remain as a bounded idle page. Keep
                    // them in the exact key/value/revision no-write oracle below.
                    let stable = store.list(request.clone()).await.unwrap();
                    assert!(stable.next_start_after.is_none());
                    if stable.records == page.records {
                        break;
                    }
                }
                tokio::task::yield_now().await;
            }
        }).await.expect("original terminal capacity and native rows stabilize before faults");
        for prefix in ["", escaped.as_str()] {
            for excess in [0, 1] {
                let mut changed = original.clone();
                Arc::make_mut(&mut changed.value)["checkpoint"]["phase"]["message"] = json!(prefix);
                // The bound belongs to the complete inline-equivalent row,
                // while v7 persists an immutable-page descriptor. This state-only
                // workflow has no settlements; preserve its real descriptor on disk.
                let mut logical = changed.value.as_ref().clone();
                let descriptor = std::mem::replace(&mut logical["checkpoint"]["settlements"], json!([]));
                assert_eq!(descriptor, json!({"count":0,"pages":[]}));
                let bound = 4 * 1_048_576 + excess;
                let padding = bound - serde_json::to_vec(&logical).unwrap().len();
                let message = format!("{prefix}{}", "x".repeat(padding));
                Arc::make_mut(&mut changed.value)["checkpoint"]["phase"]["message"] = json!(message);
                logical["checkpoint"]["phase"]["message"] = json!(message);
                assert_eq!(serde_json::to_vec(&logical).unwrap().len(), bound);
                assert_eq!(changed.value["checkpoint"]["settlements"], descriptor);
                drop(logical);
                let mut transaction = store.begin_transaction().await.unwrap();
                transaction.put(changed).await.unwrap();
                transaction.commit().await.unwrap();
                let observed = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(3), async {
                    let before = store.list(request.clone()).await.unwrap();
                    assert!(before.next_start_after.is_none());
                    let read = run.checkpoint(CheckpointSelector::Sequence(terminal.checkpoint)).await;
                    let after = store.list(request.clone()).await.unwrap();
                    (read, before.records == after.records && after.next_start_after.is_none())
                })).catch_unwind().await;
                // Restore the original recognized failure field before any assertion
                // can unwind into App cleanup. No schema extension is introduced.
                let mut transaction = store.begin_transaction().await.unwrap();
                transaction.put(original.clone()).await.unwrap();
                transaction.commit().await.unwrap();
                let (read, unchanged) = match observed {
                    Ok(result) => result.expect("native byte-boundary observation remains bounded"),
                    Err(panic) => std::panic::resume_unwind(panic),
                };
                assert!(unchanged, "accepted and rejected reads preserve all native values and revisions");
                if excess == 0 {
                    let checkpoint = read.expect("the exact inclusive durable-record byte bound is readable");
                    assert_eq!(checkpoint.sequence, selected.sequence);
                    assert_eq!(checkpoint.position, selected.position);
                    assert_eq!(checkpoint.action_ordinal, selected.action_ordinal);
                    assert_eq!(checkpoint.state.values(), selected.state.values());
                    assert!(checkpoint.outcomes == selected.outcomes);
                    assert!(matches!(checkpoint.status, RunStatus::Failed(WorkflowFailure::State(actual)) if actual == message),
                        "the original primary failure category projects the exact selected message");
                } else {
                    assert!(matches!(read, Err(AppError::Integration(message)) if message == "app durable record exceeds the private byte bound"),
                        "one additional encoded byte is rejected by the durable-record bound");
                }
            }
        }
        assert_eq!(*posts.lock().unwrap(), vec![1, 2], "native reads never replay state effects");
        assert_eq!(run.checkpoint(CheckpointSelector::Latest).await.unwrap().state.values(), selected.state.values());
    }).await;
}

/// Fresh Lance 13 connections preserve App v7 history across settled owners.
#[tokio::test]
async fn cold_lance_directory_reopen_preserves_v7_checkpoint_history() {
    let directory =
        std::env::temp_dir().join(format!("ai-kit-app-lance-history-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).expect("new owned directory");
    let checked = std::panic::AssertUnwindSafe(async {
        let original = HistoryCase::persistence_round(&directory, None).await;
        let extended = HistoryCase::persistence_round(&directory, Some(&original)).await;
        assert_eq!(extended["runs"].as_array().unwrap().len(), 2);
        let reopened = HistoryCase::persistence_round(&directory, Some(&extended)).await;
        assert_eq!(reopened, extended, "both runs survive another cold reopen");
    })
    .catch_unwind()
    .await;
    let removed = std::fs::remove_dir_all(&directory);
    if let Err(error) = &removed {
        eprintln!("directory cleanup failed: {:?}", error.kind());
    }
    if let Err(panic) = checked {
        std::panic::resume_unwind(panic);
    }
    removed.expect("owned directory cleanup");
}
