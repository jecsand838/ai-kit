//! Exact configured revision selection and durable catalog obligations.
//! Native State workflows and real LanceDB exercise public admission/reopen paths.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use agents_kit::Agent;
use app_kit::workflow::{
    ExecutionOptions, RunSelector, RunStatus, Workflow, WorkflowDefinition, WorkflowState,
};
use app_kit::{App, AppConfig, AppError, Shutdown, WorkspaceConfig, WorkspaceId};
use futures_util::FutureExt;
use serde_json::json;
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_kit::state::record::{StateKey, StateKeyPrefix, StateListRequest};
use storage_kit::{StateStore, StateStoreBackend, StateTransactionBackend};

struct Revisions {
    app: App<StateStore, Agent>,
}

impl Revisions {
    fn definition(revision: &str, count: u64) -> WorkflowDefinition {
        WorkflowDefinition::parse(
            &json!({
                "schema":"app-kit.workflow/v2", "id":"revision", "revision":revision,
                "state":{"count":0}, "required":["count"], "reducers":{"count":"reject_concurrent"},
                "entry":"set", "nodes":[{"id":"set","step":{"kind":"state","mutations":[
                    {"kind":"assign","target":"count","value":{"kind":"literal","value":count}}
                ]}}], "routes":{}
            })
            .to_string(),
        )
        .unwrap()
    }

    async fn store() -> StateStore {
        StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap()
    }

    async fn open(
        raw: &StateStore,
        revisions: &[(&str, u64)],
        workspace: Option<WorkspaceId>,
    ) -> (Self, Workflow<StateStore, Agent>, WorkspaceId) {
        let app = App::open(AppConfig::new(
            raw.clone(),
            Vec::new(),
            revisions
                .iter()
                .map(|(revision, count)| Self::definition(revision, *count))
                .collect(),
        ))
        .await
        .unwrap();
        let workspace = match workspace {
            Some(id) => app.workspace(id).await.unwrap(),
            None => app
                .create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().unwrap(),
                })
                .await
                .unwrap(),
        };
        let id = workspace.id().clone();
        let workflow = workspace
            .workflow("revision".parse().unwrap())
            .await
            .unwrap();
        (Self { app }, workflow, id)
    }

    async fn close_after<T>(mut self, superseded: bool, observation: impl Future<Output = T>) -> T {
        let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(10), observation))
            .catch_unwind()
            .await;
        let closed = tokio::time::timeout(
            Duration::from_secs(6),
            self.app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(4),
            }),
        )
        .await;
        drop(self.app);
        let value = match observed {
            Ok(result) => result.expect("bounded revision observation"),
            Err(panic) => std::panic::resume_unwind(panic),
        };
        match closed.expect("revision App cleanup joins") {
            Ok(()) => {}
            Err(AppError::Closed(_)) if superseded => {}
            Err(error) => panic!("revision App cleanup failed: {error}"),
        }
        value
    }

    async fn reject(raw: &StateStore, revisions: &[(&str, u64)], integration: bool) {
        let key = StateKey::new("app-kit", "app").unwrap();
        let before = raw.load(key.clone()).await.unwrap().unwrap();
        let result: Result<App<StateStore, Agent>, AppError> = tokio::time::timeout(
            Duration::from_secs(6),
            App::open(AppConfig::new(
                raw.clone(),
                Vec::new(),
                revisions
                    .iter()
                    .map(|(revision, count)| Self::definition(revision, *count))
                    .collect(),
            )),
        )
        .await
        .expect("rejected reopen is bounded");
        let error = match result {
            Ok(mut app) => {
                let cleanup = tokio::time::timeout(
                    Duration::from_secs(6),
                    app.shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_secs(4),
                    }),
                )
                .await;
                drop(app);
                cleanup.expect("unexpected opened App still joins").unwrap();
                None
            }
            Err(error) => Some(error),
        };
        let after = raw.load(key).await.unwrap().unwrap();
        assert!(
            if integration {
                matches!(error, Some(AppError::Integration(_)))
            } else {
                matches!(error, Some(AppError::Conflict(_)))
            },
            "exact revision rejection category"
        );
        assert_eq!(
            after.revision, before.revision,
            "rejection precedes generation mutation"
        );
        assert_eq!(after.record.value, before.record.value);
    }
}

#[tokio::test]
async fn caller_order_selects_new_runs_while_saved_runs_keep_their_exact_revision() {
    let raw = Revisions::store().await;
    let (case, workflow, workspace) = Revisions::open(&raw, &[("z-old", 1)], None).await;
    let (old, (case, workflow, _)) = case
        .close_after(true, async {
            let old = workflow
                .prepare(WorkflowState::default())
                .await
                .unwrap()
                .id()
                .clone();
            let fresh = Revisions::open(
                &raw,
                &[("z-old", 1), ("a-new/λ", 2)],
                Some(workspace.clone()),
            )
            .await;
            (old, fresh)
        })
        .await;
    case.close_after(false, async {
        let mut saved = workflow
            .open_run(RunSelector::Latest(old.clone()))
            .await
            .unwrap();
        let execution = saved.execution(ExecutionOptions::default()).await.unwrap();
        let saved = execution.finish().await.unwrap();
        assert_eq!(saved.state.values()["count"], 1);
        let current = workflow
            .start(ExecutionOptions::default())
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert_eq!(
            current.state.values()["count"],
            2,
            "caller order is not lexical revision order"
        );
    })
    .await;
    let (case, workflow, _) =
        Revisions::open(&raw, &[("a-new/λ", 2), ("z-old", 1)], Some(workspace)).await;
    case.close_after(false, async {
        let current = workflow
            .start(ExecutionOptions::default())
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert_eq!(current.state.values()["count"], 1);
        assert_eq!(
            workflow
                .open_run(RunSelector::Latest(old))
                .await
                .unwrap()
                .snapshot()
                .await
                .unwrap()
                .state
                .values()["count"],
            1
        );
    })
    .await;
}

#[tokio::test]
async fn retained_revision_is_required_until_its_history_is_explicitly_deleted() {
    let raw = Revisions::store().await;
    let (case, workflow, workspace) = Revisions::open(&raw, &[("old", 1)], None).await;
    let old = case
        .close_after(false, async {
            let run = workflow.start(ExecutionOptions::default()).await.unwrap();
            assert!(matches!(
                run.wait().await.unwrap().status,
                RunStatus::Succeeded
            ));
            run.id().clone()
        })
        .await;
    Revisions::reject(&raw, &[("new", 2)], false).await;
    let (case, workflow, _) =
        Revisions::open(&raw, &[("old", 1), ("new", 2)], Some(workspace.clone())).await;
    case.close_after(false, async {
        let mut run = workflow.open_run(RunSelector::Latest(old)).await.unwrap();
        let checkpoint = run.snapshot().await.unwrap().checkpoint;
        run.delete(checkpoint).await.unwrap();
    })
    .await;
    let (case, workflow, _) = Revisions::open(&raw, &[("new", 2)], Some(workspace)).await;
    case.close_after(false, async {
        assert_eq!(
            workflow
                .start(ExecutionOptions::default())
                .await
                .unwrap()
                .wait()
                .await
                .unwrap()
                .state
                .values()["count"],
            2
        );
    })
    .await;
}

#[tokio::test]
async fn unused_catalog_revisions_can_be_omitted_but_cannot_be_redefined_later() {
    let raw = Revisions::store().await;
    let (case, _, workspace) = Revisions::open(&raw, &[("retired/λ", 1)], None).await;
    case.close_after(false, async {}).await;
    let (case, _, _) = Revisions::open(&raw, &[("current", 2)], Some(workspace.clone())).await;
    case.close_after(false, async {}).await;
    Revisions::reject(&raw, &[("retired/λ", 99), ("current", 2)], false).await;
    let (case, workflow, _) =
        Revisions::open(&raw, &[("retired/λ", 1), ("current", 2)], Some(workspace)).await;
    case.close_after(false, async {
        assert_eq!(
            workflow
                .start(ExecutionOptions::default())
                .await
                .unwrap()
                .wait()
                .await
                .unwrap()
                .state
                .values()["count"],
            2
        );
    })
    .await;
}

#[tokio::test]
async fn changing_a_required_pair_rejects_before_generation_or_run_observation_changes() {
    let raw = Revisions::store().await;
    let (case, workflow, workspace) = Revisions::open(&raw, &[("stable", 1)], None).await;
    let (id, (case, workflow, _)) = case
        .close_after(true, async {
            let saved = workflow.prepare(WorkflowState::default()).await.unwrap();
            Revisions::reject(&raw, &[("stable", 2)], false).await;
            let before = saved.snapshot().await.unwrap();
            assert_eq!(before.checkpoint, 0);
            assert!(matches!(before.status, RunStatus::Paused));
            let fresh = Revisions::open(&raw, &[("stable", 1)], Some(workspace)).await;
            (saved.id().clone(), fresh)
        })
        .await;
    case.close_after(false, async {
        let saved = workflow
            .open_run(RunSelector::Latest(id))
            .await
            .unwrap()
            .snapshot()
            .await
            .unwrap();
        assert_eq!(saved.checkpoint, 0);
        assert_eq!(saved.state.values()["count"], 0);
        assert!(matches!(saved.status, RunStatus::Paused));
    })
    .await;
}

#[tokio::test]
async fn a_missing_current_catalog_record_is_corruption_even_without_any_run() {
    let raw = Revisions::store().await;
    let (case, _, _) = Revisions::open(&raw, &[("selected", 1)], None).await;
    case.close_after(false, async {}).await;
    let page = raw
        .list(
            StateListRequest::new("app-kit")
                .unwrap()
                .with_key_prefix(StateKeyPrefix::new("definition/").unwrap())
                .with_limit(2)
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(page.records.len(), 1);
    assert!(page.next_start_after.is_none());
    let original = page.records.into_iter().next().unwrap().record;
    assert_eq!(original.value["workflow_id"], "revision");
    assert_eq!(original.value["revision"], "selected");
    let mut transaction = raw.begin_transaction().await.unwrap();
    transaction.delete(original.key.clone()).await.unwrap();
    transaction.commit().await.unwrap();
    // reject bounds open and any unexpected successful App cleanup separately;
    // an outer observation timeout must not cancel that owned cleanup.
    let observed = AssertUnwindSafe(async {
        Revisions::reject(&raw, &[("selected", 1)], true).await;
    })
    .catch_unwind()
    .await;
    let mut transaction = raw.begin_transaction().await.unwrap();
    transaction.put(original).await.unwrap();
    transaction.commit().await.unwrap();
    match observed {
        Ok(()) => {}
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
