//! Durable action metadata is validated before public projection or recovery.
//! The fixture changes App-owned persisted JSON through the public StateStore
//! transaction contract. One append case additionally supplies a contradictory
//! predecessor response to prove rejection before native commit; that is negative
//! contract evidence, not a Lance snapshot-consistency claim. No private runtime
//! types or helpers are imported.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agents_kit::Agent;
use app_kit::workflow::{
    CheckpointKind, CheckpointSelector, ExecutionOptions, Interaction, RunSelector, RunStatus,
    WorkflowDefinition, WorkflowFailure, WorkflowRun, WorkflowState,
};
use app_kit::{App, AppConfig, AppError, Shutdown, WorkspaceConfig};
use futures_util::FutureExt;
use serde_json::{Value, json};
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_kit::state::record::{
    StateKey, StateListRequest, StateListResponse, StateRecord, StateRecordSnapshot,
};
use storage_kit::{
    StateStore, StateStoreBackend, StateStoreError, StateTransaction, StateTransactionBackend,
};
use tokio::sync::Notify;

#[derive(Clone, Copy, Debug)]
enum Corruption {
    PositionNode,
    PositionOrdinal,
    MissingPosition,
    MissingOutcomes,
    ReversedOrdinals,
    NoncontiguousOrdinals,
    ReversedNodes,
    ChangedControlResult,
    ChangedControlOrdinal,
    DroppedInputSucceeded,
    UnknownNode,
    ExcessOutcomes,
    ExcessResultBytes,
    FrontierDuplicate,
    FrontierUnknownNode,
    FrontierExcess,
    FailureOrdinal,
    FailureNode,
    FailureLineage,
}

struct OutcomeRecovery;

#[derive(Clone, Copy, Debug)]
enum RoutingCorruption {
    UnknownNode,
    UncommittedNode,
    Reverse,
    Duplicate,
    Missing,
    ChangedLineage,
    ExcessSources,
    ExcessLineageBytes,
    MissingSourceAndOutcome,
    MissingWaveAndOutcomes,
}

struct RoutingGate {
    boundary: GateBoundary,
    armed: AtomicBool,
    predecessor: Mutex<Option<StateRecord>>,
    predecessor_rolled_back: AtomicBool,
    predecessor_commit_attempted: AtomicBool,
    entered: Notify,
    release: Notify,
}

#[derive(Clone, Copy)]
enum GateBoundary {
    RoutingRead,
    BeforeAppend,
}

struct RoutingStore {
    inner: StateStore,
    gate: Arc<RoutingGate>,
}

struct RoutingTransaction {
    inner: StateTransaction,
    gate: Arc<RoutingGate>,
    mutation_receipt: bool,
    predecessor_observed: bool,
}

impl RoutingGate {
    async fn observe_routing(&self, snapshot: &Option<StateRecordSnapshot>) {
        if matches!(self.boundary, GateBoundary::RoutingRead)
            && snapshot.as_ref().is_some_and(|snapshot| {
                snapshot.record.value.pointer("/checkpoint/phase/state") == Some(&json!("routing"))
                    && snapshot
                        .record
                        .value
                        .pointer("/checkpoint/outcomes")
                        .and_then(Value::as_array)
                        .is_some_and(|outcomes| outcomes.len() == 2)
            })
            && self.armed.swap(false, Ordering::SeqCst)
        {
            self.entered.notify_one();
            self.release.notified().await;
        }
    }
}

impl StateStoreBackend for RoutingStore {
    type Transaction = RoutingTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        let snapshot = self.inner.load(key).await?;
        self.gate.observe_routing(&snapshot).await;
        Ok(snapshot)
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        self.inner.list(request).await
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        Ok(RoutingTransaction {
            inner: self.inner.begin_transaction().await?,
            gate: Arc::clone(&self.gate),
            mutation_receipt: false,
            predecessor_observed: false,
        })
    }
}

impl StateTransactionBackend for RoutingTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        let receipt = key.namespace().as_str() == "app-kit" && key.key().starts_with("receipt/");
        let mut snapshot = self.inner.load(key).await?;
        self.mutation_receipt |= receipt;
        self.gate.observe_routing(&snapshot).await;
        if self.mutation_receipt
            && matches!(self.gate.boundary, GateBoundary::BeforeAppend)
            && snapshot.as_ref().is_some_and(|snapshot| {
                snapshot.record.value.pointer("/checkpoint/sequence") == Some(&json!(0))
            })
            && self.gate.armed.swap(false, Ordering::SeqCst)
        {
            self.predecessor_observed = true;
            self.gate.entered.notify_one();
            self.gate.release.notified().await;
            // This one negative fixture returns the exact separately persisted
            // contradiction. It deliberately does not claim a fresh read from
            // Lance's original transaction snapshot. Rollback/commit witnesses
            // distinguish App's rejection from a later native stale conflict.
            let replacement = self
                .gate
                .predecessor
                .lock()
                .unwrap()
                .take()
                .ok_or_else(|| {
                    StateStoreError::unavailable(
                        "load",
                        "controlled predecessor read was released during cleanup",
                    )
                })?;
            let observed = snapshot.as_mut().expect("selected checkpoint exists");
            assert_eq!(replacement.key, observed.record.key);
            observed.record = replacement;
        }
        Ok(snapshot)
    }

    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        self.inner.put(record).await
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        self.inner.delete(key).await
    }

    async fn commit(self) -> Result<(), StateStoreError> {
        if self.predecessor_observed {
            self.gate
                .predecessor_commit_attempted
                .store(true, Ordering::SeqCst);
        }
        self.inner.commit().await
    }

    async fn rollback(self) -> Result<(), StateStoreError> {
        self.inner.rollback().await?;
        if self.predecessor_observed {
            self.gate
                .predecessor_rolled_back
                .store(true, Ordering::SeqCst);
        }
        Ok(())
    }
}

impl RoutingCorruption {
    fn apply(self, record: &mut Value, previous: &Value) {
        let sources = record["checkpoint"]["phase"]["sources"]
            .as_array_mut()
            .expect("committed route sources");
        match self {
            Self::UnknownNode => sources[1]["node"] = json!("zz-absent"),
            Self::UncommittedNode => sources[1]["node"] = json!("park"),
            Self::Reverse => sources.reverse(),
            Self::Duplicate => sources[1] = sources[0].clone(),
            Self::Missing => sources.clear(),
            Self::MissingSourceAndOutcome => {
                sources.pop().expect("second selected source");
            }
            Self::MissingWaveAndOutcomes => sources.clear(),
            Self::ChangedLineage => sources[0]["lineage"] = sources[1]["lineage"].clone(),
            Self::ExcessSources => {
                let source = sources[0].clone();
                sources.resize(4_097, source);
            }
            Self::ExcessLineageBytes => sources[0]["lineage"] = json!("x".repeat(1_048_577)),
        }
        match self {
            Self::MissingSourceAndOutcome => {
                record["checkpoint"]["outcomes"]
                    .as_array_mut()
                    .unwrap()
                    .pop()
                    .expect("second committed outcome");
                record["checkpoint"]["position"] = json!({"node":"a", "ordinal":1});
            }
            Self::MissingWaveAndOutcomes => {
                record["checkpoint"]["position"] = previous["checkpoint"]["position"].clone();
                record["checkpoint"]["outcomes"] = previous["checkpoint"]["outcomes"].clone();
            }
            Self::UnknownNode
            | Self::UncommittedNode
            | Self::Reverse
            | Self::Duplicate
            | Self::Missing
            | Self::ChangedLineage
            | Self::ExcessSources
            | Self::ExcessLineageBytes => {}
        }
    }
}

impl Corruption {
    fn failed_wave(self) -> bool {
        matches!(
            self,
            Self::FrontierDuplicate
                | Self::FrontierUnknownNode
                | Self::FrontierExcess
                | Self::FailureOrdinal
                | Self::FailureNode
                | Self::FailureLineage
        )
    }

    fn apply(self, record: &mut Value) {
        let checkpoint = &mut record["checkpoint"];
        match self {
            Self::PositionNode => checkpoint["position"]["node"] = json!("a"),
            Self::PositionOrdinal => checkpoint["position"]["ordinal"] = json!(1),
            Self::MissingPosition => checkpoint["position"] = Value::Null,
            Self::MissingOutcomes => checkpoint["outcomes"] = json!([]),
            Self::ReversedOrdinals => {
                checkpoint["outcomes"][0]["ordinal"] = json!(2);
                checkpoint["outcomes"][1]["ordinal"] = json!(1);
                checkpoint["position"]["ordinal"] = json!(1);
            }
            Self::NoncontiguousOrdinals => {
                checkpoint["outcomes"][1]["ordinal"] = json!(3);
                checkpoint["position"]["ordinal"] = json!(3);
            }
            Self::ReversedNodes => {
                checkpoint["outcomes"][0]["node"] = json!("z");
                checkpoint["outcomes"][1]["node"] = json!("a");
                checkpoint["position"]["node"] = json!("a");
            }
            Self::ChangedControlResult => {
                checkpoint["outcomes"][0]["result"] = json!({"fabricated": true});
            }
            Self::DroppedInputSucceeded => {
                checkpoint["controls"] = json!({});
                checkpoint["phase"] = json!({"state":"succeeded"});
            }
            Self::ChangedControlOrdinal => {
                checkpoint["outcomes"][0]["ordinal"] = json!(11);
                checkpoint["outcomes"][1]["ordinal"] = json!(12);
                checkpoint["position"]["ordinal"] = json!(12);
            }
            Self::UnknownNode => {
                checkpoint["outcomes"][1]["node"] = json!("zz-absent");
                checkpoint["position"]["node"] = json!("zz-absent");
            }
            Self::ExcessOutcomes => {
                checkpoint["outcomes"] = Value::Array(
                    (0..4_097)
                        .map(|ordinal| json!({"node":"a", "ordinal":ordinal, "result":{}}))
                        .collect(),
                );
                checkpoint["position"] = json!({"node":"a", "ordinal":4_096});
            }
            Self::ExcessResultBytes => {
                checkpoint["outcomes"][0]["result"] = json!({"large":"x".repeat(1_048_577)});
            }
            Self::FrontierDuplicate | Self::FrontierUnknownNode => {
                let frontier = checkpoint["frontier"].as_array_mut().expect("failed wave");
                let mut extra = frontier
                    .iter()
                    .find(|activation| activation["node"] == "a")
                    .expect("earlier real activation")
                    .clone();
                match self {
                    Self::FrontierDuplicate => frontier.push(extra),
                    Self::FrontierUnknownNode => {
                        extra["node"] = json!("zz-absent");
                        frontier.push(extra);
                    }
                    _ => unreachable!("selected frontier corruption"),
                }
            }
            Self::FrontierExcess => {
                let frontier = checkpoint["frontier"].as_array_mut().expect("failed wave");
                let failed = frontier
                    .iter()
                    .find(|activation| activation["node"] == "z")
                    .expect("original failed activation")
                    .clone();
                let lineage = failed["lineage"].as_str().unwrap();
                // Distinct bounded identities sort after the real z/2, so
                // neither duplicate rejection nor an altered rank proves this case.
                for index in 0..4_097 - frontier.len() {
                    let mut extra = failed.clone();
                    extra["lineage"] = json!(format!("{lineage}/{index}/z"));
                    frontier.push(extra);
                }
                assert_eq!(frontier.len(), 4_097);
                assert_eq!(
                    frontier
                        .iter()
                        .map(|activation| (
                            activation["node"].as_str().unwrap(),
                            activation["lineage"].as_str().unwrap(),
                        ))
                        .collect::<std::collections::BTreeSet<_>>()
                        .len(),
                    4_097
                );
                assert!(frontier.iter().skip(2).all(|activation| {
                    let added = activation["lineage"].as_str().unwrap();
                    activation["node"] == "z" && added > lineage && added.len() < 128
                }));
            }
            Self::FailureOrdinal => {
                // Keep the real failed index at 2, but assign it to the earlier
                // activation whose original attempted position is 1.
                checkpoint["phase"]["observation"]["node"] = json!("a");
            }
            Self::FailureNode => {
                checkpoint["phase"]["observation"]["node"] = json!("park");
            }
            Self::FailureLineage => {
                let frontier = checkpoint["frontier"].as_array_mut().expect("failed wave");
                let lineage = frontier
                    .iter()
                    .find(|activation| activation["node"] == "a")
                    .expect("earlier real activation")["lineage"]
                    .clone();
                frontier
                    .iter_mut()
                    .find(|activation| activation["node"] == "z")
                    .expect("failed activation")["lineage"] = lineage;
            }
        }
    }
}

impl OutcomeRecovery {
    fn definition(failed_wave: bool) -> WorkflowDefinition {
        let mutations = if failed_wave {
            json!([{"kind":"increment", "target":"count", "value":{"kind":"literal", "value":"not a number"}}])
        } else {
            json!([])
        };
        WorkflowDefinition::parse(
            &json!({
                "schema":"app-kit.workflow/v2", "id":"outcomes", "revision":"r1",
                "state":{"count":0}, "required":["count"],
                "reducers":{"count":"reject_concurrent"}, "entry":"seed",
                "nodes":[
                    {"id":"seed", "step":{"kind":"state", "mutations":[]}},
                    {"id":"z", "step":{"kind":"state", "mutations":mutations}},
                    {"id":"a", "step":{"kind":"state", "mutations":[]}},
                    {"id":"park", "step":{"kind":"input", "message":[{"kind":"text","text":"Keep the latest wave retained"}]}}
                ],
                "routes":{
                    "seed":{"mode":"broadcast", "branches":[
                        {"target":{"kind":"node", "id":"z"}},
                        {"target":{"kind":"node", "id":"a"}}
                    ]},
                    "a":{"mode":"first", "branches":[{"target":{"kind":"node", "id":"park"}}]}
                }
            })
            .to_string(),
        )
        .expect("bounded native wave and parked successor")
    }

    async fn open(
        store: StateStore,
        definition: WorkflowDefinition,
    ) -> Result<App<StateStore, Agent>, AppError> {
        App::open(AppConfig::new(store, Vec::new(), vec![definition])).await
    }

    async fn write(store: &StateStore, records: Vec<StateRecord>) {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("test-owned transaction");
        for record in records {
            assert!(
                transaction
                    .load(record.key.clone())
                    .await
                    .unwrap()
                    .is_some()
            );
            transaction.put(record).await.expect("stage exact test row");
        }
        transaction
            .commit()
            .await
            .expect("commit exact test record");
    }

    async fn rows(store: &StateStore, run: &str, sequence: u64) -> Vec<StateRecord> {
        let records = store
            .list(StateListRequest::new("app-kit").unwrap())
            .await
            .unwrap()
            .records;
        std::iter::once(sequence)
            .chain(sequence.checked_sub(1))
            .map(|sequence| {
                records
                    .iter()
                    .find(|row| {
                        row.record.value["id"] == run
                            && row.record.value.pointer("/checkpoint/sequence")
                                == Some(&json!(sequence))
                    })
                    .expect("exact immutable checkpoint row")
                    .record
                    .clone()
            })
            .collect()
    }

    async fn observe_corruption<CS: StateStoreBackend>(
        store: &StateStore,
        run: &WorkflowRun<CS, Agent>,
        original: Vec<StateRecord>,
        altered: Vec<StateRecord>,
        failed_wave: bool,
    ) -> (bool, bool) {
        let mut accepted = None;
        let observed =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(6), async {
                Self::write(store, altered).await;
                let projection_rejected =
                    matches!(run.snapshot().await, Err(AppError::Integration(_)));
                let reopened = Self::open(store.clone(), Self::definition(failed_wave)).await;
                let recovery_rejected = matches!(&reopened, Err(AppError::Integration(_)));
                accepted = reopened.ok();
                (projection_rejected, recovery_rejected)
            }))
            .catch_unwind()
            .await;

        // A failed open may already fence the old generation. Restore the
        // record and drain the accepted or a fresh root before reporting red.
        Self::write(store, original).await;
        let mut cleanup = match accepted {
            Some(app) => app,
            None => Self::open(store.clone(), Self::definition(failed_wave))
                .await
                .expect("restored record reopens"),
        };
        let closed = std::panic::AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(5),
            cleanup.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        ))
        .catch_unwind()
        .await;
        drop(cleanup);
        let result = match observed {
            Ok(result) => result.expect("corruption observation is bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        };
        match closed {
            Ok(result) => result
                .expect("restored App joins")
                .expect("restored run drains"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        result
    }

    async fn close_fenced<CS: StateStoreBackend>(app: &mut App<CS, Agent>) {
        // Reopen has superseded this generation. shutdown still awaits its
        // retained finalizer/worker join before returning the Closed outcome.
        let result = app
            .shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            })
            .await;
        assert!(
            matches!(result, Ok(()) | Err(AppError::Closed(_))),
            "fenced original App must finish its owned cleanup: {result:?}"
        );
    }

    async fn reject(corruption: Corruption) {
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("real transactional store");
        let failed_wave = corruption.failed_wave();
        let mut app = Self::open(store.clone(), Self::definition(failed_wave))
            .await
            .unwrap();
        let observed = std::panic::AssertUnwindSafe(async {
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().unwrap(),
                })
                .await
                .unwrap();
            let workflow = workspace
                .workflow("outcomes".parse().unwrap())
                .await
                .unwrap();
            let driver = if matches!(corruption,Corruption::DroppedInputSucceeded) {
                workflow.start(ExecutionOptions::default()).await.unwrap()
            } else { workflow.prepare(WorkflowState::default()).await.unwrap() };
            let run = workflow.open_run(RunSelector::Latest(driver.id().clone())).await.unwrap();
            if matches!(corruption,Corruption::DroppedInputSucceeded) {
                tokio::time::timeout(Duration::from_secs(3),async {
                    loop {
                        if matches!(run.snapshot().await.unwrap().interactions.as_slice(),[Interaction::Input { .. }]) { break; }
                        tokio::task::yield_now().await;
                    }
                }).await.expect("actual Input is admitted before exclusive ownership parks its successor");
            }
            drop(driver);
            let admission_deadline = tokio::time::Instant::now()+Duration::from_secs(3);
            loop {
            let mut driver = workflow.open_run(RunSelector::Latest(run.id().clone())).await.unwrap();
            let mut execution = match driver.execution(ExecutionOptions::default()).await {
                Ok(execution) => execution,
                Err(AppError::Busy(_)) if matches!(corruption,Corruption::DroppedInputSucceeded) => {
                    assert!(tokio::time::Instant::now()<admission_deadline,"original background owner releases its turn");
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(error) => panic!("exclusive recovery observation failed: {error}"),
            };
            tokio::time::timeout(Duration::from_secs(3), async {
                while execution.step().await.unwrap().is_some() {}
            })
            .await
            .expect("the real native wave settles before corruption");
            // Retain the idle owner while checking projection and reopen. A
            // background driver may correctly fail the App on these corrupt
            // execution reads, which is a different contract from this test.
            let snapshot = run.snapshot().await.unwrap();
            if failed_wave {
                assert!(matches!(snapshot.status, RunStatus::Failed(WorkflowFailure::State(_))));
                assert_eq!(snapshot.state.values()["count"], 0);
                assert_eq!(snapshot.last_action_ordinal, Some(0));
                assert_eq!(snapshot.outcomes.len(), 1);
                assert_eq!(snapshot.outcomes[0].node, "seed");
                assert_eq!(snapshot.outcomes[0].ordinal, 0);
                let failed = run.checkpoint(CheckpointSelector::Action(2)).await.unwrap();
                assert!(matches!(failed.kind, CheckpointKind::Failed { ref node, ordinal: 2 } if node == "z"));
                assert!(matches!(&failed.status, RunStatus::Failed(WorkflowFailure::State(message))
                    if matches!(&snapshot.status, RunStatus::Failed(WorkflowFailure::State(original)) if original == message)));
                assert_eq!(failed.action_ordinal, Some(0));
                assert_eq!(failed.position.as_deref(), Some("seed"));
                assert_eq!(failed.state.values()["count"], 0);
                assert!(failed.outcomes.is_empty());
            } else {
                assert!(matches!(snapshot.interactions.as_slice(), [Interaction::Input { .. }]));
                assert_eq!(snapshot.last_action_ordinal, Some(2));
                assert_eq!(snapshot.outcomes.iter()
                    .map(|outcome| (outcome.node.as_str(), outcome.ordinal))
                    .collect::<Vec<_>>(), vec![("a", 1), ("z", 2)]);
            }
            let original = Self::rows(&store, &run.id().to_string(), snapshot.checkpoint).await;
            assert_eq!(original.len(), 2);
            assert_eq!(original[0].value["checkpoint"]["base_sequence"],
                original[1].value["checkpoint"]["base_sequence"]);
            assert_eq!(original[0].value["checkpoint"]["outcomes"],
                original[1].value["checkpoint"]["outcomes"]);
            if failed_wave {
                for row in &original {
                    assert_eq!(row.value["checkpoint"]["phase"]["observation"],
                        json!({"node":"z", "ordinal":2}),
                        "both the real failure and its settled successor retain the original attempted position");
                    let frontier = row.value["checkpoint"]["frontier"].as_array().unwrap();
                    assert_eq!(frontier.len(), 2);
                    let a = frontier.iter().find(|activation| activation["node"] == "a").unwrap();
                    let z = frontier.iter().find(|activation| activation["node"] == "z").unwrap();
                    assert_ne!(a["lineage"], z["lineage"], "the lineage edit must change identity");
                }
            }
            if matches!(corruption,Corruption::DroppedInputSucceeded) {
                for row in &original {
                    let controls = row.value["checkpoint"]["controls"].as_object().unwrap();
                    assert_eq!(controls.len(),1);
                    assert!(controls.values().all(|control| control["state"]["kind"] == "input"));
                }
                assert_eq!(original[0].value["checkpoint"]["controls"],original[1].value["checkpoint"]["controls"],
                    "current and immediate predecessor retain the same actual Input owner");
            }
            let mut altered = original.clone();
            corruption.apply(Arc::make_mut(&mut altered[0].value));
            if !matches!(corruption, Corruption::ChangedControlResult
                | Corruption::ChangedControlOrdinal | Corruption::FailureLineage | Corruption::DroppedInputSucceeded) {
                corruption.apply(Arc::make_mut(&mut altered[1].value));
            }
            let rejected =
                Self::observe_corruption(&store, &run, original, altered, failed_wave).await;
            drop(execution);
            break rejected;
            }
        }).catch_unwind().await;
        let closed = std::panic::AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(5),
            Self::close_fenced(&mut app),
        ))
        .catch_unwind()
        .await;
        drop(app);
        let (projection_rejected, recovery_rejected) = match observed {
            Ok(result) => result,
            Err(panic) => std::panic::resume_unwind(panic),
        };
        match closed {
            Ok(result) => result.expect("original App joins after corruption observation"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        assert!(
            projection_rejected,
            "{corruption:?}: snapshot accepted corrupt action metadata"
        );
        assert!(
            recovery_rejected,
            "{corruption:?}: App reopen accepted corrupt action metadata"
        );
    }

    async fn reject_routing(corruption: RoutingCorruption) {
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let gate = Arc::new(RoutingGate {
            boundary: GateBoundary::RoutingRead,
            armed: AtomicBool::new(true),
            predecessor: Mutex::new(None),
            predecessor_rolled_back: AtomicBool::new(false),
            predecessor_commit_attempted: AtomicBool::new(false),
            entered: Notify::new(),
            release: Notify::new(),
        });
        let mut app: App<RoutingStore, Agent> = App::open(AppConfig::new(
            RoutingStore {
                inner: store.clone(),
                gate: Arc::clone(&gate),
            },
            Vec::new(),
            vec![Self::definition(false)],
        ))
        .await
        .unwrap();
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().unwrap(),
            })
            .await
            .unwrap();
        let workflow = workspace
            .workflow("outcomes".parse().unwrap())
            .await
            .unwrap();
        let run = workflow.start(ExecutionOptions::default()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), gate.entered.notified())
            .await
            .expect("driver reaches an actually committed two-action routing boundary");
        let records = store
            .list(StateListRequest::new("app-kit").unwrap())
            .await
            .unwrap()
            .records;
        let sequence = records
            .iter()
            .find(|row| {
                row.record.value["id"] == run.id().to_string()
                    && row.record.value.get("checkpoint").is_none()
            })
            .expect("compact run head")
            .record
            .value["sequence"]
            .as_u64()
            .unwrap();
        let original = Self::rows(&store, &run.id().to_string(), sequence).await;
        let sources = original[0].value["checkpoint"]["phase"]["sources"]
            .as_array()
            .expect("durable selected route sources");
        assert_eq!(
            sources
                .iter()
                .map(|source| source["node"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["a", "z"]
        );
        assert_eq!(original[0].value["checkpoint"]["outcomes"][0]["node"], "a");
        assert_eq!(original[0].value["checkpoint"]["outcomes"][1]["node"], "z");
        assert_ne!(
            sources[0]["lineage"], sources[1]["lineage"],
            "changing to the other selected lineage must change the record"
        );
        let mut altered = original.clone();
        corruption.apply(Arc::make_mut(&mut altered[0].value), &original[1].value);
        let (projection_rejected, recovery_rejected) =
            Self::observe_corruption(&store, &run, original, altered, false).await;
        gate.release.notify_one();
        Self::close_fenced(&mut app).await;
        drop(run);
        drop(workflow);
        drop(workspace);
        drop(app);
        assert!(
            projection_rejected,
            "{corruption:?}: snapshot accepted corrupt route sources"
        );
        assert!(
            recovery_rejected,
            "{corruption:?}: reopen accepted corrupt route sources"
        );
    }

    async fn reject_initial(ordinal: u64) {
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let mut app = Self::open(store.clone(), Self::definition(false))
            .await
            .unwrap();
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().unwrap(),
            })
            .await
            .unwrap();
        let workflow = workspace
            .workflow("outcomes".parse().unwrap())
            .await
            .unwrap();
        let run = workflow
            .prepare(WorkflowState::default())
            .await
            .expect("real prepared admission");
        let initial = run.snapshot().await.unwrap();
        assert_eq!(initial.checkpoint, 0);
        assert!(matches!(initial.status, RunStatus::Paused));
        assert_eq!(initial.last_action_ordinal, None);
        assert!(initial.outcomes.is_empty());
        let original = Self::rows(&store, &run.id().to_string(), 0).await;
        assert!(original[0].value.get("previous").is_none());
        let mut altered = original.clone();
        // Both tuples are individually well formed and name the actual entry.
        // This ordinary initial record has no restored source or local commit.
        Arc::make_mut(&mut altered[0].value)["checkpoint"]["position"] =
            json!({"node":"seed", "ordinal":ordinal});
        Arc::make_mut(&mut altered[0].value)["checkpoint"]["outcomes"] =
            json!([{"node":"seed", "ordinal":ordinal, "result":{}}]);
        let (projection_rejected, recovery_rejected) =
            Self::observe_corruption(&store, &run, original, altered, false).await;
        Self::close_fenced(&mut app).await;
        drop(run);
        drop(workflow);
        drop(workspace);
        drop(app);
        assert!(
            projection_rejected,
            "prepared checkpoint zero fabricated action ordinal {ordinal}"
        );
        assert!(
            recovery_rejected,
            "prepared checkpoint zero reopened with fabricated action ordinal {ordinal}"
        );
    }
}

#[tokio::test]
async fn prepared_checkpoint_zero_cannot_claim_first_action_completion() {
    OutcomeRecovery::reject_initial(0).await;
}

#[tokio::test]
async fn appending_checkpoint_rechecks_its_immutable_predecessor_inside_the_transaction() {
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let gate = Arc::new(RoutingGate {
        boundary: GateBoundary::BeforeAppend,
        armed: AtomicBool::new(false),
        predecessor: Mutex::new(None),
        predecessor_rolled_back: AtomicBool::new(false),
        predecessor_commit_attempted: AtomicBool::new(false),
        entered: Notify::new(),
        release: Notify::new(),
    });
    let mut app: App<RoutingStore, Agent> = App::open(AppConfig::new(
        RoutingStore {
            inner: store.clone(),
            gate: Arc::clone(&gate),
        },
        Vec::new(),
        vec![OutcomeRecovery::definition(false)],
    ))
    .await
    .unwrap();
    let workspace = app
        .create_workspace(WorkspaceConfig {
            name: None,
            root: std::env::current_dir().unwrap(),
        })
        .await
        .unwrap();
    let workflow = workspace
        .workflow("outcomes".parse().unwrap())
        .await
        .unwrap();
    let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
    let original = OutcomeRecovery::rows(&store, &run.id().to_string(), 0).await;
    let head = store
        .list(StateListRequest::new("app-kit").unwrap())
        .await
        .unwrap()
        .records
        .into_iter()
        .find(|row| {
            row.record.value["id"] == run.id().to_string()
                && row.record.value.get("checkpoint").is_none()
        })
        .expect("compact run head");
    let observed = std::panic::AssertUnwindSafe(async {
        gate.armed.store(true, Ordering::SeqCst);
        let mut execution = run.execution(ExecutionOptions::default()).await.unwrap();
        let mut step = Box::pin(execution.step());
        tokio::select! {
            _ = gate.entered.notified() => {}
            result = &mut step => panic!("append did not reach its native predecessor read: {}", result.is_ok()),
            _ = tokio::time::sleep(Duration::from_secs(3)) => panic!("checkpoint append did not read its predecessor"),
        }
        let mut changed = original[0].clone();
        Arc::make_mut(&mut changed.value)["checkpoint"]["state"]["count"] = json!(99);
        OutcomeRecovery::write(&store, vec![changed.clone()]).await;
        *gate.predecessor.lock().unwrap() = Some(changed);
        let unchanged_head = store.load(head.record.key.clone()).await.unwrap().unwrap();
        assert_eq!(unchanged_head.revision, head.revision, "row mutation must not touch the head fence");
        assert_eq!(unchanged_head.record.value, head.record.value);
        gate.release.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(3), &mut step).await
            .expect("append reconciliation is bounded");
        drop(step);
        let current = store.load(head.record.key.clone()).await.unwrap().unwrap();
        assert_eq!(current.record.value["sequence"], 0,
            "an unchanged head revision cannot authorize a stale predecessor row");
        assert!(result.is_err(), "the stale candidate must report rejection");
        assert!(matches!(result, Err(AppError::Conflict(_))),
            "the contradictory immutable predecessor must reject the authored append");
        assert!(gate.predecessor.lock().unwrap().is_none(),
            "the selected transaction consumed the contradictory predecessor response");
        assert!(gate.predecessor_rolled_back.load(Ordering::SeqCst),
            "App completed rollback after rejecting its native predecessor read");
        assert!(!gate.predecessor_commit_attempted.load(Ordering::SeqCst),
            "a stale native commit conflict cannot stand in for predecessor validation");
        let rows = store.list(StateListRequest::new("app-kit").unwrap()).await.unwrap().records;
        assert!(!rows.iter().any(|row| row.record.value["id"] == head.record.value["id"]
            && row.record.value.pointer("/checkpoint/sequence") == Some(&json!(1))),
            "no stale immutable successor may be published");
        drop(execution);
    }).catch_unwind().await;
    gate.release.notify_one();
    OutcomeRecovery::write(&store, original).await;
    let closed = tokio::time::timeout(
        Duration::from_secs(5),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        }),
    )
    .await;
    if let Err(panic) = observed {
        std::panic::resume_unwind(panic);
    }
    closed
        .expect("all append owners join")
        .expect("restored App drains");
}

#[tokio::test]
async fn prepared_checkpoint_zero_cannot_inherit_an_ordinal_without_restore_lineage() {
    OutcomeRecovery::reject_initial(17).await;
}

#[tokio::test]
async fn outcome_position_and_latest_result_must_agree() {
    for corruption in [
        Corruption::PositionNode,
        Corruption::PositionOrdinal,
        Corruption::MissingPosition,
        Corruption::MissingOutcomes,
    ] {
        OutcomeRecovery::reject(corruption).await;
    }
}

#[tokio::test]
async fn recovered_wave_ordinals_and_nodes_are_canonical() {
    for corruption in [
        Corruption::ReversedOrdinals,
        Corruption::NoncontiguousOrdinals,
        Corruption::ReversedNodes,
    ] {
        OutcomeRecovery::reject(corruption).await;
    }
}

#[tokio::test]
async fn control_checkpoint_cannot_change_retained_action_metadata() {
    for corruption in [
        Corruption::ChangedControlResult,
        Corruption::ChangedControlOrdinal,
    ] {
        OutcomeRecovery::reject(corruption).await;
    }
}

#[tokio::test]
async fn recovered_outcomes_must_name_nodes_in_the_exact_definition() {
    OutcomeRecovery::reject(Corruption::UnknownNode).await;
}

#[tokio::test]
async fn recovered_outcomes_preserve_existing_activation_count_and_result_bounds() {
    for corruption in [Corruption::ExcessOutcomes, Corruption::ExcessResultBytes] {
        OutcomeRecovery::reject(corruption).await;
    }
}

#[tokio::test]
async fn failed_observation_frontier_rejects_duplicate_activation_reservations() {
    OutcomeRecovery::reject(Corruption::FrontierDuplicate).await;
}

#[tokio::test]
async fn failed_observation_frontier_rejects_unknown_activation_reservations() {
    OutcomeRecovery::reject(Corruption::FrontierUnknownNode).await;
}

#[tokio::test]
async fn failed_observation_frontier_preserves_the_activation_capacity_bound() {
    OutcomeRecovery::reject(Corruption::FrontierExcess).await;
}

#[tokio::test]
async fn failed_observation_cannot_assign_a_later_ordinal_to_the_earlier_activation() {
    OutcomeRecovery::reject(Corruption::FailureOrdinal).await;
}

#[tokio::test]
async fn failed_observation_cannot_name_an_unstarted_future_wave_node() {
    OutcomeRecovery::reject(Corruption::FailureNode).await;
}

#[tokio::test]
async fn failed_observation_successor_cannot_change_the_selected_lineage() {
    OutcomeRecovery::reject(Corruption::FailureLineage).await;
}

#[tokio::test]
async fn routing_sources_reject_unknown_nodes() {
    OutcomeRecovery::reject_routing(RoutingCorruption::UnknownNode).await;
}

#[tokio::test]
async fn routing_sources_reject_known_but_uncommitted_nodes() {
    OutcomeRecovery::reject_routing(RoutingCorruption::UncommittedNode).await;
}

#[tokio::test]
async fn routing_sources_retain_canonical_order() {
    OutcomeRecovery::reject_routing(RoutingCorruption::Reverse).await;
}

#[tokio::test]
async fn routing_sources_reject_duplicate_activations() {
    OutcomeRecovery::reject_routing(RoutingCorruption::Duplicate).await;
}

#[tokio::test]
async fn routing_sources_cannot_drop_the_committed_wave() {
    OutcomeRecovery::reject_routing(RoutingCorruption::Missing).await;
}

#[tokio::test]
async fn routing_sources_retain_exact_selected_lineages() {
    OutcomeRecovery::reject_routing(RoutingCorruption::ChangedLineage).await;
}

#[tokio::test]
async fn routing_sources_preserve_the_wave_capacity_bound() {
    OutcomeRecovery::reject_routing(RoutingCorruption::ExcessSources).await;
}

#[tokio::test]
async fn routing_sources_preserve_the_lineage_byte_bound() {
    OutcomeRecovery::reject_routing(RoutingCorruption::ExcessLineageBytes).await;
}

#[tokio::test]
async fn routing_sources_cannot_erase_one_committed_source_and_its_outcome() {
    OutcomeRecovery::reject_routing(RoutingCorruption::MissingSourceAndOutcome).await;
}

#[tokio::test]
async fn routing_sources_cannot_erase_the_wave_and_restore_its_prior_outcomes() {
    OutcomeRecovery::reject_routing(RoutingCorruption::MissingWaveAndOutcomes).await;
}

#[tokio::test]
async fn outgoing_condition_failure_preserves_the_committed_serial_action() {
    // Pinned rurr: session.rs873–909 returns the committed action; the next
    // ExecutionPlan resolves outgoing conditions through state_meta.rs85–91.
    // step_txn.rs710–744 commits state and its ordinal before that next plan.
    let definition = WorkflowDefinition::parse(&json!({
        "schema":"app-kit.workflow/v2", "id":"outcomes", "revision":"route-error",
        "state":{"count":0}, "required":["count"], "reducers":{"count":"reject_concurrent"},
        "entry":"commit", "nodes":[
            {"id":"commit", "step":{"kind":"state", "mutations":[{"kind":"assign", "target":"count", "value":{"kind":"literal", "value":7}}]}},
            {"id":"unreached", "step":{"kind":"state", "mutations":[{"kind":"assign", "target":"count", "value":{"kind":"literal", "value":99}}]}}
        ], "routes":{"commit":{"mode":"first", "branches":[
            {"when":{"kind":"equal", "left":{"kind":"state", "path":"missing"}, "right":{"kind":"literal", "value":1}}, "target":{"kind":"node", "id":"unreached"}}
        ]}}
    }).to_string()).expect("well-formed condition whose required read fails at runtime");
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let mut app = OutcomeRecovery::open(store, definition).await.unwrap();
    let workspace = app
        .create_workspace(WorkspaceConfig {
            name: None,
            root: std::env::current_dir().unwrap(),
        })
        .await
        .unwrap();
    let workflow = workspace
        .workflow("outcomes".parse().unwrap())
        .await
        .unwrap();
    let run = workflow.start(ExecutionOptions::default()).await.unwrap();
    let failed = tokio::time::timeout(Duration::from_secs(3), run.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        failed.status,
        RunStatus::Failed(WorkflowFailure::Evaluation { .. })
    ));
    assert_eq!(
        failed.state.values()["count"],
        7,
        "a later route failure cannot roll back the successful action"
    );
    assert_eq!(failed.last_action_ordinal, Some(0));
    assert_eq!(failed.outcomes.len(), 1);
    assert_eq!(failed.outcomes[0].node, "commit");
    assert_eq!(failed.outcomes[0].ordinal, 0);
    assert!(failed.outcomes[0].result.is_empty());
    app.shutdown(Shutdown::CancelAndDrain {
        timeout: Duration::from_secs(3),
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn terminal_success_cannot_discard_an_unresolved_input_control() {
    OutcomeRecovery::reject(Corruption::DroppedInputSucceeded).await;
}
